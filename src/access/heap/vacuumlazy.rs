//! vacuumlazy.rs
//!   Concurrent ("lazy") vacuuming.
//!
//! Heap relations are vacuumed in three main phases. In phase I, vacuum scans
//! relation pages, pruning and freezing tuples and saving dead tuples' TIDs in
//! a TID store. If that TID store fills up or vacuum finishes scanning the
//! relation, it progresses to phase II: index vacuuming. Index vacuuming
//! deletes the dead index entries referenced in the TID store. In phase III,
//! vacuum scans the blocks of the relation referred to by the TIDs in the TID
//! store and reaps the corresponding dead items, freeing that space for future
//! tuples.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/heap/vacuumlazy.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_parens)]
#![allow(unused_imports)]
#![allow(unreachable_code)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::uint8;
use crate::c::uint16;
use crate::c::uint32;
use crate::c::int32;
use crate::c::int64;
use crate::c::Size;
use crate::c::MultiXactId;
use crate::c::TransactionId;

use crate::postgres_ext::Oid;

use crate::pg_config::BLCKSZ;

use crate::access::htup_details::{
    HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleHeaderData,
    MaxHeapTuplesPerPage,
    HeapTupleHeaderGetXmin, HeapTupleHeaderXminCommitted,
};
use crate::access::htup_details::*;

use crate::access::transam::InvalidTransactionId;
use crate::access::transam::TransactionIdIsNormal;
use crate::access::transam::TransactionIdIsValid;
use crate::access::transam::transam::TransactionIdFollows;
use crate::access::transam::transam::TransactionIdPrecedes;
use crate::access::transam::transam::TransactionIdPrecedesOrEquals;

use crate::storage::block::BlockNumber;
use crate::storage::block::InvalidBlockNumber;
use crate::storage::block::BlockNumberIsValid;

use crate::storage::off::{
    OffsetNumber, FirstOffsetNumber, InvalidOffsetNumber, MaxOffsetNumber,
    OffsetNumberIsValid, OffsetNumberNext,
};

use crate::storage::itemid::{
    ItemId, ItemIdData,
    ItemIdGetLength, ItemIdHasStorage, ItemIdIsNormal,
    ItemIdIsRedirected, ItemIdIsUsed,
};

use crate::storage::itemptr::{ItemPointer, ItemPointerData};

use crate::storage::bufpage::Page;

use crate::utils::rel::{Relation, RelationGetRelid, RelationGetRelationName};

use crate::utils::snapshot::GlobalVisState;

// from access/heap/pruneheap.rs
use crate::access::heap::pruneheap::{
    PruneFreezeResult, VacuumCutoffs,
};

// from commands/vacuumparallel.rs
use crate::commands::vacuumparallel::{
    VacDeadItemsInfo, ParallelVacuumState,
};

// from access/common/tidstore.rs
use crate::access::common::tidstore::{
    TidStore, TidStoreIter, TidStoreIterResult,
};

// from storage/buf (buffer types)
use crate::storage::buf::Buffer;
use crate::storage::buf::BufferAccessStrategy;
use crate::storage::buf::InvalidBuffer;

// from access/index/genam.rs
use crate::access::index::genam::{IndexBulkDeleteResult, IndexVacuumInfo};

// ========================================================================
// Constants
// ========================================================================

/*
 * Space/time tradeoff parameters: do these need to be user-tunable?
 *
 * To consider truncating the relation, we want there to be at least
 * REL_TRUNCATE_MINIMUM or (relsize / REL_TRUNCATE_FRACTION) (whichever
 * is less) potentially-freeable pages.
 */
const REL_TRUNCATE_MINIMUM: BlockNumber = 1000;
const REL_TRUNCATE_FRACTION: BlockNumber = 16;

/*
 * Timing parameters for truncate locking heuristics.
 */
const VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL: c_int = 20; /* ms */
const VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL:  c_int = 50; /* ms */
const VACUUM_TRUNCATE_LOCK_TIMEOUT:        c_int = 5000; /* ms */

/*
 * Threshold that controls whether we bypass index vacuuming and heap
 * vacuuming as an optimization
 */
const BYPASS_THRESHOLD_PAGES: f64 = 0.02; /* i.e. 2% of rel_pages */

/*
 * Perform a failsafe check each time we scan another 4GB of pages.
 */
fn FAILSAFE_EVERY_PAGES() -> BlockNumber {
    (((4u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) & 0xFFFF_FFFF) as BlockNumber
}

/*
 * When a table has no indexes, vacuum the FSM after every 8GB, approximately.
 */
fn VACUUM_FSM_EVERY_PAGES() -> BlockNumber {
    (((8u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) & 0xFFFF_FFFF) as BlockNumber
}

/*
 * Before we consider skipping a page that's marked as clean in
 * visibility map, we must've seen at least this many clean pages.
 */
const SKIP_PAGES_THRESHOLD: BlockNumber = 32;

/*
 * Size of the prefetch window for lazy vacuum backwards truncation scan.
 * Needs to be a power of 2.
 */
const PREFETCH_SIZE: BlockNumber = 32;

/*
 * Macro to check if we are in a parallel vacuum.  If true, we are in the
 * parallel mode and the DSM segment is initialized.
 */
macro_rules! ParallelVacuumIsActive {
    ($vacrel:expr) => {
        unsafe { !(*$vacrel).pvs.is_null() }
    };
}

/* VAC_BLK_* flags set in per_buffer_data */
const VAC_BLK_WAS_EAGER_SCANNED:           uint8 = 1 << 0;
const VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM: uint8 = 1 << 1;

/*
 * An eager scan of a page that is set all-frozen in the VM is considered
 * "successful".
 */
const MAX_EAGER_FREEZE_SUCCESS_RATE: f64 = 0.2;

/*
 * Size of each eager scan region.
 */
const EAGER_SCAN_REGION_SIZE: BlockNumber = 4096;

// ========================================================================
// Stub types (unported dependencies)
// ========================================================================

/* TODO(pg-port): real VacuumParams lives in commands/vacuum.h */
#[repr(C)]
pub struct VacuumParams {
    pub options: c_int,
    pub log_min_duration: c_int,
    pub is_wraparound: bool,
    pub index_cleanup: c_int,    /* VacOptValue */
    pub truncate: c_int,         /* VacOptValue */
    pub nworkers: c_int,
    pub max_eager_freeze_failure_rate: f64,
}

/* VacOptValue constants -- TODO(pg-port): commands/vacuum.h */
pub const VACOPTVALUE_UNSPECIFIED: c_int = 0;
pub const VACOPTVALUE_AUTO:        c_int = 1;
pub const VACOPTVALUE_DISABLED:    c_int = 2;
pub const VACOPTVALUE_ENABLED:     c_int = 3;

/* VACOPT_* flags -- TODO(pg-port): commands/vacuum.h */
pub const VACOPT_VERBOSE:              c_int = 1 << 0;
pub const VACOPT_DISABLE_PAGE_SKIPPING: c_int = 1 << 3;

/* TODO(pg-port): real BufferIsValid in storage/buf.h */
#[inline]
unsafe fn BufferIsValid(buf: Buffer) -> bool {
    buf != InvalidBuffer
}

/* TODO(pg-port): real ReadStream opaque type in storage/aio/read_stream.h */
pub type ReadStream = c_void;

/* TODO(pg-port): real instr_time in portability/instr_time.h */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct instr_time {
    pub t: u64,
}

/* TODO(pg-port): real PGRUsage in utils/pg_rusage.h */
#[repr(C)]
pub struct PGRUsage {
    _private: [u8; 0],
}

/* TODO(pg-port): real TimestampTz in utils/timestamp.h */
pub type TimestampTz = i64;

/* TODO(pg-port): real PgStat_Counter */
pub type PgStat_Counter = i64;

/* TODO(pg-port): real WalUsage in executor/instrument.h */
#[repr(C)]
pub struct WalUsage {
    pub wal_records: int64,
    pub wal_fpi:     int64,
    pub wal_bytes:   u64,
    pub wal_buffers_full: int64,
}

/* TODO(pg-port): real BufferUsage in executor/instrument.h */
#[repr(C)]
pub struct BufferUsage {
    pub shared_blks_hit:      int64,
    pub shared_blks_read:     int64,
    pub shared_blks_dirtied:  int64,
    pub local_blks_hit:       int64,
    pub local_blks_read:      int64,
    pub local_blks_dirtied:   int64,
}

/* TODO(pg-port): real StringInfoData in lib/stringinfo.h */
#[repr(C)]
pub struct StringInfoData {
    pub data:   *mut c_char,
    pub len:    c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

/* TODO(pg-port): real ErrorContextCallback in utils/elog.h */
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe fn(arg: *mut c_void)>,
    pub arg:      *mut c_void,
}

/* Phases of vacuum during which we report error context. */
pub type VacErrPhase = c_int;
pub const VACUUM_ERRCB_PHASE_UNKNOWN:       VacErrPhase = 0;
pub const VACUUM_ERRCB_PHASE_SCAN_HEAP:     VacErrPhase = 1;
pub const VACUUM_ERRCB_PHASE_VACUUM_INDEX:  VacErrPhase = 2;
pub const VACUUM_ERRCB_PHASE_VACUUM_HEAP:   VacErrPhase = 3;
pub const VACUUM_ERRCB_PHASE_INDEX_CLEANUP: VacErrPhase = 4;
pub const VACUUM_ERRCB_PHASE_TRUNCATE:      VacErrPhase = 5;

// ========================================================================
// LVRelState -- the central vacuum state struct
// ========================================================================

/*
 * LVRelState -- working state for the vacuum of a relation.
 */
#[repr(C)]
pub struct LVRelState {
    /* Target heap relation and its indexes */
    pub rel:      Relation,
    pub indrels:  *mut Relation,
    pub nindexes: c_int,

    /* Buffer access strategy and parallel vacuum state */
    pub bstrategy: BufferAccessStrategy,
    pub pvs:       *mut ParallelVacuumState,

    /* Aggressive VACUUM? (must set relfrozenxid >= FreezeLimit) */
    pub aggressive: bool,
    /* Use visibility map to skip? (disabled by DISABLE_PAGE_SKIPPING) */
    pub skipwithvm: bool,
    /* Consider index vacuuming bypass optimization? */
    pub consider_bypass_optimization: bool,

    /* Doing index vacuuming, index cleanup, rel truncation? */
    pub do_index_vacuuming: bool,
    pub do_index_cleanup:   bool,
    pub do_rel_truncate:    bool,

    /* VACUUM operation's cutoffs for freezing and pruning */
    pub cutoffs: VacuumCutoffs,
    pub vistest: *mut GlobalVisState,
    /* Tracks oldest extant XID/MXID for setting relfrozenxid/relminmxid */
    pub NewRelfrozenXid: TransactionId,
    pub NewRelminMxid:   MultiXactId,
    pub skippedallvis:   bool,

    /* Error reporting state */
    pub dbname:       *mut c_char,
    pub relnamespace: *mut c_char,
    pub relname:      *mut c_char,
    pub indname:      *mut c_char,  /* Current index name */
    pub blkno:        BlockNumber,  /* used only for heap operations */
    pub offnum:       OffsetNumber, /* used only for heap operations */
    pub phase:        VacErrPhase,
    pub verbose:      bool,         /* VACUUM VERBOSE? */

    /*
     * dead_items stores TIDs whose index tuples are deleted by index
     * vacuuming.
     */
    pub dead_items:      *mut TidStore,
    pub dead_items_info: *mut VacDeadItemsInfo,

    pub rel_pages:    BlockNumber, /* total number of pages */
    pub scanned_pages: BlockNumber, /* # pages examined (not skipped via VM) */

    /*
     * Count of all-visible blocks eagerly scanned (for logging only).
     */
    pub eager_scanned_pages:       BlockNumber,
    pub removed_pages:             BlockNumber, /* # pages removed by relation truncation */
    pub new_frozen_tuple_pages:    BlockNumber, /* # pages with newly frozen tuples */

    /* # pages newly set all-visible in the VM */
    pub vm_new_visible_pages: BlockNumber,

    /*
     * # pages newly set all-visible and all-frozen in the VM.
     */
    pub vm_new_visible_frozen_pages: BlockNumber,

    /* # all-visible pages newly set all-frozen in the VM */
    pub vm_new_frozen_pages: BlockNumber,

    pub lpdead_item_pages:   BlockNumber, /* # pages with LP_DEAD items */
    pub missed_dead_pages:   BlockNumber, /* # pages with missed dead tuples */
    pub nonempty_pages:      BlockNumber, /* actually, last nonempty page + 1 */

    /* Statistics output by us, for table */
    pub new_rel_tuples:  f64, /* new estimated total # of tuples */
    pub new_live_tuples: f64, /* new estimated total # of live tuples */
    /* Statistics output by index AMs */
    pub indstats: *mut *mut IndexBulkDeleteResult,

    /* Instrumentation counters */
    pub num_index_scans:     c_int,
    /* Counters that follow are only for scanned_pages */
    pub tuples_deleted:        int64, /* # deleted from table */
    pub tuples_frozen:         int64, /* # newly frozen */
    pub lpdead_items:          int64, /* # deleted from indexes */
    pub live_tuples:           int64, /* # live tuples remaining */
    pub recently_dead_tuples:  int64, /* # dead, but not yet removable */
    pub missed_dead_tuples:    int64, /* # removable, but not removed */

    /* State maintained by heap_vac_scan_next_block() */
    pub current_block:                  BlockNumber,  /* last block returned */
    pub next_unskippable_block:         BlockNumber,  /* next unskippable block */
    pub next_unskippable_allvis:        bool,         /* its visibility status */
    pub next_unskippable_eager_scanned: bool,         /* if it was eagerly scanned */
    pub next_unskippable_vmbuffer:      Buffer,       /* buffer containing its VM bit */

    /* State related to managing eager scanning of all-visible pages */

    /*
     * A normal vacuum that has failed to freeze too many eagerly scanned
     * blocks in a region suspends eager scanning.
     * next_eager_scan_region_start is the block number of the first block
     * eligible for resumed eager scanning.
     *
     * When eager scanning is permanently disabled, either initially
     * (including for aggressive vacuum) or due to hitting the success cap,
     * this is set to InvalidBlockNumber.
     */
    pub next_eager_scan_region_start: BlockNumber,

    /*
     * The remaining number of blocks a normal vacuum will consider eager
     * scanning when it is successful.
     */
    pub eager_scan_remaining_successes: BlockNumber,

    /*
     * The maximum number of blocks which may be eagerly scanned and not
     * frozen before eager scanning is temporarily suspended.
     */
    pub eager_scan_max_fails_per_region: BlockNumber,

    /*
     * The number of eagerly scanned blocks vacuum failed to freeze (due to
     * age) in the current eager scan region.
     */
    pub eager_scan_remaining_fails: BlockNumber,
}

/* Struct for saving and restoring vacuum error information. */
#[repr(C)]
pub struct LVSavedErrInfo {
    pub blkno:  BlockNumber,
    pub offnum: OffsetNumber,
    pub phase:  VacErrPhase,
}

// ========================================================================
// Stub functions for unported dependencies
// ========================================================================

/* TODO(pg-port): real palloc0 in prelude */
unsafe fn palloc0_lvstate() -> *mut LVRelState {
    let sz = core::mem::size_of::<LVRelState>();
    let ptr = palloc0(sz) as *mut LVRelState;
    ptr
}

/* TODO(pg-port): real get_database_name in catalog/dbcommands.h */
unsafe fn get_database_name(_dbid: Oid) -> *mut c_char {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real get_namespace_name in utils/lsyscache.h */
unsafe fn get_namespace_name(_ns: Oid) -> *mut c_char {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real pstrdup in prelude/palloc */
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real pfree in prelude/palloc */
unsafe fn pfree(_ptr: *mut c_void) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real MyDatabaseId in miscadmin.h */
static mut MyDatabaseId: Oid = 0;

/* TODO(pg-port): real vac_open_indexes in commands/vacuum.c */
unsafe fn vac_open_indexes(
    _rel: Relation,
    _lockmode: c_int,
    _nindexes: *mut c_int,
    _indrels: *mut *mut Relation,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real vac_close_indexes in commands/vacuum.c */
unsafe fn vac_close_indexes(_nindexes: c_int, _indrels: *mut Relation, _lockmode: c_int) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real RelationGetNamespace in utils/rel.h */
unsafe fn RelationGetNamespace(_rel: Relation) -> Oid {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real AmAutoVacuumWorkerProcess in postmaster/autovacuum.h */
unsafe fn AmAutoVacuumWorkerProcess() -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real autovacuum_work_mem in postmaster/autovacuum.h */
static mut autovacuum_work_mem: c_int = -1;

/* TODO(pg-port): real maintenance_work_mem in utils/guc.h */
static mut maintenance_work_mem: c_int = 65536;

/* TODO(pg-port): real track_io_timing in utils/guc.h */
static mut track_io_timing: bool = false;

/* TODO(pg-port): real track_cost_delay_timing in utils/guc.h */
static mut track_cost_delay_timing: bool = false;

/* TODO(pg-port): real VacuumFailsafeActive in commands/vacuum.c */
pub static mut VacuumFailsafeActive: bool = false;

/* TODO(pg-port): real VacuumCostActive in utils/guc.h */
static mut VacuumCostActive: bool = false;

/* TODO(pg-port): real VacuumCostBalance in utils/guc.h */
static mut VacuumCostBalance: c_int = 0;

/* TODO(pg-port): real pg_rusage_init in utils/pg_rusage.h */
unsafe fn pg_rusage_init(_ru0: *mut PGRUsage) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real pg_rusage_show in utils/pg_rusage.h */
unsafe fn pg_rusage_show(_ru0: *const PGRUsage) -> *const c_char {
    std::ptr::null() /* TODO(pg-port) */
}

/* TODO(pg-port): real pgStatBlockReadTime in pgstat.h */
static mut pgStatBlockReadTime:  PgStat_Counter = 0;
/* TODO(pg-port): real pgStatBlockWriteTime in pgstat.h */
static mut pgStatBlockWriteTime: PgStat_Counter = 0;

/* TODO(pg-port): real pgWalUsage in executor/instrument.c */
static mut pgWalUsage: WalUsage = WalUsage {
    wal_records: 0,
    wal_fpi: 0,
    wal_bytes: 0,
    wal_buffers_full: 0,
};

/* TODO(pg-port): real pgBufferUsage in executor/instrument.c */
static mut pgBufferUsage: BufferUsage = BufferUsage {
    shared_blks_hit: 0,
    shared_blks_read: 0,
    shared_blks_dirtied: 0,
    local_blks_hit: 0,
    local_blks_read: 0,
    local_blks_dirtied: 0,
};

/* TODO(pg-port): real WalUsageAccumDiff in executor/instrument.c */
unsafe fn WalUsageAccumDiff(
    _dst: *mut WalUsage,
    _add: *const WalUsage,
    _sub: *const WalUsage,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real BufferUsageAccumDiff in executor/instrument.c */
unsafe fn BufferUsageAccumDiff(
    _dst: *mut BufferUsage,
    _add: *const BufferUsage,
    _sub: *const BufferUsage,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real pgstat_progress_start_command in pgstat.h */
unsafe fn pgstat_progress_start_command(_cmd: c_int, _relid: Oid) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real pgstat_progress_end_command in pgstat.h */
unsafe fn pgstat_progress_end_command() {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real pgstat_progress_update_param in pgstat.h */
unsafe fn pgstat_progress_update_param(_index: c_int, _val: int64) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real pgstat_progress_update_multi_param in pgstat.h */
unsafe fn pgstat_progress_update_multi_param(
    _nparam: c_int,
    _index: *const c_int,
    _val: *const int64,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): progress parameter constants (commands/progress.h) */
const PROGRESS_COMMAND_VACUUM:                c_int = 2;
const PROGRESS_VACUUM_PHASE:                  c_int = 0;
const PROGRESS_VACUUM_TOTAL_HEAP_BLKS:        c_int = 1;
const PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES:   c_int = 2;
const PROGRESS_VACUUM_HEAP_BLKS_SCANNED:      c_int = 3;
const PROGRESS_VACUUM_HEAP_BLKS_VACUUMED:     c_int = 4;
const PROGRESS_VACUUM_NUM_INDEX_VACUUMS:      c_int = 5;
const PROGRESS_VACUUM_INDEXES_TOTAL:          c_int = 6;
const PROGRESS_VACUUM_INDEXES_PROCESSED:      c_int = 7;
const PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS:      c_int = 8;
const PROGRESS_VACUUM_DEAD_TUPLE_BYTES:       c_int = 9;
const PROGRESS_VACUUM_DELAY_TIME:             c_int = 10;
const PROGRESS_VACUUM_PHASE_SCAN_HEAP:        c_int = 1;
const PROGRESS_VACUUM_PHASE_VACUUM_INDEX:     c_int = 2;
const PROGRESS_VACUUM_PHASE_VACUUM_HEAP:      c_int = 3;
const PROGRESS_VACUUM_PHASE_INDEX_CLEANUP:    c_int = 4;
const PROGRESS_VACUUM_PHASE_TRUNCATE:         c_int = 5;
const PROGRESS_VACUUM_PHASE_FINAL_CLEANUP:    c_int = 6;

/* TODO(pg-port): real error_context_stack in utils/elog.h */
static mut error_context_stack: *mut ErrorContextCallback = std::ptr::null_mut();

/* TODO(pg-port): real vacuum_error_callback -- defined below */

/* TODO(pg-port): vacuum_get_cutoffs in commands/vacuum.c */
unsafe fn vacuum_get_cutoffs(
    _rel: Relation,
    _params: *mut VacuumParams,
    _cutoffs: *mut VacuumCutoffs,
) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real RelationGetNumberOfBlocks in utils/rel.h */
unsafe fn RelationGetNumberOfBlocks(_rel: Relation) -> BlockNumber {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real GlobalVisTestFor in utils/snapshot.h */
unsafe fn GlobalVisTestFor(_rel: Relation) -> *mut GlobalVisState {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real vac_update_relstats in commands/vacuum.c */
unsafe fn vac_update_relstats(
    _rel: Relation,
    _num_pages: BlockNumber,
    _num_tuples: f64,
    _num_allvisible: BlockNumber,
    _num_allfrozen: BlockNumber,
    _hasindex: bool,
    _frozenxid: TransactionId,
    _minmulti: MultiXactId,
    _frozenxid_updated: *mut bool,
    _minmulti_updated: *mut bool,
    _is_shared: bool,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real pgstat_report_vacuum in pgstat.h */
unsafe fn pgstat_report_vacuum(
    _relid: Oid,
    _shared: bool,
    _live_tuples: int64,
    _dead_tuples: int64,
    _starttime: TimestampTz,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real GetCurrentTimestamp in utils/timestamp.h */
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real TimestampDifference in utils/timestamp.h */
unsafe fn TimestampDifference(
    _start: TimestampTz,
    _end: TimestampTz,
    _secs: *mut i64,
    _usecs: *mut c_int,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real TimestampDifferenceExceeds in utils/timestamp.h */
unsafe fn TimestampDifferenceExceeds(
    _start: TimestampTz,
    _end: TimestampTz,
    _ms: c_int,
) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real ReadNextTransactionId in access/transam.h */
unsafe fn ReadNextTransactionId() -> TransactionId {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real visibilitymap_count in access/visibilitymap.h */
unsafe fn visibilitymap_count(
    _rel: Relation,
    _all_visible: *mut BlockNumber,
    _all_frozen: *mut BlockNumber,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real visibilitymap_pin in access/visibilitymap.h */
unsafe fn visibilitymap_pin(_rel: Relation, _blkno: BlockNumber, _vmbuffer: *mut Buffer) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real visibilitymap_get_status in access/visibilitymap.h */
unsafe fn visibilitymap_get_status(
    _rel: Relation,
    _blkno: BlockNumber,
    _vmbuffer: *mut Buffer,
) -> uint8 {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real visibilitymap_set in access/visibilitymap.h */
unsafe fn visibilitymap_set(
    _rel: Relation,
    _blkno: BlockNumber,
    _buf: Buffer,
    _lsn: u64,
    _vmbuffer: Buffer,
    _cutoff_xid: TransactionId,
    _flags: uint8,
) -> uint8 {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real visibilitymap_clear in access/visibilitymap.h */
unsafe fn visibilitymap_clear(
    _rel: Relation,
    _blkno: BlockNumber,
    _vmbuffer: Buffer,
    _flags: uint8,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): visibility map flags (access/visibilitymap.h) */
const VISIBILITYMAP_ALL_VISIBLE: uint8 = 1;
const VISIBILITYMAP_ALL_FROZEN:  uint8 = 2;
const VISIBILITYMAP_VALID_BITS:  uint8 = 3;

/* TODO(pg-port): real VM_ALL_FROZEN macro */
unsafe fn VM_ALL_FROZEN(_rel: Relation, _blkno: BlockNumber, _vmbuffer: *mut Buffer) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real pg_prng_uint32 in common/pg_prng.h */
unsafe fn pg_prng_uint32(_state: *mut c_void) -> u32 {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real pg_global_prng_state in common/pg_prng.h */
static mut pg_global_prng_state: u64 = 0;

/* TODO(pg-port): real vacuum_xid_failsafe_check in commands/vacuum.c */
unsafe fn vacuum_xid_failsafe_check(_cutoffs: *const VacuumCutoffs) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real vacuum_delay_point in commands/vacuum.c */
unsafe fn vacuum_delay_point(_is_analyze: bool) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real vac_estimate_reltuples in commands/vacuum.c */
unsafe fn vac_estimate_reltuples(
    _rel: Relation,
    _total_pages: BlockNumber,
    _scanned_pages: BlockNumber,
    _live_tuples: int64,
) -> f64 {
    0.0 /* TODO(pg-port) */
}

/* TODO(pg-port): real vac_bulkdel_one_index in commands/vacuum.c */
unsafe fn vac_bulkdel_one_index(
    _ivinfo: *mut IndexVacuumInfo,
    _istat: *mut IndexBulkDeleteResult,
    _dead_items: *mut TidStore,
    _dead_items_info: *mut VacDeadItemsInfo,
) -> *mut IndexBulkDeleteResult {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real vac_cleanup_one_index in commands/vacuum.c */
unsafe fn vac_cleanup_one_index(
    _ivinfo: *mut IndexVacuumInfo,
    _istat: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real parallel_vacuum_init in commands/vacuumparallel.c */
unsafe fn parallel_vacuum_init(
    _rel: Relation,
    _indrels: *mut Relation,
    _nindexes: c_int,
    _nworkers: c_int,
    _vac_work_mem: c_int,
    _elevel: c_int,
    _bstrategy: BufferAccessStrategy,
) -> *mut ParallelVacuumState {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real parallel_vacuum_end in commands/vacuumparallel.c */
unsafe fn parallel_vacuum_end(
    _pvs: *mut ParallelVacuumState,
    _indstats: *mut *mut IndexBulkDeleteResult,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real parallel_vacuum_get_dead_items in commands/vacuumparallel.c */
unsafe fn parallel_vacuum_get_dead_items(
    _pvs: *mut ParallelVacuumState,
    _dead_items_info: *mut *mut VacDeadItemsInfo,
) -> *mut TidStore {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real parallel_vacuum_reset_dead_items in commands/vacuumparallel.c */
unsafe fn parallel_vacuum_reset_dead_items(_pvs: *mut ParallelVacuumState) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real parallel_vacuum_bulkdel_all_indexes in commands/vacuumparallel.c */
unsafe fn parallel_vacuum_bulkdel_all_indexes(
    _pvs: *mut ParallelVacuumState,
    _old_live_tuples: f64,
    _num_index_scans: c_int,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real parallel_vacuum_cleanup_all_indexes in commands/vacuumparallel.c */
unsafe fn parallel_vacuum_cleanup_all_indexes(
    _pvs: *mut ParallelVacuumState,
    _reltuples: f64,
    _num_index_scans: c_int,
    _estimated_count: bool,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreMemoryUsage in access/tidstore.h */
unsafe fn TidStoreMemoryUsage(_store: *mut TidStore) -> Size {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreCreateLocal in access/tidstore.h */
unsafe fn TidStoreCreateLocal(_max_bytes: Size, _insert_only: bool) -> *mut TidStore {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreDestroy in access/tidstore.h */
unsafe fn TidStoreDestroy(_store: *mut TidStore) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreSetBlockOffsets in access/tidstore.h */
unsafe fn TidStoreSetBlockOffsets(
    _store: *mut TidStore,
    _blkno: BlockNumber,
    _offsets: *mut OffsetNumber,
    _num_offsets: c_int,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreBeginIterate in access/tidstore.h */
unsafe fn TidStoreBeginIterate(_store: *mut TidStore) -> *mut TidStoreIter {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreIterateNext in access/tidstore.h */
unsafe fn TidStoreIterateNext(_iter: *mut TidStoreIter) -> *mut TidStoreIterResult {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreEndIterate in access/tidstore.h */
unsafe fn TidStoreEndIterate(_iter: *mut TidStoreIter) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real TidStoreGetBlockOffsets in access/tidstore.h */
unsafe fn TidStoreGetBlockOffsets(
    _iter_result: *mut TidStoreIterResult,
    _offsets: *mut OffsetNumber,
    _max_offsets: c_int,
) -> c_int {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real read_stream_begin_relation in storage/read_stream.h */
unsafe fn read_stream_begin_relation(
    _flags: c_int,
    _bstrategy: BufferAccessStrategy,
    _rel: Relation,
    _forknum: c_int,
    _callback: Option<unsafe fn(*mut ReadStream, *mut c_void, *mut c_void) -> BlockNumber>,
    _private_data: *mut c_void,
    _per_buffer_data_size: usize,
) -> *mut ReadStream {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real read_stream_next_buffer in storage/read_stream.h */
unsafe fn read_stream_next_buffer(
    _stream: *mut ReadStream,
    _per_buffer_data: *mut *mut c_void,
) -> Buffer {
    InvalidBuffer /* TODO(pg-port) */
}

/* TODO(pg-port): real read_stream_end in storage/read_stream.h */
unsafe fn read_stream_end(_stream: *mut ReadStream) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): read stream flags (storage/read_stream.h) */
const READ_STREAM_MAINTENANCE:   c_int = 1 << 0;
const READ_STREAM_USE_BATCHING:  c_int = 1 << 1;
const MAIN_FORKNUM:              c_int = 0;

/* TODO(pg-port): real BufferGetPage in storage/bufmgr.h */
unsafe fn BufferGetPage(_buf: Buffer) -> Page {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real BufferGetBlockNumber in storage/bufmgr.h */
unsafe fn BufferGetBlockNumber(_buf: Buffer) -> BlockNumber {
    InvalidBlockNumber /* TODO(pg-port) */
}

/* TODO(pg-port): real CheckBufferIsPinnedOnce in storage/bufmgr.h */
unsafe fn CheckBufferIsPinnedOnce(_buf: Buffer) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real ReleaseBuffer in storage/bufmgr.h */
unsafe fn ReleaseBuffer(_buf: Buffer) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real UnlockReleaseBuffer in storage/bufmgr.h */
unsafe fn UnlockReleaseBuffer(_buf: Buffer) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real LockBuffer in storage/bufmgr.h */
unsafe fn LockBuffer(_buf: Buffer, _mode: c_int) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real LockBufferForCleanup in storage/bufmgr.h */
unsafe fn LockBufferForCleanup(_buf: Buffer) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real ConditionalLockBufferForCleanup in storage/bufmgr.h */
unsafe fn ConditionalLockBufferForCleanup(_buf: Buffer) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real buffer lock modes (storage/bufmgr.h) */
const BUFFER_LOCK_SHARE:     c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;
const BUFFER_LOCK_UNLOCK:    c_int = 0;

/* TODO(pg-port): real MarkBufferDirty in storage/bufmgr.h */
unsafe fn MarkBufferDirty(_buf: Buffer) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real ReadBufferExtended in storage/bufmgr.h */
unsafe fn ReadBufferExtended(
    _rel: Relation,
    _forknum: c_int,
    _blkno: BlockNumber,
    _mode: c_int,
    _bstrategy: BufferAccessStrategy,
) -> Buffer {
    InvalidBuffer /* TODO(pg-port) */
}

const RBM_NORMAL: c_int = 0;

/* TODO(pg-port): real PrefetchBuffer in storage/bufmgr.h */
unsafe fn PrefetchBuffer(_rel: Relation, _forknum: c_int, _blkno: BlockNumber) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real GetRecordedFreeSpace in storage/freespace.h */
unsafe fn GetRecordedFreeSpace(_rel: Relation, _blkno: BlockNumber) -> Size {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real RecordPageWithFreeSpace in storage/freespace.h */
unsafe fn RecordPageWithFreeSpace(_rel: Relation, _blkno: BlockNumber, _freespace: Size) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real FreeSpaceMapVacuumRange in storage/freespace.h */
unsafe fn FreeSpaceMapVacuumRange(_rel: Relation, _start: BlockNumber, _end: BlockNumber) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real PageGetHeapFreeSpace in storage/bufpage.h */
unsafe fn PageGetHeapFreeSpace(_page: Page) -> Size {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real PageIsNew in storage/bufpage.h */
unsafe fn PageIsNew(_page: Page) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real PageIsEmpty in storage/bufpage.h */
unsafe fn PageIsEmpty(_page: Page) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real PageIsAllVisible in storage/bufpage.h */
unsafe fn PageIsAllVisible(_page: Page) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real PageSetAllVisible in storage/bufpage.h */
unsafe fn PageSetAllVisible(_page: Page) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real PageClearAllVisible in storage/bufpage.h */
unsafe fn PageClearAllVisible(_page: Page) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real PageGetLSN in storage/bufpage.h */
unsafe fn PageGetLSN(_page: Page) -> u64 {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real PageGetMaxOffsetNumber in storage/bufpage.h */
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    0 /* TODO(pg-port) */
}

/* TODO(pg-port): real PageGetItemId in storage/bufpage.h */
unsafe fn PageGetItemId(_page: Page, _offnum: OffsetNumber) -> ItemId {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real PageGetItem in storage/bufpage.h */
unsafe fn PageGetItem(_page: Page, _itemid: ItemId) -> *mut c_void {
    std::ptr::null_mut() /* TODO(pg-port) */
}

/* TODO(pg-port): real ItemIdIsDead in storage/itemid.h */
unsafe fn ItemIdIsDead(_itemid: ItemId) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real ItemIdSetUnused in storage/itemid.h */
unsafe fn ItemIdSetUnused(_itemid: ItemId) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real PageTruncateLinePointerArray in storage/bufpage.h */
unsafe fn PageTruncateLinePointerArray(_page: Page) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real ItemPointerSet in storage/itemptr.h */
unsafe fn ItemPointerSet(_ptr: *mut ItemPointerData, _blkno: BlockNumber, _offnum: OffsetNumber) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): InvalidXLogRecPtr (access/xlogdefs.h) */
const InvalidXLogRecPtr: u64 = 0;

/* TODO(pg-port): real RelationNeedsWAL in utils/rel.h */
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real log_newpage_buffer in access/xloginsert.h */
unsafe fn log_newpage_buffer(_buf: Buffer, _page_std: bool) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real log_heap_prune_and_freeze in access/heapam.h */
unsafe fn log_heap_prune_and_freeze(
    _rel: Relation,
    _buf: Buffer,
    _cutoff_xid: TransactionId,
    _cleanup_lock: bool,
    _reason: c_int,
    _frozen: *const c_void,
    _nfrozen: c_int,
    _redirected: *const c_void,
    _nredirected: c_int,
    _dead: *const c_void,
    _ndead: c_int,
    _unused: *const OffsetNumber,
    _nunused: c_int,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real PRUNE_VACUUM_SCAN / PRUNE_VACUUM_CLEANUP (access/heapam.h) */
const PRUNE_VACUUM_SCAN:    c_int = 1;
const PRUNE_VACUUM_CLEANUP: c_int = 2;

/* TODO(pg-port): HEAP_PAGE_PRUNE_* flags (access/heapam.h) */
const HEAP_PAGE_PRUNE_FREEZE:          c_int = 1 << 0;
const HEAP_PAGE_PRUNE_MARK_UNUSED_NOW: c_int = 1 << 1;

/* TODO(pg-port): real heap_page_prune_and_freeze in access/heapam.h */
unsafe fn heap_page_prune_and_freeze(
    _rel: Relation,
    _buf: Buffer,
    _vistest: *mut GlobalVisState,
    _options: c_int,
    _cutoffs: *const VacuumCutoffs,
    _presult: *mut PruneFreezeResult,
    _reason: c_int,
    _offnum: *mut OffsetNumber,
    _new_relfrozenxid: *mut TransactionId,
    _new_relminmxid: *mut MultiXactId,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real HeapTupleSatisfiesVacuum in access/htup_details.h */
pub type HTSV_Result = c_int;
pub const HEAPTUPLE_DEAD:              HTSV_Result = 0;
pub const HEAPTUPLE_LIVE:              HTSV_Result = 1;
pub const HEAPTUPLE_RECENTLY_DEAD:     HTSV_Result = 2;
pub const HEAPTUPLE_INSERT_IN_PROGRESS: HTSV_Result = 3;
pub const HEAPTUPLE_DELETE_IN_PROGRESS: HTSV_Result = 4;

unsafe fn HeapTupleSatisfiesVacuum(
    _tuple: *const HeapTupleData,
    _oldest_xmin: TransactionId,
    _buf: Buffer,
) -> HTSV_Result {
    HEAPTUPLE_LIVE /* TODO(pg-port) */
}

/* TODO(pg-port): real heap_tuple_should_freeze in access/heapam.h */
unsafe fn heap_tuple_should_freeze(
    _tuple: HeapTupleHeader,
    _cutoffs: *const VacuumCutoffs,
    _relfrozenxid: *mut TransactionId,
    _relminmxid: *mut MultiXactId,
) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real heap_tuple_needs_eventual_freeze in access/heapam.h */
unsafe fn heap_tuple_needs_eventual_freeze(_tuple: HeapTupleHeader) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real MultiXactIdIsValid in access/multixact.h */
#[inline]
fn MultiXactIdIsValid(mxid: MultiXactId) -> bool {
    mxid != 0
}

/* TODO(pg-port): real MultiXactIdPrecedes in access/multixact.h */
unsafe fn MultiXactIdPrecedes(_a: MultiXactId, _b: MultiXactId) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real MultiXactIdPrecedesOrEquals in access/multixact.h */
unsafe fn MultiXactIdPrecedesOrEquals(_a: MultiXactId, _b: MultiXactId) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): InvalidMultiXactId */
const InvalidMultiXactId: MultiXactId = 0;

/* TODO(pg-port): real ConditionalLockRelation in storage/lmgr.h */
unsafe fn ConditionalLockRelation(_rel: Relation, _lockmode: c_int) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real UnlockRelation in storage/lmgr.h */
unsafe fn UnlockRelation(_rel: Relation, _lockmode: c_int) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real LockHasWaitersRelation in storage/lmgr.h */
unsafe fn LockHasWaitersRelation(_rel: Relation, _lockmode: c_int) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): lock mode constants (storage/lockdefs.h) */
const AccessExclusiveLock: c_int = 8;
const NoLock:              c_int = 0;

/* TODO(pg-port): real RelationTruncate in catalog/storage.h */
unsafe fn RelationTruncate(_rel: Relation, _nblocks: BlockNumber) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real CHECK_FOR_INTERRUPTS (miscadmin.h) */
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{ /* TODO(pg-port) */ }};
}

/* TODO(pg-port): real START_CRIT_SECTION / END_CRIT_SECTION (storage/bufmgr.h) */
macro_rules! START_CRIT_SECTION {
    () => {{ /* TODO(pg-port) */ }};
}
macro_rules! END_CRIT_SECTION {
    () => {{ /* TODO(pg-port) */ }};
}

/* TODO(pg-port): real WaitLatch / ResetLatch / MyLatch (storage/latch.h) */
const WL_LATCH_SET:      c_int = 1;
const WL_TIMEOUT:        c_int = 2;
const WL_EXIT_ON_PM_DEATH: c_int = 4;
const WAIT_EVENT_VACUUM_TRUNCATE: c_int = 0;
unsafe fn WaitLatch(
    _latch: *mut c_void,
    _wakeEvents: c_int,
    _timeout: c_int,
    _wait_event: c_int,
) -> c_int { 0 /* TODO(pg-port) */ }
unsafe fn ResetLatch(_latch: *mut c_void) { /* TODO(pg-port) */ }
static mut MyLatch: *mut c_void = std::ptr::null_mut();

/* TODO(pg-port): real pg_cmp_u16 in common/int.h */
unsafe fn pg_cmp_u16(a: u16, b: u16) -> c_int {
    (a as c_int) - (b as c_int)
}

/* TODO(pg-port): real qsort in libc */
unsafe fn qsort(
    _base: *mut c_void,
    _nmemb: usize,
    _size: usize,
    _cmp: Option<unsafe fn(*const c_void, *const c_void) -> c_int>,
) {
    /* TODO(pg-port) */
}

/* TODO(pg-port): real RelationUsesLocalBuffers in utils/rel.h */
unsafe fn RelationUsesLocalBuffers(_rel: Relation) -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real IsInParallelMode in access/transam/parallel.h */
unsafe fn IsInParallelMode() -> bool {
    false /* TODO(pg-port) */
}

/* TODO(pg-port): real INSTR_TIME macros from portability/instr_time.h */
macro_rules! INSTR_TIME_SET_CURRENT {
    ($t:expr) => {{ ($t).t = 0; /* TODO(pg-port) */ }};
}
macro_rules! INSTR_TIME_SUBTRACT {
    ($dst:expr, $sub:expr) => {{ /* TODO(pg-port) */ }};
}
macro_rules! INSTR_TIME_GET_MICROSEC {
    ($t:expr) => {{ 0i64 /* TODO(pg-port) */ }};
}

/* TODO(pg-port): real initStringInfo / appendStringInfo / appendStringInfoString / pfree */
unsafe fn initStringInfo(_buf: *mut StringInfoData) { /* TODO(pg-port) */ }
unsafe fn appendStringInfo(_buf: *mut StringInfoData, _fmt: *const c_char) { /* TODO(pg-port) */ }
unsafe fn appendStringInfoString(_buf: *mut StringInfoData, _s: *const c_char) { /* TODO(pg-port) */ }

/* TODO(pg-port): real MyBEEntry (utils/backend_status.h) */
#[repr(C)]
pub struct PgBackendStatus {
    pub st_progress_param: [u64; 16],
}
static mut _MyBEEntry_storage: PgBackendStatus = PgBackendStatus { st_progress_param: [0u64; 16] };
static mut MyBEEntry: *mut PgBackendStatus = std::ptr::null_mut();

/* TODO(pg-port): lengthof macro */
macro_rules! lengthof {
    ($arr:expr) => { ($arr).len() as c_int };
}

/* TODO(pg-port): real StaticAssertStmt -- no-op at runtime */
macro_rules! StaticAssertStmt {
    ($cond:expr, $msg:literal) => { const _: () = assert!($cond, $msg); };
}

/* TODO(pg-port): errcontext macro */
macro_rules! errcontext {
    ($($a:tt)*) => {{ let _ = format!($($a)*); }};
}

/* TODO(pg-port): unlikely hint */
macro_rules! unlikely {
    ($e:expr) => { $e };
}

/* TODO(pg-port): DEBUG2 / INFO / WARNING / LOG / ERROR log levels */
const DEBUG2:   c_int = 10;
const INFO:     c_int = 17;
const WARNING:  c_int = 19;
const LOG:      c_int = 15;
const ERROR:    c_int = 20;

// ========================================================================
// Part 2: heap_vacuum_eager_scan_setup + heap_vacuum_rel (first half)
// ========================================================================

/*
 * Helper to set up the eager scanning state for vacuuming a single relation.
 * Initializes the eager scan management related members of the LVRelState.
 *
 * Caller provides whether or not an aggressive vacuum is required due to
 * vacuum options or for relfrozenxid/relminmxid advancement.
 */
unsafe fn heap_vacuum_eager_scan_setup(vacrel: *mut LVRelState, params: *mut VacuumParams) {
    let mut randseed: u32;
    let mut allvisible: BlockNumber = 0;
    let mut allfrozen: BlockNumber = 0;
    let mut first_region_ratio: f32;
    let mut oldest_unfrozen_before_cutoff: bool = false;

    /*
     * Initialize eager scan management fields to their disabled values.
     * Aggressive vacuums, normal vacuums of small tables, and normal vacuums
     * of tables without sufficiently old tuples disable eager scanning.
     */
    (*vacrel).next_eager_scan_region_start = InvalidBlockNumber;
    (*vacrel).eager_scan_max_fails_per_region = 0;
    (*vacrel).eager_scan_remaining_fails = 0;
    (*vacrel).eager_scan_remaining_successes = 0;

    /* If eager scanning is explicitly disabled, just return. */
    if (*params).max_eager_freeze_failure_rate == 0.0 {
        return;
    }

    /*
     * The caller will have determined whether or not an aggressive vacuum is
     * required by either the vacuum parameters or the relative age of the
     * oldest unfrozen transaction IDs. An aggressive vacuum must scan every
     * all-visible page to safely advance the relfrozenxid and/or relminmxid,
     * so scans of all-visible pages are not considered eager.
     */
    if (*vacrel).aggressive {
        return;
    }

    /*
     * Aggressively vacuuming a small relation shouldn't take long, so it
     * isn't worth amortizing. We use two times the region size as the size
     * cutoff because the eager scan start block is a random spot somewhere in
     * the first region, making the second region the first to be eager
     * scanned normally.
     */
    if (*vacrel).rel_pages < 2 * EAGER_SCAN_REGION_SIZE {
        return;
    }

    /*
     * We only want to enable eager scanning if we are likely to be able to
     * freeze some of the pages in the relation.
     *
     * Tuples with XIDs older than OldestXmin or MXIDs older than OldestMxact
     * are technically freezable, but we won't freeze them unless the criteria
     * for opportunistic freezing is met. Only tuples with XIDs/MXIDs older
     * than the FreezeLimit/MultiXactCutoff are frozen in the common case.
     *
     * So, as a heuristic, we wait until the FreezeLimit has advanced past the
     * relfrozenxid or the MultiXactCutoff has advanced past the relminmxid to
     * enable eager scanning.
     */
    if TransactionIdIsNormal((*vacrel).cutoffs.relfrozenxid) &&
        TransactionIdPrecedes((*vacrel).cutoffs.relfrozenxid, (*vacrel).cutoffs.FreezeLimit)
    {
        oldest_unfrozen_before_cutoff = true;
    }

    if !oldest_unfrozen_before_cutoff &&
        MultiXactIdIsValid((*vacrel).cutoffs.relminmxid) &&
        MultiXactIdPrecedes((*vacrel).cutoffs.relminmxid, (*vacrel).cutoffs.MultiXactCutoff)
    {
        oldest_unfrozen_before_cutoff = true;
    }

    if !oldest_unfrozen_before_cutoff {
        return;
    }

    /* We have met the criteria to eagerly scan some pages. */

    /*
     * Our success cap is MAX_EAGER_FREEZE_SUCCESS_RATE of the number of
     * all-visible but not all-frozen blocks in the relation.
     */
    visibilitymap_count((*vacrel).rel, &mut allvisible, &mut allfrozen);

    (*vacrel).eager_scan_remaining_successes =
        (MAX_EAGER_FREEZE_SUCCESS_RATE * (allvisible - allfrozen) as f64) as BlockNumber;

    /* If every all-visible page is frozen, eager scanning is disabled. */
    if (*vacrel).eager_scan_remaining_successes == 0 {
        return;
    }

    /*
     * Now calculate the bounds of the first eager scan region. Its end block
     * will be a random spot somewhere in the first EAGER_SCAN_REGION_SIZE
     * blocks. This affects the bounds of all subsequent regions and avoids
     * eager scanning and failing to freeze the same blocks each vacuum of the
     * relation.
     */
    randseed = pg_prng_uint32(&mut pg_global_prng_state as *mut u64 as *mut c_void);

    (*vacrel).next_eager_scan_region_start = randseed % EAGER_SCAN_REGION_SIZE;

    Assert!((*params).max_eager_freeze_failure_rate > 0.0 &&
            (*params).max_eager_freeze_failure_rate <= 1.0);

    (*vacrel).eager_scan_max_fails_per_region =
        ((*params).max_eager_freeze_failure_rate * EAGER_SCAN_REGION_SIZE as f64) as BlockNumber;

    /*
     * The first region will be smaller than subsequent regions. As such,
     * adjust the eager freeze failures tolerated for this region.
     */
    first_region_ratio = 1.0 - (*vacrel).next_eager_scan_region_start as f32 /
        EAGER_SCAN_REGION_SIZE as f32;

    (*vacrel).eager_scan_remaining_fails =
        ((*vacrel).eager_scan_max_fails_per_region as f32 * first_region_ratio) as BlockNumber;
}

/*
 *	heap_vacuum_rel() -- perform VACUUM for one heap relation
 *
 *		This routine sets things up for and then calls lazy_scan_heap, where
 *		almost all work actually takes place.  Finalizes everything after call
 *		returns by managing relation truncation and updating rel's pg_class
 *		entry. (Also updates pg_class entries for any indexes that need it.)
 *
 *		At entry, we have already established a transaction and opened
 *		and locked the relation.
 */
pub unsafe fn heap_vacuum_rel(
    rel: Relation,
    params: *mut VacuumParams,
    bstrategy: BufferAccessStrategy,
) {
    let mut vacrel: *mut LVRelState;
    let mut verbose: bool;
    let mut instrument: bool;
    let mut skipwithvm: bool;
    let mut frozenxid_updated: bool = false;
    let mut minmulti_updated: bool = false;
    let mut orig_rel_pages: BlockNumber;
    let mut new_rel_pages: BlockNumber;
    let mut new_rel_allvisible: BlockNumber = 0;
    let mut new_rel_allfrozen: BlockNumber = 0;
    let mut ru0: PGRUsage = core::mem::zeroed();
    let mut starttime: TimestampTz = 0;
    let mut startreadtime: PgStat_Counter = 0;
    let mut startwritetime: PgStat_Counter = 0;
    let mut startwalusage: WalUsage = core::mem::zeroed();
    let mut startbufferusage: BufferUsage = core::mem::zeroed();
    let mut errcallback: ErrorContextCallback = core::mem::zeroed();
    let mut indnames: *mut *mut c_char = std::ptr::null_mut();

    verbose = ((*params).options & VACOPT_VERBOSE) != 0;
    instrument = verbose || (AmAutoVacuumWorkerProcess() &&
                             (*params).log_min_duration >= 0);
    if instrument {
        pg_rusage_init(&mut ru0);
        if track_io_timing {
            startreadtime = pgStatBlockReadTime;
            startwritetime = pgStatBlockWriteTime;
        }
    }

    /* Used for instrumentation and stats report */
    starttime = GetCurrentTimestamp();

    pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, RelationGetRelid(rel));

    /*
     * Setup error traceback support for ereport() first.  The idea is to set
     * up an error context callback to display additional information on any
     * error during a vacuum.  During different phases of vacuum, we update
     * the state so that the error context callback always display current
     * information.
     *
     * Copy the names of heap rel into local memory for error reporting
     * purposes, too.  It isn't always safe to assume that we can get the name
     * of each rel.  It's convenient for code in lazy_scan_heap to always use
     * these temp copies.
     */
    vacrel = palloc0_lvstate();
    (*vacrel).dbname = get_database_name(MyDatabaseId);
    (*vacrel).relnamespace = get_namespace_name(RelationGetNamespace(rel));
    (*vacrel).relname = pstrdup(RelationGetRelationName(rel) as *const c_char);
    (*vacrel).indname = std::ptr::null_mut();
    (*vacrel).phase = VACUUM_ERRCB_PHASE_UNKNOWN;
    (*vacrel).verbose = verbose;
    errcallback.callback = Some(vacuum_error_callback);
    errcallback.arg = vacrel as *mut c_void;
    errcallback.previous = error_context_stack;
    error_context_stack = &mut errcallback;

    /* Set up high level stuff about rel and its indexes */
    (*vacrel).rel = rel;
    vac_open_indexes((*vacrel).rel, RowExclusiveLock, &mut (*vacrel).nindexes,
                     &mut (*vacrel).indrels);
    (*vacrel).bstrategy = bstrategy;
    if instrument && (*vacrel).nindexes > 0 {
        /* Copy index names used by instrumentation (not error reporting) */
        indnames = palloc(
            core::mem::size_of::<*mut c_char>() * (*vacrel).nindexes as usize
        ) as *mut *mut c_char;
        for i in 0..(*vacrel).nindexes {
            *indnames.add(i as usize) = pstrdup(
                RelationGetRelationName(*(*vacrel).indrels.add(i as usize)) as *const c_char
            );
        }
    }

    /*
     * The index_cleanup param either disables index vacuuming and cleanup or
     * forces it to go ahead when we would otherwise apply the index bypass
     * optimization.  The default is 'auto', which leaves the final decision
     * up to lazy_vacuum().
     *
     * The truncate param allows user to avoid attempting relation truncation,
     * though it can't force truncation to happen.
     */
    Assert!((*params).index_cleanup != VACOPTVALUE_UNSPECIFIED);
    Assert!((*params).truncate != VACOPTVALUE_UNSPECIFIED &&
            (*params).truncate != VACOPTVALUE_AUTO);

    /*
     * While VacuumFailSafeActive is reset to false before calling this, we
     * still need to reset it here due to recursive calls.
     */
    VacuumFailsafeActive = false;
    (*vacrel).consider_bypass_optimization = true;
    (*vacrel).do_index_vacuuming = true;
    (*vacrel).do_index_cleanup = true;
    (*vacrel).do_rel_truncate = ((*params).truncate != VACOPTVALUE_DISABLED);
    if (*params).index_cleanup == VACOPTVALUE_DISABLED {
        /* Force disable index vacuuming up-front */
        (*vacrel).do_index_vacuuming = false;
        (*vacrel).do_index_cleanup = false;
    } else if (*params).index_cleanup == VACOPTVALUE_ENABLED {
        /* Force index vacuuming.  Note that failsafe can still bypass. */
        (*vacrel).consider_bypass_optimization = false;
    } else {
        /* Default/auto, make all decisions dynamically */
        Assert!((*params).index_cleanup == VACOPTVALUE_AUTO);
    }

    /* Initialize page counters explicitly (be tidy) */
    (*vacrel).scanned_pages = 0;
    (*vacrel).eager_scanned_pages = 0;
    (*vacrel).removed_pages = 0;
    (*vacrel).new_frozen_tuple_pages = 0;
    (*vacrel).lpdead_item_pages = 0;
    (*vacrel).missed_dead_pages = 0;
    (*vacrel).nonempty_pages = 0;
    /* dead_items_alloc allocates vacrel->dead_items later on */

    /* Allocate/initialize output statistics state */
    (*vacrel).new_rel_tuples = 0.0;
    (*vacrel).new_live_tuples = 0.0;
    (*vacrel).indstats = palloc0(
        (*vacrel).nindexes as usize * core::mem::size_of::<*mut IndexBulkDeleteResult>()
    ) as *mut *mut IndexBulkDeleteResult;

    /* Initialize remaining counters (be tidy) */
    (*vacrel).num_index_scans = 0;
    (*vacrel).tuples_deleted = 0;
    (*vacrel).tuples_frozen = 0;
    (*vacrel).lpdead_items = 0;
    (*vacrel).live_tuples = 0;
    (*vacrel).recently_dead_tuples = 0;
    (*vacrel).missed_dead_tuples = 0;

    (*vacrel).vm_new_visible_pages = 0;
    (*vacrel).vm_new_visible_frozen_pages = 0;
    (*vacrel).vm_new_frozen_pages = 0;

    /*
     * Get cutoffs that determine which deleted tuples are considered DEAD,
     * not just RECENTLY_DEAD, and which XIDs/MXIDs to freeze.  Then determine
     * the extent of the blocks that we'll scan in lazy_scan_heap.
     */
    (*vacrel).aggressive = vacuum_get_cutoffs(rel, params, &mut (*vacrel).cutoffs);
    (*vacrel).rel_pages = RelationGetNumberOfBlocks(rel);
    orig_rel_pages = (*vacrel).rel_pages;
    (*vacrel).vistest = GlobalVisTestFor(rel);

    /* Initialize state used to track oldest extant XID/MXID */
    (*vacrel).NewRelfrozenXid = (*vacrel).cutoffs.OldestXmin;
    (*vacrel).NewRelminMxid = (*vacrel).cutoffs.OldestMxact;

    /*
     * Initialize state related to tracking all-visible page skipping.
     */
    (*vacrel).skippedallvis = false;
    skipwithvm = true;
    if ((*params).options & VACOPT_DISABLE_PAGE_SKIPPING) != 0 {
        /*
         * Force aggressive mode, and disable skipping blocks using the
         * visibility map (even those set all-frozen)
         */
        (*vacrel).aggressive = true;
        skipwithvm = false;
    }

    (*vacrel).skipwithvm = skipwithvm;

    /*
     * Set up eager scan tracking state. This must happen after determining
     * whether or not the vacuum must be aggressive, because only normal
     * vacuums use the eager scan algorithm.
     */
    heap_vacuum_eager_scan_setup(vacrel, params);

    if verbose {
        if (*vacrel).aggressive {
            ereport!(INFO, errmsg!("aggressively vacuuming \"{}.{}.{}\"",
                std::ffi::CStr::from_ptr((*vacrel).dbname).to_string_lossy(),
                std::ffi::CStr::from_ptr((*vacrel).relnamespace).to_string_lossy(),
                std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy()));
        } else {
            ereport!(INFO, errmsg!("vacuuming \"{}.{}.{}\"",
                std::ffi::CStr::from_ptr((*vacrel).dbname).to_string_lossy(),
                std::ffi::CStr::from_ptr((*vacrel).relnamespace).to_string_lossy(),
                std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy()));
        }
    }

    /*
     * Allocate dead_items memory using dead_items_alloc.  This handles
     * parallel VACUUM initialization as part of allocating shared memory
     * space used for dead_items.  (But do a failsafe precheck first, to
     * ensure that parallel VACUUM won't be attempted at all when relfrozenxid
     * is already dangerously old.)
     */
    lazy_check_wraparound_failsafe(vacrel);
    dead_items_alloc(vacrel, (*params).nworkers);

    /*
     * Call lazy_scan_heap to perform all required heap pruning, index
     * vacuuming, and heap vacuuming (plus related processing)
     */
    lazy_scan_heap(vacrel);

    /*
     * Free resources managed by dead_items_alloc.  This ends parallel mode in
     * passing when necessary.
     */
    dead_items_cleanup(vacrel);
    Assert!(!IsInParallelMode());

    /*
     * Update pg_class entries for each of rel's indexes where appropriate.
     */
    if (*vacrel).do_index_cleanup {
        update_relstats_all_indexes(vacrel);
    }

    /* Done with rel's indexes */
    vac_close_indexes((*vacrel).nindexes, (*vacrel).indrels, NoLock);

    /* Optionally truncate rel */
    if should_attempt_truncation(vacrel) {
        lazy_truncate_heap(vacrel);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;

    /* Report that we are now doing final cleanup */
    pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
                                 PROGRESS_VACUUM_PHASE_FINAL_CLEANUP as int64);

    /*
     * Prepare to update rel's pg_class entry.
     *
     * Aggressive VACUUMs must always be able to advance relfrozenxid to a
     * value >= FreezeLimit, and relminmxid to a value >= MultiXactCutoff.
     * Non-aggressive VACUUMs may advance them by any amount, or not at all.
     */
    Assert!((*vacrel).NewRelfrozenXid == (*vacrel).cutoffs.OldestXmin ||
            TransactionIdPrecedesOrEquals(
                if (*vacrel).aggressive { (*vacrel).cutoffs.FreezeLimit }
                else { (*vacrel).cutoffs.relfrozenxid },
                (*vacrel).NewRelfrozenXid));
    Assert!((*vacrel).NewRelminMxid == (*vacrel).cutoffs.OldestMxact ||
            MultiXactIdPrecedesOrEquals(
                if (*vacrel).aggressive { (*vacrel).cutoffs.MultiXactCutoff }
                else { (*vacrel).cutoffs.relminmxid },
                (*vacrel).NewRelminMxid));
    if (*vacrel).skippedallvis {
        /*
         * Must keep original relfrozenxid in a non-aggressive VACUUM that
         * chose to skip an all-visible page range.  The state that tracks new
         * values will have missed unfrozen XIDs from the pages we skipped.
         */
        Assert!(!(*vacrel).aggressive);
        (*vacrel).NewRelfrozenXid = InvalidTransactionId;
        (*vacrel).NewRelminMxid = InvalidMultiXactId;
    }

    /*
     * For safety, clamp relallvisible to be not more than what we're setting
     * pg_class.relpages to
     */
    new_rel_pages = (*vacrel).rel_pages; /* After possible rel truncation */
    visibilitymap_count(rel, &mut new_rel_allvisible, &mut new_rel_allfrozen);
    if new_rel_allvisible > new_rel_pages {
        new_rel_allvisible = new_rel_pages;
    }

    /*
     * An all-frozen block _must_ be all-visible. As such, clamp the count of
     * all-frozen blocks to the count of all-visible blocks.
     */
    if new_rel_allfrozen > new_rel_allvisible {
        new_rel_allfrozen = new_rel_allvisible;
    }

    /*
     * Now actually update rel's pg_class entry.
     */
    vac_update_relstats(rel, new_rel_pages, (*vacrel).new_live_tuples,
                        new_rel_allvisible, new_rel_allfrozen,
                        (*vacrel).nindexes > 0,
                        (*vacrel).NewRelfrozenXid, (*vacrel).NewRelminMxid,
                        &mut frozenxid_updated, &mut minmulti_updated, false);

    /*
     * Report results to the cumulative stats system, too.
     */
    pgstat_report_vacuum(RelationGetRelid(rel),
                         /* rel->rd_rel->relisshared */ false,
                         if (*vacrel).new_live_tuples > 0.0 { (*vacrel).new_live_tuples as int64 } else { 0 },
                         (*vacrel).recently_dead_tuples + (*vacrel).missed_dead_tuples,
                         starttime);
    pgstat_progress_end_command();

    if instrument {
        let endtime: TimestampTz = GetCurrentTimestamp();

        if verbose || (*params).log_min_duration == 0 ||
            TimestampDifferenceExceeds(starttime, endtime, (*params).log_min_duration)
        {
            let mut secs_dur: i64 = 0;
            let mut usecs_dur: c_int = 0;
            let mut walusage: WalUsage = core::mem::zeroed();
            let mut bufferusage: BufferUsage = core::mem::zeroed();
            let mut buf: StringInfoData = core::mem::zeroed();
            let mut diff: int32;
            let mut read_rate: f64 = 0.0;
            let mut write_rate: f64 = 0.0;
            let total_blks_hit: int64;
            let total_blks_read: int64;
            let total_blks_dirtied: int64;

            TimestampDifference(starttime, endtime, &mut secs_dur, &mut usecs_dur);
            core::ptr::write_bytes(&mut walusage as *mut WalUsage as *mut u8, 0,
                                   core::mem::size_of::<WalUsage>());
            WalUsageAccumDiff(&mut walusage, &pgWalUsage, &startwalusage);
            core::ptr::write_bytes(&mut bufferusage as *mut BufferUsage as *mut u8, 0,
                                   core::mem::size_of::<BufferUsage>());
            BufferUsageAccumDiff(&mut bufferusage, &pgBufferUsage, &startbufferusage);

            total_blks_hit = bufferusage.shared_blks_hit + bufferusage.local_blks_hit;
            total_blks_read = bufferusage.shared_blks_read + bufferusage.local_blks_read;
            total_blks_dirtied = bufferusage.shared_blks_dirtied +
                bufferusage.local_blks_dirtied;

            initStringInfo(&mut buf);
            /* detailed logging omitted: appendStringInfo calls would go here */
            /* C also: errmsg_internal("%s", buf.data) */
            ereport!(if verbose { INFO } else { LOG },
                     errmsg!("vacuum instrumentation (omitted)"));
            pfree(buf.data as *mut c_void);
        }
    }

    /* Cleanup index statistics and index names */
    for i in 0..(*vacrel).nindexes {
        if !(*(*vacrel).indstats.add(i as usize)).is_null() {
            pfree(*(*vacrel).indstats.add(i as usize) as *mut c_void);
        }
        if instrument {
            pfree(*indnames.add(i as usize) as *mut c_void);
        }
    }
}

// Lock mode constant used above (also in vacuumparallel.rs)
const RowExclusiveLock: c_int = 3;

// ========================================================================
// Part 3: lazy_scan_heap + heap_vac_scan_next_block + find_next_unskippable_block
//         + lazy_scan_new_or_empty + cmpOffsetNumbers
// ========================================================================

/*
 *	lazy_scan_heap() -- workhorse function for VACUUM
 *
 *		This routine prunes each page in the heap, and considers the need to
 *		freeze remaining tuples with storage (not including pages that can be
 *		skipped using the visibility map).  Also performs related maintenance
 *		of the FSM and visibility map.  These steps all take place during an
 *		initial pass over the target heap relation.
 */
unsafe fn lazy_scan_heap(vacrel: *mut LVRelState) {
    let mut stream: *mut ReadStream;
    let rel_pages: BlockNumber = (*vacrel).rel_pages;
    let mut blkno: BlockNumber = 0;
    let mut next_fsm_block_to_vacuum: BlockNumber = 0;
    let orig_eager_scan_success_limit: BlockNumber =
        (*vacrel).eager_scan_remaining_successes; /* for logging */
    let mut vmbuffer: Buffer = InvalidBuffer;
    let initprog_index: [c_int; 3] = [
        PROGRESS_VACUUM_PHASE,
        PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
        PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES,
    ];
    let mut initprog_val: [int64; 3] = [0; 3];

    /* Report that we're scanning the heap, advertising total # of blocks */
    initprog_val[0] = PROGRESS_VACUUM_PHASE_SCAN_HEAP as int64;
    initprog_val[1] = rel_pages as int64;
    initprog_val[2] = (*(*vacrel).dead_items_info).max_bytes as int64;
    pgstat_progress_update_multi_param(3, initprog_index.as_ptr(), initprog_val.as_ptr());

    /* Initialize for the first heap_vac_scan_next_block() call */
    (*vacrel).current_block = InvalidBlockNumber;
    (*vacrel).next_unskippable_block = InvalidBlockNumber;
    (*vacrel).next_unskippable_allvis = false;
    (*vacrel).next_unskippable_eager_scanned = false;
    (*vacrel).next_unskippable_vmbuffer = InvalidBuffer;

    /*
     * Set up the read stream for vacuum's first pass through the heap.
     *
     * This could be made safe for READ_STREAM_USE_BATCHING, but only with
     * explicit work in heap_vac_scan_next_block.
     */
    stream = read_stream_begin_relation(
        READ_STREAM_MAINTENANCE,
        (*vacrel).bstrategy,
        (*vacrel).rel,
        MAIN_FORKNUM,
        Some(heap_vac_scan_next_block),
        vacrel as *mut c_void,
        core::mem::size_of::<uint8>(),
    );

    loop {
        let mut buf: Buffer;
        let page: Page;
        let mut blk_info: uint8 = 0;
        let mut ndeleted: c_int = 0;
        let mut has_lpdead_items: bool = false;
        let mut per_buffer_data: *mut c_void = std::ptr::null_mut();
        let mut vm_page_frozen: bool = false;
        let mut got_cleanup_lock: bool = false;

        vacuum_delay_point(false);

        /*
         * Regularly check if wraparound failsafe should trigger.
         *
         * There is a similar check inside lazy_vacuum_all_indexes(), but
         * relfrozenxid might start to look dangerously old before we reach
         * that point.  This check also provides failsafe coverage for the
         * one-pass strategy, and the two-pass strategy with the index_cleanup
         * param set to 'off'.
         */
        if (*vacrel).scanned_pages > 0 &&
            (*vacrel).scanned_pages % FAILSAFE_EVERY_PAGES() == 0
        {
            lazy_check_wraparound_failsafe(vacrel);
        }

        /*
         * Consider if we definitely have enough space to process TIDs on page
         * already.  If we are close to overrunning the available space for
         * dead_items TIDs, pause and do a cycle of vacuuming before we tackle
         * this page.
         */
        if (*(*vacrel).dead_items_info).num_items > 0 &&
            TidStoreMemoryUsage((*vacrel).dead_items) > (*(*vacrel).dead_items_info).max_bytes
        {
            /*
             * Before beginning index vacuuming, we release any pin we may
             * hold on the visibility map page.
             */
            if BufferIsValid(vmbuffer) {
                ReleaseBuffer(vmbuffer);
                vmbuffer = InvalidBuffer;
            }

            /* Perform a round of index and heap vacuuming */
            (*vacrel).consider_bypass_optimization = false;
            lazy_vacuum(vacrel);

            /*
             * Vacuum the Free Space Map to make newly-freed space visible on
             * upper-level FSM pages.
             */
            FreeSpaceMapVacuumRange((*vacrel).rel, next_fsm_block_to_vacuum, blkno + 1);
            next_fsm_block_to_vacuum = blkno;

            /* Report that we are once again scanning the heap */
            pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
                                         PROGRESS_VACUUM_PHASE_SCAN_HEAP as int64);
        }

        buf = read_stream_next_buffer(stream, &mut per_buffer_data);

        /* The relation is exhausted. */
        if !BufferIsValid(buf) {
            break;
        }

        blk_info = *(per_buffer_data as *const uint8);
        CheckBufferIsPinnedOnce(buf);
        page = BufferGetPage(buf);
        blkno = BufferGetBlockNumber(buf);

        (*vacrel).scanned_pages += 1;
        if (blk_info & VAC_BLK_WAS_EAGER_SCANNED) != 0 {
            (*vacrel).eager_scanned_pages += 1;
        }

        /* Report as block scanned, update error traceback information */
        pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno as int64);
        update_vacuum_error_info(vacrel, std::ptr::null_mut(),
                                 VACUUM_ERRCB_PHASE_SCAN_HEAP, blkno, InvalidOffsetNumber);

        /*
         * Pin the visibility map page in case we need to mark the page
         * all-visible.
         */
        visibilitymap_pin((*vacrel).rel, blkno, &mut vmbuffer);

        /*
         * We need a buffer cleanup lock to prune HOT chains and defragment
         * the page in lazy_scan_prune.
         */
        got_cleanup_lock = ConditionalLockBufferForCleanup(buf);

        if !got_cleanup_lock {
            LockBuffer(buf, BUFFER_LOCK_SHARE);
        }

        /* Check for new or empty pages before lazy_scan_[no]prune call */
        if lazy_scan_new_or_empty(vacrel, buf, blkno, page, !got_cleanup_lock, vmbuffer) {
            /* Processed as new/empty page (lock and pin released) */
            continue;
        }

        /*
         * If we didn't get the cleanup lock, we can still collect LP_DEAD
         * items in the dead_items area for later vacuuming.
         */
        if !got_cleanup_lock &&
            !lazy_scan_noprune(vacrel, buf, blkno, page, &mut has_lpdead_items)
        {
            /*
             * lazy_scan_noprune could not do all required processing.  Wait
             * for a cleanup lock, and call lazy_scan_prune in the usual way.
             */
            Assert!((*vacrel).aggressive);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            LockBufferForCleanup(buf);
            got_cleanup_lock = true;
        }

        /*
         * If we have a cleanup lock, we must now prune, freeze, and count
         * tuples.
         */
        if got_cleanup_lock {
            ndeleted = lazy_scan_prune(vacrel, buf, blkno, page,
                                       vmbuffer,
                                       (blk_info & VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM) != 0,
                                       &mut has_lpdead_items, &mut vm_page_frozen);
        }

        /*
         * Count an eagerly scanned page as a failure or a success.
         */
        if got_cleanup_lock && (blk_info & VAC_BLK_WAS_EAGER_SCANNED) != 0 {
            /* Aggressive vacuums do not eager scan. */
            Assert!(!(*vacrel).aggressive);

            if vm_page_frozen {
                if (*vacrel).eager_scan_remaining_successes > 0 {
                    (*vacrel).eager_scan_remaining_successes -= 1;
                }

                if (*vacrel).eager_scan_remaining_successes == 0 {
                    /*
                     * Report only once that we disabled eager scanning.
                     */
                    if (*vacrel).eager_scan_max_fails_per_region > 0 {
                        ereport!(if (*vacrel).verbose { INFO } else { DEBUG2 },
                                 errmsg!("disabling eager scanning after freezing {} eagerly scanned blocks of relation \"{}.{}.{}\"",
                                         orig_eager_scan_success_limit,
                                         std::ffi::CStr::from_ptr((*vacrel).dbname).to_string_lossy(),
                                         std::ffi::CStr::from_ptr((*vacrel).relnamespace).to_string_lossy(),
                                         std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy()));
                    }

                    /*
                     * If we hit our success cap, permanently disable eager
                     * scanning by setting the other eager scan management
                     * fields to their disabled values.
                     */
                    (*vacrel).eager_scan_remaining_fails = 0;
                    (*vacrel).next_eager_scan_region_start = InvalidBlockNumber;
                    (*vacrel).eager_scan_max_fails_per_region = 0;
                }
            } else if (*vacrel).eager_scan_remaining_fails > 0 {
                (*vacrel).eager_scan_remaining_fails -= 1;
            }
        }

        /*
         * Now drop the buffer lock and, potentially, update the FSM.
         */
        if (*vacrel).nindexes == 0
            || !(*vacrel).do_index_vacuuming
            || !has_lpdead_items
        {
            let freespace: Size = PageGetHeapFreeSpace(page);

            UnlockReleaseBuffer(buf);
            RecordPageWithFreeSpace((*vacrel).rel, blkno, freespace);

            /*
             * Periodically perform FSM vacuuming to make newly-freed space
             * visible on upper FSM pages.
             */
            if got_cleanup_lock && (*vacrel).nindexes == 0 && ndeleted > 0 &&
                blkno - next_fsm_block_to_vacuum >= VACUUM_FSM_EVERY_PAGES()
            {
                FreeSpaceMapVacuumRange((*vacrel).rel, next_fsm_block_to_vacuum, blkno);
                next_fsm_block_to_vacuum = blkno;
            }
        } else {
            UnlockReleaseBuffer(buf);
        }
    }

    (*vacrel).blkno = InvalidBlockNumber;
    if BufferIsValid(vmbuffer) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * Report that everything is now scanned.
     */
    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, rel_pages as int64);

    /* now we can compute the new value for pg_class.reltuples */
    (*vacrel).new_live_tuples = vac_estimate_reltuples((*vacrel).rel, rel_pages,
                                                        (*vacrel).scanned_pages,
                                                        (*vacrel).live_tuples);

    /*
     * Also compute the total number of surviving heap entries.
     */
    (*vacrel).new_rel_tuples =
        if (*vacrel).new_live_tuples > 0.0 { (*vacrel).new_live_tuples } else { 0.0 }
        + (*vacrel).recently_dead_tuples as f64
        + (*vacrel).missed_dead_tuples as f64;

    read_stream_end(stream);

    /*
     * Do index vacuuming (call each index's ambulkdelete routine), then do
     * related heap vacuuming
     */
    if (*(*vacrel).dead_items_info).num_items > 0 {
        lazy_vacuum(vacrel);
    }

    /*
     * Vacuum the remainder of the Free Space Map.
     */
    if rel_pages > next_fsm_block_to_vacuum {
        FreeSpaceMapVacuumRange((*vacrel).rel, next_fsm_block_to_vacuum, rel_pages);
    }

    /* report all blocks vacuumed */
    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, rel_pages as int64);

    /* Do final index cleanup (call each index's amvacuumcleanup routine) */
    if (*vacrel).nindexes > 0 && (*vacrel).do_index_cleanup {
        lazy_cleanup_all_indexes(vacrel);
    }
}

/*
 *	heap_vac_scan_next_block() -- read stream callback to get the next block
 *	for vacuum to process
 */
unsafe fn heap_vac_scan_next_block(
    _stream: *mut ReadStream,
    callback_private_data: *mut c_void,
    per_buffer_data: *mut c_void,
) -> BlockNumber {
    let mut next_block: BlockNumber;
    let vacrel: *mut LVRelState = callback_private_data as *mut LVRelState;
    let mut blk_info: uint8 = 0;

    /* relies on InvalidBlockNumber + 1 overflowing to 0 on first call */
    next_block = (*vacrel).current_block.wrapping_add(1);

    /* Have we reached the end of the relation? */
    if next_block >= (*vacrel).rel_pages {
        if BufferIsValid((*vacrel).next_unskippable_vmbuffer) {
            ReleaseBuffer((*vacrel).next_unskippable_vmbuffer);
            (*vacrel).next_unskippable_vmbuffer = InvalidBuffer;
        }
        return InvalidBlockNumber;
    }

    /*
     * We must be in one of the three following states:
     */
    if next_block > (*vacrel).next_unskippable_block ||
        (*vacrel).next_unskippable_block == InvalidBlockNumber
    {
        /*
         * 1. We have just processed an unskippable block (or we're at the
         * beginning of the scan).  Find the next unskippable block using the
         * visibility map.
         */
        let mut skipsallvis: bool = false;

        find_next_unskippable_block(vacrel, &mut skipsallvis);

        /*
         * We now know the next block that we must process.  It can be the
         * next block after the one we just processed, or something further
         * ahead.  If it's further ahead, we can jump to it, but we choose to
         * do so only if we can skip at least SKIP_PAGES_THRESHOLD consecutive
         * pages.
         */
        if (*vacrel).next_unskippable_block - next_block >= SKIP_PAGES_THRESHOLD {
            next_block = (*vacrel).next_unskippable_block;
            if skipsallvis {
                (*vacrel).skippedallvis = true;
            }
        }
    }

    /* Now we must be in one of the two remaining states: */
    if next_block < (*vacrel).next_unskippable_block {
        /*
         * 2. We are processing a range of blocks that we could have skipped
         * but chose not to.
         */
        (*vacrel).current_block = next_block;
        blk_info |= VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM;
        *(per_buffer_data as *mut uint8) = blk_info;
        return (*vacrel).current_block;
    } else {
        /*
         * 3. We reached the next unskippable block.  Process it.
         */
        Assert!(next_block == (*vacrel).next_unskippable_block);

        (*vacrel).current_block = next_block;
        if (*vacrel).next_unskippable_allvis {
            blk_info |= VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM;
        }
        if (*vacrel).next_unskippable_eager_scanned {
            blk_info |= VAC_BLK_WAS_EAGER_SCANNED;
        }
        *(per_buffer_data as *mut uint8) = blk_info;
        return (*vacrel).current_block;
    }
}

/*
 * Find the next unskippable block in a vacuum scan using the visibility map.
 */
unsafe fn find_next_unskippable_block(vacrel: *mut LVRelState, skipsallvis: *mut bool) {
    let rel_pages: BlockNumber = (*vacrel).rel_pages;
    let mut next_unskippable_block: BlockNumber = (*vacrel).next_unskippable_block.wrapping_add(1);
    let mut next_unskippable_vmbuffer: Buffer = (*vacrel).next_unskippable_vmbuffer;
    let mut next_unskippable_eager_scanned: bool = false;
    let mut next_unskippable_allvis: bool;

    *skipsallvis = false;

    loop {
        let mapbits: uint8 = visibilitymap_get_status((*vacrel).rel,
                                                       next_unskippable_block,
                                                       &mut next_unskippable_vmbuffer);

        next_unskippable_allvis = (mapbits & VISIBILITYMAP_ALL_VISIBLE) != 0;

        /*
         * At the start of each eager scan region, normal vacuums with eager
         * scanning enabled reset the failure counter.
         */
        if next_unskippable_block >= (*vacrel).next_eager_scan_region_start {
            (*vacrel).eager_scan_remaining_fails =
                (*vacrel).eager_scan_max_fails_per_region;
            (*vacrel).next_eager_scan_region_start += EAGER_SCAN_REGION_SIZE;
        }

        /*
         * A block is unskippable if it is not all visible according to the
         * visibility map.
         */
        if !next_unskippable_allvis {
            Assert!((mapbits & VISIBILITYMAP_ALL_FROZEN) == 0);
            break;
        }

        /*
         * Caller must scan the last page to determine whether it has tuples.
         */
        if next_unskippable_block == rel_pages - 1 {
            break;
        }

        /* DISABLE_PAGE_SKIPPING makes all skipping unsafe */
        if !(*vacrel).skipwithvm {
            break;
        }

        /*
         * All-frozen pages cannot contain XIDs < OldestXmin, so this page
         * can be skipped.
         */
        if (mapbits & VISIBILITYMAP_ALL_FROZEN) != 0 {
            next_unskippable_block += 1;
            continue;
        }

        /*
         * Aggressive vacuums cannot skip any all-visible pages that are not
         * also all-frozen.
         */
        if (*vacrel).aggressive {
            break;
        }

        /*
         * Normal vacuums with eager scanning enabled only skip all-visible
         * but not all-frozen pages if they have hit the failure limit for the
         * current eager scan region.
         */
        if (*vacrel).eager_scan_remaining_fails > 0 {
            next_unskippable_eager_scanned = true;
            break;
        }

        /*
         * All-visible blocks are safe to skip in a normal vacuum.
         */
        *skipsallvis = true;

        next_unskippable_block += 1;
    }

    /* write the local variables back to vacrel */
    (*vacrel).next_unskippable_block = next_unskippable_block;
    (*vacrel).next_unskippable_allvis = next_unskippable_allvis;
    (*vacrel).next_unskippable_eager_scanned = next_unskippable_eager_scanned;
    (*vacrel).next_unskippable_vmbuffer = next_unskippable_vmbuffer;
}

/*
 *	lazy_scan_new_or_empty() -- lazy_scan_heap() new/empty page handling.
 */
unsafe fn lazy_scan_new_or_empty(
    vacrel: *mut LVRelState,
    buf: Buffer,
    blkno: BlockNumber,
    page: Page,
    sharelock: bool,
    vmbuffer: Buffer,
) -> bool {
    let freespace: Size;

    if PageIsNew(page) {
        /*
         * All-zeroes pages can be left over if either a backend extends the
         * relation by a single page, but crashes before the newly initialized
         * page has been written out.
         *
         * Note we do not enter the page into the visibilitymap.
         *
         * Make sure these pages are in the FSM, to ensure they can be reused.
         */
        UnlockReleaseBuffer(buf);

        if GetRecordedFreeSpace((*vacrel).rel, blkno) == 0 {
            let fs: Size = BLCKSZ as Size - core::mem::size_of::<crate::storage::bufpage::PageHeaderData>();

            RecordPageWithFreeSpace((*vacrel).rel, blkno, fs);
        }

        return true;
    }

    if PageIsEmpty(page) {
        /*
         * It seems likely that caller will always be able to get a cleanup
         * lock on an empty page.  But don't take any chances -- escalate to
         * an exclusive lock.
         */
        if sharelock {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

            if !PageIsEmpty(page) {
                /* page isn't new or empty -- keep lock and pin for now */
                return false;
            }
        } else {
            /* Already have a full cleanup lock (which is more than enough) */
        }

        /*
         * Unlike new pages, empty pages are always set all-visible and
         * all-frozen.
         */
        if !PageIsAllVisible(page) {
            START_CRIT_SECTION!();

            /* mark buffer dirty before writing a WAL record */
            MarkBufferDirty(buf);

            /*
             * It's possible that another backend has extended the heap,
             * initialized the page, and then failed to WAL-log the page.
             */
            if RelationNeedsWAL((*vacrel).rel) && PageGetLSN(page) == 0 {
                log_newpage_buffer(buf, true);
            }

            PageSetAllVisible(page);
            visibilitymap_set((*vacrel).rel, blkno, buf,
                              InvalidXLogRecPtr,
                              vmbuffer, InvalidTransactionId,
                              VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);

            END_CRIT_SECTION!();

            /* Count the newly all-frozen pages for logging */
            (*vacrel).vm_new_visible_pages += 1;
            (*vacrel).vm_new_visible_frozen_pages += 1;
        }

        freespace = PageGetHeapFreeSpace(page);
        UnlockReleaseBuffer(buf);
        RecordPageWithFreeSpace((*vacrel).rel, blkno, freespace);
        return true;
    }

    /* page isn't new or empty -- keep lock and pin */
    false
}

/* qsort comparator for sorting OffsetNumbers */
unsafe fn cmpOffsetNumbers(a: *const c_void, b: *const c_void) -> c_int {
    pg_cmp_u16(*(a as *const OffsetNumber), *(b as *const OffsetNumber))
}

// ========================================================================
// Part 4: lazy_scan_prune + lazy_scan_noprune + lazy_vacuum + lazy_vacuum_all_indexes
//         + vacuum_reap_lp_read_stream_next + lazy_vacuum_heap_rel
//         + lazy_vacuum_heap_page + lazy_check_wraparound_failsafe
//         + lazy_cleanup_all_indexes + lazy_vacuum_one_index + lazy_cleanup_one_index
// ========================================================================

/*
 *	lazy_scan_prune() -- lazy_scan_heap() pruning and freezing.
 *
 * Caller must hold pin and buffer cleanup lock on the buffer.
 *
 * Returns the number of tuples deleted from the page during HOT pruning.
 */
unsafe fn lazy_scan_prune(
    vacrel: *mut LVRelState,
    buf: Buffer,
    blkno: BlockNumber,
    page: Page,
    vmbuffer: Buffer,
    all_visible_according_to_vm: bool,
    has_lpdead_items: *mut bool,
    vm_page_frozen: *mut bool,
) -> c_int {
    let rel: Relation = (*vacrel).rel;
    let mut presult: PruneFreezeResult = core::mem::zeroed();
    let mut prune_options: c_int = 0;

    Assert!(BufferGetBlockNumber(buf) == blkno);

    /*
     * Prune all HOT-update chains and potentially freeze tuples on this page.
     *
     * If the relation has no indexes, we can immediately mark would-be dead
     * items LP_UNUSED.
     */
    prune_options = HEAP_PAGE_PRUNE_FREEZE;
    if (*vacrel).nindexes == 0 {
        prune_options |= HEAP_PAGE_PRUNE_MARK_UNUSED_NOW;
    }

    heap_page_prune_and_freeze(rel, buf, (*vacrel).vistest, prune_options,
                               &(*vacrel).cutoffs, &mut presult, PRUNE_VACUUM_SCAN,
                               &mut (*vacrel).offnum,
                               &mut (*vacrel).NewRelfrozenXid, &mut (*vacrel).NewRelminMxid);

    Assert!(MultiXactIdIsValid((*vacrel).NewRelminMxid));
    Assert!(TransactionIdIsValid((*vacrel).NewRelfrozenXid));

    if presult.nfrozen > 0 {
        /*
         * We don't increment the new_frozen_tuple_pages instrumentation
         * counter when nfrozen == 0.
         */
        (*vacrel).new_frozen_tuple_pages += 1;
    }

    /*
     * VACUUM will call heap_page_is_all_visible() during the second pass over
     * the heap to determine all_visible and all_frozen for the page.
     */
    #[cfg(any())] /* USE_ASSERT_CHECKING */
    {
        /* Note that all_frozen value does not matter when !all_visible */
        if presult.all_visible {
            let mut debug_cutoff: TransactionId = 0;
            let mut debug_all_frozen: bool = false;

            Assert!(presult.lpdead_items == 0);

            if !heap_page_is_all_visible(vacrel, buf, &mut debug_cutoff, &mut debug_all_frozen) {
                Assert!(false);
            }

            Assert!(presult.all_frozen == debug_all_frozen);
            Assert!(!TransactionIdIsValid(debug_cutoff) ||
                    debug_cutoff == presult.vm_conflict_horizon);
        }
    }

    /*
     * Now save details of the LP_DEAD items from the page in vacrel
     */
    if presult.lpdead_items > 0 {
        (*vacrel).lpdead_item_pages += 1;

        /*
         * deadoffsets are collected incrementally in
         * heap_page_prune_and_freeze() as each dead line pointer is recorded,
         * with an indeterminate order, but dead_items_add requires them to be
         * sorted.
         */
        qsort(presult.deadoffsets.as_mut_ptr() as *mut c_void,
              presult.lpdead_items as usize,
              core::mem::size_of::<OffsetNumber>(),
              Some(cmpOffsetNumbers));

        dead_items_add(vacrel, blkno, presult.deadoffsets.as_mut_ptr(), presult.lpdead_items);
    }

    /* Finally, add page-local counts to whole-VACUUM counts */
    (*vacrel).tuples_deleted += presult.ndeleted as int64;
    (*vacrel).tuples_frozen += presult.nfrozen as int64;
    (*vacrel).lpdead_items += presult.lpdead_items as int64;
    (*vacrel).live_tuples += presult.live_tuples as int64;
    (*vacrel).recently_dead_tuples += presult.recently_dead_tuples as int64;

    /* Can't truncate this page */
    if presult.hastup {
        (*vacrel).nonempty_pages = blkno + 1;
    }

    /* Did we find LP_DEAD items? */
    *has_lpdead_items = presult.lpdead_items > 0;

    Assert!(!presult.all_visible || !(*has_lpdead_items));

    /*
     * Handle setting visibility map bit based on information from the VM
     * and from all_visible and all_frozen variables
     */
    if !all_visible_according_to_vm && presult.all_visible {
        let mut old_vmbits: uint8;
        let mut flags: uint8 = VISIBILITYMAP_ALL_VISIBLE;

        if presult.all_frozen {
            Assert!(!TransactionIdIsValid(presult.vm_conflict_horizon));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        /*
         * It should never be the case that the visibility map page is set
         * while the page-level bit is clear, but the reverse is allowed.
         */
        PageSetAllVisible(page);
        MarkBufferDirty(buf);
        old_vmbits = visibilitymap_set((*vacrel).rel, blkno, buf,
                                       InvalidXLogRecPtr,
                                       vmbuffer, presult.vm_conflict_horizon,
                                       flags);

        /*
         * If the page wasn't already set all-visible and/or all-frozen in the
         * VM, count it as newly set for logging.
         */
        if (old_vmbits & VISIBILITYMAP_ALL_VISIBLE) == 0 {
            (*vacrel).vm_new_visible_pages += 1;
            if presult.all_frozen {
                (*vacrel).vm_new_visible_frozen_pages += 1;
                *vm_page_frozen = true;
            }
        } else if (old_vmbits & VISIBILITYMAP_ALL_FROZEN) == 0 && presult.all_frozen {
            (*vacrel).vm_new_frozen_pages += 1;
            *vm_page_frozen = true;
        }
    }
    /*
     * As of PostgreSQL 9.2, the visibility map bit should never be set if the
     * page-level bit is clear.
     */
    else if all_visible_according_to_vm && !PageIsAllVisible(page) &&
        visibilitymap_get_status((*vacrel).rel, blkno, &mut { vmbuffer } as *mut Buffer) != 0
    {
        ereport!(WARNING,
                 errmsg!("page is not marked all-visible but visibility map bit is set in relation \"{}\" page {}",
                         std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy(),
                         blkno));
        visibilitymap_clear((*vacrel).rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
    }
    /*
     * There should never be LP_DEAD items on a page with PD_ALL_VISIBLE set.
     */
    else if presult.lpdead_items > 0 && PageIsAllVisible(page) {
        ereport!(WARNING,
                 errmsg!("page containing LP_DEAD items is marked as all-visible in relation \"{}\" page {}",
                         std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy(),
                         blkno));
        PageClearAllVisible(page);
        MarkBufferDirty(buf);
        visibilitymap_clear((*vacrel).rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
    }
    /*
     * If the all-visible page is all-frozen but not marked as such yet, mark
     * it as all-frozen.
     */
    else if all_visible_according_to_vm && presult.all_visible &&
        presult.all_frozen && !VM_ALL_FROZEN((*vacrel).rel, blkno, &mut { vmbuffer } as *mut Buffer)
    {
        let old_vmbits: uint8;

        /*
         * Avoid relying on all_visible_according_to_vm as a proxy for the
         * page-level PD_ALL_VISIBLE bit being set.
         */
        if !PageIsAllVisible(page) {
            PageSetAllVisible(page);
            MarkBufferDirty(buf);
        }

        /*
         * Set the page all-frozen (and all-visible) in the VM.
         */
        Assert!(!TransactionIdIsValid(presult.vm_conflict_horizon));
        old_vmbits = visibilitymap_set((*vacrel).rel, blkno, buf,
                                       InvalidXLogRecPtr,
                                       vmbuffer, InvalidTransactionId,
                                       VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);

        if (old_vmbits & VISIBILITYMAP_ALL_VISIBLE) == 0 {
            (*vacrel).vm_new_visible_pages += 1;
            (*vacrel).vm_new_visible_frozen_pages += 1;
            *vm_page_frozen = true;
        } else {
            /*
             * We already checked that the page was not set all-frozen in the VM
             * above, so we don't need to test the value of old_vmbits.
             */
            (*vacrel).vm_new_frozen_pages += 1;
            *vm_page_frozen = true;
        }
    }

    presult.ndeleted
}

/*
 *	lazy_scan_noprune() -- lazy_scan_prune() without pruning or freezing
 *
 * Returns true when processing is complete; false if caller needs to
 * acquire a cleanup lock and call lazy_scan_prune() instead.
 */
unsafe fn lazy_scan_noprune(
    vacrel: *mut LVRelState,
    buf: Buffer,
    blkno: BlockNumber,
    page: Page,
    has_lpdead_items: *mut bool,
) -> bool {
    let mut offnum: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut lpdead_items: c_int;
    let mut live_tuples: c_int;
    let mut recently_dead_tuples: c_int;
    let mut missed_dead_tuples: c_int;
    let mut hastup: bool;
    let mut tupleheader: HeapTupleHeader;
    let mut NoFreezePageRelfrozenXid: TransactionId = (*vacrel).NewRelfrozenXid;
    let mut NoFreezePageRelminMxid: MultiXactId = (*vacrel).NewRelminMxid;
    let mut deadoffsets: [OffsetNumber; MaxHeapTuplesPerPage as usize] =
        [0; MaxHeapTuplesPerPage as usize];

    Assert!(BufferGetBlockNumber(buf) == blkno);

    hastup = false; /* for now */

    lpdead_items = 0;
    live_tuples = 0;
    recently_dead_tuples = 0;
    missed_dead_tuples = 0;

    maxoff = PageGetMaxOffsetNumber(page);
    offnum = FirstOffsetNumber;
    while offnum <= maxoff {
        let itemid: ItemId;
        let mut tuple: HeapTupleData = core::mem::zeroed();

        (*vacrel).offnum = offnum;
        itemid = PageGetItemId(page, offnum);

        if !ItemIdIsUsed(itemid) {
            offnum = OffsetNumberNext(offnum);
            continue;
        }

        if ItemIdIsRedirected(itemid) {
            hastup = true;
            offnum = OffsetNumberNext(offnum);
            continue;
        }

        if ItemIdIsDead(itemid) {
            /*
             * Deliberately don't set hastup=true here.  See same point in
             * lazy_scan_prune for an explanation.
             */
            deadoffsets[lpdead_items as usize] = offnum;
            lpdead_items += 1;
            offnum = OffsetNumberNext(offnum);
            continue;
        }

        hastup = true; /* page prevents rel truncation */
        tupleheader = PageGetItem(page, itemid) as HeapTupleHeader;
        if heap_tuple_should_freeze(tupleheader, &(*vacrel).cutoffs,
                                    &mut NoFreezePageRelfrozenXid,
                                    &mut NoFreezePageRelminMxid)
        {
            /* Tuple with XID < FreezeLimit (or MXID < MultiXactCutoff) */
            if (*vacrel).aggressive {
                /*
                 * Aggressive VACUUMs must always be able to advance rel's
                 * relfrozenxid to a value >= FreezeLimit. The only safe
                 * option is to have caller perform processing of this page
                 * using lazy_scan_prune.
                 */
                (*vacrel).offnum = InvalidOffsetNumber;
                return false;
            }

            /*
             * Non-aggressive VACUUMs are under no obligation to advance
             * relfrozenxid (even by one XID).  We can be much laxer here.
             */
        }

        ItemPointerSet(&mut tuple.t_self, blkno, offnum);
        tuple.t_data = PageGetItem(page, itemid) as HeapTupleHeader;
        tuple.t_len = ItemIdGetLength(itemid);
        tuple.t_tableOid = RelationGetRelid((*vacrel).rel);

        match HeapTupleSatisfiesVacuum(&tuple, (*vacrel).cutoffs.OldestXmin, buf) {
            HEAPTUPLE_DELETE_IN_PROGRESS |
            HEAPTUPLE_LIVE => {
                /*
                 * Count both cases as live, just like lazy_scan_prune
                 */
                live_tuples += 1;
            }
            HEAPTUPLE_DEAD => {
                /*
                 * There is some useful work for pruning to do.
                 */
                missed_dead_tuples += 1;
            }
            HEAPTUPLE_RECENTLY_DEAD => {
                /*
                 * Count in recently_dead_tuples, just like lazy_scan_prune
                 */
                recently_dead_tuples += 1;
            }
            HEAPTUPLE_INSERT_IN_PROGRESS => {
                /*
                 * Do not count these rows as live, just like lazy_scan_prune
                 */
            }
            _ => {
                ereport!(ERROR, errmsg!("unexpected HeapTupleSatisfiesVacuum result"));
            }
        }

        offnum = OffsetNumberNext(offnum);
    }

    (*vacrel).offnum = InvalidOffsetNumber;

    /*
     * By here we know for sure that caller can put off freezing and pruning
     * this particular page until the next VACUUM.
     */
    (*vacrel).NewRelfrozenXid = NoFreezePageRelfrozenXid;
    (*vacrel).NewRelminMxid = NoFreezePageRelminMxid;

    /* Save any LP_DEAD items found on the page in dead_items */
    if (*vacrel).nindexes == 0 {
        /* Using one-pass strategy (since table has no indexes) */
        if lpdead_items > 0 {
            /*
             * Perfunctory handling for the corner case where a single pass
             * strategy VACUUM cannot get a cleanup lock, and it turns out
             * that there is one or more LP_DEAD items.
             */
            hastup = true;
            missed_dead_tuples += lpdead_items;
        }
    } else if lpdead_items > 0 {
        /*
         * Page has LP_DEAD items, and so any references/TIDs that remain in
         * indexes will be deleted during index vacuuming.
         */
        (*vacrel).lpdead_item_pages += 1;

        dead_items_add(vacrel, blkno, deadoffsets.as_mut_ptr(), lpdead_items);

        (*vacrel).lpdead_items += lpdead_items as int64;
    }

    /*
     * Finally, add relevant page-local counts to whole-VACUUM counts
     */
    (*vacrel).live_tuples += live_tuples as int64;
    (*vacrel).recently_dead_tuples += recently_dead_tuples as int64;
    (*vacrel).missed_dead_tuples += missed_dead_tuples as int64;
    if missed_dead_tuples > 0 {
        (*vacrel).missed_dead_pages += 1;
    }

    /* Can't truncate this page */
    if hastup {
        (*vacrel).nonempty_pages = blkno + 1;
    }

    /* Did we find LP_DEAD items? */
    *has_lpdead_items = lpdead_items > 0;

    /* Caller won't need to call lazy_scan_prune with same page */
    true
}

/*
 * Main entry point for index vacuuming and heap vacuuming.
 *
 * Removes items collected in dead_items from table's indexes, then marks the
 * same items LP_UNUSED in the heap.
 */
unsafe fn lazy_vacuum(vacrel: *mut LVRelState) {
    let bypass: bool;

    /* Should not end up here with no indexes */
    Assert!((*vacrel).nindexes > 0);
    Assert!((*vacrel).lpdead_item_pages > 0);

    if !(*vacrel).do_index_vacuuming {
        Assert!(!(*vacrel).do_index_cleanup);
        dead_items_reset(vacrel);
        return;
    }

    /*
     * Consider bypassing index vacuuming (and heap vacuuming) entirely.
     */
    let mut bypass_local = false;
    if (*vacrel).consider_bypass_optimization && (*vacrel).rel_pages > 0 {
        let threshold: BlockNumber;

        Assert!((*vacrel).num_index_scans == 0);
        Assert!((*vacrel).lpdead_items == (*(*vacrel).dead_items_info).num_items as int64);
        Assert!((*vacrel).do_index_vacuuming);
        Assert!((*vacrel).do_index_cleanup);

        threshold = ((*vacrel).rel_pages as f64 * BYPASS_THRESHOLD_PAGES) as BlockNumber;
        bypass_local = ((*vacrel).lpdead_item_pages < threshold &&
                        TidStoreMemoryUsage((*vacrel).dead_items) < 32 * 1024 * 1024);
    }
    bypass = bypass_local;

    if bypass {
        /*
         * There are almost zero TIDs.  Behave as if there were precisely
         * zero: bypass index vacuuming, but do index cleanup.
         */
        (*vacrel).do_index_vacuuming = false;
    } else if lazy_vacuum_all_indexes(vacrel) {
        /*
         * We successfully completed a round of index vacuuming.  Do related
         * heap vacuuming now.
         */
        lazy_vacuum_heap_rel(vacrel);
    } else {
        /*
         * Failsafe case.
         */
        Assert!(VacuumFailsafeActive);
    }

    /*
     * Forget the LP_DEAD items that we just vacuumed (or just decided to not
     * vacuum)
     */
    dead_items_reset(vacrel);
}

/*
 *	lazy_vacuum_all_indexes() -- Main entry for index vacuuming
 *
 * Returns true in the common case when all indexes were successfully
 * vacuumed.  Returns false in rare cases where we determined that the
 * ongoing VACUUM operation is at risk of taking too long to finish.
 */
unsafe fn lazy_vacuum_all_indexes(vacrel: *mut LVRelState) -> bool {
    let mut allindexes: bool = true;
    let old_live_tuples: f64 = /* rel->rd_rel->reltuples */ 0.0;
    let progress_start_index: [c_int; 2] = [
        PROGRESS_VACUUM_PHASE,
        PROGRESS_VACUUM_INDEXES_TOTAL,
    ];
    let progress_end_index: [c_int; 3] = [
        PROGRESS_VACUUM_INDEXES_TOTAL,
        PROGRESS_VACUUM_INDEXES_PROCESSED,
        PROGRESS_VACUUM_NUM_INDEX_VACUUMS,
    ];
    let mut progress_start_val: [int64; 2] = [0; 2];
    let mut progress_end_val: [int64; 3] = [0; 3];

    Assert!((*vacrel).nindexes > 0);
    Assert!((*vacrel).do_index_vacuuming);
    Assert!((*vacrel).do_index_cleanup);

    /* Precheck for XID wraparound emergencies */
    if lazy_check_wraparound_failsafe(vacrel) {
        /* Wraparound emergency -- don't even start an index scan */
        return false;
    }

    /*
     * Report that we are now vacuuming indexes.
     */
    progress_start_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_INDEX as int64;
    progress_start_val[1] = (*vacrel).nindexes as int64;
    pgstat_progress_update_multi_param(2, progress_start_index.as_ptr(), progress_start_val.as_ptr());

    if !ParallelVacuumIsActive!(vacrel) {
        for idx in 0..(*vacrel).nindexes {
            let indrel: Relation = *(*vacrel).indrels.add(idx as usize);
            let istat: *mut IndexBulkDeleteResult = *(*vacrel).indstats.add(idx as usize);

            *(*vacrel).indstats.add(idx as usize) =
                lazy_vacuum_one_index(indrel, istat, old_live_tuples, vacrel);

            /* Report the number of indexes vacuumed */
            pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED,
                                         (idx + 1) as int64);

            if lazy_check_wraparound_failsafe(vacrel) {
                /* Wraparound emergency -- end current index scan */
                allindexes = false;
                break;
            }
        }
    } else {
        /* Outsource everything to parallel variant */
        parallel_vacuum_bulkdel_all_indexes((*vacrel).pvs, old_live_tuples,
                                            (*vacrel).num_index_scans);

        /*
         * Do a postcheck to consider applying wraparound failsafe now.
         */
        if lazy_check_wraparound_failsafe(vacrel) {
            allindexes = false;
        }
    }

    Assert!((*vacrel).num_index_scans > 0 ||
            (*(*vacrel).dead_items_info).num_items as int64 == (*vacrel).lpdead_items);
    Assert!(allindexes || VacuumFailsafeActive);

    /*
     * Increase and report the number of index scans.
     */
    (*vacrel).num_index_scans += 1;
    progress_end_val[0] = 0;
    progress_end_val[1] = 0;
    progress_end_val[2] = (*vacrel).num_index_scans as int64;
    pgstat_progress_update_multi_param(3, progress_end_index.as_ptr(), progress_end_val.as_ptr());

    allindexes
}

/*
 * Read stream callback for vacuum's third phase (second pass over the heap).
 */
unsafe fn vacuum_reap_lp_read_stream_next(
    _stream: *mut ReadStream,
    callback_private_data: *mut c_void,
    per_buffer_data: *mut c_void,
) -> BlockNumber {
    let iter: *mut TidStoreIter = callback_private_data as *mut TidStoreIter;
    let iter_result: *mut TidStoreIterResult;

    iter_result = TidStoreIterateNext(iter);
    if iter_result.is_null() {
        return InvalidBlockNumber;
    }

    /*
     * Save the TidStoreIterResult for later, so we can extract the offsets.
     * It is safe to copy the result, according to TidStoreIterateNext().
     */
    core::ptr::copy_nonoverlapping(
        iter_result as *const u8,
        per_buffer_data as *mut u8,
        core::mem::size_of::<TidStoreIterResult>(),
    );

    (*iter_result).blkno
}

/*
 *	lazy_vacuum_heap_rel() -- second pass over the heap for two pass strategy
 *
 * This routine marks LP_DEAD items in vacrel->dead_items as LP_UNUSED.
 */
unsafe fn lazy_vacuum_heap_rel(vacrel: *mut LVRelState) {
    let mut stream: *mut ReadStream;
    let mut vacuumed_pages: BlockNumber = 0;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut saved_err_info: LVSavedErrInfo = core::mem::zeroed();
    let iter: *mut TidStoreIter;

    Assert!((*vacrel).do_index_vacuuming);
    Assert!((*vacrel).do_index_cleanup);
    Assert!((*vacrel).num_index_scans > 0);

    /* Report that we are now vacuuming the heap */
    pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
                                 PROGRESS_VACUUM_PHASE_VACUUM_HEAP as int64);

    /* Update error traceback information */
    update_vacuum_error_info(vacrel, &mut saved_err_info,
                             VACUUM_ERRCB_PHASE_VACUUM_HEAP,
                             InvalidBlockNumber, InvalidOffsetNumber);

    iter = TidStoreBeginIterate((*vacrel).dead_items);

    /*
     * Set up the read stream for vacuum's second pass through the heap.
     *
     * It is safe to use batchmode.
     */
    stream = read_stream_begin_relation(
        READ_STREAM_MAINTENANCE | READ_STREAM_USE_BATCHING,
        (*vacrel).bstrategy,
        (*vacrel).rel,
        MAIN_FORKNUM,
        Some(vacuum_reap_lp_read_stream_next),
        iter as *mut c_void,
        core::mem::size_of::<TidStoreIterResult>(),
    );

    loop {
        let blkno: BlockNumber;
        let buf: Buffer;
        let page: Page;
        let mut iter_result: *mut TidStoreIterResult = std::ptr::null_mut();
        let freespace: Size;
        let mut offsets: [OffsetNumber; MaxOffsetNumber as usize] =
            [0; MaxOffsetNumber as usize];
        let num_offsets: c_int;

        vacuum_delay_point(false);

        buf = read_stream_next_buffer(stream, &mut (iter_result as *mut c_void));

        /* The relation is exhausted */
        if !BufferIsValid(buf) {
            break;
        }

        (*vacrel).blkno = BufferGetBlockNumber(buf);
        blkno = (*vacrel).blkno;

        Assert!(!iter_result.is_null());
        num_offsets = TidStoreGetBlockOffsets(iter_result, offsets.as_mut_ptr(),
                                              lengthof!(offsets));
        Assert!(num_offsets <= lengthof!(offsets));

        /*
         * Pin the visibility map page in case we need to mark the page
         * all-visible.
         */
        visibilitymap_pin((*vacrel).rel, blkno, &mut vmbuffer);

        /* We need a non-cleanup exclusive lock to mark dead_items unused */
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        lazy_vacuum_heap_page(vacrel, blkno, buf, offsets.as_mut_ptr(),
                              num_offsets, vmbuffer);

        /* Now that we've vacuumed the page, record its available space */
        page = BufferGetPage(buf);
        freespace = PageGetHeapFreeSpace(page);

        UnlockReleaseBuffer(buf);
        RecordPageWithFreeSpace((*vacrel).rel, blkno, freespace);
        vacuumed_pages += 1;
    }

    read_stream_end(stream);
    TidStoreEndIterate(iter);

    (*vacrel).blkno = InvalidBlockNumber;
    if BufferIsValid(vmbuffer) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * We set all LP_DEAD items from the first heap pass to LP_UNUSED during
     * the second heap pass.
     */
    Assert!((*vacrel).num_index_scans > 1 ||
            ((*(*vacrel).dead_items_info).num_items as int64 == (*vacrel).lpdead_items &&
             vacuumed_pages == (*vacrel).lpdead_item_pages));

    ereport!(DEBUG2,
             errmsg!("table \"{}\": removed {} dead item identifiers in {} pages",
                     std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy(),
                     (*(*vacrel).dead_items_info).num_items,
                     vacuumed_pages));

    /* Revert to the previous phase information for error traceback */
    restore_vacuum_error_info(vacrel, &saved_err_info);
}

/*
 *	lazy_vacuum_heap_page() -- free page's LP_DEAD items listed in the
 *						  vacrel->dead_items store.
 */
unsafe fn lazy_vacuum_heap_page(
    vacrel: *mut LVRelState,
    blkno: BlockNumber,
    buffer: Buffer,
    deadoffsets: *mut OffsetNumber,
    num_offsets: c_int,
    vmbuffer: Buffer,
) {
    let page: Page = BufferGetPage(buffer);
    let mut unused: [OffsetNumber; MaxHeapTuplesPerPage as usize] =
        [0; MaxHeapTuplesPerPage as usize];
    let mut nunused: c_int = 0;
    let mut visibility_cutoff_xid: TransactionId = 0;
    let mut all_frozen: bool = false;
    let mut saved_err_info: LVSavedErrInfo = core::mem::zeroed();

    Assert!((*vacrel).do_index_vacuuming);

    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno as int64);

    /* Update error traceback information */
    update_vacuum_error_info(vacrel, &mut saved_err_info,
                             VACUUM_ERRCB_PHASE_VACUUM_HEAP, blkno,
                             InvalidOffsetNumber);

    START_CRIT_SECTION!();

    for i in 0..num_offsets {
        let itemid: ItemId;
        let toff: OffsetNumber = *deadoffsets.add(i as usize);

        itemid = PageGetItemId(page, toff);

        Assert!(ItemIdIsDead(itemid) && !ItemIdHasStorage(itemid));
        ItemIdSetUnused(itemid);
        unused[nunused as usize] = toff;
        nunused += 1;
    }

    Assert!(nunused > 0);

    /* Attempt to truncate line pointer array now */
    PageTruncateLinePointerArray(page);

    /*
     * Mark buffer dirty before we write WAL.
     */
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if RelationNeedsWAL((*vacrel).rel) {
        log_heap_prune_and_freeze((*vacrel).rel, buffer,
                                  InvalidTransactionId,
                                  false, /* no cleanup lock required */
                                  PRUNE_VACUUM_CLEANUP,
                                  std::ptr::null(), 0, /* frozen */
                                  std::ptr::null(), 0, /* redirected */
                                  std::ptr::null(), 0, /* dead */
                                  unused.as_ptr(), nunused);
    }

    /*
     * End critical section, so we safely can do visibility tests.
     */
    END_CRIT_SECTION!();

    /*
     * Now that we have removed the LP_DEAD items from the page, once again
     * check if the page has become all-visible.
     */
    Assert!(!PageIsAllVisible(page));
    if heap_page_is_all_visible(vacrel, buffer, &mut visibility_cutoff_xid, &mut all_frozen) {
        let mut flags: uint8 = VISIBILITYMAP_ALL_VISIBLE;

        if all_frozen {
            Assert!(!TransactionIdIsValid(visibility_cutoff_xid));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        PageSetAllVisible(page);
        visibilitymap_set((*vacrel).rel, blkno, buffer,
                          InvalidXLogRecPtr,
                          vmbuffer, visibility_cutoff_xid,
                          flags);

        /* Count the newly set VM page for logging */
        (*vacrel).vm_new_visible_pages += 1;
        if all_frozen {
            (*vacrel).vm_new_visible_frozen_pages += 1;
        }
    }

    /* Revert to the previous phase information for error traceback */
    restore_vacuum_error_info(vacrel, &saved_err_info);
}

/*
 * Trigger the failsafe to avoid wraparound failure.
 *
 * Returns true when failsafe has been triggered.
 */
unsafe fn lazy_check_wraparound_failsafe(vacrel: *mut LVRelState) -> bool {
    /* Don't warn more than once per VACUUM */
    if VacuumFailsafeActive {
        return true;
    }

    if unlikely!(vacuum_xid_failsafe_check(&(*vacrel).cutoffs)) {
        let progress_index: [c_int; 2] = [
            PROGRESS_VACUUM_INDEXES_TOTAL,
            PROGRESS_VACUUM_INDEXES_PROCESSED,
        ];
        let progress_val: [int64; 2] = [0, 0];

        VacuumFailsafeActive = true;

        /*
         * Abandon use of a buffer access strategy to allow use of all of
         * shared buffers.
         */
        (*vacrel).bstrategy = std::ptr::null_mut();

        /* Disable index vacuuming, index cleanup, and heap rel truncation */
        (*vacrel).do_index_vacuuming = false;
        (*vacrel).do_index_cleanup = false;
        (*vacrel).do_rel_truncate = false;

        /* Reset the progress counters */
        pgstat_progress_update_multi_param(2, progress_index.as_ptr(), progress_val.as_ptr());

        ereport!(WARNING,
                 errmsg!("bypassing nonessential maintenance of table \"{}.{}.{}\" as a failsafe after {} index scans",
                         std::ffi::CStr::from_ptr((*vacrel).dbname).to_string_lossy(),
                         std::ffi::CStr::from_ptr((*vacrel).relnamespace).to_string_lossy(),
                         std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy(),
                         (*vacrel).num_index_scans));
        /* C also: errdetail/errhint about relfrozenxid and maintenance_work_mem */

        /* Stop applying cost limits from this point on */
        VacuumCostActive = false;
        VacuumCostBalance = 0;

        return true;
    }

    false
}

/*
 *	lazy_cleanup_all_indexes() -- cleanup all indexes of relation.
 */
unsafe fn lazy_cleanup_all_indexes(vacrel: *mut LVRelState) {
    let reltuples: f64 = (*vacrel).new_rel_tuples;
    let estimated_count: bool = (*vacrel).scanned_pages < (*vacrel).rel_pages;
    let progress_start_index: [c_int; 2] = [
        PROGRESS_VACUUM_PHASE,
        PROGRESS_VACUUM_INDEXES_TOTAL,
    ];
    let progress_end_index: [c_int; 2] = [
        PROGRESS_VACUUM_INDEXES_TOTAL,
        PROGRESS_VACUUM_INDEXES_PROCESSED,
    ];
    let mut progress_start_val: [int64; 2] = [0; 2];
    let progress_end_val: [int64; 2] = [0, 0];

    Assert!((*vacrel).do_index_cleanup);
    Assert!((*vacrel).nindexes > 0);

    /*
     * Report that we are now cleaning up indexes.
     */
    progress_start_val[0] = PROGRESS_VACUUM_PHASE_INDEX_CLEANUP as int64;
    progress_start_val[1] = (*vacrel).nindexes as int64;
    pgstat_progress_update_multi_param(2, progress_start_index.as_ptr(), progress_start_val.as_ptr());

    if !ParallelVacuumIsActive!(vacrel) {
        for idx in 0..(*vacrel).nindexes {
            let indrel: Relation = *(*vacrel).indrels.add(idx as usize);
            let istat: *mut IndexBulkDeleteResult = *(*vacrel).indstats.add(idx as usize);

            *(*vacrel).indstats.add(idx as usize) =
                lazy_cleanup_one_index(indrel, istat, reltuples, estimated_count, vacrel);

            /* Report the number of indexes cleaned up */
            pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED,
                                         (idx + 1) as int64);
        }
    } else {
        /* Outsource everything to parallel variant */
        parallel_vacuum_cleanup_all_indexes((*vacrel).pvs, reltuples,
                                            (*vacrel).num_index_scans, estimated_count);
    }

    /* Reset the progress counters */
    pgstat_progress_update_multi_param(2, progress_end_index.as_ptr(), progress_end_val.as_ptr());
}

/*
 *	lazy_vacuum_one_index() -- vacuum index relation.
 *
 * Returns bulk delete stats derived from input stats
 */
unsafe fn lazy_vacuum_one_index(
    indrel: Relation,
    istat: *mut IndexBulkDeleteResult,
    reltuples: f64,
    vacrel: *mut LVRelState,
) -> *mut IndexBulkDeleteResult {
    let mut ivinfo: IndexVacuumInfo = core::mem::zeroed();
    let mut saved_err_info: LVSavedErrInfo = core::mem::zeroed();
    let result: *mut IndexBulkDeleteResult;

    ivinfo.index = indrel;
    ivinfo.heaprel = (*vacrel).rel;
    ivinfo.analyze_only = false;
    ivinfo.report_progress = false;
    ivinfo.estimated_count = true;
    ivinfo.message_level = DEBUG2;
    ivinfo.num_heap_tuples = reltuples;
    ivinfo.strategy = (*vacrel).bstrategy as *mut std::ffi::c_void;

    /*
     * Update error traceback information.
     */
    Assert!((*vacrel).indname.is_null());
    (*vacrel).indname = pstrdup(RelationGetRelationName(indrel) as *const c_char);
    update_vacuum_error_info(vacrel, &mut saved_err_info,
                             VACUUM_ERRCB_PHASE_VACUUM_INDEX,
                             InvalidBlockNumber, InvalidOffsetNumber);

    /* Do bulk deletion */
    let result = vac_bulkdel_one_index(&mut ivinfo, istat, (*vacrel).dead_items,
                                       (*vacrel).dead_items_info);

    /* Revert to the previous phase information for error traceback */
    restore_vacuum_error_info(vacrel, &saved_err_info);
    pfree((*vacrel).indname as *mut c_void);
    (*vacrel).indname = std::ptr::null_mut();

    result
}

/*
 *	lazy_cleanup_one_index() -- do post-vacuum cleanup for index relation.
 *
 * Returns bulk delete stats derived from input stats
 */
unsafe fn lazy_cleanup_one_index(
    indrel: Relation,
    istat: *mut IndexBulkDeleteResult,
    reltuples: f64,
    estimated_count: bool,
    vacrel: *mut LVRelState,
) -> *mut IndexBulkDeleteResult {
    let mut ivinfo: IndexVacuumInfo = core::mem::zeroed();
    let mut saved_err_info: LVSavedErrInfo = core::mem::zeroed();
    let result: *mut IndexBulkDeleteResult;

    ivinfo.index = indrel;
    ivinfo.heaprel = (*vacrel).rel;
    ivinfo.analyze_only = false;
    ivinfo.report_progress = false;
    ivinfo.estimated_count = estimated_count;
    ivinfo.message_level = DEBUG2;
    ivinfo.num_heap_tuples = reltuples;
    ivinfo.strategy = (*vacrel).bstrategy as *mut std::ffi::c_void;

    /*
     * Update error traceback information.
     */
    Assert!((*vacrel).indname.is_null());
    (*vacrel).indname = pstrdup(RelationGetRelationName(indrel) as *const c_char);
    update_vacuum_error_info(vacrel, &mut saved_err_info,
                             VACUUM_ERRCB_PHASE_INDEX_CLEANUP,
                             InvalidBlockNumber, InvalidOffsetNumber);

    let result = vac_cleanup_one_index(&mut ivinfo, istat);

    /* Revert to the previous phase information for error traceback */
    restore_vacuum_error_info(vacrel, &saved_err_info);
    pfree((*vacrel).indname as *mut c_void);
    (*vacrel).indname = std::ptr::null_mut();

    result
}

// ========================================================================
// Part 5: should_attempt_truncation + lazy_truncate_heap +
//         count_nondeletable_pages + dead_items_alloc + dead_items_add +
//         dead_items_reset + dead_items_cleanup + heap_page_is_all_visible +
//         update_relstats_all_indexes + vacuum_error_callback +
//         update_vacuum_error_info + restore_vacuum_error_info
// ========================================================================

/*
 * should_attempt_truncation - should we attempt to truncate the heap?
 */
unsafe fn should_attempt_truncation(vacrel: *mut LVRelState) -> bool {
    let possibly_freeable: BlockNumber;

    if !(*vacrel).do_rel_truncate || VacuumFailsafeActive {
        return false;
    }

    possibly_freeable = (*vacrel).rel_pages - (*vacrel).nonempty_pages;
    if possibly_freeable > 0 &&
        (possibly_freeable >= REL_TRUNCATE_MINIMUM ||
         possibly_freeable >= (*vacrel).rel_pages / REL_TRUNCATE_FRACTION)
    {
        return true;
    }

    false
}

/*
 * lazy_truncate_heap - try to truncate off any empty pages at the end
 */
unsafe fn lazy_truncate_heap(vacrel: *mut LVRelState) {
    let mut orig_rel_pages: BlockNumber = (*vacrel).rel_pages;
    let mut new_rel_pages: BlockNumber;
    let mut lock_waiter_detected: bool = false;
    let mut lock_retry: c_int;

    /* Report that we are now truncating */
    pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
                                 PROGRESS_VACUUM_PHASE_TRUNCATE as int64);

    /* Update error traceback information one last time */
    update_vacuum_error_info(vacrel, std::ptr::null_mut(), VACUUM_ERRCB_PHASE_TRUNCATE,
                             (*vacrel).nonempty_pages, InvalidOffsetNumber);

    /*
     * Loop until no more truncating can be done.
     */
    loop {
        /*
         * We need full exclusive lock on the relation in order to do
         * truncation.
         */
        lock_waiter_detected = false;
        lock_retry = 0;
        loop {
            if ConditionalLockRelation((*vacrel).rel, AccessExclusiveLock) {
                break;
            }

            /*
             * Check for interrupts while trying to (re-)acquire the exclusive
             * lock.
             */
            CHECK_FOR_INTERRUPTS!();

            lock_retry += 1;
            if lock_retry > (VACUUM_TRUNCATE_LOCK_TIMEOUT /
                             VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL)
            {
                /*
                 * We failed to establish the lock in the specified number of
                 * retries. This means we give up truncating.
                 */
                ereport!(if (*vacrel).verbose { INFO } else { DEBUG2 },
                         errmsg!("\"{}\": stopping truncate due to conflicting lock request",
                                 std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy()));
                return;
            }

            WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL,
                      WAIT_EVENT_VACUUM_TRUNCATE);
            ResetLatch(MyLatch);
        }

        /*
         * Now that we have exclusive lock, look to see if the rel has grown
         * whilst we were vacuuming with non-exclusive lock.
         */
        new_rel_pages = RelationGetNumberOfBlocks((*vacrel).rel);
        if new_rel_pages != orig_rel_pages {
            /*
             * Note: we intentionally don't update vacrel->rel_pages with the
             * new rel size here.
             */
            UnlockRelation((*vacrel).rel, AccessExclusiveLock);
            return;
        }

        /*
         * Scan backwards from the end to verify that the end pages actually
         * contain no tuples.
         */
        new_rel_pages = count_nondeletable_pages(vacrel, &mut lock_waiter_detected);
        (*vacrel).blkno = new_rel_pages;

        if new_rel_pages >= orig_rel_pages {
            /* can't do anything after all */
            UnlockRelation((*vacrel).rel, AccessExclusiveLock);
            return;
        }

        /*
         * Okay to truncate.
         */
        RelationTruncate((*vacrel).rel, new_rel_pages);

        /*
         * We can release the exclusive lock as soon as we have truncated.
         */
        UnlockRelation((*vacrel).rel, AccessExclusiveLock);

        /*
         * Update statistics.
         */
        (*vacrel).removed_pages += orig_rel_pages - new_rel_pages;
        (*vacrel).rel_pages = new_rel_pages;

        ereport!(if (*vacrel).verbose { INFO } else { DEBUG2 },
                 errmsg!("table \"{}\": truncated {} to {} pages",
                         std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy(),
                         orig_rel_pages,
                         new_rel_pages));
        orig_rel_pages = new_rel_pages;

        if !(new_rel_pages > (*vacrel).nonempty_pages && lock_waiter_detected) {
            break;
        }
    }
}

/*
 * Rescan end pages to verify that they are (still) empty of tuples.
 *
 * Returns number of nondeletable pages (last nonempty page + 1).
 */
unsafe fn count_nondeletable_pages(
    vacrel: *mut LVRelState,
    lock_waiter_detected: *mut bool,
) -> BlockNumber {
    let mut blkno: BlockNumber;
    let mut prefetchedUntil: BlockNumber;
    let mut starttime: instr_time = core::mem::zeroed();

    /* Initialize the starttime if we check for conflicting lock requests */
    INSTR_TIME_SET_CURRENT!(starttime);

    /*
     * Start checking blocks at what we believe relation end to be and move
     * backwards.  To make the scan faster, we prefetch a few blocks at a time
     * in forward direction, so that OS-level readahead can kick in.
     */
    blkno = (*vacrel).rel_pages;
    StaticAssertStmt!((PREFETCH_SIZE & (PREFETCH_SIZE - 1)) == 0,
                      "prefetch size must be power of 2");
    prefetchedUntil = InvalidBlockNumber;
    while blkno > (*vacrel).nonempty_pages {
        let buf: Buffer;
        let page: Page;
        let mut offnum: OffsetNumber;
        let maxoff: OffsetNumber;
        let mut hastup: bool;

        /*
         * Check if another process requests a lock on our relation.
         */
        if (blkno % 32) == 0 {
            let mut currenttime: instr_time = core::mem::zeroed();
            let mut elapsed: instr_time = core::mem::zeroed();

            INSTR_TIME_SET_CURRENT!(currenttime);
            elapsed = currenttime;
            INSTR_TIME_SUBTRACT!(elapsed, starttime);
            if (INSTR_TIME_GET_MICROSEC!(elapsed) / 1000) >= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL as i64 {
                if LockHasWaitersRelation((*vacrel).rel, AccessExclusiveLock) {
                    ereport!(if (*vacrel).verbose { INFO } else { DEBUG2 },
                             errmsg!("table \"{}\": suspending truncate due to conflicting lock request",
                                     std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy()));

                    *lock_waiter_detected = true;
                    return blkno;
                }
                starttime = currenttime;
            }
        }

        /*
         * We don't insert a vacuum delay point here, because we have an
         * exclusive lock on the table.
         */
        CHECK_FOR_INTERRUPTS!();

        blkno -= 1;

        /* If we haven't prefetched this lot yet, do so now. */
        if prefetchedUntil > blkno {
            let prefetchStart: BlockNumber;
            let mut pblkno: BlockNumber;

            prefetchStart = blkno & !(PREFETCH_SIZE - 1);
            pblkno = prefetchStart;
            while pblkno <= blkno {
                PrefetchBuffer((*vacrel).rel, MAIN_FORKNUM, pblkno);
                CHECK_FOR_INTERRUPTS!();
                pblkno += 1;
            }
            prefetchedUntil = prefetchStart;
        }

        buf = ReadBufferExtended((*vacrel).rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
                                 (*vacrel).bstrategy);

        /* In this phase we only need shared access to the buffer */
        LockBuffer(buf, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buf);

        if PageIsNew(page) || PageIsEmpty(page) {
            UnlockReleaseBuffer(buf);
            continue;
        }

        hastup = false;
        maxoff = PageGetMaxOffsetNumber(page);
        offnum = FirstOffsetNumber;
        while offnum <= maxoff {
            let itemid: ItemId;

            itemid = PageGetItemId(page, offnum);

            /*
             * Note: any non-unused item should be taken as a reason to keep
             * this page.  Even an LP_DEAD item makes truncation unsafe.
             */
            if ItemIdIsUsed(itemid) {
                hastup = true;
                break; /* can stop scanning */
            }

            offnum = OffsetNumberNext(offnum);
        } /* scan along page */

        UnlockReleaseBuffer(buf);

        /* Done scanning if we found a tuple here */
        if hastup {
            return blkno + 1;
        }
    }

    /*
     * If we fall out of the loop, all the previously-thought-to-be-empty
     * pages still are.
     */
    (*vacrel).nonempty_pages
}

/*
 * Allocate dead_items and dead_items_info (either using palloc, or in dynamic
 * shared memory). Sets both in vacrel for caller.
 */
unsafe fn dead_items_alloc(vacrel: *mut LVRelState, nworkers: c_int) {
    let dead_items_info: *mut VacDeadItemsInfo;
    let vac_work_mem: c_int = if AmAutoVacuumWorkerProcess() && autovacuum_work_mem != -1 {
        autovacuum_work_mem
    } else {
        maintenance_work_mem
    };

    /*
     * Initialize state for a parallel vacuum.  As of now, only one worker can
     * be used for an index, so we invoke parallelism only if there are at
     * least two indexes on a table.
     */
    if nworkers >= 0 && (*vacrel).nindexes > 1 && (*vacrel).do_index_vacuuming {
        /*
         * Since parallel workers cannot access data in temporary tables, we
         * can't perform parallel vacuum on them.
         */
        if RelationUsesLocalBuffers((*vacrel).rel) {
            /*
             * Give warning only if the user explicitly tries to perform a
             * parallel vacuum on the temporary table.
             */
            if nworkers > 0 {
                ereport!(WARNING,
                         errmsg!("disabling parallel option of vacuum on \"{}\" --- cannot vacuum temporary tables in parallel",
                                 std::ffi::CStr::from_ptr((*vacrel).relname).to_string_lossy()));
            }
        } else {
            (*vacrel).pvs = parallel_vacuum_init((*vacrel).rel, (*vacrel).indrels,
                                                  (*vacrel).nindexes, nworkers,
                                                  vac_work_mem,
                                                  if (*vacrel).verbose { INFO } else { DEBUG2 },
                                                  (*vacrel).bstrategy);
        }

        /*
         * If parallel mode started, dead_items and dead_items_info spaces are
         * allocated in DSM.
         */
        if ParallelVacuumIsActive!(vacrel) {
            (*vacrel).dead_items = parallel_vacuum_get_dead_items((*vacrel).pvs,
                                                                   &mut (*vacrel).dead_items_info);
            return;
        }
    }

    /*
     * Serial VACUUM case. Allocate both dead_items and dead_items_info
     * locally.
     */

    let dead_items_info: *mut VacDeadItemsInfo =
        palloc(core::mem::size_of::<VacDeadItemsInfo>()) as *mut VacDeadItemsInfo;
    (*dead_items_info).max_bytes = vac_work_mem as Size * 1024;
    (*dead_items_info).num_items = 0;
    (*vacrel).dead_items_info = dead_items_info;

    (*vacrel).dead_items = TidStoreCreateLocal((*dead_items_info).max_bytes, true);
}

/*
 * Add the given block number and offset numbers to dead_items.
 */
unsafe fn dead_items_add(
    vacrel: *mut LVRelState,
    blkno: BlockNumber,
    offsets: *mut OffsetNumber,
    num_offsets: c_int,
) {
    let prog_index: [c_int; 2] = [
        PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS,
        PROGRESS_VACUUM_DEAD_TUPLE_BYTES,
    ];
    let mut prog_val: [int64; 2] = [0; 2];

    TidStoreSetBlockOffsets((*vacrel).dead_items, blkno, offsets, num_offsets);
    (*(*vacrel).dead_items_info).num_items += num_offsets as i64;

    /* update the progress information */
    prog_val[0] = (*(*vacrel).dead_items_info).num_items;
    prog_val[1] = TidStoreMemoryUsage((*vacrel).dead_items) as int64;
    pgstat_progress_update_multi_param(2, prog_index.as_ptr(), prog_val.as_ptr());
}

/*
 * Forget all collected dead items.
 */
unsafe fn dead_items_reset(vacrel: *mut LVRelState) {
    if ParallelVacuumIsActive!(vacrel) {
        parallel_vacuum_reset_dead_items((*vacrel).pvs);
        (*vacrel).dead_items = parallel_vacuum_get_dead_items((*vacrel).pvs,
                                                              &mut (*vacrel).dead_items_info);
        return;
    }

    /* Recreate the tidstore with the same max_bytes limitation */
    TidStoreDestroy((*vacrel).dead_items);
    (*vacrel).dead_items = TidStoreCreateLocal((*(*vacrel).dead_items_info).max_bytes, true);

    /* Reset the counter */
    (*(*vacrel).dead_items_info).num_items = 0;
}

/*
 * Perform cleanup for resources allocated in dead_items_alloc
 */
unsafe fn dead_items_cleanup(vacrel: *mut LVRelState) {
    if !ParallelVacuumIsActive!(vacrel) {
        /* Don't bother with pfree here */
        return;
    }

    /* End parallel mode */
    parallel_vacuum_end((*vacrel).pvs, (*vacrel).indstats);
    (*vacrel).pvs = std::ptr::null_mut();
}

/*
 * Check if every tuple in the given page is visible to all current and future
 * transactions. Also return the visibility_cutoff_xid which is the highest
 * xmin amongst the visible tuples.  Set *all_frozen to true if every tuple
 * on this page is frozen.
 *
 * This is a stripped down version of lazy_scan_prune().
 */
unsafe fn heap_page_is_all_visible(
    vacrel: *mut LVRelState,
    buf: Buffer,
    visibility_cutoff_xid: *mut TransactionId,
    all_frozen: *mut bool,
) -> bool {
    let page: Page = BufferGetPage(buf);
    let blockno: BlockNumber = BufferGetBlockNumber(buf);
    let mut offnum: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut all_visible: bool = true;

    *visibility_cutoff_xid = InvalidTransactionId;
    *all_frozen = true;

    maxoff = PageGetMaxOffsetNumber(page);
    offnum = FirstOffsetNumber;
    while offnum <= maxoff && all_visible {
        let itemid: ItemId;
        let mut tuple: HeapTupleData = core::mem::zeroed();

        /*
         * Set the offset number so that we can display it along with any
         * error that occurred while processing this tuple.
         */
        (*vacrel).offnum = offnum;
        itemid = PageGetItemId(page, offnum);

        /* Unused or redirect line pointers are of no interest */
        if !ItemIdIsUsed(itemid) || ItemIdIsRedirected(itemid) {
            offnum = OffsetNumberNext(offnum);
            continue;
        }

        ItemPointerSet(&mut tuple.t_self, blockno, offnum);

        /*
         * Dead line pointers can have index pointers pointing to them. So
         * they can't be treated as visible
         */
        if ItemIdIsDead(itemid) {
            all_visible = false;
            *all_frozen = false;
            break;
        }

        Assert!(ItemIdIsNormal(itemid));

        tuple.t_data = PageGetItem(page, itemid) as HeapTupleHeader;
        tuple.t_len = ItemIdGetLength(itemid);
        tuple.t_tableOid = RelationGetRelid((*vacrel).rel);

        match HeapTupleSatisfiesVacuum(&tuple, (*vacrel).cutoffs.OldestXmin, buf) {
            HEAPTUPLE_LIVE => {
                let xmin: TransactionId;

                /* Check comments in lazy_scan_prune. */
                if !HeapTupleHeaderXminCommitted(tuple.t_data) {
                    all_visible = false;
                    *all_frozen = false;
                    break;
                }

                /*
                 * The inserter definitely committed. But is it old enough
                 * that everyone sees it as committed?
                 */
                xmin = HeapTupleHeaderGetXmin(tuple.t_data);
                if !TransactionIdPrecedes(xmin, (*vacrel).cutoffs.OldestXmin) {
                    all_visible = false;
                    *all_frozen = false;
                    break;
                }

                /* Track newest xmin on page. */
                if TransactionIdFollows(xmin, *visibility_cutoff_xid) &&
                    TransactionIdIsNormal(xmin)
                {
                    *visibility_cutoff_xid = xmin;
                }

                /* Check whether this tuple is already frozen or not */
                if all_visible && *all_frozen &&
                    heap_tuple_needs_eventual_freeze(tuple.t_data)
                {
                    *all_frozen = false;
                }
            }

            HEAPTUPLE_DEAD |
            HEAPTUPLE_RECENTLY_DEAD |
            HEAPTUPLE_INSERT_IN_PROGRESS |
            HEAPTUPLE_DELETE_IN_PROGRESS => {
                all_visible = false;
                *all_frozen = false;
            }
            _ => {
                ereport!(ERROR, errmsg!("unexpected HeapTupleSatisfiesVacuum result"));
            }
        }

        offnum = OffsetNumberNext(offnum);
    } /* scan along page */

    /* Clear the offset information once we have processed the given page. */
    (*vacrel).offnum = InvalidOffsetNumber;

    all_visible
}

/*
 * Update index statistics in pg_class if the statistics are accurate.
 */
unsafe fn update_relstats_all_indexes(vacrel: *mut LVRelState) {
    let indrels: *mut Relation = (*vacrel).indrels;
    let nindexes: c_int = (*vacrel).nindexes;
    let indstats: *mut *mut IndexBulkDeleteResult = (*vacrel).indstats;

    Assert!((*vacrel).do_index_cleanup);

    for idx in 0..nindexes {
        let indrel: Relation = *indrels.add(idx as usize);
        let istat: *mut IndexBulkDeleteResult = *indstats.add(idx as usize);

        if istat.is_null() || (*istat).estimated_count {
            continue;
        }

        /* Update index statistics */
        vac_update_relstats(indrel,
                            (*istat).num_pages,
                            (*istat).num_index_tuples,
                            0, 0,
                            false,
                            InvalidTransactionId,
                            InvalidMultiXactId,
                            std::ptr::null_mut(), std::ptr::null_mut(), false);
    }
}

/*
 * Error context callback for errors occurring during vacuum.
 */
unsafe fn vacuum_error_callback(arg: *mut c_void) {
    let errinfo: *mut LVRelState = arg as *mut LVRelState;

    match (*errinfo).phase {
        VACUUM_ERRCB_PHASE_SCAN_HEAP => {
            if BlockNumberIsValid((*errinfo).blkno) {
                if OffsetNumberIsValid((*errinfo).offnum) {
                    errcontext!("while scanning block {} offset {} of relation \"{}.{}\"",
                                (*errinfo).blkno, (*errinfo).offnum,
                                std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                                std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
                } else {
                    errcontext!("while scanning block {} of relation \"{}.{}\"",
                                (*errinfo).blkno,
                                std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                                std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
                }
            } else {
                errcontext!("while scanning relation \"{}.{}\"",
                            std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                            std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
            }
        }

        VACUUM_ERRCB_PHASE_VACUUM_HEAP => {
            if BlockNumberIsValid((*errinfo).blkno) {
                if OffsetNumberIsValid((*errinfo).offnum) {
                    errcontext!("while vacuuming block {} offset {} of relation \"{}.{}\"",
                                (*errinfo).blkno, (*errinfo).offnum,
                                std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                                std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
                } else {
                    errcontext!("while vacuuming block {} of relation \"{}.{}\"",
                                (*errinfo).blkno,
                                std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                                std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
                }
            } else {
                errcontext!("while vacuuming relation \"{}.{}\"",
                            std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                            std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
            }
        }

        VACUUM_ERRCB_PHASE_VACUUM_INDEX => {
            errcontext!("while vacuuming index \"{}\" of relation \"{}.{}\"",
                        std::ffi::CStr::from_ptr((*errinfo).indname).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
        }

        VACUUM_ERRCB_PHASE_INDEX_CLEANUP => {
            errcontext!("while cleaning up index \"{}\" of relation \"{}.{}\"",
                        std::ffi::CStr::from_ptr((*errinfo).indname).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy());
        }

        VACUUM_ERRCB_PHASE_TRUNCATE => {
            if BlockNumberIsValid((*errinfo).blkno) {
                errcontext!("while truncating relation \"{}.{}\" to {} blocks",
                            std::ffi::CStr::from_ptr((*errinfo).relnamespace).to_string_lossy(),
                            std::ffi::CStr::from_ptr((*errinfo).relname).to_string_lossy(),
                            (*errinfo).blkno);
            }
        }

        VACUUM_ERRCB_PHASE_UNKNOWN | _ => {
            return; /* do nothing; the errinfo may not be initialized */
        }
    }
}

/*
 * Updates the information required for vacuum error callback.  This also saves
 * the current information which can be later restored via restore_vacuum_error_info.
 */
unsafe fn update_vacuum_error_info(
    vacrel: *mut LVRelState,
    saved_vacrel: *mut LVSavedErrInfo,
    phase: c_int,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    if !saved_vacrel.is_null() {
        (*saved_vacrel).offnum = (*vacrel).offnum;
        (*saved_vacrel).blkno = (*vacrel).blkno;
        (*saved_vacrel).phase = (*vacrel).phase;
    }

    (*vacrel).blkno = blkno;
    (*vacrel).offnum = offnum;
    (*vacrel).phase = phase;
}

/*
 * Restores the vacuum information saved via a prior call to update_vacuum_error_info.
 */
unsafe fn restore_vacuum_error_info(
    vacrel: *mut LVRelState,
    saved_vacrel: *const LVSavedErrInfo,
) {
    (*vacrel).blkno = (*saved_vacrel).blkno;
    (*vacrel).offnum = (*saved_vacrel).offnum;
    (*vacrel).phase = (*saved_vacrel).phase;
}

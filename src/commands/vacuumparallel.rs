//! vacuumparallel.rs
//!   Support routines for parallel vacuum execution.
//! Translated 1:1 from postgres/src/backend/commands/vacuumparallel.c
//!
//! This file contains routines that are intended to support setting up, using,
//! and tearing down a ParallelVacuumState.
//!
//! In a parallel vacuum, we perform both index bulk deletion and index cleanup
//! with parallel worker processes.  Individual indexes are processed by one
//! vacuum process.  ParallelVacuumState contains shared information as well as
//! the memory space for storing dead items allocated in the DSA area.  We
//! launch parallel worker processes at the start of parallel index
//! bulk-deletion and index cleanup and once all indexes are processed, the
//! parallel worker processes exit.  Each time we process indexes in parallel,
//! the parallel context is re-initialized so that the same DSM can be used for
//! multiple passes of index bulk-deletion and index cleanup.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/vacuumparallel.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::storage::ipc::shmem::mul_size;
use crate::storage::block::BlockNumber;
type dsa_area = c_void;
unsafe fn cstr_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() { return std::borrow::Cow::Borrowed("(null)"); }
    std::ffi::CStr::from_ptr(s).to_string_lossy()
}
type dsa_handle = crate::c::uint32;
type dsa_pointer = crate::c::uint64;
macro_rules! errcontext { ($($a:tt)*) => {{ let _ = format!($($a)*); }}; }
unsafe fn RelationGetNumberOfBlocks(_rel: Relation) -> BlockNumber { unimplemented!() /* TODO(pg-port): utils/rel.h */ }
extern "C" { fn memcpy(d: *mut c_void, s: *const c_void, n: usize) -> *mut c_void; fn strlen(s: *const c_char) -> usize; }


use std::ffi::{c_char, c_int, c_void};

// Relation/relcache helpers (utils/rel.h).
use crate::utils::rel::{
    Relation, RelationGetRelid, RelationGetRelationName, RelationGetNamespace,
};

// Index access method statistics structs (access/genam.h).
use crate::access::gin::ginvacuum::{IndexBulkDeleteResult, IndexVacuumInfo};

// Shared dead-item store (access/tidstore.h).
use crate::access::common::tidstore::{
    TidStore, TidStoreCreateShared, TidStoreDestroy, TidStoreAttach, TidStoreDetach,
    TidStoreGetHandle, TidStoreGetDSA,
};

// Buffer access strategy (storage/bufmgr.h).
use crate::storage::buf::BufferAccessStrategy;

// ===================== local stub types & symbols =====================
//
// The DSM / parallel-context layer (access/parallel.h, storage/shm_toc.h,
// storage/dsm.h, utils/dsa.h) is not yet ported; stub the pieces this file
// touches.  TODO(pg-port): real symbols live in src/backend/access/transam/
// parallel.c, src/backend/storage/ipc/shm_toc.c, src/backend/storage/ipc/dsm.c,
// and src/backend/utils/mmgr/dsa.c.

/* TODO(pg-port): real shm_toc lives in storage/ipc/shm_toc.c */
#[repr(C)]
pub struct shm_toc {
    _private: [u8; 0],
}

/* TODO(pg-port): real shm_toc_estimator lives in storage/ipc/shm_toc.h */
#[repr(C)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

/* TODO(pg-port): real dsm_segment lives in storage/ipc/dsm.c */
#[repr(C)]
pub struct dsm_segment {
    _private: [u8; 0],
}

/* TODO(pg-port): real ParallelContext lives in access/transam/parallel.c */
#[repr(C)]
pub struct ParallelContext {
    pub estimator: shm_toc_estimator,
    pub toc: *mut shm_toc,
    pub nworkers: c_int,
    pub nworkers_launched: c_int,
}

/* TODO(pg-port): real pg_atomic_uint32 lives in port/atomics.h */
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: u32,
}

/* TODO(pg-port): real BufferUsage lives in executor/instrument.h */
#[repr(C)]
pub struct BufferUsage {
    _private: [u8; 0],
}

/* TODO(pg-port): real WalUsage lives in executor/instrument.h */
#[repr(C)]
pub struct WalUsage {
    _private: [u8; 0],
}

/* TODO(pg-port): real ErrorContextCallback lives in utils/elog.h */
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub arg: *mut c_void,
}

/*
 * Statistics of shared dead items.
 * TODO(pg-port): real VacDeadItemsInfo lives in commands/vacuum.h
 */
#[repr(C)]
pub struct VacDeadItemsInfo {
    pub max_bytes: Size,
    pub num_items: i64,
}

// VACUUM_OPTION_* flags from access/genam.h.
// TODO(pg-port): these live in access/genam.h
pub const VACUUM_OPTION_NO_PARALLEL: u8 = 0;
pub const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
pub const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 1;
pub const VACUUM_OPTION_PARALLEL_CLEANUP: u8 = 1 << 2;
pub const VACUUM_OPTION_MAX_VALID_VALUE: u8 =
    VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP | VACUUM_OPTION_PARALLEL_CLEANUP;

// Lock modes (storage/lockdefs.h). TODO(pg-port): real values live there.
pub const ShareUpdateExclusiveLock: c_int = 4;
pub const RowExclusiveLock: c_int = 3;

// Buffer access strategy types (storage/bufmgr.h). TODO(pg-port): real in bufmgr.h
pub const BAS_VACUUM: c_int = 2;

// LWLock tranche id (storage/lwlock.h). TODO(pg-port): real in lwlock.h
pub const LWTRANCHE_PARALLEL_VACUUM_DSA: c_int = 0;

// Buffer block size (pg_config.h). TODO(pg-port): real BLCKSZ in pg_config.h
pub const BLCKSZ: c_int = 8192;

// pgstat activity states (utils/backend_status.h). TODO(pg-port)
pub const STATE_RUNNING: c_int = 1;

// proc status flags (storage/proc.h). TODO(pg-port)
pub const PROC_IN_VACUUM: u8 = 0x02;

// Progress parameters (commands/progress.h). TODO(pg-port)
pub const PROGRESS_VACUUM_INDEXES_PROCESSED: c_int = 9;
pub const PROGRESS_VACUUM_DELAY_TIME: c_int = 10;

// GUCs and globals (guc + vacuum + miscadmin). TODO(pg-port): real homes vary.
pub static mut IsUnderPostmaster: bool = false;
pub static mut max_parallel_maintenance_workers: c_int = 0;
pub static mut min_parallel_index_scan_size: c_int = 0;
pub static mut maintenance_work_mem: c_int = 0;
pub static mut debug_query_string: *const c_char = null();
pub static mut track_cost_delay_timing: bool = false;
pub static mut ParallelWorkerNumber: c_int = 0;
pub static mut error_context_stack: *mut ErrorContextCallback = null_mut();
pub static mut parallel_vacuum_worker_delay_ns: u64 = 0;

// Vacuum cost-based delay globals (commands/vacuum.c). TODO(pg-port)
pub static mut VacuumCostBalance: c_int = 0;
pub static mut VacuumCostBalanceLocal: c_int = 0;
pub static mut VacuumSharedCostBalance: *mut pg_atomic_uint32 = null_mut();
pub static mut VacuumActiveNWorkers: *mut pg_atomic_uint32 = null_mut();

// ----- stub helper functions (no home yet) -----

// commands/vacuum.c helpers. TODO(pg-port): real in commands/vacuum.c
unsafe fn vac_bulkdel_one_index(
    ivinfo: *mut IndexVacuumInfo,
    istat: *mut IndexBulkDeleteResult,
    dead_items: *mut TidStore,
    dead_items_info: *mut VacDeadItemsInfo,
) -> *mut IndexBulkDeleteResult {
    null_mut()
}
unsafe fn vac_cleanup_one_index(
    ivinfo: *mut IndexVacuumInfo,
    istat: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    null_mut()
}
unsafe fn vac_open_indexes(
    relation: Relation,
    lockmode: c_int,
    nindexes: *mut c_int,
    Irel: *mut *mut Relation,
) {
}
unsafe fn vac_close_indexes(nindexes: c_int, Irel: *mut Relation, lockmode: c_int) {}

// access/transam/parallel.c. TODO(pg-port)
unsafe fn EnterParallelMode() {}
unsafe fn ExitParallelMode() {}
unsafe fn IsParallelWorker() -> bool {
    false
}
unsafe fn CreateParallelContext(
    library_name: *const c_char,
    function_name: *const c_char,
    nworkers: c_int,
) -> *mut ParallelContext {
    null_mut()
}
unsafe fn InitializeParallelDSM(pcxt: *mut ParallelContext) {}
unsafe fn ReinitializeParallelDSM(pcxt: *mut ParallelContext) {}
unsafe fn ReinitializeParallelWorkers(pcxt: *mut ParallelContext, nworkers: c_int) {}
unsafe fn LaunchParallelWorkers(pcxt: *mut ParallelContext) {}
unsafe fn WaitForParallelWorkersToFinish(pcxt: *mut ParallelContext) {}
unsafe fn DestroyParallelContext(pcxt: *mut ParallelContext) {}

// storage/ipc/shm_toc.c. TODO(pg-port)
unsafe fn shm_toc_estimate_chunk(e: *mut shm_toc_estimator, sz: Size) {}
unsafe fn shm_toc_estimate_keys(e: *mut shm_toc_estimator, cnt: Size) {}
unsafe fn shm_toc_allocate(toc: *mut shm_toc, nbytes: Size) -> *mut c_void {
    null_mut()
}
unsafe fn shm_toc_insert(toc: *mut shm_toc, key: u64, address: *mut c_void) {}
unsafe fn shm_toc_lookup(toc: *mut shm_toc, key: u64, noError: bool) -> *mut c_void {
    null_mut()
}

// utils/dsa.c. TODO(pg-port)
unsafe fn dsa_get_handle(area: *mut dsa_area) -> dsa_handle {
    0
}

// port/atomics.h. TODO(pg-port)
unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32) {}
unsafe fn pg_atomic_write_u32(ptr: *mut pg_atomic_uint32, val: u32) {}
unsafe fn pg_atomic_read_u32(ptr: *mut pg_atomic_uint32) -> u32 {
    0
}
unsafe fn pg_atomic_fetch_add_u32(ptr: *mut pg_atomic_uint32, add_: u32) -> u32 {
    0
}
unsafe fn pg_atomic_add_fetch_u32(ptr: *mut pg_atomic_uint32, add_: u32) -> u32 {
    0
}
unsafe fn pg_atomic_sub_fetch_u32(ptr: *mut pg_atomic_uint32, sub_: u32) -> u32 {
    0
}

// executor/instrument.c. TODO(pg-port)
unsafe fn InstrStartParallelQuery() {}
unsafe fn InstrEndParallelQuery(bufusage: *mut BufferUsage, walusage: *mut WalUsage) {}
unsafe fn InstrAccumParallelQuery(bufusage: *mut BufferUsage, walusage: *mut WalUsage) {}

// utils/activity/pgstat*.c. TODO(pg-port)
unsafe fn pgstat_get_my_query_id() -> i64 {
    0
}
unsafe fn pgstat_report_activity(state: c_int, cmd_str: *const c_char) {}
unsafe fn pgstat_report_query_id(query_id: i64, force: bool) {}
unsafe fn pgstat_progress_parallel_incr_param(index: c_int, incr: i64) {}

// access/table/table.c. TODO(pg-port)
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    null_mut()
}
unsafe fn table_close(relation: Relation, lockmode: c_int) {}

// utils/cache/lsyscache.c. TODO(pg-port)
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char {
    null_mut()
}

// storage/buffer/freelist.c. TODO(pg-port)
unsafe fn GetAccessStrategyBufferCount(strategy: BufferAccessStrategy) -> c_int {
    0
}
unsafe fn GetAccessStrategyWithSize(btype: c_int, ring_size_kb: c_int) -> BufferAccessStrategy {
    null_mut()
}
unsafe fn FreeAccessStrategy(strategy: BufferAccessStrategy) {}

// commands/vacuum.c. TODO(pg-port)
unsafe fn VacuumUpdateCosts() {}

// miscadmin.h MyProc. TODO(pg-port): real PGPROC lives in storage/proc.h
#[repr(C)]
pub struct PGPROC {
    pub statusFlags: u8,
}
extern "C" { pub static mut MyProc: *mut PGPROC; }
/*
 * DSM keys for parallel vacuum.  Unlike other parallel execution code, since
 * we don't need to worry about DSM keys conflicting with plan_node_id we can
 * use small integers.
 */
const PARALLEL_VACUUM_KEY_SHARED: u64 = 1;
const PARALLEL_VACUUM_KEY_QUERY_TEXT: u64 = 2;
const PARALLEL_VACUUM_KEY_BUFFER_USAGE: u64 = 3;
const PARALLEL_VACUUM_KEY_WAL_USAGE: u64 = 4;
const PARALLEL_VACUUM_KEY_INDEX_STATS: u64 = 5;

/*
 * Shared information among parallel workers.  So this is allocated in the DSM
 * segment.
 */
#[repr(C)]
pub struct PVShared {
    /*
     * Target table relid, log level (for messages about parallel workers
     * launched during VACUUM VERBOSE) and query ID.  These fields are not
     * modified during the parallel vacuum.
     */
    pub relid: Oid,
    pub elevel: c_int,
    pub queryid: i64,

    /*
     * Fields for both index vacuum and cleanup.
     *
     * reltuples is the total number of input heap tuples.  We set either old
     * live tuples in the index vacuum case or the new live tuples in the
     * index cleanup case.
     *
     * estimated_count is true if reltuples is an estimated value.  (Note that
     * reltuples could be -1 in this case, indicating we have no idea.)
     */
    pub reltuples: f64,
    pub estimated_count: bool,

    /*
     * In single process vacuum we could consume more memory during index
     * vacuuming or cleanup apart from the memory for heap scanning.  In
     * parallel vacuum, since individual vacuum workers can consume memory
     * equal to maintenance_work_mem, the new maintenance_work_mem for each
     * worker is set such that the parallel operation doesn't consume more
     * memory than single process vacuum.
     */
    pub maintenance_work_mem_worker: c_int,

    /*
     * The number of buffers each worker's Buffer Access Strategy ring should
     * contain.
     */
    pub ring_nbuffers: c_int,

    /*
     * Shared vacuum cost balance.  During parallel vacuum,
     * VacuumSharedCostBalance points to this value and it accumulates the
     * balance of each parallel vacuum worker.
     */
    pub cost_balance: pg_atomic_uint32,

    /*
     * Number of active parallel workers.  This is used for computing the
     * minimum threshold of the vacuum cost balance before a worker sleeps for
     * cost-based delay.
     */
    pub active_nworkers: pg_atomic_uint32,

    /* Counter for vacuuming and cleanup */
    pub idx: pg_atomic_uint32,

    /* DSA handle where the TidStore lives */
    pub dead_items_dsa_handle: dsa_handle,

    /* DSA pointer to the shared TidStore */
    pub dead_items_handle: dsa_pointer,

    /* Statistics of shared dead items */
    pub dead_items_info: VacDeadItemsInfo,
}

/* Status used during parallel index vacuum or cleanup */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PVIndVacStatus {
    PARALLEL_INDVAC_STATUS_INITIAL = 0,
    PARALLEL_INDVAC_STATUS_NEED_BULKDELETE,
    PARALLEL_INDVAC_STATUS_NEED_CLEANUP,
    PARALLEL_INDVAC_STATUS_COMPLETED,
}
use PVIndVacStatus::*;

/*
 * Struct for index vacuum statistics of an index that is used for parallel vacuum.
 * This includes the status of parallel index vacuum as well as index statistics.
 */
#[repr(C)]
pub struct PVIndStats {
    /*
     * The following two fields are set by leader process before executing
     * parallel index vacuum or parallel index cleanup.  These fields are not
     * fixed for the entire VACUUM operation.  They are only fixed for an
     * individual parallel index vacuum and cleanup.
     *
     * parallel_workers_can_process is true if both leader and worker can
     * process the index, otherwise only leader can process it.
     */
    pub status: PVIndVacStatus,
    pub parallel_workers_can_process: bool,

    /*
     * Individual worker or leader stores the result of index vacuum or
     * cleanup.
     */
    pub istat_updated: bool, /* are the stats updated? */
    pub istat: IndexBulkDeleteResult,
}

/*
 * Struct for maintaining a parallel vacuum state. typedef appears in vacuum.h.
 */
#[repr(C)]
pub struct ParallelVacuumState {
    /* NULL for worker processes */
    pub pcxt: *mut ParallelContext,

    /* Parent Heap Relation */
    pub heaprel: Relation,

    /* Target indexes */
    pub indrels: *mut Relation,
    pub nindexes: c_int,

    /* Shared information among parallel vacuum workers */
    pub shared: *mut PVShared,

    /*
     * Shared index statistics among parallel vacuum workers. The array
     * element is allocated for every index, even those indexes where parallel
     * index vacuuming is unsafe or not worthwhile (e.g.,
     * will_parallel_vacuum[] is false).  During parallel vacuum,
     * IndexBulkDeleteResult of each index is kept in DSM and is copied into
     * local memory at the end of parallel vacuum.
     */
    pub indstats: *mut PVIndStats,

    /* Shared dead items space among parallel vacuum workers */
    pub dead_items: *mut TidStore,

    /* Points to buffer usage area in DSM */
    pub buffer_usage: *mut BufferUsage,

    /* Points to WAL usage area in DSM */
    pub wal_usage: *mut WalUsage,

    /*
     * False if the index is totally unsuitable target for all parallel
     * processing. For example, the index could be <
     * min_parallel_index_scan_size cutoff.
     */
    pub will_parallel_vacuum: *mut bool,

    /*
     * The number of indexes that support parallel index bulk-deletion and
     * parallel index cleanup respectively.
     */
    pub nindexes_parallel_bulkdel: c_int,
    pub nindexes_parallel_cleanup: c_int,
    pub nindexes_parallel_condcleanup: c_int,

    /* Buffer access strategy used by leader process */
    pub bstrategy: BufferAccessStrategy,

    /*
     * Error reporting state.  The error callback is set only for workers
     * processes during parallel index vacuum.
     */
    pub relnamespace: *mut c_char,
    pub relname: *mut c_char,
    pub indname: *mut c_char,
    pub status: PVIndVacStatus,
}

/*
 * Try to enter parallel mode and create a parallel context.  Then initialize
 * shared memory state.
 *
 * On success, return parallel vacuum state.  Otherwise return NULL.
 */
pub unsafe fn parallel_vacuum_init(
    rel: Relation,
    indrels: *mut Relation,
    nindexes: c_int,
    nrequested_workers: c_int,
    vac_work_mem: c_int,
    elevel: c_int,
    bstrategy: BufferAccessStrategy,
) -> *mut ParallelVacuumState {
    let pvs: *mut ParallelVacuumState;
    let pcxt: *mut ParallelContext;
    let shared: *mut PVShared;
    let dead_items: *mut TidStore;
    let indstats: *mut PVIndStats;
    let buffer_usage: *mut BufferUsage;
    let wal_usage: *mut WalUsage;
    let will_parallel_vacuum: *mut bool;
    let est_indstats_len: Size;
    let est_shared_len: Size;
    let mut nindexes_mwm: c_int = 0;
    let mut parallel_workers: c_int = 0;
    let querylen: c_int;

    /*
     * A parallel vacuum must be requested and there must be indexes on the
     * relation
     */
    Assert!(nrequested_workers >= 0);
    Assert!(nindexes > 0);

    /*
     * Compute the number of parallel vacuum workers to launch
     */
    will_parallel_vacuum =
        palloc0(std::mem::size_of::<bool>() * nindexes as usize) as *mut bool;
    parallel_workers = parallel_vacuum_compute_workers(
        indrels,
        nindexes,
        nrequested_workers,
        will_parallel_vacuum,
    );
    if parallel_workers <= 0 {
        /* Can't perform vacuum in parallel -- return NULL */
        pfree(will_parallel_vacuum as *mut c_void);
        return null_mut();
    }

    pvs = palloc0(std::mem::size_of::<ParallelVacuumState>()) as *mut ParallelVacuumState;
    (*pvs).indrels = indrels;
    (*pvs).nindexes = nindexes;
    (*pvs).will_parallel_vacuum = will_parallel_vacuum;
    (*pvs).bstrategy = bstrategy;
    (*pvs).heaprel = rel;

    EnterParallelMode();
    pcxt = CreateParallelContext(
        b"postgres\0".as_ptr() as *const _,
        b"parallel_vacuum_main\0".as_ptr() as *const _,
        parallel_workers,
    );
    Assert!((*pcxt).nworkers > 0);
    (*pvs).pcxt = pcxt;

    /* Estimate size for index vacuum stats -- PARALLEL_VACUUM_KEY_INDEX_STATS */
    est_indstats_len = mul_size(std::mem::size_of::<PVIndStats>(), nindexes as usize);
    shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, est_indstats_len);
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);

    /* Estimate size for shared information -- PARALLEL_VACUUM_KEY_SHARED */
    est_shared_len = std::mem::size_of::<PVShared>();
    shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, est_shared_len);
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);

    /*
     * Estimate space for BufferUsage and WalUsage --
     * PARALLEL_VACUUM_KEY_BUFFER_USAGE and PARALLEL_VACUUM_KEY_WAL_USAGE.
     *
     * If there are no extensions loaded that care, we could skip this.  We
     * have no way of knowing whether anyone's looking at pgBufferUsage or
     * pgWalUsage, so do it unconditionally.
     */
    shm_toc_estimate_chunk(
        &raw mut (*pcxt).estimator,
        mul_size(std::mem::size_of::<BufferUsage>(), (*pcxt).nworkers as usize),
    );
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);
    shm_toc_estimate_chunk(
        &raw mut (*pcxt).estimator,
        mul_size(std::mem::size_of::<WalUsage>(), (*pcxt).nworkers as usize),
    );
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);

    /* Finally, estimate PARALLEL_VACUUM_KEY_QUERY_TEXT space */
    if !debug_query_string.is_null() {
        querylen = strlen(debug_query_string) as c_int;
        shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, (querylen + 1) as Size);
        shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);
    } else {
        querylen = 0; /* keep compiler quiet */
    }

    InitializeParallelDSM(pcxt);

    /* Prepare index vacuum stats */
    indstats = shm_toc_allocate((*pcxt).toc, est_indstats_len) as *mut PVIndStats;
    MemSet(indstats as *mut c_void, 0, est_indstats_len);
    for i in 0..nindexes {
        let indrel: Relation = *indrels.add(i as usize);
        let vacoptions: u8 = (*(*indrel).rd_indam).amparallelvacuumoptions;

        /*
         * Cleanup option should be either disabled, always performing in
         * parallel or conditionally performing in parallel.
         */
        Assert!(
            ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0)
                || ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0)
        );
        Assert!(vacoptions <= VACUUM_OPTION_MAX_VALID_VALUE);

        if !*will_parallel_vacuum.add(i as usize) {
            continue;
        }

        if (*(*indrel).rd_indam).amusemaintenanceworkmem {
            nindexes_mwm += 1;
        }

        /*
         * Remember the number of indexes that support parallel operation for
         * each phase.
         */
        if (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0 {
            (*pvs).nindexes_parallel_bulkdel += 1;
        }
        if (vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) != 0 {
            (*pvs).nindexes_parallel_cleanup += 1;
        }
        if (vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0 {
            (*pvs).nindexes_parallel_condcleanup += 1;
        }
    }
    shm_toc_insert((*pcxt).toc, PARALLEL_VACUUM_KEY_INDEX_STATS, indstats as *mut c_void);
    (*pvs).indstats = indstats;

    /* Prepare shared information */
    shared = shm_toc_allocate((*pcxt).toc, est_shared_len) as *mut PVShared;
    MemSet(shared as *mut c_void, 0, est_shared_len);
    (*shared).relid = RelationGetRelid(rel);
    (*shared).elevel = elevel;
    (*shared).queryid = pgstat_get_my_query_id();
    (*shared).maintenance_work_mem_worker = if nindexes_mwm > 0 {
        maintenance_work_mem / Min(parallel_workers, nindexes_mwm)
    } else {
        maintenance_work_mem
    };
    (*shared).dead_items_info.max_bytes = vac_work_mem as Size * 1024 as Size;

    /* Prepare DSA space for dead items */
    dead_items = TidStoreCreateShared(
        (*shared).dead_items_info.max_bytes,
        LWTRANCHE_PARALLEL_VACUUM_DSA,
    );
    (*pvs).dead_items = dead_items;
    (*shared).dead_items_handle = TidStoreGetHandle(dead_items);
    (*shared).dead_items_dsa_handle = dsa_get_handle(TidStoreGetDSA(dead_items));

    /* Use the same buffer size for all workers */
    (*shared).ring_nbuffers = GetAccessStrategyBufferCount(bstrategy);

    pg_atomic_init_u32(&raw mut (*shared).cost_balance, 0);
    pg_atomic_init_u32(&raw mut (*shared).active_nworkers, 0);
    pg_atomic_init_u32(&raw mut (*shared).idx, 0);

    shm_toc_insert((*pcxt).toc, PARALLEL_VACUUM_KEY_SHARED, shared as *mut c_void);
    (*pvs).shared = shared;

    /*
     * Allocate space for each worker's BufferUsage and WalUsage; no need to
     * initialize
     */
    buffer_usage = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(std::mem::size_of::<BufferUsage>(), (*pcxt).nworkers as usize),
    ) as *mut BufferUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_VACUUM_KEY_BUFFER_USAGE, buffer_usage as *mut c_void);
    (*pvs).buffer_usage = buffer_usage;
    wal_usage = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(std::mem::size_of::<WalUsage>(), (*pcxt).nworkers as usize),
    ) as *mut WalUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_VACUUM_KEY_WAL_USAGE, wal_usage as *mut c_void);
    (*pvs).wal_usage = wal_usage;

    /* Store query string for workers */
    if !debug_query_string.is_null() {
        let sharedquery: *mut c_char;

        sharedquery = shm_toc_allocate((*pcxt).toc, (querylen + 1) as Size) as *mut c_char;
        memcpy(
            sharedquery as *mut c_void,
            debug_query_string as *const c_void,
            (querylen + 1) as Size,
        );
        *sharedquery.add(querylen as usize) = b'\0' as c_char;
        shm_toc_insert((*pcxt).toc, PARALLEL_VACUUM_KEY_QUERY_TEXT, sharedquery as *mut c_void);
    }

    /* Success -- return parallel vacuum state */
    pvs
}

/*
 * Destroy the parallel context, and end parallel mode.
 *
 * Since writes are not allowed during parallel mode, copy the
 * updated index statistics from DSM into local memory and then later use that
 * to update the index statistics.  One might think that we can exit from
 * parallel mode, update the index statistics and then destroy parallel
 * context, but that won't be safe (see ExitParallelMode).
 */
pub unsafe fn parallel_vacuum_end(
    pvs: *mut ParallelVacuumState,
    istats: *mut *mut IndexBulkDeleteResult,
) {
    Assert!(!IsParallelWorker());

    /* Copy the updated statistics */
    for i in 0..(*pvs).nindexes {
        let indstats: *mut PVIndStats = &raw mut *(*pvs).indstats.add(i as usize);

        if (*indstats).istat_updated {
            *istats.add(i as usize) =
                palloc0(std::mem::size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
            memcpy(
                *istats.add(i as usize) as *mut c_void,
                &raw const (*indstats).istat as *const c_void,
                std::mem::size_of::<IndexBulkDeleteResult>(),
            );
        } else {
            *istats.add(i as usize) = null_mut();
        }
    }

    TidStoreDestroy((*pvs).dead_items);

    DestroyParallelContext((*pvs).pcxt);
    ExitParallelMode();

    pfree((*pvs).will_parallel_vacuum as *mut c_void);
    pfree(pvs as *mut c_void);
}

/*
 * Returns the dead items space and dead items information.
 */
pub unsafe fn parallel_vacuum_get_dead_items(
    pvs: *mut ParallelVacuumState,
    dead_items_info_p: *mut *mut VacDeadItemsInfo,
) -> *mut TidStore {
    *dead_items_info_p = &raw mut (*(*pvs).shared).dead_items_info;
    (*pvs).dead_items
}

/* Forget all items in dead_items */
pub unsafe fn parallel_vacuum_reset_dead_items(pvs: *mut ParallelVacuumState) {
    let dead_items_info: *mut VacDeadItemsInfo = &raw mut (*(*pvs).shared).dead_items_info;

    /*
     * Free the current tidstore and return allocated DSA segments to the
     * operating system. Then we recreate the tidstore with the same max_bytes
     * limitation we just used.
     */
    TidStoreDestroy((*pvs).dead_items);
    (*pvs).dead_items =
        TidStoreCreateShared((*dead_items_info).max_bytes, LWTRANCHE_PARALLEL_VACUUM_DSA);

    /* Update the DSA pointer for dead_items to the new one */
    (*(*pvs).shared).dead_items_dsa_handle = dsa_get_handle(TidStoreGetDSA((*pvs).dead_items));
    (*(*pvs).shared).dead_items_handle = TidStoreGetHandle((*pvs).dead_items);

    /* Reset the counter */
    (*dead_items_info).num_items = 0;
}

/*
 * Do parallel index bulk-deletion with parallel workers.
 */
pub unsafe fn parallel_vacuum_bulkdel_all_indexes(
    pvs: *mut ParallelVacuumState,
    num_table_tuples: c_long,
    num_index_scans: c_int,
) {
    Assert!(!IsParallelWorker());

    /*
     * We can only provide an approximate value of num_heap_tuples, at least
     * for now.
     */
    (*(*pvs).shared).reltuples = num_table_tuples as f64;
    (*(*pvs).shared).estimated_count = true;

    parallel_vacuum_process_all_indexes(pvs, num_index_scans, true);
}

/*
 * Do parallel index cleanup with parallel workers.
 */
pub unsafe fn parallel_vacuum_cleanup_all_indexes(
    pvs: *mut ParallelVacuumState,
    num_table_tuples: c_long,
    num_index_scans: c_int,
    estimated_count: bool,
) {
    Assert!(!IsParallelWorker());

    /*
     * We can provide a better estimate of total number of surviving tuples
     * (we assume indexes are more interested in that than in the number of
     * nominally live tuples).
     */
    (*(*pvs).shared).reltuples = num_table_tuples as f64;
    (*(*pvs).shared).estimated_count = estimated_count;

    parallel_vacuum_process_all_indexes(pvs, num_index_scans, false);
}

/*
 * Compute the number of parallel worker processes to request.  Both index
 * vacuum and index cleanup can be executed with parallel workers.
 * The index is eligible for parallel vacuum iff its size is greater than
 * min_parallel_index_scan_size as invoking workers for very small indexes
 * can hurt performance.
 *
 * nrequested is the number of parallel workers that user requested.  If
 * nrequested is 0, we compute the parallel degree based on nindexes, that is
 * the number of indexes that support parallel vacuum.  This function also
 * sets will_parallel_vacuum to remember indexes that participate in parallel
 * vacuum.
 */
unsafe fn parallel_vacuum_compute_workers(
    indrels: *mut Relation,
    nindexes: c_int,
    nrequested: c_int,
    will_parallel_vacuum: *mut bool,
) -> c_int {
    let mut nindexes_parallel: c_int = 0;
    let mut nindexes_parallel_bulkdel: c_int = 0;
    let mut nindexes_parallel_cleanup: c_int = 0;
    let mut parallel_workers: c_int;

    /*
     * We don't allow performing parallel operation in standalone backend or
     * when parallelism is disabled.
     */
    if !IsUnderPostmaster || max_parallel_maintenance_workers == 0 {
        return 0;
    }

    /*
     * Compute the number of indexes that can participate in parallel vacuum.
     */
    for i in 0..nindexes {
        let indrel: Relation = *indrels.add(i as usize);
        let vacoptions: u8 = (*(*indrel).rd_indam).amparallelvacuumoptions;

        /* Skip index that is not a suitable target for parallel index vacuum */
        if vacoptions == VACUUM_OPTION_NO_PARALLEL
            || RelationGetNumberOfBlocks(indrel) < min_parallel_index_scan_size as BlockNumber
        {
            continue;
        }

        *will_parallel_vacuum.add(i as usize) = true;

        if (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0 {
            nindexes_parallel_bulkdel += 1;
        }
        if ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) != 0)
            || ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0)
        {
            nindexes_parallel_cleanup += 1;
        }
    }

    nindexes_parallel = Max(nindexes_parallel_bulkdel, nindexes_parallel_cleanup);

    /* The leader process takes one index */
    nindexes_parallel -= 1;

    /* No index supports parallel vacuum */
    if nindexes_parallel <= 0 {
        return 0;
    }

    /* Compute the parallel degree */
    parallel_workers = if nrequested > 0 {
        Min(nrequested, nindexes_parallel)
    } else {
        nindexes_parallel
    };

    /* Cap by max_parallel_maintenance_workers */
    parallel_workers = Min(parallel_workers, max_parallel_maintenance_workers);

    parallel_workers
}

/*
 * Perform index vacuum or index cleanup with parallel workers.  This function
 * must be used by the parallel vacuum leader process.
 */
unsafe fn parallel_vacuum_process_all_indexes(
    pvs: *mut ParallelVacuumState,
    num_index_scans: c_int,
    vacuum: bool,
) {
    let mut nworkers: c_int;
    let new_status: PVIndVacStatus;

    Assert!(!IsParallelWorker());

    if vacuum {
        new_status = PARALLEL_INDVAC_STATUS_NEED_BULKDELETE;

        /* Determine the number of parallel workers to launch */
        nworkers = (*pvs).nindexes_parallel_bulkdel;
    } else {
        new_status = PARALLEL_INDVAC_STATUS_NEED_CLEANUP;

        /* Determine the number of parallel workers to launch */
        nworkers = (*pvs).nindexes_parallel_cleanup;

        /* Add conditionally parallel-aware indexes if in the first time call */
        if num_index_scans == 0 {
            nworkers += (*pvs).nindexes_parallel_condcleanup;
        }
    }

    /* The leader process will participate */
    nworkers -= 1;

    /*
     * It is possible that parallel context is initialized with fewer workers
     * than the number of indexes that need a separate worker in the current
     * phase, so we need to consider it.  See
     * parallel_vacuum_compute_workers().
     */
    nworkers = Min(nworkers, (*(*pvs).pcxt).nworkers);

    /*
     * Set index vacuum status and mark whether parallel vacuum worker can
     * process it.
     */
    for i in 0..(*pvs).nindexes {
        let indstats: *mut PVIndStats = &raw mut *(*pvs).indstats.add(i as usize);

        Assert!((*indstats).status == PARALLEL_INDVAC_STATUS_INITIAL);
        (*indstats).status = new_status;
        (*indstats).parallel_workers_can_process = *(*pvs).will_parallel_vacuum.add(i as usize)
            && parallel_vacuum_index_is_parallel_safe(
                *(*pvs).indrels.add(i as usize),
                num_index_scans,
                vacuum,
            );
    }

    /* Reset the parallel index processing and progress counters */
    pg_atomic_write_u32(&raw mut (*(*pvs).shared).idx, 0);

    /* Setup the shared cost-based vacuum delay and launch workers */
    if nworkers > 0 {
        /* Reinitialize parallel context to relaunch parallel workers */
        if num_index_scans > 0 {
            ReinitializeParallelDSM((*pvs).pcxt);
        }

        /*
         * Set up shared cost balance and the number of active workers for
         * vacuum delay.  We need to do this before launching workers as
         * otherwise, they might not see the updated values for these
         * parameters.
         */
        pg_atomic_write_u32(&raw mut (*(*pvs).shared).cost_balance, VacuumCostBalance as u32);
        pg_atomic_write_u32(&raw mut (*(*pvs).shared).active_nworkers, 0);

        /*
         * The number of workers can vary between bulkdelete and cleanup
         * phase.
         */
        ReinitializeParallelWorkers((*pvs).pcxt, nworkers);

        LaunchParallelWorkers((*pvs).pcxt);

        if (*(*pvs).pcxt).nworkers_launched > 0 {
            /*
             * Reset the local cost values for leader backend as we have
             * already accumulated the remaining balance of heap.
             */
            VacuumCostBalance = 0;
            VacuumCostBalanceLocal = 0;

            /* Enable shared cost balance for leader backend */
            VacuumSharedCostBalance = &raw mut (*(*pvs).shared).cost_balance;
            VacuumActiveNWorkers = &raw mut (*(*pvs).shared).active_nworkers;
        }

        if vacuum {
            ereport!(
                (*(*pvs).shared).elevel,
                errmsg!(
                    "launched {} parallel vacuum workers for index vacuuming (planned: {})",
                    (*(*pvs).pcxt).nworkers_launched,
                    nworkers
                )
            );
        } else {
            ereport!(
                (*(*pvs).shared).elevel,
                errmsg!(
                    "launched {} parallel vacuum workers for index cleanup (planned: {})",
                    (*(*pvs).pcxt).nworkers_launched,
                    nworkers
                )
            );
        }
    }

    /* Vacuum the indexes that can be processed by only leader process */
    parallel_vacuum_process_unsafe_indexes(pvs);

    /*
     * Join as a parallel worker.  The leader vacuums alone processes all
     * parallel-safe indexes in the case where no workers are launched.
     */
    parallel_vacuum_process_safe_indexes(pvs);

    /*
     * Next, accumulate buffer and WAL usage.  (This must wait for the workers
     * to finish, or we might get incomplete data.)
     */
    if nworkers > 0 {
        /* Wait for all vacuum workers to finish */
        WaitForParallelWorkersToFinish((*pvs).pcxt);

        for i in 0..(*(*pvs).pcxt).nworkers_launched {
            InstrAccumParallelQuery(
                &raw mut *(*pvs).buffer_usage.add(i as usize),
                &raw mut *(*pvs).wal_usage.add(i as usize),
            );
        }
    }

    /*
     * Reset all index status back to initial (while checking that we have
     * vacuumed all indexes).
     */
    for i in 0..(*pvs).nindexes {
        let indstats: *mut PVIndStats = &raw mut *(*pvs).indstats.add(i as usize);

        if (*indstats).status != PARALLEL_INDVAC_STATUS_COMPLETED {
            elog!(
                ERROR,
                "parallel index vacuum on index \"{}\" is not completed",
                cstr_display(RelationGetRelationName(*(*pvs).indrels.add(i as usize)))
            );
        }

        (*indstats).status = PARALLEL_INDVAC_STATUS_INITIAL;
    }

    /*
     * Carry the shared balance value to heap scan and disable shared costing
     */
    if !VacuumSharedCostBalance.is_null() {
        VacuumCostBalance = pg_atomic_read_u32(VacuumSharedCostBalance) as c_int;
        VacuumSharedCostBalance = null_mut();
        VacuumActiveNWorkers = null_mut();
    }
}

/*
 * Index vacuum/cleanup routine used by the leader process and parallel
 * vacuum worker processes to vacuum the indexes in parallel.
 */
unsafe fn parallel_vacuum_process_safe_indexes(pvs: *mut ParallelVacuumState) {
    /*
     * Increment the active worker count if we are able to launch any worker.
     */
    if !VacuumActiveNWorkers.is_null() {
        pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);
    }

    /* Loop until all indexes are vacuumed */
    loop {
        let idx: c_int;
        let indstats: *mut PVIndStats;

        /* Get an index number to process */
        idx = pg_atomic_fetch_add_u32(&raw mut (*(*pvs).shared).idx, 1) as c_int;

        /* Done for all indexes? */
        if idx >= (*pvs).nindexes {
            break;
        }

        indstats = &raw mut *(*pvs).indstats.add(idx as usize);

        /*
         * Skip vacuuming index that is unsafe for workers or has an
         * unsuitable target for parallel index vacuum (this is vacuumed in
         * parallel_vacuum_process_unsafe_indexes() by the leader).
         */
        if !(*indstats).parallel_workers_can_process {
            continue;
        }

        /* Do vacuum or cleanup of the index */
        parallel_vacuum_process_one_index(pvs, *(*pvs).indrels.add(idx as usize), indstats);
    }

    /*
     * We have completed the index vacuum so decrement the active worker
     * count.
     */
    if !VacuumActiveNWorkers.is_null() {
        pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
    }
}

/*
 * Perform parallel vacuuming of indexes in leader process.
 *
 * Handles index vacuuming (or index cleanup) for indexes that are not
 * parallel safe.  It's possible that this will vary for a given index, based
 * on details like whether we're performing index cleanup right now.
 *
 * Also performs vacuuming of smaller indexes that fell under the size cutoff
 * enforced by parallel_vacuum_compute_workers().
 */
unsafe fn parallel_vacuum_process_unsafe_indexes(pvs: *mut ParallelVacuumState) {
    Assert!(!IsParallelWorker());

    /*
     * Increment the active worker count if we are able to launch any worker.
     */
    if !VacuumActiveNWorkers.is_null() {
        pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);
    }

    for i in 0..(*pvs).nindexes {
        let indstats: *mut PVIndStats = &raw mut *(*pvs).indstats.add(i as usize);

        /* Skip, indexes that are safe for workers */
        if (*indstats).parallel_workers_can_process {
            continue;
        }

        /* Do vacuum or cleanup of the index */
        parallel_vacuum_process_one_index(pvs, *(*pvs).indrels.add(i as usize), indstats);
    }

    /*
     * We have completed the index vacuum so decrement the active worker
     * count.
     */
    if !VacuumActiveNWorkers.is_null() {
        pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
    }
}

/*
 * Vacuum or cleanup index either by leader process or by one of the worker
 * process.  After vacuuming the index this function copies the index
 * statistics returned from ambulkdelete and amvacuumcleanup to the DSM
 * segment.
 */
unsafe fn parallel_vacuum_process_one_index(
    pvs: *mut ParallelVacuumState,
    indrel: Relation,
    indstats: *mut PVIndStats,
) {
    let mut istat: *mut IndexBulkDeleteResult = null_mut();
    let istat_res: *mut IndexBulkDeleteResult;
    let mut ivinfo: IndexVacuumInfo = std::mem::zeroed();

    /*
     * Update the pointer to the corresponding bulk-deletion result if someone
     * has already updated it
     */
    if (*indstats).istat_updated {
        istat = &raw mut (*indstats).istat;
    }

    ivinfo.index = indrel as *mut c_void;
    ivinfo.heaprel = (*pvs).heaprel as *mut c_void;
    ivinfo.analyze_only = false;
    ivinfo.report_progress = false;
    ivinfo.message_level = DEBUG2;
    ivinfo.estimated_count = (*(*pvs).shared).estimated_count;
    ivinfo.num_heap_tuples = (*(*pvs).shared).reltuples;
    ivinfo.strategy = (*pvs).bstrategy as *mut c_void;

    /* Update error traceback information */
    (*pvs).indname = pstrdup(RelationGetRelationName(indrel));
    (*pvs).status = (*indstats).status;

    match (*indstats).status {
        PARALLEL_INDVAC_STATUS_NEED_BULKDELETE => {
            istat_res = vac_bulkdel_one_index(
                &raw mut ivinfo,
                istat,
                (*pvs).dead_items,
                &raw mut (*(*pvs).shared).dead_items_info,
            );
        }
        PARALLEL_INDVAC_STATUS_NEED_CLEANUP => {
            istat_res = vac_cleanup_one_index(&raw mut ivinfo, istat);
        }
        _ => {
            elog!(
                ERROR,
                "unexpected parallel vacuum index status {} for index \"{}\"",
                (*indstats).status as c_int,
                cstr_display(RelationGetRelationName(indrel))
            );
            return;
        }
    }

    /*
     * Copy the index bulk-deletion result returned from ambulkdelete and
     * amvacuumcleanup to the DSM segment if it's the first cycle because they
     * allocate locally and it's possible that an index will be vacuumed by a
     * different vacuum process the next cycle.  Copying the result normally
     * happens only the first time an index is vacuumed.  For any additional
     * vacuum pass, we directly point to the result on the DSM segment and
     * pass it to vacuum index APIs so that workers can update it directly.
     *
     * Since all vacuum workers write the bulk-deletion result at different
     * slots we can write them without locking.
     */
    if !(*indstats).istat_updated && !istat_res.is_null() {
        memcpy(
            &raw mut (*indstats).istat as *mut c_void,
            istat_res as *const c_void,
            std::mem::size_of::<IndexBulkDeleteResult>(),
        );
        (*indstats).istat_updated = true;

        /* Free the locally-allocated bulk-deletion result */
        pfree(istat_res as *mut c_void);
    }

    /*
     * Update the status to completed. No need to lock here since each worker
     * touches different indexes.
     */
    (*indstats).status = PARALLEL_INDVAC_STATUS_COMPLETED;

    /* Reset error traceback information */
    (*pvs).status = PARALLEL_INDVAC_STATUS_COMPLETED;
    pfree((*pvs).indname as *mut c_void);
    (*pvs).indname = null_mut();

    /*
     * Call the parallel variant of pgstat_progress_incr_param so workers can
     * report progress of index vacuum to the leader.
     */
    pgstat_progress_parallel_incr_param(PROGRESS_VACUUM_INDEXES_PROCESSED, 1);
}

/*
 * Returns false, if the given index can't participate in the next execution of
 * parallel index vacuum or parallel index cleanup.
 */
unsafe fn parallel_vacuum_index_is_parallel_safe(
    indrel: Relation,
    num_index_scans: c_int,
    vacuum: bool,
) -> bool {
    let vacoptions: u8;

    vacoptions = (*(*indrel).rd_indam).amparallelvacuumoptions;

    /* In parallel vacuum case, check if it supports parallel bulk-deletion */
    if vacuum {
        return (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL) != 0;
    }

    /* Not safe, if the index does not support parallel cleanup */
    if ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) == 0)
        && ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) == 0)
    {
        return false;
    }

    /*
     * Not safe, if the index supports parallel cleanup conditionally, but we
     * have already processed the index (for bulkdelete).  We do this to avoid
     * the need to invoke workers when parallel index cleanup doesn't need to
     * scan the index.  See the comments for option
     * VACUUM_OPTION_PARALLEL_COND_CLEANUP to know when indexes support
     * parallel cleanup conditionally.
     */
    if num_index_scans > 0 && ((vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP) != 0) {
        return false;
    }

    true
}

/*
 * Perform work within a launched parallel process.
 *
 * Since parallel vacuum workers perform only index vacuum or index cleanup,
 * we don't need to report progress information.
 */
pub unsafe fn parallel_vacuum_main(seg: *mut dsm_segment, toc: *mut shm_toc) {
    let mut pvs: ParallelVacuumState = std::mem::zeroed();
    let rel: Relation;
    let mut indrels: *mut Relation = null_mut();
    let indstats: *mut PVIndStats;
    let shared: *mut PVShared;
    let dead_items: *mut TidStore;
    let buffer_usage: *mut BufferUsage;
    let wal_usage: *mut WalUsage;
    let mut nindexes: c_int = 0;
    let sharedquery: *mut c_char;
    let mut errcallback: ErrorContextCallback = std::mem::zeroed();

    /*
     * A parallel vacuum worker must have only PROC_IN_VACUUM flag since we
     * don't support parallel vacuum for autovacuum as of now.
     */
    Assert!((*MyProc).statusFlags == PROC_IN_VACUUM);

    elog!(DEBUG1, "starting parallel vacuum worker");

    shared = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_SHARED, false) as *mut PVShared;

    /* Set debug_query_string for individual workers */
    sharedquery = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_QUERY_TEXT, true) as *mut c_char;
    debug_query_string = sharedquery;
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    /* Track query ID */
    pgstat_report_query_id((*shared).queryid, false);

    /*
     * Open table.  The lock mode is the same as the leader process.  It's
     * okay because the lock mode does not conflict among the parallel
     * workers.
     */
    rel = table_open((*shared).relid, ShareUpdateExclusiveLock);

    /*
     * Open all indexes. indrels are sorted in order by OID, which should be
     * matched to the leader's one.
     */
    vac_open_indexes(rel, RowExclusiveLock, &raw mut nindexes, &raw mut indrels);
    Assert!(nindexes > 0);

    /*
     * Apply the desired value of maintenance_work_mem within this process.
     * Really we should use SetConfigOption() to change a GUC, but since we're
     * already in parallel mode guc.c would complain about that.  Fortunately,
     * by the same token guc.c will not let any user-defined code change it.
     * So just avert your eyes while we do this:
     */
    if (*shared).maintenance_work_mem_worker > 0 {
        maintenance_work_mem = (*shared).maintenance_work_mem_worker;
    }

    /* Set index statistics */
    indstats =
        shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_INDEX_STATS, false) as *mut PVIndStats;

    /* Find dead_items in shared memory */
    dead_items = TidStoreAttach((*shared).dead_items_dsa_handle, (*shared).dead_items_handle);

    /* Set cost-based vacuum delay */
    VacuumUpdateCosts();
    VacuumCostBalance = 0;
    VacuumCostBalanceLocal = 0;
    VacuumSharedCostBalance = &raw mut (*shared).cost_balance;
    VacuumActiveNWorkers = &raw mut (*shared).active_nworkers;

    /* Set parallel vacuum state */
    pvs.indrels = indrels;
    pvs.nindexes = nindexes;
    pvs.indstats = indstats;
    pvs.shared = shared;
    pvs.dead_items = dead_items;
    pvs.relnamespace = get_namespace_name(RelationGetNamespace(rel));
    pvs.relname = pstrdup(RelationGetRelationName(rel));
    pvs.heaprel = rel;

    /* These fields will be filled during index vacuum or cleanup */
    pvs.indname = null_mut();
    pvs.status = PARALLEL_INDVAC_STATUS_INITIAL;

    /* Each parallel VACUUM worker gets its own access strategy. */
    pvs.bstrategy = GetAccessStrategyWithSize(
        BAS_VACUUM,
        (*shared).ring_nbuffers * (BLCKSZ / 1024),
    );

    /* Setup error traceback support for ereport() */
    errcallback.callback = Some(parallel_vacuum_error_callback);
    errcallback.arg = &raw mut pvs as *mut c_void;
    errcallback.previous = error_context_stack;
    error_context_stack = &raw mut errcallback;

    /* Prepare to track buffer usage during parallel execution */
    InstrStartParallelQuery();

    /* Process indexes to perform vacuum/cleanup */
    parallel_vacuum_process_safe_indexes(&raw mut pvs);

    /* Report buffer/WAL usage during parallel execution */
    buffer_usage =
        shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_BUFFER_USAGE, false) as *mut BufferUsage;
    wal_usage = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_WAL_USAGE, false) as *mut WalUsage;
    InstrEndParallelQuery(
        &raw mut *buffer_usage.add(ParallelWorkerNumber as usize),
        &raw mut *wal_usage.add(ParallelWorkerNumber as usize),
    );

    /* Report any remaining cost-based vacuum delay time */
    if track_cost_delay_timing {
        pgstat_progress_parallel_incr_param(
            PROGRESS_VACUUM_DELAY_TIME,
            parallel_vacuum_worker_delay_ns as i64,
        );
    }

    TidStoreDetach(dead_items);

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;

    vac_close_indexes(nindexes, indrels, RowExclusiveLock);
    table_close(rel, ShareUpdateExclusiveLock);
    FreeAccessStrategy(pvs.bstrategy);
}

/*
 * Error context callback for errors occurring during parallel index vacuum.
 * The error context messages should match the messages set in the lazy vacuum
 * error context.  If you change this function, change vacuum_error_callback()
 * as well.
 */
unsafe extern "C" fn parallel_vacuum_error_callback(arg: *mut c_void) {
    let errinfo: *mut ParallelVacuumState = arg as *mut ParallelVacuumState;

    match (*errinfo).status {
        PARALLEL_INDVAC_STATUS_NEED_BULKDELETE => {
            errcontext!(
                "while vacuuming index \"{}\" of relation \"{}.{}\"",
                cstr_display((*errinfo).indname),
                cstr_display((*errinfo).relnamespace),
                cstr_display((*errinfo).relname)
            );
        }
        PARALLEL_INDVAC_STATUS_NEED_CLEANUP => {
            errcontext!(
                "while cleaning up index \"{}\" of relation \"{}.{}\"",
                cstr_display((*errinfo).indname),
                cstr_display((*errinfo).relnamespace),
                cstr_display((*errinfo).relname)
            );
        }
        PARALLEL_INDVAC_STATUS_INITIAL | PARALLEL_INDVAC_STATUS_COMPLETED => {}
    }
}

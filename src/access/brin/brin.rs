/*
 * brin.c
 *		Implementation of BRIN indexes for Postgres
 *
 * See src/backend/access/brin/README for details.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin.c
 *
 * TODO
 *		* ScalarArrayOpExpr (amsearcharray -> SK_SEARCHARRAY)
 */
//! Translated from postgres/src/backend/access/brin/brin.c (and access/brin.h).

use crate::prelude::*;

use core::ffi::CStr;
use std::ffi::{c_int, c_void};

use crate::c::{int64, uint16, Size};

use crate::access::attnum::AttrNumber;
use crate::access::brin::brin_internal::{
    BrinDesc, BrinOpcInfo, BRIN_LAST_OPTIONAL_PROCNUM, BRIN_PROCNUM_ADDVALUE,
    BRIN_PROCNUM_CONSISTENT, BRIN_PROCNUM_OPCINFO, BRIN_PROCNUM_OPTIONS, BRIN_PROCNUM_UNION,
};
use crate::access::brin::brin_page::{
    BrinMetaPageData, BRIN_CURRENT_VERSION, BRIN_METAPAGE_BLKNO,
};
use crate::access::brin::brin_pageops::{
    brin_can_do_samepage_update, brin_doinsert, brin_doupdate, brin_metapage_init,
    brin_page_cleanup,
};
use crate::access::brin::brin_revmap::{
    brinGetTupleForHeapBlock, brinRevmapDesummarizeRange, brinRevmapInitialize, brinRevmapTerminate,
    BrinRevmap,
};
use crate::access::brin::brin_tuple::{
    brin_copy_tuple, brin_deform_tuple, brin_form_placeholder_tuple, brin_form_tuple,
    brin_free_tuple, brin_memtuple_initialize, brin_new_memtuple, BrinMemTuple, BrinTuple,
    BrinValues,
};
use crate::access::common::scankey::{
    ScanKey, ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use crate::access::index::amapi::IndexAmRoutine;
use crate::access::index::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo, RelationGetIndexScan,
};
use crate::access::index::indexam::{index_close, index_getprocinfo, index_open};
use crate::access::rmgrdesc::brindesc::xl_brin_createidx;
use crate::access::rmgrlist::RM_BRIN_ID;
use crate::access::table::table::{table_close, table_open};
use crate::access::table::tableam::{
    table_beginscan_parallel, table_parallelscan_estimate, table_parallelscan_initialize,
};
use crate::access::relscan::{ParallelTableScanDesc, TableScanDesc};
use crate::utils::snapshot::Snapshot;
use crate::catalog::aclchk::{aclcheck_error, object_ownercheck};
use crate::catalog::index::{BuildIndexInfo, IndexGetRelation};
use crate::catalog::catalog_oids::RelationRelationId;
use crate::catalog::pg_class::RELKIND_INDEX;
use crate::catalog::pg_known_oids::BRIN_AM_OID;
use crate::commands::vacuumparallel::VACUUM_OPTION_PARALLEL_CLEANUP;
use crate::common::relpath::{INIT_FORKNUM, MAIN_FORKNUM};
use crate::executor::instrument::{
    InstrAccumParallelQuery, InstrEndParallelQuery, InstrStartParallelQuery, BufferUsage,
    WalUsage,
};
use crate::miscadmin::{
    GetUserId, GetUserIdAndSecContext, SetUserIdAndSecContext, SECURITY_RESTRICTED_OPERATION,
};
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::parsenodes::{ObjectType, OBJECT_INDEX};
use crate::nodes::tidbitmap::{tbm_add_page, TIDBitmap};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::postmaster::autovacuum::{AutoVacuumRequestWork, AutoVacuumWorkItemType};
use crate::storage::block::{BlockNumber, InvalidBlockNumber, MaxBlockNumber};
use crate::storage::buf::{Buffer, BufferAccessStrategy, BufferIsInvalid, InvalidBuffer};
use crate::storage::buffer::bufmgr::{
    BufferGetBlockNumber, BufferGetPage, ExtendBufferedRel, LockBuffer, MarkBufferDirty, ReadBuffer,
    ReadBufferExtended, ReleaseBuffer, UnlockReleaseBuffer, BMR_REL, BUFFER_LOCK_SHARE,
    BUFFER_LOCK_UNLOCK, EB_LOCK_FIRST, EB_SKIP_EXTENSION_LOCK, RBM_NORMAL,
};
use crate::storage::bufpage::{
    Page, PageGetContents, PageGetFreeSpace, PageGetItemId, PageSetLSN,
};
use crate::storage::freespace::freespace::{
    FreeSpaceMapVacuum, FreeSpaceMapVacuumRange, RecordPageWithFreeSpace,
};
use crate::storage::ipc::shm_toc::{
    shm_toc, shm_toc_allocate, shm_toc_insert, shm_toc_lookup,
};
use crate::storage::ipc::shmem::{add_size, mul_size};
use crate::storage::itemid::{ItemId, ItemIdGetLength};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};
use crate::storage::lmgr::condition_variable::{
    ConditionVariable, ConditionVariableCancelSleep, ConditionVariableInit,
    ConditionVariableSignal, ConditionVariableSleep,
};
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::lockdefs::{
    AccessExclusiveLock, AccessShareLock, RowExclusiveLock, ShareLock, ShareUpdateExclusiveLock,
    LOCKMODE,
};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber};
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::access::transam::xloginsert::{
    log_newpage_buffer, XLogBeginInsert, XLogInsert, XLogRegisterBuffer, XLogRegisterData,
    REGBUF_STANDARD, REGBUF_WILL_INIT,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::common::relation::relation_close;
use crate::storage::lmgr::proc::{MyProc, PROC_IN_SAFE_IC};
use crate::utils::activity::backend_status::{
    pgstat_get_my_query_id, pgstat_report_activity, pgstat_report_query_id, STATE_RUNNING,
};
use crate::utils::activity::pgstat_relation::pgstat_count_index_scan;
use crate::utils::adt::acl::ACLCHECK_NOT_OWNER;
use crate::utils::adt::datum::datumCopy;
use crate::utils::fmgr::{
    fmgr_info_copy, DirectFunctionCall2Coll, FmgrInfo, FunctionCall1Coll, FunctionCall3Coll,
    FunctionCall4Coll, FunctionCallInfo, PGFunction,
};
use crate::utils::init::globals::maintenance_work_mem;
use crate::utils::misc::guc::{AtEOXact_GUC, NewGUCNestLevel, RestrictSearchPath};
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetRelationName, RelationGetRelid,
};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::tcop::postgres::debug_query_string;
use crate::{
    makeNode, Assert, AssertMacro, DirectFunctionCall2, FunctionCall1, PG_GETARG_DATUM,
    PG_GETARG_INT64, PG_GETARG_OID, PG_RETURN_INT32, PG_RETURN_POINTER, PG_RETURN_VOID,
};

// ---------------------------------------------------------------------------
// Symbols whose real home is not ported yet -- thin local stubs (mirroring the
// approach taken in access/nbtree/nbtsort.c's translation).
// ---------------------------------------------------------------------------

/// SizeOfBrinCreateIdx (access/brin_xlog.h):
///   offsetof(xl_brin_createidx, version) + sizeof(uint16)
const SizeOfBrinCreateIdx: usize =
    core::mem::offset_of!(xl_brin_createidx, version) + core::mem::size_of::<uint16>();

/// XLOG_BRIN_CREATE_INDEX (access/brin_xlog.h).
const XLOG_BRIN_CREATE_INDEX: uint8 = 0x00;

/// RelationNeedsWAL (utils/rel.h) -- TODO(pg-port): not yet centralized.
unsafe fn RelationNeedsWAL(_relation: Relation) -> bool {
    true
}

/// RelationGetNumberOfBlocks (storage/bufmgr.h) -- TODO(pg-port).
unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber {
    0
}

/// BufferIsValid (storage/buf.h) -- true iff buffer is not InvalidBuffer.
#[inline]
unsafe fn BufferIsValid(buffer: Buffer) -> bool {
    !BufferIsInvalid(buffer)
}

/// table_index_build_scan (access/tableam.h) -- TODO(pg-port).
unsafe fn table_index_build_scan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _indexInfo: *mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _callback: IndexBuildCallback,
    _callback_state: *mut c_void,
    _scan: TableScanDesc,
) -> f64 {
    0.0
}

/// table_index_build_range_scan (access/tableam.h) -- TODO(pg-port).
unsafe fn table_index_build_range_scan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _indexInfo: *mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _progress: bool,
    _start_blockno: BlockNumber,
    _numblocks: BlockNumber,
    _callback: IndexBuildCallback,
    _callback_state: *mut c_void,
    _scan: TableScanDesc,
) -> f64 {
    0.0
}

/// IndexBuildCallback (access/tableam.h).
type IndexBuildCallback = Option<
    unsafe extern "C" fn(
        index: Relation,
        tid: ItemPointer,
        values: *mut Datum,
        isnull: *mut bool,
        tupleIsAlive: bool,
        state: *mut c_void,
    ),
>;

/// RegisterSnapshot (utils/snapmgr.h) -- TODO(pg-port).
unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    snapshot
}

/// UnregisterSnapshot (utils/snapmgr.h) -- TODO(pg-port).
unsafe fn UnregisterSnapshot(_snapshot: Snapshot) {}

/// GetTransactionSnapshot (utils/snapmgr.h) -- TODO(pg-port).
unsafe fn GetTransactionSnapshot() -> Snapshot {
    null_mut()
}

/// IsMVCCSnapshot (utils/snapshot.h) -- TODO(pg-port).
#[no_mangle]
unsafe fn IsMVCCSnapshot(_snapshot: Snapshot) -> bool {
    false
}

/// SnapshotAny (utils/snapmgr.h) -- sentinel snapshot pointer. TODO(pg-port).
const SnapshotAny: Snapshot = null_mut();

/// RecoveryInProgress (access/xlog.h) -- TODO(pg-port).
unsafe fn RecoveryInProgress() -> bool {
    false
}

/// BRIN_elog placeholder (BRIN_elog((level, fmt, ...)) is debug-only tracing).
unsafe fn BRIN_elog() {}

/// reloptions support (access/reloptions.h) -- TODO(pg-port): not yet ported.
const RELOPT_TYPE_INT: c_int = 0;
const RELOPT_TYPE_BOOL: c_int = 2;
const RELOPT_KIND_BRIN: c_int = 1 << 8;

#[repr(C)]
struct relopt_parse_elt {
    optname: *const c_char,
    opttype: c_int,
    offset: c_int,
}

extern "C" {
    fn build_reloptions(
        reloptions: Datum,
        validate: bool,
        kind: c_int,
        relopt_struct_size: Size,
        relopt_elems: *const relopt_parse_elt,
        num_relopt_elems: c_int,
    ) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
}

// ---------------------------------------------------------------------------
// Parallel-build / tuplesort subsystem (access/parallel.h, utils/tuplesort.h):
// these modules are not yet wired into the crate, so we provide thin local
// stubs, mirroring access/nbtree/nbtsort.c's translation.
// ---------------------------------------------------------------------------

/// TODO(pg-port): Tuplesortstate (utils/tuplesort.h).
pub enum Tuplesortstate {}

/// TODO(pg-port): Sharedsort (utils/tuplesort.h).
pub enum Sharedsort {}

/// TODO(pg-port): SortCoordinateData (utils/sortsupport.h).
#[repr(C)]
pub struct SortCoordinateData {
    pub isWorker: bool,
    pub nParticipants: c_int,
    pub sharedsort: *mut Sharedsort,
}
pub type SortCoordinate = *mut SortCoordinateData;

pub const TUPLESORT_NONE: c_int = 0;

/// TODO(pg-port): dsm_segment (storage/dsm.h).
pub enum dsm_segment {}

/// TODO(pg-port): shm_toc_estimator (storage/shm_toc.h).
#[repr(C)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

/// TODO(pg-port): ParallelContext (access/parallel.h).
#[repr(C)]
pub struct ParallelContext {
    pub estimator: shm_toc_estimator,
    pub nworkers: c_int,
    pub nworkers_launched: c_int,
    pub seg: *mut dsm_segment,
    pub toc: *mut shm_toc,
}

pub static mut ParallelWorkerNumber: c_int = -1;

const WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN: u32 = 0;

unsafe fn EnterParallelMode() {}
unsafe fn ExitParallelMode() {}
unsafe fn CreateParallelContext(
    _library_name: *const c_char,
    _function_name: *const c_char,
    _nworkers: c_int,
) -> *mut ParallelContext {
    null_mut()
}
unsafe fn InitializeParallelDSM(_pcxt: *mut ParallelContext) {}
unsafe fn LaunchParallelWorkers(_pcxt: *mut ParallelContext) {}
unsafe fn WaitForParallelWorkersToAttach(_pcxt: *mut ParallelContext) {}
unsafe fn WaitForParallelWorkersToFinish(_pcxt: *mut ParallelContext) {}
unsafe fn DestroyParallelContext(_pcxt: *mut ParallelContext) {}

unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {}
unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: c_int) {}

unsafe fn tuplesort_begin_index_brin(
    _workMem: c_int,
    _coordinate: SortCoordinate,
    _sortopt: c_int,
) -> *mut Tuplesortstate {
    null_mut()
}
unsafe fn tuplesort_estimate_shared(_nworkers: c_int) -> Size {
    0
}
unsafe fn tuplesort_initialize_shared(
    _shared: *mut Sharedsort,
    _nWorkers: c_int,
    _seg: *mut dsm_segment,
) {
}
unsafe fn tuplesort_attach_shared(_shared: *mut Sharedsort, _seg: *mut dsm_segment) {}
unsafe fn tuplesort_performsort(_state: *mut Tuplesortstate) {}
unsafe fn tuplesort_end(_state: *mut Tuplesortstate) {}
unsafe fn tuplesort_putbrintuple(_state: *mut Tuplesortstate, _tuple: *mut BrinTuple, _size: Size) {}
unsafe fn tuplesort_getbrintuple(
    _state: *mut Tuplesortstate,
    _len: *mut Size,
    _forward: bool,
) -> *mut BrinTuple {
    null_mut()
}

// ---------------------------------------------------------------------------
// access/brin.h merged in.
// ---------------------------------------------------------------------------

/*
 * Storage type for BRIN's reloptions
 */
#[repr(C)]
pub struct BrinOptions {
    pub vl_len_: int32,         /* varlena header (do not touch directly!) */
    pub pagesPerRange: BlockNumber,
    pub autosummarize: bool,
}

/*
 * BrinStatsData represents stats data for planner use
 */
#[repr(C)]
pub struct BrinStatsData {
    pub pagesPerRange: BlockNumber,
    pub revmapNumPages: BlockNumber,
}

pub const BRIN_DEFAULT_PAGES_PER_RANGE: BlockNumber = 128;

macro_rules! BrinGetPagesPerRange {
    ($relation:expr) => {{
        AssertMacro!(
            (*(*$relation).rd_rel).relkind == RELKIND_INDEX
                && (*(*$relation).rd_rel).relam == BRIN_AM_OID
        );
        if !(*$relation).rd_options.is_null() {
            (*((*$relation).rd_options as *mut BrinOptions)).pagesPerRange
        } else {
            BRIN_DEFAULT_PAGES_PER_RANGE
        }
    }};
}

macro_rules! BrinGetAutoSummarize {
    ($relation:expr) => {{
        AssertMacro!(
            (*(*$relation).rd_rel).relkind == RELKIND_INDEX
                && (*(*$relation).rd_rel).relam == BRIN_AM_OID
        );
        if !(*$relation).rd_options.is_null() {
            (*((*$relation).rd_options as *mut BrinOptions)).autosummarize
        } else {
            false
        }
    }};
}

// CHECK_FOR_INTERRUPTS is a function in miscadmin, but this file uses it with
// macro-call syntax (matching the C macro).
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {
        crate::miscadmin::CHECK_FOR_INTERRUPTS()
    };
}

// START_CRIT_SECTION / END_CRIT_SECTION are functions in miscadmin, but this
// file uses them with macro-call syntax (matching the C macros).
macro_rules! START_CRIT_SECTION {
    () => {
        crate::miscadmin::START_CRIT_SECTION()
    };
}
macro_rules! END_CRIT_SECTION {
    () => {
        crate::miscadmin::END_CRIT_SECTION()
    };
}

/* Magic numbers for parallel state sharing */
const PARALLEL_KEY_BRIN_SHARED: u64 = 0xB000000000000001;
const PARALLEL_KEY_TUPLESORT: u64 = 0xB000000000000002;
const PARALLEL_KEY_QUERY_TEXT: u64 = 0xB000000000000003;
const PARALLEL_KEY_WAL_USAGE: u64 = 0xB000000000000004;
const PARALLEL_KEY_BUFFER_USAGE: u64 = 0xB000000000000005;

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.
 */
#[repr(C)]
pub struct BrinShared {
    /*
     * These fields are not modified during the build.  They primarily exist
     * for the benefit of worker processes that need to create state
     * corresponding to that used by the leader.
     */
    pub heaprelid: Oid,
    pub indexrelid: Oid,
    pub isconcurrent: bool,
    pub pagesPerRange: BlockNumber,
    pub scantuplesortstates: c_int,

    /* Query ID, for report in worker processes */
    pub queryid: int64,

    /*
     * workersdonecv is used to monitor the progress of workers.  All parallel
     * participants must indicate that they are done before leader can use
     * results built by the workers (and before leader can write the data into
     * the index).
     */
    pub workersdonecv: ConditionVariable,

    /*
     * mutex protects all fields before heapdesc.
     */
    pub mutex: slock_t,

    /*
     * Mutable state that is maintained by workers, and reported back to
     * leader at end of the scans.
     */
    pub nparticipantsdone: c_int,
    pub reltuples: f64,
    pub indtuples: f64,
    /*
     * ParallelTableScanDescData data follows.
     */
}

/*
 * Return pointer to a BrinShared's parallel table scan.
 *
 * c.f. shm_toc_allocate as to why BUFFERALIGN is used, rather than just
 * MAXALIGN.
 */
macro_rules! ParallelTableScanFromBrinShared {
    ($shared:expr) => {
        (($shared as *mut c_char).add(crate::c::BUFFERALIGN(core::mem::size_of::<BrinShared>())))
            as ParallelTableScanDesc
    };
}

/*
 * Status for leader in parallel index build.
 */
#[repr(C)]
pub struct BrinLeader {
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
     */
    pub brinshared: *mut BrinShared,
    pub sharedsort: *mut Sharedsort,
    pub snapshot: Snapshot,
    pub walusage: *mut WalUsage,
    pub bufferusage: *mut BufferUsage,
}

/*
 * We use a BrinBuildState during initial construction of a BRIN index.
 * The running state is kept in a BrinMemTuple.
 */
#[repr(C)]
pub struct BrinBuildState {
    pub bs_irel: Relation,
    pub bs_numtuples: f64,
    pub bs_reltuples: f64,
    pub bs_currentInsertBuf: Buffer,
    pub bs_pagesPerRange: BlockNumber,
    pub bs_currRangeStart: BlockNumber,
    pub bs_maxRangeStart: BlockNumber,
    pub bs_rmAccess: *mut BrinRevmap,
    pub bs_bdesc: *mut BrinDesc,
    pub bs_dtuple: *mut BrinMemTuple,

    pub bs_emptyTuple: *mut BrinTuple,
    pub bs_emptyTupleLen: Size,
    pub bs_context: MemoryContext,

    /*
     * bs_leader is only present when a parallel index build is performed, and
     * only in the leader process. (Actually, only the leader process has a
     * BrinBuildState.)
     */
    pub bs_leader: *mut BrinLeader,
    pub bs_worker_id: c_int,

    /*
     * The sortstate is used by workers (including the leader). It has to be
     * part of the build state, because that's the only thing passed to the
     * build callback etc.
     */
    pub bs_sortstate: *mut Tuplesortstate,
}

/*
 * We use a BrinInsertState to capture running state spanning multiple
 * brininsert invocations, within the same command.
 */
#[repr(C)]
pub struct BrinInsertState {
    pub bis_rmAccess: *mut BrinRevmap,
    pub bis_desc: *mut BrinDesc,
    pub bis_pages_per_range: BlockNumber,
}

/*
 * Struct used as "opaque" during index scans
 */
#[repr(C)]
pub struct BrinOpaque {
    pub bo_pagesPerRange: BlockNumber,
    pub bo_rmAccess: *mut BrinRevmap,
    pub bo_bdesc: *mut BrinDesc,
}

pub const BRIN_ALL_BLOCKRANGES: BlockNumber = InvalidBlockNumber;

/*
 * BRIN handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
pub unsafe fn brinhandler(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    let amroutine: *mut IndexAmRoutine = makeNode!(IndexAmRoutine, T_IndexAmRoutine);

    (*amroutine).amstrategies = 0;
    (*amroutine).amsupport = BRIN_LAST_OPTIONAL_PROCNUM as uint16;
    (*amroutine).amoptsprocnum = BRIN_PROCNUM_OPTIONS as uint16;
    (*amroutine).amcanorder = false;
    (*amroutine).amcanorderbyop = false;
    (*amroutine).amcanhash = false;
    (*amroutine).amconsistentequality = false;
    (*amroutine).amconsistentordering = false;
    (*amroutine).amcanbackward = false;
    (*amroutine).amcanunique = false;
    (*amroutine).amcanmulticol = true;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amsearcharray = false;
    (*amroutine).amsearchnulls = true;
    (*amroutine).amstorage = true;
    (*amroutine).amclusterable = false;
    (*amroutine).ampredlocks = false;
    (*amroutine).amcanparallel = false;
    (*amroutine).amcanbuildparallel = true;
    (*amroutine).amcaninclude = false;
    (*amroutine).amusemaintenanceworkmem = false;
    (*amroutine).amsummarizing = true;
    (*amroutine).amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_CLEANUP;
    (*amroutine).amkeytype = InvalidOid;

    // The IndexAmRoutine callback fields are `extern "C"` fn pointers; our
    // translated callbacks are plain `unsafe fn`.  Transmute through a thin
    // pointer to install them (matching how nbtree's handler does it).
    (*amroutine).ambuild = Some(core::mem::transmute(brinbuild as *const ()));
    (*amroutine).ambuildempty = Some(core::mem::transmute(brinbuildempty as *const ()));
    (*amroutine).aminsert = Some(core::mem::transmute(brininsert as *const ()));
    (*amroutine).aminsertcleanup = Some(core::mem::transmute(brininsertcleanup as *const ()));
    (*amroutine).ambulkdelete = Some(core::mem::transmute(brinbulkdelete as *const ()));
    (*amroutine).amvacuumcleanup = Some(core::mem::transmute(brinvacuumcleanup as *const ()));
    (*amroutine).amcanreturn = None;
    (*amroutine).amcostestimate =
        Some(core::mem::transmute(crate::utils::index_selfuncs::brincostestimate as *const ()));
    (*amroutine).amgettreeheight = None;
    (*amroutine).amoptions = Some(core::mem::transmute(brinoptions as *const ()));
    (*amroutine).amproperty = None;
    (*amroutine).ambuildphasename = None;
    (*amroutine).amvalidate =
        Some(core::mem::transmute(crate::access::brin::brin_validate::brinvalidate as *const ()));
    (*amroutine).amadjustmembers = None;
    (*amroutine).ambeginscan = Some(core::mem::transmute(brinbeginscan as *const ()));
    (*amroutine).amrescan = Some(core::mem::transmute(brinrescan as *const ()));
    (*amroutine).amgettuple = None;
    (*amroutine).amgetbitmap = Some(core::mem::transmute(bringetbitmap as *const ()));
    (*amroutine).amendscan = Some(core::mem::transmute(brinendscan as *const ()));
    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = None;
    (*amroutine).aminitparallelscan = None;
    (*amroutine).amparallelrescan = None;
    (*amroutine).amtranslatestrategy = None;
    (*amroutine).amtranslatecmptype = None;

    PG_RETURN_POINTER!(amroutine)
}

/*
 * Initialize a BrinInsertState to maintain state to be used across multiple
 * tuple inserts, within the same command.
 */
unsafe fn initialize_brin_insertstate(
    idxRel: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut BrinInsertState {
    let bistate: *mut BrinInsertState;
    let oldcxt: MemoryContext;

    oldcxt = MemoryContextSwitchTo((*indexInfo).ii_Context);
    bistate = palloc0(core::mem::size_of::<BrinInsertState>()) as *mut BrinInsertState;
    (*bistate).bis_desc = brin_build_desc(idxRel);
    (*bistate).bis_rmAccess = brinRevmapInitialize(idxRel, &mut (*bistate).bis_pages_per_range);
    (*indexInfo).ii_AmCache = bistate as *mut c_void;
    MemoryContextSwitchTo(oldcxt);

    bistate
}

/*
 * A tuple in the heap is being inserted.  To keep a brin index up to date,
 * we need to obtain the relevant index tuple and compare its stored values
 * with those of the new tuple.  If the tuple values are not consistent with
 * the summary tuple, we need to update the index tuple.
 *
 * If autosummarization is enabled, check if we need to summarize the previous
 * page range.
 *
 * If the range is not currently summarized (i.e. the revmap returns NULL for
 * it), there's nothing to do for this tuple.
 */
pub unsafe fn brininsert(
    idxRel: Relation,
    values: *mut Datum,
    nulls: *mut bool,
    heaptid: ItemPointer,
    _heapRel: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    let pagesPerRange: BlockNumber;
    let origHeapBlk: BlockNumber;
    let heapBlk: BlockNumber;
    let mut bistate: *mut BrinInsertState = (*indexInfo).ii_AmCache as *mut BrinInsertState;
    let revmap: *mut BrinRevmap;
    let bdesc: *mut BrinDesc;
    let mut buf: Buffer = InvalidBuffer;
    let mut tupcxt: MemoryContext = null_mut();
    let oldcxt: MemoryContext = CurrentMemoryContext;
    let autosummarize: bool = BrinGetAutoSummarize!(idxRel);

    /*
     * If first time through in this statement, initialize the insert state
     * that we keep for all the inserts in the command.
     */
    if bistate.is_null() {
        bistate = initialize_brin_insertstate(idxRel, indexInfo);
    }

    revmap = (*bistate).bis_rmAccess;
    bdesc = (*bistate).bis_desc;
    pagesPerRange = (*bistate).bis_pages_per_range;

    /*
     * origHeapBlk is the block number where the insertion occurred.  heapBlk
     * is the first block in the corresponding page range.
     */
    origHeapBlk = ItemPointerGetBlockNumber(heaptid);
    heapBlk = (origHeapBlk / pagesPerRange) * pagesPerRange;

    loop {
        let mut need_insert: bool = false;
        let mut off: OffsetNumber = 0;
        let brtup: *mut BrinTuple;
        let dtup: *mut BrinMemTuple;

        CHECK_FOR_INTERRUPTS!();

        /*
         * If auto-summarization is enabled and we just inserted the first
         * tuple into the first block of a new non-first page range, request a
         * summarization run of the previous range.
         */
        if autosummarize
            && heapBlk > 0
            && heapBlk == origHeapBlk
            && ItemPointerGetOffsetNumber(heaptid) == FirstOffsetNumber
        {
            let lastPageRange: BlockNumber = heapBlk - 1;
            let lastPageTuple: *mut BrinTuple;

            lastPageTuple = brinGetTupleForHeapBlock(
                revmap,
                lastPageRange,
                &mut buf,
                &mut off,
                null_mut(),
                BUFFER_LOCK_SHARE,
            );
            if lastPageTuple.is_null() {
                let recorded: bool;

                recorded = AutoVacuumRequestWork(
                    AutoVacuumWorkItemType::AVW_BRINSummarizeRange,
                    RelationGetRelid(idxRel),
                    lastPageRange,
                );
                if !recorded {
                    // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    ereport!(
                        LOG,
                        errmsg!(
                            "request for BRIN range summarization for index \"{}\" page {} was not recorded",
                            CStr::from_ptr(RelationGetRelationName(idxRel)).to_string_lossy(),
                            lastPageRange
                        )
                    );
                }
            } else {
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
        }

        brtup = brinGetTupleForHeapBlock(
            revmap,
            heapBlk,
            &mut buf,
            &mut off,
            null_mut(),
            BUFFER_LOCK_SHARE,
        );

        /* if range is unsummarized, there's nothing to do */
        if brtup.is_null() {
            break;
        }

        /* First time through in this brininsert call? */
        if tupcxt.is_null() {
            tupcxt = AllocSetContextCreate!(
                CurrentMemoryContext,
                c"brininsert cxt".as_ptr(),
                ALLOCSET_DEFAULT_SIZES
            );
            MemoryContextSwitchTo(tupcxt);
        }

        dtup = brin_deform_tuple(bdesc, brtup, null_mut());

        need_insert = add_values_to_range(idxRel, bdesc, dtup, values, nulls);

        if !need_insert {
            /*
             * The tuple is consistent with the new values, so there's nothing
             * to do.
             */
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        } else {
            let page: Page = BufferGetPage(buf);
            let lp: ItemId = PageGetItemId(page, off);
            let origsz: Size;
            let origtup: *mut BrinTuple;
            let newsz: Size = 0;
            let newtup: *mut BrinTuple;
            let samepage: bool;

            /*
             * Make a copy of the old tuple, so that we can compare it after
             * re-acquiring the lock.
             */
            origsz = ItemIdGetLength(lp) as Size;
            origtup = brin_copy_tuple(brtup, origsz, null_mut(), null_mut());

            /*
             * Before releasing the lock, check if we can attempt a same-page
             * update.  Another process could insert a tuple concurrently in
             * the same page though, so downstream we must be prepared to cope
             * if this turns out to not be possible after all.
             */
            let mut newsz = newsz;
            newtup = brin_form_tuple(bdesc, heapBlk, dtup, &mut newsz);
            samepage = brin_can_do_samepage_update(buf, origsz, newsz);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);

            /*
             * Try to update the tuple.  If this doesn't work for whatever
             * reason, we need to restart from the top; the revmap might be
             * pointing at a different tuple for this block now, so we need to
             * recompute to ensure both our new heap tuple and the other
             * inserter's are covered by the combined tuple.  It might be that
             * we don't need to update at all.
             */
            if !brin_doupdate(
                idxRel,
                pagesPerRange,
                revmap,
                heapBlk,
                buf,
                off,
                origtup,
                origsz,
                newtup,
                newsz,
                samepage,
            ) {
                /* no luck; start over */
                MemoryContextReset(tupcxt);
                continue;
            }
        }

        /* success! */
        break;
    }

    if BufferIsValid(buf) {
        ReleaseBuffer(buf);
    }
    MemoryContextSwitchTo(oldcxt);
    if !tupcxt.is_null() {
        MemoryContextDelete(tupcxt);
    }

    false
}

/*
 * Callback to clean up the BrinInsertState once all tuple inserts are done.
 */
pub unsafe fn brininsertcleanup(_index: Relation, indexInfo: *mut IndexInfo) {
    let bistate: *mut BrinInsertState = (*indexInfo).ii_AmCache as *mut BrinInsertState;

    /* bail out if cache not initialized */
    if bistate.is_null() {
        return;
    }

    /* do this first to avoid dangling pointer if we fail partway through */
    (*indexInfo).ii_AmCache = null_mut();

    /*
     * Clean up the revmap. Note that the brinDesc has already been cleaned up
     * as part of its own memory context.
     */
    brinRevmapTerminate((*bistate).bis_rmAccess);
    pfree(bistate as *mut c_void);
}

/*
 * Initialize state for a BRIN index scan.
 *
 * We read the metapage here to determine the pages-per-range number that this
 * index was built with.  Note that since this cannot be changed while we're
 * holding lock on index, it's not necessary to recompute it during brinrescan.
 */
pub unsafe fn brinbeginscan(r: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc {
    let scan: IndexScanDesc;
    let opaque: *mut BrinOpaque;

    scan = RelationGetIndexScan(r, nkeys, norderbys);

    opaque = palloc(core::mem::size_of::<BrinOpaque>()) as *mut BrinOpaque;
    (*opaque).bo_rmAccess = brinRevmapInitialize(r, &mut (*opaque).bo_pagesPerRange);
    (*opaque).bo_bdesc = brin_build_desc(r);
    (*scan).opaque = opaque as *mut c_void;

    scan
}

/*
 * Execute the index scan.
 *
 * This works by reading index TIDs from the revmap, and obtaining the index
 * tuples pointed to by them; the summary values in the index tuples are
 * compared to the scan keys.  We return into the TID bitmap all the pages in
 * ranges corresponding to index tuples that match the scan keys.
 *
 * If a TID from the revmap is read as InvalidTID, we know that range is
 * unsummarized.  Pages in those ranges need to be returned regardless of scan
 * keys.
 */
pub unsafe fn bringetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    let idxRel: Relation = (*scan).indexRelation;
    let mut buf: Buffer = InvalidBuffer;
    let bdesc: *mut BrinDesc;
    let heapOid: Oid;
    let heapRel: Relation;
    let opaque: *mut BrinOpaque;
    let nblocks: BlockNumber;
    let mut totalpages: int64 = 0;
    let consistentFn: *mut FmgrInfo;
    let oldcxt: MemoryContext;
    let perRangeCxt: MemoryContext;
    let mut dtup: *mut BrinMemTuple;
    let mut btup: *mut BrinTuple = null_mut();
    let mut btupsz: Size = 0;
    let keys: *mut *mut ScanKey;
    let nullkeys: *mut *mut ScanKey;
    let nkeys: *mut c_int;
    let nnullkeys: *mut c_int;
    let mut ptr: *mut c_char;
    let len: Size;

    opaque = (*scan).opaque as *mut BrinOpaque;
    bdesc = (*opaque).bo_bdesc;
    pgstat_count_index_scan(idxRel);
    if !(*scan).instrument.is_null() {
        if !(*scan).instrument.is_null() { *((*scan).instrument as *mut u64) += 1; }
    }

    /*
     * We need to know the size of the table so that we know how long to
     * iterate on the revmap.
     */
    heapOid = IndexGetRelation(RelationGetRelid(idxRel), false);
    heapRel = table_open(heapOid, AccessShareLock);
    nblocks = RelationGetNumberOfBlocks(heapRel);
    table_close(heapRel, AccessShareLock);

    let natts = (*(*bdesc).bd_tupdesc).natts;

    /*
     * Make room for the consistent support procedures of indexed columns.  We
     * don't look them up here; we do that lazily the first time we see a scan
     * key reference each of them.  We rely on zeroing fn_oid to InvalidOid.
     */
    consistentFn = palloc0((natts as usize) * core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;

    /*
     * Make room for per-attribute lists of scan keys that we'll pass to the
     * consistent support procedure. We don't know which attributes have scan
     * keys, so we allocate space for all attributes. That may use more memory
     * but it's probably cheaper than determining which attributes are used.
     *
     * We keep null and regular keys separate, so that we can pass just the
     * regular keys to the consistent function easily.
     *
     * To reduce the allocation overhead, we allocate one big chunk and then
     * carve it into smaller arrays ourselves. All the pieces have exactly the
     * same lifetime, so that's OK.
     */
    len = MAXALIGN(core::mem::size_of::<*mut ScanKey>() * natts as usize)	/* regular keys */
        + MAXALIGN(core::mem::size_of::<ScanKey>() * (*scan).numberOfKeys as usize) * natts as usize
        + MAXALIGN(core::mem::size_of::<c_int>() * natts as usize)
        + MAXALIGN(core::mem::size_of::<*mut ScanKey>() * natts as usize)	/* NULL keys */
        + MAXALIGN(core::mem::size_of::<ScanKey>() * (*scan).numberOfKeys as usize) * natts as usize
        + MAXALIGN(core::mem::size_of::<c_int>() * natts as usize);

    ptr = palloc(len) as *mut c_char;
    let tmp: *mut c_char = ptr;

    keys = ptr as *mut *mut ScanKey;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<*mut ScanKey>() * natts as usize));

    nullkeys = ptr as *mut *mut ScanKey;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<*mut ScanKey>() * natts as usize));

    nkeys = ptr as *mut c_int;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<c_int>() * natts as usize));

    nnullkeys = ptr as *mut c_int;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<c_int>() * natts as usize));

    for i in 0..natts {
        *keys.add(i as usize) = ptr as *mut ScanKey;
        ptr = ptr.add(MAXALIGN(core::mem::size_of::<ScanKey>() * (*scan).numberOfKeys as usize));

        *nullkeys.add(i as usize) = ptr as *mut ScanKey;
        ptr = ptr.add(MAXALIGN(core::mem::size_of::<ScanKey>() * (*scan).numberOfKeys as usize));
    }

    Assert!(tmp.add(len) == ptr);

    /* zero the number of keys */
    core::ptr::write_bytes(nkeys, 0, natts as usize);
    core::ptr::write_bytes(nnullkeys, 0, natts as usize);

    /* Preprocess the scan keys - split them into per-attribute arrays. */
    for keyno in 0..(*scan).numberOfKeys {
        let key: ScanKey = &mut *(*scan).keyData.add(keyno as usize);
        let keyattno: AttrNumber = (*key).sk_attno;

        /*
         * The collation of the scan key must match the collation used in the
         * index column (but only if the search is not IS NULL/ IS NOT NULL).
         * Otherwise we shouldn't be using this index ...
         */
        Assert!(
            ((*key).sk_flags & SK_ISNULL) != 0
                || ((*key).sk_collation
                    == (*TupleDescAttr((*bdesc).bd_tupdesc, (keyattno - 1) as c_int)).attcollation)
        );

        /*
         * First time we see this index attribute, so init as needed.
         */
        if (*consistentFn.add((keyattno - 1) as usize)).fn_oid == InvalidOid {
            let tmp_fn: *mut FmgrInfo;

            /* First time we see this attribute, so no key/null keys. */
            Assert!(*nkeys.add((keyattno - 1) as usize) == 0);
            Assert!(*nnullkeys.add((keyattno - 1) as usize) == 0);

            tmp_fn = index_getprocinfo(idxRel, keyattno, BRIN_PROCNUM_CONSISTENT as uint16);
            fmgr_info_copy(
                consistentFn.add((keyattno - 1) as usize),
                tmp_fn,
                CurrentMemoryContext,
            );
        }

        /* Add key to the proper per-attribute array. */
        if ((*key).sk_flags & SK_ISNULL) != 0 {
            let arr = *nullkeys.add((keyattno - 1) as usize);
            *arr.add(*nnullkeys.add((keyattno - 1) as usize) as usize) = key;
            *nnullkeys.add((keyattno - 1) as usize) += 1;
        } else {
            let arr = *keys.add((keyattno - 1) as usize);
            *arr.add(*nkeys.add((keyattno - 1) as usize) as usize) = key;
            *nkeys.add((keyattno - 1) as usize) += 1;
        }
    }

    /* allocate an initial in-memory tuple, out of the per-range memcxt */
    dtup = brin_new_memtuple(bdesc);

    /*
     * Setup and use a per-range memory context, which is reset every time we
     * loop below.  This avoids having to free the tuples within the loop.
     */
    perRangeCxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"bringetbitmap cxt".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    oldcxt = MemoryContextSwitchTo(perRangeCxt);

    /*
     * Now scan the revmap.  We start by querying for heap page 0,
     * incrementing by the number of pages per range; this gives us a full
     * view of the table.  We make use of uint64 for heapBlk as a BlockNumber
     * could wrap for tables with close to 2^32 pages.
     */
    let mut heapBlk: u64 = 0;
    while heapBlk < nblocks as u64 {
        let mut addrange: bool;
        let mut gottuple: bool = false;
        let tup: *mut BrinTuple;
        let mut off: OffsetNumber = 0;
        let mut size: Size = 0;

        CHECK_FOR_INTERRUPTS!();

        MemoryContextReset(perRangeCxt);

        tup = brinGetTupleForHeapBlock(
            (*opaque).bo_rmAccess,
            heapBlk as BlockNumber,
            &mut buf,
            &mut off,
            &mut size,
            BUFFER_LOCK_SHARE,
        );
        if !tup.is_null() {
            gottuple = true;
            btup = brin_copy_tuple(tup, size, btup, &mut btupsz);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        }

        /*
         * For page ranges with no indexed tuple, we must return the whole
         * range; otherwise, compare it to the scan keys.
         */
        if !gottuple {
            addrange = true;
        } else {
            dtup = brin_deform_tuple(bdesc, btup, dtup);
            if (*dtup).bt_placeholder {
                /*
                 * Placeholder tuples are always returned, regardless of the
                 * values stored in them.
                 */
                addrange = true;
            } else {
                /*
                 * Compare scan keys with summary values stored for the range.
                 * If scan keys are matched, the page range must be added to
                 * the bitmap.  We initially assume the range needs to be
                 * added; in particular this serves the case where there are
                 * no keys.
                 */
                addrange = true;
                let mut attno: c_int = 1;
                while attno <= natts {
                    let bval: *mut BrinValues;
                    let mut add: Datum;
                    let collation: Oid;

                    /*
                     * skip attributes without any scan keys (both regular and
                     * IS [NOT] NULL)
                     */
                    if *nkeys.add((attno - 1) as usize) == 0
                        && *nnullkeys.add((attno - 1) as usize) == 0
                    {
                        attno += 1;
                        continue;
                    }

                    bval = (*dtup).bt_columns.as_mut_ptr().add((attno - 1) as usize);

                    /*
                     * If the BRIN tuple indicates that this range is empty,
                     * we can skip it: there's nothing to match.  We don't
                     * need to examine the next columns.
                     */
                    if (*dtup).bt_empty_range {
                        addrange = false;
                        break;
                    }

                    /*
                     * First check if there are any IS [NOT] NULL scan keys,
                     * and if we're violating them. In that case we can
                     * terminate early, without invoking the support function.
                     */
                    if (*(*bdesc).bd_info[(attno - 1) as usize]).oi_regular_nulls
                        && !check_null_keys(
                            bval,
                            *nullkeys.add((attno - 1) as usize),
                            *nnullkeys.add((attno - 1) as usize),
                        )
                    {
                        /*
                         * If any of the IS [NOT] NULL keys failed, the page
                         * range as a whole can't pass. So terminate the loop.
                         */
                        addrange = false;
                        break;
                    }

                    /*
                     * So either there are no IS [NOT] NULL keys, or all
                     * passed. If there are no regular scan keys, we're done -
                     * the page range matches. If there are regular keys, but
                     * the page range is marked as 'all nulls' it can't
                     * possibly pass (we're assuming the operators are
                     * strict).
                     */

                    /* No regular scan keys - page range as a whole passes. */
                    if *nkeys.add((attno - 1) as usize) == 0 {
                        attno += 1;
                        continue;
                    }

                    Assert!(
                        (*nkeys.add((attno - 1) as usize) > 0)
                            && (*nkeys.add((attno - 1) as usize) <= (*scan).numberOfKeys)
                    );

                    /* If it is all nulls, it cannot possibly be consistent. */
                    if (*bval).bv_allnulls {
                        addrange = false;
                        break;
                    }

                    /*
                     * Collation from the first key (has to be the same for
                     * all keys for the same attribute).
                     */
                    collation = (*(*(*keys.add((attno - 1) as usize)).add(0))).sk_collation;

                    /*
                     * Check whether the scan key is consistent with the page
                     * range values; if so, have the pages in the range added
                     * to the output bitmap.
                     *
                     * The opclass may or may not support processing of
                     * multiple scan keys. We can determine that based on the
                     * number of arguments - functions with extra parameter
                     * (number of scan keys) do support this, otherwise we
                     * have to simply pass the scan keys one by one.
                     */
                    if (*consistentFn.add((attno - 1) as usize)).fn_nargs >= 4 {
                        /* Check all keys at once */
                        add = FunctionCall4Coll(
                            consistentFn.add((attno - 1) as usize),
                            collation,
                            PointerGetDatum(bdesc as *const c_void),
                            PointerGetDatum(bval as *const c_void),
                            PointerGetDatum(*keys.add((attno - 1) as usize) as *const c_void),
                            Int32GetDatum(*nkeys.add((attno - 1) as usize)),
                        );
                        addrange = DatumGetBool(add);
                    } else {
                        /*
                         * Check keys one by one
                         *
                         * When there are multiple scan keys, failure to meet
                         * the criteria for a single one of them is enough to
                         * discard the range as a whole, so break out of the
                         * loop as soon as a false return value is obtained.
                         */
                        let mut keyno: c_int = 0;
                        while keyno < *nkeys.add((attno - 1) as usize) {
                            add = FunctionCall3Coll(
                                consistentFn.add((attno - 1) as usize),
                                (*(*(*keys.add((attno - 1) as usize)).add(keyno as usize)))
                                    .sk_collation,
                                PointerGetDatum(bdesc as *const c_void),
                                PointerGetDatum(bval as *const c_void),
                                PointerGetDatum(
                                    *(*keys.add((attno - 1) as usize)).add(keyno as usize)
                                        as *const c_void,
                                ),
                            );
                            addrange = DatumGetBool(add);
                            if !addrange {
                                break;
                            }
                            keyno += 1;
                        }
                    }

                    /*
                     * If we found a scan key eliminating the range, no need
                     * to check additional ones.
                     */
                    if !addrange {
                        break;
                    }

                    attno += 1;
                }
            }
        }

        /* add the pages in the range to the output bitmap, if needed */
        if addrange {
            let mut pageno: u64 = heapBlk;
            while pageno
                <= Min(nblocks as u64, heapBlk + (*opaque).bo_pagesPerRange as u64) - 1
            {
                MemoryContextSwitchTo(oldcxt);
                tbm_add_page(tbm, pageno as BlockNumber);
                totalpages += 1;
                MemoryContextSwitchTo(perRangeCxt);
                pageno += 1;
            }
        }

        heapBlk += (*opaque).bo_pagesPerRange as u64;
    }

    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(perRangeCxt);

    if buf != InvalidBuffer {
        ReleaseBuffer(buf);
    }

    /*
     * XXX We have an approximation of the number of *pages* that our scan
     * returns, but we don't have a precise idea of the number of heap tuples
     * involved.
     */
    totalpages * 10
}

/*
 * Re-initialize state for a BRIN index scan
 */
pub unsafe fn brinrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    _nscankeys: c_int,
    _orderbys: ScanKey,
    _norderbys: c_int,
) {
    /*
     * Other index AMs preprocess the scan keys at this point, or sometime
     * early during the scan; this lets them optimize by removing redundant
     * keys, or doing early returns when they are impossible to satisfy; see
     * _bt_preprocess_keys for an example.  Something like that could be added
     * here someday, too.
     */

    if !scankey.is_null() && (*scan).numberOfKeys > 0 {
        core::ptr::copy_nonoverlapping(
            scankey,
            (*scan).keyData,
            (*scan).numberOfKeys as usize,
        );
    }
}

/*
 * Close down a BRIN index scan
 */
pub unsafe fn brinendscan(scan: IndexScanDesc) {
    let opaque: *mut BrinOpaque = (*scan).opaque as *mut BrinOpaque;

    brinRevmapTerminate((*opaque).bo_rmAccess);
    brin_free_desc((*opaque).bo_bdesc);
    pfree(opaque as *mut c_void);
}

/*
 * Per-heap-tuple callback for table_index_build_scan.
 *
 * Note we don't worry about the page range at the end of the table here; it is
 * present in the build state struct after we're called the last time, but not
 * inserted into the index.  Caller must ensure to do so, if appropriate.
 */
unsafe extern "C" fn brinbuildCallback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    _tupleIsAlive: bool,
    brstate: *mut c_void,
) {
    let state: *mut BrinBuildState = brstate as *mut BrinBuildState;
    let thisblock: BlockNumber;

    thisblock = ItemPointerGetBlockNumber(tid);

    /*
     * If we're in a block that belongs to a future range, summarize what
     * we've got and start afresh.  Note the scan might have skipped many
     * pages, if they were devoid of live tuples; make sure to insert index
     * tuples for those too.
     */
    while thisblock > (*state).bs_currRangeStart + (*state).bs_pagesPerRange - 1 {
        /* BRIN_elog((DEBUG2, "brinbuildCallback: completed a range: %u--%u", ...)) */
        BRIN_elog();

        /* create the index tuple and insert it */
        form_and_insert_tuple(state);

        /* set state to correspond to the next range */
        (*state).bs_currRangeStart += (*state).bs_pagesPerRange;

        /* re-initialize state for it */
        brin_memtuple_initialize((*state).bs_dtuple, (*state).bs_bdesc);
    }

    /* Accumulate the current tuple into the running state */
    add_values_to_range(index, (*state).bs_bdesc, (*state).bs_dtuple, values, isnull);
}

/*
 * Per-heap-tuple callback for table_index_build_scan with parallelism.
 *
 * A version of the callback used by parallel index builds. The main difference
 * is that instead of writing the BRIN tuples into the index, we write them
 * into a shared tuplesort, and leave the insertion up to the leader (which may
 * reorder them a bit etc.). The callback also does not generate empty ranges,
 * those will be added by the leader when merging results from workers.
 */
unsafe extern "C" fn brinbuildCallbackParallel(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    _tupleIsAlive: bool,
    brstate: *mut c_void,
) {
    let state: *mut BrinBuildState = brstate as *mut BrinBuildState;
    let thisblock: BlockNumber;

    thisblock = ItemPointerGetBlockNumber(tid);

    /*
     * If we're in a block that belongs to a different range, summarize what
     * we've got and start afresh.  Note the scan might have skipped many
     * pages, if they were devoid of live tuples; we do not create empty BRIN
     * ranges here - the leader is responsible for filling them in.
     *
     * Unlike serial builds, parallel index builds allow synchronized seqscans
     * (because that's what parallel scans do). This means the block may wrap
     * around to the beginning of the relation, so the condition needs to
     * check for both future and past ranges.
     */
    if (thisblock < (*state).bs_currRangeStart)
        || (thisblock > (*state).bs_currRangeStart + (*state).bs_pagesPerRange - 1)
    {
        /* BRIN_elog((DEBUG2, "brinbuildCallbackParallel: completed a range: %u--%u", ...)) */
        BRIN_elog();

        /* create the index tuple and write it into the tuplesort */
        form_and_spill_tuple(state);

        /*
         * Set state to correspond to the next range (for this block).
         *
         * This skips ranges that are either empty (and so we don't get any
         * tuples to summarize), or processed by other workers. We can't
         * differentiate those cases here easily, so we leave it up to the
         * leader to fill empty ranges where needed.
         */
        (*state).bs_currRangeStart =
            (*state).bs_pagesPerRange * (thisblock / (*state).bs_pagesPerRange);

        /* re-initialize state for it */
        brin_memtuple_initialize((*state).bs_dtuple, (*state).bs_bdesc);
    }

    /* Accumulate the current tuple into the running state */
    add_values_to_range(index, (*state).bs_bdesc, (*state).bs_dtuple, values, isnull);
}

/*
 * brinbuild() -- build a new BRIN index.
 */
pub unsafe fn brinbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    let result: *mut IndexBuildResult;
    let reltuples: f64;
    let idxtuples: f64;
    let revmap: *mut BrinRevmap;
    let state: *mut BrinBuildState;
    let meta: Buffer;
    let mut pagesPerRange: BlockNumber = 0;

    /*
     * We expect to be called exactly once for any index relation.
     */
    if RelationGetNumberOfBlocks(index) != 0 {
        elog!(
            ERROR,
            "index \"{}\" already contains data",
            CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
        );
    }

    /*
     * Critical section not required, because on error the creation of the
     * whole relation will be rolled back.
     */

    meta = ExtendBufferedRel(
        BMR_REL(index as crate::storage::buffer::bufmgr::Relation),
        MAIN_FORKNUM,
        null_mut(),
        EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK,
    );
    Assert!(BufferGetBlockNumber(meta) == BRIN_METAPAGE_BLKNO);

    brin_metapage_init(
        BufferGetPage(meta),
        BrinGetPagesPerRange!(index),
        BRIN_CURRENT_VERSION as uint16,
    );
    MarkBufferDirty(meta);

    if RelationNeedsWAL(index) {
        let mut xlrec: xl_brin_createidx = core::mem::zeroed();
        let recptr: XLogRecPtr;
        let page: Page;

        xlrec.version = BRIN_CURRENT_VERSION as uint16;
        xlrec.pagesPerRange = BrinGetPagesPerRange!(index);

        XLogBeginInsert();
        XLogRegisterData(
            &mut xlrec as *mut _ as *const c_void,
            SizeOfBrinCreateIdx as u32,
        );
        XLogRegisterBuffer(0, meta, REGBUF_WILL_INIT | REGBUF_STANDARD);

        recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_CREATE_INDEX);

        page = BufferGetPage(meta);
        PageSetLSN(page, recptr);
    }

    UnlockReleaseBuffer(meta);

    /*
     * Initialize our state, including the deformed tuple state.
     */
    revmap = brinRevmapInitialize(index, &mut pagesPerRange);
    state = initialize_brin_buildstate(
        index,
        revmap,
        pagesPerRange,
        RelationGetNumberOfBlocks(heap),
    );

    /*
     * Attempt to launch parallel worker scan when required
     */
    if (*indexInfo).ii_ParallelWorkers > 0 {
        _brin_begin_parallel(
            state,
            heap,
            index,
            (*indexInfo).ii_Concurrent,
            (*indexInfo).ii_ParallelWorkers,
        );
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
        (*coordinate).nParticipants = (*(*state).bs_leader).nparticipanttuplesorts;
        (*coordinate).sharedsort = (*(*state).bs_leader).sharedsort;

        /*
         * Begin leader tuplesort.
         */
        (*state).bs_sortstate =
            tuplesort_begin_index_brin(maintenance_work_mem, coordinate, TUPLESORT_NONE);

        /* scan the relation and merge per-worker results */
        reltuples = _brin_parallel_merge(state);

        _brin_end_parallel((*state).bs_leader, state);
    } else {
        /* no parallel index build */

        /*
         * Now scan the relation.  No syncscan allowed here because we want
         * the heap blocks in physical order (we want to produce the ranges
         * starting from block 0, and the callback also relies on this to not
         * generate summary for the same range twice).
         */
        reltuples = table_index_build_scan(
            heap,
            index,
            indexInfo,
            false,
            true,
            Some(brinbuildCallback),
            state as *mut c_void,
            null_mut(),
        );

        /*
         * process the final batch
         *
         * XXX Note this does not update state->bs_currRangeStart, i.e. it
         * stays set to the last range added to the index. This is OK, because
         * that's what brin_fill_empty_ranges expects.
         */
        form_and_insert_tuple(state);

        /*
         * Backfill the final ranges with empty data.
         */
        brin_fill_empty_ranges(state, (*state).bs_currRangeStart, (*state).bs_maxRangeStart);
    }

    /* release resources */
    idxtuples = (*state).bs_numtuples;
    brinRevmapTerminate((*state).bs_rmAccess);
    terminate_brin_buildstate(state);

    /*
     * Return statistics
     */
    result = palloc(core::mem::size_of::<IndexBuildResult>()) as *mut IndexBuildResult;

    (*result).heap_tuples = reltuples;
    (*result).index_tuples = idxtuples;

    result
}

pub unsafe fn brinbuildempty(index: Relation) {
    let metabuf: Buffer;

    /* An empty BRIN index has a metapage only. */
    metabuf = ExtendBufferedRel(
        BMR_REL(index as crate::storage::buffer::bufmgr::Relation),
        INIT_FORKNUM,
        null_mut(),
        EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK,
    );

    /* Initialize and xlog metabuffer. */
    START_CRIT_SECTION!();
    brin_metapage_init(
        BufferGetPage(metabuf),
        BrinGetPagesPerRange!(index),
        BRIN_CURRENT_VERSION as uint16,
    );
    MarkBufferDirty(metabuf);
    log_newpage_buffer(metabuf, true);
    END_CRIT_SECTION!();

    UnlockReleaseBuffer(metabuf);
}

/*
 * brinbulkdelete
 *		Since there are no per-heap-tuple index tuples in BRIN indexes,
 *		there's not a lot we can do here.
 */
pub unsafe fn brinbulkdelete(
    _info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    _callback: IndexBulkDeleteCallback,
    _callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let mut stats = stats;
    /* allocate stats if first time through, else re-use existing struct */
    if stats.is_null() {
        stats = palloc0(core::mem::size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
    }

    stats
}

/*
 * This routine is in charge of "vacuuming" a BRIN index: we just summarize
 * ranges that are currently unsummarized.
 */
pub unsafe fn brinvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let heapRel: Relation;

    /* No-op in ANALYZE ONLY mode */
    if (*info).analyze_only {
        return stats;
    }

    let mut stats = stats;
    if stats.is_null() {
        stats = palloc0(core::mem::size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
    }
    (*stats).num_pages = RelationGetNumberOfBlocks((*info).index);
    /* rest of stats is initialized by zeroing */

    heapRel = table_open(
        IndexGetRelation(RelationGetRelid((*info).index), false),
        AccessShareLock,
    );

    brin_vacuum_scan((*info).index, (*info).strategy as _);

    brinsummarize(
        (*info).index,
        heapRel,
        BRIN_ALL_BLOCKRANGES,
        false,
        &mut (*stats).num_index_tuples,
        &mut (*stats).num_index_tuples,
    );

    table_close(heapRel, AccessShareLock);

    stats
}

/*
 * reloptions processor for BRIN indexes
 */
pub unsafe fn brinoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    let tab: [relopt_parse_elt; 2] = [
        relopt_parse_elt {
            optname: c"pages_per_range".as_ptr(),
            opttype: RELOPT_TYPE_INT,
            offset: core::mem::offset_of!(BrinOptions, pagesPerRange) as c_int,
        },
        relopt_parse_elt {
            optname: c"autosummarize".as_ptr(),
            opttype: RELOPT_TYPE_BOOL,
            offset: core::mem::offset_of!(BrinOptions, autosummarize) as c_int,
        },
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_BRIN,
        core::mem::size_of::<BrinOptions>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 * SQL-callable function to scan through an index and summarize all ranges
 * that are not currently summarized.
 */
pub unsafe fn brin_summarize_new_values(fcinfo: FunctionCallInfo) -> Datum {
    let relation: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    DirectFunctionCall2!(
        brin_summarize_range as PGFunction,
        relation,
        Int64GetDatum(BRIN_ALL_BLOCKRANGES as int64)
    )
}

/*
 * SQL-callable function to summarize the indicated page range, if not already
 * summarized.  If the second argument is BRIN_ALL_BLOCKRANGES, all
 * unsummarized ranges are summarized.
 */
pub unsafe fn brin_summarize_range(fcinfo: FunctionCallInfo) -> Datum {
    let indexoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let heapBlk64: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let heapBlk: BlockNumber;
    let heapoid: Oid;
    let indexRel: Relation;
    let heapRel: Relation;
    let mut save_userid: Oid = InvalidOid;
    let mut save_sec_context: c_int = -1;
    let mut save_nestlevel: c_int = -1;
    let mut numSummarized: f64 = 0.0;

    if RecoveryInProgress() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        //         errhint("BRIN control functions cannot be executed during recovery.")
        ereport!(ERROR, errmsg!("recovery is in progress"));
    }

    if heapBlk64 > BRIN_ALL_BLOCKRANGES as int64 || heapBlk64 < 0 {
        // C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        ereport!(
            ERROR,
            errmsg!("block number out of range: {}", heapBlk64)
        );
    }
    heapBlk = heapBlk64 as BlockNumber;

    /*
     * We must lock table before index to avoid deadlocks.  However, if the
     * passed indexoid isn't an index then IndexGetRelation() will fail.
     * Rather than emitting a not-very-helpful error message, postpone
     * complaining, expecting that the is-it-an-index test below will fail.
     */
    heapoid = IndexGetRelation(indexoid, true);
    if OidIsValid(heapoid) {
        heapRel = table_open(heapoid, ShareUpdateExclusiveLock);

        /*
         * Autovacuum calls us.  For its benefit, switch to the table owner's
         * userid, so that any index functions are run as that user.  Also
         * lock down security-restricted operations and arrange to make GUC
         * variable changes local to this command.  This is harmless, albeit
         * unnecessary, when called from SQL, because we fail shortly if the
         * user does not own the index.
         */
        GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
        SetUserIdAndSecContext(
            (*(*heapRel).rd_rel).relowner,
            save_sec_context | SECURITY_RESTRICTED_OPERATION,
        );
        save_nestlevel = NewGUCNestLevel();
        RestrictSearchPath();
    } else {
        heapRel = null_mut();
        /* Set these just to suppress "uninitialized variable" warnings */
        save_userid = InvalidOid;
        save_sec_context = -1;
        save_nestlevel = -1;
    }

    indexRel = index_open(indexoid, ShareUpdateExclusiveLock);

    /* Must be a BRIN index */
    if (*(*indexRel).rd_rel).relkind != RELKIND_INDEX
        || (*(*indexRel).rd_rel).relam != BRIN_AM_OID
    {
        // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a BRIN index",
                CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            )
        );
    }

    /* User must own the index (comparable to privileges needed for VACUUM) */
    if !heapRel.is_null() && !object_ownercheck(RelationRelationId, indexoid, save_userid) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_INDEX,
            RelationGetRelationName(indexRel),
        );
    }

    /*
     * Since we did the IndexGetRelation call above without any lock, it's
     * barely possible that a race against an index drop/recreation could have
     * netted us the wrong table.  Recheck.
     */
    if heapRel.is_null() || heapoid != IndexGetRelation(indexoid, false) {
        // C also: errcode(ERRCODE_UNDEFINED_TABLE)
        ereport!(
            ERROR,
            errmsg!(
                "could not open parent table of index \"{}\"",
                CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            )
        );
    }

    /* see gin_clean_pending_list() */
    if (*(*indexRel).rd_index).indisvalid {
        brinsummarize(indexRel, heapRel, heapBlk, true, &mut numSummarized, null_mut());
    } else {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            DEBUG1,
            errmsg!(
                "index \"{}\" is not valid",
                CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            )
        );
    }

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    relation_close(indexRel, ShareUpdateExclusiveLock);
    relation_close(heapRel, ShareUpdateExclusiveLock);

    PG_RETURN_INT32!(numSummarized as int32)
}

/*
 * SQL-callable interface to mark a range as no longer summarized
 */
pub unsafe fn brin_desummarize_range(fcinfo: FunctionCallInfo) -> Datum {
    let indexoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let heapBlk64: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let heapBlk: BlockNumber;
    let heapoid: Oid;
    let heapRel: Relation;
    let indexRel: Relation;
    let mut done: bool;

    if RecoveryInProgress() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        //         errhint("BRIN control functions cannot be executed during recovery.")
        ereport!(ERROR, errmsg!("recovery is in progress"));
    }

    if heapBlk64 > MaxBlockNumber as int64 || heapBlk64 < 0 {
        // C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        ereport!(
            ERROR,
            errmsg!("block number out of range: {}", heapBlk64)
        );
    }
    heapBlk = heapBlk64 as BlockNumber;

    /*
     * We must lock table before index to avoid deadlocks.  However, if the
     * passed indexoid isn't an index then IndexGetRelation() will fail.
     * Rather than emitting a not-very-helpful error message, postpone
     * complaining, expecting that the is-it-an-index test below will fail.
     *
     * Unlike brin_summarize_range(), autovacuum never calls this.  Hence, we
     * don't switch userid.
     */
    heapoid = IndexGetRelation(indexoid, true);
    if OidIsValid(heapoid) {
        heapRel = table_open(heapoid, ShareUpdateExclusiveLock);
    } else {
        heapRel = null_mut();
    }

    indexRel = index_open(indexoid, ShareUpdateExclusiveLock);

    /* Must be a BRIN index */
    if (*(*indexRel).rd_rel).relkind != RELKIND_INDEX
        || (*(*indexRel).rd_rel).relam != BRIN_AM_OID
    {
        // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a BRIN index",
                CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            )
        );
    }

    /* User must own the index (comparable to privileges needed for VACUUM) */
    if !object_ownercheck(RelationRelationId, indexoid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_INDEX,
            RelationGetRelationName(indexRel),
        );
    }

    /*
     * Since we did the IndexGetRelation call above without any lock, it's
     * barely possible that a race against an index drop/recreation could have
     * netted us the wrong table.  Recheck.
     */
    if heapRel.is_null() || heapoid != IndexGetRelation(indexoid, false) {
        // C also: errcode(ERRCODE_UNDEFINED_TABLE)
        ereport!(
            ERROR,
            errmsg!(
                "could not open parent table of index \"{}\"",
                CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            )
        );
    }

    /* see gin_clean_pending_list() */
    if (*(*indexRel).rd_index).indisvalid {
        /* the revmap does the hard work */
        loop {
            done = brinRevmapDesummarizeRange(indexRel, heapBlk);
            if done {
                break;
            }
        }
    } else {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            DEBUG1,
            errmsg!(
                "index \"{}\" is not valid",
                CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            )
        );
    }

    relation_close(indexRel, ShareUpdateExclusiveLock);
    relation_close(heapRel, ShareUpdateExclusiveLock);

    PG_RETURN_VOID!()
}

/*
 * Build a BrinDesc used to create or scan a BRIN index
 */
pub unsafe fn brin_build_desc(rel: Relation) -> *mut BrinDesc {
    let opcinfo: *mut *mut BrinOpcInfo;
    let bdesc: *mut BrinDesc;
    let tupdesc: TupleDesc;
    let mut totalstored: c_int = 0;
    let mut keyno: c_int;
    let totalsize: c_long;
    let cxt: MemoryContext;
    let oldcxt: MemoryContext;

    cxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"brin desc cxt".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(cxt);
    tupdesc = RelationGetDescr(rel);

    /*
     * Obtain BrinOpcInfo for each indexed column.  While at it, accumulate
     * the number of columns stored, since the number is opclass-defined.
     */
    opcinfo = palloc((*tupdesc).natts as usize * core::mem::size_of::<*mut BrinOpcInfo>())
        as *mut *mut BrinOpcInfo;
    keyno = 0;
    while keyno < (*tupdesc).natts {
        let opcInfoFn: *mut FmgrInfo;
        let attr: Form_pg_attribute = TupleDescAttr(tupdesc, keyno);

        opcInfoFn = index_getprocinfo(rel, (keyno + 1) as AttrNumber, BRIN_PROCNUM_OPCINFO as uint16);

        *opcinfo.add(keyno as usize) = DatumGetPointer(FunctionCall1!(
            opcInfoFn,
            ObjectIdGetDatum((*attr).atttypid)
        )) as *mut BrinOpcInfo;
        totalstored += (**opcinfo.add(keyno as usize)).oi_nstored as c_int;

        keyno += 1;
    }

    /* Allocate our result struct and fill it in */
    totalsize = (core::mem::offset_of!(BrinDesc, bd_info)
        + core::mem::size_of::<*mut BrinOpcInfo>() * (*tupdesc).natts as usize)
        as c_long;

    bdesc = palloc(totalsize as usize) as *mut BrinDesc;
    (*bdesc).bd_context = cxt;
    (*bdesc).bd_index = rel;
    (*bdesc).bd_tupdesc = tupdesc;
    (*bdesc).bd_disktdesc = null_mut(); /* generated lazily */
    (*bdesc).bd_totalstored = totalstored;

    keyno = 0;
    while keyno < (*tupdesc).natts {
        *(*bdesc).bd_info.as_mut_ptr().add(keyno as usize) = *opcinfo.add(keyno as usize);
        keyno += 1;
    }
    pfree(opcinfo as *mut c_void);

    MemoryContextSwitchTo(oldcxt);

    bdesc
}

pub unsafe fn brin_free_desc(bdesc: *mut BrinDesc) {
    /* make sure the tupdesc is still valid */
    Assert!((*(*bdesc).bd_tupdesc).tdrefcount >= 1);
    /* no need for retail pfree */
    MemoryContextDelete((*bdesc).bd_context);
}

/*
 * Fetch index's statistical data into *stats
 */
pub unsafe fn brinGetStats(index: Relation, stats: *mut BrinStatsData) {
    let metabuffer: Buffer;
    let metapage: Page;
    let metadata: *mut BrinMetaPageData;

    metabuffer = ReadBuffer(
        index as crate::storage::buffer::bufmgr::Relation,
        BRIN_METAPAGE_BLKNO,
    );
    LockBuffer(metabuffer, BUFFER_LOCK_SHARE);
    metapage = BufferGetPage(metabuffer);
    metadata = PageGetContents(metapage) as *mut BrinMetaPageData;

    (*stats).pagesPerRange = (*metadata).pagesPerRange;
    (*stats).revmapNumPages = (*metadata).lastRevmapPage - 1;

    UnlockReleaseBuffer(metabuffer);
}

/*
 * Initialize a BrinBuildState appropriate to create tuples on the given index.
 */
unsafe fn initialize_brin_buildstate(
    idxRel: Relation,
    revmap: *mut BrinRevmap,
    pagesPerRange: BlockNumber,
    tablePages: BlockNumber,
) -> *mut BrinBuildState {
    let state: *mut BrinBuildState;
    let mut lastRange: BlockNumber = 0;

    state = palloc(core::mem::size_of::<BrinBuildState>()) as *mut BrinBuildState;

    (*state).bs_irel = idxRel;
    (*state).bs_numtuples = 0.0;
    (*state).bs_reltuples = 0.0;
    (*state).bs_currentInsertBuf = InvalidBuffer;
    (*state).bs_pagesPerRange = pagesPerRange;
    (*state).bs_currRangeStart = 0;
    (*state).bs_rmAccess = revmap;
    (*state).bs_bdesc = brin_build_desc(idxRel);
    (*state).bs_dtuple = brin_new_memtuple((*state).bs_bdesc);
    (*state).bs_leader = null_mut();
    (*state).bs_worker_id = 0;
    (*state).bs_sortstate = null_mut();
    (*state).bs_context = CurrentMemoryContext;
    (*state).bs_emptyTuple = null_mut();
    (*state).bs_emptyTupleLen = 0;

    /* Remember the memory context to use for an empty tuple, if needed. */
    (*state).bs_context = CurrentMemoryContext;
    (*state).bs_emptyTuple = null_mut();
    (*state).bs_emptyTupleLen = 0;

    /*
     * Calculate the start of the last page range. Page numbers are 0-based,
     * so to calculate the index we need to subtract one. The integer division
     * gives us the index of the page range.
     */
    if tablePages > 0 {
        lastRange = ((tablePages - 1) / pagesPerRange) * pagesPerRange;
    }

    /* Now calculate the start of the next range. */
    (*state).bs_maxRangeStart = lastRange + (*state).bs_pagesPerRange;

    state
}

/*
 * Release resources associated with a BrinBuildState.
 */
unsafe fn terminate_brin_buildstate(state: *mut BrinBuildState) {
    /*
     * Release the last index buffer used.  We might as well ensure that
     * whatever free space remains in that page is available in FSM, too.
     */
    if !BufferIsInvalid((*state).bs_currentInsertBuf) {
        let page: Page;
        let freespace: Size;
        let blk: BlockNumber;

        page = BufferGetPage((*state).bs_currentInsertBuf);
        freespace = PageGetFreeSpace(page);
        blk = BufferGetBlockNumber((*state).bs_currentInsertBuf);
        ReleaseBuffer((*state).bs_currentInsertBuf);
        RecordPageWithFreeSpace((*state).bs_irel, blk, freespace);
        FreeSpaceMapVacuumRange((*state).bs_irel, blk, blk + 1);
    }

    brin_free_desc((*state).bs_bdesc);
    pfree((*state).bs_dtuple as *mut c_void);
    pfree(state as *mut c_void);
}

/*
 * On the given BRIN index, summarize the heap page range that corresponds
 * to the heap block number given.
 *
 * This routine can run in parallel with insertions into the heap.  To avoid
 * missing those values from the summary tuple, we first insert a placeholder
 * index tuple into the index, then execute the heap scan; transactions
 * concurrent with the scan update the placeholder tuple.  After the scan, we
 * union the placeholder tuple with the one computed by this routine.  The
 * update of the index value happens in a loop, so that if somebody updates
 * the placeholder tuple after we read it, we detect the case and try again.
 * This ensures that the concurrently inserted tuples are not lost.
 *
 * A further corner case is this routine being asked to summarize the partial
 * range at the end of the table.  heapNumBlocks is the (possibly outdated)
 * table size; if we notice that the requested range lies beyond that size,
 * we re-compute the table size after inserting the placeholder tuple, to
 * avoid missing pages that were appended recently.
 */
unsafe fn summarize_range(
    indexInfo: *mut IndexInfo,
    state: *mut BrinBuildState,
    heapRel: Relation,
    heapBlk: BlockNumber,
    heapNumBlks: BlockNumber,
) {
    let mut phbuf: Buffer;
    let mut phtup: *mut BrinTuple;
    let mut phsz: Size = 0;
    let mut offset: OffsetNumber;
    let scanNumBlks: BlockNumber;

    /*
     * Insert the placeholder tuple
     */
    phbuf = InvalidBuffer;
    phtup = brin_form_placeholder_tuple((*state).bs_bdesc, heapBlk, &mut phsz);
    offset = brin_doinsert(
        (*state).bs_irel,
        (*state).bs_pagesPerRange,
        (*state).bs_rmAccess,
        &mut phbuf,
        heapBlk,
        phtup,
        phsz,
    );

    /*
     * Compute range end.  We hold ShareUpdateExclusive lock on table, so it
     * cannot shrink concurrently (but it can grow).
     */
    Assert!(heapBlk % (*state).bs_pagesPerRange == 0);
    if heapBlk + (*state).bs_pagesPerRange > heapNumBlks {
        /*
         * If we're asked to scan what we believe to be the final range on the
         * table (i.e. a range that might be partial) we need to recompute our
         * idea of what the latest page is after inserting the placeholder
         * tuple.  Anyone that grows the table later will update the
         * placeholder tuple, so it doesn't matter that we won't scan these
         * pages ourselves.  Careful: the table might have been extended
         * beyond the current range, so clamp our result.
         *
         * Fortunately, this should occur infrequently.
         */
        scanNumBlks = Min(
            RelationGetNumberOfBlocks(heapRel) - heapBlk,
            (*state).bs_pagesPerRange,
        );
    } else {
        /* Easy case: range is known to be complete */
        scanNumBlks = (*state).bs_pagesPerRange;
    }

    /*
     * Execute the partial heap scan covering the heap blocks in the specified
     * page range, summarizing the heap tuples in it.  This scan stops just
     * short of brinbuildCallback creating the new index entry.
     *
     * Note that it is critical we use the "any visible" mode of
     * table_index_build_range_scan here: otherwise, we would miss tuples
     * inserted by transactions that are still in progress, among other corner
     * cases.
     */
    (*state).bs_currRangeStart = heapBlk;
    table_index_build_range_scan(
        heapRel,
        (*state).bs_irel,
        indexInfo,
        false,
        true,
        false,
        heapBlk,
        scanNumBlks,
        Some(brinbuildCallback),
        state as *mut c_void,
        null_mut(),
    );

    /*
     * Now we update the values obtained by the scan with the placeholder
     * tuple.  We do this in a loop which only terminates if we're able to
     * update the placeholder tuple successfully; if we are not, this means
     * somebody else modified the placeholder tuple after we read it.
     */
    loop {
        let newtup: *mut BrinTuple;
        let mut newsize: Size = 0;
        let didupdate: bool;
        let samepage: bool;

        CHECK_FOR_INTERRUPTS!();

        /*
         * Update the summary tuple and try to update.
         */
        newtup = brin_form_tuple((*state).bs_bdesc, heapBlk, (*state).bs_dtuple, &mut newsize);
        samepage = brin_can_do_samepage_update(phbuf, phsz, newsize);
        didupdate = brin_doupdate(
            (*state).bs_irel,
            (*state).bs_pagesPerRange,
            (*state).bs_rmAccess,
            heapBlk,
            phbuf,
            offset,
            phtup,
            phsz,
            newtup,
            newsize,
            samepage,
        );
        brin_free_tuple(phtup);
        brin_free_tuple(newtup);

        /* If the update succeeded, we're done. */
        if didupdate {
            break;
        }

        /*
         * If the update didn't work, it might be because somebody updated the
         * placeholder tuple concurrently.  Extract the new version, union it
         * with the values we have from the scan, and start over.  (There are
         * other reasons for the update to fail, but it's simple to treat them
         * the same.)
         */
        phtup = brinGetTupleForHeapBlock(
            (*state).bs_rmAccess,
            heapBlk,
            &mut phbuf,
            &mut offset,
            &mut phsz,
            BUFFER_LOCK_SHARE,
        );
        /* the placeholder tuple must exist */
        if phtup.is_null() {
            elog!(ERROR, "missing placeholder tuple");
        }
        phtup = brin_copy_tuple(phtup, phsz, null_mut(), null_mut());
        LockBuffer(phbuf, BUFFER_LOCK_UNLOCK);

        /* merge it into the tuple from the heap scan */
        union_tuples((*state).bs_bdesc, (*state).bs_dtuple, phtup);
    }

    ReleaseBuffer(phbuf);
}

/*
 * Summarize page ranges that are not already summarized.  If pageRange is
 * BRIN_ALL_BLOCKRANGES then the whole table is scanned; otherwise, only the
 * page range containing the given heap page number is scanned.
 * If include_partial is true, then the partial range at the end of the table
 * is summarized, otherwise not.
 *
 * For each new index tuple inserted, *numSummarized (if not NULL) is
 * incremented; for each existing tuple, *numExisting (if not NULL) is
 * incremented.
 */
unsafe fn brinsummarize(
    index: Relation,
    heapRel: Relation,
    pageRange: BlockNumber,
    include_partial: bool,
    numSummarized: *mut f64,
    numExisting: *mut f64,
) {
    let revmap: *mut BrinRevmap;
    let mut state: *mut BrinBuildState = null_mut();
    let mut indexInfo: *mut IndexInfo = null_mut();
    let mut heapNumBlocks: BlockNumber;
    let mut pagesPerRange: BlockNumber = 0;
    let mut buf: Buffer;
    let mut startBlk: BlockNumber;

    revmap = brinRevmapInitialize(index, &mut pagesPerRange);

    /* determine range of pages to process */
    heapNumBlocks = RelationGetNumberOfBlocks(heapRel);
    if pageRange == BRIN_ALL_BLOCKRANGES {
        startBlk = 0;
    } else {
        startBlk = (pageRange / pagesPerRange) * pagesPerRange;
        heapNumBlocks = Min(heapNumBlocks, startBlk + pagesPerRange);
    }
    if startBlk > heapNumBlocks {
        /* Nothing to do if start point is beyond end of table */
        brinRevmapTerminate(revmap);
        return;
    }

    /*
     * Scan the revmap to find unsummarized items.
     */
    buf = InvalidBuffer;
    while startBlk < heapNumBlocks {
        let tup: *mut BrinTuple;
        let mut off: OffsetNumber = 0;

        /*
         * Unless requested to summarize even a partial range, go away now if
         * we think the next range is partial.  Caller would pass true when it
         * is typically run once bulk data loading is done
         * (brin_summarize_new_values), and false when it is typically the
         * result of arbitrarily-scheduled maintenance command (vacuuming).
         */
        if !include_partial && (startBlk + pagesPerRange > heapNumBlocks) {
            break;
        }

        CHECK_FOR_INTERRUPTS!();

        tup = brinGetTupleForHeapBlock(
            revmap,
            startBlk,
            &mut buf,
            &mut off,
            null_mut(),
            BUFFER_LOCK_SHARE,
        );
        if tup.is_null() {
            /* no revmap entry for this heap range. Summarize it. */
            if state.is_null() {
                /* first time through */
                Assert!(indexInfo.is_null());
                state = initialize_brin_buildstate(
                    index,
                    revmap,
                    pagesPerRange,
                    InvalidBlockNumber,
                );
                indexInfo = BuildIndexInfo(index);
            }
            summarize_range(indexInfo, state, heapRel, startBlk, heapNumBlocks);

            /* and re-initialize state for the next range */
            brin_memtuple_initialize((*state).bs_dtuple, (*state).bs_bdesc);

            if !numSummarized.is_null() {
                *numSummarized += 1.0;
            }
        } else {
            if !numExisting.is_null() {
                *numExisting += 1.0;
            }
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        }

        startBlk += pagesPerRange;
    }

    if BufferIsValid(buf) {
        ReleaseBuffer(buf);
    }

    /* free resources */
    brinRevmapTerminate(revmap);
    if !state.is_null() {
        terminate_brin_buildstate(state);
        pfree(indexInfo as *mut c_void);
    }
}

/*
 * Given a deformed tuple in the build state, convert it into the on-disk
 * format and insert it into the index, making the revmap point to it.
 */
unsafe fn form_and_insert_tuple(state: *mut BrinBuildState) {
    let tup: *mut BrinTuple;
    let mut size: Size = 0;

    tup = brin_form_tuple(
        (*state).bs_bdesc,
        (*state).bs_currRangeStart,
        (*state).bs_dtuple,
        &mut size,
    );
    brin_doinsert(
        (*state).bs_irel,
        (*state).bs_pagesPerRange,
        (*state).bs_rmAccess,
        &mut (*state).bs_currentInsertBuf,
        (*state).bs_currRangeStart,
        tup,
        size,
    );
    (*state).bs_numtuples += 1.0;

    pfree(tup as *mut c_void);
}

/*
 * Given a deformed tuple in the build state, convert it into the on-disk
 * format and write it to a (shared) tuplesort (the leader will insert it
 * into the index later).
 */
unsafe fn form_and_spill_tuple(state: *mut BrinBuildState) {
    let tup: *mut BrinTuple;
    let mut size: Size = 0;

    /* don't insert empty tuples in parallel build */
    if (*(*state).bs_dtuple).bt_empty_range {
        return;
    }

    tup = brin_form_tuple(
        (*state).bs_bdesc,
        (*state).bs_currRangeStart,
        (*state).bs_dtuple,
        &mut size,
    );

    /* write the BRIN tuple to the tuplesort */
    tuplesort_putbrintuple((*state).bs_sortstate, tup, size);

    (*state).bs_numtuples += 1.0;

    pfree(tup as *mut c_void);
}

/*
 * Given two deformed tuples, adjust the first one so that it's consistent
 * with the summary values in both.
 */
unsafe fn union_tuples(bdesc: *mut BrinDesc, a: *mut BrinMemTuple, b: *mut BrinTuple) {
    let mut keyno: c_int;
    let db: *mut BrinMemTuple;
    let cxt: MemoryContext;
    let oldcxt: MemoryContext;

    /* Use our own memory context to avoid retail pfree */
    cxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"brin union".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    oldcxt = MemoryContextSwitchTo(cxt);
    db = brin_deform_tuple(bdesc, b, null_mut());
    MemoryContextSwitchTo(oldcxt);

    /*
     * Check if the ranges are empty.
     *
     * If at least one of them is empty, we don't need to call per-key union
     * functions at all. If "b" is empty, we just use "a" as the result (it
     * might be empty fine, but that's fine). If "a" is empty but "b" is not,
     * we use "b" as the result (but we have to copy the data into "a" first).
     *
     * Only when both ranges are non-empty, we actually do the per-key merge.
     */

    /* If "b" is empty - ignore it and just use "a" (even if it's empty etc.). */
    if (*db).bt_empty_range {
        /* skip the per-key merge */
        MemoryContextDelete(cxt);
        return;
    }

    /*
     * Now we know "b" is not empty. If "a" is empty, then "b" is the result.
     * But we need to copy the data from "b" to "a" first, because that's how
     * we pass result out.
     *
     * We have to copy all the global/per-key flags etc. too.
     */
    if (*a).bt_empty_range {
        keyno = 0;
        while keyno < (*(*bdesc).bd_tupdesc).natts {
            let mut i: c_int;
            let col_a: *mut BrinValues = (*a).bt_columns.as_mut_ptr().add(keyno as usize);
            let col_b: *mut BrinValues = (*db).bt_columns.as_mut_ptr().add(keyno as usize);
            let opcinfo: *mut BrinOpcInfo = (*bdesc).bd_info[keyno as usize];

            (*col_a).bv_allnulls = (*col_b).bv_allnulls;
            (*col_a).bv_hasnulls = (*col_b).bv_hasnulls;

            /* If "b" has no data, we're done. */
            if (*col_b).bv_allnulls {
                keyno += 1;
                continue;
            }

            i = 0;
            while i < (*opcinfo).oi_nstored as c_int {
                *(*col_a).bv_values.add(i as usize) = datumCopy(
                    *(*col_b).bv_values.add(i as usize),
                    (*(*opcinfo).oi_typcache[i as usize]).typbyval,
                    (*(*opcinfo).oi_typcache[i as usize]).typlen as c_int,
                );
                i += 1;
            }

            keyno += 1;
        }

        /* "a" started empty, but "b" was not empty, so remember that */
        (*a).bt_empty_range = false;

        /* skip the per-key merge */
        MemoryContextDelete(cxt);
        return;
    }

    /* Now we know neither range is empty. */
    keyno = 0;
    while keyno < (*(*bdesc).bd_tupdesc).natts {
        let unionFn: *mut FmgrInfo;
        let col_a: *mut BrinValues = (*a).bt_columns.as_mut_ptr().add(keyno as usize);
        let col_b: *mut BrinValues = (*db).bt_columns.as_mut_ptr().add(keyno as usize);
        let opcinfo: *mut BrinOpcInfo = (*bdesc).bd_info[keyno as usize];

        if (*opcinfo).oi_regular_nulls {
            /* Does the "b" summary represent any NULL values? */
            let b_has_nulls: bool = (*col_b).bv_hasnulls || (*col_b).bv_allnulls;

            /* Adjust "hasnulls". */
            if !(*col_a).bv_allnulls && b_has_nulls {
                (*col_a).bv_hasnulls = true;
            }

            /* If there are no values in B, there's nothing left to do. */
            if (*col_b).bv_allnulls {
                keyno += 1;
                continue;
            }

            /*
             * Adjust "allnulls".  If A doesn't have values, just copy the
             * values from B into A, and we're done.  We cannot run the
             * operators in this case, because values in A might contain
             * garbage.  Note we already established that B contains values.
             *
             * Also adjust "hasnulls" in order not to forget the summary
             * represents NULL values. This is not redundant with the earlier
             * update, because that only happens when allnulls=false.
             */
            if (*col_a).bv_allnulls {
                let mut i: c_int;

                (*col_a).bv_allnulls = false;
                (*col_a).bv_hasnulls = true;

                i = 0;
                while i < (*opcinfo).oi_nstored as c_int {
                    *(*col_a).bv_values.add(i as usize) = datumCopy(
                        *(*col_b).bv_values.add(i as usize),
                        (*(*opcinfo).oi_typcache[i as usize]).typbyval,
                        (*(*opcinfo).oi_typcache[i as usize]).typlen as c_int,
                    );
                    i += 1;
                }

                keyno += 1;
                continue;
            }
        }

        unionFn = index_getprocinfo((*bdesc).bd_index, (keyno + 1) as AttrNumber, BRIN_PROCNUM_UNION as uint16);
        FunctionCall3Coll(
            unionFn,
            *(*(*bdesc).bd_index).rd_indcollation.add(keyno as usize),
            PointerGetDatum(bdesc as *const c_void),
            PointerGetDatum(col_a as *const c_void),
            PointerGetDatum(col_b as *const c_void),
        );

        keyno += 1;
    }

    MemoryContextDelete(cxt);
}

/*
 * brin_vacuum_scan
 *		Do a complete scan of the index during VACUUM.
 *
 * This routine scans the complete index looking for uncataloged index pages,
 * i.e. those that might have been lost due to a crash after index extension
 * and such.
 */
unsafe fn brin_vacuum_scan(idxrel: Relation, strategy: BufferAccessStrategy) {
    let nblocks: BlockNumber;
    let mut blkno: BlockNumber;

    /*
     * Scan the index in physical order, and clean up any possible mess in
     * each page.
     */
    nblocks = RelationGetNumberOfBlocks(idxrel);
    blkno = 0;
    while blkno < nblocks {
        let buf: Buffer;

        CHECK_FOR_INTERRUPTS!();

        buf = ReadBufferExtended(
            idxrel as crate::storage::buffer::bufmgr::Relation,
            MAIN_FORKNUM,
            blkno,
            RBM_NORMAL,
            strategy as crate::storage::buf_internals::BufferAccessStrategy,
        );

        brin_page_cleanup(idxrel, buf);

        ReleaseBuffer(buf);

        blkno += 1;
    }

    /*
     * Update all upper pages in the index's FSM, as well.  This ensures not
     * only that we propagate leaf-page FSM updates made by brin_page_cleanup,
     * but also that any pre-existing damage or out-of-dateness is repaired.
     */
    FreeSpaceMapVacuum(idxrel);
}

unsafe fn add_values_to_range(
    idxRel: Relation,
    bdesc: *mut BrinDesc,
    dtup: *mut BrinMemTuple,
    values: *const Datum,
    nulls: *const bool,
) -> bool {
    let mut keyno: c_int;

    /* If the range starts empty, we're certainly going to modify it. */
    let mut modified: bool = (*dtup).bt_empty_range;

    /*
     * Compare the key values of the new tuple to the stored index values; our
     * deformed tuple will get updated if the new tuple doesn't fit the
     * original range (note this means we can't break out of the loop early).
     * Make a note of whether this happens, so that we know to insert the
     * modified tuple later.
     */
    keyno = 0;
    while keyno < (*(*bdesc).bd_tupdesc).natts {
        let result: Datum;
        let bval: *mut BrinValues;
        let addValue: *mut FmgrInfo;
        let has_nulls: bool;

        bval = (*dtup).bt_columns.as_mut_ptr().add(keyno as usize);

        /*
         * Does the range have actual NULL values? Either of the flags can be
         * set, but we ignore the state before adding first row.
         *
         * We have to remember this, because we'll modify the flags and we
         * need to know if the range started as empty.
         */
        has_nulls = (!(*dtup).bt_empty_range) && ((*bval).bv_hasnulls || (*bval).bv_allnulls);

        /*
         * If the value we're adding is NULL, handle it locally. Otherwise
         * call the BRIN_PROCNUM_ADDVALUE procedure.
         */
        if (*(*bdesc).bd_info[keyno as usize]).oi_regular_nulls && *nulls.add(keyno as usize) {
            /*
             * If the new value is null, we record that we saw it if it's the
             * first one; otherwise, there's nothing to do.
             */
            if !(*bval).bv_hasnulls {
                (*bval).bv_hasnulls = true;
                modified = true;
            }

            keyno += 1;
            continue;
        }

        addValue = index_getprocinfo(idxRel, (keyno + 1) as AttrNumber, BRIN_PROCNUM_ADDVALUE as uint16);
        result = FunctionCall4Coll(
            addValue,
            *(*idxRel).rd_indcollation.add(keyno as usize),
            PointerGetDatum(bdesc as *const c_void),
            PointerGetDatum(bval as *const c_void),
            *values.add(keyno as usize),
            BoolGetDatum(*nulls.add(keyno as usize)),
        );
        /* if that returned true, we need to insert the updated tuple */
        modified |= DatumGetBool(result);

        /*
         * If the range was had actual NULL values (i.e. did not start empty),
         * make sure we don't forget about the NULL values. Either the
         * allnulls flag is still set to true, or (if the opclass cleared it)
         * we need to set hasnulls=true.
         *
         * XXX This can only happen when the opclass modified the tuple, so
         * the modified flag should be set.
         */
        if has_nulls && !((*bval).bv_hasnulls || (*bval).bv_allnulls) {
            Assert!(modified);
            (*bval).bv_hasnulls = true;
        }

        keyno += 1;
    }

    /*
     * After updating summaries for all the keys, mark it as not empty.
     *
     * If we're actually changing the flag value (i.e. tuple started as
     * empty), we should have modified the tuple. So we should not see empty
     * range that was not modified.
     */
    Assert!(!(*dtup).bt_empty_range || modified);
    (*dtup).bt_empty_range = false;

    modified
}

unsafe fn check_null_keys(bval: *mut BrinValues, nullkeys: *mut ScanKey, nnullkeys: c_int) -> bool {
    let mut keyno: c_int;

    /*
     * First check if there are any IS [NOT] NULL scan keys, and if we're
     * violating them.
     */
    keyno = 0;
    while keyno < nnullkeys {
        let key: ScanKey = *nullkeys.add(keyno as usize);

        Assert!((*key).sk_attno == (*bval).bv_attno);

        /* Handle only IS NULL/IS NOT NULL tests */
        if ((*key).sk_flags & SK_ISNULL) == 0 {
            keyno += 1;
            continue;
        }

        if ((*key).sk_flags & SK_SEARCHNULL) != 0 {
            /* IS NULL scan key, but range has no NULLs */
            if !(*bval).bv_allnulls && !(*bval).bv_hasnulls {
                return false;
            }
        } else if ((*key).sk_flags & SK_SEARCHNOTNULL) != 0 {
            /*
             * For IS NOT NULL, we can only skip ranges that are known to have
             * only nulls.
             */
            if (*bval).bv_allnulls {
                return false;
            }
        } else {
            /*
             * Neither IS NULL nor IS NOT NULL was used; assume all indexable
             * operators are strict and thus return false with NULL value in
             * the scan key.
             */
            return false;
        }

        keyno += 1;
    }

    true
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
 * Sets buildstate's BrinLeader, which caller must use to shut down parallel
 * mode by passing it to _brin_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
unsafe fn _brin_begin_parallel(
    buildstate: *mut BrinBuildState,
    heap: Relation,
    index: Relation,
    isconcurrent: bool,
    request: c_int,
) {
    let pcxt: *mut ParallelContext;
    let scantuplesortstates: c_int;
    let snapshot: Snapshot;
    let estbrinshared: Size;
    let estsort: Size;
    let brinshared: *mut BrinShared;
    let sharedsort: *mut Sharedsort;
    let brinleader: *mut BrinLeader =
        palloc0(core::mem::size_of::<BrinLeader>()) as *mut BrinLeader;
    let walusage: *mut WalUsage;
    let bufferusage: *mut BufferUsage;
    let leaderparticipates: bool = true;
    let querylen: c_int;

    // #ifdef DISABLE_LEADER_PARTICIPATION not defined

    /*
     * Enter parallel mode, and create context for parallel build of brin
     * index
     */
    EnterParallelMode();
    Assert!(request > 0);
    pcxt = CreateParallelContext(
        c"postgres".as_ptr(),
        c"_brin_parallel_build_main".as_ptr(),
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
     * Estimate size for our own PARALLEL_KEY_BRIN_SHARED workspace.
     */
    estbrinshared = _brin_parallel_estimate_shared(heap, snapshot);
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, estbrinshared);
    estsort = tuplesort_estimate_shared(scantuplesortstates);
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, estsort);

    shm_toc_estimate_keys(&mut (*pcxt).estimator, 2);

    /*
     * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
     * and PARALLEL_KEY_BUFFER_USAGE.
     */
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator,
        mul_size(core::mem::size_of::<WalUsage>(), (*pcxt).nworkers as Size),
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator,
        mul_size(core::mem::size_of::<BufferUsage>(), (*pcxt).nworkers as Size),
    );
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
    brinshared = shm_toc_allocate((*pcxt).toc, estbrinshared) as *mut BrinShared;
    /* Initialize immutable state */
    (*brinshared).heaprelid = RelationGetRelid(heap);
    (*brinshared).indexrelid = RelationGetRelid(index);
    (*brinshared).isconcurrent = isconcurrent;
    (*brinshared).scantuplesortstates = scantuplesortstates;
    (*brinshared).pagesPerRange = (*buildstate).bs_pagesPerRange;
    (*brinshared).queryid = pgstat_get_my_query_id();
    ConditionVariableInit(&mut (*brinshared).workersdonecv);
    SpinLockInit(&mut (*brinshared).mutex);

    /* Initialize mutable state */
    (*brinshared).nparticipantsdone = 0;
    (*brinshared).reltuples = 0.0;
    (*brinshared).indtuples = 0.0;

    table_parallelscan_initialize(
        heap,
        ParallelTableScanFromBrinShared!(brinshared),
        snapshot,
    );

    /*
     * Store shared tuplesort-private state, for which we reserved space.
     * Then, initialize opaque state using tuplesort routine.
     */
    sharedsort = shm_toc_allocate((*pcxt).toc, estsort) as *mut Sharedsort;
    tuplesort_initialize_shared(sharedsort, scantuplesortstates, (*pcxt).seg);

    /*
     * Store shared tuplesort-private state, for which we reserved space.
     * Then, initialize opaque state using tuplesort routine.
     */
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_BRIN_SHARED, brinshared as *mut c_void);
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_TUPLESORT, sharedsort as *mut c_void);

    /* Store query string for workers */
    if !debug_query_string.is_null() {
        let sharedquery: *mut c_char;

        sharedquery = shm_toc_allocate((*pcxt).toc, (querylen + 1) as Size) as *mut c_char;
        core::ptr::copy_nonoverlapping(debug_query_string, sharedquery, (querylen + 1) as usize);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_QUERY_TEXT, sharedquery as *mut c_void);
    }

    /*
     * Allocate space for each worker's WalUsage and BufferUsage; no need to
     * initialize.
     */
    walusage = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(core::mem::size_of::<WalUsage>(), (*pcxt).nworkers as Size),
    ) as *mut WalUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_WAL_USAGE, walusage as *mut c_void);
    bufferusage = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(core::mem::size_of::<BufferUsage>(), (*pcxt).nworkers as Size),
    ) as *mut BufferUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage as *mut c_void);

    /* Launch workers, saving status for leader/caller */
    LaunchParallelWorkers(pcxt);
    (*brinleader).pcxt = pcxt;
    (*brinleader).nparticipanttuplesorts = (*pcxt).nworkers_launched;
    if leaderparticipates {
        (*brinleader).nparticipanttuplesorts += 1;
    }
    (*brinleader).brinshared = brinshared;
    (*brinleader).sharedsort = sharedsort;
    (*brinleader).snapshot = snapshot;
    (*brinleader).walusage = walusage;
    (*brinleader).bufferusage = bufferusage;

    /* If no workers were successfully launched, back out (do serial build) */
    if (*pcxt).nworkers_launched == 0 {
        _brin_end_parallel(brinleader, null_mut());
        return;
    }

    /* Save leader state now that it's clear build will be parallel */
    (*buildstate).bs_leader = brinleader;

    /* Join heap scan ourselves */
    if leaderparticipates {
        _brin_leader_participate_as_worker(buildstate, heap, index);
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
unsafe fn _brin_end_parallel(brinleader: *mut BrinLeader, _state: *mut BrinBuildState) {
    let mut i: c_int;

    /* Shutdown worker processes */
    WaitForParallelWorkersToFinish((*brinleader).pcxt);

    /*
     * Next, accumulate WAL usage.  (This must wait for the workers to finish,
     * or we might get incomplete data.)
     */
    i = 0;
    while i < (*(*brinleader).pcxt).nworkers_launched {
        InstrAccumParallelQuery(
            (*brinleader).bufferusage.add(i as usize),
            (*brinleader).walusage.add(i as usize),
        );
        i += 1;
    }

    /* Free last reference to MVCC snapshot, if one was used */
    if IsMVCCSnapshot((*brinleader).snapshot) {
        UnregisterSnapshot((*brinleader).snapshot);
    }
    DestroyParallelContext((*brinleader).pcxt);
    ExitParallelMode();
}

/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _brin_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Returns the total number of heap tuples scanned.
 */
unsafe fn _brin_parallel_heapscan(state: *mut BrinBuildState) -> f64 {
    let brinshared: *mut BrinShared = (*(*state).bs_leader).brinshared;
    let nparticipanttuplesorts: c_int;

    nparticipanttuplesorts = (*(*state).bs_leader).nparticipanttuplesorts;
    loop {
        SpinLockAcquire(&mut (*brinshared).mutex);
        if (*brinshared).nparticipantsdone == nparticipanttuplesorts {
            /* copy the data into leader state */
            (*state).bs_reltuples = (*brinshared).reltuples;
            (*state).bs_numtuples = (*brinshared).indtuples;

            SpinLockRelease(&mut (*brinshared).mutex);
            break;
        }
        SpinLockRelease(&mut (*brinshared).mutex);

        ConditionVariableSleep(
            &mut (*brinshared).workersdonecv,
            WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN,
        );
    }

    ConditionVariableCancelSleep();

    (*state).bs_reltuples
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
unsafe fn _brin_parallel_merge(state: *mut BrinBuildState) -> f64 {
    let mut btup: *mut BrinTuple;
    let mut memtuple: *mut BrinMemTuple;
    let mut tuplen: Size = 0;
    let mut prevblkno: BlockNumber = InvalidBlockNumber;
    let rangeCxt: MemoryContext;
    let oldCxt: MemoryContext;
    let reltuples: f64;

    /* wait for workers to scan table and produce partial results */
    reltuples = _brin_parallel_heapscan(state);

    /* do the actual sort in the leader */
    tuplesort_performsort((*state).bs_sortstate);

    /*
     * Initialize BrinMemTuple we'll use to union summaries from workers (in
     * case they happened to produce parts of the same page range).
     */
    memtuple = brin_new_memtuple((*state).bs_bdesc);

    /*
     * Create a memory context we'll reset to combine results for a single
     * page range (received from the workers). We don't expect huge number of
     * overlaps under regular circumstances, because for large tables the
     * chunk size is likely larger than the BRIN page range), but it can
     * happen, and the union functions may do all kinds of stuff. So we better
     * reset the context once in a while.
     */
    rangeCxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"brin union".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    oldCxt = MemoryContextSwitchTo(rangeCxt);

    /*
     * Read the BRIN tuples from the shared tuplesort, sorted by block number.
     * That probably gives us an index that is cheaper to scan, thanks to
     * mostly getting data from the same index page as before.
     */
    loop {
        btup = tuplesort_getbrintuple((*state).bs_sortstate, &mut tuplen, true);
        if btup.is_null() {
            break;
        }

        /* Ranges should be multiples of pages_per_range for the index. */
        Assert!((*btup).bt_blkno % (*(*(*state).bs_leader).brinshared).pagesPerRange == 0);

        /*
         * Do we need to union summaries for the same page range?
         *
         * If this is the first brin tuple we read, then just deform it into
         * the memtuple, and continue with the next one from tuplesort. We
         * however may need to insert empty summaries into the index.
         *
         * If it's the same block as the last we saw, we simply union the brin
         * tuple into it, and we're done - we don't even need to insert empty
         * ranges, because that was done earlier when we saw the first brin
         * tuple (for this range).
         *
         * Finally, if it's not the first brin tuple, and it's not the same
         * page range, we need to do the insert and then deform the tuple into
         * the memtuple. Then we'll insert empty ranges before the new brin
         * tuple, if needed.
         */
        if prevblkno == InvalidBlockNumber {
            /* First brin tuples, just deform into memtuple. */
            memtuple = brin_deform_tuple((*state).bs_bdesc, btup, memtuple);

            /* continue to insert empty pages before thisblock */
        } else if (*memtuple).bt_blkno == (*btup).bt_blkno {
            /*
             * Not the first brin tuple, but same page range as the previous
             * one, so we can merge it into the memtuple.
             */
            union_tuples((*state).bs_bdesc, memtuple, btup);
            continue;
        } else {
            let tmp: *mut BrinTuple;
            let mut len: Size = 0;

            /*
             * We got brin tuple for a different page range, so form a brin
             * tuple from the memtuple, insert it, and re-init the memtuple
             * from the new brin tuple.
             */
            tmp = brin_form_tuple((*state).bs_bdesc, (*memtuple).bt_blkno, memtuple, &mut len);

            brin_doinsert(
                (*state).bs_irel,
                (*state).bs_pagesPerRange,
                (*state).bs_rmAccess,
                &mut (*state).bs_currentInsertBuf,
                (*tmp).bt_blkno,
                tmp,
                len,
            );

            /*
             * Reset the per-output-range context. This frees all the memory
             * possibly allocated by the union functions, and also the BRIN
             * tuple we just formed and inserted.
             */
            MemoryContextReset(rangeCxt);

            memtuple = brin_deform_tuple((*state).bs_bdesc, btup, memtuple);

            /* continue to insert empty pages before thisblock */
        }

        /* Fill empty ranges for all ranges missing in the tuplesort. */
        brin_fill_empty_ranges(state, prevblkno, (*btup).bt_blkno);

        prevblkno = (*btup).bt_blkno;
    }

    tuplesort_end((*state).bs_sortstate);

    /* Fill the BRIN tuple for the last page range with data. */
    if prevblkno != InvalidBlockNumber {
        let tmp: *mut BrinTuple;
        let mut len: Size = 0;

        tmp = brin_form_tuple((*state).bs_bdesc, (*memtuple).bt_blkno, memtuple, &mut len);

        brin_doinsert(
            (*state).bs_irel,
            (*state).bs_pagesPerRange,
            (*state).bs_rmAccess,
            &mut (*state).bs_currentInsertBuf,
            (*tmp).bt_blkno,
            tmp,
            len,
        );

        pfree(tmp as *mut c_void);
    }

    /* Fill empty ranges at the end, for all ranges missing in the tuplesort. */
    brin_fill_empty_ranges(state, prevblkno, (*state).bs_maxRangeStart);

    /*
     * Switch back to the original memory context, and destroy the one we
     * created to isolate the union_tuple calls.
     */
    MemoryContextSwitchTo(oldCxt);
    MemoryContextDelete(rangeCxt);

    reltuples
}

/*
 * Returns size of shared memory required to store state for a parallel
 * brin index build based on the snapshot its parallel scan will use.
 */
unsafe fn _brin_parallel_estimate_shared(heap: Relation, snapshot: Snapshot) -> Size {
    /* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
    add_size(
        crate::c::BUFFERALIGN(core::mem::size_of::<BrinShared>()),
        table_parallelscan_estimate(heap, snapshot),
    )
}

/*
 * Within leader, participate as a parallel worker.
 */
unsafe fn _brin_leader_participate_as_worker(
    buildstate: *mut BrinBuildState,
    heap: Relation,
    index: Relation,
) {
    let brinleader: *mut BrinLeader = (*buildstate).bs_leader;
    let sortmem: c_int;

    /*
     * Might as well use reliable figure when doling out maintenance_work_mem
     * (when requested number of workers were not launched, this will be
     * somewhat higher than it is for other workers).
     */
    sortmem = maintenance_work_mem / (*brinleader).nparticipanttuplesorts;

    /* Perform work common to all participants */
    _brin_parallel_scan_and_build(
        buildstate,
        (*brinleader).brinshared,
        (*brinleader).sharedsort,
        heap,
        index,
        sortmem,
        true,
    );
}

/*
 * Perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for the worker portion of the table.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 */
unsafe fn _brin_parallel_scan_and_build(
    state: *mut BrinBuildState,
    brinshared: *mut BrinShared,
    sharedsort: *mut Sharedsort,
    heap: Relation,
    index: Relation,
    sortmem: c_int,
    _progress: bool,
) {
    let coordinate: SortCoordinate;
    let scan: TableScanDesc;
    let reltuples: f64;
    let indexInfo: *mut IndexInfo;

    /* Initialize local tuplesort coordination state */
    coordinate = palloc0(core::mem::size_of::<SortCoordinateData>()) as SortCoordinate;
    (*coordinate).isWorker = true;
    (*coordinate).nParticipants = -1;
    (*coordinate).sharedsort = sharedsort;

    /* Begin "partial" tuplesort */
    (*state).bs_sortstate = tuplesort_begin_index_brin(sortmem, coordinate, TUPLESORT_NONE);

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(index);
    (*indexInfo).ii_Concurrent = (*brinshared).isconcurrent;

    scan = table_beginscan_parallel(heap, ParallelTableScanFromBrinShared!(brinshared));

    reltuples = table_index_build_scan(
        heap,
        index,
        indexInfo,
        true,
        true,
        Some(brinbuildCallbackParallel),
        state as *mut c_void,
        scan,
    );

    /* insert the last item */
    form_and_spill_tuple(state);

    /* sort the BRIN ranges built by this worker */
    tuplesort_performsort((*state).bs_sortstate);

    (*state).bs_reltuples += reltuples;

    /*
     * Done.  Record ambuild statistics.
     */
    SpinLockAcquire(&mut (*brinshared).mutex);
    (*brinshared).nparticipantsdone += 1;
    (*brinshared).reltuples += (*state).bs_reltuples;
    (*brinshared).indtuples += (*state).bs_numtuples;
    SpinLockRelease(&mut (*brinshared).mutex);

    /* Notify leader */
    ConditionVariableSignal(&mut (*brinshared).workersdonecv);

    tuplesort_end((*state).bs_sortstate);
}

/*
 * Perform work within a launched parallel process.
 */
pub unsafe fn _brin_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) {
    let sharedquery: *mut c_char;
    let brinshared: *mut BrinShared;
    let sharedsort: *mut Sharedsort;
    let buildstate: *mut BrinBuildState;
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
    Assert!(
        ((*MyProc).statusFlags == 0) || ((*MyProc).statusFlags == PROC_IN_SAFE_IC)
    );

    /* Set debug_query_string for individual workers first */
    sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true) as *mut c_char;
    debug_query_string = sharedquery;

    /* Report the query string from leader */
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    /* Look up brin shared state */
    brinshared = shm_toc_lookup(toc, PARALLEL_KEY_BRIN_SHARED, false) as *mut BrinShared;

    /* Open relations using lock modes known to be obtained by index.c */
    if !(*brinshared).isconcurrent {
        heapLockmode = ShareLock;
        indexLockmode = AccessExclusiveLock;
    } else {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    /* Track query ID */
    pgstat_report_query_id((*brinshared).queryid, false);

    /* Open relations within worker */
    heapRel = table_open((*brinshared).heaprelid, heapLockmode);
    indexRel = index_open((*brinshared).indexrelid, indexLockmode);

    buildstate = initialize_brin_buildstate(
        indexRel,
        null_mut(),
        (*brinshared).pagesPerRange,
        InvalidBlockNumber,
    );

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
    sortmem = maintenance_work_mem / (*brinshared).scantuplesortstates;

    _brin_parallel_scan_and_build(
        buildstate,
        brinshared,
        sharedsort,
        heapRel,
        indexRel,
        sortmem,
        false,
    );

    /* Report WAL/buffer usage during parallel execution */
    bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false) as *mut BufferUsage;
    walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false) as *mut WalUsage;
    InstrEndParallelQuery(
        bufferusage.add(ParallelWorkerNumber as usize),
        walusage.add(ParallelWorkerNumber as usize),
    );

    index_close(indexRel, indexLockmode);
    table_close(heapRel, heapLockmode);
}

/*
 * brin_build_empty_tuple
 *		Maybe initialize a BRIN tuple representing empty range.
 *
 * Returns a BRIN tuple representing an empty page range starting at the
 * specified block number. The empty tuple is initialized only once, when it's
 * needed for the first time, stored in the memory context bs_context to ensure
 * proper life span, and reused on following calls. All empty tuples are
 * exactly the same except for the bt_blkno field, which is set to the value
 * in blkno parameter.
 */
unsafe fn brin_build_empty_tuple(state: *mut BrinBuildState, blkno: BlockNumber) {
    /* First time an empty tuple is requested? If yes, initialize it. */
    if (*state).bs_emptyTuple.is_null() {
        let oldcxt: MemoryContext;
        let dtuple: *mut BrinMemTuple = brin_new_memtuple((*state).bs_bdesc);

        /* Allocate the tuple in context for the whole index build. */
        oldcxt = MemoryContextSwitchTo((*state).bs_context);

        (*state).bs_emptyTuple =
            brin_form_tuple((*state).bs_bdesc, blkno, dtuple, &mut (*state).bs_emptyTupleLen);

        MemoryContextSwitchTo(oldcxt);
    } else {
        /* If we already have an empty tuple, just update the block. */
        (*(*state).bs_emptyTuple).bt_blkno = blkno;
    }
}

/*
 * brin_fill_empty_ranges
 *		Add BRIN index tuples representing empty page ranges.
 *
 * prevRange/nextRange determine for which page ranges to add empty summaries.
 * Both boundaries are exclusive, i.e. only ranges starting at blkno for which
 * (prevRange < blkno < nextRange) will be added to the index.
 *
 * If prevRange is InvalidBlockNumber, this means there was no previous page
 * range (i.e. the first empty range to add is for blkno=0).
 *
 * The empty tuple is built only once, and then reused for all future calls.
 */
unsafe fn brin_fill_empty_ranges(
    state: *mut BrinBuildState,
    prevRange: BlockNumber,
    nextRange: BlockNumber,
) {
    let mut blkno: BlockNumber;

    /*
     * If we already summarized some ranges, we need to start with the next
     * one. Otherwise start from the first range of the table.
     */
    blkno = if prevRange == InvalidBlockNumber {
        0
    } else {
        prevRange + (*state).bs_pagesPerRange
    };

    /* Generate empty ranges until we hit the next non-empty range. */
    while blkno < nextRange {
        /* Did we already build the empty tuple? If not, do it now. */
        brin_build_empty_tuple(state, blkno);

        brin_doinsert(
            (*state).bs_irel,
            (*state).bs_pagesPerRange,
            (*state).bs_rmAccess,
            &mut (*state).bs_currentInsertBuf,
            blkno,
            (*state).bs_emptyTuple,
            (*state).bs_emptyTupleLen,
        );

        /* try next page range */
        blkno += (*state).bs_pagesPerRange;
    }
}

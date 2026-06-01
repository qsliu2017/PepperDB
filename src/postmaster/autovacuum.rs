//! postmaster/autovacuum.c - PostgreSQL Integrated Autovacuum Daemon
//!
//! The autovacuum system is structured in two different kinds of processes: the
//! autovacuum launcher and the autovacuum worker.  The launcher is an
//! always-running process, started by the postmaster when the autovacuum GUC
//! parameter is set.  The launcher schedules autovacuum workers to be started
//! when appropriate.  The workers are the processes which execute the actual
//! vacuuming; they connect to a database as determined in the launcher, and
//! once connected they examine the catalogs to select the tables to vacuum.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/postmaster/autovacuum.c

use crate::prelude::*;

use std::mem::size_of;

// errmsg_internal: add a primary error message (no translation). The shim
// produces the formatted string forwarded by `ereport!`.
macro_rules! errmsg_internal {
    ($($arg:tt)*) => { format!($($arg)*) };
}

// ---- external C stubs (TODO(pg-port): wire up when modules are ported) ------

extern "C" {
    // miscadmin.h
    static mut PostmasterContext: MemoryContext;
    static mut PostAuthDelay: c_int;
    static mut MyBackendType: c_int;
    static mut MyDatabaseId: Oid;
    static mut MyProc: *mut PGPROC;
    static mut MyProcPid: c_int;
    static mut MyLatch: *mut Latch;
    static mut PG_exception_stack: *mut sigjmp_buf;
    static mut error_context_stack: *mut ErrorContextCallback;
    static mut QueryCancelPending: bool;
    static mut InterruptPending: bool;
    static mut ShutdownRequestPending: bool;
    static mut ConfigReloadPending: bool;
    static mut ProcSignalBarrierPending: bool;
    static mut LogMemoryContextPending: bool;
    static mut PortalContext: MemoryContext;
    static mut AuxProcessResourceOwner: ResourceOwner;
    // guc.h
    static mut vacuum_cost_delay: f64;
    static mut vacuum_cost_limit: c_int;
    static mut VacuumCostDelay: f64;
    static mut VacuumCostLimit: c_int;
    static mut VacuumCostActive: bool;
    static mut VacuumCostBalance: c_int;
    static mut VacuumFailsafeActive: bool;
    static mut VacuumBufferUsageLimit: c_int;
    static mut pgstat_track_counts: bool;
    static mut synchronous_commit: c_int;
    static mut vacuum_freeze_min_age: c_int;
    static mut vacuum_freeze_table_age: c_int;
    static mut vacuum_multixact_freeze_min_age: c_int;
    static mut vacuum_multixact_freeze_table_age: c_int;
    static mut vacuum_max_eager_freeze_failure_rate: f64;

    // signal / unistd
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
    fn kill(pid: pid_t, sig: c_int) -> c_int;

    // postgres init
    fn InitProcess();
    fn BaseInit();
    fn InitPostgres(
        in_dbname: *const c_char,
        dboid: Oid,
        username: *const c_char,
        useroid: Oid,
        flags: c_uint,
        out_dbname: *mut c_char,
    );
    fn SetProcessingMode(mode: c_int);
    fn GetProcessingMode() -> c_int;
    fn init_ps_display(fixed_part: *const c_char);
    fn set_ps_display(activity: *const c_char);
    fn proc_exit(code: c_int) -> !;

    // memory
    fn AllocSetContextCreate(
        parent: MemoryContext,
        name: *const c_char,
        minContextSize: usize,
        initBlockSize: usize,
        maxBlockSize: usize,
    ) -> MemoryContext;
    fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext;
    fn MemoryContextDelete(context: MemoryContext);
    fn MemoryContextReset(context: MemoryContext);
    static mut CurrentMemoryContext: MemoryContext;
    static mut TopMemoryContext: MemoryContext;
    fn palloc(size: usize) -> *mut c_void;
    fn palloc0(size: usize) -> *mut c_void;
    fn pfree(ptr: *mut c_void);
    fn pstrdup(s: *const c_char) -> *mut c_char;

    // transactions
    fn StartTransactionCommand();
    fn CommitTransactionCommand();
    fn AbortCurrentTransaction();
    fn AbortOutOfAnyTransaction();
    fn FlushErrorState();

    // error handling
    fn EmitErrorReport();
    fn LWLockReleaseAll();
    fn pgstat_report_wait_end();
    fn pgaio_error_cleanup();
    fn UnlockBuffers();
    fn ReleaseAuxProcessResources(isCommit: bool);
    fn AtEOXact_Buffers(isCommit: bool);
    fn AtEOXact_SMgr();
    fn AtEOXact_Files(isCommit: bool);
    fn AtEOXact_HashTables(isCommit: bool);
    fn ProcessCatchupInterrupt();
    fn ProcessProcSignalBarrier();
    fn ProcessLogMemoryContextInterrupt();

    // config
    fn ProcessConfigFile(context: c_int);
    fn SetConfigOption(
        name: *const c_char,
        value: *const c_char,
        context: c_int,
        source: c_int,
    );

    // latch / wait
    fn WaitLatch(
        latch: *mut Latch,
        wakeEvents: c_int,
        timeout: c_long,
        wait_event_info: u32,
    ) -> c_int;
    fn ResetLatch(latch: *mut Latch);
    fn SetLatch(latch: *mut Latch);

    // timeouts
    fn InitializeTimeouts();
    fn disable_all_timeouts(reset_timer: bool);

    // signal handlers
    fn SignalHandlerForConfigReload(postgres_signal_arg: c_int);
    fn SignalHandlerForShutdownRequest(postgres_signal_arg: c_int);
    fn StatementCancelHandler(postgres_signal_arg: c_int);
    fn die(postgres_signal_arg: c_int);
    fn FloatExceptionHandler(postgres_signal_arg: c_int);
    fn procsignal_sigusr1_handler(postgres_signal_arg: c_int);
    fn pqsignal(signo: c_int, func: Option<unsafe extern "C" fn(c_int)>) -> *mut c_void;
    fn SendPostmasterSignal(reason: c_int);

    // LWLock
    fn LWLockAcquire(lock: *mut LWLock, mode: c_int) -> bool;
    fn LWLockRelease(lock: *mut LWLock);
    fn LWLockHeldByMe(lock: *mut LWLock) -> bool;
    static mut AutovacuumLock: LWLock;
    static mut AutovacuumScheduleLock: LWLock;

    // hash table
    fn hash_create(
        tabname: *const c_char,
        nelem: c_long,
        info: *const HASHCTL,
        flags: c_int,
    ) -> *mut HTAB;
    fn hash_search(
        hashp: *mut HTAB,
        keyPtr: *const c_void,
        action: c_int,
        foundPtr: *mut bool,
    ) -> *mut c_void;
    fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB);
    fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void;

    // qsort
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );

    // pg_cmp
    fn pg_cmp_s32(a: i32, b: i32) -> c_int;

    // dlist
    fn dlist_init(list: *mut dlist_head);
    fn dlist_push_head(list: *mut dlist_head, node: *mut dlist_node);
    fn dlist_move_head(list: *mut dlist_head, node: *mut dlist_node);
    fn dlist_delete(node: *mut dlist_node);
    fn dlist_is_empty(list: *const dlist_head) -> bool;
    fn dclist_init(list: *mut dclist_head);
    fn dclist_push_head(list: *mut dclist_head, node: *mut dlist_node);
    fn dclist_pop_head_node(list: *mut dclist_head) -> *mut dlist_node;
    fn dclist_count(list: *const dclist_head) -> c_uint;

    // timestamps
    fn GetCurrentTimestamp() -> TimestampTz;
    fn TimestampDifference(
        start_time: TimestampTz,
        stop_time: TimestampTz,
        secs: *mut c_long,
        microsecs: *mut c_int,
    );
    fn TimestampDifferenceExceeds(
        start_time: TimestampTz,
        stop_time: TimestampTz,
        msec: c_int,
    ) -> bool;
    fn TimestampTzPlusMilliseconds(t: TimestampTz, ms: c_long) -> TimestampTz;

    // xact ids
    fn ReadNextTransactionId() -> TransactionId;
    fn ReadNextMultiXactId() -> MultiXactId;
    fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool;
    fn MultiXactIdPrecedes(id1: MultiXactId, id2: MultiXactId) -> bool;
    fn TransactionIdIsNormal(xid: TransactionId) -> bool;
    fn MultiXactIdIsValid(mid: MultiXactId) -> bool;
    fn MultiXactMemberFreezeThreshold() -> c_int;

    // syscache / catalog
    fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn heap_freetuple(htup: HeapTuple);
    fn ObjectIdGetDatum(oid: Oid) -> Datum;
    fn GETSTRUCT(tup: HeapTuple) -> *mut c_void;
    fn OidIsValid(oid: Oid) -> bool;
    fn NameStr(name: NameData) -> *const c_char;

    // table scan
    fn table_open(relationId: Oid, lockmode: c_int) -> Relation;
    fn table_close(relation: Relation, lockmode: c_int);
    fn table_beginscan_catalog(
        relation: Relation,
        nkeys: c_int,
        key: *const ScanKeyData,
    ) -> TableScanDesc;
    fn table_endscan(scan: TableScanDesc);
    fn heap_getnext(scan: TableScanDesc, direction: c_int) -> HeapTuple;
    fn HeapTupleIsValid(tuple: HeapTuple) -> bool;
    fn lappend(list: *mut List, datum: *mut c_void) -> *mut List;
    fn lappend_oid(list: *mut List, datum: Oid) -> *mut List;
    fn list_free(list: *mut List);
    fn lfirst(cell: *const ListCell) -> *mut c_void;
    fn lfirst_oid(cell: *const ListCell) -> Oid;

    // rel options
    fn extractRelOptions(
        tuple: HeapTuple,
        tupdesc: TupleDesc,
        amoptions: *mut c_void,
    ) -> *mut bytea;
    fn CreateTupleDescCopy(tupdesc: TupleDesc) -> TupleDesc;
    fn RelationGetDescr(rel: Relation) -> TupleDesc;

    // vacuum
    fn vacuum(
        relations: *mut List,
        params: *const VacuumParams,
        bstrategy: BufferAccessStrategy,
        vac_context: MemoryContext,
        isTopLevel: bool,
    );
    fn vac_update_datfrozenxid();
    fn GetAccessStrategyWithSize(strategy: c_int, maxbuffers: c_int) -> BufferAccessStrategy;
    fn pgstat_fetch_stat_dbentry(dbid: Oid) -> *mut PgStat_StatDBEntry;
    fn pgstat_fetch_stat_tabentry_ext(
        shared: bool,
        relid: Oid,
    ) -> *mut PgStat_StatTabEntry;
    fn pgstat_report_autovac(dbid: Oid);
    fn pgstat_report_activity(state: c_int, cmd_str: *const c_char);

    // scan key
    fn ScanKeyInit(
        entry: *mut ScanKeyData,
        attributeNumber: c_int,
        strategy: c_uint,
        procedure: Oid,
        argument: Datum,
    );

    // namespace / name helpers
    fn get_database_name(dbid: Oid) -> *mut c_char;
    fn get_namespace_name(nspid: Oid) -> *mut c_char;
    fn get_rel_name(relid: Oid) -> *mut c_char;
    fn get_rel_namespace(relid: Oid) -> Oid;
    fn database_is_invalid_form(form: *mut FormData_pg_database) -> bool;
    fn checkTempNamespaceStatus(namespaceid: Oid) -> c_int;

    // locks
    fn ConditionalLockRelationOid(relid: Oid, lockmode: c_int) -> bool;
    fn UnlockRelationOid(relid: Oid, lockmode: c_int);
    fn ConditionalLockDatabaseObject(
        classid: Oid,
        objid: Oid,
        objsubid: u32,
        lockmode: c_int,
    ) -> bool;

    // object drop
    fn performDeletion(
        object: *const ObjectAddress,
        behavior: c_int,
        flags: c_int,
    );

    // snapshot
    fn GetTransactionSnapshot() -> Snapshot;
    fn PushActiveSnapshot(snapshot: Snapshot);
    fn PopActiveSnapshot();
    fn ActiveSnapshotSet() -> bool;

    // direct function call
    fn DirectFunctionCall2(func: PGFunction, arg1: Datum, arg2: Datum) -> Datum;
    fn Int64GetDatum(val: i64) -> Datum;
    fn brin_summarize_range(fcinfo: *mut c_void) -> Datum;

    // vacuum relation helpers
    fn makeRangeVar(
        schemaname: *mut c_char,
        relname: *mut c_char,
        location: c_int,
    ) -> *mut RangeVar;
    fn makeVacuumRelation(
        relation: *mut RangeVar,
        oid: Oid,
        va_cols: *mut List,
    ) -> *mut VacuumRelation;
    fn list_make1(datum: *mut c_void) -> *mut List;

    // statement timestamp
    fn SetCurrentStatementStartTimestamp();

    // on_shmem_exit
    fn on_shmem_exit(function: unsafe extern "C" fn(c_int, Datum), arg: Datum);

    // shmem
    fn ShmemInitStruct(
        name: *const c_char,
        size: usize,
        foundPtr: *mut bool,
    ) -> *mut c_void;

    // atomic ops
    fn pg_atomic_init_flag(ptr: *mut pg_atomic_flag);
    fn pg_atomic_test_set_flag(ptr: *mut pg_atomic_flag) -> bool;
    fn pg_atomic_clear_flag(ptr: *mut pg_atomic_flag);
    fn pg_atomic_unlocked_test_flag(ptr: *const pg_atomic_flag) -> bool;
    fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32);
    fn pg_atomic_read_u32(ptr: *const pg_atomic_uint32) -> u32;
    fn pg_atomic_write_u32(ptr: *mut pg_atomic_uint32, val: u32);

    static mut IsUnderPostmaster: bool;
    fn MAXALIGN(size: usize) -> usize;
    fn add_size(s1: usize, s2: usize) -> usize;
    fn mul_size(s1: usize, s2: usize) -> usize;
    fn message_level_is_interesting(elevel: c_int) -> bool;

    fn pg_usleep(microsec: c_long);

    // injection point
    fn InjectionPointRun(name: *const c_char, private_data: *mut c_void);

    static UnBlockSig: sigset_t;
}

// ---- type aliases / opaque stubs -------------------------------------------

pub type MemoryContext = *mut c_void;
pub type ResourceOwner = *mut c_void;
pub type Relation = *mut c_void;
pub type TableScanDesc = *mut c_void;
pub type HeapTuple = *mut HeapTupleData;
pub type TupleDesc = *mut c_void;
pub type BufferAccessStrategy = *mut c_void;
pub type Snapshot = *mut c_void;
pub type PGFunction = unsafe extern "C" fn(*mut c_void) -> Datum;
pub type Datum = usize;
pub type Oid = u32;
pub type TransactionId = u32;
pub type MultiXactId = u32;
pub type BlockNumber = u32;
pub type TimestampTz = i64;
pub type pid_t = i32;
pub type sigset_t = u32;
pub type sig_atomic_t = c_int;
pub type bytea = c_void;
pub type sigjmp_buf = [u8; 148]; // platform-sized opaque

#[repr(C)]
pub struct HeapTupleData {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct PGPROC {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct Latch {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct LWLock {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct ErrorContextCallback {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct List {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct ListCell {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct dlist_head {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct dclist_head {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct dlist_node {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct dlist_iter {
    pub cur: *mut dlist_node,
}
#[repr(C)]
pub struct HTAB {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct HASHCTL {
    pub keysize: usize,
    pub entrysize: usize,
    pub hcxt: MemoryContext,
    _pad: [u8; 128],
}
#[repr(C)]
pub struct HASH_SEQ_STATUS {
    _opaque: [u8; 64],
}
#[repr(C)]
pub struct pg_atomic_flag {
    _opaque: [u8; 4],
}
#[repr(C)]
pub struct pg_atomic_uint32 {
    _opaque: [u8; 4],
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NameData {
    pub data: [c_char; 64],
}
#[repr(C)]
pub struct ScanKeyData {
    _opaque: [u8; 32],
}
#[repr(C)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: i32,
}
#[repr(C)]
pub struct RangeVar {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct VacuumRelation {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct FormData_pg_database {
    pub oid: Oid,
    pub datname: NameData,
    pub datistemplate: bool,
    pub datallowconn: bool,
    pub datfrozenxid: TransactionId,
    pub datminmxid: MultiXactId,
    _pad: [u8; 256],
}
pub type Form_pg_database = *mut FormData_pg_database;

#[repr(C)]
pub struct FormData_pg_class {
    pub oid: Oid,
    pub relname: NameData,
    pub relnamespace: Oid,
    pub relkind: c_char,
    pub relpersistence: c_char,
    pub relisshared: bool,
    pub reltoastrelid: Oid,
    pub relfrozenxid: TransactionId,
    pub relminmxid: MultiXactId,
    pub reltuples: f32,
    pub relpages: i32,
    pub relallfrozen: i32,
    pub datistemplate: bool,
    pub datallowconn: bool,
    pub datfrozenxid: TransactionId,
    pub datminmxid: MultiXactId,
    _pad: [u8; 256],
}
pub type Form_pg_class = *mut FormData_pg_class;

#[repr(C)]
pub struct PgStat_StatDBEntry {
    pub last_autovac_time: TimestampTz,
    _pad: [u8; 128],
}
#[repr(C)]
pub struct PgStat_StatTabEntry {
    pub dead_tuples: f32,
    pub ins_since_vacuum: f32,
    pub mod_since_analyze: f32,
    _pad: [u8; 128],
}

/// AutoVacOpts - storage parameters controlling autovacuum for a relation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AutoVacOpts {
    pub enabled: bool,
    pub vacuum_threshold: i32,
    pub vacuum_max_threshold: i32,
    pub vacuum_ins_threshold: i32,
    pub analyze_threshold: i32,
    pub vacuum_cost_delay: f64,
    pub vacuum_cost_limit: i32,
    pub freeze_min_age: i32,
    pub freeze_max_age: i32,
    pub freeze_table_age: i32,
    pub multixact_freeze_min_age: i32,
    pub multixact_freeze_max_age: i32,
    pub multixact_freeze_table_age: i32,
    pub log_min_duration: i32,
    pub vacuum_scale_factor: f32,
    pub vacuum_ins_scale_factor: f32,
    pub analyze_scale_factor: f32,
    _pad: [u8; 32],
}

#[repr(C)]
pub struct StdRdOptions {
    pub vl_len: i32,
    pub fillfactor: c_int,
    pub toast_tuple_target: c_int,
    pub autovacuum: AutoVacOpts,
    _pad: [u8; 64],
}

/// VacuumParams - vacuum parameters passed to vacuum().
#[repr(C)]
pub struct VacuumParams {
    pub options: c_int,
    pub index_cleanup: c_int,
    pub truncate: c_int,
    pub nworkers: c_int,
    pub freeze_min_age: i32,
    pub freeze_table_age: i32,
    pub multixact_freeze_min_age: i32,
    pub multixact_freeze_table_age: i32,
    pub is_wraparound: bool,
    pub log_min_duration: i32,
    pub toast_parent: Oid,
    pub max_eager_freeze_failure_rate: f64,
}

// ---- GUC parameters ---------------------------------------------------------

#[no_mangle]
pub static mut autovacuum_start_daemon: bool = false;
#[no_mangle]
pub static mut autovacuum_worker_slots: c_int = 0;
#[no_mangle]
pub static mut autovacuum_max_workers: c_int = 0;
#[no_mangle]
pub static mut autovacuum_work_mem: c_int = -1;
#[no_mangle]
pub static mut autovacuum_naptime: c_int = 0;
#[no_mangle]
pub static mut autovacuum_vac_thresh: c_int = 0;
#[no_mangle]
pub static mut autovacuum_vac_max_thresh: c_int = 0;
#[no_mangle]
pub static mut autovacuum_vac_scale: f64 = 0.0;
#[no_mangle]
pub static mut autovacuum_vac_ins_thresh: c_int = 0;
#[no_mangle]
pub static mut autovacuum_vac_ins_scale: f64 = 0.0;
#[no_mangle]
pub static mut autovacuum_anl_thresh: c_int = 0;
#[no_mangle]
pub static mut autovacuum_anl_scale: f64 = 0.0;
#[no_mangle]
pub static mut autovacuum_freeze_max_age: c_int = 0;
#[no_mangle]
pub static mut autovacuum_multixact_freeze_max_age: c_int = 0;
#[no_mangle]
pub static mut autovacuum_vac_cost_delay: f64 = 0.0;
#[no_mangle]
pub static mut autovacuum_vac_cost_limit: c_int = 0;
#[no_mangle]
pub static mut Log_autovacuum_min_duration: c_int = 600000;

/* the minimum allowed time between two awakenings of the launcher */
const MIN_AUTOVAC_SLEEPTIME: f64 = 100.0; /* milliseconds */
const MAX_AUTOVAC_SLEEPTIME: c_long = 300; /* seconds */

/*
 * Variables to save the cost-related storage parameters for the current
 * relation being vacuumed by this autovacuum worker. Using these, we can
 * ensure we don't overwrite the values of vacuum_cost_delay and
 * vacuum_cost_limit after reloading the configuration file. They are
 * initialized to "invalid" values to indicate that no cost-related storage
 * parameters were specified and will be set in do_autovacuum() after checking
 * the storage parameters in table_recheck_autovac().
 */
static mut av_storage_param_cost_delay: f64 = -1.0;
static mut av_storage_param_cost_limit: c_int = -1;

/* Flags set by signal handlers */
static mut got_SIGUSR2: volatile_bool = false;

/* Comparison points for determining whether freeze_max_age is exceeded */
static mut recentXid: TransactionId = 0;
static mut recentMulti: MultiXactId = 0;

/* Default freeze ages to use for autovacuum (varies by database) */
static mut default_freeze_min_age: c_int = 0;
static mut default_freeze_table_age: c_int = 0;
static mut default_multixact_freeze_min_age: c_int = 0;
static mut default_multixact_freeze_table_age: c_int = 0;

/* Memory context for long-lived data */
static mut AutovacMemCxt: MemoryContext = std::ptr::null_mut();

// ---- struct definitions -----------------------------------------------------

/* struct to keep track of databases in launcher */
#[repr(C)]
pub struct avl_dbase {
    pub adl_datid: Oid, /* hash key -- must be first */
    pub adl_next_worker: TimestampTz,
    pub adl_score: c_int,
    pub adl_node: dlist_node,
}

/* struct to keep track of databases in worker */
#[repr(C)]
pub struct avw_dbase {
    pub adw_datid: Oid,
    pub adw_name: *mut c_char,
    pub adw_frozenxid: TransactionId,
    pub adw_minmulti: MultiXactId,
    pub adw_entry: *mut PgStat_StatDBEntry,
}

/* struct to keep track of tables to vacuum and/or analyze, in 1st pass */
#[repr(C)]
pub struct av_relation {
    pub ar_toastrelid: Oid, /* hash key - must be first */
    pub ar_relid: Oid,
    pub ar_hasrelopts: bool,
    pub ar_reloptions: AutoVacOpts, /* copy of AutoVacOpts from the main table's
                                     * reloptions, or NULL if none */
}

/* struct to keep track of tables to vacuum and/or analyze, after rechecking */
#[repr(C)]
pub struct autovac_table {
    pub at_relid: Oid,
    pub at_params: VacuumParams,
    pub at_storage_param_vac_cost_delay: f64,
    pub at_storage_param_vac_cost_limit: c_int,
    pub at_dobalance: bool,
    pub at_sharedrel: bool,
    pub at_relname: *mut c_char,
    pub at_nspname: *mut c_char,
    pub at_datname: *mut c_char,
}

/*-------------
 * This struct holds information about a single worker's whereabouts.  We keep
 * an array of these in shared memory, sized according to
 * autovacuum_worker_slots.
 *
 * wi_links     entry into free list or running list
 * wi_dboid     OID of the database this worker is supposed to work on
 * wi_tableoid  OID of the table currently being vacuumed, if any
 * wi_sharedrel flag indicating whether table is marked relisshared
 * wi_proc      pointer to PGPROC of the running worker, NULL if not started
 * wi_launchtime Time at which this worker was launched
 * wi_dobalance Whether this worker should be included in balance calculations
 *
 * All fields are protected by AutovacuumLock, except for wi_tableoid and
 * wi_sharedrel which are protected by AutovacuumScheduleLock (note these
 * two fields are read-only for everyone except that worker itself).
 *-------------
 */
#[repr(C)]
pub struct WorkerInfoData {
    pub wi_links: dlist_node,
    pub wi_dboid: Oid,
    pub wi_tableoid: Oid,
    pub wi_proc: *mut PGPROC,
    pub wi_launchtime: TimestampTz,
    pub wi_dobalance: pg_atomic_flag,
    pub wi_sharedrel: bool,
}

pub type WorkerInfo = *mut WorkerInfoData;

/*
 * Possible signals received by the launcher from remote processes.  These are
 * stored atomically in shared memory so that other processes can set them
 * without locking.
 */
#[repr(C)]
pub enum AutoVacuumSignal {
    AutoVacForkFailed = 0, /* failed trying to start a worker */
    AutoVacRebalance = 1,  /* rebalance the cost limits */
}

const AutoVacNumSignals: usize = 2; /* AutoVacRebalance + 1 */

/*
 * Autovacuum workitem array, stored in AutoVacuumShmem->av_workItems.  This
 * list is mostly protected by AutovacuumLock, except that if an item is
 * marked 'active' other processes must not modify the work-identifying
 * members.
 */
#[repr(C)]
pub struct AutoVacuumWorkItem {
    pub avw_type: AutoVacuumWorkItemType,
    pub avw_used: bool,     /* below data is valid */
    pub avw_active: bool,   /* being processed */
    pub avw_database: Oid,
    pub avw_relation: Oid,
    pub avw_blockNumber: BlockNumber,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub enum AutoVacuumWorkItemType {
    AVW_BRINSummarizeRange = 0,
}

const NUM_WORKITEMS: usize = 256;

/*-------------
 * The main autovacuum shmem struct.  On shared memory we store this main
 * struct and the array of WorkerInfo structs.  This struct keeps:
 *
 * av_signal            set by other processes to indicate various conditions
 * av_launcherpid       the PID of the autovacuum launcher
 * av_freeWorkers       the WorkerInfo freelist
 * av_runningWorkers    the WorkerInfo non-free queue
 * av_startingWorker    pointer to WorkerInfo currently being started (cleared by
 *                      the worker itself as soon as it's up and running)
 * av_workItems         work item array
 * av_nworkersForBalance the number of autovacuum workers to use when
 *                      calculating the per worker cost limit
 *
 * This struct is protected by AutovacuumLock, except for av_signal and parts
 * of the worker list (see above).
 *-------------
 */
#[repr(C)]
pub struct AutoVacuumShmemStruct {
    pub av_signal: [sig_atomic_t; AutoVacNumSignals],
    pub av_launcherpid: pid_t,
    pub av_freeWorkers: dclist_head,
    pub av_runningWorkers: dlist_head,
    pub av_startingWorker: WorkerInfo,
    pub av_workItems: [AutoVacuumWorkItem; NUM_WORKITEMS],
    pub av_nworkersForBalance: pg_atomic_uint32,
}

static mut AutoVacuumShmem: *mut AutoVacuumShmemStruct = std::ptr::null_mut();

/*
 * the database list (of avl_dbase elements) in the launcher, and the context
 * that contains it
 */
// DLIST_STATIC_INIT equivalent - zeroed dlist_head
static mut DatabaseList: dlist_head = dlist_head { _opaque: [] };
static mut DatabaseListCxt: MemoryContext = std::ptr::null_mut();

/* Pointer to my own WorkerInfo, valid on each worker */
static mut MyWorkerInfo: WorkerInfo = std::ptr::null_mut();

/* PID of launcher, valid only in worker while shutting down */
#[no_mangle]
pub static mut AutovacuumLauncherPid: c_int = 0;

// volatile_bool - mirrors C's volatile sig_atomic_t for got_SIGUSR2
type volatile_bool = bool;

// ---- forward declarations (as Rust fn prototypes) ---------------------------

// (implemented below; no separate declarations needed in Rust)

/********************************************************************
 *                  AUTOVACUUM LAUNCHER CODE
 ********************************************************************/

/*
 * Main entry point for the autovacuum launcher process.
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacLauncherMain(
    startup_data: *const c_void,
    startup_data_len: usize,
) {
    let local_sigjmp_buf: sigjmp_buf = [0u8; 148];

    // Assert(startup_data_len == 0);

    /* Release postmaster's working memory context */
    if !PostmasterContext.is_null() {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = std::ptr::null_mut();
    }

    MyBackendType = 20; /* B_AUTOVAC_LAUNCHER */
    init_ps_display(std::ptr::null());

    ereport!(
        5, /* DEBUG1 */
        errmsg_internal!("autovacuum launcher started")
    );

    if PostAuthDelay != 0 {
        pg_usleep(PostAuthDelay as c_long * 1000000);
    }

    // Assert(GetProcessingMode() == InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    pqsignal(1 /*SIGHUP*/, Some(SignalHandlerForConfigReload));
    pqsignal(2 /*SIGINT*/, Some(StatementCancelHandler));
    pqsignal(15 /*SIGTERM*/, Some(SignalHandlerForShutdownRequest));
    /* SIGQUIT handler was already set up by InitPostmasterChild */

    InitializeTimeouts(); /* establishes SIGALRM handler */

    pqsignal(13 /*SIGPIPE*/, None);
    pqsignal(10 /*SIGUSR1*/, Some(procsignal_sigusr1_handler));
    pqsignal(12 /*SIGUSR2*/, Some(avl_sigusr2_handler));
    pqsignal(8 /*SIGFPE*/, Some(FloatExceptionHandler));
    pqsignal(17 /*SIGCHLD*/, None);

    /*
     * Create a per-backend PGPROC struct in shared memory.  We must do this
     * before we can use LWLocks or access any shared memory.
     */
    InitProcess();

    /* Early initialization */
    BaseInit();

    InitPostgres(
        std::ptr::null(),
        0, /* InvalidOid */
        std::ptr::null(),
        0, /* InvalidOid */
        0,
        std::ptr::null_mut(),
    );

    SetProcessingMode(2 /* NormalProcessing */);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    AutovacMemCxt = AllocSetContextCreate(
        TopMemoryContext,
        b"Autovacuum Launcher\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );
    MemoryContextSwitchTo(AutovacMemCxt);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     *
     * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
     * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
     * signals other than SIGQUIT will be blocked until we complete error
     * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
     * call redundant, but it is not since InterruptPending might be set
     * already.
     */
    // if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    // { ... error recovery ... }
    // PG_exception_stack = &local_sigjmp_buf;
    // (C setjmp/longjmp error recovery omitted -- TODO(pg-port): wire up)

    /* must unblock signals before calling rebuild_database_list */
    sigprocmask(
        2, /* SIG_SETMASK */
        &UnBlockSig as *const sigset_t,
        std::ptr::null_mut(),
    );

    /*
     * Set always-secure search path.  Launcher doesn't connect to a database,
     * so this has no effect.
     */
    SetConfigOption(
        b"search_path\0".as_ptr() as *const c_char,
        b"\0".as_ptr() as *const c_char,
        18, /* PGC_SUSET */
        8,  /* PGC_S_OVERRIDE */
    );

    /*
     * Force zero_damaged_pages OFF in the autovac process, even if it is set
     * in postgresql.conf.  We don't really want such a dangerous option being
     * applied non-interactively.
     */
    SetConfigOption(
        b"zero_damaged_pages\0".as_ptr() as *const c_char,
        b"false\0".as_ptr() as *const c_char,
        18,
        8,
    );

    /*
     * Force settable timeouts off to avoid letting these settings prevent
     * regular maintenance from being executed.
     */
    SetConfigOption(b"statement_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);
    SetConfigOption(b"transaction_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);
    SetConfigOption(b"lock_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);
    SetConfigOption(b"idle_in_transaction_session_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);

    /*
     * Force default_transaction_isolation to READ COMMITTED.  We don't want
     * to pay the overhead of serializable mode, nor add any risk of causing
     * deadlocks or delaying other transactions.
     */
    SetConfigOption(
        b"default_transaction_isolation\0".as_ptr() as *const c_char,
        b"read committed\0".as_ptr() as *const c_char,
        18,
        8,
    );

    /*
     * Even when system is configured to use a different fetch consistency,
     * for autovac we always want fresh stats.
     */
    SetConfigOption(
        b"stats_fetch_consistency\0".as_ptr() as *const c_char,
        b"none\0".as_ptr() as *const c_char,
        18,
        8,
    );

    /*
     * In emergency mode, just start a worker (unless shutdown was requested)
     * and go away.
     */
    if !AutoVacuumingActive() {
        if !ShutdownRequestPending {
            do_start_worker();
        }
        proc_exit(0); /* done */
    }

    (*AutoVacuumShmem).av_launcherpid = MyProcPid as pid_t;

    /*
     * Create the initial database list.  The invariant we want this list to
     * keep is that it's ordered by decreasing next_time.  As soon as an entry
     * is updated to a higher time, it will be moved to the front (which is
     * correct because the only operation is to add autovacuum_naptime to the
     * entry, and time always increases).
     */
    rebuild_database_list(0 /* InvalidOid */);

    /* loop until shutdown request */
    while !ShutdownRequestPending {
        let mut nap = libc_timeval { tv_sec: 0, tv_usec: 0 };
        let mut current_time: TimestampTz = 0;
        let mut can_launch: bool;

        /*
         * This loop is a bit different from the normal use of WaitLatch,
         * because we'd like to sleep before the first launch of a child
         * process.  So it's WaitLatch, then ResetLatch, then check for
         * wakening conditions.
         */

        launcher_determine_sleep(av_worker_available(), false, &mut nap);

        /*
         * Wait until naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         */
        WaitLatch(
            MyLatch,
            0x01 | 0x04 | 0x10, /* WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH */
            (nap.tv_sec * 1000) + (nap.tv_usec / 1000),
            0x0A00_0001, /* WAIT_EVENT_AUTOVACUUM_MAIN */
        );

        ResetLatch(MyLatch);

        ProcessAutoVacLauncherInterrupts();

        /*
         * a worker finished, or postmaster signaled failure to start a worker
         */
        if got_SIGUSR2 {
            got_SIGUSR2 = false;

            /* rebalance cost limits, if needed */
            if (*AutoVacuumShmem).av_signal[AutoVacuumSignal::AutoVacRebalance as usize] != 0 {
                LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);
                (*AutoVacuumShmem).av_signal[AutoVacuumSignal::AutoVacRebalance as usize] = 0;
                autovac_recalculate_workers_for_balance();
                LWLockRelease(&mut AutovacuumLock);
            }

            if (*AutoVacuumShmem).av_signal[AutoVacuumSignal::AutoVacForkFailed as usize] != 0 {
                /*
                 * If the postmaster failed to start a new worker, we sleep
                 * for a little while and resend the signal.  The new worker's
                 * state is still in memory, so this is sufficient.  After
                 * that, we restart the main loop.
                 *
                 * XXX should we put a limit to the number of times we retry?
                 * I don't think it makes much sense, because a future start
                 * of a worker will continue to fail in the same way.
                 */
                (*AutoVacuumShmem).av_signal[AutoVacuumSignal::AutoVacForkFailed as usize] = 0;
                pg_usleep(1000000); /* 1s */
                SendPostmasterSignal(12 /* PMSIGNAL_START_AUTOVAC_WORKER */);
                continue;
            }
        }

        /*
         * There are some conditions that we need to check before trying to
         * start a worker.  First, we need to make sure that there is a worker
         * slot available.  Second, we need to make sure that no other worker
         * failed while starting up.
         */

        current_time = GetCurrentTimestamp();
        LWLockAcquire(&mut AutovacuumLock, 2 /* LW_SHARED */);

        can_launch = av_worker_available();

        if !(*AutoVacuumShmem).av_startingWorker.is_null() {
            let waittime: c_int;
            let worker: WorkerInfo = (*AutoVacuumShmem).av_startingWorker;

            /*
             * We can't launch another worker when another one is still
             * starting up (or failed while doing so), so just sleep for a bit
             * more; that worker will wake us up again as soon as it's ready.
             * We will only wait autovacuum_naptime seconds (up to a maximum
             * of 60 seconds) for this to happen however.  Note that failure
             * to connect to a particular database is not a problem here,
             * because the worker removes itself from the startingWorker
             * pointer before trying to connect.  Problems detected by the
             * postmaster (like fork() failure) are also reported and handled
             * differently.  The only problems that may cause this code to
             * fire are errors in the earlier sections of AutoVacWorkerMain,
             * before the worker removes the WorkerInfo from the
             * startingWorker pointer.
             */
            waittime = (if autovacuum_naptime < 60 { autovacuum_naptime } else { 60 }) * 1000;
            if TimestampDifferenceExceeds((*worker).wi_launchtime, current_time, waittime) {
                LWLockRelease(&mut AutovacuumLock);
                LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);

                /*
                 * No other process can put a worker in starting mode, so if
                 * startingWorker is still INVALID after exchanging our lock,
                 * we assume it's the same one we saw above (so we don't
                 * recheck the launch time).
                 */
                if !(*AutoVacuumShmem).av_startingWorker.is_null() {
                    let worker2: WorkerInfo = (*AutoVacuumShmem).av_startingWorker;
                    (*worker2).wi_dboid = 0; /* InvalidOid */
                    (*worker2).wi_tableoid = 0;
                    (*worker2).wi_sharedrel = false;
                    (*worker2).wi_proc = std::ptr::null_mut();
                    (*worker2).wi_launchtime = 0;
                    dclist_push_head(
                        &mut (*AutoVacuumShmem).av_freeWorkers,
                        &mut (*worker2).wi_links,
                    );
                    (*AutoVacuumShmem).av_startingWorker = std::ptr::null_mut();
                    ereport!(
                        19, /* WARNING */
                        errmsg!("autovacuum worker took too long to start; canceled")
                    );
                }
            } else {
                can_launch = false;
            }
        }
        LWLockRelease(&mut AutovacuumLock); /* either shared or exclusive */

        /* if we can't do anything, just go back to sleep */
        if !can_launch {
            continue;
        }

        /* We're OK to start a new worker */

        if dlist_is_empty(&DatabaseList) {
            /*
             * Special case when the list is empty: start a worker right away.
             * This covers the initial case, when no database is in pgstats
             * (thus the list is empty).  Note that the constraints in
             * launcher_determine_sleep keep us from starting workers too
             * quickly (at most once every autovacuum_naptime when the list is
             * empty).
             */
            launch_worker(current_time);
        } else {
            /*
             * because rebuild_database_list constructs a list with most
             * distant adl_next_worker first, we obtain our database from the
             * tail of the list.
             */
            let avdb: *mut avl_dbase = dlist_tail_element_avl_dbase();

            /*
             * launch a worker if next_worker is right now or it is in the
             * past
             */
            if TimestampDifferenceExceeds((*avdb).adl_next_worker, current_time, 0) {
                launch_worker(current_time);
            }
        }
    }

    AutoVacLauncherShutdown();
}

/*
 * Process any new interrupts.
 */
unsafe fn ProcessAutoVacLauncherInterrupts() {
    /* the normal shutdown case */
    if ShutdownRequestPending {
        AutoVacLauncherShutdown();
    }

    if ConfigReloadPending {
        let autovacuum_max_workers_prev: c_int = autovacuum_max_workers;

        ConfigReloadPending = false;
        ProcessConfigFile(1 /* PGC_SIGHUP */);

        /* shutdown requested in config file? */
        if !AutoVacuumingActive() {
            AutoVacLauncherShutdown();
        }

        /*
         * If autovacuum_max_workers changed, emit a WARNING if
         * autovacuum_worker_slots < autovacuum_max_workers.  If it didn't
         * change, skip this to avoid too many repeated log messages.
         */
        if autovacuum_max_workers_prev != autovacuum_max_workers {
            check_av_worker_gucs();
        }

        /* rebuild the list in case the naptime changed */
        rebuild_database_list(0 /* InvalidOid */);
    }

    /* Process barrier events */
    if ProcSignalBarrierPending {
        ProcessProcSignalBarrier();
    }

    /* Perform logging of memory contexts of this process */
    if LogMemoryContextPending {
        ProcessLogMemoryContextInterrupt();
    }

    /* Process sinval catchup interrupts that happened while sleeping */
    ProcessCatchupInterrupt();
}

/*
 * Perform a normal exit from the autovac launcher.
 */
unsafe fn AutoVacLauncherShutdown() -> ! {
    ereport!(
        5, /* DEBUG1 */
        errmsg_internal!("autovacuum launcher shutting down")
    );
    (*AutoVacuumShmem).av_launcherpid = 0;

    proc_exit(0); /* done */
}

/*
 * Determine the time to sleep, based on the database list.
 *
 * The "canlaunch" parameter indicates whether we can start a worker right now,
 * for example due to the workers being all busy.  If this is false, we will
 * cause a long sleep, which will be interrupted when a worker exits.
 */
unsafe fn launcher_determine_sleep(canlaunch: bool, recursing: bool, nap: *mut libc_timeval) {
    /*
     * We sleep until the next scheduled vacuum.  We trust that when the
     * database list was built, care was taken so that no entries have times
     * in the past; if the first entry has too close a next_worker value, or a
     * time in the past, we will sleep a small nominal time.
     */
    if !canlaunch {
        (*nap).tv_sec = autovacuum_naptime as c_long;
        (*nap).tv_usec = 0;
    } else if !dlist_is_empty(&DatabaseList) {
        let current_time: TimestampTz = GetCurrentTimestamp();
        let next_wakeup: TimestampTz;
        let avdb: *mut avl_dbase = dlist_tail_element_avl_dbase();
        let mut secs: c_long = 0;
        let mut usecs: c_int = 0;

        next_wakeup = (*avdb).adl_next_worker;
        TimestampDifference(current_time, next_wakeup, &mut secs, &mut usecs);

        (*nap).tv_sec = secs;
        (*nap).tv_usec = usecs as c_long;
    } else {
        /* list is empty, sleep for whole autovacuum_naptime seconds  */
        (*nap).tv_sec = autovacuum_naptime as c_long;
        (*nap).tv_usec = 0;
    }

    /*
     * If the result is exactly zero, it means a database had an entry with
     * time in the past.  Rebuild the list so that the databases are evenly
     * distributed again, and recalculate the time to sleep.  This can happen
     * if there are more tables needing vacuum than workers, and they all take
     * longer to vacuum than autovacuum_naptime.
     *
     * We only recurse once.  rebuild_database_list should always return times
     * in the future, but it seems best not to trust too much on that.
     */
    if (*nap).tv_sec == 0 && (*nap).tv_usec == 0 && !recursing {
        rebuild_database_list(0 /* InvalidOid */);
        launcher_determine_sleep(canlaunch, true, nap);
        return;
    }

    /* The smallest time we'll allow the launcher to sleep. */
    if (*nap).tv_sec <= 0 && (*nap).tv_usec <= (MIN_AUTOVAC_SLEEPTIME * 1000.0) as c_long {
        (*nap).tv_sec = 0;
        (*nap).tv_usec = (MIN_AUTOVAC_SLEEPTIME * 1000.0) as c_long;
    }

    /*
     * If the sleep time is too large, clamp it to an arbitrary maximum (plus
     * any fractional seconds, for simplicity).  This avoids an essentially
     * infinite sleep in strange cases like the system clock going backwards a
     * few years.
     */
    if (*nap).tv_sec > MAX_AUTOVAC_SLEEPTIME {
        (*nap).tv_sec = MAX_AUTOVAC_SLEEPTIME;
    }
}

/* timeval struct mirroring libc's struct timeval */
#[repr(C)]
struct libc_timeval {
    tv_sec: c_long,
    tv_usec: c_long,
}

/*
 * Helper: return pointer to the tail element of DatabaseList as avl_dbase.
 * Mirrors C macro dlist_tail_element(avl_dbase, adl_node, &DatabaseList).
 */
unsafe fn dlist_tail_element_avl_dbase() -> *mut avl_dbase {
    /* DatabaseList.head.prev is the tail node */
    let tail_node = (*(DatabaseList._opaque.as_ptr() as *const dlist_head_internal)).tail;
    (tail_node as *mut u8).sub(std::mem::offset_of!(avl_dbase, adl_node)) as *mut avl_dbase
}

/*
 * Internal layout of dlist_head: two pointers (next/prev).
 * Matches PostgreSQL's dlist_head { dlist_node head; }.
 */
#[repr(C)]
struct dlist_head_internal {
    next: *mut dlist_node,
    tail: *mut dlist_node,
}

/*
 * Build an updated DatabaseList.  It must only contain databases that appear
 * in pgstats, and must be sorted by next_worker from highest to lowest,
 * distributed regularly across the next autovacuum_naptime interval.
 *
 * Receives the Oid of the database that made this list be generated (we call
 * this the "new" database, because when the database was already present on
 * the list, we expect that this function is not called at all).  The
 * preexisting list, if any, will be used to preserve the order of the
 * databases in the autovacuum_naptime period.  The new database is put at the
 * end of the interval.  The actual values are not saved, which should not be
 * much of a problem.
 */
unsafe fn rebuild_database_list(newdb: Oid) {
    let newcxt: MemoryContext = AllocSetContextCreate(
        AutovacMemCxt,
        b"Autovacuum database list\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );
    let tmpcxt: MemoryContext = AllocSetContextCreate(
        newcxt,
        b"Autovacuum database list (tmp)\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );
    let oldcxt: MemoryContext = MemoryContextSwitchTo(tmpcxt);

    /*
     * Implementing this is not as simple as it sounds, because we need to put
     * the new database at the end of the list; next the databases that were
     * already on the list, and finally (at the tail of the list) all the
     * other databases that are not on the existing list.
     *
     * To do this, we build an empty hash table of scored databases.  We will
     * start with the lowest score (zero) for the new database, then
     * increasing scores for the databases in the existing list, in order, and
     * lastly increasing scores for all databases gotten via
     * get_database_list() that are not already on the hash.
     *
     * Then we will put all the hash elements into an array, sort the array by
     * score, and finally put the array elements into the new doubly linked
     * list.
     */
    let hctl = HASHCTL {
        keysize: std::mem::size_of::<Oid>(),
        entrysize: std::mem::size_of::<avl_dbase>(),
        hcxt: tmpcxt,
        _pad: [0u8; 128],
    };
    let dbhash: *mut HTAB = hash_create(
        b"autovacuum db hash\0".as_ptr() as *const c_char,
        20,
        &hctl,
        0x010 | 0x200 | 0x080, /* HASH_ELEM | HASH_BLOBS | HASH_CONTEXT */
    );

    /* start by inserting the new database */
    let mut score: c_int = 0;
    if newdb != 0 {
        let entry: *mut PgStat_StatDBEntry = pgstat_fetch_stat_dbentry(newdb);
        if !entry.is_null() {
            /* only consider this database if it has a pgstat entry */
            let db: *mut avl_dbase =
                hash_search(dbhash, &newdb as *const Oid as *const c_void, 3 /*HASH_ENTER*/, std::ptr::null_mut())
                    as *mut avl_dbase;
            /* hash_search already filled in the key */
            (*db).adl_score = score;
            score += 1;
            /* next_worker is filled in later */
        }
    }

    /* Now insert the databases from the existing list */
    {
        let mut iter_node = (*(DatabaseList._opaque.as_ptr() as *const dlist_head_internal)).next;
        let sentinel = DatabaseList._opaque.as_ptr() as *mut dlist_node;
        while iter_node != sentinel && !iter_node.is_null() {
            let avdb: *mut avl_dbase =
                (iter_node as *mut u8).sub(std::mem::offset_of!(avl_dbase, adl_node)) as *mut avl_dbase;
            let next = (*(iter_node as *const dlist_head_internal)).next;

            /*
             * skip databases with no stat entries -- in particular, this gets rid
             * of dropped databases
             */
            let entry: *mut PgStat_StatDBEntry = pgstat_fetch_stat_dbentry((*avdb).adl_datid);
            if entry.is_null() {
                iter_node = next;
                continue;
            }

            let mut found: bool = false;
            let db: *mut avl_dbase = hash_search(
                dbhash,
                &(*avdb).adl_datid as *const Oid as *const c_void,
                3, /*HASH_ENTER*/
                &mut found,
            ) as *mut avl_dbase;

            if !found {
                /* hash_search already filled in the key */
                (*db).adl_score = score;
                score += 1;
                /* next_worker is filled in later */
            }
            iter_node = next;
        }
    }

    /* finally, insert all qualifying databases not previously inserted */
    let dblist: *mut List = get_database_list();
    {
        // iterate list cells
        let mut cell = list_head(dblist);
        while !cell.is_null() {
            let avdb: *mut avw_dbase = lfirst(cell) as *mut avw_dbase;
            cell = lnext(dblist, cell);

            /* only consider databases with a pgstat entry */
            let entry: *mut PgStat_StatDBEntry = pgstat_fetch_stat_dbentry((*avdb).adw_datid);
            if entry.is_null() {
                continue;
            }

            let mut found: bool = false;
            let db: *mut avl_dbase = hash_search(
                dbhash,
                &(*avdb).adw_datid as *const Oid as *const c_void,
                3, /*HASH_ENTER*/
                &mut found,
            ) as *mut avl_dbase;
            /* only update the score if the database was not already on the hash */
            if !found {
                /* hash_search already filled in the key */
                (*db).adl_score = score;
                score += 1;
                /* next_worker is filled in later */
            }
        }
    }
    let nelems: c_int = score;

    /* from here on, the allocated memory belongs to the new list */
    MemoryContextSwitchTo(newcxt);
    dlist_init(&mut DatabaseList);

    if nelems > 0 {
        let current_time: TimestampTz;
        let millis_increment: c_long;
        let dbary: *mut avl_dbase;
        let mut db: *mut avl_dbase;
        let mut seq: HASH_SEQ_STATUS = HASH_SEQ_STATUS { _opaque: [0u8; 64] };

        /* put all the hash elements into an array */
        dbary = palloc((nelems as usize) * std::mem::size_of::<avl_dbase>()) as *mut avl_dbase;

        let mut i: c_int = 0;
        hash_seq_init(&mut seq, dbhash);
        loop {
            db = hash_seq_search(&mut seq) as *mut avl_dbase;
            if db.is_null() {
                break;
            }
            std::ptr::copy_nonoverlapping(db, dbary.add(i as usize), 1);
            i += 1;
        }

        /* sort the array */
        qsort(
            dbary as *mut c_void,
            nelems as usize,
            std::mem::size_of::<avl_dbase>(),
            db_comparator,
        );

        /*
         * Determine the time interval between databases in the schedule. If
         * we see that the configured naptime would take us to sleep times
         * lower than our min sleep time (which launcher_determine_sleep is
         * coded not to allow), silently use a larger naptime (but don't touch
         * the GUC variable).
         */
        millis_increment = (1000.0 * autovacuum_naptime as f64 / nelems as f64) as c_long;
        let millis_increment = if millis_increment <= MIN_AUTOVAC_SLEEPTIME as c_long {
            (MIN_AUTOVAC_SLEEPTIME * 1.1) as c_long
        } else {
            millis_increment
        };

        let mut current_time = GetCurrentTimestamp();

        /*
         * move the elements from the array into the dlist, setting the
         * next_worker while walking the array
         */
        for idx in 0..nelems {
            db = dbary.add(idx as usize);

            current_time = TimestampTzPlusMilliseconds(current_time, millis_increment);
            (*db).adl_next_worker = current_time;

            /* later elements should go closer to the head of the list */
            dlist_push_head(&mut DatabaseList, &mut (*db).adl_node);
        }
    }

    /* all done, clean up memory */
    if !DatabaseListCxt.is_null() {
        MemoryContextDelete(DatabaseListCxt);
    }
    MemoryContextDelete(tmpcxt);
    DatabaseListCxt = newcxt;
    MemoryContextSwitchTo(oldcxt);
}

/* Helper stubs for list iteration (TODO(pg-port): use real List API) */
unsafe fn list_head(list: *mut List) -> *mut ListCell {
    if list.is_null() {
        return std::ptr::null_mut();
    }
    /* List internals: first field is NodeTag, second is length, third is head cell ptr */
    let p = list as *const u8;
    let head_ptr = p.add(8) as *const *mut ListCell; /* offset past tag+len */
    *head_ptr
}

unsafe fn lnext(_list: *mut List, cell: *mut ListCell) -> *mut ListCell {
    if cell.is_null() {
        return std::ptr::null_mut();
    }
    /* ListCell.next is the first field */
    *(cell as *const *mut ListCell)
}

/* qsort comparator for avl_dbase, using adl_score */
unsafe extern "C" fn db_comparator(a: *const c_void, b: *const c_void) -> c_int {
    pg_cmp_s32(
        (*(a as *const avl_dbase)).adl_score,
        (*(b as *const avl_dbase)).adl_score,
    )
}

/*
 * do_start_worker
 *
 * Bare-bones procedure for starting an autovacuum worker from the launcher.
 * It determines what database to work on, sets up shared memory stuff and
 * signals postmaster to start the worker.  It fails gracefully if invoked when
 * autovacuum_workers are already active.
 *
 * Return value is the OID of the database that the worker is going to process,
 * or InvalidOid if no worker was actually started.
 */
unsafe fn do_start_worker() -> Oid {
    let mut xidForceLimit: TransactionId;
    let mut multiForceLimit: MultiXactId;
    let mut for_xid_wrap: bool = false;
    let mut for_multi_wrap: bool = false;
    let mut avdb: *mut avw_dbase = std::ptr::null_mut();
    let current_time: TimestampTz;
    let mut skipit: bool = false;
    let mut retval: Oid = 0; /* InvalidOid */

    /* return quickly when there are no free workers */
    LWLockAcquire(&mut AutovacuumLock, 2 /* LW_SHARED */);
    if !av_worker_available() {
        LWLockRelease(&mut AutovacuumLock);
        return 0; /* InvalidOid */
    }
    LWLockRelease(&mut AutovacuumLock);

    /*
     * Create and switch to a temporary context to avoid leaking the memory
     * allocated for the database list.
     */
    let tmpcxt: MemoryContext = AllocSetContextCreate(
        CurrentMemoryContext,
        b"Autovacuum start worker (tmp)\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );
    let oldcxt: MemoryContext = MemoryContextSwitchTo(tmpcxt);

    /* Get a list of databases */
    let dblist: *mut List = get_database_list();

    /*
     * Determine the oldest datfrozenxid/relfrozenxid that we will allow to
     * pass without forcing a vacuum.  (This limit can be tightened for
     * particular tables, but not loosened.)
     */
    recentXid = ReadNextTransactionId();
    xidForceLimit = recentXid.wrapping_sub(autovacuum_freeze_max_age as u32);
    /* ensure it's a "normal" XID, else TransactionIdPrecedes misbehaves */
    /* this can cause the limit to go backwards by 3, but that's OK */
    if xidForceLimit < 3 /* FirstNormalTransactionId */ {
        xidForceLimit = xidForceLimit.wrapping_sub(3);
    }

    /* Also determine the oldest datminmxid we will consider. */
    recentMulti = ReadNextMultiXactId();
    multiForceLimit = recentMulti.wrapping_sub(MultiXactMemberFreezeThreshold() as u32);
    if multiForceLimit < 1 /* FirstMultiXactId */ {
        multiForceLimit = multiForceLimit.wrapping_sub(1);
    }

    /*
     * Choose a database to connect to.  We pick the database that was least
     * recently auto-vacuumed, or one that needs vacuuming to prevent Xid
     * wraparound-related data loss.  If any db at risk of Xid wraparound is
     * found, we pick the one with oldest datfrozenxid, independently of
     * autovacuum times; similarly we pick the one with the oldest datminmxid
     * if any is in MultiXactId wraparound.  Note that those in Xid wraparound
     * danger are given more priority than those in multi wraparound danger.
     *
     * Note that a database with no stats entry is not considered, except for
     * Xid wraparound purposes.  The theory is that if no one has ever
     * connected to it since the stats were last initialized, it doesn't need
     * vacuuming.
     *
     * XXX This could be improved if we had more info about whether it needs
     * vacuuming before connecting to it.  Perhaps look through the pgstats
     * data for the database's tables?  One idea is to keep track of the
     * number of new and dead tuples per database in pgstats.  However it
     * isn't clear how to construct a metric that measures that and not cause
     * starvation for less busy databases.
     */
    current_time = GetCurrentTimestamp();
    let mut cell = list_head(dblist);
    while !cell.is_null() {
        let tmp: *mut avw_dbase = lfirst(cell) as *mut avw_dbase;
        cell = lnext(dblist, cell);

        /* Check to see if this one is at risk of wraparound */
        if TransactionIdPrecedes((*tmp).adw_frozenxid, xidForceLimit) {
            if avdb.is_null()
                || TransactionIdPrecedes((*tmp).adw_frozenxid, (*avdb).adw_frozenxid)
            {
                avdb = tmp;
            }
            for_xid_wrap = true;
            continue;
        } else if for_xid_wrap {
            continue; /* ignore not-at-risk DBs */
        } else if MultiXactIdPrecedes((*tmp).adw_minmulti, multiForceLimit) {
            if avdb.is_null()
                || MultiXactIdPrecedes((*tmp).adw_minmulti, (*avdb).adw_minmulti)
            {
                avdb = tmp;
            }
            for_multi_wrap = true;
            continue;
        } else if for_multi_wrap {
            continue; /* ignore not-at-risk DBs */
        }

        /* Find pgstat entry if any */
        (*tmp).adw_entry = pgstat_fetch_stat_dbentry((*tmp).adw_datid);

        /*
         * Skip a database with no pgstat entry; it means it hasn't seen any
         * activity.
         */
        if (*tmp).adw_entry.is_null() {
            continue;
        }

        /*
         * Also, skip a database that appears on the database list as having
         * been processed recently (less than autovacuum_naptime seconds ago).
         * We do this so that we don't select a database which we just
         * selected, but that pgstat hasn't gotten around to updating the last
         * autovacuum time yet.
         */
        skipit = false;

        /* dlist_reverse_foreach over DatabaseList */
        {
            let head_internal = &DatabaseList as *const dlist_head as *const dlist_head_internal;
            let mut iter_node = (*head_internal).tail;
            let sentinel = &DatabaseList as *const dlist_head as *mut dlist_node;
            while iter_node != sentinel && !iter_node.is_null() {
                let dbp: *mut avl_dbase =
                    (iter_node as *mut u8).sub(std::mem::offset_of!(avl_dbase, adl_node))
                        as *mut avl_dbase;
                let prev = (*(iter_node as *const dlist_head_internal)).next; /* prev in reverse */

                if (*dbp).adl_datid == (*tmp).adw_datid {
                    /*
                     * Skip this database if its next_worker value falls between
                     * the current time and the current time plus naptime.
                     */
                    if !TimestampDifferenceExceeds((*dbp).adl_next_worker, current_time, 0)
                        && !TimestampDifferenceExceeds(
                            current_time,
                            (*dbp).adl_next_worker,
                            autovacuum_naptime * 1000,
                        )
                    {
                        skipit = true;
                    }
                    break;
                }
                iter_node = prev;
            }
        }
        if skipit {
            continue;
        }

        /*
         * Remember the db with oldest autovac time.  (If we are here, both
         * tmp->entry and db->entry must be non-null.)
         */
        if avdb.is_null()
            || (*(*tmp).adw_entry).last_autovac_time < (*(*avdb).adw_entry).last_autovac_time
        {
            avdb = tmp;
        }
    }

    /* Found a database -- process it */
    if !avdb.is_null() {
        let worker: WorkerInfo;
        let wptr: *mut dlist_node;

        LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);

        /*
         * Get a worker entry from the freelist.  We checked above, so there
         * really should be a free slot.
         */
        wptr = dclist_pop_head_node(&mut (*AutoVacuumShmem).av_freeWorkers);

        worker = (wptr as *mut u8).sub(std::mem::offset_of!(WorkerInfoData, wi_links))
            as *mut WorkerInfoData;
        (*worker).wi_dboid = (*avdb).adw_datid;
        (*worker).wi_proc = std::ptr::null_mut();
        (*worker).wi_launchtime = GetCurrentTimestamp();

        (*AutoVacuumShmem).av_startingWorker = worker;

        LWLockRelease(&mut AutovacuumLock);

        SendPostmasterSignal(12 /* PMSIGNAL_START_AUTOVAC_WORKER */);

        retval = (*avdb).adw_datid;
    } else if skipit {
        /*
         * If we skipped all databases on the list, rebuild it, because it
         * probably contains a dropped database.
         */
        rebuild_database_list(0 /* InvalidOid */);
    }

    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(tmpcxt);

    retval
}

/*
 * launch_worker
 *
 * Wrapper for starting a worker from the launcher.  Besides actually starting
 * it, update the database list to reflect the next time that another one will
 * need to be started on the selected database.  The actual database choice is
 * left to do_start_worker.
 *
 * This routine is also expected to insert an entry into the database list if
 * the selected database was previously absent from the list.
 */
unsafe fn launch_worker(now: TimestampTz) {
    let dbid: Oid = do_start_worker();
    if dbid != 0 /* OidIsValid */ {
        let mut found: bool = false;

        /*
         * Walk the database list and update the corresponding entry.  If the
         * database is not on the list, we'll recreate the list.
         */
        let head_internal = &DatabaseList as *const dlist_head as *const dlist_head_internal;
        let mut iter_node = (*head_internal).next;
        let sentinel = &DatabaseList as *const dlist_head as *mut dlist_node;
        while iter_node != sentinel && !iter_node.is_null() {
            let avdb: *mut avl_dbase =
                (iter_node as *mut u8).sub(std::mem::offset_of!(avl_dbase, adl_node))
                    as *mut avl_dbase;
            let next = (*(iter_node as *const dlist_head_internal)).next;

            if (*avdb).adl_datid == dbid {
                found = true;

                /*
                 * add autovacuum_naptime seconds to the current time, and use
                 * that as the new "next_worker" field for this database.
                 */
                (*avdb).adl_next_worker =
                    TimestampTzPlusMilliseconds(now, (autovacuum_naptime * 1000) as c_long);

                dlist_move_head(&mut DatabaseList, iter_node);
                break;
            }
            iter_node = next;
        }

        /*
         * If the database was not present in the database list, we rebuild
         * the list.  It's possible that the database does not get into the
         * list anyway, for example if it's a database that doesn't have a
         * pgstat entry, but this is not a problem because we don't want to
         * schedule workers regularly into those in any case.
         */
        if !found {
            rebuild_database_list(dbid);
        }
    }
}

/*
 * Called from postmaster to signal a failure to fork a process to become
 * worker.  The postmaster should kill(SIGUSR2) the launcher shortly
 * after calling this function.
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacWorkerFailed() {
    (*AutoVacuumShmem).av_signal[AutoVacuumSignal::AutoVacForkFailed as usize] = 1;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
unsafe extern "C" fn avl_sigusr2_handler(_postgres_signal_arg: c_int) {
    got_SIGUSR2 = true;
    SetLatch(MyLatch);
}


/********************************************************************
 *                  AUTOVACUUM WORKER CODE
 ********************************************************************/

/*
 * Main entry point for autovacuum worker processes.
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacWorkerMain(
    startup_data: *const c_void,
    startup_data_len: usize,
) {
    let local_sigjmp_buf: sigjmp_buf = [0u8; 148];
    let dbid: Oid;

    // Assert(startup_data_len == 0);

    /* Release postmaster's working memory context */
    if !PostmasterContext.is_null() {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = std::ptr::null_mut();
    }

    MyBackendType = 21; /* B_AUTOVAC_WORKER */
    init_ps_display(std::ptr::null());

    // Assert(GetProcessingMode() == InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    pqsignal(1 /*SIGHUP*/, Some(SignalHandlerForConfigReload));

    /*
     * SIGINT is used to signal canceling the current table's vacuum; SIGTERM
     * means abort and exit cleanly, and SIGQUIT means abandon ship.
     */
    pqsignal(2 /*SIGINT*/, Some(StatementCancelHandler));
    pqsignal(15 /*SIGTERM*/, Some(die));
    /* SIGQUIT handler was already set up by InitPostmasterChild */

    InitializeTimeouts(); /* establishes SIGALRM handler */

    pqsignal(13 /*SIGPIPE*/, None);
    pqsignal(10 /*SIGUSR1*/, Some(procsignal_sigusr1_handler));
    pqsignal(12 /*SIGUSR2*/, None);
    pqsignal(8 /*SIGFPE*/, Some(FloatExceptionHandler));
    pqsignal(17 /*SIGCHLD*/, None);

    /*
     * Create a per-backend PGPROC struct in shared memory.  We must do this
     * before we can use LWLocks or access any shared memory.
     */
    InitProcess();

    /* Early initialization */
    BaseInit();

    /*
     * If an exception is encountered, processing resumes here.
     *
     * Unlike most auxiliary processes, we don't attempt to continue
     * processing after an error; we just clean up and exit.  The autovac
     * launcher is responsible for spawning another worker later.
     *
     * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
     * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
     * signals other than SIGQUIT will be blocked until we exit.  It might
     * seem that this policy makes the HOLD_INTERRUPTS() call redundant, but
     * it is not since InterruptPending might be set already.
     */
    // if (sigsetjmp(local_sigjmp_buf, 1) != 0) { ... proc_exit(0); }
    // PG_exception_stack = &local_sigjmp_buf;
    // (C setjmp/longjmp error recovery omitted -- TODO(pg-port): wire up)

    sigprocmask(
        2, /* SIG_SETMASK */
        &UnBlockSig as *const sigset_t,
        std::ptr::null_mut(),
    );

    /*
     * Set always-secure search path, so malicious users can't redirect user
     * code (e.g. pg_index.indexprs).  (That code runs in a
     * SECURITY_RESTRICTED_OPERATION sandbox, so malicious users could not
     * take control of the entire autovacuum worker in any case.)
     */
    SetConfigOption(b"search_path\0".as_ptr() as *const c_char, b"\0".as_ptr() as *const c_char, 18, 8);

    /*
     * Force zero_damaged_pages OFF in the autovac process, even if it is set
     * in postgresql.conf.  We don't really want such a dangerous option being
     * applied non-interactively.
     */
    SetConfigOption(b"zero_damaged_pages\0".as_ptr() as *const c_char, b"false\0".as_ptr() as *const c_char, 18, 8);

    /*
     * Force settable timeouts off to avoid letting these settings prevent
     * regular maintenance from being executed.
     */
    SetConfigOption(b"statement_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);
    SetConfigOption(b"transaction_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);
    SetConfigOption(b"lock_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);
    SetConfigOption(b"idle_in_transaction_session_timeout\0".as_ptr() as *const c_char, b"0\0".as_ptr() as *const c_char, 18, 8);

    /*
     * Force default_transaction_isolation to READ COMMITTED.  We don't want
     * to pay the overhead of serializable mode, nor add any risk of causing
     * deadlocks or delaying other transactions.
     */
    SetConfigOption(
        b"default_transaction_isolation\0".as_ptr() as *const c_char,
        b"read committed\0".as_ptr() as *const c_char,
        18,
        8,
    );

    /*
     * Force synchronous replication off to allow regular maintenance even if
     * we are waiting for standbys to connect. This is important to ensure we
     * aren't blocked from performing anti-wraparound tasks.
     */
    const SYNCHRONOUS_COMMIT_LOCAL_FLUSH: c_int = 2;
    if synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH {
        SetConfigOption(b"synchronous_commit\0".as_ptr() as *const c_char, b"local\0".as_ptr() as *const c_char, 18, 8);
    }

    /*
     * Even when system is configured to use a different fetch consistency,
     * for autovac we always want fresh stats.
     */
    SetConfigOption(b"stats_fetch_consistency\0".as_ptr() as *const c_char, b"none\0".as_ptr() as *const c_char, 18, 8);

    /*
     * Get the info about the database we're going to work on.
     */
    LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);

    /*
     * beware of startingWorker being INVALID; this should normally not
     * happen, but if a worker fails after forking and before this, the
     * launcher might have decided to remove it from the queue and start
     * again.
     */
    if !(*AutoVacuumShmem).av_startingWorker.is_null() {
        MyWorkerInfo = (*AutoVacuumShmem).av_startingWorker;
        dbid = (*MyWorkerInfo).wi_dboid;
        (*MyWorkerInfo).wi_proc = MyProc;

        /* insert into the running list */
        dlist_push_head(
            &mut (*AutoVacuumShmem).av_runningWorkers,
            &mut (*MyWorkerInfo).wi_links,
        );

        /*
         * remove from the "starting" pointer, so that the launcher can start
         * a new worker if required
         */
        (*AutoVacuumShmem).av_startingWorker = std::ptr::null_mut();
        LWLockRelease(&mut AutovacuumLock);

        on_shmem_exit(FreeWorkerInfo, 0);

        /* wake up the launcher */
        if (*AutoVacuumShmem).av_launcherpid != 0 {
            kill((*AutoVacuumShmem).av_launcherpid, 12 /* SIGUSR2 */);
        }
    } else {
        /* no worker entry for me, go away */
        elog!(19 /* WARNING */, "autovacuum worker started without a worker entry");
        dbid = 0; /* InvalidOid */
        LWLockRelease(&mut AutovacuumLock);
    }

    if dbid != 0 /* OidIsValid */ {
        let mut dbname: [c_char; 64 /* NAMEDATALEN */] = [0; 64];

        /*
         * Report autovac startup to the cumulative stats system.  We
         * deliberately do this before InitPostgres, so that the
         * last_autovac_time will get updated even if the connection attempt
         * fails.  This is to prevent autovac from getting "stuck" repeatedly
         * selecting an unopenable database, rather than making any progress
         * on stuff it can connect to.
         */
        pgstat_report_autovac(dbid);

        /*
         * Connect to the selected database, specifying no particular user,
         * and ignoring datallowconn.  Collect the database's name for
         * display.
         *
         * Note: if we have selected a just-deleted database (due to using
         * stale stats info), we'll fail and exit here.
         */
        InitPostgres(
            std::ptr::null(),
            dbid,
            std::ptr::null(),
            0, /* InvalidOid */
            0x0001, /* INIT_PG_OVERRIDE_ALLOW_CONNS */
            dbname.as_mut_ptr(),
        );
        SetProcessingMode(2 /* NormalProcessing */);
        set_ps_display(dbname.as_ptr());
        ereport!(
            5, /* DEBUG1 */
            errmsg_internal!("autovacuum: processing database \"{}\"",
                std::ffi::CStr::from_ptr(dbname.as_ptr()).to_string_lossy())
        );

        if PostAuthDelay != 0 {
            pg_usleep(PostAuthDelay as c_long * 1000000);
        }

        /* And do an appropriate amount of work */
        recentXid = ReadNextTransactionId();
        recentMulti = ReadNextMultiXactId();
        do_autovacuum();
    }

    /*
     * The launcher will be notified of my death in ProcKill, *if* we managed
     * to get a worker slot at all
     */

    /* All done, go away */
    proc_exit(0);
}

/*
 * Return a WorkerInfo to the free list
 */
unsafe extern "C" fn FreeWorkerInfo(code: c_int, arg: Datum) {
    if !MyWorkerInfo.is_null() {
        LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);

        /*
         * Wake the launcher up so that he can launch a new worker immediately
         * if required.  We only save the launcher's PID in local memory here;
         * the actual signal will be sent when the PGPROC is recycled.  Note
         * that we always do this, so that the launcher can rebalance the cost
         * limit setting of the remaining workers.
         *
         * We somewhat ignore the risk that the launcher changes its PID
         * between us reading it and the actual kill; we expect ProcKill to be
         * called shortly after us, and we assume that PIDs are not reused too
         * quickly after a process exits.
         */
        AutovacuumLauncherPid = (*AutoVacuumShmem).av_launcherpid;

        dlist_delete(&mut (*MyWorkerInfo).wi_links);
        (*MyWorkerInfo).wi_dboid = 0; /* InvalidOid */
        (*MyWorkerInfo).wi_tableoid = 0;
        (*MyWorkerInfo).wi_sharedrel = false;
        (*MyWorkerInfo).wi_proc = std::ptr::null_mut();
        (*MyWorkerInfo).wi_launchtime = 0;
        pg_atomic_clear_flag(&mut (*MyWorkerInfo).wi_dobalance);
        dclist_push_head(
            &mut (*AutoVacuumShmem).av_freeWorkers,
            &mut (*MyWorkerInfo).wi_links,
        );
        /* not mine anymore */
        MyWorkerInfo = std::ptr::null_mut();

        /*
         * now that we're inactive, cause a rebalancing of the surviving
         * workers
         */
        (*AutoVacuumShmem).av_signal[AutoVacuumSignal::AutoVacRebalance as usize] = 1;
        LWLockRelease(&mut AutovacuumLock);
    }
}

/*
 * Update vacuum cost-based delay-related parameters for autovacuum workers and
 * backends executing VACUUM or ANALYZE using the value of relevant GUCs and
 * global state. This must be called during setup for vacuum and after every
 * config reload to ensure up-to-date values.
 */
#[no_mangle]
pub unsafe extern "C" fn VacuumUpdateCosts() {
    if !MyWorkerInfo.is_null() {
        if av_storage_param_cost_delay >= 0.0 {
            vacuum_cost_delay = av_storage_param_cost_delay;
        } else if autovacuum_vac_cost_delay >= 0.0 {
            vacuum_cost_delay = autovacuum_vac_cost_delay;
        } else {
            /* fall back to VacuumCostDelay */
            vacuum_cost_delay = VacuumCostDelay;
        }

        AutoVacuumUpdateCostLimit();
    } else {
        /* Must be explicit VACUUM or ANALYZE */
        vacuum_cost_delay = VacuumCostDelay;
        vacuum_cost_limit = VacuumCostLimit;
    }

    /*
     * If configuration changes are allowed to impact VacuumCostActive, make
     * sure it is updated.
     */
    if VacuumFailsafeActive {
        // Assert(!VacuumCostActive);
    } else if vacuum_cost_delay > 0.0 {
        VacuumCostActive = true;
    } else {
        VacuumCostActive = false;
        VacuumCostBalance = 0;
    }

    /*
     * Since the cost logging requires a lock, avoid rendering the log message
     * in case we are using a message level where the log wouldn't be emitted.
     */
    if !MyWorkerInfo.is_null() && message_level_is_interesting(7 /* DEBUG2 */) {
        let dboid: Oid;
        let tableoid: Oid;

        // Assert(!LWLockHeldByMe(AutovacuumLock));

        LWLockAcquire(&mut AutovacuumLock, 2 /* LW_SHARED */);
        dboid = (*MyWorkerInfo).wi_dboid;
        tableoid = (*MyWorkerInfo).wi_tableoid;
        LWLockRelease(&mut AutovacuumLock);

        elog!(
            7, /* DEBUG2 */
            "Autovacuum VacuumUpdateCosts(db={}, rel={}, dobalance={}, cost_limit={}, cost_delay={} active={} failsafe={})",
            dboid,
            tableoid,
            if pg_atomic_unlocked_test_flag(&(*MyWorkerInfo).wi_dobalance) { "no" } else { "yes" },
            vacuum_cost_limit,
            vacuum_cost_delay,
            if vacuum_cost_delay > 0.0 { "yes" } else { "no" },
            if VacuumFailsafeActive { "yes" } else { "no" }
        );
    }
}

/*
 * Update vacuum_cost_limit with the correct value for an autovacuum worker,
 * given the value of other relevant cost limit parameters and the number of
 * workers across which the limit must be balanced. Autovacuum workers must
 * call this regularly in case av_nworkersForBalance has been updated by
 * another worker or by the autovacuum launcher. They must also call it after a
 * config reload.
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacuumUpdateCostLimit() {
    if MyWorkerInfo.is_null() {
        return;
    }

    /*
     * note: in cost_limit, zero also means use value from elsewhere, because
     * zero is not a valid value.
     */

    if av_storage_param_cost_limit > 0 {
        vacuum_cost_limit = av_storage_param_cost_limit;
    } else {
        let nworkers_for_balance: u32;

        if autovacuum_vac_cost_limit > 0 {
            vacuum_cost_limit = autovacuum_vac_cost_limit;
        } else {
            vacuum_cost_limit = VacuumCostLimit;
        }

        /* Only balance limit if no cost-related storage parameters specified */
        if pg_atomic_unlocked_test_flag(&(*MyWorkerInfo).wi_dobalance) {
            return;
        }

        // Assert(vacuum_cost_limit > 0);

        nworkers_for_balance =
            pg_atomic_read_u32(&(*AutoVacuumShmem).av_nworkersForBalance);

        /* There is at least 1 autovac worker (this worker) */
        if nworkers_for_balance == 0 {
            elog!(21 /* ERROR */, "nworkers_for_balance must be > 0");
        }

        vacuum_cost_limit = std::cmp::max(
            vacuum_cost_limit / nworkers_for_balance as c_int,
            1,
        );
    }
}

/*
 * autovac_recalculate_workers_for_balance
 *      Recalculate the number of workers to consider, given cost-related
 *      storage parameters and the current number of active workers.
 *
 * Caller must hold the AutovacuumLock in at least shared mode to access
 * worker->wi_proc.
 */
unsafe fn autovac_recalculate_workers_for_balance() {
    let orig_nworkers_for_balance: u32 =
        pg_atomic_read_u32(&(*AutoVacuumShmem).av_nworkersForBalance);
    let mut nworkers_for_balance: u32 = 0;

    // Assert(LWLockHeldByMe(AutovacuumLock));

    /* dlist_foreach over av_runningWorkers */
    {
        let head_internal = &(*AutoVacuumShmem).av_runningWorkers as *const dlist_head
            as *const dlist_head_internal;
        let mut iter_node = (*head_internal).next;
        let sentinel =
            &(*AutoVacuumShmem).av_runningWorkers as *const dlist_head as *mut dlist_node;
        while iter_node != sentinel && !iter_node.is_null() {
            let worker: WorkerInfo =
                (iter_node as *mut u8).sub(std::mem::offset_of!(WorkerInfoData, wi_links))
                    as *mut WorkerInfoData;
            let next = (*(iter_node as *const dlist_head_internal)).next;

            if (*worker).wi_proc.is_null()
                || pg_atomic_unlocked_test_flag(&(*worker).wi_dobalance)
            {
                iter_node = next;
                continue;
            }

            nworkers_for_balance += 1;
            iter_node = next;
        }
    }

    if nworkers_for_balance != orig_nworkers_for_balance {
        pg_atomic_write_u32(
            &mut (*AutoVacuumShmem).av_nworkersForBalance,
            nworkers_for_balance,
        );
    }
}

/*
 * get_database_list
 *      Return a list of all databases found in pg_database.
 *
 * The list and associated data is allocated in the caller's memory context,
 * which is in charge of ensuring that it's properly cleaned up afterwards.
 *
 * Note: this is the only function in which the autovacuum launcher uses a
 * transaction.  Although we aren't attached to any particular database and
 * therefore can't access most catalogs, we do have enough infrastructure
 * to do a seqscan on pg_database.
 */
unsafe fn get_database_list() -> *mut List {
    let mut dblist: *mut List = std::ptr::null_mut();
    let rel: Relation;
    let scan: TableScanDesc;
    let mut tup: HeapTuple;
    let resultcxt: MemoryContext;

    /* This is the context that we will allocate our output data in */
    resultcxt = CurrentMemoryContext;

    /*
     * Start a transaction so we can access pg_database.
     */
    StartTransactionCommand();

    rel = table_open(1262 /* DatabaseRelationId */, 1 /* AccessShareLock */);
    scan = table_beginscan_catalog(rel, 0, std::ptr::null());

    loop {
        tup = heap_getnext(scan, 1 /* ForwardScanDirection */);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let pgdatabase: Form_pg_database = GETSTRUCT(tup) as Form_pg_database;
        let avdb: *mut avw_dbase;
        let oldcxt: MemoryContext;

        /*
         * If database has partially been dropped, we can't, nor need to,
         * vacuum it.
         */
        if database_is_invalid_form(pgdatabase) {
            elog!(
                7, /* DEBUG2 */
                "autovacuum: skipping invalid database \"{}\"",
                std::ffi::CStr::from_ptr(NameStr((*pgdatabase).datname)).to_string_lossy()
            );
            continue;
        }

        /*
         * Allocate our results in the caller's context, not the
         * transaction's. We do this inside the loop, and restore the original
         * context at the end, so that leaky things like heap_getnext() are
         * not called in a potentially long-lived context.
         */
        oldcxt = MemoryContextSwitchTo(resultcxt);

        avdb = palloc(std::mem::size_of::<avw_dbase>()) as *mut avw_dbase;

        (*avdb).adw_datid = (*pgdatabase).oid;
        (*avdb).adw_name =
            pstrdup(NameStr((*pgdatabase).datname));
        (*avdb).adw_frozenxid = (*pgdatabase).datfrozenxid;
        (*avdb).adw_minmulti = (*pgdatabase).datminmxid;
        /* this gets set later: */
        (*avdb).adw_entry = std::ptr::null_mut();

        dblist = lappend(dblist, avdb as *mut c_void);
        MemoryContextSwitchTo(oldcxt);
    }

    table_endscan(scan);
    table_close(rel, 1 /* AccessShareLock */);

    CommitTransactionCommand();

    /* Be sure to restore caller's memory context */
    MemoryContextSwitchTo(resultcxt);

    dblist
}

/*
 * Process a database table-by-table
 *
 * Note that CHECK_FOR_INTERRUPTS is supposed to be used in certain spots in
 * order not to ignore shutdown commands for too long.
 */
unsafe fn do_autovacuum() {
    let classRel: Relation;
    let mut tuple: HeapTuple;
    let relScan: TableScanDesc;
    let dbForm: Form_pg_database;
    let mut table_oids: *mut List = std::ptr::null_mut();
    let mut orphan_oids: *mut List = std::ptr::null_mut();
    let mut ctl = HASHCTL { keysize: 0, entrysize: 0, hcxt: std::ptr::null_mut(), _pad: [0u8; 128] };
    let table_toast_map: *mut HTAB;
    let bstrategy: BufferAccessStrategy;
    let mut key: ScanKeyData = ScanKeyData { _opaque: [0u8; 32] };
    let pg_class_desc: TupleDesc;
    let effective_multixact_freeze_max_age: c_int;
    let mut did_vacuum: bool = false;
    let mut found_concurrent_worker: bool = false;

    /*
     * StartTransactionCommand and CommitTransactionCommand will automatically
     * switch to other contexts.  We need this one to keep the list of
     * relations to vacuum/analyze across transactions.
     */
    AutovacMemCxt = AllocSetContextCreate(
        TopMemoryContext,
        b"Autovacuum worker\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );
    MemoryContextSwitchTo(AutovacMemCxt);

    /* Start a transaction so our commands have one to play into. */
    StartTransactionCommand();

    /*
     * This injection point is put in a transaction block to work with a wait
     * that uses a condition variable.
     */
    InjectionPointRun(b"autovacuum-worker-start\0".as_ptr() as *const c_char, std::ptr::null_mut());

    /*
     * Compute the multixact age for which freezing is urgent.  This is
     * normally autovacuum_multixact_freeze_max_age, but may be less if we are
     * short of multixact member space.
     */
    effective_multixact_freeze_max_age = MultiXactMemberFreezeThreshold();

    /*
     * Find the pg_database entry and select the default freeze ages. We use
     * zero in template and nonconnectable databases, else the system-wide
     * default.
     */
    tuple = SearchSysCache1(65 /* DATABASEOID */, ObjectIdGetDatum(MyDatabaseId));
    if !HeapTupleIsValid(tuple) {
        elog!(21 /* ERROR */, "cache lookup failed for database {}", MyDatabaseId);
    }
    dbForm = GETSTRUCT(tuple) as Form_pg_database;

    if (*dbForm).datistemplate || !(*dbForm).datallowconn {
        default_freeze_min_age = 0;
        default_freeze_table_age = 0;
        default_multixact_freeze_min_age = 0;
        default_multixact_freeze_table_age = 0;
    } else {
        default_freeze_min_age = vacuum_freeze_min_age;
        default_freeze_table_age = vacuum_freeze_table_age;
        default_multixact_freeze_min_age = vacuum_multixact_freeze_min_age;
        default_multixact_freeze_table_age = vacuum_multixact_freeze_table_age;
    }

    ReleaseSysCache(tuple);

    /* StartTransactionCommand changed elsewhere */
    MemoryContextSwitchTo(AutovacMemCxt);

    classRel = table_open(1259 /* RelationRelationId */, 1 /* AccessShareLock */);

    /* create a copy so we can use it after closing pg_class */
    pg_class_desc = CreateTupleDescCopy(RelationGetDescr(classRel));

    /* create hash table for toast <-> main relid mapping */
    ctl.keysize = std::mem::size_of::<Oid>();
    ctl.entrysize = std::mem::size_of::<av_relation>();

    table_toast_map = hash_create(
        b"TOAST to main relid map\0".as_ptr() as *const c_char,
        100,
        &ctl,
        0x010 | 0x200, /* HASH_ELEM | HASH_BLOBS */
    );

    /*
     * Scan pg_class to determine which tables to vacuum.
     *
     * We do this in two passes: on the first one we collect the list of plain
     * relations and materialized views, and on the second one we collect
     * TOAST tables. The reason for doing the second pass is that during it we
     * want to use the main relation's pg_class.reloptions entry if the TOAST
     * table does not have any, and we cannot obtain it unless we know
     * beforehand what's the main table OID.
     *
     * We need to check TOAST tables separately because in cases with short,
     * wide tables there might be proportionally much more activity in the
     * TOAST table than in its parent.
     */
    let relScan_first = table_beginscan_catalog(classRel, 0, std::ptr::null());

    /*
     * On the first pass, we collect main tables to vacuum, and also the main
     * table relid to TOAST relid mapping.
     */
    loop {
        tuple = heap_getnext(relScan_first, 1 /* ForwardScanDirection */);
        if tuple.is_null() {
            break;
        }
        let classForm: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        let tabentry: *mut PgStat_StatTabEntry;
        let relopts: *mut AutoVacOpts;
        let relid: Oid;
        let mut dovacuum: bool = false;
        let mut doanalyze: bool = false;
        let mut wraparound: bool = false;

        /* RELKIND_RELATION = 'r', RELKIND_MATVIEW = 'm' */
        if (*classForm).relkind != b'r' as c_char && (*classForm).relkind != b'm' as c_char {
            continue;
        }

        relid = (*classForm).oid;

        /*
         * Check if it is a temp table (presumably, of some other backend's).
         * We cannot safely process other backends' temp tables.
         */
        if (*classForm).relpersistence == b't' as c_char /* RELPERSISTENCE_TEMP */ {
            /*
             * We just ignore it if the owning backend is still active and
             * using the temporary schema.  Also, for safety, ignore it if the
             * namespace doesn't exist or isn't a temp namespace after all.
             */
            const TEMP_NAMESPACE_IDLE: c_int = 2;
            if checkTempNamespaceStatus((*classForm).relnamespace) == TEMP_NAMESPACE_IDLE {
                /*
                 * The table seems to be orphaned -- although it might be that
                 * the owning backend has already deleted it and exited; our
                 * pg_class scan snapshot is not necessarily up-to-date
                 * anymore, so we could be looking at a committed-dead entry.
                 * Remember it so we can try to delete it later.
                 */
                orphan_oids = lappend_oid(orphan_oids, relid);
            }
            continue;
        }

        /* Fetch reloptions and the pgstat entry for this table */
        relopts = extract_autovac_opts(tuple, pg_class_desc);
        tabentry = pgstat_fetch_stat_tabentry_ext((*classForm).relisshared, relid);

        /* Check if it needs vacuum or analyze */
        relation_needs_vacanalyze(relid, relopts, classForm, tabentry,
                                   effective_multixact_freeze_max_age,
                                   &mut dovacuum, &mut doanalyze, &mut wraparound);

        /* Relations that need work are added to table_oids */
        if dovacuum || doanalyze {
            table_oids = lappend_oid(table_oids, relid);
        }

        /*
         * Remember TOAST associations for the second pass.  Note: we must do
         * this whether or not the table is going to be vacuumed, because we
         * don't automatically vacuum toast tables along the parent table.
         */
        if (*classForm).reltoastrelid != 0 /* OidIsValid */ {
            let mut found: bool = false;
            let hentry: *mut av_relation = hash_search(
                table_toast_map,
                &(*classForm).reltoastrelid as *const Oid as *const c_void,
                3, /* HASH_ENTER */
                &mut found,
            ) as *mut av_relation;

            if !found {
                /* hash_search already filled in the key */
                (*hentry).ar_relid = relid;
                (*hentry).ar_hasrelopts = false;
                if !relopts.is_null() {
                    (*hentry).ar_hasrelopts = true;
                    std::ptr::copy_nonoverlapping(relopts, &mut (*hentry).ar_reloptions, 1);
                }
            }
        }

        /* Release stuff to avoid per-relation leakage */
        if !relopts.is_null() {
            pfree(relopts as *mut c_void);
        }
        if !tabentry.is_null() {
            pfree(tabentry as *mut c_void);
        }
    }

    table_endscan(relScan_first);

    /* second pass: check TOAST tables */
    /* Anum_pg_class_relkind = 18, BTEqualStrategyNumber = 3, F_CHAREQ, RELKIND_TOASTVALUE = 't' */
    ScanKeyInit(
        &mut key,
        18, /* Anum_pg_class_relkind */
        3,  /* BTEqualStrategyNumber */
        1109 /* F_CHAREQ */,
        b't' as u8 as usize, /* CharGetDatum(RELKIND_TOASTVALUE) */
    );

    let relScan_toast = table_beginscan_catalog(classRel, 1, &key);
    loop {
        tuple = heap_getnext(relScan_toast, 1 /* ForwardScanDirection */);
        if tuple.is_null() {
            break;
        }
        let classForm: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        let tabentry: *mut PgStat_StatTabEntry;
        let relid: Oid;
        let mut relopts: *mut AutoVacOpts;
        let mut free_relopts: bool = false;
        let mut dovacuum: bool = false;
        let mut doanalyze: bool = false;
        let mut wraparound: bool = false;

        /*
         * We cannot safely process other backends' temp tables, so skip 'em.
         */
        if (*classForm).relpersistence == b't' as c_char /* RELPERSISTENCE_TEMP */ {
            continue;
        }

        relid = (*classForm).oid;

        /*
         * fetch reloptions -- if this toast table does not have them, try the
         * main rel
         */
        relopts = extract_autovac_opts(tuple, pg_class_desc);
        if !relopts.is_null() {
            free_relopts = true;
        } else {
            let mut found: bool = false;
            let hentry: *mut av_relation = hash_search(
                table_toast_map,
                &relid as *const Oid as *const c_void,
                2, /* HASH_FIND */
                &mut found,
            ) as *mut av_relation;
            if found && (*hentry).ar_hasrelopts {
                relopts = &mut (*hentry).ar_reloptions;
            }
        }

        /* Fetch the pgstat entry for this table */
        tabentry = pgstat_fetch_stat_tabentry_ext((*classForm).relisshared, relid);

        relation_needs_vacanalyze(relid, relopts, classForm, tabentry,
                                   effective_multixact_freeze_max_age,
                                   &mut dovacuum, &mut doanalyze, &mut wraparound);

        /* ignore analyze for toast tables */
        if dovacuum {
            table_oids = lappend_oid(table_oids, relid);
        }

        /* Release stuff to avoid leakage */
        if free_relopts {
            pfree(relopts as *mut c_void);
        }
        if !tabentry.is_null() {
            pfree(tabentry as *mut c_void);
        }
    }

    table_endscan(relScan_toast);
    table_close(classRel, 1 /* AccessShareLock */);

    /*
     * Recheck orphan temporary tables, and if they still seem orphaned, drop
     * them.  We'll eat a transaction per dropped table, which might seem
     * excessive, but we should only need to do anything as a result of a
     * previous backend crash, so this should not happen often enough to
     * justify "optimizing".  Using separate transactions ensures that we
     * don't bloat the lock table if there are many temp tables to be dropped,
     * and it ensures that we don't lose work if a deletion attempt fails.
     */
    {
        let mut cell = list_head(orphan_oids);
        while !cell.is_null() {
            let relid: Oid = lfirst_oid(cell);
            cell = lnext(orphan_oids, cell);

            let classForm: Form_pg_class;
            let mut object = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

            /*
             * Check for user-requested abort.
             */
            /* CHECK_FOR_INTERRUPTS() -- omitted: TODO(pg-port) */

            /*
             * Try to lock the table.  If we can't get the lock immediately,
             * somebody else is using (or dropping) the table, so it's not our
             * concern anymore.  Having the lock prevents race conditions below.
             */
            const AccessExclusiveLock: c_int = 8;
            const AccessShareLock: c_int = 1;
            if !ConditionalLockRelationOid(relid, AccessExclusiveLock) {
                continue;
            }

            /*
             * Re-fetch the pg_class tuple and re-check whether it still seems to
             * be an orphaned temp table.  If it's not there or no longer the same
             * relation, ignore it.
             */
            tuple = SearchSysCacheCopy1(66 /* RELOID */, ObjectIdGetDatum(relid));
            if !HeapTupleIsValid(tuple) {
                /* be sure to drop useless lock so we don't bloat lock table */
                UnlockRelationOid(relid, AccessExclusiveLock);
                continue;
            }
            classForm = GETSTRUCT(tuple) as Form_pg_class;

            /*
             * Make all the same tests made in the loop above.  In event of OID
             * counter wraparound, the pg_class entry we have now might be
             * completely unrelated to the one we saw before.
             */
            if !(((*classForm).relkind == b'r' as c_char /* RELKIND_RELATION */
                  || (*classForm).relkind == b'm' as c_char /* RELKIND_MATVIEW */)
                 && (*classForm).relpersistence == b't' as c_char /* RELPERSISTENCE_TEMP */) {
                UnlockRelationOid(relid, AccessExclusiveLock);
                continue;
            }

            const TEMP_NAMESPACE_IDLE: c_int = 2;
            if checkTempNamespaceStatus((*classForm).relnamespace) != TEMP_NAMESPACE_IDLE {
                UnlockRelationOid(relid, AccessExclusiveLock);
                continue;
            }

            /*
             * Try to lock the temp namespace, too.  Even though we have lock on
             * the table itself, there's a risk of deadlock against an incoming
             * backend trying to clean out the temp namespace, in case this table
             * has dependencies (such as sequences) that the backend's
             * performDeletion call might visit in a different order.  If we can
             * get AccessShareLock on the namespace, that's sufficient to ensure
             * we're not running concurrently with RemoveTempRelations.  If we
             * can't, back off and let RemoveTempRelations do its thing.
             */
            if !ConditionalLockDatabaseObject(
                2615, /* NamespaceRelationId */
                (*classForm).relnamespace,
                0,
                AccessShareLock,
            ) {
                UnlockRelationOid(relid, AccessExclusiveLock);
                continue;
            }

            /* OK, let's delete it */
            ereport!(
                17, /* LOG */
                errmsg!("autovacuum: dropping orphan temp table \"{}.{}.{}\"",
                    std::ffi::CStr::from_ptr(get_database_name(MyDatabaseId)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(get_namespace_name((*classForm).relnamespace)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(NameStr((*classForm).relname)).to_string_lossy()
                )
            );

            /*
             * Deletion might involve TOAST table access, so ensure we have a
             * valid snapshot.
             */
            PushActiveSnapshot(GetTransactionSnapshot());

            object.classId = 1259; /* RelationRelationId */
            object.objectId = relid;
            object.objectSubId = 0;
            performDeletion(
                &object,
                1,  /* DROP_CASCADE */
                0x0001 | 0x0002 | 0x0010, /* PERFORM_DELETION_INTERNAL | PERFORM_DELETION_QUIETLY | PERFORM_DELETION_SKIP_EXTENSIONS */
            );

            /*
             * To commit the deletion, end current transaction and start a new
             * one.  Note this also releases the locks we took.
             */
            PopActiveSnapshot();
            CommitTransactionCommand();
            StartTransactionCommand();

            /* StartTransactionCommand changed current memory context */
            MemoryContextSwitchTo(AutovacMemCxt);
        }
    }

    /*
     * Optionally, create a buffer access strategy object for VACUUM to use.
     * We use the same BufferAccessStrategy object for all tables VACUUMed by
     * this worker to prevent autovacuum from blowing out shared buffers.
     *
     * VacuumBufferUsageLimit being set to 0 results in
     * GetAccessStrategyWithSize returning NULL, effectively meaning we can
     * use up to all of shared buffers.
     *
     * If we later enter failsafe mode on any of the tables being vacuumed, we
     * will cease use of the BufferAccessStrategy only for that table.
     *
     * XXX should we consider adding code to adjust the size of this if
     * VacuumBufferUsageLimit changes?
     */
    bstrategy = GetAccessStrategyWithSize(2 /* BAS_VACUUM */, VacuumBufferUsageLimit);

    /*
     * create a memory context to act as fake PortalContext, so that the
     * contexts created in the vacuum code are cleaned up for each table.
     */
    PortalContext = AllocSetContextCreate(
        AutovacMemCxt,
        b"Autovacuum Portal\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );

    /*
     * Perform operations on collected tables.
     */
    {
        let mut cell = list_head(table_oids);
        while !cell.is_null() {
            let relid: Oid = lfirst_oid(cell);
            cell = lnext(table_oids, cell);

            let classTup: HeapTuple;
            let tab: *mut autovac_table;
            let isshared: bool;
            let mut skipit: bool;
            let mut iter: dlist_iter;

            /* CHECK_FOR_INTERRUPTS() -- omitted: TODO(pg-port) */

            /*
             * Check for config changes before processing each collected table.
             */
            if ConfigReloadPending {
                ConfigReloadPending = false;
                ProcessConfigFile(1 /* PGC_SIGHUP */);

                /*
                 * You might be tempted to bail out if we see autovacuum is now
                 * disabled.  Must resist that temptation -- this might be a
                 * for-wraparound emergency worker, in which case that would be
                 * entirely inappropriate.
                 */
            }

            /*
             * Find out whether the table is shared or not.  (It's slightly
             * annoying to fetch the syscache entry just for this, but in typical
             * cases it adds little cost because table_recheck_autovac would
             * refetch the entry anyway.  We could buy that back by copying the
             * tuple here and passing it to table_recheck_autovac, but that
             * increases the odds of that function working with stale data.)
             */
            classTup = SearchSysCache1(66 /* RELOID */, ObjectIdGetDatum(relid));
            if !HeapTupleIsValid(classTup) {
                continue; /* somebody deleted the rel, forget it */
            }
            isshared = (*(GETSTRUCT(classTup) as Form_pg_class)).relisshared;
            ReleaseSysCache(classTup);

            /*
             * Hold schedule lock from here until we've claimed the table.  We
             * also need the AutovacuumLock to walk the worker array, but that one
             * can just be a shared lock.
             */
            LWLockAcquire(&mut AutovacuumScheduleLock, 1 /* LW_EXCLUSIVE */);
            LWLockAcquire(&mut AutovacuumLock, 2 /* LW_SHARED */);

            /*
             * Check whether the table is being vacuumed concurrently by another
             * worker.
             */
            skipit = false;
            {
                let head_internal = &(*AutoVacuumShmem).av_runningWorkers as *const dlist_head
                    as *const dlist_head_internal;
                let mut iter_node = (*head_internal).next;
                let sentinel = &(*AutoVacuumShmem).av_runningWorkers as *const dlist_head as *mut dlist_node;
                while iter_node != sentinel && !iter_node.is_null() {
                    let worker: WorkerInfo =
                        (iter_node as *mut u8).sub(std::mem::offset_of!(WorkerInfoData, wi_links))
                            as *mut WorkerInfoData;
                    let next = (*(iter_node as *const dlist_head_internal)).next;

                    /* ignore myself */
                    if worker == MyWorkerInfo {
                        iter_node = next;
                        continue;
                    }

                    /* ignore workers in other databases (unless table is shared) */
                    if !(*worker).wi_sharedrel && (*worker).wi_dboid != MyDatabaseId {
                        iter_node = next;
                        continue;
                    }

                    if (*worker).wi_tableoid == relid {
                        skipit = true;
                        found_concurrent_worker = true;
                        break;
                    }
                    iter_node = next;
                }
            }
            LWLockRelease(&mut AutovacuumLock);
            if skipit {
                LWLockRelease(&mut AutovacuumScheduleLock);
                continue;
            }

            /*
             * Store the table's OID in shared memory before releasing the
             * schedule lock, so that other workers don't try to vacuum it
             * concurrently.  (We claim it here so as not to hold
             * AutovacuumScheduleLock while rechecking the stats.)
             */
            (*MyWorkerInfo).wi_tableoid = relid;
            (*MyWorkerInfo).wi_sharedrel = isshared;
            LWLockRelease(&mut AutovacuumScheduleLock);

            /*
             * Check whether pgstat data still says we need to vacuum this table.
             * It could have changed if something else processed the table while
             * we weren't looking. This doesn't entirely close the race condition,
             * but it is very small.
             */
            MemoryContextSwitchTo(AutovacMemCxt);
            tab = table_recheck_autovac(relid, table_toast_map, pg_class_desc,
                                         effective_multixact_freeze_max_age);
            if tab.is_null() {
                /* someone else vacuumed the table, or it went away */
                LWLockAcquire(&mut AutovacuumScheduleLock, 1 /* LW_EXCLUSIVE */);
                (*MyWorkerInfo).wi_tableoid = 0; /* InvalidOid */
                (*MyWorkerInfo).wi_sharedrel = false;
                LWLockRelease(&mut AutovacuumScheduleLock);
                continue;
            }

            /*
             * Save the cost-related storage parameter values in global variables
             * for reference when updating vacuum_cost_delay and vacuum_cost_limit
             * during vacuuming this table.
             */
            av_storage_param_cost_delay = (*tab).at_storage_param_vac_cost_delay;
            av_storage_param_cost_limit = (*tab).at_storage_param_vac_cost_limit;

            /*
             * We only expect this worker to ever set the flag, so don't bother
             * checking the return value. We shouldn't have to retry.
             */
            if (*tab).at_dobalance {
                pg_atomic_test_set_flag(&mut (*MyWorkerInfo).wi_dobalance);
            } else {
                pg_atomic_clear_flag(&mut (*MyWorkerInfo).wi_dobalance);
            }

            LWLockAcquire(&mut AutovacuumLock, 2 /* LW_SHARED */);
            autovac_recalculate_workers_for_balance();
            LWLockRelease(&mut AutovacuumLock);

            /*
             * We wait until this point to update cost delay and cost limit
             * values, even though we reloaded the configuration file above, so
             * that we can take into account the cost-related storage parameters.
             */
            VacuumUpdateCosts();

            /* clean up memory before each iteration */
            MemoryContextReset(PortalContext);

            /*
             * Save the relation name for a possible error message, to avoid a
             * catalog lookup in case of an error.  If any of these return NULL,
             * then the relation has been dropped since last we checked; skip it.
             * Note: they must live in a long-lived memory context because we call
             * vacuum and analyze in different transactions.
             */

            (*tab).at_relname = get_rel_name((*tab).at_relid);
            (*tab).at_nspname = get_namespace_name(get_rel_namespace((*tab).at_relid));
            (*tab).at_datname = get_database_name(MyDatabaseId);
            if (*tab).at_relname.is_null() || (*tab).at_nspname.is_null() || (*tab).at_datname.is_null() {
                // goto deleted -- jump past the vacuum call
            } else {
                /*
                 * We will abort vacuuming the current table if something errors out,
                 * and continue with the next one in schedule; in particular, this
                 * happens if we are interrupted with SIGINT.
                 */
                /* PG_TRY/PG_CATCH omitted: TODO(pg-port) */
                /* Use PortalContext for any per-table allocations */
                MemoryContextSwitchTo(PortalContext);

                /* have at it */
                autovacuum_do_vac_analyze(tab, bstrategy);

                /*
                 * Clear a possible query-cancel signal, to avoid a late reaction
                 * to an automatically-sent signal because of vacuuming the
                 * current table (we're done with it, so it would make no sense to
                 * cancel at this point.)
                 */
                QueryCancelPending = false;
            }

            /* Make sure we're back in AutovacMemCxt */
            MemoryContextSwitchTo(AutovacMemCxt);

            did_vacuum = true;

            /* ProcGlobal->statusFlags[i] are reset at the next end of xact */

            /* be tidy */
            /* deleted: */
            if !(*tab).at_datname.is_null() {
                pfree((*tab).at_datname as *mut c_void);
            }
            if !(*tab).at_nspname.is_null() {
                pfree((*tab).at_nspname as *mut c_void);
            }
            if !(*tab).at_relname.is_null() {
                pfree((*tab).at_relname as *mut c_void);
            }
            pfree(tab as *mut c_void);

            /*
             * Remove my info from shared memory.  We set wi_dobalance on the
             * assumption that we are more likely than not to vacuum a table with
             * no cost-related storage parameters next, so we want to claim our
             * share of I/O as soon as possible to avoid thrashing the global
             * balance.
             */
            LWLockAcquire(&mut AutovacuumScheduleLock, 1 /* LW_EXCLUSIVE */);
            (*MyWorkerInfo).wi_tableoid = 0; /* InvalidOid */
            (*MyWorkerInfo).wi_sharedrel = false;
            LWLockRelease(&mut AutovacuumScheduleLock);
            pg_atomic_test_set_flag(&mut (*MyWorkerInfo).wi_dobalance);
        }
    }

    list_free(table_oids);

    /*
     * Perform additional work items, as requested by backends.
     */
    LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);
    for i in 0..NUM_WORKITEMS {
        let workitem: *mut AutoVacuumWorkItem = &mut (*AutoVacuumShmem).av_workItems[i];

        if !(*workitem).avw_used {
            continue;
        }
        if (*workitem).avw_active {
            continue;
        }
        if (*workitem).avw_database != MyDatabaseId {
            continue;
        }

        /* claim this one, and release lock while performing it */
        (*workitem).avw_active = true;
        LWLockRelease(&mut AutovacuumLock);

        PushActiveSnapshot(GetTransactionSnapshot());
        perform_work_item(workitem);
        if ActiveSnapshotSet() { /* transaction could have aborted */
            PopActiveSnapshot();
        }

        /*
         * Check for config changes before acquiring lock for further jobs.
         */
        /* CHECK_FOR_INTERRUPTS() -- omitted: TODO(pg-port) */
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(1 /* PGC_SIGHUP */);
            VacuumUpdateCosts();
        }

        LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);

        /* and mark it done */
        (*workitem).avw_active = false;
        (*workitem).avw_used = false;
    }
    LWLockRelease(&mut AutovacuumLock);

    /*
     * We leak table_toast_map here (among other things), but since we're
     * going away soon, it's not a problem.
     */

    /*
     * Update pg_database.datfrozenxid, and truncate pg_xact if possible. We
     * only need to do this once, not after each table.
     *
     * Even if we didn't vacuum anything, it may still be important to do
     * this, because one indirect effect of vac_update_datfrozenxid() is to
     * update TransamVariables->xidVacLimit.  That might need to be done even
     * if we haven't vacuumed anything, because relations with older
     * relfrozenxid values or other databases with older datfrozenxid values
     * might have been dropped, allowing xidVacLimit to advance.
     *
     * However, it's also important not to do this blindly in all cases,
     * because when autovacuum=off this will restart the autovacuum launcher.
     * If we're not careful, an infinite loop can result, where workers find
     * no work to do and restart the launcher, which starts another worker in
     * the same database that finds no work to do.  To prevent that, we skip
     * this if (1) we found no work to do and (2) we skipped at least one
     * table due to concurrent autovacuum activity.  In that case, the other
     * worker has already done it, or will do so when it finishes.
     */
    if did_vacuum || !found_concurrent_worker {
        vac_update_datfrozenxid();
    }

    /* Finally close out the last transaction. */
    CommitTransactionCommand();
}

/*
 * Execute a previously registered work item.
 */
unsafe fn perform_work_item(workitem: *mut AutoVacuumWorkItem) {
    let mut cur_datname: *mut c_char = std::ptr::null_mut();
    let mut cur_nspname: *mut c_char = std::ptr::null_mut();
    let mut cur_relname: *mut c_char = std::ptr::null_mut();

    /*
     * Note we do not store table info in MyWorkerInfo, since this is not
     * vacuuming proper.
     */

    /*
     * Save the relation name for a possible error message, to avoid a catalog
     * lookup in case of an error.  If any of these return NULL, then the
     * relation has been dropped since last we checked; skip it.
     */
    // Assert(CurrentMemoryContext == AutovacMemCxt);

    cur_relname = get_rel_name((*workitem).avw_relation);
    cur_nspname = get_namespace_name(get_rel_namespace((*workitem).avw_relation));
    cur_datname = get_database_name(MyDatabaseId);
    if cur_relname.is_null() || cur_nspname.is_null() || cur_datname.is_null() {
        // goto deleted2
    } else {
        autovac_report_workitem(workitem, cur_nspname, cur_relname);

        /* clean up memory before each work item */
        MemoryContextReset(PortalContext);

        /*
         * We will abort the current work item if something errors out, and
         * continue with the next one; in particular, this happens if we are
         * interrupted with SIGINT.  Note that this means that the work item list
         * can be lossy.
         */
        /* PG_TRY/PG_CATCH omitted: TODO(pg-port) */
        /* Use PortalContext for any per-work-item allocations */
        MemoryContextSwitchTo(PortalContext);

        /*
         * Have at it.  Functions called here are responsible for any required
         * user switch and sandbox.
         */
        match (*workitem).avw_type {
            AutoVacuumWorkItemType::AVW_BRINSummarizeRange => {
                DirectFunctionCall2(
                    brin_summarize_range,
                    ObjectIdGetDatum((*workitem).avw_relation),
                    Int64GetDatum((*workitem).avw_blockNumber as i64),
                );
            }
            _ => {
                elog!(
                    19, /* WARNING */
                    "unrecognized work item found: type {}",
                    (*workitem).avw_type as c_int
                );
            }
        }

        /*
         * Clear a possible query-cancel signal, to avoid a late reaction to
         * an automatically-sent signal because of vacuuming the current table
         * (we're done with it, so it would make no sense to cancel at this
         * point.)
         */
        QueryCancelPending = false;
    }

    /* Make sure we're back in AutovacMemCxt */
    MemoryContextSwitchTo(AutovacMemCxt);

    /* We intentionally do not set did_vacuum here */

    /* be tidy */
    /* deleted2: */
    if !cur_datname.is_null() {
        pfree(cur_datname as *mut c_void);
    }
    if !cur_nspname.is_null() {
        pfree(cur_nspname as *mut c_void);
    }
    if !cur_relname.is_null() {
        pfree(cur_relname as *mut c_void);
    }
}

/*
 * extract_autovac_opts
 *
 * Given a relation's pg_class tuple, return a palloc'd copy of the
 * AutoVacOpts portion of reloptions, if set; otherwise, return NULL.
 *
 * Note: callers do not have a relation lock on the table at this point,
 * so the table could have been dropped, and its catalog rows gone, after
 * we acquired the pg_class row.  If pg_class had a TOAST table, this would
 * be a risk; fortunately, it doesn't.
 */
unsafe fn extract_autovac_opts(tup: HeapTuple, pg_class_desc: TupleDesc) -> *mut AutoVacOpts {
    let relopts: *mut bytea;
    let av: *mut AutoVacOpts;

    relopts = extractRelOptions(tup, pg_class_desc, std::ptr::null_mut());
    if relopts.is_null() {
        return std::ptr::null_mut();
    }

    av = palloc(std::mem::size_of::<AutoVacOpts>()) as *mut AutoVacOpts;
    /* copy the autovacuum field from StdRdOptions */
    let stdopts = relopts as *const StdRdOptions;
    std::ptr::copy_nonoverlapping(&(*stdopts).autovacuum, av, 1);
    pfree(relopts as *mut c_void);

    av
}


/*
 * table_recheck_autovac
 *
 * Recheck whether a table still needs vacuum or analyze.  Return value is a
 * valid autovac_table pointer if it does, NULL otherwise.
 *
 * Note that the returned autovac_table does not have the name fields set.
 */
unsafe fn table_recheck_autovac(
    relid: Oid,
    table_toast_map: *mut HTAB,
    pg_class_desc: TupleDesc,
    effective_multixact_freeze_max_age: c_int,
) -> *mut autovac_table {
    let classForm: Form_pg_class;
    let classTup: HeapTuple;
    let mut dovacuum: bool = false;
    let mut doanalyze: bool = false;
    let mut tab: *mut autovac_table = std::ptr::null_mut();
    let mut wraparound: bool = false;
    let mut avopts: *mut AutoVacOpts;
    let mut free_avopts: bool = false;

    /* fetch the relation's relcache entry */
    classTup = SearchSysCacheCopy1(66 /* RELOID */, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(classTup) {
        return std::ptr::null_mut();
    }
    classForm = GETSTRUCT(classTup) as Form_pg_class;

    /*
     * Get the applicable reloptions.  If it is a TOAST table, try to get the
     * main table reloptions if the toast table itself doesn't have.
     */
    avopts = extract_autovac_opts(classTup, pg_class_desc);
    if !avopts.is_null() {
        free_avopts = true;
    } else if (*classForm).relkind == b't' as c_char /* RELKIND_TOASTVALUE */
        && !table_toast_map.is_null()
    {
        let mut found: bool = false;
        let hentry: *mut av_relation = hash_search(
            table_toast_map,
            &relid as *const Oid as *const c_void,
            2, /* HASH_FIND */
            &mut found,
        ) as *mut av_relation;
        if found && (*hentry).ar_hasrelopts {
            avopts = &mut (*hentry).ar_reloptions;
        }
    }

    recheck_relation_needs_vacanalyze(
        relid,
        avopts,
        classForm,
        effective_multixact_freeze_max_age,
        &mut dovacuum,
        &mut doanalyze,
        &mut wraparound,
    );

    /* OK, it needs something done */
    if doanalyze || dovacuum {
        let freeze_min_age: c_int;
        let freeze_table_age: c_int;
        let multixact_freeze_min_age: c_int;
        let multixact_freeze_table_age: c_int;
        let log_min_duration: c_int;

        /*
         * Calculate the vacuum cost parameters and the freeze ages.  If there
         * are options set in pg_class.reloptions, use them; in the case of a
         * toast table, try the main table too.  Otherwise use the GUC
         * defaults, autovacuum's own first and plain vacuum second.
         */

        /* -1 in autovac setting means use log_autovacuum_min_duration */
        log_min_duration = if !avopts.is_null() && (*avopts).log_min_duration >= 0 {
            (*avopts).log_min_duration
        } else {
            Log_autovacuum_min_duration
        };

        /* these do not have autovacuum-specific settings */
        freeze_min_age = if !avopts.is_null() && (*avopts).freeze_min_age >= 0 {
            (*avopts).freeze_min_age
        } else {
            default_freeze_min_age
        };

        freeze_table_age = if !avopts.is_null() && (*avopts).freeze_table_age >= 0 {
            (*avopts).freeze_table_age
        } else {
            default_freeze_table_age
        };

        multixact_freeze_min_age = if !avopts.is_null() && (*avopts).multixact_freeze_min_age >= 0 {
            (*avopts).multixact_freeze_min_age
        } else {
            default_multixact_freeze_min_age
        };

        multixact_freeze_table_age = if !avopts.is_null() && (*avopts).multixact_freeze_table_age >= 0 {
            (*avopts).multixact_freeze_table_age
        } else {
            default_multixact_freeze_table_age
        };

        tab = palloc(std::mem::size_of::<autovac_table>()) as *mut autovac_table;
        (*tab).at_relid = relid;
        (*tab).at_sharedrel = (*classForm).relisshared;

        /*
         * Select VACUUM options.  Note we don't say VACOPT_PROCESS_TOAST, so
         * that vacuum() skips toast relations.  Also note we tell vacuum() to
         * skip vac_update_datfrozenxid(); we'll do that separately.
         */
        /* VACOPT_VACUUM=0x01, VACOPT_PROCESS_MAIN=0x20, VACOPT_SKIP_DATABASE_STATS=0x80,
           VACOPT_ANALYZE=0x02, VACOPT_SKIP_LOCKED=0x08 */
        (*tab).at_params.options =
            (if dovacuum { 0x01 | 0x20 | 0x80 } else { 0 })
            | (if doanalyze { 0x02 } else { 0 })
            | (if !wraparound { 0x08 } else { 0 });

        /*
         * index_cleanup and truncate are unspecified at first in autovacuum.
         * They will be filled in with usable values using their reloptions
         * (or reloption defaults) later.
         */
        (*tab).at_params.index_cleanup = 0; /* VACOPTVALUE_UNSPECIFIED */
        (*tab).at_params.truncate = 0;      /* VACOPTVALUE_UNSPECIFIED */
        /* As of now, we don't support parallel vacuum for autovacuum */
        (*tab).at_params.nworkers = -1;
        (*tab).at_params.freeze_min_age = freeze_min_age;
        (*tab).at_params.freeze_table_age = freeze_table_age;
        (*tab).at_params.multixact_freeze_min_age = multixact_freeze_min_age;
        (*tab).at_params.multixact_freeze_table_age = multixact_freeze_table_age;
        (*tab).at_params.is_wraparound = wraparound;
        (*tab).at_params.log_min_duration = log_min_duration;
        (*tab).at_params.toast_parent = 0; /* InvalidOid */

        /*
         * Later, in vacuum_rel(), we check reloptions for any
         * vacuum_max_eager_freeze_failure_rate override.
         */
        (*tab).at_params.max_eager_freeze_failure_rate = vacuum_max_eager_freeze_failure_rate;
        (*tab).at_storage_param_vac_cost_limit = if !avopts.is_null() {
            (*avopts).vacuum_cost_limit
        } else {
            0
        };
        (*tab).at_storage_param_vac_cost_delay = if !avopts.is_null() {
            (*avopts).vacuum_cost_delay
        } else {
            -1.0
        };
        (*tab).at_relname = std::ptr::null_mut();
        (*tab).at_nspname = std::ptr::null_mut();
        (*tab).at_datname = std::ptr::null_mut();

        /*
         * If any of the cost delay parameters has been set individually for
         * this table, disable the balancing algorithm.
         */
        (*tab).at_dobalance = !(!avopts.is_null()
            && ((*avopts).vacuum_cost_limit > 0 || (*avopts).vacuum_cost_delay >= 0.0));
    }

    if free_avopts {
        pfree(avopts as *mut c_void);
    }
    heap_freetuple(classTup);
    tab
}

/*
 * recheck_relation_needs_vacanalyze
 *
 * Subroutine for table_recheck_autovac.
 *
 * Fetch the pgstat of a relation and recheck whether a relation
 * needs to be vacuumed or analyzed.
 */
unsafe fn recheck_relation_needs_vacanalyze(
    relid: Oid,
    avopts: *mut AutoVacOpts,
    classForm: Form_pg_class,
    effective_multixact_freeze_max_age: c_int,
    dovacuum: *mut bool,
    doanalyze: *mut bool,
    wraparound: *mut bool,
) {
    let tabentry: *mut PgStat_StatTabEntry;

    /* fetch the pgstat table entry */
    tabentry = pgstat_fetch_stat_tabentry_ext((*classForm).relisshared, relid);

    relation_needs_vacanalyze(
        relid,
        avopts,
        classForm,
        tabentry,
        effective_multixact_freeze_max_age,
        dovacuum,
        doanalyze,
        wraparound,
    );

    /* Release tabentry to avoid leakage */
    if !tabentry.is_null() {
        pfree(tabentry as *mut c_void);
    }

    /* ignore ANALYZE for toast tables */
    if (*classForm).relkind == b't' as c_char /* RELKIND_TOASTVALUE */ {
        *doanalyze = false;
    }
}

/*
 * relation_needs_vacanalyze
 *
 * Check whether a relation needs to be vacuumed or analyzed; return each into
 * "dovacuum" and "doanalyze", respectively.  Also return whether the vacuum is
 * being forced because of Xid or multixact wraparound.
 *
 * relopts is a pointer to the AutoVacOpts options (either for itself in the
 * case of a plain table, or for either itself or its parent table in the case
 * of a TOAST table), NULL if none; tabentry is the pgstats entry, which can be
 * NULL.
 *
 * A table needs to be vacuumed if the number of dead tuples exceeds a
 * threshold.  This threshold is calculated as
 *
 * threshold = vac_base_thresh + vac_scale_factor * reltuples
 * if (threshold > vac_max_thresh)
 *     threshold = vac_max_thresh;
 *
 * For analyze, the analysis done is that the number of tuples inserted,
 * deleted and updated since the last analyze exceeds a threshold calculated
 * in the same fashion as above.  Note that the cumulative stats system stores
 * the number of tuples (both live and dead) that there were as of the last
 * analyze.  This is asymmetric to the VACUUM case.
 *
 * We also force vacuum if the table's relfrozenxid is more than freeze_max_age
 * transactions back, and if its relminmxid is more than
 * multixact_freeze_max_age multixacts back.
 *
 * A table whose autovacuum_enabled option is false is
 * automatically skipped (unless we have to vacuum it due to freeze_max_age).
 * Thus autovacuum can be disabled for specific tables. Also, when the cumulative
 * stats system does not have data about a table, it will be skipped.
 *
 * A table whose vac_base_thresh value is < 0 takes the base value from the
 * autovacuum_vacuum_threshold GUC variable.  Similarly, a vac_scale_factor
 * value < 0 is substituted with the value of
 * autovacuum_vacuum_scale_factor GUC variable.  Ditto for analyze.
 */
unsafe fn relation_needs_vacanalyze(
    relid: Oid,
    relopts: *mut AutoVacOpts,
    classForm: Form_pg_class,
    tabentry: *mut PgStat_StatTabEntry,
    effective_multixact_freeze_max_age: c_int,
    /* output params below */
    dovacuum: *mut bool,
    doanalyze: *mut bool,
    wraparound: *mut bool,
) {
    let force_vacuum: bool;
    let av_enabled: bool;

    /* constants from reloptions or GUC variables */
    let vac_base_thresh: c_int;
    let vac_max_thresh: c_int;
    let vac_ins_base_thresh: c_int;
    let anl_base_thresh: c_int;
    let vac_scale_factor: f32;
    let vac_ins_scale_factor: f32;
    let anl_scale_factor: f32;

    /* thresholds calculated from above constants */
    let vacthresh: f32;
    let vacinsthresh: f32;
    let anlthresh: f32;

    /* number of vacuum (resp. analyze) tuples at this time */
    let vactuples: f32;
    let instuples: f32;
    let anltuples: f32;

    /* freeze parameters */
    let freeze_max_age: c_int;
    let multixact_freeze_max_age: c_int;
    let xidForceLimit: TransactionId;
    let relfrozenxid: TransactionId;
    let multiForceLimit: MultiXactId;

    // Assert(classForm != NULL);
    // Assert(OidIsValid(relid));

    /*
     * Determine vacuum/analyze equation parameters.  We have two possible
     * sources: the passed reloptions (which could be a main table or a toast
     * table), or the autovacuum GUC variables.
     */

    /* -1 in autovac setting means use plain vacuum_scale_factor */
    vac_scale_factor = if !relopts.is_null() && (*relopts).vacuum_scale_factor >= 0.0 {
        (*relopts).vacuum_scale_factor
    } else {
        autovacuum_vac_scale as f32
    };

    vac_base_thresh = if !relopts.is_null() && (*relopts).vacuum_threshold >= 0 {
        (*relopts).vacuum_threshold
    } else {
        autovacuum_vac_thresh
    };

    /* -1 is used to disable max threshold */
    vac_max_thresh = if !relopts.is_null() && (*relopts).vacuum_max_threshold >= -1 {
        (*relopts).vacuum_max_threshold
    } else {
        autovacuum_vac_max_thresh
    };

    vac_ins_scale_factor = if !relopts.is_null() && (*relopts).vacuum_ins_scale_factor >= 0.0 {
        (*relopts).vacuum_ins_scale_factor
    } else {
        autovacuum_vac_ins_scale as f32
    };

    /* -1 is used to disable insert vacuums */
    vac_ins_base_thresh = if !relopts.is_null() && (*relopts).vacuum_ins_threshold >= -1 {
        (*relopts).vacuum_ins_threshold
    } else {
        autovacuum_vac_ins_thresh
    };

    anl_scale_factor = if !relopts.is_null() && (*relopts).analyze_scale_factor >= 0.0 {
        (*relopts).analyze_scale_factor
    } else {
        autovacuum_anl_scale as f32
    };

    anl_base_thresh = if !relopts.is_null() && (*relopts).analyze_threshold >= 0 {
        (*relopts).analyze_threshold
    } else {
        autovacuum_anl_thresh
    };

    freeze_max_age = if !relopts.is_null() && (*relopts).freeze_max_age >= 0 {
        std::cmp::min((*relopts).freeze_max_age, autovacuum_freeze_max_age)
    } else {
        autovacuum_freeze_max_age
    };

    multixact_freeze_max_age = if !relopts.is_null() && (*relopts).multixact_freeze_max_age >= 0 {
        std::cmp::min((*relopts).multixact_freeze_max_age, effective_multixact_freeze_max_age)
    } else {
        effective_multixact_freeze_max_age
    };

    av_enabled = if !relopts.is_null() { (*relopts).enabled } else { true };

    /* Force vacuum if table is at risk of wraparound */
    xidForceLimit = recentXid.wrapping_sub(freeze_max_age as u32);
    let xidForceLimit = if xidForceLimit < 3 /* FirstNormalTransactionId */ {
        xidForceLimit.wrapping_sub(3)
    } else {
        xidForceLimit
    };
    relfrozenxid = (*classForm).relfrozenxid;
    let mut force_vacuum = TransactionIdIsNormal(relfrozenxid)
        && TransactionIdPrecedes(relfrozenxid, xidForceLimit);
    if !force_vacuum {
        let relminmxid: MultiXactId = (*classForm).relminmxid;

        multiForceLimit = recentMulti.wrapping_sub(multixact_freeze_max_age as u32);
        let multiForceLimit = if multiForceLimit < 1 /* FirstMultiXactId */ {
            multiForceLimit.wrapping_sub(1)
        } else {
            multiForceLimit
        };
        force_vacuum = MultiXactIdIsValid(relminmxid)
            && MultiXactIdPrecedes(relminmxid, multiForceLimit);
    }
    *wraparound = force_vacuum;

    /* User disabled it in pg_class.reloptions?  (But ignore if at risk) */
    if !av_enabled && !force_vacuum {
        *doanalyze = false;
        *dovacuum = false;
        return;
    }

    /*
     * If we found stats for the table, and autovacuum is currently enabled,
     * make a threshold-based decision whether to vacuum and/or analyze.  If
     * autovacuum is currently disabled, we must be here for anti-wraparound
     * vacuuming only, so don't vacuum (or analyze) anything that's not being
     * forced.
     */
    /* PointerIsValid(tabentry) -> !tabentry.is_null() */
    if !tabentry.is_null() && AutoVacuumingActive() {
        let mut pcnt_unfrozen: f32 = 1.0;
        let mut reltuples: f32 = (*classForm).reltuples;
        let relpages: i32 = (*classForm).relpages;
        let relallfrozen_raw: i32 = (*classForm).relallfrozen;

        vactuples = (*tabentry).dead_tuples;
        instuples = (*tabentry).ins_since_vacuum;
        anltuples = (*tabentry).mod_since_analyze;

        /* If the table hasn't yet been vacuumed, take reltuples as zero */
        if reltuples < 0.0 {
            reltuples = 0.0;
        }

        /*
         * If we have data for relallfrozen, calculate the unfrozen percentage
         * of the table to modify insert scale factor. This helps us decide
         * whether or not to vacuum an insert-heavy table based on the number
         * of inserts to the more "active" part of the table.
         */
        if relpages > 0 && relallfrozen_raw > 0 {
            /*
             * It could be the stats were updated manually and relallfrozen >
             * relpages. Clamp relallfrozen to relpages to avoid nonsensical
             * calculations.
             */
            let relallfrozen = std::cmp::min(relallfrozen_raw, relpages);
            pcnt_unfrozen = 1.0 - (relallfrozen as f32 / relpages as f32);
        }

        let vacthresh: f32 = vac_base_thresh as f32 + vac_scale_factor * reltuples;
        let vacthresh = if vac_max_thresh >= 0 && vacthresh > vac_max_thresh as f32 {
            vac_max_thresh as f32
        } else {
            vacthresh
        };

        let vacinsthresh: f32 = vac_ins_base_thresh as f32
            + vac_ins_scale_factor * reltuples * pcnt_unfrozen;
        let anlthresh: f32 = anl_base_thresh as f32 + anl_scale_factor * reltuples;

        /*
         * Note that we don't need to take special consideration for stat
         * reset, because if that happens, the last vacuum and analyze counts
         * will be reset too.
         */
        if vac_ins_base_thresh >= 0 {
            elog!(
                8, /* DEBUG3 */
                "{}: vac: {:.0} (threshold {:.0}), ins: {:.0} (threshold {:.0}), anl: {:.0} (threshold {:.0})",
                std::ffi::CStr::from_ptr(NameStr((*classForm).relname)).to_string_lossy(),
                vactuples, vacthresh, instuples, vacinsthresh, anltuples, anlthresh
            );
        } else {
            elog!(
                8, /* DEBUG3 */
                "{}: vac: {:.0} (threshold {:.0}), ins: (disabled), anl: {:.0} (threshold {:.0})",
                std::ffi::CStr::from_ptr(NameStr((*classForm).relname)).to_string_lossy(),
                vactuples, vacthresh, anltuples, anlthresh
            );
        }

        /* Determine if this table needs vacuum or analyze. */
        *dovacuum = force_vacuum
            || (vactuples > vacthresh)
            || (vac_ins_base_thresh >= 0 && instuples > vacinsthresh);
        *doanalyze = anltuples > anlthresh;
    } else {
        /*
         * Skip a table not found in stat hash, unless we have to force vacuum
         * for anti-wrap purposes.  If it's not acted upon, there's no need to
         * vacuum it.
         */
        *dovacuum = force_vacuum;
        *doanalyze = false;
    }

    /* ANALYZE refuses to work with pg_statistic */
    /* StatisticRelationId = 2619 */
    if relid == 2619 {
        *doanalyze = false;
    }
}

/*
 * autovacuum_do_vac_analyze
 *      Vacuum and/or analyze the specified table
 *
 * We expect the caller to have switched into a memory context that won't
 * disappear at transaction commit.
 */
unsafe fn autovacuum_do_vac_analyze(tab: *mut autovac_table, bstrategy: BufferAccessStrategy) {
    let rangevar: *mut RangeVar;
    let rel: *mut VacuumRelation;
    let rel_list: *mut List;
    let vac_context: MemoryContext;
    let old_context: MemoryContext;

    /* Let pgstat know what we're doing */
    autovac_report_activity(tab);

    /* Create a context that vacuum() can use as cross-transaction storage */
    vac_context = AllocSetContextCreate(
        CurrentMemoryContext,
        b"Vacuum\0".as_ptr() as *const c_char,
        0,
        8192,
        8388608,
    );

    /* Set up one VacuumRelation target, identified by OID, for vacuum() */
    old_context = MemoryContextSwitchTo(vac_context);
    rangevar = makeRangeVar((*tab).at_nspname, (*tab).at_relname, -1);
    rel = makeVacuumRelation(rangevar, (*tab).at_relid, std::ptr::null_mut() /* NIL */);
    rel_list = list_make1(rel as *mut c_void);
    MemoryContextSwitchTo(old_context);

    vacuum(rel_list, &(*tab).at_params, bstrategy, vac_context, true);

    MemoryContextDelete(vac_context);
}

/*
 * autovac_report_activity
 *      Report to pgstat what autovacuum is doing
 *
 * We send a SQL string corresponding to what the user would see if the
 * equivalent command was to be issued manually.
 *
 * Note we assume that we are going to report the next command as soon as we're
 * done with the current one, and exit right after the last one, so we don't
 * bother to report "<IDLE>" or some such.
 */
unsafe fn autovac_report_activity(tab: *mut autovac_table) {
    /* MAX_AUTOVAC_ACTIV_LEN = NAMEDATALEN * 2 + 56 = 64*2+56 = 184 */
    const MAX_AUTOVAC_ACTIV_LEN: usize = 184;
    let mut activity = [0u8; MAX_AUTOVAC_ACTIV_LEN + 12 + 2];

    /* Report the command and possible options */
    /* VACOPT_VACUUM = 0x01, VACOPT_ANALYZE = 0x02 */
    let cmd = if (*tab).at_params.options & 0x01 != 0 {
        if (*tab).at_params.options & 0x02 != 0 {
            "autovacuum: VACUUM ANALYZE"
        } else {
            "autovacuum: VACUUM"
        }
    } else {
        "autovacuum: ANALYZE"
    };

    let nspname = std::ffi::CStr::from_ptr((*tab).at_nspname).to_string_lossy();
    let relname = std::ffi::CStr::from_ptr((*tab).at_relname).to_string_lossy();
    let wraparound_str = if (*tab).at_params.is_wraparound { " (to prevent wraparound)" } else { "" };

    let full = std::format!("{} {}.{}{}\0", cmd, nspname, relname, wraparound_str);
    let bytes = full.as_bytes();
    let len = bytes.len().min(activity.len());
    activity[..len].copy_from_slice(&bytes[..len]);

    /* Set statement_timestamp() to current time for pg_stat_activity */
    SetCurrentStatementStartTimestamp();

    pgstat_report_activity(2 /* STATE_RUNNING */, activity.as_ptr() as *const c_char);
}

/*
 * autovac_report_workitem
 *      Report to pgstat that autovacuum is processing a work item
 */
unsafe fn autovac_report_workitem(
    workitem: *mut AutoVacuumWorkItem,
    nspname: *const c_char,
    relname: *const c_char,
) {
    const MAX_AUTOVAC_ACTIV_LEN: usize = 184;
    let mut activity = [0u8; MAX_AUTOVAC_ACTIV_LEN + 12 + 2];

    let cmd = match (*workitem).avw_type {
        AutoVacuumWorkItemType::AVW_BRINSummarizeRange => "autovacuum: BRIN summarize",
    };

    /*
     * Report the qualified name of the relation, and the block number if any
     */
    let nsp = std::ffi::CStr::from_ptr(nspname).to_string_lossy();
    let rel = std::ffi::CStr::from_ptr(relname).to_string_lossy();

    /* BlockNumberIsValid: blockNumber != InvalidBlockNumber (0xFFFFFFFF) */
    let blk_str = if (*workitem).avw_blockNumber != 0xFFFF_FFFF {
        std::format!(" {}", (*workitem).avw_blockNumber)
    } else {
        String::new()
    };

    let full = std::format!("{} {}.{}{}\0", cmd, nsp, rel, blk_str);
    let bytes = full.as_bytes();
    let len = bytes.len().min(activity.len());
    activity[..len].copy_from_slice(&bytes[..len]);

    /* Set statement_timestamp() to current time for pg_stat_activity */
    SetCurrentStatementStartTimestamp();

    pgstat_report_activity(2 /* STATE_RUNNING */, activity.as_ptr() as *const c_char);
}

/*
 * AutoVacuumingActive
 *      Check GUC vars and report whether the autovacuum process should be
 *      running.
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacuumingActive() -> bool {
    if !autovacuum_start_daemon || !pgstat_track_counts {
        return false;
    }
    true
}

/*
 * Request one work item to the next autovacuum run processing our database.
 * Return false if the request can't be recorded.
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacuumRequestWork(
    r#type: AutoVacuumWorkItemType,
    relationId: Oid,
    blkno: BlockNumber,
) -> bool {
    let mut result: bool = false;

    LWLockAcquire(&mut AutovacuumLock, 1 /* LW_EXCLUSIVE */);

    /*
     * Locate an unused work item and fill it with the given data.
     */
    for i in 0..NUM_WORKITEMS {
        let workitem: *mut AutoVacuumWorkItem = &mut (*AutoVacuumShmem).av_workItems[i];

        if (*workitem).avw_used {
            continue;
        }

        (*workitem).avw_used = true;
        (*workitem).avw_active = false;
        (*workitem).avw_type = r#type;
        (*workitem).avw_database = MyDatabaseId;
        (*workitem).avw_relation = relationId;
        (*workitem).avw_blockNumber = blkno;
        result = true;

        /* done */
        break;
    }

    LWLockRelease(&mut AutovacuumLock);

    result
}

/*
 * autovac_init
 *      This is called at postmaster initialization.
 *
 * All we do here is annoy the user if he got it wrong.
 */
#[no_mangle]
pub unsafe extern "C" fn autovac_init() {
    if !autovacuum_start_daemon {
        return;
    } else if !pgstat_track_counts {
        ereport!(
            19, /* WARNING */
            errmsg!("autovacuum not started because of misconfiguration")
            /* C also: errhint("Enable the \"track_counts\" option.") */
        );
    } else {
        check_av_worker_gucs();
    }
}

/*
 * AutoVacuumShmemSize
 *      Compute space needed for autovacuum-related shared memory
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacuumShmemSize() -> usize {
    let mut size: usize;

    /*
     * Need the fixed struct and the array of WorkerInfoData.
     */
    size = std::mem::size_of::<AutoVacuumShmemStruct>();
    size = MAXALIGN(size);
    size = add_size(size, mul_size(autovacuum_worker_slots as usize, std::mem::size_of::<WorkerInfoData>()));
    size
}

/*
 * AutoVacuumShmemInit
 *      Allocate and initialize autovacuum-related shared memory
 */
#[no_mangle]
pub unsafe extern "C" fn AutoVacuumShmemInit() {
    let mut found: bool = false;

    AutoVacuumShmem = ShmemInitStruct(
        b"AutoVacuum Data\0".as_ptr() as *const c_char,
        AutoVacuumShmemSize(),
        &mut found,
    ) as *mut AutoVacuumShmemStruct;

    if !IsUnderPostmaster {
        let worker: WorkerInfo;

        // Assert(!found);

        (*AutoVacuumShmem).av_launcherpid = 0;
        dclist_init(&mut (*AutoVacuumShmem).av_freeWorkers);
        dlist_init(&mut (*AutoVacuumShmem).av_runningWorkers);
        (*AutoVacuumShmem).av_startingWorker = std::ptr::null_mut();
        std::ptr::write_bytes(
            (*AutoVacuumShmem).av_workItems.as_mut_ptr(),
            0,
            NUM_WORKITEMS,
        );

        worker = (AutoVacuumShmem as *mut u8)
            .add(MAXALIGN(std::mem::size_of::<AutoVacuumShmemStruct>()))
            as WorkerInfo;

        /* initialize the WorkerInfo free list */
        for i in 0..autovacuum_worker_slots as usize {
            dclist_push_head(
                &mut (*AutoVacuumShmem).av_freeWorkers,
                &mut (*worker.add(i)).wi_links,
            );
            pg_atomic_init_flag(&mut (*worker.add(i)).wi_dobalance);
        }

        pg_atomic_init_u32(&mut (*AutoVacuumShmem).av_nworkersForBalance, 0);
    } else {
        // Assert(found);
    }
}

/*
 * GUC check_hook for autovacuum_work_mem
 */
#[no_mangle]
pub unsafe extern "C" fn check_autovacuum_work_mem(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: c_int,
) -> bool {
    /*
     * -1 indicates fallback.
     *
     * If we haven't yet changed the boot_val default of -1, just let it be.
     * Autovacuum will look to maintenance_work_mem instead.
     */
    if *newval == -1 {
        return true;
    }

    /*
     * We clamp manually-set values to at least 64kB.  Since
     * maintenance_work_mem is always set to at least this value, do the same
     * here.
     */
    if *newval < 64 {
        *newval = 64;
    }

    true
}

/*
 * Returns whether there is a free autovacuum worker slot available.
 */
unsafe fn av_worker_available() -> bool {
    let free_slots: c_int = dclist_count(&(*AutoVacuumShmem).av_freeWorkers) as c_int;

    let reserved_slots: c_int = std::cmp::max(0, autovacuum_worker_slots - autovacuum_max_workers);

    free_slots > reserved_slots
}

/*
 * Emits a WARNING if autovacuum_worker_slots < autovacuum_max_workers.
 */
unsafe fn check_av_worker_gucs() {
    if autovacuum_worker_slots < autovacuum_max_workers {
        ereport!(
            19, /* WARNING */
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            errmsg!(
                "\"autovacuum_max_workers\" ({}) should be less than or equal to \"autovacuum_worker_slots\" ({})",
                autovacuum_max_workers,
                autovacuum_worker_slots
            )
            /* C also: errdetail("The server will only start up to \"autovacuum_worker_slots\" ({}) autovacuum workers at a given time.", autovacuum_worker_slots) */
        );
    }
}

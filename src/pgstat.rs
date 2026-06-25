//! Translated from PostgreSQL src/include/pgstat.h
//
// Cumulative statistics system. Counter structs are in-memory (idiomatic Rust, no
// #[repr(C)]); enums are sequential ordinals; the API fns are stubbed.

use crate::access::xact::xl_xact_stats_item;
use crate::c::TransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::miscadmin::{BackendType, BACKEND_NUM_TYPES};
use crate::portability::instr_time::InstrTime;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::postmaster::pgarch::MAX_XFN_CHARS;
use crate::replication::conflict::{ConflictType, CONFLICT_NUM_TYPES};
use crate::replication::slot::ReplicationSlot;
use crate::storage::procnumber::ProcNumber;
use crate::utils::pgstat_kind::PgStat_Kind;
use crate::utils::relcache::Relation;

// Paths for the statistics files (relative to $PGDATA).
pub const PGSTAT_STAT_PERMANENT_DIRECTORY: &str = "pg_stat";
pub const PGSTAT_STAT_PERMANENT_FILENAME: &str = "pg_stat/pgstat.stat";
pub const PGSTAT_STAT_PERMANENT_TMPFILE: &str = "pg_stat/pgstat.tmp";
pub const PG_STAT_TMP_DIR: &str = "pg_stat_tmp";

// track_functions GUC values -- order is significant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackFunctionsLevel {
    TRACK_FUNC_OFF,
    TRACK_FUNC_PL,
    TRACK_FUNC_ALL,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgStat_FetchConsistency {
    PGSTAT_FETCH_CONSISTENCY_NONE,
    PGSTAT_FETCH_CONSISTENCY_CACHE,
    PGSTAT_FETCH_CONSISTENCY_SNAPSHOT,
}

// Cause of session termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndType {
    DISCONNECT_NOT_YET, // still active
    DISCONNECT_NORMAL,
    DISCONNECT_CLIENT_EOF,
    DISCONNECT_FATAL,
    DISCONNECT_KILLED,
}

// The data type used for counters.
pub type PgStat_Counter = i64;

// ---- Structures kept in backend local memory while accumulating counts ----

#[derive(Debug, Clone, Copy, Default)]
pub struct PgStat_FunctionCounts {
    pub numcalls: PgStat_Counter,
    pub total_time: InstrTime,
    pub self_time: InstrTime,
}

// Working state for per-function-call timing. `fs` links to the hashtable entry.
pub struct PgStat_FunctionCallUsage<'a> {
    // NULL means we are not tracking the current function call.
    pub fs: Option<&'a mut PgStat_FunctionCounts>,
    pub save_f_total_time: InstrTime,
    pub save_total: InstrTime,
    pub start: InstrTime,
}

// Non-flushed subscription stats.
#[derive(Debug, Clone)]
pub struct PgStat_BackendSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
}

// Per-table counts kept by a backend (event counters only).
#[derive(Debug, Clone, Default)]
pub struct PgStat_TableCounts {
    pub numscans: PgStat_Counter,

    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,

    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,
    pub truncdropped: bool,

    pub delta_live_tuples: PgStat_Counter,
    pub delta_dead_tuples: PgStat_Counter,
    pub changed_tuples: PgStat_Counter,

    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
}

// Per-table status within a backend. `trans`/`relation` are pointer links in C.
pub struct PgStat_TableStatus {
    pub id: Oid,        // table's OID
    pub shared: bool,   // is it a shared catalog?
    pub trans: Option<Box<PgStat_TableXactStatus>>, // lowest subxact's counts; TODO(ptr)
    pub counts: PgStat_TableCounts,
    pub relation: Relation, // rel using this entry; TODO(ptr)
}

// Per-table, per-subtransaction status (an intrusive linked stack in C).
pub struct PgStat_TableXactStatus {
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub truncdropped: bool,
    pub inserted_pre_truncdrop: PgStat_Counter,
    pub updated_pre_truncdrop: PgStat_Counter,
    pub deleted_pre_truncdrop: PgStat_Counter,
    pub nest_level: i32,
    // intrusive links (upper subxact / per-table parent / same-subxact next); TODO(ptr).
    pub upper: Option<Box<PgStat_TableXactStatus>>,
    pub parent: *mut PgStat_TableStatus,
    pub next: Option<Box<PgStat_TableXactStatus>>,
}

// ---- Data structures on disk and in shared memory follow ----
// (In single-process Rust these are owned heap state, not shmem; the file format
// is still versioned by PGSTAT_FILE_FORMAT_ID, written via explicit serializers.)

pub const PGSTAT_FILE_FORMAT_ID: u32 = 0x01A5BCB7;

#[derive(Debug, Clone)]
pub struct PgStat_ArchiverStats {
    pub archived_count: PgStat_Counter,
    pub last_archived_wal: [u8; MAX_XFN_CHARS + 1],
    pub last_archived_timestamp: TimestampTz,
    pub failed_count: PgStat_Counter,
    pub last_failed_wal: [u8; MAX_XFN_CHARS + 1],
    pub last_failed_timestamp: TimestampTz,
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Debug, Clone, Default)]
pub struct PgStat_BgWriterStats {
    pub buf_written_clean: PgStat_Counter,
    pub maxwritten_clean: PgStat_Counter,
    pub buf_alloc: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Debug, Clone, Default)]
pub struct PgStat_CheckpointerStats {
    pub num_timed: PgStat_Counter,
    pub num_requested: PgStat_Counter,
    pub num_performed: PgStat_Counter,
    pub restartpoints_timed: PgStat_Counter,
    pub restartpoints_requested: PgStat_Counter,
    pub restartpoints_performed: PgStat_Counter,
    pub write_time: PgStat_Counter, // milliseconds
    pub sync_time: PgStat_Counter,
    pub buffers_written: PgStat_Counter,
    pub slru_written: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

// ---- IO operation counting ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOObject {
    IOOBJECT_RELATION,
    IOOBJECT_TEMP_RELATION,
    IOOBJECT_WAL,
}
pub const IOOBJECT_NUM_TYPES: usize = IOObject::IOOBJECT_WAL as usize + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOContext {
    IOCONTEXT_BULKREAD,
    IOCONTEXT_BULKWRITE,
    IOCONTEXT_INIT,
    IOCONTEXT_NORMAL,
    IOCONTEXT_VACUUM,
}
pub const IOCONTEXT_NUM_TYPES: usize = IOContext::IOCONTEXT_VACUUM as usize + 1;

// IO operations. First-in-bytes is IOOP_EXTEND, last is IOOP_WRITE; order matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOOp {
    // not tracked in bytes
    IOOP_EVICT,
    IOOP_FSYNC,
    IOOP_HIT,
    IOOP_REUSE,
    IOOP_WRITEBACK,
    // tracked in bytes
    IOOP_EXTEND,
    IOOP_READ,
    IOOP_WRITE,
}
pub const IOOP_NUM_TYPES: usize = IOOp::IOOP_WRITE as usize + 1;

pub const fn pgstat_is_ioop_tracked_in_bytes(io_op: IOOp) -> bool {
    (io_op as u32) < IOOP_NUM_TYPES as u32 && (io_op as u32) >= IOOp::IOOP_EXTEND as u32
}

pub struct PgStat_BktypeIO {
    pub bytes: [[[u64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub times: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}

pub struct PgStat_PendingIO {
    pub bytes: [[[u64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub pending_times:
        [[[InstrTime; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}

pub struct PgStat_IO {
    pub stat_reset_timestamp: TimestampTz,
    pub stats: [PgStat_BktypeIO; BACKEND_NUM_TYPES],
}

#[derive(Debug, Clone, Default)]
pub struct PgStat_StatDBEntry {
    pub xact_commit: PgStat_Counter,
    pub xact_rollback: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub last_autovac_time: TimestampTz,
    pub conflict_tablespace: PgStat_Counter,
    pub conflict_lock: PgStat_Counter,
    pub conflict_snapshot: PgStat_Counter,
    pub conflict_logicalslot: PgStat_Counter,
    pub conflict_bufferpin: PgStat_Counter,
    pub conflict_startup_deadlock: PgStat_Counter,
    pub temp_files: PgStat_Counter,
    pub temp_bytes: PgStat_Counter,
    pub deadlocks: PgStat_Counter,
    pub checksum_failures: PgStat_Counter,
    pub last_checksum_failure: TimestampTz,
    pub blk_read_time: PgStat_Counter, // microseconds
    pub blk_write_time: PgStat_Counter,
    pub sessions: PgStat_Counter,
    pub session_time: PgStat_Counter,
    pub active_time: PgStat_Counter,
    pub idle_in_transaction_time: PgStat_Counter,
    pub sessions_abandoned: PgStat_Counter,
    pub sessions_fatal: PgStat_Counter,
    pub sessions_killed: PgStat_Counter,
    pub parallel_workers_to_launch: PgStat_Counter,
    pub parallel_workers_launched: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PgStat_StatFuncEntry {
    pub numcalls: PgStat_Counter,
    pub total_time: PgStat_Counter, // microseconds
    pub self_time: PgStat_Counter,
}

#[derive(Debug, Clone, Default)]
pub struct PgStat_StatReplSlotEntry {
    pub spill_txns: PgStat_Counter,
    pub spill_count: PgStat_Counter,
    pub spill_bytes: PgStat_Counter,
    pub stream_txns: PgStat_Counter,
    pub stream_count: PgStat_Counter,
    pub stream_bytes: PgStat_Counter,
    pub total_txns: PgStat_Counter,
    pub total_bytes: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Debug, Clone, Default)]
pub struct PgStat_SLRUStats {
    pub blocks_zeroed: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub blocks_read: PgStat_Counter,
    pub blocks_written: PgStat_Counter,
    pub blocks_exists: PgStat_Counter,
    pub flush: PgStat_Counter,
    pub truncate: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Debug, Clone)]
pub struct PgStat_StatSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Debug, Clone, Default)]
pub struct PgStat_StatTabEntry {
    pub numscans: PgStat_Counter,
    pub lastscan: TimestampTz,

    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,

    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,

    pub live_tuples: PgStat_Counter,
    pub dead_tuples: PgStat_Counter,
    pub mod_since_analyze: PgStat_Counter,
    pub ins_since_vacuum: PgStat_Counter,

    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,

    pub last_vacuum_time: TimestampTz, // user initiated vacuum
    pub vacuum_count: PgStat_Counter,
    pub last_autovacuum_time: TimestampTz, // autovacuum initiated
    pub autovacuum_count: PgStat_Counter,
    pub last_analyze_time: TimestampTz, // user initiated
    pub analyze_count: PgStat_Counter,
    pub last_autoanalyze_time: TimestampTz, // autovacuum initiated
    pub autoanalyze_count: PgStat_Counter,

    pub total_vacuum_time: PgStat_Counter, // milliseconds
    pub total_autovacuum_time: PgStat_Counter,
    pub total_analyze_time: PgStat_Counter,
    pub total_autoanalyze_time: PgStat_Counter,
}

// WAL activity counters gathered from WalUsage (shared across Stats structures).
#[derive(Debug, Clone, Copy, Default)]
pub struct PgStat_WalCounters {
    pub wal_records: PgStat_Counter,
    pub wal_fpi: PgStat_Counter,
    pub wal_bytes: u64,
    pub wal_buffers_full: PgStat_Counter,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PgStat_WalStats {
    pub wal_counters: PgStat_WalCounters,
    pub stat_reset_timestamp: TimestampTz,
}

pub struct PgStat_Backend {
    pub stat_reset_timestamp: TimestampTz,
    pub io_stats: PgStat_BktypeIO,
    pub wal_counters: PgStat_WalCounters,
}

// Non-flushed backend stats (same IO data as PGSTAT_KIND_IO).
pub struct PgStat_BackendPending {
    pub pending_io: PgStat_PendingIO,
}

// ---- Functions in pgstat.c ----

// StatsShmemSize/StatsShmemInit drop out under single-process (no shared memory);
// kept as stubs for the call sites.
pub fn StatsShmemSize() -> usize {
    unimplemented!()
}
pub fn StatsShmemInit() {
    unimplemented!()
}

pub fn pgstat_restore_stats() {
    unimplemented!()
}
pub fn pgstat_discard_stats() {
    unimplemented!()
}
pub fn pgstat_before_server_shutdown(code: i32, arg: Datum) {
    unimplemented!()
}

pub fn pgstat_initialize() {
    unimplemented!()
}

pub fn pgstat_report_stat(force: bool) -> i64 {
    unimplemented!()
}
pub fn pgstat_force_next_flush() {
    unimplemented!()
}

pub fn pgstat_reset_counters() {
    unimplemented!()
}
pub fn pgstat_reset(kind: PgStat_Kind, dboid: Oid, objid: u64) {
    unimplemented!()
}
pub fn pgstat_reset_of_kind(kind: PgStat_Kind) {
    unimplemented!()
}

pub fn pgstat_clear_snapshot() {
    unimplemented!()
}
// out-param `have_snapshot` folded into Option (None = no snapshot).
pub fn pgstat_get_stat_snapshot_timestamp() -> Option<TimestampTz> {
    unimplemented!()
}

pub fn pgstat_get_kind_from_str(kind_str: &str) -> PgStat_Kind {
    unimplemented!()
}
pub fn pgstat_have_entry(kind: PgStat_Kind, dboid: Oid, objid: u64) -> bool {
    unimplemented!()
}

// ---- Functions in pgstat_archiver.c ----

pub fn pgstat_report_archiver(xlog: &str, failed: bool) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_archiver() -> &'static PgStat_ArchiverStats {
    unimplemented!()
}

// ---- Functions in pgstat_backend.c ----

pub fn pgstat_count_backend_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    io_time: InstrTime,
) {
    unimplemented!()
}
pub fn pgstat_count_backend_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: u32,
    bytes: u64,
) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_backend(proc_number: ProcNumber) -> Option<&'static PgStat_Backend> {
    unimplemented!()
}
// out-param `bktype` folded into the tuple.
pub fn pgstat_fetch_stat_backend_by_pid(
    pid: i32,
) -> Option<(&'static PgStat_Backend, BackendType)> {
    unimplemented!()
}
pub fn pgstat_tracks_backend_bktype(bktype: BackendType) -> bool {
    unimplemented!()
}
pub fn pgstat_create_backend(procnum: ProcNumber) {
    unimplemented!()
}

// ---- Functions in pgstat_bgwriter.c ----

pub fn pgstat_report_bgwriter() {
    unimplemented!()
}
pub fn pgstat_fetch_stat_bgwriter() -> &'static PgStat_BgWriterStats {
    unimplemented!()
}

// ---- Functions in pgstat_checkpointer.c ----

pub fn pgstat_report_checkpointer() {
    unimplemented!()
}
pub fn pgstat_fetch_stat_checkpointer() -> &'static PgStat_CheckpointerStats {
    unimplemented!()
}

// ---- Functions in pgstat_io.c ----

pub fn pgstat_bktype_io_stats_valid(
    backend_io: &PgStat_BktypeIO,
    bktype: BackendType,
) -> bool {
    unimplemented!()
}
pub fn pgstat_count_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: u32,
    bytes: u64,
) {
    unimplemented!()
}
pub fn pgstat_prepare_io_time(track_io_guc: bool) -> InstrTime {
    unimplemented!()
}
pub fn pgstat_count_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    start_time: InstrTime,
    cnt: u32,
    bytes: u64,
) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_io() -> &'static PgStat_IO {
    unimplemented!()
}
pub fn pgstat_get_io_context_name(io_context: IOContext) -> &'static str {
    unimplemented!()
}
pub fn pgstat_get_io_object_name(io_object: IOObject) -> &'static str {
    unimplemented!()
}
pub fn pgstat_tracks_io_bktype(bktype: BackendType) -> bool {
    unimplemented!()
}
pub fn pgstat_tracks_io_object(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
) -> bool {
    unimplemented!()
}
pub fn pgstat_tracks_io_op(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
) -> bool {
    unimplemented!()
}

// ---- Functions in pgstat_database.c ----

pub fn pgstat_drop_database(databaseid: Oid) {
    unimplemented!()
}
pub fn pgstat_report_autovac(dboid: Oid) {
    unimplemented!()
}
pub fn pgstat_report_recovery_conflict(reason: i32) {
    unimplemented!()
}
pub fn pgstat_report_deadlock() {
    unimplemented!()
}
pub fn pgstat_prepare_report_checksum_failure(dboid: Oid) {
    unimplemented!()
}
pub fn pgstat_report_checksum_failures_in_db(dboid: Oid, failurecount: i32) {
    unimplemented!()
}
pub fn pgstat_report_connect(dboid: Oid) {
    unimplemented!()
}
pub fn pgstat_update_parallel_workers_stats(
    workers_to_launch: PgStat_Counter,
    workers_launched: PgStat_Counter,
) {
    unimplemented!()
}

// pgstat_count_buffer_*_time / _conn_*_time macros bump process-global counters
// (pgStatBlockReadTime etc., deferred to session state).

pub fn pgstat_fetch_stat_dbentry(dboid: Oid) -> &'static PgStat_StatDBEntry {
    unimplemented!()
}

// ---- Functions in pgstat_function.c ----

pub fn pgstat_create_function(proid: Oid) {
    unimplemented!()
}
pub fn pgstat_drop_function(proid: Oid) {
    unimplemented!()
}
// struct FunctionCallInfoBaseData -> crate::fmgr (forward-declared in C). TODO(ptr).
pub fn pgstat_init_function_usage(
    fcinfo: &mut crate::fmgr::FunctionCallInfoBaseData,
    fcu: &mut PgStat_FunctionCallUsage,
) {
    unimplemented!()
}
pub fn pgstat_end_function_usage(fcu: &mut PgStat_FunctionCallUsage, finalize: bool) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_funcentry(func_id: Oid) -> Option<&'static PgStat_StatFuncEntry> {
    unimplemented!()
}
pub fn find_funcstat_entry(func_id: Oid) -> Option<&'static mut PgStat_FunctionCounts> {
    unimplemented!()
}

// ---- Functions in pgstat_relation.c ----

pub fn pgstat_create_relation(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_drop_relation(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_copy_relation_stats(dst: Relation, src: Relation) {
    unimplemented!()
}
pub fn pgstat_init_relation(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_assoc_relation(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_unlink_relation(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_report_vacuum(
    tableoid: Oid,
    shared: bool,
    livetuples: PgStat_Counter,
    deadtuples: PgStat_Counter,
    starttime: TimestampTz,
) {
    unimplemented!()
}
pub fn pgstat_report_analyze(
    rel: Relation,
    livetuples: PgStat_Counter,
    deadtuples: PgStat_Counter,
    resetcounter: bool,
    starttime: TimestampTz,
) {
    unimplemented!()
}

// pgstat_should_count_relation / pgstat_count_heap_*/buffer_* inline macros read
// rel->pgstat_info; deferred until RelationData carries the pgstat hook.

pub fn pgstat_count_heap_insert(rel: Relation, n: PgStat_Counter) {
    unimplemented!()
}
pub fn pgstat_count_heap_update(rel: Relation, hot: bool, newpage: bool) {
    unimplemented!()
}
pub fn pgstat_count_heap_delete(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_count_truncate(rel: Relation) {
    unimplemented!()
}
pub fn pgstat_update_heap_dead_tuples(rel: Relation, delta: i32) {
    unimplemented!()
}
pub fn pgstat_twophase_postcommit(xid: TransactionId, info: u16, recdata: &[u8]) {
    unimplemented!()
}
pub fn pgstat_twophase_postabort(xid: TransactionId, info: u16, recdata: &[u8]) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_tabentry(relid: Oid) -> Option<&'static PgStat_StatTabEntry> {
    unimplemented!()
}
pub fn pgstat_fetch_stat_tabentry_ext(
    shared: bool,
    reloid: Oid,
) -> Option<&'static PgStat_StatTabEntry> {
    unimplemented!()
}
pub fn find_tabstat_entry(rel_id: Oid) -> Option<&'static mut PgStat_TableStatus> {
    unimplemented!()
}

// ---- Functions in pgstat_replslot.c ----

pub fn pgstat_reset_replslot(name: &str) {
    unimplemented!()
}
// struct ReplicationSlot -> crate::replication::slot (forward-declared in C). TODO(ptr).
pub fn pgstat_report_replslot(
    slot: &ReplicationSlot,
    rep_slot_stat: &PgStat_StatReplSlotEntry,
) {
    unimplemented!()
}
pub fn pgstat_create_replslot(slot: &ReplicationSlot) {
    unimplemented!()
}
pub fn pgstat_acquire_replslot(slot: &ReplicationSlot) {
    unimplemented!()
}
pub fn pgstat_drop_replslot(slot: &ReplicationSlot) {
    unimplemented!()
}
// NameData -> crate::c::NameData (fixed [u8; NAMEDATALEN]).
pub fn pgstat_fetch_replslot(
    slotname: crate::c::NameData,
) -> Option<&'static PgStat_StatReplSlotEntry> {
    unimplemented!()
}

// ---- Functions in pgstat_slru.c ----

pub fn pgstat_reset_slru(name: &str) {
    unimplemented!()
}
pub fn pgstat_count_slru_page_zeroed(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_count_slru_page_hit(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_count_slru_page_read(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_count_slru_page_written(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_count_slru_page_exists(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_count_slru_flush(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_count_slru_truncate(slru_idx: i32) {
    unimplemented!()
}
pub fn pgstat_get_slru_name(slru_idx: i32) -> &'static str {
    unimplemented!()
}
pub fn pgstat_get_slru_index(name: &str) -> i32 {
    unimplemented!()
}
pub fn pgstat_fetch_slru() -> &'static PgStat_SLRUStats {
    unimplemented!()
}

// ---- Functions in pgstat_subscription.c ----

pub fn pgstat_report_subscription_error(subid: Oid, is_apply_error: bool) {
    unimplemented!()
}
pub fn pgstat_report_subscription_conflict(subid: Oid, conflict_type: ConflictType) {
    unimplemented!()
}
pub fn pgstat_create_subscription(subid: Oid) {
    unimplemented!()
}
pub fn pgstat_drop_subscription(subid: Oid) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_subscription(subid: Oid) -> Option<&'static PgStat_StatSubEntry> {
    unimplemented!()
}

// ---- Functions in pgstat_xact.c ----

pub fn AtEOXact_PgStat(is_commit: bool, parallel: bool) {
    unimplemented!()
}
pub fn AtEOSubXact_PgStat(is_commit: bool, nest_depth: i32) {
    unimplemented!()
}
pub fn AtPrepare_PgStat() {
    unimplemented!()
}
pub fn PostPrepare_PgStat() {
    unimplemented!()
}
// struct xl_xact_stats_item -> crate::access::xact (forward-declared in C). TODO(ptr).
// out-param `items` + count folded into a returned Vec.
pub fn pgstat_get_transactional_drops(
    is_commit: bool,
) -> Vec<xl_xact_stats_item> {
    unimplemented!()
}
pub fn pgstat_execute_transactional_drops(
    items: &[xl_xact_stats_item],
    is_redo: bool,
) {
    unimplemented!()
}

// ---- Functions in pgstat_wal.c ----

pub fn pgstat_report_wal(force: bool) {
    unimplemented!()
}
pub fn pgstat_fetch_stat_wal() -> &'static PgStat_WalStats {
    unimplemented!()
}

// ---- GUC parameters / process-global counters (deferred to session state) ----
// pgstat_track_counts: bool, pgstat_track_functions: i32, pgstat_fetch_consistency: i32,
// PendingBgWriterStats, PendingCheckpointerStats, pgStatBlockReadTime/WriteTime,
// pgStatActiveTime, pgStatTransactionIdleTime, pgStatSessionEndCause.

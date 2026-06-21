//! src/backend/utils/adt/pgstatfuncs.c
//!
//! pgstatfuncs.c
//!	  Functions for accessing various forms of statistics data
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/utils/adt/pgstatfuncs.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;

use core::ffi::CStr;
use std::ffi::{c_char, c_int};

use crate::c::{int32, int64, uint32, NameData, TransactionId};
use crate::postgres::{Float8GetDatum, Int64GetDatum};
use crate::utils::fmgr::FunctionCallInfo;

// ---------------------------------------------------------------------------
// Type stubs for not-yet-ported headers (pgstat.h, miscadmin.h, storage/proc.h,
// funcapi.h, ...). These all live in OTHER .c/.h files.
// ---------------------------------------------------------------------------

pub type TimestampTz = int64;
pub type ProcNumber = c_int;
pub type BackendType = c_int;
pub type PgStat_Counter = int64;
pub type PgStat_Kind = uint32;
pub type ProgressCommandType = c_int;
pub type IOOp = c_int;
pub type AttrNumber = i16;

// OID constants (catalog/pg_type_d.h)
pub const INT8OID: Oid = 20; // TODO(pg-port): catalog/pg_type_d.h
pub const INT4OID: Oid = 23; // TODO(pg-port): catalog/pg_type_d.h
pub const TEXTOID: Oid = 25; // TODO(pg-port): catalog/pg_type_d.h
pub const OIDOID: Oid = 26; // TODO(pg-port): catalog/pg_type_d.h
pub const TIMESTAMPTZOID: Oid = 1184; // TODO(pg-port): catalog/pg_type_d.h
pub const NUMERICOID: Oid = 1700; // TODO(pg-port): catalog/pg_type_d.h
pub const BOOLOID: Oid = 16; // TODO(pg-port): catalog/pg_type_d.h

// catalog/pg_authid.h
const ROLE_PG_READ_ALL_STATS: Oid = 3375; // TODO(pg-port): catalog/pg_authid_d.h

// network family constants (sys/socket.h)
const AF_INET: c_int = 2;
const AF_INET6: c_int = 30;
const AF_UNIX: c_int = 1;
const NI_MAXHOST: usize = 1025;
const NI_MAXSERV: usize = 32;
const NI_NUMERICHOST: c_int = 2;
const NI_NUMERICSERV: c_int = 8;

const InvalidPid: c_int = 0;

// pgstat.h enum BackendState
const STATE_UNDEFINED: c_int = 0;
const STATE_STARTING: c_int = 1;
const STATE_IDLE: c_int = 2;
const STATE_RUNNING: c_int = 3;
const STATE_IDLEINTRANSACTION: c_int = 4;
const STATE_FASTPATH: c_int = 5;
const STATE_IDLEINTRANSACTION_ABORTED: c_int = 6;
const STATE_DISABLED: c_int = 7;

// miscadmin.h enum BackendType subset used here
const B_BACKEND: BackendType = 1; // TODO(pg-port): miscadmin.h
const B_BG_WORKER: BackendType = 7; // TODO(pg-port): miscadmin.h
const B_WAL_SENDER: BackendType = 10; // TODO(pg-port): miscadmin.h

// commands/progress.h
const PROGRESS_COMMAND_VACUUM: ProgressCommandType = 1; // TODO(pg-port): commands/progress.h
const PROGRESS_COMMAND_ANALYZE: ProgressCommandType = 2; // TODO(pg-port): commands/progress.h
const PROGRESS_COMMAND_CLUSTER: ProgressCommandType = 3; // TODO(pg-port): commands/progress.h
const PROGRESS_COMMAND_CREATE_INDEX: ProgressCommandType = 4; // TODO(pg-port): commands/progress.h
const PROGRESS_COMMAND_BASEBACKUP: ProgressCommandType = 5; // TODO(pg-port): commands/progress.h
const PROGRESS_COMMAND_COPY: ProgressCommandType = 6; // TODO(pg-port): commands/progress.h

const PGSTAT_NUM_PROGRESS_PARAM: usize = 20; // TODO(pg-port): pgstat.h

// pgstat.h enum IOOp
const IOOP_EVICT: IOOp = 0; // TODO(pg-port): pgstat.h
const IOOP_EXTEND: IOOp = 1; // TODO(pg-port): pgstat.h
const IOOP_FSYNC: IOOp = 2; // TODO(pg-port): pgstat.h
const IOOP_HIT: IOOp = 3; // TODO(pg-port): pgstat.h
const IOOP_READ: IOOp = 4; // TODO(pg-port): pgstat.h
const IOOP_REUSE: IOOp = 5; // TODO(pg-port): pgstat.h
const IOOP_WRITE: IOOp = 6; // TODO(pg-port): pgstat.h
const IOOP_WRITEBACK: IOOp = 7; // TODO(pg-port): pgstat.h
const IOOP_NUM_TYPES: usize = 8; // TODO(pg-port): pgstat.h
const IOOBJECT_NUM_TYPES: usize = 2; // TODO(pg-port): pgstat.h
const IOCONTEXT_NUM_TYPES: usize = 4; // TODO(pg-port): pgstat.h
const BACKEND_NUM_TYPES: usize = 14; // TODO(pg-port): miscadmin.h

const CONFLICT_NUM_TYPES: usize = 7; // TODO(pg-port): replication conflict count

// pgstat.h enum PgStat_Kind builtin ids
const PGSTAT_KIND_ARCHIVER: PgStat_Kind = 6; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_BGWRITER: PgStat_Kind = 7; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_CHECKPOINTER: PgStat_Kind = 8; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_IO: PgStat_Kind = 9; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_SLRU: PgStat_Kind = 10; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_WAL: PgStat_Kind = 11; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_RELATION: PgStat_Kind = 1; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_FUNCTION: PgStat_Kind = 2; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_BACKEND: PgStat_Kind = 3; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_REPLSLOT: PgStat_Kind = 12; // TODO(pg-port): pgstat_kind.h
const PGSTAT_KIND_SUBSCRIPTION: PgStat_Kind = 13; // TODO(pg-port): pgstat_kind.h

const InvalidOid: Oid = 0;

// pgstat.h opaque/struct stubs -- field access happens through accessor stubs.
#[repr(C)]
pub struct PgStat_StatTabEntry {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct PgStat_StatFuncEntry {
    pub numcalls: PgStat_Counter,
    pub total_time: PgStat_Counter,
    pub self_time: PgStat_Counter,
}
#[repr(C)]
pub struct PgStat_StatDBEntry {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct PgStat_TableStatus {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct PgStat_FunctionCounts {
    pub numcalls: PgStat_Counter,
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct LocalPgBackendStatus {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct PgBackendStatus {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct PGPROC {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: *mut c_void,
}
#[repr(C)]
pub struct ReturnSetInfo {
    pub r#type: *mut c_void,
    pub econtext: *mut c_void,
    pub expectedDesc: *mut c_void,
    pub allowedModes: c_int,
    pub returnMode: c_int,
    pub isDone: c_int,
    pub setResult: *mut c_void,
    pub setDesc: *mut c_void,
}
#[repr(C)]
pub struct TupleDescData {
    _opaque: [u8; 0],
}
pub type TupleDesc = *mut TupleDescData;
pub type HeapTupleData = c_void;
pub type HeapTuple = *mut HeapTupleData;

#[repr(C)]
pub struct PgStat_BktypeIO {
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub times: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub bytes: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}
#[repr(C)]
pub struct PgStat_IO {
    pub stat_reset_timestamp: TimestampTz,
    pub stats: [PgStat_BktypeIO; BACKEND_NUM_TYPES],
}
#[repr(C)]
pub struct PgStat_WalCounters {
    pub wal_records: PgStat_Counter,
    pub wal_fpi: PgStat_Counter,
    pub wal_bytes: u64,
    pub wal_buffers_full: PgStat_Counter,
}
#[repr(C)]
pub struct PgStat_WalStats {
    pub wal_counters: PgStat_WalCounters,
    pub stat_reset_timestamp: TimestampTz,
}
#[repr(C)]
pub struct PgStat_Backend {
    pub io_stats: PgStat_BktypeIO,
    pub wal_counters: PgStat_WalCounters,
    pub stat_reset_timestamp: TimestampTz,
}
#[repr(C)]
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
#[repr(C)]
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
#[repr(C)]
pub struct PgStat_StatSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
    pub stat_reset_timestamp: TimestampTz,
}
#[repr(C)]
pub struct PgStat_ArchiverStats {
    pub archived_count: PgStat_Counter,
    pub last_archived_wal: [c_char; 64],
    pub last_archived_timestamp: TimestampTz,
    pub failed_count: PgStat_Counter,
    pub last_failed_wal: [c_char; 64],
    pub last_failed_timestamp: TimestampTz,
    pub stat_reset_timestamp: TimestampTz,
}
#[repr(C)]
pub struct PgStat_CheckpointerStats {
    pub num_timed: PgStat_Counter,
    pub num_requested: PgStat_Counter,
    pub num_performed: PgStat_Counter,
    pub restartpoints_timed: PgStat_Counter,
    pub restartpoints_requested: PgStat_Counter,
    pub restartpoints_performed: PgStat_Counter,
    pub write_time: PgStat_Counter,
    pub sync_time: PgStat_Counter,
    pub buffers_written: PgStat_Counter,
    pub slru_written: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}
#[repr(C)]
pub struct PgStat_BgWriterStats {
    pub buf_written_clean: PgStat_Counter,
    pub maxwritten_clean: PgStat_Counter,
    pub buf_alloc: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

// ---------------------------------------------------------------------------
// Datum conversion helpers (postgres.h) -- some live in real homes, the rest
// are stubbed here matching the green-sibling self-contained pattern.
// ---------------------------------------------------------------------------

#[inline]
unsafe fn Int32GetDatum(x: int32) -> Datum {
    x as Datum
}
#[inline]
unsafe fn ObjectIdGetDatum(x: Oid) -> Datum {
    x as Datum
}
#[inline]
unsafe fn BoolGetDatum(x: bool) -> Datum {
    if x {
        1
    } else {
        0
    }
}
#[inline]
unsafe fn TransactionIdGetDatum(x: TransactionId) -> Datum {
    x as Datum
}
#[inline]
unsafe fn TimestampTzGetDatum(x: TimestampTz) -> Datum {
    x as Datum
}
#[inline]
unsafe fn PointerGetDatum(x: *const c_void) -> Datum {
    x as Datum
}
#[inline]
unsafe fn CStringGetDatum(x: *const c_char) -> Datum {
    x as Datum
}

#[inline]
unsafe fn PG_RETURN_INT64(x: int64) -> Datum {
    Int64GetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_INT32(x: int32) -> Datum {
    Int32GetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_OID(x: Oid) -> Datum {
    ObjectIdGetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_BOOL(x: bool) -> Datum {
    BoolGetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_FLOAT8(x: f64) -> Datum {
    Float8GetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_TIMESTAMPTZ(x: TimestampTz) -> Datum {
    TimestampTzGetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_TEXT_P(x: *mut c_void) -> Datum {
    PointerGetDatum(x)
}
#[inline]
unsafe fn PG_RETURN_DATUM(x: Datum) -> Datum {
    x
}

// HAS_PGSTAT_PERMISSIONS(role)
#[inline]
unsafe fn HAS_PGSTAT_PERMISSIONS(role: Oid) -> bool {
    has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || has_privs_of_role(GetUserId(), role)
}

// UINT32_ACCESS_ONCE(var)
#[inline]
unsafe fn UINT32_ACCESS_ONCE(var: *const uint32) -> uint32 {
    core::ptr::read_volatile(var)
}

// ---------------------------------------------------------------------------
// Cross-file dependency stubs (TODO(pg-port)). All from OTHER .c/.h files.
// ---------------------------------------------------------------------------

unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/acl.c
}

unsafe fn pgstat_fetch_stat_tabentry(_relid: Oid) -> *mut PgStat_StatTabEntry {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_relation.c
}
unsafe fn pgstat_fetch_stat_funcentry(_funcid: Oid) -> *mut PgStat_StatFuncEntry {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_function.c
}
unsafe fn pgstat_fetch_stat_dbentry(_dbid: Oid) -> *mut PgStat_StatDBEntry {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_database.c
}
unsafe fn pgstat_fetch_stat_numbackends() -> c_int {
    unimplemented!() // TODO(pg-port): utils/activity/backend_status.c
}
unsafe fn pgstat_get_local_beentry_by_index(_idx: c_int) -> *mut LocalPgBackendStatus {
    unimplemented!() // TODO(pg-port): utils/activity/backend_status.c
}
unsafe fn pgstat_get_local_beentry_by_proc_number(_procNumber: ProcNumber) -> *mut LocalPgBackendStatus {
    unimplemented!() // TODO(pg-port): utils/activity/backend_status.c
}
unsafe fn pgstat_get_beentry_by_proc_number(_procNumber: ProcNumber) -> *mut PgBackendStatus {
    unimplemented!() // TODO(pg-port): utils/activity/backend_status.c
}
unsafe fn pgstat_clip_activity(_raw_activity: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/activity/backend_status.c
}
unsafe fn pgstat_get_wait_event_type(_raw_wait_event: uint32) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/activity/wait_event.c
}
unsafe fn pgstat_get_wait_event(_raw_wait_event: uint32) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/activity/wait_event.c
}
unsafe fn pgstat_fetch_stat_checkpointer() -> *mut PgStat_CheckpointerStats {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_checkpointer.c
}
unsafe fn pgstat_fetch_stat_bgwriter() -> *mut PgStat_BgWriterStats {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_bgwriter.c
}
unsafe fn pgstat_fetch_stat_io() -> *mut PgStat_IO {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_fetch_stat_backend_by_pid(_pid: c_int, _bktype: *mut BackendType) -> *mut PgStat_Backend {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_backend.c
}
unsafe fn pgstat_fetch_stat_wal() -> *mut PgStat_WalStats {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_wal.c
}
unsafe fn pgstat_fetch_slru() -> *mut PgStat_SLRUStats {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_slru.c
}
unsafe fn pgstat_get_slru_name(_idx: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_slru.c
}
unsafe fn pgstat_fetch_stat_archiver() -> *mut PgStat_ArchiverStats {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_archiver.c
}
unsafe fn pgstat_fetch_replslot(_slotname: NameData) -> *mut PgStat_StatReplSlotEntry {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_replslot.c
}
unsafe fn pgstat_fetch_stat_subscription(_subid: Oid) -> *mut PgStat_StatSubEntry {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_subscription.c
}
unsafe fn pgstat_get_io_object_name(_io_object: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_get_io_context_name(_io_context: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_tracks_io_object(_bktype: BackendType, _io_object: c_int, _io_context: c_int) -> bool {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_tracks_io_op(
    _bktype: BackendType,
    _io_object: c_int,
    _io_context: c_int,
    _io_op: IOOp,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_tracks_io_bktype(_bktype: BackendType) -> bool {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_bktype_io_stats_valid(_backend_io: *mut PgStat_BktypeIO, _bktype: BackendType) -> bool {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_io.c
}
unsafe fn pgstat_tracks_backend_bktype(_bktype: BackendType) -> bool {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_backend.c
}
unsafe fn pgstat_get_stat_snapshot_timestamp(_have_snapshot: *mut bool) -> TimestampTz {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_clear_snapshot() {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_force_next_flush() {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_reset_counters() {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_reset_of_kind(_kind: PgStat_Kind) {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_reset(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_reset_slru(_target: *const c_char) {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_slru.c
}
unsafe fn pgstat_reset_replslot(_target: *const c_char) {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_replslot.c
}
unsafe fn pgstat_get_kind_from_str(_kind_str: *mut c_char) -> PgStat_Kind {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_have_entry(_kind: PgStat_Kind, _dboid: Oid, _objid: u64) -> bool {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn find_tabstat_entry(_rel_id: Oid) -> *mut PgStat_TableStatus {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_relation.c
}
unsafe fn find_funcstat_entry(_func_id: Oid) -> *mut PgStat_FunctionCounts {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat_function.c
}

unsafe fn BackendPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): storage/lmgr/proc.c
}
unsafe fn AuxiliaryPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): storage/lmgr/proc.c
}
unsafe fn GetNumberFromPGProc(_proc: *mut PGPROC) -> ProcNumber {
    unimplemented!() // TODO(pg-port): storage/lmgr/proc.c
}
unsafe fn GetLeaderApplyWorkerPid(_pid: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): replication/logical/launcher.c
}
unsafe fn GetBackgroundWorkerTypeByPid(_pid: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): postmaster/bgworker.c
}
unsafe fn GetBackendTypeDesc(_backendType: BackendType) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/init/miscinit.c
}
unsafe fn DataChecksumsEnabled() -> bool {
    unimplemented!() // TODO(pg-port): access/transam/xlog.c
}
unsafe fn IsSharedRelation(_relationId: Oid) -> bool {
    unimplemented!() // TODO(pg-port): catalog/catalog.c
}
unsafe fn XLogPrefetchResetStats() {
    unimplemented!() // TODO(pg-port): access/transam/xlogprefetcher.c
}

unsafe fn text_to_cstring(_t: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): utils/builtins.h
}
unsafe fn pg_strcasecmp(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): port/pgstrcasecmp.c
}
unsafe fn namestrcpy(_name: *mut NameData, _str: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): utils/adt/name.c
}
unsafe fn NameStr(_name: *const NameData) -> *const c_char {
    unimplemented!() // TODO(pg-port): c.h
}
unsafe fn pg_getnameinfo_all(
    _addr: *const c_void,
    _salen: c_int,
    _node: *mut c_char,
    _nodelen: c_int,
    _service: *mut c_char,
    _servicelen: c_int,
    _flags: c_int,
) -> c_int {
    unimplemented!() // TODO(pg-port): common/ip.c
}
unsafe fn clean_ipv6_addr(_addr_family: c_int, _host: *mut c_char) {
    unimplemented!() // TODO(pg-port): utils/adt/network.c
}
unsafe fn pg_memory_is_all_zeros(_ptr: *const c_void, _len: usize) -> bool {
    unimplemented!() // TODO(pg-port): utils/memutils_internal.h
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): access/transam.h
}

unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn tuplestore_putvalues(
    _state: *mut c_void,
    _tdesc: *mut c_void,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port): utils/sort/tuplestore.c
}
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO(pg-port): access/common/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) {
    unimplemented!() // TODO(pg-port): access/common/tupdesc.c
}
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn heap_form_tuple(_tupleDescriptor: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn atoi(s: *const c_char) -> c_int;
    fn pfree(pointer: *mut c_void);
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

// Backend-status struct accessors (backend_status.h fields not modeled in the
// opaque stubs above). All field reads/writes funnel through these so the
// translated bodies stay 1:1 while the real layout is ported elsewhere.
// TODO(pg-port): utils/activity/backend_status.h
unsafe fn lbe_backendStatus(_lbe: *mut LocalPgBackendStatus) -> *mut PgBackendStatus {
    unimplemented!() // TODO(pg-port): &local_beentry->backendStatus
}
unsafe fn lbe_proc_number(_lbe: *mut LocalPgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): local_beentry->proc_number
}
unsafe fn lbe_backend_xid(_lbe: *mut LocalPgBackendStatus) -> TransactionId {
    unimplemented!() // TODO(pg-port): local_beentry->backend_xid
}
unsafe fn lbe_backend_xmin(_lbe: *mut LocalPgBackendStatus) -> TransactionId {
    unimplemented!() // TODO(pg-port): local_beentry->backend_xmin
}
unsafe fn lbe_backend_subxact_count(_lbe: *mut LocalPgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): local_beentry->backend_subxact_count
}
unsafe fn lbe_backend_subxact_overflowed(_lbe: *mut LocalPgBackendStatus) -> bool {
    unimplemented!() // TODO(pg-port): local_beentry->backend_subxact_overflowed
}

// PgStat_StatTabEntry accessor (used by the int64/float8/timestamptz macros).
// TODO(pg-port): utils/activity/pgstat.h
unsafe fn tabentry_i64(_tabentry: *mut PgStat_StatTabEntry, _which: &str) -> i64 {
    unimplemented!() // TODO(pg-port): tabentry-><stat>
}
unsafe fn tabentry_f64(_tabentry: *mut PgStat_StatTabEntry, _which: &str) -> f64 {
    unimplemented!() // TODO(pg-port): tabentry-><stat>
}
unsafe fn tabentry_ts(_tabentry: *mut PgStat_StatTabEntry, _which: &str) -> TimestampTz {
    unimplemented!() // TODO(pg-port): tabentry-><stat>
}
unsafe fn dbentry_i64(_dbentry: *mut PgStat_StatDBEntry, _which: &str) -> i64 {
    unimplemented!() // TODO(pg-port): dbentry-><stat>
}
unsafe fn dbentry_f64_ms(_dbentry: *mut PgStat_StatDBEntry, _which: &str) -> f64 {
    unimplemented!() // TODO(pg-port): ((double) dbentry-><stat>) / 1000.0
}
unsafe fn tabstatus_i64(_tabentry: *mut PgStat_TableStatus, _which: &str) -> i64 {
    unimplemented!() // TODO(pg-port): tabentry->counts.<stat>
}
unsafe fn funccounts_f64_ms(_funcentry: *mut PgStat_FunctionCounts, _which: &str) -> f64 {
    unimplemented!() // TODO(pg-port): INSTR_TIME_GET_MILLISEC(funcentry-><stat>)
}

// PG_STAT_GET_RELENTRY_INT64(stat)
macro_rules! PG_STAT_GET_RELENTRY_INT64 {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: int64;
            let tabentry: *mut PgStat_StatTabEntry;

            tabentry = pgstat_fetch_stat_tabentry(relid);
            if tabentry.is_null() {
                result = 0;
            } else {
                result = tabentry_i64(tabentry, $stat) as int64;
            }

            PG_RETURN_INT64(result)
        }
    };
}

/* pg_stat_get_analyze_count */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_analyze_count, "analyze_count");
/* pg_stat_get_autoanalyze_count */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_autoanalyze_count, "autoanalyze_count");
/* pg_stat_get_autovacuum_count */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_autovacuum_count, "autovacuum_count");
/* pg_stat_get_blocks_fetched */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_blocks_fetched, "blocks_fetched");
/* pg_stat_get_blocks_hit */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_blocks_hit, "blocks_hit");
/* pg_stat_get_dead_tuples */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_dead_tuples, "dead_tuples");
/* pg_stat_get_ins_since_vacuum */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_ins_since_vacuum, "ins_since_vacuum");
/* pg_stat_get_live_tuples */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_live_tuples, "live_tuples");
/* pg_stat_get_mod_since_analyze */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_mod_since_analyze, "mod_since_analyze");
/* pg_stat_get_numscans */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_numscans, "numscans");
/* pg_stat_get_tuples_deleted */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_deleted, "tuples_deleted");
/* pg_stat_get_tuples_fetched */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_fetched, "tuples_fetched");
/* pg_stat_get_tuples_hot_updated */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_hot_updated, "tuples_hot_updated");
/* pg_stat_get_tuples_newpage_updated */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_newpage_updated, "tuples_newpage_updated");
/* pg_stat_get_tuples_inserted */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_inserted, "tuples_inserted");
/* pg_stat_get_tuples_returned */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_returned, "tuples_returned");
/* pg_stat_get_tuples_updated */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_tuples_updated, "tuples_updated");
/* pg_stat_get_vacuum_count */
PG_STAT_GET_RELENTRY_INT64!(pg_stat_get_vacuum_count, "vacuum_count");

// PG_STAT_GET_RELENTRY_FLOAT8(stat)
macro_rules! PG_STAT_GET_RELENTRY_FLOAT8 {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: f64;
            let tabentry: *mut PgStat_StatTabEntry;

            tabentry = pgstat_fetch_stat_tabentry(relid);
            if tabentry.is_null() {
                result = 0.0;
            } else {
                result = tabentry_f64(tabentry, $stat) as f64;
            }

            PG_RETURN_FLOAT8(result)
        }
    };
}

/* pg_stat_get_total_vacuum_time */
PG_STAT_GET_RELENTRY_FLOAT8!(pg_stat_get_total_vacuum_time, "total_vacuum_time");
/* pg_stat_get_total_autovacuum_time */
PG_STAT_GET_RELENTRY_FLOAT8!(pg_stat_get_total_autovacuum_time, "total_autovacuum_time");
/* pg_stat_get_total_analyze_time */
PG_STAT_GET_RELENTRY_FLOAT8!(pg_stat_get_total_analyze_time, "total_analyze_time");
/* pg_stat_get_total_autoanalyze_time */
PG_STAT_GET_RELENTRY_FLOAT8!(pg_stat_get_total_autoanalyze_time, "total_autoanalyze_time");

// PG_STAT_GET_RELENTRY_TIMESTAMPTZ(stat)
macro_rules! PG_STAT_GET_RELENTRY_TIMESTAMPTZ {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: TimestampTz;
            let tabentry: *mut PgStat_StatTabEntry;

            tabentry = pgstat_fetch_stat_tabentry(relid);
            if tabentry.is_null() {
                result = 0;
            } else {
                result = tabentry_ts(tabentry, $stat);
            }

            if result == 0 {
                PG_RETURN_NULL!(fcinfo)
            } else {
                PG_RETURN_TIMESTAMPTZ(result)
            }
        }
    };
}

/* pg_stat_get_last_analyze_time */
PG_STAT_GET_RELENTRY_TIMESTAMPTZ!(pg_stat_get_last_analyze_time, "last_analyze_time");
/* pg_stat_get_last_autoanalyze_time */
PG_STAT_GET_RELENTRY_TIMESTAMPTZ!(pg_stat_get_last_autoanalyze_time, "last_autoanalyze_time");
/* pg_stat_get_last_autovacuum_time */
PG_STAT_GET_RELENTRY_TIMESTAMPTZ!(pg_stat_get_last_autovacuum_time, "last_autovacuum_time");
/* pg_stat_get_last_vacuum_time */
PG_STAT_GET_RELENTRY_TIMESTAMPTZ!(pg_stat_get_last_vacuum_time, "last_vacuum_time");
/* pg_stat_get_lastscan */
PG_STAT_GET_RELENTRY_TIMESTAMPTZ!(pg_stat_get_lastscan, "lastscan");

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_function_calls(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let funcentry: *mut PgStat_StatFuncEntry;

    funcentry = pgstat_fetch_stat_funcentry(funcid);
    if funcentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_INT64((*funcentry).numcalls)
}

/* convert counter from microsec to millisec for display */
// PG_STAT_GET_FUNCENTRY_FLOAT8_MS(stat)
macro_rules! PG_STAT_GET_FUNCENTRY_FLOAT8_MS {
    ($fname:ident, $stat:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: f64;
            let funcentry: *mut PgStat_StatFuncEntry;

            funcentry = pgstat_fetch_stat_funcentry(funcid);
            if funcentry.is_null() {
                return PG_RETURN_NULL!(fcinfo);
            }
            result = ((*funcentry).$stat as f64) / 1000.0;
            PG_RETURN_FLOAT8(result)
        }
    };
}

/* pg_stat_get_function_total_time */
PG_STAT_GET_FUNCENTRY_FLOAT8_MS!(pg_stat_get_function_total_time, total_time);
/* pg_stat_get_function_self_time */
PG_STAT_GET_FUNCENTRY_FLOAT8_MS!(pg_stat_get_function_self_time, self_time);

// SRF support (funcapi.h) -- not yet ported; stubbed locally.
unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(_fcinfo: FunctionCallInfo, _fctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fcinfo: FunctionCallInfo, _fctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}

macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => {
        srf_is_firstcall($fcinfo)
    };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => {
        srf_firstcall_init($fcinfo)
    };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => {
        srf_percall_setup($fcinfo)
    };
}
macro_rules! SRF_RETURN_NEXT {
    ($fcinfo:expr, $fctx:expr, $result:expr) => {
        return srf_return_next($fcinfo, $fctx, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($fcinfo:expr, $fctx:expr) => {
        return srf_return_done($fcinfo, $fctx)
    };
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_idset(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;
    let fctx: *mut c_int;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        /* create a function context for cross-call persistence */
        let funcctx = SRF_FIRSTCALL_INIT!(fcinfo);

        let fctx = MemoryContextAlloc((*funcctx).multi_call_memory_ctx, core::mem::size_of::<c_int>()) as *mut c_int;
        (*funcctx).user_fctx = fctx as *mut c_void;

        *fctx.add(0) = 0;
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    fctx = (*funcctx).user_fctx as *mut c_int;

    *fctx.add(0) += 1;

    /*
     * We recheck pgstat_fetch_stat_numbackends() each time through, just in
     * case the local status data has been refreshed since we started.  It's
     * plenty cheap enough if not.  If a refresh does happen, we'll likely
     * miss or duplicate some backend IDs, but we're content not to crash.
     * (Refreshing midway through such a query would be problematic usage
     * anyway, since the backend IDs we've already returned might no longer
     * refer to extant sessions.)
     */
    if *fctx.add(0) <= pgstat_fetch_stat_numbackends() {
        /* do when there is more left to send */
        let local_beentry: *mut LocalPgBackendStatus = pgstat_get_local_beentry_by_index(*fctx.add(0));

        SRF_RETURN_NEXT!(fcinfo, funcctx, Int32GetDatum(lbe_proc_number(local_beentry)));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE!(fcinfo, funcctx);
    }
}

// fmgr-callable functions referenced via DirectFunctionCallN! (other .c files).
unsafe fn inet_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): utils/adt/network.c
}
unsafe fn int4in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): utils/adt/int.c
}
unsafe fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): utils/adt/numeric.c
}

/*
 * Returns command progress information for the named command.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_progress_info(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_PROGRESS_COLS: usize = PGSTAT_NUM_PROGRESS_PARAM + 3;
    let num_backends: c_int = pgstat_fetch_stat_numbackends();
    let mut curr_backend: c_int;
    let cmd: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut c_void);
    let cmdtype: ProgressCommandType;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    /* Translate command name into command type code. */
    if pg_strcasecmp(cmd, c"VACUUM".as_ptr()) == 0 {
        cmdtype = PROGRESS_COMMAND_VACUUM;
    } else if pg_strcasecmp(cmd, c"ANALYZE".as_ptr()) == 0 {
        cmdtype = PROGRESS_COMMAND_ANALYZE;
    } else if pg_strcasecmp(cmd, c"CLUSTER".as_ptr()) == 0 {
        cmdtype = PROGRESS_COMMAND_CLUSTER;
    } else if pg_strcasecmp(cmd, c"CREATE INDEX".as_ptr()) == 0 {
        cmdtype = PROGRESS_COMMAND_CREATE_INDEX;
    } else if pg_strcasecmp(cmd, c"BASEBACKUP".as_ptr()) == 0 {
        cmdtype = PROGRESS_COMMAND_BASEBACKUP;
    } else if pg_strcasecmp(cmd, c"COPY".as_ptr()) == 0 {
        cmdtype = PROGRESS_COMMAND_COPY;
    } else {
        ereport!(
            ERROR,
            errmsg!("invalid command name: \"{}\"", CStr::from_ptr(cmd).to_string_lossy())
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        unreachable!()
    }

    InitMaterializedSRF(fcinfo, 0);

    /* 1-based index */
    curr_backend = 1;
    while curr_backend <= num_backends {
        let local_beentry: *mut LocalPgBackendStatus;
        let beentry: *mut PgBackendStatus;
        let mut values: [Datum; PG_STAT_GET_PROGRESS_COLS] = [0; PG_STAT_GET_PROGRESS_COLS];
        let mut nulls: [bool; PG_STAT_GET_PROGRESS_COLS] = [false; PG_STAT_GET_PROGRESS_COLS];
        let mut i: c_int;

        local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
        beentry = lbe_backendStatus(local_beentry);

        /*
         * Report values for only those backends which are running the given
         * command.
         */
        if be_st_progress_command(beentry) != cmdtype {
            curr_backend += 1;
            continue;
        }

        /* Value available to all callers */
        values[0] = Int32GetDatum(be_st_procpid(beentry));
        values[1] = ObjectIdGetDatum(be_st_databaseid(beentry));

        /* show rest of the values including relid only to role members */
        if HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
            values[2] = ObjectIdGetDatum(be_st_progress_command_target(beentry));
            i = 0;
            while (i as usize) < PGSTAT_NUM_PROGRESS_PARAM {
                values[i as usize + 3] = Int64GetDatum(be_st_progress_param(beentry, i));
                i += 1;
            }
        } else {
            nulls[2] = true;
            i = 0;
            while (i as usize) < PGSTAT_NUM_PROGRESS_PARAM {
                nulls[i as usize + 3] = true;
                i += 1;
            }
        }

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        curr_backend += 1;
    }

    0 as Datum
}

/*
 * Returns activity of PG backends.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_activity(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_ACTIVITY_COLS: usize = 31;
    let num_backends: c_int = pgstat_fetch_stat_numbackends();
    let mut curr_backend: c_int;
    let pid: c_int = if PG_ARGISNULL!(fcinfo, 0) { -1 } else { PG_GETARG_INT32!(fcinfo, 0) };
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    InitMaterializedSRF(fcinfo, 0);

    /* 1-based index */
    curr_backend = 1;
    while curr_backend <= num_backends {
        /* for each row */
        let mut values: [Datum; PG_STAT_GET_ACTIVITY_COLS] = [0; PG_STAT_GET_ACTIVITY_COLS];
        let mut nulls: [bool; PG_STAT_GET_ACTIVITY_COLS] = [false; PG_STAT_GET_ACTIVITY_COLS];
        let local_beentry: *mut LocalPgBackendStatus;
        let beentry: *mut PgBackendStatus;
        let mut proc: *mut PGPROC;
        let mut wait_event_type: *const c_char = null();
        let mut wait_event: *const c_char = null();

        /* Get the next one in the list */
        local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
        beentry = lbe_backendStatus(local_beentry);

        /* If looking for specific PID, ignore all the others */
        if pid != -1 && be_st_procpid(beentry) != pid {
            curr_backend += 1;
            continue;
        }

        /* Values available to all callers */
        if be_st_databaseid(beentry) != InvalidOid {
            values[0] = ObjectIdGetDatum(be_st_databaseid(beentry));
        } else {
            nulls[0] = true;
        }

        values[1] = Int32GetDatum(be_st_procpid(beentry));

        if be_st_userid(beentry) != InvalidOid {
            values[2] = ObjectIdGetDatum(be_st_userid(beentry));
        } else {
            nulls[2] = true;
        }

        if !be_st_appname(beentry).is_null() {
            values[3] = CStringGetTextDatum(be_st_appname(beentry));
        } else {
            nulls[3] = true;
        }

        if TransactionIdIsValid(lbe_backend_xid(local_beentry)) {
            values[15] = TransactionIdGetDatum(lbe_backend_xid(local_beentry));
        } else {
            nulls[15] = true;
        }

        if TransactionIdIsValid(lbe_backend_xmin(local_beentry)) {
            values[16] = TransactionIdGetDatum(lbe_backend_xmin(local_beentry));
        } else {
            nulls[16] = true;
        }

        /* Values only available to role member or pg_read_all_stats */
        if HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
            let clipped_activity: *mut c_char;

            match be_st_state(beentry) {
                STATE_STARTING => {
                    values[4] = CStringGetTextDatum(c"starting".as_ptr());
                }
                STATE_IDLE => {
                    values[4] = CStringGetTextDatum(c"idle".as_ptr());
                }
                STATE_RUNNING => {
                    values[4] = CStringGetTextDatum(c"active".as_ptr());
                }
                STATE_IDLEINTRANSACTION => {
                    values[4] = CStringGetTextDatum(c"idle in transaction".as_ptr());
                }
                STATE_FASTPATH => {
                    values[4] = CStringGetTextDatum(c"fastpath function call".as_ptr());
                }
                STATE_IDLEINTRANSACTION_ABORTED => {
                    values[4] = CStringGetTextDatum(c"idle in transaction (aborted)".as_ptr());
                }
                STATE_DISABLED => {
                    values[4] = CStringGetTextDatum(c"disabled".as_ptr());
                }
                STATE_UNDEFINED => {
                    nulls[4] = true;
                }
                _ => {}
            }

            clipped_activity = pgstat_clip_activity(be_st_activity_raw(beentry));
            values[5] = CStringGetTextDatum(clipped_activity);
            pfree(clipped_activity as *mut c_void);

            /* leader_pid */
            nulls[29] = true;

            proc = BackendPidGetProc(be_st_procpid(beentry));

            if proc.is_null() && be_st_backendType(beentry) != B_BACKEND {
                /*
                 * For an auxiliary process, retrieve process info from
                 * AuxiliaryProcs stored in shared-memory.
                 */
                proc = AuxiliaryPidGetProc(be_st_procpid(beentry));
            }

            /*
             * If a PGPROC entry was retrieved, display wait events and lock
             * group leader or apply leader information if any.  To avoid
             * extra overhead, no extra lock is being held, so there is no
             * guarantee of consistency across multiple rows.
             */
            if !proc.is_null() {
                let raw_wait_event: uint32;
                let leader: *mut PGPROC;

                raw_wait_event = UINT32_ACCESS_ONCE(proc_wait_event_info_ptr(proc));
                wait_event_type = pgstat_get_wait_event_type(raw_wait_event);
                wait_event = pgstat_get_wait_event(raw_wait_event);

                leader = proc_lockGroupLeader(proc);

                /*
                 * Show the leader only for active parallel workers.  This
                 * leaves the field as NULL for the leader of a parallel group
                 * or the leader of parallel apply workers.
                 */
                if !leader.is_null() && proc_pid(leader) != be_st_procpid(beentry) {
                    values[29] = Int32GetDatum(proc_pid(leader));
                    nulls[29] = false;
                } else if be_st_backendType(beentry) == B_BG_WORKER {
                    let leader_pid: c_int = GetLeaderApplyWorkerPid(be_st_procpid(beentry));

                    if leader_pid != InvalidPid {
                        values[29] = Int32GetDatum(leader_pid);
                        nulls[29] = false;
                    }
                }
            }

            if !wait_event_type.is_null() {
                values[6] = CStringGetTextDatum(wait_event_type);
            } else {
                nulls[6] = true;
            }

            if !wait_event.is_null() {
                values[7] = CStringGetTextDatum(wait_event);
            } else {
                nulls[7] = true;
            }

            /*
             * Don't expose transaction time for walsenders; it confuses
             * monitoring, particularly because we don't keep the time up-to-
             * date.
             */
            if be_st_xact_start_timestamp(beentry) != 0 && be_st_backendType(beentry) != B_WAL_SENDER {
                values[8] = TimestampTzGetDatum(be_st_xact_start_timestamp(beentry));
            } else {
                nulls[8] = true;
            }

            if be_st_activity_start_timestamp(beentry) != 0 {
                values[9] = TimestampTzGetDatum(be_st_activity_start_timestamp(beentry));
            } else {
                nulls[9] = true;
            }

            if be_st_proc_start_timestamp(beentry) != 0 {
                values[10] = TimestampTzGetDatum(be_st_proc_start_timestamp(beentry));
            } else {
                nulls[10] = true;
            }

            if be_st_state_start_timestamp(beentry) != 0 {
                values[11] = TimestampTzGetDatum(be_st_state_start_timestamp(beentry));
            } else {
                nulls[11] = true;
            }

            /* A zeroed client addr means we don't know */
            if pg_memory_is_all_zeros(be_st_clientaddr(beentry), be_st_clientaddr_size(beentry)) {
                nulls[12] = true;
                nulls[13] = true;
                nulls[14] = true;
            } else {
                if be_clientaddr_family(beentry) == AF_INET || be_clientaddr_family(beentry) == AF_INET6 {
                    let mut remote_host: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
                    let mut remote_port: [c_char; NI_MAXSERV] = [0; NI_MAXSERV];
                    let ret: c_int;

                    remote_host[0] = b'\0' as c_char;
                    remote_port[0] = b'\0' as c_char;
                    ret = pg_getnameinfo_all(
                        be_clientaddr_addr(beentry),
                        be_clientaddr_salen(beentry),
                        remote_host.as_mut_ptr(),
                        core::mem::size_of_val(&remote_host) as c_int,
                        remote_port.as_mut_ptr(),
                        core::mem::size_of_val(&remote_port) as c_int,
                        NI_NUMERICHOST | NI_NUMERICSERV,
                    );
                    if ret == 0 {
                        clean_ipv6_addr(be_clientaddr_family(beentry), remote_host.as_mut_ptr());
                        values[12] = DirectFunctionCall1!(inet_in, CStringGetDatum(remote_host.as_ptr()));
                        if !be_st_clienthostname(beentry).is_null() && *be_st_clienthostname(beentry) != 0 {
                            values[13] = CStringGetTextDatum(be_st_clienthostname(beentry));
                        } else {
                            nulls[13] = true;
                        }
                        values[14] = Int32GetDatum(atoi(remote_port.as_ptr()));
                    } else {
                        nulls[12] = true;
                        nulls[13] = true;
                        nulls[14] = true;
                    }
                } else if be_clientaddr_family(beentry) == AF_UNIX {
                    /*
                     * Unix sockets always reports NULL for host and -1 for
                     * port, so it's possible to tell the difference to
                     * connections we have no permissions to view, or with
                     * errors.
                     */
                    nulls[12] = true;
                    nulls[13] = true;
                    values[14] = Int32GetDatum(-1);
                } else {
                    /* Unknown address type, should never happen */
                    nulls[12] = true;
                    nulls[13] = true;
                    nulls[14] = true;
                }
            }
            /* Add backend type */
            if be_st_backendType(beentry) == B_BG_WORKER {
                let bgw_type: *const c_char;

                bgw_type = GetBackgroundWorkerTypeByPid(be_st_procpid(beentry));
                if !bgw_type.is_null() {
                    values[17] = CStringGetTextDatum(bgw_type);
                } else {
                    nulls[17] = true;
                }
            } else {
                values[17] = CStringGetTextDatum(GetBackendTypeDesc(be_st_backendType(beentry)));
            }

            /* SSL information */
            if be_st_ssl(beentry) {
                values[18] = BoolGetDatum(true); /* ssl */
                values[19] = CStringGetTextDatum(be_ssl_version(beentry));
                values[20] = CStringGetTextDatum(be_ssl_cipher(beentry));
                values[21] = Int32GetDatum(be_ssl_bits(beentry));

                if *be_ssl_client_dn(beentry) != 0 {
                    values[22] = CStringGetTextDatum(be_ssl_client_dn(beentry));
                } else {
                    nulls[22] = true;
                }

                if *be_ssl_client_serial(beentry) != 0 {
                    values[23] = DirectFunctionCall3!(
                        numeric_in,
                        CStringGetDatum(be_ssl_client_serial(beentry)),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1)
                    );
                } else {
                    nulls[23] = true;
                }

                if *be_ssl_issuer_dn(beentry) != 0 {
                    values[24] = CStringGetTextDatum(be_ssl_issuer_dn(beentry));
                } else {
                    nulls[24] = true;
                }
            } else {
                values[18] = BoolGetDatum(false); /* ssl */
                nulls[19] = true;
                nulls[20] = true;
                nulls[21] = true;
                nulls[22] = true;
                nulls[23] = true;
                nulls[24] = true;
            }

            /* GSSAPI information */
            if be_st_gss(beentry) {
                values[25] = BoolGetDatum(be_gss_auth(beentry)); /* gss_auth */
                values[26] = CStringGetTextDatum(be_gss_princ(beentry));
                values[27] = BoolGetDatum(be_gss_enc(beentry)); /* GSS Encryption in use */
                values[28] = BoolGetDatum(be_gss_delegation(beentry)); /* GSS credentials
                                                                        * delegated */
            } else {
                values[25] = BoolGetDatum(false); /* gss_auth */
                nulls[26] = true; /* No GSS principal */
                values[27] = BoolGetDatum(false); /* GSS Encryption not in
                                                   * use */
                values[28] = BoolGetDatum(false); /* GSS credentials not
                                                   * delegated */
            }
            if be_st_query_id(beentry) == 0 {
                nulls[30] = true;
            } else {
                values[30] = Int64GetDatum(be_st_query_id(beentry));
            }
        } else {
            /* No permissions to view data about this session */
            values[5] = CStringGetTextDatum(c"<insufficient privilege>".as_ptr());
            nulls[4] = true;
            nulls[6] = true;
            nulls[7] = true;
            nulls[8] = true;
            nulls[9] = true;
            nulls[10] = true;
            nulls[11] = true;
            nulls[12] = true;
            nulls[13] = true;
            nulls[14] = true;
            nulls[17] = true;
            nulls[18] = true;
            nulls[19] = true;
            nulls[20] = true;
            nulls[21] = true;
            nulls[22] = true;
            nulls[23] = true;
            nulls[24] = true;
            nulls[25] = true;
            nulls[26] = true;
            nulls[27] = true;
            nulls[28] = true;
            nulls[29] = true;
            nulls[30] = true;
        }

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        /* If only a single backend was requested, and we found it, break. */
        if pid != -1 {
            break;
        }

        curr_backend += 1;
    }

    0 as Datum
}

// PgBackendStatus field accessors (utils/activity/backend_status.h).
// TODO(pg-port): real struct layout ported elsewhere.
unsafe fn be_st_procpid(_be: *mut PgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): beentry->st_procpid
}
unsafe fn be_st_databaseid(_be: *mut PgBackendStatus) -> Oid {
    unimplemented!() // TODO(pg-port): beentry->st_databaseid
}
unsafe fn be_st_userid(_be: *mut PgBackendStatus) -> Oid {
    unimplemented!() // TODO(pg-port): beentry->st_userid
}
unsafe fn be_st_appname(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_appname
}
unsafe fn be_st_state(_be: *mut PgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): beentry->st_state
}
unsafe fn be_st_activity_raw(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_activity_raw
}
unsafe fn be_st_backendType(_be: *mut PgBackendStatus) -> BackendType {
    unimplemented!() // TODO(pg-port): beentry->st_backendType
}
unsafe fn be_st_progress_command(_be: *mut PgBackendStatus) -> ProgressCommandType {
    unimplemented!() // TODO(pg-port): beentry->st_progress_command
}
unsafe fn be_st_progress_command_target(_be: *mut PgBackendStatus) -> Oid {
    unimplemented!() // TODO(pg-port): beentry->st_progress_command_target
}
unsafe fn be_st_progress_param(_be: *mut PgBackendStatus, _i: c_int) -> i64 {
    unimplemented!() // TODO(pg-port): beentry->st_progress_param[i]
}
unsafe fn be_st_xact_start_timestamp(_be: *mut PgBackendStatus) -> TimestampTz {
    unimplemented!() // TODO(pg-port): beentry->st_xact_start_timestamp
}
unsafe fn be_st_activity_start_timestamp(_be: *mut PgBackendStatus) -> TimestampTz {
    unimplemented!() // TODO(pg-port): beentry->st_activity_start_timestamp
}
unsafe fn be_st_proc_start_timestamp(_be: *mut PgBackendStatus) -> TimestampTz {
    unimplemented!() // TODO(pg-port): beentry->st_proc_start_timestamp
}
unsafe fn be_st_state_start_timestamp(_be: *mut PgBackendStatus) -> TimestampTz {
    unimplemented!() // TODO(pg-port): beentry->st_state_start_timestamp
}
unsafe fn be_st_clientaddr(_be: *mut PgBackendStatus) -> *const c_void {
    unimplemented!() // TODO(pg-port): &beentry->st_clientaddr
}
unsafe fn be_st_clientaddr_size(_be: *mut PgBackendStatus) -> usize {
    unimplemented!() // TODO(pg-port): sizeof(beentry->st_clientaddr)
}
unsafe fn be_clientaddr_family(_be: *mut PgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): beentry->st_clientaddr.addr.ss_family
}
unsafe fn be_clientaddr_addr(_be: *mut PgBackendStatus) -> *const c_void {
    unimplemented!() // TODO(pg-port): &beentry->st_clientaddr.addr
}
unsafe fn be_clientaddr_salen(_be: *mut PgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): beentry->st_clientaddr.salen
}
unsafe fn be_st_clienthostname(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_clienthostname
}
unsafe fn be_st_ssl(_be: *mut PgBackendStatus) -> bool {
    unimplemented!() // TODO(pg-port): beentry->st_ssl
}
unsafe fn be_ssl_version(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_sslstatus->ssl_version
}
unsafe fn be_ssl_cipher(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_sslstatus->ssl_cipher
}
unsafe fn be_ssl_bits(_be: *mut PgBackendStatus) -> c_int {
    unimplemented!() // TODO(pg-port): beentry->st_sslstatus->ssl_bits
}
unsafe fn be_ssl_client_dn(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_sslstatus->ssl_client_dn
}
unsafe fn be_ssl_client_serial(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_sslstatus->ssl_client_serial
}
unsafe fn be_ssl_issuer_dn(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_sslstatus->ssl_issuer_dn
}
unsafe fn be_st_gss(_be: *mut PgBackendStatus) -> bool {
    unimplemented!() // TODO(pg-port): beentry->st_gss
}
unsafe fn be_gss_auth(_be: *mut PgBackendStatus) -> bool {
    unimplemented!() // TODO(pg-port): beentry->st_gssstatus->gss_auth
}
unsafe fn be_gss_princ(_be: *mut PgBackendStatus) -> *const c_char {
    unimplemented!() // TODO(pg-port): beentry->st_gssstatus->gss_princ
}
unsafe fn be_gss_enc(_be: *mut PgBackendStatus) -> bool {
    unimplemented!() // TODO(pg-port): beentry->st_gssstatus->gss_enc
}
unsafe fn be_gss_delegation(_be: *mut PgBackendStatus) -> bool {
    unimplemented!() // TODO(pg-port): beentry->st_gssstatus->gss_delegation
}
unsafe fn be_st_query_id(_be: *mut PgBackendStatus) -> i64 {
    unimplemented!() // TODO(pg-port): beentry->st_query_id
}

// PGPROC field accessors (storage/proc.h).
unsafe fn proc_wait_event_info_ptr(_proc: *mut PGPROC) -> *const uint32 {
    unimplemented!() // TODO(pg-port): &proc->wait_event_info
}
unsafe fn proc_lockGroupLeader(_proc: *mut PGPROC) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): proc->lockGroupLeader
}
unsafe fn proc_pid(_proc: *mut PGPROC) -> c_int {
    unimplemented!() // TODO(pg-port): proc->pid
}

extern "C" {
    pub static mut MyProcPid: c_int; // miscadmin.h
    pub static mut MyDatabaseId: Oid; // miscadmin.h
}

#[no_mangle]
pub unsafe extern "C" fn pg_backend_pid(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32(MyProcPid)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_pid(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32(be_st_procpid(beentry))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_dbid(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_OID(be_st_databaseid(beentry))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_userid(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_OID(be_st_userid(beentry))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_subxact(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_SUBXACT_COLS: usize = 2;
    let tupdesc: TupleDesc;
    let mut values: [Datum; PG_STAT_GET_SUBXACT_COLS] = [0; PG_STAT_GET_SUBXACT_COLS];
    let mut nulls: [bool; PG_STAT_GET_SUBXACT_COLS] = [false; PG_STAT_GET_SUBXACT_COLS];
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let local_beentry: *mut LocalPgBackendStatus;

    /* Initialise attributes information in the tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(PG_STAT_GET_SUBXACT_COLS as c_int);
    TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"subxact_count".as_ptr(), INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"subxact_overflow".as_ptr(), BOOLOID, -1, 0);

    BlessTupleDesc(tupdesc);

    local_beentry = pgstat_get_local_beentry_by_proc_number(procNumber);
    if !local_beentry.is_null() {
        /* Fill values and NULLs */
        values[0] = Int32GetDatum(lbe_backend_subxact_count(local_beentry));
        values[1] = BoolGetDatum(lbe_backend_subxact_overflowed(local_beentry));
    } else {
        nulls[0] = true;
        nulls[1] = true;
    }

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr())))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_activity(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;
    let activity: *const c_char;
    let clipped_activity: *mut c_char;
    let ret: *mut text;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        activity = c"<backend information not available>".as_ptr();
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        activity = c"<insufficient privilege>".as_ptr();
    } else if *be_st_activity_raw(beentry) == b'\0' as c_char {
        activity = c"<command string not enabled>".as_ptr();
    } else {
        activity = be_st_activity_raw(beentry);
    }

    clipped_activity = pgstat_clip_activity(activity);
    ret = cstring_to_text(clipped_activity) as *mut text;
    pfree(clipped_activity as *mut c_void);

    PG_RETURN_TEXT_P(ret as *mut c_void)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_wait_event_type(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;
    let mut proc: *mut PGPROC;
    let mut wait_event_type: *const c_char = null();

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        wait_event_type = c"<backend information not available>".as_ptr();
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        wait_event_type = c"<insufficient privilege>".as_ptr();
    } else {
        proc = BackendPidGetProc(be_st_procpid(beentry));
        if proc.is_null() {
            proc = AuxiliaryPidGetProc(be_st_procpid(beentry));
        }
        if !proc.is_null() {
            wait_event_type = pgstat_get_wait_event_type(*proc_wait_event_info_ptr(proc));
        }
    }

    if wait_event_type.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P(cstring_to_text(wait_event_type))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_wait_event(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;
    let mut proc: *mut PGPROC;
    let mut wait_event: *const c_char = null();

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        wait_event = c"<backend information not available>".as_ptr();
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        wait_event = c"<insufficient privilege>".as_ptr();
    } else {
        proc = BackendPidGetProc(be_st_procpid(beentry));
        if proc.is_null() {
            proc = AuxiliaryPidGetProc(be_st_procpid(beentry));
        }
        if !proc.is_null() {
            wait_event = pgstat_get_wait_event(*proc_wait_event_info_ptr(proc));
        }
    }

    if wait_event.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P(cstring_to_text(wait_event))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_activity_start(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let result: TimestampTz;
    let beentry: *mut PgBackendStatus;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    result = be_st_activity_start_timestamp(beentry);

    /*
     * No time recorded for start of current query -- this is the case if the
     * user hasn't enabled query-level stats collection.
     */
    if result == 0 {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TIMESTAMPTZ(result)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_xact_start(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let result: TimestampTz;
    let beentry: *mut PgBackendStatus;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    result = be_st_xact_start_timestamp(beentry);

    if result == 0 {
        /* not in a transaction */
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TIMESTAMPTZ(result)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_start(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let result: TimestampTz;
    let beentry: *mut PgBackendStatus;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    result = be_st_proc_start_timestamp(beentry);

    if result == 0 {
        /* probably can't happen? */
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TIMESTAMPTZ(result)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_client_addr(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;
    let mut remote_host: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
    let ret: c_int;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    /* A zeroed client addr means we don't know */
    if pg_memory_is_all_zeros(be_st_clientaddr(beentry), be_st_clientaddr_size(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    match be_clientaddr_family(beentry) {
        AF_INET | AF_INET6 => {}
        _ => {
            return PG_RETURN_NULL!(fcinfo);
        }
    }

    remote_host[0] = b'\0' as c_char;
    ret = pg_getnameinfo_all(
        be_clientaddr_addr(beentry),
        be_clientaddr_salen(beentry),
        remote_host.as_mut_ptr(),
        core::mem::size_of_val(&remote_host) as c_int,
        null_mut(),
        0,
        NI_NUMERICHOST | NI_NUMERICSERV,
    );
    if ret != 0 {
        return PG_RETURN_NULL!(fcinfo);
    }

    clean_ipv6_addr(be_clientaddr_family(beentry), remote_host.as_mut_ptr());

    PG_RETURN_DATUM(DirectFunctionCall1!(inet_in, CStringGetDatum(remote_host.as_ptr())))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_client_port(fcinfo: FunctionCallInfo) -> Datum {
    let procNumber: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let beentry: *mut PgBackendStatus;
    let mut remote_port: [c_char; NI_MAXSERV] = [0; NI_MAXSERV];
    let ret: c_int;

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    } else if !HAS_PGSTAT_PERMISSIONS(be_st_userid(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    /* A zeroed client addr means we don't know */
    if pg_memory_is_all_zeros(be_st_clientaddr(beentry), be_st_clientaddr_size(beentry)) {
        return PG_RETURN_NULL!(fcinfo);
    }

    match be_clientaddr_family(beentry) {
        AF_INET | AF_INET6 => {}
        AF_UNIX => {
            return PG_RETURN_INT32(-1);
        }
        _ => {
            return PG_RETURN_NULL!(fcinfo);
        }
    }

    remote_port[0] = b'\0' as c_char;
    ret = pg_getnameinfo_all(
        be_clientaddr_addr(beentry),
        be_clientaddr_salen(beentry),
        null_mut(),
        0,
        remote_port.as_mut_ptr(),
        core::mem::size_of_val(&remote_port) as c_int,
        NI_NUMERICHOST | NI_NUMERICSERV,
    );
    if ret != 0 {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM(DirectFunctionCall1!(int4in, CStringGetDatum(remote_port.as_ptr())))
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_db_numbackends(fcinfo: FunctionCallInfo) -> Datum {
    let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut result: int32;
    let tot_backends: c_int = pgstat_fetch_stat_numbackends();
    let mut idx: c_int;

    result = 0;
    idx = 1;
    while idx <= tot_backends {
        let local_beentry: *mut LocalPgBackendStatus = pgstat_get_local_beentry_by_index(idx);

        if be_st_databaseid(lbe_backendStatus(local_beentry)) == dbid {
            result += 1;
        }
        idx += 1;
    }

    PG_RETURN_INT32(result)
}

// PG_STAT_GET_DBENTRY_INT64(stat)
macro_rules! PG_STAT_GET_DBENTRY_INT64 {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: int64;
            let dbentry: *mut PgStat_StatDBEntry;

            dbentry = pgstat_fetch_stat_dbentry(dbid);
            if dbentry.is_null() {
                result = 0;
            } else {
                result = dbentry_i64(dbentry, $stat) as int64;
            }

            PG_RETURN_INT64(result)
        }
    };
}

/* pg_stat_get_db_blocks_fetched */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_blocks_fetched, "blocks_fetched");
/* pg_stat_get_db_blocks_hit */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_blocks_hit, "blocks_hit");
/* pg_stat_get_db_conflict_bufferpin */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_conflict_bufferpin, "conflict_bufferpin");
/* pg_stat_get_db_conflict_lock */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_conflict_lock, "conflict_lock");
/* pg_stat_get_db_conflict_snapshot */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_conflict_snapshot, "conflict_snapshot");
/* pg_stat_get_db_conflict_startup_deadlock */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_conflict_startup_deadlock, "conflict_startup_deadlock");
/* pg_stat_get_db_conflict_tablespace */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_conflict_tablespace, "conflict_tablespace");
/* pg_stat_get_db_deadlocks */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_deadlocks, "deadlocks");
/* pg_stat_get_db_sessions */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_sessions, "sessions");
/* pg_stat_get_db_sessions_abandoned */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_sessions_abandoned, "sessions_abandoned");
/* pg_stat_get_db_sessions_fatal */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_sessions_fatal, "sessions_fatal");
/* pg_stat_get_db_sessions_killed */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_sessions_killed, "sessions_killed");
/* pg_stat_get_db_parallel_workers_to_launch */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_parallel_workers_to_launch, "parallel_workers_to_launch");
/* pg_stat_get_db_parallel_workers_launched */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_parallel_workers_launched, "parallel_workers_launched");
/* pg_stat_get_db_temp_bytes */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_temp_bytes, "temp_bytes");
/* pg_stat_get_db_temp_files */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_temp_files, "temp_files");
/* pg_stat_get_db_tuples_deleted */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_tuples_deleted, "tuples_deleted");
/* pg_stat_get_db_tuples_fetched */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_tuples_fetched, "tuples_fetched");
/* pg_stat_get_db_tuples_inserted */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_tuples_inserted, "tuples_inserted");
/* pg_stat_get_db_tuples_returned */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_tuples_returned, "tuples_returned");
/* pg_stat_get_db_tuples_updated */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_tuples_updated, "tuples_updated");
/* pg_stat_get_db_xact_commit */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_xact_commit, "xact_commit");
/* pg_stat_get_db_xact_rollback */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_xact_rollback, "xact_rollback");
/* pg_stat_get_db_conflict_logicalslot */
PG_STAT_GET_DBENTRY_INT64!(pg_stat_get_db_conflict_logicalslot, "conflict_logicalslot");

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_db_stat_reset_time(fcinfo: FunctionCallInfo) -> Datum {
    let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: TimestampTz;
    let dbentry: *mut PgStat_StatDBEntry;

    dbentry = pgstat_fetch_stat_dbentry(dbid);
    if dbentry.is_null() {
        result = 0;
    } else {
        result = dbentry_ts(dbentry, "stat_reset_timestamp");
    }

    if result == 0 {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_TIMESTAMPTZ(result)
    }
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_db_conflict_all(fcinfo: FunctionCallInfo) -> Datum {
    let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: int64;
    let dbentry: *mut PgStat_StatDBEntry;

    dbentry = pgstat_fetch_stat_dbentry(dbid);
    if dbentry.is_null() {
        result = 0;
    } else {
        result = (dbentry_i64(dbentry, "conflict_tablespace")
            + dbentry_i64(dbentry, "conflict_lock")
            + dbentry_i64(dbentry, "conflict_snapshot")
            + dbentry_i64(dbentry, "conflict_logicalslot")
            + dbentry_i64(dbentry, "conflict_bufferpin")
            + dbentry_i64(dbentry, "conflict_startup_deadlock")) as int64;
    }

    PG_RETURN_INT64(result)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_db_checksum_failures(fcinfo: FunctionCallInfo) -> Datum {
    let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: int64;
    let dbentry: *mut PgStat_StatDBEntry;

    if !DataChecksumsEnabled() {
        return PG_RETURN_NULL!(fcinfo);
    }

    dbentry = pgstat_fetch_stat_dbentry(dbid);
    if dbentry.is_null() {
        result = 0;
    } else {
        result = dbentry_i64(dbentry, "checksum_failures") as int64;
    }

    PG_RETURN_INT64(result)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_db_checksum_last_failure(fcinfo: FunctionCallInfo) -> Datum {
    let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: TimestampTz;
    let dbentry: *mut PgStat_StatDBEntry;

    if !DataChecksumsEnabled() {
        return PG_RETURN_NULL!(fcinfo);
    }

    dbentry = pgstat_fetch_stat_dbentry(dbid);
    if dbentry.is_null() {
        result = 0;
    } else {
        result = dbentry_ts(dbentry, "last_checksum_failure");
    }

    if result == 0 {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_TIMESTAMPTZ(result)
    }
}

/* convert counter from microsec to millisec for display */
// PG_STAT_GET_DBENTRY_FLOAT8_MS(stat)
macro_rules! PG_STAT_GET_DBENTRY_FLOAT8_MS {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let dbid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: f64;
            let dbentry: *mut PgStat_StatDBEntry;

            dbentry = pgstat_fetch_stat_dbentry(dbid);
            if dbentry.is_null() {
                result = 0.0;
            } else {
                result = dbentry_f64_ms(dbentry, $stat);
            }

            PG_RETURN_FLOAT8(result)
        }
    };
}

/* pg_stat_get_db_active_time */
PG_STAT_GET_DBENTRY_FLOAT8_MS!(pg_stat_get_db_active_time, "active_time");
/* pg_stat_get_db_blk_read_time */
PG_STAT_GET_DBENTRY_FLOAT8_MS!(pg_stat_get_db_blk_read_time, "blk_read_time");
/* pg_stat_get_db_blk_write_time */
PG_STAT_GET_DBENTRY_FLOAT8_MS!(pg_stat_get_db_blk_write_time, "blk_write_time");
/* pg_stat_get_db_idle_in_transaction_time */
PG_STAT_GET_DBENTRY_FLOAT8_MS!(pg_stat_get_db_idle_in_transaction_time, "idle_in_transaction_time");
/* pg_stat_get_db_session_time */
PG_STAT_GET_DBENTRY_FLOAT8_MS!(pg_stat_get_db_session_time, "session_time");

// dbentry timestamp accessor (utils/activity/pgstat.h).
unsafe fn dbentry_ts(_dbentry: *mut PgStat_StatDBEntry, _which: &str) -> TimestampTz {
    unimplemented!() // TODO(pg-port): dbentry-><stat>
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_num_timed(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).num_timed)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_num_requested(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).num_requested)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_num_performed(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).num_performed)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_restartpoints_timed(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).restartpoints_timed)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_restartpoints_requested(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).restartpoints_requested)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_restartpoints_performed(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).restartpoints_performed)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_buffers_written(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).buffers_written)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_slru_written(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_checkpointer()).slru_written)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_bgwriter_buf_written_clean(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_bgwriter()).buf_written_clean)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_bgwriter_maxwritten_clean(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_bgwriter()).maxwritten_clean)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_write_time(fcinfo: FunctionCallInfo) -> Datum {
    /* time is already in msec, just convert to double for presentation */
    PG_RETURN_FLOAT8((*pgstat_fetch_stat_checkpointer()).write_time as f64)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_sync_time(fcinfo: FunctionCallInfo) -> Datum {
    /* time is already in msec, just convert to double for presentation */
    PG_RETURN_FLOAT8((*pgstat_fetch_stat_checkpointer()).sync_time as f64)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_checkpointer_stat_reset_time(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ((*pgstat_fetch_stat_checkpointer()).stat_reset_timestamp)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_bgwriter_stat_reset_time(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ((*pgstat_fetch_stat_bgwriter()).stat_reset_timestamp)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_buf_alloc(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64((*pgstat_fetch_stat_bgwriter()).buf_alloc)
}

/*
* When adding a new column to the pg_stat_io view and the
* pg_stat_get_backend_io() function, add a new enum value here above
* IO_NUM_COLUMNS.
*/
type io_stat_col = c_int;
const IO_COL_INVALID: io_stat_col = -1;
const IO_COL_BACKEND_TYPE: io_stat_col = 0;
const IO_COL_OBJECT: io_stat_col = 1;
const IO_COL_CONTEXT: io_stat_col = 2;
const IO_COL_READS: io_stat_col = 3;
const IO_COL_READ_BYTES: io_stat_col = 4;
const IO_COL_READ_TIME: io_stat_col = 5;
const IO_COL_WRITES: io_stat_col = 6;
const IO_COL_WRITE_BYTES: io_stat_col = 7;
const IO_COL_WRITE_TIME: io_stat_col = 8;
const IO_COL_WRITEBACKS: io_stat_col = 9;
const IO_COL_WRITEBACK_TIME: io_stat_col = 10;
const IO_COL_EXTENDS: io_stat_col = 11;
const IO_COL_EXTEND_BYTES: io_stat_col = 12;
const IO_COL_EXTEND_TIME: io_stat_col = 13;
const IO_COL_HITS: io_stat_col = 14;
const IO_COL_EVICTIONS: io_stat_col = 15;
const IO_COL_REUSES: io_stat_col = 16;
const IO_COL_FSYNCS: io_stat_col = 17;
const IO_COL_FSYNC_TIME: io_stat_col = 18;
const IO_COL_RESET_TIME: io_stat_col = 19;
const IO_NUM_COLUMNS: usize = 20;

/*
 * When adding a new IOOp, add a new io_stat_col and add a case to this
 * function returning the corresponding io_stat_col.
 */
unsafe fn pgstat_get_io_op_index(io_op: IOOp) -> io_stat_col {
    match io_op {
        IOOP_EVICT => return IO_COL_EVICTIONS,
        IOOP_EXTEND => return IO_COL_EXTENDS,
        IOOP_FSYNC => return IO_COL_FSYNCS,
        IOOP_HIT => return IO_COL_HITS,
        IOOP_READ => return IO_COL_READS,
        IOOP_REUSE => return IO_COL_REUSES,
        IOOP_WRITE => return IO_COL_WRITES,
        IOOP_WRITEBACK => return IO_COL_WRITEBACKS,
        _ => {}
    }

    elog!(ERROR, "unrecognized IOOp value: {}", io_op);
    pg_unreachable()
}

/*
 * Get the number of the column containing IO bytes for the specified IOOp.
 * If an IOOp is not tracked in bytes, IO_COL_INVALID is returned.
 */
unsafe fn pgstat_get_io_byte_index(io_op: IOOp) -> io_stat_col {
    match io_op {
        IOOP_EXTEND => return IO_COL_EXTEND_BYTES,
        IOOP_READ => return IO_COL_READ_BYTES,
        IOOP_WRITE => return IO_COL_WRITE_BYTES,
        IOOP_EVICT | IOOP_FSYNC | IOOP_HIT | IOOP_REUSE | IOOP_WRITEBACK => return IO_COL_INVALID,
        _ => {}
    }

    elog!(ERROR, "unrecognized IOOp value: {}", io_op);
    pg_unreachable()
}

/*
 * Get the number of the column containing IO times for the specified IOOp.
 * If an op has no associated time, IO_COL_INVALID is returned.
 */
unsafe fn pgstat_get_io_time_index(io_op: IOOp) -> io_stat_col {
    match io_op {
        IOOP_READ => return IO_COL_READ_TIME,
        IOOP_WRITE => return IO_COL_WRITE_TIME,
        IOOP_WRITEBACK => return IO_COL_WRITEBACK_TIME,
        IOOP_EXTEND => return IO_COL_EXTEND_TIME,
        IOOP_FSYNC => return IO_COL_FSYNC_TIME,
        IOOP_EVICT | IOOP_HIT | IOOP_REUSE => return IO_COL_INVALID,
        _ => {}
    }

    elog!(ERROR, "unrecognized IOOp value: {}", io_op);
    pg_unreachable()
}

#[inline]
unsafe fn pg_stat_us_to_ms(val_ms: PgStat_Counter) -> f64 {
    val_ms as f64 * 0.001_f64
}

unsafe fn pg_unreachable() -> ! {
    unreachable!() // TODO(pg-port): c.h pg_unreachable()
}

/*
 * pg_stat_io_build_tuples
 *
 * Helper routine for pg_stat_get_io() and pg_stat_get_backend_io()
 * filling a result tuplestore with one tuple for each object and each
 * context supported by the caller, based on the contents of bktype_stats.
 */
unsafe fn pg_stat_io_build_tuples(
    rsinfo: *mut ReturnSetInfo,
    bktype_stats: *mut PgStat_BktypeIO,
    bktype: BackendType,
    stat_reset_timestamp: TimestampTz,
) {
    let bktype_desc: Datum = CStringGetTextDatum(GetBackendTypeDesc(bktype));

    for io_obj in 0..IOOBJECT_NUM_TYPES as c_int {
        let obj_name: *const c_char = pgstat_get_io_object_name(io_obj);

        for io_context in 0..IOCONTEXT_NUM_TYPES as c_int {
            let context_name: *const c_char = pgstat_get_io_context_name(io_context);

            let mut values: [Datum; IO_NUM_COLUMNS] = [0; IO_NUM_COLUMNS];
            let mut nulls: [bool; IO_NUM_COLUMNS] = [false; IO_NUM_COLUMNS];

            /*
             * Some combinations of BackendType, IOObject, and IOContext are
             * not valid for any type of IOOp. In such cases, omit the entire
             * row from the view.
             */
            if !pgstat_tracks_io_object(bktype, io_obj, io_context) {
                continue;
            }

            values[IO_COL_BACKEND_TYPE as usize] = bktype_desc;
            values[IO_COL_CONTEXT as usize] = CStringGetTextDatum(context_name);
            values[IO_COL_OBJECT as usize] = CStringGetTextDatum(obj_name);
            if stat_reset_timestamp != 0 {
                values[IO_COL_RESET_TIME as usize] = TimestampTzGetDatum(stat_reset_timestamp);
            } else {
                nulls[IO_COL_RESET_TIME as usize] = true;
            }

            for io_op in 0..IOOP_NUM_TYPES as c_int {
                let op_idx: c_int = pgstat_get_io_op_index(io_op);
                let time_idx: c_int = pgstat_get_io_time_index(io_op);
                let byte_idx: c_int = pgstat_get_io_byte_index(io_op);

                /*
                 * Some combinations of BackendType and IOOp, of IOContext and
                 * IOOp, and of IOObject and IOOp are not tracked. Set these
                 * cells in the view NULL.
                 */
                if pgstat_tracks_io_op(bktype, io_obj, io_context, io_op) {
                    let count: PgStat_Counter =
                        (*bktype_stats).counts[io_obj as usize][io_context as usize][io_op as usize];

                    values[op_idx as usize] = Int64GetDatum(count);
                } else {
                    nulls[op_idx as usize] = true;
                }

                if !nulls[op_idx as usize] {
                    /* not every operation is timed */
                    if time_idx != IO_COL_INVALID {
                        let time: PgStat_Counter =
                            (*bktype_stats).times[io_obj as usize][io_context as usize][io_op as usize];

                        values[time_idx as usize] = Float8GetDatum(pg_stat_us_to_ms(time));
                    }

                    /* not every IO is tracked in bytes */
                    if byte_idx != IO_COL_INVALID {
                        let mut buf: [c_char; 256] = [0; 256];
                        let byte: PgStat_Counter =
                            (*bktype_stats).bytes[io_obj as usize][io_context as usize][io_op as usize];

                        /* Convert to numeric */
                        snprintf(buf.as_mut_ptr(), core::mem::size_of_val(&buf), c"%lld".as_ptr(), byte);
                        values[byte_idx as usize] = DirectFunctionCall3!(
                            numeric_in,
                            CStringGetDatum(buf.as_ptr()),
                            ObjectIdGetDatum(0),
                            Int32GetDatum(-1)
                        );
                    }
                } else {
                    if time_idx != IO_COL_INVALID {
                        nulls[time_idx as usize] = true;
                    }
                    if byte_idx != IO_COL_INVALID {
                        nulls[byte_idx as usize] = true;
                    }
                }
            }

            tuplestore_putvalues(
                (*rsinfo).setResult,
                (*rsinfo).setDesc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_io(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo;
    let backends_io_stats: *mut PgStat_IO;

    InitMaterializedSRF(fcinfo, 0);
    rsinfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    backends_io_stats = pgstat_fetch_stat_io();

    for bktype in 0..BACKEND_NUM_TYPES as c_int {
        let bktype_stats: *mut PgStat_BktypeIO = &mut (*backends_io_stats).stats[bktype as usize];

        /*
         * In Assert builds, we can afford an extra loop through all of the
         * counters (in pg_stat_io_build_tuples()), checking that only
         * expected stats are non-zero, since it keeps the non-Assert code
         * cleaner.
         */
        Assert!(pgstat_bktype_io_stats_valid(bktype_stats, bktype));

        /*
         * For those BackendTypes without IO Operation stats, skip
         * representing them in the view altogether.
         */
        if !pgstat_tracks_io_bktype(bktype) {
            continue;
        }

        /* save tuples with data from this PgStat_BktypeIO */
        pg_stat_io_build_tuples(rsinfo, bktype_stats, bktype, (*backends_io_stats).stat_reset_timestamp);
    }

    0 as Datum
}

/*
 * Returns I/O statistics for a backend with given PID.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_io(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo;
    let mut bktype: BackendType = 0;
    let pid: c_int;
    let backend_stats: *mut PgStat_Backend;
    let bktype_stats: *mut PgStat_BktypeIO;

    InitMaterializedSRF(fcinfo, 0);
    rsinfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    pid = PG_GETARG_INT32!(fcinfo, 0);
    backend_stats = pgstat_fetch_stat_backend_by_pid(pid, &mut bktype);

    if backend_stats.is_null() {
        return 0 as Datum;
    }

    bktype_stats = &mut (*backend_stats).io_stats;

    /*
     * In Assert builds, we can afford an extra loop through all of the
     * counters (in pg_stat_io_build_tuples()), checking that only expected
     * stats are non-zero, since it keeps the non-Assert code cleaner.
     */
    Assert!(pgstat_bktype_io_stats_valid(bktype_stats, bktype));

    /* save tuples with data from this PgStat_BktypeIO */
    pg_stat_io_build_tuples(rsinfo, bktype_stats, bktype, (*backend_stats).stat_reset_timestamp);
    0 as Datum
}

/*
 * pg_stat_wal_build_tuple
 *
 * Helper routine for pg_stat_get_wal() and pg_stat_get_backend_wal()
 * returning one tuple based on the contents of wal_counters.
 */
unsafe fn pg_stat_wal_build_tuple(
    wal_counters: PgStat_WalCounters,
    stat_reset_timestamp: TimestampTz,
) -> Datum {
    const PG_STAT_WAL_COLS: usize = 5;
    let tupdesc: TupleDesc;
    let mut values: [Datum; PG_STAT_WAL_COLS] = [0; PG_STAT_WAL_COLS];
    let mut nulls: [bool; PG_STAT_WAL_COLS] = [false; PG_STAT_WAL_COLS];
    let mut buf: [c_char; 256] = [0; 256];

    /* Initialise attributes information in the tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(PG_STAT_WAL_COLS as c_int);
    TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"wal_records".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"wal_fpi".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"wal_bytes".as_ptr(), NUMERICOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"wal_buffers_full".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"stats_reset".as_ptr(), TIMESTAMPTZOID, -1, 0);

    BlessTupleDesc(tupdesc);

    /* Fill values and NULLs */
    values[0] = Int64GetDatum(wal_counters.wal_records);
    values[1] = Int64GetDatum(wal_counters.wal_fpi);

    /* Convert to numeric. */
    snprintf(buf.as_mut_ptr(), core::mem::size_of_val(&buf), c"%llu".as_ptr(), wal_counters.wal_bytes);
    values[2] = DirectFunctionCall3!(
        numeric_in,
        CStringGetDatum(buf.as_ptr()),
        ObjectIdGetDatum(0),
        Int32GetDatum(-1)
    );

    values[3] = Int64GetDatum(wal_counters.wal_buffers_full);

    if stat_reset_timestamp != 0 {
        values[4] = TimestampTzGetDatum(stat_reset_timestamp);
    } else {
        nulls[4] = true;
    }

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr())))
}

/*
 * Returns WAL statistics for a backend with given PID.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_backend_wal(fcinfo: FunctionCallInfo) -> Datum {
    let pid: c_int;
    let backend_stats: *mut PgStat_Backend;
    let bktype_stats: PgStat_WalCounters;

    pid = PG_GETARG_INT32!(fcinfo, 0);
    backend_stats = pgstat_fetch_stat_backend_by_pid(pid, null_mut());

    if backend_stats.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    bktype_stats = (*backend_stats).wal_counters;

    /* save tuples with data from this PgStat_WalCounters */
    pg_stat_wal_build_tuple(bktype_stats, (*backend_stats).stat_reset_timestamp)
}

/*
 * Returns statistics of WAL activity
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_wal(fcinfo: FunctionCallInfo) -> Datum {
    let wal_stats: *mut PgStat_WalStats;

    /* Get statistics about WAL activity */
    wal_stats = pgstat_fetch_stat_wal();

    pg_stat_wal_build_tuple((*wal_stats).wal_counters, (*wal_stats).stat_reset_timestamp)
}

/*
 * Returns statistics of SLRU caches.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_slru(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_SLRU_COLS: usize = 9;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut i: c_int;
    let stats: *mut PgStat_SLRUStats;

    InitMaterializedSRF(fcinfo, 0);

    /* request SLRU stats from the cumulative stats system */
    stats = pgstat_fetch_slru();

    i = 0;
    loop {
        /* for each row */
        let mut values: [Datum; PG_STAT_GET_SLRU_COLS] = [0; PG_STAT_GET_SLRU_COLS];
        let mut nulls: [bool; PG_STAT_GET_SLRU_COLS] = [false; PG_STAT_GET_SLRU_COLS];
        let stat: PgStat_SLRUStats;
        let name: *const c_char;

        name = pgstat_get_slru_name(i);

        if name.is_null() {
            break;
        }

        stat = core::ptr::read(stats.add(i as usize));

        values[0] = PointerGetDatum(cstring_to_text(name));
        values[1] = Int64GetDatum(stat.blocks_zeroed);
        values[2] = Int64GetDatum(stat.blocks_hit);
        values[3] = Int64GetDatum(stat.blocks_read);
        values[4] = Int64GetDatum(stat.blocks_written);
        values[5] = Int64GetDatum(stat.blocks_exists);
        values[6] = Int64GetDatum(stat.flush);
        values[7] = Int64GetDatum(stat.truncate);
        values[8] = TimestampTzGetDatum(stat.stat_reset_timestamp);

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        i += 1;
    }

    0 as Datum
}

// PG_STAT_GET_XACT_RELENTRY_INT64(stat)
macro_rules! PG_STAT_GET_XACT_RELENTRY_INT64 {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let result: int64;
            let tabentry: *mut PgStat_TableStatus;

            tabentry = find_tabstat_entry(relid);
            if tabentry.is_null() {
                result = 0;
            } else {
                result = tabstatus_i64(tabentry, $stat) as int64;
            }

            PG_RETURN_INT64(result)
        }
    };
}

/* pg_stat_get_xact_numscans */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_numscans, "numscans");
/* pg_stat_get_xact_tuples_returned */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_returned, "tuples_returned");
/* pg_stat_get_xact_tuples_fetched */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_fetched, "tuples_fetched");
/* pg_stat_get_xact_tuples_hot_updated */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_hot_updated, "tuples_hot_updated");
/* pg_stat_get_xact_tuples_newpage_updated */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_newpage_updated, "tuples_newpage_updated");
/* pg_stat_get_xact_blocks_fetched */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_blocks_fetched, "blocks_fetched");
/* pg_stat_get_xact_blocks_hit */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_blocks_hit, "blocks_hit");
/* pg_stat_get_xact_tuples_inserted */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_inserted, "tuples_inserted");
/* pg_stat_get_xact_tuples_updated */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_updated, "tuples_updated");
/* pg_stat_get_xact_tuples_deleted */
PG_STAT_GET_XACT_RELENTRY_INT64!(pg_stat_get_xact_tuples_deleted, "tuples_deleted");

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_xact_function_calls(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let funcentry: *mut PgStat_FunctionCounts;

    funcentry = find_funcstat_entry(funcid);
    if funcentry.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_INT64((*funcentry).numcalls)
}

// PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS(stat)
macro_rules! PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS {
    ($fname:ident, $stat:literal) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fname(fcinfo: FunctionCallInfo) -> Datum {
            let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
            let funcentry: *mut PgStat_FunctionCounts;

            funcentry = find_funcstat_entry(funcid);
            if funcentry.is_null() {
                return PG_RETURN_NULL!(fcinfo);
            }
            PG_RETURN_FLOAT8(funccounts_f64_ms(funcentry, $stat))
        }
    };
}

/* pg_stat_get_xact_function_total_time */
PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS!(pg_stat_get_xact_function_total_time, "total_time");
/* pg_stat_get_xact_function_self_time */
PG_STAT_GET_XACT_FUNCENTRY_FLOAT8_MS!(pg_stat_get_xact_function_self_time, "self_time");

/* Get the timestamp of the current statistics snapshot */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_snapshot_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let mut have_snapshot: bool = false;
    let ts: TimestampTz;

    ts = pgstat_get_stat_snapshot_timestamp(&mut have_snapshot);

    if !have_snapshot {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TIMESTAMPTZ(ts)
}

/* Discard the active statistics snapshot */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_clear_snapshot(fcinfo: FunctionCallInfo) -> Datum {
    pgstat_clear_snapshot();

    PG_RETURN_VOID!(fcinfo)
}

/* Force statistics to be reported at the next occasion */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_force_next_flush(fcinfo: FunctionCallInfo) -> Datum {
    pgstat_force_next_flush();

    PG_RETURN_VOID!(fcinfo)
}

/* Reset all counters for the current database */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset(fcinfo: FunctionCallInfo) -> Datum {
    pgstat_reset_counters();

    PG_RETURN_VOID!(fcinfo)
}

/*
 * Reset some shared cluster-wide counters
 *
 * When adding a new reset target, ideally the name should match that in
 * pgstat_kind_builtin_infos, if relevant.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_shared(fcinfo: FunctionCallInfo) -> Datum {
    let target: *mut c_char;

    if PG_ARGISNULL!(fcinfo, 0) {
        /* Reset all the statistics when nothing is specified */
        pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER);
        pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER);
        pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER);
        pgstat_reset_of_kind(PGSTAT_KIND_IO);
        XLogPrefetchResetStats();
        pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
        pgstat_reset_of_kind(PGSTAT_KIND_WAL);

        return PG_RETURN_VOID!(fcinfo);
    }

    target = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut c_void);

    if strcmp(target, c"archiver".as_ptr()) == 0 {
        pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER);
    } else if strcmp(target, c"bgwriter".as_ptr()) == 0 {
        pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER);
    } else if strcmp(target, c"checkpointer".as_ptr()) == 0 {
        pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER);
    } else if strcmp(target, c"io".as_ptr()) == 0 {
        pgstat_reset_of_kind(PGSTAT_KIND_IO);
    } else if strcmp(target, c"recovery_prefetch".as_ptr()) == 0 {
        XLogPrefetchResetStats();
    } else if strcmp(target, c"slru".as_ptr()) == 0 {
        pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
    } else if strcmp(target, c"wal".as_ptr()) == 0 {
        pgstat_reset_of_kind(PGSTAT_KIND_WAL);
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "unrecognized reset target: \"{}\"",
                CStr::from_ptr(target).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        // C also: errhint("Target must be \"archiver\", \"bgwriter\", \"checkpointer\", \"io\", \"recovery_prefetch\", \"slru\", or \"wal\".")
    }

    PG_RETURN_VOID!(fcinfo)
}

/*
 * Reset a statistics for a single object, which may be of current
 * database or shared across all databases in the cluster.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_single_table_counters(fcinfo: FunctionCallInfo) -> Datum {
    let taboid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let dboid: Oid = if IsSharedRelation(taboid) { InvalidOid } else { MyDatabaseId };

    pgstat_reset(PGSTAT_KIND_RELATION, dboid, taboid as u64);

    PG_RETURN_VOID!(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_single_function_counters(fcinfo: FunctionCallInfo) -> Datum {
    let funcoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    pgstat_reset(PGSTAT_KIND_FUNCTION, MyDatabaseId, funcoid as u64);

    PG_RETURN_VOID!(fcinfo)
}

/*
 * Reset statistics of backend with given PID.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_backend_stats(fcinfo: FunctionCallInfo) -> Datum {
    let mut proc: *mut PGPROC;
    let beentry: *mut PgBackendStatus;
    let procNumber: ProcNumber;
    let backend_pid: c_int = PG_GETARG_INT32!(fcinfo, 0);

    proc = BackendPidGetProc(backend_pid);

    /* This could be an auxiliary process */
    if proc.is_null() {
        proc = AuxiliaryPidGetProc(backend_pid);
    }

    if proc.is_null() {
        return PG_RETURN_VOID!(fcinfo);
    }

    procNumber = GetNumberFromPGProc(proc);

    beentry = pgstat_get_beentry_by_proc_number(procNumber);
    if beentry.is_null() {
        return PG_RETURN_VOID!(fcinfo);
    }

    /* Check if the backend type tracks statistics */
    if !pgstat_tracks_backend_bktype(be_st_backendType(beentry)) {
        return PG_RETURN_VOID!(fcinfo);
    }

    pgstat_reset(PGSTAT_KIND_BACKEND, InvalidOid, procNumber as u64);

    PG_RETURN_VOID!(fcinfo)
}

/* Reset SLRU counters (a specific one or all of them). */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_slru(fcinfo: FunctionCallInfo) -> Datum {
    let target: *mut c_char;

    if PG_ARGISNULL!(fcinfo, 0) {
        pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
    } else {
        target = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut c_void);
        pgstat_reset_slru(target);
    }

    PG_RETURN_VOID!(fcinfo)
}

/* Reset replication slots stats (a specific one or all of them). */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_replication_slot(fcinfo: FunctionCallInfo) -> Datum {
    let target: *mut c_char;

    if PG_ARGISNULL!(fcinfo, 0) {
        pgstat_reset_of_kind(PGSTAT_KIND_REPLSLOT);
    } else {
        target = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut c_void);
        pgstat_reset_replslot(target);
    }

    PG_RETURN_VOID!(fcinfo)
}

/* Reset subscription stats (a specific one or all of them) */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_reset_subscription_stats(fcinfo: FunctionCallInfo) -> Datum {
    let subid: Oid;

    if PG_ARGISNULL!(fcinfo, 0) {
        /* Clear all subscription stats */
        pgstat_reset_of_kind(PGSTAT_KIND_SUBSCRIPTION);
    } else {
        subid = PG_GETARG_OID!(fcinfo, 0);

        if !OidIsValid(subid) {
            ereport!(ERROR, errmsg!("invalid subscription OID {}", subid));
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        }
        pgstat_reset(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64);
    }

    PG_RETURN_VOID!(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_archiver(fcinfo: FunctionCallInfo) -> Datum {
    let tupdesc: TupleDesc;
    let mut values: [Datum; 7] = [0; 7];
    let mut nulls: [bool; 7] = [false; 7];
    let archiver_stats: *mut PgStat_ArchiverStats;

    /* Initialise attributes information in the tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(7);
    TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"archived_count".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"last_archived_wal".as_ptr(), TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"last_archived_time".as_ptr(), TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"failed_count".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"last_failed_wal".as_ptr(), TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 6 as AttrNumber, c"last_failed_time".as_ptr(), TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 7 as AttrNumber, c"stats_reset".as_ptr(), TIMESTAMPTZOID, -1, 0);

    BlessTupleDesc(tupdesc);

    /* Get statistics about the archiver process */
    archiver_stats = pgstat_fetch_stat_archiver();

    /* Fill values and NULLs */
    values[0] = Int64GetDatum((*archiver_stats).archived_count);
    if (*archiver_stats).last_archived_wal[0] == b'\0' as c_char {
        nulls[1] = true;
    } else {
        values[1] = CStringGetTextDatum((*archiver_stats).last_archived_wal.as_ptr());
    }

    if (*archiver_stats).last_archived_timestamp == 0 {
        nulls[2] = true;
    } else {
        values[2] = TimestampTzGetDatum((*archiver_stats).last_archived_timestamp);
    }

    values[3] = Int64GetDatum((*archiver_stats).failed_count);
    if (*archiver_stats).last_failed_wal[0] == b'\0' as c_char {
        nulls[4] = true;
    } else {
        values[4] = CStringGetTextDatum((*archiver_stats).last_failed_wal.as_ptr());
    }

    if (*archiver_stats).last_failed_timestamp == 0 {
        nulls[5] = true;
    } else {
        values[5] = TimestampTzGetDatum((*archiver_stats).last_failed_timestamp);
    }

    if (*archiver_stats).stat_reset_timestamp == 0 {
        nulls[6] = true;
    } else {
        values[6] = TimestampTzGetDatum((*archiver_stats).stat_reset_timestamp);
    }

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr())))
}

/*
 * Get the statistics for the replication slot. If the slot statistics is not
 * available, return all-zeroes stats.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_replication_slot(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_REPLICATION_SLOT_COLS: usize = 10;
    let slotname_text: *mut text = PG_GETARG_TEXT_P!(fcinfo, 0) as *mut text;
    let mut slotname: NameData = core::mem::zeroed();
    let tupdesc: TupleDesc;
    let mut values: [Datum; PG_STAT_GET_REPLICATION_SLOT_COLS] = [0; PG_STAT_GET_REPLICATION_SLOT_COLS];
    let mut nulls: [bool; PG_STAT_GET_REPLICATION_SLOT_COLS] = [false; PG_STAT_GET_REPLICATION_SLOT_COLS];
    let mut slotent: *mut PgStat_StatReplSlotEntry;
    let mut allzero: PgStat_StatReplSlotEntry = core::mem::zeroed();

    /* Initialise attributes information in the tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(PG_STAT_GET_REPLICATION_SLOT_COLS as c_int);
    TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"slot_name".as_ptr(), TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"spill_txns".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"spill_count".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"spill_bytes".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"stream_txns".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 6 as AttrNumber, c"stream_count".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 7 as AttrNumber, c"stream_bytes".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 8 as AttrNumber, c"total_txns".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 9 as AttrNumber, c"total_bytes".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 10 as AttrNumber, c"stats_reset".as_ptr(), TIMESTAMPTZOID, -1, 0);
    BlessTupleDesc(tupdesc);

    namestrcpy(&mut slotname, text_to_cstring(slotname_text as *mut c_void));
    slotent = pgstat_fetch_replslot(slotname);
    if slotent.is_null() {
        /*
         * If the slot is not found, initialise its stats. This is possible if
         * the create slot message is lost.
         */
        memset(
            &mut allzero as *mut PgStat_StatReplSlotEntry as *mut c_void,
            0,
            core::mem::size_of::<PgStat_StatReplSlotEntry>(),
        );
        slotent = &mut allzero;
    }

    values[0] = CStringGetTextDatum(NameStr(&slotname));
    values[1] = Int64GetDatum((*slotent).spill_txns);
    values[2] = Int64GetDatum((*slotent).spill_count);
    values[3] = Int64GetDatum((*slotent).spill_bytes);
    values[4] = Int64GetDatum((*slotent).stream_txns);
    values[5] = Int64GetDatum((*slotent).stream_count);
    values[6] = Int64GetDatum((*slotent).stream_bytes);
    values[7] = Int64GetDatum((*slotent).total_txns);
    values[8] = Int64GetDatum((*slotent).total_bytes);

    if (*slotent).stat_reset_timestamp == 0 {
        nulls[9] = true;
    } else {
        values[9] = TimestampTzGetDatum((*slotent).stat_reset_timestamp);
    }

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr())))
}

/*
 * Get the subscription statistics for the given subscription. If the
 * subscription statistics is not available, return all-zeros stats.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_get_subscription_stats(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_SUBSCRIPTION_STATS_COLS: usize = 11;
    let subid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let tupdesc: TupleDesc;
    let mut values: [Datum; PG_STAT_GET_SUBSCRIPTION_STATS_COLS] = [0; PG_STAT_GET_SUBSCRIPTION_STATS_COLS];
    let mut nulls: [bool; PG_STAT_GET_SUBSCRIPTION_STATS_COLS] = [false; PG_STAT_GET_SUBSCRIPTION_STATS_COLS];
    let mut subentry: *mut PgStat_StatSubEntry;
    let mut allzero: PgStat_StatSubEntry = core::mem::zeroed();
    let mut i: usize = 0;

    /* Get subscription stats */
    subentry = pgstat_fetch_stat_subscription(subid);

    /* Initialise attributes information in the tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(PG_STAT_GET_SUBSCRIPTION_STATS_COLS as c_int);
    TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"subid".as_ptr(), OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"apply_error_count".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"sync_error_count".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"confl_insert_exists".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"confl_update_origin_differs".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 6 as AttrNumber, c"confl_update_exists".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 7 as AttrNumber, c"confl_update_missing".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 8 as AttrNumber, c"confl_delete_origin_differs".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 9 as AttrNumber, c"confl_delete_missing".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 10 as AttrNumber, c"confl_multiple_unique_conflicts".as_ptr(), INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 11 as AttrNumber, c"stats_reset".as_ptr(), TIMESTAMPTZOID, -1, 0);
    BlessTupleDesc(tupdesc);

    if subentry.is_null() {
        /* If the subscription is not found, initialise its stats */
        memset(
            &mut allzero as *mut PgStat_StatSubEntry as *mut c_void,
            0,
            core::mem::size_of::<PgStat_StatSubEntry>(),
        );
        subentry = &mut allzero;
    }

    /* subid */
    values[i] = ObjectIdGetDatum(subid);
    i += 1;

    /* apply_error_count */
    values[i] = Int64GetDatum((*subentry).apply_error_count);
    i += 1;

    /* sync_error_count */
    values[i] = Int64GetDatum((*subentry).sync_error_count);
    i += 1;

    /* conflict count */
    for nconflict in 0..CONFLICT_NUM_TYPES {
        values[i] = Int64GetDatum((*subentry).conflict_count[nconflict]);
        i += 1;
    }

    /* stats_reset */
    if (*subentry).stat_reset_timestamp == 0 {
        nulls[i] = true;
    } else {
        values[i] = TimestampTzGetDatum((*subentry).stat_reset_timestamp);
    }

    Assert!(i + 1 == PG_STAT_GET_SUBSCRIPTION_STATS_COLS);

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr())))
}

/*
 * Checks for presence of stats for object with provided kind, database oid,
 * object oid.
 *
 * This is useful for tests, but not really anything else. Therefore not
 * documented.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_stat_have_stats(fcinfo: FunctionCallInfo) -> Datum {
    let stats_type: *mut c_char = text_to_cstring(PG_GETARG_TEXT_P!(fcinfo, 0) as *mut c_void);
    let dboid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let objid: u64 = PG_GETARG_INT64!(fcinfo, 2) as u64;
    let kind: PgStat_Kind = pgstat_get_kind_from_str(stats_type);

    PG_RETURN_BOOL(pgstat_have_entry(kind, dboid, objid))
}

unsafe fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

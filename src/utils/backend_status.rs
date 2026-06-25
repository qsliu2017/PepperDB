//! Translated from PostgreSQL src/include/utils/backend_status.h

use crate::c::{Size, TransactionId, NAMEDATALEN};
use crate::datatype::timestamp::TimestampTz;
use crate::libpq::pqcomm::SockAddr;
use crate::miscadmin::BackendType;
use crate::postgres_ext::Oid;
use crate::storage::procnumber::ProcNumber;
use crate::utils::backend_progress::{ProgressCommandType, PGSTAT_NUM_PROGRESS_PARAM};

/// Backend states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendState {
    Undefined,
    Starting,
    Idle,
    Running,
    IdleInTransaction,
    Fastpath,
    IdleInTransactionAborted,
    Disabled,
}

/// SSL status, only filled in if SSL is enabled. All char arrays NUL-terminated.
pub struct PgBackendSSLStatus {
    pub bits: i32,
    pub version: [u8; NAMEDATALEN],
    pub cipher: [u8; NAMEDATALEN],
    pub client_dn: [u8; NAMEDATALEN],
    pub client_serial: [u8; NAMEDATALEN], // serial: max 20 octets per RFC 5280
    pub issuer_dn: [u8; NAMEDATALEN],
}

/// GSS status, only filled in if GSS is enabled. All char arrays NUL-terminated.
pub struct PgBackendGSSStatus {
    pub princ: [u8; NAMEDATALEN], // GSSAPI principal used to auth
    pub auth: bool,
    pub enc: bool,
    pub delegation: bool,
}

/// Per-backend live activity. In-memory (single-process; the C changecount
/// seqlock and volatile pointers collapse away).
pub struct PgBackendStatus {
    pub changecount: i32, // seqlock counter (kept for read/write protocol)
    pub procpid: i32,     // entry valid iff > 0

    pub backend_type: BackendType,

    pub proc_start_timestamp: TimestampTz,
    pub xact_start_timestamp: TimestampTz,
    pub activity_start_timestamp: TimestampTz,
    pub state_start_timestamp: TimestampTz,

    pub databaseid: Oid,
    pub userid: Oid,
    pub clientaddr: SockAddr,
    pub clienthostname: Option<String>, // null-terminated in C

    pub ssl: bool,
    pub sslstatus: Option<Box<PgBackendSSLStatus>>,

    pub gss: bool,
    pub gssstatus: Option<Box<PgBackendGSSStatus>>,

    pub state: BackendState,

    pub appname: Option<String>,
    pub activity_raw: Option<String>, // possibly truncated mid-multibyte char

    pub progress_command: ProgressCommandType,
    pub progress_command_target: Oid,
    pub progress_param: [i64; PGSTAT_NUM_PROGRESS_PARAM],

    pub query_id: i64,
    pub plan_id: i64,
}

/// Backend status array entry plus locally-derived fields.
pub struct LocalPgBackendStatus {
    pub backend_status: PgBackendStatus,
    pub proc_number: ProcNumber,
    pub backend_xid: TransactionId,  // InvalidTransactionId if not available
    pub backend_xmin: TransactionId, // InvalidTransactionId if not available
    pub backend_subxact_count: i32,
    pub backend_subxact_overflowed: bool,
}

// GUC parameters.
pub static mut pgstat_track_activities: bool = false;
pub static mut pgstat_track_activity_query_size: i32 = 0;

// The seqlock read/write macros (PGSTAT_BEGIN/END_WRITE_ACTIVITY,
// pgstat_begin/end_read_activity) collapse under single-process; omitted.

// Functions called from postmaster.
pub fn BackendStatusShmemSize() -> Size {
    unimplemented!()
}
pub fn BackendStatusShmemInit() {
    unimplemented!()
}

// Initialization functions.
pub fn pgstat_beinit() {
    unimplemented!()
}
pub fn pgstat_bestart_initial() {
    unimplemented!()
}
pub fn pgstat_bestart_security() {
    unimplemented!()
}
pub fn pgstat_bestart_final() {
    unimplemented!()
}
pub fn pgstat_clear_backend_activity_snapshot() {
    unimplemented!()
}

// Activity reporting functions.
pub fn pgstat_report_activity(_state: BackendState, _cmd_str: &str) {
    unimplemented!()
}
pub fn pgstat_report_query_id(_query_id: i64, _force: bool) {
    unimplemented!()
}
pub fn pgstat_report_plan_id(_plan_id: i64, _force: bool) {
    unimplemented!()
}
pub fn pgstat_report_tempfile(_filesize: usize) {
    unimplemented!()
}
pub fn pgstat_report_appname(_appname: &str) {
    unimplemented!()
}
pub fn pgstat_report_xact_timestamp(_tstamp: TimestampTz) {
    unimplemented!()
}
pub fn pgstat_get_backend_current_activity(_pid: i32, _check_user: bool) -> &'static str {
    unimplemented!()
}
pub fn pgstat_get_crashed_backend_activity(_pid: i32, _buffer: &mut [u8]) -> &'static str {
    unimplemented!()
}
pub fn pgstat_get_my_query_id() -> i64 {
    unimplemented!()
}
pub fn pgstat_get_my_plan_id() -> i64 {
    unimplemented!()
}
pub fn pgstat_get_backend_type_by_proc_number(_proc_number: ProcNumber) -> BackendType {
    unimplemented!()
}

// Support functions for the SQL-callable pgstat* views.
pub fn pgstat_fetch_stat_numbackends() -> i32 {
    unimplemented!()
}
pub fn pgstat_get_beentry_by_proc_number(_proc_number: ProcNumber) -> Option<&'static PgBackendStatus> {
    unimplemented!()
}
pub fn pgstat_get_local_beentry_by_proc_number(
    _proc_number: ProcNumber,
) -> Option<&'static LocalPgBackendStatus> {
    unimplemented!()
}
pub fn pgstat_get_local_beentry_by_index(_idx: i32) -> Option<&'static LocalPgBackendStatus> {
    unimplemented!()
}
pub fn pgstat_clip_activity(_raw_activity: &str) -> String {
    unimplemented!()
}

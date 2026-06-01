// ----------
// backend_status.rs
//	  Backend status reporting infrastructure.
//
// Copyright (c) 2001-2025, PostgreSQL Global Development Group
//
//
// IDENTIFICATION
//	  src/backend/utils/activity/backend_status.c
// ----------
//
// Rust translation of PostgreSQL 18.3 backend_status.c, with the declarations
// from utils/backend_status.h merged in (BackendState, PgBackendSSLStatus,
// PgBackendGSSStatus, PgBackendStatus, LocalPgBackendStatus, the changecount
// write/read protocol, and the GUC/global externs).
//
// NOTE: USE_SSL and ENABLE_GSS code is not compiled in this build (matching
// the rest of the port), so the #ifdef USE_SSL / #ifdef ENABLE_GSS blocks are
// omitted with comments. The unconditional struct fields st_ssl/st_sslstatus
// /st_gss/st_gssstatus/st_clientaddr are still modeled to match the header.

#![allow(static_mut_refs)]

use crate::prelude::*;
use crate::miscadmin::TimestampTz;

use crate::libpq::libpq_be::{Port, SockAddr};
use crate::miscadmin::{
    superuser, BackendType, GetSessionUserId, GetUserId, MyBackendType, MyDatabaseId, MyProcPid,
    MyProcPort, MyStartTimestamp, CHECK_FOR_INTERRUPTS, END_CRIT_SECTION, START_CRIT_SECTION,
    B_BACKEND, B_BG_WORKER, B_WAL_SENDER,
};
use crate::storage::ipc::ipc::on_shmem_exit;
use crate::storage::ipc::shmem::{add_size, mul_size, ShmemInitStruct};
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER, MyProcNumber};
use crate::utils::activity::backend_progress::{
    ProgressCommandType, PGSTAT_NUM_PROGRESS_PARAM, PROGRESS_COMMAND_INVALID,
};
use crate::utils::activity::pgstat::{GetCurrentTimestamp, PgStat_Counter};
use crate::utils::activity::pgstat_backend::{pgstat_create_backend, pgstat_tracks_backend_bktype};
use crate::utils::adt::ascii::ascii_safe_strlcpy;
use crate::utils::adt::timestamp::{GetCurrentStatementStartTimestamp, TimestampDifference};

use crate::mb::mbutils::pg_mbcliplen;

use crate::pg_config_manual::NAMEDATALEN;

use crate::utils::mmgr::mcxt::MemoryContextAllocHuge;

// C string functions used directly (matching the rest of the port, which
// declares them via extern "C" rather than going through a wrapper).
extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *mut c_void;
}

// ----------
// Stubs for symbols whose real home is not yet translated.
// ----------

// TODO(pg-port): real MyProc / PGPROC live in storage/lmgr/proc.rs (storage/proc.h).
// We only touch proc->wait_event_info in pgstat_report_activity().
#[repr(C)]
struct PGPROC {
    wait_event_info: u32,
}
static mut MyProc: *mut PGPROC = null_mut();

// TODO(pg-port): real connection-time counters live in
// utils/activity/pgstat_database.c (pgstat_count_conn_active_time /
// pgstat_count_conn_txn_idle_time). Stubbed as no-ops for now.
unsafe fn pgstat_count_conn_active_time(_n: PgStat_Counter) {}
unsafe fn pgstat_count_conn_txn_idle_time(_n: PgStat_Counter) {}

// TODO(pg-port): real ProcNumberGetTransactionIds lives in
// storage/ipc/procarray.c (storage/procarray.h).
unsafe fn ProcNumberGetTransactionIds(
    _proc_number: ProcNumber,
    xid: *mut TransactionId,
    xmin: *mut TransactionId,
    nsubxid: *mut c_int,
    overflowed: *mut bool,
) {
    *xid = InvalidTransactionId;
    *xmin = InvalidTransactionId;
    *nsubxid = 0;
    *overflowed = false;
}

// TODO(pg-port): real TransactionId / InvalidTransactionId live in
// access/transam/xact (access/transam.h).
type TransactionId = u32;
const InvalidTransactionId: TransactionId = 0;

// TODO(pg-port): real application_name GUC lives in utils/misc/guc_tables.c
// (assigned via assign_application_name in utils/guc_hooks.rs). Stubbed NULL.
static mut application_name: *mut c_char = null_mut();

// TODO(pg-port): TRACE_POSTGRESQL_STATEMENT_STATUS is a DTrace probe
// (pg_trace.h); compiled to nothing without --enable-dtrace.
#[inline]
unsafe fn TRACE_POSTGRESQL_STATEMENT_STATUS(_cmd_str: *const c_char) {}

// ----------
// Backend states (utils/backend_status.h)
// ----------
pub type BackendState = c_int;
pub const STATE_UNDEFINED: BackendState = 0;
pub const STATE_STARTING: BackendState = 1;
pub const STATE_IDLE: BackendState = 2;
pub const STATE_RUNNING: BackendState = 3;
pub const STATE_IDLEINTRANSACTION: BackendState = 4;
pub const STATE_FASTPATH: BackendState = 5;
pub const STATE_IDLEINTRANSACTION_ABORTED: BackendState = 6;
pub const STATE_DISABLED: BackendState = 7;

// ----------
// Shared-memory data structures (utils/backend_status.h)
// ----------

// PgBackendSSLStatus
//
// For each backend, we keep the SSL status in a separate struct, that
// is only filled in if SSL is enabled.
//
// All char arrays must be null-terminated.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgBackendSSLStatus {
    /* Information about SSL connection */
    pub ssl_bits: c_int,
    pub ssl_version: [c_char; NAMEDATALEN],
    pub ssl_cipher: [c_char; NAMEDATALEN],
    pub ssl_client_dn: [c_char; NAMEDATALEN],

    /*
     * serial number is max "20 octets" per RFC 5280, so this size should be
     * fine
     */
    pub ssl_client_serial: [c_char; NAMEDATALEN],

    pub ssl_issuer_dn: [c_char; NAMEDATALEN],
}

// PgBackendGSSStatus
//
// For each backend, we keep the GSS status in a separate struct, that
// is only filled in if GSS is enabled.
//
// All char arrays must be null-terminated.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgBackendGSSStatus {
    /* Information about GSSAPI connection */
    pub gss_princ: [c_char; NAMEDATALEN], /* GSSAPI Principal used to auth */
    pub gss_auth: bool,                   /* If GSSAPI authentication was used */
    pub gss_enc: bool,                    /* If encryption is being used */
    pub gss_delegation: bool,             /* If credentials delegated */
}

// ----------
// PgBackendStatus
//
// Each live backend maintains a PgBackendStatus struct in shared memory
// showing its current activity.  (The structs are allocated according to
// ProcNumber, but that is not critical.)  Note that this is unrelated to the
// cumulative stats system (i.e. pgstat.c et al).
//
// Each auxiliary process also maintains a PgBackendStatus struct in shared
// memory.
// ----------
#[repr(C)]
pub struct PgBackendStatus {
    /*
     * To avoid locking overhead, we use the following protocol: a backend
     * increments st_changecount before modifying its entry, and again after
     * finishing a modification.  A would-be reader should note the value of
     * st_changecount, copy the entry into private memory, then check
     * st_changecount again.  If the value hasn't changed, and if it's even,
     * the copy is valid; otherwise start over.  This makes updates cheap
     * while reads are potentially expensive, but that's the tradeoff we want.
     *
     * The above protocol needs memory barriers to ensure that the apparent
     * order of execution is as it desires.  Otherwise, for example, the CPU
     * might rearrange the code so that st_changecount is incremented twice
     * before the modification on a machine with weak memory ordering.  Hence,
     * use the macros defined below for manipulating st_changecount, rather
     * than touching it directly.
     */
    pub st_changecount: c_int,

    /* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
    pub st_procpid: c_int,

    /* Type of backends */
    pub st_backendType: BackendType,

    /* Times when current backend, transaction, and activity started */
    pub st_proc_start_timestamp: TimestampTz,
    pub st_xact_start_timestamp: TimestampTz,
    pub st_activity_start_timestamp: TimestampTz,
    pub st_state_start_timestamp: TimestampTz,

    /* Database OID, owning user's OID, connection client address */
    pub st_databaseid: Oid,
    pub st_userid: Oid,
    pub st_clientaddr: SockAddr,
    pub st_clienthostname: *mut c_char, /* MUST be null-terminated */

    /* Information about SSL connection */
    pub st_ssl: bool,
    pub st_sslstatus: *mut PgBackendSSLStatus,

    /* Information about GSSAPI connection */
    pub st_gss: bool,
    pub st_gssstatus: *mut PgBackendGSSStatus,

    /* current state */
    pub st_state: BackendState,

    /* application name; MUST be null-terminated */
    pub st_appname: *mut c_char,

    /*
     * Current command string; MUST be null-terminated. Note that this string
     * possibly is truncated in the middle of a multi-byte character. As
     * activity strings are stored more frequently than read, that allows to
     * move the cost of correct truncation to the display side. Use
     * pgstat_clip_activity() to truncate correctly.
     */
    pub st_activity_raw: *mut c_char,

    /*
     * Command progress reporting.  Any command which wishes can advertise
     * that it is running by setting st_progress_command,
     * st_progress_command_target, and st_progress_param[].
     * st_progress_command_target should be the OID of the relation which the
     * command targets (we assume there's just one, as this is meant for
     * utility commands), but the meaning of each element in the
     * st_progress_param array is command-specific.
     */
    pub st_progress_command: ProgressCommandType,
    pub st_progress_command_target: Oid,
    pub st_progress_param: [int64; PGSTAT_NUM_PROGRESS_PARAM],

    /* query identifier, optionally computed using post_parse_analyze_hook */
    pub st_query_id: int64,

    /* plan identifier, optionally computed using planner_hook */
    pub st_plan_id: int64,
}

// ----------
// Changecount write/read protocol (utils/backend_status.h)
//
// Use PGSTAT_BEGIN_WRITE_ACTIVITY() before, and PGSTAT_END_WRITE_ACTIVITY()
// after, modifying the current process's PgBackendStatus data.  Note that,
// since there is no mechanism for cleaning up st_changecount after an error,
// THESE MACROS FORM A CRITICAL SECTION.  Any error between them will be
// promoted to PANIC, causing a database restart to clean up shared memory!
// Hence, keep the critical section as short and straight-line as possible.
//
// For extra safety, we generally use volatile beentry pointers, although
// the memory barriers should theoretically be sufficient.
//
// NOTE: pg_write_barrier()/pg_read_barrier() are no-ops in this single-process
// subset, matching the convention used elsewhere in the port (e.g.
// backend_progress.rs and pgstat.rs).
// ----------
#[inline]
unsafe fn PGSTAT_BEGIN_WRITE_ACTIVITY(beentry: *mut PgBackendStatus) {
    START_CRIT_SECTION();
    (*beentry).st_changecount += 1;
    // pg_write_barrier(): no-op
}

#[inline]
unsafe fn PGSTAT_END_WRITE_ACTIVITY(beentry: *mut PgBackendStatus) {
    // pg_write_barrier(): no-op
    (*beentry).st_changecount += 1;
    Assert!(((*beentry).st_changecount & 1) == 0);
    END_CRIT_SECTION();
}

#[inline]
unsafe fn pgstat_begin_read_activity(beentry: *const PgBackendStatus) -> c_int {
    let before_changecount = (*beentry).st_changecount;
    // pg_read_barrier(): no-op
    before_changecount
}

#[inline]
unsafe fn pgstat_end_read_activity(beentry: *const PgBackendStatus) -> c_int {
    // pg_read_barrier(): no-op
    (*beentry).st_changecount
}

#[inline]
fn pgstat_read_activity_complete(before_changecount: c_int, after_changecount: c_int) -> bool {
    before_changecount == after_changecount && (before_changecount & 1) == 0
}

// ----------
// LocalPgBackendStatus
//
// When we build the backend status array, we use LocalPgBackendStatus to be
// able to add new values to the struct when needed without adding new fields
// to the shared memory. It contains the backend status as a first member.
// ----------
#[repr(C)]
pub struct LocalPgBackendStatus {
    /*
     * Local version of the backend status entry.
     */
    pub backendStatus: PgBackendStatus,

    /*
     * The proc number.
     */
    pub proc_number: ProcNumber,

    /*
     * The xid of the current transaction if available, InvalidTransactionId
     * if not.
     */
    pub backend_xid: TransactionId,

    /*
     * The xmin of the current session if available, InvalidTransactionId if
     * not.
     */
    pub backend_xmin: TransactionId,

    /*
     * Number of cached subtransactions in the current session.
     */
    pub backend_subxact_count: c_int,

    /*
     * The number of subtransactions in the current session which exceeded the
     * cached subtransaction limit.
     */
    pub backend_subxact_overflowed: bool,
}

// ----------
// Total number of backends including auxiliary
//
// We reserve a slot for each possible PGPROC entry, including aux processes.
// (But not including PGPROC entries reserved for prepared xacts; they are not
// real processes.)
// ----------
// TODO(pg-port): MaxBackends lives in utils/init/globals.rs; NUM_AUXILIARY_PROCS
// in storage/ipc/procsignal.rs (storage/proc.h). #define NumBackendStatSlots
// (MaxBackends + NUM_AUXILIARY_PROCS)
#[inline]
unsafe fn NumBackendStatSlots() -> c_int {
    use crate::storage::ipc::procsignal::NUM_AUXILIARY_PROCS;
    use crate::utils::init::globals::MaxBackends;
    MaxBackends + NUM_AUXILIARY_PROCS
}

// ----------
// GUC parameters
// ----------
pub static mut pgstat_track_activities: bool = false;
pub static mut pgstat_track_activity_query_size: c_int = 1024;

// exposed so that backend_progress.c can access it
pub static mut MyBEEntry: *mut PgBackendStatus = null_mut();

static mut BackendStatusArray: *mut PgBackendStatus = null_mut();
static mut BackendAppnameBuffer: *mut c_char = null_mut();
static mut BackendClientHostnameBuffer: *mut c_char = null_mut();
static mut BackendActivityBuffer: *mut c_char = null_mut();
static mut BackendActivityBufferSize: Size = 0;
// #ifdef USE_SSL -- not compiled
//   static mut BackendSslStatusBuffer: *mut PgBackendSSLStatus = null_mut();
// #ifdef ENABLE_GSS -- not compiled
//   static mut BackendGssStatusBuffer: *mut PgBackendGSSStatus = null_mut();

// Status for backends including auxiliary
static mut localBackendStatusTable: *mut LocalPgBackendStatus = null_mut();

// Total number of backends including auxiliary
static mut localNumBackends: c_int = 0;

static mut backendStatusSnapContext: MemoryContext = null_mut();

/*
 * Report shared-memory space needed by BackendStatusShmemInit.
 */
pub unsafe fn BackendStatusShmemSize() -> Size {
    let mut size: Size;

    /* BackendStatusArray: */
    size = mul_size(
        size_of::<PgBackendStatus>(),
        NumBackendStatSlots() as Size,
    );
    /* BackendAppnameBuffer: */
    size = add_size(
        size,
        mul_size(NAMEDATALEN as Size, NumBackendStatSlots() as Size),
    );
    /* BackendClientHostnameBuffer: */
    size = add_size(
        size,
        mul_size(NAMEDATALEN as Size, NumBackendStatSlots() as Size),
    );
    /* BackendActivityBuffer: */
    size = add_size(
        size,
        mul_size(
            pgstat_track_activity_query_size as Size,
            NumBackendStatSlots() as Size,
        ),
    );
    // #ifdef USE_SSL -- not compiled (BackendSslStatusBuffer)
    // #ifdef ENABLE_GSS -- not compiled (BackendGssStatusBuffer)
    size
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
pub unsafe fn BackendStatusShmemInit() {
    let mut size: Size;
    let mut found: bool = false;
    let mut i: c_int;
    let mut buffer: *mut c_char;

    /* Create or attach to the shared array */
    size = mul_size(
        size_of::<PgBackendStatus>(),
        NumBackendStatSlots() as Size,
    );
    BackendStatusArray =
        ShmemInitStruct(c"Backend Status Array".as_ptr(), size, &mut found) as *mut PgBackendStatus;

    if !found {
        /*
         * We're the first - initialize.
         */
        MemSet(BackendStatusArray as *mut c_void, 0, size);
    }

    /* Create or attach to the shared appname buffer */
    size = mul_size(NAMEDATALEN as Size, NumBackendStatSlots() as Size);
    BackendAppnameBuffer =
        ShmemInitStruct(c"Backend Application Name Buffer".as_ptr(), size, &mut found)
            as *mut c_char;

    if !found {
        MemSet(BackendAppnameBuffer as *mut c_void, 0, size);

        /* Initialize st_appname pointers. */
        buffer = BackendAppnameBuffer;
        i = 0;
        while i < NumBackendStatSlots() {
            (*BackendStatusArray.offset(i as isize)).st_appname = buffer;
            buffer = buffer.offset(NAMEDATALEN as isize);
            i += 1;
        }
    }

    /* Create or attach to the shared client hostname buffer */
    size = mul_size(NAMEDATALEN as Size, NumBackendStatSlots() as Size);
    BackendClientHostnameBuffer =
        ShmemInitStruct(c"Backend Client Host Name Buffer".as_ptr(), size, &mut found)
            as *mut c_char;

    if !found {
        MemSet(BackendClientHostnameBuffer as *mut c_void, 0, size);

        /* Initialize st_clienthostname pointers. */
        buffer = BackendClientHostnameBuffer;
        i = 0;
        while i < NumBackendStatSlots() {
            (*BackendStatusArray.offset(i as isize)).st_clienthostname = buffer;
            buffer = buffer.offset(NAMEDATALEN as isize);
            i += 1;
        }
    }

    /* Create or attach to the shared activity buffer */
    BackendActivityBufferSize = mul_size(
        pgstat_track_activity_query_size as Size,
        NumBackendStatSlots() as Size,
    );
    BackendActivityBuffer = ShmemInitStruct(
        c"Backend Activity Buffer".as_ptr(),
        BackendActivityBufferSize,
        &mut found,
    ) as *mut c_char;

    if !found {
        MemSet(
            BackendActivityBuffer as *mut c_void,
            0,
            BackendActivityBufferSize,
        );

        /* Initialize st_activity pointers. */
        buffer = BackendActivityBuffer;
        i = 0;
        while i < NumBackendStatSlots() {
            (*BackendStatusArray.offset(i as isize)).st_activity_raw = buffer;
            buffer = buffer.offset(pgstat_track_activity_query_size as isize);
            i += 1;
        }
    }

    // #ifdef USE_SSL -- not compiled
    //   Create or attach to the shared SSL status buffer and init st_sslstatus.
    // #ifdef ENABLE_GSS -- not compiled
    //   Create or attach to the shared GSSAPI status buffer and init st_gssstatus.
}

/*
 * Initialize pgstats backend activity state, and set up our on-proc-exit
 * hook.  Called from InitPostgres and AuxiliaryProcessMain.  MyProcNumber must
 * be set, but we must not have started any transaction yet (since the exit
 * hook must run after the last transaction exit).
 *
 * NOTE: MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
 */
pub unsafe fn pgstat_beinit() {
    /* Initialize MyBEEntry */
    Assert!(MyProcNumber != INVALID_PROC_NUMBER);
    Assert!(MyProcNumber >= 0 && MyProcNumber < NumBackendStatSlots());
    MyBEEntry = BackendStatusArray.offset(MyProcNumber as isize);

    /* Set up a process-exit hook to clean up */
    on_shmem_exit(pgstat_beshutdown_hook, 0);
}

// ----------
// pgstat_bestart_initial() -
//
// Initialize this backend's entry in the PgBackendStatus array.  Called
// from InitPostgres and AuxiliaryProcessMain.
//
// Clears out a new pgstat entry, initializing it to suitable defaults and
// reporting STATE_STARTING.  Backends should continue filling in any
// transport security details as needed with pgstat_bestart_security(), and
// must finally exit STATE_STARTING by calling pgstat_bestart_final().
// ----------
pub unsafe fn pgstat_bestart_initial() {
    let vbeentry: *mut PgBackendStatus = MyBEEntry;
    let mut lbeentry: PgBackendStatus = core::mem::zeroed();

    /* pgstats state must be initialized from pgstat_beinit() */
    Assert!(!vbeentry.is_null());

    /*
     * To minimize the time spent modifying the PgBackendStatus entry, and
     * avoid risk of errors inside the critical section, we first copy the
     * shared-memory struct to a local variable, then modify the data in the
     * local variable, then copy the local variable back to shared memory.
     * Only the last step has to be inside the critical section.
     *
     * Most of the data we copy from shared memory is just going to be
     * overwritten, but the struct's not so large that it's worth the
     * maintenance hassle to copy only the needful fields.
     */
    memcpy(
        &mut lbeentry as *mut PgBackendStatus as *mut c_void,
        vbeentry as *const PgBackendStatus as *const c_void,
        size_of::<PgBackendStatus>(),
    );

    /*
     * Now fill in all the fields of lbeentry, except for strings that are
     * out-of-line data.  Those have to be handled separately, below.
     */
    lbeentry.st_procpid = MyProcPid;
    lbeentry.st_backendType = MyBackendType;
    lbeentry.st_proc_start_timestamp = MyStartTimestamp;
    lbeentry.st_activity_start_timestamp = 0;
    lbeentry.st_state_start_timestamp = 0;
    lbeentry.st_xact_start_timestamp = 0;
    lbeentry.st_databaseid = InvalidOid;
    lbeentry.st_userid = InvalidOid;

    /*
     * We may not have a MyProcPort (eg, if this is the autovacuum process).
     * If so, use all-zeroes client address, which is dealt with specially in
     * pg_stat_get_backend_client_addr and pg_stat_get_backend_client_port.
     */
    if !MyProcPort.is_null() {
        memcpy(
            &mut lbeentry.st_clientaddr as *mut SockAddr as *mut c_void,
            &(*MyProcPort).raddr as *const SockAddr as *const c_void,
            size_of::<SockAddr>(),
        );
    } else {
        MemSet(
            &mut lbeentry.st_clientaddr as *mut SockAddr as *mut c_void,
            0,
            size_of::<SockAddr>(),
        );
    }

    lbeentry.st_ssl = false;
    lbeentry.st_gss = false;

    lbeentry.st_state = STATE_STARTING;
    lbeentry.st_progress_command = PROGRESS_COMMAND_INVALID;
    lbeentry.st_progress_command_target = InvalidOid;
    lbeentry.st_query_id = INT64CONST(0);
    lbeentry.st_plan_id = INT64CONST(0);

    /*
     * we don't zero st_progress_param here to save cycles; nobody should
     * examine it until st_progress_command has been set to something other
     * than PROGRESS_COMMAND_INVALID
     */

    /*
     * We're ready to enter the critical section that fills the shared-memory
     * status entry.  We follow the protocol of bumping st_changecount before
     * and after; and make sure it's even afterwards.  We use a volatile
     * pointer here to ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(vbeentry);

    /* make sure we'll memcpy the same st_changecount back */
    lbeentry.st_changecount = (*vbeentry).st_changecount;

    memcpy(
        vbeentry as *mut c_void,
        &lbeentry as *const PgBackendStatus as *const c_void,
        size_of::<PgBackendStatus>(),
    );

    /*
     * We can write the out-of-line strings and structs using the pointers
     * that are in lbeentry; this saves some de-volatilizing messiness.
     */
    *lbeentry.st_appname.offset(0) = 0;
    if !MyProcPort.is_null() && !(*MyProcPort).remote_hostname.is_null() {
        strlcpy(
            lbeentry.st_clienthostname,
            (*MyProcPort).remote_hostname,
            NAMEDATALEN,
        );
    } else {
        *lbeentry.st_clienthostname.offset(0) = 0;
    }
    *lbeentry.st_activity_raw.offset(0) = 0;
    /* Also make sure the last byte in each string area is always 0 */
    *lbeentry.st_appname.offset((NAMEDATALEN - 1) as isize) = 0;
    *lbeentry.st_clienthostname.offset((NAMEDATALEN - 1) as isize) = 0;
    *lbeentry
        .st_activity_raw
        .offset((pgstat_track_activity_query_size - 1) as isize) = 0;

    /* These structs can just start from zeroes each time */
    // #ifdef USE_SSL -- not compiled (memset st_sslstatus)
    // #ifdef ENABLE_GSS -- not compiled (memset st_gssstatus)

    PGSTAT_END_WRITE_ACTIVITY(vbeentry);
}

// ----------
// pgstat_bestart_security() -
//
// Fill in SSL and GSS information for the pgstat entry.  This is the second
// optional step taken when filling a backend's entry, not required for
// auxiliary processes.
//
// This should only be called from backends with a MyProcPort.
// ----------
pub unsafe fn pgstat_bestart_security() {
    let beentry: *mut PgBackendStatus = MyBEEntry;
    let ssl: bool = false;
    let gss: bool = false;
    // #ifdef USE_SSL -- not compiled (lsslstatus / st_sslstatus)
    // #ifdef ENABLE_GSS -- not compiled (lgssstatus / st_gssstatus)

    /* pgstats state must be initialized from pgstat_beinit() */
    Assert!(!beentry.is_null());
    Assert!(!MyProcPort.is_null()); /* otherwise there's no point */

    // #ifdef USE_SSL -- not compiled
    //   Fill lsslstatus from MyProcPort if ssl_in_use.
    // #ifdef ENABLE_GSS -- not compiled
    //   Fill lgssstatus from MyProcPort if gss != NULL.

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    (*beentry).st_ssl = ssl;
    (*beentry).st_gss = gss;

    // #ifdef USE_SSL -- not compiled (memcpy st_sslstatus)
    // #ifdef ENABLE_GSS -- not compiled (memcpy st_gssstatus)

    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// ----------
// pgstat_bestart_final() -
//
// Finalizes the state of this backend's entry by filling in the user and
// database IDs, clearing STATE_STARTING, and reporting the application_name.
//
// We must be inside a transaction if this is not an auxiliary process, as
// we may need to do encoding conversion.
// ----------
pub unsafe fn pgstat_bestart_final() {
    let beentry: *mut PgBackendStatus = MyBEEntry;
    let userid: Oid;

    /* pgstats state must be initialized from pgstat_beinit() */
    Assert!(!beentry.is_null());

    /* We have userid for client-backends, wal-sender and bgworker processes */
    if MyBackendType == B_BACKEND || MyBackendType == B_WAL_SENDER || MyBackendType == B_BG_WORKER {
        userid = GetSessionUserId();
    } else {
        userid = InvalidOid;
    }

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    (*beentry).st_databaseid = MyDatabaseId;
    (*beentry).st_userid = userid;
    (*beentry).st_state = STATE_UNDEFINED;

    PGSTAT_END_WRITE_ACTIVITY(beentry);

    /* Create the backend statistics entry */
    if pgstat_tracks_backend_bktype(MyBackendType) {
        pgstat_create_backend(MyProcNumber);
    }

    /* Update app name to current GUC setting */
    if !application_name.is_null() {
        pgstat_report_appname(application_name);
    }
}

/*
 * Clear out our entry in the PgBackendStatus array.
 */
unsafe extern "C" fn pgstat_beshutdown_hook(_code: c_int, _arg: Datum) {
    let beentry: *mut PgBackendStatus = MyBEEntry;

    /*
     * Clear my status entry, following the protocol of bumping st_changecount
     * before and after.  We use a volatile pointer here to ensure the
     * compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    (*beentry).st_procpid = 0; /* mark invalid */

    PGSTAT_END_WRITE_ACTIVITY(beentry);

    /* so that functions can check if backend_status.c is up via MyBEEntry */
    MyBEEntry = null_mut();
}

/*
 * Discard any data collected in the current transaction.  Any subsequent
 * request will cause new snapshots to be read.
 *
 * This is also invoked during transaction commit or abort to discard the
 * no-longer-wanted snapshot.
 */
pub unsafe fn pgstat_clear_backend_activity_snapshot() {
    /* Release memory, if any was allocated */
    if !backendStatusSnapContext.is_null() {
        MemoryContextDelete(backendStatusSnapContext);
        backendStatusSnapContext = null_mut();
    }

    /* Reset variables */
    localBackendStatusTable = null_mut();
    localNumBackends = 0;
}

unsafe fn pgstat_setup_backend_status_context() {
    if backendStatusSnapContext.is_null() {
        backendStatusSnapContext = AllocSetContextCreate!(
            TopMemoryContext,
            c"Backend Status Snapshot".as_ptr(),
            ALLOCSET_SMALL_SIZES
        );
    }
}

// ----------
// pgstat_report_activity() -
//
//	Called from tcop/postgres.c to report what the backend is actually doing
//	(but note cmd_str can be NULL for certain cases).
//
// All updates of the status entry follow the protocol of bumping
// st_changecount before and after.  We use a volatile pointer here to
// ensure the compiler doesn't try to get cute.
// ----------
pub unsafe fn pgstat_report_activity(state: BackendState, cmd_str: *const c_char) {
    let beentry: *mut PgBackendStatus = MyBEEntry;
    let start_timestamp: TimestampTz;
    let current_timestamp: TimestampTz;
    let mut len: c_int = 0;

    TRACE_POSTGRESQL_STATEMENT_STATUS(cmd_str);

    if beentry.is_null() {
        return;
    }

    if !pgstat_track_activities {
        if (*beentry).st_state != STATE_DISABLED {
            let proc: *mut PGPROC = MyProc;

            /*
             * track_activities is disabled, but we last reported a
             * non-disabled state.  As our final update, change the state and
             * clear fields we will not be updating anymore.
             */
            PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
            (*beentry).st_state = STATE_DISABLED;
            (*beentry).st_state_start_timestamp = 0;
            *(*beentry).st_activity_raw.offset(0) = 0;
            (*beentry).st_activity_start_timestamp = 0;
            /* st_xact_start_timestamp and wait_event_info are also disabled */
            (*beentry).st_xact_start_timestamp = 0;
            (*beentry).st_query_id = INT64CONST(0);
            (*beentry).st_plan_id = INT64CONST(0);
            (*proc).wait_event_info = 0;
            PGSTAT_END_WRITE_ACTIVITY(beentry);
        }
        return;
    }

    /*
     * To minimize the time spent modifying the entry, and avoid risk of
     * errors inside the critical section, fetch all the needed data first.
     */
    start_timestamp = GetCurrentStatementStartTimestamp();
    if !cmd_str.is_null() {
        /*
         * Compute length of to-be-stored string unaware of multi-byte
         * characters. For speed reasons that'll get corrected on read, rather
         * than computed every write.
         */
        len = Min(
            strlen(cmd_str) as c_int,
            pgstat_track_activity_query_size - 1,
        );
    }
    current_timestamp = GetCurrentTimestamp();

    /*
     * If the state has changed from "active" or "idle in transaction",
     * calculate the duration.
     */
    if ((*beentry).st_state == STATE_RUNNING
        || (*beentry).st_state == STATE_FASTPATH
        || (*beentry).st_state == STATE_IDLEINTRANSACTION
        || (*beentry).st_state == STATE_IDLEINTRANSACTION_ABORTED)
        && state != (*beentry).st_state
    {
        let mut secs: c_long = 0;
        let mut usecs: c_int = 0;

        TimestampDifference(
            (*beentry).st_state_start_timestamp,
            current_timestamp,
            &mut secs,
            &mut usecs,
        );

        if (*beentry).st_state == STATE_RUNNING || (*beentry).st_state == STATE_FASTPATH {
            pgstat_count_conn_active_time(secs as PgStat_Counter * 1000000 + usecs as PgStat_Counter);
        } else {
            pgstat_count_conn_txn_idle_time(
                secs as PgStat_Counter * 1000000 + usecs as PgStat_Counter,
            );
        }
    }

    /*
     * Now update the status entry
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    (*beentry).st_state = state;
    (*beentry).st_state_start_timestamp = current_timestamp;

    /*
     * If a new query is started, we reset the query identifier as it'll only
     * be known after parse analysis, to avoid reporting last query's
     * identifier.
     */
    if state == STATE_RUNNING {
        (*beentry).st_query_id = INT64CONST(0);
        (*beentry).st_plan_id = INT64CONST(0);
    }

    if !cmd_str.is_null() {
        memcpy(
            (*beentry).st_activity_raw as *mut c_void,
            cmd_str as *const c_void,
            len as usize,
        );
        *(*beentry).st_activity_raw.offset(len as isize) = 0;
        (*beentry).st_activity_start_timestamp = start_timestamp;
    }

    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// --------
// pgstat_report_query_id() -
//
// Called to update top-level query identifier.
// --------
pub unsafe fn pgstat_report_query_id(query_id: int64, force: bool) {
    let beentry: *mut PgBackendStatus = MyBEEntry;

    /*
     * if track_activities is disabled, st_query_id should already have been
     * reset
     */
    if beentry.is_null() || !pgstat_track_activities {
        return;
    }

    /*
     * We only report the top-level query identifiers.  The stored query_id is
     * reset when a backend calls pgstat_report_activity(STATE_RUNNING), or
     * with an explicit call to this function using the force flag.  If the
     * saved query identifier is not zero it means that it's not a top-level
     * command, so ignore the one provided unless it's an explicit call to
     * reset the identifier.
     */
    if (*beentry).st_query_id != INT64CONST(0) && !force {
        return;
    }

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    (*beentry).st_query_id = query_id;
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// --------
// pgstat_report_plan_id() -
//
// Called to update top-level plan identifier.
// --------
pub unsafe fn pgstat_report_plan_id(plan_id: int64, force: bool) {
    let beentry: *mut PgBackendStatus = MyBEEntry;

    /*
     * if track_activities is disabled, st_plan_id should already have been
     * reset
     */
    if beentry.is_null() || !pgstat_track_activities {
        return;
    }

    /*
     * We only report the top-level plan identifiers.  The stored plan_id is
     * reset when a backend calls pgstat_report_activity(STATE_RUNNING), or
     * with an explicit call to this function using the force flag.  If the
     * saved plan identifier is not zero it means that it's not a top-level
     * command, so ignore the one provided unless it's an explicit call to
     * reset the identifier.
     */
    if (*beentry).st_plan_id != 0 && !force {
        return;
    }

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    (*beentry).st_plan_id = plan_id;
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// ----------
// pgstat_report_appname() -
//
//	Called to update our application name.
// ----------
pub unsafe fn pgstat_report_appname(appname: *const c_char) {
    let beentry: *mut PgBackendStatus = MyBEEntry;
    let len: c_int;

    if beentry.is_null() {
        return;
    }

    /* This should be unnecessary if GUC did its job, but be safe */
    len = pg_mbcliplen(appname, strlen(appname) as c_int, (NAMEDATALEN - 1) as c_int);

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    memcpy(
        (*beentry).st_appname as *mut c_void,
        appname as *const c_void,
        len as usize,
    );
    *(*beentry).st_appname.offset(len as isize) = 0;

    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*
 * Report current transaction start timestamp as the specified value.
 * Zero means there is no active transaction.
 */
pub unsafe fn pgstat_report_xact_timestamp(tstamp: TimestampTz) {
    let beentry: *mut PgBackendStatus = MyBEEntry;

    if !pgstat_track_activities || beentry.is_null() {
        return;
    }

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    (*beentry).st_xact_start_timestamp = tstamp;

    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// ----------
// pgstat_read_current_status() -
//
//	Copy the current contents of the PgBackendStatus array to local memory,
//	if not already done in this transaction.
// ----------
unsafe fn pgstat_read_current_status() {
    let mut beentry: *mut PgBackendStatus;
    let localtable: *mut LocalPgBackendStatus;
    let mut localentry: *mut LocalPgBackendStatus;
    let mut localappname: *mut c_char;
    let mut localclienthostname: *mut c_char;
    let mut localactivity: *mut c_char;
    // #ifdef USE_SSL -- not compiled (localsslstatus)
    // #ifdef ENABLE_GSS -- not compiled (localgssstatus)
    let mut procNumber: ProcNumber;

    if !localBackendStatusTable.is_null() {
        return; /* already done */
    }

    pgstat_setup_backend_status_context();

    /*
     * Allocate storage for local copy of state data.  We can presume that
     * none of these requests overflow size_t, because we already calculated
     * the same values using mul_size during shmem setup.  However, with
     * probably-silly values of pgstat_track_activity_query_size and
     * max_connections, the localactivity buffer could exceed 1GB, so use
     * "huge" allocation for that one.
     */
    localtable = MemoryContextAlloc(
        backendStatusSnapContext,
        size_of::<LocalPgBackendStatus>() * NumBackendStatSlots() as usize,
    ) as *mut LocalPgBackendStatus;
    localappname = MemoryContextAlloc(
        backendStatusSnapContext,
        NAMEDATALEN * NumBackendStatSlots() as usize,
    ) as *mut c_char;
    localclienthostname = MemoryContextAlloc(
        backendStatusSnapContext,
        NAMEDATALEN * NumBackendStatSlots() as usize,
    ) as *mut c_char;
    localactivity = MemoryContextAllocHuge(
        backendStatusSnapContext as crate::utils::mmgr::memnodes::MemoryContext,
        pgstat_track_activity_query_size as Size * NumBackendStatSlots() as Size,
    ) as *mut c_char;
    // #ifdef USE_SSL -- not compiled (localsslstatus alloc)
    // #ifdef ENABLE_GSS -- not compiled (localgssstatus alloc)

    localNumBackends = 0;

    beentry = BackendStatusArray;
    localentry = localtable;
    procNumber = 0;
    while procNumber < NumBackendStatSlots() {
        /*
         * Follow the protocol of retrying if st_changecount changes while we
         * copy the entry, or if it's odd.  (The check for odd is needed to
         * cover the case where we are able to completely copy the entry while
         * the source backend is between increment steps.)	We use a volatile
         * pointer here to ensure the compiler doesn't try to get cute.
         */
        loop {
            let before_changecount: c_int;
            let after_changecount: c_int;

            before_changecount = pgstat_begin_read_activity(beentry);

            (*localentry).backendStatus.st_procpid = (*beentry).st_procpid;
            /* Skip all the data-copying work if entry is not in use */
            if (*localentry).backendStatus.st_procpid > 0 {
                memcpy(
                    &mut (*localentry).backendStatus as *mut PgBackendStatus as *mut c_void,
                    beentry as *const PgBackendStatus as *const c_void,
                    size_of::<PgBackendStatus>(),
                );

                /*
                 * For each PgBackendStatus field that is a pointer, copy the
                 * pointed-to data, then adjust the local copy of the pointer
                 * field to point at the local copy of the data.
                 *
                 * strcpy is safe even if the string is modified concurrently,
                 * because there's always a \0 at the end of the buffer.
                 */
                strcpy(localappname, (*beentry).st_appname);
                (*localentry).backendStatus.st_appname = localappname;
                strcpy(localclienthostname, (*beentry).st_clienthostname);
                (*localentry).backendStatus.st_clienthostname = localclienthostname;
                strcpy(localactivity, (*beentry).st_activity_raw);
                (*localentry).backendStatus.st_activity_raw = localactivity;
                // #ifdef USE_SSL -- not compiled (copy st_sslstatus)
                // #ifdef ENABLE_GSS -- not compiled (copy st_gssstatus)
            }

            after_changecount = pgstat_end_read_activity(beentry);

            if pgstat_read_activity_complete(before_changecount, after_changecount) {
                break;
            }

            /* Make sure we can break out of loop if stuck... */
            CHECK_FOR_INTERRUPTS();
        }

        /* Only valid entries get included into the local array */
        if (*localentry).backendStatus.st_procpid > 0 {
            /*
             * The BackendStatusArray index is exactly the ProcNumber of the
             * source backend.  Note that this means localBackendStatusTable
             * is in order by proc_number. pgstat_get_beentry_by_proc_number()
             * depends on that.
             */
            (*localentry).proc_number = procNumber;
            ProcNumberGetTransactionIds(
                procNumber,
                &mut (*localentry).backend_xid,
                &mut (*localentry).backend_xmin,
                &mut (*localentry).backend_subxact_count,
                &mut (*localentry).backend_subxact_overflowed,
            );

            localentry = localentry.offset(1);
            localappname = localappname.offset(NAMEDATALEN as isize);
            localclienthostname = localclienthostname.offset(NAMEDATALEN as isize);
            localactivity = localactivity.offset(pgstat_track_activity_query_size as isize);
            // #ifdef USE_SSL -- not compiled (localsslstatus++)
            // #ifdef ENABLE_GSS -- not compiled (localgssstatus++)
            localNumBackends += 1;
        }

        beentry = beentry.offset(1);
        procNumber += 1;
    }

    /* Set the pointer only after completion of a valid table */
    localBackendStatusTable = localtable;
}

// ----------
// pgstat_get_backend_current_activity() -
//
//	Return a string representing the current activity of the backend with
//	the specified PID.  This looks directly at the BackendStatusArray,
//	and so will provide current information regardless of the age of our
//	transaction's snapshot of the status array.
//
//	It is the caller's responsibility to invoke this only for backends whose
//	state is expected to remain stable while the result is in use.  The
//	only current use is in deadlock reporting, where we can expect that
//	the target backend is blocked on a lock.  (There are corner cases
//	where the target's wait could get aborted while we are looking at it,
//	but the very worst consequence is to return a pointer to a string
//	that's been changed, so we won't worry too much.)
//
//	Note: return strings for special cases match pg_stat_get_backend_activity.
// ----------
pub unsafe fn pgstat_get_backend_current_activity(pid: c_int, checkUser: bool) -> *const c_char {
    use crate::utils::init::globals::MaxBackends;

    let mut beentry: *mut PgBackendStatus;
    let mut i: c_int;

    beentry = BackendStatusArray;
    i = 1;
    while i <= MaxBackends {
        /*
         * Although we expect the target backend's entry to be stable, that
         * doesn't imply that anyone else's is.  To avoid identifying the
         * wrong backend, while we check for a match to the desired PID we
         * must follow the protocol of retrying if st_changecount changes
         * while we examine the entry, or if it's odd.  (This might be
         * unnecessary, since fetching or storing an int is almost certainly
         * atomic, but let's play it safe.)  We use a volatile pointer here to
         * ensure the compiler doesn't try to get cute.
         */
        let vbeentry: *mut PgBackendStatus = beentry;
        let found: bool;

        loop {
            let before_changecount: c_int;
            let after_changecount: c_int;

            before_changecount = pgstat_begin_read_activity(vbeentry);

            let f = (*vbeentry).st_procpid == pid;

            after_changecount = pgstat_end_read_activity(vbeentry);

            if pgstat_read_activity_complete(before_changecount, after_changecount) {
                found = f;
                break;
            }

            /* Make sure we can break out of loop if stuck... */
            CHECK_FOR_INTERRUPTS();
        }

        if found {
            /* Now it is safe to use the non-volatile pointer */
            if checkUser && !superuser() && (*beentry).st_userid != GetUserId() {
                return c"<insufficient privilege>".as_ptr();
            } else if *(*beentry).st_activity_raw == 0 {
                return c"<command string not enabled>".as_ptr();
            } else {
                /* this'll leak a bit of memory, but that seems acceptable */
                return pgstat_clip_activity((*beentry).st_activity_raw);
            }
        }

        beentry = beentry.offset(1);
        i += 1;
    }

    /* If we get here, caller is in error ... */
    c"<backend information not available>".as_ptr()
}

// ----------
// pgstat_get_crashed_backend_activity() -
//
//	Return a string representing the current activity of the backend with
//	the specified PID.  Like the function above, but reads shared memory with
//	the expectation that it may be corrupt.  On success, copy the string
//	into the "buffer" argument and return that pointer.  On failure,
//	return NULL.
//
//	This function is only intended to be used by the postmaster to report the
//	query that crashed a backend.  In particular, no attempt is made to
//	follow the correct concurrency protocol when accessing the
//	BackendStatusArray.  But that's OK, in the worst case we'll return a
//	corrupted message.  We also must take care not to trip on ereport(ERROR).
// ----------
pub unsafe fn pgstat_get_crashed_backend_activity(
    pid: c_int,
    buffer: *mut c_char,
    buflen: c_int,
) -> *const c_char {
    use crate::utils::init::globals::MaxBackends;

    let mut beentry: *mut PgBackendStatus;
    let mut i: c_int;

    beentry = BackendStatusArray;

    /*
     * We probably shouldn't get here before shared memory has been set up,
     * but be safe.
     */
    if beentry.is_null() || BackendActivityBuffer.is_null() {
        return null();
    }

    i = 1;
    while i <= MaxBackends {
        if (*beentry).st_procpid == pid {
            /* Read pointer just once, so it can't change after validation */
            let activity: *const c_char = (*beentry).st_activity_raw;
            let activity_last: *const c_char;

            /*
             * We mustn't access activity string before we verify that it
             * falls within the BackendActivityBuffer. To make sure that the
             * entire string including its ending is contained within the
             * buffer, subtract one activity length from the buffer size.
             */
            activity_last = BackendActivityBuffer
                .offset(BackendActivityBufferSize as isize)
                .offset(-(pgstat_track_activity_query_size as isize));

            if activity < BackendActivityBuffer || activity > activity_last {
                return null();
            }

            /* If no string available, no point in a report */
            if *activity.offset(0) == 0 {
                return null();
            }

            /*
             * Copy only ASCII-safe characters so we don't run into encoding
             * problems when reporting the message; and be sure not to run off
             * the end of memory.  As only ASCII characters are reported, it
             * doesn't seem necessary to perform multibyte aware clipping.
             */
            ascii_safe_strlcpy(
                buffer,
                activity,
                Min(buflen, pgstat_track_activity_query_size) as usize,
            );

            return buffer;
        }

        beentry = beentry.offset(1);
        i += 1;
    }

    /* PID not found */
    null()
}

// ----------
// pgstat_get_my_query_id() -
//
// Return current backend's query identifier.
// ----------
pub unsafe fn pgstat_get_my_query_id() -> int64 {
    if MyBEEntry.is_null() {
        return 0;
    }

    /*
     * There's no need for a lock around pgstat_begin_read_activity /
     * pgstat_end_read_activity here as it's only called from
     * pg_stat_get_activity which is already protected, or from the same
     * backend which means that there won't be concurrent writes.
     */
    (*MyBEEntry).st_query_id
}

// ----------
// pgstat_get_my_plan_id() -
//
// Return current backend's plan identifier.
// ----------
pub unsafe fn pgstat_get_my_plan_id() -> int64 {
    if MyBEEntry.is_null() {
        return 0;
    }

    /* No need for a lock, for roughly the same reasons as above. */
    (*MyBEEntry).st_plan_id
}

// ----------
// pgstat_get_backend_type_by_proc_number() -
//
//	Return the type of the backend with the specified ProcNumber.  This looks
//	directly at the BackendStatusArray, so the return value may be out of date.
//	The only current use of this function is in pg_signal_backend(), which is
//	inherently racy, so we don't worry too much about this.
//
//	It is the caller's responsibility to use this wisely; at minimum, callers
//	should ensure that procNumber is valid and perform the required permissions
//	checks.
// ----------
pub unsafe fn pgstat_get_backend_type_by_proc_number(procNumber: ProcNumber) -> BackendType {
    let status: *mut PgBackendStatus = BackendStatusArray.offset(procNumber as isize);

    /*
     * We bypass the changecount mechanism since fetching and storing an int
     * is almost certainly atomic.
     */
    (*status).st_backendType
}

// ----------
// cmp_lbestatus
//
//	Comparison function for bsearch() on an array of LocalPgBackendStatus.
//	The proc_number field is used to compare the arguments.
// ----------
unsafe extern "C" fn cmp_lbestatus(a: *const c_void, b: *const c_void) -> c_int {
    let lbestatus1: *const LocalPgBackendStatus = a as *const LocalPgBackendStatus;
    let lbestatus2: *const LocalPgBackendStatus = b as *const LocalPgBackendStatus;

    (*lbestatus1).proc_number - (*lbestatus2).proc_number
}

// ----------
// pgstat_get_beentry_by_proc_number() -
//
//	Support function for the SQL-callable pgstat* functions. Returns
//	our local copy of the current-activity entry for one backend,
//	or NULL if the given beid doesn't identify any known session.
//
//	The argument is the ProcNumber of the desired session
//	(note that this is unlike pgstat_get_local_beentry_by_index()).
//
//	NB: caller is responsible for a check if the user is permitted to see
//	this info (especially the querystring).
// ----------
pub unsafe fn pgstat_get_beentry_by_proc_number(procNumber: ProcNumber) -> *mut PgBackendStatus {
    let ret: *mut LocalPgBackendStatus = pgstat_get_local_beentry_by_proc_number(procNumber);

    if !ret.is_null() {
        return &mut (*ret).backendStatus;
    }

    null_mut()
}

// ----------
// pgstat_get_local_beentry_by_proc_number() -
//
//	Like pgstat_get_beentry_by_proc_number() but with locally computed additions
//	(like xid and xmin values of the backend)
//
//	The argument is the ProcNumber of the desired session
//	(note that this is unlike pgstat_get_local_beentry_by_index()).
//
//	NB: caller is responsible for checking if the user is permitted to see this
//	info (especially the querystring).
// ----------
pub unsafe fn pgstat_get_local_beentry_by_proc_number(
    procNumber: ProcNumber,
) -> *mut LocalPgBackendStatus {
    let mut key: LocalPgBackendStatus = core::mem::zeroed();

    pgstat_read_current_status();

    /*
     * Since the localBackendStatusTable is in order by proc_number, we can
     * use bsearch() to search it efficiently.
     */
    key.proc_number = procNumber;
    bsearch(
        &key as *const LocalPgBackendStatus as *const c_void,
        localBackendStatusTable as *const c_void,
        localNumBackends as usize,
        size_of::<LocalPgBackendStatus>(),
        cmp_lbestatus,
    ) as *mut LocalPgBackendStatus
}

// ----------
// pgstat_get_local_beentry_by_index() -
//
//	Like pgstat_get_beentry_by_proc_number() but with locally computed
//	additions (like xid and xmin values of the backend)
//
//	The idx argument is a 1-based index in the localBackendStatusTable
//	(note that this is unlike pgstat_get_beentry_by_proc_number()).
//	Returns NULL if the argument is out of range (no current caller does that).
//
//	NB: caller is responsible for a check if the user is permitted to see
//	this info (especially the querystring).
// ----------
pub unsafe fn pgstat_get_local_beentry_by_index(idx: c_int) -> *mut LocalPgBackendStatus {
    pgstat_read_current_status();

    if idx < 1 || idx > localNumBackends {
        return null_mut();
    }

    localBackendStatusTable.offset((idx - 1) as isize)
}

// ----------
// pgstat_fetch_stat_numbackends() -
//
//	Support function for the SQL-callable pgstat* functions. Returns
//	the number of sessions known in the localBackendStatusTable, i.e.
//	the maximum 1-based index to pass to pgstat_get_local_beentry_by_index().
// ----------
pub unsafe fn pgstat_fetch_stat_numbackends() -> c_int {
    pgstat_read_current_status();

    localNumBackends
}

/*
 * Convert a potentially unsafely truncated activity string (see
 * PgBackendStatus.st_activity_raw's documentation) into a correctly truncated
 * one.
 *
 * The returned string is allocated in the caller's memory context and may be
 * freed.
 */
pub unsafe fn pgstat_clip_activity(raw_activity: *const c_char) -> *mut c_char {
    let activity: *mut c_char;
    let rawlen: c_int;
    let cliplen: c_int;

    /*
     * Some callers, like pgstat_get_backend_current_activity(), do not
     * guarantee that the buffer isn't concurrently modified. We try to take
     * care that the buffer is always terminated by a NUL byte regardless, but
     * let's still be paranoid about the string's length. In those cases the
     * underlying buffer is guaranteed to be pgstat_track_activity_query_size
     * large.
     */
    activity = pnstrdup(raw_activity, (pgstat_track_activity_query_size - 1) as Size);

    /* now double-guaranteed to be NUL terminated */
    rawlen = strlen(activity) as c_int;

    /*
     * All supported server-encodings make it possible to determine the length
     * of a multi-byte character from its first byte (this is not the case for
     * client encodings, see GB18030). As st_activity is always stored using
     * server encoding, this allows us to perform multi-byte aware truncation,
     * even if the string earlier was truncated in the middle of a multi-byte
     * character.
     */
    cliplen = pg_mbcliplen(activity, rawlen, (pgstat_track_activity_query_size - 1) as c_int);

    *activity.offset(cliplen as isize) = 0;

    activity
}

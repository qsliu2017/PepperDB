//! src/backend/access/transam/xlogfuncs.c
//!
//! PostgreSQL write-ahead log manager user interface functions
//!
//! This file contains WAL control and information functions.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, CStr};

use crate::c::uint32;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::utils::fmgr::FunctionCallInfo;

// fmgr V1 calling-convention macros (#[macro_export] -> crate root).
use crate::{
    PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_TEXT_PP, PG_RETURN_BOOL,
    PG_RETURN_DATUM, PG_RETURN_NULL, PG_RETURN_TEXT_P, PG_RETURN_VOID,
};

// LSN/timestamp fmgr return/getarg helpers are not yet provided by the ported
// fmgr macro set; define minimal local shims here so this unit compiles.
macro_rules! PG_RETURN_LSN {
    ($x:expr) => {
        return ($x) as Datum
    };
}
macro_rules! PG_GETARG_LSN {
    ($fcinfo:expr, $n:expr) => {{
        let _ = ($fcinfo, $n);
        0 as XLogRecPtr
    }};
}
macro_rules! PG_RETURN_TIMESTAMPTZ {
    ($x:expr) => {
        return ($x) as Datum
    };
}

// ---------------------------------------------------------------------------
// Type aliases / external dependencies (stubbed where not yet ported)
// ---------------------------------------------------------------------------

type XLogRecPtr = crate::access::transam::xlogdefs::XLogRecPtr;
type TimestampTz = crate::miscadmin::TimestampTz;

// Stub foreign types
type BackupState = c_void;
type StringInfo = *mut c_void;
type MemoryContextStub = *mut c_void;
type TupleDescStub = *mut c_void;
type HeapTuple = *mut c_void;
type SessionBackupState = c_int;
type XLogSegNo = u64;
type TimeLineID = uint32;
type AttrNumber = i16;

const SESSION_BACKUP_RUNNING: SessionBackupState = 1;

const TYPEFUNC_COMPOSITE: c_int = 1;

const MAXFNAMELEN: usize = 64;

// Recovery pause states
const RECOVERY_NOT_PAUSED: c_int = 0;
const RECOVERY_PAUSE_REQUESTED: c_int = 1;
const RECOVERY_PAUSED: c_int = 2;

// Type OIDs
const TEXTOID: Oid = 25 as Oid;
const INT4OID: Oid = 23 as Oid;

// Wait event flags
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 1;
const WL_POSTMASTER_DEATH: c_int = 1 << 4;
const WAIT_EVENT_PROMOTE: u32 = 0;

const PROMOTE_SIGNAL_FILE: &CStr = c"promote";

const SIGUSR1: c_int = 30;

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn kill(pid: c_int, sig: c_int) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// Backup-related variables.
// ---------------------------------------------------------------------------

static mut backup_state: *mut BackupState = std::ptr::null_mut();
static mut tablespace_map: StringInfo = std::ptr::null_mut();

/* Session-level context for the SQL-callable backup functions */
static mut backupcontext: MemoryContextStub = std::ptr::null_mut();

/*
 * pg_backup_start: set up for taking an on-line backup dump
 *
 * Essentially what this does is to create the contents required for the
 * backup_label file and the tablespace map.
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_backup_start(fcinfo: FunctionCallInfo) -> Datum {
    let backupid: *mut c_void = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let fast: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let backupidstr: *mut c_char;
    let status: SessionBackupState = get_backup_status();
    let oldcontext: MemoryContextStub;

    backupidstr = text_to_cstring(backupid);

    if status == SESSION_BACKUP_RUNNING {
        ereport!(ERROR, "a backup is already in progress in this session");
    }

    /*
     * backup_state and tablespace_map need to be long-lived as they are used
     * in pg_backup_stop().  These are allocated in a dedicated memory context
     * child of TopMemoryContext, deleted at the end of pg_backup_stop().  If
     * an error happens before ending the backup, memory would be leaked in
     * this context until pg_backup_start() is called again.
     */
    if backupcontext.is_null() {
        backupcontext = AllocSetContextCreate(
            TopMemoryContext as MemoryContextStub,
            c"on-line backup context".as_ptr(),
            ALLOCSET_START_SMALL_SIZES,
        );
    } else {
        backup_state = std::ptr::null_mut();
        tablespace_map = std::ptr::null_mut();
        MemoryContextReset(backupcontext);
    }

    oldcontext = MemoryContextSwitchTo(backupcontext as *mut _) as MemoryContextStub;
    backup_state = palloc0(std::mem::size_of::<BackupState>()) as *mut BackupState;
    tablespace_map = makeStringInfo();
    MemoryContextSwitchTo(oldcontext as *mut _);

    register_persistent_abort_backup_handler();
    do_pg_backup_start(backupidstr, fast, std::ptr::null_mut(), backup_state, tablespace_map);

    PG_RETURN_LSN!(backup_state_startpoint(backup_state))
}

/*
 * pg_backup_stop: finish taking an on-line backup.
 *
 * The first parameter (variable 'waitforarchive'), which is optional,
 * allows the user to choose if they want to wait for the WAL to be archived
 * or if we should just return as soon as the WAL record is written.
 *
 * This function stops an in-progress backup, creates backup_label contents and
 * it returns the backup stop LSN, backup_label and tablespace_map contents.
 *
 * The backup_label contains the user-supplied label string (typically this
 * would be used to tell where the backup dump will be stored), the starting
 * time, starting WAL location for the dump and so on.  It is the caller's
 * responsibility to write the backup_label and tablespace_map files in the
 * data folder that will be restored from this backup.
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_backup_stop(fcinfo: FunctionCallInfo) -> Datum {
    const PG_BACKUP_STOP_V2_COLS: usize = 3;
    let mut tupdesc: TupleDescStub = std::ptr::null_mut();
    let mut values: [Datum; PG_BACKUP_STOP_V2_COLS] = [0; PG_BACKUP_STOP_V2_COLS];
    let nulls: [bool; PG_BACKUP_STOP_V2_COLS] = [false; PG_BACKUP_STOP_V2_COLS];
    let waitforarchive: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let backup_label: *mut c_char;
    let status: SessionBackupState = get_backup_status();

    /* Initialize attributes information in the tuple descriptor */
    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    if status != SESSION_BACKUP_RUNNING {
        ereport!(ERROR, "backup is not in progress");
    }

    Assert!(!backup_state.is_null());
    Assert!(!tablespace_map.is_null());

    /* Stop the backup */
    do_pg_backup_stop(backup_state, waitforarchive);

    /* Build the contents of backup_label */
    backup_label = build_backup_content(backup_state, false);

    values[0] = LSNGetDatum(backup_state_stoppoint(backup_state));
    values[1] = CStringGetTextDatum(backup_label);
    values[2] = CStringGetTextDatum(stringinfo_data(tablespace_map));

    /* Deallocate backup-related variables */
    pfree(backup_label as *mut c_void);

    /* Clean up the session-level state and its memory context */
    backup_state = std::ptr::null_mut();
    tablespace_map = std::ptr::null_mut();
    MemoryContextDelete(backupcontext);
    backupcontext = std::ptr::null_mut();

    /* Returns the record as Datum */
    PG_RETURN_DATUM!(HeapTupleGetDatum(heap_form_tuple(
        tupdesc,
        values.as_mut_ptr(),
        nulls.as_ptr(),
    )))
}

/*
 * pg_switch_wal: switch to next xlog file
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_switch_wal(_fcinfo: FunctionCallInfo) -> Datum {
    let switchpoint: XLogRecPtr;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    switchpoint = RequestXLogSwitch(false);

    /*
     * As a convenience, return the WAL location of the switch record
     */
    PG_RETURN_LSN!(switchpoint)
}

/*
 * pg_log_standby_snapshot: call LogStandbySnapshot()
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_log_standby_snapshot(_fcinfo: FunctionCallInfo) -> Datum {
    let recptr: XLogRecPtr;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    if !XLogStandbyInfoActive() {
        ereport!(
            ERROR,
            "pg_log_standby_snapshot() can only be used if \"wal_level\" >= \"replica\""
        );
    }

    recptr = LogStandbySnapshot();

    /*
     * As a convenience, return the WAL location of the last inserted record
     */
    PG_RETURN_LSN!(recptr)
}

/*
 * pg_create_restore_point: a named point for restore
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_create_restore_point(fcinfo: FunctionCallInfo) -> Datum {
    let restore_name: *mut c_void = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let restore_name_str: *mut c_char;
    let restorepoint: XLogRecPtr;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    if !XLogIsNeeded() {
        ereport!(ERROR, "WAL level not sufficient for creating a restore point");
    }

    restore_name_str = text_to_cstring(restore_name);

    if strlen(restore_name_str) >= MAXFNAMELEN {
        elog!(
            ERROR,
            "value too long for restore point (maximum {} characters)",
            MAXFNAMELEN - 1
        );
    }

    restorepoint = XLogRestorePoint(restore_name_str);

    /*
     * As a convenience, return the WAL location of the restore point record
     */
    PG_RETURN_LSN!(restorepoint)
}

/*
 * Report the current WAL write location (same format as pg_backup_start etc)
 *
 * This is useful for determining how much of WAL is visible to an external
 * archiving process.  Note that the data before this point is written out
 * to the kernel, but is not necessarily synced to disk.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_current_wal_lsn(_fcinfo: FunctionCallInfo) -> Datum {
    let current_recptr: XLogRecPtr;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    current_recptr = GetXLogWriteRecPtr();

    PG_RETURN_LSN!(current_recptr)
}

/*
 * Report the current WAL insert location (same format as pg_backup_start etc)
 *
 * This function is mostly for debugging purposes.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_current_wal_insert_lsn(_fcinfo: FunctionCallInfo) -> Datum {
    let current_recptr: XLogRecPtr;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    current_recptr = GetXLogInsertRecPtr();

    PG_RETURN_LSN!(current_recptr)
}

/*
 * Report the current WAL flush location (same format as pg_backup_start etc)
 *
 * This function is mostly for debugging purposes.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_current_wal_flush_lsn(_fcinfo: FunctionCallInfo) -> Datum {
    let current_recptr: XLogRecPtr;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    current_recptr = GetFlushRecPtr(std::ptr::null_mut());

    PG_RETURN_LSN!(current_recptr)
}

/*
 * Report the last WAL receive location (same format as pg_backup_start etc)
 *
 * This is useful for determining how much of WAL is guaranteed to be received
 * and synced to disk by walreceiver.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_last_wal_receive_lsn(fcinfo: FunctionCallInfo) -> Datum {
    let recptr: XLogRecPtr;

    recptr = GetWalRcvFlushRecPtr(std::ptr::null_mut(), std::ptr::null_mut());

    if recptr == 0 {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_LSN!(recptr)
}

/*
 * Report the last WAL replay location (same format as pg_backup_start etc)
 *
 * This is useful for determining how much of WAL is visible to read-only
 * connections during recovery.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_last_wal_replay_lsn(fcinfo: FunctionCallInfo) -> Datum {
    let recptr: XLogRecPtr;

    recptr = GetXLogReplayRecPtr(std::ptr::null_mut());

    if recptr == 0 {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_LSN!(recptr)
}

/*
 * Compute an xlog file name and decimal byte offset given a WAL location,
 * such as is returned by pg_backup_stop() or pg_switch_wal().
 */
#[no_mangle]
pub unsafe extern "C" fn pg_walfile_name_offset(fcinfo: FunctionCallInfo) -> Datum {
    let mut xlogsegno: XLogSegNo = 0;
    let xrecoff: uint32;
    let locationpoint: XLogRecPtr = PG_GETARG_LSN!(fcinfo, 0);
    let mut xlogfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut values: [Datum; 2] = [0; 2];
    let mut isnull: [bool; 2] = [false; 2];
    let mut resultTupleDesc: TupleDescStub;
    let resultHeapTuple: HeapTuple;
    let result: Datum;

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    resultTupleDesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(
        resultTupleDesc,
        1 as AttrNumber,
        c"file_name".as_ptr(),
        TEXTOID,
        -1,
        0,
    );
    TupleDescInitEntry(
        resultTupleDesc,
        2 as AttrNumber,
        c"file_offset".as_ptr(),
        INT4OID,
        -1,
        0,
    );

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    /*
     * xlogfilename
     */
    XLByteToSeg(locationpoint, &mut xlogsegno, wal_segment_size());
    XLogFileName(
        xlogfilename.as_mut_ptr(),
        GetWALInsertionTimeLine(),
        xlogsegno,
        wal_segment_size(),
    );

    values[0] = CStringGetTextDatum(xlogfilename.as_ptr());
    isnull[0] = false;

    /*
     * offset
     */
    xrecoff = XLogSegmentOffset(locationpoint, wal_segment_size());

    values[1] = UInt32GetDatum(xrecoff);
    isnull[1] = false;

    /*
     * Tuple jam: Having first prepared your Datums, then squash together
     */
    resultHeapTuple = heap_form_tuple(resultTupleDesc, values.as_mut_ptr(), isnull.as_ptr());

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM!(result)
}

/*
 * Compute an xlog file name given a WAL location,
 * such as is returned by pg_backup_stop() or pg_switch_wal().
 */
#[no_mangle]
pub unsafe extern "C" fn pg_walfile_name(fcinfo: FunctionCallInfo) -> Datum {
    let mut xlogsegno: XLogSegNo = 0;
    let locationpoint: XLogRecPtr = PG_GETARG_LSN!(fcinfo, 0);
    let mut xlogfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    XLByteToSeg(locationpoint, &mut xlogsegno, wal_segment_size());
    XLogFileName(
        xlogfilename.as_mut_ptr(),
        GetWALInsertionTimeLine(),
        xlogsegno,
        wal_segment_size(),
    );

    PG_RETURN_TEXT_P!(cstring_to_text(xlogfilename.as_ptr()))
}

/*
 * Extract the sequence number and the timeline ID from given a WAL file
 * name.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_split_walfile_name(fcinfo: FunctionCallInfo) -> Datum {
    const PG_SPLIT_WALFILE_NAME_COLS: usize = 2;
    let fname: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let fname_upper: *mut c_char;
    let mut p: *mut c_char;
    let mut tli: TimeLineID = 0;
    let mut segno: XLogSegNo = 0;
    let mut values: [Datum; PG_SPLIT_WALFILE_NAME_COLS] = [0; PG_SPLIT_WALFILE_NAME_COLS];
    let isnull: [bool; PG_SPLIT_WALFILE_NAME_COLS] = [false; PG_SPLIT_WALFILE_NAME_COLS];
    let mut tupdesc: TupleDescStub = std::ptr::null_mut();
    let tuple: HeapTuple;
    let mut buf: [c_char; 256] = [0; 256];
    let result: Datum;

    fname_upper = pstrdup(fname);

    /* Capitalize WAL file name. */
    p = fname_upper;
    while *p != 0 {
        *p = pg_toupper(*p as u8) as c_char;
        p = p.add(1);
    }

    if !IsXLogFileName(fname_upper) {
        elog!(ERROR, "invalid WAL file name \"{}\"", cstr_display(fname));
    }

    XLogFromFileName(fname_upper, &mut tli, &mut segno, wal_segment_size());

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    /* Convert to numeric. */
    snprintf(buf.as_mut_ptr(), 256, c"%llu".as_ptr(), segno);
    values[0] = DirectFunctionCall3(
        numeric_in,
        CStringGetDatum(buf.as_ptr()),
        ObjectIdGetDatum(0 as Oid),
        Int32GetDatum(-1),
    );

    values[1] = Int64GetDatum(tli as i64);

    tuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), isnull.as_ptr());
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM!(result)
}

/*
 * pg_wal_replay_pause - Request to pause recovery
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_wal_replay_pause(fcinfo: FunctionCallInfo) -> Datum {
    if !RecoveryInProgress() {
        ereport!(ERROR, "recovery is not in progress");
    }

    if PromoteIsTriggered() {
        ereport!(ERROR, "standby promotion is ongoing");
    }

    SetRecoveryPause(true);

    /* wake up the recovery process so that it can process the pause request */
    WakeupRecovery();

    PG_RETURN_VOID!()
}

/*
 * pg_wal_replay_resume - resume recovery now
 *
 * Permission checking for this function is managed through the normal
 * GRANT system.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_wal_replay_resume(fcinfo: FunctionCallInfo) -> Datum {
    if !RecoveryInProgress() {
        ereport!(ERROR, "recovery is not in progress");
    }

    if PromoteIsTriggered() {
        ereport!(ERROR, "standby promotion is ongoing");
    }

    SetRecoveryPause(false);

    PG_RETURN_VOID!()
}

/*
 * pg_is_wal_replay_paused
 */
#[no_mangle]
pub unsafe extern "C" fn pg_is_wal_replay_paused(_fcinfo: FunctionCallInfo) -> Datum {
    if !RecoveryInProgress() {
        ereport!(ERROR, "recovery is not in progress");
    }

    PG_RETURN_BOOL!(GetRecoveryPauseState() != RECOVERY_NOT_PAUSED)
}

/*
 * pg_get_wal_replay_pause_state - Returns the recovery pause state.
 *
 * Returned values:
 *
 * 'not paused' - if pause is not requested
 * 'pause requested' - if pause is requested but recovery is not yet paused
 * 'paused' - if recovery is paused
 */
#[no_mangle]
pub unsafe extern "C" fn pg_get_wal_replay_pause_state(_fcinfo: FunctionCallInfo) -> Datum {
    let mut statestr: *const c_char = std::ptr::null();

    if !RecoveryInProgress() {
        ereport!(ERROR, "recovery is not in progress");
    }

    /* get the recovery pause state */
    match GetRecoveryPauseState() {
        RECOVERY_NOT_PAUSED => {
            statestr = c"not paused".as_ptr();
        }
        RECOVERY_PAUSE_REQUESTED => {
            statestr = c"pause requested".as_ptr();
        }
        RECOVERY_PAUSED => {
            statestr = c"paused".as_ptr();
        }
        _ => {}
    }

    Assert!(!statestr.is_null());
    PG_RETURN_TEXT_P!(cstring_to_text(statestr))
}

/*
 * Returns timestamp of latest processed commit/abort record.
 *
 * When the server has been started normally without recovery the function
 * returns NULL.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_last_xact_replay_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let xtime: TimestampTz;

    xtime = GetLatestXTime();
    if xtime == 0 {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TIMESTAMPTZ!(xtime)
}

/*
 * Returns bool with current recovery mode, a global state.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_is_in_recovery(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(RecoveryInProgress())
}

/*
 * Compute the difference in bytes between two WAL locations.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_wal_lsn_diff(fcinfo: FunctionCallInfo) -> Datum {
    let result: Datum;

    result = DirectFunctionCall2(
        pg_lsn_mi,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
    );

    PG_RETURN_DATUM!(result)
}

/*
 * Promotes a standby server.
 *
 * A result of "true" means that promotion has been completed if "wait" is
 * "true", or initiated if "wait" is false.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_promote(fcinfo: FunctionCallInfo) -> Datum {
    let wait: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let wait_seconds: c_int = PG_GETARG_INT32!(fcinfo, 1);
    let promote_file: *mut c_void;
    let mut i: c_int;

    if !RecoveryInProgress() {
        ereport!(ERROR, "recovery is not in progress");
    }

    if wait_seconds <= 0 {
        ereport!(ERROR, "\"wait_seconds\" must not be negative or zero");
    }

    /* create the promote signal file */
    promote_file = AllocateFile(PROMOTE_SIGNAL_FILE.as_ptr(), c"w".as_ptr());
    if promote_file.is_null() {
        elog!(
            ERROR,
            "could not create file \"{}\": %m",
            cstr_display(PROMOTE_SIGNAL_FILE.as_ptr())
        );
    }

    if FreeFile(promote_file) != 0 {
        elog!(
            ERROR,
            "could not write file \"{}\": %m",
            cstr_display(PROMOTE_SIGNAL_FILE.as_ptr())
        );
    }

    /* signal the postmaster */
    if kill(PostmasterPid, SIGUSR1) != 0 {
        unlink(PROMOTE_SIGNAL_FILE.as_ptr());
        ereport!(ERROR, "failed to send signal to postmaster");
    }

    /* return immediately if waiting was not requested */
    if !wait {
        return PG_RETURN_BOOL!(true);
    }

    /* wait for the amount of time wanted until promotion */
    const WAITS_PER_SECOND: c_int = 10;
    i = 0;
    while i < WAITS_PER_SECOND * wait_seconds {
        let rc: c_int;

        ResetLatch(MyLatch);

        if !RecoveryInProgress() {
            return PG_RETURN_BOOL!(true);
        }

        CHECK_FOR_INTERRUPTS();

        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            1000_i64 / WAITS_PER_SECOND as i64,
            WAIT_EVENT_PROMOTE,
        );

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if rc & WL_POSTMASTER_DEATH != 0 {
            ereport!(
                FATAL,
                "terminating connection due to unexpected postmaster exit"
            );
        }

        i += 1;
    }

    elog!(
        WARNING,
        "server did not promote within {} seconds",
        wait_seconds
    );
    PG_RETURN_BOOL!(false)
}

// ---------------------------------------------------------------------------
// Local stubs for not-yet-ported helpers
// ---------------------------------------------------------------------------

const ALLOCSET_START_SMALL_SIZES: c_int = 0;

unsafe fn get_backup_status() -> SessionBackupState { crate::access::transam::xlog::get_backup_status() }
unsafe fn text_to_cstring(_t: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/varlena
}
unsafe fn AllocSetContextCreate(
    _parent: MemoryContextStub,
    _name: *const c_char,
    _flags: c_int,
) -> MemoryContextStub {
    unimplemented!() // TODO: utils/mmgr/aset
}
unsafe fn MemoryContextReset(_ctx: MemoryContextStub) { crate::utils::mmgr::mcxt::MemoryContextReset(_ctx as _) }
unsafe fn MemoryContextDelete(_ctx: MemoryContextStub) { crate::utils::mmgr::mcxt::MemoryContextDelete(_ctx as _) }
unsafe fn makeStringInfo() -> StringInfo { crate::lib::stringinfo::makeStringInfo() }
unsafe fn stringinfo_data(_si: StringInfo) -> *const c_char {
    unimplemented!() // TODO: lib/stringinfo (->data)
}
unsafe fn register_persistent_abort_backup_handler() { crate::access::transam::xlog::register_persistent_abort_backup_handler() }
unsafe fn do_pg_backup_start(
    _backupidstr: *mut c_char,
    _fast: bool,
    _tablespaces: *mut c_void,
    _state: *mut BackupState,
    _tblspcmapfile: StringInfo,
) {
    unimplemented!() // TODO: access/xlog
}
unsafe fn do_pg_backup_stop(_state: *mut BackupState, _waitforarchive: bool) {
    unimplemented!() // TODO: access/xlog
}
unsafe fn build_backup_content(_state: *mut BackupState, _ishistoryfile: bool) -> *mut c_char { crate::access::transam::xlogbackup::build_backup_content(_state as _, _ishistoryfile as _) }
unsafe fn backup_state_startpoint(_state: *mut BackupState) -> XLogRecPtr {
    unimplemented!() // TODO: access/xlogbackup (->startpoint)
}
unsafe fn backup_state_stoppoint(_state: *mut BackupState) -> XLogRecPtr {
    unimplemented!() // TODO: access/xlogbackup (->stoppoint)
}
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDescStub,
) -> c_int {
    unimplemented!() // TODO: utils/fmgr/funcapi
}
unsafe fn LSNGetDatum(_lsn: XLogRecPtr) -> Datum { crate::utils::adt::pg_lsn::LSNGetDatum(_lsn as _) }
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/adt/varlena
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO: access/common/heaptuple
}
unsafe fn heap_form_tuple(
    _tupdesc: TupleDescStub,
    _values: *mut Datum,
    _isnull: *const bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple
}
unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }
unsafe fn RequestXLogSwitch(_mark_unimportant: bool) -> XLogRecPtr {
    unimplemented!() // TODO: access/xlog
}
unsafe fn XLogStandbyInfoActive() -> bool {
    unimplemented!() // TODO: access/xlog
}
unsafe fn LogStandbySnapshot() -> XLogRecPtr { crate::storage::ipc::standby::LogStandbySnapshot() }
unsafe fn XLogIsNeeded() -> bool {
    unimplemented!() // TODO: access/xlog
}
unsafe fn XLogRestorePoint(_rpname: *const c_char) -> XLogRecPtr { crate::access::transam::xlog::XLogRestorePoint(_rpname as _) }
unsafe fn GetXLogWriteRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetXLogWriteRecPtr() }
unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetXLogInsertRecPtr() }
unsafe fn GetFlushRecPtr(_insertTLI: *mut TimeLineID) -> XLogRecPtr { crate::access::transam::xlog::GetFlushRecPtr(_insertTLI as _) }
unsafe fn GetWalRcvFlushRecPtr(_latestChunkStart: *mut XLogRecPtr, _receiveTLI: *mut TimeLineID) -> XLogRecPtr {
    unimplemented!() // TODO: replication/walreceiver
}
unsafe fn GetXLogReplayRecPtr(_replayTLI: *mut TimeLineID) -> XLogRecPtr { crate::access::transam::xlogrecovery::GetXLogReplayRecPtr(_replayTLI as _) }
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDescStub {
    unimplemented!() // TODO: access/common/tupdesc
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDescStub,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) {
    unimplemented!() // TODO: access/common/tupdesc
}
unsafe fn BlessTupleDesc(_tupdesc: TupleDescStub) -> TupleDescStub {
    unimplemented!() // TODO: utils/fmgr/funcapi
}
unsafe fn wal_segment_size() -> c_int {
    unimplemented!() // TODO: access/xlog (global wal_segment_size)
}
unsafe fn XLByteToSeg(_xlrp: XLogRecPtr, _logSegNo: *mut XLogSegNo, _wal_segsz_bytes: c_int) {
    unimplemented!() // TODO: access/xlog_internal
}
unsafe fn XLogFileName(
    _fname: *mut c_char,
    _tli: TimeLineID,
    _logSegNo: XLogSegNo,
    _wal_segsz_bytes: c_int,
) {
    unimplemented!() // TODO: access/xlog_internal
}
unsafe fn GetWALInsertionTimeLine() -> TimeLineID { crate::access::transam::xlog::GetWALInsertionTimeLine() }
unsafe fn UInt32GetDatum(_X: uint32) -> Datum { crate::postgres::UInt32GetDatum(_X as _) }
unsafe fn XLogSegmentOffset(_xlogptr: XLogRecPtr, _wal_segsz_bytes: c_int) -> uint32 {
    unimplemented!() // TODO: access/xlog_internal
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut c_void {
    unimplemented!() // TODO: utils/adt/varlena
}
unsafe fn pstrdup(_in: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt
}
unsafe fn pg_toupper(_ch: u8) -> u8 {
    unimplemented!() // TODO: port/pgstrcasecmp
}
unsafe fn IsXLogFileName(_fname: *const c_char) -> bool { crate::access::transam::xlog_internal::IsXLogFileName(_fname as _) }
unsafe fn XLogFromFileName(
    _fname: *const c_char,
    _tli: *mut TimeLineID,
    _logSegNo: *mut XLogSegNo,
    _wal_segsz_bytes: c_int,
) {
    unimplemented!() // TODO: access/xlog_internal
}
unsafe fn DirectFunctionCall3(
    _func: unsafe extern "C" fn(FunctionCallInfo) -> Datum,
    _arg1: Datum,
    _arg2: Datum,
    _arg3: Datum,
) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr
}
unsafe fn DirectFunctionCall2(
    _func: unsafe extern "C" fn(FunctionCallInfo) -> Datum,
    _arg1: Datum,
    _arg2: Datum,
) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr
}
unsafe extern "C" fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/adt/numeric
}
unsafe extern "C" fn pg_lsn_mi(_fcinfo: FunctionCallInfo) -> Datum { crate::utils::adt::pg_lsn::pg_lsn_mi(_fcinfo as _) }
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn ObjectIdGetDatum(_X: Oid) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int32GetDatum(_X: i32) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int64GetDatum(_X: i64) -> Datum { crate::postgres::Int64GetDatum(_X as _) }
unsafe fn PromoteIsTriggered() -> bool { crate::access::transam::xlogrecovery::PromoteIsTriggered() }
unsafe fn SetRecoveryPause(_recoveryPause: bool) { crate::access::transam::xlogrecovery::SetRecoveryPause(_recoveryPause as _) }
unsafe fn WakeupRecovery() { crate::access::transam::xlogrecovery::WakeupRecovery() }
unsafe fn GetRecoveryPauseState() -> c_int {
    unimplemented!() // TODO: access/xlogrecovery
}
unsafe fn GetLatestXTime() -> TimestampTz { crate::access::transam::xlogrecovery::GetLatestXTime() }
unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut c_void { crate::storage::file::fd::AllocateFile(_name as _, _mode as _) }
unsafe fn FreeFile(_file: *mut c_void) -> c_int { crate::storage::file::fd::FreeFile(_file as _) }
unsafe fn ResetLatch(_latch: *mut c_void) { crate::storage::ipc::latch::ResetLatch(_latch as _) }
unsafe fn WaitLatch(
    _latch: *mut c_void,
    _wakeEvents: c_int,
    _timeout: i64,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!() // TODO: storage/ipc/latch
}

// External globals referenced (stubbed)
#[allow(non_upper_case_globals)]
static mut MyLatch: *mut c_void = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
static mut PostmasterPid: c_int = 0;
#[allow(non_upper_case_globals)]
static mut TopMemoryContext: *mut c_void = std::ptr::null_mut();

// Helper to render a *const c_char in elog/ereport format strings.
unsafe fn cstr_display(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "(null)";
    }
    match CStr::from_ptr(s).to_str() {
        Ok(v) => std::mem::transmute::<&str, &'static str>(v),
        Err(_) => "(invalid)",
    }
}

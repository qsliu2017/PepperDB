/*-------------------------------------------------------------------------
 *
 * origin.rs
 *   Logical replication progress tracking support.
 *
 * Copyright (c) 2013-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/backend/replication/logical/origin.c
 *   -> src/replication/logical/origin.rs
 *
 * NOTES
 *
 * This file provides the following:
 * * An infrastructure to name nodes in a replication setup
 * * A facility to efficiently store and persist replication progress in an
 *   efficient and durable manner.
 *
 * Replication origin consist out of a descriptive, user defined, external
 * name and a short, thus space efficient, internal 2 byte one. This split
 * exists because replication origin have to be stored in WAL and shared
 * memory and long descriptors would be inefficient.  For now only use 2 bytes
 * for the internal id of a replication origin as it seems unlikely that there
 * soon will be more than 65k nodes in one replication setup; and using only
 * two bytes allow us to be more space efficient.
 *
 * Replication progress is tracked in a shared memory table
 * (ReplicationState) that's dumped to disk every checkpoint. Entries
 * ('slots') in this table are identified by the internal id. That's the case
 * because it allows to increase replication progress during crash
 * recovery. To allow doing so we store the original LSN (from the originating
 * system) of a transaction in the commit record. That allows to recover the
 * precise replayed state after crash recovery; without requiring synchronous
 * commits. Allowing logical replication to use asynchronous commit is
 * generally good for performance, but especially important as it allows a
 * single threaded replay process to keep up with a source that has multiple
 * backends generating changes concurrently.  For efficiency and simplicity
 * reasons a backend can setup one replication origin that's from then used as
 * the source of changes produced by the backend, until reset again.
 *
 * This infrastructure is intended to be used in cooperation with logical
 * decoding. When replaying from a remote system the configured origin is
 * provided to output plugins, allowing prevention of replication loops and
 * other filtering.
 *
 * There are several levels of locking at work:
 *
 * * To create and drop replication origins an exclusive lock on
 *   pg_replication_slot is required for the duration. That allows us to
 *   safely and conflict free assign new origins using a dirty snapshot.
 *
 * * When creating an in-memory replication progress slot the ReplicationOrigin
 *   LWLock has to be held exclusively; when iterating over the replication
 *   progress a shared lock has to be held, the same when advancing the
 *   replication progress of an individual backend that has not setup as the
 *   session's replication origin.
 *
 * * When manipulating or looking at the remote_lsn and local_lsn fields of a
 *   replication progress slot that slot's lwlock has to be held. That's
 *   primarily because we do not assume 8 byte writes (the LSN) is atomic on
 *   all our platforms, but it also simplifies memory ordering concerns
 *   between the remote and local lsn. We use a lwlock instead of a spinlock
 *   so it's less harmful to hold the lock over a WAL write
 *   (cf. AdvanceReplicationProgress).
 *
 * -------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

// ---------------------------------------------------------------------------
// Types re-used from canonical homes
// ---------------------------------------------------------------------------

use crate::access::transam::xlogdefs::{
    InvalidXLogRecPtr, LSN_FORMAT_ARGS, RepOriginId, XLogRecPtr,
};
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetData, XLogRecGetInfo, XLR_INFO_MASK,
};
use crate::access::transam::xloginsert::{XLogBeginInsert, XLogInsert, XLogRegisterData};
use crate::access::index::genam::{
    SysScanDesc, systable_beginscan, systable_endscan, systable_getnext,
};
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::table::table::{table_close, table_open, LOCKMODE};
// HeapTuple = *mut HeapTupleData (htup_details canonical); genam returns *mut c_void
// which we cast to HeapTuple for HeapTupleIsValid.
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::rmgrlist::RM_REPLORIGIN_ID;
use crate::c::{uint8, uint16, uint32, Size};
use crate::catalog::catalog_oids::ReplicationOriginRelationId;
use crate::catalog::pg_replication_origin::{Form_pg_replication_origin, FormData_pg_replication_origin};
use crate::catalog::pg_subscription::{LOGICALREP_ORIGIN_ANY, LOGICALREP_ORIGIN_NONE};
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, TimestampTz};
use crate::nodes::execnodes::{Relation, ReturnSetInfo, Tuplestorestate};
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::Oid;
use crate::port::pg_crc32c::{pg_crc32c, COMP_CRC32C, FIN_CRC32C, INIT_CRC32C};
use crate::storage::ipc::ipc::on_shmem_exit;
// ConditionVariable (real struct) + its API from canonical home.
use crate::storage::lmgr::condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableInit, ConditionVariableSleep,
};
use crate::storage::lmgr::lmgr::{LockSharedObject, UnlockSharedObject};
use crate::utils::builtins::{text_to_cstring, CStringGetTextDatum};
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1};
use crate::catalog::catalog::IsReservedName;

// ---------------------------------------------------------------------------
// Merged from replication/origin.h
// ---------------------------------------------------------------------------

/// XLOG stuff (replication/origin.h)
pub const XLOG_REPLORIGIN_SET: uint8 = 0x00;
pub const XLOG_REPLORIGIN_DROP: uint8 = 0x10;

/// Invalid replication origin identifier (replication/origin.h).
pub const InvalidRepOriginId: RepOriginId = 0;

/// "do not replicate" sentinel (replication/origin.h).
pub const DoNotReplicateId: RepOriginId = uint16::MAX;

/// Maximum length of a replication origin name (replication/origin.h).
///
/// To avoid needing a TOAST table for pg_replication_origin, we limit
/// replication origin names to 512 bytes.  This should be more than enough for
/// all practical use.
pub const MAX_RONAME_LEN: usize = 512;

/// WAL record for XLOG_REPLORIGIN_SET: advance a replication origin's progress.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_replorigin_set {
    pub remote_lsn: XLogRecPtr,
    pub node_id: RepOriginId,
    pub force: bool,
}

/// WAL record for XLOG_REPLORIGIN_DROP: drop a replication origin.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_replorigin_drop {
    pub node_id: RepOriginId,
}

// ---------------------------------------------------------------------------
// Paths for replication origin checkpoint files
// ---------------------------------------------------------------------------

const PG_REPLORIGIN_CHECKPOINT_FILENAME: &str = "pg_logical/replorigin_checkpoint";
const PG_REPLORIGIN_CHECKPOINT_TMPFILE: &str = "pg_logical/replorigin_checkpoint.tmp";

// ---------------------------------------------------------------------------
// GUC variables
// ---------------------------------------------------------------------------

/// GUC: maximum number of concurrently active replication origins.
pub static mut max_active_replication_origins: c_int = 10;

// ---------------------------------------------------------------------------
// Replay progress of a single remote node (in-memory shared state)
// ---------------------------------------------------------------------------

/*
 * Replay progress of a single remote node.
 */
#[repr(C)]
pub struct ReplicationState {
    /*
     * Local identifier for the remote node.
     */
    pub roident: RepOriginId,

    /*
     * Location of the latest commit from the remote side.
     */
    pub remote_lsn: XLogRecPtr,

    /*
     * Remember the local lsn of the commit record so we can XLogFlush() to it
     * during a checkpoint so we know the commit record actually is safe on
     * disk.
     */
    pub local_lsn: XLogRecPtr,

    /*
     * PID of backend that's acquired slot, or 0 if none.
     */
    pub acquired_by: c_int,

    /*
     * Condition variable that's signaled when acquired_by changes.
     */
    pub origin_cv: ConditionVariable,

    /*
     * Lock protecting remote_lsn and local_lsn.
     */
    pub lock: LWLock,
}

/*
 * On disk version of ReplicationState.
 */
#[repr(C)]
struct ReplicationStateOnDisk {
    roident: RepOriginId,
    remote_lsn: XLogRecPtr,
}

#[repr(C)]
struct ReplicationStateCtl {
    /* Tranche to use for per-origin LWLocks */
    tranche_id: c_int,
    /* Array of length max_active_replication_origins */
    states: [ReplicationState; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

// ---------------------------------------------------------------------------
// External (session-level) variables
// ---------------------------------------------------------------------------

/* external variables */
/// Assumed replication origin identity for this session.
pub static mut replorigin_session_origin: RepOriginId = InvalidRepOriginId;
pub static mut replorigin_session_origin_lsn: XLogRecPtr = InvalidXLogRecPtr;
pub static mut replorigin_session_origin_timestamp: TimestampTz = 0;

// ---------------------------------------------------------------------------
// Module-level statics
// ---------------------------------------------------------------------------

/*
 * Base address into a shared memory array of replication states of size
 * max_active_replication_origins.
 */
static mut replication_states: *mut ReplicationState = core::ptr::null_mut();

/*
 * Actual shared memory block (replication_states[] is now part of this).
 */
static mut replication_states_ctl: *mut ReplicationStateCtl = core::ptr::null_mut();

/*
 * We keep a pointer to this backend's ReplicationState to avoid having to
 * search the replication_states array in replorigin_session_advance for each
 * remote commit.  (Ownership of a backend's own entry can only be changed by
 * that backend.)
 */
static mut session_replication_state: *mut ReplicationState = core::ptr::null_mut();

/* Magic for on disk files. */
const REPLICATION_STATE_MAGIC: uint32 = 0x1257DADE;

// ---------------------------------------------------------------------------
// Prerequisite stubs (not yet ported)
// ---------------------------------------------------------------------------

// Syscache ids for pg_replication_origin (catalog/pg_replication_origin.h
// MAKE_SYSCACHE macros; exact integer values come from the generated
// syscache_ids.h, which has not been ported yet).
// TODO(pg-port): replace with crate::utils::cache::syscache::{REPLORIGIDENT,REPLORIGNAME}
const REPLORIGIDENT: c_int = 58; // TODO(pg-port): real REPLORIGIDENT lives in utils/syscache.h (generated)
const REPLORIGNAME: c_int = 59;  // TODO(pg-port): real REPLORIGNAME lives in utils/syscache.h (generated)

// Catalog index OID for pg_replication_origin_roiident_index
// (value 6001 from pg_replication_origin.h DECLARE_UNIQUE_INDEX_PKEY).
// TODO(pg-port): real ReplicationOriginIdentIndex lives in catalog/pg_replication_origin.h
const ReplicationOriginIdentIndex: Oid = 6001;

// Column attribute numbers for pg_replication_origin
// (pg_replication_origin_d.h, generated from pg_replication_origin.h).
// TODO(pg-port): real Anum_* live in catalog/pg_replication_origin_d.h
const Anum_pg_replication_origin_roident: c_int = 1;
const Anum_pg_replication_origin_roname: c_int = 2;
const Natts_pg_replication_origin: usize = 2;

// TODO(pg-port): real GETSTRUCT lives in access/htup_details.rs
unsafe fn GETSTRUCT_replication_origin(tup: HeapTuple) -> Form_pg_replication_origin {
    use crate::access::htup_details::GETSTRUCT;
    GETSTRUCT(tup) as Form_pg_replication_origin
}

// TODO(pg-port): real F_OIDEQ lives in utils/fmgroids.h
const F_OIDEQ: Oid = 184;

// TODO(pg-port): real InvalidOid lives in postgres_ext.h (already in c.rs)
use crate::postgres_ext::InvalidOid;

// Lock modes (storage/lockdefs.h) -- TODO(pg-port): real values from storage/lockdefs.h
const NoLock: LOCKMODE = 0;
const RowExclusiveLock: LOCKMODE = 3;
const ExclusiveLock: LOCKMODE = 7;
const AccessExclusiveLock: LOCKMODE = 8;

// LWLock stubs -- TODO(pg-port): real LWLock lives in storage/lwlock.h
#[repr(C)]
pub struct LWLock {
    _opaque: [u8; 0],
}
const LW_EXCLUSIVE: c_int = 0; // TODO(pg-port): real value lives in storage/lwlock.h
const LW_SHARED: c_int = 1;    // TODO(pg-port): real value lives in storage/lwlock.h

const LWTRANCHE_REPLICATION_ORIGIN_STATE: c_int = 0; // TODO(pg-port): real value lives in storage/lwlock.h

unsafe fn ReplicationOriginLock() -> *mut LWLock {
    crate::backend_link_shims::ReplicationOriginLock as *mut LWLock
}
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(_lock as _, if _mode == 1 { crate::storage::lmgr::lwlock::LWLockMode::LW_SHARED } else { crate::storage::lmgr::lwlock::LWLockMode::LW_EXCLUSIVE })
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(_lock as _)
}
unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    crate::storage::lmgr::lwlock::LWLockInitialize(_lock as _, _tranche_id)
}

// Shared memory init -- TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(
    _name: *const c_char,
    _size: Size,
    _found: *mut bool,
) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size, _found)
}

// Size helpers -- TODO(pg-port): real add_size/mul_size live in storage/ipc/shmem.c
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // TODO(pg-port): real add_size lives in storage/ipc/shmem.c (with overflow check)
}
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2 // TODO(pg-port): real mul_size lives in storage/ipc/shmem.c (with overflow check)
}

// MemSet -- TODO(pg-port): real MemSet lives in c.h
unsafe fn MemSet(start: *mut c_void, val: c_int, len: Size) {
    use crate::c::MemSet as crate_MemSet;
    crate_MemSet(start, val, len);
}

// XLogFlush -- TODO(pg-port): real XLogFlush lives in access/xlog.c
unsafe fn XLogFlush(lsn: XLogRecPtr) { crate::access::transam::xlog::XLogFlush(lsn as _) }

// RecoveryInProgress -- TODO(pg-port): real RecoveryInProgress lives in access/xlog.c
unsafe fn RecoveryInProgress() -> bool {
    crate::access::transam::xlog::RecoveryInProgress()
}

// IsTransactionState -- TODO(pg-port): real IsTransactionState lives in access/xact.c
unsafe fn IsTransactionState() -> bool {
    crate::access::transam::xact::IsTransactionState()
}

// pg_strcasecmp -- real impl lives in port/pgstrcasecmp.rs; imported below.
use crate::port::pgstrcasecmp::pg_strcasecmp;

// CatalogTupleInsert/CatalogTupleDelete -- real impl lives in catalog/indexing.rs
use crate::catalog::indexing::{CatalogTupleDelete, CatalogTupleInsert};

// heap_form_tuple / heap_freetuple -- real impl lives in access/common/heaptuple.rs
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};

// RelationGetDescr -- real impl lives in utils/rel.rs
use crate::utils::rel::RelationGetDescr;

// CommandCounterIncrement -- TODO(pg-port): real CommandCounterIncrement lives in access/xact.c
unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO(pg-port): real CommandCounterIncrement lives in access/xact.c
}

// LockRelationOid/UnlockRelationOid -- real impl lives in storage/lmgr/lmgr.rs
use crate::storage::lmgr::lmgr::{LockRelationOid, UnlockRelationOid};

// InitDirtySnapshot -- TODO(pg-port): real InitDirtySnapshot lives in utils/snapmgr.c
// SnapshotData (opaque for now)
type SnapshotData = c_void;
unsafe fn InitDirtySnapshot(_snap: *mut SnapshotData) {
    unimplemented!() // TODO(pg-port): real InitDirtySnapshot lives in utils/snapmgr.c
}

// OpenTransientFile / CloseTransientFile -- TODO(pg-port): real defs in storage/file/fd.c
unsafe fn OpenTransientFile(_path: *const c_char, _flags: c_int) -> c_int {
    crate::storage::file::fd::OpenTransientFile(_path, _flags)
}
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    crate::storage::file::fd::CloseTransientFile(_fd)
}

// durable_rename -- real impl lives in common/file_utils.rs
use crate::common::file_utils::durable_rename;

// errcode_for_file_access -- TODO(pg-port): real errcode_for_file_access lives in storage/file/fd.c
unsafe fn errcode_for_file_access() -> c_int {
    0 // TODO(pg-port): real errcode_for_file_access lives in storage/file/fd.c
}

// MyProcPid -- real value lives in miscadmin.rs extern block
use crate::miscadmin::MyProcPid;

// WAIT_EVENT_REPLICATION_ORIGIN_DROP -- TODO(pg-port): real value lives in utils/wait_event_types.h (generated)
const WAIT_EVENT_REPLICATION_ORIGIN_DROP: uint32 = 0; // TODO(pg-port): real value lives in utils/wait_event_types.h

// errcode constants -- TODO(pg-port): real values live in utils/errcodes.h (generated)
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0; // TODO(pg-port): utils/errcodes.h
const ERRCODE_READ_ONLY_SQL_TRANSACTION: c_int = 0;        // TODO(pg-port): utils/errcodes.h
const ERRCODE_OBJECT_IN_USE: c_int = 0;                    // TODO(pg-port): utils/errcodes.h
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;           // TODO(pg-port): utils/errcodes.h
const ERRCODE_CONFIGURATION_LIMIT_EXCEEDED: c_int = 0;     // TODO(pg-port): utils/errcodes.h
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;                 // TODO(pg-port): utils/errcodes.h
const ERRCODE_RESERVED_NAME: c_int = 0;                    // TODO(pg-port): utils/errcodes.h
const ERRCODE_DATA_CORRUPTED: c_int = 0;                   // TODO(pg-port): utils/errcodes.h

// errdetail / errhint -- TODO(pg-port): real impls live in utils/error/elog.c
unsafe fn errdetail(_fmt: &str) -> c_int {
    0 // TODO(pg-port): real errdetail lives in utils/error/elog.c
}
unsafe fn errhint(_fmt: &str) -> c_int {
    0 // TODO(pg-port): real errhint lives in utils/error/elog.c
}

// O_CREAT / O_EXCL / O_WRONLY / O_RDONLY / O_WRONLY / PG_BINARY
// These map to POSIX constants; declared locally to avoid depending on libc.
// TODO(pg-port): real values from <fcntl.h> via pg platform abstraction.
const O_RDONLY: c_int = 0;
const O_WRONLY: c_int = 1;
const O_CREAT: c_int = 64;
const O_EXCL: c_int = 128;
const PG_BINARY: c_int = 0; // TODO(pg-port): real PG_BINARY lives in port.h

// errno constant
const ENOENT: c_int = 2;
const ENOSPC: c_int = 28;

// FunctionCallInfo -- real type lives in utils/fmgr.rs
use crate::utils::fmgr::FunctionCallInfo;

// Datum helpers used by SQL functions
use crate::postgres::{Datum, DatumGetPointer};

// PG_FUNCTION_ARGS / PG_GETARG_* / PG_RETURN_* macros from crate root
use crate::{
    PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_TEXT_PP, PG_RETURN_BOOL,
    PG_RETURN_NULL, PG_RETURN_VOID, PG_RETURN_OID,
};

// LSN return/getarg helpers (local shims matching xlogfuncs.rs convention)
macro_rules! PG_RETURN_LSN {
    ($x:expr) => {
        return ($x) as Datum
    };
}
macro_rules! PG_GETARG_LSN {
    ($fcinfo:expr, $n:expr) => {{
        let _ = $fcinfo;
        0 as XLogRecPtr // TODO(pg-port): real PG_GETARG_LSN lives in utils/pg_lsn.h
    }};
}

// LSNGetDatum -- TODO(pg-port): real LSNGetDatum lives in utils/adt/pg_lsn.c
unsafe fn LSNGetDatum(lsn: XLogRecPtr) -> Datum { crate::utils::adt::pg_lsn::LSNGetDatum(lsn as _) }

// text type alias
type text = c_void;

// InitMaterializedSRF -- TODO(pg-port): real InitMaterializedSRF lives in utils/fmgr/funcapi.c
unsafe fn InitMaterializedSRF(fcinfo: FunctionCallInfo, flags: c_int) { crate::utils::fmgr::funcapi::InitMaterializedSRF(fcinfo as _, flags as _) }
// tuplestore_putvalues -- TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.c
unsafe fn tuplestore_putvalues(
    state: *mut Tuplestorestate,
    tdesc: *mut c_void,
    values: *mut Datum,
    nulls: *mut bool,
) { crate::utils::sort::tuplestore::tuplestore_putvalues(state as _, tdesc as _, values as _, nulls as _) }

// TupleDesc alias
use crate::access::common::tupdesc::TupleDesc;

// write(2) -- thin wrapper over libc write; use raw syscall via extern
extern "C" {
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn unlink(path: *const c_char) -> c_int;
    // errno as a variable is not portable; model it as extern
    static mut errno: c_int;
}

// ---------------------------------------------------------------------------
// Helper: check prerequisites
// ---------------------------------------------------------------------------

unsafe fn replorigin_check_prerequisites(check_origins: bool, recovery_ok: bool) {
    if check_origins && max_active_replication_origins == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot query or manipulate replication origin when \"max_active_replication_origins\" is 0"
            )
        );
    }

    if !recovery_ok && RecoveryInProgress() {
        ereport!(
            ERROR,
            errmsg!("cannot manipulate replication origins during recovery")
        );
    }
}

// ---------------------------------------------------------------------------
// Helper: check for reserved names
// ---------------------------------------------------------------------------

/*
 * IsReservedOriginName
 *   True iff name is either "none" or "any".
 */
unsafe fn IsReservedOriginName(name: *const c_char) -> bool {
    use std::ffi::CStr;
    let none_c = std::ffi::CString::new(LOGICALREP_ORIGIN_NONE).unwrap();
    let any_c = std::ffi::CString::new(LOGICALREP_ORIGIN_ANY).unwrap();
    pg_strcasecmp(name, none_c.as_ptr()) == 0 || pg_strcasecmp(name, any_c.as_ptr()) == 0
}

/* ---------------------------------------------------------------------------
 * Functions for working with replication origins themselves.
 * ---------------------------------------------------------------------------
 */

/*
 * Check for a persistent replication origin identified by name.
 *
 * Returns InvalidOid if the node isn't known yet and missing_ok is true.
 */
#[no_mangle]
pub unsafe fn replorigin_by_name(roname: *const c_char, missing_ok: bool) -> RepOriginId {
    let mut roident: Oid = InvalidOid;

    let roname_d: Datum = CStringGetTextDatum(roname);

    let tuple: HeapTuple = SearchSysCache1(REPLORIGNAME, roname_d);
    if HeapTupleIsValid(tuple) {
        let ident: Form_pg_replication_origin = GETSTRUCT_replication_origin(tuple);
        roident = (*ident).roident;
        ReleaseSysCache(tuple);
    } else if !missing_ok {
        ereport!(
            ERROR,
            errmsg!("replication origin {:?} does not exist", roname)
        );
    }

    roident as RepOriginId
}

/*
 * Create a replication origin.
 *
 * Needs to be called in a transaction.
 */
#[no_mangle]
pub unsafe fn replorigin_create(roname: *const c_char) -> RepOriginId {
    let mut roident: Oid;
    let mut tuple: HeapTuple = core::ptr::null_mut();
    let rel: Relation;
    let roname_d: Datum;
    let mut SnapshotDirty: SnapshotData = core::mem::zeroed();
    let mut scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();

    /*
     * To avoid needing a TOAST table for pg_replication_origin, we limit
     * replication origin names to 512 bytes.  This should be more than enough
     * for all practical use.
     */
    let name_len = {
        let mut p = roname;
        let mut n = 0usize;
        while *p != 0 {
            n += 1;
            p = p.add(1);
        }
        n
    };
    if name_len > MAX_RONAME_LEN {
        ereport!(
            ERROR,
            errmsg!("replication origin name is too long")
        );
    }

    roname_d = CStringGetTextDatum(roname);

    Assert!(IsTransactionState());

    /*
     * We need the numeric replication origin to be 16bit wide, so we cannot
     * rely on the normal oid allocation. Instead we simply scan
     * pg_replication_origin for the first unused id. That's not particularly
     * efficient, but this should be a fairly infrequent operation - we can
     * easily spend a bit more code on this when it turns out it needs to be
     * faster.
     *
     * We handle concurrency by taking an exclusive lock (allowing reads!)
     * over the table for the duration of the search. Because we use a "dirty
     * snapshot" we can read rows that other in-progress sessions have
     * written, even though they would be invisible with normal snapshots. Due
     * to the exclusive lock there's no danger that new rows can appear while
     * we're checking.
     */
    InitDirtySnapshot(&mut SnapshotDirty);

    rel = table_open(ReplicationOriginRelationId, ExclusiveLock);

    /*
     * We want to be able to access pg_replication_origin without setting up a
     * snapshot.  To make that safe, it needs to not have a TOAST table, since
     * TOASTed data cannot be fetched without a snapshot.  As of this writing,
     * its only varlena column is roname, which we limit to 512 bytes to avoid
     * needing out-of-line storage.  If you add a TOAST table to this catalog,
     * be sure to set up a snapshot everywhere it might be needed.
     */
    // Assert(!OidIsValid(rel->rd_rel->reltoastrelid));  -- TODO when RelationData is ported

    roident = InvalidOid + 1;
    loop {
        if roident >= u16::MAX as Oid {
            break;
        }

        let mut nulls = [false; 2]; // Natts_pg_replication_origin
        let mut values: [Datum; 2] = [0; 2]; // Natts_pg_replication_origin
        let collides: bool;

        CHECK_FOR_INTERRUPTS();

        ScanKeyInit(
            &mut key,
            Anum_pg_replication_origin_roident as crate::access::attnum::AttrNumber,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(roident),
        );

        scan = systable_beginscan(
            rel,
            ReplicationOriginIdentIndex,
            true, /* indexOK */
            &mut SnapshotDirty as *mut SnapshotData,
            1,
            &mut key,
        );

        // systable_getnext returns *mut c_void (genam convention); cast to HeapTuple
        collides = HeapTupleIsValid(systable_getnext(scan) as *mut HeapTupleData);

        systable_endscan(scan);

        if !collides {
            /*
             * Ok, found an unused roident, insert the new row and do a CCI,
             * so our callers can look it up if they want to.
             */
            // zero already set via mem::zeroed above in array init

            values[(Anum_pg_replication_origin_roident - 1) as usize] =
                ObjectIdGetDatum(roident);
            values[(Anum_pg_replication_origin_roname - 1) as usize] = roname_d;

            tuple = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());
            CatalogTupleInsert(rel, tuple);
            CommandCounterIncrement();
            break;
        }

        roident += 1;
    }

    /* now release lock again, */
    table_close(rel, ExclusiveLock);

    if tuple.is_null() {
        ereport!(
            ERROR,
            errmsg!("could not find free replication origin ID")
        );
    }

    heap_freetuple(tuple);
    roident as RepOriginId
}

/*
 * Helper function to drop a replication origin.
 */
unsafe fn replorigin_state_clear(roident: RepOriginId, nowait: bool) {
    let mut i: c_int;

    /*
     * Clean up the slot state info, if there is any matching slot.
     */
    // restart: (loop replaces goto)
    loop {
        LWLockAcquire(ReplicationOriginLock(), LW_EXCLUSIVE);

        i = 0;
        let mut found = false;
        while i < max_active_replication_origins {
            let state: *mut ReplicationState =
                replication_states.offset(i as isize);

            if (*state).roident == roident {
                /* found our slot, is it busy? */
                if (*state).acquired_by != 0 {
                    let cv: *mut ConditionVariable;

                    if nowait {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "could not drop replication origin with ID {}, in use by PID {}",
                                (*state).roident,
                                (*state).acquired_by
                            )
                        );
                    }

                    /*
                     * We must wait and then retry.  Since we don't know which CV
                     * to wait on until here, we can't readily use
                     * ConditionVariablePrepareToSleep (calling it here would be
                     * wrong, since we could miss the signal if we did so); just
                     * use ConditionVariableSleep directly.
                     */
                    cv = &mut (*state).origin_cv;

                    LWLockRelease(ReplicationOriginLock());

                    ConditionVariableSleep(cv, WAIT_EVENT_REPLICATION_ORIGIN_DROP);
                    // goto restart -> continue outer loop
                    break; // will re-enter outer loop
                }

                /* first make a WAL log entry */
                {
                    let mut xlrec = xl_replorigin_drop { node_id: roident };

                    XLogBeginInsert();
                    XLogRegisterData(
                        &mut xlrec as *mut _ as *const c_void,
                        size_of::<xl_replorigin_drop>() as u32,
                    );
                    XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_DROP);
                }

                /* then clear the in-memory slot */
                (*state).roident = InvalidRepOriginId;
                (*state).remote_lsn = InvalidXLogRecPtr;
                (*state).local_lsn = InvalidXLogRecPtr;
                found = true;
                break;
            }
            i += 1;
        }
        LWLockRelease(ReplicationOriginLock());
        ConditionVariableCancelSleep();
        if found || i >= max_active_replication_origins {
            // Normal exit: either cleaned up or not found
            break;
        }
        // else: we released the lock and slept waiting; retry
    }
}

/*
 * Drop replication origin (by name).
 *
 * Needs to be called in a transaction.
 */
pub unsafe fn replorigin_drop_by_name(name: *const c_char, missing_ok: bool, nowait: bool) {
    let rel: Relation;
    let tuple: HeapTuple;

    Assert!(IsTransactionState());

    rel = table_open(ReplicationOriginRelationId, RowExclusiveLock);

    let roident: RepOriginId = replorigin_by_name(name, missing_ok);

    /* Lock the origin to prevent concurrent drops. */
    LockSharedObject(
        ReplicationOriginRelationId,
        roident as Oid,
        0,
        AccessExclusiveLock,
    );

    tuple = SearchSysCache1(REPLORIGIDENT, ObjectIdGetDatum(roident as Oid));
    if !HeapTupleIsValid(tuple) {
        if !missing_ok {
            elog!(
                ERROR,
                "cache lookup failed for replication origin with ID {}",
                roident
            );
        }

        /*
         * We don't need to retain the locks if the origin is already dropped.
         */
        UnlockSharedObject(
            ReplicationOriginRelationId,
            roident as Oid,
            0,
            AccessExclusiveLock,
        );
        table_close(rel, RowExclusiveLock);
        return;
    }

    replorigin_state_clear(roident, nowait);

    /*
     * Now, we can delete the catalog entry.
     */
    {
        // CatalogTupleDelete takes &tuple->t_self (ItemPointer = *mut ItemPointerData)
        let t_self_ptr: *mut crate::storage::itemptr::ItemPointerData =
            &mut (*tuple).t_self as *mut _;
        CatalogTupleDelete(rel, t_self_ptr);
    }
    ReleaseSysCache(tuple);

    CommandCounterIncrement();

    /* We keep the lock on pg_replication_origin until commit */
    table_close(rel, NoLock);
}

/*
 * Lookup replication origin via its oid and return the name.
 *
 * The external name is palloc'd in the calling context.
 *
 * Returns true if the origin is known, false otherwise.
 */
pub unsafe fn replorigin_by_oid(
    roident: RepOriginId,
    missing_ok: bool,
    roname: *mut *mut c_char,
) -> bool {
    let tuple: HeapTuple;

    Assert!(crate::c::OidIsValid(roident as Oid));
    Assert!(roident != InvalidRepOriginId);
    Assert!(roident != DoNotReplicateId);

    tuple = SearchSysCache1(REPLORIGIDENT, ObjectIdGetDatum(roident as Oid));

    if HeapTupleIsValid(tuple) {
        let ric: Form_pg_replication_origin = GETSTRUCT_replication_origin(tuple);
        // roname is a text field accessed via the struct pointer; text_to_cstring
        // requires a *const text but the field is declared as a varlen text.
        // TODO(pg-port): use heap_getattr when tupdesc access is available;
        // for now cast the trailing varlen field after the fixed struct.
        let text_ptr = (ric as *const u8)
            .add(size_of::<FormData_pg_replication_origin>()) as *const text;
        *roname = text_to_cstring(text_ptr as *const crate::c::varlena);
        ReleaseSysCache(tuple);

        return true;
    } else {
        *roname = core::ptr::null_mut();

        if !missing_ok {
            ereport!(
                ERROR,
                errmsg!("replication origin with ID {} does not exist", roident)
            );
        }

        return false;
    }
}


/* ---------------------------------------------------------------------------
 * Functions for handling replication progress.
 * ---------------------------------------------------------------------------
 */

pub unsafe fn ReplicationOriginShmemSize() -> Size {
    let mut size: Size = 0;

    if max_active_replication_origins == 0 {
        return size;
    }

    size = add_size(size, core::mem::offset_of!(ReplicationStateCtl, states));

    size = add_size(
        size,
        mul_size(
            max_active_replication_origins as Size,
            size_of::<ReplicationState>(),
        ),
    );
    size
}

pub unsafe fn ReplicationOriginShmemInit() {
    let mut found: bool = false;

    if max_active_replication_origins == 0 {
        return;
    }

    replication_states_ctl = ShmemInitStruct(
        c"ReplicationOriginState".as_ptr(),
        ReplicationOriginShmemSize(),
        &mut found,
    ) as *mut ReplicationStateCtl;
    replication_states = (*replication_states_ctl).states.as_mut_ptr();

    if !found {
        let mut i: c_int;

        MemSet(
            replication_states_ctl as *mut c_void,
            0,
            ReplicationOriginShmemSize(),
        );

        (*replication_states_ctl).tranche_id = LWTRANCHE_REPLICATION_ORIGIN_STATE;

        i = 0;
        while i < max_active_replication_origins {
            LWLockInitialize(
                &mut (*replication_states.offset(i as isize)).lock,
                (*replication_states_ctl).tranche_id,
            );
            ConditionVariableInit(&mut (*replication_states.offset(i as isize)).origin_cv);
            i += 1;
        }
    }
}

/* ---------------------------------------------------------------------------
 * Perform a checkpoint of each replication origin's progress with respect to
 * the replayed remote_lsn. Make sure that all transactions we refer to in the
 * checkpoint (local_lsn) are actually on-disk. This might not yet be the case
 * if the transactions were originally committed asynchronously.
 *
 * We store checkpoints in the following format:
 * +-------+------------------------+------------------+-----+--------+
 * | MAGIC | ReplicationStateOnDisk | struct Replic... | ... | CRC32C | EOF
 * +-------+------------------------+------------------+-----+--------+
 *
 * So its just the magic, followed by the statically sized
 * ReplicationStateOnDisk structs. Note that the maximum number of
 * ReplicationState is determined by max_active_replication_origins.
 * ---------------------------------------------------------------------------
 */
pub unsafe fn CheckPointReplicationOrigin() {
    let tmppath = PG_REPLORIGIN_CHECKPOINT_TMPFILE;
    let path = PG_REPLORIGIN_CHECKPOINT_FILENAME;
    let tmpfd: c_int;
    let mut i: c_int;
    let magic: uint32 = REPLICATION_STATE_MAGIC;
    let mut crc: pg_crc32c;

    if max_active_replication_origins == 0 {
        return;
    }

    crc = INIT_CRC32C();

    /* make sure no old temp file is remaining */
    {
        let tmppath_c = std::ffi::CString::new(tmppath).unwrap();
        if unlink(tmppath_c.as_ptr()) < 0 && errno != ENOENT {
            ereport!(
                PANIC,
                errmsg!(
                    "could not remove file \"{}\": {}",
                    tmppath,
                    errno
                )
            );
        }
    }

    /*
     * no other backend can perform this at the same time; only one checkpoint
     * can happen at a time.
     */
    {
        let tmppath_c = std::ffi::CString::new(tmppath).unwrap();
        tmpfd = OpenTransientFile(tmppath_c.as_ptr(), O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    }
    if tmpfd < 0 {
        ereport!(
            PANIC,
            errmsg!("could not create file \"{}\": {}", tmppath, errno)
        );
    }
    let tmpfd = tmpfd; // shadow as immutable

    /* write magic */
    errno = 0;
    if write(
        tmpfd,
        &magic as *const uint32 as *const c_void,
        size_of::<uint32>(),
    ) != size_of::<uint32>() as isize
    {
        /* if write didn't set errno, assume problem is no disk space */
        if errno == 0 {
            errno = ENOSPC;
        }
        ereport!(
            PANIC,
            errmsg!("could not write to file \"{}\": {}", tmppath, errno)
        );
    }
    crc = COMP_CRC32C(
        crc,
        &magic as *const uint32 as *const c_void,
        size_of::<uint32>(),
    );

    /* prevent concurrent creations/drops */
    LWLockAcquire(ReplicationOriginLock(), LW_SHARED);

    /* write actual data */
    i = 0;
    while i < max_active_replication_origins {
        let mut disk_state: ReplicationStateOnDisk = core::mem::zeroed();
        let curstate: *mut ReplicationState = replication_states.offset(i as isize);
        let local_lsn: XLogRecPtr;

        if (*curstate).roident == InvalidRepOriginId {
            i += 1;
            continue;
        }

        /* zero, to avoid uninitialized padding bytes */
        // already zeroed above via core::mem::zeroed()

        LWLockAcquire(&mut (*curstate).lock, LW_SHARED);

        disk_state.roident = (*curstate).roident;
        disk_state.remote_lsn = (*curstate).remote_lsn;
        local_lsn = (*curstate).local_lsn;

        LWLockRelease(&mut (*curstate).lock);

        /* make sure we only write out a commit that's persistent */
        XLogFlush(local_lsn);

        errno = 0;
        if write(
            tmpfd,
            &disk_state as *const ReplicationStateOnDisk as *const c_void,
            size_of::<ReplicationStateOnDisk>(),
        ) != size_of::<ReplicationStateOnDisk>() as isize
        {
            /* if write didn't set errno, assume problem is no disk space */
            if errno == 0 {
                errno = ENOSPC;
            }
            ereport!(
                PANIC,
                errmsg!("could not write to file \"{}\": {}", tmppath, errno)
            );
        }

        crc = COMP_CRC32C(
            crc,
            &disk_state as *const ReplicationStateOnDisk as *const c_void,
            size_of::<ReplicationStateOnDisk>(),
        );

        i += 1;
    }

    LWLockRelease(ReplicationOriginLock());

    /* write out the CRC */
    crc = FIN_CRC32C(crc);
    errno = 0;
    if write(
        tmpfd,
        &crc as *const pg_crc32c as *const c_void,
        size_of::<pg_crc32c>(),
    ) != size_of::<pg_crc32c>() as isize
    {
        /* if write didn't set errno, assume problem is no disk space */
        if errno == 0 {
            errno = ENOSPC;
        }
        ereport!(
            PANIC,
            errmsg!("could not write to file \"{}\": {}", tmppath, errno)
        );
    }

    if CloseTransientFile(tmpfd) != 0 {
        ereport!(
            PANIC,
            errmsg!("could not close file \"{}\": {}", tmppath, errno)
        );
    }

    /* fsync, rename to permanent file, fsync file and directory */
    {
        let tmppath_c = std::ffi::CString::new(tmppath).unwrap();
        let path_c = std::ffi::CString::new(path).unwrap();
        // C: durable_rename(tmppath, path, PANIC) -- third arg is error level
        // for failure; crate signature only takes the two paths (error handling
        // inside). TODO(pg-port): add elevel param when file_utils.c is ported.
        durable_rename(tmppath_c.as_ptr(), path_c.as_ptr());
    }
}

/*
 * Recover replication replay status from checkpoint data saved earlier by
 * CheckPointReplicationOrigin.
 *
 * This only needs to be called at startup and *not* during every checkpoint
 * read during recovery (e.g. in HS or PITR from a base backup) afterwards. All
 * state thereafter can be recovered by looking at commit records.
 */
pub unsafe fn StartupReplicationOrigin() {
    let path = PG_REPLORIGIN_CHECKPOINT_FILENAME;
    let fd: c_int;
    let mut readBytes: isize;
    let mut magic: uint32 = REPLICATION_STATE_MAGIC;
    let mut last_state: c_int = 0;
    let file_crc: pg_crc32c;
    let mut crc: pg_crc32c;

    /* don't want to overwrite already existing state */
    #[cfg(debug_assertions)]
    {
        static mut already_started: bool = false;
        Assert!(!already_started);
        already_started = true;
    }

    if max_active_replication_origins == 0 {
        return;
    }

    crc = INIT_CRC32C();

    elog!(DEBUG2, "starting up replication origin progress state");

    {
        let path_c = std::ffi::CString::new(path).unwrap();
        fd = OpenTransientFile(path_c.as_ptr(), O_RDONLY | PG_BINARY);
    }

    /*
     * might have had max_active_replication_origins == 0 last run, or we just
     * brought up a standby.
     */
    if fd < 0 && errno == ENOENT {
        return;
    } else if fd < 0 {
        ereport!(
            PANIC,
            errmsg!("could not open file \"{}\": {}", path, errno)
        );
    }

    /* verify magic, that is written even if nothing was active */
    readBytes = read(
        fd,
        &mut magic as *mut uint32 as *mut c_void,
        size_of::<uint32>(),
    );
    if readBytes != size_of::<uint32>() as isize {
        if readBytes < 0 {
            ereport!(
                PANIC,
                errmsg!("could not read file \"{}\": {}", path, errno)
            );
        } else {
            ereport!(
                PANIC,
                errmsg!(
                    "could not read file \"{}\": read {} of {}",
                    path,
                    readBytes,
                    size_of::<uint32>()
                )
            );
        }
    }
    crc = COMP_CRC32C(
        crc,
        &magic as *const uint32 as *const c_void,
        size_of::<uint32>(),
    );

    if magic != REPLICATION_STATE_MAGIC {
        ereport!(
            PANIC,
            errmsg!(
                "replication checkpoint has wrong magic {} instead of {}",
                magic,
                REPLICATION_STATE_MAGIC
            )
        );
    }

    /* we can skip locking here, no other access is possible */

    /* recover individual states, until there are no more to be found */
    loop {
        let mut disk_state: ReplicationStateOnDisk = core::mem::zeroed();

        readBytes = read(
            fd,
            &mut disk_state as *mut ReplicationStateOnDisk as *mut c_void,
            size_of::<ReplicationStateOnDisk>(),
        );

        /* no further data */
        if readBytes == size_of::<pg_crc32c>() as isize {
            /* not pretty, but simple ... */
            file_crc = *(&disk_state as *const ReplicationStateOnDisk as *const pg_crc32c);
            break;
        }

        if readBytes < 0 {
            ereport!(
                PANIC,
                errmsg!("could not read file \"{}\": {}", path, errno)
            );
        }

        if readBytes != size_of::<ReplicationStateOnDisk>() as isize {
            ereport!(
                PANIC,
                errmsg!(
                    "could not read file \"{}\": read {} of {}",
                    path,
                    readBytes,
                    size_of::<ReplicationStateOnDisk>()
                )
            );
        }

        crc = COMP_CRC32C(
            crc,
            &disk_state as *const ReplicationStateOnDisk as *const c_void,
            size_of::<ReplicationStateOnDisk>(),
        );

        if last_state == max_active_replication_origins {
            ereport!(
                PANIC,
                errmsg!(
                    "could not find free replication state, increase \"max_active_replication_origins\""
                )
            );
        }

        /* copy data to shared memory */
        (*replication_states.offset(last_state as isize)).roident = disk_state.roident;
        (*replication_states.offset(last_state as isize)).remote_lsn = disk_state.remote_lsn;
        last_state += 1;

        let (lsn_hi, lsn_lo) = LSN_FORMAT_ARGS(disk_state.remote_lsn);
        ereport!(
            LOG,
            errmsg!(
                "recovered replication state of node {} to {:X}/{:X}",
                disk_state.roident,
                lsn_hi,
                lsn_lo
            )
        );
    }

    /* now check checksum */
    crc = FIN_CRC32C(crc);
    if file_crc != crc {
        ereport!(
            PANIC,
            errmsg!(
                "replication slot checkpoint has wrong checksum {}, expected {}",
                crc,
                file_crc
            )
        );
    }

    if CloseTransientFile(fd) != 0 {
        ereport!(
            PANIC,
            errmsg!("could not close file \"{}\": {}", path, errno)
        );
    }
}

pub unsafe fn replorigin_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_REPLORIGIN_SET => {
            let xlrec: *mut xl_replorigin_set =
                XLogRecGetData(record) as *mut xl_replorigin_set;

            replorigin_advance(
                (*xlrec).node_id,
                (*xlrec).remote_lsn,
                (*record).EndRecPtr,
                (*xlrec).force,  /* backward */
                false,           /* WAL log */
            );
        }
        XLOG_REPLORIGIN_DROP => {
            let xlrec: *mut xl_replorigin_drop =
                XLogRecGetData(record) as *mut xl_replorigin_drop;
            let mut i: c_int = 0;

            while i < max_active_replication_origins {
                let state: *mut ReplicationState =
                    replication_states.offset(i as isize);

                /* found our slot */
                if (*state).roident == (*xlrec).node_id {
                    /* reset entry */
                    (*state).roident = InvalidRepOriginId;
                    (*state).remote_lsn = InvalidXLogRecPtr;
                    (*state).local_lsn = InvalidXLogRecPtr;
                    break;
                }
                i += 1;
            }
        }
        _ => {
            elog!(PANIC, "replorigin_redo: unknown op code {}", info);
        }
    }
}


/*
 * Tell the replication origin progress machinery that a commit from 'node'
 * that originated at the LSN remote_commit on the remote node was replayed
 * successfully and that we don't need to do so again. In combination with
 * setting up replorigin_session_origin_lsn and replorigin_session_origin
 * that ensures we won't lose knowledge about that after a crash if the
 * transaction had a persistent effect (think of asynchronous commits).
 *
 * local_commit needs to be a local LSN of the commit so that we can make sure
 * upon a checkpoint that enough WAL has been persisted to disk.
 *
 * Needs to be called with a RowExclusiveLock on pg_replication_origin,
 * unless running in recovery.
 */
pub unsafe fn replorigin_advance(
    node: RepOriginId,
    remote_commit: XLogRecPtr,
    local_commit: XLogRecPtr,
    go_backward: bool,
    wal_log: bool,
) {
    let mut i: c_int;
    let mut replication_state: *mut ReplicationState = core::ptr::null_mut();
    let mut free_state: *mut ReplicationState = core::ptr::null_mut();

    Assert!(node != InvalidRepOriginId);

    /* we don't track DoNotReplicateId */
    if node == DoNotReplicateId {
        return;
    }

    /*
     * XXX: For the case where this is called by WAL replay, it'd be more
     * efficient to restore into a backend local hashtable and only dump into
     * shmem after recovery is finished. Let's wait with implementing that
     * till it's shown to be a measurable expense
     */

    /* Lock exclusively, as we may have to create a new table entry. */
    LWLockAcquire(ReplicationOriginLock(), LW_EXCLUSIVE);

    /*
     * Search for either an existing slot for the origin, or a free one we can
     * use.
     */
    i = 0;
    while i < max_active_replication_origins {
        let curstate: *mut ReplicationState = replication_states.offset(i as isize);

        /* remember where to insert if necessary */
        if (*curstate).roident == InvalidRepOriginId && free_state.is_null() {
            free_state = curstate;
            i += 1;
            continue;
        }

        /* not our slot */
        if (*curstate).roident != node {
            i += 1;
            continue;
        }

        /* ok, found slot */
        replication_state = curstate;

        LWLockAcquire(&mut (*replication_state).lock, LW_EXCLUSIVE);

        /* Make sure it's not used by somebody else */
        if (*replication_state).acquired_by != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "replication origin with ID {} is already active for PID {}",
                    (*replication_state).roident,
                    (*replication_state).acquired_by
                )
            );
        }

        break;
    }

    if replication_state.is_null() && free_state.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "could not find free replication state slot for replication origin with ID {}",
                node
            )
        );
    }

    if replication_state.is_null() {
        /* initialize new slot */
        LWLockAcquire(&mut (*free_state).lock, LW_EXCLUSIVE);
        replication_state = free_state;
        Assert!((*replication_state).remote_lsn == InvalidXLogRecPtr);
        Assert!((*replication_state).local_lsn == InvalidXLogRecPtr);
        (*replication_state).roident = node;
    }

    Assert!((*replication_state).roident != InvalidRepOriginId);

    /*
     * If somebody "forcefully" sets this slot, WAL log it, so it's durable
     * and the standby gets the message. Primarily this will be called during
     * WAL replay (of commit records) where no WAL logging is necessary.
     */
    if wal_log {
        let mut xlrec = xl_replorigin_set {
            remote_lsn: remote_commit,
            node_id: node,
            force: go_backward,
        };

        XLogBeginInsert();
        XLogRegisterData(
            &mut xlrec as *mut _ as *const c_void,
            size_of::<xl_replorigin_set>() as u32,
        );

        XLogInsert(RM_REPLORIGIN_ID, XLOG_REPLORIGIN_SET);
    }

    /*
     * Due to - harmless - race conditions during a checkpoint we could see
     * values here that are older than the ones we already have in memory. We
     * could also see older values for prepared transactions when the prepare
     * is sent at a later point of time along with commit prepared and there
     * are other transactions commits between prepare and commit prepared. See
     * ReorderBufferFinishPrepared. Don't overwrite those.
     */
    if go_backward || (*replication_state).remote_lsn < remote_commit {
        (*replication_state).remote_lsn = remote_commit;
    }
    if local_commit != InvalidXLogRecPtr
        && (go_backward || (*replication_state).local_lsn < local_commit)
    {
        (*replication_state).local_lsn = local_commit;
    }
    LWLockRelease(&mut (*replication_state).lock);

    /*
     * Release *after* changing the LSNs, slot isn't acquired and thus could
     * otherwise be dropped anytime.
     */
    LWLockRelease(ReplicationOriginLock());
}


pub unsafe fn replorigin_get_progress(node: RepOriginId, flush: bool) -> XLogRecPtr {
    let mut i: c_int;
    let mut local_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut remote_lsn: XLogRecPtr = InvalidXLogRecPtr;

    /* prevent slots from being concurrently dropped */
    LWLockAcquire(ReplicationOriginLock(), LW_SHARED);

    i = 0;
    while i < max_active_replication_origins {
        let state: *mut ReplicationState = replication_states.offset(i as isize);

        if (*state).roident == node {
            LWLockAcquire(&mut (*state).lock, LW_SHARED);

            remote_lsn = (*state).remote_lsn;
            local_lsn = (*state).local_lsn;

            LWLockRelease(&mut (*state).lock);

            break;
        }
        i += 1;
    }

    LWLockRelease(ReplicationOriginLock());

    if flush && local_lsn != InvalidXLogRecPtr {
        XLogFlush(local_lsn);
    }

    remote_lsn
}

/*
 * Tear down a (possibly) configured session replication origin during process
 * exit.
 */
unsafe extern "C" fn ReplicationOriginExitCleanup(code: c_int, arg: Datum) {
    let mut cv: *mut ConditionVariable = core::ptr::null_mut();

    if session_replication_state.is_null() {
        return;
    }

    LWLockAcquire(ReplicationOriginLock(), LW_EXCLUSIVE);

    if (*session_replication_state).acquired_by == MyProcPid {
        cv = &mut (*session_replication_state).origin_cv;

        (*session_replication_state).acquired_by = 0;
        session_replication_state = core::ptr::null_mut();
    }

    LWLockRelease(ReplicationOriginLock());

    if !cv.is_null() {
        ConditionVariableBroadcast(cv);
    }
}

/*
 * Setup a replication origin in the shared memory struct if it doesn't
 * already exist and cache access to the specific ReplicationSlot so the
 * array doesn't have to be searched when calling
 * replorigin_session_advance().
 *
 * Normally only one such cached origin can exist per process so the cached
 * value can only be set again after the previous value is torn down with
 * replorigin_session_reset(). For this normal case pass acquired_by = 0
 * (meaning the slot is not allowed to be already acquired by another process).
 *
 * However, sometimes multiple processes can safely re-use the same origin slot
 * (for example, multiple parallel apply processes can safely use the same
 * origin, provided they maintain commit order by allowing only one process to
 * commit at a time). For this case the first process must pass acquired_by =
 * 0, and then the other processes sharing that same origin can pass
 * acquired_by = PID of the first process.
 */
#[no_mangle]
pub unsafe fn replorigin_session_setup(node: RepOriginId, acquired_by: c_int) {
    static mut registered_cleanup: bool = false;
    let mut i: c_int;
    let mut free_slot: c_int = -1;

    if !registered_cleanup {
        on_shmem_exit(ReplicationOriginExitCleanup, 0);
        registered_cleanup = true;
    }

    Assert!(max_active_replication_origins > 0);

    if !session_replication_state.is_null() {
        ereport!(
            ERROR,
            errmsg!("cannot setup replication origin when one is already setup")
        );
    }

    /* Lock exclusively, as we may have to create a new table entry. */
    LWLockAcquire(ReplicationOriginLock(), LW_EXCLUSIVE);

    /*
     * Search for either an existing slot for the origin, or a free one we can
     * use.
     */
    i = 0;
    while i < max_active_replication_origins {
        let curstate: *mut ReplicationState = replication_states.offset(i as isize);

        /* remember where to insert if necessary */
        if (*curstate).roident == InvalidRepOriginId && free_slot == -1 {
            free_slot = i;
            i += 1;
            continue;
        }

        /* not our slot */
        if (*curstate).roident != node {
            i += 1;
            continue;
        }

        if (*curstate).acquired_by != 0 && acquired_by == 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "replication origin with ID {} is already active for PID {}",
                    (*curstate).roident,
                    (*curstate).acquired_by
                )
            );
        }

        /* ok, found slot */
        session_replication_state = curstate;
        break;
    }

    if session_replication_state.is_null() && free_slot == -1 {
        ereport!(
            ERROR,
            errmsg!(
                "could not find free replication state slot for replication origin with ID {}",
                node
            )
        );
    } else if session_replication_state.is_null() {
        /* initialize new slot */
        session_replication_state = replication_states.offset(free_slot as isize);
        Assert!((*session_replication_state).remote_lsn == InvalidXLogRecPtr);
        Assert!((*session_replication_state).local_lsn == InvalidXLogRecPtr);
        (*session_replication_state).roident = node;
    }

    Assert!((*session_replication_state).roident != InvalidRepOriginId);

    if acquired_by == 0 {
        (*session_replication_state).acquired_by = MyProcPid;
    } else if (*session_replication_state).acquired_by != acquired_by {
        elog!(
            ERROR,
            "could not find replication state slot for replication origin with OID {} which was acquired by {}",
            node,
            acquired_by
        );
    }

    LWLockRelease(ReplicationOriginLock());

    /* probably this one is pointless */
    ConditionVariableBroadcast(&mut (*session_replication_state).origin_cv);
}

/*
 * Reset replay state previously setup in this session.
 *
 * This function may only be called if an origin was setup with
 * replorigin_session_setup().
 */
pub unsafe fn replorigin_session_reset() {
    let cv: *mut ConditionVariable;

    Assert!(max_active_replication_origins != 0);

    if session_replication_state.is_null() {
        ereport!(
            ERROR,
            errmsg!("no replication origin is configured")
        );
    }

    LWLockAcquire(ReplicationOriginLock(), LW_EXCLUSIVE);

    (*session_replication_state).acquired_by = 0;
    cv = &mut (*session_replication_state).origin_cv;
    session_replication_state = core::ptr::null_mut();

    LWLockRelease(ReplicationOriginLock());

    ConditionVariableBroadcast(cv);
}

/*
 * Do the same work replorigin_advance() does, just on the session's
 * configured origin.
 *
 * This is noticeably cheaper than using replorigin_advance().
 */
pub unsafe fn replorigin_session_advance(remote_commit: XLogRecPtr, local_commit: XLogRecPtr) {
    Assert!(!session_replication_state.is_null());
    Assert!((*session_replication_state).roident != InvalidRepOriginId);

    LWLockAcquire(&mut (*session_replication_state).lock, LW_EXCLUSIVE);
    if (*session_replication_state).local_lsn < local_commit {
        (*session_replication_state).local_lsn = local_commit;
    }
    if (*session_replication_state).remote_lsn < remote_commit {
        (*session_replication_state).remote_lsn = remote_commit;
    }
    LWLockRelease(&mut (*session_replication_state).lock);
}

/*
 * Ask the machinery about the point up to which we successfully replayed
 * changes from an already setup replication origin.
 */
#[no_mangle]
pub unsafe fn replorigin_session_get_progress(flush: bool) -> XLogRecPtr {
    let remote_lsn: XLogRecPtr;
    let local_lsn: XLogRecPtr;

    Assert!(!session_replication_state.is_null());

    LWLockAcquire(&mut (*session_replication_state).lock, LW_SHARED);
    remote_lsn = (*session_replication_state).remote_lsn;
    local_lsn = (*session_replication_state).local_lsn;
    LWLockRelease(&mut (*session_replication_state).lock);

    if flush && local_lsn != InvalidXLogRecPtr {
        XLogFlush(local_lsn);
    }

    remote_lsn
}



/* ---------------------------------------------------------------------------
 * SQL functions for working with replication origin.
 *
 * These mostly should be fairly short wrappers around more generic functions.
 * ---------------------------------------------------------------------------
 */

/*
 * Create replication origin for the passed in name, and return the assigned
 * oid.
 */
pub unsafe fn pg_replication_origin_create(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char;
    let roident: RepOriginId;

    replorigin_check_prerequisites(false, false);

    name = text_to_cstring(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *const crate::c::varlena);

    /*
     * Replication origins "any" and "none" are reserved for system options.
     * The origins "pg_xxx" are reserved for internal use.
     */
    if IsReservedName(name) || IsReservedOriginName(name) {
        ereport!(
            ERROR,
            errmsg!(
                "replication origin name {:?} is reserved",
                name
            )
        );
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for replication origin names are violated.
     */
    /* ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS is normally undefined; gated check omitted: */
#[cfg(any())]
    {
        use std::ffi::CStr;
        if !CStr::from_ptr(name).to_bytes().starts_with(b"regress_") {
            elog!(
                WARNING,
                "replication origins created by regression test cases should have names starting with \"regress_\""
            );
        }
    }

    roident = replorigin_create(name);

    pfree(name as *mut c_void);

    roident as Datum
}

/*
 * Drop replication origin.
 */
pub unsafe fn pg_replication_origin_drop(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char;

    replorigin_check_prerequisites(false, false);

    name = text_to_cstring(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *const crate::c::varlena);

    replorigin_drop_by_name(name, false, true);

    pfree(name as *mut c_void);

    PG_RETURN_VOID!()
}

/*
 * Return oid of a replication origin.
 */
pub unsafe fn pg_replication_origin_oid(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char;
    let roident: RepOriginId;

    replorigin_check_prerequisites(false, false);

    name = text_to_cstring(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *const crate::c::varlena);
    roident = replorigin_by_name(name, true);

    pfree(name as *mut c_void);

    if crate::c::OidIsValid(roident as Oid) {
        return roident as Datum;
    }
    PG_RETURN_NULL!(fcinfo)
}

/*
 * Setup a replication origin for this session.
 */
pub unsafe fn pg_replication_origin_session_setup(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char;
    let origin: RepOriginId;

    replorigin_check_prerequisites(true, false);

    name = text_to_cstring(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *const crate::c::varlena);
    origin = replorigin_by_name(name, false);
    replorigin_session_setup(origin, 0);

    replorigin_session_origin = origin;

    pfree(name as *mut c_void);

    PG_RETURN_VOID!()
}

/*
 * Reset previously setup origin in this session
 */
pub unsafe fn pg_replication_origin_session_reset(fcinfo: FunctionCallInfo) -> Datum {
    replorigin_check_prerequisites(true, false);

    replorigin_session_reset();

    replorigin_session_origin = InvalidRepOriginId;
    replorigin_session_origin_lsn = InvalidXLogRecPtr;
    replorigin_session_origin_timestamp = 0;

    PG_RETURN_VOID!()
}

/*
 * Has a replication origin been setup for this session.
 */
pub unsafe fn pg_replication_origin_session_is_setup(fcinfo: FunctionCallInfo) -> Datum {
    replorigin_check_prerequisites(false, false);

    (replorigin_session_origin != InvalidRepOriginId) as Datum
}


/*
 * Return the replication progress for origin setup in the current session.
 *
 * If 'flush' is set to true it is ensured that the returned value corresponds
 * to a local transaction that has been flushed. This is useful if asynchronous
 * commits are used when replaying replicated transactions.
 */
pub unsafe fn pg_replication_origin_session_progress(fcinfo: FunctionCallInfo) -> Datum {
    let mut remote_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let flush: bool = PG_GETARG_BOOL!(fcinfo, 0);

    replorigin_check_prerequisites(true, false);

    if session_replication_state.is_null() {
        ereport!(
            ERROR,
            errmsg!("no replication origin is configured")
        );
    }

    remote_lsn = replorigin_session_get_progress(flush);

    if remote_lsn == InvalidXLogRecPtr {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_LSN!(remote_lsn)
}

pub unsafe fn pg_replication_origin_xact_setup(fcinfo: FunctionCallInfo) -> Datum {
    let location: XLogRecPtr = PG_GETARG_LSN!(fcinfo, 0);

    replorigin_check_prerequisites(true, false);

    if session_replication_state.is_null() {
        ereport!(
            ERROR,
            errmsg!("no replication origin is configured")
        );
    }

    replorigin_session_origin_lsn = location;
    // PG_GETARG_TIMESTAMPTZ(1)
    replorigin_session_origin_timestamp = PG_GETARG_DATUM!(fcinfo, 1) as TimestampTz;

    PG_RETURN_VOID!()
}

pub unsafe fn pg_replication_origin_xact_reset(fcinfo: FunctionCallInfo) -> Datum {
    replorigin_check_prerequisites(true, false);

    replorigin_session_origin_lsn = InvalidXLogRecPtr;
    replorigin_session_origin_timestamp = 0;

    PG_RETURN_VOID!()
}


pub unsafe fn pg_replication_origin_advance(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_void = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut c_void;
    let remote_commit: XLogRecPtr = PG_GETARG_LSN!(fcinfo, 1);
    let node: RepOriginId;

    replorigin_check_prerequisites(true, false);

    /* lock to prevent the replication origin from vanishing */
    LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    node = replorigin_by_name(text_to_cstring(name as *const crate::c::varlena), false);

    /*
     * Can't sensibly pass a local commit to be flushed at checkpoint - this
     * xact hasn't committed yet. This is why this function should be used to
     * set up the initial replication state, but not for replay.
     */
    replorigin_advance(
        node,
        remote_commit,
        InvalidXLogRecPtr,
        true,  /* go backward */
        true,  /* WAL log */
    );

    UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    PG_RETURN_VOID!()
}


/*
 * Return the replication progress for an individual replication origin.
 *
 * If 'flush' is set to true it is ensured that the returned value corresponds
 * to a local transaction that has been flushed. This is useful if asynchronous
 * commits are used when replaying replicated transactions.
 */
pub unsafe fn pg_replication_origin_progress(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char;
    let flush: bool;
    let roident: RepOriginId;
    let mut remote_lsn: XLogRecPtr = InvalidXLogRecPtr;

    replorigin_check_prerequisites(true, true);

    name = text_to_cstring(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *const crate::c::varlena);
    flush = PG_GETARG_BOOL!(fcinfo, 1);

    roident = replorigin_by_name(name, false);
    Assert!(crate::c::OidIsValid(roident as Oid));

    remote_lsn = replorigin_get_progress(roident, flush);

    if remote_lsn == InvalidXLogRecPtr {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_LSN!(remote_lsn)
}


pub unsafe fn pg_show_replication_origin_status(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut i: c_int;
    const REPLICATION_ORIGIN_PROGRESS_COLS: usize = 4;

    /* we want to return 0 rows if slot is set to zero */
    replorigin_check_prerequisites(false, true);

    InitMaterializedSRF(fcinfo, 0);

    /* prevent slots from being concurrently dropped */
    LWLockAcquire(ReplicationOriginLock(), LW_SHARED);

    /*
     * Iterate through all possible replication_states, display if they are
     * filled. Note that we do not take any locks, so slightly corrupted/out
     * of date values are a possibility.
     */
    i = 0;
    while i < max_active_replication_origins {
        let state: *mut ReplicationState = replication_states.offset(i as isize);
        let mut values: [Datum; REPLICATION_ORIGIN_PROGRESS_COLS] =
            [0; REPLICATION_ORIGIN_PROGRESS_COLS];
        let mut nulls: [bool; REPLICATION_ORIGIN_PROGRESS_COLS] =
            [true; REPLICATION_ORIGIN_PROGRESS_COLS];
        let mut roname: *mut c_char = core::ptr::null_mut();

        /* unused slot, nothing to display */
        if (*state).roident == InvalidRepOriginId {
            i += 1;
            continue;
        }

        for v in values.iter_mut() {
            *v = 0;
        }
        for n in nulls.iter_mut() {
            *n = true;
        }

        values[0] = ObjectIdGetDatum((*state).roident as Oid);
        nulls[0] = false;

        /*
         * We're not preventing the origin to be dropped concurrently, so
         * silently accept that it might be gone.
         */
        if replorigin_by_oid((*state).roident, true, &mut roname) {
            values[1] = CStringGetTextDatum(roname);
            nulls[1] = false;
        }

        LWLockAcquire(&mut (*state).lock, LW_SHARED);

        values[2] = LSNGetDatum((*state).remote_lsn);
        nulls[2] = false;

        values[3] = LSNGetDatum((*state).local_lsn);
        nulls[3] = false;

        LWLockRelease(&mut (*state).lock);

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc as *mut c_void,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        i += 1;
    }

    LWLockRelease(ReplicationOriginLock());

    0 as Datum
}

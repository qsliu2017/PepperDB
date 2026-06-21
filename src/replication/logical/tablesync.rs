/*-------------------------------------------------------------------------
 * tablesync.rs
 *   PostgreSQL logical replication: initial table data synchronization
 *
 * Copyright (c) 2012-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/backend/replication/logical/tablesync.c
 *
 * NOTES
 *   This file contains code for initial table data synchronization for
 *   logical replication.
 *
 *   The initial data synchronization is done separately for each table,
 *   in a separate apply worker that only fetches the initial snapshot data
 *   from the publisher and then synchronizes the position in the stream with
 *   the leader apply worker.
 *
 *   There are several reasons for doing the synchronization this way:
 *    - It allows us to parallelize the initial data synchronization
 *      which lowers the time needed for it to happen.
 *    - The initial synchronization does not have to hold the xid and LSN
 *      for the time it takes to copy data of all tables, causing less
 *      bloat and lower disk consumption compared to doing the
 *      synchronization in a single process for the whole database.
 *    - It allows us to synchronize any tables added after the initial
 *      synchronization has finished.
 *
 *   The stream position synchronization works in multiple steps:
 *    - Apply worker requests a tablesync worker to start, setting the new
 *      table state to INIT.
 *    - Tablesync worker starts; changes table state from INIT to DATASYNC while
 *      copying.
 *    - Tablesync worker does initial table copy; there is a FINISHEDCOPY (sync
 *      worker specific) state to indicate when the copy phase has completed, so
 *      if the worker crashes with this (non-memory) state then the copy will not
 *      be re-attempted.
 *    - Tablesync worker then sets table state to SYNCWAIT; waits for state change.
 *    - Apply worker periodically checks for tables in SYNCWAIT state.  When
 *      any appear, it sets the table state to CATCHUP and starts loop-waiting
 *      until either the table state is set to SYNCDONE or the sync worker
 *      exits.
 *    - After the sync worker has seen the state change to CATCHUP, it will
 *      read the stream and apply changes (acting like an apply worker) until
 *      it catches up to the specified stream position.  Then it sets the
 *      state to SYNCDONE.  There might be zero changes applied between
 *      CATCHUP and SYNCDONE, because the sync worker might be ahead of the
 *      apply worker.
 *    - Once the state is set to SYNCDONE, the apply will continue tracking
 *      the table until it reaches the SYNCDONE stream position, at which
 *      point it sets state to READY and stops tracking.  Again, there might
 *      be zero changes in between.
 *
 *   So the state progression is always: INIT -> DATASYNC -> FINISHEDCOPY
 *   -> SYNCWAIT -> CATCHUP -> SYNCDONE -> READY.
 *
 *   The catalog pg_subscription_rel is used to keep information about
 *   subscribed tables and their state.  The catalog holds all states
 *   except SYNCWAIT and CATCHUP which are only in shared memory.
 *
 *   Example flows look like this:
 *    - Apply is in front:
 *       sync:8
 *         -> set in catalog FINISHEDCOPY
 *         -> set in memory SYNCWAIT
 *       apply:10
 *         -> set in memory CATCHUP
 *         -> enter wait-loop
 *       sync:10
 *         -> set in catalog SYNCDONE
 *         -> exit
 *       apply:10
 *         -> exit wait-loop
 *         -> continue rep
 *       apply:11
 *         -> set in catalog READY
 *
 *    - Sync is in front:
 *       sync:10
 *         -> set in catalog FINISHEDCOPY
 *         -> set in memory SYNCWAIT
 *       apply:8
 *         -> set in memory CATCHUP
 *         -> continue per-table filtering
 *       sync:10
 *         -> set in catalog SYNCDONE
 *         -> exit
 *       apply:10
 *         -> set in catalog READY
 *         -> stop per-table filtering
 *         -> continue rep
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use std::ptr::{null, null_mut};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::catalog::pg_subscription_rel::{
    SUBREL_STATE_CATCHUP, SUBREL_STATE_DATASYNC, SUBREL_STATE_FINISHEDCOPY,
    SUBREL_STATE_INIT, SUBREL_STATE_READY, SUBREL_STATE_SYNCDONE,
    SUBREL_STATE_SYNCWAIT, SUBREL_STATE_UNKNOWN,
};
use crate::catalog::pg_subscription::{
    LOGICALREP_TWOPHASE_STATE_DISABLED, LOGICALREP_TWOPHASE_STATE_ENABLED,
    LOGICALREP_TWOPHASE_STATE_PENDING,
};
use crate::c::{uint32, Size};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::replication::logical::launcher::ApplyLauncherForgetWorkerStartTime;
use crate::replication::logical::origin::{
    replorigin_advance, replorigin_by_name, replorigin_create, replorigin_drop_by_name,
    replorigin_session_get_progress, replorigin_session_reset, replorigin_session_setup,
};
use crate::replication::logicalrelation::{
    logicalrep_rel_close, logicalrep_rel_open, logicalrep_relmap_update,
    LogicalRepRelMapEntry, LogicalRepRelation,
};
use crate::replication::libpqwalreceiver::libpqwalreceiver::{
    CRS_USE_SNAPSHOT, WALRCV_OK_COMMAND, WALRCV_OK_COPY_OUT, WALRCV_OK_TUPLES,
    WalRcvExecResult, WalRcvStreamOptions, WalReceiverConn,
};
use crate::replication::walreceiver::{
    walrcv_connect, walrcv_create_slot, walrcv_endstreaming, walrcv_receive,
    walrcv_startstreaming, GetSystemIdentifier,
};
use crate::replication::worker_internal::{
    am_tablesync_worker, logicalrep_sync_worker_count, logicalrep_worker_find,
    logicalrep_worker_launch, logicalrep_worker_wakeup, logicalrep_worker_wakeup_ptr,
    set_apply_error_context_origin, set_stream_options, start_apply,
    ApplyContext, DisableSubscriptionAndExit, LogRepWorkerWalRcvConn,
    LogicalRepWorker, MyLogicalRepWorker, MySubscription, ReplicationOriginNameForLogicalRep,
    SetupApplyOrSyncWorker, TimestampTz, WORKERTYPE_APPLY, WORKERTYPE_PARALLEL_APPLY,
    WORKERTYPE_TABLESYNC, WORKERTYPE_UNKNOWN,
};
use crate::access::transam::xlogreader::{InvalidRepOriginId, RepOriginId};
use crate::access::transam::twophase::{
    replorigin_session_origin, replorigin_session_origin_lsn,
    replorigin_session_origin_timestamp,
};
use crate::storage::ipc::latch::{
    ResetLatch, WaitLatch, WaitLatchOrSocket, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET,
    WL_SOCKET_READABLE, WL_TIMEOUT,
};
use crate::nodes::pg_list::{List, NIL};
use crate::utils::mmgr::memnodes::MemoryContext;

// ---------------------------------------------------------------------------
// Local stubs for types/functions not yet ported.  Each carries TODO(pg-port).
// ---------------------------------------------------------------------------

/// catalog/pg_subscription.h - TODO(pg-port): real Subscription struct not yet ported.
#[repr(C)]
pub struct Subscription {
    pub oid: Oid,
    pub name: *const c_char,
    pub conninfo: *const c_char,
    pub publications: *mut List,
    pub binary: bool,
    pub twophasestate: c_char,
    pub disableonerr: bool,
    pub passwordrequired: bool,
    pub ownersuperuser: bool,
    pub runasowner: bool,
    pub failover: bool,
}

/// catalog/pg_subscription_rel.h SubscriptionRelState -
/// TODO(pg-port): real struct not yet ported.
#[repr(C)]
pub struct SubscriptionRelState {
    pub relid: Oid,
    pub state: c_char,
    pub lsn: XLogRecPtr,
}

// pg_subscription.h column number constants (1-based) -
// TODO(pg-port): real catalog/pg_subscription.h not yet ported.
const Natts_pg_subscription: usize = 32;
const Anum_pg_subscription_subtwophasestate: usize = 20; // approximate

// OID constants - TODO(pg-port): real catalog OIDs from pg_class.h.
const SubscriptionRelationId: Oid = 6100;
const SubscriptionRelRelationId: Oid = 6101;
const ReplicationOriginRelationId: Oid = 6102;

// Lock modes - TODO(pg-port): real values from storage/lockdefs.h.
const NoLock: c_int = 0;
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 5;

// Relation kind constants (pg_class.h).
const RELKIND_RELATION: c_char = b'r' as c_char;

// MaxTupleAttributeNumber - TODO(pg-port): access/tupdesc.h.
const MaxTupleAttributeNumber: usize = 1664;

// NAMEDATALEN.
const NAMEDATALEN: usize = 64;

// pgsocket type - port/noblock.h.
type pgsocket = c_int;

// TupleTableSlot / TupleDesc stubs - TODO(pg-port): nodes/execnodes.h.
type TupleTableSlot = c_void;
type TupleDesc = c_void;
type TTSOpsVTable = c_void;

// ParseState stub - TODO(pg-port): parser/parse_node.h.
type ParseState = c_void;

// CopyFromState stub - TODO(pg-port): commands/copy.h.
type CopyFromState = c_void;

// Node/DefElem stubs - TODO(pg-port): nodes/parsenodes.h.
type Node = c_void;
type DefElem = c_void;

// Bitmapset stub - TODO(pg-port): nodes/bitmapset.h.
type Bitmapset = c_void;

// ArrayType stub - TODO(pg-port): utils/array.h.
type ArrayType = c_void;

// HeapTuple / Relation stubs - TODO(pg-port).
type HeapTuple = *mut c_void;
type Relation = *mut c_void;

// UserContext stub - TODO(pg-port): utils/acl.h / utils/usercontext.h.
type UserContext = c_void;

// AclResult stub - TODO(pg-port): utils/acl.h.
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;

// TimeLineID.
type TimeLineID = uint32;

// AttrNumber.
type AttrNumber = i16;

// Wait event constants - TODO(pg-port): utils/activity/wait_event_names.h.
const WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE: uint32 = 0;
const WAIT_EVENT_LOGICAL_SYNC_DATA: uint32 = 0;

// UINT64_FORMAT equivalent handled inline via format string.

// DSM_HANDLE_INVALID - storage/dsm_impl.h.
const DSM_HANDLE_INVALID: uint32 = 0;

// max_sync_workers_per_subscription GUC.
// TODO(pg-port): real GUC declared in replication/replication.c.
static mut max_sync_workers_per_subscription: c_int = 2;

// LWLock constants.
const LW_SHARED: c_int = 1;
const LW_EXCLUSIVE: c_int = 2;

// Syscache IDs - TODO(pg-port): utils/syscache.h.
const SUBSCRIPTIONOID: c_int = 67;

// int2 oid, text oid, etc. - TODO(pg-port): catalog/pg_type.h.
const OIDOID: Oid = 26;
const CHAROID: Oid = 18;
const INT2OID: Oid = 21;
const TEXTOID: Oid = 25;
const BOOLOID: Oid = 16;
const INT2VECTOROID: Oid = 22;

// wal_retrieve_retry_interval GUC (milliseconds).
// TODO(pg-port): real GUC from replication/walreceiver.c.
static mut wal_retrieve_retry_interval: c_int = 5000;

// MyProc - storage/proc.h.
// TODO(pg-port): declared in storage/proc.c.
// (plain Rust statics - extern FFI not needed for stubs; real homes: proc.c, globals.c, mcxt.c)
extern "C" { pub static mut MyProc: *mut c_void; }
static mut MyLatch: *mut c_void = std::ptr::null_mut();
static mut CacheMemoryContext: MemoryContext = std::ptr::null_mut();
use crate::backend_link_shims::LogicalRepWorkerLock; // canonical runtime-assigned global

// ---------------------------------------------------------------------------
// Local stub: HTAB hash table.  Re-uses the xlogutils pattern.
// TODO(pg-port): real dynahash.h types.
// ---------------------------------------------------------------------------
type HTAB = c_void;

#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
}

const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0010;
const HASH_ENTER: c_int = 1;

// ---------------------------------------------------------------------------
// Local stub functions not yet ported.
// ---------------------------------------------------------------------------

// commands/copy.h - TODO(pg-port).
unsafe fn BeginCopyFrom(
    _pstate: *mut ParseState,
    _rel: Relation,
    _where_clause: *mut c_void,
    _filename: *const c_char,
    _is_program: bool,
    _data_source_cb: unsafe fn(*mut c_void, c_int, c_int) -> c_int,
    _attnamelist: *mut List,
    _options: *mut List,
) -> CopyFromState {
    unimplemented!() // TODO(pg-port): BeginCopyFrom not yet ported
}

unsafe fn CopyFrom(cstate: CopyFromState) -> u64 { unimplemented!() }

// parser/parse_relation.h - TODO(pg-port).
unsafe fn addRangeTableEntryForRelation(
    pstate: *mut ParseState,
    rel: Relation,
    lockmode: c_int,
    alias: *mut c_void,
    inh: bool,
    inFromCl: bool,
) -> *mut c_void { unimplemented!() }

unsafe fn make_parsestate(parent: *mut ParseState) -> *mut ParseState { unimplemented!() }

// nodes/makefuncs.h - TODO(pg-port).
unsafe fn makeString(_str: *mut c_char) -> *mut c_void {
    unimplemented!() // TODO(pg-port): makeString not yet ported
}

unsafe fn makeDefElem(
    _name: *const c_char,
    _arg: *mut Node,
    _location: c_int,
) -> *mut DefElem {
    unimplemented!() // TODO(pg-port): makeDefElem not yet ported
}

// utils/lsyscache.h - TODO(pg-port).
unsafe fn get_rel_name(relid: Oid) -> *const c_char { crate::utils::cache::lsyscache::get_rel_name(relid as _) as _ }

unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_namespace_name(nspid as _) }

// utils/acl.h - TODO(pg-port).
unsafe fn pg_class_aclcheck(reloid: Oid, roleid: Oid, mode: c_int) -> AclResult { unimplemented!() }

unsafe fn aclcheck_error(result: AclResult, objtype: c_int, objectname: *const c_char) { unimplemented!() }

unsafe fn get_relkind_objtype(_relkind: c_char) -> c_int {
    unimplemented!() // TODO(pg-port): get_relkind_objtype not yet ported
}

// utils/rls.h - TODO(pg-port).
const RLS_ENABLED: c_int = 2;

unsafe fn check_enable_rls(reloid: Oid, checkAsUserId: Oid, noError: bool) -> c_int { crate::utils::misc::rls::check_enable_rls(reloid as _, checkAsUserId as _, noError) }

// utils/snapmgr.h - TODO(pg-port).
unsafe fn PushActiveSnapshot(snap: *mut c_void) { crate::utils::time::snapmgr::PushActiveSnapshot(snap as _) }

unsafe fn PopActiveSnapshot() { crate::utils::time::snapmgr::PopActiveSnapshot() }

unsafe fn GetTransactionSnapshot() -> *mut c_void {
    unimplemented!() // TODO(pg-port): GetTransactionSnapshot not yet ported
}

// utils/usercontext.h - TODO(pg-port).
unsafe fn SwitchToUntrustedUser(roleid: Oid, ucxt: *mut UserContext) { crate::utils::init::usercontext::SwitchToUntrustedUser(roleid as _, ucxt as _) }

unsafe fn RestoreUserContext(ucxt: *mut UserContext) { crate::utils::init::usercontext::RestoreUserContext(ucxt as _) }

// utils/memutils.h - TODO(pg-port).
unsafe fn MemoryContextSwitchTo(_cxt: MemoryContext) -> MemoryContext {
    unimplemented!() // TODO(pg-port): MemoryContextSwitchTo not yet ported
}

unsafe fn MemoryContextStrdup(_cxt: MemoryContext, _s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): MemoryContextStrdup not yet ported
}

// miscadmin.h - TODO(pg-port).
unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO(pg-port): IsTransactionState not yet ported
}

unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }

unsafe fn GetUserNameFromId(_roleid: Oid, _noerr: bool) -> *const c_char {
    unimplemented!() // TODO(pg-port): GetUserNameFromId not yet ported
}

// access/xact.h - TODO(pg-port).
unsafe fn CommitTransactionCommand() { crate::access::transam::xact::CommitTransactionCommand() }

unsafe fn StartTransactionCommand() { crate::access::transam::xact::StartTransactionCommand() }

unsafe fn AbortOutOfAnyTransaction() { crate::access::transam::xact::AbortOutOfAnyTransaction() }

unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO(pg-port): CommandCounterIncrement not yet ported
}

// access/table.h - TODO(pg-port).
unsafe fn table_open(_reloid: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO(pg-port): table_open not yet ported
}

unsafe fn table_close(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): table_close not yet ported
}

// storage/lmgr.h - TODO(pg-port).
unsafe fn LockSharedObject(
    _classid: Oid,
    _objoid: Oid,
    _objsubid: uint32,
    _lockmode: c_int,
) {
    unimplemented!() // TODO(pg-port): LockSharedObject not yet ported
}

unsafe fn LockRelationOid(_reloid: Oid, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): LockRelationOid not yet ported
}

unsafe fn UnlockRelationOid(_reloid: Oid, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): UnlockRelationOid not yet ported
}

// storage/lwlock.h - TODO(pg-port).
unsafe fn LWLockAcquire(_lock: *mut c_void, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): LWLockAcquire not yet ported
}

unsafe fn LWLockRelease(_lock: *mut c_void) {
    unimplemented!() // TODO(pg-port): LWLockRelease not yet ported
}

// storage/s_lock.h - TODO(pg-port): SpinLockAcquire/Release expand to macros.
unsafe fn SpinLockAcquire(_lock: *mut c_void) {
    unimplemented!() // TODO(pg-port): SpinLockAcquire not yet ported
}

unsafe fn SpinLockRelease(_lock: *mut c_void) {
    unimplemented!() // TODO(pg-port): SpinLockRelease not yet ported
}

// access/xlog.h - TODO(pg-port).
unsafe fn XLogFlush(lsn: XLogRecPtr) { crate::access::transam::xlog::XLogFlush(lsn as _) }

unsafe fn GetXLogWriteRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetXLogWriteRecPtr() }

// pgstat.h - TODO(pg-port).
unsafe fn pgstat_report_stat(force: bool) { crate::utils::activity::pgstat::pgstat_report_stat(force); }

unsafe fn pgstat_report_subscription_error(suboid: Oid, isexit: bool) { crate::utils::activity::pgstat_subscription::pgstat_report_subscription_error(suboid as _, isexit) }

// catalog/indexing.h - TODO(pg-port).
unsafe fn CatalogTupleUpdate(
    heapRel: Relation,
    otid: *mut c_void,
    tup: HeapTuple,
) { unimplemented!() }

// utils/syscache.h - TODO(pg-port).
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple { unimplemented!() }

// utils/rel.h - TODO(pg-port).
unsafe fn RelationGetRelid(_rel: Relation) -> Oid {
    unimplemented!() // TODO(pg-port): RelationGetRelid not yet ported
}

unsafe fn RelationGetNamespace(_rel: Relation) -> Oid {
    unimplemented!() // TODO(pg-port): RelationGetNamespace not yet ported
}

unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char {
    unimplemented!() // TODO(pg-port): RelationGetRelationName not yet ported
}

unsafe fn RelationGetDescr(_rel: Relation) -> *mut TupleDesc {
    unimplemented!() // TODO(pg-port): RelationGetDescr not yet ported
}

// utils/builtins.h - TODO(pg-port).
unsafe fn quote_literal_cstr(_rawstr: *const c_char) -> *const c_char {
    unimplemented!() // TODO(pg-port): quote_literal_cstr not yet ported
}

unsafe fn quote_identifier(_ident: *const c_char) -> *const c_char {
    unimplemented!() // TODO(pg-port): quote_identifier not yet ported
}

unsafe fn quote_qualified_identifier(
    _qualifier: *const c_char,
    _ident: *const c_char,
) -> *const c_char {
    unimplemented!() // TODO(pg-port): quote_qualified_identifier not yet ported
}

// utils/array.h - TODO(pg-port).
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): DatumGetArrayTypeP not yet ported
}

unsafe fn ARR_DIMS(_arr: *mut ArrayType) -> *mut c_int {
    unimplemented!() // TODO(pg-port): ARR_DIMS not yet ported
}

unsafe fn ARR_DATA_PTR(arr: *mut ArrayType) -> *mut c_void { crate::utils::array::ARR_DATA_PTR(arr as _) as _ }

// nodes/bitmapset.h - TODO(pg-port).
unsafe fn bms_add_member(_a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset {
    unimplemented!() // TODO(pg-port): bms_add_member not yet ported
}

unsafe fn bms_is_member(_x: c_int, _a: *const Bitmapset) -> bool {
    unimplemented!() // TODO(pg-port): bms_is_member not yet ported
}

// catalog/pg_subscription_rel.h - TODO(pg-port): subscription relation catalog access.
unsafe fn GetSubscriptionRelState(
    _subid: Oid,
    _relid: Oid,
    _lsn: *mut XLogRecPtr,
) -> c_char {
    unimplemented!() // TODO(pg-port): GetSubscriptionRelState not yet ported
}

unsafe fn UpdateSubscriptionRelState(
    subid: Oid,
    relid: Oid,
    state: c_char,
    sublsn: XLogRecPtr,
    acquire_lock: bool,
) { crate::catalog::pg_subscription::UpdateSubscriptionRelState(subid as _, relid as _, state as _, sublsn as _, acquire_lock) }

unsafe fn GetSubscriptionRelations(subid: Oid, not_ready: bool) -> *mut List { crate::catalog::pg_subscription::GetSubscriptionRelations(subid as _, not_ready) }

unsafe fn HasSubscriptionRelations(subid: Oid) -> bool { crate::catalog::pg_subscription::HasSubscriptionRelations(subid as _) }

// catalog/pg_publication.h - TODO(pg-port).
unsafe fn GetPublicationsStr(
    publications: *mut List,
    buf: *mut crate::lib::stringinfo::StringInfoData,
    is_where: bool,
) { crate::catalog::pg_subscription::GetPublicationsStr(publications as _, buf as _, is_where) }

// Replication slot drop at publisher node - replication/slot.h - TODO(pg-port).
unsafe fn ReplicationSlotDropAtPubNode(
    conn: *mut c_void,
    slotname: *const c_char,
    missing_ok: bool,
) { unimplemented!() }

// walrcv_exec / walrcv_clear_result - vtable wrappers.
// TODO(pg-port): real implementations live in walreceiver.c via WalReceiverFunctions vtable.
unsafe fn walrcv_exec(
    _conn: *mut c_void,
    _cmd: *const c_char,
    _nRetTypes: c_int,
    _retTypes: *const Oid,
) -> *mut WalRcvExecResult {
    unimplemented!() // TODO(pg-port): walrcv_exec vtable wrapper not yet ported
}

unsafe fn walrcv_clear_result(_walres: *mut WalRcvExecResult) {
    unimplemented!() // TODO(pg-port): walrcv_clear_result not yet ported
}

#[no_mangle]
unsafe fn walrcv_server_version(_conn: *mut c_void) -> c_int {
    unimplemented!() // TODO(pg-port): walrcv_server_version not yet ported
}

// Tuple table slot helpers - executor/tuptable.h - TODO(pg-port).
unsafe fn MakeSingleTupleTableSlot(
    _tupdesc: *mut TupleDesc,
    _ops: *const TTSOpsVTable,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO(pg-port): MakeSingleTupleTableSlot not yet ported
}

unsafe fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot) { crate::executor::execTuples::ExecDropSingleTupleTableSlot(slot as _) }

unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot { unimplemented!() }

unsafe fn slot_getattr(
    _slot: *mut TupleTableSlot,
    _attnum: c_int,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): slot_getattr not yet ported
}

unsafe fn tuplestore_gettupleslot(
    state: *mut c_void,
    forward: bool,
    copy: bool,
    slot: *mut TupleTableSlot,
) -> bool { crate::utils::sort::tuplestore::tuplestore_gettupleslot(state as _, forward, copy, slot as _) }

unsafe fn tuplestore_tuple_count(state: *mut c_void) -> i64 { crate::utils::sort::tuplestore::tuplestore_tuple_count(state as _) as _ }

// Datum getters - TODO(pg-port): postgres.h / fmgr.h.
unsafe fn DatumGetObjectId(_d: Datum) -> Oid {
    unimplemented!() // TODO(pg-port): DatumGetObjectId not yet ported
}

unsafe fn DatumGetChar(d: Datum) -> c_char { crate::postgres::DatumGetChar(d as _) }

unsafe fn DatumGetInt16(d: Datum) -> i16 { crate::postgres::DatumGetInt16(d as _) as _ }

unsafe fn DatumGetBool(d: Datum) -> bool { crate::postgres::DatumGetBool(d as _) }

unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO(pg-port): TextDatumGetCString not yet ported
}

unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO(pg-port): ObjectIdGetDatum not yet ported
}

unsafe fn CharGetDatum(_c: c_char) -> Datum {
    unimplemented!() // TODO(pg-port): CharGetDatum not yet ported
}

unsafe fn OidIsValid(_oid: Oid) -> bool {
    _oid != 0
}

// palloc / pfree - utils/palloc.h - TODO(pg-port).
unsafe fn palloc(_size: Size) -> *mut c_void {
    unimplemented!() // TODO(pg-port): palloc not yet ported
}

unsafe fn palloc0(_size: Size) -> *mut c_void {
    unimplemented!() // TODO(pg-port): palloc0 not yet ported
}

unsafe fn pfree(_ptr: *mut c_void) {
    unimplemented!() // TODO(pg-port): pfree not yet ported
}

// list helpers - nodes/pg_list.h - TODO(pg-port).
unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List {
    unimplemented!() // TODO(pg-port): lappend not yet ported
}

unsafe fn list_free_deep(_list: *mut List) {
    unimplemented!() // TODO(pg-port): list_free_deep not yet ported
}

unsafe fn list_make1(_datum: *mut c_void) -> *mut List {
    unimplemented!() // TODO(pg-port): list_make1 not yet ported
}

unsafe fn lfirst(_lc: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO(pg-port): lfirst not yet ported
}

unsafe fn linitial(_list: *mut List) -> *mut c_void {
    unimplemented!() // TODO(pg-port): linitial not yet ported
}

unsafe fn strVal(_node: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO(pg-port): strVal not yet ported
}

// StringInfo helpers - lib/stringinfo.h.
use crate::lib::stringinfo::StringInfoData;

unsafe fn initStringInfo(_buf: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): initStringInfo not yet ported
}

unsafe fn resetStringInfo(buf: *mut StringInfoData) { crate::lib::stringinfo::resetStringInfo(buf as _) }

unsafe fn appendStringInfo(_buf: *mut StringInfoData, _fmt: *const c_char) {
    unimplemented!() // TODO(pg-port): appendStringInfo not yet ported
}

unsafe fn appendStringInfoString(_buf: *mut StringInfoData, _str: *const c_char) {
    unimplemented!() // TODO(pg-port): appendStringInfoString not yet ported
}

unsafe fn appendStringInfoChar(_buf: *mut StringInfoData, _ch: c_char) {
    unimplemented!() // TODO(pg-port): appendStringInfoChar not yet ported
}

unsafe fn makeStringInfo() -> *mut StringInfoData { crate::lib::stringinfo::makeStringInfo() as _ }

unsafe fn destroyStringInfo(buf: *mut StringInfoData) { crate::lib::stringinfo::destroyStringInfo(buf as _) }

// heap_modify_tuple / heap_freetuple - access/htup_details.h - TODO(pg-port).
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: *mut TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple { unimplemented!() }

unsafe fn heap_freetuple(htup: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(htup as _) }

unsafe fn HeapTupleIsValid(_htup: HeapTuple) -> bool {
    unimplemented!() // TODO(pg-port): HeapTupleIsValid not yet ported
}

// proc_exit - storage/ipc.h - TODO(pg-port).
unsafe fn proc_exit(code: c_int) -> ! { crate::storage::ipc::ipc::proc_exit(code as _) }

// CHECK_FOR_INTERRUPTS - miscadmin.h.
// In PostgreSQL this is a macro that checks for pending interrupts.
// We model it as an inline function.
#[inline]
unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): real interrupt check via QueryCancelPending / ProcDiePending
}

// TTSOpsMinimalTuple - executor/tuptable.h - TODO(pg-port).
// c_void cannot be instantiated as a static; use a zero-size placeholder.
static TTSOpsMinimalTuple: u8 = 0; // placeholder; cast to *const TTSOpsVTable at use sites

// InvalidXLogRecPtr - access/xlogdefs.h.
const InvalidXLogRecPtr: XLogRecPtr = 0;

// hash table helpers - utils/dynahash.h - TODO(pg-port).
unsafe fn hash_create(
    tabname: *const c_char,
    nelem: c_int,
    info: *const HASHCTL,
    flags: c_int,
) -> *mut HTAB { unimplemented!() }

unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void { todo!("TODO(pg-port): hash_search") }

unsafe fn hash_destroy(hashp: *mut HTAB) { crate::utils::hash::dynahash::hash_destroy(hashp as _) }

unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}

unsafe fn TimestampDifferenceExceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: c_int,
) -> bool { crate::utils::adt::timestamp::TimestampDifferenceExceeds(start_time as _, stop_time as _, msec as _) }

unsafe fn InvalidateCatalogSnapshot() { crate::utils::time::snapmgr::InvalidateCatalogSnapshot() }

// ---------------------------------------------------------------------------
// Module state
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq, Clone, Copy)]
#[repr(C)]
enum SyncingTablesState {
    SYNC_TABLE_STATE_NEEDS_REBUILD,
    SYNC_TABLE_STATE_REBUILD_STARTED,
    SYNC_TABLE_STATE_VALID,
}

use SyncingTablesState::*;

static mut table_states_validity: SyncingTablesState =
    SYNC_TABLE_STATE_NEEDS_REBUILD;
static mut table_states_not_ready: *mut List = null_mut(); // NIL initially
static mut copybuf: *mut StringInfoData = null_mut();

// ---------------------------------------------------------------------------
// finish_sync_worker
// ---------------------------------------------------------------------------

/*
 * Exit routine for synchronization worker.
 */
pub unsafe fn finish_sync_worker() -> ! {
    /*
     * Commit any outstanding transaction. This is the usual case, unless
     * there was nothing to do for the table.
     */
    if IsTransactionState() {
        CommitTransactionCommand();
        pgstat_report_stat(true);
    }

    /* And flush all writes. */
    XLogFlush(GetXLogWriteRecPtr());

    StartTransactionCommand();
    ereport!(LOG, errmsg!(
            "logical replication table synchronization worker for subscription \"{}\", table \"{}\" has finished",
            std::ffi::CStr::from_ptr((*(MySubscription as *mut Subscription)).name).to_string_lossy(),
            std::ffi::CStr::from_ptr(get_rel_name((*MyLogicalRepWorker).relid)).to_string_lossy()
        ));
    CommitTransactionCommand();

    /* Find the leader apply worker and signal it. */
    logicalrep_worker_wakeup((*MyLogicalRepWorker).subid, 0 /* InvalidOid */);

    /* Stop gracefully */
    proc_exit(0);
}

// ---------------------------------------------------------------------------
// wait_for_relation_state_change
// ---------------------------------------------------------------------------

/*
 * Wait until the relation sync state is set in the catalog to the expected
 * one; return true when it happens.
 *
 * Returns false if the table sync worker or the table itself have
 * disappeared, or the table state has been reset.
 *
 * Currently, this is used in the apply worker when transitioning from
 * CATCHUP state to SYNCDONE.
 */
unsafe fn wait_for_relation_state_change(relid: Oid, expected_state: c_char) -> bool {
    let mut state: c_char;

    loop {
        let worker: *mut LogicalRepWorker;
        let mut statelsn: XLogRecPtr = 0;

        CHECK_FOR_INTERRUPTS();

        InvalidateCatalogSnapshot();
        state = GetSubscriptionRelState(
            (*MyLogicalRepWorker).subid,
            relid,
            &mut statelsn,
        );

        if state == SUBREL_STATE_UNKNOWN {
            break;
        }

        if state == expected_state {
            return true;
        }

        /* Check if the sync worker is still running and bail if not. */
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        worker = logicalrep_worker_find(
            (*MyLogicalRepWorker).subid,
            relid,
            false,
        );
        LWLockRelease(LogicalRepWorkerLock);
        if worker.is_null() {
            break;
        }

        WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            1000,
            WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
        );

        ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
    }

    false
}

// ---------------------------------------------------------------------------
// wait_for_worker_state_change
// ---------------------------------------------------------------------------

/*
 * Wait until the apply worker changes the state of our synchronization
 * worker to the expected one.
 *
 * Used when transitioning from SYNCWAIT state to CATCHUP.
 *
 * Returns false if the apply worker has disappeared.
 */
unsafe fn wait_for_worker_state_change(expected_state: c_char) -> bool {
    let mut rc: c_int;

    loop {
        let worker: *mut LogicalRepWorker;

        CHECK_FOR_INTERRUPTS();

        /*
         * Done if already in correct state.  (We assume this fetch is atomic
         * enough to not give a misleading answer if we do it with no lock.)
         */
        if (*MyLogicalRepWorker).relstate == expected_state {
            return true;
        }

        /*
         * Bail out if the apply worker has died, else signal it we're
         * waiting.
         */
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        worker = logicalrep_worker_find(
            (*MyLogicalRepWorker).subid,
            0, /* InvalidOid */
            false,
        );
        if !worker.is_null() && !(*worker).proc.is_null() {
            logicalrep_worker_wakeup_ptr(worker);
        }
        LWLockRelease(LogicalRepWorkerLock);
        if worker.is_null() {
            break;
        }

        /*
         * Wait.  We expect to get a latch signal back from the apply worker,
         * but use a timeout in case it dies without sending one.
         */
        rc = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            1000,
            WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
        }
    }

    false
}

// ---------------------------------------------------------------------------
// invalidate_syncing_table_states
// ---------------------------------------------------------------------------

/*
 * Callback from syscache invalidation.
 */
#[no_mangle]
pub unsafe fn invalidate_syncing_table_states(
    _arg: Datum,
    _cacheid: c_int,
    _hashvalue: uint32,
) {
    table_states_validity = SYNC_TABLE_STATE_NEEDS_REBUILD;
}

// ---------------------------------------------------------------------------
// process_syncing_tables_for_sync
// ---------------------------------------------------------------------------

/*
 * Handle table synchronization cooperation from the synchronization
 * worker.
 *
 * If the sync worker is in CATCHUP state and reached (or passed) the
 * predetermined synchronization point in the WAL stream, mark the table as
 * SYNCDONE and finish.
 */
unsafe fn process_syncing_tables_for_sync(current_lsn: XLogRecPtr) {
    SpinLockAcquire(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);

    if (*MyLogicalRepWorker).relstate == SUBREL_STATE_CATCHUP
        && current_lsn >= (*MyLogicalRepWorker).relstate_lsn
    {
        let mut tli: TimeLineID = 0;
        let mut syncslotname = [0u8; NAMEDATALEN];
        let mut originname = [0u8; NAMEDATALEN];

        (*MyLogicalRepWorker).relstate = SUBREL_STATE_SYNCDONE;
        (*MyLogicalRepWorker).relstate_lsn = current_lsn;

        SpinLockRelease(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);

        /*
         * UpdateSubscriptionRelState must be called within a transaction.
         */
        if !IsTransactionState() {
            StartTransactionCommand();
        }

        UpdateSubscriptionRelState(
            (*MyLogicalRepWorker).subid,
            (*MyLogicalRepWorker).relid,
            (*MyLogicalRepWorker).relstate,
            (*MyLogicalRepWorker).relstate_lsn,
            false,
        );

        /*
         * End streaming so that LogRepWorkerWalRcvConn can be used to drop
         * the slot.
         */
        walrcv_endstreaming(LogRepWorkerWalRcvConn as *mut WalReceiverConn, &mut tli);

        /*
         * Cleanup the tablesync slot.
         *
         * This has to be done after updating the state because otherwise if
         * there is an error while doing the database operations we won't be
         * able to rollback dropped slot.
         */
        ReplicationSlotNameForTablesync(
            (*MyLogicalRepWorker).subid,
            (*MyLogicalRepWorker).relid,
            syncslotname.as_mut_ptr() as *mut c_char,
            syncslotname.len(),
        );

        /*
         * It is important to give an error if we are unable to drop the slot,
         * otherwise, it won't be dropped till the corresponding subscription
         * is dropped. So passing missing_ok = false.
         */
        ReplicationSlotDropAtPubNode(
            LogRepWorkerWalRcvConn,
            syncslotname.as_ptr() as *const c_char,
            false,
        );

        CommitTransactionCommand();
        pgstat_report_stat(false);

        /*
         * Start a new transaction to clean up the tablesync origin tracking.
         * This transaction will be ended within the finish_sync_worker().
         * Now, even, if we fail to remove this here, the apply worker will
         * ensure to clean it up afterward.
         *
         * We need to do this after the table state is set to SYNCDONE.
         * Otherwise, if an error occurs while performing the database
         * operation, the worker will be restarted and the in-memory state of
         * replication progress (remote_lsn) won't be rolled-back which would
         * have been cleared before restart. So, the restarted worker will use
         * invalid replication progress state resulting in replay of
         * transactions that have already been applied.
         */
        StartTransactionCommand();

        ReplicationOriginNameForLogicalRep(
            (*MyLogicalRepWorker).subid,
            (*MyLogicalRepWorker).relid,
            originname.as_mut_ptr() as *mut c_char,
            originname.len(),
        );

        /*
         * Resetting the origin session removes the ownership of the slot.
         * This is needed to allow the origin to be dropped.
         */
        replorigin_session_reset();
        replorigin_session_origin = InvalidRepOriginId;
        replorigin_session_origin_lsn = InvalidXLogRecPtr;
        replorigin_session_origin_timestamp = 0;

        /*
         * Drop the tablesync's origin tracking if exists.
         *
         * There is a chance that the user is concurrently performing refresh
         * for the subscription where we remove the table state and its origin
         * or the apply worker would have removed this origin. So passing
         * missing_ok = true.
         */
        replorigin_drop_by_name(
            originname.as_ptr() as *const c_char,
            true,
            false,
        );

        finish_sync_worker();
    } else {
        SpinLockRelease(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// process_syncing_tables_for_apply
// ---------------------------------------------------------------------------

/*
 * Handle table synchronization cooperation from the apply worker.
 *
 * Walk over all subscription tables that are individually tracked by the
 * apply process (currently, all that have state other than
 * SUBREL_STATE_READY) and manage synchronization for them.
 *
 * If there are tables that need synchronizing and are not being synchronized
 * yet, start sync workers for them (if there are free slots for sync
 * workers).  To prevent starting the sync worker for the same relation at a
 * high frequency after a failure, we store its last start time with each sync
 * state info.  We start the sync worker for the same relation after waiting
 * at least wal_retrieve_retry_interval.
 *
 * For tables that are being synchronized already, check if sync workers
 * either need action from the apply worker or have finished.  This is the
 * SYNCWAIT to CATCHUP transition.
 *
 * If the synchronization position is reached (SYNCDONE), then the table can
 * be marked as READY and is no longer tracked.
 */
unsafe fn process_syncing_tables_for_apply(current_lsn: XLogRecPtr) {
    #[repr(C)]
    struct tablesync_start_time_mapping {
        relid: Oid,
        last_start_time: TimestampTz,
    }

    static mut last_start_times: *mut HTAB = null_mut();

    let mut lc: *mut c_void;
    let mut started_tx: bool = false;
    let mut should_exit: bool = false;
    let mut rel: Relation = null_mut();

    // Assert(!IsTransactionState());

    /* We need up-to-date sync state info for subscription tables here. */
    FetchTableStates(&mut started_tx);

    /*
     * Prepare a hash table for tracking last start times of workers, to avoid
     * immediate restarts.  We don't need it if there are no tables that need
     * syncing.
     */
    if !table_states_not_ready.is_null() /* != NIL */ && last_start_times.is_null() {
        let ctl = HASHCTL {
            keysize: std::mem::size_of::<Oid>(),
            entrysize: std::mem::size_of::<tablesync_start_time_mapping>(),
        };

        last_start_times = hash_create(
            b"Logical replication table sync worker start times\0".as_ptr()
                as *const c_char,
            256,
            &ctl,
            HASH_ELEM | HASH_BLOBS,
        );
    }
    /*
     * Clean up the hash table when we're done with all tables (just to
     * release the bit of memory).
     */
    else if table_states_not_ready.is_null() /* == NIL */ && !last_start_times.is_null() {
        hash_destroy(last_start_times);
        last_start_times = null_mut();
    }

    /*
     * Process all tables that are being synchronized.
     *
     * We iterate the list using a raw pointer so we can break out early, matching
     * the foreach(lc, table_states_not_ready) C pattern.
     */
    // TODO(pg-port): real foreach iteration requires List internals; using stub.
    // The body below mirrors the C logic 1:1 pending list iteration landing.
    let list_head: *mut c_void = if !table_states_not_ready.is_null() {
        // placeholder: treat list ptr as opaque for now
        table_states_not_ready as *mut c_void
    } else {
        null_mut()
    };
    lc = list_head; // placeholder

    while !lc.is_null() {
        let rstate = lfirst(lc) as *mut SubscriptionRelState;

        if (*rstate).state == SUBREL_STATE_SYNCDONE {
            /*
             * Apply has caught up to the position where the table sync has
             * finished.  Mark the table as ready so that the apply will just
             * continue to replicate it normally.
             */
            if current_lsn >= (*rstate).lsn {
                let mut originname = [0u8; NAMEDATALEN];

                (*rstate).state = SUBREL_STATE_READY;
                (*rstate).lsn = current_lsn;
                if !started_tx {
                    StartTransactionCommand();
                    started_tx = true;
                }

                /*
                 * Remove the tablesync origin tracking if exists.
                 *
                 * There is a chance that the user is concurrently performing
                 * refresh for the subscription where we remove the table
                 * state and its origin or the tablesync worker would have
                 * already removed this origin. We can't rely on tablesync
                 * worker to remove the origin tracking as if there is any
                 * error while dropping we won't restart it to drop the
                 * origin. So passing missing_ok = true.
                 *
                 * Lock the subscription and origin in the same order as we
                 * are doing during DDL commands to avoid deadlocks. See
                 * AlterSubscription_refresh.
                 */
                LockSharedObject(
                    SubscriptionRelationId,
                    (*MyLogicalRepWorker).subid,
                    0,
                    AccessShareLock,
                );

                if rel.is_null() {
                    rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);
                }

                ReplicationOriginNameForLogicalRep(
                    (*MyLogicalRepWorker).subid,
                    (*rstate).relid,
                    originname.as_mut_ptr() as *mut c_char,
                    originname.len(),
                );
                replorigin_drop_by_name(
                    originname.as_ptr() as *const c_char,
                    true,
                    false,
                );

                /*
                 * Update the state to READY only after the origin cleanup.
                 */
                UpdateSubscriptionRelState(
                    (*MyLogicalRepWorker).subid,
                    (*rstate).relid,
                    (*rstate).state,
                    (*rstate).lsn,
                    true,
                );
            }
        } else {
            let syncworker: *mut LogicalRepWorker;

            /*
             * Look for a sync worker for this relation.
             */
            LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

            syncworker = logicalrep_worker_find(
                (*MyLogicalRepWorker).subid,
                (*rstate).relid,
                false,
            );

            if !syncworker.is_null() {
                /* Found one, update our copy of its state */
                SpinLockAcquire(&mut (*syncworker).relmutex as *mut _ as *mut c_void);
                (*rstate).state = (*syncworker).relstate;
                (*rstate).lsn = (*syncworker).relstate_lsn;
                if (*rstate).state == SUBREL_STATE_SYNCWAIT {
                    /*
                     * Sync worker is waiting for apply.  Tell sync worker it
                     * can catchup now.
                     */
                    (*syncworker).relstate = SUBREL_STATE_CATCHUP;
                    (*syncworker).relstate_lsn =
                        if (*syncworker).relstate_lsn > current_lsn {
                            (*syncworker).relstate_lsn
                        } else {
                            current_lsn
                        };
                }
                SpinLockRelease(&mut (*syncworker).relmutex as *mut _ as *mut c_void);

                /* If we told worker to catch up, wait for it. */
                if (*rstate).state == SUBREL_STATE_SYNCWAIT {
                    /* Signal the sync worker, as it may be waiting for us. */
                    if !(*syncworker).proc.is_null() {
                        logicalrep_worker_wakeup_ptr(syncworker);
                    }

                    /* Now safe to release the LWLock */
                    LWLockRelease(LogicalRepWorkerLock);

                    if started_tx {
                        /*
                         * We must commit the existing transaction to release
                         * the existing locks before entering a busy loop.
                         * This is required to avoid any undetected deadlocks
                         * due to any existing lock as deadlock detector won't
                         * be able to detect the waits on the latch.
                         *
                         * Also close any tables prior to the commit.
                         */
                        if !rel.is_null() {
                            table_close(rel, NoLock);
                            rel = null_mut();
                        }
                        CommitTransactionCommand();
                        pgstat_report_stat(false);
                    }

                    /*
                     * Enter busy loop and wait for synchronization worker to
                     * reach expected state (or die trying).
                     */
                    StartTransactionCommand();
                    started_tx = true;

                    wait_for_relation_state_change(
                        (*rstate).relid,
                        SUBREL_STATE_SYNCDONE,
                    );
                } else {
                    LWLockRelease(LogicalRepWorkerLock);
                }
            } else {
                /*
                 * If there is no sync worker for this table yet, count
                 * running sync workers for this subscription, while we have
                 * the lock.
                 */
                let nsyncworkers: c_int =
                    logicalrep_sync_worker_count((*MyLogicalRepWorker).subid);

                /* Now safe to release the LWLock */
                LWLockRelease(LogicalRepWorkerLock);

                /*
                 * If there are free sync worker slot(s), start a new sync
                 * worker for the table.
                 */
                if nsyncworkers < max_sync_workers_per_subscription {
                    let now: TimestampTz = GetCurrentTimestamp();
                    let mut found: bool = false;

                    let hentry = hash_search(
                        last_start_times,
                        &(*rstate).relid as *const Oid as *const c_void,
                        HASH_ENTER,
                        &mut found,
                    ) as *mut tablesync_start_time_mapping;

                    if !found
                        || TimestampDifferenceExceeds(
                            (*hentry).last_start_time,
                            now,
                            wal_retrieve_retry_interval,
                        )
                    {
                        /*
                         * Set the last_start_time even if we fail to start
                         * the worker, so that we won't retry until
                         * wal_retrieve_retry_interval has elapsed.
                         */
                        (*hentry).last_start_time = now;
                        logicalrep_worker_launch(
                            WORKERTYPE_TABLESYNC,
                            (*MyLogicalRepWorker).dbid,
                            (*(MySubscription as *mut Subscription)).oid,
                            (*(MySubscription as *mut Subscription)).name,
                            (*MyLogicalRepWorker).userid,
                            (*rstate).relid,
                            DSM_HANDLE_INVALID,
                        );
                    }
                }
            }
        }

        // Advance list cell - TODO(pg-port): real lnext() when List internals land.
        lc = null_mut(); // placeholder: terminates after first iteration stub
    }

    /* Close table if opened */
    if !rel.is_null() {
        table_close(rel, NoLock);
    }

    if started_tx {
        /*
         * Even when the two_phase mode is requested by the user, it remains
         * as 'pending' until all tablesyncs have reached READY state.
         *
         * When this happens, we restart the apply worker and (if the
         * conditions are still ok) then the two_phase tri-state will become
         * 'enabled' at that time.
         *
         * Note: If the subscription has no tables then leave the state as
         * PENDING, which allows ALTER SUBSCRIPTION ... REFRESH PUBLICATION to
         * work.
         */
        if (*(MySubscription as *mut Subscription)).twophasestate == LOGICALREP_TWOPHASE_STATE_PENDING {
            CommandCounterIncrement(); /* make updates visible */
            if AllTablesyncsReady() {
                ereport!(LOG, errmsg!(
                        "logical replication apply worker for subscription \"{}\" will restart so that two_phase can be enabled",
                        std::ffi::CStr::from_ptr((*(MySubscription as *mut Subscription)).name).to_string_lossy()
                    ));
                should_exit = true;
            }
        }

        CommitTransactionCommand();
        pgstat_report_stat(true);
    }

    if should_exit {
        /*
         * Reset the last-start time for this worker so that the launcher will
         * restart it without waiting for wal_retrieve_interval.
         */
        ApplyLauncherForgetWorkerStartTime((*(MySubscription as *mut Subscription)).oid);

        proc_exit(0);
    }
}

// ---------------------------------------------------------------------------
// process_syncing_tables
// ---------------------------------------------------------------------------

/*
 * Process possible state change(s) of tables that are being synchronized.
 */
#[no_mangle]
pub unsafe fn process_syncing_tables(current_lsn: XLogRecPtr) {
    match (*MyLogicalRepWorker).type_ {
        WORKERTYPE_PARALLEL_APPLY => {
            /*
             * Skip for parallel apply workers because they only operate on
             * tables that are in a READY state. See pa_can_start() and
             * should_apply_changes_for_rel().
             */
        }
        WORKERTYPE_TABLESYNC => {
            process_syncing_tables_for_sync(current_lsn);
        }
        WORKERTYPE_APPLY => {
            process_syncing_tables_for_apply(current_lsn);
        }
        _ /* WORKERTYPE_UNKNOWN */ => {
            /* Should never happen. */
            elog!(ERROR, "Unknown worker type");
        }
    }
}


// ---------------------------------------------------------------------------
// make_copy_attnamelist
// ---------------------------------------------------------------------------

/*
 * Create list of columns for COPY based on logical relation mapping.
 */
unsafe fn make_copy_attnamelist(rel: *mut LogicalRepRelMapEntry) -> *mut List {
    let mut attnamelist: *mut List = null_mut(); /* NIL */
    let mut i: c_int;

    // TODO(pg-port): remoterel.natts / remoterel.attnames require LogicalRepRelation internals.
    // The cast below treats LogicalRepRelation as opaque (c_void); accessing fields
    // requires the real struct when it lands.
    let remoterel = &(*rel).remoterel as *const LogicalRepRelation as *const c_void;
    let natts: c_int = 0; // TODO(pg-port): (*remoterel).natts
    i = 0;
    while i < natts {
        let attname: *mut c_char = null_mut(); // TODO(pg-port): (*remoterel).attnames[i]
        attnamelist = lappend(attnamelist, makeString(attname));
        i += 1;
    }

    attnamelist
}

// ---------------------------------------------------------------------------
// copy_read_data
// ---------------------------------------------------------------------------

/*
 * Data source callback for the COPY FROM, which reads from the remote
 * connection and passes the data back to our local COPY.
 */
unsafe fn copy_read_data(outbuf: *mut c_void, minread: c_int, maxread: c_int) -> c_int {
    let mut bytesread: c_int = 0;
    let mut avail: c_int;
    let mut maxread = maxread;

    /* If there are some leftover data from previous read, use it. */
    avail = (*copybuf).len - (*copybuf).cursor;
    if avail > 0 {
        if avail > maxread {
            avail = maxread;
        }
        std::ptr::copy_nonoverlapping(
            ((*copybuf).data as *const u8).add((*copybuf).cursor as usize),
            outbuf as *mut u8,
            avail as usize,
        );
        (*copybuf).cursor += avail;
        maxread -= avail;
        bytesread += avail;
    }

    while maxread > 0 && bytesread < minread {
        let mut fd: pgsocket = crate::port::port_api::PGINVALID_SOCKET;
        let mut len: c_int;
        let mut buf: *mut c_char = null_mut();

        loop {
            /* Try read the data. */
            len = walrcv_receive(LogRepWorkerWalRcvConn as *mut WalReceiverConn, &mut buf, &mut fd);

            CHECK_FOR_INTERRUPTS();

            if len == 0 {
                break;
            } else if len < 0 {
                return bytesread;
            } else {
                /* Process the data */
                (*copybuf).data = buf;
                (*copybuf).len = len;
                (*copybuf).cursor = 0;

                avail = (*copybuf).len - (*copybuf).cursor;
                if avail > maxread {
                    avail = maxread;
                }
                std::ptr::copy_nonoverlapping(
                    ((*copybuf).data as *const u8).add((*copybuf).cursor as usize),
                    outbuf as *mut u8,
                    avail as usize,
                );
                // outbuf = (char *) outbuf + avail  -- no pointer arithmetic on *mut c_void,
                // so the write pointer advance is done via the mutable local offset implicitly;
                // real COPY callback advances via the caller's buffer pointer semantics.
                (*copybuf).cursor += avail;
                maxread -= avail;
                bytesread += avail;
            }

            if maxread <= 0 || bytesread >= minread {
                return bytesread;
            }
        }

        /*
         * Wait for more data or latch.
         */
        WaitLatchOrSocket(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_SOCKET_READABLE | WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            fd,
            1000,
            WAIT_EVENT_LOGICAL_SYNC_DATA,
        );

        ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
    }

    bytesread
}

// ---------------------------------------------------------------------------
// fetch_remote_table_info
// ---------------------------------------------------------------------------

/*
 * Get information about remote relation in similar fashion the RELATION
 * message provides during replication.
 *
 * This function also returns (a) the relation qualifications to be used in
 * the COPY command, and (b) whether the remote relation has published any
 * generated column.
 */
unsafe fn fetch_remote_table_info(
    nspname: *const c_char,
    relname: *const c_char,
    lrel: *mut LogicalRepRelation,
    qual: *mut *mut List,
    gencol_published: *mut bool,
) {
    let res: *mut WalRcvExecResult;
    let mut cmd = std::mem::MaybeUninit::<StringInfoData>::uninit();
    let cmd = cmd.as_mut_ptr();
    let slot: *mut TupleTableSlot;
    let tableRow: [Oid; 3] = [OIDOID, CHAROID, CHAROID];
    let attrRow: [Oid; 5] = [INT2OID, TEXTOID, OIDOID, BOOLOID, BOOLOID];
    let qualRow: [Oid; 1] = [TEXTOID];
    let mut isnull: bool = false;
    let mut natt: c_int;
    let mut pub_names: *mut StringInfoData = null_mut();
    let mut included_cols: *mut Bitmapset = null_mut();
    let server_version: c_int = walrcv_server_version(LogRepWorkerWalRcvConn);

    // TODO(pg-port): lrel fields require LogicalRepRelation struct internals.
    // Using opaque pointer for now; real field writes follow when struct lands.
    // (*lrel).nspname = nspname;
    // (*lrel).relname = relname;

    /* First fetch Oid and replica identity. */
    initStringInfo(cmd);
    // appendStringInfo -- format strings use C variadics; render as string slices.
    // "SELECT c.oid, c.relreplident, c.relkind ..."
    // TODO(pg-port): appendStringInfo variadics -> concrete Rust format calls when
    // format_str! or equiv lands.  Placeholder:
    appendStringInfo(cmd, b"SELECT c.oid, c.relreplident, c.relkind\
  FROM pg_catalog.pg_class c\
  INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)\
  WHERE n.nspname = {} AND c.relname = {}\0".as_ptr() as *const c_char);

    let res = walrcv_exec(
        LogRepWorkerWalRcvConn,
        (*cmd).data,
        tableRow.len() as c_int,
        tableRow.as_ptr(),
    );

    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(ERROR, errmsg!(
                "could not fetch table info for table \"{}.{}\" from publisher: {}",
                std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                std::ffi::CStr::from_ptr(relname).to_string_lossy(),
                std::ffi::CStr::from_ptr((*res).err).to_string_lossy()
            )) /* C also: errcode */;
    }

    slot = MakeSingleTupleTableSlot((*res).tupledesc, &TTSOpsMinimalTuple as *const u8 as *const TTSOpsVTable);
    if !tuplestore_gettupleslot((*res).tuplestore, true, false, slot) {
        ereport!(ERROR, errmsg!(
                "table \"{}.{}\" not found on publisher",
                std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                std::ffi::CStr::from_ptr(relname).to_string_lossy()
            )) /* C also: errcode */;
    }

    // (*lrel).remoteid = DatumGetObjectId(slot_getattr(slot, 1, &mut isnull));
    // Assert(!isnull);
    // (*lrel).replident = DatumGetChar(slot_getattr(slot, 2, &mut isnull));
    // Assert(!isnull);
    // (*lrel).relkind = DatumGetChar(slot_getattr(slot, 3, &mut isnull));
    // Assert(!isnull);
    // TODO(pg-port): lrel field writes deferred to LogicalRepRelation struct landing.

    ExecDropSingleTupleTableSlot(slot);
    walrcv_clear_result(res);

    /*
     * Get column lists for each relation.
     *
     * We need to do this before fetching info about column names and types,
     * so that we can skip columns that should not be replicated.
     */
    if server_version >= 150000 {
        let attrsRow: [Oid; 1] = [INT2VECTOROID];

        /* Build the pub_names comma-separated string. */
        pub_names = makeStringInfo();
        GetPublicationsStr((*(MySubscription as *mut Subscription)).publications, pub_names, true);

        /*
         * Fetch info about column lists for the relation (from all the
         * publications).
         */
        resetStringInfo(cmd);
        // appendStringInfo for column list query
        // TODO(pg-port): appendStringInfo variadics -> concrete; placeholder:
        appendStringInfo(
            cmd,
            b"SELECT DISTINCT (CASE WHEN (array_length(gpt.attrs, 1) = c.relnatts) THEN NULL ELSE gpt.attrs END) FROM pg_publication p, LATERAL pg_get_publication_tables(p.pubname) gpt, pg_class c WHERE gpt.relid = {} AND c.oid = gpt.relid AND p.pubname IN ( {} )\0"
                .as_ptr() as *const c_char,
        );

        let pubres = walrcv_exec(
            LogRepWorkerWalRcvConn,
            (*cmd).data,
            attrsRow.len() as c_int,
            attrsRow.as_ptr(),
        );

        if (*pubres).status != WALRCV_OK_TUPLES {
            ereport!(ERROR, errmsg!(
                    "could not fetch column list info for table \"{}.{}\" from publisher: {}",
                    std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(relname).to_string_lossy(),
                    std::ffi::CStr::from_ptr((*pubres).err).to_string_lossy()
                )) /* C also: errcode */;
        }

        /*
         * We don't support the case where the column list is different for
         * the same table when combining publications. See comments atop
         * fetch_table_list. So there should be only one row returned.
         * Although we already checked this when creating the subscription, we
         * still need to check here in case the column list was changed after
         * creating the subscription and before the sync worker is started.
         */
        if tuplestore_tuple_count((*pubres).tuplestore) > 1 {
            ereport!(ERROR, errmsg!(
                    "cannot use different column lists for table \"{}.{}\" in different publications",
                    std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(relname).to_string_lossy()
                )) /* C also: errcode */;
        }

        /*
         * Get the column list and build a single bitmap with the attnums.
         *
         * If we find a NULL value, it means all the columns should be
         * replicated.
         */
        let tslot = MakeSingleTupleTableSlot((*pubres).tupledesc, &TTSOpsMinimalTuple as *const u8 as *const TTSOpsVTable);
        if tuplestore_gettupleslot((*pubres).tuplestore, true, false, tslot) {
            let cfval: Datum = slot_getattr(tslot, 1, &mut isnull);

            if !isnull {
                let arr: *mut ArrayType = DatumGetArrayTypeP(cfval);
                let nelems: c_int = *ARR_DIMS(arr);
                let elems: *mut i16 = ARR_DATA_PTR(arr) as *mut i16;

                natt = 0;
                while natt < nelems {
                    included_cols = bms_add_member(
                        included_cols,
                        *elems.add(natt as usize) as c_int,
                    );
                    natt += 1;
                }
            }

            ExecClearTuple(tslot);
        }
        ExecDropSingleTupleTableSlot(tslot);

        walrcv_clear_result(pubres);
    }

    /*
     * Now fetch column names and types.
     */
    resetStringInfo(cmd);
    appendStringInfoString(
        cmd,
        b"SELECT a.attnum, a.attname, a.atttypid, a.attnum = ANY(i.indkey)\0".as_ptr()
            as *const c_char,
    );

    /* Generated columns can be replicated since version 18. */
    if server_version >= 180000 {
        appendStringInfoString(
            cmd,
            b", a.attgenerated != ''\0".as_ptr() as *const c_char,
        );
    }

    // appendStringInfo for attribute query body
    // TODO(pg-port): appendStringInfo variadics -> concrete when format landing.
    appendStringInfo(
        cmd,
        b"  FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_index i ON (i.indexrelid = pg_get_replica_identity_index({})) WHERE a.attnum > 0::pg_catalog.int2 AND NOT a.attisdropped {} AND a.attrelid = {} ORDER BY a.attnum\0"
            .as_ptr() as *const c_char,
    );

    let nret: c_int = if server_version >= 180000 {
        attrRow.len() as c_int
    } else {
        (attrRow.len() - 1) as c_int
    };
    let res = walrcv_exec(
        LogRepWorkerWalRcvConn,
        (*cmd).data,
        nret,
        attrRow.as_ptr(),
    );

    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(ERROR, errmsg!(
                "could not fetch table info for table \"{}.{}\" from publisher: {}",
                std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                std::ffi::CStr::from_ptr(relname).to_string_lossy(),
                std::ffi::CStr::from_ptr((*res).err).to_string_lossy()
            )) /* C also: errcode */;
    }

    /* We don't know the number of rows coming, so allocate enough space. */
    // (*lrel).attnames = palloc0(MaxTupleAttributeNumber * mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    // (*lrel).atttyps  = palloc0(MaxTupleAttributeNumber * mem::size_of::<Oid>()) as *mut Oid;
    // (*lrel).attkeys  = null_mut();
    // TODO(pg-port): lrel field writes deferred.

    /*
     * Store the columns as a list of names.  Ignore those that are not
     * present in the column list, if there is one.
     */
    natt = 0;
    let slot = MakeSingleTupleTableSlot((*res).tupledesc, &TTSOpsMinimalTuple as *const u8 as *const TTSOpsVTable);
    while tuplestore_gettupleslot((*res).tuplestore, true, false, slot) {
        let attnum: i16;
        let rel_colname: *mut c_char;

        attnum = DatumGetInt16(slot_getattr(slot, 1, &mut isnull));
        // Assert(!isnull);

        /* If the column is not in the column list, skip it. */
        if !included_cols.is_null()
            && !bms_is_member(attnum as c_int, included_cols)
        {
            ExecClearTuple(slot);
            continue;
        }

        rel_colname = TextDatumGetCString(slot_getattr(slot, 2, &mut isnull));
        // Assert(!isnull);

        // (*lrel).attnames[natt] = rel_colname;
        // (*lrel).atttyps[natt]  = DatumGetObjectId(slot_getattr(slot, 3, &mut isnull));
        // Assert(!isnull);

        // if DatumGetBool(slot_getattr(slot, 4, &mut isnull)) {
        //     (*lrel).attkeys = bms_add_member((*lrel).attkeys, natt);
        // }

        /* Remember if the remote table has published any generated column. */
        if server_version >= 180000 && !(*gencol_published) {
            *gencol_published = DatumGetBool(slot_getattr(slot, 5, &mut isnull));
            // Assert(!isnull);
        }

        /* Should never happen. */
        natt += 1;
        if natt >= MaxTupleAttributeNumber as c_int {
            elog!(
                ERROR,
                "too many columns in remote table \"{}.{}\"",
                std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                std::ffi::CStr::from_ptr(relname).to_string_lossy()
            );
        }

        ExecClearTuple(slot);
    }
    ExecDropSingleTupleTableSlot(slot);

    // (*lrel).natts = natt;

    walrcv_clear_result(res);

    /*
     * Get relation's row filter expressions. DISTINCT avoids the same
     * expression of a table in multiple publications from being included
     * multiple times in the final expression.
     *
     * We need to copy the row even if it matches just one of the
     * publications, so we later combine all the quals with OR.
     *
     * For initial synchronization, row filtering can be ignored in following
     * cases:
     *
     * 1) one of the subscribed publications for the table hasn't specified
     * any row filter
     *
     * 2) one of the subscribed publications has puballtables set to true
     *
     * 3) one of the subscribed publications is declared as TABLES IN SCHEMA
     * that includes this relation
     */
    if server_version >= 150000 {
        /* Reuse the already-built pub_names. */
        // Assert(pub_names != NULL);

        /* Check for row filters. */
        resetStringInfo(cmd);
        // appendStringInfo for qual query
        // TODO(pg-port): appendStringInfo variadics -> concrete.
        appendStringInfo(
            cmd,
            b"SELECT DISTINCT pg_get_expr(gpt.qual, gpt.relid) FROM pg_publication p, LATERAL pg_get_publication_tables(p.pubname) gpt WHERE gpt.relid = {} AND p.pubname IN ( {} )\0"
                .as_ptr() as *const c_char,
        );

        let res = walrcv_exec(
            LogRepWorkerWalRcvConn,
            (*cmd).data,
            1,
            qualRow.as_ptr(),
        );

        if (*res).status != WALRCV_OK_TUPLES {
            ereport!(ERROR, errmsg!(
                    "could not fetch table WHERE clause info for table \"{}.{}\" from publisher: {}",
                    std::ffi::CStr::from_ptr(nspname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(relname).to_string_lossy(),
                    std::ffi::CStr::from_ptr((*res).err).to_string_lossy()
                ));
        }

        /*
         * Multiple row filter expressions for the same table will be combined
         * by COPY using OR. If any of the filter expressions for this table
         * are null, it means the whole table will be copied. In this case it
         * is not necessary to construct a unified row filter expression at
         * all.
         */
        let slot = MakeSingleTupleTableSlot((*res).tupledesc, &TTSOpsMinimalTuple as *const u8 as *const TTSOpsVTable);
        'qual_loop: while tuplestore_gettupleslot((*res).tuplestore, true, false, slot) {
            let rf: Datum = slot_getattr(slot, 1, &mut isnull);

            if !isnull {
                *qual = lappend(
                    *qual,
                    makeString(TextDatumGetCString(rf)),
                );
            } else {
                /* Ignore filters and cleanup as necessary. */
                if !(*qual).is_null() {
                    list_free_deep(*qual);
                    *qual = null_mut(); /* NIL */
                }
                break 'qual_loop;
            }

            ExecClearTuple(slot);
        }
        ExecDropSingleTupleTableSlot(slot);

        walrcv_clear_result(res);
        destroyStringInfo(pub_names);
    }

    pfree((*cmd).data as *mut c_void);
}


// ---------------------------------------------------------------------------
// copy_table
// ---------------------------------------------------------------------------

/*
 * Copy existing data of a table from publisher.
 *
 * Caller is responsible for locking the local relation.
 */
unsafe fn copy_table(rel: Relation) {
    let relmapentry: *mut LogicalRepRelMapEntry;
    let mut lrel = std::mem::MaybeUninit::<LogicalRepRelation>::uninit();
    let lrel = lrel.as_mut_ptr();
    let mut qual: *mut List = null_mut(); /* NIL */
    let res: *mut WalRcvExecResult;
    let mut cmd = std::mem::MaybeUninit::<StringInfoData>::uninit();
    let cmd = cmd.as_mut_ptr();
    let cstate: CopyFromState;
    let attnamelist: *mut List;
    let pstate: *mut ParseState;
    let mut options: *mut List = null_mut(); /* NIL */
    let mut gencol_published: bool = false;

    /* Get the publisher relation info. */
    fetch_remote_table_info(
        get_namespace_name(RelationGetNamespace(rel)),
        RelationGetRelationName(rel),
        lrel,
        &mut qual,
        &mut gencol_published,
    );

    /* Put the relation into relmap. */
    logicalrep_relmap_update(lrel);

    /* Map the publisher relation to local one. */
    // relmapentry = logicalrep_rel_open((*lrel).remoteid, NoLock);
    // TODO(pg-port): lrel.remoteid field deferred; placeholder:
    relmapentry = logicalrep_rel_open(0, NoLock);
    // Assert(rel == relmapentry->localrel);

    /* Start copy on the publisher. */
    initStringInfo(cmd);

    // TODO(pg-port): lrel.relkind / lrel.natts / lrel.nspname / lrel.relname / lrel.attnames
    // all require LogicalRepRelation struct internals.  Placeholders used below.
    let lrel_relkind: c_char = RELKIND_RELATION; // TODO(pg-port)
    let lrel_natts: c_int = 0;                    // TODO(pg-port)
    let lrel_nspname: *const c_char = null();     // TODO(pg-port)
    let lrel_relname: *const c_char = null();     // TODO(pg-port)

    /* Regular table with no row filter or generated columns */
    if lrel_relkind == RELKIND_RELATION && qual.is_null() /* NIL */ && !gencol_published {
        // appendStringInfo(&cmd, "COPY {}", quote_qualified_identifier(...))
        appendStringInfo(cmd, b"COPY {}\0".as_ptr() as *const c_char);

        /* If the table has columns, then specify the columns */
        if lrel_natts > 0 {
            appendStringInfoString(cmd, b" (\0".as_ptr() as *const c_char);

            /*
             * XXX Do we need to list the columns in all cases? Maybe we're
             * replicating all columns?
             */
            // TODO(pg-port): iterate lrel.attnames[i]
            for i in 0..lrel_natts {
                if i > 0 {
                    appendStringInfoString(cmd, b", \0".as_ptr() as *const c_char);
                }
                // appendStringInfoString(&cmd, quote_identifier(lrel.attnames[i]));
                // TODO(pg-port): lrel.attnames[i]
            }

            appendStringInfoChar(cmd, b')' as c_char);
        }

        appendStringInfoString(cmd, b" TO STDOUT\0".as_ptr() as *const c_char);
    } else {
        /*
         * For non-tables and tables with row filters, we need to do COPY
         * (SELECT ...), but we can't just do SELECT * because we may need to
         * copy only subset of columns including generated columns. For tables
         * with any row filters, build a SELECT query with OR'ed row filters
         * for COPY.
         *
         * We also need to use this same COPY (SELECT ...) syntax when
         * generated columns are published, because copy of generated columns
         * is not supported by the normal COPY.
         */
        appendStringInfoString(cmd, b"COPY (SELECT \0".as_ptr() as *const c_char);
        for i in 0..lrel_natts {
            // appendStringInfoString(&cmd, quote_identifier(lrel.attnames[i]));
            // TODO(pg-port): lrel.attnames[i]
            if i < lrel_natts - 1 {
                appendStringInfoString(cmd, b", \0".as_ptr() as *const c_char);
            }
        }

        appendStringInfoString(cmd, b" FROM \0".as_ptr() as *const c_char);

        /*
         * For regular tables, make sure we don't copy data from a child that
         * inherits the named table as those will be copied separately.
         */
        if lrel_relkind == RELKIND_RELATION {
            appendStringInfoString(cmd, b"ONLY \0".as_ptr() as *const c_char);
        }

        // appendStringInfoString(&cmd, quote_qualified_identifier(lrel.nspname, lrel.relname));
        /* list of OR'ed filters */
        if !qual.is_null() /* != NIL */ {
            // ListCell *lc;
            // char *q = strVal(linitial(qual));
            // appendStringInfo(&cmd, " WHERE {}", q);
            // for_each_from(lc, qual, 1) { q = strVal(lfirst(lc)); appendStringInfo(&cmd, " OR {}", q); }
            // TODO(pg-port): List iteration requires real list internals.
            list_free_deep(qual);
        }

        appendStringInfoString(cmd, b") TO STDOUT\0".as_ptr() as *const c_char);
    }

    /*
     * Prior to v16, initial table synchronization will use text format even
     * if the binary option is enabled for a subscription.
     */
    if walrcv_server_version(LogRepWorkerWalRcvConn) >= 160000
        && (*(MySubscription as *mut Subscription)).binary
    {
        appendStringInfoString(
            cmd,
            b" WITH (FORMAT binary)\0".as_ptr() as *const c_char,
        );
        options = list_make1(
            makeDefElem(
                b"format\0".as_ptr() as *const c_char,
                makeString(b"binary\0".as_ptr() as *mut c_char) as *mut Node,
                -1,
            ) as *mut c_void,
        );
    }

    let res = walrcv_exec(
        LogRepWorkerWalRcvConn,
        (*cmd).data,
        0,
        null(),
    );
    pfree((*cmd).data as *mut c_void);
    if (*res).status != WALRCV_OK_COPY_OUT {
        ereport!(ERROR, errmsg!(
                "could not start initial contents copy for table \"{}.{}\": {}",
                if lrel_nspname.is_null() { std::borrow::Cow::Borrowed("") } else { std::ffi::CStr::from_ptr(lrel_nspname).to_string_lossy() },
                if lrel_relname.is_null() { std::borrow::Cow::Borrowed("") } else { std::ffi::CStr::from_ptr(lrel_relname).to_string_lossy() },
                std::ffi::CStr::from_ptr((*res).err).to_string_lossy()
            )) /* C also: errcode */;
    }
    walrcv_clear_result(res);

    copybuf = makeStringInfo();

    pstate = make_parsestate(null_mut());
    addRangeTableEntryForRelation(pstate, rel, AccessShareLock, null_mut(), false, false);

    attnamelist = make_copy_attnamelist(relmapentry);
    cstate = BeginCopyFrom(
        pstate,
        rel,
        null_mut(),
        null(),
        false,
        copy_read_data,
        attnamelist,
        options,
    );

    /* Do the copy */
    CopyFrom(cstate);

    logicalrep_rel_close(relmapentry, NoLock);
}

// ---------------------------------------------------------------------------
// ReplicationSlotNameForTablesync
// ---------------------------------------------------------------------------

/*
 * Determine the tablesync slot name.
 *
 * The name must not exceed NAMEDATALEN - 1 because of remote node constraints
 * on slot name length. We append system_identifier to avoid slot_name
 * collision with subscriptions in other clusters. With the current scheme
 * pg_{}_sync_{}_UINT64_FORMAT (3 + 10 + 6 + 10 + 20 + '\0'), the maximum
 * length of slot_name will be 50.
 *
 * The returned slot name is stored in the supplied buffer (syncslotname) with
 * the given size.
 *
 * Note: We don't use the subscription slot name as part of tablesync slot name
 * because we are responsible for cleaning up these slots and it could become
 * impossible to recalculate what name to cleanup if the subscription slot name
 * had changed.
 */
pub unsafe fn ReplicationSlotNameForTablesync(
    suboid: Oid,
    relid: Oid,
    syncslotname: *mut c_char,
    szslot: Size,
) {
    let sysid: u64 = GetSystemIdentifier();
    let name = format!("pg_{}_sync_{}_{}", suboid, relid, sysid);
    let bytes = name.as_bytes();
    let len = bytes.len().min(szslot.saturating_sub(1));
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), syncslotname as *mut u8, len);
    *syncslotname.add(len) = 0;
}

// ---------------------------------------------------------------------------
// LogicalRepSyncTableStart
// ---------------------------------------------------------------------------

/*
 * Start syncing the table in the sync worker.
 *
 * If nothing needs to be done to sync the table, we exit the worker without
 * any further action.
 *
 * The returned slot name is palloc'ed in current memory context.
 */
unsafe fn LogicalRepSyncTableStart(origin_startpos: *mut XLogRecPtr) -> *mut c_char {
    let slotname: *mut c_char;
    let mut err: *mut c_char = null_mut();
    let relstate: c_char;
    let mut relstate_lsn: XLogRecPtr = 0;
    let rel: Relation;
    let aclresult: AclResult;
    let res: *mut WalRcvExecResult;
    let mut originname = [0u8; NAMEDATALEN];
    let mut originid: RepOriginId;
    let mut ucxt = std::mem::MaybeUninit::<UserContext>::uninit();
    let must_use_password: bool;
    let run_as_owner: bool;

    /* Check the state of the table synchronization. */
    StartTransactionCommand();
    relstate = GetSubscriptionRelState(
        (*MyLogicalRepWorker).subid,
        (*MyLogicalRepWorker).relid,
        &mut relstate_lsn,
    );
    CommitTransactionCommand();

    /* Is the use of a password mandatory? */
    must_use_password = (*(MySubscription as *mut Subscription)).passwordrequired
        && !(*(MySubscription as *mut Subscription)).ownersuperuser;

    SpinLockAcquire(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);
    (*MyLogicalRepWorker).relstate = relstate;
    (*MyLogicalRepWorker).relstate_lsn = relstate_lsn;
    SpinLockRelease(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);

    /*
     * If synchronization is already done or no longer necessary, exit now
     * that we've updated shared memory state.
     */
    match relstate {
        s if s == SUBREL_STATE_SYNCDONE
            || s == SUBREL_STATE_READY
            || s == SUBREL_STATE_UNKNOWN =>
        {
            finish_sync_worker(); /* doesn't return */
        }
        _ => {}
    }

    /* Calculate the name of the tablesync slot. */
    slotname = palloc(NAMEDATALEN) as *mut c_char;
    ReplicationSlotNameForTablesync(
        (*(MySubscription as *mut Subscription)).oid,
        (*MyLogicalRepWorker).relid,
        slotname,
        NAMEDATALEN,
    );

    /*
     * Here we use the slot name instead of the subscription name as the
     * application_name, so that it is different from the leader apply worker,
     * so that synchronous replication can distinguish them.
     */
    LogRepWorkerWalRcvConn = walrcv_connect(
        (*(MySubscription as *mut Subscription)).conninfo,
        true,
        true,
        must_use_password,
        slotname as *const c_char,
        &mut err,
    ) as *mut c_void;
    if LogRepWorkerWalRcvConn.is_null() {
        ereport!(ERROR, errmsg!(
                "table synchronization worker for subscription \"{}\" could not connect to the publisher: {}",
                std::ffi::CStr::from_ptr((*(MySubscription as *mut Subscription)).name).to_string_lossy(),
                if err.is_null() { std::borrow::Cow::Borrowed("") } else { std::ffi::CStr::from_ptr(err).to_string_lossy() }
            )) /* C also: errcode */;
    }

    // Assert(MyLogicalRepWorker->relstate == SUBREL_STATE_INIT ||
    //        MyLogicalRepWorker->relstate == SUBREL_STATE_DATASYNC ||
    //        MyLogicalRepWorker->relstate == SUBREL_STATE_FINISHEDCOPY);

    /* Assign the origin tracking record name. */
    ReplicationOriginNameForLogicalRep(
        (*(MySubscription as *mut Subscription)).oid,
        (*MyLogicalRepWorker).relid,
        originname.as_mut_ptr() as *mut c_char,
        originname.len(),
    );

    if (*MyLogicalRepWorker).relstate == SUBREL_STATE_DATASYNC {
        /*
         * We have previously errored out before finishing the copy so the
         * replication slot might exist. We want to remove the slot if it
         * already exists and proceed.
         *
         * XXX We could also instead try to drop the slot, last time we failed
         * but for that, we might need to clean up the copy state as it might
         * be in the middle of fetching the rows. Also, if there is a network
         * breakdown then it wouldn't have succeeded so trying it next time
         * seems like a better bet.
         */
        ReplicationSlotDropAtPubNode(LogRepWorkerWalRcvConn, slotname, true);
    } else if (*MyLogicalRepWorker).relstate == SUBREL_STATE_FINISHEDCOPY {
        /*
         * The COPY phase was previously done, but tablesync then crashed
         * before it was able to finish normally.
         */
        StartTransactionCommand();

        /*
         * The origin tracking name must already exist. It was created first
         * time this tablesync was launched.
         */
        originid = replorigin_by_name(originname.as_ptr() as *const c_char, false);
        replorigin_session_setup(originid, 0);
        replorigin_session_origin = originid;
        *origin_startpos = replorigin_session_get_progress(false);

        CommitTransactionCommand();

        // goto copy_table_done  -- handled via label below
        // Rust: use a labeled block to jump past the copy section.
        return LogicalRepSyncTableStart_after_copy(
            origin_startpos,
            originname.as_ptr() as *const c_char,
            slotname,
        );
    }

    SpinLockAcquire(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);
    (*MyLogicalRepWorker).relstate = SUBREL_STATE_DATASYNC;
    (*MyLogicalRepWorker).relstate_lsn = InvalidXLogRecPtr;
    SpinLockRelease(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);

    /*
     * Update the state, create the replication origin, and make them visible
     * to others.
     */
    StartTransactionCommand();
    UpdateSubscriptionRelState(
        (*MyLogicalRepWorker).subid,
        (*MyLogicalRepWorker).relid,
        (*MyLogicalRepWorker).relstate,
        (*MyLogicalRepWorker).relstate_lsn,
        false,
    );

    /*
     * Create the replication origin in a separate transaction from the one
     * that sets up the origin in shared memory. This prevents the risk that
     * changes to the origin in shared memory cannot be rolled back if the
     * transaction aborts.
     */
    originid = replorigin_by_name(originname.as_ptr() as *const c_char, true);
    if !OidIsValid(originid as Oid) {
        originid = replorigin_create(originname.as_ptr() as *const c_char);
    }

    CommitTransactionCommand();
    pgstat_report_stat(true);

    StartTransactionCommand();

    /*
     * Use a standard write lock here. It might be better to disallow access
     * to the table while it's being synchronized. But we don't want to block
     * the main apply process from working and it has to open the relation in
     * RowExclusiveLock when remapping remote relation id to local one.
     */
    rel = table_open((*MyLogicalRepWorker).relid, RowExclusiveLock);

    /*
     * Start a transaction in the remote node in REPEATABLE READ mode.  This
     * ensures that both the replication slot we create (see below) and the
     * COPY are consistent with each other.
     */
    let res = walrcv_exec(
        LogRepWorkerWalRcvConn,
        b"BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ\0".as_ptr() as *const c_char,
        0,
        null(),
    );
    if (*res).status != WALRCV_OK_COMMAND {
        ereport!(ERROR, errmsg!(
                "table copy could not start transaction on publisher: {}",
                std::ffi::CStr::from_ptr((*res).err).to_string_lossy()
            )) /* C also: errcode */;
    }
    walrcv_clear_result(res);

    /*
     * Create a new permanent logical decoding slot. This slot will be used
     * for the catchup phase after COPY is done, so tell it to use the
     * snapshot to make the final data consistent.
     */
    walrcv_create_slot(
        LogRepWorkerWalRcvConn as *mut WalReceiverConn,
        slotname as *const c_char,
        false, /* permanent */
        false, /* two_phase */
        (*(MySubscription as *mut Subscription)).failover,
        CRS_USE_SNAPSHOT,
        origin_startpos,
    );

    /*
     * Advance the origin to the LSN got from walrcv_create_slot and then set
     * up the origin. The advancement is WAL logged for the purpose of
     * recovery. Locks are to prevent the replication origin from vanishing
     * while advancing.
     *
     * The purpose of doing these before the copy is to avoid doing the copy
     * again due to any error in advancing or setting up origin tracking.
     */
    LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);
    replorigin_advance(
        originid,
        *origin_startpos,
        InvalidXLogRecPtr,
        true,  /* go backward */
        true,  /* WAL log */
    );
    UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    replorigin_session_setup(originid, 0);
    replorigin_session_origin = originid;

    /*
     * Make sure that the copy command runs as the table owner, unless the
     * user has opted out of that behaviour.
     */
    run_as_owner = (*(MySubscription as *mut Subscription)).runasowner;
    if !run_as_owner {
        SwitchToUntrustedUser(
            // rel->rd_rel->relowner -- TODO(pg-port): Relation->rd_rel->relowner
            0, // placeholder
            ucxt.as_mut_ptr(),
        );
    }

    /*
     * Check that our table sync worker has permission to insert into the
     * target table.
     */
    aclresult = pg_class_aclcheck(
        RelationGetRelid(rel),
        GetUserId(),
        2, /* ACL_INSERT -- TODO(pg-port): utils/acl.h */
    );
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            get_relkind_objtype(0 /* rel->rd_rel->relkind */), // TODO(pg-port)
            RelationGetRelationName(rel),
        );
    }

    /*
     * COPY FROM does not honor RLS policies.  That is not a problem for
     * subscriptions owned by roles with BYPASSRLS privilege (or superuser,
     * who has it implicitly), but other roles should not be able to
     * circumvent RLS.  Disallow logical replication into RLS enabled
     * relations for such roles.
     */
    if check_enable_rls(RelationGetRelid(rel), 0 /* InvalidOid */, false) == RLS_ENABLED {
        ereport!(ERROR, errmsg!(
                "user \"{}\" cannot replicate into relation with row-level security enabled: \"{}\"",
                std::ffi::CStr::from_ptr(GetUserNameFromId(GetUserId(), true)).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )) /* C also: errcode */;
    }

    /* Now do the initial data copy */
    PushActiveSnapshot(GetTransactionSnapshot());
    copy_table(rel);
    PopActiveSnapshot();

    let res = walrcv_exec(
        LogRepWorkerWalRcvConn,
        b"COMMIT\0".as_ptr() as *const c_char,
        0,
        null(),
    );
    if (*res).status != WALRCV_OK_COMMAND {
        ereport!(ERROR, errmsg!(
                "table copy could not finish transaction on publisher: {}",
                std::ffi::CStr::from_ptr((*res).err).to_string_lossy()
            )) /* C also: errcode */;
    }
    walrcv_clear_result(res);

    if !run_as_owner {
        RestoreUserContext(ucxt.as_mut_ptr());
    }

    table_close(rel, NoLock);

    /* Make the copy visible. */
    CommandCounterIncrement();

    /*
     * Update the persisted state to indicate the COPY phase is done; make it
     * visible to others.
     */
    UpdateSubscriptionRelState(
        (*MyLogicalRepWorker).subid,
        (*MyLogicalRepWorker).relid,
        SUBREL_STATE_FINISHEDCOPY,
        (*MyLogicalRepWorker).relstate_lsn,
        false,
    );

    CommitTransactionCommand();

    // copy_table_done:
    LogicalRepSyncTableStart_after_copy(
        origin_startpos,
        originname.as_ptr() as *const c_char,
        slotname,
    )
}

/*
 * Tail of LogicalRepSyncTableStart after the copy_table_done label.
 * Extracted to avoid goto; called both from the FINISHEDCOPY path and
 * the normal DATASYNC path after CommitTransactionCommand().
 */
unsafe fn LogicalRepSyncTableStart_after_copy(
    origin_startpos: *mut XLogRecPtr,
    originname: *const c_char,
    slotname: *mut c_char,
) -> *mut c_char {
    elog!(
        DEBUG1,
        "LogicalRepSyncTableStart: '{}' origin_startpos lsn {}/{}",
        std::ffi::CStr::from_ptr(originname).to_string_lossy(),
        ((*origin_startpos) >> 32) as u32,
        (*origin_startpos) as u32
    );

    /*
     * We are done with the initial data synchronization, update the state.
     */
    SpinLockAcquire(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);
    (*MyLogicalRepWorker).relstate = SUBREL_STATE_SYNCWAIT;
    (*MyLogicalRepWorker).relstate_lsn = *origin_startpos;
    SpinLockRelease(&mut (*MyLogicalRepWorker).relmutex as *mut _ as *mut c_void);

    /*
     * Finally, wait until the leader apply worker tells us to catch up and
     * then return to let LogicalRepApplyLoop do it.
     */
    wait_for_worker_state_change(SUBREL_STATE_CATCHUP);
    slotname
}

// ---------------------------------------------------------------------------
// FetchTableStates
// ---------------------------------------------------------------------------

/*
 * Common code to fetch the up-to-date sync state info into the static lists.
 *
 * Returns true if subscription has 1 or more tables, else false.
 *
 * Note: If this function started the transaction (indicated by the parameter)
 * then it is the caller's responsibility to commit it.
 */
unsafe fn FetchTableStates(started_tx: *mut bool) -> bool {
    static mut has_subrels: bool = false;

    *started_tx = false;

    if table_states_validity != SYNC_TABLE_STATE_VALID {
        let oldctx: MemoryContext;
        let rstates: *mut List;
        let mut lc: *mut c_void;
        let mut rstate: *mut SubscriptionRelState = null_mut();

        table_states_validity = SYNC_TABLE_STATE_REBUILD_STARTED;

        /* Clean the old lists. */
        list_free_deep(table_states_not_ready);
        table_states_not_ready = null_mut(); /* NIL */

        if !IsTransactionState() {
            StartTransactionCommand();
            *started_tx = true;
        }

        /* Fetch all non-ready tables. */
        rstates = GetSubscriptionRelations((*(MySubscription as *mut Subscription)).oid, true);

        /* Allocate the tracking info in a permanent memory context. */
        oldctx = MemoryContextSwitchTo(CacheMemoryContext);
        // TODO(pg-port): real foreach iteration requires List internals.
        // Placeholder: assumes empty list.
        lc = null_mut(); // TODO(pg-port): foreach start
        while !lc.is_null() {
            rstate = palloc(std::mem::size_of::<SubscriptionRelState>()) as *mut SubscriptionRelState;
            std::ptr::copy_nonoverlapping(
                lfirst(lc) as *const SubscriptionRelState,
                rstate,
                1,
            );
            table_states_not_ready = lappend(table_states_not_ready, rstate as *mut c_void);
            lc = null_mut(); // TODO(pg-port): lnext
        }
        MemoryContextSwitchTo(oldctx);

        /*
         * Does the subscription have tables?
         *
         * If there were not-READY relations found then we know it does. But
         * if table_states_not_ready was empty we still need to check again to
         * see if there are 0 tables.
         */
        has_subrels = !table_states_not_ready.is_null() /* != NIL */
            || HasSubscriptionRelations((*(MySubscription as *mut Subscription)).oid);

        /*
         * If the subscription relation cache has been invalidated since we
         * entered this routine, we still use and return the relations we just
         * finished constructing, to avoid infinite loops, but we leave the
         * table states marked as stale so that we'll rebuild it again on next
         * access. Otherwise, we mark the table states as valid.
         */
        if table_states_validity == SYNC_TABLE_STATE_REBUILD_STARTED {
            table_states_validity = SYNC_TABLE_STATE_VALID;
        }
    }

    has_subrels
}

// ---------------------------------------------------------------------------
// start_table_sync
// ---------------------------------------------------------------------------

/*
 * Execute the initial sync with error handling. Disable the subscription,
 * if it's required.
 *
 * Allocate the slot name in long-lived context on return. Note that we don't
 * handle FATAL errors which are probably because of system resource error and
 * are not repeatable.
 */
unsafe fn start_table_sync(
    origin_startpos: *mut XLogRecPtr,
    slotname: *mut *mut c_char,
) {
    let mut sync_slotname: *mut c_char = null_mut();

    // Assert(am_tablesync_worker());

    // PG_TRY / PG_CATCH / PG_END_TRY -- modelled as a Rust catch_unwind analog.
    // TODO(pg-port): real PG_TRY semantics require setjmp/longjmp integration.
    // For now, inline the try body; error handling stubs below.
    {
        /* Call initial sync. */
        sync_slotname = LogicalRepSyncTableStart(origin_startpos);
    }
    // PG_CATCH block would go here; see TODO above.
    // if disableonerr { DisableSubscriptionAndExit(); }
    // else { AbortOutOfAnyTransaction(); pgstat_report_subscription_error(...); PG_RE_THROW(); }

    /* allocate slot name in long-lived context */
    *slotname = MemoryContextStrdup(ApplyContext, sync_slotname);
    pfree(sync_slotname as *mut c_void);
}

// ---------------------------------------------------------------------------
// run_tablesync_worker
// ---------------------------------------------------------------------------

/*
 * Runs the tablesync worker.
 *
 * It starts syncing tables. After a successful sync, sets streaming options
 * and starts streaming to catchup with apply worker.
 */
unsafe fn run_tablesync_worker() {
    let mut originname = [0u8; NAMEDATALEN];
    let mut origin_startpos: XLogRecPtr = InvalidXLogRecPtr;
    let mut slotname: *mut c_char = null_mut();
    let mut options = std::mem::MaybeUninit::<WalRcvStreamOptions>::uninit();

    start_table_sync(&mut origin_startpos, &mut slotname);

    ReplicationOriginNameForLogicalRep(
        (*(MySubscription as *mut Subscription)).oid,
        (*MyLogicalRepWorker).relid,
        originname.as_mut_ptr() as *mut c_char,
        originname.len(),
    );

    set_apply_error_context_origin(originname.as_mut_ptr() as *mut c_char);

    set_stream_options(options.as_mut_ptr() as *mut c_void, slotname, &mut origin_startpos);

    walrcv_startstreaming(LogRepWorkerWalRcvConn as *mut WalReceiverConn, options.as_ptr());

    /* Apply the changes till we catchup with the apply worker. */
    start_apply(origin_startpos);
}

// ---------------------------------------------------------------------------
// TablesyncWorkerMain
// ---------------------------------------------------------------------------

/* Logical Replication Tablesync worker entry point */
pub unsafe fn TablesyncWorkerMain(main_arg: crate::postgres::Datum) {
    let worker_slot: c_int = crate::postgres::DatumGetInt32(main_arg);

    SetupApplyOrSyncWorker(worker_slot);

    run_tablesync_worker();

    finish_sync_worker();
}

// ---------------------------------------------------------------------------
// AllTablesyncsReady
// ---------------------------------------------------------------------------

/*
 * If the subscription has no tables then return false.
 *
 * Otherwise, are all tablesyncs READY?
 *
 * Note: This function is not suitable to be called from outside of apply or
 * tablesync workers because MySubscription needs to be already initialized.
 */
#[no_mangle]
pub unsafe fn AllTablesyncsReady() -> bool {
    let mut started_tx: bool = false;
    let has_subrels: bool;

    /* We need up-to-date sync state info for subscription tables here. */
    has_subrels = FetchTableStates(&mut started_tx);

    if started_tx {
        CommitTransactionCommand();
        pgstat_report_stat(true);
    }

    /*
     * Return false when there are no tables in subscription or not all tables
     * are in ready state; true otherwise.
     */
    has_subrels && table_states_not_ready.is_null() /* == NIL */
}

// ---------------------------------------------------------------------------
// UpdateTwoPhaseState
// ---------------------------------------------------------------------------

/*
 * Update the two_phase state of the specified subscription in pg_subscription.
 */
#[no_mangle]
pub unsafe fn UpdateTwoPhaseState(suboid: Oid, new_state: c_char) {
    let rel: Relation;
    let mut tup: HeapTuple;
    let mut nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    let mut values: [Datum; Natts_pg_subscription] = [0; Natts_pg_subscription];

    // Assert(new_state == LOGICALREP_TWOPHASE_STATE_DISABLED ||
    //        new_state == LOGICALREP_TWOPHASE_STATE_PENDING ||
    //        new_state == LOGICALREP_TWOPHASE_STATE_ENABLED);

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(suboid));
    if !HeapTupleIsValid(tup) {
        elog!(
            ERROR,
            "cache lookup failed for subscription oid {}",
            suboid
        );
    }

    /* Form a new tuple. */
    // memset(values, 0, ...) -- values already zero-initialized above.
    // nulls and replaces zero-initialized above.

    /* And update/set two_phase state */
    values[Anum_pg_subscription_subtwophasestate - 1] = CharGetDatum(new_state);
    replaces[Anum_pg_subscription_subtwophasestate - 1] = true;

    tup = heap_modify_tuple(
        tup,
        RelationGetDescr(rel),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
        replaces.as_mut_ptr(),
    );
    CatalogTupleUpdate(rel, null_mut() /* &tup->t_self */, tup);
    // TODO(pg-port): tup->t_self requires HeapTupleData struct internals.

    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);
}

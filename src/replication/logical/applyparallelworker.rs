//! applyparallelworker.c
//!    Support routines for applying xact by parallel apply worker
//!
//! Copyright (c) 2023-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/replication/logical/applyparallelworker.c
//!
//! This file contains the code to launch, set up, and teardown a parallel apply
//! worker which receives the changes from the leader worker and invokes routines
//! to apply those on the subscriber database. Additionally, this file contains
//! routines that are intended to support setting up, using, and tearing down a
//! ParallelApplyWorkerInfo which is required so the leader worker and parallel
//! apply workers can communicate with each other.
//!
//! See comments atop the original C file for the detailed description of the
//! worker pool, the dynamic shared memory layout, locking considerations,
//! deadlock scenarios, and lock types.

use crate::prelude::*;

use core::ffi::CStr;

use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, XLogRecPtr, XLogRecPtrIsInvalid};
use crate::access::transam::TransactionIdIsValid;
use crate::nodes::pg_list::{
    lappend, lappend_xid, lfirst, lfirst_xid, list_delete_ptr, list_length, list_member_xid,
    list_nth_cell, list_truncate, List, NIL,
};
use crate::port::atomics::pg_atomic_uint32;
use crate::storage::lmgr::lockdefs::LOCKMODE;
use crate::utils::error::elog_impl::{error_context_stack, ErrorContextCallback};

use crate::replication::worker_internal::{
    am_leader_apply_worker, am_parallel_apply_worker, apply_dispatch, apply_error_callback,
    apply_error_context_stack, apply_spooled_messages, dsm_handle, dsm_segment,
    invalidate_syncing_table_states, logicalrep_pa_worker_stop, logicalrep_worker_attach,
    logicalrep_worker_launch, maybe_reread_subscription, set_apply_error_context_origin,
    shm_mq_handle, store_flush_position, stream_cleanup_files, stream_start_internal,
    AllTablesyncsReady, ApplyContext, ApplyMessageContext, InitializingApplyWorker,
    LogicalRepStreamAbortData, LogicalRepWorker, MyLogicalRepWorker, MyParallelShared,
    MySubscription, ParallelApplyWorkerInfo, ParallelApplyWorkerShared, ParallelTransState,
    PartialFileSetState, ReplicationOriginNameForLogicalRep, Subscription, TimestampTz,
    WORKERTYPE_PARALLEL_APPLY,
};
use crate::replication::worker_internal::{
    FS_EMPTY, FS_READY, FS_SERIALIZE_DONE, FS_SERIALIZE_IN_PROGRESS, PARALLEL_TRANS_FINISHED,
    PARALLEL_TRANS_STARTED, PARALLEL_TRANS_UNKNOWN,
};

// ----- gettext no-op: C `_(x)` marks a string for translation; identity here. -----
// Takes a NUL-terminated CStr and returns its `*const c_char`, mirroring the C
// `_()` macro which returns `char *`.
#[inline]
fn _(s: &CStr) -> *const c_char {
    s.as_ptr()
}

const PG_LOGICAL_APPLY_SHM_MAGIC: uint32 = 0x787ca067;

// DSM keys for parallel apply worker. Unlike other parallel execution code,
// since we don't need to worry about DSM keys conflicting with plan_node_id we
// can use small integers.
const PARALLEL_APPLY_KEY_SHARED: uint64 = 1;
const PARALLEL_APPLY_KEY_MQ: uint64 = 2;
const PARALLEL_APPLY_KEY_ERROR_QUEUE: uint64 = 3;

// Queue size of DSM, 16 MB for now.
const DSM_QUEUE_SIZE: Size = 16 * 1024 * 1024;

// Error queue size of DSM. It is desirable to make it large enough that a
// typical ErrorResponse can be sent without blocking. That way, a worker that
// errors out can write the whole message into the queue and terminate without
// waiting for the user backend.
const DSM_ERROR_QUEUE_SIZE: Size = 16 * 1024;

// There are three fields in each message received by the parallel apply
// worker: start_lsn, end_lsn and send_time. Because we have updated these
// statistics in the leader apply worker, we can ignore these fields in the
// parallel apply worker (see function LogicalRepApplyLoop).
const SIZE_STATS_MESSAGE: usize =
    2 * core::mem::size_of::<XLogRecPtr>() + core::mem::size_of::<TimestampTz>();

// The type of session-level lock on a transaction being applied on a logical
// replication subscriber.
const PARALLEL_APPLY_LOCK_STREAM: uint16 = 0;
const PARALLEL_APPLY_LOCK_XACT: uint16 = 1;

// Hash table entry to map xid to the parallel apply worker state.
#[repr(C)]
struct ParallelApplyWorkerEntry {
    xid: TransactionId, // Hash key -- must be first
    winfo: *mut ParallelApplyWorkerInfo,
}

// A hash table used to cache the state of streaming transactions being applied
// by the parallel apply workers.
static mut ParallelApplyTxnHash: *mut HTAB = null_mut();

// A list (pool) of active parallel apply workers. The information for
// the new worker is added to the list after successfully launching it. The
// list entry is removed if there are already enough workers in the worker
// pool at the end of the transaction. For more information about the worker
// pool, see comments atop this file.
static mut ParallelApplyWorkerPool: *mut List = NIL;

// Information shared between leader apply worker and parallel apply worker.
// (MyParallelShared is the real definition; declared extern in worker_internal.)

// Is there a message sent by a parallel apply worker that the leader apply
// worker needs to receive?
#[no_mangle]
pub static mut ParallelApplyMessagePending: sig_atomic_t = 0; // false

// Cache the parallel apply worker information required for applying the
// current streaming transaction. It is used to save the cost of searching the
// hash table when applying the changes between STREAM_START and STREAM_STOP.
static mut stream_apply_worker: *mut ParallelApplyWorkerInfo = null_mut();

// A list to maintain subtransactions, if any.
static mut subxactlist: *mut List = NIL;

// =====================================================================
// TODO(pg-port): dependencies that live in other (not-yet-ported) .c files.
// =====================================================================

type sig_atomic_t = c_int;
type uint64 = u64;

// TODO(pg-port): shm_toc (storage/shm_toc.h)
enum shm_toc {}

// TODO(pg-port): shm_mq (storage/shm_mq.h)
enum shm_mq {}

// TODO(pg-port): shm_mq_result (storage/shm_mq.h)
#[derive(PartialEq, Eq, Clone, Copy)]
enum shm_mq_result {
    SHM_MQ_SUCCESS,
    SHM_MQ_WOULD_BLOCK,
    SHM_MQ_DETACHED,
}
use shm_mq_result::*;

// TODO(pg-port): shm_toc_estimator (storage/shm_toc.h)
#[repr(C)]
struct shm_toc_estimator {
    number_of_keys: Size,
    space_for_chunks: Size,
}

// TODO(pg-port): StringInfoData (lib/stringinfo.h)
#[repr(C)]
struct StringInfoData {
    data: *mut c_char,
    len: c_int,
    maxlen: c_int,
    cursor: c_int,
}
type StringInfo = *mut StringInfoData;

// TODO(pg-port): ErrorData (utils/elog.h)
#[repr(C)]
struct ErrorData {
    context: *mut c_char,
    len: c_int,
}

// TODO(pg-port): HTAB (utils/hsearch.h)
enum HTAB {}

// TODO(pg-port): HASHCTL (utils/hsearch.h)
#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
    hcxt: MemoryContext,
}

// TODO(pg-port): HASHACTION (utils/hsearch.h)
#[derive(PartialEq, Eq, Clone, Copy)]
enum HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
}
use HASHACTION::*;

const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0020;
const HASH_CONTEXT: c_int = 0x0400;

// TODO(pg-port): PGPROC / MyProc (storage/proc.h)
type PGPROC = c_void;
extern "C" {
    static mut MyProc: *mut PGPROC;
    static mut MyLatch: *mut c_void;
    static mut MyBgworkerEntry: *mut BackgroundWorker;
    static mut ShutdownRequestPending: bool;
    static mut ConfigReloadPending: bool;
    static mut InterruptPending: bool;
    static mut replorigin_session_origin: RepOriginId;
    static mut replorigin_session_origin_lsn: XLogRecPtr;
    static mut replorigin_session_origin_timestamp: TimestampTz;
    static mut max_parallel_apply_workers_per_subscription: c_int;
    static mut debug_logical_replication_streaming: c_int;
}

// TODO(pg-port): BackgroundWorker (postmaster/bgworker.h)
#[repr(C)]
struct BackgroundWorker {
    bgw_extra: [c_char; 0],
}

type RepOriginId = uint16;

const NAMEDATALEN: usize = 64;
const INVALID_PROC_NUMBER: c_int = -1;
const PROCSIG_PARALLEL_APPLY_MESSAGE: c_int = 0;

// TODO(pg-port): debug_logical_replication_streaming values (utils/guc / logical worker)
const DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE: c_int = 1;

// TODO(pg-port): lock modes (storage/lockdefs.h)
const AccessShareLock: LOCKMODE = 1;
const AccessExclusiveLock: LOCKMODE = 8;

// TODO(pg-port): wait flags (storage/latch.h)
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 3;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// TODO(pg-port): wait events (utils/wait_event.h)
const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN: uint32 = 0;
const WAIT_EVENT_LOGICAL_APPLY_SEND_DATA: uint32 = 0;
const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE: uint32 = 0;

// TODO(pg-port): pgstat activity states (utils/backend_status.h)
const STATE_IDLE: c_int = 0;

// TODO(pg-port): GUC SIGHUP context (utils/guc.h)
const PGC_SIGHUP: c_int = 1;

// TODO(pg-port): ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE (utils/errcodes.h)
// folded into ereport "C also:" comments below.

// TODO(pg-port): shm_toc_initialize_estimator (storage/shm_toc.h)
unsafe fn shm_toc_initialize_estimator(e: *mut shm_toc_estimator) { crate::storage::ipc::shm_toc::shm_toc_initialize_estimator(e as _) }

// TODO(pg-port): shm_toc_estimate_chunk (storage/shm_toc.h)
unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {
    unimplemented!()
}

// TODO(pg-port): shm_toc_estimate_keys (storage/shm_toc.h)
unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: Size) {
    unimplemented!()
}

// TODO(pg-port): shm_toc_estimate (storage/shm_toc.h)
unsafe fn shm_toc_estimate(e: *mut shm_toc_estimator) -> Size { crate::storage::ipc::shm_toc::shm_toc_estimate(e as _) }

// TODO(pg-port): shm_toc_create (storage/shm_toc.h)
unsafe fn shm_toc_create(magic: uint64, address: *mut c_void, nbytes: Size) -> *mut shm_toc { crate::storage::ipc::shm_toc::shm_toc_create(magic as _, address as _, nbytes as _) }

// TODO(pg-port): shm_toc_attach (storage/shm_toc.h)
unsafe fn shm_toc_attach(magic: uint64, address: *mut c_void) -> *mut shm_toc { crate::storage::ipc::shm_toc::shm_toc_attach(magic as _, address as _) }

// TODO(pg-port): shm_toc_allocate (storage/shm_toc.h)
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): shm_toc_insert (storage/shm_toc.h)
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: uint64, _address: *mut c_void) {
    unimplemented!()
}

// TODO(pg-port): shm_toc_lookup (storage/shm_toc.h)
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: uint64, _noError: bool) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): dsm_create (storage/dsm.h)
unsafe fn dsm_create(size: Size, flags: c_int) -> *mut dsm_segment { crate::storage::ipc::dsm::dsm_create(size as _, flags as _) }

// TODO(pg-port): dsm_attach (storage/dsm.h)
unsafe fn dsm_attach(h: dsm_handle) -> *mut dsm_segment { crate::storage::ipc::dsm::dsm_attach(h) }

// TODO(pg-port): dsm_detach (storage/dsm.h)
unsafe fn dsm_detach(seg: *mut dsm_segment) { crate::storage::ipc::dsm::dsm_detach(seg as _) }

// TODO(pg-port): dsm_segment_address (storage/dsm.h)
unsafe fn dsm_segment_address(seg: *mut dsm_segment) -> *mut c_void { crate::storage::ipc::dsm::dsm_segment_address(seg as _) }

// TODO(pg-port): dsm_segment_handle (storage/dsm.h)
unsafe fn dsm_segment_handle(seg: *mut dsm_segment) -> dsm_handle { crate::storage::ipc::dsm::dsm_segment_handle(seg as _) }

// TODO(pg-port): shm_mq_create (storage/shm_mq.h)
unsafe fn shm_mq_create(address: *mut c_void, size: Size) -> *mut shm_mq { crate::storage::ipc::shm_mq::shm_mq_create(address as _, size as _) }

// TODO(pg-port): shm_mq_set_sender (storage/shm_mq.h)
unsafe fn shm_mq_set_sender(mq: *mut shm_mq, proc: *mut PGPROC) { crate::storage::ipc::shm_mq::shm_mq_set_sender(mq as _, proc as _) }

// TODO(pg-port): shm_mq_set_receiver (storage/shm_mq.h)
unsafe fn shm_mq_set_receiver(mq: *mut shm_mq, proc: *mut PGPROC) { crate::storage::ipc::shm_mq::shm_mq_set_receiver(mq as _, proc as _) }

// TODO(pg-port): shm_mq_attach (storage/shm_mq.h)
unsafe fn shm_mq_attach(
    mq: *mut shm_mq,
    seg: *mut dsm_segment,
    handle: *mut c_void,
) -> *mut shm_mq_handle { crate::storage::ipc::shm_mq::shm_mq_attach(mq as _, seg as _, handle as _) }

// TODO(pg-port): shm_mq_detach (storage/shm_mq.h)
unsafe fn shm_mq_detach(mqh: *mut shm_mq_handle) { crate::storage::ipc::shm_mq::shm_mq_detach(mqh as _) }

// TODO(pg-port): shm_mq_send (storage/shm_mq.h)
unsafe fn shm_mq_send(
    mqh: *mut shm_mq_handle,
    nbytes: Size,
    data: *const c_void,
    nowait: bool,
    force_flush: bool,
) -> shm_mq_result { crate::storage::ipc::shm_mq::shm_mq_send(mqh as _, nbytes, data as _, nowait, force_flush) }

// TODO(pg-port): shm_mq_receive (storage/shm_mq.h)
unsafe fn shm_mq_receive(
    mqh: *mut shm_mq_handle,
    nbytesp: *mut Size,
    datap: *mut *mut c_void,
    nowait: bool,
) -> shm_mq_result { crate::storage::ipc::shm_mq::shm_mq_receive(mqh as _, nbytesp as _, datap as _, nowait) }

// TODO(pg-port): SpinLockInit (storage/spin.h)
unsafe fn SpinLockInit(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockInit(_lock as _)
}

// TODO(pg-port): SpinLockAcquire (storage/spin.h)
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockAcquire(_lock as _)
}

// TODO(pg-port): SpinLockRelease (storage/spin.h)
unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockRelease(_lock as _)
}

// TODO(pg-port): pg_atomic_init_u32 (port/atomics.h)
unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: uint32) { crate::backend_link_shims::pg_atomic_init_u32(ptr as _, val as _) }

// TODO(pg-port): pg_atomic_read_u32 (port/atomics.h)
unsafe fn pg_atomic_read_u32(_ptr: *mut pg_atomic_uint32) -> uint32 {
    unimplemented!()
}

// TODO(pg-port): pg_atomic_sub_fetch_u32 (port/atomics.h)
unsafe fn pg_atomic_sub_fetch_u32(_ptr: *mut pg_atomic_uint32, _sub: uint32) -> uint32 {
    unimplemented!()
}

// TODO(pg-port): WaitLatch (storage/latch.h)
unsafe fn WaitLatch(
    latch: *mut c_void,
    wakeEvents: c_int,
    timeout: c_long,
    wait_event_info: uint32,
) -> c_int { crate::storage::ipc::latch::WaitLatch(latch as _, wakeEvents as _, timeout as _, wait_event_info as _) }

// TODO(pg-port): ResetLatch (storage/latch.h)
unsafe fn ResetLatch(latch: *mut c_void) { crate::storage::ipc::latch::ResetLatch(latch as _) }

// TODO(pg-port): SetLatch (storage/latch.h)
unsafe fn SetLatch(latch: *mut c_void) { crate::storage::ipc::latch::SetLatch(latch as _) }

// TODO(pg-port): hash_create (utils/hsearch.h)
unsafe fn hash_create(
    tabname: *const c_char,
    nelem: c_long,
    info: *mut HASHCTL,
    flags: c_int,
) -> *mut HTAB { crate::utils::hash::dynahash::hash_create(tabname as _, nelem as _, info as _, flags as _) }

// TODO(pg-port): hash_search (utils/hsearch.h)
unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: HASHACTION,
    foundPtr: *mut bool,
) -> *mut c_void { todo!("TODO(pg-port): hash_search") }

// TODO(pg-port): GetCurrentTimestamp (utils/timestamp)
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}

// TODO(pg-port): TimestampDifferenceExceeds (utils/timestamp)
unsafe fn TimestampDifferenceExceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: c_int,
) -> bool { crate::utils::adt::timestamp::TimestampDifferenceExceeds(start_time as _, stop_time as _, msec as _) }

// TODO(pg-port): IsTransactionState (access/xact.h)
unsafe fn IsTransactionState() -> bool {
    unimplemented!()
}

// TODO(pg-port): IsTransactionBlock (access/xact.h)
unsafe fn IsTransactionBlock() -> bool { crate::access::transam::xact::IsTransactionBlock() }

// TODO(pg-port): StartTransactionCommand (access/xact.h)
unsafe fn StartTransactionCommand() { crate::access::transam::xact::StartTransactionCommand() }

// TODO(pg-port): CommitTransactionCommand (access/xact.h)
unsafe fn CommitTransactionCommand() { crate::access::transam::xact::CommitTransactionCommand() }

// TODO(pg-port): AbortCurrentTransaction (access/xact.h)
unsafe fn AbortCurrentTransaction() { crate::access::transam::xact::AbortCurrentTransaction() }

// TODO(pg-port): BeginTransactionBlock (access/xact.h)
unsafe fn BeginTransactionBlock() -> bool { todo!("TODO(pg-port): BeginTransactionBlock") }

// TODO(pg-port): EndTransactionBlock (access/xact.h)
unsafe fn EndTransactionBlock(chain: bool) -> bool { crate::access::transam::xact::EndTransactionBlock(chain) }

// TODO(pg-port): DefineSavepoint (access/xact.h)
unsafe fn DefineSavepoint(name: *const c_char) { crate::access::transam::xact::DefineSavepoint(name as _) }

// TODO(pg-port): RollbackToSavepoint (access/xact.h)
unsafe fn RollbackToSavepoint(name: *const c_char) { crate::access::transam::xact::RollbackToSavepoint(name as _) }

// TODO(pg-port): TopTransactionContext (utils/memutils.h)
extern "C" {
    static mut TopTransactionContext: MemoryContext;
}

// TODO(pg-port): pqsignal (libpq/pqsignal.h)
unsafe fn pqsignal(signo: c_int, func: extern "C" fn(c_int)) { todo!("TODO(pg-port): pqsignal") }

// TODO(pg-port): signal handlers (postmaster/interrupt.h, tcop/tcopprot.h)
extern "C" {
    fn SignalHandlerForConfigReload(signo: c_int);
    fn die(signo: c_int);
    fn SignalHandlerForShutdownRequest(signo: c_int);
}

const SIGHUP: c_int = 1;
const SIGTERM: c_int = 15;
const SIGUSR2: c_int = 31;

// TODO(pg-port): BackgroundWorkerUnblockSignals (postmaster/bgworker.h)
unsafe fn BackgroundWorkerUnblockSignals() { crate::postmaster::bgworker::BackgroundWorkerUnblockSignals() }

// TODO(pg-port): CHECK_FOR_INTERRUPTS (miscadmin.h)
unsafe fn CHECK_FOR_INTERRUPTS() {
    unimplemented!()
}

// TODO(pg-port): HOLD_INTERRUPTS / RESUME_INTERRUPTS (miscadmin.h)
unsafe fn HOLD_INTERRUPTS() { crate::miscadmin::HOLD_INTERRUPTS() }

unsafe fn RESUME_INTERRUPTS() { crate::miscadmin::RESUME_INTERRUPTS() }

// TODO(pg-port): proc_exit (storage/ipc.h)
unsafe fn proc_exit(code: c_int) { crate::storage::ipc::ipc::proc_exit(code as _) }

// TODO(pg-port): before_shmem_exit (storage/ipc.h)
unsafe fn before_shmem_exit(function: unsafe extern "C" fn(c_int, Datum), arg: Datum) { crate::storage::ipc::ipc::before_shmem_exit(function, arg as _) }

// TODO(pg-port): ProcessConfigFile (utils/guc.h)
unsafe fn ProcessConfigFile(context: c_int) { crate::utils::misc::guc::ProcessConfigFile(context as _) }

// TODO(pg-port): SendProcSignal (storage/procsignal.h)
unsafe fn SendProcSignal(pid: pid_t, reason: c_int, procNumber: c_int) { crate::storage::ipc::procsignal::SendProcSignal(pid, reason as _, procNumber as _); }

type pid_t = c_int;

// TODO(pg-port): pq_getmsgbyte / pq_parse_errornotice (libpq/pqformat.h)
unsafe fn pq_getmsgbyte(msg: StringInfo) -> c_int { crate::libpq::pqformat::pq_getmsgbyte(msg as _) }

unsafe fn pq_parse_errornotice(msg: StringInfo, edata: *mut ErrorData) { crate::libpq::pqmq::pq_parse_errornotice(msg as _, edata as _) }

// TODO(pg-port): pq_redirect_to_shm_mq / pq_set_parallel_leader (libpq/pqmq.h)
unsafe fn pq_redirect_to_shm_mq(seg: *mut dsm_segment, mqh: *mut shm_mq_handle) { crate::libpq::pqmq::pq_redirect_to_shm_mq(seg as _, mqh as _) }

unsafe fn pq_set_parallel_leader(pid: pid_t, procNumber: c_int) { crate::libpq::pqmq::pq_set_parallel_leader(pid, procNumber as _) }

// TODO(pg-port): initReadOnlyStringInfo / initStringInfo / appendBinaryStringInfo (lib/stringinfo.h)
unsafe fn initReadOnlyStringInfo(str: *mut StringInfoData, data: *mut c_char, len: c_int) { crate::lib::stringinfo::initReadOnlyStringInfo(str as _, data as _, len as _) }

unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!()
}

unsafe fn appendBinaryStringInfo(str: *mut StringInfoData, data: *const c_void, datalen: c_int) { crate::lib::stringinfo::appendBinaryStringInfo(str as _, data as _, datalen as _) }

// TODO(pg-port): psprintf / pstrdup wrappers (utils/builtins.h, utils/palloc.h)
unsafe fn psprintf_2(_fmt: *const c_char, _a: *const c_char, _b: *const c_char) -> *mut c_char {
    unimplemented!()
}

// TODO(pg-port): pgstat_report_activity (utils/backend_status.h)
unsafe fn pgstat_report_activity(_state: c_int, _cmd_str: *const c_char) {
    unimplemented!()
}

// TODO(pg-port): replorigin_by_name / replorigin_session_setup (replication/origin.h)
unsafe fn replorigin_by_name(roname: *const c_char, missing_ok: bool) -> RepOriginId { crate::replication::logical::origin::replorigin_by_name(roname as _, missing_ok) }

unsafe fn replorigin_session_setup(node: RepOriginId, acquired_by: c_int) { crate::replication::logical::origin::replorigin_session_setup(node as _, acquired_by as _) }

// TODO(pg-port): CacheRegisterSyscacheCallback (utils/inval.h)
unsafe fn CacheRegisterSyscacheCallback(
    cacheid: c_int,
    func: unsafe extern "C" fn(Datum, c_int, uint32),
    arg: Datum,
) { crate::utils::cache::inval::CacheRegisterSyscacheCallback(cacheid as _, func, arg as _) }

const SUBSCRIPTIONRELMAP: c_int = 68;

// TODO(pg-port): LockApplyTransactionForSession / UnlockApplyTransactionForSession (storage/lmgr.h)
unsafe fn LockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: uint16,
    lockmode: LOCKMODE,
) { crate::storage::lmgr::lmgr::LockApplyTransactionForSession(suboid as _, xid as _, objid as _, lockmode as _) }

unsafe fn UnlockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: uint16,
    lockmode: LOCKMODE,
) { crate::storage::lmgr::lmgr::UnlockApplyTransactionForSession(suboid as _, xid as _, objid as _, lockmode as _) }

// TODO(pg-port): MemSet (c.h)
unsafe fn MemSet(ptr: *mut c_void, val: c_int, len: usize) {
    core::ptr::write_bytes(ptr as *mut u8, val as u8, len);
}

// TODO(pg-port): subscription field accessors (catalog/pg_subscription.h)
unsafe fn Subscription_oid(_sub: *mut Subscription) -> Oid {
    unimplemented!()
}

unsafe fn Subscription_name(_sub: *mut Subscription) -> *mut c_char {
    unimplemented!()
}

unsafe fn Subscription_skiplsn(_sub: *mut Subscription) -> XLogRecPtr {
    unimplemented!()
}

// TODO(pg-port): MyLogicalRepWorker->stream_fileset usage (replication/worker_internal.h)
// FileSet is defined as c_void in worker_internal; we only copy it via shared.fileset.

// =====================================================================
// Local functions (forward declarations are implicit in Rust).
// =====================================================================

// Returns true if it is OK to start a parallel apply worker, false otherwise.
unsafe fn pa_can_start() -> bool {
    // Only leader apply workers can start parallel apply workers.
    if !am_leader_apply_worker() {
        return false;
    }

    // It is good to check for any change in the subscription parameter to
    // avoid the case where for a very long time the change doesn't get
    // reflected. This can happen when there is a constant flow of streaming
    // transactions that are handled by parallel apply workers.
    //
    // It is better to do it before the below checks so that the latest values
    // of subscription can be used for the checks.
    maybe_reread_subscription();

    // Don't start a new parallel apply worker if the subscription is not
    // using parallel streaming mode, or if the publisher does not support
    // parallel apply.
    if !(*MyLogicalRepWorker).parallel_apply {
        return false;
    }

    // Don't start a new parallel worker if user has set skiplsn as it's
    // possible that they want to skip the streaming transaction. For
    // streaming transactions, we need to serialize the transaction to a file
    // so that we can get the last LSN of the transaction to judge whether to
    // skip before starting to apply the change.
    //
    // One might think that we could allow parallelism if the first lsn of the
    // transaction is greater than skiplsn, but we don't send it with the
    // STREAM START message, and it doesn't seem worth sending the extra eight
    // bytes with the STREAM START to enable parallelism for this case.
    if !XLogRecPtrIsInvalid(Subscription_skiplsn(MySubscription)) {
        return false;
    }

    // For streaming transactions that are being applied using a parallel
    // apply worker, we cannot decide whether to apply the change for a
    // relation that is not in the READY state (see
    // should_apply_changes_for_rel) as we won't know remote_final_lsn by that
    // time. So, we don't start the new parallel apply worker in this case.
    if !AllTablesyncsReady() {
        return false;
    }

    true
}

// Set up a dynamic shared memory segment.
//
// We set up a control region that contains a fixed-size worker info
// (ParallelApplyWorkerShared), a message queue, and an error queue.
//
// Returns true on success, false on failure.
unsafe fn pa_setup_dsm(winfo: *mut ParallelApplyWorkerInfo) -> bool {
    let mut e: shm_toc_estimator = core::mem::zeroed();
    let segsize: Size;
    let seg: *mut dsm_segment;
    let toc: *mut shm_toc;
    let shared: *mut ParallelApplyWorkerShared;
    let mut mq: *mut shm_mq;
    let queue_size: Size = DSM_QUEUE_SIZE;
    let error_queue_size: Size = DSM_ERROR_QUEUE_SIZE;

    // Estimate how much shared memory we need.
    //
    // Because the TOC machinery may choose to insert padding of oddly-sized
    // requests, we must estimate each chunk separately.
    //
    // We need one key to register the location of the header, and two other
    // keys to track the locations of the message queue and the error message
    // queue.
    shm_toc_initialize_estimator(&mut e);
    shm_toc_estimate_chunk(&mut e, core::mem::size_of::<ParallelApplyWorkerShared>());
    shm_toc_estimate_chunk(&mut e, queue_size);
    shm_toc_estimate_chunk(&mut e, error_queue_size);

    shm_toc_estimate_keys(&mut e, 3);
    segsize = shm_toc_estimate(&mut e);

    // Create the shared memory segment and establish a table of contents.
    seg = dsm_create(shm_toc_estimate(&mut e), 0);
    if seg.is_null() {
        return false;
    }

    toc = shm_toc_create(
        PG_LOGICAL_APPLY_SHM_MAGIC as uint64,
        dsm_segment_address(seg),
        segsize,
    );

    // Set up the header region.
    shared = shm_toc_allocate(toc, core::mem::size_of::<ParallelApplyWorkerShared>())
        as *mut ParallelApplyWorkerShared;
    SpinLockInit(&mut (*shared).mutex);

    (*shared).xact_state = PARALLEL_TRANS_UNKNOWN;
    pg_atomic_init_u32(&mut (*shared).pending_stream_count, 0);
    (*shared).last_commit_end = InvalidXLogRecPtr;
    (*shared).fileset_state = FS_EMPTY;

    shm_toc_insert(toc, PARALLEL_APPLY_KEY_SHARED, shared as *mut c_void);

    // Set up message queue for the worker.
    mq = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);
    shm_toc_insert(toc, PARALLEL_APPLY_KEY_MQ, mq as *mut c_void);
    shm_mq_set_sender(mq, MyProc);

    // Attach the queue.
    (*winfo).mq_handle = shm_mq_attach(mq, seg, null_mut());

    // Set up error queue for the worker.
    mq = shm_mq_create(shm_toc_allocate(toc, error_queue_size), error_queue_size);
    shm_toc_insert(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, mq as *mut c_void);
    shm_mq_set_receiver(mq, MyProc);

    // Attach the queue.
    (*winfo).error_mq_handle = shm_mq_attach(mq, seg, null_mut());

    // Return results to caller.
    (*winfo).dsm_seg = seg;
    (*winfo).shared = shared;

    true
}

// Try to get a parallel apply worker from the pool. If none is available then
// start a new one.
unsafe fn pa_launch_parallel_worker() -> *mut ParallelApplyWorkerInfo {
    let oldcontext: MemoryContext;
    let launched: bool;
    let mut winfo: *mut ParallelApplyWorkerInfo;

    // Try to get an available parallel apply worker from the worker pool.
    foreach!(lc, ParallelApplyWorkerPool, {
        winfo = lfirst(current_cell!(lc)) as *mut ParallelApplyWorkerInfo;

        if !(*winfo).in_use {
            return winfo;
        }
    });

    // Start a new parallel apply worker.
    //
    // The worker info can be used for the lifetime of the worker process, so
    // create it in a permanent context.
    oldcontext = MemoryContextSwitchTo(ApplyContext);

    winfo = palloc0(core::mem::size_of::<ParallelApplyWorkerInfo>()) as *mut ParallelApplyWorkerInfo;

    // Setup shared memory.
    if !pa_setup_dsm(winfo) {
        MemoryContextSwitchTo(oldcontext);
        pfree(winfo as *mut c_void);
        return null_mut();
    }

    launched = logicalrep_worker_launch(
        WORKERTYPE_PARALLEL_APPLY,
        (*MyLogicalRepWorker).dbid,
        Subscription_oid(MySubscription),
        Subscription_name(MySubscription),
        (*MyLogicalRepWorker).userid,
        InvalidOid,
        dsm_segment_handle((*winfo).dsm_seg),
    );

    if launched {
        ParallelApplyWorkerPool = lappend(ParallelApplyWorkerPool, winfo as *mut c_void);
    } else {
        pa_free_worker_info(winfo);
        winfo = null_mut();
    }

    MemoryContextSwitchTo(oldcontext);

    winfo
}

// Allocate a parallel apply worker that will be used for the specified xid.
//
// We first try to get an available worker from the pool, if any and then try
// to launch a new worker. On successful allocation, remember the worker
// information in the hash table so that we can get it later for processing the
// streaming changes.
#[no_mangle]
pub unsafe extern "C" fn pa_allocate_worker(xid: TransactionId) {
    let mut found: bool = false;
    let mut winfo: *mut ParallelApplyWorkerInfo;
    let entry: *mut ParallelApplyWorkerEntry;

    if !pa_can_start() {
        return;
    }

    winfo = pa_launch_parallel_worker();
    if winfo.is_null() {
        return;
    }

    // First time through, initialize parallel apply worker state hashtable.
    if ParallelApplyTxnHash.is_null() {
        let mut ctl: HASHCTL = core::mem::zeroed();

        MemSet(&mut ctl as *mut _ as *mut c_void, 0, core::mem::size_of::<HASHCTL>());
        ctl.keysize = core::mem::size_of::<TransactionId>();
        ctl.entrysize = core::mem::size_of::<ParallelApplyWorkerEntry>();
        ctl.hcxt = ApplyContext;

        ParallelApplyTxnHash = hash_create(
            c"logical replication parallel apply workers hash".as_ptr(),
            16,
            &mut ctl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
        );
    }

    // Create an entry for the requested transaction.
    entry = hash_search(
        ParallelApplyTxnHash,
        &xid as *const _ as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut ParallelApplyWorkerEntry;
    if found {
        elog!(ERROR, "hash table corrupted");
    }

    // Update the transaction information in shared memory.
    SpinLockAcquire(&mut (*(*winfo).shared).mutex);
    (*(*winfo).shared).xact_state = PARALLEL_TRANS_UNKNOWN;
    (*(*winfo).shared).xid = xid;
    SpinLockRelease(&mut (*(*winfo).shared).mutex);

    (*winfo).in_use = true;
    (*winfo).serialize_changes = false;
    (*entry).winfo = winfo;
}

// Find the assigned worker for the given transaction, if any.
#[no_mangle]
pub unsafe extern "C" fn pa_find_worker(xid: TransactionId) -> *mut ParallelApplyWorkerInfo {
    let mut found: bool = false;
    let entry: *mut ParallelApplyWorkerEntry;

    if !TransactionIdIsValid(xid) {
        return null_mut();
    }

    if ParallelApplyTxnHash.is_null() {
        return null_mut();
    }

    // Return the cached parallel apply worker if valid.
    if !stream_apply_worker.is_null() {
        return stream_apply_worker;
    }

    // Find an entry for the requested transaction.
    entry = hash_search(
        ParallelApplyTxnHash,
        &xid as *const _ as *const c_void,
        HASH_FIND,
        &mut found,
    ) as *mut ParallelApplyWorkerEntry;
    if found {
        // The worker must not have exited.
        Assert!((*(*entry).winfo).in_use);
        return (*entry).winfo;
    }

    null_mut()
}

// Makes the worker available for reuse.
//
// This removes the parallel apply worker entry from the hash table so that it
// can't be used. If there are enough workers in the pool, it stops the worker
// and frees the corresponding info. Otherwise it just marks the worker as
// available for reuse.
//
// For more information about the worker pool, see comments atop this file.
unsafe fn pa_free_worker(winfo: *mut ParallelApplyWorkerInfo) {
    Assert!(!am_parallel_apply_worker());
    Assert!((*winfo).in_use);
    Assert!(pa_get_xact_state((*winfo).shared) == PARALLEL_TRANS_FINISHED);

    if hash_search(
        ParallelApplyTxnHash,
        &(*(*winfo).shared).xid as *const _ as *const c_void,
        HASH_REMOVE,
        null_mut(),
    )
    .is_null()
    {
        elog!(ERROR, "hash table corrupted");
    }

    // Stop the worker if there are enough workers in the pool.
    //
    // XXX Additionally, we also stop the worker if the leader apply worker
    // serialize part of the transaction data due to a send timeout. This is
    // because the message could be partially written to the queue and there
    // is no way to clean the queue other than resending the message until it
    // succeeds. Instead of trying to send the data which anyway would have
    // been serialized and then letting the parallel apply worker deal with
    // the spurious message, we stop the worker.
    if (*winfo).serialize_changes
        || list_length(ParallelApplyWorkerPool)
            > (max_parallel_apply_workers_per_subscription / 2)
    {
        logicalrep_pa_worker_stop(winfo);
        pa_free_worker_info(winfo);

        return;
    }

    (*winfo).in_use = false;
    (*winfo).serialize_changes = false;
}

// Free the parallel apply worker information and unlink the files with
// serialized changes if any.
unsafe fn pa_free_worker_info(winfo: *mut ParallelApplyWorkerInfo) {
    Assert!(!winfo.is_null());

    if !(*winfo).mq_handle.is_null() {
        shm_mq_detach((*winfo).mq_handle);
    }

    if !(*winfo).error_mq_handle.is_null() {
        shm_mq_detach((*winfo).error_mq_handle);
    }

    // Unlink the files with serialized changes.
    if (*winfo).serialize_changes {
        stream_cleanup_files((*MyLogicalRepWorker).subid, (*(*winfo).shared).xid);
    }

    if !(*winfo).dsm_seg.is_null() {
        dsm_detach((*winfo).dsm_seg);
    }

    // Remove from the worker pool.
    ParallelApplyWorkerPool = list_delete_ptr(ParallelApplyWorkerPool, winfo as *mut c_void);

    pfree(winfo as *mut c_void);
}

// Detach the error queue for all parallel apply workers.
#[no_mangle]
pub unsafe extern "C" fn pa_detach_all_error_mq() {
    foreach!(lc, ParallelApplyWorkerPool, {
        let winfo = lfirst(current_cell!(lc)) as *mut ParallelApplyWorkerInfo;

        if !(*winfo).error_mq_handle.is_null() {
            shm_mq_detach((*winfo).error_mq_handle);
            (*winfo).error_mq_handle = null_mut();
        }
    });
}

// Check if there are any pending spooled messages.
unsafe fn pa_has_spooled_message_pending() -> bool {
    let fileset_state: PartialFileSetState;

    fileset_state = pa_get_fileset_state();

    fileset_state != FS_EMPTY
}

// Replay the spooled messages once the leader apply worker has finished
// serializing changes to the file.
//
// Returns false if there aren't any pending spooled messages, true otherwise.
unsafe fn pa_process_spooled_messages_if_required() -> bool {
    let mut fileset_state: PartialFileSetState;

    fileset_state = pa_get_fileset_state();

    if fileset_state == FS_EMPTY {
        return false;
    }

    // If the leader apply worker is busy serializing the partial changes then
    // acquire the stream lock now and wait for the leader worker to finish
    // serializing the changes. Otherwise, the parallel apply worker won't get
    // a chance to receive a STREAM_STOP (and acquire the stream lock) until
    // the leader had serialized all changes which can lead to undetected
    // deadlock.
    //
    // Note that the fileset state can be FS_SERIALIZE_DONE once the leader
    // worker has finished serializing the changes.
    if fileset_state == FS_SERIALIZE_IN_PROGRESS {
        pa_lock_stream((*MyParallelShared).xid, AccessShareLock);
        pa_unlock_stream((*MyParallelShared).xid, AccessShareLock);

        fileset_state = pa_get_fileset_state();
    }

    // We cannot read the file immediately after the leader has serialized all
    // changes to the file because there may still be messages in the memory
    // queue. We will apply all spooled messages the next time we call this
    // function and that will ensure there are no messages left in the memory
    // queue.
    if fileset_state == FS_SERIALIZE_DONE {
        pa_set_fileset_state(MyParallelShared, FS_READY);
    } else if fileset_state == FS_READY {
        apply_spooled_messages(
            &mut (*MyParallelShared).fileset,
            (*MyParallelShared).xid,
            InvalidXLogRecPtr,
        );
        pa_set_fileset_state(MyParallelShared, FS_EMPTY);
    }

    true
}

// Interrupt handler for main loop of parallel apply worker.
unsafe fn ProcessParallelApplyInterrupts() {
    CHECK_FOR_INTERRUPTS();

    if ShutdownRequestPending {
        ereport!(
            LOG,
            errmsg!(
                "logical replication parallel apply worker for subscription \"{}\" has finished",
                CStr::from_ptr(Subscription_name(MySubscription)).to_string_lossy()
            )
        );

        proc_exit(0);
    }

    if ConfigReloadPending {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
    }
}

// Parallel apply worker main loop.
unsafe fn LogicalParallelApplyLoop(mqh: *mut shm_mq_handle) {
    let mut shmq_res: shm_mq_result;
    let mut errcallback: ErrorContextCallback = core::mem::zeroed();
    let oldcxt: MemoryContext = CurrentMemoryContext;

    // Init the ApplyMessageContext which we clean up after each replication
    // protocol message.
    ApplyMessageContext = AllocSetContextCreate!(
        ApplyContext,
        c"ApplyMessageContext".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    // Push apply error context callback. Fields will be filled while applying
    // a change.
    errcallback.callback = apply_error_callback;
    errcallback.previous = error_context_stack;
    error_context_stack = &mut errcallback;

    loop {
        let mut data: *mut c_void = null_mut();
        let mut len: Size = 0;

        ProcessParallelApplyInterrupts();

        // Ensure we are reading the data into our memory context.
        MemoryContextSwitchTo(ApplyMessageContext);

        shmq_res = shm_mq_receive(mqh, &mut len, &mut data, true);

        if shmq_res == SHM_MQ_SUCCESS {
            let mut s: StringInfoData = core::mem::zeroed();
            let c: c_int;

            if len == 0 {
                elog!(ERROR, "invalid message length");
            }

            initReadOnlyStringInfo(&mut s, data as *mut c_char, len as c_int);

            // The first byte of messages sent from leader apply worker to
            // parallel apply workers can only be 'w'.
            c = pq_getmsgbyte(&mut s);
            if c != 'w' as c_int {
                elog!(ERROR, "unexpected message \"{}\"", c as u8 as char);
            }

            // Ignore statistics fields that have been updated by the leader
            // apply worker.
            //
            // XXX We can avoid sending the statistics fields from the leader
            // apply worker but for that, it needs to rebuild the entire
            // message by removing these fields which could be more work than
            // simply ignoring these fields in the parallel apply worker.
            s.cursor += SIZE_STATS_MESSAGE as c_int;

            apply_dispatch(&mut s as *mut StringInfoData as *mut _);
        } else if shmq_res == SHM_MQ_WOULD_BLOCK {
            // Replay the changes from the file, if any.
            if !pa_process_spooled_messages_if_required() {
                let rc: c_int;

                // Wait for more work.
                rc = WaitLatch(
                    MyLatch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    1000,
                    WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN,
                );

                if rc & WL_LATCH_SET != 0 {
                    ResetLatch(MyLatch);
                }
            }
        } else {
            Assert!(shmq_res == SHM_MQ_DETACHED);

            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            ereport!(
                ERROR,
                errmsg!("lost connection to the logical replication apply worker")
            );
        }

        MemoryContextReset(ApplyMessageContext);
        MemoryContextSwitchTo(oldcxt);
    }

    // Pop the error context stack.
    #[allow(unreachable_code)]
    {
        error_context_stack = errcallback.previous;

        MemoryContextSwitchTo(oldcxt);
    }
}

// Make sure the leader apply worker tries to read from our error queue one more
// time. This guards against the case where we exit uncleanly without sending
// an ErrorResponse, for example because some code calls proc_exit directly.
//
// Also explicitly detach from dsm segment to invoke on_dsm_detach callbacks,
// if any. See ParallelWorkerShutdown for details.
unsafe extern "C" fn pa_shutdown(_code: c_int, arg: Datum) {
    SendProcSignal(
        (*MyLogicalRepWorker).leader_pid,
        PROCSIG_PARALLEL_APPLY_MESSAGE,
        INVALID_PROC_NUMBER,
    );

    dsm_detach(DatumGetPointer(arg) as *mut dsm_segment);
}

// Parallel apply worker entry point.
#[no_mangle]
pub unsafe extern "C" fn ParallelApplyWorkerMain(main_arg: Datum) {
    let shared: *mut ParallelApplyWorkerShared;
    let mut handle: dsm_handle = 0;
    let seg: *mut dsm_segment;
    let toc: *mut shm_toc;
    let mut mq: *mut shm_mq;
    let mqh: *mut shm_mq_handle;
    let error_mqh: *mut shm_mq_handle;
    let originid: RepOriginId;
    let worker_slot: c_int = DatumGetInt32(main_arg);
    let mut originname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

    InitializingApplyWorker = true;

    // Setup signal handling.
    //
    // Note: We intentionally used SIGUSR2 to trigger a graceful shutdown
    // initiated by the leader apply worker. This helps to differentiate it
    // from the case where we abort the current transaction and exit on
    // receiving SIGTERM.
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    pqsignal(SIGUSR2, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();

    // Attach to the dynamic shared memory segment for the parallel apply, and
    // find its table of contents.
    //
    // Like parallel query, we don't need resource owner by this time. See
    // ParallelWorkerMain.
    core::ptr::copy_nonoverlapping(
        (*MyBgworkerEntry).bgw_extra.as_ptr() as *const u8,
        &mut handle as *mut dsm_handle as *mut u8,
        core::mem::size_of::<dsm_handle>(),
    );
    seg = dsm_attach(handle);
    if seg.is_null() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            ERROR,
            errmsg!("could not map dynamic shared memory segment")
        );
    }

    toc = shm_toc_attach(PG_LOGICAL_APPLY_SHM_MAGIC as uint64, dsm_segment_address(seg));
    if toc.is_null() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            ERROR,
            errmsg!("invalid magic number in dynamic shared memory segment")
        );
    }

    // Look up the shared information.
    shared = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_SHARED, false) as *mut ParallelApplyWorkerShared;
    MyParallelShared = shared;

    // Attach to the message queue.
    mq = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_MQ, false) as *mut shm_mq;
    shm_mq_set_receiver(mq, MyProc);
    mqh = shm_mq_attach(mq, seg, null_mut());

    // Primary initialization is complete. Now, we can attach to our slot.
    // This is to ensure that the leader apply worker does not write data to
    // the uninitialized memory queue.
    logicalrep_worker_attach(worker_slot);

    // Register the shutdown callback after we are attached to the worker
    // slot. This is to ensure that MyLogicalRepWorker remains valid when this
    // callback is invoked.
    before_shmem_exit(pa_shutdown, PointerGetDatum(seg as *mut c_void));

    SpinLockAcquire(&mut (*MyParallelShared).mutex);
    (*MyParallelShared).logicalrep_worker_generation = (*MyLogicalRepWorker).generation;
    (*MyParallelShared).logicalrep_worker_slot_no = worker_slot;
    SpinLockRelease(&mut (*MyParallelShared).mutex);

    // Attach to the error queue.
    mq = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, false) as *mut shm_mq;
    shm_mq_set_sender(mq, MyProc);
    error_mqh = shm_mq_attach(mq, seg, null_mut());

    pq_redirect_to_shm_mq(seg, error_mqh);
    pq_set_parallel_leader((*MyLogicalRepWorker).leader_pid, INVALID_PROC_NUMBER);

    (*MyLogicalRepWorker).last_recv_time = 0;
    (*MyLogicalRepWorker).last_send_time = 0;
    (*MyLogicalRepWorker).reply_time = 0;

    InitializeLogRepWorker();

    InitializingApplyWorker = false;

    // Setup replication origin tracking.
    StartTransactionCommand();
    ReplicationOriginNameForLogicalRep(
        Subscription_oid(MySubscription),
        InvalidOid,
        originname.as_mut_ptr(),
        core::mem::size_of::<[c_char; NAMEDATALEN]>(),
    );
    originid = replorigin_by_name(originname.as_ptr(), false);

    // The parallel apply worker doesn't need to monopolize this replication
    // origin which was already acquired by its leader process.
    replorigin_session_setup(originid, (*MyLogicalRepWorker).leader_pid);
    replorigin_session_origin = originid;
    CommitTransactionCommand();

    // Setup callback for syscache so that we know when something changes in
    // the subscription relation state.
    CacheRegisterSyscacheCallback(
        SUBSCRIPTIONRELMAP,
        invalidate_syncing_table_states,
        0 as Datum,
    );

    set_apply_error_context_origin(originname.as_mut_ptr());

    LogicalParallelApplyLoop(mqh);

    // The parallel apply worker must not get here because the parallel apply
    // worker will only stop when it receives a SIGTERM or SIGUSR2 from the
    // leader, or SIGINT from itself, or when there is an error. None of these
    // cases will allow the code to reach here.
    #[allow(unreachable_code)]
    {
        Assert!(false);
    }
}

// Handle receipt of an interrupt indicating a parallel apply worker message.
//
// Note: this is called within a signal handler! All we can do is set a flag
// that will cause the next CHECK_FOR_INTERRUPTS() to invoke
// ProcessParallelApplyMessages().
#[no_mangle]
pub unsafe extern "C" fn HandleParallelApplyMessageInterrupt() {
    InterruptPending = true;
    ParallelApplyMessagePending = true;
    SetLatch(MyLatch);
}

// Process a single protocol message received from a single parallel apply
// worker.
unsafe fn ProcessParallelApplyMessage(msg: StringInfo) {
    let msgtype: c_char;

    msgtype = pq_getmsgbyte(msg) as c_char;

    match msgtype as u8 as char {
        'E' => {
            // ErrorResponse
            let mut edata: ErrorData = core::mem::zeroed();

            // Parse ErrorResponse.
            pq_parse_errornotice(msg, &mut edata);

            // If desired, add a context line to show that this is a
            // message propagated from a parallel apply worker. Otherwise,
            // it can sometimes be confusing to understand what actually
            // happened.
            if !edata.context.is_null() {
                edata.context = psprintf_2(
                    c"%s\n%s".as_ptr(),
                    edata.context,
                    _(c"logical replication parallel apply worker"),
                );
            } else {
                edata.context = pstrdup(_(c"logical replication parallel apply worker"));
            }

            // Context beyond that should use the error context callbacks
            // that were in effect in LogicalRepApplyLoop().
            error_context_stack = apply_error_context_stack as *mut ErrorContextCallback;

            // The actual error must have been reported by the parallel
            // apply worker.
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            //         errcontext("%s", edata.context)
            ereport!(
                ERROR,
                errmsg!("logical replication parallel apply worker exited due to error")
            );
        }

        // Don't need to do anything about NoticeResponse and
        // NotifyResponse as the logical replication worker doesn't need
        // to send messages to the client.
        'N' | 'A' => {}

        _ => {
            elog!(
                ERROR,
                "unrecognized message type received from logical replication parallel apply worker: {} (message length {} bytes)",
                msgtype as u8 as char,
                (*msg).len
            );
        }
    }
}

// Handle any queued protocol messages received from parallel apply workers.
#[no_mangle]
pub unsafe extern "C" fn ProcessParallelApplyMessages() {
    let oldcontext: MemoryContext;

    static mut hpam_context: MemoryContext = null_mut();

    // This is invoked from ProcessInterrupts(), and since some of the
    // functions it calls contain CHECK_FOR_INTERRUPTS(), there is a potential
    // for recursive calls if more signals are received while this runs. It's
    // unclear that recursive entry would be safe, and it doesn't seem useful
    // even if it is safe, so let's block interrupts until done.
    HOLD_INTERRUPTS();

    // Moreover, CurrentMemoryContext might be pointing almost anywhere. We
    // don't want to risk leaking data into long-lived contexts, so let's do
    // our work here in a private context that we can reset on each use.
    if hpam_context.is_null() {
        // first time through?
        hpam_context = AllocSetContextCreate!(
            TopMemoryContext,
            c"ProcessParallelApplyMessages".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    } else {
        MemoryContextReset(hpam_context);
    }

    oldcontext = MemoryContextSwitchTo(hpam_context);

    ParallelApplyMessagePending = false;

    foreach!(lc, ParallelApplyWorkerPool, {
        let res: shm_mq_result;
        let mut nbytes: Size = 0;
        let mut data: *mut c_void = null_mut();
        let winfo = lfirst(current_cell!(lc)) as *mut ParallelApplyWorkerInfo;

        // The leader will detach from the error queue and set it to NULL
        // before preparing to stop all parallel apply workers, so we don't
        // need to handle error messages anymore. See
        // logicalrep_worker_detach.
        if (*winfo).error_mq_handle.is_null() {
            continue;
        }

        res = shm_mq_receive((*winfo).error_mq_handle, &mut nbytes, &mut data, true);

        if res == SHM_MQ_WOULD_BLOCK {
            continue;
        } else if res == SHM_MQ_SUCCESS {
            let mut msg: StringInfoData = core::mem::zeroed();

            initStringInfo(&mut msg);
            appendBinaryStringInfo(&mut msg, data, nbytes as c_int);
            ProcessParallelApplyMessage(&mut msg);
            pfree(msg.data as *mut c_void);
        } else {
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            ereport!(
                ERROR,
                errmsg!("lost connection to the logical replication parallel apply worker")
            );
        }
    });

    MemoryContextSwitchTo(oldcontext);

    // Might as well clear the context on our way out
    MemoryContextReset(hpam_context);

    RESUME_INTERRUPTS();
}

// Send the data to the specified parallel apply worker via shared-memory
// queue.
//
// Returns false if the attempt to send data via shared memory times out, true
// otherwise.
#[no_mangle]
pub unsafe extern "C" fn pa_send_data(
    winfo: *mut ParallelApplyWorkerInfo,
    nbytes: Size,
    data: *const c_void,
) -> bool {
    let mut rc: c_int;
    let mut result: shm_mq_result;
    let mut startTime: TimestampTz = 0;

    Assert!(!IsTransactionState());
    Assert!(!(*winfo).serialize_changes);

    // We don't try to send data to parallel worker for 'immediate' mode. This
    // is primarily used for testing purposes.
    if debug_logical_replication_streaming == DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE {
        return false;
    }

    // This timeout is a bit arbitrary but testing revealed that it is sufficient
    // to send the message unless the parallel apply worker is waiting on some
    // lock or there is a serious resource crunch. See the comments atop this file
    // to know why we are using a non-blocking way to send the message.
    const SHM_SEND_RETRY_INTERVAL_MS: c_long = 1000;
    const SHM_SEND_TIMEOUT_MS: c_int = 10000 - SHM_SEND_RETRY_INTERVAL_MS as c_int;

    loop {
        result = shm_mq_send((*winfo).mq_handle, nbytes, data, true, true);

        if result == SHM_MQ_SUCCESS {
            return true;
        } else if result == SHM_MQ_DETACHED {
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            ereport!(
                ERROR,
                errmsg!("could not send data to shared-memory queue")
            );
        }

        Assert!(result == SHM_MQ_WOULD_BLOCK);

        // Wait before retrying.
        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            SHM_SEND_RETRY_INTERVAL_MS,
            WAIT_EVENT_LOGICAL_APPLY_SEND_DATA,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        if startTime == 0 {
            startTime = GetCurrentTimestamp();
        } else if TimestampDifferenceExceeds(startTime, GetCurrentTimestamp(), SHM_SEND_TIMEOUT_MS) {
            return false;
        }
    }
}

// Switch to PARTIAL_SERIALIZE mode for the current transaction -- this means
// that the current data and any subsequent data for this transaction will be
// serialized to a file. This is done to prevent possible deadlocks with
// another parallel apply worker (refer to the comments atop this file).
#[no_mangle]
pub unsafe extern "C" fn pa_switch_to_partial_serialize(
    winfo: *mut ParallelApplyWorkerInfo,
    stream_locked: bool,
) {
    ereport!(
        LOG,
        errmsg!(
            "logical replication apply worker will serialize the remaining changes of remote transaction {} to a file",
            (*(*winfo).shared).xid
        )
    );

    // The parallel apply worker could be stuck for some reason (say waiting
    // on some lock by other backend), so stop trying to send data directly to
    // it and start serializing data to the file instead.
    (*winfo).serialize_changes = true;

    // Initialize the stream fileset.
    stream_start_internal((*(*winfo).shared).xid, true);

    // Acquires the stream lock if not already to make sure that the parallel
    // apply worker will wait for the leader to release the stream lock until
    // the end of the transaction.
    if !stream_locked {
        pa_lock_stream((*(*winfo).shared).xid, AccessExclusiveLock);
    }

    pa_set_fileset_state((*winfo).shared, FS_SERIALIZE_IN_PROGRESS);
}

// Wait until the parallel apply worker's transaction state has reached or
// exceeded the given xact_state.
unsafe fn pa_wait_for_xact_state(
    winfo: *mut ParallelApplyWorkerInfo,
    xact_state: ParallelTransState,
) {
    loop {
        // Stop if the transaction state has reached or exceeded the given
        // xact_state.
        if pa_get_xact_state((*winfo).shared) >= xact_state {
            break;
        }

        // Wait to be signalled.
        WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE,
        );

        // Reset the latch so we don't spin.
        ResetLatch(MyLatch);

        // An interrupt may have occurred while we were waiting.
        CHECK_FOR_INTERRUPTS();
    }
}

// Wait until the parallel apply worker's transaction finishes.
unsafe fn pa_wait_for_xact_finish(winfo: *mut ParallelApplyWorkerInfo) {
    // Wait until the parallel apply worker set the state to
    // PARALLEL_TRANS_STARTED which means it has acquired the transaction
    // lock. This is to prevent leader apply worker from acquiring the
    // transaction lock earlier than the parallel apply worker.
    pa_wait_for_xact_state(winfo, PARALLEL_TRANS_STARTED);

    // Wait for the transaction lock to be released. This is required to
    // detect deadlock among leader and parallel apply workers. Refer to the
    // comments atop this file.
    pa_lock_transaction((*(*winfo).shared).xid, AccessShareLock);
    pa_unlock_transaction((*(*winfo).shared).xid, AccessShareLock);

    // Check if the state becomes PARALLEL_TRANS_FINISHED in case the parallel
    // apply worker failed while applying changes causing the lock to be
    // released.
    if pa_get_xact_state((*winfo).shared) != PARALLEL_TRANS_FINISHED {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            ERROR,
            errmsg!("lost connection to the logical replication parallel apply worker")
        );
    }
}

// Set the transaction state for a given parallel apply worker.
#[no_mangle]
pub unsafe extern "C" fn pa_set_xact_state(
    wshared: *mut ParallelApplyWorkerShared,
    xact_state: ParallelTransState,
) {
    SpinLockAcquire(&mut (*wshared).mutex);
    (*wshared).xact_state = xact_state;
    SpinLockRelease(&mut (*wshared).mutex);
}

// Get the transaction state for a given parallel apply worker.
unsafe fn pa_get_xact_state(wshared: *mut ParallelApplyWorkerShared) -> ParallelTransState {
    let xact_state: ParallelTransState;

    SpinLockAcquire(&mut (*wshared).mutex);
    xact_state = (*wshared).xact_state;
    SpinLockRelease(&mut (*wshared).mutex);

    xact_state
}

// Cache the parallel apply worker information.
#[no_mangle]
pub unsafe extern "C" fn pa_set_stream_apply_worker(winfo: *mut ParallelApplyWorkerInfo) {
    stream_apply_worker = winfo;
}

// Form a unique savepoint name for the streaming transaction.
//
// Note that different subscriptions for publications on different nodes can
// receive same remote xid, so we need to use subscription id along with it.
//
// Returns the name in the supplied buffer.
unsafe fn pa_savepoint_name(suboid: Oid, xid: TransactionId, spname: *mut c_char, szsp: Size) {
    snprintf_sp(spname, szsp, suboid, xid);
}

// TODO(pg-port): snprintf(spname, szsp, "pg_sp_%u_%u", suboid, xid) (port/snprintf)
unsafe fn snprintf_sp(_spname: *mut c_char, _szsp: Size, _suboid: Oid, _xid: TransactionId) {
    unimplemented!()
}

// Define a savepoint for a subxact in parallel apply worker if needed.
//
// The parallel apply worker can figure out if a new subtransaction was
// started by checking if the new change arrived with a different xid. In that
// case define a named savepoint, so that we are able to rollback to it
// if required.
#[no_mangle]
pub unsafe extern "C" fn pa_start_subtrans(current_xid: TransactionId, top_xid: TransactionId) {
    if current_xid != top_xid && !list_member_xid(subxactlist, current_xid) {
        let oldctx: MemoryContext;
        let mut spname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

        pa_savepoint_name(
            Subscription_oid(MySubscription),
            current_xid,
            spname.as_mut_ptr(),
            core::mem::size_of::<[c_char; NAMEDATALEN]>(),
        );

        elog!(
            DEBUG1,
            "defining savepoint {} in logical replication parallel apply worker",
            CStr::from_ptr(spname.as_ptr()).to_string_lossy()
        );

        // We must be in transaction block to define the SAVEPOINT.
        if !IsTransactionBlock() {
            if !IsTransactionState() {
                StartTransactionCommand();
            }

            BeginTransactionBlock();
            CommitTransactionCommand();
        }

        DefineSavepoint(spname.as_ptr());

        // CommitTransactionCommand is needed to start a subtransaction after
        // issuing a SAVEPOINT inside a transaction block (see
        // StartSubTransaction()).
        CommitTransactionCommand();

        oldctx = MemoryContextSwitchTo(TopTransactionContext);
        subxactlist = lappend_xid(subxactlist, current_xid);
        MemoryContextSwitchTo(oldctx);
    }
}

// Reset the list that maintains subtransactions.
#[no_mangle]
pub unsafe extern "C" fn pa_reset_subtrans() {
    // We don't need to free this explicitly as the allocated memory will be
    // freed at the transaction end.
    subxactlist = NIL;
}

// Handle STREAM ABORT message when the transaction was applied in a parallel
// apply worker.
#[no_mangle]
pub unsafe extern "C" fn pa_stream_abort(abort_data: *mut LogicalRepStreamAbortData) {
    let xid: TransactionId = LogicalRepStreamAbortData_xid(abort_data);
    let subxid: TransactionId = LogicalRepStreamAbortData_subxid(abort_data);

    // Update origin state so we can restart streaming from correct position
    // in case of crash.
    replorigin_session_origin_lsn = LogicalRepStreamAbortData_abort_lsn(abort_data);
    replorigin_session_origin_timestamp = LogicalRepStreamAbortData_abort_time(abort_data);

    // If the two XIDs are the same, it's in fact abort of toplevel xact, so
    // just free the subxactlist.
    if subxid == xid {
        pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_FINISHED);

        // Release the lock as we might be processing an empty streaming
        // transaction in which case the lock won't be released during
        // transaction rollback.
        //
        // Note that it's ok to release the transaction lock before aborting
        // the transaction because even if the parallel apply worker dies due
        // to crash or some other reason, such a transaction would still be
        // considered aborted.
        pa_unlock_transaction(xid, AccessExclusiveLock);

        AbortCurrentTransaction();

        if IsTransactionBlock() {
            EndTransactionBlock(false);
            CommitTransactionCommand();
        }

        pa_reset_subtrans();

        pgstat_report_activity(STATE_IDLE, null());
    } else {
        // OK, so it's a subxact. Rollback to the savepoint.
        let mut i: c_int;
        let mut spname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

        pa_savepoint_name(
            Subscription_oid(MySubscription),
            subxid,
            spname.as_mut_ptr(),
            core::mem::size_of::<[c_char; NAMEDATALEN]>(),
        );

        elog!(
            DEBUG1,
            "rolling back to savepoint {} in logical replication parallel apply worker",
            CStr::from_ptr(spname.as_ptr()).to_string_lossy()
        );

        // Search the subxactlist, determine the offset tracked for the
        // subxact, and truncate the list.
        //
        // Note that for an empty sub-transaction we won't find the subxid
        // here.
        i = list_length(subxactlist) - 1;
        while i >= 0 {
            let xid_tmp: TransactionId = lfirst_xid(list_nth_cell(subxactlist, i));

            if xid_tmp == subxid {
                RollbackToSavepoint(spname.as_ptr());
                CommitTransactionCommand();
                subxactlist = list_truncate(subxactlist, i);
                break;
            }
            i -= 1;
        }
    }
}

// TODO(pg-port): LogicalRepStreamAbortData field accessors (replication/logicalproto.h)
unsafe fn LogicalRepStreamAbortData_xid(_d: *mut LogicalRepStreamAbortData) -> TransactionId {
    unimplemented!()
}

unsafe fn LogicalRepStreamAbortData_subxid(_d: *mut LogicalRepStreamAbortData) -> TransactionId {
    unimplemented!()
}

unsafe fn LogicalRepStreamAbortData_abort_lsn(_d: *mut LogicalRepStreamAbortData) -> XLogRecPtr {
    unimplemented!()
}

unsafe fn LogicalRepStreamAbortData_abort_time(_d: *mut LogicalRepStreamAbortData) -> TimestampTz {
    unimplemented!()
}

// Set the fileset state for a particular parallel apply worker. The fileset
// will be set once the leader worker serialized all changes to the file
// so that it can be used by parallel apply worker.
#[no_mangle]
pub unsafe extern "C" fn pa_set_fileset_state(
    wshared: *mut ParallelApplyWorkerShared,
    fileset_state: PartialFileSetState,
) {
    SpinLockAcquire(&mut (*wshared).mutex);
    (*wshared).fileset_state = fileset_state;

    if fileset_state == FS_SERIALIZE_DONE {
        Assert!(am_leader_apply_worker());
        Assert!(!(*MyLogicalRepWorker).stream_fileset.is_null());
        (*wshared).fileset = *(*MyLogicalRepWorker).stream_fileset;
    }

    SpinLockRelease(&mut (*wshared).mutex);
}

// Get the fileset state for the current parallel apply worker.
unsafe fn pa_get_fileset_state() -> PartialFileSetState {
    let fileset_state: PartialFileSetState;

    Assert!(am_parallel_apply_worker());

    SpinLockAcquire(&mut (*MyParallelShared).mutex);
    fileset_state = (*MyParallelShared).fileset_state;
    SpinLockRelease(&mut (*MyParallelShared).mutex);

    fileset_state
}

// Helper functions to acquire and release a lock for each stream block.
//
// Set locktag_field4 to PARALLEL_APPLY_LOCK_STREAM to indicate that it's a
// stream lock.
//
// Refer to the comments atop this file to see how the stream lock is used.
#[no_mangle]
pub unsafe extern "C" fn pa_lock_stream(xid: TransactionId, lockmode: LOCKMODE) {
    LockApplyTransactionForSession(
        (*MyLogicalRepWorker).subid,
        xid,
        PARALLEL_APPLY_LOCK_STREAM,
        lockmode,
    );
}

#[no_mangle]
pub unsafe extern "C" fn pa_unlock_stream(xid: TransactionId, lockmode: LOCKMODE) {
    UnlockApplyTransactionForSession(
        (*MyLogicalRepWorker).subid,
        xid,
        PARALLEL_APPLY_LOCK_STREAM,
        lockmode,
    );
}

// Helper functions to acquire and release a lock for each local transaction
// apply.
//
// Set locktag_field4 to PARALLEL_APPLY_LOCK_XACT to indicate that it's a
// transaction lock.
//
// Note that all the callers must pass a remote transaction ID instead of a
// local transaction ID as xid. This is because the local transaction ID will
// only be assigned while applying the first change in the parallel apply but
// it's possible that the first change in the parallel apply worker is blocked
// by a concurrently executing transaction in another parallel apply worker. We
// can only communicate the local transaction id to the leader after applying
// the first change so it won't be able to wait after sending the xact finish
// command using this lock.
//
// Refer to the comments atop this file to see how the transaction lock is
// used.
#[no_mangle]
pub unsafe extern "C" fn pa_lock_transaction(xid: TransactionId, lockmode: LOCKMODE) {
    LockApplyTransactionForSession(
        (*MyLogicalRepWorker).subid,
        xid,
        PARALLEL_APPLY_LOCK_XACT,
        lockmode,
    );
}

#[no_mangle]
pub unsafe extern "C" fn pa_unlock_transaction(xid: TransactionId, lockmode: LOCKMODE) {
    UnlockApplyTransactionForSession(
        (*MyLogicalRepWorker).subid,
        xid,
        PARALLEL_APPLY_LOCK_XACT,
        lockmode,
    );
}

// Decrement the number of pending streaming blocks and wait on the stream lock
// if there is no pending block available.
#[no_mangle]
pub unsafe extern "C" fn pa_decr_and_wait_stream_block() {
    Assert!(am_parallel_apply_worker());

    // It is only possible to not have any pending stream chunks when we are
    // applying spooled messages.
    if pg_atomic_read_u32(&mut (*MyParallelShared).pending_stream_count) == 0 {
        if pa_has_spooled_message_pending() {
            return;
        }

        elog!(ERROR, "invalid pending streaming chunk 0");
    }

    if pg_atomic_sub_fetch_u32(&mut (*MyParallelShared).pending_stream_count, 1) == 0 {
        pa_lock_stream((*MyParallelShared).xid, AccessShareLock);
        pa_unlock_stream((*MyParallelShared).xid, AccessShareLock);
    }
}

// Finish processing the streaming transaction in the leader apply worker.
#[no_mangle]
pub unsafe extern "C" fn pa_xact_finish(
    winfo: *mut ParallelApplyWorkerInfo,
    remote_lsn: XLogRecPtr,
) {
    Assert!(am_leader_apply_worker());

    // Unlock the shared object lock so that parallel apply worker can
    // continue to receive and apply changes.
    pa_unlock_stream((*(*winfo).shared).xid, AccessExclusiveLock);

    // Wait for that worker to finish. This is necessary to maintain commit
    // order which avoids failures due to transaction dependencies and
    // deadlocks.
    pa_wait_for_xact_finish(winfo);

    if !XLogRecPtrIsInvalid(remote_lsn) {
        store_flush_position(remote_lsn, (*(*winfo).shared).last_commit_end);
    }

    pa_free_worker(winfo);
}

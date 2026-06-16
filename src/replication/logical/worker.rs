/*-------------------------------------------------------------------------
 * worker.rs
 *   PostgreSQL logical replication worker (apply)
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 * NOTES
 *   This file contains the worker which applies logical changes as they come
 *   from remote logical replication stream.
 *
 *   The main worker (apply) is started by logical replication worker
 *   launcher for every enabled subscription in a database. It uses
 *   walsender protocol to communicate with publisher.
 *
 *   This module includes server facing code and shares libpqwalreceiver
 *   module with walreceiver for providing the libpq specific functionality.
 *
 *
 * STREAMED TRANSACTIONS
 * ---------------------
 * Streamed transactions (large transactions exceeding a memory limit on the
 * upstream) are applied using one of two approaches:
 *
 * 1) Write to temporary files and apply when the final commit arrives
 *
 * This approach is used when the user has set the subscription's streaming
 * option as on.
 *
 * Unlike the regular (non-streamed) case, handling streamed transactions has
 * to handle aborts of both the toplevel transaction and subtransactions. This
 * is achieved by tracking offsets for subtransactions, which is then used
 * to truncate the file with serialized changes.
 *
 * The files are placed in tmp file directory by default, and the filenames
 * include both the XID of the toplevel transaction and OID of the
 * subscription. This is necessary so that different workers processing a
 * remote transaction with the same XID doesn't interfere.
 *
 * We use BufFiles instead of using normal temporary files because (a) the
 * BufFile infrastructure supports temporary files that exceed the OS file size
 * limit, (b) provides a way for automatic clean up on the error and (c) provides
 * a way to survive these files across local transactions and allow to open and
 * close at stream start and close. We decided to use FileSet
 * infrastructure as without that it deletes the files on the closure of the
 * file and if we decide to keep stream files open across the start/stop stream
 * then it will consume a lot of memory (more than 8K for each BufFile and
 * there could be multiple such BufFiles as the subscriber could receive
 * multiple start/stop streams for different transactions before getting the
 * commit). Moreover, if we don't use FileSet then we also need to invent
 * a new way to pass filenames to BufFile APIs so that we are allowed to open
 * the file we desired across multiple stream-open calls for the same
 * transaction.
 *
 * 2) Parallel apply workers.
 *
 * This approach is used when the user has set the subscription's streaming
 * option as parallel. See logical/applyparallelworker.c for information about
 * this approach.
 *
 * TWO_PHASE TRANSACTIONS
 * ----------------------
 * Two phase transactions are replayed at prepare and then committed or
 * rolled back at commit prepared and rollback prepared respectively.
 *
 * FAILOVER
 * ----------------------
 * The logical slot on the primary can be synced to the standby by specifying
 * failover = true when creating the subscription.
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
// Rust-ABI MemoryContext helpers (extern decls would be improper_ctypes).
// TODO(pg-port): real homes utils/snapmgr.h + utils/mmgr.
unsafe fn GetPerTupleMemoryContext(_estate: *mut EState) -> MemoryContext { std::ptr::null_mut() }
unsafe fn MemoryContextSwitchTo(ctx: MemoryContext) -> MemoryContext { crate::utils::palloc::MemoryContextSwitchTo(ctx as crate::utils::palloc::MemoryContext) as MemoryContext }
unsafe fn MemoryContextReset(ctx: MemoryContext) { crate::utils::memutils::MemoryContextReset(ctx as crate::utils::palloc::MemoryContext) }
unsafe fn MemoryContextStrdup(ctx: MemoryContext, str_: *const c_char) -> *mut c_char { crate::utils::mmgr::mcxt::MemoryContextStrdup(ctx as crate::utils::mmgr::memnodes::MemoryContext, str_) }
unsafe fn AllocSetContextCreate(_parent: MemoryContext, _name: *const c_char, _min: Size, _init: Size, _max: Size) -> MemoryContext { std::ptr::null_mut() }
// Rust-ABI stubs pulled out of extern blocks (improper_ctypes).
unsafe fn TopTransactionContext() -> MemoryContext { std::ptr::null_mut() }
unsafe fn logicalrep_begin_data_extract(_d: *mut LogicalRepBeginData) -> (TransactionId, XLogRecPtr) { (0, 0) }
// off_t: POSIX type
#[allow(non_camel_case_types)] pub type off_t = i64;

use std::ffi::{c_char, c_int, c_void};
use std::ptr::{null, null_mut};

use crate::c::{int64, uint32, Size, TransactionId};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::postgres_ext::Oid as PgOid;

// ---------------------------------------------------------------------------
// Locally stubbed unported types -- TODO(pg-port) replace as modules land
// ---------------------------------------------------------------------------

/// executor/executor.h - EState
pub type EState = c_void;
/// executor/execPartition.h - PartitionTupleRouting
pub type PartitionTupleRouting = c_void;
/// executor/tuptable.h - TupleTableSlot
pub type TupleTableSlot = c_void;
/// executor/tuptable.h - ResultRelInfo
pub type ResultRelInfo = c_void;
/// nodes/plannodes.h - ModifyTableState (partial)
pub type ModifyTableState = c_void;
/// nodes/parsenodes.h - RangeTblEntry
pub type RangeTblEntry = c_void;
/// utils/rel.h - Relation
pub type Relation = c_void;
/// access/htup_details.h - HeapTuple
pub type HeapTuple = *mut c_void;
/// utils/epqstate.h - EPQState
pub type EPQState = c_void;
/// replication/logicalrelation.h - LogicalRepRelMapEntry
pub type LogicalRepRelMapEntry = c_void;
/// replication/logicalrelation.h - LogicalRepRelId
pub type LogicalRepRelId = Oid;
/// replication/logicalproto.h - LogicalRepTupleData
pub type LogicalRepTupleData = c_void;
/// replication/logicalproto.h - LogicalRepBeginData
pub type LogicalRepBeginData = c_void;
/// replication/logicalproto.h - LogicalRepCommitData
pub type LogicalRepCommitData = c_void;
/// replication/logicalproto.h - LogicalRepPreparedTxnData
pub type LogicalRepPreparedTxnData = c_void;
/// replication/logicalproto.h - LogicalRepCommitPreparedTxnData
pub type LogicalRepCommitPreparedTxnData = c_void;
/// replication/logicalproto.h - LogicalRepRollbackPreparedTxnData
pub type LogicalRepRollbackPreparedTxnData = c_void;
/// replication/logicalproto.h - LogicalRepStreamAbortData
pub type LogicalRepStreamAbortData = c_void;
/// replication/logicalproto.h - LogicalRepRelation
pub type LogicalRepRelation = c_void;
/// replication/logicalproto.h - LogicalRepTyp
pub type LogicalRepTyp = c_void;
/// replication/logicalproto.h - LogicalRepMsgType
pub type LogicalRepMsgType = c_char;
/// catalog/pg_subscription.h - Subscription
pub type Subscription = c_void;
/// catalog/pg_subscription.h - Form_pg_subscription
pub type Form_pg_subscription = *mut c_void;
/// replication/walreceiver.h - WalRcvStreamOptions
pub type WalRcvStreamOptions = c_void;
/// replication/walreceiver.h - WalReceiverConn
pub type WalReceiverConn = c_void;
/// storage/buffile.h - BufFile
pub type BufFile = c_void;
/// storage/fileset.h - FileSet
pub type FileSet = c_void;
/// access/attmap.h - AttrMap
pub type AttrMap = c_void;
/// access/tupconvert.h - TupleConversionMap
pub type TupleConversionMap = c_void;
/// utils/acl.h - AclMode
pub type AclMode = uint32;
/// utils/acl.h - AclResult
pub type AclResult = c_int;
/// nodes/pg_list.h - List
pub type List = c_void;
/// nodes/pg_list.h - ListCell
pub type ListCell = c_void;
/// utils/usercontext.h - UserContext
pub type UserContext = c_void;
/// nodes/value.h - CmdType
pub type CmdType = c_int;
/// replication/origin.h - RepOriginId
pub type RepOriginId = uint32;
/// utils/timestamp.h - TimestampTz
pub type TimestampTz = int64;
/// utils/timestamp.h - TimeLineID
pub type TimeLineID = uint32;
/// access/xact.h - ResourceOwner (opaque)
pub type ResourceOwner = *mut c_void;
/// nodes/bitmapset.h - Bitmapset
pub type Bitmapset = c_void;
/// RTEPermissionInfo (parse_relation.h)
pub type RTEPermissionInfo = c_void;
/// ConflictTupleInfo (replication/conflict.h)
pub type ConflictTupleInfo = c_void;
/// latch
pub type Latch = c_void;
/// pgsocket
pub type pgsocket = c_int;

use crate::replication::worker_internal::{
    LogicalRepWorker, LogicalRepWorkerType, MyLogicalRepWorker,
    ParallelApplyWorkerInfo,
    WORKERTYPE_APPLY, WORKERTYPE_PARALLEL_APPLY, WORKERTYPE_TABLESYNC,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NAPTIME_PER_CYCLE: i64 = 1000; /* max sleep time between cycles (1s) */

// LogicalRepMsgType constants -- TODO(pg-port): import from logicalproto when ported
const LOGICAL_REP_MSG_BEGIN: c_char = b'B' as c_char;
const LOGICAL_REP_MSG_COMMIT: c_char = b'C' as c_char;
const LOGICAL_REP_MSG_INSERT: c_char = b'I' as c_char;
const LOGICAL_REP_MSG_UPDATE: c_char = b'U' as c_char;
const LOGICAL_REP_MSG_DELETE: c_char = b'D' as c_char;
const LOGICAL_REP_MSG_TRUNCATE: c_char = b't' as c_char;
const LOGICAL_REP_MSG_RELATION: c_char = b'R' as c_char;
const LOGICAL_REP_MSG_TYPE: c_char = b'Y' as c_char;
const LOGICAL_REP_MSG_ORIGIN: c_char = b'O' as c_char;
const LOGICAL_REP_MSG_MESSAGE: c_char = b'M' as c_char;
const LOGICAL_REP_MSG_STREAM_START: c_char = b'S' as c_char;
const LOGICAL_REP_MSG_STREAM_STOP: c_char = b'E' as c_char;
const LOGICAL_REP_MSG_STREAM_ABORT: c_char = b'A' as c_char;
const LOGICAL_REP_MSG_STREAM_COMMIT: c_char = b'c' as c_char;
const LOGICAL_REP_MSG_BEGIN_PREPARE: c_char = b'b' as c_char;
const LOGICAL_REP_MSG_PREPARE: c_char = b'P' as c_char;
const LOGICAL_REP_MSG_COMMIT_PREPARED: c_char = b'K' as c_char;
const LOGICAL_REP_MSG_ROLLBACK_PREPARED: c_char = b'r' as c_char;
const LOGICAL_REP_MSG_STREAM_PREPARE: c_char = b'p' as c_char;

// LOGICALREP_COLUMN status constants -- TODO(pg-port) from logicalproto
const LOGICALREP_COLUMN_TEXT: c_char = b't' as c_char;
const LOGICALREP_COLUMN_BINARY: c_char = b'b' as c_char;
const LOGICALREP_COLUMN_NULL: c_char = b'n' as c_char;
const LOGICALREP_COLUMN_UNCHANGED: c_char = b'u' as c_char;

// Two-phase state constants -- TODO(pg-port) from logicalproto
const LOGICALREP_TWOPHASE_STATE_DISABLED: c_char = b'd' as c_char;
const LOGICALREP_TWOPHASE_STATE_PENDING: c_char = b'p' as c_char;
const LOGICALREP_TWOPHASE_STATE_ENABLED: c_char = b'e' as c_char;

// Streaming mode -- TODO(pg-port) from pg_subscription.h
const LOGICALREP_STREAM_OFF: c_int = 0;
const LOGICALREP_STREAM_PARALLEL: c_int = 2;

// Protocol version numbers -- TODO(pg-port) from logicalproto
const LOGICALREP_PROTO_VERSION_NUM: c_int = 1;
const LOGICALREP_PROTO_STREAM_VERSION_NUM: c_int = 2;
const LOGICALREP_PROTO_TWOPHASE_VERSION_NUM: c_int = 3;
const LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM: c_int = 4;

// MAXPGPATH
const MAXPGPATH: usize = 1024;

// GIDSIZE -- access/twophase.h
const GIDSIZE: usize = 200;

// Natts_pg_subscription, Anum_pg_subscription_subskiplsn -- TODO(pg-port)
const Natts_pg_subscription: usize = 32;
const Anum_pg_subscription_subskiplsn: usize = 20;

// AccessShareLock, RowExclusiveLock, AccessExclusiveLock, NoLock -- storage/lockdefs.h
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 3;
const AccessExclusiveLock: c_int = 8;
const NoLock: c_int = 0;

// Wait event -- pgstat.h
const WAIT_EVENT_LOGICAL_APPLY_MAIN: uint32 = 0;

// WaitLatch flags
const WL_SOCKET_READABLE: c_int = 1;
const WL_LATCH_SET: c_int = 2;
const WL_TIMEOUT: c_int = 4;
const WL_EXIT_ON_PM_DEATH: c_int = 8;

// PGINVALID_SOCKET
const PGINVALID_SOCKET: pgsocket = -1;

// GUC placeholders
const wal_receiver_timeout: i64 = 60000;
const wal_receiver_status_interval: i64 = 10;
const WalWriterDelay: i64 = 200;

// BLCKSZ
const BLCKSZ: usize = 8192;

// Locking for LWLock
const LW_SHARED: c_int = 1;
const LW_EXCLUSIVE: c_int = 2;

// AclCheck result
const ACLCHECK_OK: AclResult = 0;

// ACL modes
const ACL_SELECT: AclMode = 1 << 0;
const ACL_INSERT: AclMode = 1 << 2;
const ACL_UPDATE: AclMode = 1 << 3;
const ACL_DELETE: AclMode = 1 << 4;
const ACL_TRUNCATE: AclMode = 1 << 6;

// RLS
const RLS_ENABLED: c_int = 1;

// RELKIND
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

// REPLICA_IDENTITY_FULL -- access/relation.h
const REPLICA_IDENTITY_FULL: c_char = b'f' as c_char;

// subrel state -- catalog/pg_subscription_rel.h
const SUBREL_STATE_READY: c_char = b'r' as c_char;
const SUBREL_STATE_SYNCDONE: c_char = b'u' as c_char;
const SUBREL_STATE_UNKNOWN: c_char = b'\0' as c_char;

// FS_SERIALIZE_DONE -- parallel apply
const FS_SERIALIZE_DONE: c_int = 2;

// CmdType values
const CMD_INSERT: CmdType = 1;
const CMD_UPDATE: CmdType = 2;
const CMD_DELETE: CmdType = 3;

// DROP_RESTRICT -- commands/tablecmds.h
const DROP_RESTRICT: c_int = 0;

// InvalidXLogRecPtr, InvalidTransactionId, InvalidOid
const InvalidXLogRecPtr: XLogRecPtr = 0;
const InvalidTransactionId: TransactionId = 0;
const InvalidOid: Oid = 0;
const InvalidRepOriginId: RepOriginId = 0;

// PARALLEL_TRANS_STARTED/FINISHED
const PARALLEL_TRANS_STARTED: c_int = 1;
const PARALLEL_TRANS_FINISHED: c_int = 2;

// Conflict types -- TODO(pg-port) from replication/conflict.h
const CT_UPDATE_ORIGIN_DIFFERS: c_int = 1;
const CT_UPDATE_MISSING: c_int = 2;
const CT_DELETE_ORIGIN_DIFFERS: c_int = 3;
const CT_DELETE_MISSING: c_int = 4;

// SubscriptionRelationId -- catalog/pg_subscription.h
const SubscriptionRelationId: Oid = 6100;

// SUBSCRIPTIONOID, SUBSCRIPTIONRELMAP, AUTHOID -- utils/syscache.h
const SUBSCRIPTIONOID: c_int = 50;
const SUBSCRIPTIONRELMAP: c_int = 51;
const AUTHOID: c_int = 9;

// GUC context
const PGC_SUSET: c_int = 4;
const PGC_BACKEND: c_int = 3;
const PGC_S_OVERRIDE: c_int = 6;
const PGC_SIGHUP: c_int = 1;

// pgstat activity state
const STATE_RUNNING: c_int = 1;
const STATE_IDLE: c_int = 2;
const STATE_IDLEINTRANSACTION: c_int = 3;

// O_RDONLY / O_RDWR
const O_RDONLY: c_int = 0;
const O_RDWR: c_int = 2;

// SEEK_END
const SEEK_END: c_int = 2;

// FirstLowInvalidHeapAttributeNumber
const FirstLowInvalidHeapAttributeNumber: c_int = -8;

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// Track LSN pairs for flush position reporting.
#[repr(C)]
struct FlushPosition {
    node: DListNode,
    local_end: XLogRecPtr,
    remote_end: XLogRecPtr,
}

/// Executor state for applying one operation.
#[repr(C)]
pub struct ApplyExecutionData {
    pub estate: *mut EState,       /* executor state, used to track resources */
    pub targetRel: *mut LogicalRepRelMapEntry, /* replication target rel */
    pub targetRelInfo: *mut ResultRelInfo,     /* ResultRelInfo for same */
    /* These fields are used when the target relation is partitioned: */
    pub mtstate: *mut ModifyTableState,        /* dummy ModifyTable state */
    pub proute: *mut PartitionTupleRouting,    /* partition routing info */
}

/// Struct for saving and restoring apply errcontext information.
#[repr(C)]
struct ApplyErrorCallbackArg {
    command: LogicalRepMsgType, /* 0 if invalid */
    rel: *mut LogicalRepRelMapEntry,
    /* Remote node information */
    remote_attnum: c_int,       /* -1 if invalid */
    remote_xid: TransactionId,
    finish_lsn: XLogRecPtr,
    origin_name: *mut c_char,
}

/*
 * The action to be taken for the changes in the transaction.
 *
 * TRANS_LEADER_APPLY:
 * This action means that we are in the leader apply worker or table sync
 * worker. The changes of the transaction are either directly applied or
 * are read from temporary files (for streaming transactions) and then
 * applied by the worker.
 *
 * TRANS_LEADER_SERIALIZE:
 * This action means that we are in the leader apply worker or table sync
 * worker. Changes are written to temporary files and then applied when the
 * final commit arrives.
 *
 * TRANS_LEADER_SEND_TO_PARALLEL:
 * This action means that we are in the leader apply worker and need to send
 * the changes to the parallel apply worker.
 *
 * TRANS_LEADER_PARTIAL_SERIALIZE:
 * This action means that we are in the leader apply worker and have sent some
 * changes directly to the parallel apply worker and the remaining changes are
 * serialized to a file, due to timeout while sending data.
 *
 * TRANS_PARALLEL_APPLY:
 * This action means that we are in the parallel apply worker and changes of
 * the transaction are applied directly by the worker.
 */
#[repr(C)]
#[derive(PartialEq, Clone, Copy)]
enum TransApplyAction {
    /* The action for non-streaming transactions. */
    TRANS_LEADER_APPLY,
    /* Actions for streaming transactions. */
    TRANS_LEADER_SERIALIZE,
    TRANS_LEADER_SEND_TO_PARALLEL,
    TRANS_LEADER_PARTIAL_SERIALIZE,
    TRANS_PARALLEL_APPLY,
}
use TransApplyAction::*;

/// Sub-transaction info within a streaming transaction.
#[repr(C)]
#[derive(Clone, Copy)]
struct SubXactInfo {
    xid: TransactionId,    /* XID of the subxact */
    fileno: c_int,         /* file number in the buffile */
    offset: off_t,         /* offset in the file */
}

/// Sub-transaction data for the current streaming transaction.
#[repr(C)]
struct ApplySubXactData {
    nsubxacts: uint32,          /* number of sub-transactions */
    nsubxacts_max: uint32,      /* current capacity of subxacts */
    subxact_last: TransactionId, /* xid of the last sub-transaction */
    subxacts: *mut SubXactInfo, /* sub-xact offset in changes file */
}

/// Minimal dlist_node stub -- TODO(pg-port) use real ilist
#[repr(C)]
struct DListNode {
    prev: *mut DListNode,
    next: *mut DListNode,
}

/// Minimal dlist_head stub
#[repr(C)]
struct DListHead {
    head: DListNode,
}

impl DListHead {
    const fn new() -> Self {
        DListHead {
            head: DListNode { prev: null_mut(), next: null_mut() },
        }
    }
}

// ---------------------------------------------------------------------------
// Module-level state (globals)
// ---------------------------------------------------------------------------

/* errcontext tracker */
static mut apply_error_callback_arg: ApplyErrorCallbackArg = ApplyErrorCallbackArg {
    command: 0,
    rel: null_mut(),
    remote_attnum: -1,
    remote_xid: InvalidTransactionId,
    finish_lsn: InvalidXLogRecPtr,
    origin_name: null_mut(),
};

pub static mut apply_error_context_stack: *mut c_void = null_mut();

pub static mut ApplyMessageContext: MemoryContext = null_mut();
pub static mut ApplyContext: MemoryContext = null_mut();

/* per stream context for streaming transactions */
static mut LogicalStreamingContext: MemoryContext = null_mut();

pub static mut LogRepWorkerWalRcvConn: *mut WalReceiverConn = null_mut();

pub static mut MySubscription: *mut Subscription = null_mut();
static mut MySubscriptionValid: bool = false;

static mut on_commit_wakeup_workers_subids: *mut List = null_mut();

pub static mut in_remote_transaction: bool = false;
static mut remote_final_lsn: XLogRecPtr = InvalidXLogRecPtr;

/* fields valid only when processing streamed transaction */
static mut in_streamed_transaction: bool = false;
static mut stream_xid: TransactionId = InvalidTransactionId;

/*
 * The number of changes applied by parallel apply worker during one streaming
 * block.
 */
static mut parallel_stream_nchanges: uint32 = 0;

/* Are we initializing an apply worker? */
pub static mut InitializingApplyWorker: bool = false;

/*
 * We enable skipping all data modification changes (INSERT, UPDATE, etc.) for
 * the subscription if the remote transaction's finish LSN matches the subskiplsn.
 */
static mut skip_xact_finish_lsn: XLogRecPtr = InvalidXLogRecPtr;

#[inline(always)]
unsafe fn is_skipping_changes() -> bool {
    skip_xact_finish_lsn != InvalidXLogRecPtr
}

/* BufFile handle of the current streaming file */
static mut stream_fd: *mut BufFile = null_mut();

static mut subxact_data: ApplySubXactData = ApplySubXactData {
    nsubxacts: 0,
    nsubxacts_max: 0,
    subxact_last: InvalidTransactionId,
    subxacts: null_mut(),
};

static mut lsn_mapping: DListHead = DListHead::new();

/* replorigin_session_origin / lsn / timestamp -- replication/origin.h globals */
extern "C" {
    static mut replorigin_session_origin: RepOriginId;
    static mut replorigin_session_origin_lsn: XLogRecPtr;
    static mut replorigin_session_origin_timestamp: TimestampTz;
    static mut error_context_stack: *mut c_void;
    static mut ConfigReloadPending: bool;
    static mut CurrentResourceOwner: ResourceOwner;
    static mut TopTransactionResourceOwner: ResourceOwner;
    static mut MyLatch: *mut Latch;
    static mut XactLastCommitEnd: XLogRecPtr;
    /* parallel apply shared memory -- applyparallelworker.h */
    static mut MyParallelShared: *mut ParallelApplyShared;
}

/// ParallelApplyShared -- TODO(pg-port) stub from applyparallelworker.h
#[repr(C)]
pub struct ParallelApplyShared {
    pub xid: TransactionId,
    pub last_commit_end: XLogRecPtr,
}

// ---------------------------------------------------------------------------
// Unported function stubs -- TODO(pg-port)
// ---------------------------------------------------------------------------

extern "C" {
    /* miscadmin.h */
    fn SetCurrentStatementStartTimestamp();
    fn IsTransactionState() -> bool;
    fn IsTransactionOrTransactionBlock() -> bool;
    fn IsTransactionBlock() -> bool;
    fn StartTransactionCommand();
    fn CommitTransactionCommand();
    fn AbortOutOfAnyTransaction();
    fn BeginTransactionBlock();
    fn EndTransactionBlock(chain: bool);
    fn CommandCounterIncrement();
    fn GetCurrentCommandId(used: bool) -> uint32;
    fn GetTransactionSnapshot() -> *mut c_void;
    fn PushActiveSnapshot(snap: *mut c_void);
    fn PopActiveSnapshot();
    fn AcceptInvalidationMessages();

    fn GetPerTupleExprContext(estate: *mut EState) -> *mut c_void;

    /* palloc */
    fn palloc(size: Size) -> *mut c_void;
    fn palloc0(size: Size) -> *mut c_void;
    fn pfree(ptr: *mut c_void);
    fn repalloc(ptr: *mut c_void, size: Size) -> *mut c_void;
    fn pstrdup(s: *const c_char) -> *mut c_char;

    /* stringinfo */
    fn makeStringInfo() -> *mut StringInfoData;
    fn resetStringInfo(info: *mut StringInfoData);
    fn initReadOnlyStringInfo(str_: *mut StringInfoData, buf: *mut c_char, len: c_int);
    fn pq_getmsgbyte(msg: *mut StringInfoData) -> c_int;
    fn pq_getmsgint(msg: *mut StringInfoData, b: c_int) -> uint32;
    fn pq_getmsgint64(msg: *mut StringInfoData) -> int64;
    fn pq_sendbyte(buf: *mut StringInfoData, byt: u8);
    fn pq_sendint64(buf: *mut StringInfoData, i: int64);

    /* pgstat */
    fn pgstat_report_activity(state: c_int, cmd_str: *const c_char);
    fn pgstat_report_stat(force: bool);
    fn pgstat_report_subscription_error(subid: Oid, is_apply_worker: bool);

    /* postmaster/interrupt.h */
    fn SignalHandlerForConfigReload(postgres_signal_arg: c_int);
    fn die(postgres_signal_arg: c_int);
    fn pqsignal(signo: c_int, func: unsafe extern "C" fn(c_int));
    fn BackgroundWorkerUnblockSignals();
    fn BackgroundWorkerInitializeConnectionByOid(dboid: Oid, useroid: Oid, flags: uint32);

    /* proc_exit */
    fn proc_exit(code: c_int);

    /* utils/guc.h */
    fn SetConfigOption(name: *const c_char, value: *const c_char, context: c_int, source: c_int);
    fn ProcessConfigFile(context: c_int);

    /* utils/syscache.h */
    fn CacheRegisterSyscacheCallback(
        cacheid: c_int,
        func: unsafe extern "C" fn(Datum, c_int, uint32),
        arg: Datum,
    );
    fn SearchSysCacheCopy1(cacheid: c_int, key1: Datum) -> HeapTuple;

    /* utils/acl.h */
    fn pg_class_aclcheck(relid: Oid, roleid: Oid, mode: AclMode) -> AclResult;
    fn aclcheck_error(aclerr: AclResult, objtype: c_int, objectname: *const c_char);
    fn check_enable_rls(relid: Oid, checkasuser: Oid, noerr: bool) -> c_int;
    fn GetUserId() -> Oid;
    fn GetUserNameFromId(roleid: Oid, noerr: bool) -> *mut c_char;

    /* utils/lsyscache.h */
    fn get_rel_name(relid: Oid) -> *mut c_char;
    fn get_namespace_name(nspid: Oid) -> *mut c_char;
    fn getTypeInputInfo(typid: Oid, typinput: *mut Oid, typioparam: *mut Oid);
    fn getTypeBinaryInputInfo(typid: Oid, typreceive: *mut Oid, typioparam: *mut Oid);
    fn OidInputFunctionCall(funcid: Oid, str_: *mut c_char, typioparam: Oid, typmod: i32) -> Datum;
    fn OidReceiveFunctionCall(funcid: Oid, buf: *mut StringInfoData, typioparam: Oid, typmod: i32) -> Datum;
    fn get_relkind_objtype(relkind: c_char) -> c_int;
    fn GetRelationIdentityOrPK(rel: *mut c_void) -> Oid;
    fn find_all_inheritors(parentrelId: Oid, lockmode: c_int, numparents: *mut c_int) -> *mut List;
    fn list_member_oid(list: *mut List, datum: Oid) -> bool;
    fn lappend_oid(list: *mut List, datum: Oid) -> *mut List;
    fn lappend(list: *mut List, datum: *mut c_void) -> *mut List;
    fn lfirst_oid(lc: *mut ListCell) -> Oid;
    fn lfirst(lc: *mut ListCell) -> *mut c_void;
    fn list_make1(x1: *mut c_void) -> *mut List;
    fn list_append_unique_oid(list: *mut List, datum: Oid) -> *mut List;
    fn list_nth(list: *mut List, n: c_int) -> *mut c_void;
    fn foreach_begin(list: *mut List) -> *mut ListCell;
    fn foreach_next(lc: *mut ListCell) -> *mut ListCell;
    /* NIL */

    /* utils/rel.h */
    fn RelationGetRelid(rel: *mut c_void) -> Oid;
    fn RelationGetDescr(rel: *mut c_void) -> *mut c_void;
    fn RelationGetRelationName(rel: *mut c_void) -> *const c_char;
    fn RelationGetNamespace(rel: *mut c_void) -> Oid;
    fn RelationGetIndexList(rel: *mut c_void) -> *mut List;
    fn RELATION_IS_OTHER_TEMP(rel: *mut c_void) -> bool;
    fn RelationIsLogicallyLogged(rel: *mut c_void) -> bool;
    fn RelationFindReplTupleByIndex(
        rel: *mut c_void,
        idxoid: Oid,
        lockmode: c_int,
        searchslot: *mut TupleTableSlot,
        outslot: *mut TupleTableSlot,
    ) -> bool;
    fn RelationFindReplTupleSeq(
        rel: *mut c_void,
        lockmode: c_int,
        searchslot: *mut TupleTableSlot,
        outslot: *mut TupleTableSlot,
    ) -> bool;

    /* access/table.h */
    fn table_open(relid: Oid, lockmode: c_int) -> *mut c_void;
    fn table_close(rel: *mut c_void, lockmode: c_int);
    fn table_slot_create(rel: *mut c_void, tupleTable: *mut *mut c_void) -> *mut TupleTableSlot;

    /* executor */
    fn CreateExecutorState() -> *mut EState;
    fn FreeExecutorState(estate: *mut EState);
    fn ExecInitRangeTable(estate: *mut EState, rtable: *mut List, perminfos: *mut List, validRTIs: *mut c_void);
    fn ExecResetTupleTable(tupleTable: *mut c_void, shouldFree: bool);
    fn ExecInitExtraTupleSlot(estate: *mut EState, desc: *mut c_void, ops: *mut c_void) -> *mut TupleTableSlot;
    fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot;
    fn ExecStoreVirtualTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot;
    fn ExecCopySlot(dstslot: *mut TupleTableSlot, srcslot: *mut TupleTableSlot) -> *mut TupleTableSlot;
    fn slot_getallattrs(slot: *mut TupleTableSlot);
    fn ExecOpenIndices(resultRelInfo: *mut ResultRelInfo, speculative: bool);
    fn ExecCloseIndices(resultRelInfo: *mut ResultRelInfo);
    fn InitResultRelInfo(
        resultRelInfo: *mut ResultRelInfo,
        resultRelationDesc: *mut c_void,
        resultRelationIndex: c_int,
        partition_root_rri: *mut ResultRelInfo,
        instrument_options: c_int,
    );
    fn InitConflictIndexes(resultRelInfo: *mut ResultRelInfo);
    fn AfterTriggerBeginQuery();
    fn AfterTriggerEndQuery(estate: *mut EState);
    fn ExecSimpleRelationInsert(resultRelInfo: *mut ResultRelInfo, estate: *mut EState, slot: *mut TupleTableSlot);
    fn ExecSimpleRelationUpdate(
        resultRelInfo: *mut ResultRelInfo,
        estate: *mut EState,
        epqstate: *mut EPQState,
        searchslot: *mut TupleTableSlot,
        slot: *mut TupleTableSlot,
    );
    fn ExecSimpleRelationDelete(
        resultRelInfo: *mut ResultRelInfo,
        estate: *mut EState,
        epqstate: *mut EPQState,
        searchslot: *mut TupleTableSlot,
    );
    fn EvalPlanQualInit(
        epqstate: *mut EPQState,
        estate: *mut EState,
        subplan: *mut c_void,
        auxrowmarks: *mut List,
        epqParam: c_int,
        defaultSlot: *mut List,
    );
    fn EvalPlanQualEnd(epqstate: *mut EPQState);
    fn EvalPlanQualSetSlot(epqstate: *mut EPQState, slot: *mut TupleTableSlot);
    fn ExecSetupPartitionTupleRouting(estate: *mut EState, rel: *mut c_void) -> *mut PartitionTupleRouting;
    fn ExecFindPartition(
        mtstate: *mut ModifyTableState,
        rootResultRelInfo: *mut ResultRelInfo,
        proute: *mut PartitionTupleRouting,
        slot: *mut TupleTableSlot,
        estate: *mut EState,
    ) -> *mut ResultRelInfo;
    fn ExecCleanupTupleRouting(mtstate: *mut ModifyTableState, proute: *mut PartitionTupleRouting);
    fn ExecPartitionCheck(
        resultRelInfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        estate: *mut EState,
        emitError: bool,
    ) -> bool;
    fn ExecGetRootToChildMap(resultRelInfo: *mut ResultRelInfo, estate: *mut EState) -> *mut TupleConversionMap;
    fn execute_attr_map_slot(attrMap: *mut AttrMap, in_slot: *mut TupleTableSlot, out_slot: *mut TupleTableSlot) -> *mut TupleTableSlot;
    fn convert_tuples_by_name(indesc: *mut c_void, outdesc: *mut c_void) -> *mut TupleConversionMap;
    fn ExecPartitionCheckEmitError(resultRelInfo: *mut ResultRelInfo, slot: *mut TupleTableSlot, estate: *mut EState);

    /* TTSOpsVirtual */
    static TTSOpsVirtual: c_void;

    /* nodes */
    fn makeNode_RangeTblEntry() -> *mut RangeTblEntry;
    fn makeNode_ResultRelInfo() -> *mut ResultRelInfo;
    fn makeNode_ModifyTableState() -> *mut ModifyTableState;
    fn addRTEPermissionInfo(perminfos: *mut *mut List, rte: *mut RangeTblEntry);
    fn bms_make_singleton(x: c_int) -> *mut c_void;

    /* optimizer */
    fn build_column_default(rel: *mut c_void, attnum: c_int) -> *mut c_void;
    fn expression_planner(expr: *mut c_void) -> *mut c_void;
    fn ExecInitExpr(node: *mut c_void, parent: *mut c_void) -> *mut c_void;
    fn ExecEvalExpr(state: *mut c_void, econtext: *mut c_void, isNull: *mut bool) -> Datum;

    /* replication/logicalproto.h */
    fn logicalrep_read_begin(in_: *mut StringInfoData, begin_data: *mut LogicalRepBeginData);
    fn logicalrep_read_commit(in_: *mut StringInfoData, commit_data: *mut LogicalRepCommitData);
    fn logicalrep_read_begin_prepare(in_: *mut StringInfoData, begin_data: *mut LogicalRepPreparedTxnData);
    fn logicalrep_read_prepare(in_: *mut StringInfoData, prepare_data: *mut LogicalRepPreparedTxnData);
    fn logicalrep_read_commit_prepared(in_: *mut StringInfoData, prepare_data: *mut LogicalRepCommitPreparedTxnData);
    fn logicalrep_read_rollback_prepared(in_: *mut StringInfoData, rollback_data: *mut LogicalRepRollbackPreparedTxnData);
    fn logicalrep_read_stream_start(in_: *mut StringInfoData, first_segment: *mut bool) -> TransactionId;
    fn logicalrep_read_stream_commit(in_: *mut StringInfoData, commit_data: *mut LogicalRepCommitData) -> TransactionId;
    fn logicalrep_read_stream_prepare(in_: *mut StringInfoData, prepare_data: *mut LogicalRepPreparedTxnData);
    fn logicalrep_read_stream_abort(in_: *mut StringInfoData, abort_data: *mut LogicalRepStreamAbortData, needsToken: bool);
    fn logicalrep_read_insert(in_: *mut StringInfoData, newtup: *mut LogicalRepTupleData) -> LogicalRepRelId;
    fn logicalrep_read_update(
        in_: *mut StringInfoData,
        has_oldtup: *mut bool,
        oldtup: *mut LogicalRepTupleData,
        newtup: *mut LogicalRepTupleData,
    ) -> LogicalRepRelId;
    fn logicalrep_read_delete(in_: *mut StringInfoData, oldtup: *mut LogicalRepTupleData) -> LogicalRepRelId;
    fn logicalrep_read_truncate(
        in_: *mut StringInfoData,
        cascade: *mut bool,
        restart_seqs: *mut bool,
    ) -> *mut List;
    fn logicalrep_read_rel(in_: *mut StringInfoData) -> *mut LogicalRepRelation;
    fn logicalrep_read_typ(in_: *mut StringInfoData, ltyp: *mut LogicalRepTyp);
    fn logicalrep_message_type(msgtype: LogicalRepMsgType) -> *const c_char;

    /* replication/logicalrelation.h */
    fn logicalrep_rel_open(relid: LogicalRepRelId, lockmode: c_int) -> *mut LogicalRepRelMapEntry;
    fn logicalrep_rel_close(rel: *mut LogicalRepRelMapEntry, lockmode: c_int);
    fn logicalrep_relmap_update(rel: *mut LogicalRepRelation);
    fn logicalrep_partmap_reset_relmap(rel: *mut LogicalRepRelation);
    fn logicalrep_partition_open(
        root: *mut LogicalRepRelMapEntry,
        partrel: *mut c_void,
        map: *mut AttrMap,
    ) -> *mut LogicalRepRelMapEntry;

    /* replication/origin.h */
    fn replorigin_by_name(roname: *const c_char, missing_ok: bool) -> RepOriginId;
    fn replorigin_create(roname: *const c_char) -> RepOriginId;
    fn replorigin_session_setup(node: RepOriginId, reconnect_lsn: c_int);
    fn replorigin_session_get_progress(flush: bool) -> XLogRecPtr;

    /* replication/worker_internal.h */
    fn logicalrep_worker_attach(slot: c_int);
    fn logicalrep_worker_wakeup(subid: Oid, relid: Oid);
    fn logicalrep_workers_find(subid: Oid, only_running: bool, acquire_lock: bool) -> *mut List;

    /* replication/logicallauncher.h */
    fn ApplyLauncherForgetWorkerStartTime(subid: Oid);
    fn AllTablesyncsReady() -> bool;
    fn invalidate_syncing_table_states(arg: Datum, cacheid: c_int, hashvalue: uint32);
    fn process_syncing_tables(current_lsn: XLogRecPtr);
    fn process_syncing_tables_for_apply(current_lsn: XLogRecPtr);

    /* replication/walreceiver.h */
    fn walrcv_connect(
        conninfo: *const c_char,
        replication: bool,
        logical: bool,
        password_required: bool,
        appname: *const c_char,
        err: *mut *mut c_char,
    ) -> *mut WalReceiverConn;
    fn walrcv_identify_system(conn: *mut WalReceiverConn, sysIdentifier: *mut TimeLineID) -> *mut c_char;
    fn walrcv_startstreaming(conn: *mut WalReceiverConn, options: *const WalRcvStreamOptions);
    fn walrcv_endstreaming(conn: *mut WalReceiverConn, next_tli: *mut TimeLineID);
    fn walrcv_receive(conn: *mut WalReceiverConn, buffer: *mut *mut c_char, pgsocket: *mut pgsocket) -> c_int;
    fn walrcv_send(conn: *mut WalReceiverConn, buffer: *const c_char, nbytes: c_int);
    fn walrcv_server_version(conn: *mut WalReceiverConn) -> c_int;

    /* storage/ipc.h */
    fn before_shmem_exit(f: unsafe extern "C" fn(c_int, Datum), arg: Datum);

    /* storage/lmgr.h */
    fn LockSharedObject(classid: Oid, objid: Oid, objsubid: uint32, lockmode: c_int);
    fn LWLockAcquire(lock: *mut c_void, mode: c_int) -> bool;
    fn LWLockRelease(lock: *mut c_void);

    /* storage/latch.h */
    fn WaitLatchOrSocket(
        latch: *mut Latch,
        wakeEvents: c_int,
        sock: pgsocket,
        timeout: i64,
        wait_event_info: uint32,
    ) -> c_int;
    fn ResetLatch(latch: *mut Latch);
    fn GetFlushRecPtr(tli: *mut TimeLineID) -> XLogRecPtr;

    /* storage/buffile.h */
    fn BufFileCreateFileSet(fileset: *mut FileSet, name: *const c_char) -> *mut BufFile;
    fn BufFileOpenFileSet(
        fileset: *mut FileSet,
        name: *const c_char,
        fileFlags: c_int,
        missing_ok: bool,
    ) -> *mut BufFile;
    fn BufFileDeleteFileSet(fileset: *mut FileSet, name: *const c_char, missing_ok: bool);
    fn BufFileClose(file: *mut BufFile);
    fn BufFileWrite(file: *mut BufFile, ptr: *const c_void, size: Size) -> Size;
    fn BufFileReadExact(file: *mut BufFile, ptr: *mut c_void, size: Size);
    fn BufFileReadMaybeEOF(file: *mut BufFile, ptr: *mut c_void, size: Size, eofOK: bool) -> Size;
    fn BufFileTell(file: *mut BufFile, fileno: *mut c_int, offset: *mut off_t);
    fn BufFileSeek(file: *mut BufFile, fileno: c_int, offset: off_t, whence: c_int) -> c_int;
    fn BufFileTruncateFileSet(file: *mut BufFile, fileno: c_int, offset: off_t);

    /* storage/fileset.h */
    fn FileSetInit(fileset: *mut FileSet);

    /* catalog/indexing.h */
    fn CatalogTupleUpdate(heapRel: *mut c_void, otid: *mut c_void, tup: HeapTuple);

    /* access/twophase.h */
    fn TwoPhaseTransactionGid(suboid: Oid, xid: TransactionId, gid: *mut c_char, size: c_int);
    fn PrepareTransactionBlock(gid: *const c_char);
    fn FinishPreparedTransaction(gid: *const c_char, isCommit: bool);
    fn LookupGXact(gid: *const c_char, lsn: XLogRecPtr, ts: TimestampTz) -> bool;

    /* catalog/pg_subscription.h */
    fn GetSubscription(subid: Oid, missing_ok: bool) -> *mut Subscription;
    fn FreeSubscription(sub: *mut Subscription);
    fn GetSubscriptionNotNull(subid: Oid) -> *mut Subscription;
    fn UpdateTwoPhaseState(subid: Oid, newstate: c_char);
    fn DisableSubscription(subid: Oid);

    /* commands/tablecmds.h */
    fn CheckSubscriptionRelkind(relkind: c_char, nspname: *const c_char, relname: *const c_char);
    fn ExecuteTruncateGuts(
        rels: *mut List,
        relids: *mut List,
        relids_logged: *mut List,
        behavior: c_int,
        restart_seqs: bool,
        run_as_table_owner: bool,
    );

    /* utils/usercontext.h */
    fn SwitchToUntrustedUser(userid: Oid, context: *mut UserContext);
    fn RestoreUserContext(context: *mut UserContext);

    /* utils/inval.h */
    fn errcontext(fmt: *const c_char, ...) -> c_int;

    /* miscadmin */
    fn GetCurrentTimestamp() -> TimestampTz;
    fn TimestampTzPlusMilliseconds(t: TimestampTz, ms: i64) -> TimestampTz;
    fn TimestampDifferenceExceeds(start: TimestampTz, stop: TimestampTz, msec: c_int) -> bool;

    /* pgstat */
    fn HOLD_INTERRUPTS();
    fn RESUME_INTERRUPTS();
    fn EmitErrorReport();
    fn FlushErrorState();

    /* rewrite/rewriteHandler.h */
    fn CheckTableForSerializableConflictIn(rel: *mut c_void);


    /* heap */
    fn heap_freetuple(htup: HeapTuple);
    fn heap_modify_tuple(
        tuple: HeapTuple,
        tupleDesc: *mut c_void,
        replValues: *mut Datum,
        replIsnull: *mut bool,
        doReplace: *mut bool,
    ) -> HeapTuple;
    fn GETSTRUCT(tup: HeapTuple) -> *mut c_void;
    fn HeapTupleIsValid(htup: HeapTuple) -> bool;

    /* misc helpers */
    fn OidIsValid(oid: Oid) -> bool;
    fn XLogRecPtrIsInvalid(lsn: XLogRecPtr) -> bool;
    fn TransactionIdIsValid(xid: TransactionId) -> bool;
    fn ObjectIdGetDatum(oid: Oid) -> Datum;
    fn DatumGetInt32(d: Datum) -> c_int;
    fn LSNGetDatum(lsn: XLogRecPtr) -> Datum;
    fn snprintf_(s: *mut c_char, maxlen: Size, fmt: *const c_char, ...) -> c_int;
    fn my_log2(num: c_int) -> c_int;
    fn load_file(filename: *const c_char, restricted: bool);
    fn equal(a: *mut c_void, b: *mut c_void) -> bool;

    /* replication/conflict.h */
    fn GetTupleTransactionInfo(
        slot: *mut TupleTableSlot,
        xmin: *mut TransactionId,
        origin: *mut RepOriginId,
        ts: *mut TimestampTz,
    ) -> bool;
    fn ReportApplyConflict(
        estate: *mut EState,
        resultRelInfo: *mut ResultRelInfo,
        elevel: c_int,
        conflict_type: c_int,
        searchslot: *mut TupleTableSlot,
        newslot: *mut TupleTableSlot,
        conflicttuples: *mut List,
    );

    /* parallel apply */
    fn pa_send_data(winfo: *mut ParallelApplyWorkerInfo, len: usize, data: *const c_char) -> bool;
    fn pa_switch_to_partial_serialize(winfo: *mut ParallelApplyWorkerInfo, stream_locked: bool);
    fn pa_start_subtrans(current_xid: TransactionId, top_xid: TransactionId);
    fn pa_allocate_worker(xid: TransactionId);
    fn pa_set_stream_apply_worker(winfo: *mut ParallelApplyWorkerInfo);
    fn pa_lock_stream(xid: TransactionId, lockmode: c_int);
    fn pa_unlock_stream(xid: TransactionId, lockmode: c_int);
    fn pa_lock_transaction(xid: TransactionId, lockmode: c_int);
    fn pa_unlock_transaction(xid: TransactionId, lockmode: c_int);
    fn pa_xact_finish(winfo: *mut ParallelApplyWorkerInfo, end_lsn: XLogRecPtr);
    fn pa_find_worker(xid: TransactionId) -> *mut ParallelApplyWorkerInfo;
    fn pa_set_fileset_state(shared: *mut c_void, state: c_int);
    fn pa_set_xact_state(shared: *mut ParallelApplyShared, state: c_int);
    fn pa_decr_and_wait_stream_block();
    fn pa_reset_subtrans();
    fn pa_stream_abort(abort_data: *mut LogicalRepStreamAbortData);
    fn pg_atomic_add_fetch_u32(ptr: *mut c_void, add: uint32) -> uint32;

    /* tablesync */

    /* IsIndexUsableForReplicaIdentityFull */
    fn IsIndexUsableForReplicaIdentityFull(idxrel: *mut c_void, map: *mut AttrMap) -> bool;
    fn index_open(indexid: Oid, lockmode: c_int) -> *mut c_void;
    fn index_close(index: *mut c_void, lockmode: c_int);

    /* TupleDescAttr */
    fn TupleDescAttr(desc: *mut c_void, attnum: c_int) -> *mut c_void;

    /* LWLock for logical rep worker */
    fn LogicalRepWorkerLock_ptr() -> *mut c_void;

    fn apply_error_callback(arg: *mut c_void);
    fn set_apply_error_context_origin_c(originname: *mut c_char);
}

// ---------------------------------------------------------------------------
// Helper macros (as inline unsafe fns)
// ---------------------------------------------------------------------------

/// CHECK_FOR_INTERRUPTS -- miscadmin.h
#[inline(always)]
unsafe fn CHECK_FOR_INTERRUPTS() {
    extern "C" { fn ProcessInterrupts(); static mut InterruptPending: bool; }
    if InterruptPending { ProcessInterrupts(); }
}

// ---------------------------------------------------------------------------
// Inline helpers for dlist (minimal)
// ---------------------------------------------------------------------------

#[inline(always)]
unsafe fn dlist_is_empty(head: *const DListHead) -> bool {
    let h = &(*head).head;
    h.next.is_null() || std::ptr::eq(h.next, &(*head).head as *const DListNode as *mut DListNode)
}

#[inline(always)]
unsafe fn dlist_push_tail(head: *mut DListHead, node: *mut DListNode) {
    let tail = (*head).head.prev;
    (*node).prev = tail;
    (*node).next = &mut (*head).head as *mut DListNode;
    if tail.is_null() {
        (*head).head.prev = node;
        (*head).head.next = node;
    } else {
        (*tail).next = node;
        (*head).head.prev = node;
    }
}

#[inline(always)]
unsafe fn dlist_delete(node: *mut DListNode) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
}

// ---------------------------------------------------------------------------
// NIL list helper
// ---------------------------------------------------------------------------
const NIL: *mut List = null_mut();

// ---------------------------------------------------------------------------
// snprintf wrapper
// ---------------------------------------------------------------------------
macro_rules! c_snprintf {
    ($buf:expr, $fmt:literal $(, $arg:expr)*) => {
        snprintf_($buf.as_mut_ptr() as *mut c_char, $buf.len(), concat!($fmt, "\0").as_ptr() as *const c_char $(, $arg)*)
    };
}

// elog / ereport wrappers -- TODO(pg-port): replace with real macro infrastructure
macro_rules! elog {
    ($level:expr, $msg:literal $(, $arg:expr)*) => {
        {
            extern "C" {
                fn elog_start(filename: *const c_char, lineno: c_int, funcname: *const c_char);
                fn elog_finish(elevel: c_int, fmt: *const c_char, ...);
            }
            elog_start(
                concat!(file!(), "\0").as_ptr() as *const c_char,
                line!() as c_int,
                concat!(stringify!($level), "\0").as_ptr() as *const c_char,
            );
            elog_finish($level, concat!($msg, "\0").as_ptr() as *const c_char $(, $arg)*);
        }
    };
}

macro_rules! ereport {
    ($level:expr, ($($inner:expr),+)) => {
        {
            extern "C" {
                fn errstart(elevel: c_int, domain: *const c_char) -> bool;
                fn errfinish(filename: *const c_char, lineno: c_int, funcname: *const c_char);
            }
            if errstart($level, std::ptr::null()) {
                $( $inner; )+
                errfinish(
                    concat!(file!(), "\0").as_ptr() as *const c_char,
                    line!() as c_int,
                    concat!(module_path!(), "\0").as_ptr() as *const c_char,
                );
            }
        }
    };
}

macro_rules! errmsg {
    ($msg:literal $(, $arg:expr)*) => {
        { extern "C" { fn errmsg(fmt: *const c_char, ...) -> c_int; }
          errmsg(concat!($msg, "\0").as_ptr() as *const c_char $(, $arg)*) }
    };
}

macro_rules! errmsg_internal {
    ($msg:literal $(, $arg:expr)*) => {
        { extern "C" { fn errmsg_internal(fmt: *const c_char, ...) -> c_int; }
          errmsg_internal(concat!($msg, "\0").as_ptr() as *const c_char $(, $arg)*) }
    };
}

macro_rules! errcode {
    ($code:expr) => {
        { extern "C" { fn errcode(sqlerrcode: c_int) -> c_int; } errcode($code) }
    };
}

macro_rules! errdetail {
    ($msg:literal $(, $arg:expr)*) => {
        { extern "C" { fn errdetail(fmt: *const c_char, ...) -> c_int; }
          errdetail(concat!($msg, "\0").as_ptr() as *const c_char $(, $arg)*) }
    };
}

// Error codes -- errcodes.h (MAKE_SQLSTATE values as decimal placeholders)
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0x0803_0001; // SQLSTATE 08P01
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0x5500_0000; // SQLSTATE 55000
const ERRCODE_INVALID_BINARY_REPRESENTATION: c_int = 0x2203_0003; // SQLSTATE 22P03
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0x0A00_0000; // SQLSTATE 0A000
const ERRCODE_CONNECTION_FAILURE: c_int = 0x0800_0600; // SQLSTATE 08006
// log levels
const DEBUG1: c_int = 15;
const DEBUG2: c_int = 14;
const LOG: c_int = 17;
const ERROR: c_int = 21;
const WARNING: c_int = 19;

// ===========================================================================
// Part 2: Core functions
// ===========================================================================

/*
 * Form the origin name for the subscription.
 *
 * This is a common function for tablesync and other workers. Tablesync workers
 * must pass a valid relid. Other callers must pass relid = InvalidOid.
 *
 * Return the name in the supplied buffer.
 */
pub unsafe fn ReplicationOriginNameForLogicalRep(
    suboid: Oid,
    relid: Oid,
    originname: *mut c_char,
    szoriginname: Size,
) {
    if OidIsValid(relid) {
        /* Replication origin name for tablesync workers. */
        snprintf_(originname, szoriginname, b"%u_%u\0".as_ptr() as *const c_char, suboid, relid);
    } else {
        /* Replication origin name for non-tablesync workers. */
        snprintf_(originname, szoriginname, b"pg_%u\0".as_ptr() as *const c_char, suboid);
    }
}

/*
 * Should this worker apply changes for given relation.
 *
 * This is mainly needed for initial relation data sync as that runs in
 * separate worker process running in parallel and we need some way to skip
 * changes coming to the leader apply worker during the sync of a table.
 */
unsafe fn should_apply_changes_for_rel(rel: *mut LogicalRepRelMapEntry) -> bool {
    // Access typed fields via pointer arithmetic / casts.
    // LogicalRepRelMapEntry fields accessed via TODO(pg-port) field offsets.
    // For now use helper accessor stubs.
    let worker_type = (*MyLogicalRepWorker).type_;
    match worker_type {
        t if t == WORKERTYPE_TABLESYNC => {
            // return MyLogicalRepWorker->relid == rel->localreloid
            logicalrep_rel_mapentry_relid(rel) == (*MyLogicalRepWorker).relid
        }
        t if t == WORKERTYPE_PARALLEL_APPLY => {
            let state = logicalrep_rel_mapentry_state(rel);
            /* We don't synchronize rel's that are in unknown state. */
            if state != SUBREL_STATE_READY && state != SUBREL_STATE_UNKNOWN {
                ereport!(
                    ERROR,
                    (
                        errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg!(
                            "logical replication parallel apply worker for subscription \"%s\" will stop",
                            subscription_name(MySubscription)
                        ),
                        errdetail!(
                            "Cannot handle streamed replication transactions using parallel apply workers until all tables have been synchronized."
                        )
                    )
                );
            }
            state == SUBREL_STATE_READY
        }
        t if t == WORKERTYPE_APPLY => {
            let state = logicalrep_rel_mapentry_state(rel);
            let statelsn = logicalrep_rel_mapentry_statelsn(rel);
            state == SUBREL_STATE_READY
                || (state == SUBREL_STATE_SYNCDONE && statelsn <= remote_final_lsn)
        }
        _ => {
            /* Should never happen. */
            elog!(ERROR, "Unknown worker type");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Field accessor stubs for LogicalRepRelMapEntry -- TODO(pg-port): use real struct
// ---------------------------------------------------------------------------
extern "C" {
    fn logicalrep_rel_mapentry_relid(rel: *mut LogicalRepRelMapEntry) -> Oid;
    fn logicalrep_rel_mapentry_state(rel: *mut LogicalRepRelMapEntry) -> c_char;
    fn logicalrep_rel_mapentry_statelsn(rel: *mut LogicalRepRelMapEntry) -> XLogRecPtr;
    fn logicalrep_rel_mapentry_localreloid(rel: *mut LogicalRepRelMapEntry) -> Oid;
    fn logicalrep_rel_mapentry_localrel(rel: *mut LogicalRepRelMapEntry) -> *mut c_void;
    fn logicalrep_rel_mapentry_remoterel(rel: *mut LogicalRepRelMapEntry) -> *mut LogicalRepRelation;
    fn logicalrep_rel_mapentry_attrmap(rel: *mut LogicalRepRelMapEntry) -> *mut c_void; // AttrMap*
    fn logicalrep_rel_mapentry_updatable(rel: *mut LogicalRepRelMapEntry) -> bool;
    fn logicalrep_rel_mapentry_localindexoid(rel: *mut LogicalRepRelMapEntry) -> Oid;
    fn logicalrep_relmaentry_attrmap_maplen(rel: *mut LogicalRepRelMapEntry) -> c_int;
    fn logicalrep_relmaentry_attrmap_attnums(rel: *mut LogicalRepRelMapEntry, i: c_int) -> c_int;
    fn logicalrep_reldata_natts(rel: *mut LogicalRepRelation) -> c_int;
    fn logicalrep_reldata_replident(rel: *mut LogicalRepRelation) -> c_char;
    fn logicalrep_reldata_attnames(rel: *mut LogicalRepRelation, i: c_int) -> *const c_char;
    fn logicalrep_reldata_nspname(rel: *mut LogicalRepRelation) -> *const c_char;
    fn logicalrep_reldata_relname(rel: *mut LogicalRepRelation) -> *const c_char;
    // TupleData accessors
    fn logicalrep_tupledata_ncols(td: *mut LogicalRepTupleData) -> c_int;
    fn logicalrep_tupledata_colstatus(td: *mut LogicalRepTupleData, i: c_int) -> c_char;
    fn logicalrep_tupledata_colvalue(td: *mut LogicalRepTupleData, i: c_int) -> *mut StringInfoData;
    // Subscription field accessors
    fn subscription_name(sub: *mut Subscription) -> *const c_char;
    fn subscription_oid(sub: *mut Subscription) -> Oid;
    fn subscription_dbid(sub: *mut Subscription) -> Oid;
    fn subscription_enabled(sub: *mut Subscription) -> bool;
    fn subscription_slotname(sub: *mut Subscription) -> *const c_char;
    fn subscription_conninfo(sub: *mut Subscription) -> *const c_char;
    fn subscription_binary(sub: *mut Subscription) -> bool;
    fn subscription_stream(sub: *mut Subscription) -> c_int;
    fn subscription_passwordrequired(sub: *mut Subscription) -> bool;
    fn subscription_ownersuperuser(sub: *mut Subscription) -> bool;
    fn subscription_runasowner(sub: *mut Subscription) -> bool;
    fn subscription_twophasestate(sub: *mut Subscription) -> c_char;
    fn subscription_synccommit(sub: *mut Subscription) -> *const c_char;
    fn subscription_publications(sub: *mut Subscription) -> *mut List;
    fn subscription_origin(sub: *mut Subscription) -> *const c_char;
    fn subscription_owner(sub: *mut Subscription) -> Oid;
    fn subscription_skiplsn(sub: *mut Subscription) -> XLogRecPtr;
    fn subscription_disableonerr(sub: *mut Subscription) -> bool;
    // Relation field accessors
    fn relation_rd_rel(rel: *mut c_void) -> *mut c_void;
    fn pg_class_relkind(pg_class_ptr: *mut c_void) -> c_char;
    fn pg_class_relhasindex(pg_class_ptr: *mut c_void) -> bool;
    fn pg_class_relispartition(pg_class_ptr: *mut c_void) -> bool;
    fn pg_class_relowner(pg_class_ptr: *mut c_void) -> Oid;
    fn form_pg_attribute_attisdropped(att: *mut c_void) -> bool;
    fn form_pg_attribute_attgenerated(att: *mut c_void) -> bool;
    fn form_pg_attribute_atttypid(att: *mut c_void) -> Oid;
    fn form_pg_attribute_atttypmod(att: *mut c_void) -> i32;
    fn estate_es_tupleTable(estate: *mut EState) -> *mut *mut c_void;
    fn estate_es_rteperminfos(estate: *mut EState) -> *mut List;
    fn estate_es_opened_result_relations(estate: *mut EState) -> *mut List;
    fn estate_set_opened_result_relations(estate: *mut EState, list: *mut List);
    fn estate_set_output_cid(estate: *mut EState, cid: uint32);
    fn rte_set_rtekind(rte: *mut RangeTblEntry, kind: c_int);
    fn rte_set_relid(rte: *mut RangeTblEntry, relid: Oid);
    fn rte_set_relkind(rte: *mut RangeTblEntry, relkind: c_char);
    fn rte_set_rellockmode(rte: *mut RangeTblEntry, lockmode: c_int);
    fn mtstate_set_ps_plan(mtstate: *mut ModifyTableState, plan: *mut c_void);
    fn mtstate_set_ps_state(mtstate: *mut ModifyTableState, state: *mut EState);
    fn mtstate_set_operation(mtstate: *mut ModifyTableState, op: CmdType);
    fn mtstate_set_resultRelInfo(mtstate: *mut ModifyTableState, rri: *mut ResultRelInfo);
    fn resultrelinfo_ri_RelationDesc(rri: *mut ResultRelInfo) -> *mut c_void;
    fn resultrelinfo_ri_IndexRelationDescs(rri: *mut ResultRelInfo) -> *mut c_void;
    fn resultrelinfo_ri_onConflictArbiterIndexes(rri: *mut ResultRelInfo) -> *mut List;
    fn resultrelinfo_ri_PartitionTupleSlot(rri: *mut ResultRelInfo) -> *mut TupleTableSlot;
    fn tupleslot_tts_tupleDescriptor(slot: *mut TupleTableSlot) -> *mut c_void;
    fn tupleslot_tts_values(slot: *mut TupleTableSlot) -> *mut Datum;
    fn tupleslot_tts_isnull(slot: *mut TupleTableSlot) -> *mut bool;
    fn tupleslot_natts(slot: *mut TupleTableSlot) -> c_int;
    fn tupleconversionmap_attrMap(map: *mut TupleConversionMap) -> *mut AttrMap;
    fn epqstate_set_slot(epqstate: *mut EPQState, slot: *mut TupleTableSlot);
    fn rteperminfo_updatedCols(rte: *mut RTEPermissionInfo) -> *mut Bitmapset;
    fn rteperminfo_set_updatedCols(rte: *mut RTEPermissionInfo, bms: *mut Bitmapset);
    fn bms_add_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset;
    fn parallel_apply_winfo_shared(winfo: *mut ParallelApplyWorkerInfo) -> *mut c_void;
    fn parallel_apply_winfo_serialize_changes(winfo: *mut ParallelApplyWorkerInfo) -> bool;
    fn parallel_apply_winfo_shared_xid(winfo: *mut ParallelApplyWorkerInfo) -> TransactionId;
    fn parallel_apply_winfo_shared_pending_stream_count(winfo: *mut ParallelApplyWorkerInfo) -> *mut c_void;
    /* Logical rep worker parallel_apply field */
    fn logicalrep_worker_parallel_apply(w: *mut LogicalRepWorker) -> bool;
}

/*
 * Begin one step (one INSERT, UPDATE, etc) of a replication transaction.
 *
 * Start a transaction, if this is the first step (else we keep using the
 * existing transaction).
 * Also provide a global snapshot and ensure we run in ApplyMessageContext.
 */
unsafe fn begin_replication_step() {
    SetCurrentStatementStartTimestamp();

    if !IsTransactionState() {
        StartTransactionCommand();
        maybe_reread_subscription();
    }

    PushActiveSnapshot(GetTransactionSnapshot());

    MemoryContextSwitchTo(ApplyMessageContext);
}

/*
 * Finish up one step of a replication transaction.
 * Callers of begin_replication_step() must also call this.
 *
 * We don't close out the transaction here, but we should increment
 * the command counter to make the effects of this step visible.
 */
unsafe fn end_replication_step() {
    PopActiveSnapshot();
    CommandCounterIncrement();
}

/*
 * Handle streamed transactions for both the leader apply worker and the
 * parallel apply workers.
 *
 * Returns true for streamed transactions (when the change is either serialized
 * to file or sent to parallel apply worker), false otherwise.
 */
unsafe fn handle_streamed_transaction(action: LogicalRepMsgType, s: *mut StringInfoData) -> bool {
    let current_xid: TransactionId;
    let mut winfo: *mut ParallelApplyWorkerInfo = null_mut();
    let apply_action: TransApplyAction;
    let original_msg: StringInfoData;

    apply_action = get_transaction_apply_action(stream_xid, &mut winfo);

    /* not in streaming mode */
    if apply_action == TRANS_LEADER_APPLY {
        return false;
    }

    // Assert(TransactionIdIsValid(stream_xid));

    /*
     * The parallel apply worker needs the xid in this message to decide
     * whether to define a savepoint, so save the original message that has
     * not moved the cursor after the xid.
     */
    original_msg = core::ptr::read(s);

    /*
     * We should have received XID of the subxact as the first part of the
     * message, so extract it.
     */
    current_xid = pq_getmsgint(s, 4);

    if !TransactionIdIsValid(current_xid) {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("invalid transaction ID in streamed replication transaction")
            )
        );
    }

    match apply_action {
        TRANS_LEADER_SERIALIZE => {
            // Assert(stream_fd);
            /* Add the new subxact to the array (unless already there). */
            subxact_info_add(current_xid);
            /* Write the change to the current file */
            stream_write_change(action, s);
            return true;
        }
        TRANS_LEADER_SEND_TO_PARALLEL => {
            // Assert(winfo);

            /*
             * XXX The publisher side doesn't always send relation/type update
             * messages after the streaming transaction, so also update the
             * relation/type in leader apply worker.
             */
            if pa_send_data(winfo, (*s).len as usize, (*s).data) {
                return action != LOGICAL_REP_MSG_RELATION && action != LOGICAL_REP_MSG_TYPE;
            }

            /*
             * Switch to serialize mode when we are not able to send the
             * change to parallel apply worker.
             */
            pa_switch_to_partial_serialize(winfo, false);

            /* fall through to PARTIAL_SERIALIZE */
            let orig = original_msg;
            stream_write_change(action, &orig as *const StringInfoData as *mut StringInfoData);
            return action != LOGICAL_REP_MSG_RELATION && action != LOGICAL_REP_MSG_TYPE;
        }
        TRANS_LEADER_PARTIAL_SERIALIZE => {
            let orig = original_msg;
            stream_write_change(action, &orig as *const StringInfoData as *mut StringInfoData);
            return action != LOGICAL_REP_MSG_RELATION && action != LOGICAL_REP_MSG_TYPE;
        }
        TRANS_PARALLEL_APPLY => {
            parallel_stream_nchanges += 1;
            /* Define a savepoint for a subxact if needed. */
            pa_start_subtrans(current_xid, stream_xid);
            return false;
        }
        _ => {
            elog!(ERROR, "unexpected apply action: %d", apply_action as c_int);
            return false; /* silence compiler warning */
        }
    }
}

/*
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers for the specified relation.
 */
unsafe fn create_edata_for_relation(rel: *mut LogicalRepRelMapEntry) -> *mut ApplyExecutionData {
    let edata: *mut ApplyExecutionData;
    let estate: *mut EState;
    let rte: *mut RangeTblEntry;
    let mut perminfos: *mut List = NIL;
    let resultrelinfo: *mut ResultRelInfo;
    let localrel = logicalrep_rel_mapentry_localrel(rel);

    edata = palloc0(std::mem::size_of::<ApplyExecutionData>()) as *mut ApplyExecutionData;
    (*edata).targetRel = rel;

    (*edata).estate = CreateExecutorState();
    estate = (*edata).estate;

    rte = makeNode_RangeTblEntry();
    rte_set_rtekind(rte, 0 /* RTE_RELATION */);
    rte_set_relid(rte, RelationGetRelid(localrel));
    rte_set_relkind(rte, pg_class_relkind(relation_rd_rel(localrel)));
    rte_set_rellockmode(rte, AccessShareLock);

    addRTEPermissionInfo(&mut perminfos, rte);

    ExecInitRangeTable(estate, list_make1(rte as *mut c_void), perminfos,
                       bms_make_singleton(1));

    (*edata).targetRelInfo = makeNode_ResultRelInfo();
    resultrelinfo = (*edata).targetRelInfo;

    /*
     * Use Relation opened by logicalrep_rel_open() instead of opening it
     * again.
     */
    InitResultRelInfo(resultrelinfo, localrel, 1, null_mut(), 0);

    /*
     * We put the ResultRelInfo in the es_opened_result_relations list, even
     * though we don't populate the es_result_relations array.
     */
    let cur_list = estate_es_opened_result_relations(estate);
    let new_list = lappend(cur_list, resultrelinfo as *mut c_void);
    estate_set_opened_result_relations(estate, new_list);

    estate_set_output_cid(estate, GetCurrentCommandId(true));

    /* Prepare to catch AFTER triggers. */
    AfterTriggerBeginQuery();

    /* other fields of edata remain NULL for now */

    edata
}

/*
 * Finish any operations related to the executor state created by
 * create_edata_for_relation().
 */
unsafe fn finish_edata(edata: *mut ApplyExecutionData) {
    let estate = (*edata).estate;

    /* Handle any queued AFTER triggers. */
    AfterTriggerEndQuery(estate);

    /* Shut down tuple routing, if any was done. */
    if !(*edata).proute.is_null() {
        ExecCleanupTupleRouting((*edata).mtstate, (*edata).proute);
    }

    /*
     * Cleanup.  It might seem that we should call ExecCloseResultRelations()
     * here, but we intentionally don't.
     */
    ExecResetTupleTable(*estate_es_tupleTable(estate), false);
    FreeExecutorState(estate);
    pfree(edata as *mut c_void);
}

/*
 * Executes default values for columns for which we can't map to remote
 * relation columns.
 *
 * This allows us to support tables which have more columns on the downstream
 * than on the upstream.
 */
unsafe fn slot_fill_defaults(
    rel: *mut LogicalRepRelMapEntry,
    estate: *mut EState,
    slot: *mut TupleTableSlot,
) {
    let localrel = logicalrep_rel_mapentry_localrel(rel);
    let desc = RelationGetDescr(localrel);
    let num_phys_attrs = tupleslot_natts(slot); /* use slot's descriptor */
    let mut i: c_int;
    let mut attnum: c_int;
    let mut num_defaults: c_int = 0;
    let defmap: *mut c_int;
    let defexprs: *mut *mut c_void;
    let econtext: *mut c_void;

    econtext = GetPerTupleExprContext(estate);

    /* We got all the data via replication, no need to evaluate anything. */
    if num_phys_attrs == logicalrep_reldata_natts(logicalrep_rel_mapentry_remoterel(rel)) {
        return;
    }

    defmap = palloc((num_phys_attrs as usize) * std::mem::size_of::<c_int>()) as *mut c_int;
    defexprs = palloc((num_phys_attrs as usize) * std::mem::size_of::<*mut c_void>()) as *mut *mut c_void;

    attnum = 0;
    while attnum < num_phys_attrs {
        let att = TupleDescAttr(desc, attnum);

        if !form_pg_attribute_attisdropped(att) && !form_pg_attribute_attgenerated(att) {
            if logicalrep_relmaentry_attrmap_attnums(rel, attnum) < 0 {
                let defexpr = build_column_default(localrel, attnum + 1);
                if !defexpr.is_null() {
                    /* Run the expression through planner */
                    let defexpr = expression_planner(defexpr);
                    /* Initialize executable expression in copycontext */
                    *defexprs.add(num_defaults as usize) = ExecInitExpr(defexpr, null_mut());
                    *defmap.add(num_defaults as usize) = attnum;
                    num_defaults += 1;
                }
            }
        }
        attnum += 1;
    }

    i = 0;
    while i < num_defaults {
        let attnum_d = *defmap.add(i as usize);
        let slot_values = tupleslot_tts_values(slot);
        let slot_isnull = tupleslot_tts_isnull(slot);
        *slot_values.add(attnum_d as usize) = ExecEvalExpr(
            *defexprs.add(i as usize),
            econtext,
            slot_isnull.add(attnum_d as usize),
        );
        i += 1;
    }
}

/*
 * Store tuple data into slot.
 *
 * Incoming data can be either text or binary format.
 */
unsafe fn slot_store_data(
    slot: *mut TupleTableSlot,
    rel: *mut LogicalRepRelMapEntry,
    tupledata: *mut LogicalRepTupleData,
) {
    let natts = tupleslot_natts(slot);
    let mut i: c_int;

    ExecClearTuple(slot);

    /* Call the "in" function for each non-dropped, non-null attribute */
    // Assert(natts == rel->attrmap->maplen);
    i = 0;
    while i < natts {
        let desc = tupleslot_tts_tupleDescriptor(slot);
        let att = TupleDescAttr(desc, i);
        let remoteattnum = logicalrep_relmaentry_attrmap_attnums(rel, i);
        let slot_values = tupleslot_tts_values(slot);
        let slot_isnull = tupleslot_tts_isnull(slot);

        if !form_pg_attribute_attisdropped(att) && remoteattnum >= 0 {
            // Assert(remoteattnum < tupledata->ncols);

            /* Set attnum for error callback */
            apply_error_callback_arg.remote_attnum = remoteattnum;

            let colvalue = logicalrep_tupledata_colvalue(tupledata, remoteattnum);
            let colstatus = logicalrep_tupledata_colstatus(tupledata, remoteattnum);

            if colstatus == LOGICALREP_COLUMN_TEXT {
                let mut typinput: Oid = 0;
                let mut typioparam: Oid = 0;
                getTypeInputInfo(form_pg_attribute_atttypid(att), &mut typinput, &mut typioparam);
                *slot_values.add(i as usize) = OidInputFunctionCall(
                    typinput,
                    (*colvalue).data,
                    typioparam,
                    form_pg_attribute_atttypmod(att),
                );
                *slot_isnull.add(i as usize) = false;
            } else if colstatus == LOGICALREP_COLUMN_BINARY {
                let mut typreceive: Oid = 0;
                let mut typioparam: Oid = 0;

                /*
                 * In some code paths we may be asked to re-parse the same
                 * tuple data. Reset the StringInfo's cursor so that works.
                 */
                (*colvalue).cursor = 0;

                getTypeBinaryInputInfo(form_pg_attribute_atttypid(att), &mut typreceive, &mut typioparam);
                *slot_values.add(i as usize) = OidReceiveFunctionCall(
                    typreceive,
                    colvalue,
                    typioparam,
                    form_pg_attribute_atttypmod(att),
                );

                /* Trouble if it didn't eat the whole buffer */
                if (*colvalue).cursor != (*colvalue).len {
                    ereport!(
                        ERROR,
                        (
                            errcode!(ERRCODE_INVALID_BINARY_REPRESENTATION),
                            errmsg!(
                                "incorrect binary data format in logical replication column %d",
                                remoteattnum + 1
                            )
                        )
                    );
                }
                *slot_isnull.add(i as usize) = false;
            } else {
                /*
                 * NULL value from remote. (We don't expect to see
                 * LOGICALREP_COLUMN_UNCHANGED here, but if we do, treat it as
                 * NULL.)
                 */
                *slot_values.add(i as usize) = 0;
                *slot_isnull.add(i as usize) = true;
            }

            /* Reset attnum for error callback */
            apply_error_callback_arg.remote_attnum = -1;
        } else {
            /*
             * We assign NULL to dropped attributes and missing values
             * (missing values should be later filled using
             * slot_fill_defaults).
             */
            *slot_values.add(i as usize) = 0;
            *slot_isnull.add(i as usize) = true;
        }

        i += 1;
    }

    ExecStoreVirtualTuple(slot);
}

/*
 * Replace updated columns with data from the LogicalRepTupleData struct.
 * This is somewhat similar to heap_modify_tuple but also calls the type
 * input functions on the user data.
 *
 * "slot" is filled with a copy of the tuple in "srcslot", replacing
 * columns provided in "tupleData" and leaving others as-is.
 */
unsafe fn slot_modify_data(
    slot: *mut TupleTableSlot,
    srcslot: *mut TupleTableSlot,
    rel: *mut LogicalRepRelMapEntry,
    tupledata: *mut LogicalRepTupleData,
) {
    let natts = tupleslot_natts(slot);
    let mut i: c_int;

    /* We'll fill "slot" with a virtual tuple, so we must start with ... */
    ExecClearTuple(slot);

    /*
     * Copy all the column data from srcslot, so that we'll have valid values
     * for unreplaced columns.
     */
    // Assert(natts == srcslot->tts_tupleDescriptor->natts);
    slot_getallattrs(srcslot);
    std::ptr::copy_nonoverlapping(
        tupleslot_tts_values(srcslot),
        tupleslot_tts_values(slot),
        natts as usize,
    );
    std::ptr::copy_nonoverlapping(
        tupleslot_tts_isnull(srcslot),
        tupleslot_tts_isnull(slot),
        natts as usize,
    );

    /* Call the "in" function for each replaced attribute */
    // Assert(natts == rel->attrmap->maplen);
    i = 0;
    while i < natts {
        let desc = tupleslot_tts_tupleDescriptor(slot);
        let att = TupleDescAttr(desc, i);
        let remoteattnum = logicalrep_relmaentry_attrmap_attnums(rel, i);

        if remoteattnum < 0 {
            i += 1;
            continue;
        }

        // Assert(remoteattnum < tupleData->ncols);

        let colstatus = logicalrep_tupledata_colstatus(tupledata, remoteattnum);

        if colstatus != LOGICALREP_COLUMN_UNCHANGED {
            let colvalue = logicalrep_tupledata_colvalue(tupledata, remoteattnum);
            let slot_values = tupleslot_tts_values(slot);
            let slot_isnull = tupleslot_tts_isnull(slot);

            /* Set attnum for error callback */
            apply_error_callback_arg.remote_attnum = remoteattnum;

            if colstatus == LOGICALREP_COLUMN_TEXT {
                let mut typinput: Oid = 0;
                let mut typioparam: Oid = 0;
                getTypeInputInfo(form_pg_attribute_atttypid(att), &mut typinput, &mut typioparam);
                *slot_values.add(i as usize) = OidInputFunctionCall(
                    typinput,
                    (*colvalue).data,
                    typioparam,
                    form_pg_attribute_atttypmod(att),
                );
                *slot_isnull.add(i as usize) = false;
            } else if colstatus == LOGICALREP_COLUMN_BINARY {
                let mut typreceive: Oid = 0;
                let mut typioparam: Oid = 0;

                /*
                 * In some code paths we may be asked to re-parse the same
                 * tuple data. Reset the StringInfo's cursor so that works.
                 */
                (*colvalue).cursor = 0;

                getTypeBinaryInputInfo(form_pg_attribute_atttypid(att), &mut typreceive, &mut typioparam);
                *slot_values.add(i as usize) = OidReceiveFunctionCall(
                    typreceive,
                    colvalue,
                    typioparam,
                    form_pg_attribute_atttypmod(att),
                );

                /* Trouble if it didn't eat the whole buffer */
                if (*colvalue).cursor != (*colvalue).len {
                    ereport!(
                        ERROR,
                        (
                            errcode!(ERRCODE_INVALID_BINARY_REPRESENTATION),
                            errmsg!(
                                "incorrect binary data format in logical replication column %d",
                                remoteattnum + 1
                            )
                        )
                    );
                }
                *slot_isnull.add(i as usize) = false;
            } else {
                /* must be LOGICALREP_COLUMN_NULL */
                *slot_values.add(i as usize) = 0;
                *slot_isnull.add(i as usize) = true;
            }

            /* Reset attnum for error callback */
            apply_error_callback_arg.remote_attnum = -1;
        }

        i += 1;
    }

    /* And finally, declare that "slot" contains a valid virtual tuple */
    ExecStoreVirtualTuple(slot);
}

// ===========================================================================
// Part 3: apply_handle_begin through apply_handle_stream_start
// ===========================================================================

/*
 * Handle BEGIN message.
 */
unsafe fn apply_handle_begin(s: *mut StringInfoData) {
    let mut begin_data: [u8; 256] = [0u8; 256]; // LogicalRepBeginData placeholder
    let begin_data_ptr = begin_data.as_mut_ptr() as *mut LogicalRepBeginData;

    /* There must not be an active streaming transaction. */
    // Assert(!TransactionIdIsValid(stream_xid));

    logicalrep_read_begin(s, begin_data_ptr);
    let (xid, final_lsn) = logicalrep_begin_data_extract(begin_data_ptr);
    set_apply_error_context_xact(xid, final_lsn);

    remote_final_lsn = final_lsn;

    maybe_start_skipping_changes(final_lsn);

    in_remote_transaction = true;

    pgstat_report_activity(STATE_RUNNING, null());
}

// Accessor for LogicalRepBeginData fields -- TODO(pg-port)
extern "C" {
    fn logicalrep_commit_data_commit_lsn(d: *mut LogicalRepCommitData) -> XLogRecPtr;
    fn logicalrep_commit_data_end_lsn(d: *mut LogicalRepCommitData) -> XLogRecPtr;
    fn logicalrep_commit_data_committime(d: *mut LogicalRepCommitData) -> TimestampTz;
    fn logicalrep_prepared_data_xid(d: *mut LogicalRepPreparedTxnData) -> TransactionId;
    fn logicalrep_prepared_data_prepare_lsn(d: *mut LogicalRepPreparedTxnData) -> XLogRecPtr;
    fn logicalrep_prepared_data_end_lsn(d: *mut LogicalRepPreparedTxnData) -> XLogRecPtr;
    fn logicalrep_prepared_data_prepare_time(d: *mut LogicalRepPreparedTxnData) -> TimestampTz;
    fn logicalrep_commit_prepared_data_xid(d: *mut LogicalRepCommitPreparedTxnData) -> TransactionId;
    fn logicalrep_commit_prepared_data_commit_lsn(d: *mut LogicalRepCommitPreparedTxnData) -> XLogRecPtr;
    fn logicalrep_commit_prepared_data_end_lsn(d: *mut LogicalRepCommitPreparedTxnData) -> XLogRecPtr;
    fn logicalrep_commit_prepared_data_commit_time(d: *mut LogicalRepCommitPreparedTxnData) -> TimestampTz;
    fn logicalrep_rollback_prepared_data_xid(d: *mut LogicalRepRollbackPreparedTxnData) -> TransactionId;
    fn logicalrep_rollback_prepared_data_prepare_end_lsn(d: *mut LogicalRepRollbackPreparedTxnData) -> XLogRecPtr;
    fn logicalrep_rollback_prepared_data_prepare_time(d: *mut LogicalRepRollbackPreparedTxnData) -> TimestampTz;
    fn logicalrep_rollback_prepared_data_rollback_end_lsn(d: *mut LogicalRepRollbackPreparedTxnData) -> XLogRecPtr;
    fn logicalrep_rollback_prepared_data_rollback_time(d: *mut LogicalRepRollbackPreparedTxnData) -> TimestampTz;
    fn logicalrep_stream_abort_data_xid(d: *mut LogicalRepStreamAbortData) -> TransactionId;
    fn logicalrep_stream_abort_data_subxid(d: *mut LogicalRepStreamAbortData) -> TransactionId;
    fn logicalrep_stream_abort_data_abort_lsn(d: *mut LogicalRepStreamAbortData) -> XLogRecPtr;
    fn logicalrep_stream_abort_data_ptr(d: *mut LogicalRepStreamAbortData) -> *mut LogicalRepStreamAbortData;
}

/*
 * Handle COMMIT message.
 *
 * TODO, support tracking of multiple origins
 */
unsafe fn apply_handle_commit(s: *mut StringInfoData) {
    let mut commit_data_buf: [u8; 256] = [0u8; 256];
    let commit_data = commit_data_buf.as_mut_ptr() as *mut LogicalRepCommitData;

    logicalrep_read_commit(s, commit_data);

    let commit_lsn = logicalrep_commit_data_commit_lsn(commit_data);
    let end_lsn = logicalrep_commit_data_end_lsn(commit_data);

    if commit_lsn != remote_final_lsn {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!(
                    "incorrect commit LSN %X/%X in commit message (expected %X/%X)",
                    commit_lsn >> 32,
                    commit_lsn & 0xFFFFFFFF,
                    remote_final_lsn >> 32,
                    remote_final_lsn & 0xFFFFFFFF
                )
            )
        );
    }

    apply_handle_commit_internal(commit_data);

    /* Process any tables that are being synchronized in parallel. */
    process_syncing_tables(end_lsn);

    pgstat_report_activity(STATE_IDLE, null());
    reset_apply_error_context_info();
}

/*
 * Handle BEGIN PREPARE message.
 */
unsafe fn apply_handle_begin_prepare(s: *mut StringInfoData) {
    let mut begin_data_buf: [u8; 256] = [0u8; 256];
    let begin_data = begin_data_buf.as_mut_ptr() as *mut LogicalRepPreparedTxnData;

    /* Tablesync should never receive prepare. */
    if am_tablesync_worker() {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("tablesync worker received a BEGIN PREPARE message")
            )
        );
    }

    /* There must not be an active streaming transaction. */
    // Assert(!TransactionIdIsValid(stream_xid));

    logicalrep_read_begin_prepare(s, begin_data);
    let xid = logicalrep_prepared_data_xid(begin_data);
    let prepare_lsn = logicalrep_prepared_data_prepare_lsn(begin_data);
    set_apply_error_context_xact(xid, prepare_lsn);

    remote_final_lsn = prepare_lsn;

    maybe_start_skipping_changes(prepare_lsn);

    in_remote_transaction = true;

    pgstat_report_activity(STATE_RUNNING, null());
}

/*
 * Common function to prepare the GID.
 */
unsafe fn apply_handle_prepare_internal(prepare_data: *mut LogicalRepPreparedTxnData) {
    let mut gid: [c_char; GIDSIZE] = [0; GIDSIZE];

    /*
     * Compute unique GID for two_phase transactions. We don't use GID of
     * prepared transaction sent by server as that can lead to deadlock when
     * we have multiple subscriptions from same node point to publications on
     * the same node. See comments atop worker.c
     */
    let sub_oid = subscription_oid(MySubscription);
    let xid = logicalrep_prepared_data_xid(prepare_data);
    TwoPhaseTransactionGid(sub_oid, xid, gid.as_mut_ptr(), GIDSIZE as c_int);

    /*
     * BeginTransactionBlock is necessary to balance the EndTransactionBlock
     * called within the PrepareTransactionBlock below.
     */
    if !IsTransactionBlock() {
        BeginTransactionBlock();
        CommitTransactionCommand(); /* Completes the preceding Begin command. */
    }

    /*
     * Update origin state so we can restart streaming from correct position
     * in case of crash.
     */
    replorigin_session_origin_lsn = logicalrep_prepared_data_end_lsn(prepare_data);
    replorigin_session_origin_timestamp = logicalrep_prepared_data_prepare_time(prepare_data);

    PrepareTransactionBlock(gid.as_ptr());
}

/*
 * Handle PREPARE message.
 */
unsafe fn apply_handle_prepare(s: *mut StringInfoData) {
    let mut prepare_data_buf: [u8; 256] = [0u8; 256];
    let prepare_data = prepare_data_buf.as_mut_ptr() as *mut LogicalRepPreparedTxnData;

    logicalrep_read_prepare(s, prepare_data);

    let prepare_lsn = logicalrep_prepared_data_prepare_lsn(prepare_data);
    let end_lsn = logicalrep_prepared_data_end_lsn(prepare_data);

    if prepare_lsn != remote_final_lsn {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!(
                    "incorrect prepare LSN %X/%X in prepare message (expected %X/%X)",
                    prepare_lsn >> 32,
                    prepare_lsn & 0xFFFFFFFF,
                    remote_final_lsn >> 32,
                    remote_final_lsn & 0xFFFFFFFF
                )
            )
        );
    }

    /*
     * Unlike commit, here, we always prepare the transaction even though no
     * change has happened in this transaction or all changes are skipped.
     */
    begin_replication_step();

    apply_handle_prepare_internal(prepare_data);

    end_replication_step();
    CommitTransactionCommand();
    pgstat_report_stat(false);

    /*
     * It is okay not to set the local_end LSN for the prepare because we
     * always flush the prepare record.
     */
    store_flush_position(end_lsn, InvalidXLogRecPtr);

    in_remote_transaction = false;

    /* Process any tables that are being synchronized in parallel. */
    process_syncing_tables(end_lsn);

    /*
     * Since we have already prepared the transaction, in a case where the
     * server crashes before clearing the subskiplsn, it will be left but the
     * transaction won't be resent. But that's okay because it's a rare case
     * and the subskiplsn will be cleared when finishing the next transaction.
     */
    stop_skipping_changes();
    clear_subscription_skip_lsn(prepare_lsn);

    pgstat_report_activity(STATE_IDLE, null());
    reset_apply_error_context_info();
}

/*
 * Handle a COMMIT PREPARED of a previously PREPARED transaction.
 */
unsafe fn apply_handle_commit_prepared(s: *mut StringInfoData) {
    let mut prepare_data_buf: [u8; 256] = [0u8; 256];
    let prepare_data = prepare_data_buf.as_mut_ptr() as *mut LogicalRepCommitPreparedTxnData;
    let mut gid: [c_char; GIDSIZE] = [0; GIDSIZE];

    logicalrep_read_commit_prepared(s, prepare_data);
    let xid = logicalrep_commit_prepared_data_xid(prepare_data);
    let commit_lsn = logicalrep_commit_prepared_data_commit_lsn(prepare_data);
    let end_lsn = logicalrep_commit_prepared_data_end_lsn(prepare_data);
    let commit_time = logicalrep_commit_prepared_data_commit_time(prepare_data);
    set_apply_error_context_xact(xid, commit_lsn);

    /* Compute GID for two_phase transactions. */
    TwoPhaseTransactionGid(subscription_oid(MySubscription), xid, gid.as_mut_ptr(), GIDSIZE as c_int);

    /* There is no transaction when COMMIT PREPARED is called */
    begin_replication_step();

    /*
     * Update origin state so we can restart streaming from correct position
     * in case of crash.
     */
    replorigin_session_origin_lsn = end_lsn;
    replorigin_session_origin_timestamp = commit_time;

    FinishPreparedTransaction(gid.as_ptr(), true);
    end_replication_step();
    CommitTransactionCommand();
    pgstat_report_stat(false);

    store_flush_position(end_lsn, XactLastCommitEnd);
    in_remote_transaction = false;

    /* Process any tables that are being synchronized in parallel. */
    process_syncing_tables(end_lsn);

    clear_subscription_skip_lsn(end_lsn);

    pgstat_report_activity(STATE_IDLE, null());
    reset_apply_error_context_info();
}

/*
 * Handle a ROLLBACK PREPARED of a previously PREPARED TRANSACTION.
 */
unsafe fn apply_handle_rollback_prepared(s: *mut StringInfoData) {
    let mut rollback_data_buf: [u8; 256] = [0u8; 256];
    let rollback_data = rollback_data_buf.as_mut_ptr() as *mut LogicalRepRollbackPreparedTxnData;
    let mut gid: [c_char; GIDSIZE] = [0; GIDSIZE];

    logicalrep_read_rollback_prepared(s, rollback_data);
    let xid = logicalrep_rollback_prepared_data_xid(rollback_data);
    let rollback_end_lsn = logicalrep_rollback_prepared_data_rollback_end_lsn(rollback_data);
    let prepare_end_lsn = logicalrep_rollback_prepared_data_prepare_end_lsn(rollback_data);
    let prepare_time = logicalrep_rollback_prepared_data_prepare_time(rollback_data);
    let rollback_time = logicalrep_rollback_prepared_data_rollback_time(rollback_data);
    set_apply_error_context_xact(xid, rollback_end_lsn);

    /* Compute GID for two_phase transactions. */
    TwoPhaseTransactionGid(subscription_oid(MySubscription), xid, gid.as_mut_ptr(), GIDSIZE as c_int);

    /*
     * It is possible that we haven't received prepare because it occurred
     * before walsender reached a consistent point or the two_phase was still
     * not enabled by that time, so in such cases, we need to skip rollback
     * prepared.
     */
    if LookupGXact(gid.as_ptr(), prepare_end_lsn, prepare_time) {
        /*
         * Update origin state so we can restart streaming from correct
         * position in case of crash.
         */
        replorigin_session_origin_lsn = rollback_end_lsn;
        replorigin_session_origin_timestamp = rollback_time;

        /* There is no transaction when ABORT/ROLLBACK PREPARED is called */
        begin_replication_step();
        FinishPreparedTransaction(gid.as_ptr(), false);
        end_replication_step();
        CommitTransactionCommand();

        clear_subscription_skip_lsn(rollback_end_lsn);
    }

    pgstat_report_stat(false);

    /*
     * It is okay not to set the local_end LSN for the rollback of prepared
     * transaction because we always flush the WAL record for it.
     */
    store_flush_position(rollback_end_lsn, InvalidXLogRecPtr);
    in_remote_transaction = false;

    /* Process any tables that are being synchronized in parallel. */
    process_syncing_tables(rollback_end_lsn);

    pgstat_report_activity(STATE_IDLE, null());
    reset_apply_error_context_info();
}

/*
 * Handle STREAM PREPARE.
 */
unsafe fn apply_handle_stream_prepare(s: *mut StringInfoData) {
    let mut prepare_data_buf: [u8; 256] = [0u8; 256];
    let prepare_data = prepare_data_buf.as_mut_ptr() as *mut LogicalRepPreparedTxnData;
    let mut winfo: *mut ParallelApplyWorkerInfo = null_mut();
    let apply_action: TransApplyAction;

    /* Save the message before it is consumed. */
    let original_msg: StringInfoData = core::ptr::read(s);

    if in_streamed_transaction {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("STREAM PREPARE message without STREAM STOP")
            )
        );
    }

    /* Tablesync should never receive prepare. */
    if am_tablesync_worker() {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("tablesync worker received a STREAM PREPARE message")
            )
        );
    }

    logicalrep_read_stream_prepare(s, prepare_data);
    let xid = logicalrep_prepared_data_xid(prepare_data);
    let prepare_lsn = logicalrep_prepared_data_prepare_lsn(prepare_data);
    let end_lsn = logicalrep_prepared_data_end_lsn(prepare_data);
    set_apply_error_context_xact(xid, prepare_lsn);

    apply_action = get_transaction_apply_action(xid, &mut winfo);

    match apply_action {
        TRANS_LEADER_APPLY => {
            /*
             * The transaction has been serialized to file, so replay all the
             * spooled operations.
             */
            apply_spooled_messages((*MyLogicalRepWorker).stream_fileset, xid, prepare_lsn);

            /* Mark the transaction as prepared. */
            apply_handle_prepare_internal(prepare_data);

            CommitTransactionCommand();

            /*
             * It is okay not to set the local_end LSN for the prepare because
             * we always flush the prepare record.
             */
            store_flush_position(end_lsn, InvalidXLogRecPtr);

            in_remote_transaction = false;

            /* Unlink the files with serialized changes and subxact info. */
            stream_cleanup_files((*MyLogicalRepWorker).subid, xid);

            elog!(DEBUG1, "finished processing the STREAM PREPARE command");
        }
        TRANS_LEADER_SEND_TO_PARALLEL => {
            // Assert(winfo);
            if pa_send_data(winfo, (*s).len as usize, (*s).data) {
                /* Finish processing the streaming transaction. */
                pa_xact_finish(winfo, end_lsn);
            } else {
                /*
                 * Switch to serialize mode when we are not able to send the
                 * change to parallel apply worker.
                 */
                pa_switch_to_partial_serialize(winfo, true);

                /* fall through to PARTIAL_SERIALIZE */
                let orig = original_msg;
                stream_open_and_write_change(xid, LOGICAL_REP_MSG_STREAM_PREPARE,
                                             &orig as *const StringInfoData as *mut StringInfoData);
                pa_set_fileset_state(parallel_apply_winfo_shared(winfo), FS_SERIALIZE_DONE);
                pa_xact_finish(winfo, end_lsn);
            }
        }
        TRANS_LEADER_PARTIAL_SERIALIZE => {
            // Assert(winfo);
            let orig = original_msg;
            stream_open_and_write_change(xid, LOGICAL_REP_MSG_STREAM_PREPARE,
                                         &orig as *const StringInfoData as *mut StringInfoData);
            pa_set_fileset_state(parallel_apply_winfo_shared(winfo), FS_SERIALIZE_DONE);
            pa_xact_finish(winfo, end_lsn);
        }
        TRANS_PARALLEL_APPLY => {
            /*
             * If the parallel apply worker is applying spooled messages then
             * close the file before preparing.
             */
            if !stream_fd.is_null() {
                stream_close_file();
            }

            begin_replication_step();

            /* Mark the transaction as prepared. */
            apply_handle_prepare_internal(prepare_data);

            end_replication_step();

            CommitTransactionCommand();

            /*
             * It is okay not to set the local_end LSN for the prepare because
             * we always flush the prepare record.
             */
            (*MyParallelShared).last_commit_end = InvalidXLogRecPtr;

            pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_FINISHED);
            pa_unlock_transaction((*MyParallelShared).xid, AccessExclusiveLock);

            pa_reset_subtrans();

            elog!(DEBUG1, "finished processing the STREAM PREPARE command");
        }
        _ => {
            elog!(ERROR, "unexpected apply action: %d", apply_action as c_int);
        }
    }

    pgstat_report_stat(false);

    /* Process any tables that are being synchronized in parallel. */
    process_syncing_tables(end_lsn);

    /*
     * Similar to prepare case, the subskiplsn could be left in a case of
     * server crash but it's okay.
     */
    stop_skipping_changes();
    clear_subscription_skip_lsn(prepare_lsn);

    pgstat_report_activity(STATE_IDLE, null());

    reset_apply_error_context_info();
}

/*
 * Handle ORIGIN message.
 *
 * TODO, support tracking of multiple origins
 */
unsafe fn apply_handle_origin(s: *mut StringInfoData) {
    /*
     * ORIGIN message can only come inside streaming transaction or inside
     * remote transaction and before any actual writes.
     */
    if !in_streamed_transaction
        && (!in_remote_transaction
            || (IsTransactionState() && !am_tablesync_worker()))
    {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("ORIGIN message sent out of order")
            )
        );
    }
}

/*
 * Initialize fileset (if not already done).
 *
 * Create a new file when first_segment is true, otherwise open the existing
 * file.
 */
pub unsafe fn stream_start_internal(xid: TransactionId, first_segment: bool) {
    begin_replication_step();

    /*
     * Initialize the worker's stream_fileset if we haven't yet.
     */
    if (*MyLogicalRepWorker).stream_fileset.is_null() {
        let oldctx = MemoryContextSwitchTo(ApplyContext);

        (*MyLogicalRepWorker).stream_fileset = palloc(std::mem::size_of::<FileSet>()) as *mut FileSet;
        FileSetInit((*MyLogicalRepWorker).stream_fileset);

        MemoryContextSwitchTo(oldctx);
    }

    /* Open the spool file for this transaction. */
    stream_open_file((*MyLogicalRepWorker).subid, xid, first_segment);

    /* If this is not the first segment, open existing subxact file. */
    if !first_segment {
        subxact_info_read((*MyLogicalRepWorker).subid, xid);
    }

    end_replication_step();
}

/*
 * Handle STREAM START message.
 */
unsafe fn apply_handle_stream_start(s: *mut StringInfoData) {
    let mut first_segment: bool = false;
    let mut winfo: *mut ParallelApplyWorkerInfo = null_mut();
    let apply_action: TransApplyAction;

    /* Save the message before it is consumed. */
    let original_msg: StringInfoData = core::ptr::read(s);

    if in_streamed_transaction {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("duplicate STREAM START message")
            )
        );
    }

    /* There must not be an active streaming transaction. */
    // Assert(!TransactionIdIsValid(stream_xid));

    /* notify handle methods we're processing a remote transaction */
    in_streamed_transaction = true;

    /* extract XID of the top-level transaction */
    stream_xid = logicalrep_read_stream_start(s, &mut first_segment);

    if !TransactionIdIsValid(stream_xid) {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("invalid transaction ID in streamed replication transaction")
            )
        );
    }

    set_apply_error_context_xact(stream_xid, InvalidXLogRecPtr);

    /* Try to allocate a worker for the streaming transaction. */
    if first_segment {
        pa_allocate_worker(stream_xid);
    }

    apply_action = get_transaction_apply_action(stream_xid, &mut winfo);

    match apply_action {
        TRANS_LEADER_SERIALIZE => {
            /*
             * Function stream_start_internal starts a transaction. This
             * transaction will be committed on the stream stop unless it is a
             * tablesync worker in which case it will be committed after
             * processing all the messages.
             */
            stream_start_internal(stream_xid, first_segment);
        }
        TRANS_LEADER_SEND_TO_PARALLEL => {
            // Assert(winfo);
            if pa_send_data(winfo, (*s).len as usize, (*s).data) {
                /*
                 * Unlock the shared object lock so that the parallel apply
                 * worker can continue to receive changes.
                 */
                if !first_segment {
                    pa_unlock_stream(parallel_apply_winfo_shared_xid(winfo), AccessExclusiveLock);
                }

                /*
                 * Increment the number of streaming blocks waiting to be
                 * processed by parallel apply worker.
                 */
                pg_atomic_add_fetch_u32(
                    parallel_apply_winfo_shared_pending_stream_count(winfo),
                    1,
                );

                /* Cache the parallel apply worker for this transaction. */
                pa_set_stream_apply_worker(winfo);
            } else {
                /*
                 * Switch to serialize mode when we are not able to send the
                 * change to parallel apply worker.
                 */
                pa_switch_to_partial_serialize(winfo, !first_segment);

                /* fall through to PARTIAL_SERIALIZE */
                if apply_action != TRANS_LEADER_SEND_TO_PARALLEL {
                    stream_start_internal(stream_xid, first_segment);
                }
                let orig = original_msg;
                stream_write_change(LOGICAL_REP_MSG_STREAM_START,
                                    &orig as *const StringInfoData as *mut StringInfoData);
                pa_set_stream_apply_worker(winfo);
            }
        }
        TRANS_LEADER_PARTIAL_SERIALIZE => {
            // Assert(winfo);
            /*
             * Open the spool file unless it was already opened when switching
             * to serialize mode.
             */
            stream_start_internal(stream_xid, first_segment);
            let orig = original_msg;
            stream_write_change(LOGICAL_REP_MSG_STREAM_START,
                                &orig as *const StringInfoData as *mut StringInfoData);
            pa_set_stream_apply_worker(winfo);
        }
        TRANS_PARALLEL_APPLY => {
            if first_segment {
                /* Hold the lock until the end of the transaction. */
                pa_lock_transaction((*MyParallelShared).xid, AccessExclusiveLock);
                pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_STARTED);

                /*
                 * Signal the leader apply worker, as it may be waiting for
                 * us.
                 */
                logicalrep_worker_wakeup((*MyLogicalRepWorker).subid, InvalidOid);
            }

            parallel_stream_nchanges = 0;
        }
        _ => {
            elog!(ERROR, "unexpected apply action: %d", apply_action as c_int);
        }
    }

    pgstat_report_activity(STATE_RUNNING, null());
}

// ===========================================================================
// Part 4: stream_stop_internal through apply_handle_stream_commit
// ===========================================================================

/*
 * Update the information about subxacts and close the file.
 *
 * This function should be called when the stream_start_internal function has
 * been called.
 */
pub unsafe fn stream_stop_internal(xid: TransactionId) {
    /*
     * Serialize information about subxacts for the toplevel transaction, then
     * close the stream messages spool file.
     */
    subxact_info_write((*MyLogicalRepWorker).subid, xid);
    stream_close_file();

    /* We must be in a valid transaction state */
    // Assert(IsTransactionState());

    /* Commit the per-stream transaction */
    CommitTransactionCommand();

    /* Reset per-stream context */
    MemoryContextReset(LogicalStreamingContext);
}

/*
 * Handle STREAM STOP message.
 */
unsafe fn apply_handle_stream_stop(s: *mut StringInfoData) {
    let mut winfo: *mut ParallelApplyWorkerInfo = null_mut();
    let apply_action: TransApplyAction;

    if !in_streamed_transaction {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("STREAM STOP message without STREAM START")
            )
        );
    }

    apply_action = get_transaction_apply_action(stream_xid, &mut winfo);

    match apply_action {
        TRANS_LEADER_SERIALIZE => {
            stream_stop_internal(stream_xid);
        }
        TRANS_LEADER_SEND_TO_PARALLEL => {
            // Assert(winfo);
            /*
             * Lock before sending the STREAM_STOP message so that the leader
             * can hold the lock first and the parallel apply worker will wait
             * for leader to release the lock.
             */
            pa_lock_stream(parallel_apply_winfo_shared_xid(winfo), AccessExclusiveLock);

            if pa_send_data(winfo, (*s).len as usize, (*s).data) {
                pa_set_stream_apply_worker(null_mut());
            } else {
                /*
                 * Switch to serialize mode when we are not able to send the
                 * change to parallel apply worker.
                 */
                pa_switch_to_partial_serialize(winfo, true);

                /* fall through to PARTIAL_SERIALIZE */
                stream_write_change(LOGICAL_REP_MSG_STREAM_STOP, s);
                stream_stop_internal(stream_xid);
                pa_set_stream_apply_worker(null_mut());
            }
        }
        TRANS_LEADER_PARTIAL_SERIALIZE => {
            stream_write_change(LOGICAL_REP_MSG_STREAM_STOP, s);
            stream_stop_internal(stream_xid);
            pa_set_stream_apply_worker(null_mut());
        }
        TRANS_PARALLEL_APPLY => {
            elog!(
                DEBUG1,
                "applied %u changes in the streaming chunk",
                parallel_stream_nchanges
            );

            /*
             * By the time parallel apply worker is processing the changes in
             * the current streaming block, the leader apply worker may have
             * sent multiple streaming blocks.
             */
            pa_decr_and_wait_stream_block();
        }
        _ => {
            elog!(ERROR, "unexpected apply action: %d", apply_action as c_int);
        }
    }

    in_streamed_transaction = false;
    stream_xid = InvalidTransactionId;

    /*
     * The parallel apply worker could be in a transaction in which case we
     * need to report the state as STATE_IDLEINTRANSACTION.
     */
    if IsTransactionOrTransactionBlock() {
        pgstat_report_activity(STATE_IDLEINTRANSACTION, null());
    } else {
        pgstat_report_activity(STATE_IDLE, null());
    }

    reset_apply_error_context_info();
}

/*
 * Helper function to handle STREAM ABORT message when the transaction was
 * serialized to file.
 */
unsafe fn stream_abort_internal(xid: TransactionId, subxid: TransactionId) {
    /*
     * If the two XIDs are the same, it's in fact abort of toplevel xact, so
     * just delete the files with serialized info.
     */
    if xid == subxid {
        stream_cleanup_files((*MyLogicalRepWorker).subid, xid);
    } else {
        /*
         * OK, so it's a subxact. We need to read the subxact file for the
         * toplevel transaction, determine the offset tracked for the subxact,
         * and truncate the file with changes.
         */
        let mut i: i64;
        let mut subidx: i64;
        let fd: *mut BufFile;
        let mut found = false;
        let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        subidx = -1;
        begin_replication_step();
        subxact_info_read((*MyLogicalRepWorker).subid, xid);

        i = subxact_data.nsubxacts as i64;
        while i > 0 {
            i -= 1;
            if (*subxact_data.subxacts.add(i as usize)).xid == subxid {
                subidx = i;
                found = true;
                break;
            }
        }

        /*
         * If it's an empty sub-transaction then we will not find the subxid
         * here so just cleanup the subxact info and return.
         */
        if !found {
            /* Cleanup the subxact info */
            cleanup_subxact_info();
            end_replication_step();
            CommitTransactionCommand();
            return;
        }

        /* open the changes file */
        changes_filename(path.as_mut_ptr(), (*MyLogicalRepWorker).subid, xid);
        fd = BufFileOpenFileSet(
            (*MyLogicalRepWorker).stream_fileset,
            path.as_ptr(),
            O_RDWR,
            false,
        );

        /* OK, truncate the file at the right offset */
        BufFileTruncateFileSet(
            fd,
            (*subxact_data.subxacts.add(subidx as usize)).fileno,
            (*subxact_data.subxacts.add(subidx as usize)).offset,
        );
        BufFileClose(fd);

        /* discard the subxacts added later */
        subxact_data.nsubxacts = subidx as uint32;

        /* write the updated subxact list */
        subxact_info_write((*MyLogicalRepWorker).subid, xid);

        end_replication_step();
        CommitTransactionCommand();
    }
}

/*
 * Handle STREAM ABORT message.
 */
unsafe fn apply_handle_stream_abort(s: *mut StringInfoData) {
    let xid: TransactionId;
    let subxid: TransactionId;
    let mut abort_data_buf: [u8; 256] = [0u8; 256];
    let abort_data = abort_data_buf.as_mut_ptr() as *mut LogicalRepStreamAbortData;
    let mut winfo: *mut ParallelApplyWorkerInfo = null_mut();
    let apply_action: TransApplyAction;

    /* Save the message before it is consumed. */
    let original_msg: StringInfoData = core::ptr::read(s);

    if in_streamed_transaction {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("STREAM ABORT message without STREAM STOP")
            )
        );
    }

    /* We receive abort information only when we can apply in parallel. */
    logicalrep_read_stream_abort(
        s,
        abort_data,
        logicalrep_worker_parallel_apply(MyLogicalRepWorker),
    );

    xid = logicalrep_stream_abort_data_xid(abort_data);
    subxid = logicalrep_stream_abort_data_subxid(abort_data);
    let abort_lsn = logicalrep_stream_abort_data_abort_lsn(abort_data);
    let toplevel_xact = xid == subxid;

    set_apply_error_context_xact(subxid, abort_lsn);

    apply_action = get_transaction_apply_action(xid, &mut winfo);

    match apply_action {
        TRANS_LEADER_APPLY => {
            /*
             * We are in the leader apply worker and the transaction has been
             * serialized to file.
             */
            stream_abort_internal(xid, subxid);

            elog!(DEBUG1, "finished processing the STREAM ABORT command");
        }
        TRANS_LEADER_SEND_TO_PARALLEL => {
            // Assert(winfo);

            /*
             * For the case of aborting the subtransaction, we increment the
             * number of streaming blocks and take the lock again before
             * sending the STREAM_ABORT.
             */
            if !toplevel_xact {
                pa_unlock_stream(xid, AccessExclusiveLock);
                pg_atomic_add_fetch_u32(
                    parallel_apply_winfo_shared_pending_stream_count(winfo),
                    1,
                );
                pa_lock_stream(xid, AccessExclusiveLock);
            }

            if pa_send_data(winfo, (*s).len as usize, (*s).data) {
                if toplevel_xact {
                    pa_xact_finish(winfo, InvalidXLogRecPtr);
                }
            } else {
                /*
                 * Switch to serialize mode when we are not able to send the
                 * change to parallel apply worker.
                 */
                pa_switch_to_partial_serialize(winfo, true);

                /* fall through to PARTIAL_SERIALIZE */
                let orig = original_msg;
                stream_open_and_write_change(xid, LOGICAL_REP_MSG_STREAM_ABORT,
                                             &orig as *const StringInfoData as *mut StringInfoData);
                if toplevel_xact {
                    pa_set_fileset_state(parallel_apply_winfo_shared(winfo), FS_SERIALIZE_DONE);
                    pa_xact_finish(winfo, InvalidXLogRecPtr);
                }
            }
        }
        TRANS_LEADER_PARTIAL_SERIALIZE => {
            // Assert(winfo);
            /*
             * Parallel apply worker might have applied some changes, so write
             * the STREAM_ABORT message so that it can rollback the
             * subtransaction if needed.
             */
            let orig = original_msg;
            stream_open_and_write_change(xid, LOGICAL_REP_MSG_STREAM_ABORT,
                                         &orig as *const StringInfoData as *mut StringInfoData);
            if toplevel_xact {
                pa_set_fileset_state(parallel_apply_winfo_shared(winfo), FS_SERIALIZE_DONE);
                pa_xact_finish(winfo, InvalidXLogRecPtr);
            }
        }
        TRANS_PARALLEL_APPLY => {
            /*
             * If the parallel apply worker is applying spooled messages then
             * close the file before aborting.
             */
            if toplevel_xact && !stream_fd.is_null() {
                stream_close_file();
            }

            pa_stream_abort(abort_data);

            /*
             * We need to wait after processing rollback to savepoint for the
             * next set of changes.
             */
            if !toplevel_xact {
                pa_decr_and_wait_stream_block();
            }

            elog!(DEBUG1, "finished processing the STREAM ABORT command");
        }
        _ => {
            elog!(ERROR, "unexpected apply action: %d", apply_action as c_int);
        }
    }

    reset_apply_error_context_info();
}

/*
 * Ensure that the passed location is fileset's end.
 */
unsafe fn ensure_last_message(
    stream_fileset: *mut FileSet,
    xid: TransactionId,
    fileno: c_int,
    offset: off_t,
) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: *mut BufFile;
    let mut last_fileno: c_int = 0;
    let mut last_offset: off_t = 0;

    // Assert(!IsTransactionState());

    begin_replication_step();

    changes_filename(path.as_mut_ptr(), (*MyLogicalRepWorker).subid, xid);

    fd = BufFileOpenFileSet(stream_fileset, path.as_ptr(), O_RDONLY, false);

    BufFileSeek(fd, 0, 0, SEEK_END);
    BufFileTell(fd, &mut last_fileno, &mut last_offset);

    BufFileClose(fd);

    end_replication_step();

    if last_fileno != fileno || last_offset != offset {
        elog!(
            ERROR,
            "unexpected message left in streaming transaction's changes file \"%s\"",
            path.as_ptr()
        );
    }
}

/*
 * Common spoolfile processing.
 */
pub unsafe fn apply_spooled_messages(
    stream_fileset: *mut FileSet,
    xid: TransactionId,
    lsn: XLogRecPtr,
) {
    let mut nchanges: c_int;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut buffer: *mut c_char = null_mut();
    let oldcxt: MemoryContext;
    let oldowner: ResourceOwner;
    let mut fileno: c_int = 0;
    let mut offset: off_t = 0;

    if !am_parallel_apply_worker() {
        maybe_start_skipping_changes(lsn);
    }

    /* Make sure we have an open transaction */
    begin_replication_step();

    /*
     * Allocate file handle and memory required to process all the messages in
     * TopTransactionContext to avoid them getting reset after each message is
     * processed.
     */
    oldcxt = MemoryContextSwitchTo(TopTransactionContext());

    /* Open the spool file for the committed/prepared transaction */
    changes_filename(path.as_mut_ptr(), (*MyLogicalRepWorker).subid, xid);
    elog!(DEBUG1, "replaying changes from file \"%s\"", path.as_ptr());

    /*
     * Make sure the file is owned by the toplevel transaction so that the
     * file will not be accidentally closed when aborting a subtransaction.
     */
    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = TopTransactionResourceOwner;

    stream_fd = BufFileOpenFileSet(stream_fileset, path.as_ptr(), O_RDONLY, false);

    CurrentResourceOwner = oldowner;

    buffer = palloc(BLCKSZ) as *mut c_char;

    MemoryContextSwitchTo(oldcxt);

    remote_final_lsn = lsn;

    /*
     * Make sure the handle apply_dispatch methods are aware we're in a remote
     * transaction.
     */
    in_remote_transaction = true;
    pgstat_report_activity(STATE_RUNNING, null());

    end_replication_step();

    /*
     * Read the entries one by one and pass them through the same logic as in
     * apply_dispatch.
     */
    nchanges = 0;
    loop {
        let mut s2: StringInfoData = std::mem::zeroed();
        let nbytes: Size;
        let mut len: c_int = 0;

        CHECK_FOR_INTERRUPTS();

        /* read length of the on-disk record */
        nbytes = BufFileReadMaybeEOF(
            stream_fd,
            &mut len as *mut c_int as *mut c_void,
            std::mem::size_of::<c_int>(),
            true,
        );

        /* have we reached end of the file? */
        if nbytes == 0 {
            break;
        }

        /* do we have a correct length? */
        if len <= 0 {
            elog!(
                ERROR,
                "incorrect length %d in streaming transaction's changes file \"%s\"",
                len,
                path.as_ptr()
            );
        }

        /* make sure we have sufficiently large buffer */
        buffer = repalloc(buffer as *mut c_void, len as Size) as *mut c_char;

        /* and finally read the data into the buffer */
        BufFileReadExact(stream_fd, buffer as *mut c_void, len as Size);

        BufFileTell(stream_fd, &mut fileno, &mut offset);

        /* init a stringinfo using the buffer and call apply_dispatch */
        initReadOnlyStringInfo(&mut s2, buffer, len);

        /* Ensure we are reading the data into our memory context. */
        let oldcxt2 = MemoryContextSwitchTo(ApplyMessageContext);

        apply_dispatch(&mut s2);

        MemoryContextReset(ApplyMessageContext);

        MemoryContextSwitchTo(oldcxt2);

        nchanges += 1;

        /*
         * It is possible the file has been closed because we have processed
         * the transaction end message like stream_commit in which case that
         * must be the last message.
         */
        if stream_fd.is_null() {
            ensure_last_message(stream_fileset, xid, fileno, offset);
            break;
        }

        if nchanges % 1000 == 0 {
            elog!(
                DEBUG1,
                "replayed %d changes from file \"%s\"",
                nchanges,
                path.as_ptr()
            );
        }
    }

    if !stream_fd.is_null() {
        stream_close_file();
    }

    elog!(
        DEBUG1,
        "replayed %d (all) changes from file \"%s\"",
        nchanges,
        path.as_ptr()
    );
}

/*
 * Handle STREAM COMMIT message.
 */
unsafe fn apply_handle_stream_commit(s: *mut StringInfoData) {
    let xid: TransactionId;
    let mut commit_data_buf: [u8; 256] = [0u8; 256];
    let commit_data = commit_data_buf.as_mut_ptr() as *mut LogicalRepCommitData;
    let mut winfo: *mut ParallelApplyWorkerInfo = null_mut();
    let apply_action: TransApplyAction;

    /* Save the message before it is consumed. */
    let original_msg: StringInfoData = core::ptr::read(s);

    if in_streamed_transaction {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_PROTOCOL_VIOLATION),
                errmsg_internal!("STREAM COMMIT message without STREAM STOP")
            )
        );
    }

    xid = logicalrep_read_stream_commit(s, commit_data);
    let commit_lsn = logicalrep_commit_data_commit_lsn(commit_data);
    let end_lsn = logicalrep_commit_data_end_lsn(commit_data);
    set_apply_error_context_xact(xid, commit_lsn);

    apply_action = get_transaction_apply_action(xid, &mut winfo);

    match apply_action {
        TRANS_LEADER_APPLY => {
            /*
             * The transaction has been serialized to file, so replay all the
             * spooled operations.
             */
            apply_spooled_messages(
                (*MyLogicalRepWorker).stream_fileset,
                xid,
                commit_lsn,
            );

            apply_handle_commit_internal(commit_data);

            /* Unlink the files with serialized changes and subxact info. */
            stream_cleanup_files((*MyLogicalRepWorker).subid, xid);

            elog!(DEBUG1, "finished processing the STREAM COMMIT command");
        }
        TRANS_LEADER_SEND_TO_PARALLEL => {
            // Assert(winfo);
            if pa_send_data(winfo, (*s).len as usize, (*s).data) {
                /* Finish processing the streaming transaction. */
                pa_xact_finish(winfo, end_lsn);
            } else {
                /*
                 * Switch to serialize mode when we are not able to send the
                 * change to parallel apply worker.
                 */
                pa_switch_to_partial_serialize(winfo, true);

                /* fall through to PARTIAL_SERIALIZE */
                let orig = original_msg;
                stream_open_and_write_change(xid, LOGICAL_REP_MSG_STREAM_COMMIT,
                                             &orig as *const StringInfoData as *mut StringInfoData);
                pa_set_fileset_state(parallel_apply_winfo_shared(winfo), FS_SERIALIZE_DONE);
                pa_xact_finish(winfo, end_lsn);
            }
        }
        TRANS_LEADER_PARTIAL_SERIALIZE => {
            // Assert(winfo);
            let orig = original_msg;
            stream_open_and_write_change(xid, LOGICAL_REP_MSG_STREAM_COMMIT,
                                         &orig as *const StringInfoData as *mut StringInfoData);
            pa_set_fileset_state(parallel_apply_winfo_shared(winfo), FS_SERIALIZE_DONE);
            pa_xact_finish(winfo, end_lsn);
        }
        TRANS_PARALLEL_APPLY => {
            /*
             * If the parallel apply worker is applying spooled messages then
             * close the file before committing.
             */
            if !stream_fd.is_null() {
                stream_close_file();
            }

            apply_handle_commit_internal(commit_data);

            (*MyParallelShared).last_commit_end = XactLastCommitEnd;

            /*
             * It is important to set the transaction state as finished before
             * releasing the lock.
             */
            pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_FINISHED);
            pa_unlock_transaction(xid, AccessExclusiveLock);

            pa_reset_subtrans();

            elog!(DEBUG1, "finished processing the STREAM COMMIT command");
        }
        _ => {
            elog!(ERROR, "unexpected apply action: %d", apply_action as c_int);
        }
    }

    /* Process any tables that are being synchronized in parallel. */
    process_syncing_tables(end_lsn);

    pgstat_report_activity(STATE_IDLE, null());

    reset_apply_error_context_info();
}

/*
 * Helper function for apply_handle_commit and apply_handle_stream_commit.
 */
unsafe fn apply_handle_commit_internal(commit_data: *mut LogicalRepCommitData) {
    if is_skipping_changes() {
        stop_skipping_changes();

        /*
         * Start a new transaction to clear the subskiplsn, if not started
         * yet.
         */
        if !IsTransactionState() {
            StartTransactionCommand();
        }
    }

    if IsTransactionState() {
        /*
         * The transaction is either non-empty or skipped, so we clear the
         * subskiplsn.
         */
        let commit_lsn = logicalrep_commit_data_commit_lsn(commit_data);
        let end_lsn = logicalrep_commit_data_end_lsn(commit_data);
        let committime = logicalrep_commit_data_committime(commit_data);

        clear_subscription_skip_lsn(commit_lsn);

        /*
         * Update origin state so we can restart streaming from correct
         * position in case of crash.
         */
        replorigin_session_origin_lsn = end_lsn;
        replorigin_session_origin_timestamp = committime;

        CommitTransactionCommand();

        if IsTransactionBlock() {
            EndTransactionBlock(false);
            CommitTransactionCommand();
        }

        pgstat_report_stat(false);

        store_flush_position(end_lsn, XactLastCommitEnd);
    } else {
        /* Process any invalidation messages that might have accumulated. */
        AcceptInvalidationMessages();
        maybe_reread_subscription();
    }

    in_remote_transaction = false;
}

// ===========================================================================
// Part 5: apply_handle_relation through apply_handle_truncate + apply_dispatch
// ===========================================================================

/*
 * Handle RELATION message.
 *
 * Note we don't do validation against local schema here.
 */
unsafe fn apply_handle_relation(s: *mut StringInfoData) {
    if handle_streamed_transaction(LOGICAL_REP_MSG_RELATION, s) {
        return;
    }

    let rel = logicalrep_read_rel(s);
    logicalrep_relmap_update(rel);

    /* Also reset all entries in the partition map that refer to remoterel. */
    logicalrep_partmap_reset_relmap(rel);
}

/*
 * Handle TYPE message.
 *
 * This implementation pays no attention to TYPE messages; we expect the user
 * to have set things up so that the incoming data is acceptable to the input
 * functions for the locally subscribed tables.  Hence, we just read and
 * discard the message.
 */
unsafe fn apply_handle_type(s: *mut StringInfoData) {
    let mut typ: [u8; 128] = [0u8; 128]; // LogicalRepTyp placeholder

    if handle_streamed_transaction(LOGICAL_REP_MSG_TYPE, s) {
        return;
    }

    logicalrep_read_typ(s, typ.as_mut_ptr() as *mut LogicalRepTyp);
}

/*
 * Check that we (the subscription owner) have sufficient privileges on the
 * target relation to perform the given operation.
 */
unsafe fn TargetPrivilegesCheck(rel: *mut c_void, mode: AclMode) {
    let relid = RelationGetRelid(rel);
    let aclresult = pg_class_aclcheck(relid, GetUserId(), mode);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            get_relkind_objtype(pg_class_relkind(relation_rd_rel(rel))),
            get_rel_name(relid),
        );
    }

    /*
     * We lack the infrastructure to honor RLS policies.
     */
    if check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "user \"%s\" cannot replicate into relation with row-level security enabled: \"%s\"",
                    GetUserNameFromId(GetUserId(), true),
                    RelationGetRelationName(rel)
                )
            )
        );
    }
}

/*
 * Handle INSERT message.
 */
unsafe fn apply_handle_insert(s: *mut StringInfoData) {
    let rel: *mut LogicalRepRelMapEntry;
    let mut newtup_buf: [u8; 512] = [0u8; 512];
    let newtup = newtup_buf.as_mut_ptr() as *mut LogicalRepTupleData;
    let relid: LogicalRepRelId;
    let mut ucxt_buf: [u8; 128] = [0u8; 128];
    let ucxt = ucxt_buf.as_mut_ptr() as *mut UserContext;
    let edata: *mut ApplyExecutionData;
    let estate: *mut EState;
    let remoteslot: *mut TupleTableSlot;
    let oldctx: MemoryContext;
    let run_as_owner: bool;

    /*
     * Quick return if we are skipping data modification changes or handling
     * streamed transactions.
     */
    if is_skipping_changes() || handle_streamed_transaction(LOGICAL_REP_MSG_INSERT, s) {
        return;
    }

    begin_replication_step();

    relid = logicalrep_read_insert(s, newtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);
    if !should_apply_changes_for_rel(rel) {
        /*
         * The relation can't become interesting in the middle of the
         * transaction so it's safe to unlock it.
         */
        logicalrep_rel_close(rel, RowExclusiveLock);
        end_replication_step();
        return;
    }

    /*
     * Make sure that any user-supplied code runs as the table owner, unless
     * the user has opted out of that behavior.
     */
    run_as_owner = subscription_runasowner(MySubscription);
    if !run_as_owner {
        SwitchToUntrustedUser(pg_class_relowner(relation_rd_rel(logicalrep_rel_mapentry_localrel(rel))), ucxt);
    }

    /* Set relation for error callback */
    apply_error_callback_arg.rel = rel;

    /* Initialize the executor state. */
    edata = create_edata_for_relation(rel);
    estate = (*edata).estate;
    remoteslot = ExecInitExtraTupleSlot(
        estate,
        RelationGetDescr(logicalrep_rel_mapentry_localrel(rel)),
        &TTSOpsVirtual as *const c_void as *mut c_void,
    );

    /* Process and store remote tuple in the slot */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    slot_store_data(remoteslot, rel, newtup);
    slot_fill_defaults(rel, estate, remoteslot);
    MemoryContextSwitchTo(oldctx);

    /* For a partitioned table, insert the tuple into a partition. */
    let localrel = logicalrep_rel_mapentry_localrel(rel);
    if pg_class_relkind(relation_rd_rel(localrel)) == RELKIND_PARTITIONED_TABLE {
        apply_handle_tuple_routing(edata, remoteslot, null_mut(), CMD_INSERT);
    } else {
        let relinfo = (*edata).targetRelInfo;
        ExecOpenIndices(relinfo, false);
        apply_handle_insert_internal(edata, relinfo, remoteslot);
        ExecCloseIndices(relinfo);
    }

    finish_edata(edata);

    /* Reset relation for error callback */
    apply_error_callback_arg.rel = null_mut();

    if !run_as_owner {
        RestoreUserContext(ucxt);
    }

    logicalrep_rel_close(rel, NoLock);

    end_replication_step();
}

/*
 * Workhorse for apply_handle_insert()
 * relinfo is for the relation we're actually inserting into
 * (could be a child partition of edata->targetRelInfo)
 */
unsafe fn apply_handle_insert_internal(
    edata: *mut ApplyExecutionData,
    relinfo: *mut ResultRelInfo,
    remoteslot: *mut TupleTableSlot,
) {
    let estate = (*edata).estate;

    /* Caller should have opened indexes already. */
    /* Caller will not have done this bit. */
    InitConflictIndexes(relinfo);

    /* Do the insert. */
    TargetPrivilegesCheck(resultrelinfo_ri_RelationDesc(relinfo), ACL_INSERT);
    ExecSimpleRelationInsert(relinfo, estate, remoteslot);
}

/*
 * Check if the logical replication relation is updatable and throw
 * appropriate error if it isn't.
 */
unsafe fn check_relation_updatable(rel: *mut LogicalRepRelMapEntry) {
    /*
     * For partitioned tables, we only need to care if the target partition is
     * updatable (aka has PK or RI defined for it).
     */
    let localrel = logicalrep_rel_mapentry_localrel(rel);
    if pg_class_relkind(relation_rd_rel(localrel)) == RELKIND_PARTITIONED_TABLE {
        return;
    }

    /* Updatable, no error. */
    if logicalrep_rel_mapentry_updatable(rel) {
        return;
    }

    /*
     * We are in error mode so it's fine this is somewhat slow.
     */
    let remoterel = logicalrep_rel_mapentry_remoterel(rel);
    if OidIsValid(GetRelationIdentityOrPK(localrel)) {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "publisher did not send replica identity column expected by the logical replication target relation \"%s.%s\"",
                    logicalrep_reldata_nspname(remoterel),
                    logicalrep_reldata_relname(remoterel)
                )
            )
        );
    }

    ereport!(
        ERROR,
        (
            errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "logical replication target relation \"%s.%s\" has neither REPLICA IDENTITY index nor PRIMARY KEY and published relation does not have REPLICA IDENTITY FULL",
                logicalrep_reldata_nspname(remoterel),
                logicalrep_reldata_relname(remoterel)
            )
        )
    );
}

/*
 * Handle UPDATE message.
 *
 * TODO: FDW support
 */
unsafe fn apply_handle_update(s: *mut StringInfoData) {
    let rel: *mut LogicalRepRelMapEntry;
    let relid: LogicalRepRelId;
    let mut ucxt_buf: [u8; 128] = [0u8; 128];
    let ucxt = ucxt_buf.as_mut_ptr() as *mut UserContext;
    let edata: *mut ApplyExecutionData;
    let estate: *mut EState;
    let mut oldtup_buf: [u8; 512] = [0u8; 512];
    let oldtup = oldtup_buf.as_mut_ptr() as *mut LogicalRepTupleData;
    let mut newtup_buf: [u8; 512] = [0u8; 512];
    let newtup = newtup_buf.as_mut_ptr() as *mut LogicalRepTupleData;
    let mut has_oldtup: bool = false;
    let remoteslot: *mut TupleTableSlot;
    let oldctx: MemoryContext;
    let run_as_owner: bool;

    /*
     * Quick return if we are skipping data modification changes or handling
     * streamed transactions.
     */
    if is_skipping_changes() || handle_streamed_transaction(LOGICAL_REP_MSG_UPDATE, s) {
        return;
    }

    begin_replication_step();

    relid = logicalrep_read_update(s, &mut has_oldtup, oldtup, newtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);
    if !should_apply_changes_for_rel(rel) {
        logicalrep_rel_close(rel, RowExclusiveLock);
        end_replication_step();
        return;
    }

    /* Set relation for error callback */
    apply_error_callback_arg.rel = rel;

    /* Check if we can do the update. */
    check_relation_updatable(rel);

    /*
     * Make sure that any user-supplied code runs as the table owner, unless
     * the user has opted out of that behavior.
     */
    run_as_owner = subscription_runasowner(MySubscription);
    if !run_as_owner {
        SwitchToUntrustedUser(pg_class_relowner(relation_rd_rel(logicalrep_rel_mapentry_localrel(rel))), ucxt);
    }

    /* Initialize the executor state. */
    edata = create_edata_for_relation(rel);
    estate = (*edata).estate;
    remoteslot = ExecInitExtraTupleSlot(
        estate,
        RelationGetDescr(logicalrep_rel_mapentry_localrel(rel)),
        &TTSOpsVirtual as *const c_void as *mut c_void,
    );

    /*
     * Populate updatedCols so that per-column triggers can fire, and so
     * executor can correctly pass down indexUnchanged hint.
     */
    let target_perminfo = list_nth(estate_es_rteperminfos(estate), 0) as *mut RTEPermissionInfo;
    let natts = tupleslot_natts(remoteslot);
    let mut i: c_int = 0;
    while i < natts {
        let desc = tupleslot_tts_tupleDescriptor(remoteslot);
        let att = TupleDescAttr(desc, i);
        let remoteattnum = logicalrep_relmaentry_attrmap_attnums(rel, i);

        if !form_pg_attribute_attisdropped(att) && remoteattnum >= 0 {
            // Assert(remoteattnum < newtup.ncols);
            let colstatus = logicalrep_tupledata_colstatus(newtup, remoteattnum);
            if colstatus != LOGICALREP_COLUMN_UNCHANGED {
                let cur_bms = rteperminfo_updatedCols(target_perminfo);
                let new_bms = bms_add_member(cur_bms, i + 1 - FirstLowInvalidHeapAttributeNumber);
                rteperminfo_set_updatedCols(target_perminfo, new_bms);
            }
        }
        i += 1;
    }

    /* Build the search tuple. */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    slot_store_data(remoteslot, rel, if has_oldtup { oldtup } else { newtup });
    MemoryContextSwitchTo(oldctx);

    /* For a partitioned table, apply update to correct partition. */
    let localrel = logicalrep_rel_mapentry_localrel(rel);
    if pg_class_relkind(relation_rd_rel(localrel)) == RELKIND_PARTITIONED_TABLE {
        apply_handle_tuple_routing(edata, remoteslot, newtup, CMD_UPDATE);
    } else {
        apply_handle_update_internal(edata, (*edata).targetRelInfo, remoteslot, newtup,
                                     logicalrep_rel_mapentry_localindexoid(rel));
    }

    finish_edata(edata);

    /* Reset relation for error callback */
    apply_error_callback_arg.rel = null_mut();

    if !run_as_owner {
        RestoreUserContext(ucxt);
    }

    logicalrep_rel_close(rel, NoLock);

    end_replication_step();
}

/*
 * Workhorse for apply_handle_update()
 * relinfo is for the relation we're actually updating in
 * (could be a child partition of edata->targetRelInfo)
 */
unsafe fn apply_handle_update_internal(
    edata: *mut ApplyExecutionData,
    relinfo: *mut ResultRelInfo,
    remoteslot: *mut TupleTableSlot,
    newtup: *mut LogicalRepTupleData,
    localindexoid: Oid,
) {
    let estate = (*edata).estate;
    let relmapentry = (*edata).targetRel;
    let localrel = resultrelinfo_ri_RelationDesc(relinfo);
    let mut epqstate_buf: [u8; 512] = [0u8; 512];
    let epqstate = epqstate_buf.as_mut_ptr() as *mut EPQState;
    let mut localslot: *mut TupleTableSlot = null_mut();
    let mut conflicttuple_buf: [u8; 128] = [0u8; 128];
    let conflicttuple = conflicttuple_buf.as_mut_ptr() as *mut ConflictTupleInfo;
    let found: bool;
    let oldctx: MemoryContext;

    EvalPlanQualInit(epqstate, estate, null_mut(), NIL, -1, NIL);
    ExecOpenIndices(relinfo, false);

    found = FindReplTupleInLocalRel(
        edata,
        localrel,
        logicalrep_rel_mapentry_remoterel(relmapentry),
        localindexoid,
        remoteslot,
        &mut localslot,
    );

    /* Tuple found. */
    if found {
        let mut xmin: TransactionId = 0;
        let mut origin: RepOriginId = 0;
        let mut ts: TimestampTz = 0;

        /*
         * Report the conflict if the tuple was modified by a different
         * origin.
         */
        if GetTupleTransactionInfo(localslot, &mut xmin, &mut origin, &mut ts)
            && origin != replorigin_session_origin
        {
            let newslot = table_slot_create(localrel, estate_es_tupleTable(estate));
            slot_store_data(newslot, relmapentry, newtup);

            // conflicttuple.slot = localslot (set via accessor)
            ReportApplyConflict(estate, relinfo, LOG, CT_UPDATE_ORIGIN_DIFFERS,
                                remoteslot, newslot, list_make1(conflicttuple as *mut c_void));
        }

        /* Process and store remote tuple in the slot */
        oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
        slot_modify_data(remoteslot, localslot, relmapentry, newtup);
        MemoryContextSwitchTo(oldctx);

        EvalPlanQualSetSlot(epqstate, remoteslot);

        InitConflictIndexes(relinfo);

        /* Do the actual update. */
        TargetPrivilegesCheck(resultrelinfo_ri_RelationDesc(relinfo), ACL_UPDATE);
        ExecSimpleRelationUpdate(relinfo, estate, epqstate, localslot, remoteslot);
    } else {
        let newslot = localslot;

        /* Store the new tuple for conflict reporting */
        slot_store_data(newslot, relmapentry, newtup);

        /*
         * The tuple to be updated could not be found.  Do nothing except for
         * emitting a log message.
         */
        ReportApplyConflict(estate, relinfo, LOG, CT_UPDATE_MISSING,
                            remoteslot, newslot, list_make1(conflicttuple as *mut c_void));
    }

    /* Cleanup. */
    ExecCloseIndices(relinfo);
    EvalPlanQualEnd(epqstate);
}

/*
 * Handle DELETE message.
 *
 * TODO: FDW support
 */
unsafe fn apply_handle_delete(s: *mut StringInfoData) {
    let rel: *mut LogicalRepRelMapEntry;
    let mut oldtup_buf: [u8; 512] = [0u8; 512];
    let oldtup = oldtup_buf.as_mut_ptr() as *mut LogicalRepTupleData;
    let relid: LogicalRepRelId;
    let mut ucxt_buf: [u8; 128] = [0u8; 128];
    let ucxt = ucxt_buf.as_mut_ptr() as *mut UserContext;
    let edata: *mut ApplyExecutionData;
    let estate: *mut EState;
    let remoteslot: *mut TupleTableSlot;
    let oldctx: MemoryContext;
    let run_as_owner: bool;

    /*
     * Quick return if we are skipping data modification changes or handling
     * streamed transactions.
     */
    if is_skipping_changes() || handle_streamed_transaction(LOGICAL_REP_MSG_DELETE, s) {
        return;
    }

    begin_replication_step();

    relid = logicalrep_read_delete(s, oldtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);
    if !should_apply_changes_for_rel(rel) {
        logicalrep_rel_close(rel, RowExclusiveLock);
        end_replication_step();
        return;
    }

    /* Set relation for error callback */
    apply_error_callback_arg.rel = rel;

    /* Check if we can do the delete. */
    check_relation_updatable(rel);

    /*
     * Make sure that any user-supplied code runs as the table owner, unless
     * the user has opted out of that behavior.
     */
    run_as_owner = subscription_runasowner(MySubscription);
    if !run_as_owner {
        SwitchToUntrustedUser(pg_class_relowner(relation_rd_rel(logicalrep_rel_mapentry_localrel(rel))), ucxt);
    }

    /* Initialize the executor state. */
    edata = create_edata_for_relation(rel);
    estate = (*edata).estate;
    remoteslot = ExecInitExtraTupleSlot(
        estate,
        RelationGetDescr(logicalrep_rel_mapentry_localrel(rel)),
        &TTSOpsVirtual as *const c_void as *mut c_void,
    );

    /* Build the search tuple. */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    slot_store_data(remoteslot, rel, oldtup);
    MemoryContextSwitchTo(oldctx);

    /* For a partitioned table, apply delete to correct partition. */
    let localrel = logicalrep_rel_mapentry_localrel(rel);
    if pg_class_relkind(relation_rd_rel(localrel)) == RELKIND_PARTITIONED_TABLE {
        apply_handle_tuple_routing(edata, remoteslot, null_mut(), CMD_DELETE);
    } else {
        let relinfo = (*edata).targetRelInfo;
        ExecOpenIndices(relinfo, false);
        apply_handle_delete_internal(edata, relinfo, remoteslot,
                                     logicalrep_rel_mapentry_localindexoid(rel));
        ExecCloseIndices(relinfo);
    }

    finish_edata(edata);

    /* Reset relation for error callback */
    apply_error_callback_arg.rel = null_mut();

    if !run_as_owner {
        RestoreUserContext(ucxt);
    }

    logicalrep_rel_close(rel, NoLock);

    end_replication_step();
}

/*
 * Workhorse for apply_handle_delete()
 */
unsafe fn apply_handle_delete_internal(
    edata: *mut ApplyExecutionData,
    relinfo: *mut ResultRelInfo,
    remoteslot: *mut TupleTableSlot,
    localindexoid: Oid,
) {
    let estate = (*edata).estate;
    let localrel = resultrelinfo_ri_RelationDesc(relinfo);
    let remoterel = logicalrep_rel_mapentry_remoterel((*edata).targetRel);
    let mut epqstate_buf: [u8; 512] = [0u8; 512];
    let epqstate = epqstate_buf.as_mut_ptr() as *mut EPQState;
    let mut localslot: *mut TupleTableSlot = null_mut();
    let mut conflicttuple_buf: [u8; 128] = [0u8; 128];
    let conflicttuple = conflicttuple_buf.as_mut_ptr() as *mut ConflictTupleInfo;
    let found: bool;

    EvalPlanQualInit(epqstate, estate, null_mut(), NIL, -1, NIL);

    found = FindReplTupleInLocalRel(edata, localrel, remoterel, localindexoid,
                                    remoteslot, &mut localslot);

    /* If found delete it. */
    if found {
        let mut xmin: TransactionId = 0;
        let mut origin: RepOriginId = 0;
        let mut ts: TimestampTz = 0;

        /*
         * Report the conflict if the tuple was modified by a different
         * origin.
         */
        if GetTupleTransactionInfo(localslot, &mut xmin, &mut origin, &mut ts)
            && origin != replorigin_session_origin
        {
            ReportApplyConflict(estate, relinfo, LOG, CT_DELETE_ORIGIN_DIFFERS,
                                remoteslot, null_mut(), list_make1(conflicttuple as *mut c_void));
        }

        EvalPlanQualSetSlot(epqstate, localslot);

        /* Do the actual delete. */
        TargetPrivilegesCheck(resultrelinfo_ri_RelationDesc(relinfo), ACL_DELETE);
        ExecSimpleRelationDelete(relinfo, estate, epqstate, localslot);
    } else {
        /*
         * The tuple to be deleted could not be found.  Do nothing except for
         * emitting a log message.
         */
        ReportApplyConflict(estate, relinfo, LOG, CT_DELETE_MISSING,
                            remoteslot, null_mut(), list_make1(conflicttuple as *mut c_void));
    }

    /* Cleanup. */
    EvalPlanQualEnd(epqstate);
}

/*
 * Try to find a tuple received from the publication side (in 'remoteslot') in
 * the corresponding local relation using either replica identity index,
 * primary key, index or if needed, sequential scan.
 *
 * Local tuple, if found, is returned in '*localslot'.
 */
unsafe fn FindReplTupleInLocalRel(
    edata: *mut ApplyExecutionData,
    localrel: *mut c_void,
    remoterel: *mut LogicalRepRelation,
    localidxoid: Oid,
    remoteslot: *mut TupleTableSlot,
    localslot: *mut *mut TupleTableSlot,
) -> bool {
    let estate = (*edata).estate;
    let found: bool;

    /*
     * Regardless of the top-level operation, we're performing a read here, so
     * check for SELECT privileges.
     */
    TargetPrivilegesCheck(localrel, ACL_SELECT);

    *localslot = table_slot_create(localrel, estate_es_tupleTable(estate));

    // Assert(OidIsValid(localidxoid) || (remoterel->replident == REPLICA_IDENTITY_FULL));

    if OidIsValid(localidxoid) {
        found = RelationFindReplTupleByIndex(
            localrel,
            localidxoid,
            /* LockTupleExclusive */ 4,
            remoteslot,
            *localslot,
        );
    } else {
        found = RelationFindReplTupleSeq(
            localrel,
            /* LockTupleExclusive */ 4,
            remoteslot,
            *localslot,
        );
    }

    found
}

// ===========================================================================
// Part 6: apply_handle_tuple_routing, apply_handle_truncate, apply_dispatch,
//         get_flush_position, store_flush_position, UpdateWorkerStats,
//         LogicalRepApplyLoop, send_feedback
// ===========================================================================

/*
 * This handles insert, update, delete on a partitioned table.
 */
unsafe fn apply_handle_tuple_routing(
    edata: *mut ApplyExecutionData,
    remoteslot: *mut TupleTableSlot,
    newtup: *mut LogicalRepTupleData,
    operation: CmdType,
) {
    let estate = (*edata).estate;
    let relmapentry = (*edata).targetRel;
    let relinfo = (*edata).targetRelInfo;
    let parentrel = resultrelinfo_ri_RelationDesc(relinfo);
    let mtstate: *mut ModifyTableState;
    let proute: *mut PartitionTupleRouting;
    let partrelinfo: *mut ResultRelInfo;
    let partrel: *mut c_void;
    let mut remoteslot_part: *mut TupleTableSlot;
    let map: *mut TupleConversionMap;
    let oldctx: MemoryContext;
    let mut part_entry: *mut LogicalRepRelMapEntry = null_mut();
    let mut attrmap: *mut AttrMap = null_mut();

    /* ModifyTableState is needed for ExecFindPartition(). */
    (*edata).mtstate = makeNode_ModifyTableState();
    mtstate = (*edata).mtstate;
    mtstate_set_ps_plan(mtstate, null_mut());
    mtstate_set_ps_state(mtstate, estate);
    mtstate_set_operation(mtstate, operation);
    mtstate_set_resultRelInfo(mtstate, relinfo);

    /* ... as is PartitionTupleRouting. */
    (*edata).proute = ExecSetupPartitionTupleRouting(estate, parentrel);
    proute = (*edata).proute;

    /*
     * Find the partition to which the "search tuple" belongs.
     */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    partrelinfo = ExecFindPartition(mtstate, relinfo, proute, remoteslot, estate);
    // Assert(partrelinfo != NULL);
    partrel = resultrelinfo_ri_RelationDesc(partrelinfo);

    /*
     * Check for supported relkind. We need this since partitions might be of
     * unsupported relkinds.
     */
    CheckSubscriptionRelkind(
        pg_class_relkind(relation_rd_rel(partrel)),
        get_namespace_name(RelationGetNamespace(partrel)),
        RelationGetRelationName(partrel),
    );

    /*
     * To perform any of the operations below, the tuple must match the
     * partition's rowtype. Convert if needed or just copy.
     */
    remoteslot_part = resultrelinfo_ri_PartitionTupleSlot(partrelinfo);
    if remoteslot_part.is_null() {
        remoteslot_part = table_slot_create(partrel, estate_es_tupleTable(estate));
    }
    map = ExecGetRootToChildMap(partrelinfo, estate);
    if !map.is_null() {
        attrmap = tupleconversionmap_attrMap(map);
        remoteslot_part = execute_attr_map_slot(attrmap, remoteslot, remoteslot_part);
    } else {
        remoteslot_part = ExecCopySlot(remoteslot_part, remoteslot);
        slot_getallattrs(remoteslot_part);
    }
    MemoryContextSwitchTo(oldctx);

    /* Check if we can do the update or delete on the leaf partition. */
    if operation == CMD_UPDATE || operation == CMD_DELETE {
        part_entry = logicalrep_partition_open(relmapentry, partrel, attrmap);
        check_relation_updatable(part_entry);
    }

    match operation {
        CMD_INSERT => {
            apply_handle_insert_internal(edata, partrelinfo, remoteslot_part);
        }
        CMD_DELETE => {
            apply_handle_delete_internal(
                edata,
                partrelinfo,
                remoteslot_part,
                logicalrep_rel_mapentry_localindexoid(part_entry),
            );
        }
        CMD_UPDATE => {
            /*
             * For UPDATE, depending on whether or not the updated tuple
             * satisfies the partition's constraint, perform a simple UPDATE
             * of the partition or move the updated tuple into a different
             * suitable partition.
             */
            let mut localslot: *mut TupleTableSlot = null_mut();
            let partrelinfo_new: *mut ResultRelInfo;
            let partrel_new: *mut c_void;
            let found: bool;
            let mut epqstate_buf: [u8; 512] = [0u8; 512];
            let epqstate = epqstate_buf.as_mut_ptr() as *mut EPQState;
            let mut conflicttuple_buf: [u8; 128] = [0u8; 128];
            let conflicttuple = conflicttuple_buf.as_mut_ptr() as *mut ConflictTupleInfo;

            /* Get the matching local tuple from the partition. */
            found = FindReplTupleInLocalRel(
                edata,
                partrel,
                logicalrep_rel_mapentry_remoterel(part_entry),
                logicalrep_rel_mapentry_localindexoid(part_entry),
                remoteslot_part,
                &mut localslot,
            );
            if !found {
                let newslot = localslot;
                slot_store_data(newslot, part_entry, newtup);
                ReportApplyConflict(
                    estate, partrelinfo, LOG, CT_UPDATE_MISSING,
                    remoteslot_part, newslot, list_make1(conflicttuple as *mut c_void),
                );
                return;
            }

            /*
             * Report the conflict if the tuple was modified by a different origin.
             */
            let mut xmin: TransactionId = 0;
            let mut origin: RepOriginId = 0;
            let mut ts: TimestampTz = 0;
            if GetTupleTransactionInfo(localslot, &mut xmin, &mut origin, &mut ts)
                && origin != replorigin_session_origin
            {
                let newslot = table_slot_create(partrel, estate_es_tupleTable(estate));
                slot_store_data(newslot, part_entry, newtup);
                ReportApplyConflict(
                    estate, partrelinfo, LOG, CT_UPDATE_ORIGIN_DIFFERS,
                    remoteslot_part, newslot, list_make1(conflicttuple as *mut c_void),
                );
            }

            /*
             * Apply the update to the local tuple, putting the result in
             * remoteslot_part.
             */
            let oldctx2 = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
            slot_modify_data(remoteslot_part, localslot, part_entry, newtup);
            MemoryContextSwitchTo(oldctx2);

            EvalPlanQualInit(epqstate, estate, null_mut(), NIL, -1, NIL);

            /*
             * Does the updated tuple still satisfy the current partition's constraint?
             */
            if !pg_class_relispartition(relation_rd_rel(partrel))
                || ExecPartitionCheck(partrelinfo, remoteslot_part, estate, false)
            {
                /* Yes, so simply UPDATE the partition. */
                InitConflictIndexes(partrelinfo);
                EvalPlanQualSetSlot(epqstate, remoteslot_part);
                TargetPrivilegesCheck(resultrelinfo_ri_RelationDesc(partrelinfo), ACL_UPDATE);
                ExecSimpleRelationUpdate(partrelinfo, estate, epqstate, localslot, remoteslot_part);
            } else {
                /* Move the tuple into the new partition. */

                /*
                 * New partition will be found using tuple routing, which
                 * can only occur via the parent table. We might need to
                 * convert the tuple to the parent's rowtype.
                 */
                let mut remoteslot_mut = remoteslot;
                if !map.is_null() {
                    let partition_to_root = convert_tuples_by_name(
                        RelationGetDescr(partrel),
                        RelationGetDescr(parentrel),
                    );
                    remoteslot_mut = execute_attr_map_slot(
                        tupleconversionmap_attrMap(partition_to_root),
                        remoteslot_part,
                        remoteslot_mut,
                    );
                } else {
                    remoteslot_mut = ExecCopySlot(remoteslot_mut, remoteslot_part);
                    slot_getallattrs(remoteslot_mut);
                }

                /* Find the new partition. */
                let oldctx3 = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                partrelinfo_new = ExecFindPartition(mtstate, relinfo, proute,
                                                    remoteslot_mut, estate);
                MemoryContextSwitchTo(oldctx3);
                // Assert(partrelinfo_new != partrelinfo);
                partrel_new = resultrelinfo_ri_RelationDesc(partrelinfo_new);

                /* Check that new partition also has supported relkind. */
                CheckSubscriptionRelkind(
                    pg_class_relkind(relation_rd_rel(partrel_new)),
                    get_namespace_name(RelationGetNamespace(partrel_new)),
                    RelationGetRelationName(partrel_new),
                );

                /* DELETE old tuple found in the old partition. */
                EvalPlanQualSetSlot(epqstate, localslot);
                TargetPrivilegesCheck(resultrelinfo_ri_RelationDesc(partrelinfo), ACL_DELETE);
                ExecSimpleRelationDelete(partrelinfo, estate, epqstate, localslot);

                /* INSERT new tuple into the new partition. */

                /*
                 * Convert the replacement tuple to match the destination
                 * partition rowtype.
                 */
                let oldctx4 = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                let mut remoteslot_part_new = resultrelinfo_ri_PartitionTupleSlot(partrelinfo_new);
                if remoteslot_part_new.is_null() {
                    remoteslot_part_new = table_slot_create(partrel_new, estate_es_tupleTable(estate));
                }
                let map2 = ExecGetRootToChildMap(partrelinfo_new, estate);
                if !map2.is_null() {
                    remoteslot_part_new = execute_attr_map_slot(
                        tupleconversionmap_attrMap(map2),
                        remoteslot_mut,
                        remoteslot_part_new,
                    );
                } else {
                    remoteslot_part_new = ExecCopySlot(remoteslot_part_new, remoteslot_mut);
                    slot_getallattrs(remoteslot_mut);
                }
                MemoryContextSwitchTo(oldctx4);
                apply_handle_insert_internal(edata, partrelinfo_new, remoteslot_part_new);
            }

            EvalPlanQualEnd(epqstate);
        }
        _ => {
            elog!(ERROR, "unrecognized CmdType: %d", operation);
        }
    }
}

/*
 * Handle TRUNCATE message.
 *
 * TODO: FDW support
 */
unsafe fn apply_handle_truncate(s: *mut StringInfoData) {
    let mut cascade: bool = false;
    let mut restart_seqs: bool = false;
    let mut remote_relids: *mut List = NIL;
    let mut remote_rels: *mut List = NIL;
    let mut rels: *mut List = NIL;
    let mut part_rels: *mut List = NIL;
    let mut relids: *mut List = NIL;
    let mut relids_logged: *mut List = NIL;
    let lockmode: c_int = AccessExclusiveLock;

    /*
     * Quick return if we are skipping data modification changes or handling
     * streamed transactions.
     */
    if is_skipping_changes() || handle_streamed_transaction(LOGICAL_REP_MSG_TRUNCATE, s) {
        return;
    }

    begin_replication_step();

    remote_relids = logicalrep_read_truncate(s, &mut cascade, &mut restart_seqs);

    let mut lc = foreach_begin(remote_relids);
    while !lc.is_null() {
        let relid: LogicalRepRelId = lfirst_oid(lc);
        let rel: *mut LogicalRepRelMapEntry;

        rel = logicalrep_rel_open(relid, lockmode);
        if !should_apply_changes_for_rel(rel) {
            /*
             * The relation can't become interesting in the middle of the
             * transaction so it's safe to unlock it.
             */
            logicalrep_rel_close(rel, lockmode);
            lc = foreach_next(lc);
            continue;
        }

        remote_rels = lappend(remote_rels, rel as *mut c_void);
        TargetPrivilegesCheck(logicalrep_rel_mapentry_localrel(rel), ACL_TRUNCATE);
        rels = lappend(rels, logicalrep_rel_mapentry_localrel(rel));
        relids = lappend_oid(relids, logicalrep_rel_mapentry_localreloid(rel));
        let localrel = logicalrep_rel_mapentry_localrel(rel);
        if RelationIsLogicallyLogged(localrel) {
            relids_logged = lappend_oid(relids_logged, logicalrep_rel_mapentry_localreloid(rel));
        }

        /*
         * Truncate partitions if we got a message to truncate a partitioned
         * table.
         */
        if pg_class_relkind(relation_rd_rel(localrel)) == RELKIND_PARTITIONED_TABLE {
            let mut num_children: c_int = 0;
            let children = find_all_inheritors(
                logicalrep_rel_mapentry_localreloid(rel),
                lockmode,
                &mut num_children,
            );

            let mut child_lc = foreach_begin(children);
            while !child_lc.is_null() {
                let childrelid: Oid = lfirst_oid(child_lc);
                let childrel: *mut c_void;

                if list_member_oid(relids, childrelid) {
                    child_lc = foreach_next(child_lc);
                    continue;
                }

                /* find_all_inheritors already got lock */
                childrel = table_open(childrelid, NoLock);

                /*
                 * Ignore temp tables of other backends.
                 */
                if RELATION_IS_OTHER_TEMP(childrel) {
                    table_close(childrel, lockmode);
                    child_lc = foreach_next(child_lc);
                    continue;
                }

                TargetPrivilegesCheck(childrel, ACL_TRUNCATE);
                rels = lappend(rels, childrel);
                part_rels = lappend(part_rels, childrel);
                relids = lappend_oid(relids, childrelid);
                /* Log this relation only if needed for logical decoding */
                if RelationIsLogicallyLogged(childrel) {
                    relids_logged = lappend_oid(relids_logged, childrelid);
                }

                child_lc = foreach_next(child_lc);
            }
        }

        lc = foreach_next(lc);
    }

    /*
     * Even if we used CASCADE on the upstream primary we explicitly default
     * to replaying changes without further cascading.
     */
    ExecuteTruncateGuts(
        rels,
        relids,
        relids_logged,
        DROP_RESTRICT,
        restart_seqs,
        !subscription_runasowner(MySubscription),
    );

    let mut lc2 = foreach_begin(remote_rels);
    while !lc2.is_null() {
        let rel = lfirst(lc2) as *mut LogicalRepRelMapEntry;
        logicalrep_rel_close(rel, NoLock);
        lc2 = foreach_next(lc2);
    }
    let mut lc3 = foreach_begin(part_rels);
    while !lc3.is_null() {
        let rel = lfirst(lc3) as *mut c_void;
        table_close(rel, NoLock);
        lc3 = foreach_next(lc3);
    }

    end_replication_step();
}

/*
 * Logical replication protocol message dispatcher.
 */
pub unsafe fn apply_dispatch(s: *mut StringInfoData) {
    let action: LogicalRepMsgType = pq_getmsgbyte(s) as LogicalRepMsgType;
    let saved_command: LogicalRepMsgType;

    /*
     * Set the current command being applied. Since this function can be
     * called recursively when applying spooled changes, save the current
     * command.
     */
    saved_command = apply_error_callback_arg.command;
    apply_error_callback_arg.command = action;

    match action {
        a if a == LOGICAL_REP_MSG_BEGIN => apply_handle_begin(s),
        a if a == LOGICAL_REP_MSG_COMMIT => apply_handle_commit(s),
        a if a == LOGICAL_REP_MSG_INSERT => apply_handle_insert(s),
        a if a == LOGICAL_REP_MSG_UPDATE => apply_handle_update(s),
        a if a == LOGICAL_REP_MSG_DELETE => apply_handle_delete(s),
        a if a == LOGICAL_REP_MSG_TRUNCATE => apply_handle_truncate(s),
        a if a == LOGICAL_REP_MSG_RELATION => apply_handle_relation(s),
        a if a == LOGICAL_REP_MSG_TYPE => apply_handle_type(s),
        a if a == LOGICAL_REP_MSG_ORIGIN => apply_handle_origin(s),
        a if a == LOGICAL_REP_MSG_MESSAGE => {
            /*
             * Logical replication does not use generic logical messages yet.
             */
        }
        a if a == LOGICAL_REP_MSG_STREAM_START => apply_handle_stream_start(s),
        a if a == LOGICAL_REP_MSG_STREAM_STOP => apply_handle_stream_stop(s),
        a if a == LOGICAL_REP_MSG_STREAM_ABORT => apply_handle_stream_abort(s),
        a if a == LOGICAL_REP_MSG_STREAM_COMMIT => apply_handle_stream_commit(s),
        a if a == LOGICAL_REP_MSG_BEGIN_PREPARE => apply_handle_begin_prepare(s),
        a if a == LOGICAL_REP_MSG_PREPARE => apply_handle_prepare(s),
        a if a == LOGICAL_REP_MSG_COMMIT_PREPARED => apply_handle_commit_prepared(s),
        a if a == LOGICAL_REP_MSG_ROLLBACK_PREPARED => apply_handle_rollback_prepared(s),
        a if a == LOGICAL_REP_MSG_STREAM_PREPARE => apply_handle_stream_prepare(s),
        _ => {
            ereport!(
                ERROR,
                (
                    errcode!(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg!("invalid logical replication message type \"??? (%d)\"", action as c_int)
                )
            );
        }
    }

    /* Reset the current command */
    apply_error_callback_arg.command = saved_command;
}

/*
 * Figure out which write/flush positions to report to the walsender process.
 */
unsafe fn get_flush_position(
    write: *mut XLogRecPtr,
    flush: *mut XLogRecPtr,
    have_pending_txes: *mut bool,
) {
    let local_flush = GetFlushRecPtr(null_mut());

    *write = InvalidXLogRecPtr;
    *flush = InvalidXLogRecPtr;

    // Iterate lsn_mapping dlist
    let head = &mut lsn_mapping as *mut DListHead;
    if dlist_is_empty(head) {
        *have_pending_txes = false;
        return;
    }

    // Walk the list manually
    let mut cur = (*head).head.next;
    let end = &(*head).head as *const DListNode as *mut DListNode;
    let keep_going = true;
    while keep_going && !std::ptr::eq(cur, end) {
        let pos = (cur as usize - std::mem::offset_of!(FlushPosition, node)) as *mut FlushPosition;
        *write = (*pos).remote_end;

        if (*pos).local_end <= local_flush {
            *flush = (*pos).remote_end;
            let next = (*cur).next;
            dlist_delete(cur);
            pfree(pos as *mut c_void);
            cur = next;
        } else {
            /*
             * Don't want to uselessly iterate over the rest of the list which
             * could potentially be long.
             */
            // Get the tail element and grab the write position from there.
            // Walk to tail
            let mut tail_cur = cur;
            while !std::ptr::eq((*tail_cur).next, end) {
                tail_cur = (*tail_cur).next;
            }
            let tail_pos = (tail_cur as usize - std::mem::offset_of!(FlushPosition, node)) as *mut FlushPosition;
            *write = (*tail_pos).remote_end;
            *have_pending_txes = true;
            return;
        }
    }

    *have_pending_txes = !dlist_is_empty(head);
}

/*
 * Store current remote/local lsn pair in the tracking list.
 */
pub unsafe fn store_flush_position(remote_lsn: XLogRecPtr, local_lsn: XLogRecPtr) {
    let flushpos: *mut FlushPosition;

    /*
     * Skip for parallel apply workers, because the lsn_mapping is maintained
     * by the leader apply worker.
     */
    if am_parallel_apply_worker() {
        return;
    }

    /* Need to do this in permanent context */
    MemoryContextSwitchTo(ApplyContext);

    /* Track commit lsn */
    flushpos = palloc(std::mem::size_of::<FlushPosition>()) as *mut FlushPosition;
    (*flushpos).local_end = local_lsn;
    (*flushpos).remote_end = remote_lsn;

    dlist_push_tail(&mut lsn_mapping, &mut (*flushpos).node);
    MemoryContextSwitchTo(ApplyMessageContext);
}

/* Update statistics of the worker. */
unsafe fn UpdateWorkerStats(last_lsn: XLogRecPtr, send_time: TimestampTz, reply: bool) {
    (*MyLogicalRepWorker).last_lsn = last_lsn;
    (*MyLogicalRepWorker).last_send_time = send_time;
    (*MyLogicalRepWorker).last_recv_time = GetCurrentTimestamp();
    if reply {
        (*MyLogicalRepWorker).reply_lsn = last_lsn;
        (*MyLogicalRepWorker).reply_time = send_time;
    }
}

/*
 * Apply main loop.
 */
unsafe fn LogicalRepApplyLoop(last_received: XLogRecPtr) {
    let mut last_received = last_received;
    let mut last_recv_timestamp: TimestampTz = GetCurrentTimestamp();
    let mut ping_sent: bool = false;
    let mut tli: TimeLineID = 0;
    // ErrorContextCallback errcallback -- TODO(pg-port): real errcontext

    /*
     * Init the ApplyMessageContext which we clean up after each replication
     * protocol message.
     */
    ApplyMessageContext = AllocSetContextCreate(
        ApplyContext,
        b"ApplyMessageContext\0".as_ptr() as *const c_char,
        0, 8192, 8192 * 1024,
    );

    /*
     * This memory context is used for per-stream data when the streaming mode
     * is enabled. This context is reset on each stream stop.
     */
    LogicalStreamingContext = AllocSetContextCreate(
        ApplyContext,
        b"LogicalStreamingContext\0".as_ptr() as *const c_char,
        0, 8192, 8192 * 1024,
    );

    /* mark as idle, before starting to loop */
    pgstat_report_activity(STATE_IDLE, null());

    /*
     * Push apply error context callback. Fields will be filled while applying
     * a change.
     */
    // errcallback.callback = apply_error_callback;
    // errcallback.previous = error_context_stack;
    // error_context_stack = &errcallback;
    // apply_error_context_stack = error_context_stack;

    /* This outer loop iterates once per wait. */
    #[allow(unused_labels)]
    'outer: loop {
        let fd: pgsocket;
        let rc: c_int;
        let mut len: c_int;
        let mut buf: *mut c_char = null_mut();
        let mut endofstream: bool = false;
        let wait_time: i64;

        CHECK_FOR_INTERRUPTS();

        MemoryContextSwitchTo(ApplyMessageContext);

        len = walrcv_receive(LogRepWorkerWalRcvConn, &mut buf, &mut (PGINVALID_SOCKET as pgsocket));
        let mut fd_val: pgsocket = PGINVALID_SOCKET;
        len = walrcv_receive(LogRepWorkerWalRcvConn, &mut buf, &mut fd_val);
        let fd = fd_val;

        if len != 0 {
            /* Loop to process all available data (without blocking). */
            loop {
                CHECK_FOR_INTERRUPTS();

                if len == 0 {
                    break;
                } else if len < 0 {
                    ereport!(
                        LOG,
                        (errmsg!("data stream from publisher has ended"))
                    );
                    endofstream = true;
                    break;
                } else {
                    let c: c_int;
                    let mut s: StringInfoData = std::mem::zeroed();

                    if ConfigReloadPending {
                        ConfigReloadPending = false;
                        ProcessConfigFile(PGC_SIGHUP);
                    }

                    /* Reset timeout. */
                    last_recv_timestamp = GetCurrentTimestamp();
                    ping_sent = false;

                    /* Ensure we are reading the data into our memory context. */
                    MemoryContextSwitchTo(ApplyMessageContext);

                    initReadOnlyStringInfo(&mut s, buf, len);

                    c = pq_getmsgbyte(&mut s);

                    if c == b'w' as c_int {
                        let start_lsn: XLogRecPtr = pq_getmsgint64(&mut s) as XLogRecPtr;
                        let end_lsn: XLogRecPtr = pq_getmsgint64(&mut s) as XLogRecPtr;
                        let send_time: TimestampTz = pq_getmsgint64(&mut s);

                        if last_received < start_lsn {
                            last_received = start_lsn;
                        }
                        if last_received < end_lsn {
                            last_received = end_lsn;
                        }

                        UpdateWorkerStats(last_received, send_time, false);

                        apply_dispatch(&mut s);
                    } else if c == b'k' as c_int {
                        let end_lsn: XLogRecPtr = pq_getmsgint64(&mut s) as XLogRecPtr;
                        let timestamp: TimestampTz = pq_getmsgint64(&mut s);
                        let reply_requested: bool = pq_getmsgbyte(&mut s) != 0;

                        if last_received < end_lsn {
                            last_received = end_lsn;
                        }

                        send_feedback(last_received, reply_requested, false);
                        UpdateWorkerStats(last_received, timestamp, true);
                    }
                    /* other message types are purposefully ignored */

                    MemoryContextReset(ApplyMessageContext);
                }

                len = walrcv_receive(LogRepWorkerWalRcvConn, &mut buf, &mut fd_val);
                len = 0; // dummy to break loop -- real receive happens above
                break;
            }
        }

        /* confirm all writes so far */
        send_feedback(last_received, false, false);

        if !in_remote_transaction && !in_streamed_transaction {
            /*
             * If we didn't get any transactions for a while there might be
             * unconsumed invalidation messages in the queue, consume them
             * now.
             */
            AcceptInvalidationMessages();
            maybe_reread_subscription();

            /* Process any table synchronization changes. */
            process_syncing_tables(last_received);
        }

        /* Cleanup the memory. */
        MemoryContextReset(ApplyMessageContext);
        MemoryContextSwitchTo(crate::utils::mmgr::mcxt::TopMemoryContext as MemoryContext);

        /* Check if we need to exit the streaming loop. */
        if endofstream {
            break;
        }

        /*
         * Wait for more data or latch.
         */
        if !dlist_is_empty(&lsn_mapping) {
            wait_time = WalWriterDelay;
        } else {
            wait_time = NAPTIME_PER_CYCLE;
        }

        let rc = WaitLatchOrSocket(
            MyLatch,
            WL_SOCKET_READABLE | WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            fd,
            wait_time,
            WAIT_EVENT_LOGICAL_APPLY_MAIN,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if rc & WL_TIMEOUT != 0 {
            /*
             * We didn't receive anything new. If we haven't heard anything
             * from the server for more than wal_receiver_timeout / 2, ping
             * the server.
             */
            let mut request_reply: bool = false;

            /*
             * Check if time since last receive from primary has reached the
             * configured limit.
             */
            if wal_receiver_timeout > 0 {
                let now: TimestampTz = GetCurrentTimestamp();
                let timeout: TimestampTz = TimestampTzPlusMilliseconds(last_recv_timestamp, wal_receiver_timeout);

                if now >= timeout {
                    ereport!(
                        ERROR,
                        (
                            errcode!(ERRCODE_CONNECTION_FAILURE),
                            errmsg!("terminating logical replication worker due to timeout")
                        )
                    );
                }

                /* Check to see if it's time for a ping. */
                if !ping_sent {
                    let timeout2 = TimestampTzPlusMilliseconds(last_recv_timestamp, wal_receiver_timeout / 2);
                    if now >= timeout2 {
                        request_reply = true;
                        ping_sent = true;
                    }
                }
            }

            send_feedback(last_received, request_reply, request_reply);

            /*
             * Force reporting to ensure long idle periods don't lead to
             * arbitrarily delayed stats.
             */
            if !IsTransactionState() {
                pgstat_report_stat(true);
            }
        }
    }

    /* Pop the error context stack */
    // error_context_stack = errcallback.previous;
    // apply_error_context_stack = error_context_stack;

    /* All done */
    walrcv_endstreaming(LogRepWorkerWalRcvConn, &mut tli);
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
unsafe fn send_feedback(recvpos: XLogRecPtr, force: bool, request_reply: bool) {
    static mut reply_message: *mut StringInfoData = null_mut();
    static mut send_time_s: TimestampTz = 0;

    static mut last_recvpos: XLogRecPtr = InvalidXLogRecPtr;
    static mut last_writepos: XLogRecPtr = InvalidXLogRecPtr;
    static mut last_flushpos: XLogRecPtr = InvalidXLogRecPtr;

    let mut recvpos = recvpos;
    let mut writepos: XLogRecPtr = InvalidXLogRecPtr;
    let mut flushpos: XLogRecPtr = InvalidXLogRecPtr;
    let now: TimestampTz;
    let mut have_pending_txes: bool = false;

    /*
     * If the user doesn't want status to be reported to the publisher, be
     * sure to exit before doing anything at all.
     */
    if !force && wal_receiver_status_interval <= 0 {
        return;
    }

    /* It's legal to not pass a recvpos */
    if recvpos < last_recvpos {
        recvpos = last_recvpos;
    }

    get_flush_position(&mut writepos, &mut flushpos, &mut have_pending_txes);

    /*
     * No outstanding transactions to flush, we can report the latest received
     * position. This is important for synchronous replication.
     */
    if !have_pending_txes {
        flushpos = recvpos;
        writepos = recvpos;
    }

    if writepos < last_writepos {
        writepos = last_writepos;
    }
    if flushpos < last_flushpos {
        flushpos = last_flushpos;
    }

    now = GetCurrentTimestamp();

    /* if we've already reported everything we're good */
    if !force
        && writepos == last_writepos
        && flushpos == last_flushpos
        && !TimestampDifferenceExceeds(send_time_s, now, (wal_receiver_status_interval * 1000) as c_int)
    {
        return;
    }
    send_time_s = now;

    if reply_message.is_null() {
        let oldctx = MemoryContextSwitchTo(ApplyContext);
        reply_message = makeStringInfo();
        MemoryContextSwitchTo(oldctx);
    } else {
        resetStringInfo(reply_message);
    }

    pq_sendbyte(reply_message, b'r');
    pq_sendint64(reply_message, recvpos as int64);   /* write */
    pq_sendint64(reply_message, flushpos as int64);  /* flush */
    pq_sendint64(reply_message, writepos as int64);  /* apply */
    pq_sendint64(reply_message, now);                /* sendTime */
    pq_sendbyte(reply_message, request_reply as u8); /* replyRequested */

    elog!(
        DEBUG2,
        "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
        force as c_int,
        (recvpos >> 32) as c_int,
        (recvpos & 0xFFFFFFFF) as c_int,
        (writepos >> 32) as c_int,
        (writepos & 0xFFFFFFFF) as c_int,
        (flushpos >> 32) as c_int,
        (flushpos & 0xFFFFFFFF) as c_int
    );

    walrcv_send(LogRepWorkerWalRcvConn, (*reply_message).data, (*reply_message).len);

    if recvpos > last_recvpos { last_recvpos = recvpos; }
    if writepos > last_writepos { last_writepos = writepos; }
    if flushpos > last_flushpos { last_flushpos = flushpos; }
}

// ===========================================================================
// Part 7: apply_worker_exit, maybe_reread_subscription, subscription_change_cb,
//         subxact_info_write/read/add, file helpers, start_apply, run_apply_worker
// ===========================================================================

/*
 * Exit routine for apply workers due to subscription parameter changes.
 */
unsafe fn apply_worker_exit() {
    if am_parallel_apply_worker() {
        /*
         * Don't stop the parallel apply worker as the leader will detect the
         * subscription parameter change and restart logical replication later
         * anyway.
         */
        return;
    }

    /*
     * Reset the last-start time for this apply worker so that the launcher
     * will restart it without waiting for wal_retrieve_retry_interval.
     */
    if am_leader_apply_worker() {
        ApplyLauncherForgetWorkerStartTime((*MyLogicalRepWorker).subid);
    }

    proc_exit(0);
}

/*
 * Reread subscription info if needed.
 *
 * For significant changes, we react by exiting the current process; a new
 * one will be launched afterwards if needed.
 */
pub unsafe fn maybe_reread_subscription() {
    let oldctx: MemoryContext;
    let newsub: *mut Subscription;
    let mut started_tx = false;

    /* When cache state is valid there is nothing to do here. */
    if MySubscriptionValid {
        return;
    }

    /* This function might be called inside or outside of transaction. */
    if !IsTransactionState() {
        StartTransactionCommand();
        started_tx = true;
    }

    /* Ensure allocations in permanent context. */
    oldctx = MemoryContextSwitchTo(ApplyContext);

    newsub = GetSubscription((*MyLogicalRepWorker).subid, true);

    /*
     * Exit if the subscription was removed.
     */
    if newsub.is_null() {
        ereport!(
            LOG,
            (errmsg!(
                "logical replication worker for subscription \"%s\" will stop because the subscription was removed",
                subscription_name(MySubscription)
            ))
        );

        /* Ensure we remove no-longer-useful entry for worker's start time */
        if am_leader_apply_worker() {
            ApplyLauncherForgetWorkerStartTime((*MyLogicalRepWorker).subid);
        }

        proc_exit(0);
    }

    /* Exit if the subscription was disabled. */
    if !subscription_enabled(newsub) {
        ereport!(
            LOG,
            (errmsg!(
                "logical replication worker for subscription \"%s\" will stop because the subscription was disabled",
                subscription_name(MySubscription)
            ))
        );

        apply_worker_exit();
    }

    /* !slotname should never happen when enabled is true. */
    // Assert(newsub->slotname);

    /* two-phase cannot be altered while the worker is running */
    // Assert(newsub->twophasestate == MySubscription->twophasestate);

    /*
     * Exit if any parameter that affects the remote connection was changed.
     */
    extern "C" { fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int; }
    if strcmp(subscription_conninfo(newsub), subscription_conninfo(MySubscription)) != 0
        || strcmp(subscription_name(newsub), subscription_name(MySubscription)) != 0
        || strcmp(subscription_slotname(newsub), subscription_slotname(MySubscription)) != 0
        || subscription_binary(newsub) != subscription_binary(MySubscription)
        || subscription_stream(newsub) != subscription_stream(MySubscription)
        || subscription_passwordrequired(newsub) != subscription_passwordrequired(MySubscription)
        || strcmp(subscription_origin(newsub), subscription_origin(MySubscription)) != 0
        || subscription_owner(newsub) != subscription_owner(MySubscription)
        || !equal(subscription_publications(newsub) as *mut c_void,
                   subscription_publications(MySubscription) as *mut c_void)
    {
        if am_parallel_apply_worker() {
            ereport!(
                LOG,
                (errmsg!(
                    "logical replication parallel apply worker for subscription \"%s\" will stop because of a parameter change",
                    subscription_name(MySubscription)
                ))
            );
        } else {
            ereport!(
                LOG,
                (errmsg!(
                    "logical replication worker for subscription \"%s\" will restart because of a parameter change",
                    subscription_name(MySubscription)
                ))
            );
        }

        apply_worker_exit();
    }

    /*
     * Exit if the subscription owner's superuser privileges have been revoked.
     */
    if !subscription_ownersuperuser(newsub) && subscription_ownersuperuser(MySubscription) {
        if am_parallel_apply_worker() {
            ereport!(
                LOG,
                (errmsg!(
                    "logical replication parallel apply worker for subscription \"%s\" will stop because the subscription owner's superuser privileges have been revoked",
                    subscription_name(MySubscription)
                ))
            );
        } else {
            ereport!(
                LOG,
                (errmsg!(
                    "logical replication worker for subscription \"%s\" will restart because the subscription owner's superuser privileges have been revoked",
                    subscription_name(MySubscription)
                ))
            );
        }

        apply_worker_exit();
    }

    /* Check for other changes that should never happen too. */
    if subscription_dbid(newsub) != subscription_dbid(MySubscription) {
        elog!(
            ERROR,
            "subscription %u changed unexpectedly",
            (*MyLogicalRepWorker).subid
        );
    }

    /* Clean old subscription info and switch to new one. */
    FreeSubscription(MySubscription);
    MySubscription = newsub;

    MemoryContextSwitchTo(oldctx);

    /* Change synchronous commit according to the user's wishes */
    SetConfigOption(
        b"synchronous_commit\0".as_ptr() as *const c_char,
        subscription_synccommit(MySubscription),
        PGC_BACKEND,
        PGC_S_OVERRIDE,
    );

    if started_tx {
        CommitTransactionCommand();
    }

    MySubscriptionValid = true;
}

/*
 * Callback from subscription syscache invalidation.
 */
pub unsafe extern "C" fn subscription_change_cb(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    MySubscriptionValid = false;
}

/*
 * subxact_info_write
 *   Store information about subxacts for a toplevel transaction.
 */
unsafe fn subxact_info_write(subid: Oid, xid: TransactionId) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let len: Size;
    let fd: *mut BufFile;

    // Assert(TransactionIdIsValid(xid));

    /* construct the subxact filename */
    subxact_filename(path.as_mut_ptr(), subid, xid);

    /* Delete the subxacts file, if exists. */
    if subxact_data.nsubxacts == 0 {
        cleanup_subxact_info();
        BufFileDeleteFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr(), true);
        return;
    }

    /*
     * Create the subxact file if it not already created, otherwise open the
     * existing file.
     */
    let mut fd = BufFileOpenFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr(), O_RDWR, true);
    if fd.is_null() {
        fd = BufFileCreateFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr());
    }

    len = std::mem::size_of::<SubXactInfo>() * subxact_data.nsubxacts as usize;

    /* Write the subxact count and subxact info */
    BufFileWrite(
        fd,
        &subxact_data.nsubxacts as *const uint32 as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    BufFileWrite(fd, subxact_data.subxacts as *const c_void, len);

    BufFileClose(fd);

    /* free the memory allocated for subxact info */
    cleanup_subxact_info();
}

/*
 * subxact_info_read
 *   Restore information about subxacts of a streamed transaction.
 */
unsafe fn subxact_info_read(subid: Oid, xid: TransactionId) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let len: Size;
    let fd: *mut BufFile;
    let oldctx: MemoryContext;

    // Assert(!subxact_data.subxacts);
    // Assert(subxact_data.nsubxacts == 0);
    // Assert(subxact_data.nsubxacts_max == 0);

    /*
     * If the subxact file doesn't exist that means we don't have any subxact info.
     */
    subxact_filename(path.as_mut_ptr(), subid, xid);
    let fd = BufFileOpenFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr(), O_RDONLY, true);
    if fd.is_null() {
        return;
    }

    /* read number of subxact items */
    BufFileReadExact(
        fd,
        &mut subxact_data.nsubxacts as *mut uint32 as *mut c_void,
        std::mem::size_of::<uint32>(),
    );

    len = std::mem::size_of::<SubXactInfo>() * subxact_data.nsubxacts as usize;

    /* we keep the maximum as a power of 2 */
    subxact_data.nsubxacts_max = 1 << my_log2(subxact_data.nsubxacts as c_int);

    /*
     * Allocate subxact information in the logical streaming context.
     */
    oldctx = MemoryContextSwitchTo(LogicalStreamingContext);
    subxact_data.subxacts = palloc(
        subxact_data.nsubxacts_max as usize * std::mem::size_of::<SubXactInfo>(),
    ) as *mut SubXactInfo;
    MemoryContextSwitchTo(oldctx);

    if len > 0 {
        BufFileReadExact(fd, subxact_data.subxacts as *mut c_void, len);
    }

    BufFileClose(fd);
}

/*
 * subxact_info_add
 *   Add information about a subxact (offset in the main file).
 */
unsafe fn subxact_info_add(xid: TransactionId) {
    let mut subxacts = subxact_data.subxacts;
    let mut i: i64;

    /* We must have a valid top level stream xid and a stream fd. */
    // Assert(TransactionIdIsValid(stream_xid));
    // Assert(stream_fd != NULL);

    /*
     * If the XID matches the toplevel transaction, we don't want to add it.
     */
    if stream_xid == xid {
        return;
    }

    /*
     * In most cases we're checking the same subxact as we've already seen in
     * the last call, so make sure to ignore it.
     */
    if subxact_data.subxact_last == xid {
        return;
    }

    /* OK, remember we're processing this XID. */
    subxact_data.subxact_last = xid;

    /*
     * Check if the transaction is already present in the array of subxact.
     */
    i = subxact_data.nsubxacts as i64;
    while i > 0 {
        i -= 1;
        /* found, so we're done */
        if (*subxacts.add(i as usize)).xid == xid {
            return;
        }
    }

    /* This is a new subxact, so we need to add it to the array. */
    if subxact_data.nsubxacts == 0 {
        let oldctx: MemoryContext;

        subxact_data.nsubxacts_max = 128;

        /*
         * Allocate this memory for subxacts in per-stream context.
         */
        oldctx = MemoryContextSwitchTo(LogicalStreamingContext);
        subxacts = palloc(subxact_data.nsubxacts_max as usize * std::mem::size_of::<SubXactInfo>())
            as *mut SubXactInfo;
        MemoryContextSwitchTo(oldctx);
    } else if subxact_data.nsubxacts == subxact_data.nsubxacts_max {
        subxact_data.nsubxacts_max *= 2;
        subxacts = repalloc(
            subxacts as *mut c_void,
            subxact_data.nsubxacts_max as usize * std::mem::size_of::<SubXactInfo>(),
        ) as *mut SubXactInfo;
    }

    (*subxacts.add(subxact_data.nsubxacts as usize)).xid = xid;

    /*
     * Get the current offset of the stream file and store it as offset of
     * this subxact.
     */
    BufFileTell(
        stream_fd,
        &mut (*subxacts.add(subxact_data.nsubxacts as usize)).fileno,
        &mut (*subxacts.add(subxact_data.nsubxacts as usize)).offset,
    );

    subxact_data.nsubxacts += 1;
    subxact_data.subxacts = subxacts;
}

/* format filename for file containing the info about subxacts */
#[inline(always)]
unsafe fn subxact_filename(path: *mut c_char, subid: Oid, xid: TransactionId) {
    snprintf_(
        path,
        MAXPGPATH,
        b"%u-%u.subxacts\0".as_ptr() as *const c_char,
        subid,
        xid,
    );
}

/* format filename for file containing serialized changes */
#[inline(always)]
unsafe fn changes_filename(path: *mut c_char, subid: Oid, xid: TransactionId) {
    snprintf_(
        path,
        MAXPGPATH,
        b"%u-%u.changes\0".as_ptr() as *const c_char,
        subid,
        xid,
    );
}

/*
 * stream_cleanup_files
 *   Cleanup files for a subscription / toplevel transaction.
 */
pub unsafe fn stream_cleanup_files(subid: Oid, xid: TransactionId) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /* Delete the changes file. */
    changes_filename(path.as_mut_ptr(), subid, xid);
    BufFileDeleteFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr(), false);

    /* Delete the subxact file, if it exists. */
    subxact_filename(path.as_mut_ptr(), subid, xid);
    BufFileDeleteFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr(), true);
}

/*
 * stream_open_file
 *   Open a file that we'll use to serialize changes for a toplevel
 * transaction.
 */
unsafe fn stream_open_file(subid: Oid, xid: TransactionId, first_segment: bool) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let oldcxt: MemoryContext;

    // Assert(OidIsValid(subid));
    // Assert(TransactionIdIsValid(xid));
    // Assert(stream_fd == NULL);

    changes_filename(path.as_mut_ptr(), subid, xid);
    elog!(DEBUG1, "opening file \"%s\" for streamed changes", path.as_ptr());

    /*
     * Create/open the buffiles under the logical streaming context so that we
     * have those files until stream stop.
     */
    oldcxt = MemoryContextSwitchTo(LogicalStreamingContext);

    /*
     * If this is the first streamed segment, create the changes file.
     * Otherwise, just open the file for writing, in append mode.
     */
    if first_segment {
        stream_fd = BufFileCreateFileSet((*MyLogicalRepWorker).stream_fileset, path.as_ptr());
    } else {
        /*
         * Open the file and seek to the end of the file because we always
         * append the changes file.
         */
        stream_fd = BufFileOpenFileSet(
            (*MyLogicalRepWorker).stream_fileset,
            path.as_ptr(),
            O_RDWR,
            false,
        );
        BufFileSeek(stream_fd, 0, 0, SEEK_END);
    }

    MemoryContextSwitchTo(oldcxt);
}

/*
 * stream_close_file
 *   Close the currently open file with streamed changes.
 */
unsafe fn stream_close_file() {
    // Assert(stream_fd != NULL);

    BufFileClose(stream_fd);

    stream_fd = null_mut();
}

/*
 * stream_write_change
 *   Serialize a change to a file for the current toplevel transaction.
 */
unsafe fn stream_write_change(action: LogicalRepMsgType, s: *const StringInfoData) {
    let len: c_int;

    // Assert(stream_fd != NULL);

    /* total on-disk size, including the action type character */
    let remaining = (*s).len - (*s).cursor;
    let len_val: c_int = remaining + std::mem::size_of::<c_char>() as c_int;

    /* first write the size */
    BufFileWrite(
        stream_fd,
        &len_val as *const c_int as *const c_void,
        std::mem::size_of::<c_int>(),
    );

    /* then the action */
    BufFileWrite(
        stream_fd,
        &action as *const c_char as *const c_void,
        std::mem::size_of::<c_char>(),
    );

    /* and finally the remaining part of the buffer (after the XID) */
    let data_len = (*s).len - (*s).cursor;
    BufFileWrite(
        stream_fd,
        (*s).data.add((*s).cursor as usize) as *const c_void,
        data_len as Size,
    );
}

/*
 * stream_open_and_write_change
 *   Serialize a message to a file for the given transaction.
 *
 * This function is similar to stream_write_change except that it will open the
 * target file if not already before writing the message and close the file at
 * the end.
 */
unsafe fn stream_open_and_write_change(xid: TransactionId, action: LogicalRepMsgType, s: *const StringInfoData) {
    // Assert(!in_streamed_transaction);

    if stream_fd.is_null() {
        stream_start_internal(xid, false);
    }

    stream_write_change(action, s);
    stream_stop_internal(xid);
}

/*
 * Sets streaming options including replication slot name and origin start
 * position. Workers need these options for logical replication.
 */
pub unsafe fn set_stream_options(
    options: *mut WalRcvStreamOptions,
    slotname: *mut c_char,
    origin_startpos: *mut XLogRecPtr,
) {
    let server_version: c_int;

    // options->logical = true;
    // options->startpoint = *origin_startpos;
    // options->slotname = slotname;
    set_walrcv_stream_options_logical(options, true);
    set_walrcv_stream_options_startpoint(options, *origin_startpos);
    set_walrcv_stream_options_slotname(options, slotname);

    server_version = walrcv_server_version(LogRepWorkerWalRcvConn);
    let proto_version: c_int = if server_version >= 160000 {
        LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM
    } else if server_version >= 150000 {
        LOGICALREP_PROTO_TWOPHASE_VERSION_NUM
    } else if server_version >= 140000 {
        LOGICALREP_PROTO_STREAM_VERSION_NUM
    } else {
        LOGICALREP_PROTO_VERSION_NUM
    };
    set_walrcv_options_proto_version(options, proto_version);
    set_walrcv_options_publication_names(options, subscription_publications(MySubscription));
    set_walrcv_options_binary(options, subscription_binary(MySubscription));

    /*
     * Assign the appropriate option value for streaming option according to
     * the 'streaming' mode and the publisher's ability to support that mode.
     */
    if server_version >= 160000 && subscription_stream(MySubscription) == LOGICALREP_STREAM_PARALLEL {
        set_walrcv_options_streaming_str(options, b"parallel\0".as_ptr() as *const c_char);
        (*MyLogicalRepWorker).parallel_apply = true;
    } else if server_version >= 140000 && subscription_stream(MySubscription) != LOGICALREP_STREAM_OFF {
        set_walrcv_options_streaming_str(options, b"on\0".as_ptr() as *const c_char);
        (*MyLogicalRepWorker).parallel_apply = false;
    } else {
        set_walrcv_options_streaming_str(options, null());
        (*MyLogicalRepWorker).parallel_apply = false;
    }

    set_walrcv_options_twophase(options, false);
    set_walrcv_options_origin(options, pstrdup(subscription_origin(MySubscription)));
}

// WalRcvStreamOptions accessors -- TODO(pg-port)
extern "C" {
    fn set_walrcv_stream_options_logical(opts: *mut WalRcvStreamOptions, v: bool);
    fn set_walrcv_stream_options_startpoint(opts: *mut WalRcvStreamOptions, v: XLogRecPtr);
    fn set_walrcv_stream_options_slotname(opts: *mut WalRcvStreamOptions, v: *mut c_char);
    fn set_walrcv_options_proto_version(opts: *mut WalRcvStreamOptions, v: c_int);
    fn set_walrcv_options_publication_names(opts: *mut WalRcvStreamOptions, v: *mut List);
    fn set_walrcv_options_binary(opts: *mut WalRcvStreamOptions, v: bool);
    fn set_walrcv_options_streaming_str(opts: *mut WalRcvStreamOptions, v: *const c_char);
    fn set_walrcv_options_twophase(opts: *mut WalRcvStreamOptions, v: bool);
    fn set_walrcv_options_origin(opts: *mut WalRcvStreamOptions, v: *mut c_char);
    fn get_walrcv_options_twophase(opts: *mut WalRcvStreamOptions) -> bool;
    fn set_walrcv_options_twophase_val(opts: *mut WalRcvStreamOptions, v: bool);
}

/*
 * Cleanup the memory for subxacts and reset the related variables.
 */
#[inline(always)]
unsafe fn cleanup_subxact_info() {
    if !subxact_data.subxacts.is_null() {
        pfree(subxact_data.subxacts as *mut c_void);
    }

    subxact_data.subxacts = null_mut();
    subxact_data.subxact_last = InvalidTransactionId;
    subxact_data.nsubxacts = 0;
    subxact_data.nsubxacts_max = 0;
}

/*
 * Common function to run the apply loop with error handling. Disable the
 * subscription, if necessary.
 *
 * Note that we don't handle FATAL errors which are probably because
 * of system resource error and are not repeatable.
 */
pub unsafe fn start_apply(origin_startpos: XLogRecPtr) {
    extern "C" {
        fn PG_TRY_start() -> c_int;
        fn PG_CATCH_start() -> c_int;
        fn PG_END_TRY();
        fn PG_RE_THROW();
    }
    // TODO(pg-port): use real PG_TRY/PG_CATCH infrastructure
    // For now, translate as a direct call (error handling not complete)
    LogicalRepApplyLoop(origin_startpos);
    // In real code: wrap in PG_TRY / PG_CATCH for disableonerr handling
}

/*
 * Runs the leader apply worker.
 *
 * It sets up replication origin, streaming options and then starts streaming.
 */
unsafe fn run_apply_worker() {
    let mut originname: [c_char; 1024 /* NAMEDATALEN */] = [0; 1024];
    let mut origin_startpos: XLogRecPtr = InvalidXLogRecPtr;
    let slotname: *const c_char;
    let mut options_buf: [u8; 512] = [0u8; 512]; // WalRcvStreamOptions
    let options = options_buf.as_mut_ptr() as *mut WalRcvStreamOptions;
    let mut originid: RepOriginId;
    let mut startpoint_tli: TimeLineID = 0;
    let mut err: *mut c_char = null_mut();
    let must_use_password: bool;

    slotname = subscription_slotname(MySubscription);

    /*
     * This shouldn't happen if the subscription is enabled, but guard against
     * DDL bugs or manual catalog changes.
     */
    if slotname.is_null() {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!("subscription has no replication slot set")
            )
        );
    }

    /* Setup replication origin tracking. */
    ReplicationOriginNameForLogicalRep(
        subscription_oid(MySubscription),
        InvalidOid,
        originname.as_mut_ptr(),
        originname.len(),
    );
    StartTransactionCommand();
    originid = replorigin_by_name(originname.as_ptr(), true);
    if !OidIsValid(originid) {
        originid = replorigin_create(originname.as_ptr());
    }
    replorigin_session_setup(originid, 0);
    replorigin_session_origin = originid;
    origin_startpos = replorigin_session_get_progress(false);
    CommitTransactionCommand();

    /* Is the use of a password mandatory? */
    must_use_password = subscription_passwordrequired(MySubscription)
        && !subscription_ownersuperuser(MySubscription);

    LogRepWorkerWalRcvConn = walrcv_connect(
        subscription_conninfo(MySubscription),
        true,
        true,
        must_use_password,
        subscription_name(MySubscription),
        &mut err,
    );

    if LogRepWorkerWalRcvConn.is_null() {
        ereport!(
            ERROR,
            (
                errcode!(ERRCODE_CONNECTION_FAILURE),
                errmsg!(
                    "apply worker for subscription \"%s\" could not connect to the publisher: %s",
                    subscription_name(MySubscription),
                    err
                )
            )
        );
    }

    /*
     * We don't really use the output identify_system for anything but it does
     * some initializations on the upstream so let's still call it.
     */
    walrcv_identify_system(LogRepWorkerWalRcvConn, &mut startpoint_tli);

    set_apply_error_context_origin_c(originname.as_mut_ptr());

    set_stream_options(options, slotname as *mut c_char, &mut origin_startpos);

    /*
     * Even when the two_phase mode is requested by the user, it remains as
     * the tri-state PENDING until all tablesyncs have reached READY state.
     */
    if subscription_twophasestate(MySubscription) == LOGICALREP_TWOPHASE_STATE_PENDING
        && AllTablesyncsReady()
    {
        /* Start streaming with two_phase enabled */
        set_walrcv_options_twophase_val(options, true);
        walrcv_startstreaming(LogRepWorkerWalRcvConn, options);

        StartTransactionCommand();

        /*
         * Updating pg_subscription might involve TOAST table access, so
         * ensure we have a valid snapshot.
         */
        PushActiveSnapshot(GetTransactionSnapshot());

        UpdateTwoPhaseState(subscription_oid(MySubscription), LOGICALREP_TWOPHASE_STATE_ENABLED);
        // MySubscription->twophasestate = LOGICALREP_TWOPHASE_STATE_ENABLED;
        // (updated via the C-side catalog, MySubscription will be re-read)
        PopActiveSnapshot();
        CommitTransactionCommand();
    } else {
        walrcv_startstreaming(LogRepWorkerWalRcvConn, options);
    }

    ereport!(
        DEBUG1,
        (errmsg_internal!(
            "logical replication apply worker for subscription \"%s\" two_phase is %s",
            subscription_name(MySubscription),
            if subscription_twophasestate(MySubscription) == LOGICALREP_TWOPHASE_STATE_DISABLED {
                b"DISABLED\0".as_ptr() as *const c_char
            } else if subscription_twophasestate(MySubscription) == LOGICALREP_TWOPHASE_STATE_PENDING {
                b"PENDING\0".as_ptr() as *const c_char
            } else if subscription_twophasestate(MySubscription) == LOGICALREP_TWOPHASE_STATE_ENABLED {
                b"ENABLED\0".as_ptr() as *const c_char
            } else {
                b"?\0".as_ptr() as *const c_char
            }
        ))
    );

    /* Run the main loop. */
    start_apply(origin_startpos);
}

// ===========================================================================
// Part 8: InitializeLogRepWorker, replorigin_reset, SetupApplyOrSyncWorker,
//         ApplyWorkerMain, DisableSubscriptionAndExit, IsLogicalWorker,
//         IsLogicalParallelApplyWorker, maybe_start_skipping_changes,
//         stop_skipping_changes, clear_subscription_skip_lsn,
//         apply_error_callback, set/reset helpers, wakeup functions,
//         set_apply_error_context_origin, get_transaction_apply_action,
//         am_* predicates
// ===========================================================================

/*
 * Common initialization for leader apply worker, parallel apply worker and
 * tablesync worker.
 *
 * Initialize the database connection, in-memory subscription and necessary
 * config options.
 */
pub unsafe fn InitializeLogRepWorker() {
    let oldctx: MemoryContext;

    /* Run as replica session replication role. */
    SetConfigOption(
        b"session_replication_role\0".as_ptr() as *const c_char,
        b"replica\0".as_ptr() as *const c_char,
        PGC_SUSET,
        PGC_S_OVERRIDE,
    );

    /* Connect to our database. */
    BackgroundWorkerInitializeConnectionByOid(
        (*MyLogicalRepWorker).dbid,
        (*MyLogicalRepWorker).userid,
        0,
    );

    /*
     * Set always-secure search path, so malicious users can't redirect user
     * code (e.g. pg_index.indexprs).
     */
    SetConfigOption(
        b"search_path\0".as_ptr() as *const c_char,
        b"\0".as_ptr() as *const c_char,
        PGC_SUSET,
        PGC_S_OVERRIDE,
    );

    /* Load the subscription into persistent memory context. */
    ApplyContext = AllocSetContextCreate(
        crate::utils::mmgr::mcxt::TopMemoryContext as MemoryContext,
        b"ApplyContext\0".as_ptr() as *const c_char,
        0, 8192, 8192 * 1024,
    );
    StartTransactionCommand();
    oldctx = MemoryContextSwitchTo(ApplyContext);

    /*
     * Lock the subscription to prevent it from being concurrently dropped,
     * then re-verify its existence.
     */
    LockSharedObject(SubscriptionRelationId, (*MyLogicalRepWorker).subid, 0, AccessShareLock);
    MySubscription = GetSubscription((*MyLogicalRepWorker).subid, true);
    if MySubscription.is_null() {
        ereport!(
            LOG,
            (errmsg!(
                "logical replication worker for subscription %u will not start because the subscription was removed during startup",
                (*MyLogicalRepWorker).subid
            ))
        );

        /* Ensure we remove no-longer-useful entry for worker's start time */
        if am_leader_apply_worker() {
            ApplyLauncherForgetWorkerStartTime((*MyLogicalRepWorker).subid);
        }

        proc_exit(0);
    }

    MySubscriptionValid = true;
    MemoryContextSwitchTo(oldctx);

    if !subscription_enabled(MySubscription) {
        ereport!(
            LOG,
            (errmsg!(
                "logical replication worker for subscription \"%s\" will not start because the subscription was disabled during startup",
                subscription_name(MySubscription)
            ))
        );

        apply_worker_exit();
    }

    /* Setup synchronous commit according to the user's wishes */
    SetConfigOption(
        b"synchronous_commit\0".as_ptr() as *const c_char,
        subscription_synccommit(MySubscription),
        PGC_BACKEND,
        PGC_S_OVERRIDE,
    );

    /*
     * Keep us informed about subscription or role changes. Note that the
     * role's superuser privilege can be revoked.
     */
    CacheRegisterSyscacheCallback(SUBSCRIPTIONOID, subscription_change_cb, 0);
    CacheRegisterSyscacheCallback(AUTHOID, subscription_change_cb, 0);

    if am_tablesync_worker() {
        ereport!(
            LOG,
            (errmsg!(
                "logical replication table synchronization worker for subscription \"%s\", table \"%s\" has started",
                subscription_name(MySubscription),
                get_rel_name((*MyLogicalRepWorker).relid)
            ))
        );
    } else {
        ereport!(
            LOG,
            (errmsg!(
                "logical replication apply worker for subscription \"%s\" has started",
                subscription_name(MySubscription)
            ))
        );
    }

    CommitTransactionCommand();

    /*
     * Register a callback to reset the origin state before aborting any
     * pending transaction during shutdown (see ShutdownPostgres()).
     */
    before_shmem_exit(replorigin_reset, 0);
}

/*
 * Reset the origin state.
 */
unsafe extern "C" fn replorigin_reset(_code: c_int, _arg: Datum) {
    replorigin_session_origin = InvalidRepOriginId;
    replorigin_session_origin_lsn = InvalidXLogRecPtr;
    replorigin_session_origin_timestamp = 0;
}

/* Common function to setup the leader apply or tablesync worker. */
pub unsafe fn SetupApplyOrSyncWorker(worker_slot: c_int) {
    /* Attach to slot */
    logicalrep_worker_attach(worker_slot);

    // Assert(am_tablesync_worker() || am_leader_apply_worker());

    /* Setup signal handling */
    pqsignal(1 /* SIGHUP */, SignalHandlerForConfigReload);
    pqsignal(15 /* SIGTERM */, die);
    BackgroundWorkerUnblockSignals();

    /*
     * We don't currently need any ResourceOwner in a walreceiver process.
     */

    /* Initialise stats to a sanish value */
    let now = GetCurrentTimestamp();
    (*MyLogicalRepWorker).last_send_time = now;
    (*MyLogicalRepWorker).last_recv_time = now;
    (*MyLogicalRepWorker).reply_time = now;

    /* Load the libpq-specific functions */
    load_file(b"libpqwalreceiver\0".as_ptr() as *const c_char, false);

    InitializeLogRepWorker();

    /* Connect to the origin and start the replication. */
    elog!(
        DEBUG1,
        "connecting to publisher using connection string \"%s\"",
        subscription_conninfo(MySubscription)
    );

    /*
     * Setup callback for syscache so that we know when something changes in
     * the subscription relation state.
     */
    CacheRegisterSyscacheCallback(SUBSCRIPTIONRELMAP, invalidate_syncing_table_states, 0);
}

/* Logical Replication Apply worker entry point */
pub unsafe extern "C" fn ApplyWorkerMain(main_arg: Datum) {
    let worker_slot = DatumGetInt32(main_arg);

    InitializingApplyWorker = true;

    SetupApplyOrSyncWorker(worker_slot);

    InitializingApplyWorker = false;

    run_apply_worker();

    proc_exit(0);
}

/*
 * After error recovery, disable the subscription in a new transaction
 * and exit cleanly.
 */
pub unsafe fn DisableSubscriptionAndExit() {
    /*
     * Emit the error message, and recover from the error state to an idle
     * state
     */
    HOLD_INTERRUPTS();

    EmitErrorReport();
    AbortOutOfAnyTransaction();
    FlushErrorState();

    RESUME_INTERRUPTS();

    /* Report the worker failed during either table synchronization or apply */
    pgstat_report_subscription_error(
        (*MyLogicalRepWorker).subid,
        !am_tablesync_worker(),
    );

    /* Disable the subscription */
    StartTransactionCommand();

    /*
     * Updating pg_subscription might involve TOAST table access, so ensure we
     * have a valid snapshot.
     */
    PushActiveSnapshot(GetTransactionSnapshot());

    DisableSubscription(subscription_oid(MySubscription));
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Ensure we remove no-longer-useful entry for worker's start time */
    if am_leader_apply_worker() {
        ApplyLauncherForgetWorkerStartTime((*MyLogicalRepWorker).subid);
    }

    /* Notify the subscription has been disabled and exit */
    ereport!(
        LOG,
        (errmsg!(
            "subscription \"%s\" has been disabled because of an error",
            subscription_name(MySubscription)
        ))
    );

    proc_exit(0);
}

/*
 * Is current process a logical replication worker?
 */
pub unsafe fn IsLogicalWorker() -> bool {
    !MyLogicalRepWorker.is_null()
}

/*
 * Is current process a logical replication parallel apply worker?
 */
pub unsafe fn IsLogicalParallelApplyWorker() -> bool {
    IsLogicalWorker() && am_parallel_apply_worker()
}

/*
 * Start skipping changes of the transaction if the given LSN matches the
 * LSN specified by subscription's skiplsn.
 */
unsafe fn maybe_start_skipping_changes(finish_lsn: XLogRecPtr) {
    // Assert(!is_skipping_changes());
    // Assert(!in_remote_transaction);
    // Assert(!in_streamed_transaction);

    /*
     * Quick return if it's not requested to skip this transaction.
     */
    let skiplsn = subscription_skiplsn(MySubscription);
    if XLogRecPtrIsInvalid(skiplsn) || skiplsn != finish_lsn {
        return;
    }

    /* Start skipping all changes of this transaction */
    skip_xact_finish_lsn = finish_lsn;

    ereport!(
        LOG,
        (errmsg!(
            "logical replication starts skipping transaction at LSN %X/%X",
            (skip_xact_finish_lsn >> 32) as c_int,
            (skip_xact_finish_lsn & 0xFFFFFFFF) as c_int
        ))
    );
}

/*
 * Stop skipping changes by resetting skip_xact_finish_lsn if enabled.
 */
unsafe fn stop_skipping_changes() {
    if !is_skipping_changes() {
        return;
    }

    ereport!(
        LOG,
        (errmsg!(
            "logical replication completed skipping transaction at LSN %X/%X",
            (skip_xact_finish_lsn >> 32) as c_int,
            (skip_xact_finish_lsn & 0xFFFFFFFF) as c_int
        ))
    );

    /* Stop skipping changes */
    skip_xact_finish_lsn = InvalidXLogRecPtr;
}

/*
 * Clear subskiplsn of pg_subscription catalog.
 */
unsafe fn clear_subscription_skip_lsn(finish_lsn: XLogRecPtr) {
    let rel: *mut c_void;
    let subform: Form_pg_subscription;
    let mut tup: HeapTuple;
    let myskiplsn: XLogRecPtr = subscription_skiplsn(MySubscription);
    let mut started_tx = false;

    if XLogRecPtrIsInvalid(myskiplsn) || am_parallel_apply_worker() {
        return;
    }

    if !IsTransactionState() {
        StartTransactionCommand();
        started_tx = true;
    }

    /*
     * Updating pg_subscription might involve TOAST table access, so ensure we
     * have a valid snapshot.
     */
    PushActiveSnapshot(GetTransactionSnapshot());

    /*
     * Protect subskiplsn of pg_subscription from being concurrently updated
     * while clearing it.
     */
    LockSharedObject(SubscriptionRelationId, subscription_oid(MySubscription), 0, AccessShareLock);

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    /* Fetch the existing tuple. */
    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subscription_oid(MySubscription)));

    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "subscription \"%s\" does not exist", subscription_name(MySubscription));
    }

    subform = GETSTRUCT(tup) as Form_pg_subscription;

    /*
     * Clear the subskiplsn.
     */
    // if (subform->subskiplsn == myskiplsn)
    // We access via accessor -- TODO(pg-port): use real Form_pg_subscription
    if get_form_pg_subscription_subskiplsn(subform) == myskiplsn {
        let mut values: [Datum; Natts_pg_subscription] = [0; Natts_pg_subscription];
        let mut nulls: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];
        let mut replaces: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];

        /* reset subskiplsn */
        values[Anum_pg_subscription_subskiplsn - 1] = LSNGetDatum(InvalidXLogRecPtr);
        replaces[Anum_pg_subscription_subskiplsn - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr(),
                                replaces.as_mut_ptr());
        CatalogTupleUpdate(rel, &raw mut (*(tup as *mut crate::access::htup_details::HeapTupleData)).t_self as *mut _ as *mut c_void, tup);

        if myskiplsn != finish_lsn {
            ereport!(
                WARNING,
                (
                    errmsg!("skip-LSN of subscription \"%s\" cleared", subscription_name(MySubscription)),
                    errdetail!(
                        "Remote transaction's finish WAL location (LSN) %X/%X did not match skip-LSN %X/%X.",
                        (finish_lsn >> 32) as c_int,
                        (finish_lsn & 0xFFFFFFFF) as c_int,
                        (myskiplsn >> 32) as c_int,
                        (myskiplsn & 0xFFFFFFFF) as c_int
                    )
                )
            );
        }
    }

    heap_freetuple(tup);
    table_close(rel, NoLock);

    PopActiveSnapshot();

    if started_tx {
        CommitTransactionCommand();
    }
}

// Accessor for HeapTupleData.t_self field -- TODO(pg-port)
#[repr(C)]
struct HeapTupleData {
    t_len: uint32,
    t_self: [u8; 6], // ItemPointerData
    t_tableOid: Oid,
    t_data: *mut c_void,
}

extern "C" {
    fn get_form_pg_subscription_subskiplsn(form: Form_pg_subscription) -> XLogRecPtr;
}

/* Error callback to give more context info about the change being applied */
pub unsafe extern "C" fn apply_error_callback_local(_arg: *mut c_void) {
    let errarg = &apply_error_callback_arg;

    if apply_error_callback_arg.command == 0 {
        return;
    }

    // Assert(errarg->origin_name);

    if errarg.rel.is_null() {
        if !TransactionIdIsValid(errarg.remote_xid) {
            errcontext(
                b"processing remote data for replication origin \"%s\" during message type \"%s\"\0"
                    .as_ptr() as *const c_char,
                errarg.origin_name,
                logicalrep_message_type(errarg.command),
            );
        } else if XLogRecPtrIsInvalid(errarg.finish_lsn) {
            errcontext(
                b"processing remote data for replication origin \"%s\" during message type \"%s\" in transaction %u\0"
                    .as_ptr() as *const c_char,
                errarg.origin_name,
                logicalrep_message_type(errarg.command),
                errarg.remote_xid,
            );
        } else {
            errcontext(
                b"processing remote data for replication origin \"%s\" during message type \"%s\" in transaction %u, finished at %X/%X\0"
                    .as_ptr() as *const c_char,
                errarg.origin_name,
                logicalrep_message_type(errarg.command),
                errarg.remote_xid,
                (errarg.finish_lsn >> 32) as c_int,
                (errarg.finish_lsn & 0xFFFFFFFF) as c_int,
            );
        }
    } else {
        let remoterel = logicalrep_rel_mapentry_remoterel(errarg.rel);
        if errarg.remote_attnum < 0 {
            if XLogRecPtrIsInvalid(errarg.finish_lsn) {
                errcontext(
                    b"processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" in transaction %u\0"
                        .as_ptr() as *const c_char,
                    errarg.origin_name,
                    logicalrep_message_type(errarg.command),
                    logicalrep_reldata_nspname(remoterel),
                    logicalrep_reldata_relname(remoterel),
                    errarg.remote_xid,
                );
            } else {
                errcontext(
                    b"processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" in transaction %u, finished at %X/%X\0"
                        .as_ptr() as *const c_char,
                    errarg.origin_name,
                    logicalrep_message_type(errarg.command),
                    logicalrep_reldata_nspname(remoterel),
                    logicalrep_reldata_relname(remoterel),
                    errarg.remote_xid,
                    (errarg.finish_lsn >> 32) as c_int,
                    (errarg.finish_lsn & 0xFFFFFFFF) as c_int,
                );
            }
        } else {
            if XLogRecPtrIsInvalid(errarg.finish_lsn) {
                errcontext(
                    b"processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" column \"%s\" in transaction %u\0"
                        .as_ptr() as *const c_char,
                    errarg.origin_name,
                    logicalrep_message_type(errarg.command),
                    logicalrep_reldata_nspname(remoterel),
                    logicalrep_reldata_relname(remoterel),
                    logicalrep_reldata_attnames(remoterel, errarg.remote_attnum),
                    errarg.remote_xid,
                );
            } else {
                errcontext(
                    b"processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" column \"%s\" in transaction %u, finished at %X/%X\0"
                        .as_ptr() as *const c_char,
                    errarg.origin_name,
                    logicalrep_message_type(errarg.command),
                    logicalrep_reldata_nspname(remoterel),
                    logicalrep_reldata_relname(remoterel),
                    logicalrep_reldata_attnames(remoterel, errarg.remote_attnum),
                    errarg.remote_xid,
                    (errarg.finish_lsn >> 32) as c_int,
                    (errarg.finish_lsn & 0xFFFFFFFF) as c_int,
                );
            }
        }
    }
}

/* Set transaction information of apply error callback */
#[inline(always)]
unsafe fn set_apply_error_context_xact(xid: TransactionId, lsn: XLogRecPtr) {
    apply_error_callback_arg.remote_xid = xid;
    apply_error_callback_arg.finish_lsn = lsn;
}

/* Reset all information of apply error callback */
#[inline(always)]
unsafe fn reset_apply_error_context_info() {
    apply_error_callback_arg.command = 0;
    apply_error_callback_arg.rel = null_mut();
    apply_error_callback_arg.remote_attnum = -1;
    set_apply_error_context_xact(InvalidTransactionId, InvalidXLogRecPtr);
}

/*
 * Request wakeup of the workers for the given subscription OID
 * at commit of the current transaction.
 */
pub unsafe fn LogicalRepWorkersWakeupAtCommit(subid: Oid) {
    let oldcxt: MemoryContext;

    oldcxt = MemoryContextSwitchTo(TopTransactionContext());
    on_commit_wakeup_workers_subids =
        list_append_unique_oid(on_commit_wakeup_workers_subids, subid);
    MemoryContextSwitchTo(oldcxt);
}

/*
 * Wake up the workers of any subscriptions that were changed in this xact.
 */
pub unsafe fn AtEOXact_LogicalRepWorkers(is_commit: bool) {
    if is_commit && !on_commit_wakeup_workers_subids.is_null() {
        LWLockAcquire(LogicalRepWorkerLock_ptr(), LW_SHARED);

        let mut lc = foreach_begin(on_commit_wakeup_workers_subids);
        while !lc.is_null() {
            let subid: Oid = lfirst_oid(lc);
            let workers: *mut List;
            let mut lc2: *mut ListCell;

            workers = logicalrep_workers_find(subid, true, false);
            lc2 = foreach_begin(workers);
            while !lc2.is_null() {
                let worker = lfirst(lc2) as *mut LogicalRepWorker;
                crate::replication::worker_internal::logicalrep_worker_wakeup_ptr(worker);
                lc2 = foreach_next(lc2);
            }

            lc = foreach_next(lc);
        }
        LWLockRelease(LogicalRepWorkerLock_ptr());
    }

    /* The List storage will be reclaimed automatically in xact cleanup. */
    on_commit_wakeup_workers_subids = NIL;
}

/*
 * Allocate the origin name in long-lived context for error context message.
 */
pub unsafe fn set_apply_error_context_origin(originname: *mut c_char) {
    apply_error_callback_arg.origin_name =
        MemoryContextStrdup(ApplyContext, originname);
}

/*
 * Return the action to be taken for the given transaction.
 *
 * *winfo is assigned to the destination parallel worker info when the leader
 * apply worker has to pass all the transaction's changes to the parallel
 * apply worker.
 */
unsafe fn get_transaction_apply_action(
    xid: TransactionId,
    winfo: *mut *mut ParallelApplyWorkerInfo,
) -> TransApplyAction {
    *winfo = null_mut();

    if am_parallel_apply_worker() {
        return TRANS_PARALLEL_APPLY;
    }

    /*
     * If we are processing this transaction using a parallel apply worker
     * then either we send the changes to the parallel worker or if the worker
     * is busy then serialize the changes to the file.
     */
    *winfo = pa_find_worker(xid);

    if !(*winfo).is_null() && parallel_apply_winfo_serialize_changes(*winfo) {
        return TRANS_LEADER_PARTIAL_SERIALIZE;
    } else if !(*winfo).is_null() {
        return TRANS_LEADER_SEND_TO_PARALLEL;
    }

    /*
     * If there is no parallel worker involved to process this transaction
     * then we either directly apply the change or serialize it to a file.
     */
    if in_streamed_transaction {
        TRANS_LEADER_SERIALIZE
    } else {
        TRANS_LEADER_APPLY
    }
}

// ---------------------------------------------------------------------------
// Worker type predicates (wrappers around worker_internal.h inlines)
// ---------------------------------------------------------------------------

pub unsafe fn am_tablesync_worker() -> bool {
    !MyLogicalRepWorker.is_null()
        && (*MyLogicalRepWorker).type_ == WORKERTYPE_TABLESYNC
}

pub unsafe fn am_parallel_apply_worker() -> bool {
    !MyLogicalRepWorker.is_null()
        && (*MyLogicalRepWorker).type_ == WORKERTYPE_PARALLEL_APPLY
}

pub unsafe fn am_leader_apply_worker() -> bool {
    !MyLogicalRepWorker.is_null()
        && (*MyLogicalRepWorker).type_ == WORKERTYPE_APPLY
}

/*-------------------------------------------------------------------------
 *
 * xact.rs
 *    top level transaction system support routines
 *
 * See src/backend/access/transam/README for more information.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/access/transam/xact.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types, dead_code, unused_variables, unused_mut)]

use std::ptr;

/* ---- stub type aliases for unported dependencies ---- */

pub type TransactionId = u32;
pub type FullTransactionId = FullTransactionIdData;
pub type SubTransactionId = u32;
pub type CommandId = u32;
pub type Oid = u32;
pub type TimestampTz = i64;
pub type XLogRecPtr = u64;
pub type RepOriginId = u16;
pub type Size = usize;
pub type LocalTransactionId = u32;
pub type ProcNumber = i32;

/* TODO(pg-port): real HeapTuple */
pub type HeapTupleData = u8;
pub type HeapTuple = *mut HeapTupleData;

/* TODO(pg-port): real MemoryContext */
pub type MemoryContextData = u8;
pub type MemoryContext = *mut MemoryContextData;

/* TODO(pg-port): real ResourceOwner */
pub type ResourceOwnerData = u8;
pub type ResourceOwner = *mut ResourceOwnerData;

/* TODO(pg-port): real GlobalTransaction */
pub type GlobalTransactionData = u8;
pub type GlobalTransaction = *mut GlobalTransactionData;

/* TODO(pg-port): real XLogReaderState */
pub type XLogReaderState = u8;

/* TODO(pg-port): real RelFileLocator */
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: u32,
}

/* TODO(pg-port): real SharedInvalidationMessage */
#[derive(Clone, Copy)]
pub struct SharedInvalidationMessage {
    pub id: i8,
}

/* TODO(pg-port): real StringInfoData */
pub struct StringInfoData {
    pub data: *mut std::os::raw::c_char,
    pub len: i32,
    pub maxlen: i32,
    pub cursor: i32,
}

/* TODO(pg-port): xl_xact_* types */
#[derive(Clone, Copy, Default)]
pub struct xl_xact_assignment {
    pub xtop: TransactionId,
    pub nsubxacts: i32,
    pub xsub: [TransactionId; 0],
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_commit {
    pub xact_time: TimestampTz,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_abort {
    pub xact_time: TimestampTz,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_xinfo {
    pub xinfo: u32,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_dbinfo {
    pub dbId: Oid,
    pub tsId: Oid,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_subxacts {
    pub nsubxacts: i32,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_relfilelocators {
    pub nrels: i32,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_stats_item {
    pub kind: u8,
    pub dboid: Oid,
    pub objoid: Oid,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_stats_items {
    pub nitems: i32,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_invals {
    pub nmsgs: i32,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_twophase {
    pub xid: TransactionId,
}

#[derive(Clone, Copy, Default)]
pub struct xl_xact_origin {
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/* TODO(pg-port): parsed commit/abort */
pub struct xl_xact_parsed_commit {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub dbId: Oid,
    pub tsId: Oid,
    pub nsubxacts: i32,
    pub subxacts: *mut TransactionId,
    pub nrels: i32,
    pub xlocators: *mut RelFileLocator,
    pub nstats: i32,
    pub stats: *mut xl_xact_stats_item,
    pub nmsgs: i32,
    pub msgs: *mut SharedInvalidationMessage,
    pub twophase_xid: TransactionId,
    pub twophase_gid: *const std::os::raw::c_char,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

pub struct xl_xact_parsed_abort {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub nsubxacts: i32,
    pub subxacts: *mut TransactionId,
    pub nrels: i32,
    pub xlocators: *mut RelFileLocator,
    pub nstats: i32,
    pub stats: *mut xl_xact_stats_item,
    pub twophase_xid: TransactionId,
    pub twophase_gid: *const std::os::raw::c_char,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

#[derive(Clone, Copy, Default)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

#[derive(Clone, Copy, Default)]
pub struct FullTransactionIdData {
    pub value: u64,
}

/* TODO(pg-port): SavedTransactionCharacteristics (public, defined in xact.h) */
#[derive(Clone, Copy, Default)]
pub struct SavedTransactionCharacteristics {
    pub save_XactIsoLevel: i32,
    pub save_XactReadOnly: bool,
    pub save_XactDeferrable: bool,
}

/* ---- constants ---- */

pub const XACT_READ_COMMITTED: i32 = 1;
pub const SYNCHRONOUS_COMMIT_ON: i32 = 2;
pub const SYNCHRONOUS_COMMIT_OFF: i32 = 0;
pub const SYNCHRONOUS_COMMIT_REMOTE_APPLY: i32 = 4;
pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const InvalidSubTransactionId: SubTransactionId = 0;
pub const TopSubTransactionId: SubTransactionId = 1;
pub const FirstCommandId: CommandId = 0;
pub const InvalidCommandId: CommandId = u32::MAX;
pub const InvalidRepOriginId: RepOriginId = 0;
pub const DoNotReplicateId: RepOriginId = u16::MAX;
pub const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;
pub const InvalidLocalTransactionId: LocalTransactionId = 0;
pub const DELAY_CHKPT_START: u32 = 0x01;
pub const TRANSACTION_TIMEOUT: i32 = 4;
pub const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: i32 = 0x0001;
pub const XACT_FLAGS_NEEDIMMEDIATECOMMIT: i32 = 0x0002;
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: i32 = 0x0004;
pub const RESOURCE_RELEASE_BEFORE_LOCKS: i32 = 1;
pub const RESOURCE_RELEASE_LOCKS: i32 = 2;
pub const RESOURCE_RELEASE_AFTER_LOCKS: i32 = 3;
pub const RM_XACT_ID: u8 = 1;
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub const XLOG_XACT_INVALIDATIONS: u8 = 0x60;
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;
pub const XLOG_XACT_OPMASK: u8 = 0x70;
pub const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
pub const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
pub const XACT_XINFO_HAS_GID: u32 = 1 << 5;
pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1 << 6;
pub const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 7;
pub const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;
pub const XACT_COMPLETION_UPDATE_RELCACHE_FILE: u32 = 1 << 30;
pub const XACT_COMPLETION_FORCE_SYNC_COMMIT: u32 = 1 << 29;
pub const XACT_COMPLETION_APPLY_FEEDBACK: u32 = 1 << 28;
pub const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;
pub const XLOG_INCLUDE_ORIGIN: u8 = 0x01;
pub const MinSizeOfXactAssignment: usize = core::mem::size_of::<xl_xact_assignment>();
pub const MinSizeOfXactSubxacts: usize = core::mem::size_of::<xl_xact_subxacts>();
pub const MinSizeOfXactRelfileLocators: usize = core::mem::size_of::<xl_xact_relfilelocators>();
pub const MinSizeOfXactStatsItems: usize = core::mem::size_of::<xl_xact_stats_items>();
pub const MinSizeOfXactInvals: usize = core::mem::size_of::<xl_xact_invals>();
pub const MinSizeOfXactAbort: usize = core::mem::size_of::<xl_xact_abort>();
pub const STANDBY_DISABLED: i32 = 0;
pub const STANDBY_INITIALIZED: i32 = 1;
pub const DEBUG5: i32 = 10;
pub const WARNING: i32 = 19;
pub const ERROR: i32 = 20;
pub const FATAL: i32 = 21;
pub const PANIC: i32 = 22;
pub const MaxAllocSize: usize = 0x3fffffff;

/* ---- XactEvent / SubXactEvent ---- */

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum XactEvent {
    XACT_EVENT_COMMIT,
    XACT_EVENT_PARALLEL_COMMIT,
    XACT_EVENT_ABORT,
    XACT_EVENT_PARALLEL_ABORT,
    XACT_EVENT_PREPARE,
    XACT_EVENT_PRE_COMMIT,
    XACT_EVENT_PARALLEL_PRE_COMMIT,
    XACT_EVENT_PRE_PREPARE,
}
pub use XactEvent::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SubXactEvent {
    SUBXACT_EVENT_START_SUB,
    SUBXACT_EVENT_COMMIT_SUB,
    SUBXACT_EVENT_ABORT_SUB,
    SUBXACT_EVENT_PRE_COMMIT_SUB,
}
pub use SubXactEvent::*;

/* ---- callback types ---- */
pub type XactCallback = unsafe fn(event: XactEvent, arg: *mut std::ffi::c_void);
pub type SubXactCallback = unsafe fn(event: SubXactEvent, mySubid: SubTransactionId,
                                     parentSubid: SubTransactionId, arg: *mut std::ffi::c_void);

/*
 * User-tweakable parameters
 */
pub static mut DefaultXactIsoLevel: i32 = XACT_READ_COMMITTED;
pub static mut XactIsoLevel: i32 = XACT_READ_COMMITTED;

pub static mut DefaultXactReadOnly: bool = false;
pub static mut XactReadOnly: bool = false;

pub static mut DefaultXactDeferrable: bool = false;
pub static mut XactDeferrable: bool = false;

pub static mut synchronous_commit: i32 = SYNCHRONOUS_COMMIT_ON;

/*
 * CheckXidAlive is a xid value pointing to a possibly ongoing (sub)
 * transaction.  Currently, it is used in logical decoding.
 */
pub static mut CheckXidAlive: TransactionId = InvalidTransactionId;
pub static mut bsysscan: bool = false;

/*
 * XactTopFullTransactionId stores the XID of our toplevel transaction, which
 * will be the same as TopTransactionStateData.fullTransactionId in an
 * ordinary backend.
 *
 * nParallelCurrentXids will be 0 and ParallelCurrentXids NULL in an ordinary
 * backend.
 */
static mut XactTopFullTransactionId: FullTransactionId = FullTransactionId { value: 0 };
static mut nParallelCurrentXids: i32 = 0;
static mut ParallelCurrentXids: *mut TransactionId = ptr::null_mut();

/*
 * Miscellaneous flag bits to record events which occur on the top level
 * transaction.
 */
pub static mut MyXactFlags: i32 = 0;

/*
 * transaction states - transaction state from server perspective
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TransState {
    TRANS_DEFAULT,      /* idle */
    TRANS_START,        /* transaction starting */
    TRANS_INPROGRESS,   /* inside a valid transaction */
    TRANS_COMMIT,       /* commit in progress */
    TRANS_ABORT,        /* abort in progress */
    TRANS_PREPARE,      /* prepare in progress */
}
use TransState::*;

/*
 * transaction block states - transaction state of client queries
 *
 * Note: the subtransaction states are used only for non-topmost
 * transactions; the others appear only in the topmost transaction.
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TBlockState {
    /* not-in-transaction-block states */
    TBLOCK_DEFAULT,             /* idle */
    TBLOCK_STARTED,             /* running single-query transaction */

    /* transaction block states */
    TBLOCK_BEGIN,               /* starting transaction block */
    TBLOCK_INPROGRESS,          /* live transaction */
    TBLOCK_IMPLICIT_INPROGRESS, /* live transaction after implicit BEGIN */
    TBLOCK_PARALLEL_INPROGRESS, /* live transaction inside parallel worker */
    TBLOCK_END,                 /* COMMIT received */
    TBLOCK_ABORT,               /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,           /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING,       /* live xact, ROLLBACK received */
    TBLOCK_PREPARE,             /* live xact, PREPARE received */

    /* subtransaction states */
    TBLOCK_SUBBEGIN,            /* starting a subtransaction */
    TBLOCK_SUBINPROGRESS,       /* live subtransaction */
    TBLOCK_SUBRELEASE,          /* RELEASE received */
    TBLOCK_SUBCOMMIT,           /* COMMIT received while TBLOCK_SUBINPROGRESS */
    TBLOCK_SUBABORT,            /* failed subxact, awaiting ROLLBACK */
    TBLOCK_SUBABORT_END,        /* failed subxact, ROLLBACK received */
    TBLOCK_SUBABORT_PENDING,    /* live subxact, ROLLBACK received */
    TBLOCK_SUBRESTART,          /* live subxact, ROLLBACK TO received */
    TBLOCK_SUBABORT_RESTART,    /* failed subxact, ROLLBACK TO received */
}
pub use TBlockState::*;

/*
 * transaction state structure
 *
 * Note: parallelModeLevel counts the number of unmatched EnterParallelMode
 * calls done at this transaction level.  parallelChildXact is true if any
 * upper transaction level has nonzero parallelModeLevel.
 */
struct TransactionStateData {
    fullTransactionId: FullTransactionId,           /* my FullTransactionId */
    subTransactionId: SubTransactionId,             /* my subxact ID */
    name: *mut std::os::raw::c_char,                /* savepoint name, if any */
    savepointLevel: i32,                            /* savepoint level */
    state: TransState,                              /* low-level state */
    blockState: TBlockState,                        /* high-level state */
    nestingLevel: i32,                              /* transaction nesting depth */
    gucNestLevel: i32,                              /* GUC context nesting depth */
    curTransactionContext: MemoryContext,           /* my xact-lifetime context */
    curTransactionOwner: ResourceOwner,             /* my query resources */
    priorContext: MemoryContext,                    /* CurrentMemoryContext before xact started */
    childXids: *mut TransactionId,                  /* subcommitted child XIDs, in XID order */
    nChildXids: i32,                                /* # of subcommitted child XIDs */
    maxChildXids: i32,                              /* allocated size of childXids[] */
    prevUser: Oid,                                  /* previous CurrentUserId setting */
    prevSecContext: i32,                            /* previous SecurityRestrictionContext */
    prevXactReadOnly: bool,                         /* entry-time xact r/o state */
    startedInRecovery: bool,                        /* did we start in recovery? */
    didLogXid: bool,                                /* has xid been included in WAL record? */
    parallelModeLevel: i32,                         /* Enter/ExitParallelMode counter */
    parallelChildXact: bool,                        /* is any parent transaction parallel? */
    chain: bool,                                    /* start a new block after this one */
    topXidLogged: bool,                             /* for a subxact: is top-level XID logged? */
    parent: *mut TransactionStateData,              /* back link to parent */
}

type TransactionState = *mut TransactionStateData;

/*
 * Serialized representation used to transmit transaction state to parallel
 * workers through shared memory.
 */
#[repr(C)]
pub struct SerializedTransactionState {
    pub xactIsoLevel: i32,
    pub xactDeferrable: bool,
    pub topFullTransactionId: FullTransactionId,
    pub currentFullTransactionId: FullTransactionId,
    pub currentCommandId: CommandId,
    pub nParallelCurrentXids: i32,
    /* parallelCurrentXids[FLEXIBLE_ARRAY_MEMBER] follows */
}

/* The size of SerializedTransactionState, not including the final array. */
pub const SerializedTransactionStateHeaderSize: usize =
    core::mem::offset_of!(SerializedTransactionState, nParallelCurrentXids)
    + core::mem::size_of::<i32>();

/*
 * CurrentTransactionState always points to the current transaction state
 * block.
 */
static mut TopTransactionStateData: TransactionStateData = TransactionStateData {
    fullTransactionId: FullTransactionId { value: 0 },
    subTransactionId: 0,
    name: ptr::null_mut(),
    savepointLevel: 0,
    state: TRANS_DEFAULT,
    blockState: TBLOCK_DEFAULT,
    nestingLevel: 0,
    gucNestLevel: 0,
    curTransactionContext: ptr::null_mut(),
    curTransactionOwner: ptr::null_mut(),
    priorContext: ptr::null_mut(),
    childXids: ptr::null_mut(),
    nChildXids: 0,
    maxChildXids: 0,
    prevUser: 0,
    prevSecContext: 0,
    prevXactReadOnly: false,
    startedInRecovery: false,
    didLogXid: false,
    parallelModeLevel: 0,
    parallelChildXact: false,
    chain: false,
    topXidLogged: false,
    parent: ptr::null_mut(),
};

/*
 * unreportedXids holds XIDs of all subtransactions that have not yet been
 * reported in an XLOG_XACT_ASSIGNMENT record.
 */
static mut nUnreportedXids: i32 = 0;
static mut unreportedXids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS] =
    [0u32; PGPROC_MAX_CACHED_SUBXIDS];

static mut CurrentTransactionState: TransactionState =
    ptr::addr_of_mut!(TopTransactionStateData);

/*
 * The subtransaction ID and command ID assignment counters are global
 * to a whole transaction, so we do not keep them in the state stack.
 */
static mut currentSubTransactionId: SubTransactionId = 0;
static mut currentCommandId: CommandId = 0;
static mut currentCommandIdUsed: bool = false;

/*
 * xactStartTimestamp is the value of transaction_timestamp().
 * stmtStartTimestamp is the value of statement_timestamp().
 * xactStopTimestamp is the time at which we log a commit / abort WAL record.
 */
static mut xactStartTimestamp: TimestampTz = 0;
static mut stmtStartTimestamp: TimestampTz = 0;
static mut xactStopTimestamp: TimestampTz = 0;

/*
 * GID to be used for preparing the current transaction.
 */
static mut prepareGID: *mut std::os::raw::c_char = ptr::null_mut();

/*
 * Some commands want to force synchronous commit.
 */
static mut forceSyncCommit: bool = false;

/* Flag for logging statements in a transaction. */
pub static mut xact_is_sampled: bool = false;

/*
 * Private context for transaction-abort work --- we reserve space for this
 * at startup to ensure that AbortTransaction and AbortSubTransaction can work
 * when we've run out of memory.
 */
static mut TransactionAbortContext: MemoryContext = ptr::null_mut();

/*
 * List of add-on start- and end-of-xact callbacks
 */
struct XactCallbackItem {
    next: *mut XactCallbackItem,
    callback: XactCallback,
    arg: *mut std::ffi::c_void,
}

static mut Xact_callbacks: *mut XactCallbackItem = ptr::null_mut();

/*
 * List of add-on start- and end-of-subxact callbacks
 */
struct SubXactCallbackItem {
    next: *mut SubXactCallbackItem,
    callback: SubXactCallback,
    arg: *mut std::ffi::c_void,
}

static mut SubXact_callbacks: *mut SubXactCallbackItem = ptr::null_mut();

/* ----------------------------------------------------------------
 * stub functions for unported dependencies (TODO(pg-port))
 * ---------------------------------------------------------------- */

unsafe fn FullTransactionIdIsValid(fxid: FullTransactionId) -> bool {
    /* TODO(pg-port): TransactionIdIsValid(XidFromFullTransactionId(fxid)) */
    fxid.value != 0
}

unsafe fn XidFromFullTransactionId(fxid: FullTransactionId) -> TransactionId {
    /* TODO(pg-port): extract low 32 bits */
    fxid.value as TransactionId
}

unsafe fn InvalidFullTransactionId() -> FullTransactionId {
    FullTransactionId { value: 0 }
}

unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

unsafe fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= 3 /* FirstNormalTransactionId */
}

unsafe fn TransactionIdEquals(a: TransactionId, b: TransactionId) -> bool { a == b }

unsafe fn TransactionIdPrecedes(_a: TransactionId, _b: TransactionId) -> bool {
    /* TODO(pg-port): subtract-and-compare */
    false
}

unsafe fn TransactionIdLatest(main_xid: TransactionId,
                               nxids: i32, xids: *mut TransactionId) -> TransactionId {
    /* TODO(pg-port): real impl */
    let mut result = main_xid;
    for i in 0..nxids as usize {
        let x = *xids.add(i);
        if x > result { result = x; }
    }
    result
}

unsafe fn AssignTransactionId(s: TransactionState) {
    /* TODO(pg-port): full implementation below */
    assign_transaction_id_impl(s);
}

/* placeholder for external calls - real stubs below */
unsafe fn GetNewTransactionId(_is_sub_xact: bool) -> FullTransactionId {
    core::mem::transmute(crate::access::transam::varsup::GetNewTransactionId(_is_sub_xact))
}
unsafe fn SubTransSetParent(_xid: TransactionId, _parent: TransactionId) { /* TODO(pg-port) */ }
unsafe fn RegisterPredicateLockingXid(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn XactLockTableInsert(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn XactLockTableDelete(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn XLogStandbyInfoActive() -> bool { /* TODO(pg-port) */ false }
unsafe fn XLogLogicalInfoActive() -> bool { /* TODO(pg-port) */ false }
unsafe fn XLogBeginInsert() { /* TODO(pg-port) */ }
unsafe fn XLogRegisterData(_data: *const std::ffi::c_void, _len: usize) { /* TODO(pg-port) */ }
unsafe fn XLogInsert(_rmgr: u8, _info: u8) -> XLogRecPtr { /* TODO(pg-port) */ 0 }
unsafe fn XLogSetRecordFlags(_flags: u8) { /* TODO(pg-port) */ }
unsafe fn XLogFlush(_lsn: XLogRecPtr) { /* TODO(pg-port) */ }
unsafe fn XLogSetAsyncXactLSN(_lsn: XLogRecPtr) { /* TODO(pg-port) */ }
unsafe fn XLogResetInsertion() { /* TODO(pg-port) */ }
unsafe fn XLogRequestWalReceiverReply() { /* TODO(pg-port) */ }
pub static mut XactLastRecEnd: XLogRecPtr = 0;
pub static mut XactLastCommitEnd: XLogRecPtr = 0;
unsafe fn IsInParallelMode() -> bool { /* TODO(pg-port) */ false }
unsafe fn IsParallelWorker() -> bool { /* TODO(pg-port) */ false }
unsafe fn ParallelContextActive() -> bool { /* TODO(pg-port) */ false }
unsafe fn RecoveryInProgress() -> bool { /* TODO(pg-port) */ false }
unsafe fn AcceptInvalidationMessages() { /* TODO(pg-port) */ }
unsafe fn AtCCI_RelationMap() { /* TODO(pg-port) */ }
unsafe fn CommandEndInvalidationMessages() { crate::utils::cache::inval::CommandEndInvalidationMessages() }
unsafe fn SnapshotSetCommandId(_cid: CommandId) { crate::utils::time::snapmgr::SnapshotSetCommandId(_cid as _) }
unsafe fn AtStart_GUC() { /* TODO(pg-port) */ }
unsafe fn AtEOXact_GUC(_is_commit: bool, _nestlevel: i32) { /* TODO(pg-port) */ }
unsafe fn NewGUCNestLevel() -> i32 { /* TODO(pg-port) */ 0 }
unsafe fn AtAbort_Portals() { crate::utils::mmgr::portalmem::AtAbort_Portals() }
unsafe fn AtCleanup_Portals() { crate::utils::mmgr::portalmem::AtCleanup_Portals() }
unsafe fn AtSubAbort_Portals(_s: SubTransactionId, _p: SubTransactionId,
                              _o: ResourceOwner, _po: ResourceOwner) { /* TODO(pg-port) */ }
unsafe fn AtSubCleanup_Portals(_s: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtSubCommit_Portals(_s: SubTransactionId, _p: SubTransactionId,
                               _nl: i32, _owner: ResourceOwner) { /* TODO(pg-port) */ }
unsafe fn PreCommit_Portals(_hold: bool) -> bool { crate::utils::mmgr::portalmem::PreCommit_Portals(_hold) }
unsafe fn AfterTriggerBeginXact() { /* TODO(pg-port) */ }
unsafe fn AfterTriggerEndXact(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AfterTriggerFireDeferred() { /* TODO(pg-port) */ }
unsafe fn AfterTriggerBeginSubXact() { /* TODO(pg-port) */ }
unsafe fn AfterTriggerEndSubXact(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_Parallel(_is_commit: bool, _s: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Parallel(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Aio(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Buffers(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_RelationCache(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_RelationCache(_is_commit: bool, _s: SubTransactionId, _p: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_TypeCache() { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_TypeCache() { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Inval(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_Inval(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_MultiXact() { /* TODO(pg-port) */ }
unsafe fn AtEOXact_SMgr() { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Files(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_ComboCid() { /* TODO(pg-port) */ }
unsafe fn AtEOXact_HashTables(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_HashTables(_is_commit: bool, _nl: i32) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_PgStat(_is_commit: bool, _is_parallel: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_PgStat(_is_commit: bool, _nl: i32) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_ApplyLauncher(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_LogicalRepWorkers(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_SPI(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_SPI(_is_commit: bool, _s: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Enum() { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Namespace(_is_commit: bool, _is_parallel: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_Namespace(_is_commit: bool, _s: SubTransactionId, _p: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_RelationMap(_is_commit: bool, _is_parallel: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_on_commit_actions(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_on_commit_actions(_is_commit: bool, _s: SubTransactionId, _p: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_Snapshot(_is_commit: bool, _overwrite_ok: bool) { /* TODO(pg-port) */ }
unsafe fn AtSubCommit_Snapshot(_nl: i32) { /* TODO(pg-port) */ }
unsafe fn AtSubAbort_Snapshot(_nl: i32) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_LargeObject(_is_commit: bool, _s: SubTransactionId, _p: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtEOXact_LargeObject(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn AtEOSubXact_Files(_is_commit: bool, _s: SubTransactionId, _p: SubTransactionId) { /* TODO(pg-port) */ }
unsafe fn AtSubAbort_smgr() { /* TODO(pg-port) */ }
unsafe fn AtSubCommit_smgr() { /* TODO(pg-port) */ }
unsafe fn smgrDoPendingSyncs(_is_commit: bool, _is_parallel: bool) { /* TODO(pg-port) */ }
unsafe fn smgrDoPendingDeletes(_is_commit: bool) { /* TODO(pg-port) */ }
unsafe fn smgrGetPendingDeletes(_is_commit: bool, rels: *mut *mut RelFileLocator) -> i32 {
    /* TODO(pg-port) */
    unsafe { *rels = ptr::null_mut(); }
    0
}
unsafe fn PreCommit_CheckForSerializationFailure() { /* TODO(pg-port) */ }
unsafe fn PreCommit_Notify() { /* TODO(pg-port) */ }
unsafe fn AtCommit_Notify() { /* TODO(pg-port) */ }
unsafe fn AtAbort_Notify() { /* TODO(pg-port) */ }
unsafe fn AtSubCommit_Notify() { /* TODO(pg-port) */ }
unsafe fn AtSubAbort_Notify() { /* TODO(pg-port) */ }
unsafe fn AtPrepare_Notify() { /* TODO(pg-port) */ }
unsafe fn AtPrepare_Locks() { /* TODO(pg-port) */ }
unsafe fn AtPrepare_PredicateLocks() { /* TODO(pg-port) */ }
unsafe fn AtPrepare_PgStat() { /* TODO(pg-port) */ }
unsafe fn AtPrepare_MultiXact() { /* TODO(pg-port) */ }
unsafe fn AtPrepare_RelationMap() { /* TODO(pg-port) */ }
unsafe fn PostPrepare_Locks(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn PostPrepare_PgStat() { /* TODO(pg-port) */ }
unsafe fn PostPrepare_Inval() { /* TODO(pg-port) */ }
unsafe fn PostPrepare_smgr() { /* TODO(pg-port) */ }
unsafe fn PostPrepare_MultiXact(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn PostPrepare_PredicateLocks(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn PostPrepare_Twophase() { /* TODO(pg-port) */ }
unsafe fn PreCommit_on_commit_actions() { /* TODO(pg-port) */ }
unsafe fn StartPrepare(_gxact: GlobalTransaction) { /* TODO(pg-port) */ }
unsafe fn EndPrepare(_gxact: GlobalTransaction) { /* TODO(pg-port) */ }
unsafe fn MarkAsPreparing(_xid: TransactionId, _gid: *const std::os::raw::c_char,
                           _ts: TimestampTz, _owner: Oid, _db: Oid) -> GlobalTransaction {
    /* TODO(pg-port) */ ptr::null_mut()
}
unsafe fn ProcArrayEndTransaction(proc_: *mut PGProcStub, xid: TransactionId) {
    crate::storage::ipc::procarray::ProcArrayEndTransaction(proc_ as *mut _, xid)
}
unsafe fn ProcArrayClearTransaction(proc_: *mut PGProcStub) {
    crate::storage::ipc::procarray::ProcArrayClearTransaction(proc_ as *mut _)
}
unsafe fn ProcArrayApplyXidAssignment(_xtop: TransactionId, _n: i32, _xids: *mut TransactionId) { /* TODO(pg-port) */ }
unsafe fn ParallelWorkerReportLastRecEnd(_lsn: XLogRecPtr) { /* TODO(pg-port) */ }
unsafe fn XidCacheRemoveRunningXids(_xid: TransactionId, _n: i32, _xids: *mut TransactionId, _max: TransactionId) { /* TODO(pg-port) */ }
unsafe fn TransactionIdCommitTree(xid: TransactionId, n: i32, xids: *mut TransactionId) {
    crate::access::transam::transam::TransactionIdCommitTree(xid, n, xids)
}
unsafe fn TransactionIdAsyncCommitTree(xid: TransactionId, n: i32, xids: *mut TransactionId, lsn: XLogRecPtr) {
    crate::access::transam::transam::TransactionIdAsyncCommitTree(xid, n, xids, lsn)
}
unsafe fn TransactionIdAbortTree(xid: TransactionId, n: i32, xids: *mut TransactionId) {
    crate::access::transam::transam::TransactionIdAbortTree(xid, n, xids)
}
unsafe fn TransactionIdDidCommit(xid: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdDidCommit(xid)
}
unsafe fn TransactionTreeSetCommitTsData(xid: TransactionId, n: i32, xids: *mut TransactionId,
                                         ts: TimestampTz, origin: RepOriginId) {
    crate::access::transam::commit_ts::TransactionTreeSetCommitTsData(xid, n, xids, ts, origin)
}
unsafe fn SyncRepWaitForLSN(_lsn: XLogRecPtr, _flag: bool) { /* TODO(pg-port) */ }
unsafe fn LogStandbyInvalidations(_n: i32, _msgs: *mut SharedInvalidationMessage, _file: bool) { /* TODO(pg-port) */ }
unsafe fn LogLogicalInvalidations() { /* TODO(pg-port) */ }
unsafe fn xactGetCommittedInvalidationMessages(msgs: *mut *mut SharedInvalidationMessage,
                                               _file_inval: *mut bool) -> i32 {
    /* TODO(pg-port) */
    unsafe { *msgs = ptr::null_mut(); }
    0
}
unsafe fn pgstat_get_transactional_drops(_is_commit: bool, items: *mut *mut xl_xact_stats_item) -> i32 {
    /* TODO(pg-port) */
    unsafe { *items = ptr::null_mut(); }
    0
}
unsafe fn pgstat_execute_transactional_drops(_n: i32, _items: *mut xl_xact_stats_item, _is_redo: bool) { /* TODO(pg-port) */ }
unsafe fn pgstat_report_xact_timestamp(_ts: TimestampTz) { /* TODO(pg-port) */ }
unsafe fn pgstat_report_wait_end() { /* TODO(pg-port) */ }
unsafe fn pgstat_progress_end_command() { /* TODO(pg-port) */ }
unsafe fn pgaio_error_cleanup() { /* TODO(pg-port) */ }
unsafe fn LWLockReleaseAll() { /* TODO(pg-port) */ }
unsafe fn LWLockAcquire(_lock: *mut std::ffi::c_void, _mode: i32) -> bool { /* TODO(pg-port) */ true }
unsafe fn LWLockRelease(_lock: *mut std::ffi::c_void) { /* TODO(pg-port) */ }
unsafe fn UnlockBuffers() { /* TODO(pg-port) */ }
unsafe fn LockErrorCleanup() { /* TODO(pg-port) */ }
unsafe fn ConditionVariableCancelSleep() { /* TODO(pg-port) */ }
unsafe fn reschedule_timeouts() { /* TODO(pg-port) */ }
unsafe fn enable_timeout_after(_id: i32, _ms: i64) { /* TODO(pg-port) */ }
unsafe fn disable_timeout(_id: i32, _flag: bool) { /* TODO(pg-port) */ }
pub static mut TransactionTimeout: i64 = 0;
unsafe fn ResourceOwnerCreate(_parent: ResourceOwner, _name: *const std::os::raw::c_char) -> ResourceOwner {
    crate::utils::resowner::resowner::ResourceOwnerCreate(_parent as _, _name) as _
}
unsafe fn ResourceOwnerRelease(owner: ResourceOwner, phase: i32, is_commit: bool, is_top: bool) {
    crate::utils::resowner::resowner::ResourceOwnerRelease(owner as _, core::mem::transmute(phase), is_commit, is_top)
}
unsafe fn ResourceOwnerDelete(owner: ResourceOwner) {
    crate::utils::resowner::resowner::ResourceOwnerDelete(owner as _)
}
unsafe fn GetUserIdAndSecContext(uid: *mut Oid, ctx: *mut i32) {
    /* TODO(pg-port) */
    if !uid.is_null() { unsafe { *uid = 0; } }
    if !ctx.is_null() { unsafe { *ctx = 0; } }
}
unsafe fn SetUserIdAndSecContext(_uid: Oid, _ctx: i32) { /* TODO(pg-port) */ }
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn GetCurrentTimestamp() -> TimestampTz { /* TODO(pg-port) */ 0 }
unsafe fn ReadNextTransactionId() -> TransactionId { /* TODO(pg-port) */ 0 }
unsafe fn AllocSetContextCreate(parent: MemoryContext, name: *const std::os::raw::c_char,
                                 min: usize, init: usize, max: usize) -> MemoryContext {
    crate::utils::mmgr::aset::AllocSetContextCreateInternal(parent as _, name, min, init, max) as _
}
unsafe fn MemoryContextReset(ctx: MemoryContext) {
    crate::utils::mmgr::mcxt::MemoryContextReset(ctx as _)
}
unsafe fn MemoryContextDelete(ctx: MemoryContext) {
    crate::utils::mmgr::mcxt::MemoryContextDelete(ctx as _)
}
unsafe fn MemoryContextIsEmpty(ctx: MemoryContext) -> bool {
    crate::utils::mmgr::mcxt::MemoryContextIsEmpty(ctx as _)
}
unsafe fn MemoryContextAlloc(ctx: MemoryContext, size: usize) -> *mut std::ffi::c_void {
    crate::utils::mmgr::mcxt::MemoryContextAlloc(ctx as _, size)
}
unsafe fn MemoryContextAllocZero(ctx: MemoryContext, size: usize) -> *mut std::ffi::c_void {
    crate::utils::mmgr::mcxt::MemoryContextAllocZero(ctx as _, size)
}
unsafe fn MemoryContextStrdup(ctx: MemoryContext, s: *const std::os::raw::c_char) -> *mut std::os::raw::c_char {
    crate::utils::mmgr::mcxt::MemoryContextStrdup(ctx as _, s)
}
unsafe fn MemoryContextSwitchTo(ctx: MemoryContext) -> MemoryContext {
    crate::utils::mmgr::mcxt::MemoryContextSwitchTo(ctx as _) as _
}
pub use crate::utils::mmgr::mcxt::CurrentMemoryContext;
pub use crate::utils::mmgr::mcxt::TopMemoryContext;
pub use crate::utils::mmgr::mcxt::TopTransactionContext;
pub static mut CurTransactionContext: MemoryContext = ptr::null_mut();
#[no_mangle]
pub static mut TopTransactionResourceOwner: ResourceOwner = ptr::null_mut();
pub static mut CurTransactionResourceOwner: ResourceOwner = ptr::null_mut();
extern "C" { pub static mut CurrentResourceOwner: ResourceOwner; }
pub static mut MyDatabaseId: Oid = 0;
pub static mut MyDatabaseTableSpace: Oid = 0;
pub static mut CritSectionCount: i32 = 0;
pub static mut ExitOnAnyError: bool = false;
pub static mut log_xact_sample_rate: f64 = 0.0;
pub static mut replorigin_session_origin: RepOriginId = 0;
pub static mut replorigin_session_origin_lsn: XLogRecPtr = 0;
pub static mut replorigin_session_origin_timestamp: TimestampTz = 0;
pub static mut standbyState: i32 = 0;

/* TODO(pg-port): real PGPROC */
pub struct PGProcStub {
    pub vxid: VxidData,
    pub delayChkptFlags: u32,
}
pub struct VxidData {
    pub procNumber: ProcNumber,
    pub lxid: LocalTransactionId,
}
extern "C" { pub static mut MyProc: *mut PGProcStub; }
extern "C" { pub static mut MyProcNumber: ProcNumber; }
unsafe fn GetNextLocalTransactionId() -> LocalTransactionId { /* TODO(pg-port) */ 0 }
unsafe fn VirtualXactLockTableInsert(_vxid: VirtualTransactionId) { /* TODO(pg-port) */ }
unsafe fn TRACE_POSTGRESQL_TRANSACTION_START(_lxid: LocalTransactionId) { /* TODO(pg-port) */ }
unsafe fn TRACE_POSTGRESQL_TRANSACTION_COMMIT(_lxid: LocalTransactionId) { /* TODO(pg-port) */ }
unsafe fn TRACE_POSTGRESQL_TRANSACTION_ABORT(_lxid: LocalTransactionId) { /* TODO(pg-port) */ }
unsafe fn SPI_inside_nonatomic_context() -> bool { /* TODO(pg-port) */ false }
unsafe fn ResetReindexState(_nl: i32) { /* TODO(pg-port) */ }
unsafe fn ResetLogicalStreamingState() { /* TODO(pg-port) */ }
unsafe fn SnapBuildResetExportedSnapshotState() { /* TODO(pg-port) */ }
unsafe fn XactHasExportedSnapshots() -> bool { /* TODO(pg-port) */ false }
/* IsSubTransaction defined below */
unsafe fn stack_is_too_deep() -> bool { /* TODO(pg-port) */ false }
unsafe fn message_level_is_interesting(_level: i32) -> bool { /* TODO(pg-port) */ false }
unsafe fn check_stack_depth() { /* TODO(pg-port) */ }
unsafe fn initStringInfo(_buf: *mut StringInfoData) { /* TODO(pg-port) */ }
unsafe fn appendStringInfo(_buf: *mut StringInfoData, _fmt: *const std::os::raw::c_char) { /* TODO(pg-port) */ }
unsafe fn pfree(_ptr: *mut std::ffi::c_void) { /* TODO(pg-port) */ }
unsafe fn palloc(size: usize) -> *mut std::ffi::c_void { /* TODO(pg-port) */ ptr::null_mut() }
unsafe fn repalloc(_ptr: *mut std::ffi::c_void, _size: usize) -> *mut std::ffi::c_void { /* TODO(pg-port) */ ptr::null_mut() }
unsafe fn qsort(_base: *mut std::ffi::c_void, _n: usize, _size: usize,
                 _cmp: unsafe fn(*const std::ffi::c_void, *const std::ffi::c_void) -> i32) { /* TODO(pg-port) */ }
unsafe fn xidComparator(a: *const std::ffi::c_void, b: *const std::ffi::c_void) -> i32 {
    /* TODO(pg-port) */
    let xa = *(a as *const TransactionId);
    let xb = *(b as *const TransactionId);
    if xa < xb { -1 } else if xa > xb { 1 } else { 0 }
}
unsafe fn add_size(a: Size, b: Size) -> Size { a.saturating_add(b) }
unsafe fn mul_size(a: Size, b: Size) -> Size { a.saturating_mul(b) }
unsafe fn DropRelationFiles(_locs: *mut RelFileLocator, _n: i32, _redo: bool) { /* TODO(pg-port) */ }
unsafe fn AdvanceNextFullTransactionIdPastXid(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn RecordKnownAssignedTransactionIds(_xid: TransactionId) { /* TODO(pg-port) */ }
unsafe fn ExpireTreeKnownAssignedTransactionIds(_xid: TransactionId, _n: i32,
    _xids: *mut TransactionId, _max: TransactionId) { /* TODO(pg-port) */ }
unsafe fn ProcessCommittedInvalidationMessages(_msgs: *mut SharedInvalidationMessage, _n: i32,
    _file_inval: bool, _db: Oid, _ts: Oid) { /* TODO(pg-port) */ }
unsafe fn StandbyReleaseLockTree(_xid: TransactionId, _n: i32, _xids: *mut TransactionId) { /* TODO(pg-port) */ }
unsafe fn replorigin_session_advance(_lsn: XLogRecPtr, _end: XLogRecPtr) { /* TODO(pg-port) */ }
unsafe fn replorigin_advance(_origin: RepOriginId, _remote_lsn: XLogRecPtr,
    _local_lsn: XLogRecPtr, _backward: bool, _wal: bool) { /* TODO(pg-port) */ }
unsafe fn ParseCommitRecord(_info: u8, _rec: *mut xl_xact_commit, _parsed: *mut xl_xact_parsed_commit) { /* TODO(pg-port) */ }
unsafe fn ParseAbortRecord(_info: u8, _rec: *mut xl_xact_abort, _parsed: *mut xl_xact_parsed_abort) { /* TODO(pg-port) */ }
unsafe fn XLogRecGetInfo(_rec: *mut XLogReaderState) -> u8 { /* TODO(pg-port) */ 0 }
unsafe fn XLogRecGetData(_rec: *mut XLogReaderState) -> *mut std::ffi::c_void { /* TODO(pg-port) */ ptr::null_mut() }
unsafe fn XLogRecGetXid(_rec: *mut XLogReaderState) -> TransactionId { /* TODO(pg-port) */ 0 }
unsafe fn XLogRecGetOrigin(_rec: *mut XLogReaderState) -> RepOriginId { /* TODO(pg-port) */ 0 }
unsafe fn XLogRecHasAnyBlockRefs(_rec: *mut XLogReaderState) -> bool { /* TODO(pg-port) */ false }
unsafe fn XactCompletionRelcacheInitFileInval(_xinfo: u32) -> bool { /* TODO(pg-port) */ false }
unsafe fn XactCompletionForceSyncCommit(_xinfo: u32) -> bool { /* TODO(pg-port) */ false }
unsafe fn XactCompletionApplyFeedback(_xinfo: u32) -> bool { /* TODO(pg-port) */ false }
unsafe fn PrepareRedoAdd(_data: *mut std::ffi::c_void, _start: XLogRecPtr, _end: XLogRecPtr, _origin: RepOriginId) { /* TODO(pg-port) */ }
unsafe fn PrepareRedoRemove(_xid: TransactionId, _sent_to_standby: bool) { /* TODO(pg-port) */ }
unsafe fn AbortOutOfAnyTransaction_portals_cleanup() { /* TODO(pg-port) */ }
/* TODO(pg-port): TwoPhaseStateLock */
use crate::backend_link_shims::TwoPhaseStateLock;
pub const LW_EXCLUSIVE: i32 = 1;
unsafe fn AtAbort_Twophase() { /* TODO(pg-port) */ }
unsafe fn pg_prng_double(_state: *mut std::ffi::c_void) -> f64 { /* TODO(pg-port) */ 0.0 }
pub static mut pg_global_prng_state: *mut std::ffi::c_void = ptr::null_mut();

macro_rules! Assert {
    ($e:expr) => {
        debug_assert!($e);
    }
}

macro_rules! elog {
    ($level:expr, $($arg:tt)*) => {
        /* TODO(pg-port): elog */
        eprintln!("[elog level={}] {}", $level, format!($($arg)*));
        if $level >= PANIC {
            panic!("elog PANIC");
        }
    }
}

macro_rules! ereport {
    ($level:expr, $msg:expr) => {
        /* TODO(pg-port): ereport - errcode/errdetail folded as comment */
        eprintln!("[ereport level={}] {}", $level, $msg);
        if $level >= PANIC {
            panic!("ereport PANIC");
        }
    }
}

macro_rules! errmsg {
    ($($arg:tt)*) => {
        format!($($arg)*)
    }
}

macro_rules! START_CRIT_SECTION {
    () => { unsafe { CritSectionCount += 1; } }
}

macro_rules! END_CRIT_SECTION {
    () => { unsafe { CritSectionCount -= 1; } }
}

macro_rules! HOLD_INTERRUPTS {
    () => { /* TODO(pg-port) */ }
}

macro_rules! RESUME_INTERRUPTS {
    () => { /* TODO(pg-port) */ }
}

/* TODO(pg-port): sigprocmask */
unsafe fn sigprocmask(_how: i32, _set: *const std::ffi::c_void, _oset: *mut std::ffi::c_void) {}
pub static mut UnBlockSig: i64 = 0;
pub const SIG_SETMASK: i32 = 2;

/* TODO(pg-port): ALLOCSET_DEFAULT_SIZES */
pub const ALLOCSET_DEFAULT_SIZES: (usize, usize, usize) = (0, 8 * 1024, 8 * 1024 * 1024);

unsafe fn AllocSetContextCreateWrapper(parent: MemoryContext, name: *const std::os::raw::c_char,
    sizes: (usize, usize, usize)) -> MemoryContext {
    AllocSetContextCreate(parent, name, sizes.0, sizes.1, sizes.2)
}

/* ----------------------------------------------------------------
 *    transaction state accessors
 * ---------------------------------------------------------------- */

/*
 *    IsTransactionState
 *
 *    This returns true if we are inside a valid transaction; that is,
 *    it is safe to initiate database access, take heavyweight locks, etc.
 */
pub unsafe fn IsTransactionState() -> bool {
    let s: TransactionState = CurrentTransactionState;

    /*
     * TRANS_DEFAULT and TRANS_ABORT are obviously unsafe states.  However, we
     * also reject the startup/shutdown states TRANS_START, TRANS_COMMIT,
     * TRANS_PREPARE since it might be too soon or too late within those
     * transition states to do anything interesting.  Hence, the only "valid"
     * state is TRANS_INPROGRESS.
     */
    (*s).state == TRANS_INPROGRESS
}

/*
 *    IsAbortedTransactionBlockState
 *
 *    This returns true if we are within an aborted transaction block.
 */
pub unsafe fn IsAbortedTransactionBlockState() -> bool {
    let s: TransactionState = CurrentTransactionState;

    if (*s).blockState == TBLOCK_ABORT ||
       (*s).blockState == TBLOCK_SUBABORT {
        return true;
    }

    false
}


/*
 *    GetTopTransactionId
 *
 * This will return the XID of the main transaction, assigning one if
 * it's not yet set.  Be careful to call this only inside a valid xact.
 */
pub unsafe fn GetTopTransactionId() -> TransactionId {
    if !FullTransactionIdIsValid(XactTopFullTransactionId) {
        AssignTransactionId(&mut TopTransactionStateData as TransactionState);
    }
    XidFromFullTransactionId(XactTopFullTransactionId)
}

/*
 *    GetTopTransactionIdIfAny
 *
 * This will return the XID of the main transaction, if one is assigned.
 * It will return InvalidTransactionId if we are not currently inside a
 * transaction, or inside a transaction that hasn't yet been assigned an XID.
 */
pub unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    XidFromFullTransactionId(XactTopFullTransactionId)
}

/*
 *    GetCurrentTransactionId
 *
 * This will return the XID of the current transaction (main or sub
 * transaction), assigning one if it's not yet set.  Be careful to call this
 * only inside a valid xact.
 */
pub unsafe fn GetCurrentTransactionId() -> TransactionId {
    let s: TransactionState = CurrentTransactionState;

    if !FullTransactionIdIsValid((*s).fullTransactionId) {
        AssignTransactionId(s);
    }
    XidFromFullTransactionId((*s).fullTransactionId)
}

/*
 *    GetCurrentTransactionIdIfAny
 *
 * This will return the XID of the current sub xact, if one is assigned.
 */
pub unsafe fn GetCurrentTransactionIdIfAny() -> TransactionId {
    XidFromFullTransactionId((*CurrentTransactionState).fullTransactionId)
}

/*
 *    GetTopFullTransactionId
 *
 * This will return the FullTransactionId of the main transaction, assigning
 * one if it's not yet set.
 */
pub unsafe fn GetTopFullTransactionId() -> FullTransactionId {
    if !FullTransactionIdIsValid(XactTopFullTransactionId) {
        AssignTransactionId(&mut TopTransactionStateData as TransactionState);
    }
    XactTopFullTransactionId
}

/*
 *    GetTopFullTransactionIdIfAny
 */
pub unsafe fn GetTopFullTransactionIdIfAny() -> FullTransactionId {
    XactTopFullTransactionId
}

/*
 *    GetCurrentFullTransactionId
 */
pub unsafe fn GetCurrentFullTransactionId() -> FullTransactionId {
    let s: TransactionState = CurrentTransactionState;

    if !FullTransactionIdIsValid((*s).fullTransactionId) {
        AssignTransactionId(s);
    }
    (*s).fullTransactionId
}

/*
 *    GetCurrentFullTransactionIdIfAny
 */
pub unsafe fn GetCurrentFullTransactionIdIfAny() -> FullTransactionId {
    (*CurrentTransactionState).fullTransactionId
}

/*
 *    MarkCurrentTransactionIdLoggedIfAny
 */
pub unsafe fn MarkCurrentTransactionIdLoggedIfAny() {
    if FullTransactionIdIsValid((*CurrentTransactionState).fullTransactionId) {
        (*CurrentTransactionState).didLogXid = true;
    }
}

/*
 * IsSubxactTopXidLogPending
 *
 * This is used to decide whether we need to WAL log the top-level XID for
 * operation in a subtransaction.
 */
pub unsafe fn IsSubxactTopXidLogPending() -> bool {
    /* check whether it is already logged */
    if (*CurrentTransactionState).topXidLogged {
        return false;
    }

    /* wal_level has to be logical */
    if !XLogLogicalInfoActive() {
        return false;
    }

    /* we need to be in a transaction state */
    if !IsTransactionState() {
        return false;
    }

    /* it has to be a subtransaction */
    if !IsSubTransaction() {
        return false;
    }

    /* the subtransaction has to have a XID assigned */
    if !TransactionIdIsValid(GetCurrentTransactionIdIfAny()) {
        return false;
    }

    true
}

/*
 * MarkSubxactTopXidLogged
 */
pub unsafe fn MarkSubxactTopXidLogged() {
    Assert!(IsSubxactTopXidLogPending());

    (*CurrentTransactionState).topXidLogged = true;
}

/*
 *    GetStableLatestTransactionId
 *
 * Get the transaction's XID if it has one, else read the next-to-be-assigned
 * XID.
 */
pub unsafe fn GetStableLatestTransactionId() -> TransactionId {
    static mut lxid: LocalTransactionId = InvalidLocalTransactionId;
    static mut stablexid: TransactionId = InvalidTransactionId;

    if lxid != (*MyProc).vxid.lxid {
        lxid = (*MyProc).vxid.lxid;
        stablexid = GetTopTransactionIdIfAny();
        if !TransactionIdIsValid(stablexid) {
            stablexid = ReadNextTransactionId();
        }
    }

    Assert!(TransactionIdIsValid(stablexid));

    stablexid
}

/*
 * AssignTransactionId (internal impl)
 *
 * Assigns a new permanent FullTransactionId to the given TransactionState.
 */
unsafe fn assign_transaction_id_impl(s: TransactionState) {
    let is_sub_xact: bool = !(*s).parent.is_null();
    let current_owner: ResourceOwner;
    let mut log_unknown_top: bool = false;

    /* Assert that caller didn't screw up */
    Assert!(!FullTransactionIdIsValid((*s).fullTransactionId));
    Assert!((*s).state == TRANS_INPROGRESS);

    /*
     * Workers synchronize transaction state at the beginning of each parallel
     * operation, so we can't account for new XIDs at this point.
     */
    if IsInParallelMode() || IsParallelWorker() {
        ereport!(ERROR,
            errmsg!("cannot assign transaction IDs during a parallel operation"));
        /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
    }

    /*
     * Ensure parent(s) have XIDs, so that a child always has an XID later
     * than its parent.  Mustn't recurse here, or we might get a stack
     * overflow if we're at the bottom of a huge stack of subtransactions none
     * of which have XIDs yet.
     */
    if is_sub_xact && !FullTransactionIdIsValid((*(*s).parent).fullTransactionId) {
        let mut p: TransactionState = (*s).parent;
        let parents: *mut TransactionState =
            palloc(core::mem::size_of::<TransactionState>() * (*s).nestingLevel as usize)
            as *mut TransactionState;
        let mut parent_offset: usize = 0;

        while !p.is_null() && !FullTransactionIdIsValid((*p).fullTransactionId) {
            *parents.add(parent_offset) = p;
            parent_offset += 1;
            p = (*p).parent;
        }

        /*
         * This is technically a recursive call, but the recursion will never
         * be more than one layer deep.
         */
        while parent_offset != 0 {
            parent_offset -= 1;
            AssignTransactionId(*parents.add(parent_offset));
        }

        pfree(parents as *mut std::ffi::c_void);
    }

    /*
     * When wal_level=logical, guarantee that a subtransaction's xid can only
     * be seen in the WAL stream if its toplevel xid has been logged before.
     */
    if is_sub_xact && XLogLogicalInfoActive() &&
       !TopTransactionStateData.didLogXid {
        log_unknown_top = true;
    }

    /*
     * Generate a new FullTransactionId and record its xid in PGPROC and
     * pg_subtrans.
     *
     * NB: we must make the subtrans entry BEFORE the Xid appears anywhere in
     * shared storage other than PGPROC.
     */
    (*s).fullTransactionId = GetNewTransactionId(is_sub_xact);
    if !is_sub_xact {
        XactTopFullTransactionId = (*s).fullTransactionId;
    }

    if is_sub_xact {
        SubTransSetParent(XidFromFullTransactionId((*s).fullTransactionId),
                          XidFromFullTransactionId((*(*s).parent).fullTransactionId));
    }

    /*
     * If it's a top-level transaction, the predicate locking system needs to
     * be told about it too.
     */
    if !is_sub_xact {
        RegisterPredicateLockingXid(XidFromFullTransactionId((*s).fullTransactionId));
    }

    /*
     * Acquire lock on the transaction XID.  We have to ensure that the lock
     * is assigned to the transaction's own ResourceOwner.
     */
    current_owner = CurrentResourceOwner;
    CurrentResourceOwner = (*s).curTransactionOwner;

    XactLockTableInsert(XidFromFullTransactionId((*s).fullTransactionId));

    CurrentResourceOwner = current_owner;

    /*
     * Every PGPROC_MAX_CACHED_SUBXIDS assigned transaction ids within each
     * top-level transaction we issue a WAL record for the assignment.
     */
    if is_sub_xact && XLogStandbyInfoActive() {
        unreportedXids[nUnreportedXids as usize] =
            XidFromFullTransactionId((*s).fullTransactionId);
        nUnreportedXids += 1;

        /*
         * ensure this test matches similar one in RecoverPreparedTransactions()
         */
        if nUnreportedXids >= PGPROC_MAX_CACHED_SUBXIDS as i32 ||
           log_unknown_top {
            let mut xlrec = xl_xact_assignment::default();

            /*
             * xtop is always set by now because we recurse up transaction
             * stack to the highest unassigned xid and then come back down
             */
            xlrec.xtop = GetTopTransactionId();
            Assert!(TransactionIdIsValid(xlrec.xtop));
            xlrec.nsubxacts = nUnreportedXids;

            XLogBeginInsert();
            XLogRegisterData(&xlrec as *const xl_xact_assignment as *const std::ffi::c_void,
                             MinSizeOfXactAssignment);
            XLogRegisterData(unreportedXids.as_ptr() as *const std::ffi::c_void,
                             nUnreportedXids as usize * core::mem::size_of::<TransactionId>());

            let _ = XLogInsert(RM_XACT_ID, XLOG_XACT_ASSIGNMENT);

            nUnreportedXids = 0;
            /* mark top, not current xact as having been logged */
            TopTransactionStateData.didLogXid = true;
        }
    }
}

/*
 *    GetCurrentSubTransactionId
 */
pub unsafe fn GetCurrentSubTransactionId() -> SubTransactionId {
    let s: TransactionState = CurrentTransactionState;

    (*s).subTransactionId
}

/*
 *    SubTransactionIsActive
 *
 * Test if the specified subxact ID is still active.
 */
pub unsafe fn SubTransactionIsActive(subxid: SubTransactionId) -> bool {
    let mut s: TransactionState = CurrentTransactionState;

    while !s.is_null() {
        if (*s).state == TRANS_ABORT {
            s = (*s).parent;
            continue;
        }
        if (*s).subTransactionId == subxid {
            return true;
        }
        s = (*s).parent;
    }
    false
}


/*
 *    GetCurrentCommandId
 *
 * "used" must be true if the caller intends to use the command ID to mark
 * inserted/updated/deleted tuples.
 */
#[no_mangle]
pub unsafe fn GetCurrentCommandId(used: bool) -> CommandId {
    /* this is global to a transaction, not subtransaction-local */
    if used {
        /*
         * Forbid setting currentCommandIdUsed in a parallel worker.
         */
        if IsParallelWorker() {
            ereport!(ERROR, errmsg!("cannot modify data in a parallel worker"));
            /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
        }

        currentCommandIdUsed = true;
    }
    currentCommandId
}

/*
 *    SetParallelStartTimestamps
 *
 * In a parallel worker, we should inherit the parent transaction's timestamps.
 */
pub unsafe fn SetParallelStartTimestamps(xact_ts: TimestampTz, stmt_ts: TimestampTz) {
    Assert!(IsParallelWorker());
    xactStartTimestamp = xact_ts;
    stmtStartTimestamp = stmt_ts;
}

/*
 *    GetCurrentTransactionStartTimestamp
 */
pub unsafe fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    xactStartTimestamp
}

/*
 *    GetCurrentStatementStartTimestamp
 */
pub unsafe fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    stmtStartTimestamp
}

/*
 *    GetCurrentTransactionStopTimestamp
 *
 * If the transaction stop time hasn't already been set, set xactStopTimestamp.
 */
pub unsafe fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    /* s only used for assert */
    let s: TransactionState = CurrentTransactionState;

    /* should only be called after commit / abort processing */
    Assert!((*s).state == TRANS_DEFAULT ||
            (*s).state == TRANS_COMMIT ||
            (*s).state == TRANS_ABORT ||
            (*s).state == TRANS_PREPARE);

    if xactStopTimestamp == 0 {
        xactStopTimestamp = GetCurrentTimestamp();
    }

    xactStopTimestamp
}

/*
 *    SetCurrentStatementStartTimestamp
 */
#[no_mangle]
pub unsafe fn SetCurrentStatementStartTimestamp() {
    if !IsParallelWorker() {
        stmtStartTimestamp = GetCurrentTimestamp();
    } else {
        Assert!(stmtStartTimestamp != 0);
    }
}

/*
 *    GetCurrentTransactionNestLevel
 *
 * Note: this will return zero when not inside any transaction.
 */
pub unsafe fn GetCurrentTransactionNestLevel() -> i32 {
    let s: TransactionState = CurrentTransactionState;

    (*s).nestingLevel
}


/*
 *    TransactionIdIsCurrentTransactionId
 */
pub unsafe fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    let mut s: TransactionState;

    /*
     * We always say that BootstrapTransactionId is "not my transaction ID"
     * even when it is (ie, during bootstrap).
     *
     * Likewise, InvalidTransactionId and FrozenTransactionId are certainly
     * not my transaction ID.
     */
    if !TransactionIdIsNormal(xid) {
        return false;
    }

    if TransactionIdEquals(xid, GetTopTransactionIdIfAny()) {
        return true;
    }

    /*
     * In parallel workers, the XIDs we must consider as current are stored in
     * ParallelCurrentXids rather than the transaction-state stack.
     */
    if nParallelCurrentXids > 0 {
        let mut low: i32 = 0;
        let mut high: i32 = nParallelCurrentXids - 1;
        while low <= high {
            let middle: i32 = low + (high - low) / 2;
            let probe: TransactionId = *ParallelCurrentXids.add(middle as usize);
            if probe == xid {
                return true;
            } else if probe < xid {
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        return false;
    }

    /*
     * We will return true for the Xid of the current subtransaction, any of
     * its subcommitted children, any of its parents, or any of their
     * previously subcommitted children.
     */
    s = CurrentTransactionState;
    while !s.is_null() {
        let mut low: i32;
        let mut high: i32;

        if (*s).state == TRANS_ABORT {
            s = (*s).parent;
            continue;
        }
        if !FullTransactionIdIsValid((*s).fullTransactionId) {
            s = (*s).parent;
            continue; /* it can't have any child XIDs either */
        }
        if TransactionIdEquals(xid, XidFromFullTransactionId((*s).fullTransactionId)) {
            return true;
        }
        /* As the childXids array is ordered, we can use binary search */
        low = 0;
        high = (*s).nChildXids - 1;
        while low <= high {
            let middle: i32 = low + (high - low) / 2;
            let probe: TransactionId = *(*s).childXids.add(middle as usize);
            if TransactionIdEquals(probe, xid) {
                return true;
            } else if TransactionIdPrecedes(probe, xid) {
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        s = (*s).parent;
    }

    false
}

/*
 *    TransactionStartedDuringRecovery
 */
pub unsafe fn TransactionStartedDuringRecovery() -> bool {
    (*CurrentTransactionState).startedInRecovery
}

/*
 *    EnterParallelMode
 */
pub unsafe fn EnterParallelMode() {
    let s: TransactionState = CurrentTransactionState;

    Assert!((*s).parallelModeLevel >= 0);

    (*s).parallelModeLevel += 1;
}

/*
 *    ExitParallelMode
 */
pub unsafe fn ExitParallelMode() {
    let s: TransactionState = CurrentTransactionState;

    Assert!((*s).parallelModeLevel > 0);
    Assert!((*s).parallelModeLevel > 1 || (*s).parallelChildXact ||
            !ParallelContextActive());

    (*s).parallelModeLevel -= 1;
}

/*
 *    IsInParallelMode (real implementation)
 *
 * Are we in a parallel operation, as either the leader or a worker?
 */
pub unsafe fn IsInParallelMode_real() -> bool {
    let s: TransactionState = CurrentTransactionState;

    (*s).parallelModeLevel != 0 || (*s).parallelChildXact
}

/*
 *    CommandCounterIncrement
 */
pub unsafe fn CommandCounterIncrement() {
    /*
     * If the current value of the command counter hasn't been "used" to mark
     * tuples, we need not increment it.
     */
    if currentCommandIdUsed {
        /*
         * Workers synchronize transaction state at the beginning of each
         * parallel operation, so we can't account for new commands after that
         * point.
         */
        if IsInParallelMode() || IsParallelWorker() {
            ereport!(ERROR, errmsg!("cannot start commands during a parallel operation"));
            /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
        }

        currentCommandId = currentCommandId.wrapping_add(1);
        if currentCommandId == InvalidCommandId {
            currentCommandId = currentCommandId.wrapping_sub(1);
            ereport!(ERROR, errmsg!("cannot have more than 2^32-2 commands in a transaction"));
            /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        }
        currentCommandIdUsed = false;

        /* Propagate new command ID into static snapshots */
        SnapshotSetCommandId(currentCommandId);

        /*
         * Make any catalog changes done by the just-completed command visible
         * in the local syscache.
         */
        AtCCI_LocalCache();

        /*
         * Drop the cached catalog snapshot so the next catalog access re-reads
         * it with the new command id, making this command's own catalog changes
         * (e.g. a just-inserted pg_index row) visible within the same xact.
         */
        crate::utils::time::snapmgr::InvalidateCatalogSnapshot();
    }
}

/*
 * ForceSyncCommit
 *
 * Interface routine to allow commands to force a synchronous commit.
 */
pub unsafe fn ForceSyncCommit() {
    forceSyncCommit = true;
}

/* ----------------------------------------------------------------
 *                StartTransaction stuff
 * ---------------------------------------------------------------- */

/*
 *    AtStart_Cache
 */
unsafe fn AtStart_Cache() {
    AcceptInvalidationMessages();
}

/*
 *    AtStart_Memory
 */
unsafe fn AtStart_Memory() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * Remember the memory context that was active prior to transaction start.
     */
    (*s).priorContext = CurrentMemoryContext as _;

    /*
     * If this is the first time through, create a private context for
     * AbortTransaction to work in.
     */
    if TransactionAbortContext.is_null() {
        TransactionAbortContext =
            AllocSetContextCreate(TopMemoryContext as _,
                                  b"TransactionAbortContext\0".as_ptr() as *const _,
                                  32 * 1024, 32 * 1024, 32 * 1024);
    }

    /*
     * Likewise, if this is the first time through, create a top-level context
     * for transaction-local data.
     */
    if TopTransactionContext.is_null() {
        TopTransactionContext =
            AllocSetContextCreateWrapper(TopMemoryContext as _,
                                         b"TopTransactionContext\0".as_ptr() as *const _,
                                         ALLOCSET_DEFAULT_SIZES) as _;
    }

    /*
     * In a top-level transaction, CurTransactionContext is the same as
     * TopTransactionContext.
     */
    CurTransactionContext = TopTransactionContext as _;
    (*s).curTransactionContext = CurTransactionContext;

    /* Make the CurTransactionContext active. */
    MemoryContextSwitchTo(CurTransactionContext);
}

/*
 *    AtStart_ResourceOwner
 */
unsafe fn AtStart_ResourceOwner() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * We shouldn't have a transaction resource owner already.
     */
    Assert!(TopTransactionResourceOwner.is_null());

    /*
     * Create a toplevel resource owner for the transaction.
     */
    (*s).curTransactionOwner = ResourceOwnerCreate(
        ptr::null_mut(), b"TopTransaction\0".as_ptr() as *const _);

    TopTransactionResourceOwner = (*s).curTransactionOwner;
    CurTransactionResourceOwner = (*s).curTransactionOwner;
    CurrentResourceOwner = (*s).curTransactionOwner;
}

/* ----------------------------------------------------------------
 *                StartSubTransaction stuff
 * ---------------------------------------------------------------- */

/*
 * AtSubStart_Memory
 */
unsafe fn AtSubStart_Memory() {
    let s: TransactionState = CurrentTransactionState;

    Assert!(!CurTransactionContext.is_null());

    /*
     * Remember the context that was active prior to subtransaction start.
     */
    (*s).priorContext = CurrentMemoryContext as _;

    /*
     * Create a CurTransactionContext, which will be used to hold data that
     * survives subtransaction commit but disappears on subtransaction abort.
     */
    CurTransactionContext = AllocSetContextCreateWrapper(CurTransactionContext,
                                                          b"CurTransactionContext\0".as_ptr() as *const _,
                                                          ALLOCSET_DEFAULT_SIZES);
    (*s).curTransactionContext = CurTransactionContext;

    /* Make the CurTransactionContext active. */
    MemoryContextSwitchTo(CurTransactionContext);
}

/*
 * AtSubStart_ResourceOwner
 */
unsafe fn AtSubStart_ResourceOwner() {
    let s: TransactionState = CurrentTransactionState;

    Assert!(!(*s).parent.is_null());

    /*
     * Create a resource owner for the subtransaction.  We make it a child of
     * the immediate parent's resource owner.
     */
    (*s).curTransactionOwner =
        ResourceOwnerCreate((*(*s).parent).curTransactionOwner,
                            b"SubTransaction\0".as_ptr() as *const _);

    CurTransactionResourceOwner = (*s).curTransactionOwner;
    CurrentResourceOwner = (*s).curTransactionOwner;
}

/* ----------------------------------------------------------------
 *                CommitTransaction stuff
 * ---------------------------------------------------------------- */

/*
 *    RecordTransactionCommit
 *
 * Returns latest XID among xact and its children, or InvalidTransactionId
 * if the xact has no XID.
 *
 * If you change this function, see RecordTransactionCommitPrepared also.
 */
unsafe fn RecordTransactionCommit() -> TransactionId {
    let xid: TransactionId = GetTopTransactionIdIfAny();
    let mark_xid_committed: bool = TransactionIdIsValid(xid);
    let mut latest_xid: TransactionId = InvalidTransactionId;
    let mut nrels: i32;
    let mut rels: *mut RelFileLocator = ptr::null_mut();
    let mut nchildren: i32;
    let mut children: *mut TransactionId = ptr::null_mut();
    let mut ndroppedstats: i32 = 0;
    let mut droppedstats: *mut xl_xact_stats_item = ptr::null_mut();
    let mut nmsgs: i32 = 0;
    let mut inval_messages: *mut SharedInvalidationMessage = ptr::null_mut();
    let mut relcache_init_file_inval: bool = false;
    let mut wrote_xlog: bool;

    /*
     * Log pending invalidations for logical decoding of in-progress
     * transactions.
     */
    if XLogLogicalInfoActive() {
        LogLogicalInvalidations();
    }

    /* Get data needed for commit record */
    nrels = smgrGetPendingDeletes(true, &mut rels);
    nchildren = xactGetCommittedChildren(&mut children);
    ndroppedstats = pgstat_get_transactional_drops(true, &mut droppedstats);
    if XLogStandbyInfoActive() {
        nmsgs = xactGetCommittedInvalidationMessages(&mut inval_messages,
                                                     &mut relcache_init_file_inval);
    }
    wrote_xlog = (XactLastRecEnd != 0);

    /*
     * If we haven't been assigned an XID yet, we neither can, nor do we want
     * to write a COMMIT record.
     */
    if !mark_xid_committed {
        /*
         * We expect that every RelationDropStorage is followed by a catalog
         * update, and hence XID assignment, so we shouldn't get here with any
         * pending deletes. Same is true for dropping stats.
         */
        if nrels != 0 || ndroppedstats != 0 {
            elog!(ERROR,
                "cannot commit a transaction that deleted files but has no xid");
        }

        /* Can't have child XIDs either; AssignTransactionId enforces this */
        Assert!(nchildren == 0);

        /*
         * Transactions without an assigned xid can contain invalidation
         * messages.
         *
         * XXX Every known use of this capability is a defect.
         *
         * ON COMMIT DELETE ROWS does a nontransactional index_build(), which
         * queues a relcache inval.
         */
        if nmsgs != 0 {
            LogStandbyInvalidations(nmsgs, inval_messages, relcache_init_file_inval);
            wrote_xlog = true; /* not strictly necessary */
        }

        /*
         * If we didn't create XLOG entries, we're done here.
         */
        if !wrote_xlog {
            /* goto cleanup */
            if !rels.is_null() { pfree(rels as *mut std::ffi::c_void); }
            if ndroppedstats != 0 { pfree(droppedstats as *mut std::ffi::c_void); }
            return latest_xid;
        }
    } else {
        let replorigin: bool = replorigin_session_origin != InvalidRepOriginId &&
                               replorigin_session_origin != DoNotReplicateId;

        /*
         * Mark ourselves as within our "commit critical section".
         */
        Assert!(((*MyProc).delayChkptFlags & DELAY_CHKPT_START) == 0);
        START_CRIT_SECTION!();
        (*MyProc).delayChkptFlags |= DELAY_CHKPT_START;

        /*
         * Insert the commit XLOG record.
         */
        XactLogCommitRecord(GetCurrentTransactionStopTimestamp(),
                            nchildren, children, nrels, rels,
                            ndroppedstats, droppedstats,
                            nmsgs, inval_messages,
                            relcache_init_file_inval,
                            MyXactFlags,
                            InvalidTransactionId, ptr::null() /* plain commit */);

        if replorigin {
            /* Move LSNs forward for this replication origin */
            replorigin_session_advance(replorigin_session_origin_lsn,
                                       XactLastRecEnd);
        }

        /*
         * Record commit timestamp.
         */
        if !replorigin || replorigin_session_origin_timestamp == 0 {
            replorigin_session_origin_timestamp = GetCurrentTransactionStopTimestamp();
        }

        TransactionTreeSetCommitTsData(xid, nchildren, children,
                                       replorigin_session_origin_timestamp,
                                       replorigin_session_origin);
    }

    /*
     * Check if we want to commit asynchronously.
     */
    if (wrote_xlog && mark_xid_committed &&
        synchronous_commit > SYNCHRONOUS_COMMIT_OFF) ||
       forceSyncCommit || nrels > 0 {
        XLogFlush(XactLastRecEnd);

        /*
         * Now we may update the CLOG, if we wrote a COMMIT record above
         */
        if mark_xid_committed {
            TransactionIdCommitTree(xid, nchildren, children);
        }
    } else {
        /*
         * Asynchronous commit case:
         *
         * Report the latest async commit LSN.
         */
        XLogSetAsyncXactLSN(XactLastRecEnd);

        /*
         * We must not immediately update the CLOG.
         */
        if mark_xid_committed {
            TransactionIdAsyncCommitTree(xid, nchildren, children, XactLastRecEnd);
        }
    }

    /*
     * If we entered a commit critical section, leave it now.
     */
    if mark_xid_committed {
        (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;
        END_CRIT_SECTION!();
    }

    /* Compute latestXid while we have the child XIDs handy */
    latest_xid = TransactionIdLatest(xid, nchildren, children);

    /*
     * Wait for synchronous replication, if required.
     */
    if wrote_xlog && mark_xid_committed {
        SyncRepWaitForLSN(XactLastRecEnd, true);
    }

    /* remember end of last commit record */
    XactLastCommitEnd = XactLastRecEnd;

    /* Reset XactLastRecEnd until the next transaction writes something */
    XactLastRecEnd = 0;

    /* cleanup: */
    if !rels.is_null() { pfree(rels as *mut std::ffi::c_void); }
    if ndroppedstats != 0 { pfree(droppedstats as *mut std::ffi::c_void); }

    latest_xid
}


/*
 *    AtCCI_LocalCache
 */
unsafe fn AtCCI_LocalCache() {
    /*
     * Make any pending relation map changes visible.
     */
    AtCCI_RelationMap();

    /*
     * Make catalog changes visible to me for the next command.
     */
    CommandEndInvalidationMessages();
}

/*
 *    AtCommit_Memory
 */
unsafe fn AtCommit_Memory() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * Return to the memory context that was current before we started the
     * transaction.
     */
    MemoryContextSwitchTo((*s).priorContext);

    /*
     * Release all transaction-local memory.
     */
    Assert!(!TopTransactionContext.is_null());
    MemoryContextReset(TopTransactionContext as _);

    /*
     * Clear these pointers as a pro-forma matter.
     */
    CurTransactionContext = ptr::null_mut();
    (*s).curTransactionContext = ptr::null_mut();
}

/* ----------------------------------------------------------------
 *                CommitSubTransaction stuff
 * ---------------------------------------------------------------- */

/*
 * AtSubCommit_Memory
 */
unsafe fn AtSubCommit_Memory() {
    let s: TransactionState = CurrentTransactionState;

    Assert!(!(*s).parent.is_null());

    /* Return to parent transaction level's memory context. */
    CurTransactionContext = (*(*s).parent).curTransactionContext;
    MemoryContextSwitchTo(CurTransactionContext);

    /*
     * Ordinarily we cannot throw away the child's CurTransactionContext.
     * However, if there isn't actually anything in it, we can throw it away.
     */
    if MemoryContextIsEmpty((*s).curTransactionContext) {
        MemoryContextDelete((*s).curTransactionContext);
        (*s).curTransactionContext = ptr::null_mut();
    }
}

/*
 * AtSubCommit_childXids
 *
 * Pass my own XID and my child XIDs up to my parent as committed children.
 */
unsafe fn AtSubCommit_childXids() {
    let s: TransactionState = CurrentTransactionState;
    let new_n_child_xids: i32;

    Assert!(!(*s).parent.is_null());

    /*
     * The parent childXids array will need to hold my XID and all my
     * childXids, in addition to the XIDs already there.
     */
    new_n_child_xids = (*(*s).parent).nChildXids + (*s).nChildXids + 1;

    /* Allocate or enlarge the parent array if necessary */
    if (*(*s).parent).maxChildXids < new_n_child_xids {
        let new_max_child_xids: i32;
        let new_child_xids: *mut TransactionId;

        /*
         * Make it 2x what's needed right now, to avoid having to enlarge it
         * repeatedly. But we can't go above MaxAllocSize.
         */
        new_max_child_xids = std::cmp::min(
            new_n_child_xids * 2,
            (MaxAllocSize / core::mem::size_of::<TransactionId>()) as i32);

        if new_max_child_xids < new_n_child_xids {
            ereport!(ERROR,
                errmsg!("maximum number of committed subtransactions ({}) exceeded",
                    (MaxAllocSize / core::mem::size_of::<TransactionId>())));
            /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        }

        /*
         * We keep the child-XID arrays in TopTransactionContext.
         */
        if (*(*s).parent).childXids.is_null() {
            new_child_xids = MemoryContextAlloc(
                TopTransactionContext as _,
                new_max_child_xids as usize * core::mem::size_of::<TransactionId>())
                as *mut TransactionId;
        } else {
            new_child_xids = repalloc(
                (*(*s).parent).childXids as *mut std::ffi::c_void,
                new_max_child_xids as usize * core::mem::size_of::<TransactionId>())
                as *mut TransactionId;
        }

        (*(*s).parent).childXids = new_child_xids;
        (*(*s).parent).maxChildXids = new_max_child_xids;
    }

    /*
     * Copy all my XIDs to parent's array.
     *
     * Note: We rely on the fact that the XID of a child always follows that
     * of its parent.
     */
    *(*(*s).parent).childXids.add((*(*s).parent).nChildXids as usize) =
        XidFromFullTransactionId((*s).fullTransactionId);

    if (*s).nChildXids > 0 {
        ptr::copy_nonoverlapping(
            (*s).childXids,
            (*(*s).parent).childXids.add(((*(*s).parent).nChildXids + 1) as usize),
            (*s).nChildXids as usize);
    }

    (*(*s).parent).nChildXids = new_n_child_xids;

    /* Release child's array to avoid leakage */
    if !(*s).childXids.is_null() {
        pfree((*s).childXids as *mut std::ffi::c_void);
    }
    /* We must reset these to avoid double-free if fail later in commit */
    (*s).childXids = ptr::null_mut();
    (*s).nChildXids = 0;
    (*s).maxChildXids = 0;
}

/* ----------------------------------------------------------------
 *                AbortTransaction stuff
 * ---------------------------------------------------------------- */

/*
 *    RecordTransactionAbort
 *
 * Returns latest XID among xact and its children, or InvalidTransactionId
 * if the xact has no XID.
 */
unsafe fn RecordTransactionAbort(is_sub_xact: bool) -> TransactionId {
    let xid: TransactionId = GetCurrentTransactionIdIfAny();
    let latest_xid: TransactionId;
    let mut nrels: i32;
    let mut rels: *mut RelFileLocator = ptr::null_mut();
    let mut ndroppedstats: i32 = 0;
    let mut droppedstats: *mut xl_xact_stats_item = ptr::null_mut();
    let mut nchildren: i32;
    let mut children: *mut TransactionId = ptr::null_mut();
    let xact_time: TimestampTz;
    let replorigin: bool;

    /*
     * If we haven't been assigned an XID, nobody will care whether we aborted
     * or not.
     */
    if !TransactionIdIsValid(xid) {
        /* Reset XactLastRecEnd until the next transaction writes something */
        if !is_sub_xact {
            XactLastRecEnd = 0;
        }
        return InvalidTransactionId;
    }

    /*
     * We have a valid XID, so we should write an ABORT record for it.
     */

    /*
     * Check that we haven't aborted halfway through RecordTransactionCommit.
     */
    if TransactionIdDidCommit(xid) {
        elog!(PANIC, "cannot abort transaction {}, it was already committed", xid);
    }

    /*
     * Are we using the replication origins feature?
     */
    replorigin = replorigin_session_origin != InvalidRepOriginId &&
                 replorigin_session_origin != DoNotReplicateId;

    /* Fetch the data we need for the abort record */
    nrels = smgrGetPendingDeletes(false, &mut rels);
    nchildren = xactGetCommittedChildren(&mut children);
    ndroppedstats = pgstat_get_transactional_drops(false, &mut droppedstats);

    /* XXX do we really need a critical section here? */
    START_CRIT_SECTION!();

    /* Write the ABORT record */
    if is_sub_xact {
        xact_time = GetCurrentTimestamp();
    } else {
        xact_time = GetCurrentTransactionStopTimestamp();
    }

    XactLogAbortRecord(xact_time,
                       nchildren, children,
                       nrels, rels,
                       ndroppedstats, droppedstats,
                       MyXactFlags, InvalidTransactionId,
                       ptr::null());

    if replorigin {
        /* Move LSNs forward for this replication origin */
        replorigin_session_advance(replorigin_session_origin_lsn,
                                   XactLastRecEnd);
    }

    /*
     * Report the latest async abort LSN, so that the WAL writer knows to
     * flush this abort.
     */
    if !is_sub_xact {
        XLogSetAsyncXactLSN(XactLastRecEnd);
    }

    /*
     * Mark the transaction aborted in clog.
     */
    TransactionIdAbortTree(xid, nchildren, children);

    END_CRIT_SECTION!();

    /* Compute latestXid while we have the child XIDs handy */
    latest_xid = TransactionIdLatest(xid, nchildren, children);

    /*
     * If we're aborting a subtransaction, we can immediately remove failed
     * XIDs from PGPROC's cache of running child XIDs.
     */
    if is_sub_xact {
        XidCacheRemoveRunningXids(xid, nchildren, children, latest_xid);
    }

    /* Reset XactLastRecEnd until the next transaction writes something */
    if !is_sub_xact {
        XactLastRecEnd = 0;
    }

    /* And clean up local data */
    if !rels.is_null() { pfree(rels as *mut std::ffi::c_void); }
    if ndroppedstats != 0 { pfree(droppedstats as *mut std::ffi::c_void); }

    latest_xid
}

/*
 *    AtAbort_Memory
 */
unsafe fn AtAbort_Memory() {
    /*
     * Switch into TransactionAbortContext, which should have some free space
     * even if nothing else does.
     */
    if !TransactionAbortContext.is_null() {
        MemoryContextSwitchTo(TransactionAbortContext);
    } else {
        MemoryContextSwitchTo(TopMemoryContext as _);
    }
}

/*
 * AtSubAbort_Memory
 */
unsafe fn AtSubAbort_Memory() {
    Assert!(!TransactionAbortContext.is_null());

    MemoryContextSwitchTo(TransactionAbortContext);
}


/*
 *    AtAbort_ResourceOwner
 */
unsafe fn AtAbort_ResourceOwner() {
    /*
     * Make sure we have a valid ResourceOwner, if possible
     */
    CurrentResourceOwner = TopTransactionResourceOwner;
}

/*
 * AtSubAbort_ResourceOwner
 */
unsafe fn AtSubAbort_ResourceOwner() {
    let s: TransactionState = CurrentTransactionState;

    /* Make sure we have a valid ResourceOwner */
    CurrentResourceOwner = (*s).curTransactionOwner;
}


/*
 * AtSubAbort_childXids
 */
unsafe fn AtSubAbort_childXids() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * We keep the child-XID arrays in TopTransactionContext (see
     * AtSubCommit_childXids).
     */
    if !(*s).childXids.is_null() {
        pfree((*s).childXids as *mut std::ffi::c_void);
    }
    (*s).childXids = ptr::null_mut();
    (*s).nChildXids = 0;
    (*s).maxChildXids = 0;

    /*
     * We could prune the unreportedXids array here. But we don't bother.
     */
}

/* ----------------------------------------------------------------
 *                CleanupTransaction stuff
 * ---------------------------------------------------------------- */

/*
 *    AtCleanup_Memory
 */
unsafe fn AtCleanup_Memory() {
    let s: TransactionState = CurrentTransactionState;

    /* Should be at top level */
    Assert!((*s).parent.is_null());

    /*
     * Return to the memory context that was current before we started the
     * transaction.
     */
    MemoryContextSwitchTo((*s).priorContext);

    /*
     * Clear the special abort context for next time.
     */
    if !TransactionAbortContext.is_null() {
        MemoryContextReset(TransactionAbortContext);
    }

    /*
     * Release all transaction-local memory, the same as in AtCommit_Memory,
     * except we must cope with the possibility that we didn't get as far as
     * creating TopTransactionContext.
     */
    if !TopTransactionContext.is_null() {
        MemoryContextReset(TopTransactionContext as _);
    }

    /*
     * Clear these pointers as a pro-forma matter.
     */
    CurTransactionContext = ptr::null_mut();
    (*s).curTransactionContext = ptr::null_mut();
}


/* ----------------------------------------------------------------
 *                CleanupSubTransaction stuff
 * ---------------------------------------------------------------- */

/*
 * AtSubCleanup_Memory
 */
unsafe fn AtSubCleanup_Memory() {
    let s: TransactionState = CurrentTransactionState;

    Assert!(!(*s).parent.is_null());

    /*
     * Return to the memory context that was current before we started the
     * subtransaction.
     */
    MemoryContextSwitchTo((*s).priorContext);

    /* Update CurTransactionContext (might not be same as priorContext) */
    CurTransactionContext = (*(*s).parent).curTransactionContext;

    /*
     * Clear the special abort context for next time.
     */
    if !TransactionAbortContext.is_null() {
        MemoryContextReset(TransactionAbortContext);
    }

    /*
     * Delete the subxact local memory contexts.
     */
    if !(*s).curTransactionContext.is_null() {
        MemoryContextDelete((*s).curTransactionContext);
    }
    (*s).curTransactionContext = ptr::null_mut();
}

/* ----------------------------------------------------------------
 *                interface routines
 * ---------------------------------------------------------------- */

/*
 *    StartTransaction
 */
unsafe fn StartTransaction() {
    let s: TransactionState;
    let mut vxid = VirtualTransactionId::default();

    /*
     * Let's just make sure the state stack is empty
     */
    s = &mut TopTransactionStateData as TransactionState;
    CurrentTransactionState = s;

    Assert!(!FullTransactionIdIsValid(XactTopFullTransactionId));

    /* check the current transaction state */
    Assert!((*s).state == TRANS_DEFAULT);

    /*
     * Set the current transaction state information appropriately during
     * start processing.
     */
    (*s).state = TRANS_START;
    (*s).fullTransactionId = InvalidFullTransactionId(); /* until assigned */

    /* Determine if statements are logged in this transaction */
    xact_is_sampled = log_xact_sample_rate != 0.0 &&
        (log_xact_sample_rate == 1.0 ||
         pg_prng_double(pg_global_prng_state) <= log_xact_sample_rate);

    /*
     * initialize current transaction state fields
     *
     * note: prevXactReadOnly is not used at the outermost level
     */
    (*s).nestingLevel = 1;
    (*s).gucNestLevel = 1;
    (*s).childXids = ptr::null_mut();
    (*s).nChildXids = 0;
    (*s).maxChildXids = 0;

    /*
     * Once the current user ID and the security context flags are fetched,
     * both will be properly reset even if transaction startup fails.
     */
    GetUserIdAndSecContext(&mut (*s).prevUser, &mut (*s).prevSecContext);

    /* SecurityRestrictionContext should never be set outside a transaction */
    Assert!((*s).prevSecContext == 0);

    /*
     * Make sure we've reset xact state variables
     *
     * If recovery is still in progress, mark this transaction as read-only.
     */
    if RecoveryInProgress() {
        (*s).startedInRecovery = true;
        XactReadOnly = true;
    } else {
        (*s).startedInRecovery = false;
        XactReadOnly = DefaultXactReadOnly;
    }
    XactDeferrable = DefaultXactDeferrable;
    XactIsoLevel = DefaultXactIsoLevel;
    forceSyncCommit = false;
    MyXactFlags = 0;

    /*
     * reinitialize within-transaction counters
     */
    (*s).subTransactionId = TopSubTransactionId;
    currentSubTransactionId = TopSubTransactionId;
    currentCommandId = FirstCommandId;
    currentCommandIdUsed = false;

    /*
     * initialize reported xid accounting
     */
    nUnreportedXids = 0;
    (*s).didLogXid = false;

    /*
     * must initialize resource-management stuff first
     */
    AtStart_Memory();
    AtStart_ResourceOwner();

    /*
     * Assign a new LocalTransactionId, and combine it with the proc number to
     * form a virtual transaction id.
     */
    vxid.procNumber = MyProcNumber;
    vxid.localTransactionId = GetNextLocalTransactionId();

    /*
     * Lock the virtual transaction id before we announce it in the proc array
     */
    VirtualXactLockTableInsert(vxid);

    /*
     * Advertise it in the proc array.
     */
    Assert!((*MyProc).vxid.procNumber == vxid.procNumber);
    (*MyProc).vxid.lxid = vxid.localTransactionId;

    TRACE_POSTGRESQL_TRANSACTION_START(vxid.localTransactionId);

    /*
     * set transaction_timestamp() (a/k/a now()).
     */
    if !IsParallelWorker() {
        if !SPI_inside_nonatomic_context() {
            xactStartTimestamp = stmtStartTimestamp;
        } else {
            xactStartTimestamp = GetCurrentTimestamp();
        }
    } else {
        Assert!(xactStartTimestamp != 0);
    }
    pgstat_report_xact_timestamp(xactStartTimestamp);
    /* Mark xactStopTimestamp as unset. */
    xactStopTimestamp = 0;

    /*
     * initialize other subsystems for new transaction
     */
    AtStart_GUC();
    AtStart_Cache();
    AfterTriggerBeginXact();

    /*
     * done with start processing, set current transaction state to "in
     * progress"
     */
    (*s).state = TRANS_INPROGRESS;

    /* Schedule transaction timeout */
    if TransactionTimeout > 0 {
        enable_timeout_after(TRANSACTION_TIMEOUT, TransactionTimeout);
    }

    ShowTransactionState(b"StartTransaction\0".as_ptr() as *const _);
}


/*
 *    CommitTransaction
 *
 * NB: if you change this routine, better look at PrepareTransaction too!
 */
unsafe fn CommitTransaction() {
    let s: TransactionState = CurrentTransactionState;
    let latest_xid: TransactionId;
    let is_parallel_worker: bool;

    is_parallel_worker = (*s).blockState == TBLOCK_PARALLEL_INPROGRESS;

    /* Enforce parallel mode restrictions during parallel worker commit. */
    if is_parallel_worker {
        EnterParallelMode();
    }

    ShowTransactionState(b"CommitTransaction\0".as_ptr() as *const _);

    /*
     * check the current transaction state
     */
    if (*s).state != TRANS_INPROGRESS {
        elog!(WARNING, "CommitTransaction while in {} state",
             TransStateAsString((*s).state));
    }
    Assert!((*s).parent.is_null());

    /*
     * Do pre-commit processing that involves calling user-defined code.
     * We have to keep looping until there's nothing left to do.
     */
    loop {
        /*
         * Fire all currently pending deferred triggers.
         */
        AfterTriggerFireDeferred();

        /*
         * Close open portals (converting holdable ones into static portals).
         */
        if !PreCommit_Portals(false) {
            break;
        }
    }

    /*
     * The remaining actions cannot call any user-defined code.
     */

    CallXactCallbacks(if is_parallel_worker { XACT_EVENT_PARALLEL_PRE_COMMIT }
                      else { XACT_EVENT_PRE_COMMIT });

    /*
     * If this xact has started any unfinished parallel operation, clean up.
     */
    AtEOXact_Parallel(true);
    if is_parallel_worker {
        if (*s).parallelModeLevel != 1 {
            elog!(WARNING, "parallelModeLevel is {} not 1 at end of parallel worker transaction",
                 (*s).parallelModeLevel);
        }
    } else {
        if (*s).parallelModeLevel != 0 {
            elog!(WARNING, "parallelModeLevel is {} not 0 at end of transaction",
                 (*s).parallelModeLevel);
        }
    }

    /* Shut down the deferred-trigger manager */
    AfterTriggerEndXact(true);

    /*
     * Let ON COMMIT management do its thing.
     */
    PreCommit_on_commit_actions();

    /*
     * Synchronize files that are created and not WAL-logged during this
     * transaction.
     */
    smgrDoPendingSyncs(true, is_parallel_worker);

    /* close large objects before lower-level cleanup */
    AtEOXact_LargeObject(true);

    /*
     * Insert notifications sent by NOTIFY commands into the queue.
     */
    PreCommit_Notify();

    /*
     * Mark serializable transaction as complete for predicate locking
     * purposes.
     */
    if !is_parallel_worker {
        PreCommit_CheckForSerializationFailure();
    }

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS!();

    /* Commit updates to the relation map --- do this as late as possible */
    AtEOXact_RelationMap(true, is_parallel_worker);

    /*
     * set the current transaction state information appropriately during
     * commit processing
     */
    (*s).state = TRANS_COMMIT;
    (*s).parallelModeLevel = 0;
    (*s).parallelChildXact = false; /* should be false already */

    /* Disable transaction timeout */
    if TransactionTimeout > 0 {
        disable_timeout(TRANSACTION_TIMEOUT, false);
    }

    if !is_parallel_worker {
        /*
         * We need to mark our XIDs as committed in pg_xact.
         */
        latest_xid = RecordTransactionCommit();
    } else {
        /*
         * We must not mark our XID committed; the parallel leader is
         * responsible for that.
         */
        /* latestXid = InvalidTransactionId; */
        let _ = InvalidTransactionId;

        /*
         * Make sure the leader will know about any WAL we wrote before it
         * commits.
         */
        ParallelWorkerReportLastRecEnd(XactLastRecEnd);
        latest_xid = InvalidTransactionId;
    }

    TRACE_POSTGRESQL_TRANSACTION_COMMIT((*MyProc).vxid.lxid);

    /*
     * Let others know about no transaction in progress by me.
     */
    ProcArrayEndTransaction(MyProc, latest_xid);

    /*
     * This is all post-commit cleanup.
     */

    CallXactCallbacks(if is_parallel_worker { XACT_EVENT_PARALLEL_COMMIT }
                      else { XACT_EVENT_COMMIT });

    CurrentResourceOwner = ptr::null_mut();
    ResourceOwnerRelease(TopTransactionResourceOwner,
                         RESOURCE_RELEASE_BEFORE_LOCKS, true, true);

    AtEOXact_Aio(true);

    /* Check we've released all buffer pins */
    AtEOXact_Buffers(true);

    /* Clean up the relation cache */
    AtEOXact_RelationCache(true);

    /* Clean up the type cache */
    AtEOXact_TypeCache();

    /*
     * Make catalog changes visible to all backends.
     */
    AtEOXact_Inval(true);

    AtEOXact_MultiXact();

    ResourceOwnerRelease(TopTransactionResourceOwner,
                         RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(TopTransactionResourceOwner,
                         RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    /*
     * Likewise, dropping of files deleted during the transaction.
     */
    smgrDoPendingDeletes(true);

    /*
     * Send out notification signals to other backends.
     */
    AtCommit_Notify();

    /*
     * Everything after this should be purely internal-to-this-backend cleanup.
     */
    AtEOXact_GUC(true, 1);
    AtEOXact_SPI(true);
    AtEOXact_Enum();
    AtEOXact_on_commit_actions(true);
    AtEOXact_Namespace(true, is_parallel_worker);
    AtEOXact_SMgr();
    AtEOXact_Files(true);
    AtEOXact_ComboCid();
    AtEOXact_HashTables(true);
    AtEOXact_PgStat(true, is_parallel_worker);
    AtEOXact_Snapshot(true, false);
    AtEOXact_ApplyLauncher(true);
    AtEOXact_LogicalRepWorkers(true);
    pgstat_report_xact_timestamp(0);

    ResourceOwnerDelete(TopTransactionResourceOwner);
    (*s).curTransactionOwner = ptr::null_mut();
    CurTransactionResourceOwner = ptr::null_mut();
    TopTransactionResourceOwner = ptr::null_mut();

    AtCommit_Memory();

    (*s).fullTransactionId = InvalidFullTransactionId();
    (*s).subTransactionId = InvalidSubTransactionId;
    (*s).nestingLevel = 0;
    (*s).gucNestLevel = 0;
    (*s).childXids = ptr::null_mut();
    (*s).nChildXids = 0;
    (*s).maxChildXids = 0;

    XactTopFullTransactionId = InvalidFullTransactionId();
    nParallelCurrentXids = 0;

    /*
     * done with commit processing, set current transaction state back to
     * default
     */
    (*s).state = TRANS_DEFAULT;

    RESUME_INTERRUPTS!();
}

/*
 *    PrepareTransaction
 *
 * NB: if you change this routine, better look at CommitTransaction too!
 */
unsafe fn PrepareTransaction() {
    let s: TransactionState = CurrentTransactionState;
    let xid: TransactionId = GetCurrentTransactionId();
    let gxact: GlobalTransaction;
    let prepared_at: TimestampTz;

    Assert!(!IsInParallelMode());

    ShowTransactionState(b"PrepareTransaction\0".as_ptr() as *const _);

    /*
     * check the current transaction state
     */
    if (*s).state != TRANS_INPROGRESS {
        elog!(WARNING, "PrepareTransaction while in {} state",
             TransStateAsString((*s).state));
    }
    Assert!((*s).parent.is_null());

    /*
     * Do pre-commit processing that involves calling user-defined code.
     */
    loop {
        AfterTriggerFireDeferred();
        if !PreCommit_Portals(true) { break; }
    }

    CallXactCallbacks(XACT_EVENT_PRE_PREPARE);

    /* Shut down the deferred-trigger manager */
    AfterTriggerEndXact(true);

    /*
     * Let ON COMMIT management do its thing.
     */
    PreCommit_on_commit_actions();

    /*
     * Synchronize files that are created and not WAL-logged during this
     * transaction.
     */
    smgrDoPendingSyncs(true, false);

    /* close large objects before lower-level cleanup */
    AtEOXact_LargeObject(true);

    /* NOTIFY requires no work at this point */

    /*
     * Mark serializable transaction as complete for predicate locking
     * purposes.
     */
    PreCommit_CheckForSerializationFailure();

    /*
     * Don't allow PREPARE TRANSACTION if we've accessed a temporary table.
     */
    if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0 {
        ereport!(ERROR,
            errmsg!("cannot PREPARE a transaction that has operated on temporary objects"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /*
     * Likewise, don't allow PREPARE after pg_export_snapshot.
     */
    if XactHasExportedSnapshots() {
        ereport!(ERROR,
            errmsg!("cannot PREPARE a transaction that has exported snapshots"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS!();

    /*
     * set the current transaction state information appropriately during
     * prepare processing
     */
    (*s).state = TRANS_PREPARE;

    /* Disable transaction timeout */
    if TransactionTimeout > 0 {
        disable_timeout(TRANSACTION_TIMEOUT, false);
    }

    prepared_at = GetCurrentTimestamp();

    /*
     * Reserve the GID for this transaction.
     */
    gxact = MarkAsPreparing(xid, prepareGID, prepared_at,
                            GetUserId(), MyDatabaseId);
    prepareGID = ptr::null_mut();

    /*
     * Collect data for the 2PC state file.
     */
    StartPrepare(gxact);

    AtPrepare_Notify();
    AtPrepare_Locks();
    AtPrepare_PredicateLocks();
    AtPrepare_PgStat();
    AtPrepare_MultiXact();
    AtPrepare_RelationMap();

    /*
     * Here is where we really truly prepare.
     */
    EndPrepare(gxact);

    /*
     * Now we clean up backend-internal state and release internal resources.
     */

    /* Reset XactLastRecEnd until the next transaction writes something */
    XactLastRecEnd = 0;

    /*
     * Transfer our locks to a dummy PGPROC.
     */
    PostPrepare_Locks(xid);

    /*
     * Let others know about no transaction in progress by me.
     */
    ProcArrayClearTransaction(MyProc);

    /*
     * In normal commit-processing, this is all non-critical post-transaction
     * cleanup.
     */

    CallXactCallbacks(XACT_EVENT_PREPARE);

    ResourceOwnerRelease(TopTransactionResourceOwner,
                         RESOURCE_RELEASE_BEFORE_LOCKS, true, true);

    AtEOXact_Aio(true);

    /* Check we've released all buffer pins */
    AtEOXact_Buffers(true);

    /* Clean up the relation cache */
    AtEOXact_RelationCache(true);

    /* Clean up the type cache */
    AtEOXact_TypeCache();

    /* notify doesn't need a postprepare call */

    PostPrepare_PgStat();
    PostPrepare_Inval();
    PostPrepare_smgr();
    PostPrepare_MultiXact(xid);
    PostPrepare_PredicateLocks(xid);

    ResourceOwnerRelease(TopTransactionResourceOwner,
                         RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(TopTransactionResourceOwner,
                         RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    /*
     * Allow another backend to finish the transaction.
     */
    PostPrepare_Twophase();

    /* PREPARE acts the same as COMMIT as far as GUC is concerned */
    AtEOXact_GUC(true, 1);
    AtEOXact_SPI(true);
    AtEOXact_Enum();
    AtEOXact_on_commit_actions(true);
    AtEOXact_Namespace(true, false);
    AtEOXact_SMgr();
    AtEOXact_Files(true);
    AtEOXact_ComboCid();
    AtEOXact_HashTables(true);
    /* don't call AtEOXact_PgStat here; we fixed pgstat state above */
    AtEOXact_Snapshot(true, true);
    /* we treat PREPARE as ROLLBACK so far as waking workers goes */
    AtEOXact_ApplyLauncher(false);
    AtEOXact_LogicalRepWorkers(false);
    pgstat_report_xact_timestamp(0);

    CurrentResourceOwner = ptr::null_mut();
    ResourceOwnerDelete(TopTransactionResourceOwner);
    (*s).curTransactionOwner = ptr::null_mut();
    CurTransactionResourceOwner = ptr::null_mut();
    TopTransactionResourceOwner = ptr::null_mut();

    AtCommit_Memory();

    (*s).fullTransactionId = InvalidFullTransactionId();
    (*s).subTransactionId = InvalidSubTransactionId;
    (*s).nestingLevel = 0;
    (*s).gucNestLevel = 0;
    (*s).childXids = ptr::null_mut();
    (*s).nChildXids = 0;
    (*s).maxChildXids = 0;

    XactTopFullTransactionId = InvalidFullTransactionId();
    nParallelCurrentXids = 0;

    /*
     * done with 1st phase commit processing, set current transaction state
     * back to default
     */
    (*s).state = TRANS_DEFAULT;

    RESUME_INTERRUPTS!();
}


/*
 *    AbortTransaction
 */
unsafe fn AbortTransaction() {
    let s: TransactionState = CurrentTransactionState;
    let latest_xid: TransactionId;
    let is_parallel_worker: bool;

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS!();

    /* Disable transaction timeout */
    if TransactionTimeout > 0 {
        disable_timeout(TRANSACTION_TIMEOUT, false);
    }

    /* Make sure we have a valid memory context and resource owner */
    AtAbort_Memory();
    AtAbort_ResourceOwner();

    /*
     * Release any LW locks we might be holding as quickly as possible.
     */
    LWLockReleaseAll();

    /* Clear wait information and command progress indicator */
    pgstat_report_wait_end();
    pgstat_progress_end_command();

    pgaio_error_cleanup();

    /* Clean up buffer content locks, too */
    UnlockBuffers();

    /* Reset WAL record construction state */
    XLogResetInsertion();

    /* Cancel condition variable sleep */
    ConditionVariableCancelSleep();

    /*
     * Also clean up any open wait for lock.
     */
    LockErrorCleanup();

    /*
     * If any timeout events are still active, make sure the timeout interrupt
     * is scheduled.
     */
    reschedule_timeouts();

    /*
     * Re-enable signals, in case we got here by longjmp'ing out of a signal
     * handler.
     */
    sigprocmask(SIG_SETMASK, &UnBlockSig as *const i64 as *const std::ffi::c_void, ptr::null_mut());

    /*
     * check the current transaction state
     */
    is_parallel_worker = (*s).blockState == TBLOCK_PARALLEL_INPROGRESS;
    if (*s).state != TRANS_INPROGRESS && (*s).state != TRANS_PREPARE {
        elog!(WARNING, "AbortTransaction while in {} state",
             TransStateAsString((*s).state));
    }
    Assert!((*s).parent.is_null());

    /*
     * set the current transaction state information appropriately during the
     * abort processing
     */
    (*s).state = TRANS_ABORT;

    /*
     * Reset user ID which might have been changed transiently.
     */
    SetUserIdAndSecContext((*s).prevUser, (*s).prevSecContext);

    /* Forget about any active REINDEX. */
    ResetReindexState((*s).nestingLevel);

    /* Reset logical streaming state. */
    ResetLogicalStreamingState();

    /* Reset snapshot export state. */
    SnapBuildResetExportedSnapshotState();

    /*
     * If this xact has started any unfinished parallel operation, clean up.
     */
    AtEOXact_Parallel(false);
    (*s).parallelModeLevel = 0;
    (*s).parallelChildXact = false; /* should be false already */

    /*
     * do abort processing
     */
    AfterTriggerEndXact(false); /* 'false' means it's abort */
    AtAbort_Portals();
    smgrDoPendingSyncs(false, is_parallel_worker);
    AtEOXact_LargeObject(false);
    AtAbort_Notify();
    AtEOXact_RelationMap(false, is_parallel_worker);
    AtAbort_Twophase();

    /*
     * Advertise the fact that we aborted in pg_xact.
     */
    if !is_parallel_worker {
        latest_xid = RecordTransactionAbort(false);
    } else {
        /* latestXid = InvalidTransactionId; */

        /*
         * Since the parallel leader won't get our value of XactLastRecEnd in
         * this case, we nudge WAL-writer ourselves.
         */
        XLogSetAsyncXactLSN(XactLastRecEnd);
        latest_xid = InvalidTransactionId;
    }

    TRACE_POSTGRESQL_TRANSACTION_ABORT((*MyProc).vxid.lxid);

    /*
     * Let others know about no transaction in progress by me.
     */
    ProcArrayEndTransaction(MyProc, latest_xid);

    /*
     * Post-abort cleanup.  We can skip all of it if the transaction failed
     * before creating a resource owner.
     */
    if !TopTransactionResourceOwner.is_null() {
        if is_parallel_worker {
            CallXactCallbacks(XACT_EVENT_PARALLEL_ABORT);
        } else {
            CallXactCallbacks(XACT_EVENT_ABORT);
        }

        ResourceOwnerRelease(TopTransactionResourceOwner,
                             RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        AtEOXact_Aio(false);
        AtEOXact_Buffers(false);
        AtEOXact_RelationCache(false);
        AtEOXact_TypeCache();
        AtEOXact_Inval(false);
        AtEOXact_MultiXact();
        ResourceOwnerRelease(TopTransactionResourceOwner,
                             RESOURCE_RELEASE_LOCKS, false, true);
        ResourceOwnerRelease(TopTransactionResourceOwner,
                             RESOURCE_RELEASE_AFTER_LOCKS, false, true);
        smgrDoPendingDeletes(false);

        AtEOXact_GUC(false, 1);
        AtEOXact_SPI(false);
        AtEOXact_Enum();
        AtEOXact_on_commit_actions(false);
        AtEOXact_Namespace(false, is_parallel_worker);
        AtEOXact_SMgr();
        AtEOXact_Files(false);
        AtEOXact_ComboCid();
        AtEOXact_HashTables(false);
        AtEOXact_PgStat(false, is_parallel_worker);
        AtEOXact_ApplyLauncher(false);
        AtEOXact_LogicalRepWorkers(false);
        pgstat_report_xact_timestamp(0);
    }

    /*
     * State remains TRANS_ABORT until CleanupTransaction().
     */
    RESUME_INTERRUPTS!();
}

/*
 *    CleanupTransaction
 */
unsafe fn CleanupTransaction() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * State should still be TRANS_ABORT from AbortTransaction().
     */
    if (*s).state != TRANS_ABORT {
        elog!(FATAL, "CleanupTransaction: unexpected state {}",
             TransStateAsString((*s).state));
    }

    /*
     * do abort cleanup processing
     */
    AtCleanup_Portals();        /* now safe to release portal memory */
    AtEOXact_Snapshot(false, true); /* and release the transaction's snapshots */

    CurrentResourceOwner = ptr::null_mut(); /* and resource owner */
    if !TopTransactionResourceOwner.is_null() {
        ResourceOwnerDelete(TopTransactionResourceOwner);
    }
    (*s).curTransactionOwner = ptr::null_mut();
    CurTransactionResourceOwner = ptr::null_mut();
    TopTransactionResourceOwner = ptr::null_mut();

    AtCleanup_Memory();         /* and transaction memory */

    (*s).fullTransactionId = InvalidFullTransactionId();
    (*s).subTransactionId = InvalidSubTransactionId;
    (*s).nestingLevel = 0;
    (*s).gucNestLevel = 0;
    (*s).childXids = ptr::null_mut();
    (*s).nChildXids = 0;
    (*s).maxChildXids = 0;
    (*s).parallelModeLevel = 0;
    (*s).parallelChildXact = false;

    XactTopFullTransactionId = InvalidFullTransactionId();
    nParallelCurrentXids = 0;

    /*
     * done with abort processing, set current transaction state back to
     * default
     */
    (*s).state = TRANS_DEFAULT;
}

/*
 *    StartTransactionCommand
 */
#[no_mangle]
pub unsafe fn StartTransactionCommand() {
    let s: TransactionState = CurrentTransactionState;

    match (*s).blockState {
        /*
         * if we aren't in a transaction block, we just do our usual start
         * transaction.
         */
        TBLOCK_DEFAULT => {
            StartTransaction();
            (*s).blockState = TBLOCK_STARTED;
        }

        /*
         * We are somewhere in a transaction block or subtransaction and
         * about to start a new command.
         */
        TBLOCK_INPROGRESS |
        TBLOCK_IMPLICIT_INPROGRESS |
        TBLOCK_SUBINPROGRESS => {}

        /*
         * Here we are in a failed transaction block (one of the commands
         * caused an abort) so we do nothing but remain in the abort state.
         */
        TBLOCK_ABORT |
        TBLOCK_SUBABORT => {}

        /* These cases are invalid. */
        TBLOCK_STARTED |
        TBLOCK_BEGIN |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(ERROR, "StartTransactionCommand: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    /*
     * We must switch to CurTransactionContext before returning.
     */
    Assert!(!CurTransactionContext.is_null());
    MemoryContextSwitchTo(CurTransactionContext);
}


/*
 * Simple system for saving and restoring transaction characteristics
 * (isolation level, read only, deferrable).
 */
pub unsafe fn SaveTransactionCharacteristics(s: *mut SavedTransactionCharacteristics) {
    (*s).save_XactIsoLevel = XactIsoLevel;
    (*s).save_XactReadOnly = XactReadOnly;
    (*s).save_XactDeferrable = XactDeferrable;
}

pub unsafe fn RestoreTransactionCharacteristics(s: *const SavedTransactionCharacteristics) {
    XactIsoLevel = (*s).save_XactIsoLevel;
    XactReadOnly = (*s).save_XactReadOnly;
    XactDeferrable = (*s).save_XactDeferrable;
}

/*
 *    CommitTransactionCommand
 */
#[no_mangle]
pub unsafe fn CommitTransactionCommand() {
    /*
     * Repeatedly call CommitTransactionCommandInternal() until all the work
     * is done.
     */
    while !CommitTransactionCommandInternal() {}
}

/*
 *    CommitTransactionCommandInternal
 */
unsafe fn CommitTransactionCommandInternal() -> bool {
    let s: TransactionState = CurrentTransactionState;
    let mut savetc = SavedTransactionCharacteristics::default();

    /* Must save in case we need to restore below */
    SaveTransactionCharacteristics(&mut savetc);

    match (*s).blockState {
        /*
         * These shouldn't happen.
         */
        TBLOCK_DEFAULT |
        TBLOCK_PARALLEL_INPROGRESS => {
            elog!(FATAL, "CommitTransactionCommand: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }

        /*
         * If we aren't in a transaction block, just do our usual transaction
         * commit.
         */
        TBLOCK_STARTED => {
            CommitTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * We are completing a "BEGIN TRANSACTION" command.
         */
        TBLOCK_BEGIN => {
            (*s).blockState = TBLOCK_INPROGRESS;
        }

        /*
         * This is the case when we have finished executing a command inside
         * a transaction block.
         */
        TBLOCK_INPROGRESS |
        TBLOCK_IMPLICIT_INPROGRESS |
        TBLOCK_SUBINPROGRESS => {
            CommandCounterIncrement();
        }

        /*
         * We are completing a "COMMIT" command.
         */
        TBLOCK_END => {
            CommitTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
            if (*s).chain {
                StartTransaction();
                (*s).blockState = TBLOCK_INPROGRESS;
                (*s).chain = false;
                RestoreTransactionCharacteristics(&savetc);
            }
        }

        /*
         * Here we are in the middle of a transaction block but one of the
         * commands caused an abort.
         */
        TBLOCK_ABORT |
        TBLOCK_SUBABORT => {}

        /*
         * Here we were in an aborted transaction block and we just got
         * the ROLLBACK command.
         */
        TBLOCK_ABORT_END => {
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
            if (*s).chain {
                StartTransaction();
                (*s).blockState = TBLOCK_INPROGRESS;
                (*s).chain = false;
                RestoreTransactionCharacteristics(&savetc);
            }
        }

        /*
         * Here we were in a perfectly good transaction block but the user
         * told us to ROLLBACK anyway.
         */
        TBLOCK_ABORT_PENDING => {
            AbortTransaction();
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
            if (*s).chain {
                StartTransaction();
                (*s).blockState = TBLOCK_INPROGRESS;
                (*s).chain = false;
                RestoreTransactionCharacteristics(&savetc);
            }
        }

        /*
         * We are completing a "PREPARE TRANSACTION" command.
         */
        TBLOCK_PREPARE => {
            PrepareTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * The user issued a SAVEPOINT inside a transaction block.
         */
        TBLOCK_SUBBEGIN => {
            StartSubTransaction();
            (*s).blockState = TBLOCK_SUBINPROGRESS;
        }

        /*
         * The user issued a RELEASE command.
         */
        TBLOCK_SUBRELEASE => {
            let mut s2 = s;
            loop {
                CommitSubTransaction();
                s2 = CurrentTransactionState; /* changed by pop */
                if (*s2).blockState != TBLOCK_SUBRELEASE { break; }
            }

            Assert!((*CurrentTransactionState).blockState == TBLOCK_INPROGRESS ||
                    (*CurrentTransactionState).blockState == TBLOCK_SUBINPROGRESS);
        }

        /*
         * The user issued a COMMIT, so we end the current subtransaction
         * hierarchy.
         */
        TBLOCK_SUBCOMMIT => {
            let mut s2 = s;
            loop {
                CommitSubTransaction();
                s2 = CurrentTransactionState; /* changed by pop */
                if (*s2).blockState != TBLOCK_SUBCOMMIT { break; }
            }
            /* If we had a COMMIT command, finish off the main xact too */
            if (*CurrentTransactionState).blockState == TBLOCK_END {
                Assert!((*CurrentTransactionState).parent.is_null());
                CommitTransaction();
                (*CurrentTransactionState).blockState = TBLOCK_DEFAULT;
                if (*CurrentTransactionState).chain {
                    StartTransaction();
                    (*CurrentTransactionState).blockState = TBLOCK_INPROGRESS;
                    (*CurrentTransactionState).chain = false;
                    RestoreTransactionCharacteristics(&savetc);
                }
            } else if (*CurrentTransactionState).blockState == TBLOCK_PREPARE {
                Assert!((*CurrentTransactionState).parent.is_null());
                PrepareTransaction();
                (*CurrentTransactionState).blockState = TBLOCK_DEFAULT;
            } else {
                elog!(ERROR, "CommitTransactionCommand: unexpected state {}",
                     BlockStateAsString((*CurrentTransactionState).blockState));
            }
        }

        /*
         * The current already-failed subtransaction is ending due to a
         * ROLLBACK or ROLLBACK TO command.
         */
        TBLOCK_SUBABORT_END => {
            CleanupSubTransaction();
            return false;
        }

        /*
         * As above, but it's not dead yet, so abort first.
         */
        TBLOCK_SUBABORT_PENDING => {
            AbortSubTransaction();
            CleanupSubTransaction();
            return false;
        }

        /*
         * The current subtransaction is the target of a ROLLBACK TO command.
         */
        TBLOCK_SUBRESTART => {
            let name: *mut std::os::raw::c_char;
            let savepoint_level: i32;

            /* save name and keep Cleanup from freeing it */
            name = (*s).name;
            (*s).name = ptr::null_mut();
            savepoint_level = (*s).savepointLevel;

            AbortSubTransaction();
            CleanupSubTransaction();

            DefineSavepoint(ptr::null());
            let s2 = CurrentTransactionState; /* changed by push */
            (*s2).name = name;
            (*s2).savepointLevel = savepoint_level;

            /* This is the same as TBLOCK_SUBBEGIN case */
            Assert!((*s2).blockState == TBLOCK_SUBBEGIN);
            StartSubTransaction();
            (*s2).blockState = TBLOCK_SUBINPROGRESS;
        }

        /*
         * Same as above, but the subtransaction had already failed.
         */
        TBLOCK_SUBABORT_RESTART => {
            let name: *mut std::os::raw::c_char;
            let savepoint_level: i32;

            /* save name and keep Cleanup from freeing it */
            name = (*s).name;
            (*s).name = ptr::null_mut();
            savepoint_level = (*s).savepointLevel;

            CleanupSubTransaction();

            DefineSavepoint(ptr::null());
            let s2 = CurrentTransactionState; /* changed by push */
            (*s2).name = name;
            (*s2).savepointLevel = savepoint_level;

            /* This is the same as TBLOCK_SUBBEGIN case */
            Assert!((*s2).blockState == TBLOCK_SUBBEGIN);
            StartSubTransaction();
            (*s2).blockState = TBLOCK_SUBINPROGRESS;
        }
    }

    /* Done, no more iterations required */
    true
}

/*
 *    AbortCurrentTransaction
 */
pub unsafe fn AbortCurrentTransaction() {
    /*
     * Repeatedly call AbortCurrentTransactionInternal() until all the work is
     * done.
     */
    while !AbortCurrentTransactionInternal() {}
}

/*
 *    AbortCurrentTransactionInternal
 */
unsafe fn AbortCurrentTransactionInternal() -> bool {
    let s: TransactionState = CurrentTransactionState;

    match (*s).blockState {
        TBLOCK_DEFAULT => {
            if (*s).state == TRANS_DEFAULT {
                /* we are idle, so nothing to do */
            } else {
                /*
                 * We can get here after an error during transaction start
                 * (state will be TRANS_START).  Need to clean up the
                 * incompletely started transaction.
                 */
                if (*s).state == TRANS_START {
                    (*s).state = TRANS_INPROGRESS;
                }
                AbortTransaction();
                CleanupTransaction();
            }
        }

        /*
         * If we aren't in a transaction block, we just do the basic abort
         * & cleanup transaction.
         */
        TBLOCK_STARTED |
        TBLOCK_IMPLICIT_INPROGRESS => {
            AbortTransaction();
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * If we are in TBLOCK_BEGIN it means something screwed up right
         * after reading "BEGIN TRANSACTION".
         */
        TBLOCK_BEGIN => {
            AbortTransaction();
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * We are somewhere in a transaction block and we've gotten a failure.
         */
        TBLOCK_INPROGRESS |
        TBLOCK_PARALLEL_INPROGRESS => {
            AbortTransaction();
            (*s).blockState = TBLOCK_ABORT;
            /* CleanupTransaction happens when we exit TBLOCK_ABORT_END */
        }

        /*
         * Here, we failed while trying to COMMIT.
         */
        TBLOCK_END => {
            AbortTransaction();
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * Here, we are already in an aborted transaction state and are
         * waiting for a ROLLBACK.
         */
        TBLOCK_ABORT |
        TBLOCK_SUBABORT => {}

        /*
         * We are in a failed transaction and we got the ROLLBACK command.
         */
        TBLOCK_ABORT_END => {
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * We are in a live transaction and we got a ROLLBACK command.
         */
        TBLOCK_ABORT_PENDING => {
            AbortTransaction();
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * Here, we failed while trying to PREPARE.
         */
        TBLOCK_PREPARE => {
            AbortTransaction();
            CleanupTransaction();
            (*s).blockState = TBLOCK_DEFAULT;
        }

        /*
         * We got an error inside a subtransaction.
         */
        TBLOCK_SUBINPROGRESS => {
            AbortSubTransaction();
            (*s).blockState = TBLOCK_SUBABORT;
        }

        /*
         * If we failed while trying to create a subtransaction, clean up
         * the broken subtransaction and abort the parent.
         */
        TBLOCK_SUBBEGIN |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART => {
            AbortSubTransaction();
            CleanupSubTransaction();
            return false;
        }

        /*
         * Same as above, except the Abort() was already done.
         */
        TBLOCK_SUBABORT_END |
        TBLOCK_SUBABORT_RESTART => {
            CleanupSubTransaction();
            return false;
        }
    }

    /* Done, no more iterations required */
    true
}

/*
 *    PreventInTransactionBlock
 */
#[no_mangle]
pub unsafe fn PreventInTransactionBlock(is_top_level: bool, stmt_type: *const std::os::raw::c_char) {
    /*
     * xact block already started?
     */
    if IsTransactionBlock() {
        ereport!(ERROR,
            errmsg!("{} cannot run inside a transaction block",
                std::ffi::CStr::from_ptr(stmt_type).to_string_lossy()));
        /* C also: errcode(ERRCODE_ACTIVE_SQL_TRANSACTION) */
        /* translator: %s represents an SQL statement name */
    }

    /*
     * subtransaction?
     */
    if IsSubTransaction() {
        ereport!(ERROR,
            errmsg!("{} cannot run inside a subtransaction",
                std::ffi::CStr::from_ptr(stmt_type).to_string_lossy()));
        /* C also: errcode(ERRCODE_ACTIVE_SQL_TRANSACTION) */
    }

    /*
     * inside a function call?
     */
    if !is_top_level {
        ereport!(ERROR,
            errmsg!("{} cannot be executed from a function",
                std::ffi::CStr::from_ptr(stmt_type).to_string_lossy()));
        /* C also: errcode(ERRCODE_ACTIVE_SQL_TRANSACTION) */
    }

    /* If we got past IsTransactionBlock test, should be in default state */
    if (*CurrentTransactionState).blockState != TBLOCK_DEFAULT &&
       (*CurrentTransactionState).blockState != TBLOCK_STARTED {
        elog!(FATAL, "cannot prevent transaction chain");
    }

    /* All okay.  Set the flag to make sure the right thing happens later. */
    MyXactFlags |= XACT_FLAGS_NEEDIMMEDIATECOMMIT;
}

/*
 *    WarnNoTransactionBlock
 *    RequireTransactionBlock
 */
pub unsafe fn WarnNoTransactionBlock(is_top_level: bool, stmt_type: *const std::os::raw::c_char) {
    CheckTransactionBlock(is_top_level, false, stmt_type);
}

pub unsafe fn RequireTransactionBlock(is_top_level: bool, stmt_type: *const std::os::raw::c_char) {
    CheckTransactionBlock(is_top_level, true, stmt_type);
}

/*
 * This is the implementation of the above two.
 */
unsafe fn CheckTransactionBlock(is_top_level: bool, throw_error: bool,
                                 stmt_type: *const std::os::raw::c_char) {
    /*
     * xact block already started?
     */
    if IsTransactionBlock() { return; }

    /*
     * subtransaction?
     */
    if IsSubTransaction() { return; }

    /*
     * inside a function call?
     */
    if !is_top_level { return; }

    ereport!(if throw_error { ERROR } else { WARNING },
        errmsg!("{} can only be used in transaction blocks",
            std::ffi::CStr::from_ptr(stmt_type).to_string_lossy()));
    /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
    /* translator: %s represents an SQL statement name */
}

/*
 *    IsInTransactionBlock
 */
pub unsafe fn IsInTransactionBlock(is_top_level: bool) -> bool {
    /*
     * Return true on same conditions that would make
     * PreventInTransactionBlock error out
     */
    if IsTransactionBlock() { return true; }
    if IsSubTransaction() { return true; }
    if !is_top_level { return true; }
    if (*CurrentTransactionState).blockState != TBLOCK_DEFAULT &&
       (*CurrentTransactionState).blockState != TBLOCK_STARTED {
        return true;
    }

    false
}


/*
 * Register or deregister callback functions for start- and end-of-xact
 * operations.
 */
#[no_mangle]
pub unsafe fn RegisterXactCallback(callback: XactCallback, arg: *mut std::ffi::c_void) {
    let item: *mut XactCallbackItem =
        MemoryContextAlloc(TopMemoryContext as _,
                           core::mem::size_of::<XactCallbackItem>())
        as *mut XactCallbackItem;
    (*item).callback = callback;
    (*item).arg = arg;
    (*item).next = Xact_callbacks;
    Xact_callbacks = item;
}

pub unsafe fn UnregisterXactCallback(callback: XactCallback, arg: *mut std::ffi::c_void) {
    let mut item: *mut XactCallbackItem = Xact_callbacks;
    let mut prev: *mut XactCallbackItem = ptr::null_mut();

    while !item.is_null() {
        if (*item).callback as usize == callback as usize && (*item).arg == arg {
            if !prev.is_null() {
                (*prev).next = (*item).next;
            } else {
                Xact_callbacks = (*item).next;
            }
            pfree(item as *mut std::ffi::c_void);
            break;
        }
        prev = item;
        item = (*item).next;
    }
}

unsafe fn CallXactCallbacks(event: XactEvent) {
    let mut item: *mut XactCallbackItem = Xact_callbacks;

    while !item.is_null() {
        /* allow callbacks to unregister themselves when called */
        let next = (*item).next;
        ((*item).callback)(event, (*item).arg);
        item = next;
    }
}


/*
 * Register or deregister callback functions for start- and end-of-subxact
 * operations.
 *
 * Pretty much same as above, but for subtransaction events.
 */
#[no_mangle]
pub unsafe fn RegisterSubXactCallback(callback: SubXactCallback, arg: *mut std::ffi::c_void) {
    let item: *mut SubXactCallbackItem =
        MemoryContextAlloc(TopMemoryContext as _,
                           core::mem::size_of::<SubXactCallbackItem>())
        as *mut SubXactCallbackItem;
    (*item).callback = callback;
    (*item).arg = arg;
    (*item).next = SubXact_callbacks;
    SubXact_callbacks = item;
}

pub unsafe fn UnregisterSubXactCallback(callback: SubXactCallback, arg: *mut std::ffi::c_void) {
    let mut item: *mut SubXactCallbackItem = SubXact_callbacks;
    let mut prev: *mut SubXactCallbackItem = ptr::null_mut();

    while !item.is_null() {
        if (*item).callback as usize == callback as usize && (*item).arg == arg {
            if !prev.is_null() {
                (*prev).next = (*item).next;
            } else {
                SubXact_callbacks = (*item).next;
            }
            pfree(item as *mut std::ffi::c_void);
            break;
        }
        prev = item;
        item = (*item).next;
    }
}

unsafe fn CallSubXactCallbacks(event: SubXactEvent,
                                my_subid: SubTransactionId,
                                parent_subid: SubTransactionId) {
    let mut item: *mut SubXactCallbackItem = SubXact_callbacks;

    while !item.is_null() {
        /* allow callbacks to unregister themselves when called */
        let next = (*item).next;
        ((*item).callback)(event, my_subid, parent_subid, (*item).arg);
        item = next;
    }
}


/* ----------------------------------------------------------------
 *                   transaction block support
 * ---------------------------------------------------------------- */

/*
 *    BeginTransactionBlock
 *        This executes a BEGIN command.
 */
#[no_mangle]
pub unsafe fn BeginTransactionBlock() {
    let s: TransactionState = CurrentTransactionState;

    match (*s).blockState {
        /*
         * We are not inside a transaction block, so allow one to begin.
         */
        TBLOCK_STARTED => {
            (*s).blockState = TBLOCK_BEGIN;
        }

        /*
         * BEGIN converts an implicit transaction block to a regular one.
         */
        TBLOCK_IMPLICIT_INPROGRESS => {
            (*s).blockState = TBLOCK_BEGIN;
        }

        /*
         * Already a transaction block in progress.
         */
        TBLOCK_INPROGRESS |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBINPROGRESS |
        TBLOCK_ABORT |
        TBLOCK_SUBABORT => {
            ereport!(WARNING,
                errmsg!("there is already a transaction in progress"));
            /* C also: errcode(ERRCODE_ACTIVE_SQL_TRANSACTION) */
        }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_BEGIN |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "BeginTransactionBlock: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }
}

/*
 *    PrepareTransactionBlock
 *        This executes a PREPARE command.
 */
#[no_mangle]
pub unsafe fn PrepareTransactionBlock(gid: *const std::os::raw::c_char) -> bool {
    let s: TransactionState;
    let mut result: bool;

    /* Set up to commit the current transaction */
    result = EndTransactionBlock(false);

    /* If successful, change outer tblock state to PREPARE */
    if result {
        s = CurrentTransactionState;

        let mut sp = s;
        while !(*sp).parent.is_null() {
            sp = (*sp).parent;
        }

        if (*sp).blockState == TBLOCK_END {
            /* Save GID where PrepareTransaction can find it again */
            prepareGID = MemoryContextStrdup(TopTransactionContext as _, gid);

            (*sp).blockState = TBLOCK_PREPARE;
        } else {
            /*
             * ignore case where we are not in a transaction;
             * EndTransactionBlock already issued a warning.
             */
            Assert!((*sp).blockState == TBLOCK_STARTED ||
                    (*sp).blockState == TBLOCK_IMPLICIT_INPROGRESS);
            /* Don't send back a PREPARE result tag... */
            result = false;
        }
    }

    result
}

/*
 *    EndTransactionBlock
 *        This executes a COMMIT command.
 *
 * Since COMMIT may actually do a ROLLBACK, the result indicates what
 * happened: true for COMMIT, false for ROLLBACK.
 */
#[no_mangle]
pub unsafe fn EndTransactionBlock(chain: bool) -> bool {
    let mut s: TransactionState = CurrentTransactionState;
    let mut result: bool = false;

    match (*s).blockState {
        /*
         * We are in a transaction block, so tell CommitTransactionCommand to COMMIT.
         */
        TBLOCK_INPROGRESS => {
            (*s).blockState = TBLOCK_END;
            result = true;
        }

        /*
         * We are in an implicit transaction block.
         */
        TBLOCK_IMPLICIT_INPROGRESS => {
            if chain {
                ereport!(ERROR,
                    errmsg!("{} can only be used in transaction blocks",
                        "COMMIT AND CHAIN"));
                /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
                /* translator: %s represents an SQL statement name */
            } else {
                ereport!(WARNING,
                    errmsg!("there is no transaction in progress"));
                /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
            }
            (*s).blockState = TBLOCK_END;
            result = true;
        }

        /*
         * We are in a failed transaction block.
         */
        TBLOCK_ABORT => {
            (*s).blockState = TBLOCK_ABORT_END;
        }

        /*
         * We are in a live subtransaction block.
         */
        TBLOCK_SUBINPROGRESS => {
            while !(*s).parent.is_null() {
                if (*s).blockState == TBLOCK_SUBINPROGRESS {
                    (*s).blockState = TBLOCK_SUBCOMMIT;
                } else {
                    elog!(FATAL, "EndTransactionBlock: unexpected state {}",
                         BlockStateAsString((*s).blockState));
                }
                s = (*s).parent;
            }
            if (*s).blockState == TBLOCK_INPROGRESS {
                (*s).blockState = TBLOCK_END;
            } else {
                elog!(FATAL, "EndTransactionBlock: unexpected state {}",
                     BlockStateAsString((*s).blockState));
            }
            result = true;
        }

        /*
         * Here we are inside an aborted subtransaction.
         */
        TBLOCK_SUBABORT => {
            while !(*s).parent.is_null() {
                if (*s).blockState == TBLOCK_SUBINPROGRESS {
                    (*s).blockState = TBLOCK_SUBABORT_PENDING;
                } else if (*s).blockState == TBLOCK_SUBABORT {
                    (*s).blockState = TBLOCK_SUBABORT_END;
                } else {
                    elog!(FATAL, "EndTransactionBlock: unexpected state {}",
                         BlockStateAsString((*s).blockState));
                }
                s = (*s).parent;
            }
            if (*s).blockState == TBLOCK_INPROGRESS {
                (*s).blockState = TBLOCK_ABORT_PENDING;
            } else if (*s).blockState == TBLOCK_ABORT {
                (*s).blockState = TBLOCK_ABORT_END;
            } else {
                elog!(FATAL, "EndTransactionBlock: unexpected state {}",
                     BlockStateAsString((*s).blockState));
            }
        }

        /*
         * The user issued COMMIT when not inside a transaction.
         */
        TBLOCK_STARTED => {
            if chain {
                ereport!(ERROR,
                    errmsg!("{} can only be used in transaction blocks",
                        "COMMIT AND CHAIN"));
                /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
            } else {
                ereport!(WARNING,
                    errmsg!("there is no transaction in progress"));
                /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
            }
            result = true;
        }

        /*
         * The user issued a COMMIT that somehow ran inside a parallel worker.
         */
        TBLOCK_PARALLEL_INPROGRESS => {
            ereport!(FATAL,
                errmsg!("cannot commit during a parallel operation"));
            /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
        }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_BEGIN |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "EndTransactionBlock: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    Assert!((*s).blockState == TBLOCK_STARTED ||
            (*s).blockState == TBLOCK_END ||
            (*s).blockState == TBLOCK_ABORT_END ||
            (*s).blockState == TBLOCK_ABORT_PENDING);

    (*s).chain = chain;

    result
}

/*
 *    UserAbortTransactionBlock
 *        This executes a ROLLBACK command.
 */
pub unsafe fn UserAbortTransactionBlock(chain: bool) {
    let mut s: TransactionState = CurrentTransactionState;

    match (*s).blockState {
        /*
         * We are inside a transaction block and we got a ROLLBACK command.
         */
        TBLOCK_INPROGRESS => {
            (*s).blockState = TBLOCK_ABORT_PENDING;
        }

        /*
         * We are inside a failed transaction block and we got a ROLLBACK command.
         */
        TBLOCK_ABORT => {
            (*s).blockState = TBLOCK_ABORT_END;
        }

        /*
         * We are inside a subtransaction.  Mark everything up to top level.
         */
        TBLOCK_SUBINPROGRESS |
        TBLOCK_SUBABORT => {
            while !(*s).parent.is_null() {
                if (*s).blockState == TBLOCK_SUBINPROGRESS {
                    (*s).blockState = TBLOCK_SUBABORT_PENDING;
                } else if (*s).blockState == TBLOCK_SUBABORT {
                    (*s).blockState = TBLOCK_SUBABORT_END;
                } else {
                    elog!(FATAL, "UserAbortTransactionBlock: unexpected state {}",
                         BlockStateAsString((*s).blockState));
                }
                s = (*s).parent;
            }
            if (*s).blockState == TBLOCK_INPROGRESS {
                (*s).blockState = TBLOCK_ABORT_PENDING;
            } else if (*s).blockState == TBLOCK_ABORT {
                (*s).blockState = TBLOCK_ABORT_END;
            } else {
                elog!(FATAL, "UserAbortTransactionBlock: unexpected state {}",
                     BlockStateAsString((*s).blockState));
            }
        }

        /*
         * The user issued ABORT when not inside a transaction.
         */
        TBLOCK_STARTED |
        TBLOCK_IMPLICIT_INPROGRESS => {
            if chain {
                ereport!(ERROR,
                    errmsg!("{} can only be used in transaction blocks",
                        "ROLLBACK AND CHAIN"));
                /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
            } else {
                ereport!(WARNING,
                    errmsg!("there is no transaction in progress"));
                /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
            }
            (*s).blockState = TBLOCK_ABORT_PENDING;
        }

        /*
         * The user issued an ABORT that somehow ran inside a parallel worker.
         */
        TBLOCK_PARALLEL_INPROGRESS => {
            ereport!(FATAL,
                errmsg!("cannot abort during a parallel operation"));
            /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
        }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_BEGIN |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "UserAbortTransactionBlock: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    Assert!((*s).blockState == TBLOCK_ABORT_END ||
            (*s).blockState == TBLOCK_ABORT_PENDING);

    (*s).chain = chain;
}

/*
 * BeginImplicitTransactionBlock
 *    Start an implicit transaction block if we're not already in one.
 */
pub unsafe fn BeginImplicitTransactionBlock() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * If we are in STARTED state (that is, no transaction block is open),
     * switch to IMPLICIT_INPROGRESS state, creating an implicit transaction
     * block.
     */
    if (*s).blockState == TBLOCK_STARTED {
        (*s).blockState = TBLOCK_IMPLICIT_INPROGRESS;
    }
}

/*
 * EndImplicitTransactionBlock
 *    End an implicit transaction block, if we're in one.
 */
pub unsafe fn EndImplicitTransactionBlock() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * If we are in IMPLICIT_INPROGRESS state, switch back to STARTED state,
     * allowing CommitTransactionCommand to commit.
     */
    if (*s).blockState == TBLOCK_IMPLICIT_INPROGRESS {
        (*s).blockState = TBLOCK_STARTED;
    }
}

/*
 * DefineSavepoint
 *    This executes a SAVEPOINT command.
 */
pub unsafe fn DefineSavepoint(name: *const std::os::raw::c_char) {
    let mut s: TransactionState = CurrentTransactionState;

    /*
     * Workers synchronize transaction state at the beginning of each parallel
     * operation, so we can't account for new subtransactions after that point.
     */
    if IsInParallelMode() || IsParallelWorker() {
        ereport!(ERROR, errmsg!("cannot define savepoints during a parallel operation"));
        /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
    }

    match (*s).blockState {
        TBLOCK_INPROGRESS |
        TBLOCK_SUBINPROGRESS => {
            /* Normal subtransaction start */
            PushTransaction();
            s = CurrentTransactionState; /* changed by push */

            /*
             * Savepoint names, like the TransactionState block itself, live
             * in TopTransactionContext.
             */
            if !name.is_null() {
                (*s).name = MemoryContextStrdup(TopTransactionContext as _, name);
            }
        }

        /*
         * We disallow savepoint commands in implicit transaction blocks.
         */
        TBLOCK_IMPLICIT_INPROGRESS => {
            ereport!(ERROR,
                errmsg!("{} can only be used in transaction blocks", "SAVEPOINT"));
            /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
            /* translator: %s represents an SQL statement name */
        }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_STARTED |
        TBLOCK_BEGIN |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT |
        TBLOCK_SUBABORT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "DefineSavepoint: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }
}

/*
 * ReleaseSavepoint
 *    This executes a RELEASE command.
 */
pub unsafe fn ReleaseSavepoint(name: *const std::os::raw::c_char) {
    let s: TransactionState = CurrentTransactionState;

    /*
     * Workers synchronize transaction state at the beginning of each parallel
     * operation, so we can't account for transaction state change after that point.
     */
    if IsInParallelMode() || IsParallelWorker() {
        ereport!(ERROR, errmsg!("cannot release savepoints during a parallel operation"));
        /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
    }

    match (*s).blockState {
        /*
         * We can't release a savepoint if there is no savepoint defined.
         */
        TBLOCK_INPROGRESS => {
            ereport!(ERROR,
                errmsg!("savepoint \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(name).to_string_lossy()));
            /* C also: errcode(ERRCODE_S_E_INVALID_SPECIFICATION) */
        }

        TBLOCK_IMPLICIT_INPROGRESS => {
            /* See comment about implicit transactions in DefineSavepoint */
            ereport!(ERROR,
                errmsg!("{} can only be used in transaction blocks", "RELEASE SAVEPOINT"));
            /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
        }

        /*
         * We are in a non-aborted subtransaction. This is the only valid case.
         */
        TBLOCK_SUBINPROGRESS => { /* OK */ }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_STARTED |
        TBLOCK_BEGIN |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT |
        TBLOCK_SUBABORT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "ReleaseSavepoint: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    let mut target: TransactionState = s;
    while !target.is_null() {
        if !(*target).name.is_null() &&
           libc_strcmp((*target).name, name) == 0 {
            break;
        }
        target = (*target).parent;
    }

    if target.is_null() {
        ereport!(ERROR,
            errmsg!("savepoint \"{}\" does not exist",
                std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_S_E_INVALID_SPECIFICATION) */
    }

    /* disallow crossing savepoint level boundaries */
    if (*target).savepointLevel != (*s).savepointLevel {
        ereport!(ERROR,
            errmsg!("savepoint \"{}\" does not exist within current savepoint level",
                std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_S_E_INVALID_SPECIFICATION) */
    }

    /*
     * Mark "commit pending" all subtransactions up to the target
     * subtransaction.  The actual commits will happen when control gets to
     * CommitTransactionCommand.
     */
    let mut xact: TransactionState = CurrentTransactionState;
    loop {
        Assert!((*xact).blockState == TBLOCK_SUBINPROGRESS);
        (*xact).blockState = TBLOCK_SUBRELEASE;
        if xact == target {
            break;
        }
        xact = (*xact).parent;
        Assert!(!xact.is_null());
    }
}

/* helper: strcmp for raw C strings */
unsafe fn libc_strcmp(a: *const std::os::raw::c_char, b: *const std::os::raw::c_char) -> i32 {
    /* TODO(pg-port): replace with libc::strcmp when libc is available */
    let sa = std::ffi::CStr::from_ptr(a);
    let sb = std::ffi::CStr::from_ptr(b);
    sa.cmp(sb) as i32
}

/*
 * RollbackToSavepoint
 *    This executes a ROLLBACK TO <savepoint> command.
 */
pub unsafe fn RollbackToSavepoint(name: *const std::os::raw::c_char) {
    let s: TransactionState = CurrentTransactionState;

    /*
     * Workers synchronize transaction state at the beginning of each parallel
     * operation.
     */
    if IsInParallelMode() || IsParallelWorker() {
        ereport!(ERROR, errmsg!("cannot rollback to savepoints during a parallel operation"));
        /* C also: errcode(ERRCODE_INVALID_TRANSACTION_STATE) */
    }

    match (*s).blockState {
        /*
         * We can't rollback to a savepoint if there is no savepoint defined.
         */
        TBLOCK_INPROGRESS |
        TBLOCK_ABORT => {
            ereport!(ERROR,
                errmsg!("savepoint \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(name).to_string_lossy()));
            /* C also: errcode(ERRCODE_S_E_INVALID_SPECIFICATION) */
        }

        TBLOCK_IMPLICIT_INPROGRESS => {
            /* See comment about implicit transactions in DefineSavepoint */
            ereport!(ERROR,
                errmsg!("{} can only be used in transaction blocks", "ROLLBACK TO SAVEPOINT"));
            /* C also: errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION) */
        }

        /*
         * There is at least one savepoint, so proceed.
         */
        TBLOCK_SUBINPROGRESS |
        TBLOCK_SUBABORT => { /* OK */ }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_STARTED |
        TBLOCK_BEGIN |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBBEGIN |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "RollbackToSavepoint: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    let mut target: TransactionState = s;
    while !target.is_null() {
        if !(*target).name.is_null() &&
           libc_strcmp((*target).name, name) == 0 {
            break;
        }
        target = (*target).parent;
    }

    if target.is_null() {
        ereport!(ERROR,
            errmsg!("savepoint \"{}\" does not exist",
                std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_S_E_INVALID_SPECIFICATION) */
    }

    /* disallow crossing savepoint level boundaries */
    if (*target).savepointLevel != (*s).savepointLevel {
        ereport!(ERROR,
            errmsg!("savepoint \"{}\" does not exist within current savepoint level",
                std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_S_E_INVALID_SPECIFICATION) */
    }

    /*
     * Mark "abort pending" all subtransactions up to the target
     * subtransaction.  The actual aborts will happen when control gets to
     * CommitTransactionCommand.
     */
    let mut xact: TransactionState = CurrentTransactionState;
    loop {
        if xact == target {
            break;
        }
        if (*xact).blockState == TBLOCK_SUBINPROGRESS {
            (*xact).blockState = TBLOCK_SUBABORT_PENDING;
        } else if (*xact).blockState == TBLOCK_SUBABORT {
            (*xact).blockState = TBLOCK_SUBABORT_END;
        } else {
            elog!(FATAL, "RollbackToSavepoint: unexpected state {}",
                 BlockStateAsString((*xact).blockState));
        }
        xact = (*xact).parent;
        Assert!(!xact.is_null());
    }

    /* And mark the target as "restart pending" */
    if (*xact).blockState == TBLOCK_SUBINPROGRESS {
        (*xact).blockState = TBLOCK_SUBRESTART;
    } else if (*xact).blockState == TBLOCK_SUBABORT {
        (*xact).blockState = TBLOCK_SUBABORT_RESTART;
    } else {
        elog!(FATAL, "RollbackToSavepoint: unexpected state {}",
             BlockStateAsString((*xact).blockState));
    }
}

/*
 * BeginInternalSubTransaction
 *    This is the same as DefineSavepoint except it allows more states,
 *    and automatically does CommitTransactionCommand/StartTransactionCommand.
 */
#[no_mangle]
pub unsafe fn BeginInternalSubTransaction(name: *const std::os::raw::c_char) {
    let mut s: TransactionState = CurrentTransactionState;
    let save_ExitOnAnyError: bool = ExitOnAnyError;

    /*
     * Errors within this function are improbable, but if one does happen we
     * force a FATAL exit.
     */
    ExitOnAnyError = true;

    /*
     * We do not check for parallel mode here.  It's permissible to start and
     * end "internal" subtransactions while in parallel mode, so long as no
     * new XIDs or command IDs are assigned.
     */

    match (*s).blockState {
        TBLOCK_STARTED |
        TBLOCK_INPROGRESS |
        TBLOCK_IMPLICIT_INPROGRESS |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_END |
        TBLOCK_PREPARE |
        TBLOCK_SUBINPROGRESS => {
            /* Normal subtransaction start */
            PushTransaction();
            s = CurrentTransactionState; /* changed by push */

            /*
             * Savepoint names, like the TransactionState block itself, live
             * in TopTransactionContext.
             */
            if !name.is_null() {
                (*s).name = MemoryContextStrdup(TopTransactionContext as _, name);
            }
        }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_BEGIN |
        TBLOCK_SUBBEGIN |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT |
        TBLOCK_SUBABORT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART => {
            elog!(FATAL, "BeginInternalSubTransaction: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    CommitTransactionCommand();
    StartTransactionCommand();

    ExitOnAnyError = save_ExitOnAnyError;
}

/*
 * ReleaseCurrentSubTransaction
 *
 * RELEASE (ie, commit) the innermost subtransaction, regardless of its
 * savepoint name (if any).
 */
#[no_mangle]
pub unsafe fn ReleaseCurrentSubTransaction() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * We do not check for parallel mode here.
     */

    if (*s).blockState != TBLOCK_SUBINPROGRESS {
        elog!(ERROR, "ReleaseCurrentSubTransaction: unexpected state {}",
             BlockStateAsString((*s).blockState));
    }
    Assert!((*s).state == TRANS_INPROGRESS);
    MemoryContextSwitchTo(CurTransactionContext);
    CommitSubTransaction();
    let s2: TransactionState = CurrentTransactionState; /* changed by pop */
    Assert!((*s2).state == TRANS_INPROGRESS);
}

/*
 * RollbackAndReleaseCurrentSubTransaction
 *
 * ROLLBACK and RELEASE (ie, abort) the innermost subtransaction.
 */
#[no_mangle]
pub unsafe fn RollbackAndReleaseCurrentSubTransaction() {
    let s: TransactionState = CurrentTransactionState;

    /*
     * We do not check for parallel mode here.
     */

    match (*s).blockState {
        /* Must be in a subtransaction */
        TBLOCK_SUBINPROGRESS |
        TBLOCK_SUBABORT => { /* OK */ }

        /* These cases are invalid. */
        TBLOCK_DEFAULT |
        TBLOCK_STARTED |
        TBLOCK_BEGIN |
        TBLOCK_IMPLICIT_INPROGRESS |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBBEGIN |
        TBLOCK_INPROGRESS |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_ABORT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART |
        TBLOCK_PREPARE => {
            elog!(FATAL, "RollbackAndReleaseCurrentSubTransaction: unexpected state {}",
                 BlockStateAsString((*s).blockState));
        }
    }

    /*
     * Abort the current subtransaction, if needed.
     */
    if (*s).blockState == TBLOCK_SUBINPROGRESS {
        AbortSubTransaction();
    }

    /* And clean it up, too */
    CleanupSubTransaction();

    let s2: TransactionState = CurrentTransactionState; /* changed by pop */
    Assert!((*s2).blockState == TBLOCK_SUBINPROGRESS ||
            (*s2).blockState == TBLOCK_INPROGRESS ||
            (*s2).blockState == TBLOCK_IMPLICIT_INPROGRESS ||
            (*s2).blockState == TBLOCK_PARALLEL_INPROGRESS ||
            (*s2).blockState == TBLOCK_STARTED);
}

/*
 *    AbortOutOfAnyTransaction
 *
 *    This routine is provided for error recovery purposes.  It aborts any
 *    active transaction or transaction block, leaving the system in a known
 *    idle state.
 */
pub unsafe fn AbortOutOfAnyTransaction() {
    let mut s: TransactionState = CurrentTransactionState;

    /* Ensure we're not running in a doomed memory context */
    AtAbort_Memory();

    /*
     * Get out of any transaction or nested transaction
     */
    loop {
        match (*s).blockState {
            TBLOCK_DEFAULT => {
                if (*s).state == TRANS_DEFAULT {
                    /* Not in a transaction, do nothing */
                } else {
                    /*
                     * We can get here after an error during transaction start
                     * (state will be TRANS_START).  Need to clean up the
                     * incompletely started transaction.  First, adjust the
                     * low-level state to suppress warning message from
                     * AbortTransaction.
                     */
                    if (*s).state == TRANS_START {
                        (*s).state = TRANS_INPROGRESS;
                    }
                    AbortTransaction();
                    CleanupTransaction();
                }
            }
            TBLOCK_STARTED |
            TBLOCK_BEGIN |
            TBLOCK_INPROGRESS |
            TBLOCK_IMPLICIT_INPROGRESS |
            TBLOCK_PARALLEL_INPROGRESS |
            TBLOCK_END |
            TBLOCK_ABORT_PENDING |
            TBLOCK_PREPARE => {
                /* In a transaction, so clean up */
                AbortTransaction();
                CleanupTransaction();
                (*s).blockState = TBLOCK_DEFAULT;
            }
            TBLOCK_ABORT |
            TBLOCK_ABORT_END => {
                /*
                 * AbortTransaction is already done, still need Cleanup.
                 * However, if we failed partway through running ROLLBACK,
                 * there will be an active portal running that command, which
                 * we need to shut down before doing CleanupTransaction.
                 */
                AtAbort_Portals();
                CleanupTransaction();
                (*s).blockState = TBLOCK_DEFAULT;
            }

            /*
             * In a subtransaction, so clean it up and abort parent too
             */
            TBLOCK_SUBBEGIN |
            TBLOCK_SUBINPROGRESS |
            TBLOCK_SUBRELEASE |
            TBLOCK_SUBCOMMIT |
            TBLOCK_SUBABORT_PENDING |
            TBLOCK_SUBRESTART => {
                AbortSubTransaction();
                CleanupSubTransaction();
                s = CurrentTransactionState; /* changed by pop */
            }

            TBLOCK_SUBABORT |
            TBLOCK_SUBABORT_END |
            TBLOCK_SUBABORT_RESTART => {
                /* As above, but AbortSubTransaction already done */
                if !(*s).curTransactionOwner.is_null() {
                    /* As in TBLOCK_ABORT, might have a live portal to zap */
                    AtSubAbort_Portals((*s).subTransactionId,
                                       (*(*s).parent).subTransactionId,
                                       (*s).curTransactionOwner,
                                       (*(*s).parent).curTransactionOwner);
                }
                CleanupSubTransaction();
                s = CurrentTransactionState; /* changed by pop */
            }
        }
        if (*s).blockState == TBLOCK_DEFAULT {
            break;
        }
    }

    /* Should be out of all subxacts now */
    Assert!((*s).parent.is_null());

    /*
     * Revert to TopMemoryContext, to ensure we exit in a well-defined state
     * whether there were any transactions to close or not.
     */
    MemoryContextSwitchTo(TopMemoryContext as _);
}

/*
 * IsTransactionBlock --- are we within a transaction block?
 */
#[no_mangle]
pub unsafe fn IsTransactionBlock() -> bool {
    let s: TransactionState = CurrentTransactionState;

    if (*s).blockState == TBLOCK_DEFAULT || (*s).blockState == TBLOCK_STARTED {
        return false;
    }

    true
}

/*
 * IsTransactionOrTransactionBlock --- are we within either a transaction
 * or a transaction block?  (The backend is only really "idle" when this
 * returns false.)
 */
#[no_mangle]
pub unsafe fn IsTransactionOrTransactionBlock() -> bool {
    let s: TransactionState = CurrentTransactionState;

    if (*s).blockState == TBLOCK_DEFAULT {
        return false;
    }

    true
}

/*
 * TransactionBlockStatusCode - return status code to send in ReadyForQuery
 */
pub unsafe fn TransactionBlockStatusCode() -> u8 {
    let s: TransactionState = CurrentTransactionState;

    match (*s).blockState {
        TBLOCK_DEFAULT |
        TBLOCK_STARTED => b'I',  /* idle --- not in transaction */
        TBLOCK_BEGIN |
        TBLOCK_SUBBEGIN |
        TBLOCK_INPROGRESS |
        TBLOCK_IMPLICIT_INPROGRESS |
        TBLOCK_PARALLEL_INPROGRESS |
        TBLOCK_SUBINPROGRESS |
        TBLOCK_END |
        TBLOCK_SUBRELEASE |
        TBLOCK_SUBCOMMIT |
        TBLOCK_PREPARE => b'T', /* in transaction */
        TBLOCK_ABORT |
        TBLOCK_SUBABORT |
        TBLOCK_ABORT_END |
        TBLOCK_SUBABORT_END |
        TBLOCK_ABORT_PENDING |
        TBLOCK_SUBABORT_PENDING |
        TBLOCK_SUBRESTART |
        TBLOCK_SUBABORT_RESTART => b'E', /* in failed transaction */
    }
}

/*
 * IsSubTransaction
 */
pub unsafe fn IsSubTransaction() -> bool {
    let s: TransactionState = CurrentTransactionState;

    (*s).nestingLevel >= 2
}

/*
 * StartSubTransaction
 *
 * If you're wondering why this is separate from PushTransaction: it's because
 * we can't conveniently do this stuff right inside DefineSavepoint.
 */
unsafe fn StartSubTransaction() {
    let s: TransactionState = CurrentTransactionState;

    if (*s).state != TRANS_DEFAULT {
        elog!(WARNING, "StartSubTransaction while in {} state",
             TransStateAsString((*s).state));
    }

    (*s).state = TRANS_START;

    /*
     * Initialize subsystems for new subtransaction
     *
     * must initialize resource-management stuff first
     */
    AtSubStart_Memory();
    AtSubStart_ResourceOwner();
    AfterTriggerBeginSubXact();

    (*s).state = TRANS_INPROGRESS;

    /*
     * Call start-of-subxact callbacks
     */
    CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, (*s).subTransactionId,
                         (*(*s).parent).subTransactionId);

    ShowTransactionState(b"StartSubTransaction\0".as_ptr() as *const _);
}

/*
 * CommitSubTransaction
 *
 *    The caller has to make sure to always reassign CurrentTransactionState
 *    if it has a local pointer to it after calling this function.
 */
unsafe fn CommitSubTransaction() {
    let s: TransactionState = CurrentTransactionState;

    ShowTransactionState(b"CommitSubTransaction\0".as_ptr() as *const _);

    if (*s).state != TRANS_INPROGRESS {
        elog!(WARNING, "CommitSubTransaction while in {} state",
             TransStateAsString((*s).state));
    }

    /* Pre-commit processing goes here */

    CallSubXactCallbacks(SUBXACT_EVENT_PRE_COMMIT_SUB, (*s).subTransactionId,
                         (*(*s).parent).subTransactionId);

    /*
     * If this subxact has started any unfinished parallel operation, clean up
     * its workers and exit parallel mode.  Warn about leaked resources.
     */
    AtEOSubXact_Parallel(true, (*s).subTransactionId);
    if (*s).parallelModeLevel != 0 {
        elog!(WARNING, "parallelModeLevel is {} not 0 at end of subtransaction",
             (*s).parallelModeLevel);
        (*s).parallelModeLevel = 0;
    }

    /* Do the actual "commit", such as it is */
    (*s).state = TRANS_COMMIT;

    /* Must CCI to ensure commands of subtransaction are seen as done */
    CommandCounterIncrement();

    /*
     * Prior to 8.4 we marked subcommit in clog at this point.  We now only
     * perform that step, if required, as part of the atomic update of the
     * whole transaction tree at top level commit or abort.
     */

    /* Post-commit cleanup */
    if FullTransactionIdIsValid((*s).fullTransactionId) {
        AtSubCommit_childXids();
    }
    AfterTriggerEndSubXact(true);
    AtSubCommit_Portals((*s).subTransactionId,
                        (*(*s).parent).subTransactionId,
                        (*(*s).parent).nestingLevel,
                        (*(*s).parent).curTransactionOwner);
    AtEOSubXact_LargeObject(true, (*s).subTransactionId,
                             (*(*s).parent).subTransactionId);
    AtSubCommit_Notify();

    CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, (*s).subTransactionId,
                         (*(*s).parent).subTransactionId);

    ResourceOwnerRelease((*s).curTransactionOwner,
                         RESOURCE_RELEASE_BEFORE_LOCKS,
                         true, false);
    AtEOSubXact_RelationCache(true, (*s).subTransactionId,
                               (*(*s).parent).subTransactionId);
    AtEOSubXact_TypeCache();
    AtEOSubXact_Inval(true);
    AtSubCommit_smgr();

    /*
     * The only lock we actually release here is the subtransaction XID lock.
     */
    CurrentResourceOwner = (*s).curTransactionOwner;
    if FullTransactionIdIsValid((*s).fullTransactionId) {
        XactLockTableDelete(XidFromFullTransactionId((*s).fullTransactionId));
    }

    /*
     * Other locks should get transferred to their parent resource owner.
     */
    ResourceOwnerRelease((*s).curTransactionOwner,
                         RESOURCE_RELEASE_LOCKS,
                         true, false);
    ResourceOwnerRelease((*s).curTransactionOwner,
                         RESOURCE_RELEASE_AFTER_LOCKS,
                         true, false);

    AtEOXact_GUC(true, (*s).gucNestLevel);
    AtEOSubXact_SPI(true, (*s).subTransactionId);
    AtEOSubXact_on_commit_actions(true, (*s).subTransactionId,
                                   (*(*s).parent).subTransactionId);
    AtEOSubXact_Namespace(true, (*s).subTransactionId,
                           (*(*s).parent).subTransactionId);
    AtEOSubXact_Files(true, (*s).subTransactionId,
                      (*(*s).parent).subTransactionId);
    AtEOSubXact_HashTables(true, (*s).nestingLevel);
    AtEOSubXact_PgStat(true, (*s).nestingLevel);
    AtSubCommit_Snapshot((*s).nestingLevel);

    /*
     * We need to restore the upper transaction's read-only state, in case the
     * upper is read-write while the child is read-only; GUC will incorrectly
     * think it should leave the child state in place.
     */
    XactReadOnly = (*s).prevXactReadOnly;

    CurrentResourceOwner = (*(*s).parent).curTransactionOwner;
    CurTransactionResourceOwner = (*(*s).parent).curTransactionOwner;
    ResourceOwnerDelete((*s).curTransactionOwner);
    (*s).curTransactionOwner = ptr::null_mut();

    AtSubCommit_Memory();

    (*s).state = TRANS_DEFAULT;

    PopTransaction();
}

/*
 * AbortSubTransaction
 */
unsafe fn AbortSubTransaction() {
    let s: TransactionState = CurrentTransactionState;

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS!();

    /* Make sure we have a valid memory context and resource owner */
    AtSubAbort_Memory();
    AtSubAbort_ResourceOwner();

    /*
     * Release any LW locks we might be holding as quickly as possible.
     * (Regular locks, however, must be held till we finish aborting.)
     * Releasing LW locks is critical since we might try to grab them again
     * while cleaning up!
     *
     * FIXME This may be incorrect --- Are there some locks we should keep?
     * Buffer locks, for example?  I don't think so but I'm not sure.
     */
    LWLockReleaseAll();

    pgstat_report_wait_end();
    pgstat_progress_end_command();

    pgaio_error_cleanup();

    UnlockBuffers();

    /* Reset WAL record construction state */
    XLogResetInsertion();

    /* Cancel condition variable sleep */
    ConditionVariableCancelSleep();

    /*
     * Also clean up any open wait for lock, since the lock manager will choke
     * if we try to wait for another lock before doing this.
     */
    LockErrorCleanup();

    /*
     * If any timeout events are still active, make sure the timeout interrupt
     * is scheduled.
     */
    reschedule_timeouts();

    /*
     * Re-enable signals, in case we got here by longjmp'ing out of a signal
     * handler.
     */
    sigprocmask(SIG_SETMASK, &UnBlockSig as *const i64 as *const std::ffi::c_void,
                ptr::null_mut());

    /*
     * check the current transaction state
     */
    ShowTransactionState(b"AbortSubTransaction\0".as_ptr() as *const _);

    if (*s).state != TRANS_INPROGRESS {
        elog!(WARNING, "AbortSubTransaction while in {} state",
             TransStateAsString((*s).state));
    }

    (*s).state = TRANS_ABORT;

    /*
     * Reset user ID which might have been changed transiently.  (See notes in
     * AbortTransaction.)
     */
    SetUserIdAndSecContext((*s).prevUser, (*s).prevSecContext);

    /* Forget about any active REINDEX. */
    ResetReindexState((*s).nestingLevel);

    /* Reset logical streaming state. */
    ResetLogicalStreamingState();

    /*
     * No need for SnapBuildResetExportedSnapshotState() here, snapshot
     * exports are not supported in subtransactions.
     */

    /*
     * If this subxact has started any unfinished parallel operation, clean up
     * its workers and exit parallel mode.  Don't warn about leaked resources.
     */
    AtEOSubXact_Parallel(false, (*s).subTransactionId);
    (*s).parallelModeLevel = 0;

    /*
     * We can skip all this stuff if the subxact failed before creating a
     * ResourceOwner...
     */
    if !(*s).curTransactionOwner.is_null() {
        AfterTriggerEndSubXact(false);
        AtSubAbort_Portals((*s).subTransactionId,
                           (*(*s).parent).subTransactionId,
                           (*s).curTransactionOwner,
                           (*(*s).parent).curTransactionOwner);
        AtEOSubXact_LargeObject(false, (*s).subTransactionId,
                                 (*(*s).parent).subTransactionId);
        AtSubAbort_Notify();

        /* Advertise the fact that we aborted in pg_xact. */
        let _ = RecordTransactionAbort(true);

        /* Post-abort cleanup */
        if FullTransactionIdIsValid((*s).fullTransactionId) {
            AtSubAbort_childXids();
        }

        CallSubXactCallbacks(SUBXACT_EVENT_ABORT_SUB, (*s).subTransactionId,
                             (*(*s).parent).subTransactionId);

        ResourceOwnerRelease((*s).curTransactionOwner,
                             RESOURCE_RELEASE_BEFORE_LOCKS,
                             false, false);

        AtEOXact_Aio(false);
        AtEOSubXact_RelationCache(false, (*s).subTransactionId,
                                   (*(*s).parent).subTransactionId);
        AtEOSubXact_TypeCache();
        AtEOSubXact_Inval(false);
        ResourceOwnerRelease((*s).curTransactionOwner,
                             RESOURCE_RELEASE_LOCKS,
                             false, false);
        ResourceOwnerRelease((*s).curTransactionOwner,
                             RESOURCE_RELEASE_AFTER_LOCKS,
                             false, false);
        AtSubAbort_smgr();

        AtEOXact_GUC(false, (*s).gucNestLevel);
        AtEOSubXact_SPI(false, (*s).subTransactionId);
        AtEOSubXact_on_commit_actions(false, (*s).subTransactionId,
                                       (*(*s).parent).subTransactionId);
        AtEOSubXact_Namespace(false, (*s).subTransactionId,
                               (*(*s).parent).subTransactionId);
        AtEOSubXact_Files(false, (*s).subTransactionId,
                          (*(*s).parent).subTransactionId);
        AtEOSubXact_HashTables(false, (*s).nestingLevel);
        AtEOSubXact_PgStat(false, (*s).nestingLevel);
        AtSubAbort_Snapshot((*s).nestingLevel);
    }

    /*
     * Restore the upper transaction's read-only state, too.  This should be
     * redundant with GUC's cleanup but we may as well do it for consistency
     * with the commit case.
     */
    XactReadOnly = (*s).prevXactReadOnly;

    RESUME_INTERRUPTS!();
}

/*
 * CleanupSubTransaction
 *
 *    The caller has to make sure to always reassign CurrentTransactionState
 *    if it has a local pointer to it after calling this function.
 */
unsafe fn CleanupSubTransaction() {
    let s: TransactionState = CurrentTransactionState;

    ShowTransactionState(b"CleanupSubTransaction\0".as_ptr() as *const _);

    if (*s).state != TRANS_ABORT {
        elog!(WARNING, "CleanupSubTransaction while in {} state",
             TransStateAsString((*s).state));
    }

    AtSubCleanup_Portals((*s).subTransactionId);

    CurrentResourceOwner = (*(*s).parent).curTransactionOwner;
    CurTransactionResourceOwner = (*(*s).parent).curTransactionOwner;
    if !(*s).curTransactionOwner.is_null() {
        ResourceOwnerDelete((*s).curTransactionOwner);
    }
    (*s).curTransactionOwner = ptr::null_mut();

    AtSubCleanup_Memory();

    (*s).state = TRANS_DEFAULT;

    PopTransaction();
}

/*
 * PushTransaction
 *        Create transaction state stack entry for a subtransaction
 *
 *    The caller has to make sure to always reassign CurrentTransactionState
 *    if it has a local pointer to it after calling this function.
 */
unsafe fn PushTransaction() {
    let p: TransactionState = CurrentTransactionState;

    /*
     * We keep subtransaction state nodes in TopTransactionContext.
     */
    let s: TransactionState =
        MemoryContextAllocZero(TopTransactionContext as _,
                               core::mem::size_of::<TransactionStateData>())
        as TransactionState;

    /*
     * Assign a subtransaction ID, watching out for counter wraparound.
     */
    currentSubTransactionId = currentSubTransactionId.wrapping_add(1);
    if currentSubTransactionId == InvalidSubTransactionId {
        currentSubTransactionId = currentSubTransactionId.wrapping_sub(1);
        pfree(s as *mut std::ffi::c_void);
        ereport!(ERROR, errmsg!("cannot have more than 2^32-1 subtransactions in a transaction"));
        /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
    }

    /*
     * We can now stack a minimally valid subtransaction without fear of
     * failure.
     */
    (*s).fullTransactionId = InvalidFullTransactionId(); /* until assigned */
    (*s).subTransactionId = currentSubTransactionId;
    (*s).parent = p;
    (*s).nestingLevel = (*p).nestingLevel + 1;
    (*s).gucNestLevel = NewGUCNestLevel();
    (*s).savepointLevel = (*p).savepointLevel;
    (*s).state = TRANS_DEFAULT;
    (*s).blockState = TBLOCK_SUBBEGIN;
    GetUserIdAndSecContext(&mut (*s).prevUser, &mut (*s).prevSecContext);
    (*s).prevXactReadOnly = XactReadOnly;
    (*s).startedInRecovery = (*p).startedInRecovery;
    (*s).parallelModeLevel = 0;
    (*s).parallelChildXact = ((*p).parallelModeLevel != 0 || (*p).parallelChildXact);
    (*s).topXidLogged = false;

    CurrentTransactionState = s;

    /*
     * AbortSubTransaction and CleanupSubTransaction have to be able to cope
     * with the subtransaction from here on out; in particular they should not
     * assume that it necessarily has a transaction context, resource owner,
     * or XID.
     */
}

/*
 * PopTransaction
 *        Pop back to parent transaction state
 *
 *    The caller has to make sure to always reassign CurrentTransactionState
 *    if it has a local pointer to it after calling this function.
 */
unsafe fn PopTransaction() {
    let s: TransactionState = CurrentTransactionState;

    if (*s).state != TRANS_DEFAULT {
        elog!(WARNING, "PopTransaction while in {} state",
             TransStateAsString((*s).state));
    }

    if (*s).parent.is_null() {
        elog!(FATAL, "PopTransaction with no parent");
    }

    CurrentTransactionState = (*s).parent;

    /* Let's just make sure CurTransactionContext is good */
    CurTransactionContext = (*(*s).parent).curTransactionContext;
    MemoryContextSwitchTo(CurTransactionContext);

    /* Ditto for ResourceOwner links */
    CurTransactionResourceOwner = (*(*s).parent).curTransactionOwner;
    CurrentResourceOwner = (*(*s).parent).curTransactionOwner;

    /* Free the old child structure */
    if !(*s).name.is_null() {
        pfree((*s).name as *mut std::ffi::c_void);
    }
    pfree(s as *mut std::ffi::c_void);
}

/*
 * EstimateTransactionStateSpace
 *        Estimate the amount of space that will be needed by
 *        SerializeTransactionState.
 */
pub unsafe fn EstimateTransactionStateSpace() -> Size {
    let mut s: TransactionState = CurrentTransactionState;
    let mut nxids: Size = 0;
    let size: Size = SerializedTransactionStateHeaderSize;

    while !s.is_null() {
        if FullTransactionIdIsValid((*s).fullTransactionId) {
            nxids = add_size(nxids, 1);
        }
        nxids = add_size(nxids, (*s).nChildXids as Size);
        s = (*s).parent;
    }

    add_size(size, mul_size(core::mem::size_of::<TransactionId>(), nxids))
}

/*
 * SerializeTransactionState
 *        Write out relevant details of our transaction state that will be
 *        needed by a parallel worker.
 */
pub unsafe fn SerializeTransactionState(maxsize: Size, start_address: *mut std::os::raw::c_char) {
    let mut s: TransactionState = CurrentTransactionState;
    let mut nxids: Size = 0;
    let mut i: Size = 0;

    let result: *mut SerializedTransactionState =
        start_address as *mut SerializedTransactionState;

    (*result).xactIsoLevel = XactIsoLevel;
    (*result).xactDeferrable = XactDeferrable;
    (*result).topFullTransactionId = XactTopFullTransactionId;
    (*result).currentFullTransactionId =
        (*CurrentTransactionState).fullTransactionId;
    (*result).currentCommandId = currentCommandId;

    /*
     * If we're running in a parallel worker and launching a parallel worker
     * of our own, we can just pass along the information that was passed to
     * us.
     */
    if nParallelCurrentXids > 0 {
        (*result).nParallelCurrentXids = nParallelCurrentXids;
        /* parallelCurrentXids immediately follows the struct header */
        let dest = (result as *mut u8).add(SerializedTransactionStateHeaderSize)
                    as *mut TransactionId;
        ptr::copy_nonoverlapping(ParallelCurrentXids, dest,
                                 nParallelCurrentXids as usize);
        return;
    }

    /*
     * OK, we need to generate a sorted list of XIDs that our workers should
     * view as current.  First, figure out how many there are.
     */
    s = CurrentTransactionState;
    while !s.is_null() {
        if FullTransactionIdIsValid((*s).fullTransactionId) {
            nxids = add_size(nxids, 1);
        }
        nxids = add_size(nxids, (*s).nChildXids as Size);
        s = (*s).parent;
    }
    Assert!(SerializedTransactionStateHeaderSize
            + nxids * core::mem::size_of::<TransactionId>() <= maxsize);

    /* Copy them to our scratch space. */
    let workspace: *mut TransactionId =
        palloc(nxids * core::mem::size_of::<TransactionId>()) as *mut TransactionId;
    s = CurrentTransactionState;
    while !s.is_null() {
        if FullTransactionIdIsValid((*s).fullTransactionId) {
            *workspace.add(i) = XidFromFullTransactionId((*s).fullTransactionId);
            i += 1;
        }
        if (*s).nChildXids > 0 {
            ptr::copy_nonoverlapping((*s).childXids, workspace.add(i),
                                     (*s).nChildXids as usize);
        }
        i += (*s).nChildXids as Size;
        s = (*s).parent;
    }
    Assert!(i == nxids);

    /* Sort them. */
    qsort(workspace as *mut std::ffi::c_void, nxids,
          core::mem::size_of::<TransactionId>(), xidComparator);

    /* Copy data into output area. */
    (*result).nParallelCurrentXids = nxids as i32;
    let dest = (result as *mut u8).add(SerializedTransactionStateHeaderSize)
                as *mut TransactionId;
    ptr::copy_nonoverlapping(workspace, dest, nxids);
}

/*
 * StartParallelWorkerTransaction
 *        Start a parallel worker transaction, restoring the relevant
 *        transaction state serialized by SerializeTransactionState.
 */
pub unsafe fn StartParallelWorkerTransaction(tstatespace: *mut std::os::raw::c_char) {
    Assert!((*CurrentTransactionState).blockState == TBLOCK_DEFAULT);
    StartTransaction();

    let tstate: *mut SerializedTransactionState =
        tstatespace as *mut SerializedTransactionState;
    XactIsoLevel = (*tstate).xactIsoLevel;
    XactDeferrable = (*tstate).xactDeferrable;
    XactTopFullTransactionId = (*tstate).topFullTransactionId;
    (*CurrentTransactionState).fullTransactionId =
        (*tstate).currentFullTransactionId;
    currentCommandId = (*tstate).currentCommandId;
    nParallelCurrentXids = (*tstate).nParallelCurrentXids;
    ParallelCurrentXids = (tstatespace as *mut u8)
        .add(SerializedTransactionStateHeaderSize) as *mut TransactionId;

    (*CurrentTransactionState).blockState = TBLOCK_PARALLEL_INPROGRESS;
}

/*
 * EndParallelWorkerTransaction
 *        End a parallel worker transaction.
 */
pub unsafe fn EndParallelWorkerTransaction() {
    Assert!((*CurrentTransactionState).blockState == TBLOCK_PARALLEL_INPROGRESS);
    CommitTransaction();
    (*CurrentTransactionState).blockState = TBLOCK_DEFAULT;
}

/*
 * ShowTransactionState
 *        Debug support
 */
unsafe fn ShowTransactionState(str_: *const std::os::raw::c_char) {
    /* skip work if message will definitely not be printed */
    if message_level_is_interesting(DEBUG5) {
        ShowTransactionStateRec(str_, CurrentTransactionState);
    }
}

/*
 * ShowTransactionStateRec
 *        Recursive subroutine for ShowTransactionState
 */
unsafe fn ShowTransactionStateRec(str_: *const std::os::raw::c_char, s: TransactionState) {
    let mut buf = StringInfoData {
        data: ptr::null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };

    if !(*s).parent.is_null() {
        /*
         * Since this function recurses, it could be driven to stack overflow.
         * This is just a debugging aid, so we can leave out some details
         * instead of erroring out with check_stack_depth().
         */
        if stack_is_too_deep() {
            ereport!(DEBUG5,
                errmsg!("{} ({}): parent omitted to avoid stack overflow",
                    std::ffi::CStr::from_ptr(str_).to_string_lossy(),
                    (*s).nestingLevel));
            /* C also: errmsg_internal */
        } else {
            ShowTransactionStateRec(str_, (*s).parent);
        }
    }

    initStringInfo(&mut buf);
    if (*s).nChildXids > 0 {
        appendStringInfo(&mut buf,
            b", children: %u\0".as_ptr() as *const _);
        let _ = *(*s).childXids;
        for i in 1..(*s).nChildXids as usize {
            appendStringInfo(&mut buf,
                b" %u\0".as_ptr() as *const _);
            let _ = *(*s).childXids.add(i);
        }
    }
    ereport!(DEBUG5,
        errmsg!("{} ({}) name: {}; blockState: {}; state: {}, xid/subid/cid: {}/{}/{}{}",
            std::ffi::CStr::from_ptr(str_).to_string_lossy(),
            (*s).nestingLevel,
            if !(*s).name.is_null() { std::ffi::CStr::from_ptr((*s).name).to_string_lossy().into_owned() } else { "unnamed".to_owned() },
            BlockStateAsString((*s).blockState),
            TransStateAsString((*s).state),
            XidFromFullTransactionId((*s).fullTransactionId),
            (*s).subTransactionId,
            currentCommandId,
            if currentCommandIdUsed { " (used)" } else { "" }));
    if !buf.data.is_null() {
        pfree(buf.data as *mut std::ffi::c_void);
    }
}

/*
 * BlockStateAsString
 *        Debug support
 */
unsafe fn BlockStateAsString(blockState: TBlockState) -> &'static str {
    match blockState {
        TBLOCK_DEFAULT              => "DEFAULT",
        TBLOCK_STARTED              => "STARTED",
        TBLOCK_BEGIN                => "BEGIN",
        TBLOCK_INPROGRESS           => "INPROGRESS",
        TBLOCK_IMPLICIT_INPROGRESS  => "IMPLICIT_INPROGRESS",
        TBLOCK_PARALLEL_INPROGRESS  => "PARALLEL_INPROGRESS",
        TBLOCK_END                  => "END",
        TBLOCK_ABORT                => "ABORT",
        TBLOCK_ABORT_END            => "ABORT_END",
        TBLOCK_ABORT_PENDING        => "ABORT_PENDING",
        TBLOCK_PREPARE              => "PREPARE",
        TBLOCK_SUBBEGIN             => "SUBBEGIN",
        TBLOCK_SUBINPROGRESS        => "SUBINPROGRESS",
        TBLOCK_SUBRELEASE           => "SUBRELEASE",
        TBLOCK_SUBCOMMIT            => "SUBCOMMIT",
        TBLOCK_SUBABORT             => "SUBABORT",
        TBLOCK_SUBABORT_END         => "SUBABORT_END",
        TBLOCK_SUBABORT_PENDING     => "SUBABORT_PENDING",
        TBLOCK_SUBRESTART           => "SUBRESTART",
        TBLOCK_SUBABORT_RESTART     => "SUBABORT_RESTART",
    }
}

/*
 * TransStateAsString
 *        Debug support
 */
unsafe fn TransStateAsString(state: TransState) -> &'static str {
    match state {
        TRANS_DEFAULT    => "DEFAULT",
        TRANS_START      => "START",
        TRANS_INPROGRESS => "INPROGRESS",
        TRANS_COMMIT     => "COMMIT",
        TRANS_ABORT      => "ABORT",
        TRANS_PREPARE    => "PREPARE",
    }
}

/*
 * xactGetCommittedChildren
 *
 * Gets the list of committed children of the current transaction.
 */
pub unsafe fn xactGetCommittedChildren(ptr: *mut *mut TransactionId) -> i32 {
    let s: TransactionState = CurrentTransactionState;

    if (*s).nChildXids == 0 {
        *ptr = ptr::null_mut();
    } else {
        *ptr = (*s).childXids;
    }

    (*s).nChildXids
}

/*
 *    XLOG support routines
 */

/* stub for strlen */
unsafe fn strlen_c(s: *const std::os::raw::c_char) -> usize {
    /* TODO(pg-port) */
    std::ffi::CStr::from_ptr(s).to_bytes().len()
}

/*
 * Log the commit record for a plain or twophase transaction commit.
 *
 * A 2pc commit will be emitted when twophase_xid is valid, a plain one
 * otherwise.
 */
pub unsafe fn XactLogCommitRecord(
    commit_time: TimestampTz,
    nsubxacts: i32, subxacts: *mut TransactionId,
    nrels: i32, rels: *mut RelFileLocator,
    ndroppedstats: i32, droppedstats: *mut xl_xact_stats_item,
    nmsgs: i32, msgs: *mut SharedInvalidationMessage,
    relcacheInval: bool,
    xactflags: i32, twophase_xid: TransactionId,
    twophase_gid: *const std::os::raw::c_char,
) -> XLogRecPtr {
    let mut xlrec = xl_xact_commit::default();
    let mut xl_xinfo = xl_xact_xinfo::default();
    let mut xl_dbinfo = xl_xact_dbinfo::default();
    let mut xl_subxacts = xl_xact_subxacts::default();
    let mut xl_relfilelocators = xl_xact_relfilelocators::default();
    let mut xl_dropped_stats = xl_xact_stats_items::default();
    let mut xl_invals = xl_xact_invals::default();
    let mut xl_twophase = xl_xact_twophase::default();
    let mut xl_origin = xl_xact_origin::default();
    let mut info: u8;

    Assert!(CritSectionCount > 0);

    xl_xinfo.xinfo = 0;

    /* decide between a plain and 2pc commit */
    if !TransactionIdIsValid(twophase_xid) {
        info = XLOG_XACT_COMMIT;
    } else {
        info = XLOG_XACT_COMMIT_PREPARED;
    }

    /* First figure out and collect all the information needed */

    xlrec.xact_time = commit_time;

    if relcacheInval {
        xl_xinfo.xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
    }
    if forceSyncCommit {
        xl_xinfo.xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
    }
    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK) != 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }

    /*
     * Check if the caller would like to ask standbys for immediate feedback
     * once this commit is applied.
     */
    if synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY {
        xl_xinfo.xinfo |= XACT_COMPLETION_APPLY_FEEDBACK;
    }

    /*
     * Relcache invalidations requires information about the current database
     * and so does logical decoding.
     */
    if nmsgs > 0 || XLogLogicalInfoActive() {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DBINFO;
        xl_dbinfo.dbId = MyDatabaseId;
        xl_dbinfo.tsId = MyDatabaseTableSpace;
    }

    if nsubxacts > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
        xl_subxacts.nsubxacts = nsubxacts;
    }

    if nrels > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        xl_relfilelocators.nrels = nrels;
        info |= XLR_SPECIAL_REL_UPDATE;
    }

    if ndroppedstats > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
        xl_dropped_stats.nitems = ndroppedstats;
    }

    if nmsgs > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_INVALS;
        xl_invals.nmsgs = nmsgs;
    }

    if TransactionIdIsValid(twophase_xid) {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_TWOPHASE;
        xl_twophase.xid = twophase_xid;
        Assert!(!twophase_gid.is_null());

        if XLogLogicalInfoActive() {
            xl_xinfo.xinfo |= XACT_XINFO_HAS_GID;
        }
    }

    /* dump transaction origin information */
    if replorigin_session_origin != InvalidRepOriginId {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_ORIGIN;

        xl_origin.origin_lsn = replorigin_session_origin_lsn;
        xl_origin.origin_timestamp = replorigin_session_origin_timestamp;
    }

    if xl_xinfo.xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    /* Then include all the collected data into the commit record. */

    XLogBeginInsert();

    XLogRegisterData(&xlrec as *const xl_xact_commit as *const std::ffi::c_void,
                     core::mem::size_of::<xl_xact_commit>());

    if xl_xinfo.xinfo != 0 {
        XLogRegisterData(&xl_xinfo.xinfo as *const u32 as *const std::ffi::c_void,
                         core::mem::size_of::<u32>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        XLogRegisterData(&xl_dbinfo as *const xl_xact_dbinfo as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_dbinfo>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        XLogRegisterData(&xl_subxacts as *const xl_xact_subxacts as *const std::ffi::c_void,
                         MinSizeOfXactSubxacts);
        XLogRegisterData(subxacts as *const std::ffi::c_void,
                         nsubxacts as usize * core::mem::size_of::<TransactionId>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        XLogRegisterData(&xl_relfilelocators as *const xl_xact_relfilelocators as *const std::ffi::c_void,
                         MinSizeOfXactRelfileLocators);
        XLogRegisterData(rels as *const std::ffi::c_void,
                         nrels as usize * core::mem::size_of::<RelFileLocator>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_DROPPED_STATS != 0 {
        XLogRegisterData(&xl_dropped_stats as *const xl_xact_stats_items as *const std::ffi::c_void,
                         MinSizeOfXactStatsItems);
        XLogRegisterData(droppedstats as *const std::ffi::c_void,
                         ndroppedstats as usize * core::mem::size_of::<xl_xact_stats_item>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_INVALS != 0 {
        XLogRegisterData(&xl_invals as *const xl_xact_invals as *const std::ffi::c_void,
                         MinSizeOfXactInvals);
        XLogRegisterData(msgs as *const std::ffi::c_void,
                         nmsgs as usize * core::mem::size_of::<SharedInvalidationMessage>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        XLogRegisterData(&xl_twophase as *const xl_xact_twophase as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_twophase>());
        if xl_xinfo.xinfo & XACT_XINFO_HAS_GID != 0 {
            XLogRegisterData(twophase_gid as *const std::ffi::c_void,
                             strlen_c(twophase_gid) + 1);
        }
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        XLogRegisterData(&xl_origin as *const xl_xact_origin as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_origin>());
    }

    /* we allow filtering by xacts */
    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    XLogInsert(RM_XACT_ID, info)
}

/*
 * Log the commit record for a plain or twophase transaction abort.
 *
 * A 2pc abort will be emitted when twophase_xid is valid, a plain one
 * otherwise.
 */
pub unsafe fn XactLogAbortRecord(
    abort_time: TimestampTz,
    nsubxacts: i32, subxacts: *mut TransactionId,
    nrels: i32, rels: *mut RelFileLocator,
    ndroppedstats: i32, droppedstats: *mut xl_xact_stats_item,
    xactflags: i32, twophase_xid: TransactionId,
    twophase_gid: *const std::os::raw::c_char,
) -> XLogRecPtr {
    let mut xlrec = xl_xact_abort::default();
    let mut xl_xinfo = xl_xact_xinfo::default();
    let mut xl_subxacts = xl_xact_subxacts::default();
    let mut xl_relfilelocators = xl_xact_relfilelocators::default();
    let mut xl_dropped_stats = xl_xact_stats_items::default();
    let mut xl_twophase = xl_xact_twophase::default();
    let mut xl_dbinfo = xl_xact_dbinfo::default();
    let mut xl_origin = xl_xact_origin::default();
    let mut info: u8;

    Assert!(CritSectionCount > 0);

    xl_xinfo.xinfo = 0;

    /* decide between a plain and 2pc abort */
    if !TransactionIdIsValid(twophase_xid) {
        info = XLOG_XACT_ABORT;
    } else {
        info = XLOG_XACT_ABORT_PREPARED;
    }

    /* First figure out and collect all the information needed */

    xlrec.xact_time = abort_time;

    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK) != 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }

    if nsubxacts > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
        xl_subxacts.nsubxacts = nsubxacts;
    }

    if nrels > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        xl_relfilelocators.nrels = nrels;
        info |= XLR_SPECIAL_REL_UPDATE;
    }

    if ndroppedstats > 0 {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
        xl_dropped_stats.nitems = ndroppedstats;
    }

    if TransactionIdIsValid(twophase_xid) {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_TWOPHASE;
        xl_twophase.xid = twophase_xid;
        Assert!(!twophase_gid.is_null());

        if XLogLogicalInfoActive() {
            xl_xinfo.xinfo |= XACT_XINFO_HAS_GID;
        }
    }

    if TransactionIdIsValid(twophase_xid) && XLogLogicalInfoActive() {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DBINFO;
        xl_dbinfo.dbId = MyDatabaseId;
        xl_dbinfo.tsId = MyDatabaseTableSpace;
    }

    /*
     * Dump transaction origin information. We need this during recovery to
     * update the replication origin progress.
     */
    if replorigin_session_origin != InvalidRepOriginId {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_ORIGIN;

        xl_origin.origin_lsn = replorigin_session_origin_lsn;
        xl_origin.origin_timestamp = replorigin_session_origin_timestamp;
    }

    if xl_xinfo.xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    /* Then include all the collected data into the abort record. */

    XLogBeginInsert();

    XLogRegisterData(&xlrec as *const xl_xact_abort as *const std::ffi::c_void,
                     MinSizeOfXactAbort);

    if xl_xinfo.xinfo != 0 {
        XLogRegisterData(&xl_xinfo as *const xl_xact_xinfo as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_xinfo>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        XLogRegisterData(&xl_dbinfo as *const xl_xact_dbinfo as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_dbinfo>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        XLogRegisterData(&xl_subxacts as *const xl_xact_subxacts as *const std::ffi::c_void,
                         MinSizeOfXactSubxacts);
        XLogRegisterData(subxacts as *const std::ffi::c_void,
                         nsubxacts as usize * core::mem::size_of::<TransactionId>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        XLogRegisterData(&xl_relfilelocators as *const xl_xact_relfilelocators as *const std::ffi::c_void,
                         MinSizeOfXactRelfileLocators);
        XLogRegisterData(rels as *const std::ffi::c_void,
                         nrels as usize * core::mem::size_of::<RelFileLocator>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_DROPPED_STATS != 0 {
        XLogRegisterData(&xl_dropped_stats as *const xl_xact_stats_items as *const std::ffi::c_void,
                         MinSizeOfXactStatsItems);
        XLogRegisterData(droppedstats as *const std::ffi::c_void,
                         ndroppedstats as usize * core::mem::size_of::<xl_xact_stats_item>());
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        XLogRegisterData(&xl_twophase as *const xl_xact_twophase as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_twophase>());
        if xl_xinfo.xinfo & XACT_XINFO_HAS_GID != 0 {
            XLogRegisterData(twophase_gid as *const std::ffi::c_void,
                             strlen_c(twophase_gid) + 1);
        }
    }

    if xl_xinfo.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        XLogRegisterData(&xl_origin as *const xl_xact_origin as *const std::ffi::c_void,
                         core::mem::size_of::<xl_xact_origin>());
    }

    /* Include the replication origin */
    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    XLogInsert(RM_XACT_ID, info)
}

/*
 * Before 9.0 this was a fairly short function, but now it performs many
 * actions for which the order of execution is critical.
 */
unsafe fn xact_redo_commit(
    parsed: *mut xl_xact_parsed_commit,
    xid: TransactionId,
    lsn: XLogRecPtr,
    origin_id: RepOriginId,
) {
    let max_xid: TransactionId;
    let commit_time: TimestampTz;

    Assert!(TransactionIdIsValid(xid));

    max_xid = TransactionIdLatest(xid, (*parsed).nsubxacts, (*parsed).subxacts);

    /* Make sure nextXid is beyond any XID mentioned in the record. */
    AdvanceNextFullTransactionIdPastXid(max_xid);

    Assert!((((*parsed).xinfo & XACT_XINFO_HAS_ORIGIN) == 0) ==
            (origin_id == InvalidRepOriginId));

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        commit_time = (*parsed).origin_timestamp;
    } else {
        commit_time = (*parsed).xact_time;
    }

    /* Set the transaction commit timestamp and metadata */
    TransactionTreeSetCommitTsData(xid, (*parsed).nsubxacts, (*parsed).subxacts,
                                   commit_time, origin_id);

    if standbyState == STANDBY_DISABLED {
        /*
         * Mark the transaction committed in pg_xact.
         */
        TransactionIdCommitTree(xid, (*parsed).nsubxacts, (*parsed).subxacts);
    } else {
        /*
         * If a transaction completion record arrives that has as-yet
         * unobserved subtransactions then this will not have been fully
         * handled by the call to RecordKnownAssignedTransactionIds() in the
         * main recovery loop in xlog.c.
         */
        RecordKnownAssignedTransactionIds(max_xid);

        /*
         * Mark the transaction committed in pg_xact. We use async commit
         * protocol during recovery to provide information on database
         * consistency for when users try to set hint bits.
         */
        TransactionIdAsyncCommitTree(xid, (*parsed).nsubxacts, (*parsed).subxacts, lsn);

        /*
         * We must mark clog before we update the ProcArray.
         */
        ExpireTreeKnownAssignedTransactionIds(xid, (*parsed).nsubxacts, (*parsed).subxacts, max_xid);

        /*
         * Send any cache invalidations attached to the commit.
         */
        ProcessCommittedInvalidationMessages((*parsed).msgs, (*parsed).nmsgs,
                                             XactCompletionRelcacheInitFileInval((*parsed).xinfo),
                                             (*parsed).dbId, (*parsed).tsId);

        /*
         * Release locks, if any.
         */
        if (*parsed).xinfo & XACT_XINFO_HAS_AE_LOCKS != 0 {
            StandbyReleaseLockTree(xid, (*parsed).nsubxacts, (*parsed).subxacts);
        }
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        /* recover apply progress */
        replorigin_advance(origin_id, (*parsed).origin_lsn, lsn,
                           false /* backward */, false /* WAL */);
    }

    /* Make sure files supposed to be dropped are dropped */
    if (*parsed).nrels > 0 {
        /*
         * First update minimum recovery point to cover this WAL record.
         */
        XLogFlush(lsn);

        /* Make sure files supposed to be dropped are dropped */
        DropRelationFiles((*parsed).xlocators, (*parsed).nrels, true);
    }

    if (*parsed).nstats > 0 {
        /* see equivalent call for relations above */
        XLogFlush(lsn);

        pgstat_execute_transactional_drops((*parsed).nstats, (*parsed).stats, true);
    }

    /*
     * We issue an XLogFlush() for the same reason we emit ForceSyncCommit()
     * in normal operation.
     */
    if XactCompletionForceSyncCommit((*parsed).xinfo) {
        XLogFlush(lsn);
    }

    /*
     * If asked by the primary (because someone is waiting for a synchronous
     * commit = remote_apply), we will need to ask walreceiver to send a reply
     * immediately.
     */
    if XactCompletionApplyFeedback((*parsed).xinfo) {
        XLogRequestWalReceiverReply();
    }
}

/*
 * Be careful with the order of execution, as with xact_redo_commit().
 * The two functions are similar but differ in key places.
 *
 * Note also that an abort can be for a subtransaction and its children,
 * not just for a top level abort.
 */
unsafe fn xact_redo_abort(
    parsed: *mut xl_xact_parsed_abort,
    xid: TransactionId,
    lsn: XLogRecPtr,
    origin_id: RepOriginId,
) {
    let max_xid: TransactionId;

    Assert!(TransactionIdIsValid(xid));

    /* Make sure nextXid is beyond any XID mentioned in the record. */
    max_xid = TransactionIdLatest(xid,
                                   (*parsed).nsubxacts,
                                   (*parsed).subxacts);
    AdvanceNextFullTransactionIdPastXid(max_xid);

    if standbyState == STANDBY_DISABLED {
        /* Mark the transaction aborted in pg_xact, no need for async stuff */
        TransactionIdAbortTree(xid, (*parsed).nsubxacts, (*parsed).subxacts);
    } else {
        /*
         * If a transaction completion record arrives that has as-yet
         * unobserved subtransactions then this will not have been fully
         * handled by the call to RecordKnownAssignedTransactionIds() in the
         * main recovery loop in xlog.c.
         */
        RecordKnownAssignedTransactionIds(max_xid);

        /* Mark the transaction aborted in pg_xact, no need for async stuff */
        TransactionIdAbortTree(xid, (*parsed).nsubxacts, (*parsed).subxacts);

        /*
         * We must update the ProcArray after we have marked clog.
         */
        ExpireTreeKnownAssignedTransactionIds(xid, (*parsed).nsubxacts, (*parsed).subxacts, max_xid);

        /*
         * There are no invalidation messages to send or undo.
         */

        /*
         * Release locks, if any. There are no invalidations to send.
         */
        if (*parsed).xinfo & XACT_XINFO_HAS_AE_LOCKS != 0 {
            StandbyReleaseLockTree(xid, (*parsed).nsubxacts, (*parsed).subxacts);
        }
    }

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        /* recover apply progress */
        replorigin_advance(origin_id, (*parsed).origin_lsn, lsn,
                           false /* backward */, false /* WAL */);
    }

    /* Make sure files supposed to be dropped are dropped */
    if (*parsed).nrels > 0 {
        /*
         * See comments about update of minimum recovery point on truncation,
         * in xact_redo_commit().
         */
        XLogFlush(lsn);

        DropRelationFiles((*parsed).xlocators, (*parsed).nrels, true);
    }

    if (*parsed).nstats > 0 {
        /* see equivalent call for relations above */
        XLogFlush(lsn);

        pgstat_execute_transactional_drops((*parsed).nstats, (*parsed).stats, true);
    }
}

pub unsafe fn xact_redo(record: *mut XLogReaderState) {
    let info: u8 = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

    /* Backup blocks are not used in xact records */
    Assert!(!XLogRecHasAnyBlockRefs(record));

    if info == XLOG_XACT_COMMIT {
        let xlrec: *mut xl_xact_commit = XLogRecGetData(record) as *mut xl_xact_commit;
        let mut parsed = core::mem::zeroed::<xl_xact_parsed_commit>();

        ParseCommitRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
        xact_redo_commit(&mut parsed, XLogRecGetXid(record),
                         /* record->EndRecPtr */ 0, XLogRecGetOrigin(record));
    } else if info == XLOG_XACT_COMMIT_PREPARED {
        let xlrec: *mut xl_xact_commit = XLogRecGetData(record) as *mut xl_xact_commit;
        let mut parsed = core::mem::zeroed::<xl_xact_parsed_commit>();

        ParseCommitRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
        xact_redo_commit(&mut parsed, parsed.twophase_xid,
                         /* record->EndRecPtr */ 0, XLogRecGetOrigin(record));

        /* Delete TwoPhaseState gxact entry and/or 2PC file. */
        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
        PrepareRedoRemove(parsed.twophase_xid, false);
        LWLockRelease(TwoPhaseStateLock);
    } else if info == XLOG_XACT_ABORT {
        let xlrec: *mut xl_xact_abort = XLogRecGetData(record) as *mut xl_xact_abort;
        let mut parsed = core::mem::zeroed::<xl_xact_parsed_abort>();

        ParseAbortRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
        xact_redo_abort(&mut parsed, XLogRecGetXid(record),
                        /* record->EndRecPtr */ 0, XLogRecGetOrigin(record));
    } else if info == XLOG_XACT_ABORT_PREPARED {
        let xlrec: *mut xl_xact_abort = XLogRecGetData(record) as *mut xl_xact_abort;
        let mut parsed = core::mem::zeroed::<xl_xact_parsed_abort>();

        ParseAbortRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
        xact_redo_abort(&mut parsed, parsed.twophase_xid,
                        /* record->EndRecPtr */ 0, XLogRecGetOrigin(record));

        /* Delete TwoPhaseState gxact entry and/or 2PC file. */
        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
        PrepareRedoRemove(parsed.twophase_xid, false);
        LWLockRelease(TwoPhaseStateLock);
    } else if info == XLOG_XACT_PREPARE {
        /*
         * Store xid and start/end pointers of the WAL record in TwoPhaseState
         * gxact entry.
         */
        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
        PrepareRedoAdd(XLogRecGetData(record),
                       /* record->ReadRecPtr */ 0,
                       /* record->EndRecPtr */ 0,
                       XLogRecGetOrigin(record));
        LWLockRelease(TwoPhaseStateLock);
    } else if info == XLOG_XACT_ASSIGNMENT {
        let xlrec: *mut xl_xact_assignment =
            XLogRecGetData(record) as *mut xl_xact_assignment;

        if standbyState >= STANDBY_INITIALIZED {
            ProcArrayApplyXidAssignment((*xlrec).xtop,
                                        (*xlrec).nsubxacts,
                                        /* xlrec->xsub follows inline */ ptr::null_mut());
        }
    } else if info == XLOG_XACT_INVALIDATIONS {
        /*
         * XXX we do ignore this for now, what matters are invalidations
         * written into the commit record.
         */
    } else {
        elog!(PANIC, "xact_redo: unknown op code {}", info);
    }
}

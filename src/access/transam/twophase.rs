//! twophase.rs
//!   Two-phase commit support functions.
//!
//! Translated 1:1 from postgres/src/backend/access/transam/twophase.c
//! (decls merged from postgres/src/include/access/twophase.h).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES
//!     Each global transaction is associated with a global transaction
//!     identifier (GID). The client assigns a GID to a postgres
//!     transaction with the PREPARE TRANSACTION command.
//!
//!     We keep all active global transactions in a shared memory array.
//!     When the PREPARE TRANSACTION command is issued, the GID is
//!     reserved for the transaction in the array. This is done before
//!     a WAL entry is made, because the reservation checks for duplicate
//!     GIDs and aborts the transaction if there already is a global
//!     transaction in prepared state with the same GID.
//!
//!     A global transaction (gxact) also has dummy PGPROC; this is what keeps
//!     the XID considered running by TransactionIdIsInProgress.  It is also
//!     convenient as a PGPROC to hook the gxact's locks to.
//!
//!     Information to recover prepared transactions in case of crash is
//!     now stored in WAL for the common case. In some cases there will be
//!     an extended period between preparing a GXACT and commit/abort, in
//!     which case we need to separately record prepared transaction data
//!     in permanent storage. This includes locking information, pending
//!     notifications etc. All that state information is written to the
//!     per-transaction state file in the pg_twophase directory.
//!     All prepared transactions will be written prior to shutdown.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]

use crate::prelude::*;
use crate::pg_config_manual::MAXPGPATH;

use std::ffi::CStr;

use crate::c::{int64, uint16, uint32, uint64, Size, TransactionId};
use crate::access::transam::{
    FullTransactionId, EpochFromFullTransactionId, XidFromFullTransactionId,
    FullTransactionIdFromU64, InvalidTransactionId, TransactionIdEquals, TransactionIdIsValid,
};
use crate::access::transam::xlogdefs::{XLogRecPtr, InvalidXLogRecPtr};
use crate::access::transam::xlogreader::{RepOriginId, InvalidRepOriginId};
use crate::access::transam::twophase_rmgr::{
    TwoPhaseCallback, TWOPHASE_RM_END_ID, TWOPHASE_RM_MAX_ID,
    twophase_postcommit_callbacks, twophase_postabort_callbacks, twophase_recover_callbacks,
};
use crate::access::rmgrdesc::xactdesc::{xl_xact_prepare, xl_xact_stats_item, GIDSIZE};
use crate::port::pg_crc32c::{pg_crc32c, INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C};

/*
 * Directory where Two-phase commit files reside within PGDATA
 */
const TWOPHASE_DIR: &CStr = c"pg_twophase";

/* GUC variable, can't be changed after startup */
#[no_mangle]
pub static mut max_prepared_xacts: c_int = 0;

/*
 * This struct describes one global transaction that is in prepared state
 * or attempting to become prepared.
 *
 * typedef struct GlobalTransactionData *GlobalTransaction appears in
 * twophase.h
 */
pub type GlobalTransaction = *mut GlobalTransactionData;

#[repr(C)]
pub struct GlobalTransactionData {
    pub next: GlobalTransaction,      /* list link for free list */
    pub pgprocno: c_int,              /* ID of associated dummy PGPROC */
    pub prepared_at: TimestampTz,     /* time of preparation */

    /*
     * Note that we need to keep track of two LSNs for each GXACT. We keep
     * track of the start LSN because this is the address we must use to read
     * state data back from WAL when committing a prepared GXACT. We keep
     * track of the end LSN because that is the LSN we need to wait for prior
     * to commit.
     */
    pub prepare_start_lsn: XLogRecPtr, /* XLOG offset of prepare record start */
    pub prepare_end_lsn: XLogRecPtr,   /* XLOG offset of prepare record end */
    pub xid: TransactionId,            /* The GXACT id */

    pub owner: Oid,                    /* ID of user that executed the xact */
    pub locking_backend: ProcNumber,   /* backend currently working on the xact */
    pub valid: bool,                   /* true if PGPROC entry is in proc array */
    pub ondisk: bool,                  /* true if prepare state file is on disk */
    pub inredo: bool,                  /* true if entry was added via xlog_redo */
    pub gid: [c_char; GIDSIZE],        /* The GID assigned to the prepared xact */
}

/*
 * Two Phase Commit shared state.  Access to this struct is protected
 * by TwoPhaseStateLock.
 */
#[repr(C)]
pub struct TwoPhaseStateData {
    /* Head of linked list of free GlobalTransactionData structs */
    pub freeGXacts: GlobalTransaction,

    /* Number of valid prepXacts entries. */
    pub numPrepXacts: c_int,

    /* There are max_prepared_xacts items in this array */
    pub prepXacts: [GlobalTransaction; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

static mut TwoPhaseState: *mut TwoPhaseStateData = core::ptr::null_mut();

/*
 * Global transaction entry currently locked by us, if any.  Note that any
 * access to the entry pointed to by this variable must be protected by
 * TwoPhaseStateLock, though obviously the pointer itself doesn't need to be
 * (since it's just local memory).
 */
static mut MyLockedGxact: GlobalTransaction = core::ptr::null_mut();

static mut twophaseExitRegistered: bool = false;

/*
 * Initialization of shared memory
 */
pub unsafe fn TwoPhaseShmemSize() -> Size {
    let mut size: Size;

    /* Need the fixed struct, the array of pointers, and the GTD structs */
    size = offsetof_TwoPhaseStateData_prepXacts();
    size = add_size(size, mul_size(max_prepared_xacts as Size,
                                   core::mem::size_of::<GlobalTransaction>() as Size));
    size = MAXALIGN(size);
    size = add_size(size, mul_size(max_prepared_xacts as Size,
                                   core::mem::size_of::<GlobalTransactionData>() as Size));

    size
}

pub unsafe fn TwoPhaseShmemInit() {
    let mut found: bool = false;

    TwoPhaseState = ShmemInitStruct(c"Prepared Transaction Table".as_ptr(),
                                    TwoPhaseShmemSize(),
                                    &raw mut found) as *mut TwoPhaseStateData;
    if !IsUnderPostmaster {
        let gxacts: GlobalTransaction;
        let mut i: c_int;

        Assert!(!found);
        (*TwoPhaseState).freeGXacts = core::ptr::null_mut();
        (*TwoPhaseState).numPrepXacts = 0;

        /*
         * Initialize the linked list of free GlobalTransactionData structs
         */
        gxacts = ((TwoPhaseState as *mut c_char).add(
            MAXALIGN(offsetof_TwoPhaseStateData_prepXacts() +
                     core::mem::size_of::<GlobalTransaction>() as Size * max_prepared_xacts as Size)
                as usize)) as GlobalTransaction;
        i = 0;
        while i < max_prepared_xacts {
            /* insert into linked list */
            (*gxacts.add(i as usize)).next = (*TwoPhaseState).freeGXacts;
            (*TwoPhaseState).freeGXacts = gxacts.add(i as usize);

            /* associate it with a PGPROC assigned by InitProcGlobal */
            (*gxacts.add(i as usize)).pgprocno =
                GetNumberFromPGProc(&raw mut PreparedXactProcs[i as usize]);
            i += 1;
        }
    } else {
        Assert!(found);
    }
}

/*
 * Exit hook to unlock the global transaction entry we're working on.
 */
unsafe fn AtProcExit_Twophase(code: c_int, arg: Datum) {
    /* same logic as abort */
    AtAbort_Twophase();
}

/*
 * Abort hook to unlock the global transaction entry we're working on.
 */
pub unsafe fn AtAbort_Twophase() {
    if MyLockedGxact.is_null() {
        return;
    }

    /*
     * What to do with the locked global transaction entry?  If we were in the
     * process of preparing the transaction, but haven't written the WAL
     * record and state file yet, the transaction must not be considered as
     * prepared.  Likewise, if we are in the process of finishing an
     * already-prepared transaction, and fail after having already written the
     * 2nd phase commit or rollback record to the WAL, the transaction should
     * not be considered as prepared anymore.  In those cases, just remove the
     * entry from shared memory.
     *
     * Otherwise, the entry must be left in place so that the transaction can
     * be finished later, so just unlock it.
     *
     * If we abort during prepare, after having written the WAL record, we
     * might not have transferred all locks and other state to the prepared
     * transaction yet.  Likewise, if we abort during commit or rollback,
     * after having written the WAL record, we might not have released all the
     * resources held by the transaction yet.  In those cases, the in-memory
     * state can be wrong, but it's too late to back out.
     */
    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    if !(*MyLockedGxact).valid {
        RemoveGXact(MyLockedGxact);
    } else {
        (*MyLockedGxact).locking_backend = INVALID_PROC_NUMBER;
    }
    LWLockRelease(TwoPhaseStateLock());

    MyLockedGxact = core::ptr::null_mut();
}

/*
 * This is called after we have finished transferring state to the prepared
 * PGPROC entry.
 */
pub unsafe fn PostPrepare_Twophase() {
    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    (*MyLockedGxact).locking_backend = INVALID_PROC_NUMBER;
    LWLockRelease(TwoPhaseStateLock());

    MyLockedGxact = core::ptr::null_mut();
}


/*
 * MarkAsPreparing
 *		Reserve the GID for the given transaction.
 */
pub unsafe fn MarkAsPreparing(
    xid: TransactionId,
    gid: *const c_char,
    prepared_at: TimestampTz,
    owner: Oid,
    databaseid: Oid,
) -> GlobalTransaction {
    let mut gxact: GlobalTransaction;
    let mut i: c_int;

    if strlen(gid) >= GIDSIZE {
        ereport!(ERROR,
                 errmsg!("transaction identifier \"{}\" is too long",
                         CStr::from_ptr(gid).to_string_lossy()));
    }

    /* fail immediately if feature is disabled */
    if max_prepared_xacts == 0 {
        ereport!(ERROR,
                 errmsg!("prepared transactions are disabled"));
    }

    /* on first call, register the exit hook */
    if !twophaseExitRegistered {
        before_shmem_exit(AtProcExit_Twophase, 0);
        twophaseExitRegistered = true;
    }

    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);

    /* Check for conflicting GID */
    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        gxact = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);
        if strcmp((*gxact).gid.as_ptr(), gid) == 0 {
            ereport!(ERROR,
                     errmsg!("transaction identifier \"{}\" is already in use",
                             CStr::from_ptr(gid).to_string_lossy()));
        }
        i += 1;
    }

    /* Get a free gxact from the freelist */
    if (*TwoPhaseState).freeGXacts.is_null() {
        ereport!(ERROR,
                 errmsg!("maximum number of prepared transactions reached"));
    }
    gxact = (*TwoPhaseState).freeGXacts;
    (*TwoPhaseState).freeGXacts = (*gxact).next;

    MarkAsPreparingGuts(gxact, xid, gid, prepared_at, owner, databaseid);

    (*gxact).ondisk = false;

    /* And insert it into the active array */
    Assert!((*TwoPhaseState).numPrepXacts < max_prepared_xacts);
    let idx = (*TwoPhaseState).numPrepXacts;
    (*TwoPhaseState).numPrepXacts += 1;
    *(*TwoPhaseState).prepXacts.as_mut_ptr().add(idx as usize) = gxact;

    LWLockRelease(TwoPhaseStateLock());

    gxact
}

/*
 * MarkAsPreparingGuts
 *
 * This uses a gxact struct and puts it into the active array.
 * NOTE: this is also used when reloading a gxact after a crash; so avoid
 * assuming that we can use very much backend context.
 *
 * Note: This function should be called with appropriate locks held.
 */
unsafe fn MarkAsPreparingGuts(
    gxact: GlobalTransaction,
    xid: TransactionId,
    gid: *const c_char,
    prepared_at: TimestampTz,
    owner: Oid,
    databaseid: Oid,
) {
    let proc: *mut PGPROC;
    let mut i: c_int;

    Assert!(LWLockHeldByMeInMode(TwoPhaseStateLock(), LW_EXCLUSIVE));

    Assert!(!gxact.is_null());
    proc = GetPGProcByNumber((*gxact).pgprocno);

    /* Initialize the PGPROC entry */
    MemSet(proc as *mut c_void, 0, core::mem::size_of::<PGPROC>() as Size);
    dlist_node_init(&raw mut (*proc).links);
    (*proc).waitStatus = PROC_WAIT_STATUS_OK;
    if LocalTransactionIdIsValid((*MyProc).vxid.lxid) {
        /* clone VXID, for TwoPhaseGetXidByVirtualXID() to find */
        (*proc).vxid.lxid = (*MyProc).vxid.lxid;
        (*proc).vxid.procNumber = MyProcNumber;
    } else {
        Assert!(AmStartupProcess() || !IsPostmasterEnvironment);
        /* GetLockConflicts() uses this to specify a wait on the XID */
        (*proc).vxid.lxid = xid;
        (*proc).vxid.procNumber = INVALID_PROC_NUMBER;
    }
    (*proc).xid = xid;
    Assert!((*proc).xmin == InvalidTransactionId);
    (*proc).delayChkptFlags = 0;
    (*proc).statusFlags = 0;
    (*proc).pid = 0;
    (*proc).databaseId = databaseid;
    (*proc).roleId = owner;
    (*proc).tempNamespaceId = InvalidOid;
    (*proc).isRegularBackend = false;
    (*proc).lwWaiting = LW_WS_NOT_WAITING;
    (*proc).lwWaitMode = 0;
    (*proc).waitLock = core::ptr::null_mut();
    (*proc).waitProcLock = core::ptr::null_mut();
    pg_atomic_init_u64(&raw mut (*proc).waitStart, 0);
    i = 0;
    while i < NUM_LOCK_PARTITIONS {
        dlist_init(&raw mut (*proc).myProcLocks[i as usize]);
        i += 1;
    }
    /* subxid data must be filled later by GXactLoadSubxactData */
    (*proc).subxidStatus.overflowed = false;
    (*proc).subxidStatus.count = 0;

    (*gxact).prepared_at = prepared_at;
    (*gxact).xid = xid;
    (*gxact).owner = owner;
    (*gxact).locking_backend = MyProcNumber;
    (*gxact).valid = false;
    (*gxact).inredo = false;
    strcpy((*gxact).gid.as_mut_ptr(), gid);

    /*
     * Remember that we have this GlobalTransaction entry locked for us. If we
     * abort after this, we must release it.
     */
    MyLockedGxact = gxact;
}

/*
 * GXactLoadSubxactData
 *
 * If the transaction being persisted had any subtransactions, this must
 * be called before MarkAsPrepared() to load information into the dummy
 * PGPROC.
 */
unsafe fn GXactLoadSubxactData(
    gxact: GlobalTransaction,
    mut nsubxacts: c_int,
    children: *mut TransactionId,
) {
    let proc: *mut PGPROC = GetPGProcByNumber((*gxact).pgprocno);

    /* We need no extra lock since the GXACT isn't valid yet */
    if nsubxacts > PGPROC_MAX_CACHED_SUBXIDS {
        (*proc).subxidStatus.overflowed = true;
        nsubxacts = PGPROC_MAX_CACHED_SUBXIDS;
    }
    if nsubxacts > 0 {
        memcpy((*proc).subxids.xids.as_mut_ptr() as *mut c_void, children as *const c_void,
               nsubxacts as usize * core::mem::size_of::<TransactionId>());
        (*proc).subxidStatus.count = nsubxacts as uint8;
    }
}

/*
 * MarkAsPrepared
 *		Mark the GXACT as fully valid, and enter it into the global ProcArray.
 *
 * lock_held indicates whether caller already holds TwoPhaseStateLock.
 */
unsafe fn MarkAsPrepared(gxact: GlobalTransaction, lock_held: bool) {
    /* Lock here may be overkill, but I'm not convinced of that ... */
    if !lock_held {
        LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    }
    Assert!(!(*gxact).valid);
    (*gxact).valid = true;
    if !lock_held {
        LWLockRelease(TwoPhaseStateLock());
    }

    /*
     * Put it into the global ProcArray so TransactionIdIsInProgress considers
     * the XID as still running.
     */
    ProcArrayAdd(GetPGProcByNumber((*gxact).pgprocno));
}

/*
 * LockGXact
 *		Locate the prepared transaction and mark it busy for COMMIT or PREPARE.
 */
unsafe fn LockGXact(gid: *const c_char, user: Oid) -> GlobalTransaction {
    let mut i: c_int;

    /* on first call, register the exit hook */
    if !twophaseExitRegistered {
        before_shmem_exit(AtProcExit_Twophase, 0);
        twophaseExitRegistered = true;
    }

    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);

    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);
        let proc: *mut PGPROC = GetPGProcByNumber((*gxact).pgprocno);

        /* Ignore not-yet-valid GIDs */
        if !(*gxact).valid {
            i += 1;
            continue;
        }
        if strcmp((*gxact).gid.as_ptr(), gid) != 0 {
            i += 1;
            continue;
        }

        /* Found it, but has someone else got it locked? */
        if (*gxact).locking_backend != INVALID_PROC_NUMBER {
            ereport!(ERROR,
                     errmsg!("prepared transaction with identifier \"{}\" is busy",
                             CStr::from_ptr(gid).to_string_lossy()));
        }

        if user != (*gxact).owner && !superuser_arg(user) {
            ereport!(ERROR,
                     errmsg!("permission denied to finish prepared transaction"));
        }

        /*
         * Note: it probably would be possible to allow committing from
         * another database; but at the moment NOTIFY is known not to work and
         * there may be some other issues as well.  Hence disallow until
         * someone gets motivated to make it work.
         */
        if MyDatabaseId != (*proc).databaseId {
            ereport!(ERROR,
                     errmsg!("prepared transaction belongs to another database"));
        }

        /* OK for me to lock it */
        (*gxact).locking_backend = MyProcNumber;
        MyLockedGxact = gxact;

        LWLockRelease(TwoPhaseStateLock());

        return gxact;
    }

    LWLockRelease(TwoPhaseStateLock());

    ereport!(ERROR,
             errmsg!("prepared transaction with identifier \"{}\" does not exist",
                     CStr::from_ptr(gid).to_string_lossy()));

    /* NOTREACHED */
    #[allow(unreachable_code)]
    core::ptr::null_mut()
}

/*
 * RemoveGXact
 *		Remove the prepared transaction from the shared memory array.
 *
 * NB: caller should have already removed it from ProcArray
 */
unsafe fn RemoveGXact(gxact: GlobalTransaction) {
    let mut i: c_int;

    Assert!(LWLockHeldByMeInMode(TwoPhaseStateLock(), LW_EXCLUSIVE));

    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        if gxact == *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize) {
            /* remove from the active array */
            (*TwoPhaseState).numPrepXacts -= 1;
            let last = (*TwoPhaseState).numPrepXacts;
            *(*TwoPhaseState).prepXacts.as_mut_ptr().add(i as usize) =
                *(*TwoPhaseState).prepXacts.as_ptr().add(last as usize);

            /* and put it back in the freelist */
            (*gxact).next = (*TwoPhaseState).freeGXacts;
            (*TwoPhaseState).freeGXacts = gxact;

            return;
        }
        i += 1;
    }

    elog!(ERROR, "failed to find {:p} in GlobalTransaction array", gxact);
}

/*
 * Returns an array of all prepared transactions for the user-level
 * function pg_prepared_xact.
 *
 * The returned array and all its elements are copies of internal data
 * structures, to minimize the time we need to hold the TwoPhaseStateLock.
 *
 * WARNING -- we return even those transactions that are not fully prepared
 * yet.  The caller should filter them out if he doesn't want them.
 *
 * The returned array is palloc'd.
 */
unsafe fn GetPreparedTransactionList(gxacts: *mut GlobalTransaction) -> c_int {
    let array: GlobalTransaction;
    let num: c_int;
    let mut i: c_int;

    LWLockAcquire(TwoPhaseStateLock(), LW_SHARED);

    if (*TwoPhaseState).numPrepXacts == 0 {
        LWLockRelease(TwoPhaseStateLock());

        *gxacts = core::ptr::null_mut();
        return 0;
    }

    num = (*TwoPhaseState).numPrepXacts;
    array = palloc(core::mem::size_of::<GlobalTransactionData>() as Size * num as Size)
        as GlobalTransaction;
    *gxacts = array;
    i = 0;
    while i < num {
        memcpy(array.add(i as usize) as *mut c_void,
               *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize) as *const c_void,
               core::mem::size_of::<GlobalTransactionData>());
        i += 1;
    }

    LWLockRelease(TwoPhaseStateLock());

    num
}


/* Working status for pg_prepared_xact */
#[repr(C)]
struct Working_State {
    array: GlobalTransaction,
    ngxacts: c_int,
    currIdx: c_int,
}

/*
 * pg_prepared_xact
 *		Produce a view with one row per prepared transaction.
 *
 * This function is here so we don't have to export the
 * GlobalTransactionData struct definition.
 */
pub unsafe fn pg_prepared_xact(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;
    let mut status: *mut Working_State;

    if SRF_IS_FIRSTCALL() {
        let tupdesc: TupleDesc;
        let oldcontext: MemoryContext;

        /* create a function context for cross-call persistence */
        let funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pg_prepared_xacts view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(5);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"transaction".as_ptr(),
                           XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"gid".as_ptr(),
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"prepared".as_ptr(),
                           TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"ownerid".as_ptr(),
                           OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"dbid".as_ptr(),
                           OIDOID, -1, 0);

        (*funcctx).tuple_desc = BlessTupleDesc(tupdesc);

        /*
         * Collect all the 2PC status information that we will format and send
         * out as a result set.
         */
        status = palloc(core::mem::size_of::<Working_State>() as Size) as *mut Working_State;
        (*funcctx).user_fctx = status as *mut c_void;

        (*status).ngxacts = GetPreparedTransactionList(&raw mut (*status).array);
        (*status).currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status = (*funcctx).user_fctx as *mut Working_State;

    while !(*status).array.is_null() && (*status).currIdx < (*status).ngxacts {
        let gxact: GlobalTransaction = (*status).array.add((*status).currIdx as usize);
        (*status).currIdx += 1;
        let proc: *mut PGPROC = GetPGProcByNumber((*gxact).pgprocno);
        let mut values: [Datum; 5] = [0; 5];
        let mut nulls: [bool; 5] = [false; 5];
        let tuple: HeapTuple;
        let result: Datum;

        if !(*gxact).valid {
            continue;
        }

        /*
         * Form tuple with appropriate data.
         */

        values[0] = TransactionIdGetDatum((*proc).xid);
        values[1] = CStringGetTextDatum((*gxact).gid.as_ptr());
        values[2] = TimestampTzGetDatum((*gxact).prepared_at);
        values[3] = ObjectIdGetDatum((*gxact).owner);
        values[4] = ObjectIdGetDatum((*proc).databaseId);

        tuple = heap_form_tuple((*funcctx).tuple_desc, values.as_mut_ptr(), nulls.as_mut_ptr());
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT!(funcctx, result);
    }

    SRF_RETURN_DONE!(funcctx)
}

/*
 * TwoPhaseGetGXact
 *		Get the GlobalTransaction struct for a prepared transaction
 *		specified by XID
 *
 * If lock_held is set to true, TwoPhaseStateLock will not be taken, so the
 * caller had better hold it.
 */
unsafe fn TwoPhaseGetGXact(xid: TransactionId, lock_held: bool) -> GlobalTransaction {
    let mut result: GlobalTransaction = core::ptr::null_mut();
    let mut i: c_int;

    static mut cached_xid: TransactionId = InvalidTransactionId;
    static mut cached_gxact: GlobalTransaction = core::ptr::null_mut();

    Assert!(!lock_held || LWLockHeldByMe(TwoPhaseStateLock()));

    /*
     * During a recovery, COMMIT PREPARED, or ABORT PREPARED, we'll be called
     * repeatedly for the same XID.  We can save work with a simple cache.
     */
    if xid == cached_xid {
        return cached_gxact;
    }

    if !lock_held {
        LWLockAcquire(TwoPhaseStateLock(), LW_SHARED);
    }

    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        if (*gxact).xid == xid {
            result = gxact;
            break;
        }
        i += 1;
    }

    if !lock_held {
        LWLockRelease(TwoPhaseStateLock());
    }

    if result.is_null() {
        /* should not happen */
        elog!(ERROR, "failed to find GlobalTransaction for xid {}", xid);
    }

    cached_xid = xid;
    cached_gxact = result;

    result
}

/*
 * TwoPhaseGetXidByVirtualXID
 *		Lookup VXID among xacts prepared since last startup.
 *
 * (This won't find recovered xacts.)  If more than one matches, return any
 * and set "have_more" to true.  To witness multiple matches, a single
 * proc number must consume 2^32 LXIDs, with no intervening database restart.
 */
pub unsafe fn TwoPhaseGetXidByVirtualXID(
    vxid: VirtualTransactionId,
    have_more: *mut bool,
) -> TransactionId {
    let mut i: c_int;
    let mut result: TransactionId = InvalidTransactionId;

    Assert!(VirtualTransactionIdIsValid(vxid));
    LWLockAcquire(TwoPhaseStateLock(), LW_SHARED);

    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);
        let proc: *mut PGPROC;
        let mut proc_vxid: VirtualTransactionId = core::mem::zeroed();

        if !(*gxact).valid {
            i += 1;
            continue;
        }
        proc = GetPGProcByNumber((*gxact).pgprocno);
        GET_VXID_FROM_PGPROC(&raw mut proc_vxid, &*proc);
        if VirtualTransactionIdEquals(vxid, proc_vxid) {
            /*
             * Startup process sets proc->vxid.procNumber to
             * INVALID_PROC_NUMBER.
             */
            Assert!(!(*gxact).inredo);

            if result != InvalidTransactionId {
                *have_more = true;
                break;
            }
            result = (*gxact).xid;
        }
        i += 1;
    }

    LWLockRelease(TwoPhaseStateLock());

    result
}

/*
 * TwoPhaseGetDummyProcNumber
 *		Get the dummy proc number for prepared transaction specified by XID
 *
 * Dummy proc numbers are similar to proc numbers of real backends.  They
 * start at FIRST_PREPARED_XACT_PROC_NUMBER, and are unique across all
 * currently active real backends and prepared transactions.  If lock_held is
 * set to true, TwoPhaseStateLock will not be taken, so the caller had better
 * hold it.
 */
pub unsafe fn TwoPhaseGetDummyProcNumber(xid: TransactionId, lock_held: bool) -> ProcNumber {
    let gxact: GlobalTransaction = TwoPhaseGetGXact(xid, lock_held);

    (*gxact).pgprocno
}

/*
 * TwoPhaseGetDummyProc
 *		Get the PGPROC that represents a prepared transaction specified by XID
 *
 * If lock_held is set to true, TwoPhaseStateLock will not be taken, so the
 * caller had better hold it.
 */
pub unsafe fn TwoPhaseGetDummyProc(xid: TransactionId, lock_held: bool) -> *mut PGPROC {
    let gxact: GlobalTransaction = TwoPhaseGetGXact(xid, lock_held);

    GetPGProcByNumber((*gxact).pgprocno)
}

/************************************************************************/
/* State file support													*/
/************************************************************************/

/*
 * Compute the FullTransactionId for the given TransactionId.
 *
 * This is safe if the xid has not yet reached COMMIT PREPARED or ROLLBACK
 * PREPARED.  After those commands, concurrent vac_truncate_clog() may make
 * the xid cease to qualify as allowable.  XXX Not all callers limit their
 * calls accordingly.
 */
#[inline]
unsafe fn AdjustToFullTransactionId(xid: TransactionId) -> FullTransactionId {
    Assert!(TransactionIdIsValid(xid));
    FullTransactionIdFromAllowableAt(ReadNextFullTransactionId(), xid)
}

#[inline]
unsafe fn TwoPhaseFilePath(path: *mut c_char, xid: TransactionId) -> c_int {
    let fxid: FullTransactionId = AdjustToFullTransactionId(xid);

    snprintf_twophase_path(path, MAXPGPATH,
                           EpochFromFullTransactionId(fxid),
                           XidFromFullTransactionId(fxid))
}

/*
 * 2PC state file format:
 *
 *	1. TwoPhaseFileHeader
 *	2. TransactionId[] (subtransactions)
 *	3. RelFileLocator[] (files to be deleted at commit)
 *	4. RelFileLocator[] (files to be deleted at abort)
 *	5. SharedInvalidationMessage[] (inval messages to be sent at commit)
 *	6. TwoPhaseRecordOnDisk
 *	7. ...
 *	8. TwoPhaseRecordOnDisk (end sentinel, rmid == TWOPHASE_RM_END_ID)
 *	9. checksum (CRC-32C)
 *
 * Each segment except the final checksum is MAXALIGN'd.
 */

/*
 * Header for a 2PC state file
 */
const TWOPHASE_MAGIC: uint32 = 0x57F94534; /* format identifier */

pub type TwoPhaseFileHeader = xl_xact_prepare;

/*
 * Header for each record in a state file
 *
 * NOTE: len counts only the rmgr data, not the TwoPhaseRecordOnDisk header.
 * The rmgr data will be stored starting on a MAXALIGN boundary.
 */
#[repr(C)]
pub struct TwoPhaseRecordOnDisk {
    pub len: uint32,           /* length of rmgr data */
    pub rmid: TwoPhaseRmgrId,  /* resource manager for this record */
    pub info: uint16,          /* flag bits for use by rmgr */
}

/*
 * During prepare, the state file is assembled in memory before writing it
 * to WAL and the actual state file.  We use a chain of StateFileChunk blocks
 * for that.
 */
#[repr(C)]
pub struct StateFileChunk {
    pub data: *mut c_char,
    pub len: uint32,
    pub next: *mut StateFileChunk,
}

#[repr(C)]
struct xllist {
    head: *mut StateFileChunk, /* first data block in the chain */
    tail: *mut StateFileChunk, /* last block in chain */
    num_chunks: uint32,
    bytes_free: uint32,        /* free bytes left in tail block */
    total_len: uint32,         /* total data bytes in chain */
}

static mut records: xllist = xllist {
    head: core::ptr::null_mut(),
    tail: core::ptr::null_mut(),
    num_chunks: 0,
    bytes_free: 0,
    total_len: 0,
};


/*
 * Append a block of data to records data structure.
 *
 * NB: each block is padded to a MAXALIGN multiple.  This must be
 * accounted for when the file is later read!
 *
 * The data is copied, so the caller is free to modify it afterwards.
 */
unsafe fn save_state_data(data: *const c_void, len: uint32) {
    let padlen: uint32 = MAXALIGN(len as Size) as uint32;

    if padlen > records.bytes_free {
        (*records.tail).next = palloc0(core::mem::size_of::<StateFileChunk>() as Size)
            as *mut StateFileChunk;
        records.tail = (*records.tail).next;
        (*records.tail).len = 0;
        (*records.tail).next = core::ptr::null_mut();
        records.num_chunks += 1;

        records.bytes_free = Max(padlen, 512);
        (*records.tail).data = palloc(records.bytes_free as Size) as *mut c_char;
    }

    memcpy(((*records.tail).data).add((*records.tail).len as usize) as *mut c_void,
           data, len as usize);
    (*records.tail).len += padlen;
    records.bytes_free -= padlen;
    records.total_len += padlen;
}

/*
 * Start preparing a state file.
 *
 * Initializes data structure and inserts the 2PC file header record.
 */
pub unsafe fn StartPrepare(gxact: GlobalTransaction) {
    let proc: *mut PGPROC = GetPGProcByNumber((*gxact).pgprocno);
    let xid: TransactionId = (*gxact).xid;
    let mut hdr: TwoPhaseFileHeader = core::mem::zeroed();
    let mut children: *mut TransactionId = core::ptr::null_mut();
    let mut commitrels: *mut RelFileLocator = core::ptr::null_mut();
    let mut abortrels: *mut RelFileLocator = core::ptr::null_mut();
    let mut abortstats: *mut xl_xact_stats_item = core::ptr::null_mut();
    let mut commitstats: *mut xl_xact_stats_item = core::ptr::null_mut();
    let mut invalmsgs: *mut SharedInvalidationMessage = core::ptr::null_mut();

    /* Initialize linked list */
    records.head = palloc0(core::mem::size_of::<StateFileChunk>() as Size) as *mut StateFileChunk;
    (*records.head).len = 0;
    (*records.head).next = core::ptr::null_mut();

    records.bytes_free = Max(core::mem::size_of::<TwoPhaseFileHeader>() as uint32, 512);
    (*records.head).data = palloc(records.bytes_free as Size) as *mut c_char;

    records.tail = records.head;
    records.num_chunks = 1;

    records.total_len = 0;

    /* Create header */
    hdr.magic = TWOPHASE_MAGIC;
    hdr.total_len = 0; /* EndPrepare will fill this in */
    hdr.xid = xid;
    hdr.database = (*proc).databaseId;
    hdr.prepared_at = (*gxact).prepared_at;
    hdr.owner = (*gxact).owner;
    hdr.nsubxacts = xactGetCommittedChildren(&raw mut children);
    hdr.ncommitrels = smgrGetPendingDeletes(true, &raw mut commitrels);
    hdr.nabortrels = smgrGetPendingDeletes(false, &raw mut abortrels);
    hdr.ncommitstats =
        pgstat_get_transactional_drops(true, &raw mut commitstats);
    hdr.nabortstats =
        pgstat_get_transactional_drops(false, &raw mut abortstats);
    hdr.ninvalmsgs = xactGetCommittedInvalidationMessages(&raw mut invalmsgs,
                                                          &raw mut hdr.initfileinval);
    hdr.gidlen = (strlen((*gxact).gid.as_ptr()) + 1) as uint16; /* Include '\0' */
    /* EndPrepare will fill the origin data, if necessary */
    hdr.origin_lsn = InvalidXLogRecPtr;
    hdr.origin_timestamp = 0;

    save_state_data(&raw const hdr as *const c_void, core::mem::size_of::<TwoPhaseFileHeader>() as uint32);
    save_state_data((*gxact).gid.as_ptr() as *const c_void, hdr.gidlen as uint32);

    /*
     * Add the additional info about subxacts, deletable files and cache
     * invalidation messages.
     */
    if hdr.nsubxacts > 0 {
        save_state_data(children as *const c_void,
                        hdr.nsubxacts as uint32 * core::mem::size_of::<TransactionId>() as uint32);
        /* While we have the child-xact data, stuff it in the gxact too */
        GXactLoadSubxactData(gxact, hdr.nsubxacts, children);
    }
    if hdr.ncommitrels > 0 {
        save_state_data(commitrels as *const c_void,
                        hdr.ncommitrels as uint32 * core::mem::size_of::<RelFileLocator>() as uint32);
        pfree(commitrels as *mut c_void);
    }
    if hdr.nabortrels > 0 {
        save_state_data(abortrels as *const c_void,
                        hdr.nabortrels as uint32 * core::mem::size_of::<RelFileLocator>() as uint32);
        pfree(abortrels as *mut c_void);
    }
    if hdr.ncommitstats > 0 {
        save_state_data(commitstats as *const c_void,
                        hdr.ncommitstats as uint32 * core::mem::size_of::<xl_xact_stats_item>() as uint32);
        pfree(commitstats as *mut c_void);
    }
    if hdr.nabortstats > 0 {
        save_state_data(abortstats as *const c_void,
                        hdr.nabortstats as uint32 * core::mem::size_of::<xl_xact_stats_item>() as uint32);
        pfree(abortstats as *mut c_void);
    }
    if hdr.ninvalmsgs > 0 {
        save_state_data(invalmsgs as *const c_void,
                        hdr.ninvalmsgs as uint32 * core::mem::size_of::<SharedInvalidationMessage>() as uint32);
        pfree(invalmsgs as *mut c_void);
    }
}

/*
 * Finish preparing state data and writing it to WAL.
 */
pub unsafe fn EndPrepare(gxact: GlobalTransaction) {
    let hdr: *mut TwoPhaseFileHeader;
    let mut record: *mut StateFileChunk;
    let replorigin: bool;

    /* Add the end sentinel to the list of 2PC records */
    RegisterTwoPhaseRecord(TWOPHASE_RM_END_ID, 0,
                           core::ptr::null(), 0);

    /* Go back and fill in total_len in the file header record */
    hdr = (*records.head).data as *mut TwoPhaseFileHeader;
    Assert!((*hdr).magic == TWOPHASE_MAGIC);
    (*hdr).total_len = records.total_len + core::mem::size_of::<pg_crc32c>() as uint32;

    replorigin = replorigin_session_origin != InvalidRepOriginId &&
                 replorigin_session_origin != DoNotReplicateId;

    if replorigin {
        (*hdr).origin_lsn = replorigin_session_origin_lsn;
        (*hdr).origin_timestamp = replorigin_session_origin_timestamp;
    }

    /*
     * If the data size exceeds MaxAllocSize, we won't be able to read it in
     * ReadTwoPhaseFile. Check for that now, rather than fail in the case
     * where we write data to file and then re-read at commit time.
     */
    if (*hdr).total_len as Size > MaxAllocSize {
        ereport!(ERROR,
                 errmsg!("two-phase state file maximum length exceeded"));
    }

    /*
     * Now writing 2PC state data to WAL. We let the WAL's CRC protection
     * cover us, so no need to calculate a separate CRC.
     *
     * We have to set DELAY_CHKPT_START here, too; otherwise a checkpoint
     * starting immediately after the WAL record is inserted could complete
     * without fsync'ing our state file.  (This is essentially the same kind
     * of race condition as the COMMIT-to-clog-write case that
     * RecordTransactionCommit uses DELAY_CHKPT_START for; see notes there.)
     *
     * We save the PREPARE record's location in the gxact for later use by
     * CheckPointTwoPhase.
     */
    XLogEnsureRecordSpace(0, records.num_chunks as c_int);

    START_CRIT_SECTION();

    Assert!(((*MyProc).delayChkptFlags & DELAY_CHKPT_START) == 0);
    (*MyProc).delayChkptFlags |= DELAY_CHKPT_START;

    XLogBeginInsert();
    record = records.head;
    while !record.is_null() {
        XLogRegisterData((*record).data, (*record).len as usize);
        record = (*record).next;
    }

    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    (*gxact).prepare_end_lsn = XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE);

    if replorigin {
        /* Move LSNs forward for this replication origin */
        replorigin_session_advance(replorigin_session_origin_lsn,
                                   (*gxact).prepare_end_lsn);
    }

    XLogFlush((*gxact).prepare_end_lsn);

    /* If we crash now, we have prepared: WAL replay will fix things */

    /* Store record's start location to read that later on Commit */
    (*gxact).prepare_start_lsn = ProcLastRecPtr;

    /*
     * Mark the prepared transaction as valid.  As soon as xact.c marks MyProc
     * as not running our XID (which it will do immediately after this
     * function returns), others can commit/rollback the xact.
     *
     * NB: a side effect of this is to make a dummy ProcArray entry for the
     * prepared XID.  This must happen before we clear the XID from MyProc /
     * ProcGlobal->xids[], else there is a window where the XID is not running
     * according to TransactionIdIsInProgress, and onlookers would be entitled
     * to assume the xact crashed.  Instead we have a window where the same
     * XID appears twice in ProcArray, which is OK.
     */
    MarkAsPrepared(gxact, false);

    /*
     * Now we can mark ourselves as out of the commit critical section: a
     * checkpoint starting after this will certainly see the gxact as a
     * candidate for fsyncing.
     */
    (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;

    /*
     * Remember that we have this GlobalTransaction entry locked for us.  If
     * we crash after this point, it's too late to abort, but we must unlock
     * it so that the prepared transaction can be committed or rolled back.
     */
    MyLockedGxact = gxact;

    END_CRIT_SECTION();

    /*
     * Wait for synchronous replication, if required.
     *
     * Note that at this stage we have marked the prepare, but still show as
     * running in the procarray (twice!) and continue to hold locks.
     */
    SyncRepWaitForLSN((*gxact).prepare_end_lsn, false);

    records.tail = core::ptr::null_mut();
    records.head = core::ptr::null_mut();
    records.num_chunks = 0;
}

/*
 * Register a 2PC record to be written to state file.
 */
pub unsafe fn RegisterTwoPhaseRecord(
    rmid: TwoPhaseRmgrId,
    info: uint16,
    data: *const c_void,
    len: uint32,
) {
    let mut record: TwoPhaseRecordOnDisk = core::mem::zeroed();

    record.rmid = rmid;
    record.info = info;
    record.len = len;
    save_state_data(&raw const record as *const c_void,
                    core::mem::size_of::<TwoPhaseRecordOnDisk>() as uint32);
    if len > 0 {
        save_state_data(data, len);
    }
}


/*
 * Read and validate the state file for xid.
 *
 * If it looks OK (has a valid magic number and CRC), return the palloc'd
 * contents of the file, issuing an error when finding corrupted data.  If
 * missing_ok is true, which indicates that missing files can be safely
 * ignored, then return NULL.  This state can be reached when doing recovery.
 */
unsafe fn ReadTwoPhaseFile(xid: TransactionId, missing_ok: bool) -> *mut c_char {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let buf: *mut c_char;
    let hdr: *mut TwoPhaseFileHeader;
    let fd: c_int;
    let mut stat: stat_t = core::mem::zeroed();
    let crc_offset: uint32;
    let mut calc_crc: pg_crc32c;
    let file_crc: pg_crc32c;
    let r: c_int;

    TwoPhaseFilePath(path.as_mut_ptr(), xid);

    fd = OpenTransientFile(path.as_ptr(), O_RDONLY | PG_BINARY);
    if fd < 0 {
        if missing_ok && errno() == ENOENT {
            return core::ptr::null_mut();
        }

        ereport!(ERROR,
                 errmsg!("could not open file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }

    /*
     * Check file length.  We can determine a lower bound pretty easily. We
     * set an upper bound to avoid palloc() failure on a corrupt file, though
     * we can't guarantee that we won't get an out of memory error anyway,
     * even on a valid file.
     */
    if fstat(fd, &raw mut stat) != 0 {
        ereport!(ERROR,
                 errmsg!("could not stat file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }

    if (stat.st_size as Size) < (MAXALIGN(core::mem::size_of::<TwoPhaseFileHeader>() as Size) +
                                 MAXALIGN(core::mem::size_of::<TwoPhaseRecordOnDisk>() as Size) +
                                 core::mem::size_of::<pg_crc32c>() as Size) ||
        stat.st_size as Size > MaxAllocSize {
        ereport!(ERROR,
                 errmsg!("incorrect size of file \"{}\": {} bytes",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(),
                         stat.st_size as i64));
    }

    crc_offset = stat.st_size as uint32 - core::mem::size_of::<pg_crc32c>() as uint32;
    if crc_offset != MAXALIGN(crc_offset as Size) as uint32 {
        ereport!(ERROR,
                 errmsg!("incorrect alignment of CRC offset for file \"{}\"",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy()));
    }

    /*
     * OK, slurp in the file.
     */
    buf = palloc(stat.st_size as Size) as *mut c_char;

    pgstat_report_wait_start(WAIT_EVENT_TWOPHASE_FILE_READ);
    r = read(fd, buf as *mut c_void, stat.st_size as usize) as c_int;
    if r as i64 != stat.st_size as i64 {
        if r < 0 {
            ereport!(ERROR,
                     errmsg!("could not read file \"{}\": {}",
                             CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
        } else {
            ereport!(ERROR,
                     errmsg!("could not read file \"{}\": read {} of {}",
                             CStr::from_ptr(path.as_ptr()).to_string_lossy(),
                             r, stat.st_size as i64));
        }
    }

    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR,
                 errmsg!("could not close file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }

    hdr = buf as *mut TwoPhaseFileHeader;
    if (*hdr).magic != TWOPHASE_MAGIC {
        ereport!(ERROR,
                 errmsg!("invalid magic number stored in file \"{}\"",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy()));
    }

    if (*hdr).total_len != stat.st_size as uint32 {
        ereport!(ERROR,
                 errmsg!("invalid size stored in file \"{}\"",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy()));
    }

    calc_crc = INIT_CRC32C();
    calc_crc = COMP_CRC32C(calc_crc, buf as *const c_void, crc_offset as Size);
    calc_crc = FIN_CRC32C(calc_crc);

    file_crc = *((buf.add(crc_offset as usize)) as *const pg_crc32c);

    if !EQ_CRC32C(calc_crc, file_crc) {
        ereport!(ERROR,
                 errmsg!("calculated CRC checksum does not match value stored in file \"{}\"",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy()));
    }

    buf
}


/*
 * Reads 2PC data from xlog. During checkpoint this data will be moved to
 * twophase files and ReadTwoPhaseFile should be used instead.
 *
 * Note clearly that this function can access WAL during normal operation,
 * similarly to the way WALSender or Logical Decoding would do.
 */
unsafe fn XlogReadTwoPhaseData(lsn: XLogRecPtr, buf: *mut *mut c_char, len: *mut c_int) {
    let record: *mut XLogRecord;
    let xlogreader: *mut XLogReaderState;
    let mut errormsg: *mut c_char = core::ptr::null_mut();

    xlogreader = XLogReaderAllocate(wal_segment_size, core::ptr::null(),
                                    XL_ROUTINE_two_phase(),
                                    core::ptr::null_mut());
    if xlogreader.is_null() {
        ereport!(ERROR,
                 errmsg!("out of memory"));
    }

    XLogBeginRead(xlogreader, lsn);
    record = XLogReadRecord(xlogreader, &raw mut errormsg);

    if record.is_null() {
        if !errormsg.is_null() {
            ereport!(ERROR,
                     errmsg!("could not read two-phase state from WAL at {}: {}",
                             LSN_FORMAT_ARGS(lsn), CStr::from_ptr(errormsg).to_string_lossy()));
        } else {
            ereport!(ERROR,
                     errmsg!("could not read two-phase state from WAL at {}",
                             LSN_FORMAT_ARGS(lsn)));
        }
    }

    if XLogRecGetRmid(xlogreader) != RM_XACT_ID ||
        (XLogRecGetInfo(xlogreader) & XLOG_XACT_OPMASK) != XLOG_XACT_PREPARE {
        ereport!(ERROR,
                 errmsg!("expected two-phase state data is not present in WAL at {}",
                         LSN_FORMAT_ARGS(lsn)));
    }

    if !len.is_null() {
        *len = XLogRecGetDataLen(xlogreader) as int32;
    }

    *buf = palloc(core::mem::size_of::<c_char>() as Size * XLogRecGetDataLen(xlogreader) as Size)
        as *mut c_char;
    memcpy(*buf as *mut c_void, XLogRecGetData(xlogreader) as *const c_void,
           core::mem::size_of::<c_char>() * XLogRecGetDataLen(xlogreader) as usize);

    XLogReaderFree(xlogreader);
}


/*
 * Confirms an xid is prepared, during recovery
 */
pub unsafe fn StandbyTransactionIdIsPrepared(xid: TransactionId) -> bool {
    let buf: *mut c_char;
    let hdr: *mut TwoPhaseFileHeader;
    let result: bool;

    Assert!(TransactionIdIsValid(xid));

    if max_prepared_xacts <= 0 {
        return false; /* nothing to do */
    }

    /* Read and validate file */
    buf = ReadTwoPhaseFile(xid, true);
    if buf.is_null() {
        return false;
    }

    /* Check header also */
    hdr = buf as *mut TwoPhaseFileHeader;
    result = TransactionIdEquals((*hdr).xid, xid);
    pfree(buf as *mut c_void);

    result
}

/*
 * FinishPreparedTransaction: execute COMMIT PREPARED or ROLLBACK PREPARED
 */
#[no_mangle]
pub unsafe fn FinishPreparedTransaction(gid: *const c_char, isCommit: bool) {
    let gxact: GlobalTransaction;
    let proc: *mut PGPROC;
    let xid: TransactionId;
    let ondisk: bool;
    let mut buf: *mut c_char = core::ptr::null_mut();
    let mut bufptr: *mut c_char;
    let hdr: *mut TwoPhaseFileHeader;
    let latestXid: TransactionId;
    let children: *mut TransactionId;
    let commitrels: *mut RelFileLocator;
    let abortrels: *mut RelFileLocator;
    let delrels: *mut RelFileLocator;
    let ndelrels: c_int;
    let commitstats: *mut xl_xact_stats_item;
    let abortstats: *mut xl_xact_stats_item;
    let invalmsgs: *mut SharedInvalidationMessage;

    /*
     * Validate the GID, and lock the GXACT to ensure that two backends do not
     * try to commit the same GID at once.
     */
    gxact = LockGXact(gid, GetUserId());
    proc = GetPGProcByNumber((*gxact).pgprocno);
    xid = (*gxact).xid;

    /*
     * Read and validate 2PC state data. State data will typically be stored
     * in WAL files if the LSN is after the last checkpoint record, or moved
     * to disk if for some reason they have lived for a long time.
     */
    if (*gxact).ondisk {
        buf = ReadTwoPhaseFile(xid, false);
    } else {
        XlogReadTwoPhaseData((*gxact).prepare_start_lsn, &raw mut buf, core::ptr::null_mut());
    }


    /*
     * Disassemble the header area
     */
    hdr = buf as *mut TwoPhaseFileHeader;
    Assert!(TransactionIdEquals((*hdr).xid, xid));
    bufptr = buf.add(MAXALIGN(core::mem::size_of::<TwoPhaseFileHeader>() as Size) as usize);
    bufptr = bufptr.add(MAXALIGN((*hdr).gidlen as Size) as usize);
    children = bufptr as *mut TransactionId;
    bufptr = bufptr.add(MAXALIGN((*hdr).nsubxacts as Size * core::mem::size_of::<TransactionId>() as Size) as usize);
    commitrels = bufptr as *mut RelFileLocator;
    bufptr = bufptr.add(MAXALIGN((*hdr).ncommitrels as Size * core::mem::size_of::<RelFileLocator>() as Size) as usize);
    abortrels = bufptr as *mut RelFileLocator;
    bufptr = bufptr.add(MAXALIGN((*hdr).nabortrels as Size * core::mem::size_of::<RelFileLocator>() as Size) as usize);
    commitstats = bufptr as *mut xl_xact_stats_item;
    bufptr = bufptr.add(MAXALIGN((*hdr).ncommitstats as Size * core::mem::size_of::<xl_xact_stats_item>() as Size) as usize);
    abortstats = bufptr as *mut xl_xact_stats_item;
    bufptr = bufptr.add(MAXALIGN((*hdr).nabortstats as Size * core::mem::size_of::<xl_xact_stats_item>() as Size) as usize);
    invalmsgs = bufptr as *mut SharedInvalidationMessage;
    bufptr = bufptr.add(MAXALIGN((*hdr).ninvalmsgs as Size * core::mem::size_of::<SharedInvalidationMessage>() as Size) as usize);

    /* compute latestXid among all children */
    latestXid = TransactionIdLatest(xid, (*hdr).nsubxacts, children);

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

    /*
     * The order of operations here is critical: make the XLOG entry for
     * commit or abort, then mark the transaction committed or aborted in
     * pg_xact, then remove its PGPROC from the global ProcArray (which means
     * TransactionIdIsInProgress will stop saying the prepared xact is in
     * progress), then run the post-commit or post-abort callbacks. The
     * callbacks will release the locks the transaction held.
     */
    if isCommit {
        RecordTransactionCommitPrepared(xid,
                                        (*hdr).nsubxacts, children,
                                        (*hdr).ncommitrels, commitrels,
                                        (*hdr).ncommitstats,
                                        commitstats,
                                        (*hdr).ninvalmsgs, invalmsgs,
                                        (*hdr).initfileinval, gid);
    } else {
        RecordTransactionAbortPrepared(xid,
                                       (*hdr).nsubxacts, children,
                                       (*hdr).nabortrels, abortrels,
                                       (*hdr).nabortstats,
                                       abortstats,
                                       gid);
    }

    ProcArrayRemove(proc, latestXid);

    /*
     * In case we fail while running the callbacks, mark the gxact invalid so
     * no one else will try to commit/rollback, and so it will be recycled if
     * we fail after this point.  It is still locked by our backend so it
     * won't go away yet.
     *
     * (We assume it's safe to do this without taking TwoPhaseStateLock.)
     */
    (*gxact).valid = false;

    /*
     * We have to remove any files that were supposed to be dropped. For
     * consistency with the regular xact.c code paths, must do this before
     * releasing locks, so do it before running the callbacks.
     *
     * NB: this code knows that we couldn't be dropping any temp rels ...
     */
    if isCommit {
        delrels = commitrels;
        ndelrels = (*hdr).ncommitrels;
    } else {
        delrels = abortrels;
        ndelrels = (*hdr).nabortrels;
    }

    /* Make sure files supposed to be dropped are dropped */
    DropRelationFiles(delrels, ndelrels, false);

    if isCommit {
        pgstat_execute_transactional_drops((*hdr).ncommitstats, commitstats, false);
    } else {
        pgstat_execute_transactional_drops((*hdr).nabortstats, abortstats, false);
    }

    /*
     * Handle cache invalidation messages.
     *
     * Relcache init file invalidation requires processing both before and
     * after we send the SI messages, only when committing.  See
     * AtEOXact_Inval().
     */
    if isCommit {
        if (*hdr).initfileinval {
            RelationCacheInitFilePreInvalidate();
        }
        SendSharedInvalidMessages(invalmsgs, (*hdr).ninvalmsgs);
        if (*hdr).initfileinval {
            RelationCacheInitFilePostInvalidate();
        }
    }

    /*
     * Acquire the two-phase lock.  We want to work on the two-phase callbacks
     * while holding it to avoid potential conflicts with other transactions
     * attempting to use the same GID, so the lock is released once the shared
     * memory state is cleared.
     */
    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);

    /* And now do the callbacks */
    if isCommit {
        ProcessRecords(bufptr, xid, twophase_postcommit_callbacks.as_ptr());
    } else {
        ProcessRecords(bufptr, xid, twophase_postabort_callbacks.as_ptr());
    }

    PredicateLockTwoPhaseFinish(xid, isCommit);

    /*
     * Read this value while holding the two-phase lock, as the on-disk 2PC
     * file is physically removed after the lock is released.
     */
    ondisk = (*gxact).ondisk;

    /* Clear shared memory state */
    RemoveGXact(gxact);

    /*
     * Release the lock as all callbacks are called and shared memory cleanup
     * is done.
     */
    LWLockRelease(TwoPhaseStateLock());

    /* Count the prepared xact as committed or aborted */
    AtEOXact_PgStat(isCommit, false);

    /*
     * And now we can clean up any files we may have left.
     */
    if ondisk {
        RemoveTwoPhaseFile(xid, true);
    }

    MyLockedGxact = core::ptr::null_mut();

    RESUME_INTERRUPTS();

    pfree(buf as *mut c_void);
}

/*
 * Scan 2PC state data in memory and call the indicated callbacks for each 2PC record.
 */
unsafe fn ProcessRecords(
    mut bufptr: *mut c_char,
    xid: TransactionId,
    callbacks: *const Option<TwoPhaseCallback>,
) {
    loop {
        let record: *mut TwoPhaseRecordOnDisk = bufptr as *mut TwoPhaseRecordOnDisk;

        Assert!((*record).rmid <= TWOPHASE_RM_MAX_ID);
        if (*record).rmid == TWOPHASE_RM_END_ID {
            break;
        }

        bufptr = bufptr.add(MAXALIGN(core::mem::size_of::<TwoPhaseRecordOnDisk>() as Size) as usize);

        if let Some(cb) = *callbacks.add((*record).rmid as usize) {
            cb(xid, (*record).info, bufptr as *mut c_void, (*record).len);
        }

        bufptr = bufptr.add(MAXALIGN((*record).len as Size) as usize);
    }
}

/*
 * Remove the 2PC file for the specified XID.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 */
unsafe fn RemoveTwoPhaseFile(xid: TransactionId, giveWarning: bool) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    TwoPhaseFilePath(path.as_mut_ptr(), xid);
    if unlink(path.as_ptr()) != 0 {
        if errno() != ENOENT || giveWarning {
            ereport!(WARNING,
                     errmsg!("could not remove file \"{}\": {}",
                             CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
        }
    }
}

/*
 * Recreates a state file. This is used in WAL replay and during
 * checkpoint creation.
 *
 * Note: content and len don't include CRC.
 */
unsafe fn RecreateTwoPhaseFile(xid: TransactionId, content: *mut c_void, len: c_int) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut statefile_crc: pg_crc32c;
    let fd: c_int;

    /* Recompute CRC */
    statefile_crc = INIT_CRC32C();
    statefile_crc = COMP_CRC32C(statefile_crc, content, len as Size);
    statefile_crc = FIN_CRC32C(statefile_crc);

    TwoPhaseFilePath(path.as_mut_ptr(), xid);

    fd = OpenTransientFile(path.as_ptr(),
                           O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
    if fd < 0 {
        ereport!(ERROR,
                 errmsg!("could not recreate file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }

    /* Write content and CRC */
    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_TWOPHASE_FILE_WRITE);
    if write(fd, content as *const c_void, len as usize) as c_int != len {
        /* if write didn't set errno, assume problem is no disk space */
        if errno() == 0 {
            set_errno(ENOSPC);
        }
        ereport!(ERROR,
                 errmsg!("could not write file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }
    if write(fd, &raw const statefile_crc as *const c_void, core::mem::size_of::<pg_crc32c>())
        as usize != core::mem::size_of::<pg_crc32c>() {
        /* if write didn't set errno, assume problem is no disk space */
        if errno() == 0 {
            set_errno(ENOSPC);
        }
        ereport!(ERROR,
                 errmsg!("could not write file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }
    pgstat_report_wait_end();

    /*
     * We must fsync the file because the end-of-replay checkpoint will not do
     * so, there being no GXACT in shared memory yet to tell it to.
     */
    pgstat_report_wait_start(WAIT_EVENT_TWOPHASE_FILE_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(ERROR,
                 errmsg!("could not fsync file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR,
                 errmsg!("could not close file \"{}\": {}",
                         CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
    }
}

/*
 * CheckPointTwoPhase -- handle 2PC component of checkpointing.
 *
 * We must fsync the state file of any GXACT that is valid or has been
 * generated during redo and has a PREPARE LSN <= the checkpoint's redo
 * horizon.  (If the gxact isn't valid yet, has not been generated in
 * redo, or has a later LSN, this checkpoint is not responsible for
 * fsyncing it.)
 *
 * This is deliberately run as late as possible in the checkpoint sequence,
 * because GXACTs ordinarily have short lifespans, and so it is quite
 * possible that GXACTs that were valid at checkpoint start will no longer
 * exist if we wait a little bit. With typical checkpoint settings this
 * will be about 3 minutes for an online checkpoint, so as a result we
 * expect that there will be no GXACTs that need to be copied to disk.
 *
 * If a GXACT remains valid across multiple checkpoints, it will already
 * be on disk so we don't bother to repeat that write.
 */
pub unsafe fn CheckPointTwoPhase(redo_horizon: XLogRecPtr) {
    let mut i: c_int;
    let mut serialized_xacts: c_int = 0;

    if max_prepared_xacts <= 0 {
        return; /* nothing to do */
    }

    TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_START();

    /*
     * We are expecting there to be zero GXACTs that need to be copied to
     * disk, so we perform all I/O while holding TwoPhaseStateLock for
     * simplicity. This prevents any new xacts from preparing while this
     * occurs, which shouldn't be a problem since the presence of long-lived
     * prepared xacts indicates the transaction manager isn't active.
     *
     * It's also possible to move I/O out of the lock, but on every error we
     * should check whether somebody committed our transaction in different
     * backend. Let's leave this optimization for future, if somebody will
     * spot that this place cause bottleneck.
     *
     * Note that it isn't possible for there to be a GXACT with a
     * prepare_end_lsn set prior to the last checkpoint yet is marked invalid,
     * because of the efforts with delayChkptFlags.
     */
    LWLockAcquire(TwoPhaseStateLock(), LW_SHARED);
    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        /*
         * Note that we are using gxact not PGPROC so this works in recovery
         * also
         */
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        if ((*gxact).valid || (*gxact).inredo) &&
            !(*gxact).ondisk &&
            (*gxact).prepare_end_lsn <= redo_horizon {
            let mut buf: *mut c_char = core::ptr::null_mut();
            let mut len: c_int = 0;

            XlogReadTwoPhaseData((*gxact).prepare_start_lsn, &raw mut buf, &raw mut len);
            RecreateTwoPhaseFile((*gxact).xid, buf as *mut c_void, len);
            (*gxact).ondisk = true;
            (*gxact).prepare_start_lsn = InvalidXLogRecPtr;
            (*gxact).prepare_end_lsn = InvalidXLogRecPtr;
            pfree(buf as *mut c_void);
            serialized_xacts += 1;
        }
        i += 1;
    }
    LWLockRelease(TwoPhaseStateLock());

    /*
     * Flush unconditionally the parent directory to make any information
     * durable on disk.  Two-phase files could have been removed and those
     * removals need to be made persistent as well as any files newly created
     * previously since the last checkpoint.
     */
    fsync_fname(TWOPHASE_DIR.as_ptr(), true);

    TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_DONE();

    if log_checkpoints && serialized_xacts > 0 {
        ereport!(LOG,
                 errmsg!("{} two-phase state files were written for long-running prepared transactions",
                         serialized_xacts));
    }
}

/*
 * restoreTwoPhaseData
 *
 * Scan pg_twophase and fill TwoPhaseState depending on the on-disk data.
 * This is called once at the beginning of recovery, saving any extra
 * lookups in the future.  Two-phase files that are newer than the
 * minimum XID horizon are discarded on the way.
 */
pub unsafe fn restoreTwoPhaseData() {
    let cldir: *mut DIR;
    let mut clde: *mut dirent;

    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    cldir = AllocateDir(TWOPHASE_DIR.as_ptr());
    loop {
        clde = ReadDir(cldir, TWOPHASE_DIR.as_ptr());
        if clde.is_null() {
            break;
        }
        if strlen((*clde).d_name.as_ptr()) == 16 &&
            strspn((*clde).d_name.as_ptr(), c"0123456789ABCDEF".as_ptr()) == 16 {
            let xid: TransactionId;
            let fxid: FullTransactionId;
            let buf: *mut c_char;

            fxid = FullTransactionIdFromU64(strtou64((*clde).d_name.as_ptr(), core::ptr::null_mut(), 16));
            xid = XidFromFullTransactionId(fxid);

            buf = ProcessTwoPhaseBuffer(xid, InvalidXLogRecPtr,
                                        true, false, false);
            if buf.is_null() {
                continue;
            }

            PrepareRedoAdd(buf, InvalidXLogRecPtr,
                           InvalidXLogRecPtr, InvalidRepOriginId);
        }
    }
    LWLockRelease(TwoPhaseStateLock());
    FreeDir(cldir);
}

/*
 * PrescanPreparedTransactions
 *
 * Scan the shared memory entries of TwoPhaseState and determine the range
 * of valid XIDs present.  This is run during database startup, after we
 * have completed reading WAL.  TransamVariables->nextXid has been set to
 * one more than the highest XID for which evidence exists in WAL.
 *
 * We throw away any prepared xacts with main XID beyond nextXid --- if any
 * are present, it suggests that the DBA has done a PITR recovery to an
 * earlier point in time without cleaning out pg_twophase.  We dare not
 * try to recover such prepared xacts since they likely depend on database
 * state that doesn't exist now.
 *
 * However, we will advance nextXid beyond any subxact XIDs belonging to
 * valid prepared xacts.  We need to do this since subxact commit doesn't
 * write a WAL entry, and so there might be no evidence in WAL of those
 * subxact XIDs.
 *
 * On corrupted two-phase files, fail immediately.  Keeping around broken
 * entries and let replay continue causes harm on the system, and a new
 * backup should be rolled in.
 *
 * Our other responsibility is to determine and return the oldest valid XID
 * among the prepared xacts (if none, return TransamVariables->nextXid).
 * This is needed to synchronize pg_subtrans startup properly.
 *
 * If xids_p and nxids_p are not NULL, pointer to a palloc'd array of all
 * top-level xids is stored in *xids_p. The number of entries in the array
 * is returned in *nxids_p.
 */
pub unsafe fn PrescanPreparedTransactions(
    xids_p: *mut *mut TransactionId,
    nxids_p: *mut c_int,
) -> TransactionId {
    let nextXid: FullTransactionId = (*TransamVariables).nextXid;
    let origNextXid: TransactionId = XidFromFullTransactionId(nextXid);
    let mut result: TransactionId = origNextXid;
    let mut xids: *mut TransactionId = core::ptr::null_mut();
    let mut nxids: c_int = 0;
    let mut allocsize: c_int = 0;
    let mut i: c_int;

    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let xid: TransactionId;
        let buf: *mut c_char;
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        Assert!((*gxact).inredo);

        xid = (*gxact).xid;

        buf = ProcessTwoPhaseBuffer(xid,
                                    (*gxact).prepare_start_lsn,
                                    (*gxact).ondisk, false, true);

        if buf.is_null() {
            i += 1;
            continue;
        }

        /*
         * OK, we think this file is valid.  Incorporate xid into the
         * running-minimum result.
         */
        if TransactionIdPrecedes(xid, result) {
            result = xid;
        }

        if !xids_p.is_null() {
            if nxids == allocsize {
                if nxids == 0 {
                    allocsize = 10;
                    xids = palloc(allocsize as Size * core::mem::size_of::<TransactionId>() as Size)
                        as *mut TransactionId;
                } else {
                    allocsize = allocsize * 2;
                    xids = repalloc(xids as *mut c_void,
                                    allocsize as Size * core::mem::size_of::<TransactionId>() as Size)
                        as *mut TransactionId;
                }
            }
            *xids.add(nxids as usize) = xid;
            nxids += 1;
        }

        pfree(buf as *mut c_void);
        i += 1;
    }
    LWLockRelease(TwoPhaseStateLock());

    if !xids_p.is_null() {
        *xids_p = xids;
        *nxids_p = nxids;
    }

    result
}

/*
 * StandbyRecoverPreparedTransactions
 *
 * Scan the shared memory entries of TwoPhaseState and setup all the required
 * information to allow standby queries to treat prepared transactions as still
 * active.
 *
 * This is never called at the end of recovery - we use
 * RecoverPreparedTransactions() at that point.
 *
 * This updates pg_subtrans, so that any subtransactions will be correctly
 * seen as in-progress in snapshots taken during recovery.
 */
pub unsafe fn StandbyRecoverPreparedTransactions() {
    let mut i: c_int;

    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let xid: TransactionId;
        let buf: *mut c_char;
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        Assert!((*gxact).inredo);

        xid = (*gxact).xid;

        buf = ProcessTwoPhaseBuffer(xid,
                                    (*gxact).prepare_start_lsn,
                                    (*gxact).ondisk, true, false);
        if !buf.is_null() {
            pfree(buf as *mut c_void);
        }
        i += 1;
    }
    LWLockRelease(TwoPhaseStateLock());
}

/*
 * RecoverPreparedTransactions
 *
 * Scan the shared memory entries of TwoPhaseState and reload the state for
 * each prepared transaction (reacquire locks, etc).
 *
 * This is run at the end of recovery, but before we allow backends to write
 * WAL.
 *
 * At the end of recovery the way we take snapshots will change. We now need
 * to mark all running transactions with their full SubTransSetParent() info
 * to allow normal snapshots to work correctly if snapshots overflow.
 * We do this here because by definition prepared transactions are the only
 * type of write transaction still running, so this is necessary and
 * complete.
 */
pub unsafe fn RecoverPreparedTransactions() {
    let mut i: c_int;

    LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let xid: TransactionId;
        let buf: *mut c_char;
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);
        let mut bufptr: *mut c_char;
        let hdr: *mut TwoPhaseFileHeader;
        let subxids: *mut TransactionId;
        let gid: *const c_char;

        xid = (*gxact).xid;

        /*
         * Reconstruct subtrans state for the transaction --- needed because
         * pg_subtrans is not preserved over a restart.  Note that we are
         * linking all the subtransactions directly to the top-level XID;
         * there may originally have been a more complex hierarchy, but
         * there's no need to restore that exactly. It's possible that
         * SubTransSetParent has been set before, if the prepared transaction
         * generated xid assignment records.
         */
        buf = ProcessTwoPhaseBuffer(xid,
                                    (*gxact).prepare_start_lsn,
                                    (*gxact).ondisk, true, false);
        if buf.is_null() {
            i += 1;
            continue;
        }

        ereport!(LOG,
                 errmsg!("recovering prepared transaction {} from shared memory", xid));

        hdr = buf as *mut TwoPhaseFileHeader;
        Assert!(TransactionIdEquals((*hdr).xid, xid));
        bufptr = buf.add(MAXALIGN(core::mem::size_of::<TwoPhaseFileHeader>() as Size) as usize);
        gid = bufptr as *const c_char;
        bufptr = bufptr.add(MAXALIGN((*hdr).gidlen as Size) as usize);
        subxids = bufptr as *mut TransactionId;
        bufptr = bufptr.add(MAXALIGN((*hdr).nsubxacts as Size * core::mem::size_of::<TransactionId>() as Size) as usize);
        bufptr = bufptr.add(MAXALIGN((*hdr).ncommitrels as Size * core::mem::size_of::<RelFileLocator>() as Size) as usize);
        bufptr = bufptr.add(MAXALIGN((*hdr).nabortrels as Size * core::mem::size_of::<RelFileLocator>() as Size) as usize);
        bufptr = bufptr.add(MAXALIGN((*hdr).ncommitstats as Size * core::mem::size_of::<xl_xact_stats_item>() as Size) as usize);
        bufptr = bufptr.add(MAXALIGN((*hdr).nabortstats as Size * core::mem::size_of::<xl_xact_stats_item>() as Size) as usize);
        bufptr = bufptr.add(MAXALIGN((*hdr).ninvalmsgs as Size * core::mem::size_of::<SharedInvalidationMessage>() as Size) as usize);

        /*
         * Recreate its GXACT and dummy PGPROC. But, check whether it was
         * added in redo and already has a shmem entry for it.
         */
        MarkAsPreparingGuts(gxact, xid, gid,
                            (*hdr).prepared_at,
                            (*hdr).owner, (*hdr).database);

        /* recovered, so reset the flag for entries generated by redo */
        (*gxact).inredo = false;

        GXactLoadSubxactData(gxact, (*hdr).nsubxacts, subxids);
        MarkAsPrepared(gxact, true);

        LWLockRelease(TwoPhaseStateLock());

        /*
         * Recover other state (notably locks) using resource managers.
         */
        ProcessRecords(bufptr, xid, twophase_recover_callbacks.as_ptr());

        /*
         * Release locks held by the standby process after we process each
         * prepared transaction. As a result, we don't need too many
         * additional locks at any one time.
         */
        if InHotStandby {
            StandbyReleaseLockTree(xid, (*hdr).nsubxacts, subxids);
        }

        /*
         * We're done with recovering this transaction. Clear MyLockedGxact,
         * like we do in PrepareTransaction() during normal operation.
         */
        PostPrepare_Twophase();

        pfree(buf as *mut c_void);

        LWLockAcquire(TwoPhaseStateLock(), LW_EXCLUSIVE);
        i += 1;
    }

    LWLockRelease(TwoPhaseStateLock());
}

/*
 * ProcessTwoPhaseBuffer
 *
 * Given a transaction id, read it either from disk or read it directly
 * via shmem xlog record pointer using the provided "prepare_start_lsn".
 *
 * If setParent is true, set up subtransaction parent linkages.
 *
 * If setNextXid is true, set TransamVariables->nextXid to the newest
 * value scanned.
 */
unsafe fn ProcessTwoPhaseBuffer(
    xid: TransactionId,
    prepare_start_lsn: XLogRecPtr,
    fromdisk: bool,
    setParent: bool,
    setNextXid: bool,
) -> *mut c_char {
    let nextXid: FullTransactionId = (*TransamVariables).nextXid;
    let origNextXid: TransactionId = XidFromFullTransactionId(nextXid);
    let subxids: *mut TransactionId;
    let mut buf: *mut c_char = core::ptr::null_mut();
    let hdr: *mut TwoPhaseFileHeader;
    let mut i: c_int;

    Assert!(LWLockHeldByMeInMode(TwoPhaseStateLock(), LW_EXCLUSIVE));

    if !fromdisk {
        Assert!(prepare_start_lsn != InvalidXLogRecPtr);
    }

    /* Already processed? */
    if TransactionIdDidCommit(xid) || TransactionIdDidAbort(xid) {
        if fromdisk {
            ereport!(WARNING,
                     errmsg!("removing stale two-phase state file for transaction {}",
                             xid));
            RemoveTwoPhaseFile(xid, true);
        } else {
            ereport!(WARNING,
                     errmsg!("removing stale two-phase state from memory for transaction {}",
                             xid));
            PrepareRedoRemove(xid, true);
        }
        return core::ptr::null_mut();
    }

    /* Reject XID if too new */
    if TransactionIdFollowsOrEquals(xid, origNextXid) {
        if fromdisk {
            ereport!(WARNING,
                     errmsg!("removing future two-phase state file for transaction {}",
                             xid));
            RemoveTwoPhaseFile(xid, true);
        } else {
            ereport!(WARNING,
                     errmsg!("removing future two-phase state from memory for transaction {}",
                             xid));
            PrepareRedoRemove(xid, true);
        }
        return core::ptr::null_mut();
    }

    if fromdisk {
        /* Read and validate file */
        buf = ReadTwoPhaseFile(xid, false);
    } else {
        /* Read xlog data */
        XlogReadTwoPhaseData(prepare_start_lsn, &raw mut buf, core::ptr::null_mut());
    }

    /* Deconstruct header */
    hdr = buf as *mut TwoPhaseFileHeader;
    if !TransactionIdEquals((*hdr).xid, xid) {
        if fromdisk {
            ereport!(ERROR,
                     errmsg!("corrupted two-phase state file for transaction {}",
                             xid));
        } else {
            ereport!(ERROR,
                     errmsg!("corrupted two-phase state in memory for transaction {}",
                             xid));
        }
    }

    /*
     * Examine subtransaction XIDs ... they should all follow main XID, and
     * they may force us to advance nextXid.
     */
    subxids = (buf.add(MAXALIGN(core::mem::size_of::<TwoPhaseFileHeader>() as Size) as usize)
        .add(MAXALIGN((*hdr).gidlen as Size) as usize)) as *mut TransactionId;
    i = 0;
    while i < (*hdr).nsubxacts {
        let subxid: TransactionId = *subxids.add(i as usize);

        Assert!(TransactionIdFollows(subxid, xid));

        /* update nextXid if needed */
        if setNextXid {
            AdvanceNextFullTransactionIdPastXid(subxid);
        }

        if setParent {
            SubTransSetParent(subxid, xid);
        }
        i += 1;
    }

    buf
}


/*
 *	RecordTransactionCommitPrepared
 *
 * This is basically the same as RecordTransactionCommit (q.v. if you change
 * this function): in particular, we must set DELAY_CHKPT_START to avoid a
 * race condition.
 *
 * We know the transaction made at least one XLOG entry (its PREPARE),
 * so it is never possible to optimize out the commit record.
 */
unsafe fn RecordTransactionCommitPrepared(
    xid: TransactionId,
    nchildren: c_int,
    children: *mut TransactionId,
    nrels: c_int,
    rels: *mut RelFileLocator,
    nstats: c_int,
    stats: *mut xl_xact_stats_item,
    ninvalmsgs: c_int,
    invalmsgs: *mut SharedInvalidationMessage,
    initfileinval: bool,
    gid: *const c_char,
) {
    let recptr: XLogRecPtr;
    let committs: TimestampTz = GetCurrentTimestamp();
    let replorigin: bool;

    /*
     * Are we using the replication origins feature?  Or, in other words, are
     * we replaying remote actions?
     */
    replorigin = replorigin_session_origin != InvalidRepOriginId &&
                 replorigin_session_origin != DoNotReplicateId;

    START_CRIT_SECTION();

    /* See notes in RecordTransactionCommit */
    Assert!(((*MyProc).delayChkptFlags & DELAY_CHKPT_START) == 0);
    (*MyProc).delayChkptFlags |= DELAY_CHKPT_START;

    /*
     * Emit the XLOG commit record. Note that we mark 2PC commits as
     * potentially having AccessExclusiveLocks since we don't know whether or
     * not they do.
     */
    recptr = XactLogCommitRecord(committs,
                                 nchildren, children, nrels, rels,
                                 nstats, stats,
                                 ninvalmsgs, invalmsgs,
                                 initfileinval,
                                 MyXactFlags | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
                                 xid, gid);


    if replorigin {
        /* Move LSNs forward for this replication origin */
        replorigin_session_advance(replorigin_session_origin_lsn,
                                   XactLastRecEnd);
    }

    /*
     * Record commit timestamp.  The value comes from plain commit timestamp
     * if replorigin is not enabled, or replorigin already set a value for us
     * in replorigin_session_origin_timestamp otherwise.
     *
     * We don't need to WAL-log anything here, as the commit record written
     * above already contains the data.
     */
    if !replorigin || replorigin_session_origin_timestamp == 0 {
        replorigin_session_origin_timestamp = committs;
    }

    TransactionTreeSetCommitTsData(xid, nchildren, children,
                                   replorigin_session_origin_timestamp,
                                   replorigin_session_origin);

    /*
     * We don't currently try to sleep before flush here ... nor is there any
     * support for async commit of a prepared xact (the very idea is probably
     * a contradiction)
     */

    /* Flush XLOG to disk */
    XLogFlush(recptr);

    /* Mark the transaction committed in pg_xact */
    TransactionIdCommitTree(xid, nchildren, children);

    /* Checkpoint can proceed now */
    (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;

    END_CRIT_SECTION();

    /*
     * Wait for synchronous replication, if required.
     *
     * Note that at this stage we have marked clog, but still show as running
     * in the procarray and continue to hold locks.
     */
    SyncRepWaitForLSN(recptr, true);
}

/*
 *	RecordTransactionAbortPrepared
 *
 * This is basically the same as RecordTransactionAbort.
 *
 * We know the transaction made at least one XLOG entry (its PREPARE),
 * so it is never possible to optimize out the abort record.
 */
unsafe fn RecordTransactionAbortPrepared(
    xid: TransactionId,
    nchildren: c_int,
    children: *mut TransactionId,
    nrels: c_int,
    rels: *mut RelFileLocator,
    nstats: c_int,
    stats: *mut xl_xact_stats_item,
    gid: *const c_char,
) {
    let recptr: XLogRecPtr;
    let replorigin: bool;

    /*
     * Are we using the replication origins feature?  Or, in other words, are
     * we replaying remote actions?
     */
    replorigin = replorigin_session_origin != InvalidRepOriginId &&
                 replorigin_session_origin != DoNotReplicateId;

    /*
     * Catch the scenario where we aborted partway through
     * RecordTransactionCommitPrepared ...
     */
    if TransactionIdDidCommit(xid) {
        elog!(PANIC, "cannot abort transaction {}, it was already committed",
              xid);
    }

    START_CRIT_SECTION();

    /*
     * Emit the XLOG commit record. Note that we mark 2PC aborts as
     * potentially having AccessExclusiveLocks since we don't know whether or
     * not they do.
     */
    recptr = XactLogAbortRecord(GetCurrentTimestamp(),
                                nchildren, children,
                                nrels, rels,
                                nstats, stats,
                                MyXactFlags | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
                                xid, gid);

    if replorigin {
        /* Move LSNs forward for this replication origin */
        replorigin_session_advance(replorigin_session_origin_lsn,
                                   XactLastRecEnd);
    }

    /* Always flush, since we're about to remove the 2PC state file */
    XLogFlush(recptr);

    /*
     * Mark the transaction aborted in clog.  This is not absolutely necessary
     * but we may as well do it while we are here.
     */
    TransactionIdAbortTree(xid, nchildren, children);

    END_CRIT_SECTION();

    /*
     * Wait for synchronous replication, if required.
     *
     * Note that at this stage we have marked clog, but still show as running
     * in the procarray and continue to hold locks.
     */
    SyncRepWaitForLSN(recptr, false);
}

/*
 * PrepareRedoAdd
 *
 * Store pointers to the start/end of the WAL record along with the xid in
 * a gxact entry in shared memory TwoPhaseState structure.  If caller
 * specifies InvalidXLogRecPtr as WAL location to fetch the two-phase
 * data, the entry is marked as located on disk.
 */
pub unsafe fn PrepareRedoAdd(
    buf: *mut c_char,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    origin_id: RepOriginId,
) {
    let hdr: *mut TwoPhaseFileHeader = buf as *mut TwoPhaseFileHeader;
    let bufptr: *mut c_char;
    let gid: *const c_char;
    let gxact: GlobalTransaction;

    Assert!(LWLockHeldByMeInMode(TwoPhaseStateLock(), LW_EXCLUSIVE));
    Assert!(RecoveryInProgress());

    bufptr = buf.add(MAXALIGN(core::mem::size_of::<TwoPhaseFileHeader>() as Size) as usize);
    gid = bufptr as *const c_char;

    /*
     * Reserve the GID for the given transaction in the redo code path.
     *
     * This creates a gxact struct and puts it into the active array.
     *
     * In redo, this struct is mainly used to track PREPARE/COMMIT entries in
     * shared memory. Hence, we only fill up the bare minimum contents here.
     * The gxact also gets marked with gxact->inredo set to true to indicate
     * that it got added in the redo phase
     */

    /*
     * In the event of a crash while a checkpoint was running, it may be
     * possible that some two-phase data found its way to disk while its
     * corresponding record needs to be replayed in the follow-up recovery. As
     * the 2PC data was on disk, it has already been restored at the beginning
     * of recovery with restoreTwoPhaseData(), so skip this record to avoid
     * duplicates in TwoPhaseState.  If a consistent state has been reached,
     * the record is added to TwoPhaseState and it should have no
     * corresponding file in pg_twophase.
     */
    if !XLogRecPtrIsInvalid(start_lsn) {
        let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        TwoPhaseFilePath(path.as_mut_ptr(), (*hdr).xid);

        if access(path.as_ptr(), F_OK) == 0 {
            ereport!(if reachedConsistency { ERROR } else { WARNING },
                     errmsg!("could not recover two-phase state file for transaction {}",
                             (*hdr).xid));
            return;
        }

        if errno() != ENOENT {
            ereport!(ERROR,
                     errmsg!("could not access file \"{}\": {}",
                             CStr::from_ptr(path.as_ptr()).to_string_lossy(), strerror_errno()));
        }
    }

    /* Get a free gxact from the freelist */
    if (*TwoPhaseState).freeGXacts.is_null() {
        ereport!(ERROR,
                 errmsg!("maximum number of prepared transactions reached"));
    }
    gxact = (*TwoPhaseState).freeGXacts;
    (*TwoPhaseState).freeGXacts = (*gxact).next;

    (*gxact).prepared_at = (*hdr).prepared_at;
    (*gxact).prepare_start_lsn = start_lsn;
    (*gxact).prepare_end_lsn = end_lsn;
    (*gxact).xid = (*hdr).xid;
    (*gxact).owner = (*hdr).owner;
    (*gxact).locking_backend = INVALID_PROC_NUMBER;
    (*gxact).valid = false;
    (*gxact).ondisk = XLogRecPtrIsInvalid(start_lsn);
    (*gxact).inredo = true; /* yes, added in redo */
    strcpy((*gxact).gid.as_mut_ptr(), gid);

    /* And insert it into the active array */
    Assert!((*TwoPhaseState).numPrepXacts < max_prepared_xacts);
    let idx = (*TwoPhaseState).numPrepXacts;
    (*TwoPhaseState).numPrepXacts += 1;
    *(*TwoPhaseState).prepXacts.as_mut_ptr().add(idx as usize) = gxact;

    if origin_id != InvalidRepOriginId {
        /* recover apply progress */
        replorigin_advance(origin_id, (*hdr).origin_lsn, end_lsn,
                           false /* backward */, false /* WAL */);
    }

    elog!(DEBUG2, "added 2PC data in shared memory for transaction {}", (*gxact).xid);
}

/*
 * PrepareRedoRemove
 *
 * Remove the corresponding gxact entry from TwoPhaseState. Also remove
 * the 2PC file if a prepared transaction was saved via an earlier checkpoint.
 *
 * Caller must hold TwoPhaseStateLock in exclusive mode, because TwoPhaseState
 * is updated.
 */
pub unsafe fn PrepareRedoRemove(xid: TransactionId, giveWarning: bool) {
    let mut gxact: GlobalTransaction = core::ptr::null_mut();
    let mut i: c_int;
    let mut found: bool = false;

    Assert!(LWLockHeldByMeInMode(TwoPhaseStateLock(), LW_EXCLUSIVE));
    Assert!(RecoveryInProgress());

    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        gxact = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        if (*gxact).xid == xid {
            Assert!((*gxact).inredo);
            found = true;
            break;
        }
        i += 1;
    }

    /*
     * Just leave if there is nothing, this is expected during WAL replay.
     */
    if !found {
        return;
    }

    /*
     * And now we can clean up any files we may have left.
     */
    elog!(DEBUG2, "removing 2PC data for transaction {}", xid);
    if (*gxact).ondisk {
        RemoveTwoPhaseFile(xid, giveWarning);
    }
    RemoveGXact(gxact);
}

/*
 * LookupGXact
 *		Check if the prepared transaction with the given GID, lsn and timestamp
 *		exists.
 *
 * Note that we always compare with the LSN where prepare ends because that is
 * what is stored as origin_lsn in the 2PC file.
 *
 * This function is primarily used to check if the prepared transaction
 * received from the upstream (remote node) already exists. Checking only GID
 * is not sufficient because a different prepared xact with the same GID can
 * exist on the same node. So, we are ensuring to match origin_lsn and
 * origin_timestamp of prepared xact to avoid the possibility of a match of
 * prepared xact from two different nodes.
 */
#[no_mangle]
pub unsafe fn LookupGXact(
    gid: *const c_char,
    prepare_end_lsn: XLogRecPtr,
    origin_prepare_timestamp: TimestampTz,
) -> bool {
    let mut i: c_int;
    let mut found: bool = false;

    LWLockAcquire(TwoPhaseStateLock(), LW_SHARED);
    i = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        /* Ignore not-yet-valid GIDs. */
        if (*gxact).valid && strcmp((*gxact).gid.as_ptr(), gid) == 0 {
            let buf: *mut c_char;
            let hdr: *mut TwoPhaseFileHeader;

            /*
             * We are not expecting collisions of GXACTs (same gid) between
             * publisher and subscribers, so we perform all I/O while holding
             * TwoPhaseStateLock for simplicity.
             *
             * To move the I/O out of the lock, we need to ensure that no
             * other backend commits the prepared xact in the meantime. We can
             * do this optimization if we encounter many collisions in GID
             * between publisher and subscriber.
             */
            if (*gxact).ondisk {
                buf = ReadTwoPhaseFile((*gxact).xid, false);
            } else {
                let mut tmpbuf: *mut c_char = core::ptr::null_mut();
                Assert!((*gxact).prepare_start_lsn != 0);
                XlogReadTwoPhaseData((*gxact).prepare_start_lsn, &raw mut tmpbuf, core::ptr::null_mut());
                buf = tmpbuf;
            }

            hdr = buf as *mut TwoPhaseFileHeader;

            if (*hdr).origin_lsn == prepare_end_lsn &&
                (*hdr).origin_timestamp == origin_prepare_timestamp {
                found = true;
                pfree(buf as *mut c_void);
                break;
            }

            pfree(buf as *mut c_void);
        }
        i += 1;
    }
    LWLockRelease(TwoPhaseStateLock());
    found
}

/*
 * TwoPhaseTransactionGid
 *		Form the prepared transaction GID for two_phase transactions.
 *
 * Return the GID in the supplied buffer.
 */
#[no_mangle]
pub unsafe fn TwoPhaseTransactionGid(
    subid: Oid,
    xid: TransactionId,
    gid_res: *mut c_char,
    szgid: c_int,
) {
    Assert!(OidIsValid(subid));

    if !TransactionIdIsValid(xid) {
        ereport!(ERROR,
                 errmsg!("invalid two-phase transaction ID"));
    }

    snprintf_pg_gid(gid_res, szgid, subid, xid);
}

/*
 * IsTwoPhaseTransactionGidForSubid
 *		Check whether the given GID (as formed by TwoPhaseTransactionGid) is
 *		for the specified 'subid'.
 */
unsafe fn IsTwoPhaseTransactionGidForSubid(subid: Oid, gid: *mut c_char) -> bool {
    let ret: c_int;
    let mut subid_from_gid: Oid = 0;
    let mut xid_from_gid: TransactionId = 0;
    let mut gid_tmp: [c_char; GIDSIZE] = [0; GIDSIZE];

    /* Extract the subid and xid from the given GID */
    ret = sscanf_pg_gid(gid, &raw mut subid_from_gid, &raw mut xid_from_gid);

    /*
     * Check that the given GID has expected format, and at least the subid
     * matches.
     */
    if ret != 2 || subid != subid_from_gid {
        return false;
    }

    /*
     * Reconstruct a temporary GID based on the subid and xid extracted from
     * the given GID and check whether the temporary GID and the given GID
     * match.
     */
    TwoPhaseTransactionGid(subid, xid_from_gid, gid_tmp.as_mut_ptr(),
                           core::mem::size_of_val(&gid_tmp) as c_int);

    strcmp(gid, gid_tmp.as_ptr()) == 0
}

/*
 * LookupGXactBySubid
 *		Check if the prepared transaction done by apply worker exists.
 */
pub unsafe fn LookupGXactBySubid(subid: Oid) -> bool {
    let mut found: bool = false;

    LWLockAcquire(TwoPhaseStateLock(), LW_SHARED);
    let mut i: c_int = 0;
    while i < (*TwoPhaseState).numPrepXacts {
        let gxact: GlobalTransaction = *(*TwoPhaseState).prepXacts.as_ptr().add(i as usize);

        /* Ignore not-yet-valid GIDs. */
        if (*gxact).valid &&
            IsTwoPhaseTransactionGidForSubid(subid, (*gxact).gid.as_ptr() as *mut c_char) {
            found = true;
            break;
        }
        i += 1;
    }
    LWLockRelease(TwoPhaseStateLock());

    found
}

// ===========================================================================
// Self-contained stubs.  Per the PepperDB porting convention (mirroring
// multixact.rs / clog.rs), symbols whose real homes are not yet ported are
// stubbed locally here with `// TODO(pg-port): real SYM lives in <file>`.
// ===========================================================================

// offsetof(TwoPhaseStateData, prepXacts) ------------------------------------
#[inline]
unsafe fn offsetof_TwoPhaseStateData_prepXacts() -> Size {
    core::mem::offset_of!(TwoPhaseStateData, prepXacts) as Size
}

// MAXALIGN --- TODO(pg-port): real macro lives in c.h
#[inline]
fn MAXALIGN(len: Size) -> Size {
    const ALIGNOF_LONG: Size = core::mem::align_of::<i64>() as Size; // MAXIMUM_ALIGNOF
    (len + (ALIGNOF_LONG - 1)) & !(ALIGNOF_LONG - 1)
}

// Max(a, b) --- TODO(pg-port): real macro lives in c.h
#[inline]
fn Max(a: uint32, b: uint32) -> uint32 {
    if a > b { a } else { b }
}

// MemSet --- TODO(pg-port): real MemSet lives in c.h (utils macro)
#[inline]
unsafe fn MemSet(start: *mut c_void, val: c_int, len: Size) {
    extern "C" {
        fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    }
    memset(start, val, len as usize);
}

// libc string / memory helpers ----------------------------------------------
extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// add_size / mul_size --- TODO(pg-port): real ones live in storage/ipc/shmem.c
#[inline]
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}
#[inline]
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

// errno helpers --- TODO(pg-port): real ones live in c lib + src/port
extern "C" {
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn unlink(path: *const c_char) -> c_int;
    fn access(path: *const c_char, mode: c_int) -> c_int;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn sscanf(s: *const c_char, fmt: *const c_char, ...) -> c_int;
    #[link_name = "strtoull"]
    fn strtou64(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> uint64;
    fn strerror(errnum: c_int) -> *mut c_char;
}
#[inline]
unsafe fn errno() -> c_int {
    extern "C" {
        fn __error() -> *mut c_int; // macOS errno location
    }
    *__error()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    extern "C" {
        fn __error() -> *mut c_int;
    }
    *__error() = v;
}
#[inline]
unsafe fn strerror_errno() -> std::borrow::Cow<'static, str> {
    CStr::from_ptr(strerror(errno())).to_string_lossy()
}

pub const ENOENT: c_int = 2; // TODO(pg-port): errno.h
pub const ENOSPC: c_int = 28; // TODO(pg-port): errno.h
pub const O_RDONLY: c_int = 0x0000; // TODO(pg-port): fcntl.h
pub const O_WRONLY: c_int = 0x0001; // TODO(pg-port): fcntl.h
pub const O_CREAT: c_int = 0x0200; // TODO(pg-port): fcntl.h (macOS)
pub const O_TRUNC: c_int = 0x0400; // TODO(pg-port): fcntl.h (macOS)
pub const PG_BINARY: c_int = 0; // TODO(pg-port): real value lives in port.h
pub const F_OK: c_int = 0; // TODO(pg-port): unistd.h

// struct stat + fstat --- TODO(pg-port): real ones live in <sys/stat.h>
#[repr(C)]
pub struct stat_t {
    pub st_size: int64,
    // ... TODO(pg-port): <sys/stat.h> (only st_size is used here)
}
unsafe fn fstat(_fd: c_int, _buf: *mut stat_t) -> c_int {
    unimplemented!() // TODO(pg-port): real fstat lives in <sys/stat.h>
}

// snprintf wrappers (formatting matches the C printf templates) --------------
#[inline]
unsafe fn snprintf_twophase_path(path: *mut c_char, _n: usize, epoch: uint32, xid: uint32) -> c_int {
    // TWOPHASE_DIR "/%08X%08X"
    snprintf(path, MAXPGPATH, c"pg_twophase/%08X%08X".as_ptr(), epoch, xid)
}
#[inline]
unsafe fn snprintf_pg_gid(gid_res: *mut c_char, szgid: c_int, subid: Oid, xid: TransactionId) -> c_int {
    snprintf(gid_res, szgid as usize, c"pg_gid_%u_%u".as_ptr(), subid, xid)
}
#[inline]
unsafe fn sscanf_pg_gid(gid: *const c_char, subid: *mut Oid, xid: *mut TransactionId) -> c_int {
    sscanf(gid, c"pg_gid_%u_%u".as_ptr(), subid, xid)
}

// TwoPhaseRmgrId --- TODO(pg-port): real typedef lives in access/twophase_rmgr.h
pub type TwoPhaseRmgrId = u8;

// Oid helpers --- TODO(pg-port): real ones live in c.h / postgres_ext.h
pub const InvalidOid: Oid = 0; // TODO(pg-port): postgres_ext.h
#[inline]
fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

// MemoryContext / palloc --- already provided by prelude (palloc, palloc0,
// pfree, repalloc, MemoryContext, MemoryContextSwitchTo).

// ProcNumber --- TODO(pg-port): real defs live in storage/procnumber.h
pub type ProcNumber = c_int;
pub const INVALID_PROC_NUMBER: ProcNumber = -1; // TODO(pg-port): storage/procnumber.h
pub static mut MyProcNumber: ProcNumber = 0; // TODO(pg-port): storage/procnumber.h

// Misc / process state --- TODO(pg-port): real defs live in miscadmin.h etc.
pub static mut IsUnderPostmaster: bool = false; // TODO(pg-port): miscadmin.h
pub static mut IsPostmasterEnvironment: bool = false; // TODO(pg-port): miscadmin.h
pub static mut MyDatabaseId: Oid = 0; // TODO(pg-port): miscadmin.h
pub static mut InHotStandby: bool = false; // TODO(pg-port): storage/standby.h / xlogutils.h
pub static mut reachedConsistency: bool = false; // TODO(pg-port): access/xlogrecovery.c
pub static mut log_checkpoints: bool = false; // TODO(pg-port): access/xlog.c (GUC)
pub static mut MyXactFlags: c_int = 0; // TODO(pg-port): access/xact.h
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: c_int = 1 << 1; // TODO(pg-port): access/xact.h
unsafe fn AmStartupProcess() -> bool { crate::miscadmin::AmStartupProcess() }
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn superuser_arg(_roleid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): real superuser_arg lives in utils/misc/superuser.c
}

// before_shmem_exit --- TODO(pg-port): real one lives in storage/ipc/ipc.c
unsafe fn before_shmem_exit(_function: unsafe fn(c_int, Datum), _arg: Datum) { unimplemented!() }

// Critical section / interrupt macros --- TODO(pg-port): real defs in miscadmin.h
#[inline]
unsafe fn START_CRIT_SECTION() {}
#[inline]
unsafe fn END_CRIT_SECTION() {}
#[inline]
unsafe fn HOLD_INTERRUPTS() {}
#[inline]
unsafe fn RESUME_INTERRUPTS() {}

// Shared memory --- TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size, _found)
}

// LWLock --- TODO(pg-port): real definitions live in storage/lwlock.h
#[repr(C)]
pub struct LWLock {
    _private: [u8; 0],
}
pub const LW_EXCLUSIVE: c_int = 0; // TODO(pg-port): storage/lwlock.h
pub const LW_SHARED: c_int = 1; // TODO(pg-port): storage/lwlock.h
unsafe fn TwoPhaseStateLock() -> *mut LWLock {
    crate::backend_link_shims::TwoPhaseStateLock as *mut LWLock
}
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(_lock as _, if _mode == 1 { crate::storage::lmgr::lwlock::LWLockMode::LW_SHARED } else { crate::storage::lmgr::lwlock::LWLockMode::LW_EXCLUSIVE })
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(_lock as _)
}
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockHeldByMe lives in storage/lwlock.c
}
unsafe fn LWLockHeldByMeInMode(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockHeldByMeInMode lives in storage/lwlock.c
}

// PGPROC --- TODO(pg-port): real PGPROC lives in storage/proc.h ---------------
pub const NUM_LOCK_PARTITIONS: c_int = 16; // TODO(pg-port): storage/lwlock.h
pub const PGPROC_MAX_CACHED_SUBXIDS: c_int = 64; // TODO(pg-port): storage/proc.h
pub const PROC_WAIT_STATUS_OK: c_int = 0; // TODO(pg-port): storage/proc.h
pub const LW_WS_NOT_WAITING: u8 = 0; // TODO(pg-port): storage/lwlock.h
pub const DELAY_CHKPT_START: c_int = 1 << 0; // TODO(pg-port): storage/proc.h

#[repr(C)]
pub struct PGPROC_vxid {
    pub procNumber: ProcNumber,
    pub lxid: TransactionId, // LocalTransactionId
}
#[repr(C)]
pub struct XidCacheStatus {
    pub count: uint8,
    pub overflowed: bool,
}
#[repr(C)]
pub struct PGPROC_subxids {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS as usize],
}
#[repr(C)]
pub struct dlist_node_p {
    pub prev: *mut dlist_node_p,
    pub next: *mut dlist_node_p,
}
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: u64,
}
#[repr(C)]
pub struct PGPROC {
    pub links: dlist_node_p,
    pub waitStatus: c_int,
    pub vxid: PGPROC_vxid,
    pub xid: TransactionId,
    pub xmin: TransactionId,
    pub delayChkptFlags: c_int,
    pub statusFlags: uint8,
    pub pid: c_int,
    pub databaseId: Oid,
    pub roleId: Oid,
    pub tempNamespaceId: Oid,
    pub isRegularBackend: bool,
    pub lwWaiting: u8,
    pub lwWaitMode: u8,
    pub waitLock: *mut c_void,
    pub waitProcLock: *mut c_void,
    pub waitStart: pg_atomic_uint64,
    pub myProcLocks: [dlist_node_p; NUM_LOCK_PARTITIONS as usize],
    pub subxidStatus: XidCacheStatus,
    pub subxids: PGPROC_subxids,
    // ... TODO(pg-port): storage/proc.h
}
pub static mut MyProc: *mut PGPROC = core::ptr::null_mut(); // TODO(pg-port): storage/lmgr/proc.c
pub static mut PreparedXactProcs: [PGPROC; 0] = []; // TODO(pg-port): storage/lmgr/proc.c
pub static mut ProcLastRecPtr: XLogRecPtr = 0; // TODO(pg-port): access/transam/xlog.c

unsafe fn GetPGProcByNumber(_n: c_int) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): real GetPGProcByNumber lives in storage/proc.h
}
unsafe fn GetNumberFromPGProc(_proc: *mut PGPROC) -> c_int {
    unimplemented!() // TODO(pg-port): real GetNumberFromPGProc lives in storage/proc.h
}
unsafe fn dlist_node_init(_node: *mut dlist_node_p) { crate::lib::ilist::dlist_node_init(_node as _) }
unsafe fn dlist_init(_head: *mut dlist_node_p) {
    unimplemented!() // TODO(pg-port): real dlist_init lives in lib/ilist.h
}
unsafe fn pg_atomic_init_u64(_ptr: *mut pg_atomic_uint64, _val: u64) {
    unimplemented!() // TODO(pg-port): real pg_atomic_init_u64 lives in port/atomics.h
}

// VirtualTransactionId --- TODO(pg-port): real defs live in storage/lock.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: TransactionId,
}
unsafe fn VirtualTransactionIdIsValid(_vxid: VirtualTransactionId) -> bool { unimplemented!() }
unsafe fn VirtualTransactionIdEquals(_a: VirtualTransactionId, _b: VirtualTransactionId) -> bool { unimplemented!() }
unsafe fn GET_VXID_FROM_PGPROC(_vxid: *mut VirtualTransactionId, _proc: &PGPROC) { unimplemented!() }
unsafe fn LocalTransactionIdIsValid(_lxid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real LocalTransactionIdIsValid lives in storage/lock.h
}

// ProcArray --- TODO(pg-port): real defs live in storage/ipc/procarray.c
unsafe fn ProcArrayAdd(_proc: *mut PGPROC) {
    unimplemented!() // TODO(pg-port): real ProcArrayAdd lives in storage/ipc/procarray.c
}
unsafe fn ProcArrayRemove(_proc: *mut PGPROC, _latestXid: TransactionId) {
    unimplemented!() // TODO(pg-port): real ProcArrayRemove lives in storage/ipc/procarray.c
}

// RelFileLocator + SharedInvalidationMessage ---------------------------------
// TODO(pg-port): real RelFileLocator lives in storage/relfilelocator.rs (ported),
// but to keep this unit self-contained per convention we alias minimally.
pub use crate::storage::relfilelocator::RelFileLocator;
pub type SharedInvalidationMessage = c_void; // TODO(pg-port): storage/sinval.h (union)

// TimestampTz --- TODO(pg-port): real type lives in datatype/timestamp.h
pub type TimestampTz = int64;
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}

// AttrNumber + Datum getters/SRF --- TODO(pg-port): real defs live in fmgr/funcapi
pub type AttrNumber = i16; // TODO(pg-port): access/attnum.h
pub type FunctionCallInfo = *mut c_void; // TODO(pg-port): fmgr.h
pub type HeapTuple = *mut c_void; // TODO(pg-port): access/htup.h
pub type TupleDesc = *mut c_void; // TODO(pg-port): access/tupdesc.h
#[repr(C)]
pub struct FuncCallContext {
    pub user_fctx: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
    // ... TODO(pg-port): funcapi.h
}
pub const XIDOID: Oid = 28; // TODO(pg-port): catalog/pg_type_d.h
pub const TEXTOID: Oid = 25; // TODO(pg-port): catalog/pg_type_d.h
pub const TIMESTAMPTZOID: Oid = 1184; // TODO(pg-port): catalog/pg_type_d.h
pub const OIDOID: Oid = 26; // TODO(pg-port): catalog/pg_type_d.h

unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!() // TODO(pg-port): real SRF_IS_FIRSTCALL lives in funcapi.h
}
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): real SRF_FIRSTCALL_INIT lives in funcapi.h
}
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): real SRF_PERCALL_SETUP lives in funcapi.h
}
macro_rules! SRF_RETURN_NEXT {
    ($funcctx:expr, $result:expr) => {{
        return SRF_RETURN_NEXT_impl($funcctx, $result); // TODO(pg-port): real macro lives in funcapi.h
    }};
}
use SRF_RETURN_NEXT;
unsafe fn SRF_RETURN_NEXT_impl(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO(pg-port): real SRF_RETURN_NEXT lives in funcapi.h
}
macro_rules! SRF_RETURN_DONE {
    ($funcctx:expr) => {
        SRF_RETURN_DONE_impl($funcctx) // TODO(pg-port): real macro lives in funcapi.h
    };
}
use SRF_RETURN_DONE;
unsafe fn SRF_RETURN_DONE_impl(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): real SRF_RETURN_DONE lives in funcapi.h
}
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc { unimplemented!() }
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) {
    unimplemented!() // TODO(pg-port): real TupleDescInitEntry lives in access/common/tupdesc.c
}
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!() // TODO(pg-port): real BlessTupleDesc lives in utils/fmgr/funcapi.c
}
unsafe fn heap_form_tuple(_tupleDescriptor: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple {
    unimplemented!() // TODO(pg-port): real heap_form_tuple lives in access/common/heaptuple.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO(pg-port): real HeapTupleGetDatum lives in funcapi.h
}
unsafe fn TransactionIdGetDatum(_xid: TransactionId) -> Datum {
    unimplemented!() // TODO(pg-port): real TransactionIdGetDatum lives in postgres.h
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): real CStringGetTextDatum lives in utils/builtins.h
}
unsafe fn TimestampTzGetDatum(_tz: TimestampTz) -> Datum {
    unimplemented!() // TODO(pg-port): real TimestampTzGetDatum lives in utils/timestamp.h
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO(pg-port): real ObjectIdGetDatum lives in postgres.h
}

// Transaction id helpers --- TODO(pg-port): real defs live in access/transam.*
unsafe fn TransactionIdPrecedes(_id1: TransactionId, _id2: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real TransactionIdPrecedes lives in access/transam/transam.c
}
unsafe fn TransactionIdFollows(_id1: TransactionId, _id2: TransactionId) -> bool { crate::access::transam::transam::TransactionIdFollows(_id1 as _, _id2 as _) }
unsafe fn TransactionIdFollowsOrEquals(_id1: TransactionId, _id2: TransactionId) -> bool { crate::access::transam::transam::TransactionIdFollowsOrEquals(_id1 as _, _id2 as _) }
unsafe fn TransactionIdLatest(_mainxid: TransactionId, _nxids: c_int, _xids: *const TransactionId) -> TransactionId {
    unimplemented!() // TODO(pg-port): real TransactionIdLatest lives in access/transam/transam.c
}
unsafe fn TransactionIdDidCommit(_transactionId: TransactionId) -> bool { crate::access::transam::transam::TransactionIdDidCommit(_transactionId as _) }
unsafe fn TransactionIdDidAbort(_transactionId: TransactionId) -> bool { crate::access::transam::transam::TransactionIdDidAbort(_transactionId as _) }
unsafe fn TransactionIdCommitTree(_xid: TransactionId, _nxids: c_int, _xids: *mut TransactionId) { crate::access::transam::transam::TransactionIdCommitTree(_xid as _, _nxids as _, _xids as _) }
unsafe fn TransactionIdAbortTree(_xid: TransactionId, _nxids: c_int, _xids: *mut TransactionId) { crate::access::transam::transam::TransactionIdAbortTree(_xid as _, _nxids as _, _xids as _) }
unsafe fn SubTransSetParent(_xid: TransactionId, _parent: TransactionId) { crate::access::transam::subtrans::SubTransSetParent(_xid as _, _parent as _) }

pub use crate::access::transam::varsup::TransamVariablesData as VariableCacheData;
pub use crate::access::transam::varsup::TransamVariables;
unsafe fn ReadNextFullTransactionId() -> FullTransactionId { crate::access::transam::varsup::ReadNextFullTransactionId() }
unsafe fn FullTransactionIdFromAllowableAt(_nextFullXid: FullTransactionId, _xid: TransactionId) -> FullTransactionId {
    unimplemented!() // TODO(pg-port): real FullTransactionIdFromAllowableAt lives in access/transam.h
}
unsafe fn AdvanceNextFullTransactionIdPastXid(_xid: TransactionId) { crate::access::transam::varsup::AdvanceNextFullTransactionIdPastXid(_xid as _) }

// XLOG --- TODO(pg-port): real definitions live in access/xlog*.h
pub const RM_XACT_ID: u8 = 1; // TODO(pg-port): access/rmgrlist.h
pub const XLOG_XACT_PREPARE: uint8 = 0x10; // TODO(pg-port): access/xact.h
pub const XLOG_XACT_OPMASK: uint8 = 0x70; // TODO(pg-port): access/xact.h
pub const XLOG_INCLUDE_ORIGIN: uint8 = 0x01; // TODO(pg-port): access/xlog.h
pub static mut XactLastRecEnd: XLogRecPtr = 0; // TODO(pg-port): access/transam/xlog.c
pub static mut wal_segment_size: c_int = 0; // TODO(pg-port): access/xlog.c (GUC)

#[repr(C)]
pub struct XLogRecord {
    _private: [u8; 0],
}
#[repr(C)]
pub struct XLogReaderState {
    _private: [u8; 0],
}
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO(pg-port): real XLogBeginInsert lives in access/transam/xloginsert.c
}
unsafe fn XLogRegisterData(_data: *mut c_char, _len: usize) {
    unimplemented!() // TODO(pg-port): real XLogRegisterData lives in access/transam/xloginsert.c
}
unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real XLogInsert lives in access/transam/xloginsert.c
}
unsafe fn XLogSetRecordFlags(_flags: uint8) { crate::access::transam::xloginsert::XLogSetRecordFlags(_flags as _) }
unsafe fn XLogEnsureRecordSpace(_max_block_id: c_int, _ndatas: c_int) { crate::access::transam::xloginsert::XLogEnsureRecordSpace(_max_block_id as _, _ndatas as _) }
unsafe fn XLogFlush(_record: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real XLogFlush lives in access/transam/xlog.c
}
unsafe fn XLogReaderAllocate(
    _wal_segsz_bytes: c_int,
    _waldir: *const c_char,
    _routine: *mut c_void,
    _private_data: *mut c_void,
) -> *mut XLogReaderState {
    unimplemented!() // TODO(pg-port): real XLogReaderAllocate lives in access/transam/xlogreader.c
}
unsafe fn XLogReaderFree(_state: *mut XLogReaderState) { crate::access::transam::xlogreader::XLogReaderFree(_state as _) }
unsafe fn XLogBeginRead(_state: *mut XLogReaderState, _RecPtr: XLogRecPtr) { crate::access::transam::xlogreader::XLogBeginRead(_state as _, _RecPtr as _) }
unsafe fn XLogReadRecord(_state: *mut XLogReaderState, _errormsg: *mut *mut c_char) -> *mut XLogRecord {
    unimplemented!() // TODO(pg-port): real XLogReadRecord lives in access/transam/xlogreader.c
}
unsafe fn XLogRecGetRmid(_decoder: *mut XLogReaderState) -> u8 {
    unimplemented!() // TODO(pg-port): real XLogRecGetRmid lives in access/xlogreader.h
}
unsafe fn XLogRecGetInfo(_decoder: *mut XLogReaderState) -> uint8 { crate::access::transam::xlogreader::XLogRecGetInfo(_decoder as _) }
unsafe fn XLogRecGetData(_decoder: *mut XLogReaderState) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetData(_decoder as _) }
unsafe fn XLogRecGetDataLen(_decoder: *mut XLogReaderState) -> uint32 { crate::access::transam::xlogreader::XLogRecGetDataLen(_decoder as _) }
unsafe fn XL_ROUTINE_two_phase() -> *mut c_void {
    unimplemented!() // TODO(pg-port): real XL_ROUTINE macro lives in access/xlogreader.h
}
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr // TODO(pg-port): real macro lives in access/xlogdefs.h
}
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as uint32, lsn as uint32) // TODO(pg-port): access/xlogdefs.h
}

// Recovery state --- TODO(pg-port): real defs live in access/xlog*.h
unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }

// XACT log records --- TODO(pg-port): real defs live in access/transam/xact.c
unsafe fn XactLogCommitRecord(
    _commit_time: TimestampTz,
    _nsubxacts: c_int,
    _subxacts: *mut TransactionId,
    _nrels: c_int,
    _rels: *mut RelFileLocator,
    _nstats: c_int,
    _stats: *mut xl_xact_stats_item,
    _nmsgs: c_int,
    _msgs: *mut SharedInvalidationMessage,
    _relcacheInval: bool,
    _xactflags: c_int,
    _twophase_xid: TransactionId,
    _twophase_gid: *const c_char,
) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real XactLogCommitRecord lives in access/transam/xact.c
}
unsafe fn XactLogAbortRecord(
    _abort_time: TimestampTz,
    _nsubxacts: c_int,
    _subxacts: *mut TransactionId,
    _nrels: c_int,
    _rels: *mut RelFileLocator,
    _nstats: c_int,
    _stats: *mut xl_xact_stats_item,
    _xactflags: c_int,
    _twophase_xid: TransactionId,
    _twophase_gid: *const c_char,
) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real XactLogAbortRecord lives in access/transam/xact.c
}
unsafe fn xactGetCommittedChildren(_ptr: *mut *mut TransactionId) -> c_int { crate::access::transam::xact::xactGetCommittedChildren(_ptr as _) }
unsafe fn xactGetCommittedInvalidationMessages(
    _msgs: *mut *mut SharedInvalidationMessage,
    _RelcacheInitFileInval: *mut bool,
) -> c_int {
    unimplemented!() // TODO(pg-port): real xactGetCommittedInvalidationMessages lives in access/transam/xact.c
}

// commit timestamps --- TODO(pg-port): real defs live in access/transam/commit_ts.c
unsafe fn TransactionTreeSetCommitTsData(
    _xid: TransactionId,
    _nsubxids: c_int,
    _subxids: *mut TransactionId,
    _timestamp: TimestampTz,
    _nodeid: RepOriginId,
) {
    unimplemented!() // TODO(pg-port): real TransactionTreeSetCommitTsData lives in access/transam/commit_ts.c
}

// Replication origin --- TODO(pg-port): real defs live in replication/logical/origin.c
pub const DoNotReplicateId: RepOriginId = 0xFFFF; // TODO(pg-port): replication/origin.h
pub static mut replorigin_session_origin: RepOriginId = InvalidRepOriginId; // TODO(pg-port): origin.c
pub static mut replorigin_session_origin_lsn: XLogRecPtr = 0; // TODO(pg-port): origin.c
pub static mut replorigin_session_origin_timestamp: TimestampTz = 0; // TODO(pg-port): origin.c
unsafe fn replorigin_session_advance(_remote_commit: XLogRecPtr, _local_commit: XLogRecPtr) { crate::replication::logical::origin::replorigin_session_advance(_remote_commit as _, _local_commit as _) }
unsafe fn replorigin_advance(
    _node: RepOriginId,
    _remote_commit: XLogRecPtr,
    _local_commit: XLogRecPtr,
    _go_backward: bool,
    _wal_log: bool,
) {
    unimplemented!() // TODO(pg-port): real replorigin_advance lives in replication/logical/origin.c
}

// Synchronous replication --- TODO(pg-port): real defs live in replication/syncrep.c
unsafe fn SyncRepWaitForLSN(_lsn: XLogRecPtr, _commit: bool) {
    unimplemented!() // TODO(pg-port): real SyncRepWaitForLSN lives in replication/syncrep.c
}

// storage / smgr --- TODO(pg-port): real defs live in catalog/storage.c & storage/smgr
unsafe fn smgrGetPendingDeletes(_forCommit: bool, _ptr: *mut *mut RelFileLocator) -> c_int {
    unimplemented!() // TODO(pg-port): real smgrGetPendingDeletes lives in catalog/storage.c
}
unsafe fn DropRelationFiles(_delrels: *mut RelFileLocator, _ndelrels: c_int, _isRedo: bool) { crate::storage::smgr::md::DropRelationFiles(_delrels as _, _ndelrels as _, _isRedo as _) }

// pgstat --- TODO(pg-port): real defs live in utils/activity/pgstat*.c
pub const WAIT_EVENT_TWOPHASE_FILE_READ: uint32 = 0; // TODO(pg-port): utils/activity/wait_event_names
pub const WAIT_EVENT_TWOPHASE_FILE_WRITE: uint32 = 0; // TODO(pg-port): utils/activity/wait_event_names
pub const WAIT_EVENT_TWOPHASE_FILE_SYNC: uint32 = 0; // TODO(pg-port): utils/activity/wait_event_names
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) { crate::parser_link_shims::pgstat_report_wait_start(_wait_event_info as _) }
unsafe fn pgstat_report_wait_end() { crate::parser_link_shims::pgstat_report_wait_end() }
unsafe fn pgstat_get_transactional_drops(_isCommit: bool, _ptr: *mut *mut xl_xact_stats_item) -> c_int {
    unimplemented!() // TODO(pg-port): real pgstat_get_transactional_drops lives in utils/activity/pgstat_xact.c
}
unsafe fn pgstat_execute_transactional_drops(_ndrops: c_int, _drops: *mut xl_xact_stats_item, _is_redo: bool) {
    unimplemented!() // TODO(pg-port): real pgstat_execute_transactional_drops lives in utils/activity/pgstat_xact.c
}
unsafe fn AtEOXact_PgStat(_isCommit: bool, _parallel: bool) { crate::utils::activity::pgstat_xact::AtEOXact_PgStat(_isCommit as _, _parallel as _) }

// Cache invalidation --- TODO(pg-port): real defs live in utils/cache/inval.c
unsafe fn SendSharedInvalidMessages(_msgs: *const SharedInvalidationMessage, _n: c_int) { crate::storage::ipc::sinval::SendSharedInvalidMessages(_msgs as _, _n as _) }
unsafe fn RelationCacheInitFilePreInvalidate() { crate::utils::cache::relcache::RelationCacheInitFilePreInvalidate() }
unsafe fn RelationCacheInitFilePostInvalidate() { crate::utils::cache::relcache::RelationCacheInitFilePostInvalidate() }

// Predicate locking --- TODO(pg-port): real defs live in storage/lmgr/predicate.c
unsafe fn PredicateLockTwoPhaseFinish(_xid: TransactionId, _isCommit: bool) { crate::storage::lmgr::predicate::PredicateLockTwoPhaseFinish(_xid as _, _isCommit as _) }

// Standby locks --- TODO(pg-port): real defs live in storage/ipc/standby.c
unsafe fn StandbyReleaseLockTree(_xid: TransactionId, _nsubxids: c_int, _subxids: *mut TransactionId) { unimplemented!() }

// fd / dir helpers: use canonical storage/file/fd definitions.
use crate::storage::file::fd::{DIR, dirent};
unsafe fn OpenTransientFile(_fileName: *const c_char, _fileFlags: c_int) -> c_int {
    crate::storage::file::fd::OpenTransientFile(_fileName, _fileFlags)
}
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    crate::storage::file::fd::CloseTransientFile(_fd)
}
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    crate::storage::file::fd::AllocateDir(_dirname)
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    crate::storage::file::fd::ReadDir(_dir, _dirname)
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    crate::storage::file::fd::FreeDir(_dir)
}
unsafe fn pg_fsync(_fd: c_int) -> c_int {
    crate::storage::file::fd::pg_fsync(_fd)
}
unsafe fn fsync_fname(_fname: *const c_char, _isdir: bool) {
    crate::storage::file::fd::fsync_fname(_fname, _isdir)
}

// Tracepoints --- TODO(pg-port): real defs live in pg_trace.h (DTrace)
#[inline]
unsafe fn TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_START() {}
#[inline]
unsafe fn TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_DONE() {}

//! src/backend/utils/adt/lockfuncs.c
//!
//! lockfuncs.c
//!		Functions for SQL access to various lock-manager capabilities.
//!
//! Copyright (c) 2002-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!		src/backend/utils/adt/lockfuncs.c

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::c::{int64, int32, uint32, TransactionId};
use crate::access::attnum::AttrNumber;
use crate::miscadmin::{MaxBackends, MyDatabaseId};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/*
 * This must match enum LockTagType!  Also, be sure to document any changes
 * in the docs for the pg_locks view and update the WaitEventLOCK section in
 * src/backend/utils/activity/wait_event_names.txt.
 */
pub const LockTagTypeNames: [*const c_char; 12] = [
    c"relation".as_ptr(),
    c"extend".as_ptr(),
    c"frozenid".as_ptr(),
    c"page".as_ptr(),
    c"tuple".as_ptr(),
    c"transactionid".as_ptr(),
    c"virtualxid".as_ptr(),
    c"spectoken".as_ptr(),
    c"object".as_ptr(),
    c"userlock".as_ptr(),
    c"advisory".as_ptr(),
    c"applytransaction".as_ptr(),
];

/* This must match enum PredicateLockTargetType (predicate_internals.h) */
const PredicateLockTagTypeNames: [*const c_char; 3] = [
    c"relation".as_ptr(),
    c"page".as_ptr(),
    c"tuple".as_ptr(),
];

/* Working status for pg_lock_status */
#[repr(C)]
struct PG_Lock_Status {
    lockData: *mut LockData,           /* state data from lmgr */
    currIdx: c_int,                    /* current PROCLOCK index */
    predLockData: *mut PredicateLockData, /* state data for pred locks */
    predLockIdx: c_int,                /* current index for pred lock */
}

/* Number of columns in pg_locks output */
const NUM_LOCK_STATUS_COLUMNS: usize = 16;

/*
 * VXIDGetDatum - Construct a text representation of a VXID
 *
 * This is currently only used in pg_lock_status, so we put it here.
 */
unsafe fn VXIDGetDatum(procNumber: ProcNumber, lxid: LocalTransactionId) -> Datum {
    /*
     * The representation is "<procNumber>/<lxid>", decimal and unsigned
     * decimal respectively.  Note that elog.c also knows how to format a
     * vxid.
     */
    let mut vxidstr: [c_char; 32] = [0; 32];

    snprintf(
        vxidstr.as_mut_ptr(),
        std::mem::size_of_val(&vxidstr),
        c"%d/%u".as_ptr(),
        procNumber,
        lxid,
    );

    CStringGetTextDatum(vxidstr.as_ptr())
}

/*
 * pg_lock_status - produce a view with one row per held or awaited lock mode
 */
#[no_mangle]
pub unsafe extern "C" fn pg_lock_status(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;
    let mystatus: *mut PG_Lock_Status;
    let lockData: *mut LockData;
    let predLockData: *mut PredicateLockData;

    if SRF_IS_FIRSTCALL() {
        let tupdesc: TupleDesc;
        let oldcontext: MemoryContext;

        /* create a function context for cross-call persistence */
        let funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match function's declaration in pg_proc.h */
        tupdesc = CreateTemplateTupleDesc(NUM_LOCK_STATUS_COLUMNS as c_int);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"locktype".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"database".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"relation".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"page".as_ptr(), INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"tuple".as_ptr(), INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, 6 as AttrNumber, c"virtualxid".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 7 as AttrNumber, c"transactionid".as_ptr(), XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 8 as AttrNumber, c"classid".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 9 as AttrNumber, c"objid".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 10 as AttrNumber, c"objsubid".as_ptr(), INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, 11 as AttrNumber, c"virtualtransaction".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 12 as AttrNumber, c"pid".as_ptr(), INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 13 as AttrNumber, c"mode".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 14 as AttrNumber, c"granted".as_ptr(), BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, 15 as AttrNumber, c"fastpath".as_ptr(), BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, 16 as AttrNumber, c"waitstart".as_ptr(), TIMESTAMPTZOID, -1, 0);

        (*funcctx).tuple_desc = BlessTupleDesc(tupdesc);

        /*
         * Collect all the locking information that we will format and send
         * out as a result set.
         */
        let mystatus = palloc(std::mem::size_of::<PG_Lock_Status>()) as *mut PG_Lock_Status;
        (*funcctx).user_fctx = mystatus as *mut std::ffi::c_void;

        (*mystatus).lockData = GetLockStatusData();
        (*mystatus).currIdx = 0;
        (*mystatus).predLockData = GetPredicateLockStatusData();
        (*mystatus).predLockIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    mystatus = (*funcctx).user_fctx as *mut PG_Lock_Status;
    lockData = (*mystatus).lockData;

    while (*mystatus).currIdx < (*lockData).nelements {
        let mut granted: bool;
        let mut mode: LOCKMODE = 0;
        let locktypename: *const c_char;
        let mut tnbuf: [c_char; 32] = [0; 32];
        let mut values: [Datum; NUM_LOCK_STATUS_COLUMNS] = [0; NUM_LOCK_STATUS_COLUMNS];
        let mut nulls: [bool; NUM_LOCK_STATUS_COLUMNS] = [false; NUM_LOCK_STATUS_COLUMNS];
        let tuple: HeapTuple;
        let result: Datum;
        let instance: *mut LockInstanceData;

        instance = &mut (*(*lockData).locks.offset((*mystatus).currIdx as isize)) as *mut LockInstanceData;

        /*
         * Look to see if there are any held lock modes in this PROCLOCK. If
         * so, report, and destructively modify lockData so we don't report
         * again.
         */
        granted = false;
        if (*instance).holdMask != 0 {
            mode = 0;
            while mode < MAX_LOCKMODES {
                if (*instance).holdMask & LOCKBIT_ON(mode) != 0 {
                    granted = true;
                    (*instance).holdMask &= LOCKBIT_OFF(mode);
                    break;
                }
                mode += 1;
            }
        }

        /*
         * If no (more) held modes to report, see if PROC is waiting for a
         * lock on this lock.
         */
        if !granted {
            if (*instance).waitLockMode != NoLock {
                /* Yes, so report it with proper mode */
                mode = (*instance).waitLockMode;

                /*
                 * We are now done with this PROCLOCK, so advance pointer to
                 * continue with next one on next call.
                 */
                (*mystatus).currIdx += 1;
            } else {
                /*
                 * Okay, we've displayed all the locks associated with this
                 * PROCLOCK, proceed to the next one.
                 */
                (*mystatus).currIdx += 1;
                continue;
            }
        }

        /*
         * Form tuple with appropriate data.
         */

        if (*instance).locktag.locktag_type <= LOCKTAG_LAST_TYPE as u8 {
            locktypename = LockTagTypeNames[(*instance).locktag.locktag_type as usize];
        } else {
            snprintf(
                tnbuf.as_mut_ptr(),
                std::mem::size_of_val(&tnbuf),
                c"unknown %d".as_ptr(),
                (*instance).locktag.locktag_type as c_int,
            );
            locktypename = tnbuf.as_ptr();
        }
        values[0] = CStringGetTextDatum(locktypename);

        match (*instance).locktag.locktag_type as LockTagType {
            LOCKTAG_RELATION | LOCKTAG_RELATION_EXTEND => {
                values[1] = ObjectIdGetDatum((*instance).locktag.locktag_field1);
                values[2] = ObjectIdGetDatum((*instance).locktag.locktag_field2);
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
            }
            LOCKTAG_DATABASE_FROZEN_IDS => {
                values[1] = ObjectIdGetDatum((*instance).locktag.locktag_field1);
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
            }
            LOCKTAG_PAGE => {
                values[1] = ObjectIdGetDatum((*instance).locktag.locktag_field1);
                values[2] = ObjectIdGetDatum((*instance).locktag.locktag_field2);
                values[3] = UInt32GetDatum((*instance).locktag.locktag_field3);
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
            }
            LOCKTAG_TUPLE => {
                values[1] = ObjectIdGetDatum((*instance).locktag.locktag_field1);
                values[2] = ObjectIdGetDatum((*instance).locktag.locktag_field2);
                values[3] = UInt32GetDatum((*instance).locktag.locktag_field3);
                values[4] = UInt16GetDatum((*instance).locktag.locktag_field4 as u16);
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
            }
            LOCKTAG_TRANSACTION => {
                values[6] = TransactionIdGetDatum((*instance).locktag.locktag_field1);
                nulls[1] = true;
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
            }
            LOCKTAG_VIRTUALTRANSACTION => {
                values[5] = VXIDGetDatum(
                    (*instance).locktag.locktag_field1 as ProcNumber,
                    (*instance).locktag.locktag_field2,
                );
                nulls[1] = true;
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
            }
            LOCKTAG_SPECULATIVE_TOKEN => {
                values[6] = TransactionIdGetDatum((*instance).locktag.locktag_field1);
                values[8] = ObjectIdGetDatum((*instance).locktag.locktag_field2);
                nulls[1] = true;
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[7] = true;
                nulls[9] = true;
            }
            LOCKTAG_APPLY_TRANSACTION => {
                values[1] = ObjectIdGetDatum((*instance).locktag.locktag_field1);
                values[8] = ObjectIdGetDatum((*instance).locktag.locktag_field2);
                values[6] = ObjectIdGetDatum((*instance).locktag.locktag_field3);
                values[9] = Int16GetDatum((*instance).locktag.locktag_field4 as i16);
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[7] = true;
            }
            /* LOCKTAG_OBJECT | LOCKTAG_USERLOCK | LOCKTAG_ADVISORY */
            /* default: treat unknown locktags like OBJECT */
            _ => {
                values[1] = ObjectIdGetDatum((*instance).locktag.locktag_field1);
                values[7] = ObjectIdGetDatum((*instance).locktag.locktag_field2);
                values[8] = ObjectIdGetDatum((*instance).locktag.locktag_field3);
                values[9] = Int16GetDatum((*instance).locktag.locktag_field4 as i16);
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
            }
        }

        values[10] = VXIDGetDatum((*instance).vxid.procNumber, (*instance).vxid.localTransactionId);
        if (*instance).pid != 0 {
            values[11] = Int32GetDatum((*instance).pid);
        } else {
            nulls[11] = true;
        }
        values[12] = CStringGetTextDatum(GetLockmodeName(
            (*instance).locktag.locktag_lockmethodid,
            mode,
        ));
        values[13] = BoolGetDatum(granted);
        values[14] = BoolGetDatum((*instance).fastpath);
        if !granted && (*instance).waitStart != 0 {
            values[15] = TimestampTzGetDatum((*instance).waitStart);
        } else {
            nulls[15] = true;
        }

        tuple = heap_form_tuple((*funcctx).tuple_desc, values.as_mut_ptr(), nulls.as_mut_ptr());
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    /*
     * Have returned all regular locks. Now start on the SIREAD predicate
     * locks.
     */
    predLockData = (*mystatus).predLockData;
    if (*mystatus).predLockIdx < (*predLockData).nelements {
        let lockType: PredicateLockTargetType;

        let predTag: *mut PREDICATELOCKTARGETTAG =
            &mut (*(*predLockData).locktags.offset((*mystatus).predLockIdx as isize))
                as *mut PREDICATELOCKTARGETTAG;
        let xact: *mut SERIALIZABLEXACT =
            &mut (*(*predLockData).xacts.offset((*mystatus).predLockIdx as isize))
                as *mut SERIALIZABLEXACT;
        let mut values: [Datum; NUM_LOCK_STATUS_COLUMNS] = [0; NUM_LOCK_STATUS_COLUMNS];
        let mut nulls: [bool; NUM_LOCK_STATUS_COLUMNS] = [false; NUM_LOCK_STATUS_COLUMNS];
        let tuple: HeapTuple;
        let result: Datum;

        (*mystatus).predLockIdx += 1;

        /*
         * Form tuple with appropriate data.
         */

        /* lock type */
        lockType = GET_PREDICATELOCKTARGETTAG_TYPE(*predTag);

        values[0] = CStringGetTextDatum(PredicateLockTagTypeNames[lockType as usize]);

        /* lock target */
        values[1] = GET_PREDICATELOCKTARGETTAG_DB(*predTag);
        values[2] = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag);
        if lockType == PREDLOCKTAG_TUPLE {
            values[4] = GET_PREDICATELOCKTARGETTAG_OFFSET(*predTag);
        } else {
            nulls[4] = true;
        }
        if (lockType == PREDLOCKTAG_TUPLE) || (lockType == PREDLOCKTAG_PAGE) {
            values[3] = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);
        } else {
            nulls[3] = true;
        }

        /* these fields are targets for other types of locks */
        nulls[5] = true; /* virtualxid */
        nulls[6] = true; /* transactionid */
        nulls[7] = true; /* classid */
        nulls[8] = true; /* objid */
        nulls[9] = true; /* objsubid */

        /* lock holder */
        values[10] = VXIDGetDatum((*xact).vxid.procNumber, (*xact).vxid.localTransactionId);
        if (*xact).pid != 0 {
            values[11] = Int32GetDatum((*xact).pid);
        } else {
            nulls[11] = true;
        }

        /*
         * Lock mode. Currently all predicate locks are SIReadLocks, which are
         * always held (never waiting) and have no fast path
         */
        values[12] = CStringGetTextDatum(c"SIReadLock".as_ptr());
        values[13] = BoolGetDatum(true);
        values[14] = BoolGetDatum(false);
        nulls[15] = true;

        tuple = heap_form_tuple((*funcctx).tuple_desc, values.as_mut_ptr(), nulls.as_mut_ptr());
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx)
}

/*
 * pg_blocking_pids - produce an array of the PIDs blocking given PID
 *
 * The reported PIDs are those that hold a lock conflicting with blocked_pid's
 * current request (hard block), or are requesting such a lock and are ahead
 * of blocked_pid in the lock's wait queue (soft block).
 *
 * In parallel-query cases, we report all PIDs blocking any member of the
 * given PID's lock group, and the reported PIDs are those of the blocking
 * PIDs' lock group leaders.  This allows callers to compare the result to
 * lists of clients' pg_backend_pid() results even during a parallel query.
 *
 * Parallel query makes it possible for there to be duplicate PIDs in the
 * result (either because multiple waiters are blocked by same PID, or
 * because multiple blockers have same group leader PID).  We do not bother
 * to eliminate such duplicates from the result.
 *
 * We need not consider predicate locks here, since those don't block anything.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_blocking_pids(fcinfo: FunctionCallInfo) -> Datum {
    let blocked_pid: c_int = PG_GETARG_INT32(0);
    let arrayelems: *mut Datum;
    let mut narrayelems: c_int;
    let lockData: *mut BlockedProcsData; /* state data from lmgr */
    let mut i: c_int;
    let mut j: c_int;

    /* Collect a snapshot of lock manager state */
    lockData = GetBlockerStatusData(blocked_pid);

    /* We can't need more output entries than there are reported PROCLOCKs */
    arrayelems = palloc((*lockData).nlocks as usize * std::mem::size_of::<Datum>()) as *mut Datum;
    narrayelems = 0;

    /* For each blocked proc in the lock group ... */
    i = 0;
    while i < (*lockData).nprocs {
        let bproc: *mut BlockedProcData = &mut (*(*lockData).procs.offset(i as isize));
        let instances: *mut LockInstanceData =
            &mut (*(*lockData).locks.offset((*bproc).first_lock as isize));
        let preceding_waiters: *mut c_int =
            &mut (*(*lockData).waiter_pids.offset((*bproc).first_waiter as isize));
        let mut blocked_instance: *mut LockInstanceData;
        let lockMethodTable: LockMethod;
        let conflictMask: c_int;

        /*
         * Locate the blocked proc's own entry in the LockInstanceData array.
         * There should be exactly one matching entry.
         */
        blocked_instance = std::ptr::null_mut();
        j = 0;
        while j < (*bproc).num_locks {
            let instance: *mut LockInstanceData = &mut (*instances.offset(j as isize));

            if (*instance).pid == (*bproc).pid {
                Assert(blocked_instance.is_null());
                blocked_instance = instance;
            }
            j += 1;
        }
        Assert(!blocked_instance.is_null());

        lockMethodTable = GetLockTagsMethodTable(&(*blocked_instance).locktag);
        conflictMask =
            *(*lockMethodTable).conflictTab.add((*blocked_instance).waitLockMode as usize) as c_int;

        /* Now scan the PROCLOCK data for conflicting procs */
        j = 0;
        while j < (*bproc).num_locks {
            let instance: *mut LockInstanceData = &mut (*instances.offset(j as isize));

            /* A proc never blocks itself, so ignore that entry */
            if instance == blocked_instance {
                j += 1;
                continue;
            }
            /* Members of same lock group never block each other, either */
            if (*instance).leaderPid == (*blocked_instance).leaderPid {
                j += 1;
                continue;
            }

            if conflictMask & (*instance).holdMask as c_int != 0 {
                /* hard block: blocked by lock already held by this entry */
            } else if (*instance).waitLockMode != NoLock
                && (conflictMask & LOCKBIT_ON((*instance).waitLockMode) as c_int != 0)
            {
                /* conflict in lock requests; who's in front in wait queue? */
                let mut ahead: bool = false;
                let mut k: c_int;

                k = 0;
                while k < (*bproc).num_waiters {
                    if *preceding_waiters.offset(k as isize) == (*instance).pid {
                        /* soft block: this entry is ahead of blocked proc */
                        ahead = true;
                        break;
                    }
                    k += 1;
                }
                if !ahead {
                    j += 1;
                    continue; /* not blocked by this entry */
                }
            } else {
                /* not blocked by this entry */
                j += 1;
                continue;
            }

            /* blocked by this entry, so emit a record */
            *arrayelems.offset(narrayelems as isize) = Int32GetDatum((*instance).leaderPid);
            narrayelems += 1;
            j += 1;
        }
        i += 1;
    }

    /* Assert we didn't overrun arrayelems[] */
    Assert(narrayelems <= (*lockData).nlocks);

    PG_RETURN_ARRAYTYPE_P(construct_array_builtin(arrayelems, narrayelems, INT4OID))
}

/*
 * pg_safe_snapshot_blocking_pids - produce an array of the PIDs blocking
 * given PID from getting a safe snapshot
 *
 * XXX this does not consider parallel-query cases; not clear how big a
 * problem that is in practice
 */
#[no_mangle]
pub unsafe extern "C" fn pg_safe_snapshot_blocking_pids(fcinfo: FunctionCallInfo) -> Datum {
    let blocked_pid: c_int = PG_GETARG_INT32(0);
    let blockers: *mut c_int;
    let num_blockers: c_int;
    let blocker_datums: *mut Datum;

    /* A buffer big enough for any possible blocker list without truncation */
    blockers = palloc(MaxBackends as usize * std::mem::size_of::<c_int>()) as *mut c_int;

    /* Collect a snapshot of processes waited for by GetSafeSnapshot */
    num_blockers = GetSafeSnapshotBlockingPids(blocked_pid, blockers, MaxBackends);

    /* Convert int array to Datum array */
    if num_blockers > 0 {
        let mut i: c_int;

        blocker_datums = palloc(num_blockers as usize * std::mem::size_of::<Datum>()) as *mut Datum;
        i = 0;
        while i < num_blockers {
            *blocker_datums.offset(i as isize) = Int32GetDatum(*blockers.offset(i as isize));
            i += 1;
        }
    } else {
        blocker_datums = std::ptr::null_mut();
    }

    PG_RETURN_ARRAYTYPE_P(construct_array_builtin(blocker_datums, num_blockers, INT4OID))
}

/*
 * Functions for manipulating advisory locks
 *
 * We make use of the locktag fields as follows:
 *
 *	field1: MyDatabaseId ... ensures locks are local to each database
 *	field2: first of 2 int4 keys, or high-order half of an int8 key
 *	field3: second of 2 int4 keys, or low-order half of an int8 key
 *	field4: 1 if using an int8 key, 2 if using 2 int4 keys
 */
unsafe fn SET_LOCKTAG_INT64(tag: &mut LOCKTAG, key64: int64) {
    SET_LOCKTAG_ADVISORY(
        tag,
        MyDatabaseId,
        (key64 >> 32) as uint32,
        key64 as uint32,
        1,
    )
}

unsafe fn SET_LOCKTAG_INT32(tag: &mut LOCKTAG, key1: uint32, key2: uint32) {
    SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, key1, key2, 2)
}

/*
 * pg_advisory_lock(int8) - acquire exclusive lock on an int8 key
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_lock_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT64(&mut tag, key);

    LockAcquire(&mut tag, ExclusiveLock, true, false);

    PG_RETURN_VOID()
}

/*
 * pg_advisory_xact_lock(int8) - acquire xact scoped
 * exclusive lock on an int8 key
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_xact_lock_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT64(&mut tag, key);

    LockAcquire(&mut tag, ExclusiveLock, false, false);

    PG_RETURN_VOID()
}

/*
 * pg_advisory_lock_shared(int8) - acquire share lock on an int8 key
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_lock_shared_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT64(&mut tag, key);

    LockAcquire(&mut tag, ShareLock, true, false);

    PG_RETURN_VOID()
}

/*
 * pg_advisory_xact_lock_shared(int8) - acquire xact scoped
 * share lock on an int8 key
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_xact_lock_shared_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT64(&mut tag, key);

    LockAcquire(&mut tag, ShareLock, false, false);

    PG_RETURN_VOID()
}

/*
 * pg_try_advisory_lock(int8) - acquire exclusive lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_lock_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT64(&mut tag, key);

    res = LockAcquire(&mut tag, ExclusiveLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_try_advisory_xact_lock(int8) - acquire xact scoped
 * exclusive lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_xact_lock_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT64(&mut tag, key);

    res = LockAcquire(&mut tag, ExclusiveLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_try_advisory_lock_shared(int8) - acquire share lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_lock_shared_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT64(&mut tag, key);

    res = LockAcquire(&mut tag, ShareLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_try_advisory_xact_lock_shared(int8) - acquire xact scoped
 * share lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_xact_lock_shared_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT64(&mut tag, key);

    res = LockAcquire(&mut tag, ShareLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_advisory_unlock(int8) - release exclusive lock on an int8 key
 *
 * Returns true if successful, false if lock was not held
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_unlock_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: bool;

    SET_LOCKTAG_INT64(&mut tag, key);

    res = LockRelease(&mut tag, ExclusiveLock, true);

    PG_RETURN_BOOL(res)
}

/*
 * pg_advisory_unlock_shared(int8) - release share lock on an int8 key
 *
 * Returns true if successful, false if lock was not held
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_unlock_shared_int8(fcinfo: FunctionCallInfo) -> Datum {
    let key: int64 = PG_GETARG_INT64(0);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: bool;

    SET_LOCKTAG_INT64(&mut tag, key);

    res = LockRelease(&mut tag, ShareLock, true);

    PG_RETURN_BOOL(res)
}

/*
 * pg_advisory_lock(int4, int4) - acquire exclusive lock on 2 int4 keys
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_lock_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    LockAcquire(&mut tag, ExclusiveLock, true, false);

    PG_RETURN_VOID()
}

/*
 * pg_advisory_xact_lock(int4, int4) - acquire xact scoped
 * exclusive lock on 2 int4 keys
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_xact_lock_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    LockAcquire(&mut tag, ExclusiveLock, false, false);

    PG_RETURN_VOID()
}

/*
 * pg_advisory_lock_shared(int4, int4) - acquire share lock on 2 int4 keys
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_lock_shared_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    LockAcquire(&mut tag, ShareLock, true, false);

    PG_RETURN_VOID()
}

/*
 * pg_advisory_xact_lock_shared(int4, int4) - acquire xact scoped
 * share lock on 2 int4 keys
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_xact_lock_shared_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    LockAcquire(&mut tag, ShareLock, false, false);

    PG_RETURN_VOID()
}

/*
 * pg_try_advisory_lock(int4, int4) - acquire exclusive lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_lock_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    res = LockAcquire(&mut tag, ExclusiveLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_try_advisory_xact_lock(int4, int4) - acquire xact scoped
 * exclusive lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_xact_lock_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    res = LockAcquire(&mut tag, ExclusiveLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_try_advisory_lock_shared(int4, int4) - acquire share lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_lock_shared_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    res = LockAcquire(&mut tag, ShareLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_try_advisory_xact_lock_shared(int4, int4) - acquire xact scoped
 * share lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
#[no_mangle]
pub unsafe extern "C" fn pg_try_advisory_xact_lock_shared_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: LockAcquireResult;

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    res = LockAcquire(&mut tag, ShareLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL)
}

/*
 * pg_advisory_unlock(int4, int4) - release exclusive lock on 2 int4 keys
 *
 * Returns true if successful, false if lock was not held
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_unlock_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: bool;

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    res = LockRelease(&mut tag, ExclusiveLock, true);

    PG_RETURN_BOOL(res)
}

/*
 * pg_advisory_unlock_shared(int4, int4) - release share lock on 2 int4 keys
 *
 * Returns true if successful, false if lock was not held
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_unlock_shared_int4(fcinfo: FunctionCallInfo) -> Datum {
    let key1: int32 = PG_GETARG_INT32(0);
    let key2: int32 = PG_GETARG_INT32(1);
    let mut tag: LOCKTAG = std::mem::zeroed();
    let res: bool;

    SET_LOCKTAG_INT32(&mut tag, key1 as uint32, key2 as uint32);

    res = LockRelease(&mut tag, ShareLock, true);

    PG_RETURN_BOOL(res)
}

/*
 * pg_advisory_unlock_all() - release all advisory locks
 */
#[no_mangle]
pub unsafe extern "C" fn pg_advisory_unlock_all(fcinfo: FunctionCallInfo) -> Datum {
    LockReleaseSession(USER_LOCKMETHOD);

    PG_RETURN_VOID()
}

/* ---------------------------------------------------------------------------
 * Local stubs for unported helper functions and types.
 * --------------------------------------------------------------------------- */

// types from various unported modules
pub type LOCKMODE = c_int;
pub type ProcNumber = c_int;
pub type LocalTransactionId = u32;
pub type LockTagType = c_int;
pub type PredicateLockTargetType = c_int;
pub type LockAcquireResult = c_int;
pub type TimestampTz = int64;

#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut std::ffi::c_void,
    pub attinmeta: *mut std::ffi::c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

pub type FunctionCallInfo = *mut std::ffi::c_void;
pub type MemoryContext = *mut std::ffi::c_void;
pub type TupleDesc = *mut std::ffi::c_void;
pub type HeapTuple = *mut std::ffi::c_void;

#[repr(C)]
pub struct LOCKTAG {
    pub locktag_field1: u32,
    pub locktag_field2: u32,
    pub locktag_field3: u32,
    pub locktag_field4: u16,
    pub locktag_type: u8,
    pub locktag_lockmethodid: u8,
}

#[repr(C)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

#[repr(C)]
pub struct LockInstanceData {
    pub locktag: LOCKTAG,
    pub holdMask: u32,
    pub waitLockMode: LOCKMODE,
    pub vxid: VirtualTransactionId,
    pub pid: c_int,
    pub leaderPid: c_int,
    pub fastpath: bool,
    pub waitStart: TimestampTz,
}

#[repr(C)]
pub struct LockData {
    pub nelements: c_int,
    pub locks: *mut LockInstanceData,
}

#[repr(C)]
pub struct BlockedProcData {
    pub pid: c_int,
    pub first_lock: c_int,
    pub num_locks: c_int,
    pub first_waiter: c_int,
    pub num_waiters: c_int,
}

#[repr(C)]
pub struct BlockedProcsData {
    pub procs: *mut BlockedProcData,
    pub nprocs: c_int,
    pub locks: *mut LockInstanceData,
    pub nlocks: c_int,
    pub waiter_pids: *mut c_int,
    pub nwaiter_pids: c_int,
}

#[repr(C)]
pub struct LockMethodData {
    pub conflictTab: *const u32,
}
pub type LockMethod = *const LockMethodData;

#[repr(C)]
pub struct SERIALIZABLEXACT {
    pub vxid: VirtualTransactionId,
    pub pid: c_int,
}

#[repr(C)]
pub struct PredicateLockData {
    pub nelements: c_int,
    pub locktags: *mut PREDICATELOCKTARGETTAG,
    pub xacts: *mut SERIALIZABLEXACT,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PREDICATELOCKTARGETTAG {
    pub locktag_field1: u32,
    pub locktag_field2: u32,
    pub locktag_field3: u32,
    pub locktag_field4: u32,
    pub locktag_field5: u32,
}

// enum LockTagType
pub const LOCKTAG_RELATION: LockTagType = 0;
pub const LOCKTAG_RELATION_EXTEND: LockTagType = 1;
pub const LOCKTAG_DATABASE_FROZEN_IDS: LockTagType = 2;
pub const LOCKTAG_PAGE: LockTagType = 3;
pub const LOCKTAG_TUPLE: LockTagType = 4;
pub const LOCKTAG_TRANSACTION: LockTagType = 5;
pub const LOCKTAG_VIRTUALTRANSACTION: LockTagType = 6;
pub const LOCKTAG_SPECULATIVE_TOKEN: LockTagType = 7;
pub const LOCKTAG_OBJECT: LockTagType = 8;
pub const LOCKTAG_USERLOCK: LockTagType = 9;
pub const LOCKTAG_ADVISORY: LockTagType = 10;
pub const LOCKTAG_APPLY_TRANSACTION: LockTagType = 11;
pub const LOCKTAG_LAST_TYPE: LockTagType = LOCKTAG_APPLY_TRANSACTION;

// enum PredicateLockTargetType
pub const PREDLOCKTAG_RELATION: PredicateLockTargetType = 0;
pub const PREDLOCKTAG_PAGE: PredicateLockTargetType = 1;
pub const PREDLOCKTAG_TUPLE: PredicateLockTargetType = 2;

pub const NoLock: LOCKMODE = 0;
pub const ShareLock: LOCKMODE = 5;
pub const ExclusiveLock: LOCKMODE = 7;
pub const MAX_LOCKMODES: LOCKMODE = 10;

pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;

pub const USER_LOCKMETHOD: u16 = 2;

unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!() // TODO: funcapi.h
}
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!() // TODO: funcapi.h
}
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!() // TODO: funcapi.h
}
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO: funcapi.h
}
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO: funcapi.h
}

unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO: access/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attno: AttrNumber,
    _attname: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) {
    unimplemented!() // TODO: access/tupdesc.c
}
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn heap_form_tuple(_desc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO: funcapi.h
}

unsafe fn GetLockStatusData() -> *mut LockData {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn GetPredicateLockStatusData() -> *mut PredicateLockData {
    unimplemented!() // TODO: storage/lmgr/predicate.c
}
unsafe fn GetBlockerStatusData(_blocked_pid: c_int) -> *mut BlockedProcsData {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn GetLockTagsMethodTable(_locktag: *const LOCKTAG) -> LockMethod {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn GetLockmodeName(_lockmethodid: u8, _mode: LOCKMODE) -> *const c_char {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn GetSafeSnapshotBlockingPids(
    _blocked_pid: c_int,
    _output: *mut c_int,
    _output_size: c_int,
) -> c_int {
    unimplemented!() // TODO: storage/lmgr/predicate.c
}

unsafe fn LockAcquire(
    _locktag: *mut LOCKTAG,
    _lockmode: LOCKMODE,
    _sessionLock: bool,
    _dontWait: bool,
) -> LockAcquireResult {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn LockRelease(_locktag: *mut LOCKTAG, _lockmode: LOCKMODE, _sessionLock: bool) -> bool {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn LockReleaseSession(_lockmethodid: u16) {
    unimplemented!() // TODO: storage/lmgr/lock.c
}
unsafe fn SET_LOCKTAG_ADVISORY(
    _tag: &mut LOCKTAG,
    _id1: Oid,
    _id2: uint32,
    _id3: uint32,
    _id4: u16,
) {
    unimplemented!() // TODO: storage/lmgr/lock.h
}

unsafe fn LOCKBIT_ON(_lockmode: LOCKMODE) -> u32 {
    unimplemented!() // TODO: storage/lock.h
}
unsafe fn LOCKBIT_OFF(_lockmode: LOCKMODE) -> u32 {
    unimplemented!() // TODO: storage/lock.h
}

unsafe fn GET_PREDICATELOCKTARGETTAG_TYPE(_tag: PREDICATELOCKTARGETTAG) -> PredicateLockTargetType {
    unimplemented!() // TODO: storage/predicate_internals.h
}
unsafe fn GET_PREDICATELOCKTARGETTAG_DB(_tag: PREDICATELOCKTARGETTAG) -> Datum {
    unimplemented!() // TODO: storage/predicate_internals.h
}
unsafe fn GET_PREDICATELOCKTARGETTAG_RELATION(_tag: PREDICATELOCKTARGETTAG) -> Datum {
    unimplemented!() // TODO: storage/predicate_internals.h
}
unsafe fn GET_PREDICATELOCKTARGETTAG_PAGE(_tag: PREDICATELOCKTARGETTAG) -> Datum {
    unimplemented!() // TODO: storage/predicate_internals.h
}
unsafe fn GET_PREDICATELOCKTARGETTAG_OFFSET(_tag: PREDICATELOCKTARGETTAG) -> Datum {
    unimplemented!() // TODO: storage/predicate_internals.h
}

unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: utils/adt/arrayfuncs.c
}

unsafe fn PG_GETARG_INT32(_n: c_int) -> i32 {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_GETARG_INT64(_n: c_int) -> int64 {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_VOID() -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_BOOL(_b: bool) -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_ARRAYTYPE_P(_x: *mut std::ffi::c_void) -> Datum {
    unimplemented!() // TODO: array.h
}

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: builtins.h
}
unsafe fn ObjectIdGetDatum(_oid: u32) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn UInt32GetDatum(_x: u32) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn UInt16GetDatum(_x: u16) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int16GetDatum(_x: i16) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int32GetDatum(_x: i32) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn TransactionIdGetDatum(_x: u32) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn BoolGetDatum(_x: bool) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn TimestampTzGetDatum(_x: TimestampTz) -> Datum {
    unimplemented!() // TODO: timestamp.h
}

unsafe fn Assert(_cond: bool) {}

unsafe fn palloc(_size: usize) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn MemoryContextSwitchTo(_context: MemoryContext) -> MemoryContext {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}

// OID constants (from pg_type.h / builtins) - stubs
pub const TEXTOID: Oid = 25;
pub const OIDOID: Oid = 26;
pub const INT4OID: Oid = 23;
pub const INT2OID: Oid = 21;
pub const XIDOID: Oid = 28;
pub const BOOLOID: Oid = 16;
pub const TIMESTAMPTZOID: Oid = 1184;

//! lmgr.rs
//!   POSTGRES lock manager code
//!
//! Translated 1:1 from postgres/src/backend/storage/lmgr/lmgr.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/lmgr/lmgr.c

#![allow(unused_variables)]
#![allow(dead_code)]

// The prelude glob (crate::c::*, crate::postgres_ext::*, core::ffi::*) already
// provides: c_char, c_int, c_void, Oid, InvalidOid, OidIsValid, TransactionId,
// int64, uint8, uint16, uint32, null_mut, Assert!, elog!/ereport!.
use crate::prelude::*;

use std::ptr;

// nodes/pg_list.h -- List / ListCell and helpers (ported).
use crate::nodes::pg_list::{lappend, lfirst, list_free, list_free_deep, List, ListCell, NIL};
use crate::{current_cell, foreach, list_make1};

// lib/stringinfo.h -- appendStringInfo! macro.
use crate::appendStringInfo;

// storage/lockdefs.h -- LOCKMODE and the standard lock-mode constants.
use crate::storage::lockdefs::{ExclusiveLock, ShareLock, LOCKMODE};

// utils/rel.h -- Relation, LockRelId and relation accessors.
use crate::utils::rel::{
    LockRelId, Relation, RelationGetRelationName, RelationGetRelid, RelationIsValid,
};

// storage/block.h / storage/off.h -- BlockNumber, OffsetNumber.
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;

// storage/itemptr.h -- ItemPointer and accessors.
use crate::storage::itemptr::{
    ItemPointer, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
};

// access/transam.h -- TransactionId helpers (the type itself comes from the
// prelude glob via crate::c).
use crate::access::transam::{TransactionIdEquals, TransactionIdIsValid};

// catalog/catalog.h -- IsSharedRelation.
use crate::catalog::catalog::IsSharedRelation;

// lib/stringinfo.h -- StringInfo / StringInfoData (for DescribeLockTag).
type StringInfoData = crate::lib::stringinfo::StringInfoData; // lib/stringinfo.h
type StringInfo = *mut StringInfoData; // lib/stringinfo.h

// miscadmin.h -- CHECK_FOR_INTERRUPTS (stubbed per-file, as in sibling files).
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{ /* TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS() */ }};
}
use CHECK_FOR_INTERRUPTS;

// === Stub types, constants and helpers for unported dependencies ===
//
// storage/lock.h, storage/proc.h, storage/procarray.h, access/xact.h,
// access/subtrans.h, utils/inval.h, miscadmin.h and pgstat.h are not yet
// ported.  The types, constants and routines they provide are stubbed
// locally with just what this translation unit references, following the
// conventions in the sibling deadlock.rs.  TODO(pg-port): replace with the
// real definitions once those files land.

// LockTagType enum values (storage/lock.h).
type LockTagType = c_int;
const LOCKTAG_RELATION: LockTagType = 0;
const LOCKTAG_RELATION_EXTEND: LockTagType = 1;
const LOCKTAG_DATABASE_FROZEN_IDS: LockTagType = 2;
const LOCKTAG_PAGE: LockTagType = 3;
const LOCKTAG_TUPLE: LockTagType = 4;
const LOCKTAG_TRANSACTION: LockTagType = 5;
const LOCKTAG_VIRTUALTRANSACTION: LockTagType = 6;
const LOCKTAG_SPECULATIVE_TOKEN: LockTagType = 7;
const LOCKTAG_OBJECT: LockTagType = 8;
const LOCKTAG_USERLOCK: LockTagType = 9;
const LOCKTAG_ADVISORY: LockTagType = 10;
const LOCKTAG_APPLY_TRANSACTION: LockTagType = 11;
const LOCKTAG_LAST_TYPE: uint16 = LOCKTAG_APPLY_TRANSACTION as uint16;

// LOCKTAG (storage/lock.h).  Full four-field locktag layout.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LOCKTAGData {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}
type LOCKTAG = LOCKTAGData; // storage/lock.h

// LOCALLOCK (storage/lock.h) -- only used opaquely here.
#[repr(C)]
pub struct LOCALLOCK {
    _private: [u8; 0],
}

// LockAcquireResult (storage/lock.h).
type LockAcquireResult = c_int;
const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
const LOCKACQUIRE_OK: LockAcquireResult = 1;
const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

// XLTW_Oper (storage/lmgr.h) -- operations for transaction lock waits.
type XLTW_Oper = c_int;
const XLTW_None: XLTW_Oper = 0;
const XLTW_Update: XLTW_Oper = 1;
const XLTW_Delete: XLTW_Oper = 2;
const XLTW_Lock: XLTW_Oper = 3;
const XLTW_LockUpdated: XLTW_Oper = 4;
const XLTW_InsertIndex: XLTW_Oper = 5;
const XLTW_InsertIndexUnique: XLTW_Oper = 6;
const XLTW_FetchUpdated: XLTW_Oper = 7;
const XLTW_RecheckExclusionConstr: XLTW_Oper = 8;

// storage/lock.h -- VirtualTransactionId / ProcNumber.
type ProcNumber = c_int; // storage/procnumber.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: uint32,
}

// storage/proc.h -- PGPROC (only .pid referenced here).
#[repr(C)]
pub struct PGPROC {
    pub pid: c_int,
}

// utils/elog.h -- ErrorContextCallback.
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub arg: *mut c_void,
}

// commands/progress.h -- progress reporting parameter indexes.
const PROGRESS_WAITFOR_TOTAL: c_int = 0; // TODO(pg-port): real value lives in commands/progress.h
const PROGRESS_WAITFOR_DONE: c_int = 1; // TODO(pg-port): real value lives in commands/progress.h
const PROGRESS_WAITFOR_CURRENT_PID: c_int = 2; // TODO(pg-port): real value lives in commands/progress.h

// miscadmin.h -- MyDatabaseId and error_context_stack.
static mut MyDatabaseId: Oid = InvalidOid; // TODO(pg-port): real MyDatabaseId lives in catalog/pg_database / miscinit.c
static mut error_context_stack: *mut ErrorContextCallback = ptr::null_mut(); // TODO(pg-port): real error_context_stack lives in utils/error/elog.c

// === SET_LOCKTAG_* macros (storage/lock.h). ===
// These all write into the LOCKTAG fields, hence callers pass `&raw mut tag`.

unsafe fn SET_LOCKTAG_RELATION(tag: *mut LOCKTAG, dboid: Oid, reloid: Oid) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_RELATION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_RELATION_EXTEND(tag: *mut LOCKTAG, dboid: Oid, reloid: Oid) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_RELATION_EXTEND as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_DATABASE_FROZEN_IDS(tag: *mut LOCKTAG, dboid: Oid) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = 0;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_DATABASE_FROZEN_IDS as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_PAGE(tag: *mut LOCKTAG, dboid: Oid, reloid: Oid, blocknum: BlockNumber) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = blocknum;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_PAGE as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_TUPLE(
    tag: *mut LOCKTAG,
    dboid: Oid,
    reloid: Oid,
    blocknum: BlockNumber,
    offnum: OffsetNumber,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = blocknum;
    (*tag).locktag_field4 = offnum;
    (*tag).locktag_type = LOCKTAG_TUPLE as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_TRANSACTION(tag: *mut LOCKTAG, xid: TransactionId) {
    (*tag).locktag_field1 = xid;
    (*tag).locktag_field2 = 0;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_TRANSACTION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_SPECULATIVE_INSERTION(tag: *mut LOCKTAG, xid: TransactionId, token: uint32) {
    (*tag).locktag_field1 = xid;
    (*tag).locktag_field2 = token;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_SPECULATIVE_TOKEN as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_OBJECT(
    tag: *mut LOCKTAG,
    dboid: Oid,
    classoid: Oid,
    objoid: Oid,
    objsubid: uint16,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = classoid;
    (*tag).locktag_field3 = objoid;
    (*tag).locktag_field4 = objsubid;
    (*tag).locktag_type = LOCKTAG_OBJECT as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

unsafe fn SET_LOCKTAG_APPLY_TRANSACTION(
    tag: *mut LOCKTAG,
    dboid: Oid,
    suboid: Oid,
    xid: uint32,
    objid: uint16,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = suboid;
    (*tag).locktag_field3 = xid;
    (*tag).locktag_field4 = objid;
    (*tag).locktag_type = LOCKTAG_APPLY_TRANSACTION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

const DEFAULT_LOCKMETHOD: c_int = 1; // storage/lock.h

// LockTagTypeNames -- parallel to LockTagType (storage/lock.c).
const LockTagTypeNames: [*const c_char; 12] = [
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

// === storage/lock.c core routines (heavy; stubbed locally). ===
// TODO(pg-port): real LockAcquire/LockRelease/etc. live in storage/lmgr/lock.c.

unsafe fn LockAcquire(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
    dontWait: bool,
) -> LockAcquireResult {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn LockAcquireExtended(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
    dontWait: bool,
    reportMemoryError: bool,
    locallockp: *mut *mut LOCALLOCK,
    logLockFailure: bool,
) -> LockAcquireResult {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn LockRelease(locktag: *const LOCKTAG, lockmode: LOCKMODE, sessionLock: bool) -> bool {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn LockHeldByMe(locktag: *const LOCKTAG, lockmode: LOCKMODE, orstronger: bool) -> bool {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn LockHasWaiters(locktag: *const LOCKTAG, lockmode: LOCKMODE, sessionLock: bool) -> bool {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn LockWaiterCount(locktag: *const LOCKTAG) -> c_int {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn MarkLockClear(locallock: *mut LOCALLOCK) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn GetLockConflicts(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    countp: *mut c_int,
) -> *mut VirtualTransactionId {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn VirtualXactLock(vxid: VirtualTransactionId, wait: bool) -> bool {
    unimplemented!() // TODO(pg-port): storage/lmgr/lock.c
}

unsafe fn VirtualTransactionIdIsValid(vxid: VirtualTransactionId) -> bool {
    vxid.localTransactionId != 0 // storage/lock.h
}

// access/subtrans.h -- subtransaction parent walk.
unsafe fn SubTransGetTopmostTransaction(xid: TransactionId) -> TransactionId {
    unimplemented!() // TODO(pg-port): access/transam/subtrans.c
}

// access/xact.h -- top transaction id (if any).
unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    unimplemented!() // TODO(pg-port): access/transam/xact.c
}

// storage/procarray.h -- in-progress check.
unsafe fn TransactionIdIsInProgress(xid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): storage/ipc/procarray.c
}

// storage/procnumber.h -- map ProcNumber -> PGPROC.
unsafe fn ProcNumberGetProc(procNumber: ProcNumber) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): storage/lmgr/proc.c
}

// utils/inval.h -- absorb invalidation messages.
unsafe fn AcceptInvalidationMessages() {
    unimplemented!() // TODO(pg-port): utils/cache/inval.c
}

// port -- pg_usleep (port/pgsleep.c).
unsafe fn pg_usleep(microsec: i64) {
    unimplemented!() // TODO(pg-port): port/pgsleep.c
}

// pgstat.h -- progress reporting (stubbed).
unsafe fn pgstat_progress_update_param(index: c_int, val: int64) {
    unimplemented!() // TODO(pg-port): utils/activity/backend_progress.c
}
unsafe fn pgstat_progress_update_multi_param(
    nparam: c_int,
    index: *const c_int,
    val: *const int64,
) {
    unimplemented!() // TODO(pg-port): utils/activity/backend_progress.c
}

/*
 * Per-backend counter for generating speculative insertion tokens.
 *
 * This may wrap around, but that's OK as it's only used for the short
 * duration between inserting a tuple and checking that there are no (unique)
 * constraint violations.  It's theoretically possible that a backend sees a
 * tuple that was speculatively inserted by another backend, but before it has
 * started waiting on the token, the other backend completes its insertion,
 * and then performs 2^32 unrelated insertions.  And after all that, the
 * first backend finally calls SpeculativeInsertionLockAcquire(), with the
 * intention of waiting for the first insertion to complete, but ends up
 * waiting for the latest unrelated insertion instead.  Even then, nothing
 * particularly bad happens: in the worst case they deadlock, causing one of
 * the transactions to abort.
 */
static mut speculativeInsertionToken: uint32 = 0;

/*
 * Struct to hold context info for transaction lock waits.
 *
 * 'oper' is the operation that needs to wait for the other transaction; 'rel'
 * and 'ctid' specify the address of the tuple being waited for.
 */
#[repr(C)]
pub struct XactLockTableWaitInfo {
    pub oper: XLTW_Oper,
    pub rel: Relation,
    pub ctid: ItemPointer,
}

/*
 * RelationInitLockInfo
 *		Initializes the lock information in a relation descriptor.
 *
 *		relcache.c must call this during creation of any reldesc.
 */
pub unsafe fn RelationInitLockInfo(relation: Relation) {
    Assert!(RelationIsValid(relation));
    Assert!(OidIsValid(RelationGetRelid(relation)));

    (*relation).rd_lockInfo.lockRelId.relId = RelationGetRelid(relation);

    if (*(*relation).rd_rel).relisshared {
        (*relation).rd_lockInfo.lockRelId.dbId = InvalidOid;
    } else {
        (*relation).rd_lockInfo.lockRelId.dbId = MyDatabaseId;
    }
}

/*
 * SetLocktagRelationOid
 *		Set up a locktag for a relation, given only relation OID
 */
unsafe fn SetLocktagRelationOid(tag: *mut LOCKTAG, relid: Oid) {
    let dbid: Oid;

    if IsSharedRelation(relid) {
        dbid = InvalidOid;
    } else {
        dbid = MyDatabaseId;
    }

    SET_LOCKTAG_RELATION(tag, dbid, relid);
}

/*
 *		LockRelationOid
 *
 * Lock a relation given only its OID.  This should generally be used
 * before attempting to open the relation's relcache entry.
 */
pub unsafe fn LockRelationOid(relid: Oid, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SetLocktagRelationOid(&raw mut tag, relid);

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        false,
        true,
        &raw mut locallock,
        false,
    );

    /*
     * Now that we have the lock, check for invalidation messages, so that we
     * will update or flush any stale relcache entry before we try to use it.
     * RangeVarGetRelid() specifically relies on us for this.  We can skip
     * this in the not-uncommon case that we already had the same type of lock
     * being requested, since then no one else could have modified the
     * relcache entry in an undesirable way.  (In the case where our own xact
     * modifies the rel, the relcache update happens via
     * CommandCounterIncrement, not here.)
     *
     * However, in corner cases where code acts on tables (usually catalogs)
     * recursively, we might get here while still processing invalidation
     * messages in some outer execution of this function or a sibling.  The
     * "cleared" status of the lock tells us whether we really are done
     * absorbing relevant inval messages.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }
}

/*
 *		ConditionalLockRelationOid
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true iff the lock was acquired.
 *
 * NOTE: we do not currently need conditional versions of all the
 * LockXXX routines in this file, but they could easily be added if needed.
 */
pub unsafe fn ConditionalLockRelationOid(relid: Oid, lockmode: LOCKMODE) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SetLocktagRelationOid(&raw mut tag, relid);

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        true,
        true,
        &raw mut locallock,
        false,
    );

    if res == LOCKACQUIRE_NOT_AVAIL {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }

    true
}

/*
 *		LockRelationId
 *
 * Lock, given a LockRelId.  Same as LockRelationOid but take LockRelId as an
 * input.
 */
pub unsafe fn LockRelationId(relid: *mut LockRelId, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SET_LOCKTAG_RELATION(&raw mut tag, (*relid).dbId, (*relid).relId);

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        false,
        true,
        &raw mut locallock,
        false,
    );

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }
}

/*
 *		UnlockRelationId
 *
 * Unlock, given a LockRelId.  This is preferred over UnlockRelationOid
 * for speed reasons.
 */
pub unsafe fn UnlockRelationId(relid: *mut LockRelId, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION(&raw mut tag, (*relid).dbId, (*relid).relId);

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		UnlockRelationOid
 *
 * Unlock, given only a relation Oid.  Use UnlockRelationId if you can.
 */
pub unsafe fn UnlockRelationOid(relid: Oid, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SetLocktagRelationOid(&raw mut tag, relid);

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		LockRelation
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
pub unsafe fn LockRelation(relation: Relation, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SET_LOCKTAG_RELATION(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        false,
        true,
        &raw mut locallock,
        false,
    );

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }
}

/*
 *		ConditionalLockRelation
 *
 * This is a convenience routine for acquiring an additional lock on an
 * already-open relation.  Never try to do "relation_open(foo, NoLock)"
 * and then lock with this.
 */
pub unsafe fn ConditionalLockRelation(relation: Relation, lockmode: LOCKMODE) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SET_LOCKTAG_RELATION(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        true,
        true,
        &raw mut locallock,
        false,
    );

    if res == LOCKACQUIRE_NOT_AVAIL {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }

    true
}

/*
 *		UnlockRelation
 *
 * This is a convenience routine for unlocking a relation without also
 * closing it.
 */
pub unsafe fn UnlockRelation(relation: Relation, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		CheckRelationLockedByMe
 *
 * Returns true if current transaction holds a lock on 'relation' of mode
 * 'lockmode'.  If 'orstronger' is true, a stronger lockmode is also OK.
 * ("Stronger" is defined as "numerically higher", which is a bit
 * semantically dubious but is OK for the purposes we use this for.)
 */
pub unsafe fn CheckRelationLockedByMe(
    relation: Relation,
    lockmode: LOCKMODE,
    orstronger: bool,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockHeldByMe(&raw const tag, lockmode, orstronger)
}

/*
 *		CheckRelationOidLockedByMe
 *
 * Like the above, but takes an OID as argument.
 */
pub unsafe fn CheckRelationOidLockedByMe(
    relid: Oid,
    lockmode: LOCKMODE,
    orstronger: bool,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SetLocktagRelationOid(&raw mut tag, relid);

    LockHeldByMe(&raw const tag, lockmode, orstronger)
}

/*
 *		LockHasWaitersRelation
 *
 * This is a function to check whether someone else is waiting for a
 * lock which we are currently holding.
 */
pub unsafe fn LockHasWaitersRelation(relation: Relation, lockmode: LOCKMODE) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockHasWaiters(&raw const tag, lockmode, false)
}

/*
 *		LockRelationIdForSession
 *
 * This routine grabs a session-level lock on the target relation.  The
 * session lock persists across transaction boundaries.  It will be removed
 * when UnlockRelationIdForSession() is called, or if an ereport(ERROR) occurs,
 * or if the backend exits.
 *
 * Note that one should also grab a transaction-level lock on the rel
 * in any transaction that actually uses the rel, to ensure that the
 * relcache entry is up to date.
 */
pub unsafe fn LockRelationIdForSession(relid: *mut LockRelId, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION(&raw mut tag, (*relid).dbId, (*relid).relId);

    LockAcquire(&raw const tag, lockmode, true, false);
}

/*
 *		UnlockRelationIdForSession
 */
pub unsafe fn UnlockRelationIdForSession(relid: *mut LockRelId, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION(&raw mut tag, (*relid).dbId, (*relid).relId);

    LockRelease(&raw const tag, lockmode, true);
}

/*
 *		LockRelationForExtension
 *
 * This lock tag is used to interlock addition of pages to relations.
 * We need such locking because bufmgr/smgr definition of P_NEW is not
 * race-condition-proof.
 *
 * We assume the caller is already holding some type of regular lock on
 * the relation, so no AcceptInvalidationMessages call is needed here.
 */
pub unsafe fn LockRelationForExtension(relation: Relation, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION_EXTEND(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockAcquire(&raw const tag, lockmode, false, false);
}

/*
 *		ConditionalLockRelationForExtension
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true iff the lock was acquired.
 */
pub unsafe fn ConditionalLockRelationForExtension(relation: Relation, lockmode: LOCKMODE) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION_EXTEND(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockAcquire(&raw const tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL
}

/*
 *		RelationExtensionLockWaiterCount
 *
 * Count the number of processes waiting for the given relation extension lock.
 */
pub unsafe fn RelationExtensionLockWaiterCount(relation: Relation) -> c_int {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION_EXTEND(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockWaiterCount(&raw const tag)
}

/*
 *		UnlockRelationForExtension
 */
pub unsafe fn UnlockRelationForExtension(relation: Relation, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_RELATION_EXTEND(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
    );

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		LockDatabaseFrozenIds
 *
 * This allows one backend per database to execute vac_update_datfrozenxid().
 */
pub unsafe fn LockDatabaseFrozenIds(lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_DATABASE_FROZEN_IDS(&raw mut tag, MyDatabaseId);

    LockAcquire(&raw const tag, lockmode, false, false);
}

/*
 *		LockPage
 *
 * Obtain a page-level lock.  This is currently used by some index access
 * methods to lock individual index pages.
 */
pub unsafe fn LockPage(relation: Relation, blkno: BlockNumber, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_PAGE(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
        blkno,
    );

    LockAcquire(&raw const tag, lockmode, false, false);
}

/*
 *		ConditionalLockPage
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true iff the lock was acquired.
 */
pub unsafe fn ConditionalLockPage(
    relation: Relation,
    blkno: BlockNumber,
    lockmode: LOCKMODE,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_PAGE(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
        blkno,
    );

    LockAcquire(&raw const tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL
}

/*
 *		UnlockPage
 */
pub unsafe fn UnlockPage(relation: Relation, blkno: BlockNumber, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_PAGE(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
        blkno,
    );

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		LockTuple
 *
 * Obtain a tuple-level lock.  This is used in a less-than-intuitive fashion
 * because we can't afford to keep a separate lock in shared memory for every
 * tuple.  See heap_lock_tuple before using this!
 */
pub unsafe fn LockTuple(relation: Relation, tid: ItemPointer, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_TUPLE(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
        ItemPointerGetBlockNumber(tid),
        ItemPointerGetOffsetNumber(tid),
    );

    LockAcquire(&raw const tag, lockmode, false, false);
}

/*
 *		ConditionalLockTuple
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true iff the lock was acquired.
 */
pub unsafe fn ConditionalLockTuple(
    relation: Relation,
    tid: ItemPointer,
    lockmode: LOCKMODE,
    logLockFailure: bool,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_TUPLE(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
        ItemPointerGetBlockNumber(tid),
        ItemPointerGetOffsetNumber(tid),
    );

    LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        true,
        true,
        ptr::null_mut(),
        logLockFailure,
    ) != LOCKACQUIRE_NOT_AVAIL
}

/*
 *		UnlockTuple
 */
pub unsafe fn UnlockTuple(relation: Relation, tid: ItemPointer, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_TUPLE(
        &raw mut tag,
        (*relation).rd_lockInfo.lockRelId.dbId,
        (*relation).rd_lockInfo.lockRelId.relId,
        ItemPointerGetBlockNumber(tid),
        ItemPointerGetOffsetNumber(tid),
    );

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		XactLockTableInsert
 *
 * Insert a lock showing that the given transaction ID is running ---
 * this is done when an XID is acquired by a transaction or subtransaction.
 * The lock can then be used to wait for the transaction to finish.
 */
pub unsafe fn XactLockTableInsert(xid: TransactionId) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_TRANSACTION(&raw mut tag, xid);

    LockAcquire(&raw const tag, ExclusiveLock, false, false);
}

/*
 *		XactLockTableDelete
 *
 * Delete the lock showing that the given transaction ID is running.
 * (This is never used for main transaction IDs; those locks are only
 * released implicitly at transaction end.  But we do use it for subtrans IDs.)
 */
pub unsafe fn XactLockTableDelete(xid: TransactionId) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_TRANSACTION(&raw mut tag, xid);

    LockRelease(&raw const tag, ExclusiveLock, false);
}

/*
 *		XactLockTableWait
 *
 * Wait for the specified transaction to commit or abort.  If an operation
 * is specified, an error context callback is set up.  If 'oper' is passed as
 * None, no error context callback is set up.
 *
 * Note that this does the right thing for subtransactions: if we wait on a
 * subtransaction, we will exit as soon as it aborts or its top parent commits.
 * It takes some extra work to ensure this, because to save on shared memory
 * the XID lock of a subtransaction is released when it ends, whether
 * successfully or unsuccessfully.  So we have to check if it's "still running"
 * and if so wait for its parent.
 */
pub unsafe fn XactLockTableWait(
    mut xid: TransactionId,
    rel: Relation,
    ctid: ItemPointer,
    oper: XLTW_Oper,
) {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut info: XactLockTableWaitInfo = std::mem::zeroed();
    let mut callback: ErrorContextCallback = std::mem::zeroed();
    let mut first: bool = true;

    /*
     * If an operation is specified, set up our verbose error context
     * callback.
     */
    if oper != XLTW_None {
        Assert!(RelationIsValid(rel));
        Assert!(ItemPointerIsValid(ctid));

        info.rel = rel;
        info.ctid = ctid;
        info.oper = oper;

        callback.callback = Some(XactLockTableWaitErrorCb);
        callback.arg = &raw mut info as *mut c_void;
        callback.previous = error_context_stack;
        error_context_stack = &raw mut callback;
    }

    loop {
        Assert!(TransactionIdIsValid(xid));
        Assert!(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

        SET_LOCKTAG_TRANSACTION(&raw mut tag, xid);

        LockAcquire(&raw const tag, ShareLock, false, false);

        LockRelease(&raw const tag, ShareLock, false);

        if !TransactionIdIsInProgress(xid) {
            break;
        }

        /*
         * If the Xid belonged to a subtransaction, then the lock would have
         * gone away as soon as it was finished; for correct tuple visibility,
         * the right action is to wait on its parent transaction to go away.
         * But instead of going levels up one by one, we can just wait for the
         * topmost transaction to finish with the same end result, which also
         * incurs less locktable traffic.
         *
         * Some uses of this function don't involve tuple visibility -- such
         * as when building snapshots for logical decoding.  It is possible to
         * see a transaction in ProcArray before it registers itself in the
         * locktable.  The topmost transaction in that case is the same xid,
         * so we try again after a short sleep.  (Don't sleep the first time
         * through, to avoid slowing down the normal case.)
         */
        if !first {
            CHECK_FOR_INTERRUPTS!();
            pg_usleep(1000);
        }
        first = false;
        xid = SubTransGetTopmostTransaction(xid);
    }

    if oper != XLTW_None {
        error_context_stack = callback.previous;
    }
}

/*
 *		ConditionalXactLockTableWait
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true if the lock was acquired.
 */
pub unsafe fn ConditionalXactLockTableWait(
    mut xid: TransactionId,
    logLockFailure: bool,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut first: bool = true;

    loop {
        Assert!(TransactionIdIsValid(xid));
        Assert!(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

        SET_LOCKTAG_TRANSACTION(&raw mut tag, xid);

        if LockAcquireExtended(
            &raw const tag,
            ShareLock,
            false,
            true,
            true,
            ptr::null_mut(),
            logLockFailure,
        ) == LOCKACQUIRE_NOT_AVAIL
        {
            return false;
        }

        LockRelease(&raw const tag, ShareLock, false);

        if !TransactionIdIsInProgress(xid) {
            break;
        }

        /* See XactLockTableWait about this case */
        if !first {
            CHECK_FOR_INTERRUPTS!();
            pg_usleep(1000);
        }
        first = false;
        xid = SubTransGetTopmostTransaction(xid);
    }

    true
}

/*
 *		SpeculativeInsertionLockAcquire
 *
 * Insert a lock showing that the given transaction ID is inserting a tuple,
 * but hasn't yet decided whether it's going to keep it.  The lock can then be
 * used to wait for the decision to go ahead with the insertion, or aborting
 * it.
 *
 * The token is used to distinguish multiple insertions by the same
 * transaction.  It is returned to caller.
 */
pub unsafe fn SpeculativeInsertionLockAcquire(xid: TransactionId) -> uint32 {
    let mut tag: LOCKTAG = std::mem::zeroed();

    speculativeInsertionToken += 1;

    /*
     * Check for wrap-around. Zero means no token is held, so don't use that.
     */
    if speculativeInsertionToken == 0 {
        speculativeInsertionToken = 1;
    }

    SET_LOCKTAG_SPECULATIVE_INSERTION(&raw mut tag, xid, speculativeInsertionToken);

    LockAcquire(&raw const tag, ExclusiveLock, false, false);

    speculativeInsertionToken
}

/*
 *		SpeculativeInsertionLockRelease
 *
 * Delete the lock showing that the given transaction is speculatively
 * inserting a tuple.
 */
pub unsafe fn SpeculativeInsertionLockRelease(xid: TransactionId) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_SPECULATIVE_INSERTION(&raw mut tag, xid, speculativeInsertionToken);

    LockRelease(&raw const tag, ExclusiveLock, false);
}

/*
 *		SpeculativeInsertionWait
 *
 * Wait for the specified transaction to finish or abort the insertion of a
 * tuple.
 */
pub unsafe fn SpeculativeInsertionWait(xid: TransactionId, token: uint32) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_SPECULATIVE_INSERTION(&raw mut tag, xid, token);

    Assert!(TransactionIdIsValid(xid));
    Assert!(token != 0);

    LockAcquire(&raw const tag, ShareLock, false, false);
    LockRelease(&raw const tag, ShareLock, false);
}

/*
 * XactLockTableWaitErrorCb
 *		Error context callback for transaction lock waits.
 */
unsafe extern "C" fn XactLockTableWaitErrorCb(arg: *mut c_void) {
    let info: *mut XactLockTableWaitInfo = arg as *mut XactLockTableWaitInfo;

    /*
     * We would like to print schema name too, but that would require a
     * syscache lookup.
     */
    if (*info).oper != XLTW_None
        && ItemPointerIsValid((*info).ctid)
        && RelationIsValid((*info).rel)
    {
        let cxt: *const c_char;

        match (*info).oper {
            XLTW_Update => {
                cxt = gettext_noop(c"while updating tuple (%u,%u) in relation \"%s\"".as_ptr());
            }
            XLTW_Delete => {
                cxt = gettext_noop(c"while deleting tuple (%u,%u) in relation \"%s\"".as_ptr());
            }
            XLTW_Lock => {
                cxt = gettext_noop(c"while locking tuple (%u,%u) in relation \"%s\"".as_ptr());
            }
            XLTW_LockUpdated => {
                cxt = gettext_noop(
                    c"while locking updated version (%u,%u) of tuple in relation \"%s\"".as_ptr(),
                );
            }
            XLTW_InsertIndex => {
                cxt =
                    gettext_noop(c"while inserting index tuple (%u,%u) in relation \"%s\"".as_ptr());
            }
            XLTW_InsertIndexUnique => {
                cxt = gettext_noop(
                    c"while checking uniqueness of tuple (%u,%u) in relation \"%s\"".as_ptr(),
                );
            }
            XLTW_FetchUpdated => {
                cxt = gettext_noop(
                    c"while rechecking updated tuple (%u,%u) in relation \"%s\"".as_ptr(),
                );
            }
            XLTW_RecheckExclusionConstr => {
                cxt = gettext_noop(
                    c"while checking exclusion constraint on tuple (%u,%u) in relation \"%s\""
                        .as_ptr(),
                );
            }

            _ => {
                return;
            }
        }

        errcontext(
            cxt,
            ItemPointerGetBlockNumber((*info).ctid),
            ItemPointerGetOffsetNumber((*info).ctid),
            RelationGetRelationName((*info).rel),
        );
    }
}

/*
 * WaitForLockersMultiple
 *		Wait until no transaction holds locks that conflict with the given
 *		locktags at the given lockmode.
 *
 * To do this, obtain the current list of lockers, and wait on their VXIDs
 * until they are finished.
 *
 * Note we don't try to acquire the locks on the given locktags, only the
 * VXIDs and XIDs of their lock holders; if somebody grabs a conflicting lock
 * on the objects after we obtained our initial list of lockers, we will not
 * wait for them.
 */
pub unsafe fn WaitForLockersMultiple(locktags: *mut List, lockmode: LOCKMODE, progress: bool) {
    let mut holders: *mut List = NIL;
    // `lc` is introduced by the foreach! macro below (ForEachState cursor).
    let mut total: c_int = 0;
    let mut done: c_int = 0;

    /* Done if no locks to wait for */
    if locktags == NIL {
        return;
    }

    /* Collect the transactions we need to wait on */
    foreach!(lc, locktags, {
        let locktag: *mut LOCKTAG = lfirst(current_cell!(lc)) as *mut LOCKTAG;
        let mut count: c_int = 0;

        holders = lappend(
            holders,
            GetLockConflicts(
                locktag,
                lockmode,
                if progress { &raw mut count } else { ptr::null_mut() },
            ) as *mut c_void,
        );
        if progress {
            total += count;
        }
    });

    if progress {
        pgstat_progress_update_param(PROGRESS_WAITFOR_TOTAL, total as int64);
    }

    /*
     * Note: GetLockConflicts() never reports our own xid, hence we need not
     * check for that.  Also, prepared xacts are reported and awaited.
     */

    /* Finally wait for each such transaction to complete */
    foreach!(lc, holders, {
        let mut lockholders: *mut VirtualTransactionId =
            lfirst(current_cell!(lc)) as *mut VirtualTransactionId;

        while VirtualTransactionIdIsValid(*lockholders) {
            /* If requested, publish who we're going to wait for. */
            if progress {
                let holder: *mut PGPROC = ProcNumberGetProc((*lockholders).procNumber);

                if !holder.is_null() {
                    pgstat_progress_update_param(
                        PROGRESS_WAITFOR_CURRENT_PID,
                        (*holder).pid as int64,
                    );
                }
            }
            VirtualXactLock(*lockholders, true);
            lockholders = lockholders.add(1);

            if progress {
                done += 1;
                pgstat_progress_update_param(PROGRESS_WAITFOR_DONE, done as int64);
            }
        }
    });
    if progress {
        let index: [c_int; 3] = [
            PROGRESS_WAITFOR_TOTAL,
            PROGRESS_WAITFOR_DONE,
            PROGRESS_WAITFOR_CURRENT_PID,
        ];
        let values: [int64; 3] = [0, 0, 0];

        pgstat_progress_update_multi_param(3, index.as_ptr(), values.as_ptr());
    }

    list_free_deep(holders);
}

/*
 * WaitForLockers
 *
 * Same as WaitForLockersMultiple, for a single lock tag.
 */
pub unsafe fn WaitForLockers(mut heaplocktag: LOCKTAG, lockmode: LOCKMODE, progress: bool) {
    let l: *mut List;

    l = list_make1!(&raw mut heaplocktag as *mut c_void);
    WaitForLockersMultiple(l, lockmode, progress);
    list_free(l);
}

/*
 *		LockDatabaseObject
 *
 * Obtain a lock on a general object of the current database.  Don't use
 * this for shared objects (such as tablespaces).  It's unwise to apply it
 * to relations, also, since a lock taken this way will NOT conflict with
 * locks taken via LockRelation and friends.
 */
pub unsafe fn LockDatabaseObject(classid: Oid, objid: Oid, objsubid: uint16, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_OBJECT(&raw mut tag, MyDatabaseId, classid, objid, objsubid);

    LockAcquire(&raw const tag, lockmode, false, false);

    /* Make sure syscaches are up-to-date with any changes we waited for */
    AcceptInvalidationMessages();
}

/*
 *		ConditionalLockDatabaseObject
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true iff the lock was acquired.
 */
pub unsafe fn ConditionalLockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: uint16,
    lockmode: LOCKMODE,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SET_LOCKTAG_OBJECT(&raw mut tag, MyDatabaseId, classid, objid, objsubid);

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        true,
        true,
        &raw mut locallock,
        false,
    );

    if res == LOCKACQUIRE_NOT_AVAIL {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }

    true
}

/*
 *		UnlockDatabaseObject
 */
pub unsafe fn UnlockDatabaseObject(classid: Oid, objid: Oid, objsubid: uint16, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_OBJECT(&raw mut tag, MyDatabaseId, classid, objid, objsubid);

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		LockSharedObject
 *
 * Obtain a lock on a shared-across-databases object.
 */
pub unsafe fn LockSharedObject(classid: Oid, objid: Oid, objsubid: uint16, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_OBJECT(&raw mut tag, InvalidOid, classid, objid, objsubid);

    LockAcquire(&raw const tag, lockmode, false, false);

    /* Make sure syscaches are up-to-date with any changes we waited for */
    AcceptInvalidationMessages();
}

/*
 *		ConditionalLockSharedObject
 *
 * As above, but only lock if we can get the lock without blocking.
 * Returns true iff the lock was acquired.
 */
pub unsafe fn ConditionalLockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: uint16,
    lockmode: LOCKMODE,
) -> bool {
    let mut tag: LOCKTAG = std::mem::zeroed();
    let mut locallock: *mut LOCALLOCK = ptr::null_mut();
    let res: LockAcquireResult;

    SET_LOCKTAG_OBJECT(&raw mut tag, InvalidOid, classid, objid, objsubid);

    res = LockAcquireExtended(
        &raw const tag,
        lockmode,
        false,
        true,
        true,
        &raw mut locallock,
        false,
    );

    if res == LOCKACQUIRE_NOT_AVAIL {
        return false;
    }

    /*
     * Now that we have the lock, check for invalidation messages; see notes
     * in LockRelationOid.
     */
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }

    true
}

/*
 *		UnlockSharedObject
 */
pub unsafe fn UnlockSharedObject(classid: Oid, objid: Oid, objsubid: uint16, lockmode: LOCKMODE) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_OBJECT(&raw mut tag, InvalidOid, classid, objid, objsubid);

    LockRelease(&raw const tag, lockmode, false);
}

/*
 *		LockSharedObjectForSession
 *
 * Obtain a session-level lock on a shared-across-databases object.
 * See LockRelationIdForSession for notes about session-level locks.
 */
pub unsafe fn LockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: uint16,
    lockmode: LOCKMODE,
) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_OBJECT(&raw mut tag, InvalidOid, classid, objid, objsubid);

    LockAcquire(&raw const tag, lockmode, true, false);
}

/*
 *		UnlockSharedObjectForSession
 */
pub unsafe fn UnlockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: uint16,
    lockmode: LOCKMODE,
) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_OBJECT(&raw mut tag, InvalidOid, classid, objid, objsubid);

    LockRelease(&raw const tag, lockmode, true);
}

/*
 *		LockApplyTransactionForSession
 *
 * Obtain a session-level lock on a transaction being applied on a logical
 * replication subscriber. See LockRelationIdForSession for notes about
 * session-level locks.
 */
pub unsafe fn LockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: uint16,
    lockmode: LOCKMODE,
) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_APPLY_TRANSACTION(&raw mut tag, MyDatabaseId, suboid, xid, objid);

    LockAcquire(&raw const tag, lockmode, true, false);
}

/*
 *		UnlockApplyTransactionForSession
 */
pub unsafe fn UnlockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: uint16,
    lockmode: LOCKMODE,
) {
    let mut tag: LOCKTAG = std::mem::zeroed();

    SET_LOCKTAG_APPLY_TRANSACTION(&raw mut tag, MyDatabaseId, suboid, xid, objid);

    LockRelease(&raw const tag, lockmode, true);
}

/*
 * Append a description of a lockable object to buf.
 *
 * Ideally we would print names for the numeric values, but that requires
 * getting locks on system tables, which might cause problems since this is
 * typically used to report deadlock situations.
 */
pub unsafe fn DescribeLockTag(buf: StringInfo, tag: *const LOCKTAG) {
    // NOTE: C wraps each format string in _() for translation; the port's
    // appendStringInfo! takes a Rust format string, so the %u/%d conversions
    // become {} placeholders and the gettext wrapper is dropped.
    match (*tag).locktag_type as LockTagType {
        LOCKTAG_RELATION => {
            appendStringInfo!(
                buf,
                "relation {} of database {}",
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        LOCKTAG_RELATION_EXTEND => {
            appendStringInfo!(
                buf,
                "extension of relation {} of database {}",
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        LOCKTAG_DATABASE_FROZEN_IDS => {
            appendStringInfo!(
                buf,
                "pg_database.datfrozenxid of database {}",
                (*tag).locktag_field1
            );
        }
        LOCKTAG_PAGE => {
            appendStringInfo!(
                buf,
                "page {} of relation {} of database {}",
                (*tag).locktag_field3,
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        LOCKTAG_TUPLE => {
            appendStringInfo!(
                buf,
                "tuple ({},{}) of relation {} of database {}",
                (*tag).locktag_field3,
                (*tag).locktag_field4,
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        LOCKTAG_TRANSACTION => {
            appendStringInfo!(buf, "transaction {}", (*tag).locktag_field1);
        }
        LOCKTAG_VIRTUALTRANSACTION => {
            appendStringInfo!(
                buf,
                "virtual transaction {}/{}",
                (*tag).locktag_field1,
                (*tag).locktag_field2
            );
        }
        LOCKTAG_SPECULATIVE_TOKEN => {
            appendStringInfo!(
                buf,
                "speculative token {} of transaction {}",
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        LOCKTAG_OBJECT => {
            appendStringInfo!(
                buf,
                "object {} of class {} of database {}",
                (*tag).locktag_field3,
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        LOCKTAG_USERLOCK => {
            /* reserved for old contrib code, now on pgfoundry */
            appendStringInfo!(
                buf,
                "user lock [{},{},{}]",
                (*tag).locktag_field1,
                (*tag).locktag_field2,
                (*tag).locktag_field3
            );
        }
        LOCKTAG_ADVISORY => {
            appendStringInfo!(
                buf,
                "advisory lock [{},{},{},{}]",
                (*tag).locktag_field1,
                (*tag).locktag_field2,
                (*tag).locktag_field3,
                (*tag).locktag_field4
            );
        }
        LOCKTAG_APPLY_TRANSACTION => {
            appendStringInfo!(
                buf,
                "remote transaction {} of subscription {} of database {}",
                (*tag).locktag_field3,
                (*tag).locktag_field2,
                (*tag).locktag_field1
            );
        }
        _ => {
            appendStringInfo!(
                buf,
                "unrecognized locktag type {}",
                (*tag).locktag_type as c_int
            );
        }
    }
}

/*
 * GetLockNameFromTagType
 *
 *	Given locktag type, return the corresponding lock name.
 */
pub unsafe fn GetLockNameFromTagType(locktag_type: uint16) -> *const c_char {
    if locktag_type > LOCKTAG_LAST_TYPE {
        return c"???".as_ptr();
    }
    LockTagTypeNames[locktag_type as usize]
}

// === Local helper stubs for symbols not yet provided elsewhere. ===
// (OidIsValid is provided by the prelude glob via crate::c.)

// utils/elog.h -- errcontext (variadic).  Stubbed as a no-op marker.
unsafe fn errcontext(fmt: *const c_char, blk: BlockNumber, off: OffsetNumber, relname: *mut c_char) {
    unimplemented!() // TODO(pg-port): real errcontext lives in utils/error/elog.c
}

// c.h / utils/elog.h -- gettext_noop just returns its argument.
unsafe fn gettext_noop(x: *const c_char) -> *const c_char {
    x
}

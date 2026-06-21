//! multixact.rs
//!   PostgreSQL multi-transaction-log manager
//! Translated 1:1 from postgres/src/backend/access/transam/multixact.c
//!
//! The pg_multixact manager is a pg_xact-like manager that stores an array of
//! MultiXactMember for each MultiXactId.  It is a fundamental part of the
//! shared-row-lock implementation.  Each MultiXactMember is comprised of a
//! TransactionId and a set of flag bits.  The name is a bit historical:
//! originally, a MultiXactId consisted of more than one TransactionId (except
//! in rare corner cases), hence "multi".  Nowadays, however, it's perfectly
//! legitimate to have MultiXactIds that only include a single Xid.
//!
//! The meaning of the flag bits is opaque to this module, but they are mostly
//! used in heapam.c to identify lock modes that each of the member transactions
//! is holding on any given tuple.  This module just contains support to store
//! and retrieve the arrays.
//!
//! We use two SLRU areas, one for storing the offsets at which the data
//! starts for each MultiXactId in the other one.  This trick allows us to
//! store variable length arrays of TransactionIds.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/transam/multixact.c

use crate::prelude::*;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::pg_config::BLCKSZ;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int32, int64, uint8, uint16, uint32, uint64, Size, TransactionId, MultiXactId, MultiXactOffset};

// ----------------------------------------------------------------------------
// multixact.h
//
// src/include/access/multixact.h
// ----------------------------------------------------------------------------

/*
 * The first two MultiXactId values are reserved to store the truncation Xid
 * and epoch of the first segment, so we start assigning multixact values from
 * 2.
 */
pub const InvalidMultiXactId: MultiXactId = 0;
pub const FirstMultiXactId: MultiXactId = 1;
pub const MaxMultiXactId: MultiXactId = 0xFFFFFFFF;

#[inline]
fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

pub const MaxMultiXactOffset: MultiXactOffset = 0xFFFFFFFF;

/*
 * Possible multixact lock modes ("status").  The first four modes are for
 * tuple locks (FOR KEY SHARE, FOR SHARE, FOR NO KEY UPDATE, FOR UPDATE); the
 * next two are used for update and delete modes.
 */
pub type MultiXactStatus = c_int;

pub const MultiXactStatusForKeyShare: MultiXactStatus = 0x00;
pub const MultiXactStatusForShare: MultiXactStatus = 0x01;
pub const MultiXactStatusForNoKeyUpdate: MultiXactStatus = 0x02;
pub const MultiXactStatusForUpdate: MultiXactStatus = 0x03;
/* an update that doesn't touch "key" columns */
pub const MultiXactStatusNoKeyUpdate: MultiXactStatus = 0x04;
/* other updates, and delete */
pub const MultiXactStatusUpdate: MultiXactStatus = 0x05;

pub const MaxMultiXactStatus: MultiXactStatus = MultiXactStatusUpdate;

/* does a status value correspond to a tuple update? */
#[inline]
fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    status > MultiXactStatusForUpdate
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: MultiXactStatus,
}

/* ----------------
 *		multixact-related XLOG entries
 * ----------------
 */

pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: uint8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: uint8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: uint8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: uint8 = 0x30;

#[repr(C)]
pub struct xl_multixact_create {
    pub mid: MultiXactId,       /* new MultiXact's ID */
    pub moff: MultiXactOffset,  /* its starting offset in members file */
    pub nmembers: int32,        /* number of member XIDs */
    pub members: [MultiXactMember; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* offsetof(xl_multixact_create, members) */
pub const SizeOfMultiXactCreate: usize = core::mem::offset_of!(xl_multixact_create, members);

#[repr(C)]
pub struct xl_multixact_truncate {
    pub oldestMultiDB: Oid,

    /* to-be-truncated range of multixact offsets */
    pub startTruncOff: MultiXactId, /* just for completeness' sake */
    pub endTruncOff: MultiXactId,

    /* to-be-truncated range of multixact members */
    pub startTruncMemb: MultiXactOffset,
    pub endTruncMemb: MultiXactOffset,
}

pub const SizeOfMultiXactTruncate: usize = core::mem::size_of::<xl_multixact_truncate>();

// ----------------------------------------------------------------------------
// multixact.c
// ----------------------------------------------------------------------------

/*
 * Defines for MultiXactOffset page sizes.  A page is the same BLCKSZ as is
 * used everywhere else in Postgres.
 *
 * Note: because MultiXactOffsets are 32 bits and wrap around at 0xFFFFFFFF,
 * MultiXact page numbering also wraps around at
 * 0xFFFFFFFF/MULTIXACT_OFFSETS_PER_PAGE, and segment numbering at
 * 0xFFFFFFFF/MULTIXACT_OFFSETS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need
 * take no explicit notice of that fact in this module, except when comparing
 * segment and page numbers in TruncateMultiXact (see
 * MultiXactOffsetPagePrecedes).
 */

/* We need four bytes per offset */
const MULTIXACT_OFFSETS_PER_PAGE: c_int =
    BLCKSZ as c_int / core::mem::size_of::<MultiXactOffset>() as c_int;

#[inline]
fn MultiXactIdToOffsetPage(multi: MultiXactId) -> int64 {
    multi as int64 / MULTIXACT_OFFSETS_PER_PAGE as int64
}

#[inline]
fn MultiXactIdToOffsetEntry(multi: MultiXactId) -> c_int {
    (multi % MULTIXACT_OFFSETS_PER_PAGE as MultiXactId) as c_int
}

#[inline]
fn MultiXactIdToOffsetSegment(multi: MultiXactId) -> int64 {
    MultiXactIdToOffsetPage(multi) / SLRU_PAGES_PER_SEGMENT as int64
}

/*
 * The situation for members is a bit more complex: we store one byte of
 * additional flag bits for each TransactionId.  To do this without getting
 * into alignment issues, we store four bytes of flags, and then the
 * corresponding 4 Xids.  Each such 5-word (20-byte) set we call a "group", and
 * are stored as a whole in pages.  Thus, with 8kB BLCKSZ, we keep 409 groups
 * per page.  This wastes 12 bytes per page, but that's OK -- simplicity (and
 * performance) trumps space efficiency here.
 *
 * Note that the "offset" macros work with byte offset, not array indexes, so
 * arithmetic must be done using "char *" pointers.
 */
/* We need eight bits per xact, so one xact fits in a byte */
const MXACT_MEMBER_BITS_PER_XACT: c_int = 8;
const MXACT_MEMBER_FLAGS_PER_BYTE: c_int = 1;
const MXACT_MEMBER_XACT_BITMASK: c_int = (1 << MXACT_MEMBER_BITS_PER_XACT) - 1;

/* how many full bytes of flags are there in a group? */
const MULTIXACT_FLAGBYTES_PER_GROUP: c_int = 4;
const MULTIXACT_MEMBERS_PER_MEMBERGROUP: c_int =
    MULTIXACT_FLAGBYTES_PER_GROUP * MXACT_MEMBER_FLAGS_PER_BYTE;
/* size in bytes of a complete group */
const MULTIXACT_MEMBERGROUP_SIZE: c_int = core::mem::size_of::<TransactionId>() as c_int
    * MULTIXACT_MEMBERS_PER_MEMBERGROUP
    + MULTIXACT_FLAGBYTES_PER_GROUP;
const MULTIXACT_MEMBERGROUPS_PER_PAGE: c_int = BLCKSZ as c_int / MULTIXACT_MEMBERGROUP_SIZE;
const MULTIXACT_MEMBERS_PER_PAGE: c_int =
    MULTIXACT_MEMBERGROUPS_PER_PAGE * MULTIXACT_MEMBERS_PER_MEMBERGROUP;

/*
 * Because the number of items per page is not a divisor of the last item
 * number (member 0xFFFFFFFF), the last segment does not use the maximum number
 * of pages, and moreover the last used page therein does not use the same
 * number of items as previous pages.  (Another way to say it is that the
 * 0xFFFFFFFF member is somewhere in the middle of the last page, so the page
 * has some empty space after that item.)
 *
 * This constant is the number of members in the last page of the last segment.
 */
const MAX_MEMBERS_IN_LAST_MEMBERS_PAGE: uint32 =
    (0xFFFFFFFFu32 % MULTIXACT_MEMBERS_PER_PAGE as u32) + 1;

/* page in which a member is to be found */
#[inline]
fn MXOffsetToMemberPage(offset: MultiXactOffset) -> int64 {
    offset as int64 / MULTIXACT_MEMBERS_PER_PAGE as int64
}

#[inline]
fn MXOffsetToMemberSegment(offset: MultiXactOffset) -> int64 {
    MXOffsetToMemberPage(offset) / SLRU_PAGES_PER_SEGMENT as int64
}

/* Location (byte offset within page) of flag word for a given member */
#[inline]
fn MXOffsetToFlagsOffset(offset: MultiXactOffset) -> c_int {
    let group: MultiXactOffset = offset / MULTIXACT_MEMBERS_PER_MEMBERGROUP as MultiXactOffset;
    let grouponpg: c_int = (group % MULTIXACT_MEMBERGROUPS_PER_PAGE as MultiXactOffset) as c_int;
    let byteoff: c_int = grouponpg * MULTIXACT_MEMBERGROUP_SIZE;

    byteoff
}

#[inline]
fn MXOffsetToFlagsBitShift(offset: MultiXactOffset) -> c_int {
    let member_in_group: c_int =
        (offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP as MultiXactOffset) as c_int;
    let bshift: c_int = member_in_group * MXACT_MEMBER_BITS_PER_XACT;

    bshift
}

/* Location (byte offset within page) of TransactionId of given member */
#[inline]
fn MXOffsetToMemberOffset(offset: MultiXactOffset) -> c_int {
    let member_in_group: c_int =
        (offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP as MultiXactOffset) as c_int;

    MXOffsetToFlagsOffset(offset)
        + MULTIXACT_FLAGBYTES_PER_GROUP
        + member_in_group * core::mem::size_of::<TransactionId>() as c_int
}

/* Multixact members wraparound thresholds. */
const MULTIXACT_MEMBER_SAFE_THRESHOLD: MultiXactOffset = MaxMultiXactOffset / 2;
const MULTIXACT_MEMBER_DANGER_THRESHOLD: MultiXactOffset =
    MaxMultiXactOffset - MaxMultiXactOffset / 4;

#[inline]
fn PreviousMultiXactId(multi: MultiXactId) -> MultiXactId {
    if multi == FirstMultiXactId {
        MaxMultiXactId
    } else {
        multi - 1
    }
}

/*
 * Links to shared-memory data structures for MultiXact control
 */
static mut MultiXactOffsetCtlData: SlruCtlData = unsafe { core::mem::zeroed() };
static mut MultiXactMemberCtlData: SlruCtlData = unsafe { core::mem::zeroed() };

#[inline]
fn MultiXactOffsetCtl() -> SlruCtl {
    core::ptr::addr_of_mut!(MultiXactOffsetCtlData)
}

#[inline]
fn MultiXactMemberCtl() -> SlruCtl {
    core::ptr::addr_of_mut!(MultiXactMemberCtlData)
}

/*
 * MultiXact state shared across all backends.  All this state is protected
 * by MultiXactGenLock.  (We also use SLRU bank's lock of MultiXactOffset and
 * MultiXactMember to guard accesses to the two sets of SLRU buffers.  For
 * concurrency's sake, we avoid holding more than one of these locks at a
 * time.)
 */
#[repr(C)]
pub struct MultiXactStateData {
    /* next-to-be-assigned MultiXactId */
    pub nextMXact: MultiXactId,

    /* next-to-be-assigned offset */
    pub nextOffset: MultiXactOffset,

    /* Have we completed multixact startup? */
    pub finishedStartup: bool,

    /*
     * Oldest multixact that is still potentially referenced by a relation.
     * Anything older than this should not be consulted.  These values are
     * updated by vacuum.
     */
    pub oldestMultiXactId: MultiXactId,
    pub oldestMultiXactDB: Oid,

    /*
     * Oldest multixact offset that is potentially referenced by a multixact
     * referenced by a relation.  We don't always know this value, so there's
     * a flag here to indicate whether or not we currently do.
     */
    pub oldestOffset: MultiXactOffset,
    pub oldestOffsetKnown: bool,

    /* support for anti-wraparound measures */
    pub multiVacLimit: MultiXactId,
    pub multiWarnLimit: MultiXactId,
    pub multiStopLimit: MultiXactId,
    pub multiWrapLimit: MultiXactId,

    /* support for members anti-wraparound measures */
    pub offsetStopLimit: MultiXactOffset, /* known if oldestOffsetKnown */

    /*
     * Per-backend data starts here.  We have two arrays stored in the area
     * immediately following the MultiXactStateData struct:
     *
     * OldestMemberMXactId[k] is the oldest MultiXactId each backend's current
     * transaction(s) could possibly be a member of [...].
     *
     * OldestVisibleMXactId[k] is the oldest MultiXactId each backend's
     * current transaction(s) think is potentially live [...].
     */
    pub perBackendXactIds: [MultiXactId; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * Sizes of OldestMemberMXactId and OldestVisibleMXactId arrays.
 */
#[inline]
unsafe fn NumMemberSlots() -> c_int {
    MaxBackends + max_prepared_xacts
}
#[inline]
unsafe fn NumVisibleSlots() -> c_int {
    MaxBackends
}

/* Pointers to the state data in shared memory */
static mut MultiXactState: *mut MultiXactStateData = core::ptr::null_mut();
static mut OldestMemberMXactId: *mut MultiXactId = core::ptr::null_mut();
static mut OldestVisibleMXactId: *mut MultiXactId = core::ptr::null_mut();

#[inline]
unsafe fn MyOldestMemberMXactIdSlot() -> *mut MultiXactId {
    /*
     * The first MaxBackends entries in the OldestMemberMXactId array are
     * reserved for regular backends.  MyProcNumber should index into one of
     * them.
     */
    Assert!(MyProcNumber >= 0 && MyProcNumber < MaxBackends);
    OldestMemberMXactId.add(MyProcNumber as usize)
}

#[inline]
unsafe fn PreparedXactOldestMemberMXactIdSlot(procno: ProcNumber) -> *mut MultiXactId {
    let prepared_xact_idx: c_int;

    Assert!(procno >= FIRST_PREPARED_XACT_PROC_NUMBER);
    prepared_xact_idx = procno - FIRST_PREPARED_XACT_PROC_NUMBER;

    /*
     * The first MaxBackends entries in the OldestMemberMXactId array are
     * reserved for regular backends.  Prepared xacts come after them.
     */
    Assert!(MaxBackends + prepared_xact_idx < NumMemberSlots());
    OldestMemberMXactId.add((MaxBackends + prepared_xact_idx) as usize)
}

#[inline]
unsafe fn MyOldestVisibleMXactIdSlot() -> *mut MultiXactId {
    Assert!(MyProcNumber >= 0 && MyProcNumber < NumVisibleSlots());
    OldestVisibleMXactId.add(MyProcNumber as usize)
}

/*
 * Definitions for the backend-local MultiXactId cache.
 */
#[repr(C)]
pub struct mXactCacheEnt {
    pub multi: MultiXactId,
    pub nmembers: c_int,
    pub node: dlist_node,
    pub members: [MultiXactMember; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

const MAX_CACHE_ENTRIES: c_int = 256;
static mut MXactCache: dclist_head = DCLIST_STATIC_INIT();
static mut MXactContext: MemoryContext = core::ptr::null_mut();

// MULTIXACT_DEBUG is not defined; debug_elogN expand to no-ops.
macro_rules! debug_elog2 { ($a:expr, $b:expr) => {}; }
macro_rules! debug_elog3 { ($a:expr, $b:expr, $c:expr) => {}; }
macro_rules! debug_elog4 { ($a:expr, $b:expr, $c:expr, $d:expr) => {}; }
macro_rules! debug_elog5 { ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr) => {}; }
macro_rules! debug_elog6 { ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr) => {}; }

/* hack to deal with WAL generated with older minor versions */
static mut pre_initialized_offsets_page: int64 = -1;

/*
 * MultiXactIdCreate
 *		Construct a MultiXactId representing two TransactionIds.
 *
 * The two XIDs must be different, or be requesting different statuses.
 *
 * NB - we don't worry about our local MultiXactId cache here, because that
 * is handled by the lower-level routines.
 */
pub unsafe fn MultiXactIdCreate(
    xid1: TransactionId,
    status1: MultiXactStatus,
    xid2: TransactionId,
    status2: MultiXactStatus,
) -> MultiXactId {
    let newMulti: MultiXactId;
    let mut members: [MultiXactMember; 2] = core::mem::zeroed();

    Assert!(TransactionIdIsValid(xid1));
    Assert!(TransactionIdIsValid(xid2));

    Assert!(!TransactionIdEquals(xid1, xid2) || (status1 != status2));

    /* MultiXactIdSetOldestMember() must have been called already. */
    Assert!(MultiXactIdIsValid(*MyOldestMemberMXactIdSlot()));

    /*
     * Note: unlike MultiXactIdExpand, we don't bother to check that both XIDs
     * are still running.  In typical usage, xid2 will be our own XID and the
     * caller just did a check on xid1, so it'd be wasted effort.
     */

    members[0].xid = xid1;
    members[0].status = status1;
    members[1].xid = xid2;
    members[1].status = status2;

    newMulti = MultiXactIdCreateFromMembers(2, members.as_mut_ptr());

    debug_elog3!(DEBUG2, "Create: %s", mxid_to_string(newMulti, 2, members.as_mut_ptr()));

    newMulti
}

/*
 * MultiXactIdExpand
 *		Add a TransactionId to a pre-existing MultiXactId.
 *
 * If the TransactionId is already a member of the passed MultiXactId with the
 * same status, just return it as-is.
 *
 * Note that we do NOT actually modify the membership of a pre-existing
 * MultiXactId; instead we create a new one.  This is necessary to avoid
 * a race condition against code trying to wait for one MultiXactId to finish;
 * see notes in heapam.c.
 *
 * NB - we don't worry about our local MultiXactId cache here, because that
 * is handled by the lower-level routines.
 *
 * Note: It is critical that MultiXactIds that come from an old cluster (i.e.
 * one upgraded by pg_upgrade from a cluster older than this feature) are not
 * passed in.
 */
pub unsafe fn MultiXactIdExpand(
    multi: MultiXactId,
    xid: TransactionId,
    status: MultiXactStatus,
) -> MultiXactId {
    let newMulti: MultiXactId;
    let mut members: *mut MultiXactMember = core::ptr::null_mut();
    let newMembers: *mut MultiXactMember;
    let nmembers: c_int;
    let mut i: c_int;
    let mut j: c_int;

    Assert!(MultiXactIdIsValid(multi));
    Assert!(TransactionIdIsValid(xid));

    /* MultiXactIdSetOldestMember() must have been called already. */
    Assert!(MultiXactIdIsValid(*MyOldestMemberMXactIdSlot()));

    debug_elog5!(DEBUG2, "Expand: received multi %u, xid %u status %s",
                 multi, xid, mxstatus_to_string(status));

    /*
     * Note: we don't allow for old multis here.  The reason is that the only
     * caller of this function does a check that the multixact is no longer
     * running.
     */
    nmembers = GetMultiXactIdMembers(multi, &mut members, false, false);

    if nmembers < 0 {
        let mut member: MultiXactMember = core::mem::zeroed();

        /*
         * The MultiXactId is obsolete.  This can only happen if all the
         * MultiXactId members stop running between the caller checking and
         * passing it to us.  It would be better to return that fact to the
         * caller, but it would complicate the API and it's unlikely to happen
         * too often, so just deal with it by creating a singleton MultiXact.
         */
        member.xid = xid;
        member.status = status;
        newMulti = MultiXactIdCreateFromMembers(1, &mut member);

        debug_elog4!(DEBUG2, "Expand: %u has no members, create singleton %u",
                     multi, newMulti);
        return newMulti;
    }

    /*
     * If the TransactionId is already a member of the MultiXactId with the
     * same status, just return the existing MultiXactId.
     */
    i = 0;
    while i < nmembers {
        if TransactionIdEquals((*members.add(i as usize)).xid, xid)
            && ((*members.add(i as usize)).status == status)
        {
            debug_elog4!(DEBUG2, "Expand: %u is already a member of %u", xid, multi);
            pfree(members as *mut c_void);
            return multi;
        }
        i += 1;
    }

    /*
     * Determine which of the members of the MultiXactId are still of
     * interest. This is any running transaction, and also any transaction
     * that grabbed something stronger than just a lock and was committed. (An
     * update that aborted is of no interest here; and having more than one
     * update Xid in a multixact would cause errors elsewhere.)
     *
     * Removing dead members is not just an optimization: freezing of tuples
     * whose Xmax are multis depends on this behavior.
     *
     * Note we have the same race condition here as above: j could be 0 at the
     * end of the loop.
     */
    newMembers = palloc(
        core::mem::size_of::<MultiXactMember>() * (nmembers + 1) as usize,
    ) as *mut MultiXactMember;

    i = 0;
    j = 0;
    while i < nmembers {
        if TransactionIdIsInProgress((*members.add(i as usize)).xid)
            || (ISUPDATE_from_mxstatus((*members.add(i as usize)).status)
                && TransactionIdDidCommit((*members.add(i as usize)).xid))
        {
            (*newMembers.add(j as usize)).xid = (*members.add(i as usize)).xid;
            (*newMembers.add(j as usize)).status = (*members.add(i as usize)).status;
            j += 1;
        }
        i += 1;
    }

    (*newMembers.add(j as usize)).xid = xid;
    (*newMembers.add(j as usize)).status = status;
    j += 1;
    newMulti = MultiXactIdCreateFromMembers(j, newMembers);

    pfree(members as *mut c_void);
    pfree(newMembers as *mut c_void);

    debug_elog3!(DEBUG2, "Expand: returning new multi %u", newMulti);

    newMulti
}

/*
 * MultiXactIdIsRunning
 *		Returns whether a MultiXactId is "running".
 *
 * We return true if at least one member of the given MultiXactId is still
 * running.  Note that a "false" result is certain not to change,
 * because it is not legal to add members to an existing MultiXactId.
 *
 * Caller is expected to have verified that the multixact does not come from
 * a pg_upgraded share-locked tuple.
 */
pub unsafe fn MultiXactIdIsRunning(multi: MultiXactId, isLockOnly: bool) -> bool {
    let mut members: *mut MultiXactMember = core::ptr::null_mut();
    let nmembers: c_int;
    let mut i: c_int;

    debug_elog3!(DEBUG2, "IsRunning %u?", multi);

    /*
     * "false" here means we assume our callers have checked that the given
     * multi cannot possibly come from a pg_upgraded database.
     */
    nmembers = GetMultiXactIdMembers(multi, &mut members, false, isLockOnly);

    if nmembers <= 0 {
        debug_elog2!(DEBUG2, "IsRunning: no members");
        return false;
    }

    /*
     * Checking for myself is cheap compared to looking in shared memory;
     * return true if any live subtransaction of the current top-level
     * transaction is a member.
     *
     * This is not needed for correctness, it's just a fast path.
     */
    i = 0;
    while i < nmembers {
        if TransactionIdIsCurrentTransactionId((*members.add(i as usize)).xid) {
            debug_elog3!(DEBUG2, "IsRunning: I (%d) am running!", i);
            pfree(members as *mut c_void);
            return true;
        }
        i += 1;
    }

    /*
     * This could be made faster by having another entry point in procarray.c,
     * walking the PGPROC array only once for all the members.  But in most
     * cases nmembers should be small enough that it doesn't much matter.
     */
    i = 0;
    while i < nmembers {
        if TransactionIdIsInProgress((*members.add(i as usize)).xid) {
            debug_elog4!(DEBUG2, "IsRunning: member %d (%u) is running",
                         i, (*members.add(i as usize)).xid);
            pfree(members as *mut c_void);
            return true;
        }
        i += 1;
    }

    pfree(members as *mut c_void);

    debug_elog3!(DEBUG2, "IsRunning: %u is not running", multi);

    false
}

/*
 * MultiXactIdSetOldestMember
 *		Save the oldest MultiXactId this transaction could be a member of.
 *
 * We set the OldestMemberMXactId for a given transaction the first time it's
 * going to do some operation that might require a MultiXactId (tuple lock,
 * update or delete).  We need to do this even if we end up using a
 * TransactionId instead of a MultiXactId, because there is a chance that
 * another transaction would add our XID to a MultiXactId.
 *
 * The value to set is the next-to-be-assigned MultiXactId, so this is meant to
 * be called just before doing any such possibly-MultiXactId-able operation.
 */
pub unsafe fn MultiXactIdSetOldestMember() {
    if !MultiXactIdIsValid(*MyOldestMemberMXactIdSlot()) {
        let mut nextMXact: MultiXactId;

        /*
         * You might think we don't need to acquire a lock here, since
         * fetching and storing of TransactionIds is probably atomic, but in
         * fact we do: suppose we pick up nextMXact and then lose the CPU for
         * a long time.  Someone else could advance nextMXact, and then
         * another someone else could compute an OldestVisibleMXactId that
         * would be after the value we are going to store when we get control
         * back.  Which would be wrong.
         *
         * Note that a shared lock is sufficient, because it's enough to stop
         * someone from advancing nextMXact; and nobody else could be trying
         * to write to our OldestMember entry, only reading (and we assume
         * storing it is atomic.)
         */
        LWLockAcquire(MultiXactGenLock(), LW_SHARED);

        /*
         * We have to beware of the possibility that nextMXact is in the
         * wrapped-around state.  We don't fix the counter itself here, but we
         * must be sure to store a valid value in our array entry.
         */
        nextMXact = (*MultiXactState).nextMXact;
        if nextMXact < FirstMultiXactId {
            nextMXact = FirstMultiXactId;
        }

        *MyOldestMemberMXactIdSlot() = nextMXact;

        LWLockRelease(MultiXactGenLock());

        debug_elog4!(DEBUG2, "MultiXact: setting OldestMember[%d] = %u",
                     MyProcNumber, nextMXact);
    }
}

/*
 * MultiXactIdSetOldestVisible
 *		Save the oldest MultiXactId this transaction considers possibly live.
 *
 * We set the OldestVisibleMXactId for a given transaction the first time
 * it's going to inspect any MultiXactId.  Once we have set this, we are
 * guaranteed that SLRU data for MultiXactIds >= our own OldestVisibleMXactId
 * won't be truncated away.
 */
unsafe fn MultiXactIdSetOldestVisible() {
    if !MultiXactIdIsValid(*MyOldestVisibleMXactIdSlot()) {
        let mut oldestMXact: MultiXactId;
        let mut i: c_int;

        LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);

        /*
         * We have to beware of the possibility that nextMXact is in the
         * wrapped-around state.  We don't fix the counter itself here, but we
         * must be sure to store a valid value in our array entry.
         */
        oldestMXact = (*MultiXactState).nextMXact;
        if oldestMXact < FirstMultiXactId {
            oldestMXact = FirstMultiXactId;
        }

        i = 0;
        while i < NumMemberSlots() {
            let thisoldest: MultiXactId = *OldestMemberMXactId.add(i as usize);

            if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldestMXact) {
                oldestMXact = thisoldest;
            }
            i += 1;
        }

        *MyOldestVisibleMXactIdSlot() = oldestMXact;

        LWLockRelease(MultiXactGenLock());

        debug_elog4!(DEBUG2, "MultiXact: setting OldestVisible[%d] = %u",
                     MyProcNumber, oldestMXact);
    }
}

/*
 * ReadNextMultiXactId
 *		Return the next MultiXactId to be assigned, but don't allocate it
 */
pub unsafe fn ReadNextMultiXactId() -> MultiXactId {
    let mut mxid: MultiXactId;

    /* XXX we could presumably do this without a lock. */
    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    mxid = (*MultiXactState).nextMXact;
    LWLockRelease(MultiXactGenLock());

    if mxid < FirstMultiXactId {
        mxid = FirstMultiXactId;
    }

    mxid
}

/*
 * ReadMultiXactIdRange
 *		Get the range of IDs that may still be referenced by a relation.
 */
pub unsafe fn ReadMultiXactIdRange(oldest: *mut MultiXactId, next: *mut MultiXactId) {
    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    *oldest = (*MultiXactState).oldestMultiXactId;
    *next = (*MultiXactState).nextMXact;
    LWLockRelease(MultiXactGenLock());

    if *oldest < FirstMultiXactId {
        *oldest = FirstMultiXactId;
    }
    if *next < FirstMultiXactId {
        *next = FirstMultiXactId;
    }
}

/*
 * MultiXactIdCreateFromMembers
 *		Make a new MultiXactId from the specified set of members
 *
 * Make XLOG, SLRU and cache entries for a new MultiXactId, recording the
 * given TransactionIds as members.  Returns the newly created MultiXactId.
 *
 * NB: the passed members[] array will be sorted in-place.
 */
pub unsafe fn MultiXactIdCreateFromMembers(
    nmembers: c_int,
    members: *mut MultiXactMember,
) -> MultiXactId {
    let mut multi: MultiXactId;
    let mut offset: MultiXactOffset = 0;
    let mut xlrec: xl_multixact_create = core::mem::zeroed();

    debug_elog3!(DEBUG2, "Create: %s",
                 mxid_to_string(InvalidMultiXactId, nmembers, members));

    /*
     * See if the same set of members already exists in our cache; if so, just
     * re-use that MultiXactId.  (Note: it might seem that looking in our
     * cache is insufficient, and we ought to search disk to see if a
     * duplicate definition already exists.  But since we only ever create
     * MultiXacts containing our own XID, in most cases any such MultiXacts
     * were in fact created by us, and so will be in our cache.  There are
     * corner cases where someone else added us to a MultiXact without our
     * knowledge, but it's not worth checking for.)
     */
    multi = mXactCacheGetBySet(nmembers, members);
    if MultiXactIdIsValid(multi) {
        debug_elog2!(DEBUG2, "Create: in cache!");
        return multi;
    }

    /* Verify that there is a single update Xid among the given members. */
    {
        let mut i: c_int;
        let mut has_update: bool = false;

        i = 0;
        while i < nmembers {
            if ISUPDATE_from_mxstatus((*members.add(i as usize)).status) {
                if has_update {
                    elog!(ERROR, "new multixact has more than one updating member: {}",
                          std::ffi::CStr::from_ptr(mxid_to_string(InvalidMultiXactId, nmembers, members)).to_string_lossy());
                }
                has_update = true;
            }
            i += 1;
        }
    }

    /* Load the injection point before entering the critical section */
    INJECTION_POINT_LOAD(c"multixact-create-from-members".as_ptr());

    /*
     * Assign the MXID and offsets range to use, and make sure there is space
     * in the OFFSETs and MEMBERs files.  NB: this routine does
     * START_CRIT_SECTION().
     */
    multi = GetNewMultiXactId(nmembers, &mut offset);

    INJECTION_POINT_CACHED(c"multixact-create-from-members".as_ptr(), core::ptr::null_mut());

    /* Make an XLOG entry describing the new MXID. */
    xlrec.mid = multi;
    xlrec.moff = offset;
    xlrec.nmembers = nmembers;

    /*
     * XXX Note: there's a lot of padding space in MultiXactMember.  We could
     * find a more compact representation of this Xlog record -- perhaps all
     * the status flags in one XLogRecData, then all the xids in another one?
     * Not clear that it's worth the trouble though.
     */
    XLogBeginInsert();
    XLogRegisterData(core::ptr::addr_of!(xlrec) as *mut c_char, SizeOfMultiXactCreate);
    XLogRegisterData(
        members as *mut c_char,
        nmembers as usize * core::mem::size_of::<MultiXactMember>(),
    );

    XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID);

    /* Now enter the information into the OFFSETs and MEMBERs logs */
    RecordNewMultiXact(multi, offset, nmembers, members);

    /* Done with critical section */
    END_CRIT_SECTION();

    /* Store the new MultiXactId in the local cache, too */
    mXactCachePut(multi, nmembers, members);

    debug_elog2!(DEBUG2, "Create: all done");

    multi
}

/*
 * RecordNewMultiXact
 *		Write info about a new multixact into the offsets and members files
 *
 * This is broken out of MultiXactIdCreateFromMembers so that xlog replay can
 * use it.
 */
unsafe fn RecordNewMultiXact(
    multi: MultiXactId,
    mut offset: MultiXactOffset,
    nmembers: c_int,
    members: *mut MultiXactMember,
) {
    let mut pageno: int64;
    let mut prev_pageno: int64;
    let entryno: c_int;
    let mut slotno: c_int;
    let mut offptr: *mut MultiXactOffset;
    let mut next: MultiXactId;
    let next_pageno: int64;
    let next_entryno: c_int;
    let next_offptr: *mut MultiXactOffset;
    let mut next_offset: MultiXactOffset;
    let mut lock: *mut LWLock;
    let mut prevlock: *mut LWLock = core::ptr::null_mut();

    /* position of this multixid in the offsets SLRU area  */
    pageno = MultiXactIdToOffsetPage(multi);
    entryno = MultiXactIdToOffsetEntry(multi);

    /* position of the next multixid */
    next = multi + 1;
    if next < FirstMultiXactId {
        next = FirstMultiXactId;
    }
    next_pageno = MultiXactIdToOffsetPage(next);
    next_entryno = MultiXactIdToOffsetEntry(next);

    /*
     * Older minor versions didn't set the next multixid's offset in this
     * function, and therefore didn't initialize the next page until the next
     * multixid was assigned.  If we're replaying WAL that was generated by
     * such a version, the next page might not be initialized yet.  Initialize
     * it now.
     */
    if InRecovery
        && next_pageno != pageno
        && pg_atomic_read_u64(&raw mut (*(*MultiXactOffsetCtl()).shared).latest_page_number as *mut _)
            == pageno as u64
    {
        elog!(DEBUG1, "next offsets page is not initialized, initializing it now");

        lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), next_pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        /* Create and zero the page */
        slotno = SimpleLruZeroPage(MultiXactOffsetCtl(), next_pageno);

        /* Make sure it's written out */
        SimpleLruWritePage(MultiXactOffsetCtl(), slotno);
        Assert!(!*(*(*MultiXactOffsetCtl()).shared).page_dirty.add(slotno as usize));

        LWLockRelease(lock);

        /*
         * Remember that we initialized the page, so that we don't zero it
         * again at the XLOG_MULTIXACT_ZERO_OFF_PAGE record.
         */
        pre_initialized_offsets_page = next_pageno;
    }

    /*
     * Set the starting offset of this multixid's members.
     */
    lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    /*
     * Note: we pass the MultiXactId to SimpleLruReadPage as the "transaction"
     * to complain about if there's any I/O error.
     */
    slotno = SimpleLruReadPage(MultiXactOffsetCtl(), pageno, true, multi);
    offptr = *(*(*MultiXactOffsetCtl()).shared).page_buffer.add(slotno as usize) as *mut MultiXactOffset;
    offptr = offptr.add(entryno as usize);

    if *offptr != offset {
        /* should already be set to the correct value, or not at all */
        Assert!(*offptr == 0);
        *offptr = offset;
        *(*(*MultiXactOffsetCtl()).shared).page_dirty.add(slotno as usize) = true;
    }

    /*
     * Set the next multixid's offset to the end of this multixid's members.
     */
    if next_pageno == pageno {
        next_offptr = offptr.add(1);
    } else {
        /* must be the first entry on the page */
        Assert!(next_entryno == 0 || next == FirstMultiXactId);

        /* Swap the lock for a lock on the next page */
        LWLockRelease(lock);
        lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), next_pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        slotno = SimpleLruReadPage(MultiXactOffsetCtl(), next_pageno, true, next);
        next_offptr = (*(*(*MultiXactOffsetCtl()).shared).page_buffer.add(slotno as usize)
            as *mut MultiXactOffset)
            .add(next_entryno as usize);
    }

    /* Like in GetNewMultiXactId(), skip over offset 0 */
    next_offset = offset + nmembers as MultiXactOffset;
    if next_offset == 0 {
        next_offset = 1;
    }
    if *next_offptr != next_offset {
        /* should already be set to the correct value, or not at all */
        Assert!(*next_offptr == 0);
        *next_offptr = next_offset;
        *(*(*MultiXactOffsetCtl()).shared).page_dirty.add(slotno as usize) = true;
    }

    /* Release MultiXactOffset SLRU lock. */
    LWLockRelease(lock);

    prev_pageno = -1;

    let mut i: c_int = 0;
    while i < nmembers {
        let memberptr: *mut TransactionId;
        let flagsptr: *mut uint32;
        let mut flagsval: uint32;
        let bshift: c_int;
        let flagsoff: c_int;
        let memberoff: c_int;

        Assert!((*members.add(i as usize)).status <= MultiXactStatusUpdate);

        pageno = MXOffsetToMemberPage(offset);
        memberoff = MXOffsetToMemberOffset(offset);
        flagsoff = MXOffsetToFlagsOffset(offset);
        bshift = MXOffsetToFlagsBitShift(offset);

        if pageno != prev_pageno {
            /*
             * MultiXactMember SLRU page is changed so check if this new page
             * fall into the different SLRU bank then release the old bank's
             * lock and acquire lock on the new bank.
             */
            lock = SimpleLruGetBankLock(MultiXactMemberCtl(), pageno);
            if lock != prevlock {
                if !prevlock.is_null() {
                    LWLockRelease(prevlock);
                }

                LWLockAcquire(lock, LW_EXCLUSIVE);
                prevlock = lock;
            }
            slotno = SimpleLruReadPage(MultiXactMemberCtl(), pageno, true, multi);
            prev_pageno = pageno;
        }

        memberptr = (*(*(*MultiXactMemberCtl()).shared).page_buffer.add(slotno as usize))
            .add(memberoff as usize) as *mut TransactionId;

        *memberptr = (*members.add(i as usize)).xid;

        flagsptr = (*(*(*MultiXactMemberCtl()).shared).page_buffer.add(slotno as usize))
            .add(flagsoff as usize) as *mut uint32;

        flagsval = *flagsptr;
        flagsval &= !((((1 << MXACT_MEMBER_BITS_PER_XACT) - 1) << bshift) as uint32);
        flagsval |= ((*members.add(i as usize)).status << bshift) as uint32;
        *flagsptr = flagsval;

        *(*(*MultiXactMemberCtl()).shared).page_dirty.add(slotno as usize) = true;

        i += 1;
        offset += 1;
    }

    if !prevlock.is_null() {
        LWLockRelease(prevlock);
    }
}

/*
 * GetNewMultiXactId
 *		Get the next MultiXactId.
 *
 * Also, reserve the needed amount of space in the "members" area.  The
 * starting offset of the reserved space is returned in *offset.
 *
 * This may generate XLOG records for expansion of the offsets and/or members
 * files.  Unfortunately, we have to do that while holding MultiXactGenLock
 * to avoid race conditions --- the XLOG record for zeroing a page must appear
 * before any backend can possibly try to store data in that page!
 *
 * We start a critical section before advancing the shared counters.  The
 * caller must end the critical section after writing SLRU data.
 */
unsafe fn GetNewMultiXactId(mut nmembers: c_int, offset: *mut MultiXactOffset) -> MultiXactId {
    let mut result: MultiXactId;
    let nextOffset: MultiXactOffset;

    debug_elog3!(DEBUG2, "GetNew: for %d xids", nmembers);

    /* safety check, we should never get this far in a HS standby */
    if RecoveryInProgress() {
        elog!(ERROR, "cannot assign MultiXactIds during recovery");
    }

    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);

    /* Handle wraparound of the nextMXact counter */
    if (*MultiXactState).nextMXact < FirstMultiXactId {
        (*MultiXactState).nextMXact = FirstMultiXactId;
    }

    /* Assign the MXID */
    result = (*MultiXactState).nextMXact;

    /*----------
     * Check to see if it's safe to assign another MultiXactId.  This protects
     * against catastrophic data loss due to multixact wraparound.  The basic
     * rules are:
     *
     * If we're past multiVacLimit or the safe threshold for member storage
     * space, or we don't know what the safe threshold for member storage is,
     * start trying to force autovacuum cycles.
     * If we're past multiWarnLimit, start issuing warnings.
     * If we're past multiStopLimit, refuse to create new MultiXactIds.
     *
     * Note these are pretty much the same protections in GetNewTransactionId.
     *----------
     */
    if !MultiXactIdPrecedes(result, (*MultiXactState).multiVacLimit) {
        /*
         * For safety's sake, we release MultiXactGenLock while sending
         * signals, warnings, etc.
         */
        let _multiWarnLimit: MultiXactId = (*MultiXactState).multiWarnLimit;
        let multiStopLimit: MultiXactId = (*MultiXactState).multiStopLimit;
        let _multiWrapLimit: MultiXactId = (*MultiXactState).multiWrapLimit;
        let oldest_datoid: Oid = (*MultiXactState).oldestMultiXactDB;

        LWLockRelease(MultiXactGenLock());

        if IsUnderPostmaster && !MultiXactIdPrecedes(result, multiStopLimit) {
            let oldest_datname: *mut c_char = get_database_name(oldest_datoid);

            /*
             * Immediately kick autovacuum into action as we're already in
             * ERROR territory.
             */
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

            /* complain even if that DB has disappeared */
            if !oldest_datname.is_null() {
                ereport!(ERROR, errmsg!(
                    "database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database \"{}\"",
                    std::ffi::CStr::from_ptr(oldest_datname).to_string_lossy()));
            } else {
                ereport!(ERROR, errmsg!(
                    "database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database with OID {}",
                    oldest_datoid));
            }
        }

        /*
         * To avoid swamping the postmaster with signals, we issue the autovac
         * request only once per 64K multis generated.  This still gives
         * plenty of chances before we get into real trouble.
         */
        if IsUnderPostmaster && (result % 65536) == 0 {
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
        }

        if !MultiXactIdPrecedes(result, _multiWarnLimit) {
            let oldest_datname: *mut c_char = get_database_name(oldest_datoid);

            /* complain even if that DB has disappeared */
            if !oldest_datname.is_null() {
                ereport!(WARNING, errmsg!(
                    "database \"{}\" must be vacuumed before {} more MultiXactIds are used",
                    std::ffi::CStr::from_ptr(oldest_datname).to_string_lossy(),
                    _multiWrapLimit - result));
            } else {
                ereport!(WARNING, errmsg!(
                    "database with OID {} must be vacuumed before {} more MultiXactIds are used",
                    oldest_datoid, _multiWrapLimit - result));
            }
        }

        /* Re-acquire lock and start over */
        LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
        result = (*MultiXactState).nextMXact;
        if result < FirstMultiXactId {
            result = FirstMultiXactId;
        }
    }

    /*
     * Make sure there is room for the next MXID in the file.  Assigning this
     * MXID sets the next MXID's offset already.
     */
    ExtendMultiXactOffset(result + 1);

    /*
     * Reserve the members space, similarly to above.  Also, be careful not to
     * return zero as the starting offset for any multixact. See
     * GetMultiXactIdMembers() for motivation.
     */
    nextOffset = (*MultiXactState).nextOffset;
    if nextOffset == 0 {
        *offset = 1;
        nmembers += 1; /* allocate member slot 0 too */
    } else {
        *offset = nextOffset;
    }

    /*----------
     * Protect against overrun of the members space as well, with the
     * following rules:
     *
     * If we're past offsetStopLimit, refuse to generate more multis.
     * If we're close to offsetStopLimit, emit a warning.
     *----------
     */
    const OFFSET_WARN_SEGMENTS: c_int = 20;
    if (*MultiXactState).oldestOffsetKnown
        && MultiXactOffsetWouldWrap(
            (*MultiXactState).offsetStopLimit,
            nextOffset,
            nmembers as uint32,
        )
    {
        /* see comment in the corresponding offsets wraparound case */
        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

        ereport!(ERROR, errmsg!("multixact \"members\" limit exceeded"));
    }

    /*
     * Check whether we should kick autovacuum into action, to prevent members
     * wraparound. NB we use a much larger window to trigger autovacuum than
     * just the warning limit.
     */
    if !(*MultiXactState).oldestOffsetKnown
        || ((*MultiXactState).nextOffset - (*MultiXactState).oldestOffset
            > MULTIXACT_MEMBER_SAFE_THRESHOLD)
    {
        /*
         * To avoid swamping the postmaster with signals, we issue the autovac
         * request only when crossing a segment boundary.
         */
        if (MXOffsetToMemberPage(nextOffset) / SLRU_PAGES_PER_SEGMENT as int64)
            != (MXOffsetToMemberPage(nextOffset + nmembers as MultiXactOffset)
                / SLRU_PAGES_PER_SEGMENT as int64)
        {
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
        }
    }

    if (*MultiXactState).oldestOffsetKnown
        && MultiXactOffsetWouldWrap(
            (*MultiXactState).offsetStopLimit,
            nextOffset,
            nmembers as uint32
                + (MULTIXACT_MEMBERS_PER_PAGE
                    * SLRU_PAGES_PER_SEGMENT
                    * OFFSET_WARN_SEGMENTS) as uint32,
        )
    {
        ereport!(WARNING, errmsg!(
            "database with OID {} must be vacuumed before {} more multixact members are used",
            (*MultiXactState).oldestMultiXactDB,
            (*MultiXactState).offsetStopLimit - nextOffset + nmembers as MultiXactOffset));
    }

    ExtendMultiXactMember(nextOffset, nmembers);

    /*
     * Critical section from here until caller has written the data into the
     * just-reserved SLRU space; we don't want to error out with a partly
     * written MultiXact structure.
     */
    START_CRIT_SECTION();

    /*
     * Advance counters.  As in GetNewTransactionId(), this must not happen
     * until after file extension has succeeded!
     */
    (*MultiXactState).nextMXact += 1;

    (*MultiXactState).nextOffset += nmembers as MultiXactOffset;

    LWLockRelease(MultiXactGenLock());

    debug_elog4!(DEBUG2, "GetNew: returning %u offset %u", result, *offset);
    result
}

/*
 * GetMultiXactIdMembers
 *		Return the set of MultiXactMembers that make up a MultiXactId
 *
 * Return value is the number of members found, or -1 if there are none,
 * and *members is set to a newly palloc'ed array of members.  It's the
 * caller's responsibility to free it when done with it.
 */
pub unsafe fn GetMultiXactIdMembers(
    multi: MultiXactId,
    members: *mut *mut MultiXactMember,
    from_pgupgrade: bool,
    isLockOnly: bool,
) -> c_int {
    let mut pageno: int64;
    let mut prev_pageno: int64;
    let mut entryno: c_int;
    let mut slotno: c_int;
    let mut offptr: *mut MultiXactOffset;
    let mut offset: MultiXactOffset;
    let mut length: c_int;
    let mut truelength: c_int;
    let oldestMXact: MultiXactId;
    let nextMXact: MultiXactId;
    let mut tmpMXact: MultiXactId;
    let nextOffset: MultiXactOffset;
    let ptr: *mut MultiXactMember;
    let mut lock: *mut LWLock;

    debug_elog3!(DEBUG2, "GetMembers: asked for %u", multi);

    if !MultiXactIdIsValid(multi) || from_pgupgrade {
        *members = core::ptr::null_mut();
        return -1;
    }

    /* See if the MultiXactId is in the local cache */
    length = mXactCacheGetById(multi, members);
    if length >= 0 {
        debug_elog3!(DEBUG2, "GetMembers: found %s in the cache",
                     mxid_to_string(multi, length, *members));
        return length;
    }

    /* Set our OldestVisibleMXactId[] entry if we didn't already */
    MultiXactIdSetOldestVisible();

    /*
     * If we know the multi is used only for locking and not for updates, then
     * we can skip checking if the value is older than our oldest visible
     * multi.  It cannot possibly still be running.
     */
    if isLockOnly && MultiXactIdPrecedes(multi, *MyOldestVisibleMXactIdSlot()) {
        debug_elog2!(DEBUG2, "GetMembers: a locker-only multi is too old");
        *members = core::ptr::null_mut();
        return -1;
    }

    /*
     * We check known limits on MultiXact before resorting to the SLRU area.
     */
    LWLockAcquire(MultiXactGenLock(), LW_SHARED);

    oldestMXact = (*MultiXactState).oldestMultiXactId;
    nextMXact = (*MultiXactState).nextMXact;
    nextOffset = (*MultiXactState).nextOffset;

    LWLockRelease(MultiXactGenLock());

    if MultiXactIdPrecedes(multi, oldestMXact) {
        ereport!(ERROR, errmsg!(
            "MultiXactId {} does no longer exist -- apparent wraparound", multi));
    }

    if !MultiXactIdPrecedes(multi, nextMXact) {
        ereport!(ERROR, errmsg!(
            "MultiXactId {} has not been created yet -- apparent wraparound", multi));
    }

    /*
     * Find out the offset at which we need to start reading MultiXactMembers
     * and the number of members in the multixact.
     */
    pageno = MultiXactIdToOffsetPage(multi);
    entryno = MultiXactIdToOffsetEntry(multi);

    /* Acquire the bank lock for the page we need. */
    lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    slotno = SimpleLruReadPage(MultiXactOffsetCtl(), pageno, true, multi);
    offptr = *(*(*MultiXactOffsetCtl()).shared).page_buffer.add(slotno as usize) as *mut MultiXactOffset;
    offptr = offptr.add(entryno as usize);
    offset = *offptr;

    Assert!(offset != 0);

    /*
     * Use the same increment rule as GetNewMultiXactId(), that is, don't
     * handle wraparound explicitly until needed.
     */
    tmpMXact = multi + 1;

    if nextMXact == tmpMXact {
        /* Corner case 1: there is no next multixact */
        length = (nextOffset - offset) as c_int;
    } else {
        let nextMXOffset: MultiXactOffset;

        /* handle wraparound if needed */
        if tmpMXact < FirstMultiXactId {
            tmpMXact = FirstMultiXactId;
        }

        prev_pageno = pageno;

        pageno = MultiXactIdToOffsetPage(tmpMXact);
        entryno = MultiXactIdToOffsetEntry(tmpMXact);

        if pageno != prev_pageno {
            let newlock: *mut LWLock;

            /*
             * Since we're going to access a different SLRU page, if this page
             * falls under a different bank, release the old bank's lock and
             * acquire the lock of the new bank.
             */
            newlock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);
            if newlock != lock {
                LWLockRelease(lock);
                LWLockAcquire(newlock, LW_EXCLUSIVE);
                lock = newlock;
            }
            slotno = SimpleLruReadPage(MultiXactOffsetCtl(), pageno, true, tmpMXact);
        }

        offptr = *(*(*MultiXactOffsetCtl()).shared).page_buffer.add(slotno as usize) as *mut MultiXactOffset;
        offptr = offptr.add(entryno as usize);
        nextMXOffset = *offptr;

        if nextMXOffset == 0 {
            ereport!(ERROR, errmsg!("MultiXact {} has invalid next offset", multi));
        }

        length = (nextMXOffset - offset) as c_int;
    }

    LWLockRelease(lock);
    lock = core::ptr::null_mut();

    ptr = palloc(length as usize * core::mem::size_of::<MultiXactMember>()) as *mut MultiXactMember;

    truelength = 0;
    prev_pageno = -1;
    let mut i: c_int = 0;
    while i < length {
        let xactptr: *mut TransactionId;
        let flagsptr: *mut uint32;
        let flagsoff: c_int;
        let bshift: c_int;
        let memberoff: c_int;

        pageno = MXOffsetToMemberPage(offset);
        memberoff = MXOffsetToMemberOffset(offset);

        if pageno != prev_pageno {
            let newlock: *mut LWLock;

            /*
             * Since we're going to access a different SLRU page, if this page
             * falls under a different bank, release the old bank's lock and
             * acquire the lock of the new bank.
             */
            newlock = SimpleLruGetBankLock(MultiXactMemberCtl(), pageno);
            if newlock != lock {
                if !lock.is_null() {
                    LWLockRelease(lock);
                }
                LWLockAcquire(newlock, LW_EXCLUSIVE);
                lock = newlock;
            }

            slotno = SimpleLruReadPage(MultiXactMemberCtl(), pageno, true, multi);
            prev_pageno = pageno;
        }

        xactptr = (*(*(*MultiXactMemberCtl()).shared).page_buffer.add(slotno as usize))
            .add(memberoff as usize) as *mut TransactionId;

        if !TransactionIdIsValid(*xactptr) {
            /*
             * Corner case 2: offset must have wrapped around to unused slot
             * zero.
             */
            Assert!(offset == 0);
            i += 1;
            offset += 1;
            continue;
        }

        flagsoff = MXOffsetToFlagsOffset(offset);
        bshift = MXOffsetToFlagsBitShift(offset);
        flagsptr = (*(*(*MultiXactMemberCtl()).shared).page_buffer.add(slotno as usize))
            .add(flagsoff as usize) as *mut uint32;

        (*ptr.add(truelength as usize)).xid = *xactptr;
        (*ptr.add(truelength as usize)).status =
            ((*flagsptr >> bshift) & MXACT_MEMBER_XACT_BITMASK as uint32) as MultiXactStatus;
        truelength += 1;

        i += 1;
        offset += 1;
    }

    LWLockRelease(lock);

    /* A multixid with zero members should not happen */
    Assert!(truelength > 0);

    /*
     * Copy the result into the local cache.
     */
    mXactCachePut(multi, truelength, ptr);

    debug_elog3!(DEBUG2, "GetMembers: no cache for %s",
                 mxid_to_string(multi, truelength, ptr));
    *members = ptr;
    truelength
}

/*
 * mxactMemberComparator
 *		qsort comparison function for MultiXactMember
 *
 * We can't use wraparound comparison for XIDs because that does not respect
 * the triangle inequality!  Any old sort order will do.
 */
unsafe extern "C" fn mxactMemberComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    let member1: MultiXactMember = *(arg1 as *const MultiXactMember);
    let member2: MultiXactMember = *(arg2 as *const MultiXactMember);

    if member1.xid > member2.xid {
        return 1;
    }
    if member1.xid < member2.xid {
        return -1;
    }
    if member1.status > member2.status {
        return 1;
    }
    if member1.status < member2.status {
        return -1;
    }
    0
}

/*
 * mXactCacheGetBySet
 *		returns a MultiXactId from the cache based on the set of
 *		TransactionIds that compose it, or InvalidMultiXactId if
 *		none matches.
 *
 * NB: the passed members array will be sorted in-place.
 */
unsafe fn mXactCacheGetBySet(nmembers: c_int, members: *mut MultiXactMember) -> MultiXactId {
    let mut iter: dlist_iter = core::mem::zeroed();

    debug_elog3!(DEBUG2, "CacheGet: looking for %s",
                 mxid_to_string(InvalidMultiXactId, nmembers, members));

    /* sort the array so comparison is easy */
    qsort(
        members as *mut c_void,
        nmembers as usize,
        core::mem::size_of::<MultiXactMember>(),
        mxactMemberComparator,
    );

    dclist_foreach!(iter, core::ptr::addr_of_mut!(MXactCache), {
        let entry: *mut mXactCacheEnt = dclist_container!(mXactCacheEnt, node, iter.cur);

        if (*entry).nmembers != nmembers {
            continue;
        }

        /*
         * We assume the cache entries are sorted, and that the unused bits in
         * "status" are zeroed.
         */
        if memcmp(
            members as *const c_void,
            (*entry).members.as_ptr() as *const c_void,
            nmembers as usize * core::mem::size_of::<MultiXactMember>(),
        ) == 0
        {
            debug_elog3!(DEBUG2, "CacheGet: found %u", (*entry).multi);
            dclist_move_head(core::ptr::addr_of_mut!(MXactCache), iter.cur);
            return (*entry).multi;
        }
    });

    debug_elog2!(DEBUG2, "CacheGet: not found :-(");
    InvalidMultiXactId
}

/*
 * mXactCacheGetById
 *		returns the composing MultiXactMember set from the cache for a
 *		given MultiXactId, if present.
 *
 * If successful, *xids is set to the address of a palloc'd copy of the
 * MultiXactMember set.  Return value is number of members, or -1 on failure.
 */
unsafe fn mXactCacheGetById(multi: MultiXactId, members: *mut *mut MultiXactMember) -> c_int {
    let mut iter: dlist_iter = core::mem::zeroed();

    debug_elog3!(DEBUG2, "CacheGet: looking for %u", multi);

    dclist_foreach!(iter, core::ptr::addr_of_mut!(MXactCache), {
        let entry: *mut mXactCacheEnt = dclist_container!(mXactCacheEnt, node, iter.cur);

        if (*entry).multi == multi {
            let ptr: *mut MultiXactMember;
            let size: Size;

            size = core::mem::size_of::<MultiXactMember>() * (*entry).nmembers as usize;
            ptr = palloc(size) as *mut MultiXactMember;

            memcpy(
                ptr as *mut c_void,
                (*entry).members.as_ptr() as *const c_void,
                size,
            );

            debug_elog3!(DEBUG2, "CacheGet: found %s",
                         mxid_to_string(multi, (*entry).nmembers, (*entry).members.as_mut_ptr()));

            /*
             * Note we modify the list while not using a modifiable iterator.
             * This is acceptable only because we exit the iteration
             * immediately afterwards.
             */
            dclist_move_head(core::ptr::addr_of_mut!(MXactCache), iter.cur);

            *members = ptr;
            return (*entry).nmembers;
        }
    });

    debug_elog2!(DEBUG2, "CacheGet: not found");
    -1
}

/*
 * mXactCachePut
 *		Add a new MultiXactId and its composing set into the local cache.
 */
unsafe fn mXactCachePut(multi: MultiXactId, nmembers: c_int, members: *mut MultiXactMember) {
    let mut entry: *mut mXactCacheEnt;

    debug_elog3!(DEBUG2, "CachePut: storing %s",
                 mxid_to_string(multi, nmembers, members));

    if MXactContext.is_null() {
        /* The cache only lives as long as the current transaction */
        debug_elog2!(DEBUG2, "CachePut: initializing memory context");
        MXactContext = AllocSetContextCreate(
            TopTransactionContext,
            c"MultiXact cache context".as_ptr(),
            ALLOCSET_SMALL_SIZES,
        );
    }

    entry = MemoryContextAlloc(
        MXactContext,
        core::mem::offset_of!(mXactCacheEnt, members)
            + nmembers as usize * core::mem::size_of::<MultiXactMember>(),
    ) as *mut mXactCacheEnt;

    (*entry).multi = multi;
    (*entry).nmembers = nmembers;
    memcpy(
        (*entry).members.as_mut_ptr() as *mut c_void,
        members as *const c_void,
        nmembers as usize * core::mem::size_of::<MultiXactMember>(),
    );

    /* mXactCacheGetBySet assumes the entries are sorted, so sort them */
    qsort(
        (*entry).members.as_mut_ptr() as *mut c_void,
        nmembers as usize,
        core::mem::size_of::<MultiXactMember>(),
        mxactMemberComparator,
    );

    dclist_push_head(core::ptr::addr_of_mut!(MXactCache), core::ptr::addr_of_mut!((*entry).node));
    if dclist_count(core::ptr::addr_of_mut!(MXactCache)) > MAX_CACHE_ENTRIES as Size {
        let node: *mut dlist_node;

        node = dclist_tail_node(core::ptr::addr_of_mut!(MXactCache));
        dclist_delete_from(core::ptr::addr_of_mut!(MXactCache), node);

        entry = dclist_container!(mXactCacheEnt, node, node);
        debug_elog3!(DEBUG2, "CachePut: pruning cached multi %u", (*entry).multi);

        pfree(entry as *mut c_void);
    }
}

unsafe fn mxstatus_to_string(status: MultiXactStatus) -> *mut c_char {
    match status {
        MultiXactStatusForKeyShare => c"keysh".as_ptr() as *mut c_char,
        MultiXactStatusForShare => c"sh".as_ptr() as *mut c_char,
        MultiXactStatusForNoKeyUpdate => c"fornokeyupd".as_ptr() as *mut c_char,
        MultiXactStatusForUpdate => c"forupd".as_ptr() as *mut c_char,
        MultiXactStatusNoKeyUpdate => c"nokeyupd".as_ptr() as *mut c_char,
        MultiXactStatusUpdate => c"upd".as_ptr() as *mut c_char,
        _ => {
            elog!(ERROR, "unrecognized multixact status {}", status);
            #[allow(unreachable_code)]
            (c"".as_ptr() as *mut c_char)
        }
    }
}

pub unsafe fn mxid_to_string(
    multi: MultiXactId,
    nmembers: c_int,
    members: *mut MultiXactMember,
) -> *mut c_char {
    static mut str: *mut c_char = core::ptr::null_mut();
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut i: c_int;

    if !str.is_null() {
        pfree(str as *mut c_void);
    }

    initStringInfo(&mut buf);

    // TODO(pg-port): appendStringInfo is a non-variadic stub; printf args dropped.
    appendStringInfo(&mut buf, c"%u %d[%u (%s)".as_ptr());
    let _ = (multi, nmembers, (*members.add(0)).xid, mxstatus_to_string((*members.add(0)).status));

    i = 1;
    while i < nmembers {
        appendStringInfo(&mut buf, c", %u (%s)".as_ptr());
        let _ = ((*members.add(i as usize)).xid, mxstatus_to_string((*members.add(i as usize)).status));
        i += 1;
    }

    appendStringInfoChar(&mut buf, b']' as c_char);
    str = MemoryContextStrdup(TopMemoryContext, buf.data);
    pfree(buf.data as *mut c_void);
    str
}

/*
 * AtEOXact_MultiXact
 *		Handle transaction end for MultiXact
 *
 * This is called at top transaction commit or abort (we don't care which).
 */
pub unsafe fn AtEOXact_MultiXact() {
    /*
     * Reset our OldestMemberMXactId and OldestVisibleMXactId values, both of
     * which should only be valid while within a transaction.
     */
    *MyOldestMemberMXactIdSlot() = InvalidMultiXactId;
    *MyOldestVisibleMXactIdSlot() = InvalidMultiXactId;

    /*
     * Discard the local MultiXactId cache.  Since MXactContext was created as
     * a child of TopTransactionContext, we needn't delete it explicitly.
     */
    MXactContext = core::ptr::null_mut();
    dclist_init(core::ptr::addr_of_mut!(MXactCache));
}

/*
 * AtPrepare_MultiXact
 *		Save multixact state at 2PC transaction prepare
 *
 * In this phase, we only store our OldestMemberMXactId value in the two-phase
 * state file.
 */
pub unsafe fn AtPrepare_MultiXact() {
    let mut myOldestMember: MultiXactId = *MyOldestMemberMXactIdSlot();

    if MultiXactIdIsValid(myOldestMember) {
        RegisterTwoPhaseRecord(
            TWOPHASE_RM_MULTIXACT_ID,
            0,
            core::ptr::addr_of_mut!(myOldestMember) as *mut c_void,
            core::mem::size_of::<MultiXactId>() as u32,
        );
    }
}

/*
 * PostPrepare_MultiXact
 *		Clean up after successful PREPARE TRANSACTION
 */
pub unsafe fn PostPrepare_MultiXact(xid: TransactionId) {
    let myOldestMember: MultiXactId;

    /*
     * Transfer our OldestMemberMXactId value to the slot reserved for the
     * prepared transaction.
     */
    myOldestMember = *MyOldestMemberMXactIdSlot();
    if MultiXactIdIsValid(myOldestMember) {
        let dummyProcNumber: ProcNumber = TwoPhaseGetDummyProcNumber(xid, false);

        /*
         * Even though storing MultiXactId is atomic, acquire lock to make
         * sure others see both changes, not just the reset of the slot of the
         * current backend.
         */
        LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);

        *PreparedXactOldestMemberMXactIdSlot(dummyProcNumber) = myOldestMember;
        *MyOldestMemberMXactIdSlot() = InvalidMultiXactId;

        LWLockRelease(MultiXactGenLock());
    }

    /*
     * We don't need to transfer OldestVisibleMXactId value, because the
     * transaction is not going to be looking at any more multixacts once it's
     * prepared.
     */
    *MyOldestVisibleMXactIdSlot() = InvalidMultiXactId;

    /*
     * Discard the local MultiXactId cache like in AtEOXact_MultiXact.
     */
    MXactContext = core::ptr::null_mut();
    dclist_init(core::ptr::addr_of_mut!(MXactCache));
}

/*
 * multixact_twophase_recover
 *		Recover the state of a prepared transaction at startup
 */
pub unsafe fn multixact_twophase_recover(
    xid: TransactionId,
    _info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    let dummyProcNumber: ProcNumber = TwoPhaseGetDummyProcNumber(xid, false);
    let oldestMember: MultiXactId;

    /*
     * Get the oldest member XID from the state file record, and set it in the
     * OldestMemberMXactId slot reserved for this prepared transaction.
     */
    Assert!(len as usize == core::mem::size_of::<MultiXactId>());
    oldestMember = *(recdata as *mut MultiXactId);

    *PreparedXactOldestMemberMXactIdSlot(dummyProcNumber) = oldestMember;
}

/*
 * multixact_twophase_postcommit
 *		Similar to AtEOXact_MultiXact but for COMMIT PREPARED
 */
pub unsafe fn multixact_twophase_postcommit(
    xid: TransactionId,
    _info: uint16,
    _recdata: *mut c_void,
    len: uint32,
) {
    let dummyProcNumber: ProcNumber = TwoPhaseGetDummyProcNumber(xid, true);

    Assert!(len as usize == core::mem::size_of::<MultiXactId>());

    *PreparedXactOldestMemberMXactIdSlot(dummyProcNumber) = InvalidMultiXactId;
}

/*
 * multixact_twophase_postabort
 *		This is actually just the same as the COMMIT case.
 */
pub unsafe fn multixact_twophase_postabort(
    xid: TransactionId,
    info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    multixact_twophase_postcommit(xid, info, recdata, len);
}

/*
 * Initialization of shared memory for MultiXact.
 */
unsafe fn MultiXactSharedStateShmemSize() -> Size {
    let mut size: Size;

    size = core::mem::offset_of!(MultiXactStateData, perBackendXactIds);
    size = add_size(
        size,
        mul_size(core::mem::size_of::<MultiXactId>(), NumMemberSlots() as Size),
    );
    size = add_size(
        size,
        mul_size(core::mem::size_of::<MultiXactId>(), NumVisibleSlots() as Size),
    );
    size
}

pub unsafe fn MultiXactShmemSize() -> Size {
    let mut size: Size;

    size = MultiXactSharedStateShmemSize();
    size = add_size(size, SimpleLruShmemSize(multixact_offset_buffers, 0));
    size = add_size(size, SimpleLruShmemSize(multixact_member_buffers, 0));

    size
}

pub unsafe fn MultiXactShmemInit() {
    let mut found: bool = false;

    debug_elog2!(DEBUG2, "Shared Memory Init for MultiXact");

    (*MultiXactOffsetCtl()).PagePrecedes = Some(MultiXactOffsetPagePrecedes);
    (*MultiXactMemberCtl()).PagePrecedes = Some(MultiXactMemberPagePrecedes);

    SimpleLruInit(
        MultiXactOffsetCtl(),
        c"multixact_offset".as_ptr(),
        multixact_offset_buffers,
        0,
        c"pg_multixact/offsets".as_ptr(),
        LWTRANCHE_MULTIXACTOFFSET_BUFFER,
        LWTRANCHE_MULTIXACTOFFSET_SLRU,
        SYNC_HANDLER_MULTIXACT_OFFSET,
        false,
    );
    SlruPagePrecedesUnitTests(MultiXactOffsetCtl(), MULTIXACT_OFFSETS_PER_PAGE);
    SimpleLruInit(
        MultiXactMemberCtl(),
        c"multixact_member".as_ptr(),
        multixact_member_buffers,
        0,
        c"pg_multixact/members".as_ptr(),
        LWTRANCHE_MULTIXACTMEMBER_BUFFER,
        LWTRANCHE_MULTIXACTMEMBER_SLRU,
        SYNC_HANDLER_MULTIXACT_MEMBER,
        false,
    );
    /* doesn't call SimpleLruTruncate() or meet criteria for unit tests */

    /* Initialize our shared state struct */
    MultiXactState = ShmemInitStruct(
        c"Shared MultiXact State".as_ptr(),
        MultiXactSharedStateShmemSize(),
        &mut found,
    ) as *mut MultiXactStateData;
    if !IsUnderPostmaster {
        Assert!(!found);

        /* Make sure we zero out the per-backend state */
        MemSet(
            MultiXactState as *mut c_void,
            0,
            MultiXactSharedStateShmemSize(),
        );
    } else {
        Assert!(found);
    }

    /*
     * Set up array pointers.
     */
    OldestMemberMXactId = (*MultiXactState).perBackendXactIds.as_mut_ptr();
    OldestVisibleMXactId = OldestMemberMXactId.add(NumMemberSlots() as usize);
}

/*
 * GUC check_hook for multixact_offset_buffers
 */
pub unsafe fn check_multixact_offset_buffers(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    check_slru_buffers(c"multixact_offset_buffers".as_ptr(), newval)
}

/*
 * GUC check_hook for multixact_member_buffers
 */
pub unsafe fn check_multixact_member_buffers(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    check_slru_buffers(c"multixact_member_buffers".as_ptr(), newval)
}

/*
 * This func must be called ONCE on system install.  It creates the initial
 * MultiXact segments.  (The MultiXacts directories are assumed to have been
 * created by initdb, and MultiXactShmemInit must have been called already.)
 */
pub unsafe fn BootStrapMultiXact() {
    let mut slotno: c_int;
    let mut lock: *mut LWLock;

    lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), 0);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Create and zero the first page of the offsets log */
    slotno = ZeroMultiXactOffsetPage(0, false);

    /* Make sure it's written out */
    SimpleLruWritePage(MultiXactOffsetCtl(), slotno);
    Assert!(!*(*(*MultiXactOffsetCtl()).shared).page_dirty.add(slotno as usize));

    LWLockRelease(lock);

    lock = SimpleLruGetBankLock(MultiXactMemberCtl(), 0);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Create and zero the first page of the members log */
    slotno = ZeroMultiXactMemberPage(0, false);

    /* Make sure it's written out */
    SimpleLruWritePage(MultiXactMemberCtl(), slotno);
    Assert!(!*(*(*MultiXactMemberCtl()).shared).page_dirty.add(slotno as usize));

    LWLockRelease(lock);
}

/*
 * Initialize (or reinitialize) a page of MultiXactOffset to zeroes.
 * If writeXlog is true, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
unsafe fn ZeroMultiXactOffsetPage(pageno: int64, writeXlog: bool) -> c_int {
    let slotno: c_int;

    slotno = SimpleLruZeroPage(MultiXactOffsetCtl(), pageno);

    if writeXlog {
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_OFF_PAGE);
    }

    slotno
}

/*
 * Ditto, for MultiXactMember
 */
unsafe fn ZeroMultiXactMemberPage(pageno: int64, writeXlog: bool) -> c_int {
    let slotno: c_int;

    slotno = SimpleLruZeroPage(MultiXactMemberCtl(), pageno);

    if writeXlog {
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_MEM_PAGE);
    }

    slotno
}

/*
 * MaybeExtendOffsetSlru
 *		Extend the offsets SLRU area, if necessary
 *
 * After a binary upgrade from <= 9.2, the pg_multixact/offsets SLRU area might
 * contain files that are shorter than necessary [...].  This routine is in
 * charge of creating such pages.
 */
unsafe fn MaybeExtendOffsetSlru() {
    let pageno: int64;
    let lock: *mut LWLock;

    pageno = MultiXactIdToOffsetPage((*MultiXactState).nextMXact);
    lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    if !SimpleLruDoesPhysicalPageExist(MultiXactOffsetCtl(), pageno) {
        let slotno: c_int;

        /*
         * Fortunately for us, SimpleLruWritePage is already prepared to deal
         * with creating a new segment file even if the page we're writing is
         * not the first in it, so this is enough.
         */
        slotno = ZeroMultiXactOffsetPage(pageno, false);
        SimpleLruWritePage(MultiXactOffsetCtl(), slotno);
    }

    LWLockRelease(lock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup.
 */
pub unsafe fn StartupMultiXact() {
    let multi: MultiXactId = (*MultiXactState).nextMXact;
    let offset: MultiXactOffset = (*MultiXactState).nextOffset;
    let mut pageno: int64;

    /*
     * Initialize offset's idea of the latest page number.
     */
    pageno = MultiXactIdToOffsetPage(multi);
    pg_atomic_write_u64(
        &raw mut (*(*MultiXactOffsetCtl()).shared).latest_page_number as *mut _,
        pageno as u64,
    );

    /*
     * Initialize member's idea of the latest page number.
     */
    pageno = MXOffsetToMemberPage(offset);
    pg_atomic_write_u64(
        &raw mut (*(*MultiXactMemberCtl()).shared).latest_page_number as *mut _,
        pageno as u64,
    );
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
pub unsafe fn TrimMultiXact() {
    let nextMXact: MultiXactId;
    let offset: MultiXactOffset;
    let oldestMXact: MultiXactId;
    let oldestMXactDB: Oid;
    let mut pageno: int64;
    let entryno: c_int;
    let flagsoff: c_int;

    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    nextMXact = (*MultiXactState).nextMXact;
    offset = (*MultiXactState).nextOffset;
    oldestMXact = (*MultiXactState).oldestMultiXactId;
    oldestMXactDB = (*MultiXactState).oldestMultiXactDB;
    LWLockRelease(MultiXactGenLock());

    /* Clean up offsets state */

    /*
     * (Re-)Initialize our idea of the latest page number for offsets.
     */
    pageno = MultiXactIdToOffsetPage(nextMXact);
    pg_atomic_write_u64(
        &raw mut (*(*MultiXactOffsetCtl()).shared).latest_page_number as *mut _,
        pageno as u64,
    );

    /*
     * Set the offset of nextMXact on the offsets page.  [...]  Zero out the
     * remainder of the page.
     */
    entryno = MultiXactIdToOffsetEntry(nextMXact);
    {
        let slotno: c_int;
        let mut offptr: *mut MultiXactOffset;
        let lock: *mut LWLock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);

        LWLockAcquire(lock, LW_EXCLUSIVE);
        if entryno == 0 {
            slotno = SimpleLruZeroPage(MultiXactOffsetCtl(), pageno);
        } else {
            slotno = SimpleLruReadPage(MultiXactOffsetCtl(), pageno, true, nextMXact);
        }
        offptr = *(*(*MultiXactOffsetCtl()).shared).page_buffer.add(slotno as usize) as *mut MultiXactOffset;
        offptr = offptr.add(entryno as usize);

        *offptr = offset;
        if entryno != 0
            && (entryno + 1) as usize * core::mem::size_of::<MultiXactOffset>() != BLCKSZ as usize
        {
            MemSet(
                offptr.add(1) as *mut c_void,
                0,
                (BLCKSZ as usize
                    - (entryno + 1) as usize * core::mem::size_of::<MultiXactOffset>())
                    as Size,
            );
        }

        *(*(*MultiXactOffsetCtl()).shared).page_dirty.add(slotno as usize) = true;
        LWLockRelease(lock);
    }

    /*
     * And the same for members.
     *
     * (Re-)Initialize our idea of the latest page number for members.
     */
    pageno = MXOffsetToMemberPage(offset);
    pg_atomic_write_u64(
        &raw mut (*(*MultiXactMemberCtl()).shared).latest_page_number as *mut _,
        pageno as u64,
    );

    /*
     * Zero out the remainder of the current members page.
     */
    flagsoff = MXOffsetToFlagsOffset(offset);
    if flagsoff != 0 {
        let slotno: c_int;
        let xidptr: *mut TransactionId;
        let memberoff: c_int;
        let lock: *mut LWLock = SimpleLruGetBankLock(MultiXactMemberCtl(), pageno);

        LWLockAcquire(lock, LW_EXCLUSIVE);
        memberoff = MXOffsetToMemberOffset(offset);
        slotno = SimpleLruReadPage(MultiXactMemberCtl(), pageno, true, offset);
        xidptr = (*(*(*MultiXactMemberCtl()).shared).page_buffer.add(slotno as usize))
            .add(memberoff as usize) as *mut TransactionId;

        MemSet(xidptr as *mut c_void, 0, (BLCKSZ as c_int - memberoff) as Size);

        /*
         * Note: we don't need to zero out the flag bits in the remaining
         * members of the current group, because they are always reset before
         * writing.
         */

        *(*(*MultiXactMemberCtl()).shared).page_dirty.add(slotno as usize) = true;
        LWLockRelease(lock);
    }

    /* signal that we're officially up */
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
    (*MultiXactState).finishedStartup = true;
    LWLockRelease(MultiXactGenLock());

    /* Now compute how far away the next members wraparound is. */
    SetMultiXactIdLimit(oldestMXact, oldestMXactDB, true);
}

/*
 * Get the MultiXact data to save in a checkpoint record
 */
pub unsafe fn MultiXactGetCheckptMulti(
    _is_shutdown: bool,
    nextMulti: *mut MultiXactId,
    nextMultiOffset: *mut MultiXactOffset,
    oldestMulti: *mut MultiXactId,
    oldestMultiDB: *mut Oid,
) {
    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    *nextMulti = (*MultiXactState).nextMXact;
    *nextMultiOffset = (*MultiXactState).nextOffset;
    *oldestMulti = (*MultiXactState).oldestMultiXactId;
    *oldestMultiDB = (*MultiXactState).oldestMultiXactDB;
    LWLockRelease(MultiXactGenLock());

    debug_elog6!(DEBUG2,
        "MultiXact: checkpoint is nextMulti %u, nextOffset %u, oldestMulti %u in DB %u",
        *nextMulti, *nextMultiOffset, *oldestMulti, *oldestMultiDB);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
pub unsafe fn CheckPointMultiXact() {
    TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_START(true);

    /*
     * Write dirty MultiXact pages to disk.  This may result in sync requests
     * queued for later handling by ProcessSyncRequests(), as part of the
     * checkpoint.
     */
    SimpleLruWriteAll(MultiXactOffsetCtl(), true);
    SimpleLruWriteAll(MultiXactMemberCtl(), true);

    TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_DONE(true);
}

/*
 * Set the next-to-be-assigned MultiXactId and offset
 *
 * This is used when we can determine the correct next ID/offset exactly
 * from a checkpoint record.
 */
pub unsafe fn MultiXactSetNextMXact(nextMulti: MultiXactId, nextMultiOffset: MultiXactOffset) {
    debug_elog4!(DEBUG2, "MultiXact: setting next multi to %u offset %u",
                 nextMulti, nextMultiOffset);
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
    (*MultiXactState).nextMXact = nextMulti;
    (*MultiXactState).nextOffset = nextMultiOffset;
    LWLockRelease(MultiXactGenLock());

    /*
     * During a binary upgrade, make sure that the offsets SLRU is large
     * enough to contain the next value that would be created.
     */
    if IsBinaryUpgrade {
        MaybeExtendOffsetSlru();
    }
}

/*
 * Determine the last safe MultiXactId to allocate given the currently oldest
 * datminmxid (ie, the oldest MultiXactId that might exist in any database
 * of our cluster), and the OID of the (or a) database with that value.
 *
 * is_startup is true when we are just starting the cluster, false when we
 * are updating state in a running cluster.  This only affects log messages.
 */
pub unsafe fn SetMultiXactIdLimit(
    oldest_datminmxid: MultiXactId,
    oldest_datoid: Oid,
    is_startup: bool,
) {
    let multiVacLimit: MultiXactId;
    let multiWarnLimit: MultiXactId;
    let mut multiStopLimit: MultiXactId;
    let mut multiWrapLimit: MultiXactId;
    let curMulti: MultiXactId;
    let needs_offset_vacuum: bool;

    Assert!(MultiXactIdIsValid(oldest_datminmxid));

    /*
     * We pretend that a wrap will happen halfway through the multixact ID
     * space, but that's not really true [...].
     */
    multiWrapLimit = oldest_datminmxid + (MaxMultiXactId >> 1);
    if multiWrapLimit < FirstMultiXactId {
        multiWrapLimit += FirstMultiXactId;
    }

    /*
     * We'll refuse to continue assigning MultiXactIds once we get within 3M
     * multi of data loss.  See SetTransactionIdLimit.
     */
    multiStopLimit = multiWrapLimit - 3000000;
    if multiStopLimit < FirstMultiXactId {
        multiStopLimit -= FirstMultiXactId;
    }

    /*
     * We'll start complaining loudly when we get within 40M multis of data
     * loss.
     */
    let mut multiWarnLimit_tmp: MultiXactId = multiWrapLimit - 40000000;
    if multiWarnLimit_tmp < FirstMultiXactId {
        multiWarnLimit_tmp -= FirstMultiXactId;
    }
    multiWarnLimit = multiWarnLimit_tmp;

    /*
     * We'll start trying to force autovacuums when oldest_datminmxid gets to
     * be more than autovacuum_multixact_freeze_max_age mxids old.
     */
    let mut multiVacLimit_tmp: MultiXactId =
        oldest_datminmxid + autovacuum_multixact_freeze_max_age as MultiXactId;
    if multiVacLimit_tmp < FirstMultiXactId {
        multiVacLimit_tmp += FirstMultiXactId;
    }
    multiVacLimit = multiVacLimit_tmp;

    /* Grab lock for just long enough to set the new limit values */
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
    (*MultiXactState).oldestMultiXactId = oldest_datminmxid;
    (*MultiXactState).oldestMultiXactDB = oldest_datoid;
    (*MultiXactState).multiVacLimit = multiVacLimit;
    (*MultiXactState).multiWarnLimit = multiWarnLimit;
    (*MultiXactState).multiStopLimit = multiStopLimit;
    (*MultiXactState).multiWrapLimit = multiWrapLimit;
    curMulti = (*MultiXactState).nextMXact;
    LWLockRelease(MultiXactGenLock());

    /* Log the info */
    ereport!(DEBUG1, errmsg!(
        "MultiXactId wrap limit is {}, limited by database with OID {}",
        multiWrapLimit, oldest_datoid));

    /*
     * Computing the actual limits is only possible once the data directory is
     * in a consistent state.
     */
    if !(*MultiXactState).finishedStartup {
        return;
    }

    Assert!(!InRecovery);

    /* Set limits for offset vacuum. */
    needs_offset_vacuum = SetOffsetVacuumLimit(is_startup);

    /*
     * If past the autovacuum force point, immediately signal an autovac
     * request.
     */
    if (MultiXactIdPrecedes(multiVacLimit, curMulti) || needs_offset_vacuum) && IsUnderPostmaster {
        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
    }

    /* Give an immediate warning if past the wrap warn point */
    if MultiXactIdPrecedes(multiWarnLimit, curMulti) {
        let oldest_datname: *mut c_char;

        /*
         * We can be called when not inside a transaction, for example during
         * StartupXLOG().  In such a case we cannot do database access, so we
         * must just report the oldest DB's OID.
         */
        if IsTransactionState() {
            oldest_datname = get_database_name(oldest_datoid);
        } else {
            oldest_datname = core::ptr::null_mut();
        }

        if !oldest_datname.is_null() {
            ereport!(WARNING, errmsg!(
                "database \"{}\" must be vacuumed before {} more MultiXactIds are used",
                std::ffi::CStr::from_ptr(oldest_datname).to_string_lossy(),
                multiWrapLimit - curMulti));
        } else {
            ereport!(WARNING, errmsg!(
                "database with OID {} must be vacuumed before {} more MultiXactIds are used",
                oldest_datoid, multiWrapLimit - curMulti));
        }
    }
}

/*
 * Ensure the next-to-be-assigned MultiXactId is at least minMulti,
 * and similarly nextOffset is at least minMultiOffset.
 */
pub unsafe fn MultiXactAdvanceNextMXact(minMulti: MultiXactId, minMultiOffset: MultiXactOffset) {
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
    if MultiXactIdPrecedes((*MultiXactState).nextMXact, minMulti) {
        debug_elog3!(DEBUG2, "MultiXact: setting next multi to %u", minMulti);
        (*MultiXactState).nextMXact = minMulti;
    }
    if MultiXactOffsetPrecedes((*MultiXactState).nextOffset, minMultiOffset) {
        debug_elog3!(DEBUG2, "MultiXact: setting next offset to %u", minMultiOffset);
        (*MultiXactState).nextOffset = minMultiOffset;
    }
    LWLockRelease(MultiXactGenLock());
}

/*
 * Update our oldestMultiXactId value, but only if it's more recent than what
 * we had.
 *
 * This may only be called during WAL replay.
 */
pub unsafe fn MultiXactAdvanceOldest(oldestMulti: MultiXactId, oldestMultiDB: Oid) {
    Assert!(InRecovery);

    if MultiXactIdPrecedes((*MultiXactState).oldestMultiXactId, oldestMulti) {
        SetMultiXactIdLimit(oldestMulti, oldestMultiDB, false);
    }
}

/*
 * Make sure that MultiXactOffset has room for a newly-allocated MultiXactId.
 *
 * NB: this is called while holding MultiXactGenLock.
 */
unsafe fn ExtendMultiXactOffset(multi: MultiXactId) {
    let pageno: int64;
    let lock: *mut LWLock;

    /*
     * No work except at first MultiXactId of a page.  But beware: just after
     * wraparound, the first MultiXactId of page zero is FirstMultiXactId.
     */
    if MultiXactIdToOffsetEntry(multi) != 0 && multi != FirstMultiXactId {
        return;
    }

    pageno = MultiXactIdToOffsetPage(multi);
    lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Zero the page and make an XLOG entry about it */
    ZeroMultiXactOffsetPage(pageno, true);

    LWLockRelease(lock);
}

/*
 * Make sure that MultiXactMember has room for the members of a newly-
 * allocated MultiXactId.
 *
 * Like the above routine, this is called while holding MultiXactGenLock;
 * same comments apply.
 */
unsafe fn ExtendMultiXactMember(mut offset: MultiXactOffset, mut nmembers: c_int) {
    /*
     * It's possible that the members span more than one page of the members
     * file, so we loop to ensure we consider each page.
     */
    while nmembers > 0 {
        let flagsoff: c_int;
        let flagsbit: c_int;
        let difference: uint32;

        /*
         * Only zero when at first entry of a page.
         */
        flagsoff = MXOffsetToFlagsOffset(offset);
        flagsbit = MXOffsetToFlagsBitShift(offset);
        if flagsoff == 0 && flagsbit == 0 {
            let pageno: int64;
            let lock: *mut LWLock;

            pageno = MXOffsetToMemberPage(offset);
            lock = SimpleLruGetBankLock(MultiXactMemberCtl(), pageno);

            LWLockAcquire(lock, LW_EXCLUSIVE);

            /* Zero the page and make an XLOG entry about it */
            ZeroMultiXactMemberPage(pageno, true);

            LWLockRelease(lock);
        }

        /*
         * Compute the number of items till end of current page.  Careful: if
         * addition of unsigned ints wraps around, we're at the last page of
         * the last segment; since that page holds a different number of items
         * than other pages, we need to do it differently.
         */
        if offset + MAX_MEMBERS_IN_LAST_MEMBERS_PAGE < offset {
            /*
             * This is the last page of the last segment; we can compute the
             * number of items left to allocate in it without modulo
             * arithmetic.
             */
            difference = MaxMultiXactOffset - offset + 1;
        } else {
            difference =
                MULTIXACT_MEMBERS_PER_PAGE as uint32 - offset % MULTIXACT_MEMBERS_PER_PAGE as uint32;
        }

        /*
         * Advance to next page, taking care to properly handle the wraparound
         * case.  OK if nmembers goes negative.
         */
        nmembers -= difference as c_int;
        offset = offset.wrapping_add(difference);
    }
}

/*
 * GetOldestMultiXactId
 *
 * Return the oldest MultiXactId that's still possibly still seen as live by
 * any running transaction.
 */
pub unsafe fn GetOldestMultiXactId() -> MultiXactId {
    let mut oldestMXact: MultiXactId;
    let mut nextMXact: MultiXactId;

    /*
     * This is the oldest valid value among all the OldestMemberMXactId[] and
     * OldestVisibleMXactId[] entries, or nextMXact if none are valid.
     */
    LWLockAcquire(MultiXactGenLock(), LW_SHARED);

    /*
     * We have to beware of the possibility that nextMXact is in the
     * wrapped-around state.
     */
    nextMXact = (*MultiXactState).nextMXact;
    if nextMXact < FirstMultiXactId {
        nextMXact = FirstMultiXactId;
    }

    oldestMXact = nextMXact;
    let mut i: c_int = 0;
    while i < NumMemberSlots() {
        let thisoldest: MultiXactId;

        thisoldest = *OldestMemberMXactId.add(i as usize);
        if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldestMXact) {
            oldestMXact = thisoldest;
        }
        i += 1;
    }
    let mut i: c_int = 0;
    while i < NumVisibleSlots() {
        let thisoldest: MultiXactId;

        thisoldest = *OldestVisibleMXactId.add(i as usize);
        if MultiXactIdIsValid(thisoldest) && MultiXactIdPrecedes(thisoldest, oldestMXact) {
            oldestMXact = thisoldest;
        }
        i += 1;
    }

    LWLockRelease(MultiXactGenLock());

    oldestMXact
}

/*
 * Determine how aggressively we need to vacuum in order to prevent member
 * wraparound.
 *
 * The return value is true if emergency autovacuum is required and false
 * otherwise.
 */
unsafe fn SetOffsetVacuumLimit(is_startup: bool) -> bool {
    let oldestMultiXactId: MultiXactId;
    let nextMXact: MultiXactId;
    let mut oldestOffset: MultiXactOffset = 0; /* placate compiler */
    let prevOldestOffset: MultiXactOffset;
    let nextOffset: MultiXactOffset;
    let mut oldestOffsetKnown: bool = false;
    let prevOldestOffsetKnown: bool;
    let mut offsetStopLimit: MultiXactOffset = 0;
    let prevOffsetStopLimit: MultiXactOffset;

    /*
     * NB: Have to prevent concurrent truncation, we might otherwise try to
     * lookup an oldestMulti that's concurrently getting truncated away.
     */
    LWLockAcquire(MultiXactTruncationLock(), LW_SHARED);

    /* Read relevant fields from shared memory. */
    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    oldestMultiXactId = (*MultiXactState).oldestMultiXactId;
    nextMXact = (*MultiXactState).nextMXact;
    nextOffset = (*MultiXactState).nextOffset;
    prevOldestOffsetKnown = (*MultiXactState).oldestOffsetKnown;
    prevOldestOffset = (*MultiXactState).oldestOffset;
    prevOffsetStopLimit = (*MultiXactState).offsetStopLimit;
    Assert!((*MultiXactState).finishedStartup);
    LWLockRelease(MultiXactGenLock());

    /*
     * Determine the offset of the oldest multixact.
     */
    if oldestMultiXactId == nextMXact {
        /*
         * When the next multixact gets created, it will be stored at the next
         * offset.
         */
        oldestOffset = nextOffset;
        oldestOffsetKnown = true;
    } else {
        /*
         * Figure out where the oldest existing multixact's offsets are
         * stored.
         */
        oldestOffsetKnown = find_multixact_start(oldestMultiXactId, &mut oldestOffset);

        if oldestOffsetKnown {
            ereport!(DEBUG1, errmsg!(
                "oldest MultiXactId member is at offset {}", oldestOffset));
        } else {
            ereport!(LOG, errmsg!(
                "MultiXact member wraparound protections are disabled because oldest checkpointed MultiXact {} does not exist on disk",
                oldestMultiXactId));
        }
    }

    LWLockRelease(MultiXactTruncationLock());

    /*
     * If we can, compute limits (and install them MultiXactState) to prevent
     * overrun of old data in the members SLRU area.
     */
    if oldestOffsetKnown {
        /* move back to start of the corresponding segment */
        offsetStopLimit = oldestOffset
            - (oldestOffset
                % (MULTIXACT_MEMBERS_PER_PAGE as MultiXactOffset
                    * SLRU_PAGES_PER_SEGMENT as MultiXactOffset));

        /* always leave one segment before the wraparound point */
        offsetStopLimit -= MULTIXACT_MEMBERS_PER_PAGE as MultiXactOffset
            * SLRU_PAGES_PER_SEGMENT as MultiXactOffset;

        if !prevOldestOffsetKnown && !is_startup {
            ereport!(LOG, errmsg!(
                "MultiXact member wraparound protections are now enabled"));
        }

        ereport!(DEBUG1, errmsg!(
            "MultiXact member stop limit is now {} based on MultiXact {}",
            offsetStopLimit, oldestMultiXactId));
    } else if prevOldestOffsetKnown {
        /*
         * If we failed to get the oldest offset this time, but we have a
         * value from a previous pass through this function, use the old
         * values rather than automatically forcing an emergency autovacuum
         * cycle again.
         */
        oldestOffset = prevOldestOffset;
        oldestOffsetKnown = true;
        offsetStopLimit = prevOffsetStopLimit;
    }

    /* Install the computed values */
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
    (*MultiXactState).oldestOffset = oldestOffset;
    (*MultiXactState).oldestOffsetKnown = oldestOffsetKnown;
    (*MultiXactState).offsetStopLimit = offsetStopLimit;
    LWLockRelease(MultiXactGenLock());

    /*
     * Do we need an emergency autovacuum?	If we're not sure, assume yes.
     */
    !oldestOffsetKnown || (nextOffset - oldestOffset > MULTIXACT_MEMBER_SAFE_THRESHOLD)
}

/*
 * Return whether adding "distance" to "start" would move past "boundary".
 */
unsafe fn MultiXactOffsetWouldWrap(
    boundary: MultiXactOffset,
    start: MultiXactOffset,
    distance: uint32,
) -> bool {
    let mut finish: MultiXactOffset;

    /*
     * Note that offset number 0 is not used (see GetMultiXactIdMembers), so
     * if the addition wraps around the UINT_MAX boundary, skip that value.
     */
    finish = start.wrapping_add(distance);
    if finish < start {
        finish += 1;
    }

    /*
     * When the boundary is numerically greater than the starting point, any
     * value numerically between the two is not wrapped [...].
     */
    if start < boundary {
        finish >= boundary || finish < start
    } else {
        finish >= boundary && finish < start
    }
}

/*
 * Find the starting offset of the given MultiXactId.
 *
 * Returns false if the file containing the multi does not exist on disk.
 * Otherwise, returns true and sets *result to the starting member offset.
 */
unsafe fn find_multixact_start(multi: MultiXactId, result: *mut MultiXactOffset) -> bool {
    let offset: MultiXactOffset;
    let pageno: int64;
    let entryno: c_int;
    let slotno: c_int;
    let mut offptr: *mut MultiXactOffset;

    Assert!((*MultiXactState).finishedStartup);

    pageno = MultiXactIdToOffsetPage(multi);
    entryno = MultiXactIdToOffsetEntry(multi);

    /*
     * Write out dirty data, so PhysicalPageExists can work correctly.
     */
    SimpleLruWriteAll(MultiXactOffsetCtl(), true);
    SimpleLruWriteAll(MultiXactMemberCtl(), true);

    if !SimpleLruDoesPhysicalPageExist(MultiXactOffsetCtl(), pageno) {
        return false;
    }

    /* lock is acquired by SimpleLruReadPage_ReadOnly */
    slotno = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl(), pageno, multi);
    offptr = *(*(*MultiXactOffsetCtl()).shared).page_buffer.add(slotno as usize) as *mut MultiXactOffset;
    offptr = offptr.add(entryno as usize);
    offset = *offptr;
    LWLockRelease(SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno));

    *result = offset;
    true
}

/*
 * Determine how many multixacts, and how many multixact members, currently
 * exist.  Return false if unable to determine.
 */
unsafe fn ReadMultiXactCounts(multixacts: *mut uint32, members: *mut MultiXactOffset) -> bool {
    let nextOffset: MultiXactOffset;
    let oldestOffset: MultiXactOffset;
    let oldestMultiXactId: MultiXactId;
    let nextMultiXactId: MultiXactId;
    let oldestOffsetKnown: bool;

    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    nextOffset = (*MultiXactState).nextOffset;
    oldestMultiXactId = (*MultiXactState).oldestMultiXactId;
    nextMultiXactId = (*MultiXactState).nextMXact;
    oldestOffset = (*MultiXactState).oldestOffset;
    oldestOffsetKnown = (*MultiXactState).oldestOffsetKnown;
    LWLockRelease(MultiXactGenLock());

    if !oldestOffsetKnown {
        return false;
    }

    *members = nextOffset - oldestOffset;
    *multixacts = nextMultiXactId - oldestMultiXactId;
    true
}

/*
 * Multixact members can be removed once the multixacts that refer to them
 * are older than every datminmxid.  [...]
 */
pub unsafe fn MultiXactMemberFreezeThreshold() -> c_int {
    let mut members: MultiXactOffset = 0;
    let mut multixacts: uint32 = 0;
    let victim_multixacts: uint32;
    let fraction: f64;
    let result: c_int;

    /* If we can't determine member space utilization, assume the worst. */
    if !ReadMultiXactCounts(&mut multixacts, &mut members) {
        return 0;
    }

    /* If member space utilization is low, no special action is required. */
    if members <= MULTIXACT_MEMBER_SAFE_THRESHOLD {
        return autovacuum_multixact_freeze_max_age;
    }

    /*
     * Compute a target for relminmxid advancement.
     */
    fraction = (members - MULTIXACT_MEMBER_SAFE_THRESHOLD) as f64
        / (MULTIXACT_MEMBER_DANGER_THRESHOLD - MULTIXACT_MEMBER_SAFE_THRESHOLD) as f64;
    victim_multixacts = (multixacts as f64 * fraction) as uint32;

    /* fraction could be > 1.0, but lowest possible freeze age is zero */
    if victim_multixacts > multixacts {
        return 0;
    }
    result = (multixacts - victim_multixacts) as c_int;

    /*
     * Clamp to autovacuum_multixact_freeze_max_age, so that we never make
     * autovacuum less aggressive than it would otherwise be.
     */
    Min(result, autovacuum_multixact_freeze_max_age)
}

#[repr(C)]
pub struct mxtruncinfo {
    pub earliestExistingPage: int64,
}

/*
 * SlruScanDirectory callback
 *		This callback determines the earliest existing page number.
 */
unsafe extern "C" fn SlruScanDirCbFindEarliest(
    ctl: SlruCtl,
    _filename: *mut c_char,
    segpage: int64,
    data: *mut c_void,
) -> bool {
    let trunc: *mut mxtruncinfo = data as *mut mxtruncinfo;

    if (*trunc).earliestExistingPage == -1
        || (*ctl).PagePrecedes.unwrap()(segpage, (*trunc).earliestExistingPage)
    {
        (*trunc).earliestExistingPage = segpage;
    }

    false /* keep going */
}

/*
 * Delete members segments [oldest, newOldest)
 */
unsafe fn PerformMembersTruncation(
    oldestOffset: MultiXactOffset,
    newOldestOffset: MultiXactOffset,
) {
    let maxsegment: int64 = MXOffsetToMemberSegment(MaxMultiXactOffset);
    let startsegment: int64 = MXOffsetToMemberSegment(oldestOffset);
    let endsegment: int64 = MXOffsetToMemberSegment(newOldestOffset);
    let mut segment: int64 = startsegment;

    /*
     * Delete all the segments but the last one. The last segment can still
     * contain, possibly partially, valid data.
     */
    while segment != endsegment {
        elog!(DEBUG2, "truncating multixact members segment {:x}", segment);
        SlruDeleteSegment(MultiXactMemberCtl(), segment);

        /* move to next segment, handling wraparound correctly */
        if segment == maxsegment {
            segment = 0;
        } else {
            segment += 1;
        }
    }
}

/*
 * Delete offsets segments [oldest, newOldest)
 */
unsafe fn PerformOffsetsTruncation(_oldestMulti: MultiXactId, newOldestMulti: MultiXactId) {
    /*
     * We step back one multixact to avoid passing a cutoff page that hasn't
     * been created yet [...].
     */
    SimpleLruTruncate(
        MultiXactOffsetCtl(),
        MultiXactIdToOffsetPage(PreviousMultiXactId(newOldestMulti)),
    );
}

/*
 * Remove all MultiXactOffset and MultiXactMember segments before the oldest
 * ones still of interest.
 */
pub unsafe fn TruncateMultiXact(newOldestMulti: MultiXactId, newOldestMultiDB: Oid) {
    let oldestMulti: MultiXactId;
    let nextMulti: MultiXactId;
    let newOldestOffset: MultiXactOffset;
    let mut oldestOffset: MultiXactOffset = 0;
    let nextOffset: MultiXactOffset;
    let mut trunc: mxtruncinfo = core::mem::zeroed();
    let mut earliest: MultiXactId;

    Assert!(!RecoveryInProgress());
    Assert!((*MultiXactState).finishedStartup);

    /*
     * We can only allow one truncation to happen at once.
     */
    LWLockAcquire(MultiXactTruncationLock(), LW_EXCLUSIVE);

    LWLockAcquire(MultiXactGenLock(), LW_SHARED);
    nextMulti = (*MultiXactState).nextMXact;
    nextOffset = (*MultiXactState).nextOffset;
    oldestMulti = (*MultiXactState).oldestMultiXactId;
    LWLockRelease(MultiXactGenLock());
    Assert!(MultiXactIdIsValid(oldestMulti));

    /*
     * Make sure to only attempt truncation if there's values to truncate
     * away.
     */
    if MultiXactIdPrecedesOrEquals(newOldestMulti, oldestMulti) {
        LWLockRelease(MultiXactTruncationLock());
        return;
    }

    /*
     * Note we can't just plow ahead with the truncation [...].  So we first
     * scan the directory to determine the earliest offsets page number that
     * we can read without error.
     */
    trunc.earliestExistingPage = -1;
    SlruScanDirectory(
        MultiXactOffsetCtl(),
        SlruScanDirCbFindEarliest,
        core::ptr::addr_of_mut!(trunc) as *mut c_void,
    );
    earliest = (trunc.earliestExistingPage * MULTIXACT_OFFSETS_PER_PAGE as int64) as MultiXactId;
    if earliest < FirstMultiXactId {
        earliest = FirstMultiXactId;
    }

    /* If there's nothing to remove, we can bail out early. */
    if MultiXactIdPrecedes(oldestMulti, earliest) {
        LWLockRelease(MultiXactTruncationLock());
        return;
    }

    /*
     * First, compute the safe truncation point for MultiXactMember. This is
     * the starting offset of the oldest multixact.
     */
    if oldestMulti == nextMulti {
        /* there are NO MultiXacts */
        oldestOffset = nextOffset;
    } else if !find_multixact_start(oldestMulti, &mut oldestOffset) {
        ereport!(LOG, errmsg!(
            "oldest MultiXact {} not found, earliest MultiXact {}, skipping truncation",
            oldestMulti, earliest));
        LWLockRelease(MultiXactTruncationLock());
        return;
    }

    /*
     * Secondly compute up to where to truncate. Lookup the corresponding
     * member offset for newOldestMulti for that.
     */
    let mut newOldestOffset_tmp: MultiXactOffset = 0;
    if newOldestMulti == nextMulti {
        /* there are NO MultiXacts */
        newOldestOffset_tmp = nextOffset;
    } else if !find_multixact_start(newOldestMulti, &mut newOldestOffset_tmp) {
        ereport!(LOG, errmsg!(
            "cannot truncate up to MultiXact {} because it does not exist on disk, skipping truncation",
            newOldestMulti));
        LWLockRelease(MultiXactTruncationLock());
        return;
    }
    newOldestOffset = newOldestOffset_tmp;

    /*
     * On crash, MultiXactIdCreateFromMembers() can leave behind multixids
     * that were not yet written out and hence have zero offset on disk.
     */
    if newOldestOffset == 0 {
        ereport!(LOG, errmsg!(
            "cannot truncate up to MultiXact {} because it has invalid offset, skipping truncation",
            newOldestMulti));
        LWLockRelease(MultiXactTruncationLock());
        return;
    }

    elog!(DEBUG1, "performing multixact truncation: offsets [{}, {}), offsets segments [{:x}, {:x}), members [{}, {}), members segments [{:x}, {:x})",
        oldestMulti, newOldestMulti,
        MultiXactIdToOffsetSegment(oldestMulti),
        MultiXactIdToOffsetSegment(newOldestMulti),
        oldestOffset, newOldestOffset,
        MXOffsetToMemberSegment(oldestOffset),
        MXOffsetToMemberSegment(newOldestOffset));

    /*
     * Do truncation, and the WAL logging of the truncation, in a critical
     * section.
     */
    START_CRIT_SECTION();

    /*
     * Prevent checkpoints from being scheduled concurrently.
     */
    Assert!(((*MyProc).delayChkptFlags & DELAY_CHKPT_START) == 0);
    (*MyProc).delayChkptFlags |= DELAY_CHKPT_START;

    /* WAL log truncation */
    WriteMTruncateXlogRec(
        newOldestMultiDB,
        oldestMulti,
        newOldestMulti,
        oldestOffset,
        newOldestOffset,
    );

    /*
     * Update in-memory limits before performing the truncation, while inside
     * the critical section.
     */
    LWLockAcquire(MultiXactGenLock(), LW_EXCLUSIVE);
    (*MultiXactState).oldestMultiXactId = newOldestMulti;
    (*MultiXactState).oldestMultiXactDB = newOldestMultiDB;
    LWLockRelease(MultiXactGenLock());

    /* First truncate members */
    PerformMembersTruncation(oldestOffset, newOldestOffset);

    /* Then offsets */
    PerformOffsetsTruncation(oldestMulti, newOldestMulti);

    (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;

    END_CRIT_SECTION();
    LWLockRelease(MultiXactTruncationLock());
}

/*
 * Decide whether a MultiXactOffset page number is "older" for truncation
 * purposes.  Analogous to CLOGPagePrecedes().
 */
unsafe extern "C" fn MultiXactOffsetPagePrecedes(page1: int64, page2: int64) -> bool {
    let mut multi1: MultiXactId;
    let mut multi2: MultiXactId;

    multi1 = (page1 as MultiXactId).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE as MultiXactId);
    multi1 = multi1.wrapping_add(FirstMultiXactId + 1);
    multi2 = (page2 as MultiXactId).wrapping_mul(MULTIXACT_OFFSETS_PER_PAGE as MultiXactId);
    multi2 = multi2.wrapping_add(FirstMultiXactId + 1);

    MultiXactIdPrecedes(multi1, multi2)
        && MultiXactIdPrecedes(
            multi1,
            multi2.wrapping_add(MULTIXACT_OFFSETS_PER_PAGE as MultiXactId - 1),
        )
}

/*
 * Decide whether a MultiXactMember page number is "older" for truncation
 * purposes.  There is no "invalid offset number" so use the numbers verbatim.
 */
unsafe extern "C" fn MultiXactMemberPagePrecedes(page1: int64, page2: int64) -> bool {
    let offset1: MultiXactOffset;
    let offset2: MultiXactOffset;

    offset1 = (page1 as MultiXactOffset).wrapping_mul(MULTIXACT_MEMBERS_PER_PAGE as MultiXactOffset);
    offset2 = (page2 as MultiXactOffset).wrapping_mul(MULTIXACT_MEMBERS_PER_PAGE as MultiXactOffset);

    MultiXactOffsetPrecedes(offset1, offset2)
        && MultiXactOffsetPrecedes(
            offset1,
            offset2.wrapping_add(MULTIXACT_MEMBERS_PER_PAGE as MultiXactOffset - 1),
        )
}

/*
 * Decide which of two MultiXactIds is earlier.
 */
pub unsafe fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    let diff: int32 = multi1.wrapping_sub(multi2) as int32;

    diff < 0
}

/*
 * MultiXactIdPrecedesOrEquals -- is multi1 logically <= multi2?
 */
pub unsafe fn MultiXactIdPrecedesOrEquals(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    let diff: int32 = multi1.wrapping_sub(multi2) as int32;

    diff <= 0
}

/*
 * Decide which of two offsets is earlier.
 */
unsafe fn MultiXactOffsetPrecedes(offset1: MultiXactOffset, offset2: MultiXactOffset) -> bool {
    let diff: int32 = offset1.wrapping_sub(offset2) as int32;

    diff < 0
}

/*
 * Write an xlog record reflecting the zeroing of either a MEMBERs or
 * OFFSETs page (info shows which)
 */
unsafe fn WriteMZeroPageXlogRec(pageno: int64, info: uint8) {
    XLogBeginInsert();
    XLogRegisterData(
        core::ptr::addr_of!(pageno) as *mut c_char,
        core::mem::size_of_val(&pageno),
    );
    XLogInsert(RM_MULTIXACT_ID, info);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes in
 * TruncateCLOG().
 */
unsafe fn WriteMTruncateXlogRec(
    oldestMultiDB: Oid,
    startTruncOff: MultiXactId,
    endTruncOff: MultiXactId,
    startTruncMemb: MultiXactOffset,
    endTruncMemb: MultiXactOffset,
) {
    let recptr: XLogRecPtr;
    let mut xlrec: xl_multixact_truncate = core::mem::zeroed();

    xlrec.oldestMultiDB = oldestMultiDB;

    xlrec.startTruncOff = startTruncOff;
    xlrec.endTruncOff = endTruncOff;

    xlrec.startTruncMemb = startTruncMemb;
    xlrec.endTruncMemb = endTruncMemb;

    XLogBeginInsert();
    XLogRegisterData(
        core::ptr::addr_of!(xlrec) as *mut c_char,
        SizeOfMultiXactTruncate,
    );
    recptr = XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID);
    XLogFlush(recptr);
}

/*
 * MULTIXACT resource manager's routines
 */
pub unsafe fn multixact_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in multixact records */
    Assert!(!XLogRecHasAnyBlockRefs(record));

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE {
        let mut pageno: int64 = 0;
        let slotno: c_int;
        let lock: *mut LWLock;

        memcpy(
            core::ptr::addr_of_mut!(pageno) as *mut c_void,
            XLogRecGetData(record) as *const c_void,
            core::mem::size_of_val(&pageno),
        );

        /*
         * Skip the record if we already initialized the page at the previous
         * XLOG_MULTIXACT_CREATE_ID record. See RecordNewMultiXact().
         */
        if pre_initialized_offsets_page != pageno {
            lock = SimpleLruGetBankLock(MultiXactOffsetCtl(), pageno);
            LWLockAcquire(lock, LW_EXCLUSIVE);

            slotno = ZeroMultiXactOffsetPage(pageno, false);
            SimpleLruWritePage(MultiXactOffsetCtl(), slotno);
            Assert!(!*(*(*MultiXactOffsetCtl()).shared).page_dirty.add(slotno as usize));

            LWLockRelease(lock);
        } else {
            elog!(DEBUG1, "skipping initialization of offsets page {} because it was already initialized on multixid creation", pageno);
        }
        pre_initialized_offsets_page = -1;
    } else if info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        let mut pageno: int64 = 0;
        let slotno: c_int;
        let lock: *mut LWLock;

        memcpy(
            core::ptr::addr_of_mut!(pageno) as *mut c_void,
            XLogRecGetData(record) as *const c_void,
            core::mem::size_of_val(&pageno),
        );

        lock = SimpleLruGetBankLock(MultiXactMemberCtl(), pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        slotno = ZeroMultiXactMemberPage(pageno, false);
        SimpleLruWritePage(MultiXactMemberCtl(), slotno);
        Assert!(!*(*(*MultiXactMemberCtl()).shared).page_dirty.add(slotno as usize));

        LWLockRelease(lock);
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        let xlrec: *mut xl_multixact_create = XLogRecGetData(record) as *mut xl_multixact_create;
        let mut max_xid: TransactionId;
        let mut i: c_int;

        if pre_initialized_offsets_page != -1 {
            /*
             * If we implicitly initialized the next offsets page while
             * replaying an XLOG_MULTIXACT_CREATE_ID record that was generated
             * with an older minor version, we still expect to see an
             * XLOG_MULTIXACT_ZERO_OFF_PAGE record for it [...].
             */
            elog!(LOG, "expected to see an XLOG_MULTIXACT_ZERO_OFF_PAGE record for page {} that was implicitly initialized earlier",
                pre_initialized_offsets_page);
            pre_initialized_offsets_page = -1;
        }

        /* Store the data back into the SLRU files */
        RecordNewMultiXact(
            (*xlrec).mid,
            (*xlrec).moff,
            (*xlrec).nmembers,
            (*xlrec).members.as_mut_ptr(),
        );

        /* Make sure nextMXact/nextOffset are beyond what this record has */
        MultiXactAdvanceNextMXact(
            (*xlrec).mid + 1,
            (*xlrec).moff + (*xlrec).nmembers as MultiXactOffset,
        );

        /*
         * Make sure nextXid is beyond any XID mentioned in the record.
         */
        max_xid = XLogRecGetXid(record);
        i = 0;
        while i < (*xlrec).nmembers {
            let memberxid: TransactionId = (*(*xlrec).members.as_mut_ptr().add(i as usize)).xid;
            if TransactionIdPrecedes(max_xid, memberxid) {
                max_xid = memberxid;
            }
            i += 1;
        }

        AdvanceNextFullTransactionIdPastXid(max_xid);
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        let mut xlrec: xl_multixact_truncate = core::mem::zeroed();

        memcpy(
            core::ptr::addr_of_mut!(xlrec) as *mut c_void,
            XLogRecGetData(record) as *const c_void,
            SizeOfMultiXactTruncate,
        );

        elog!(DEBUG1, "replaying multixact truncation: offsets [{}, {}), offsets segments [{:x}, {:x}), members [{}, {}), members segments [{:x}, {:x})",
            xlrec.startTruncOff, xlrec.endTruncOff,
            MultiXactIdToOffsetSegment(xlrec.startTruncOff),
            MultiXactIdToOffsetSegment(xlrec.endTruncOff),
            xlrec.startTruncMemb, xlrec.endTruncMemb,
            MXOffsetToMemberSegment(xlrec.startTruncMemb),
            MXOffsetToMemberSegment(xlrec.endTruncMemb));

        /* should not be required, but more than cheap enough */
        LWLockAcquire(MultiXactTruncationLock(), LW_EXCLUSIVE);

        /*
         * Advance the horizon values, so they're current at the end of
         * recovery.
         */
        SetMultiXactIdLimit(xlrec.endTruncOff, xlrec.oldestMultiDB, false);

        PerformMembersTruncation(xlrec.startTruncMemb, xlrec.endTruncMemb);
        PerformOffsetsTruncation(xlrec.startTruncOff, xlrec.endTruncOff);

        LWLockRelease(MultiXactTruncationLock());
    } else {
        elog!(PANIC, "multixact_redo: unknown op code {}", info);
    }
}

#[repr(C)]
struct pg_get_multixact_members_mxact {
    members: *mut MultiXactMember,
    nmembers: c_int,
    iter: c_int,
}

pub unsafe fn pg_get_multixact_members(fcinfo: FunctionCallInfo) -> Datum {
    type mxact = pg_get_multixact_members_mxact;
    let mxid: MultiXactId = PG_GETARG_TRANSACTIONID(fcinfo, 0);
    let mut multi: *mut mxact;
    let funccxt: *mut FuncCallContext;

    if mxid < FirstMultiXactId {
        ereport!(ERROR, errmsg!("invalid MultiXactId: {}", mxid));
    }

    if SRF_IS_FIRSTCALL() {
        let oldcxt: MemoryContext;
        let mut tupdesc: TupleDesc = core::ptr::null_mut();

        let funccxt0 = SRF_FIRSTCALL_INIT();
        oldcxt = MemoryContextSwitchTo((*funccxt0).multi_call_memory_ctx);

        multi = palloc(core::mem::size_of::<mxact>()) as *mut mxact;
        /* no need to allow for old values here */
        (*multi).nmembers =
            GetMultiXactIdMembers(mxid, &mut (*multi).members, false, false);
        (*multi).iter = 0;

        if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
            elog!(ERROR, "return type must be a row type");
        }
        (*funccxt0).tuple_desc = tupdesc;
        (*funccxt0).attinmeta = TupleDescGetAttInMetadata(tupdesc);
        (*funccxt0).user_fctx = multi as *mut c_void;

        MemoryContextSwitchTo(oldcxt);
    }

    funccxt = SRF_PERCALL_SETUP();
    multi = (*funccxt).user_fctx as *mut mxact;

    while (*multi).iter < (*multi).nmembers {
        let tuple: HeapTuple;
        let mut values: [*mut c_char; 2] = [core::ptr::null_mut(); 2];

        values[0] = psprintf(
            c"%u".as_ptr(),
            (*(*multi).members.add((*multi).iter as usize)).xid,
        );
        values[1] = mxstatus_to_string((*(*multi).members.add((*multi).iter as usize)).status);

        tuple = BuildTupleFromCStrings((*funccxt).attinmeta, values.as_mut_ptr());

        (*multi).iter += 1;
        pfree(values[0] as *mut c_void);
        SRF_RETURN_NEXT(funccxt, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funccxt)
}

/*
 * Entrypoint for sync.c to sync offsets files.
 */
pub unsafe fn multixactoffsetssyncfiletag(ftag: *const FileTag, path: *mut c_char) -> c_int {
    SlruSyncFileTag(MultiXactOffsetCtl(), ftag, path)
}

/*
 * Entrypoint for sync.c to sync members files.
 */
pub unsafe fn multixactmemberssyncfiletag(ftag: *const FileTag, path: *mut c_char) -> c_int {
    SlruSyncFileTag(MultiXactMemberCtl(), ftag, path)
}

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies.
//
// multixact.c is built on the SLRU layer (access/slru.c) which is not yet
// ported; the siblings clog.rs / commit_ts.rs carry the same self-contained
// stub set, so we mirror it here.  Each stub is marked with the C file where
// the real symbol lives.
// ----------------------------------------------------------------------------

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn psprintf(fmt: *const c_char, ...) -> *mut c_char; // TODO(pg-port): real psprintf lives in lib/psprintf.c
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );
}

// TODO(pg-port): real MemSet lives in c.h (utils macro)
#[inline]
unsafe fn MemSet(start: *mut c_void, val: c_int, len: Size) {
    extern "C" {
        fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    }
    memset(start, val, len as usize);
}

// Critical section / interrupt macros --- TODO(pg-port): real defs in miscadmin.h
#[inline]
unsafe fn START_CRIT_SECTION() {} // TODO(pg-port): real START_CRIT_SECTION lives in miscadmin.h
#[inline]
unsafe fn END_CRIT_SECTION() {} // TODO(pg-port): real END_CRIT_SECTION lives in miscadmin.h

// Injection points --- TODO(pg-port): real defs in utils/injection_point.h
#[inline]
unsafe fn INJECTION_POINT_LOAD(_name: *const c_char) {}
#[inline]
unsafe fn INJECTION_POINT_CACHED(_name: *const c_char, _arg: *mut c_void) {}

// memory mgmt helpers --- TODO(pg-port): real palloc/pfree live in utils/mmgr/mcxt.c
unsafe fn palloc(size: Size) -> *mut c_void {
    crate::utils::palloc::palloc(size)
}
unsafe fn pfree(ptr: *mut c_void) {
    crate::utils::palloc::pfree(ptr)
}
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // TODO(pg-port): real add_size lives in storage/ipc/shmem.c (with overflow check)
}
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2 // TODO(pg-port): real mul_size lives in storage/ipc/shmem.c (with overflow check)
}

pub type MemoryContext = *mut c_void; // TODO(pg-port): real MemoryContext lives in utils/palloc.h / nodes/memnodes.h
#[no_mangle]
pub static mut TopTransactionContext: MemoryContext = core::ptr::null_mut(); // TODO(pg-port): real TopTransactionContext lives in utils/mmgr/mcxt.c
pub static mut TopMemoryContext: MemoryContext = core::ptr::null_mut(); // TODO(pg-port): real TopMemoryContext lives in utils/mmgr/mcxt.c
pub const ALLOCSET_SMALL_SIZES: c_int = 0; // TODO(pg-port): real macro lives in utils/memutils.h
unsafe fn AllocSetContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    _sizes: c_int,
) -> MemoryContext {
    // ALLOCSET_SMALL_SIZES (utils/memutils.h) expands to the small-sizes triple.
    crate::utils::mmgr::aset::AllocSetContextCreate(
        parent as _,
        name,
        crate::utils::memutils::ALLOCSET_SMALL_SIZES,
    ) as MemoryContext
}
unsafe fn MemoryContextAlloc(context: MemoryContext, size: Size) -> *mut c_void {
    crate::utils::mmgr::mcxt::MemoryContextAlloc(context as _, size)
}
unsafe fn MemoryContextStrdup(context: MemoryContext, str: *const c_char) -> *mut c_char {
    crate::utils::mmgr::mcxt::MemoryContextStrdup(context as _, str)
}
unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    crate::utils::mmgr::mcxt::MemoryContextSwitchTo(context as _) as MemoryContext
}

// StringInfo --- TODO(pg-port): real defs live in lib/stringinfo.h
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): real initStringInfo lives in lib/stringinfo.c
}
unsafe fn appendStringInfo(_str: *mut StringInfoData, _fmt: *const c_char) {
    unimplemented!() // TODO(pg-port): real appendStringInfo lives in lib/stringinfo.c
}
unsafe fn appendStringInfoChar(_str: *mut StringInfoData, _ch: c_char) {
    unimplemented!() // TODO(pg-port): real appendStringInfoChar lives in lib/stringinfo.c
}

// ilist (doubly-linked lists) --- TODO(pg-port): real defs live in lib/ilist.h
#[repr(C)]
pub struct dlist_node {
    pub prev: *mut dlist_node,
    pub next: *mut dlist_node,
}
#[repr(C)]
pub struct dlist_head {
    pub head: dlist_node,
}
#[repr(C)]
pub struct dclist_head {
    pub dlist: dlist_head,
    pub count: u32,
}
#[repr(C)]
pub struct dlist_iter {
    pub end: *mut dlist_node,
    pub cur: *mut dlist_node,
}

const fn DCLIST_STATIC_INIT() -> dclist_head {
    dclist_head {
        dlist: dlist_head {
            head: dlist_node {
                prev: core::ptr::null_mut(),
                next: core::ptr::null_mut(),
            },
        },
        count: 0,
    }
}

// dclist_container(type, membername, ptr): offsetof-based downcast
macro_rules! dclist_container {
    ($t:ty, $member:ident, $ptr:expr) => {
        ($ptr as *mut u8).sub(core::mem::offset_of!($t, $member)) as *mut $t
    };
}
use dclist_container;

// dclist_foreach(iter, lhead) { ... } --- TODO(pg-port): real macro in lib/ilist.h
macro_rules! dclist_foreach {
    ($iter:ident, $lhead:expr, $body:block) => {
        $iter.end = core::ptr::addr_of_mut!((*$lhead).dlist.head);
        $iter.cur = if (*$iter.end).next.is_null() {
            $iter.end
        } else {
            (*$iter.end).next
        };
        while $iter.cur != $iter.end {
            let _next = (*$iter.cur).next;
            $body
            $iter.cur = _next;
        }
    };
}
use dclist_foreach;

unsafe fn dclist_init(_head: *mut dclist_head) {
    unimplemented!() // TODO(pg-port): real dclist_init lives in lib/ilist.h
}
unsafe fn dclist_push_head(_head: *mut dclist_head, _node: *mut dlist_node) {
    unimplemented!() // TODO(pg-port): real dclist_push_head lives in lib/ilist.h
}
unsafe fn dclist_count(_head: *mut dclist_head) -> Size {
    unimplemented!() // TODO(pg-port): real dclist_count lives in lib/ilist.h
}
unsafe fn dclist_tail_node(_head: *mut dclist_head) -> *mut dlist_node { unimplemented!() }
unsafe fn dclist_delete_from(_head: *mut dclist_head, _node: *mut dlist_node) { crate::lib::ilist::dclist_delete_from(_head as _, _node as _) }
unsafe fn dclist_move_head(_head: *mut dclist_head, _node: *mut dlist_node) { crate::lib::ilist::dclist_move_head(_head as _, _node as _) }

// SLRU --- TODO(pg-port): real definitions live in access/slru.h / slru.c -----
pub const SLRU_PAGES_PER_SEGMENT: c_int = 32; // TODO(pg-port): real value lives in access/slru.h

#[repr(C)]
pub struct SlruSharedData {
    pub page_buffer: *mut *mut c_char,
    pub page_dirty: *mut bool,
    pub page_number: *mut int64,
    pub latest_page_number: pg_atomic_uint64,
    // ... TODO(pg-port): access/slru.h
}
pub type SlruShared = *mut SlruSharedData;

pub type SlruPagePrecedesFunction = unsafe extern "C" fn(int64, int64) -> bool;

pub use crate::access::transam::slru::{SlruCtlData, SlruCtl};

pub type SlruScanCallback =
    unsafe extern "C" fn(SlruCtl, *mut c_char, int64, *mut c_void) -> bool;

unsafe fn SimpleLruGetBankLock(ctl: SlruCtl, pageno: int64) -> *mut LWLock {
    crate::access::transam::slru::SimpleLruGetBankLock(ctl as _, pageno) as *mut LWLock
}
unsafe fn SimpleLruReadPage(
    ctl: SlruCtl,
    pageno: int64,
    write_ok: bool,
    xid: TransactionId,
) -> c_int {
    crate::access::transam::slru::SimpleLruReadPage(ctl as _, pageno, write_ok, xid)
}
unsafe fn SimpleLruReadPage_ReadOnly(ctl: SlruCtl, pageno: int64, xid: TransactionId) -> c_int {
    crate::access::transam::slru::SimpleLruReadPage_ReadOnly(ctl as _, pageno, xid)
}
unsafe fn SimpleLruZeroPage(ctl: SlruCtl, pageno: int64) -> c_int {
    crate::access::transam::slru::SimpleLruZeroPage(ctl as _, pageno)
}
unsafe fn SimpleLruWritePage(ctl: SlruCtl, slotno: c_int) {
    crate::access::transam::slru::SimpleLruWritePage(ctl as _, slotno)
}
unsafe fn SimpleLruWriteAll(ctl: SlruCtl, allow_redirtied: bool) {
    crate::access::transam::slru::SimpleLruWriteAll(ctl as _, allow_redirtied)
}
unsafe fn SimpleLruTruncate(ctl: SlruCtl, cutoffPage: int64) {
    crate::access::transam::slru::SimpleLruTruncate(ctl as _, cutoffPage)
}
unsafe fn SimpleLruDoesPhysicalPageExist(ctl: SlruCtl, pageno: int64) -> bool {
    crate::access::transam::slru::SimpleLruDoesPhysicalPageExist(ctl as _, pageno)
}
unsafe fn SimpleLruInit(
    ctl: SlruCtl,
    name: *const c_char,
    nslots: c_int,
    nlsns: c_int,
    subdir: *const c_char,
    buffer_tranche_id: c_int,
    bank_tranche_id: c_int,
    sync_handler: c_int,
    long_segment_names: bool,
) {
    crate::access::transam::slru::SimpleLruInit(
        ctl as _,
        name,
        nslots,
        nlsns,
        subdir,
        buffer_tranche_id,
        bank_tranche_id,
        sync_handler,
        long_segment_names,
    )
}
unsafe fn SimpleLruShmemSize(nslots: c_int, nlsns: c_int) -> Size {
    crate::access::transam::slru::SimpleLruShmemSize(nslots, nlsns)
}
unsafe fn SlruPagePrecedesUnitTests(ctl: SlruCtl, per_page: c_int) {
    #[cfg(debug_assertions)] crate::access::transam::slru::SlruPagePrecedesUnitTests(ctl as _, per_page);
}
unsafe fn SlruScanDirectory(ctl: SlruCtl, callback: SlruScanCallback, data: *mut c_void) -> bool {
    crate::access::transam::slru::SlruScanDirectory(
        ctl as _,
        core::mem::transmute::<
            SlruScanCallback,
            crate::access::transam::slru::SlruScanCallback,
        >(callback),
        data,
    )
}
unsafe fn SlruDeleteSegment(ctl: SlruCtl, segno: int64) {
    crate::access::transam::slru::SlruDeleteSegment(ctl as _, segno)
}
unsafe fn SlruSyncFileTag(ctl: SlruCtl, ftag: *const FileTag, path: *mut c_char) -> c_int {
    crate::access::transam::slru::SlruSyncFileTag(ctl as _, ftag as _, path)
}
unsafe fn check_slru_buffers(name: *const c_char, newval: *mut c_int) -> bool {
    crate::access::transam::slru::check_slru_buffers(name, newval)
}

// GUC variables --- TODO(pg-port): real defs live in access/slru.c / utils/misc/guc_tables.c
extern "C" {
    pub static mut multixact_offset_buffers: c_int; // canonical: utils/init/globals.rs
    pub static mut multixact_member_buffers: c_int; // canonical: utils/init/globals.rs
}
pub static mut autovacuum_multixact_freeze_max_age: c_int = 0; // TODO(pg-port): postmaster/autovacuum.c

// GUC types -------------------------------------------------------------------
pub type GucSource = c_int; // TODO(pg-port): real GucSource lives in utils/guc.h

// LWLock --- TODO(pg-port): real definitions live in storage/lwlock.h ---------
#[repr(C)]
pub struct LWLock {
    _private: [u8; 0],
}

pub const LW_EXCLUSIVE: c_int = 0; // TODO(pg-port): real value lives in storage/lwlock.h
pub const LW_SHARED: c_int = 1; // TODO(pg-port): real value lives in storage/lwlock.h

pub const LWTRANCHE_MULTIXACTOFFSET_BUFFER: c_int = 0; // TODO(pg-port): storage/lwlock.h
pub const LWTRANCHE_MULTIXACTOFFSET_SLRU: c_int = 0; // TODO(pg-port): storage/lwlock.h
pub const LWTRANCHE_MULTIXACTMEMBER_BUFFER: c_int = 0; // TODO(pg-port): storage/lwlock.h
pub const LWTRANCHE_MULTIXACTMEMBER_SLRU: c_int = 0; // TODO(pg-port): storage/lwlock.h

pub const SYNC_HANDLER_MULTIXACT_OFFSET: c_int = 0; // TODO(pg-port): storage/sync.h
pub const SYNC_HANDLER_MULTIXACT_MEMBER: c_int = 0; // TODO(pg-port): storage/sync.h

// Named LWLocks --- TODO(pg-port): real ones live in storage/lwlocknames.h
unsafe fn MultiXactGenLock() -> *mut LWLock {
    // GetNamedLWLock(MultiXactGenLock) == &MainLWLockArray[id].lock
    &raw mut (*crate::storage::lmgr::lwlock::MainLWLockArray
        .add(crate::storage::lwlocklist::MultiXactGen_LWLOCK_ID as usize))
    .lock as *mut LWLock
}
unsafe fn MultiXactTruncationLock() -> *mut LWLock {
    &raw mut (*crate::storage::lmgr::lwlock::MainLWLockArray
        .add(crate::storage::lwlocklist::MultiXactTruncation_LWLOCK_ID as usize))
    .lock as *mut LWLock
}

unsafe fn LWLockAcquire(lock: *mut LWLock, mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(
        lock as _,
        core::mem::transmute::<c_int, crate::storage::lmgr::lwlock::LWLockMode>(mode),
    )
}
unsafe fn LWLockRelease(lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(lock as _)
}

// Sync / file tags --- TODO(pg-port): real FileTag lives in storage/sync.h ----
#[repr(C)]
pub struct FileTag {
    _private: [u8; 0],
}

// XLOG --- TODO(pg-port): real definitions live in access/xlog*.h -------------
#[repr(C)]
pub struct XLogReaderState {
    _private: [u8; 0],
}

pub const RM_MULTIXACT_ID: u8 = 0; // TODO(pg-port): real value lives in access/rmgrlist.h
pub const XLR_INFO_MASK: uint8 = 0x0F; // TODO(pg-port): real value lives in access/xlogrecord.h

unsafe fn XLogBeginInsert() {
    crate::access::transam::xloginsert::XLogBeginInsert()
}
unsafe fn XLogRegisterData(data: *mut c_char, len: usize) {
    crate::access::transam::xloginsert::XLogRegisterData(data as *const c_void, len as u32)
}
unsafe fn XLogInsert(rmid: u8, info: uint8) -> XLogRecPtr {
    crate::access::transam::xloginsert::XLogInsert(rmid, info)
}
unsafe fn XLogFlush(record: XLogRecPtr) {
    crate::access::transam::xlog::XLogFlush(record)
}
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 { crate::access::transam::xlogreader::XLogRecGetInfo(_record as _) }
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetData(_record as _) }
unsafe fn XLogRecGetXid(_record: *mut XLogReaderState) -> TransactionId { crate::access::transam::xlogreader::XLogRecGetXid(_record as _) }
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool { crate::access::transam::xlogreader::XLogRecHasAnyBlockRefs(_record as _) }

// Recovery state --- TODO(pg-port): real defs live in access/xlog*.h
pub static mut InRecovery: bool = false; // TODO(pg-port): real InRecovery lives in access/xlogutils.h
unsafe fn RecoveryInProgress() -> bool {
    crate::access::transam::xlog::RecoveryInProgress()
}

// Shared memory --- TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(name: *const c_char, size: Size, found: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(name, size, found)
}

// Atomics --- TODO(pg-port): real pg_atomic_* live in port/atomics.h ----------
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: u64,
}
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> u64 {
    crate::port::atomics::pg_atomic_read_u64_impl_native(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint64),
    )
}
unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    crate::port::atomics::generic::pg_atomic_write_u64_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint64),
        val,
    )
}

// Transaction id helpers --- TODO(pg-port): real defs live in access/transam.* */
pub const FirstNormalTransactionId: TransactionId = 3; // TODO(pg-port): real value lives in access/transam.h
unsafe fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdPrecedes(id1, id2)
}
unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool {
    crate::access::transam::TransactionIdIsValid(xid)
}
unsafe fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    crate::access::transam::TransactionIdEquals(id1, id2)
}
unsafe fn TransactionIdIsInProgress(_xid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real TransactionIdIsInProgress lives in storage/ipc/procarray.c
}
unsafe fn TransactionIdDidCommit(xid: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdDidCommit(xid)
}
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool { crate::access::transam::xact::TransactionIdIsCurrentTransactionId(_xid as _) }
unsafe fn AdvanceNextFullTransactionIdPastXid(xid: TransactionId) {
    crate::access::transam::varsup::AdvanceNextFullTransactionIdPastXid(xid)
}

// Misc / process state --- TODO(pg-port): real defs live in miscadmin.h etc.
pub static mut IsUnderPostmaster: bool = false; // TODO(pg-port): miscadmin.h
pub static mut IsBinaryUpgrade: bool = false; // TODO(pg-port): miscadmin.h
pub static mut MaxBackends: c_int = 0; // TODO(pg-port): storage/proc.h (postmaster)
pub static mut max_prepared_xacts: c_int = 0; // TODO(pg-port): access/twophase.c
unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO(pg-port): real IsTransactionState lives in access/transam/xact.c
}

// ProcNumber --- TODO(pg-port): real defs live in storage/procnumber.h --------
pub type ProcNumber = c_int;
pub static mut MyProcNumber: ProcNumber = 0; // TODO(pg-port): storage/procnumber.h
pub const FIRST_PREPARED_XACT_PROC_NUMBER: ProcNumber = 0; // TODO(pg-port): storage/procnumber.h

// PGPROC --- TODO(pg-port): real PGPROC lives in storage/proc.h ---------------
#[repr(C)]
pub struct PGPROC {
    pub delayChkptFlags: c_int,
    // ... TODO(pg-port): storage/proc.h
}
pub static mut MyProc: *mut PGPROC = core::ptr::null_mut(); // TODO(pg-port): real MyProc lives in storage/lmgr/proc.c
pub const DELAY_CHKPT_START: c_int = 1 << 0; // TODO(pg-port): real value lives in storage/proc.h

// Postmaster signals --- TODO(pg-port): real defs live in storage/pmsignal.h --
pub const PMSIGNAL_START_AUTOVAC_LAUNCHER: c_int = 0; // TODO(pg-port): storage/pmsignal.h
unsafe fn SendPostmasterSignal(_reason: c_int) { crate::storage::ipc::pmsignal::SendPostmasterSignal(_reason as _) }

// dbcommands --- TODO(pg-port): real get_database_name lives in commands/dbcommands.c
unsafe fn get_database_name(_dbid: Oid) -> *mut c_char {
    unimplemented!() // TODO(pg-port): real get_database_name lives in commands/dbcommands.c
}

// twophase --- TODO(pg-port): real defs live in access/twophase.c / twophase_rmgr.h
pub const TWOPHASE_RM_MULTIXACT_ID: uint8 = 0; // TODO(pg-port): access/twophase_rmgr.h
unsafe fn RegisterTwoPhaseRecord(_rmid: uint8, _info: uint16, _data: *mut c_void, _len: u32) {
    unimplemented!() // TODO(pg-port): real RegisterTwoPhaseRecord lives in access/transam/twophase.c
}
unsafe fn TwoPhaseGetDummyProcNumber(_xid: TransactionId, _lock_held: bool) -> ProcNumber { crate::access::transam::twophase::TwoPhaseGetDummyProcNumber(_xid as _, _lock_held as _) }

// funcapi / SRF --- TODO(pg-port): real defs live in funcapi.h ----------------
pub type FunctionCallInfo = *mut c_void; // TODO(pg-port): fmgr.h
pub type HeapTuple = *mut c_void; // TODO(pg-port): access/htup.h
pub type TupleDesc = *mut c_void; // TODO(pg-port): access/tupdesc.h
pub type TypeFuncClass = c_int; // TODO(pg-port): utils/funcapi.h
pub const TYPEFUNC_COMPOSITE: TypeFuncClass = 1; // TODO(pg-port): utils/funcapi.h

#[repr(C)]
pub struct FuncCallContext {
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
    // ... TODO(pg-port): funcapi.h
}

unsafe fn PG_GETARG_TRANSACTIONID(_fcinfo: FunctionCallInfo, _n: c_int) -> TransactionId {
    unimplemented!() // TODO(pg-port): real PG_GETARG_TRANSACTIONID lives in fmgr.h
}
unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!() // TODO(pg-port): real SRF_IS_FIRSTCALL lives in funcapi.h
}
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): real SRF_FIRSTCALL_INIT lives in funcapi.h
}
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): real SRF_PERCALL_SETUP lives in funcapi.h
}
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO(pg-port): real SRF_RETURN_NEXT lives in funcapi.h
}
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): real SRF_RETURN_DONE lives in funcapi.h
}
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    unimplemented!() // TODO(pg-port): real get_call_result_type lives in utils/fmgr/funcapi.c
}
unsafe fn TupleDescGetAttInMetadata(_tupdesc: TupleDesc) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real TupleDescGetAttInMetadata lives in utils/fmgr/funcapi.c
}
unsafe fn BuildTupleFromCStrings(_attinmeta: *mut c_void, _values: *mut *mut c_char) -> HeapTuple {
    unimplemented!() // TODO(pg-port): real BuildTupleFromCStrings lives in utils/fmgr/funcapi.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO(pg-port): real HeapTupleGetDatum lives in access/htup.h
}

// Tracepoints --- TODO(pg-port): real defs live in pg_trace.h (DTrace) --------
#[inline]
unsafe fn TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_START(_arg: bool) {}
#[inline]
unsafe fn TRACE_POSTGRESQL_MULTIXACT_CHECKPOINT_DONE(_arg: bool) {}

// Misc helpers ----------------------------------------------------------------
#[allow(non_snake_case)]
unsafe fn Min(a: c_int, b: c_int) -> c_int {
    if a < b {
        a
    } else {
        b
    }
}

// keep unused-import warnings quiet for types referenced only via stubs/casts
#[allow(dead_code)]
type _UnusedU64 = uint64;

/*-------------------------------------------------------------------------
 *
 * slot.c
 *   Replication slot management.
 *
 *
 * Copyright (c) 2012-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *   src/backend/replication/slot.c
 *
 * NOTES
 *
 * Replication slots are used to keep state about replication streams
 * originating from this cluster.  Their primary purpose is to prevent the
 * premature removal of WAL or of old tuple versions in a manner that would
 * interfere with replication; they are also useful for monitoring purposes.
 * Slots need to be permanent (to allow restarts), crash-safe, and allocatable
 * on standbys (to support cascading setups).  The requirement that slots be
 * usable on standbys precludes storing them in the system catalogs.
 *
 * Each replication slot gets its own directory inside the directory
 * $PGDATA / PG_REPLSLOT_DIR.  Inside that directory the state file will
 * contain the slot's own data.  Additional data can be stored alongside that
 * file if required.  While the server is running, the state data is also
 * cached in memory for efficiency.
 *
 * ReplicationSlotAllocationLock must be taken in exclusive mode to allocate
 * or free a slot. ReplicationSlotControlLock must be taken in shared mode
 * to iterate over the slots, and in exclusive mode to change the in_use flag
 * of a slot.  The remaining data in each slot is protected by its mutex.
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_void};

use crate::access::transam::xlogdefs::{XLogRecPtr, XLogSegNo};
use crate::c::{
    int64, uint32, uint64, NameData, Size, TransactionId,
    OidIsValid, MemSet, PG_BINARY,
};
use crate::postgres_ext::Oid;
use crate::access::transam::{TransactionIdIsValid, InvalidTransactionId};
use crate::access::transam::transam::{TransactionIdPrecedes, TransactionIdPrecedesOrEquals};
use crate::access::transam::xlogdefs::InvalidXLogRecPtr;
use crate::access::transam::xlogreader::XLogRecPtrIsInvalid;
use crate::miscadmin::{
    CHECK_FOR_INTERRUPTS, START_CRIT_SECTION, END_CRIT_SECTION,
    TimestampTz, B_STARTUP,
};
use crate::pg_config_manual::MAXPGPATH;
use crate::port::pg_crc32c::{pg_crc32c, INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C};
use crate::storage::lmgr::condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableInit, ConditionVariablePrepareToSleep, ConditionVariableSleep,
    ConditionVariableTimedSleep,
};
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::utils::adt::timestamp::{
    GetCurrentTimestamp, TimestampDifference, TimestampDifferenceExceedsSeconds,
};

// Re-export constants and types shared with slotfuncs
use crate::replication::slotfuncs::{
    ReplicationSlotCtl, ReplicationSlotControlLock,
    RS_PERSISTENT, RS_EPHEMERAL, RS_TEMPORARY,
    RS_INVAL_NONE, RS_INVAL_WAL_REMOVED, RS_INVAL_HORIZON, RS_INVAL_WAL_LEVEL,
    LW_SHARED, LW_EXCLUSIVE, LWLock, LWLockMode,
    max_replication_slots,
};

// InvalidOid comes from prelude (postgres_ext::*)
use crate::postgres_ext::InvalidOid;

// -----------------------------------------------------------------------
// Canonical struct definitions (slot.h / slot.c is the C home)
// -----------------------------------------------------------------------

/// ReplicationSlotInvalidationCause enum (replication/slot.h)
#[allow(non_camel_case_types)]
pub type ReplicationSlotInvalidationCause = c_int;

/// Persistent slot data written to disk (ReplicationSlotPersistentData in slot.h)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReplicationSlotPersistentData {
    /// Slot name
    pub name: NameData,
    /// Plugin name for logical slots; empty for physical
    pub plugin: NameData,
    /// Which database this slot is for (InvalidOid = physical)
    pub database: Oid,
    /// RS_PERSISTENT / RS_EPHEMERAL / RS_TEMPORARY
    pub persistency: c_int,
    /// oldest transaction that might be needed by this slot
    pub xmin: TransactionId,
    /// oldest catalog xmin
    pub catalog_xmin: TransactionId,
    /// oldest WAL needed for crash recovery
    pub restart_lsn: XLogRecPtr,
    /// confirmed flush LSN (logical slots)
    pub confirmed_flush: XLogRecPtr,
    /// cause for slot invalidation
    pub invalidated: ReplicationSlotInvalidationCause,
    /// is two-phase decode enabled?
    pub two_phase: bool,
    /// LSN at which two_phase was enabled
    pub two_phase_at: XLogRecPtr,
    /// is failover enabled?
    pub failover: bool,
    /// is this a synced slot on a standby?
    pub synced: bool,
}

/// In-memory replication slot (ReplicationSlot in slot.h)
#[repr(C)]
pub struct ReplicationSlot {
    /// protected by ReplicationSlotControlLock
    pub in_use: bool,
    /// PID of backend using this slot, 0 if free; protected by mutex
    pub active_pid: c_int,
    /// spinlock protects in-memory data of the slot
    pub mutex: slock_t,
    /// oldest effective xmin (not on-disk); protected by mutex
    pub effective_xmin: TransactionId,
    /// oldest catalog effective xmin; protected by mutex
    pub effective_catalog_xmin: TransactionId,
    /// timestamp since slot became inactive (0 if still active)
    pub inactive_since: TimestampTz,
    /// on-disk data
    pub data: ReplicationSlotPersistentData,
    /// condition variable for active_pid changes
    pub active_cv: ConditionVariable,
    /// LWLock for I/O in progress on this slot's file
    pub io_in_progress_lock: LWLock,
    /// dirty flag: needs flush to disk
    pub dirty: bool,
    /// just_dirtied: set before write, cleared after successful write
    pub just_dirtied: bool,
    /// candidate for advancing xmin (logical decoding internal)
    pub candidate_catalog_xmin: TransactionId,
    pub candidate_xmin_lsn: XLogRecPtr,
    pub candidate_restart_valid: XLogRecPtr,
    pub candidate_restart_lsn: XLogRecPtr,
    /// restart_lsn at the time of last successful SaveSlotToPath
    pub last_saved_restart_lsn: XLogRecPtr,
    /// confirmed_flush at the time of last successful SaveSlotToPath
    pub last_saved_confirmed_flush: XLogRecPtr,
}

/// Shared control array (ReplicationSlotCtlData in slot.h)
#[repr(C)]
pub struct ReplicationSlotCtlData {
    pub replication_slots: [ReplicationSlot; 1], // FLEXIBLE_ARRAY_MEMBER
}

// Additional invalidation cause (not in slotfuncs.rs yet)
pub const RS_INVAL_IDLE_TIMEOUT: ReplicationSlotInvalidationCause = (1 << 3);
pub const RS_INVAL_MAX_CAUSES: c_int = 4;

// Directory constant
pub const PG_REPLSLOT_DIR: &core::ffi::CStr = c"pg_replslot";

// Slot file magic/version
const SLOT_MAGIC: u32 = 0x1051CA1;
const SLOT_VERSION: u32 = 5;

// On-disk structure
#[repr(C)]
struct ReplicationSlotOnDisk {
    magic: u32,
    checksum: pg_crc32c,
    version: u32,
    length: u32,
    slotdata: ReplicationSlotPersistentData,
}

// Size constants mirroring C macros
const REPLICATION_SLOT_ON_DISK_CONSTANT_SIZE: usize =
    core::mem::offset_of!(ReplicationSlotOnDisk, slotdata);
const REPLICATION_SLOT_ON_DISK_NOT_CHECKSUMMED_SIZE: usize =
    core::mem::offset_of!(ReplicationSlotOnDisk, version);
const REPLICATION_SLOT_ON_DISK_CHECKSUMMED_SIZE: usize =
    core::mem::size_of::<ReplicationSlotOnDisk>() - REPLICATION_SLOT_ON_DISK_NOT_CHECKSUMMED_SIZE;
const REPLICATION_SLOT_ON_DISK_V2_SIZE: usize =
    core::mem::size_of::<ReplicationSlotOnDisk>() - REPLICATION_SLOT_ON_DISK_CONSTANT_SIZE;

// GUC variables
#[allow(non_upper_case_globals)]
pub static mut idle_replication_slot_timeout_secs: c_int = 0;
#[allow(non_upper_case_globals)]
pub static mut synchronized_standby_slots: *mut c_char = core::ptr::null_mut();

// Parsed config cache for synchronized_standby_slots
#[repr(C)]
struct SyncStandbySlotsConfigData {
    nslotnames: c_int,
    slot_names: [c_char; 1], // FLEXIBLE_ARRAY_MEMBER
}

#[allow(non_upper_case_globals)]
static mut synchronized_standby_slots_config: *mut SyncStandbySlotsConfigData =
    core::ptr::null_mut();

// Oldest flush LSN confirmed to standbys
#[allow(non_upper_case_globals)]
static mut ss_oldest_flush_lsn: XLogRecPtr = 0; // InvalidXLogRecPtr

// Lookup table for invalidation cause names
struct SlotInvalidationCauseMap {
    cause: ReplicationSlotInvalidationCause,
    cause_name: &'static str,
}

static SLOT_INVALIDATION_CAUSES: [SlotInvalidationCauseMap; 5] = [
    SlotInvalidationCauseMap { cause: RS_INVAL_NONE, cause_name: "none" },
    SlotInvalidationCauseMap { cause: RS_INVAL_WAL_REMOVED, cause_name: "wal_removed" },
    SlotInvalidationCauseMap { cause: RS_INVAL_HORIZON, cause_name: "rows_removed" },
    SlotInvalidationCauseMap { cause: RS_INVAL_WAL_LEVEL, cause_name: "wal_level_insufficient" },
    SlotInvalidationCauseMap { cause: RS_INVAL_IDLE_TIMEOUT, cause_name: "idle_timeout" },
];

// Shared pointer to our slot
#[allow(non_upper_case_globals)]
pub static mut MyReplicationSlot: *mut ReplicationSlot = core::ptr::null_mut();

// Shared pointer to the control array (already in slotfuncs; mirror here)
#[allow(non_upper_case_globals)]
pub static mut ReplicationSlotAllocationLock: LWLock = core::ptr::null_mut();

// ------------------------------------------------------------------
// Slot inline helper (from slot.h)
// ------------------------------------------------------------------

/// Set slot's inactive_since unless it was previously invalidated.
/// (Corresponds to ReplicationSlotSetInactiveSince inline in slot.h)
pub unsafe fn ReplicationSlotSetInactiveSince(
    s: *mut ReplicationSlot,
    ts: TimestampTz,
    acquire_lock: bool,
) {
    if acquire_lock {
        SpinLockAcquire(&mut (*s).mutex);
    }
    if (*s).data.invalidated == RS_INVAL_NONE {
        (*s).inactive_since = ts;
    }
    if acquire_lock {
        SpinLockRelease(&mut (*s).mutex);
    }
}

/// Inline macro equivalents
#[inline]
pub unsafe fn SlotIsPhysical(slot: *const ReplicationSlot) -> bool {
    (*slot).data.database == InvalidOid
}

#[inline]
pub unsafe fn SlotIsLogical(slot: *const ReplicationSlot) -> bool {
    (*slot).data.database != InvalidOid
}

// ------------------------------------------------------------------
// Shmem sizing / init
// ------------------------------------------------------------------

/// Report shared-memory space needed by ReplicationSlotsShmemInit.
pub unsafe fn ReplicationSlotsShmemSize() -> Size {
    if max_replication_slots == 0 {
        return 0;
    }
    let size = core::mem::offset_of!(ReplicationSlotCtlData, replication_slots);
    add_size(
        size,
        mul_size(max_replication_slots as usize, core::mem::size_of::<ReplicationSlot>()),
    )
}

/// Allocate and initialize shared memory for replication slots.
pub unsafe fn ReplicationSlotsShmemInit() {
    if max_replication_slots == 0 {
        return;
    }

    let mut found: bool = false;
    let ctl = ShmemInitStruct(
        c"ReplicationSlot Ctl".as_ptr(),
        ReplicationSlotsShmemSize(),
        &mut found,
    ) as *mut ReplicationSlotCtlData;
    // Assign to the module-level pointer (in slotfuncs, accessed via crate path)
    crate::replication::slotfuncs::ReplicationSlotCtl = ctl;

    if !found {
        MemSet(ctl as *mut c_void, 0, ReplicationSlotsShmemSize());

        for i in 0..max_replication_slots as usize {
            let slot = &mut (*ctl).replication_slots[i];
            SpinLockInit(&mut slot.mutex);
            LWLockInitialize(
                &mut slot.io_in_progress_lock,
                LWTRANCHE_REPLICATION_SLOT_IO,
            );
            ConditionVariableInit(&mut slot.active_cv);
        }
    }
}

/// Register the callback for replication slot cleanup and releasing.
pub unsafe fn ReplicationSlotInitialize() {
    before_shmem_exit(ReplicationSlotShmemExit, 0);
}

/// Release and cleanup replication slots on shmem exit.
unsafe extern "C" fn ReplicationSlotShmemExit(_code: c_int, _arg: Datum) {
    if !MyReplicationSlot.is_null() {
        ReplicationSlotRelease();
    }
    ReplicationSlotCleanup(false);
}

// ------------------------------------------------------------------
// Name validation
// ------------------------------------------------------------------

/// Check whether the passed slot name is valid and report errors at elevel.
pub unsafe fn ReplicationSlotValidateName(name: *const c_char, elevel: c_int) -> bool {
    let mut err_code: c_int = 0;
    let mut err_msg: *mut c_char = core::ptr::null_mut();
    let mut err_hint: *mut c_char = core::ptr::null_mut();

    if !ReplicationSlotValidateNameInternal(name, &mut err_code, &mut err_msg, &mut err_hint) {
        ereport!(elevel, errmsg!("(message folded)")) /* C also: errcode, errmsg_internal */;
        if !err_hint.is_null() {
            // errhint_internal was also requested by C; we do best-effort here
        }
        pfree(err_msg as *mut c_void);
        if !err_hint.is_null() {
            pfree(err_hint as *mut c_void);
        }
        return false;
    }
    true
}

/// Check whether the passed slot name is valid.
/// Returns true on success; fills err_code/err_msg/err_hint on failure.
/// Slot names may consist of [a-z0-9_]{1,NAMEDATALEN-1}.
pub unsafe fn ReplicationSlotValidateNameInternal(
    name: *const c_char,
    err_code: *mut c_int,
    err_msg: *mut *mut c_char,
    err_hint: *mut *mut c_char,
) -> bool {
    let len = strlen(name);

    if len == 0 {
        *err_code = ERRCODE_INVALID_NAME;
        *err_msg = psprintf(c"replication slot name \"<name>\" is too short".as_ptr());
        *err_hint = core::ptr::null_mut();
        return false;
    }

    if len >= crate::pg_config_manual::NAMEDATALEN {
        *err_code = ERRCODE_NAME_TOO_LONG;
        *err_msg = psprintf(c"replication slot name \"<name>\" is too long".as_ptr());
        *err_hint = core::ptr::null_mut();
        return false;
    }

    let mut cp = name;
    while *cp != 0 {
        let c = *cp as u8;
        if !((c >= b'a' && c <= b'z') || (c >= b'0' && c <= b'9') || c == b'_') {
            *err_code = ERRCODE_INVALID_NAME;
            *err_msg = psprintf(
                c"replication slot name \"<name>\" contains invalid character".as_ptr(),
            );
            *err_hint = psprintf(
                c"Replication slot names may only contain lower case letters, numbers, and the underscore character.".as_ptr(),
            );
            return false;
        }
        cp = cp.add(1);
    }
    true
}

// ------------------------------------------------------------------
// Create / acquire / release / drop
// ------------------------------------------------------------------

/// Create a new replication slot and mark it as used by this backend.
pub unsafe fn ReplicationSlotCreate(
    name: *const c_char,
    db_specific: bool,
    persistency: c_int,
    two_phase: bool,
    failover: bool,
    synced: bool,
) {
    let mut slot: *mut ReplicationSlot = core::ptr::null_mut();

    Assert!(MyReplicationSlot.is_null());

    ReplicationSlotValidateName(name, ERROR);

    if failover {
        if RecoveryInProgress() && !IsSyncingReplicationSlots() {
            ereport!(ERROR, errmsg!("cannot enable failover for a replication slot created on the standby")) /* C also: errcode */;
        }
        if persistency == RS_TEMPORARY && !IsSyncingReplicationSlots() {
            ereport!(ERROR, errmsg!("cannot enable failover for a temporary replication slot")) /* C also: errcode */;
        }
    }

    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    for i in 0..max_replication_slots as usize {
        let s = &mut (*ctl).replication_slots[i];
        if s.in_use && strcmp(name, NameStr(&s.data.name)) == 0 {
            ereport!(ERROR, errmsg!("replication slot \"{}\" already exists", CStr(name))) /* C also: errcode */;
        }
        if !s.in_use && slot.is_null() {
            slot = s;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    if slot.is_null() {
        ereport!(ERROR, errmsg!("all replication slots are in use")) /* C also: errcode, errhint */;
    }

    Assert!(!(*slot).in_use);
    Assert!((*slot).active_pid == 0);

    // Initialize persistent data
    core::ptr::write_bytes(
        &mut (*slot).data as *mut ReplicationSlotPersistentData as *mut u8,
        0,
        core::mem::size_of::<ReplicationSlotPersistentData>(),
    );
    namestrcpy(&mut (*slot).data.name, name);
    (*slot).data.database = if db_specific { MyDatabaseId } else { InvalidOid };
    (*slot).data.persistency = persistency;
    (*slot).data.two_phase = two_phase;
    (*slot).data.two_phase_at = InvalidXLogRecPtr;
    (*slot).data.failover = failover;
    (*slot).data.synced = synced;

    // In-memory only fields
    (*slot).just_dirtied = false;
    (*slot).dirty = false;
    (*slot).effective_xmin = InvalidTransactionId;
    (*slot).effective_catalog_xmin = InvalidTransactionId;
    (*slot).candidate_catalog_xmin = InvalidTransactionId;
    (*slot).candidate_xmin_lsn = InvalidXLogRecPtr;
    (*slot).candidate_restart_valid = InvalidXLogRecPtr;
    (*slot).candidate_restart_lsn = InvalidXLogRecPtr;
    (*slot).last_saved_confirmed_flush = InvalidXLogRecPtr;
    (*slot).last_saved_restart_lsn = InvalidXLogRecPtr;
    (*slot).inactive_since = 0;

    CreateSlotOnDisk(slot);

    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
    (*slot).in_use = true;

    SpinLockAcquire(&mut (*slot).mutex);
    Assert!((*slot).active_pid == 0);
    (*slot).active_pid = MyProcPid;
    SpinLockRelease(&mut (*slot).mutex);
    MyReplicationSlot = slot;

    LWLockRelease(ReplicationSlotControlLock);

    if SlotIsLogical(slot) {
        pgstat_create_replslot(slot);
    }

    LWLockRelease(ReplicationSlotAllocationLock);

    ConditionVariableBroadcast(&mut (*slot).active_cv);
}

/// Search for the named replication slot. Returns NULL if not found.
pub unsafe fn SearchNamedReplicationSlot(
    name: *const c_char,
    need_lock: bool,
) -> *mut ReplicationSlot {
    let mut slot: *mut ReplicationSlot = core::ptr::null_mut();

    if need_lock {
        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    }

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    for i in 0..max_replication_slots as usize {
        let s = &mut (*ctl).replication_slots[i];
        if s.in_use && strcmp(name, NameStr(&s.data.name)) == 0 {
            slot = s;
            break;
        }
    }

    if need_lock {
        LWLockRelease(ReplicationSlotControlLock);
    }

    slot
}

/// Return the index of the replication slot in the control array.
pub unsafe fn ReplicationSlotIndex(slot: *mut ReplicationSlot) -> c_int {
    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    let base = (*ctl).replication_slots.as_ptr();
    slot.offset_from(base) as c_int
}

/// If slot at index is unused, return false; otherwise copy name and return true.
pub unsafe fn ReplicationSlotName(index: c_int, name: *mut NameData) -> bool {
    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    let slot = &(*ctl).replication_slots[index as usize];

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    let found = slot.in_use;
    if slot.in_use {
        namestrcpy(name, NameStr(&slot.data.name));
    }
    LWLockRelease(ReplicationSlotControlLock);

    found
}

/// Find a previously created slot and mark it as used by this process.
pub unsafe fn ReplicationSlotAcquire(
    name: *const c_char,
    nowait: bool,
    error_if_invalid: bool,
) {
    Assert!(!name.is_null());

    // 'retry label replaced with loop + break/continue pattern
    loop {
        Assert!(MyReplicationSlot.is_null());

        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

        let s = SearchNamedReplicationSlot(name, false);
        if s.is_null() || !(*s).in_use {
            LWLockRelease(ReplicationSlotControlLock);
            ereport!(ERROR, errmsg!("replication slot \"{}\" does not exist", CStr(name))) /* C also: errcode */;
        }

        let active_pid: c_int;
        if IsUnderPostmaster {
            if !nowait {
                ConditionVariablePrepareToSleep(&mut (*s).active_cv);
            }
            SpinLockAcquire(&mut (*s).mutex);
            if (*s).active_pid == 0 {
                (*s).active_pid = MyProcPid;
            }
            active_pid = (*s).active_pid;
            ReplicationSlotSetInactiveSince(s, 0, false);
            SpinLockRelease(&mut (*s).mutex);
        } else {
            (*s).active_pid = MyProcPid;
            active_pid = MyProcPid;
            ReplicationSlotSetInactiveSince(s, 0, true);
        }
        LWLockRelease(ReplicationSlotControlLock);

        if active_pid != MyProcPid {
            if !nowait {
                ConditionVariableSleep(&mut (*s).active_cv, WAIT_EVENT_REPLICATION_SLOT_DROP);
                ConditionVariableCancelSleep();
                // retry
                continue;
            }
            ereport!(ERROR, errmsg!(
                    "replication slot \"{}\" is active for PID {}",
                    CStr(NameStr(&(*s).data.name)),
                    active_pid
                )) /* C also: errcode */;
        } else if !nowait {
            ConditionVariableCancelSleep();
        }

        MyReplicationSlot = s;

        if error_if_invalid && (*s).data.invalidated != RS_INVAL_NONE {
            ereport!(ERROR, errmsg!("can no longer access replication slot \"{}\"", CStr(NameStr(&(*s).data.name)))) /* C also: errcode, errdetail */;
        }

        ConditionVariableBroadcast(&mut (*s).active_cv);

        if SlotIsLogical(s) {
            pgstat_acquire_replslot(s);
        }

        if am_walsender {
            ereport!(if log_replication_commands { LOG } else { DEBUG1 }, errmsg!("(message folded)")) /* C also: if SlotIsLogical */;
        }

        break;
    }
}

/// Release the replication slot that this backend considers to own.
pub unsafe fn ReplicationSlotRelease() {
    let slot = MyReplicationSlot;
    let mut slotname: *mut c_char = core::ptr::null_mut();
    let mut is_logical = false;
    let mut now: TimestampTz = 0;

    Assert!(!slot.is_null() && (*slot).active_pid != 0);

    if am_walsender {
        slotname = pstrdup(NameStr(&(*slot).data.name));
        is_logical = SlotIsLogical(slot);
    }

    if (*slot).data.persistency == RS_EPHEMERAL {
        ReplicationSlotDropAcquired();
    }

    if !TransactionIdIsValid((*slot).data.xmin)
        && TransactionIdIsValid((*slot).effective_xmin)
    {
        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).effective_xmin = InvalidTransactionId;
        SpinLockRelease(&mut (*slot).mutex);
        ReplicationSlotsComputeRequiredXmin(false);
    }

    now = GetCurrentTimestamp();

    if (*slot).data.persistency == RS_PERSISTENT {
        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).active_pid = 0;
        ReplicationSlotSetInactiveSince(slot, now, false);
        SpinLockRelease(&mut (*slot).mutex);
        ConditionVariableBroadcast(&mut (*slot).active_cv);
    } else {
        ReplicationSlotSetInactiveSince(slot, now, true);
    }

    MyReplicationSlot = core::ptr::null_mut();

    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    (*MyProc).statusFlags &= !PROC_IN_LOGICAL_DECODING;
    (*ProcGlobal).statusFlags[(*MyProc).pgxactoff as usize] = (*MyProc).statusFlags;
    LWLockRelease(ProcArrayLock);

    if am_walsender {
        ereport!(if log_replication_commands { LOG } else { DEBUG1 }, errmsg!("(message folded)")) /* C also: if is_logical {
                errmsg */;
        pfree(slotname as *mut c_void);
    }
}

/// Cleanup temporary slots created in the current session.
/// If synced_only, only clean up synced temporary slots.
pub unsafe fn ReplicationSlotCleanup(synced_only: bool) {
    Assert!(MyReplicationSlot.is_null());

    loop {
        // restart:
        let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
        let mut restarted = false;
        'inner: for i in 0..max_replication_slots as usize {
            let s = &mut (*ctl).replication_slots[i];
            if !s.in_use {
                continue;
            }
            SpinLockAcquire(&mut s.mutex);
            if s.active_pid == MyProcPid && (!synced_only || s.data.synced) {
                Assert!(s.data.persistency == RS_TEMPORARY);
                SpinLockRelease(&mut s.mutex);
                LWLockRelease(ReplicationSlotControlLock);

                ReplicationSlotDropPtr(s);
                ConditionVariableBroadcast(&mut s.active_cv);
                restarted = true;
                break 'inner;
            } else {
                SpinLockRelease(&mut s.mutex);
            }
        }
        if !restarted {
            LWLockRelease(ReplicationSlotControlLock);
            break;
        }
        // else: loop again (goto restart)
    }
}

/// Permanently drop replication slot identified by name.
pub unsafe fn ReplicationSlotDrop(name: *const c_char, nowait: bool) {
    Assert!(MyReplicationSlot.is_null());

    ReplicationSlotAcquire(name, nowait, false);

    if RecoveryInProgress() && (*MyReplicationSlot).data.synced {
        ereport!(ERROR, errmsg!("cannot drop replication slot \"{}\"", CStr(name))) /* C also: errcode, errdetail */;
    }

    ReplicationSlotDropAcquired();
}

/// Change the definition of the slot identified by name.
pub unsafe fn ReplicationSlotAlter(
    name: *const c_char,
    failover: *const bool,
    two_phase: *const bool,
) {
    let mut update_slot = false;

    Assert!(MyReplicationSlot.is_null());
    Assert!(!failover.is_null() || !two_phase.is_null());

    ReplicationSlotAcquire(name, false, true);

    if SlotIsPhysical(MyReplicationSlot) {
        ereport!(ERROR, errmsg!("cannot use {} with a physical replication slot", "ALTER_REPLICATION_SLOT")) /* C also: errcode */;
    }

    if RecoveryInProgress() {
        if (*MyReplicationSlot).data.synced {
            ereport!(ERROR, errmsg!("cannot alter replication slot \"{}\"", CStr(name))) /* C also: errcode, errdetail */;
        }
        if !failover.is_null() && *failover {
            ereport!(ERROR, errmsg!("cannot enable failover for a replication slot on the standby")) /* C also: errcode */;
        }
    }

    if !failover.is_null() {
        if *failover && (*MyReplicationSlot).data.persistency == RS_TEMPORARY {
            ereport!(ERROR, errmsg!("cannot enable failover for a temporary replication slot")) /* C also: errcode */;
        }
        if (*MyReplicationSlot).data.failover != *failover {
            SpinLockAcquire(&mut (*MyReplicationSlot).mutex);
            (*MyReplicationSlot).data.failover = *failover;
            SpinLockRelease(&mut (*MyReplicationSlot).mutex);
            update_slot = true;
        }
    }

    if !two_phase.is_null() && (*MyReplicationSlot).data.two_phase != *two_phase {
        SpinLockAcquire(&mut (*MyReplicationSlot).mutex);
        (*MyReplicationSlot).data.two_phase = *two_phase;
        SpinLockRelease(&mut (*MyReplicationSlot).mutex);
        update_slot = true;
    }

    if update_slot {
        ReplicationSlotMarkDirty();
        ReplicationSlotSave();
    }

    ReplicationSlotRelease();
}

/// Permanently drop the currently acquired replication slot.
pub unsafe fn ReplicationSlotDropAcquired() {
    let slot = MyReplicationSlot;
    Assert!(!MyReplicationSlot.is_null());
    MyReplicationSlot = core::ptr::null_mut();
    ReplicationSlotDropPtr(slot);
}

/// Permanently drop the given slot pointer.
unsafe fn ReplicationSlotDropPtr(slot: *mut ReplicationSlot) {
    let mut path = [0i8; MAXPGPATH];
    let mut tmppath = [0i8; MAXPGPATH];

    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        PG_REPLSLOT_DIR.as_ptr(),
        NameStr(&(*slot).data.name),
    );
    snprintf(
        tmppath.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s.tmp".as_ptr(),
        PG_REPLSLOT_DIR.as_ptr(),
        NameStr(&(*slot).data.name),
    );

    if rename(path.as_ptr(), tmppath.as_ptr()) == 0 {
        START_CRIT_SECTION();
        fsync_fname(tmppath.as_ptr(), true);
        fsync_fname(PG_REPLSLOT_DIR.as_ptr(), true);
        END_CRIT_SECTION();
    } else {
        let fail_softly = (*slot).data.persistency != RS_PERSISTENT;

        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).active_pid = 0;
        SpinLockRelease(&mut (*slot).mutex);

        ConditionVariableBroadcast(&mut (*slot).active_cv);

        ereport!(if fail_softly { WARNING } else { ERROR }, errmsg!(
                "could not rename file \"{}\" to \"{}\": {}",
                CStr(path.as_ptr()),
                CStr(tmppath.as_ptr()),
                strerror_r()
            )) /* C also: errcode_for_file_access */;
    }

    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
    (*slot).active_pid = 0;
    (*slot).in_use = false;
    LWLockRelease(ReplicationSlotControlLock);
    ConditionVariableBroadcast(&mut (*slot).active_cv);

    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN();

    if !rmtree(tmppath.as_ptr(), true) {
        ereport!(WARNING, errmsg!("could not remove directory \"{}\"", CStr(tmppath.as_ptr())));
    }

    if SlotIsLogical(slot) {
        pgstat_drop_replslot(slot);
    }

    LWLockRelease(ReplicationSlotAllocationLock);
}

// ------------------------------------------------------------------
// Persistence helpers
// ------------------------------------------------------------------

/// Serialize the currently acquired slot's state from memory to disk.
pub unsafe fn ReplicationSlotSave() {
    let mut path = [0i8; MAXPGPATH];
    Assert!(!MyReplicationSlot.is_null());
    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        PG_REPLSLOT_DIR.as_ptr(),
        NameStr(&(*MyReplicationSlot).data.name),
    );
    SaveSlotToPath(MyReplicationSlot, path.as_ptr(), ERROR);
}

/// Signal that the currently acquired slot should be flushed to disk.
pub unsafe fn ReplicationSlotMarkDirty() {
    let slot = MyReplicationSlot;
    Assert!(!MyReplicationSlot.is_null());
    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).just_dirtied = true;
    (*slot).dirty = true;
    SpinLockRelease(&mut (*slot).mutex);
}

/// Convert RS_EPHEMERAL or RS_TEMPORARY slot to RS_PERSISTENT.
pub unsafe fn ReplicationSlotPersist() {
    let slot = MyReplicationSlot;
    Assert!(!slot.is_null());
    Assert!((*slot).data.persistency != RS_PERSISTENT);
    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).data.persistency = RS_PERSISTENT;
    SpinLockRelease(&mut (*slot).mutex);
    ReplicationSlotMarkDirty();
    ReplicationSlotSave();
}

// ------------------------------------------------------------------
// Xmin / LSN computation
// ------------------------------------------------------------------

/// Compute the oldest xmin across all slots and store it in the ProcArray.
pub unsafe fn ReplicationSlotsComputeRequiredXmin(already_locked: bool) {
    let mut agg_xmin: TransactionId = InvalidTransactionId;
    let mut agg_catalog_xmin: TransactionId = InvalidTransactionId;

    Assert!(!crate::replication::slotfuncs::ReplicationSlotCtl.is_null());

    if !already_locked {
        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    }

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    for i in 0..max_replication_slots as usize {
        let s = &(*ctl).replication_slots[i];
        if !s.in_use {
            continue;
        }
        SpinLockAcquire(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);
        let effective_xmin = s.effective_xmin;
        let effective_catalog_xmin = s.effective_catalog_xmin;
        let invalidated = s.data.invalidated != RS_INVAL_NONE;
        SpinLockRelease(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);

        if invalidated {
            continue;
        }

        if TransactionIdIsValid(effective_xmin)
            && (!TransactionIdIsValid(agg_xmin)
                || TransactionIdPrecedes(effective_xmin, agg_xmin))
        {
            agg_xmin = effective_xmin;
        }

        if TransactionIdIsValid(effective_catalog_xmin)
            && (!TransactionIdIsValid(agg_catalog_xmin)
                || TransactionIdPrecedes(effective_catalog_xmin, agg_catalog_xmin))
        {
            agg_catalog_xmin = effective_catalog_xmin;
        }
    }

    ProcArraySetReplicationSlotXmin(agg_xmin, agg_catalog_xmin, already_locked);

    if !already_locked {
        LWLockRelease(ReplicationSlotControlLock);
    }
}

/// Compute the oldest restart LSN across all slots and inform xlog module.
pub unsafe fn ReplicationSlotsComputeRequiredLSN() {
    let mut min_required: XLogRecPtr = InvalidXLogRecPtr;

    Assert!(!crate::replication::slotfuncs::ReplicationSlotCtl.is_null());

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for i in 0..max_replication_slots as usize {
        let s = &(*ctl).replication_slots[i];
        if !s.in_use {
            continue;
        }
        SpinLockAcquire(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);
        let persistency = s.data.persistency;
        let mut restart_lsn = s.data.restart_lsn;
        let invalidated = s.data.invalidated != RS_INVAL_NONE;
        let last_saved_restart_lsn = s.last_saved_restart_lsn;
        SpinLockRelease(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);

        if invalidated {
            continue;
        }

        if persistency == RS_PERSISTENT
            && last_saved_restart_lsn != InvalidXLogRecPtr
            && restart_lsn > last_saved_restart_lsn
        {
            restart_lsn = last_saved_restart_lsn;
        }

        if restart_lsn != InvalidXLogRecPtr
            && (min_required == InvalidXLogRecPtr || restart_lsn < min_required)
        {
            min_required = restart_lsn;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    XLogSetReplicationSlotMinimumLSN(min_required);
}

/// Compute the oldest WAL LSN required by logical decoding slots.
pub unsafe fn ReplicationSlotsComputeLogicalRestartLSN() -> XLogRecPtr {
    let mut result: XLogRecPtr = InvalidXLogRecPtr;

    if max_replication_slots <= 0 {
        return InvalidXLogRecPtr;
    }

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    for i in 0..max_replication_slots as usize {
        let s = &(*ctl).replication_slots[i];
        if !s.in_use {
            continue;
        }
        if !SlotIsLogical(s) {
            continue;
        }
        SpinLockAcquire(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);
        let persistency = s.data.persistency;
        let mut restart_lsn = s.data.restart_lsn;
        let invalidated = s.data.invalidated != RS_INVAL_NONE;
        let last_saved_restart_lsn = s.last_saved_restart_lsn;
        SpinLockRelease(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);

        if invalidated {
            continue;
        }

        if persistency == RS_PERSISTENT
            && last_saved_restart_lsn != InvalidXLogRecPtr
            && restart_lsn > last_saved_restart_lsn
        {
            restart_lsn = last_saved_restart_lsn;
        }

        if restart_lsn == InvalidXLogRecPtr {
            continue;
        }

        if result == InvalidXLogRecPtr || restart_lsn < result {
            result = restart_lsn;
        }
    }

    LWLockRelease(ReplicationSlotControlLock);
    result
}

/// Count slots that refer to the passed database oid.
pub unsafe fn ReplicationSlotsCountDBSlots(
    dboid: Oid,
    nslots: *mut c_int,
    nactive: *mut c_int,
) -> bool {
    *nslots = 0;
    *nactive = 0;

    if max_replication_slots <= 0 {
        return false;
    }

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for i in 0..max_replication_slots as usize {
        let s = &(*ctl).replication_slots[i];
        if !s.in_use {
            continue;
        }
        if !SlotIsLogical(s) {
            continue;
        }
        if s.data.database != dboid {
            continue;
        }
        SpinLockAcquire(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);
        *nslots += 1;
        if s.active_pid != 0 {
            *nactive += 1;
        }
        SpinLockRelease(&mut (*(s as *const _ as *mut ReplicationSlot)).mutex);
    }
    LWLockRelease(ReplicationSlotControlLock);

    *nslots > 0
}

/// Drop all db-specific slots for the passed database oid.
pub unsafe fn ReplicationSlotsDropDBSlots(dboid: Oid) {
    if max_replication_slots <= 0 {
        return;
    }

    loop {
        // restart:
        let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
        let mut restarted = false;
        'inner: for i in 0..max_replication_slots as usize {
            let s = &mut (*ctl).replication_slots[i];
            if !s.in_use {
                continue;
            }
            if !SlotIsLogical(s) {
                continue;
            }
            if s.data.database != dboid {
                continue;
            }

            SpinLockAcquire(&mut s.mutex);
            let slotname = NameStr(&s.data.name);
            let active_pid = s.active_pid;
            if active_pid == 0 {
                MyReplicationSlot = s;
                s.active_pid = MyProcPid;
            }
            SpinLockRelease(&mut s.mutex);

            if active_pid != 0 {
                ereport!(ERROR, errmsg!(
                        "replication slot \"{}\" is active for PID {}",
                        CStr(slotname),
                        active_pid
                    )) /* C also: errcode */;
            }

            LWLockRelease(ReplicationSlotControlLock);
            ReplicationSlotDropAcquired();
            restarted = true;
            break 'inner;
        }
        if !restarted {
            LWLockRelease(ReplicationSlotControlLock);
            break;
        }
    }
}

// ------------------------------------------------------------------
// Requirement / permission checks
// ------------------------------------------------------------------

/// Check whether the server's configuration supports using replication slots.
pub unsafe fn CheckSlotRequirements() {
    if max_replication_slots == 0 {
        ereport!(ERROR, errmsg!("replication slots can only be used if \"max_replication_slots\" > 0")) /* C also: errcode */;
    }
    if wal_level < WAL_LEVEL_REPLICA {
        ereport!(ERROR, errmsg!("replication slots can only be used if \"wal_level\" >= \"replica\"")) /* C also: errcode */;
    }
}

/// Check whether the user has privilege to use replication slots.
pub unsafe fn CheckSlotPermissions() {
    if !crate::miscadmin::has_rolreplication(crate::miscadmin::GetUserId()) {
        ereport!(ERROR, errmsg!("permission denied to use replication slots")) /* C also: errcode, errdetail */;
    }
}

// ------------------------------------------------------------------
// WAL reservation
// ------------------------------------------------------------------

/// Reserve WAL for the currently active slot.
pub unsafe fn ReplicationSlotReserveWal() {
    let slot = MyReplicationSlot;
    let restart_lsn: XLogRecPtr;

    Assert!(!slot.is_null());
    Assert!((*slot).data.restart_lsn == InvalidXLogRecPtr);
    Assert!((*slot).last_saved_restart_lsn == InvalidXLogRecPtr);

    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    if SlotIsPhysical(slot) {
        restart_lsn = GetRedoRecPtr();
    } else if RecoveryInProgress() {
        restart_lsn = GetXLogReplayRecPtr(core::ptr::null_mut());
    } else {
        restart_lsn = GetXLogInsertRecPtr();
    }

    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).data.restart_lsn = restart_lsn;
    SpinLockRelease(&mut (*slot).mutex);

    ReplicationSlotsComputeRequiredLSN();

    let mut segno: XLogSegNo = 0;
    XLByteToSeg((*slot).data.restart_lsn, &mut segno, wal_segment_size);
    if XLogGetLastRemovedSegno() >= segno {
        elog!(
            ERROR,
            "WAL required by replication slot {} has been removed concurrently",
            CStr(NameStr(&(*slot).data.name))
        );
    }

    LWLockRelease(ReplicationSlotAllocationLock);

    if !RecoveryInProgress() && SlotIsLogical(slot) {
        let flushptr = LogStandbySnapshot();
        XLogFlush(flushptr);
    }
}

// ------------------------------------------------------------------
// Invalidation
// ------------------------------------------------------------------

/// Report that a replication slot needs to be invalidated.
unsafe fn ReportSlotInvalidation(
    cause: ReplicationSlotInvalidationCause,
    terminating: bool,
    pid: c_int,
    slotname: NameData,
    restart_lsn: XLogRecPtr,
    oldest_lsn: XLogRecPtr,
    snapshot_conflict_horizon: TransactionId,
    slot_idle_seconds: i64,
) {
    let mut err_detail = StringInfoData {
        data: core::ptr::null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };
    let mut err_hint = StringInfoData {
        data: core::ptr::null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };

    initStringInfo(&mut err_detail);
    initStringInfo(&mut err_hint);

    match cause {
        RS_INVAL_WAL_REMOVED => {
            let ex = oldest_lsn.wrapping_sub(restart_lsn);
            appendStringInfo(
                &mut err_detail,
                c"The slot's restart_lsn exceeds the limit.".as_ptr(),
            );
            appendStringInfo(
                &mut err_hint,
                c"You might need to increase max_slot_wal_keep_size.".as_ptr(),
            );
        }
        RS_INVAL_HORIZON => {
            appendStringInfo(
                &mut err_detail,
                c"The slot conflicted with xid horizon.".as_ptr(),
            );
        }
        RS_INVAL_WAL_LEVEL => {
            appendStringInfoString(
                &mut err_detail,
                c"Logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary server.".as_ptr(),
            );
        }
        RS_INVAL_IDLE_TIMEOUT => {
            appendStringInfo(
                &mut err_detail,
                c"The slot's idle time exceeds idle_replication_slot_timeout.".as_ptr(),
            );
            appendStringInfo(
                &mut err_hint,
                c"You might need to increase idle_replication_slot_timeout.".as_ptr(),
            );
        }
        _ => {
            unreachable!();
        }
    }

    if terminating {
        ereport!(LOG, errmsg!(
                "terminating process {} to release replication slot \"{}\"",
                pid,
                CStr(NameStr(&slotname))
            )) /* C also: errdetail_internal */;
    } else {
        ereport!(LOG, errmsg!(
                "invalidating obsolete replication slot \"{}\"",
                CStr(NameStr(&slotname))
            )) /* C also: errdetail_internal */;
    }

    pfree(err_detail.data as *mut c_void);
    pfree(err_hint.data as *mut c_void);
}

/// Can we invalidate an idle replication slot?
#[inline]
unsafe fn CanInvalidateIdleSlot(s: *const ReplicationSlot) -> bool {
    idle_replication_slot_timeout_secs != 0
        && !XLogRecPtrIsInvalid((*s).data.restart_lsn)
        && (*s).inactive_since > 0
        && !(RecoveryInProgress() && (*s).data.synced)
}

/// Determine the invalidation cause for a slot among the given possible causes.
unsafe fn DetermineSlotInvalidationCause(
    possible_causes: uint32,
    s: *const ReplicationSlot,
    oldest_lsn: XLogRecPtr,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
    inactive_since: *mut TimestampTz,
    now: TimestampTz,
) -> ReplicationSlotInvalidationCause {
    Assert!(possible_causes != RS_INVAL_NONE as u32);

    if possible_causes & RS_INVAL_WAL_REMOVED as u32 != 0 {
        let restart_lsn = (*s).data.restart_lsn;
        if restart_lsn != InvalidXLogRecPtr && restart_lsn < oldest_lsn {
            return RS_INVAL_WAL_REMOVED;
        }
    }

    if possible_causes & RS_INVAL_HORIZON as u32 != 0 {
        if SlotIsLogical(s)
            && (dboid == InvalidOid || dboid == (*s).data.database)
        {
            let effective_xmin = (*s).effective_xmin;
            let catalog_effective_xmin = (*s).effective_catalog_xmin;
            if TransactionIdIsValid(effective_xmin)
                && TransactionIdPrecedesOrEquals(effective_xmin, snapshot_conflict_horizon)
            {
                return RS_INVAL_HORIZON;
            } else if TransactionIdIsValid(catalog_effective_xmin)
                && TransactionIdPrecedesOrEquals(catalog_effective_xmin, snapshot_conflict_horizon)
            {
                return RS_INVAL_HORIZON;
            }
        }
    }

    if possible_causes & RS_INVAL_WAL_LEVEL as u32 != 0 {
        if SlotIsLogical(s) {
            return RS_INVAL_WAL_LEVEL;
        }
    }

    if possible_causes & RS_INVAL_IDLE_TIMEOUT as u32 != 0 {
        Assert!(now > 0);
        if CanInvalidateIdleSlot(s) {
            if TimestampDifferenceExceedsSeconds(
                (*s).inactive_since,
                now,
                idle_replication_slot_timeout_secs,
            ) {
                *inactive_since = (*s).inactive_since;
                return RS_INVAL_IDLE_TIMEOUT;
            }
        }
    }

    RS_INVAL_NONE
}

/// Acquires the given slot and marks it invalid, if necessary.
/// Returns whether ReplicationSlotControlLock was released in the interim.
unsafe fn InvalidatePossiblyObsoleteSlot(
    possible_causes: uint32,
    s: *mut ReplicationSlot,
    oldest_lsn: XLogRecPtr,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
    invalidated: *mut bool,
) -> bool {
    let mut last_signaled_pid: c_int = 0;
    let mut released_lock = false;
    let mut inactive_since: TimestampTz = 0;

    loop {
        let restart_lsn: XLogRecPtr;
        let slotname: NameData;
        let mut active_pid: c_int = 0;
        let mut invalidation_cause: ReplicationSlotInvalidationCause = RS_INVAL_NONE;
        let mut now: TimestampTz = 0;
        let mut slot_idle_secs: i64 = 0;

        Assert!(LWLockHeldByMeInMode(ReplicationSlotControlLock, LW_SHARED));

        if !(*s).in_use {
            if released_lock {
                LWLockRelease(ReplicationSlotControlLock);
            }
            break;
        }

        if possible_causes & RS_INVAL_IDLE_TIMEOUT as u32 != 0 {
            now = GetCurrentTimestamp();
        }

        SpinLockAcquire(&mut (*s).mutex);

        restart_lsn = (*s).data.restart_lsn;

        if (*s).data.invalidated == RS_INVAL_NONE {
            invalidation_cause = DetermineSlotInvalidationCause(
                possible_causes,
                s,
                oldest_lsn,
                dboid,
                snapshot_conflict_horizon,
                &mut inactive_since,
                now,
            );
        }

        if invalidation_cause == RS_INVAL_NONE {
            SpinLockRelease(&mut (*s).mutex);
            if released_lock {
                LWLockRelease(ReplicationSlotControlLock);
            }
            break;
        }

        slotname = (*s).data.name;
        active_pid = (*s).active_pid;

        if active_pid == 0 {
            MyReplicationSlot = s;
            (*s).active_pid = MyProcPid;
            (*s).data.invalidated = invalidation_cause;

            if invalidation_cause == RS_INVAL_WAL_REMOVED {
                (*s).data.restart_lsn = InvalidXLogRecPtr;
                (*s).last_saved_restart_lsn = InvalidXLogRecPtr;
            }

            *invalidated = true;
        }

        SpinLockRelease(&mut (*s).mutex);

        if invalidation_cause == RS_INVAL_IDLE_TIMEOUT {
            let mut slot_idle_usecs: c_int = 0;
            TimestampDifference(inactive_since, now, &mut slot_idle_secs, &mut slot_idle_usecs);
        }

        if active_pid != 0 {
            ConditionVariablePrepareToSleep(&mut (*s).active_cv);

            LWLockRelease(ReplicationSlotControlLock);
            released_lock = true;

            if last_signaled_pid != active_pid {
                ReportSlotInvalidation(
                    invalidation_cause,
                    true,
                    active_pid,
                    slotname,
                    restart_lsn,
                    oldest_lsn,
                    snapshot_conflict_horizon,
                    slot_idle_secs,
                );

                if MyBackendType == B_STARTUP {
                    let _ = SendProcSignal(
                        active_pid,
                        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
                        INVALID_PROC_NUMBER,
                    );
                } else {
                    let _ = kill(active_pid, SIGTERM);
                }

                last_signaled_pid = active_pid;
            }

            ConditionVariableSleep(&mut (*s).active_cv, WAIT_EVENT_REPLICATION_SLOT_DROP);

            LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
            // continue loop
        } else {
            LWLockRelease(ReplicationSlotControlLock);
            released_lock = true;

            ReplicationSlotMarkDirty();
            ReplicationSlotSave();
            ReplicationSlotRelease();

            ReportSlotInvalidation(
                invalidation_cause,
                false,
                active_pid,
                slotname,
                restart_lsn,
                oldest_lsn,
                snapshot_conflict_horizon,
                slot_idle_secs,
            );

            break;
        }
    }

    released_lock
}

/// Invalidate slots that require resources about to be removed.
/// Returns true when any slot has been invalidated.
pub unsafe fn InvalidateObsoleteReplicationSlots(
    possible_causes: uint32,
    oldest_segno: XLogSegNo,
    dboid: Oid,
    snapshot_conflict_horizon: TransactionId,
) -> bool {
    let mut oldest_lsn: XLogRecPtr = 0;
    let mut invalidated = false;

    Assert!(
        (possible_causes & RS_INVAL_HORIZON as u32 == 0)
            || TransactionIdIsValid(snapshot_conflict_horizon)
    );
    Assert!(
        (possible_causes & RS_INVAL_WAL_REMOVED as u32 == 0) || oldest_segno > 0
    );
    Assert!(possible_causes != RS_INVAL_NONE as u32);

    if max_replication_slots == 0 {
        return invalidated;
    }

    XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size, &mut oldest_lsn);

    loop {
        // restart:
        let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
        LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
        let mut restarted = false;
        for i in 0..max_replication_slots as usize {
            let s = &mut (*ctl).replication_slots[i];
            if !s.in_use {
                continue;
            }
            if SlotIsLogical(s) && IsBinaryUpgrade {
                continue;
            }
            if InvalidatePossiblyObsoleteSlot(
                possible_causes,
                s,
                oldest_lsn,
                dboid,
                snapshot_conflict_horizon,
                &mut invalidated,
            ) {
                restarted = true;
                break;
            }
        }
        if !restarted {
            LWLockRelease(ReplicationSlotControlLock);
            break;
        }
    }

    if invalidated {
        ReplicationSlotsComputeRequiredXmin(false);
        ReplicationSlotsComputeRequiredLSN();
    }

    invalidated
}

// ------------------------------------------------------------------
// Checkpoint
// ------------------------------------------------------------------

/// Flush all replication slots to disk at checkpoint time.
pub unsafe fn CheckPointReplicationSlots(is_shutdown: bool) {
    let mut last_saved_restart_lsn_updated = false;

    elog!(DEBUG1, "performing replication slot checkpoint");

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);

    for i in 0..max_replication_slots as usize {
        let s = &mut (*ctl).replication_slots[i];
        if !s.in_use {
            continue;
        }

        let mut path = [0i8; MAXPGPATH];
        snprintf(
            path.as_mut_ptr(),
            MAXPGPATH,
            c"%s/%s".as_ptr(),
            PG_REPLSLOT_DIR.as_ptr(),
            NameStr(&s.data.name),
        );

        if is_shutdown && SlotIsLogical(s) {
            SpinLockAcquire(&mut s.mutex);
            if s.data.invalidated == RS_INVAL_NONE
                && s.data.confirmed_flush > s.last_saved_confirmed_flush
            {
                s.just_dirtied = true;
                s.dirty = true;
            }
            SpinLockRelease(&mut s.mutex);
        }

        if s.last_saved_restart_lsn != s.data.restart_lsn {
            last_saved_restart_lsn_updated = true;
        }

        SaveSlotToPath(s, path.as_ptr(), LOG);
    }
    LWLockRelease(ReplicationSlotAllocationLock);

    if last_saved_restart_lsn_updated {
        ReplicationSlotsComputeRequiredLSN();
    }
}

// ------------------------------------------------------------------
// Startup / disk restore
// ------------------------------------------------------------------

/// Load all replication slots from disk into memory at server startup.
pub unsafe fn StartupReplicationSlots() {
    elog!(DEBUG1, "starting up replication slots");

    let replication_dir = AllocateDir(PG_REPLSLOT_DIR.as_ptr());
    loop {
        let replication_de = ReadDir(replication_dir, PG_REPLSLOT_DIR.as_ptr());
        if replication_de.is_null() {
            break;
        }

        let d_name = (*replication_de).d_name.as_ptr();

        if strcmp(d_name, c".".as_ptr()) == 0 || strcmp(d_name, c"..".as_ptr()) == 0 {
            continue;
        }

        let mut path = [0i8; MAXPGPATH + 12]; // sizeof(PG_REPLSLOT_DIR)
        snprintf(
            path.as_mut_ptr(),
            path.len(),
            c"%s/%s".as_ptr(),
            PG_REPLSLOT_DIR.as_ptr(),
            d_name,
        );
        let de_type = get_dirent_type(path.as_ptr(), replication_de, false, DEBUG1);

        if de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_DIR {
            continue;
        }

        if pg_str_endswith(d_name, c".tmp".as_ptr()) {
            if !rmtree(path.as_ptr(), true) {
                ereport!(WARNING, errmsg!("could not remove directory \"{}\"", CStr(path.as_ptr())));
                continue;
            }
            fsync_fname(PG_REPLSLOT_DIR.as_ptr(), true);
            continue;
        }

        RestoreSlotFromDisk(d_name);
    }
    FreeDir(replication_dir);

    if max_replication_slots <= 0 {
        return;
    }

    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN();
}

// ------------------------------------------------------------------
// On-disk manipulation
// ------------------------------------------------------------------

unsafe fn CreateSlotOnDisk(slot: *mut ReplicationSlot) {
    let mut path = [0i8; MAXPGPATH];
    let mut tmppath = [0i8; MAXPGPATH];

    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        PG_REPLSLOT_DIR.as_ptr(),
        NameStr(&(*slot).data.name),
    );
    snprintf(
        tmppath.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s.tmp".as_ptr(),
        PG_REPLSLOT_DIR.as_ptr(),
        NameStr(&(*slot).data.name),
    );

    // Clean up leftover temp directory if present
    let mut st: libc_stat = core::mem::zeroed();
    if stat(tmppath.as_ptr(), &mut st) == 0 && S_ISDIR(st.st_mode) {
        rmtree(tmppath.as_ptr(), true);
    }

    if MakePGDirectory(tmppath.as_ptr()) < 0 {
        ereport!(ERROR, errmsg!("could not create directory \"{}\": {}", CStr(tmppath.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
    }
    fsync_fname(tmppath.as_ptr(), true);

    (*slot).dirty = true;
    SaveSlotToPath(slot, tmppath.as_ptr(), ERROR);

    if rename(tmppath.as_ptr(), path.as_ptr()) != 0 {
        ereport!(ERROR, errmsg!(
                "could not rename file \"{}\" to \"{}\": {}",
                CStr(tmppath.as_ptr()),
                CStr(path.as_ptr()),
                strerror_r()
            )) /* C also: errcode_for_file_access */;
    }

    START_CRIT_SECTION();
    fsync_fname(path.as_ptr(), true);
    fsync_fname(PG_REPLSLOT_DIR.as_ptr(), true);
    END_CRIT_SECTION();
}

unsafe fn SaveSlotToPath(slot: *mut ReplicationSlot, dir: *const c_char, elevel: c_int) {
    let mut tmppath = [0i8; MAXPGPATH];
    let mut path = [0i8; MAXPGPATH];

    SpinLockAcquire(&mut (*slot).mutex);
    let was_dirty = (*slot).dirty;
    (*slot).just_dirtied = false;
    SpinLockRelease(&mut (*slot).mutex);

    if !was_dirty {
        return;
    }

    LWLockAcquire((*slot).io_in_progress_lock, LW_EXCLUSIVE);

    let mut cp: ReplicationSlotOnDisk = core::mem::zeroed();

    snprintf(tmppath.as_mut_ptr(), MAXPGPATH, c"%s/state.tmp".as_ptr(), dir);
    snprintf(path.as_mut_ptr(), MAXPGPATH, c"%s/state".as_ptr(), dir);

    let fd = OpenTransientFile(tmppath.as_ptr(), O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    if fd < 0 {
        let save_errno = errno();
        LWLockRelease((*slot).io_in_progress_lock);
        set_errno(save_errno);
        ereport!(elevel, errmsg!("could not create file \"{}\": {}", CStr(tmppath.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
        return;
    }

    cp.magic = SLOT_MAGIC;
    cp.checksum = INIT_CRC32C();
    cp.version = SLOT_VERSION;
    cp.length = REPLICATION_SLOT_ON_DISK_V2_SIZE as u32;

    SpinLockAcquire(&mut (*slot).mutex);
    core::ptr::copy_nonoverlapping(
        &(*slot).data as *const ReplicationSlotPersistentData,
        &mut cp.slotdata as *mut ReplicationSlotPersistentData,
        1,
    );
    SpinLockRelease(&mut (*slot).mutex);

    let checksum_start =
        ((&cp as *const ReplicationSlotOnDisk as *const u8)
            .add(REPLICATION_SLOT_ON_DISK_NOT_CHECKSUMMED_SIZE));
    cp.checksum = COMP_CRC32C(cp.checksum, checksum_start as *const c_void, REPLICATION_SLOT_ON_DISK_CHECKSUMMED_SIZE);
    cp.checksum = FIN_CRC32C(cp.checksum);

    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_WRITE);
    let written = write(fd, &cp as *const ReplicationSlotOnDisk as *const c_void,
                        core::mem::size_of::<ReplicationSlotOnDisk>());
    if written != core::mem::size_of::<ReplicationSlotOnDisk>() as isize {
        let save_errno = errno();
        pgstat_report_wait_end();
        CloseTransientFile(fd);
        unlink(tmppath.as_ptr());
        LWLockRelease((*slot).io_in_progress_lock);
        set_errno(if save_errno != 0 { save_errno } else { ENOSPC });
        ereport!(elevel, errmsg!("could not write to file \"{}\": {}", CStr(tmppath.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
        return;
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_SYNC);
    if pg_fsync(fd) != 0 {
        let save_errno = errno();
        pgstat_report_wait_end();
        CloseTransientFile(fd);
        unlink(tmppath.as_ptr());
        LWLockRelease((*slot).io_in_progress_lock);
        set_errno(save_errno);
        ereport!(elevel, errmsg!("could not fsync file \"{}\": {}", CStr(tmppath.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
        return;
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        let save_errno = errno();
        unlink(tmppath.as_ptr());
        LWLockRelease((*slot).io_in_progress_lock);
        set_errno(save_errno);
        ereport!(elevel, errmsg!("could not close file \"{}\": {}", CStr(tmppath.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
        return;
    }

    if rename(tmppath.as_ptr(), path.as_ptr()) != 0 {
        let save_errno = errno();
        unlink(tmppath.as_ptr());
        LWLockRelease((*slot).io_in_progress_lock);
        set_errno(save_errno);
        ereport!(elevel, errmsg!(
                "could not rename file \"{}\" to \"{}\": {}",
                CStr(tmppath.as_ptr()),
                CStr(path.as_ptr()),
                strerror_r()
            )) /* C also: errcode_for_file_access */;
        return;
    }

    START_CRIT_SECTION();
    fsync_fname(path.as_ptr(), false);
    fsync_fname(dir, true);
    fsync_fname(PG_REPLSLOT_DIR.as_ptr(), true);
    END_CRIT_SECTION();

    SpinLockAcquire(&mut (*slot).mutex);
    if !(*slot).just_dirtied {
        (*slot).dirty = false;
    }
    (*slot).last_saved_confirmed_flush = cp.slotdata.confirmed_flush;
    (*slot).last_saved_restart_lsn = cp.slotdata.restart_lsn;
    SpinLockRelease(&mut (*slot).mutex);

    LWLockRelease((*slot).io_in_progress_lock);
}

unsafe fn RestoreSlotFromDisk(name: *const c_char) {
    let mut cp: ReplicationSlotOnDisk = core::mem::zeroed();
    let mut slotdir = [0i8; MAXPGPATH + 12];
    let mut path = [0i8; MAXPGPATH + 12 + 10];
    let mut restored = false;
    let mut now: TimestampTz = 0;

    snprintf(slotdir.as_mut_ptr(), slotdir.len(), c"%s/%s".as_ptr(), PG_REPLSLOT_DIR.as_ptr(), name);
    snprintf(path.as_mut_ptr(), path.len(), c"%s/state.tmp".as_ptr(), slotdir.as_ptr());

    if unlink(path.as_ptr()) < 0 && errno() != ENOENT {
        ereport!(PANIC, errmsg!("could not remove file \"{}\": {}", CStr(path.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
    }

    snprintf(path.as_mut_ptr(), path.len(), c"%s/state".as_ptr(), slotdir.as_ptr());

    elog!(DEBUG1, "restoring replication slot from \"{}\"", CStr(path.as_ptr()));

    let fd = OpenTransientFile(path.as_ptr(), O_RDWR | PG_BINARY);
    if fd < 0 {
        ereport!(PANIC, errmsg!("could not open file \"{}\": {}", CStr(path.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
    }

    pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(PANIC, errmsg!("could not fsync file \"{}\": {}", CStr(path.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
    }
    pgstat_report_wait_end();

    START_CRIT_SECTION();
    fsync_fname(slotdir.as_ptr(), true);
    END_CRIT_SECTION();

    pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_READ);
    let read_bytes = read(
        fd,
        &mut cp as *mut ReplicationSlotOnDisk as *mut c_void,
        REPLICATION_SLOT_ON_DISK_CONSTANT_SIZE,
    );
    pgstat_report_wait_end();

    if read_bytes != REPLICATION_SLOT_ON_DISK_CONSTANT_SIZE as isize {
        if read_bytes < 0 {
            ereport!(PANIC, errmsg!("could not read file \"{}\": {}", CStr(path.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
        } else {
            ereport!(PANIC, errmsg!(
                    "could not read file \"{}\": read {} of {}",
                    CStr(path.as_ptr()),
                    read_bytes,
                    REPLICATION_SLOT_ON_DISK_CONSTANT_SIZE
                )) /* C also: errcode */;
        }
    }

    if cp.magic != SLOT_MAGIC {
        ereport!(PANIC, errmsg!(
                "replication slot file \"{}\" has wrong magic number: {} instead of {}",
                CStr(path.as_ptr()),
                cp.magic,
                SLOT_MAGIC
            )) /* C also: errcode */;
    }

    if cp.version != SLOT_VERSION {
        ereport!(PANIC, errmsg!(
                "replication slot file \"{}\" has unsupported version {}",
                CStr(path.as_ptr()),
                cp.version
            )) /* C also: errcode */;
    }

    if cp.length != REPLICATION_SLOT_ON_DISK_V2_SIZE as u32 {
        ereport!(PANIC, errmsg!(
                "replication slot file \"{}\" has corrupted length {}",
                CStr(path.as_ptr()),
                cp.length
            )) /* C also: errcode */;
    }

    pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_READ);
    let read_bytes = read(
        fd,
        ((&mut cp as *mut ReplicationSlotOnDisk as *mut u8)
            .add(REPLICATION_SLOT_ON_DISK_CONSTANT_SIZE)) as *mut c_void,
        cp.length as usize,
    );
    pgstat_report_wait_end();

    if read_bytes != cp.length as isize {
        if read_bytes < 0 {
            ereport!(PANIC, errmsg!("could not read file \"{}\": {}", CStr(path.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
        } else {
            ereport!(PANIC, errmsg!(
                    "could not read file \"{}\": read {} of {}",
                    CStr(path.as_ptr()),
                    read_bytes,
                    cp.length
                )) /* C also: errcode */;
        }
    }

    if CloseTransientFile(fd) != 0 {
        ereport!(PANIC, errmsg!("could not close file \"{}\": {}", CStr(path.as_ptr()), strerror_r())) /* C also: errcode_for_file_access */;
    }

    let mut checksum = INIT_CRC32C();
    let checksum_start =
        ((&cp as *const ReplicationSlotOnDisk as *const u8)
            .add(REPLICATION_SLOT_ON_DISK_NOT_CHECKSUMMED_SIZE));
    checksum = COMP_CRC32C(checksum, checksum_start as *const c_void, REPLICATION_SLOT_ON_DISK_CHECKSUMMED_SIZE);
    checksum = FIN_CRC32C(checksum);

    if !EQ_CRC32C(checksum, cp.checksum) {
        ereport!(PANIC, errmsg!(
                "checksum mismatch for replication slot file \"{}\": is {}, should be {}",
                CStr(path.as_ptr()),
                checksum,
                cp.checksum
            ));
    }

    if cp.slotdata.persistency != RS_PERSISTENT {
        if !rmtree(slotdir.as_ptr(), true) {
            ereport!(WARNING, errmsg!("could not remove directory \"{}\"", CStr(slotdir.as_ptr())));
        }
        fsync_fname(PG_REPLSLOT_DIR.as_ptr(), true);
        return;
    }

    if cp.slotdata.database != InvalidOid {
        if wal_level < WAL_LEVEL_LOGICAL {
            ereport!(FATAL, errmsg!(
                    "logical replication slot \"{}\" exists, but \"wal_level\" < \"logical\"",
                    CStr(NameStr(&cp.slotdata.name))
                )) /* C also: errcode, errhint */;
        }
        if StandbyMode && !EnableHotStandby {
            ereport!(FATAL, errmsg!(
                    "logical replication slot \"{}\" exists on the standby, but \"hot_standby\" = \"off\"",
                    CStr(NameStr(&cp.slotdata.name))
                )) /* C also: errcode, errhint */;
        }
    } else if wal_level < WAL_LEVEL_REPLICA {
        ereport!(FATAL, errmsg!(
                "physical replication slot \"{}\" exists, but \"wal_level\" < \"replica\"",
                CStr(NameStr(&cp.slotdata.name))
            )) /* C also: errcode, errhint */;
    }

    let ctl = crate::replication::slotfuncs::ReplicationSlotCtl;
    'outer: for i in 0..max_replication_slots as usize {
        let slot = &mut (*ctl).replication_slots[i];
        if slot.in_use {
            continue;
        }

        core::ptr::copy_nonoverlapping(
            &cp.slotdata as *const ReplicationSlotPersistentData,
            &mut slot.data as *mut ReplicationSlotPersistentData,
            1,
        );

        slot.effective_xmin = cp.slotdata.xmin;
        slot.effective_catalog_xmin = cp.slotdata.catalog_xmin;
        slot.last_saved_confirmed_flush = cp.slotdata.confirmed_flush;
        slot.last_saved_restart_lsn = cp.slotdata.restart_lsn;

        slot.candidate_catalog_xmin = InvalidTransactionId;
        slot.candidate_xmin_lsn = InvalidXLogRecPtr;
        slot.candidate_restart_lsn = InvalidXLogRecPtr;
        slot.candidate_restart_valid = InvalidXLogRecPtr;

        slot.in_use = true;
        slot.active_pid = 0;

        if now == 0 {
            now = GetCurrentTimestamp();
        }
        ReplicationSlotSetInactiveSince(slot, now, false);

        restored = true;
        break 'outer;
    }

    if !restored {
        ereport!(FATAL, errmsg!("too many replication slots active before shutdown")) /* C also: errhint */;
    }
}

// ------------------------------------------------------------------
// Invalidation cause name maps
// ------------------------------------------------------------------

/// Map an invalidation reason name to ReplicationSlotInvalidationCause.
pub fn GetSlotInvalidationCause(cause_name: *const c_char) -> ReplicationSlotInvalidationCause {
    Assert!(!cause_name.is_null());
    for entry in &SLOT_INVALIDATION_CAUSES {
        if unsafe { strcmp(cause_name, entry.cause_name.as_ptr() as *const c_char) } == 0 {
            return entry.cause;
        }
    }
    Assert!(false);
    RS_INVAL_NONE
}

/// Map a ReplicationSlotInvalidationCause to its name string.
pub fn GetSlotInvalidationCauseName(
    cause: ReplicationSlotInvalidationCause,
) -> *const c_char {
    for entry in &SLOT_INVALIDATION_CAUSES {
        if entry.cause == cause {
            return entry.cause_name.as_ptr() as *const c_char;
        }
    }
    Assert!(false);
    c"none".as_ptr()
}

// ------------------------------------------------------------------
// synchronized_standby_slots GUC helpers
// ------------------------------------------------------------------

/// Validate slot names given in the synchronized_standby_slots GUC.
unsafe fn validate_sync_standby_slots(
    rawname: *mut c_char,
    elemlist: *mut *mut List,
) -> bool {
    if !SplitIdentifierString(rawname, b',' as c_char, elemlist) {
        GUC_check_errdetail(c"List syntax is invalid.".as_ptr());
        return false;
    }

    // Iterate the list
    let mut cell = list_head(*elemlist);
    while !cell.is_null() {
        let name = lfirst(cell) as *const c_char;
        let mut err_code: c_int = 0;
        let mut err_msg: *mut c_char = core::ptr::null_mut();
        let mut err_hint: *mut c_char = core::ptr::null_mut();

        if !ReplicationSlotValidateNameInternal(name, &mut err_code, &mut err_msg, &mut err_hint) {
            GUC_check_errcode(err_code);
            GUC_check_errdetail(err_msg as *const c_char);
            if !err_hint.is_null() {
                GUC_check_errhint(err_hint as *const c_char);
            }
            return false;
        }
        cell = lnext(*elemlist, cell);
    }
    true
}

/// GUC check_hook for synchronized_standby_slots.
pub unsafe fn check_synchronized_standby_slots(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if **newval == 0 {
        return true;
    }

    let rawname = pstrdup(*newval);
    let mut elemlist: *mut List = core::ptr::null_mut();

    let ok = validate_sync_standby_slots(rawname, &mut elemlist);

    if !ok || elemlist.is_null() {
        pfree(rawname as *mut c_void);
        list_free(elemlist);
        return ok;
    }

    // Compute size for SyncStandbySlotsConfigData
    let mut size = core::mem::offset_of!(SyncStandbySlotsConfigData, slot_names);
    let mut cell = list_head(elemlist);
    while !cell.is_null() {
        let slot_name = lfirst(cell) as *const c_char;
        size += strlen(slot_name) + 1;
        cell = lnext(elemlist, cell);
    }

    let config = guc_malloc(LOG, size) as *mut SyncStandbySlotsConfigData;
    if config.is_null() {
        return false;
    }

    (*config).nslotnames = list_length(elemlist);

    let mut ptr = (*config).slot_names.as_mut_ptr();
    let mut cell = list_head(elemlist);
    while !cell.is_null() {
        let slot_name = lfirst(cell) as *const c_char;
        let slen = strlen(slot_name);
        core::ptr::copy_nonoverlapping(slot_name, ptr, slen + 1);
        ptr = ptr.add(slen + 1);
        cell = lnext(elemlist, cell);
    }

    *extra = config as *mut c_void;

    pfree(rawname as *mut c_void);
    list_free(elemlist);
    true
}

/// GUC assign_hook for synchronized_standby_slots.
pub unsafe fn assign_synchronized_standby_slots(_newval: *const c_char, extra: *mut c_void) {
    ss_oldest_flush_lsn = InvalidXLogRecPtr;
    synchronized_standby_slots_config = extra as *mut SyncStandbySlotsConfigData;
}

/// Check if slot_name is in the synchronized_standby_slots GUC.
pub unsafe fn SlotExistsInSyncStandbySlots(slot_name: *const c_char) -> bool {
    if synchronized_standby_slots_config.is_null() {
        return false;
    }

    let mut standby_slot_name = (*synchronized_standby_slots_config).slot_names.as_ptr();
    for _ in 0..(*synchronized_standby_slots_config).nslotnames {
        if strcmp(standby_slot_name, slot_name) == 0 {
            return true;
        }
        standby_slot_name = standby_slot_name.add(strlen(standby_slot_name) + 1);
    }
    false
}

/// Return true if the slots in synchronized_standby_slots have caught up to wait_for_lsn.
pub unsafe fn StandbySlotsHaveCaughtup(wait_for_lsn: XLogRecPtr, elevel: c_int) -> bool {
    let mut caught_up_slot_num: c_int = 0;
    let mut min_restart_lsn: XLogRecPtr = InvalidXLogRecPtr;

    if synchronized_standby_slots_config.is_null() {
        return true;
    }

    if RecoveryInProgress() {
        return true;
    }

    if !XLogRecPtrIsInvalid(ss_oldest_flush_lsn) && ss_oldest_flush_lsn >= wait_for_lsn {
        return true;
    }

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    let mut name = (*synchronized_standby_slots_config).slot_names.as_ptr();
    for _ in 0..(*synchronized_standby_slots_config).nslotnames {
        let slot = SearchNamedReplicationSlot(name, false);

        if slot.is_null() {
            ereport!(elevel, errmsg!(
                    "replication slot \"{}\" specified in parameter \"{}\" does not exist",
                    CStr(name),
                    "synchronized_standby_slots"
                )) /* C also: errcode, errdetail, errhint */;
            break;
        }

        if SlotIsLogical(slot) {
            ereport!(elevel, errmsg!(
                    "cannot specify logical replication slot \"{}\" in parameter \"{}\"",
                    CStr(name),
                    "synchronized_standby_slots"
                )) /* C also: errcode, errdetail, errhint */;
            break;
        }

        SpinLockAcquire(&mut (*slot).mutex);
        let restart_lsn = (*slot).data.restart_lsn;
        let invalidated = (*slot).data.invalidated != RS_INVAL_NONE;
        let inactive = (*slot).active_pid == 0;
        SpinLockRelease(&mut (*slot).mutex);

        if invalidated {
            ereport!(elevel, errmsg!(
                    "physical replication slot \"{}\" specified in parameter \"{}\" has been invalidated",
                    CStr(name),
                    "synchronized_standby_slots"
                )) /* C also: errcode, errdetail, errhint */;
            break;
        }

        if XLogRecPtrIsInvalid(restart_lsn) || restart_lsn < wait_for_lsn {
            if inactive {
                ereport!(elevel, errmsg!(
                        "replication slot \"{}\" specified in parameter \"{}\" does not have active_pid",
                        CStr(name),
                        "synchronized_standby_slots"
                    )) /* C also: errcode, errdetail, errhint */;
            }
            break;
        }

        Assert!(restart_lsn >= wait_for_lsn);

        if XLogRecPtrIsInvalid(min_restart_lsn) || min_restart_lsn > restart_lsn {
            min_restart_lsn = restart_lsn;
        }

        caught_up_slot_num += 1;
        name = name.add(strlen(name) + 1);
    }

    LWLockRelease(ReplicationSlotControlLock);

    if caught_up_slot_num != (*synchronized_standby_slots_config).nslotnames {
        return false;
    }

    Assert!(XLogRecPtrIsInvalid(ss_oldest_flush_lsn) || min_restart_lsn >= ss_oldest_flush_lsn);
    ss_oldest_flush_lsn = min_restart_lsn;

    true
}

/// Wait for physical standbys to confirm receiving wait_for_lsn.
pub unsafe fn WaitForStandbyConfirmation(wait_for_lsn: XLogRecPtr) {
    if !(*MyReplicationSlot).data.failover || synchronized_standby_slots_config.is_null() {
        return;
    }

    use crate::replication::walsender_private::WalSndCtl;
    // wal_confirm_rcv_cv is a c_void stub in walsender_private; cast to real ConditionVariable ptr
    let cv = &raw mut (*WalSndCtl).wal_confirm_rcv_cv as *mut ConditionVariable;
    ConditionVariablePrepareToSleep(cv);

    loop {
        CHECK_FOR_INTERRUPTS();

        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if StandbySlotsHaveCaughtup(wait_for_lsn, WARNING) {
            break;
        }

        ConditionVariableTimedSleep(
            cv,
            1000,
            WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION,
        );
    }

    ConditionVariableCancelSleep();
}

// ------------------------------------------------------------------
// TODO stubs for unported symbols
// ------------------------------------------------------------------

// TODO(pg-port): real LWLockAcquire lives in storage/lmgr/lwlock.c
unsafe fn LWLockAcquire(_lock: LWLock, _mode: LWLockMode) -> bool { unimplemented!() }
// TODO(pg-port): real LWLockRelease lives in storage/lmgr/lwlock.c
unsafe fn LWLockRelease(_lock: LWLock) { unimplemented!() }
// TODO(pg-port): real LWLockInitialize lives in storage/lmgr/lwlock.c
unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) { unimplemented!() }
// TODO(pg-port): real LWLockHeldByMeInMode lives in storage/lmgr/lwlock.c
unsafe fn LWLockHeldByMeInMode(_lock: LWLock, _mode: LWLockMode) -> bool { unimplemented!() }
// TODO(pg-port): real LWLockHeldByMe lives in storage/lmgr/lwlock.c
unsafe fn LWLockHeldByMe(_lock: LWLock) -> bool { unimplemented!() }

const LWTRANCHE_REPLICATION_SLOT_IO: c_int = 0; // TODO(pg-port): real value lives in storage/lmgr/lwlock.h

// TODO(pg-port): real before_shmem_exit lives in storage/ipc/ipc.c
unsafe fn before_shmem_exit(_func: unsafe extern "C" fn(c_int, Datum), _arg: Datum) { unimplemented!() }

// TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void { unimplemented!() }
// TODO(pg-port): real add_size lives in storage/ipc/shmem.c
unsafe fn add_size(s1: Size, s2: Size) -> Size { s1.saturating_add(s2) }
// TODO(pg-port): real mul_size lives in storage/ipc/shmem.c
unsafe fn mul_size(s1: Size, s2: Size) -> Size { s1.saturating_mul(s2) }

// TODO(pg-port): real ProcArraySetReplicationSlotXmin lives in storage/ipc/procarray.c
unsafe fn ProcArraySetReplicationSlotXmin(
    _xmin: TransactionId,
    _catalog_xmin: TransactionId,
    _already_locked: bool,
) { unimplemented!() }
// TODO(pg-port): real ProcArrayLock lives in storage/ipc/procarray.c
static mut ProcArrayLock: LWLock = core::ptr::null_mut();

// TODO(pg-port): real MyProc lives in storage/lmgr/proc.c
static mut MyProc: *mut PGPROC = core::ptr::null_mut();
// TODO(pg-port): real ProcGlobal lives in storage/lmgr/proc.c
static mut ProcGlobal: *mut PROC_HDR = core::ptr::null_mut();

#[repr(C)]
struct PGPROC {
    pub statusFlags: u8,
    pub pgxactoff: c_int,
}
#[repr(C)]
struct PROC_HDR {
    pub statusFlags: [u8; 1], // FLEXIBLE_ARRAY_MEMBER
}

const PROC_IN_LOGICAL_DECODING: u8 = 0x10; // TODO(pg-port): real value lives in storage/lmgr/proc.h

// TODO(pg-port): real XLogSetReplicationSlotMinimumLSN lives in access/transam/xlog.c
unsafe fn XLogSetReplicationSlotMinimumLSN(_lsn: XLogRecPtr) { unimplemented!() }
// TODO(pg-port): real XLogGetLastRemovedSegno lives in access/transam/xlog.c
unsafe fn XLogGetLastRemovedSegno() -> XLogSegNo { unimplemented!() }
// TODO(pg-port): real XLByteToSeg lives in access/transam/xlog_internal.h
unsafe fn XLByteToSeg(_xlrp: XLogRecPtr, _segno: *mut XLogSegNo, _wal_segsz: c_int) { unimplemented!() }
// TODO(pg-port): real XLogSegNoOffsetToRecPtr lives in access/transam/xlog_internal.h
unsafe fn XLogSegNoOffsetToRecPtr(_seg: XLogSegNo, _offset: u32, _sz: c_int, _lsn: *mut XLogRecPtr) { unimplemented!() }
// TODO(pg-port): real LogStandbySnapshot lives in access/transam/standby.c
unsafe fn LogStandbySnapshot() -> XLogRecPtr { unimplemented!() }
// TODO(pg-port): real XLogFlush lives in access/transam/xlog.c
unsafe fn XLogFlush(_lsn: XLogRecPtr) { unimplemented!() }
// TODO(pg-port): real GetRedoRecPtr lives in access/transam/xloginsert.c
unsafe fn GetRedoRecPtr() -> XLogRecPtr { unimplemented!() }
// TODO(pg-port): real GetXLogReplayRecPtr lives in access/transam/xlogrecovery.c
unsafe fn GetXLogReplayRecPtr(_tli: *mut c_int) -> XLogRecPtr { unimplemented!() }
// TODO(pg-port): real GetXLogInsertRecPtr lives in access/transam/xloginsert.c
unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr { unimplemented!() }
// TODO(pg-port): real RecoveryInProgress lives in access/transam/xlog.c
unsafe fn RecoveryInProgress() -> bool { unimplemented!() }

// TODO(pg-port): real wal_level lives in access/transam/xlog.c (GUC)
static mut wal_level: c_int = 0;
const WAL_LEVEL_REPLICA: c_int = 1;
const WAL_LEVEL_LOGICAL: c_int = 2;
// TODO(pg-port): real wal_segment_size lives in access/transam/xlog.c (GUC)
static mut wal_segment_size: c_int = 16 * 1024 * 1024;

// TODO(pg-port): real StandbyMode lives in access/transam/xlog_internal.h
static mut StandbyMode: bool = false;
// TODO(pg-port): real EnableHotStandby lives in access/transam/xlog.c
static mut EnableHotStandby: bool = false;

// TODO(pg-port): real IsSyncingReplicationSlots lives in replication/slotsync.c
unsafe fn IsSyncingReplicationSlots() -> bool { unimplemented!() }
// TODO(pg-port): real IsBinaryUpgrade lives in miscadmin.h
static mut IsBinaryUpgrade: bool = false;

// TODO(pg-port): real MyDatabaseId lives in utils/init/globals.c
static mut MyDatabaseId: Oid = 0;
// TODO(pg-port): real MyProcPid lives in utils/init/globals.c
static mut MyProcPid: c_int = 0;
// TODO(pg-port): real IsUnderPostmaster lives in utils/init/globals.c
static mut IsUnderPostmaster: bool = false;
// TODO(pg-port): real MyBackendType lives in utils/init/globals.c
static mut MyBackendType: c_int = 0;

// TODO(pg-port): real am_walsender lives in replication/walsender.c
static mut am_walsender: bool = false;
// TODO(pg-port): real log_replication_commands lives in replication/walsender.c
static mut log_replication_commands: bool = false;

// TODO(pg-port): real pgstat_create_replslot lives in utils/activity/pgstat_replslot.c
unsafe fn pgstat_create_replslot(_slot: *mut ReplicationSlot) { unimplemented!() }
// TODO(pg-port): real pgstat_acquire_replslot lives in utils/activity/pgstat_replslot.c
unsafe fn pgstat_acquire_replslot(_slot: *mut ReplicationSlot) { unimplemented!() }
// TODO(pg-port): real pgstat_drop_replslot lives in utils/activity/pgstat_replslot.c
unsafe fn pgstat_drop_replslot(_slot: *mut ReplicationSlot) { unimplemented!() }
// TODO(pg-port): real pgstat_report_wait_start lives in utils/activity/wait_event.c
unsafe fn pgstat_report_wait_start(_info: uint32) {}
// TODO(pg-port): real pgstat_report_wait_end lives in utils/activity/wait_event.c
unsafe fn pgstat_report_wait_end() {}

// Wait event constants -- TODO(pg-port): real values live in utils/activity/wait_event_types.h
const WAIT_EVENT_REPLICATION_SLOT_DROP: uint32 = 0;
const WAIT_EVENT_REPLICATION_SLOT_WRITE: uint32 = 1;
const WAIT_EVENT_REPLICATION_SLOT_SYNC: uint32 = 2;
const WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC: uint32 = 3;
const WAIT_EVENT_REPLICATION_SLOT_READ: uint32 = 4;
const WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION: uint32 = 5;

// TODO(pg-port): real SendProcSignal lives in storage/ipc/procsignal.c
unsafe fn SendProcSignal(_pid: c_int, _reason: c_int, _backendid: c_int) -> c_int { 0 }
const INVALID_PROC_NUMBER: c_int = -1;
const PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT: c_int = 5;
const SIGTERM: c_int = 15;
// TODO(pg-port): real kill lives in port/win32_port.c or libc
unsafe fn kill(_pid: c_int, _sig: c_int) -> c_int { 0 }

// GUC helpers
type GucSource = c_int;
// TODO(pg-port): real PGC_SIGHUP lives in utils/guc.h
const PGC_SIGHUP: c_int = 0;
// TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
unsafe fn ProcessConfigFile(_context: c_int) {}
// TODO(pg-port): real ConfigReloadPending lives in postmaster/interrupt.h
static mut ConfigReloadPending: bool = false;
// TODO(pg-port): real GUC_check_errdetail lives in utils/misc/guc.c
unsafe fn GUC_check_errdetail(_fmt: *const c_char) {}
// TODO(pg-port): real GUC_check_errcode lives in utils/misc/guc.c
unsafe fn GUC_check_errcode(_sqlerrcode: c_int) {}
// TODO(pg-port): real GUC_check_errhint lives in utils/misc/guc.c
unsafe fn GUC_check_errhint(_fmt: *const c_char) {}
// TODO(pg-port): real guc_malloc lives in utils/misc/guc.c
unsafe fn guc_malloc(_elevel: c_int, _size: usize) -> *mut c_void { unimplemented!() }
// TODO(pg-port): real SplitIdentifierString lives in utils/adt/varlena.c
unsafe fn SplitIdentifierString(_rawstring: *mut c_char, _separator: c_char, _namelist: *mut *mut List) -> bool { unimplemented!() }

// List helpers -- TODO(pg-port): real ones live in nodes/pg_list.c
type List = c_void;
unsafe fn list_head(_l: *const List) -> *mut ListCell { unimplemented!() }
unsafe fn lnext(_l: *const List, _c: *mut ListCell) -> *mut ListCell { unimplemented!() }
unsafe fn lfirst(_c: *mut ListCell) -> *mut c_void { unimplemented!() }
unsafe fn list_length(_l: *const List) -> c_int { unimplemented!() }
unsafe fn list_free(_l: *mut List) { unimplemented!() }
enum ListCell {}

// File / OS helpers
// TODO(pg-port): real OpenTransientFile lives in storage/file/fd.c
unsafe fn OpenTransientFile(_path: *const c_char, _flags: c_int) -> c_int { unimplemented!() }
// TODO(pg-port): real CloseTransientFile lives in storage/file/fd.c
unsafe fn CloseTransientFile(_fd: c_int) -> c_int { unimplemented!() }
// TODO(pg-port): real fsync_fname lives in storage/file/fd.c
unsafe fn fsync_fname(_fname: *const c_char, _isdir: bool) { unimplemented!() }
// TODO(pg-port): real MakePGDirectory lives in storage/file/fd.c
unsafe fn MakePGDirectory(_directoryName: *const c_char) -> c_int { unimplemented!() }
// TODO(pg-port): real AllocateDir lives in storage/file/fd.c
unsafe fn AllocateDir(_path: *const c_char) -> *mut DIR { unimplemented!() }
// TODO(pg-port): real ReadDir lives in storage/file/fd.c
unsafe fn ReadDir(_dir: *mut DIR, _path: *const c_char) -> *mut dirent { unimplemented!() }
// TODO(pg-port): real FreeDir lives in storage/file/fd.c
unsafe fn FreeDir(_dir: *mut DIR) { unimplemented!() }
// TODO(pg-port): real pg_fsync lives in storage/file/fd.c
unsafe fn pg_fsync(_fd: c_int) -> c_int { unimplemented!() }
// TODO(pg-port): real rmtree lives in common/file_utils.c
unsafe fn rmtree(_path: *const c_char, _rmtopdir: bool) -> bool { unimplemented!() }
// TODO(pg-port): real get_dirent_type lives in common/file_utils.c
unsafe fn get_dirent_type(_path: *const c_char, _de: *const dirent, _look_through_symlinks: bool, _elevel: c_int) -> PGFileType { unimplemented!() }
// TODO(pg-port): real pg_str_endswith lives in common/string.c
unsafe fn pg_str_endswith(_str: *const c_char, _end: *const c_char) -> bool { unimplemented!() }
type PGFileType = c_int;
const PGFILETYPE_ERROR: PGFileType = -1;
const PGFILETYPE_DIR: PGFileType = 2;

// C stdlib wrappers
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    fn rename(old: *const c_char, new: *const c_char) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn stat(path: *const c_char, buf: *mut libc_stat) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
}

// errno helpers (platform)
unsafe fn errno() -> c_int { *libc_errno() }
unsafe fn set_errno(e: c_int) { *libc_errno() = e; }
extern "C" { fn __errno_location() -> *mut c_int; }
unsafe fn libc_errno() -> *mut c_int { __errno_location() }
const ENOENT: c_int = 2;
const ENOSPC: c_int = 28;
const O_CREAT: c_int = 64;
const O_EXCL: c_int = 128;
const O_WRONLY: c_int = 1;
const O_RDWR: c_int = 2;
const O_RDONLY: c_int = 0;
unsafe fn strerror_r() -> &'static str { "" } // display only

// Opaque C types
enum DIR {}
#[repr(C)]
struct dirent {
    pub d_name: [c_char; 256],
}
#[repr(C)]
struct libc_stat {
    pub st_mode: u32,
    _pad: [u8; 128],
}
unsafe fn S_ISDIR(mode: u32) -> bool { mode & 0o170000 == 0o040000 }

// palloc / pfree / pstrdup
unsafe fn palloc(size: Size) -> *mut c_void { unimplemented!() }
unsafe fn pfree(ptr: *mut c_void) { unimplemented!() }
unsafe fn pstrdup(s: *const c_char) -> *mut c_char { unimplemented!() }
// psprintf: stub returning null (format + optional arg dropped; stubs only)
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char { core::ptr::null_mut() }

// namestrcpy
unsafe fn namestrcpy(name: *mut NameData, s: *const c_char) { unimplemented!() }
unsafe fn NameStr(name: *const NameData) -> *const c_char {
    (*name).data.as_ptr()
}

// StringInfo
#[repr(C)]
struct StringInfoData {
    data: *mut c_char,
    len: c_int,
    maxlen: c_int,
    cursor: c_int,
}
type StringInfo = *mut StringInfoData;
unsafe fn initStringInfo(_str: StringInfo) { unimplemented!() }
// appendStringInfo: stub (format args dropped; stubs only)
unsafe fn appendStringInfo(_str: StringInfo, _fmt: *const c_char) { unimplemented!() }
unsafe fn appendStringInfoString(_str: StringInfo, _s: *const c_char) { unimplemented!() }

// Error reporting stubs (real ones come from prelude)
unsafe fn errcode(_sqlerrcode: c_int) -> c_int { 0 }
unsafe fn errcode_for_file_access() -> c_int { 0 }
unsafe fn errmsg_internal(_fmt: *const c_char) -> c_int { 0 }
unsafe fn errdetail_internal(_fmt: *const c_char) -> c_int { 0 }

// Error code constants
const ERRCODE_INVALID_NAME: c_int = 0;
const ERRCODE_NAME_TOO_LONG: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_CONFIGURATION_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_OBJECT_IN_USE: c_int = 0;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_DATA_CORRUPTED: c_int = 0;

// Log level constants
const DEBUG1: c_int = -1;
const LOG: c_int = 15;
const WARNING: c_int = 19;
const ERROR: c_int = 20;
const FATAL: c_int = 21;
const PANIC: c_int = 22;

// Helpers from C.rs (NameStr / NameData already defined above)
// CStr helper for display in errmsg! (not emitted, just for documentation)
struct CStr(*const c_char);
impl core::fmt::Display for CStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<cstr>")
    }
}

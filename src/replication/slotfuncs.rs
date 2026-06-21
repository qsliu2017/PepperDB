//! src/backend/replication/slotfuncs.c
//!
//! slotfuncs.c
//!    Support functions for replication slots
//!
//! Copyright (c) 2012-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/replication/slotfuncs.c

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::c::{uint64, TransactionId};
use crate::postgres_ext::Oid;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn memset(s: *mut std::ffi::c_void, c: c_int, n: usize) -> *mut std::ffi::c_void;
}

// Macro stubs (kept local to this module - defined before first use so they are
// in textual scope for the whole file; not #[macro_export] to avoid colliding
// with same-named macros that already live at the crate root).
macro_rules! XLByteToSeg {
    ($xlrp:expr, $logSegNo:ident, $wal_segsz:expr) => {
        $logSegNo = ($xlrp / ($wal_segsz as $crate::access::transam::xlogdefs::XLogRecPtr))
            as $crate::replication::slotfuncs::XLogSegNo;
    };
}
macro_rules! XLogSegNoOffsetToRecPtr {
    ($segno:expr, $offset:expr, $wal_segsz:expr, $dest:ident) => {
        $dest = (($segno) * ($wal_segsz as u64) + ($offset as u64))
            as $crate::access::transam::xlogdefs::XLogRecPtr;
    };
}
macro_rules! Max {
    ($a:expr, $b:expr) => {
        if $a > $b {
            $a
        } else {
            $b
        }
    };
}
macro_rules! Min {
    ($a:expr, $b:expr) => {
        if $a < $b {
            $a
        } else {
            $b
        }
    };
}
macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *mut c_char
    };
}
macro_rules! appendStringInfo {
    ($si:expr, $($arg:tt)*) => {{
        let _ = $si;
        let _ = format!($($arg)*);
        // TODO: lib/stringinfo.c appendStringInfo
    }};
}

/*
 * Helper function for creating a new physical replication slot with
 * given arguments. Note that this function doesn't release the created
 * slot.
 *
 * If restart_lsn is a valid value, we use it without WAL reservation
 * routine. So the caller must guarantee that WAL is available.
 */
unsafe fn create_physical_replication_slot(
    name: *mut c_char,
    immediately_reserve: bool,
    temporary: bool,
    restart_lsn: XLogRecPtr,
) {
    Assert!(MyReplicationSlot().is_null());

    /* acquire replication slot, this will check for conflicting names */
    ReplicationSlotCreate(
        name,
        false,
        if temporary { RS_TEMPORARY } else { RS_PERSISTENT },
        false,
        false,
        false,
    );

    if immediately_reserve {
        /* Reserve WAL as the user asked for it */
        if XLogRecPtrIsInvalid(restart_lsn) {
            ReplicationSlotReserveWal();
        } else {
            (*MyReplicationSlot()).data.restart_lsn = restart_lsn;
        }

        /* Write this slot to disk */
        ReplicationSlotMarkDirty();
        ReplicationSlotSave();
    }
}

/*
 * SQL function for creating a new physical (streaming replication)
 * replication slot.
 */
pub unsafe fn pg_create_physical_replication_slot(fcinfo: FunctionCallInfo) -> Datum {
    let name: Name = PG_GETARG_NAME(fcinfo, 0);
    let immediately_reserve: bool = PG_GETARG_BOOL(fcinfo, 1);
    let temporary: bool = PG_GETARG_BOOL(fcinfo, 2);
    let mut values: [Datum; 2] = [0; 2];
    let mut nulls: [bool; 2] = [false; 2];
    let mut tupdesc: TupleDesc = std::ptr::null_mut();

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    CheckSlotPermissions();

    CheckSlotRequirements();

    create_physical_replication_slot(
        NameStr!(*name),
        immediately_reserve,
        temporary,
        InvalidXLogRecPtr,
    );

    values[0] = NameGetDatum(&(*MyReplicationSlot()).data.name);
    nulls[0] = false;

    if immediately_reserve {
        values[1] = LSNGetDatum((*MyReplicationSlot()).data.restart_lsn);
        nulls[1] = false;
    } else {
        nulls[1] = true;
    }

    let tuple: HeapTuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    let result: Datum = HeapTupleGetDatum(tuple);

    ReplicationSlotRelease();

    return result;
}

/*
 * Helper function for creating a new logical replication slot with
 * given arguments. Note that this function doesn't release the created
 * slot.
 *
 * When find_startpoint is false, the slot's confirmed_flush is not set; it's
 * caller's responsibility to ensure it's set to something sensible.
 */
unsafe fn create_logical_replication_slot(
    name: *mut c_char,
    plugin: *mut c_char,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    restart_lsn: XLogRecPtr,
    find_startpoint: bool,
) {
    let ctx: *mut LogicalDecodingContext;

    Assert!(MyReplicationSlot().is_null());

    /*
     * Acquire a logical decoding slot, this will check for conflicting names.
     * Initially create persistent slot as ephemeral - that allows us to
     * nicely handle errors during initialization because it'll get dropped if
     * this transaction fails. We'll make it persistent at the end. Temporary
     * slots can be created as temporary from beginning as they get dropped on
     * error as well.
     */
    ReplicationSlotCreate(
        name,
        true,
        if temporary { RS_TEMPORARY } else { RS_EPHEMERAL },
        two_phase,
        failover,
        false,
    );

    /*
     * Create logical decoding context to find start point or, if we don't
     * need it, to 1) bump slot's restart_lsn and xmin 2) check plugin sanity.
     *
     * Note: when !find_startpoint this is still important, because it's at
     * this point that the output plugin is validated.
     */
    ctx = CreateInitDecodingContext(
        plugin,
        NIL,
        false, /* just catalogs is OK */
        restart_lsn,
        XL_ROUTINE_slotfuncs(),
        None,
        None,
        None,
    );

    /*
     * If caller needs us to determine the decoding start point, do so now.
     * This might take a while.
     */
    if find_startpoint {
        DecodingContextFindStartpoint(ctx);
    }

    /* don't need the decoding context anymore */
    FreeDecodingContext(ctx);
}

/*
 * SQL function for creating a new logical replication slot.
 */
pub unsafe fn pg_create_logical_replication_slot(fcinfo: FunctionCallInfo) -> Datum {
    let name: Name = PG_GETARG_NAME(fcinfo, 0);
    let plugin: Name = PG_GETARG_NAME(fcinfo, 1);
    let temporary: bool = PG_GETARG_BOOL(fcinfo, 2);
    let two_phase: bool = PG_GETARG_BOOL(fcinfo, 3);
    let failover: bool = PG_GETARG_BOOL(fcinfo, 4);
    let mut tupdesc: TupleDesc = std::ptr::null_mut();
    let mut values: [Datum; 2] = [0; 2];
    let mut nulls: [bool; 2] = [false; 2];

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    CheckSlotPermissions();

    CheckLogicalDecodingRequirements();

    create_logical_replication_slot(
        NameStr!(*name),
        NameStr!(*plugin),
        temporary,
        two_phase,
        failover,
        InvalidXLogRecPtr,
        true,
    );

    values[0] = NameGetDatum(&(*MyReplicationSlot()).data.name);
    values[1] = LSNGetDatum((*MyReplicationSlot()).data.confirmed_flush);

    memset(
        nulls.as_mut_ptr() as *mut std::ffi::c_void,
        0,
        std::mem::size_of_val(&nulls),
    );

    let tuple: HeapTuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    let result: Datum = HeapTupleGetDatum(tuple);

    /* ok, slot is now fully created, mark it as persistent if needed */
    if !temporary {
        ReplicationSlotPersist();
    }
    ReplicationSlotRelease();

    return result;
}

/*
 * SQL function for dropping a replication slot.
 */
pub unsafe fn pg_drop_replication_slot(fcinfo: FunctionCallInfo) -> Datum {
    let name: Name = PG_GETARG_NAME(fcinfo, 0);

    CheckSlotPermissions();

    CheckSlotRequirements();

    ReplicationSlotDrop(NameStr!(*name), true);

    return PG_RETURN_VOID();
}

/*
 * pg_get_replication_slots - SQL SRF showing all replication slots
 * that currently exist on the database cluster.
 */
pub unsafe fn pg_get_replication_slots(fcinfo: FunctionCallInfo) -> Datum {
    const PG_GET_REPLICATION_SLOTS_COLS: usize = 20;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let currlsn: XLogRecPtr;
    let mut slotno: c_int;

    /*
     * We don't require any special permission to see this function's data
     * because nothing should be sensitive. The most critical being the slot
     * name, which shouldn't contain anything particularly sensitive.
     */

    InitMaterializedSRF(fcinfo, 0);

    currlsn = GetXLogWriteRecPtr();

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    slotno = 0;
    while slotno < max_replication_slots {
        let slot: *mut ReplicationSlot =
            &mut (*ReplicationSlotCtl).replication_slots[slotno as usize];
        let mut slot_contents: ReplicationSlot;
        let mut values: [Datum; PG_GET_REPLICATION_SLOTS_COLS] = [0; PG_GET_REPLICATION_SLOTS_COLS];
        let mut nulls: [bool; PG_GET_REPLICATION_SLOTS_COLS] =
            [false; PG_GET_REPLICATION_SLOTS_COLS];
        let mut walstate: WALAvailability;
        let mut i: usize;
        let cause: ReplicationSlotInvalidationCause;

        if !(*slot).in_use {
            slotno += 1;
            continue;
        }

        /* Copy slot contents while holding spinlock, then examine at leisure */
        SpinLockAcquire(&mut (*slot).mutex);
        slot_contents = core::ptr::read(slot); // C: slot_contents = *slot (memcopy)
        SpinLockRelease(&mut (*slot).mutex);

        memset(
            values.as_mut_ptr() as *mut std::ffi::c_void,
            0,
            std::mem::size_of_val(&values),
        );
        memset(
            nulls.as_mut_ptr() as *mut std::ffi::c_void,
            0,
            std::mem::size_of_val(&nulls),
        );

        i = 0;
        values[i] = NameGetDatum(&slot_contents.data.name);
        i += 1;

        if slot_contents.data.database == InvalidOid {
            nulls[i] = true;
            i += 1;
        } else {
            values[i] = NameGetDatum(&slot_contents.data.plugin);
            i += 1;
        }

        if slot_contents.data.database == InvalidOid {
            values[i] = CStringGetTextDatum(c"physical".as_ptr());
            i += 1;
        } else {
            values[i] = CStringGetTextDatum(c"logical".as_ptr());
            i += 1;
        }

        if slot_contents.data.database == InvalidOid {
            nulls[i] = true;
            i += 1;
        } else {
            values[i] = ObjectIdGetDatum(slot_contents.data.database);
            i += 1;
        }

        values[i] = BoolGetDatum(slot_contents.data.persistency == RS_TEMPORARY);
        i += 1;
        values[i] = BoolGetDatum(slot_contents.active_pid != 0);
        i += 1;

        if slot_contents.active_pid != 0 {
            values[i] = Int32GetDatum(slot_contents.active_pid);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.xmin != InvalidTransactionId {
            values[i] = TransactionIdGetDatum(slot_contents.data.xmin);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.catalog_xmin != InvalidTransactionId {
            values[i] = TransactionIdGetDatum(slot_contents.data.catalog_xmin);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.restart_lsn != InvalidXLogRecPtr {
            values[i] = LSNGetDatum(slot_contents.data.restart_lsn);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.confirmed_flush != InvalidXLogRecPtr {
            values[i] = LSNGetDatum(slot_contents.data.confirmed_flush);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        /*
         * If the slot has not been invalidated, test availability from
         * restart_lsn.
         */
        if slot_contents.data.invalidated != RS_INVAL_NONE {
            walstate = WALAVAIL_REMOVED;
        } else {
            walstate = GetWALAvailability(slot_contents.data.restart_lsn);
        }

        match walstate {
            WALAVAIL_INVALID_LSN => {
                nulls[i] = true;
                i += 1;
            }

            WALAVAIL_RESERVED => {
                values[i] = CStringGetTextDatum(c"reserved".as_ptr());
                i += 1;
            }

            WALAVAIL_EXTENDED => {
                values[i] = CStringGetTextDatum(c"extended".as_ptr());
                i += 1;
            }

            WALAVAIL_UNRESERVED => {
                values[i] = CStringGetTextDatum(c"unreserved".as_ptr());
                i += 1;
            }

            WALAVAIL_REMOVED => {
                /*
                 * If we read the restart_lsn long enough ago, maybe that file
                 * has been removed by now.  However, the walsender could have
                 * moved forward enough that it jumped to another file after
                 * we looked.  If checkpointer signalled the process to
                 * termination, then it's definitely lost; but if a process is
                 * still alive, then "unreserved" seems more appropriate.
                 *
                 * If we do change it, save the state for safe_wal_size below.
                 */
                let mut handled = false;
                if !XLogRecPtrIsInvalid(slot_contents.data.restart_lsn) {
                    let pid: c_int;

                    SpinLockAcquire(&mut (*slot).mutex);
                    pid = (*slot).active_pid;
                    slot_contents.data.restart_lsn = (*slot).data.restart_lsn;
                    SpinLockRelease(&mut (*slot).mutex);
                    if pid != 0 {
                        values[i] = CStringGetTextDatum(c"unreserved".as_ptr());
                        i += 1;
                        walstate = WALAVAIL_UNRESERVED;
                        handled = true;
                    }
                }
                if !handled {
                    values[i] = CStringGetTextDatum(c"lost".as_ptr());
                    i += 1;
                }
            }

            _ => {}
        }

        /*
         * safe_wal_size is only computed for slots that have not been lost,
         * and only if there's a configured maximum size.
         */
        if walstate == WALAVAIL_REMOVED || max_slot_wal_keep_size_mb < 0 {
            nulls[i] = true;
            i += 1;
        } else {
            let targetSeg: XLogSegNo;
            let slotKeepSegs: uint64;
            let keepSegs: uint64;
            let failSeg: XLogSegNo;
            let failLSN: XLogRecPtr;

            XLByteToSeg!(slot_contents.data.restart_lsn, targetSeg, wal_segment_size);

            /* determine how many segments can be kept by slots */
            slotKeepSegs = XLogMBVarToSegs(max_slot_wal_keep_size_mb, wal_segment_size);
            /* ditto for wal_keep_size */
            keepSegs = XLogMBVarToSegs(wal_keep_size_mb, wal_segment_size);

            /* if currpos reaches failLSN, we lose our segment */
            failSeg = targetSeg + Max!(slotKeepSegs, keepSegs) + 1;
            XLogSegNoOffsetToRecPtr!(failSeg, 0, wal_segment_size, failLSN);

            values[i] = Int64GetDatum((failLSN - currlsn) as i64);
            i += 1;
        }

        values[i] = BoolGetDatum(slot_contents.data.two_phase);
        i += 1;

        if slot_contents.data.two_phase
            && !XLogRecPtrIsInvalid(slot_contents.data.two_phase_at)
        {
            values[i] = LSNGetDatum(slot_contents.data.two_phase_at);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.inactive_since > 0 {
            values[i] = TimestampTzGetDatum(slot_contents.inactive_since);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        cause = slot_contents.data.invalidated;

        if SlotIsPhysical(&slot_contents) {
            nulls[i] = true;
            i += 1;
        } else {
            /*
             * rows_removed and wal_level_insufficient are the only two
             * reasons for the logical slot's conflict with recovery.
             */
            if cause == RS_INVAL_HORIZON || cause == RS_INVAL_WAL_LEVEL {
                values[i] = BoolGetDatum(true);
                i += 1;
            } else {
                values[i] = BoolGetDatum(false);
                i += 1;
            }
        }

        if cause == RS_INVAL_NONE {
            nulls[i] = true;
            i += 1;
        } else {
            values[i] = CStringGetTextDatum(GetSlotInvalidationCauseName(cause));
            i += 1;
        }

        values[i] = BoolGetDatum(slot_contents.data.failover);
        i += 1;

        values[i] = BoolGetDatum(slot_contents.data.synced);
        i += 1;

        Assert!(i == PG_GET_REPLICATION_SLOTS_COLS);

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        slotno += 1;
    }

    LWLockRelease(ReplicationSlotControlLock);

    return 0 as Datum;
}

/*
 * Helper function for advancing our physical replication slot forward.
 *
 * The LSN position to move to is compared simply to the slot's restart_lsn,
 * knowing that any position older than that would be removed by successive
 * checkpoints.
 */
unsafe fn pg_physical_replication_slot_advance(moveto: XLogRecPtr) -> XLogRecPtr {
    let startlsn: XLogRecPtr = (*MyReplicationSlot()).data.restart_lsn;
    let mut retlsn: XLogRecPtr = startlsn;

    Assert!(moveto != InvalidXLogRecPtr);

    if startlsn < moveto {
        SpinLockAcquire(&mut (*MyReplicationSlot()).mutex);
        (*MyReplicationSlot()).data.restart_lsn = moveto;
        SpinLockRelease(&mut (*MyReplicationSlot()).mutex);
        retlsn = moveto;

        /*
         * Dirty the slot so as it is written out at the next checkpoint. Note
         * that the LSN position advanced may still be lost in the event of a
         * crash, but this makes the data consistent after a clean shutdown.
         */
        ReplicationSlotMarkDirty();

        /*
         * Wake up logical walsenders holding logical failover slots after
         * updating the restart_lsn of the physical slot.
         */
        PhysicalWakeupLogicalWalSnd();
    }

    return retlsn;
}

/*
 * Advance our logical replication slot forward. See
 * LogicalSlotAdvanceAndCheckSnapState for details.
 */
unsafe fn pg_logical_replication_slot_advance(moveto: XLogRecPtr) -> XLogRecPtr {
    return LogicalSlotAdvanceAndCheckSnapState(moveto, std::ptr::null_mut());
}

/*
 * SQL function for moving the position in a replication slot.
 */
pub unsafe fn pg_replication_slot_advance(fcinfo: FunctionCallInfo) -> Datum {
    let slotname: Name = PG_GETARG_NAME(fcinfo, 0);
    let mut moveto: XLogRecPtr = PG_GETARG_LSN(fcinfo, 1);
    let endlsn: XLogRecPtr;
    let minlsn: XLogRecPtr;
    let mut tupdesc: TupleDesc = std::ptr::null_mut();
    let mut values: [Datum; 2] = [0; 2];
    let mut nulls: [bool; 2] = [false; 2];

    Assert!(MyReplicationSlot().is_null());

    CheckSlotPermissions();

    if XLogRecPtrIsInvalid(moveto) {
        ereport!(ERROR, "invalid target WAL LSN");
    }

    /* Build a tuple descriptor for our result type */
    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    /*
     * We can't move slot past what's been flushed/replayed so clamp the
     * target position accordingly.
     */
    if !RecoveryInProgress() {
        moveto = Min!(moveto, GetFlushRecPtr(std::ptr::null_mut()));
    } else {
        moveto = Min!(moveto, GetXLogReplayRecPtr(std::ptr::null_mut()));
    }

    /* Acquire the slot so we "own" it */
    ReplicationSlotAcquire(NameStr!(*slotname), true, true);

    /* A slot whose restart_lsn has never been reserved cannot be advanced */
    if XLogRecPtrIsInvalid((*MyReplicationSlot()).data.restart_lsn) {
        elog!(
            ERROR,
            "replication slot \"{}\" cannot be advanced",
            cstr_to_display(NameStr!(*slotname))
        );
    }

    /*
     * Check if the slot is not moving backwards.  Physical slots rely simply
     * on restart_lsn as a minimum point, while logical slots have confirmed
     * consumption up to confirmed_flush, meaning that in both cases data
     * older than that is not available anymore.
     */
    if OidIsValid((*MyReplicationSlot()).data.database) {
        minlsn = (*MyReplicationSlot()).data.confirmed_flush;
    } else {
        minlsn = (*MyReplicationSlot()).data.restart_lsn;
    }

    if moveto < minlsn {
        elog!(
            ERROR,
            "cannot advance replication slot to {:X}/{:X}, minimum is {:X}/{:X}",
            LSN_FORMAT_HI(moveto),
            LSN_FORMAT_LO(moveto),
            LSN_FORMAT_HI(minlsn),
            LSN_FORMAT_LO(minlsn)
        );
    }

    /* Do the actual slot update, depending on the slot type */
    if OidIsValid((*MyReplicationSlot()).data.database) {
        endlsn = pg_logical_replication_slot_advance(moveto);
    } else {
        endlsn = pg_physical_replication_slot_advance(moveto);
    }

    values[0] = NameGetDatum(&(*MyReplicationSlot()).data.name);
    nulls[0] = false;

    /*
     * Recompute the minimum LSN and xmin across all slots to adjust with the
     * advancing potentially done.
     */
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN();

    ReplicationSlotRelease();

    /* Return the reached position. */
    values[1] = LSNGetDatum(endlsn);
    nulls[1] = false;

    let tuple: HeapTuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    let result: Datum = HeapTupleGetDatum(tuple);

    return result;
}

/*
 * Helper function of copying a replication slot.
 */
unsafe fn copy_replication_slot(fcinfo: FunctionCallInfo, logical_slot: bool) -> Datum {
    let src_name: Name = PG_GETARG_NAME(fcinfo, 0);
    let dst_name: Name = PG_GETARG_NAME(fcinfo, 1);
    let mut src: *mut ReplicationSlot = std::ptr::null_mut();
    let mut first_slot_contents: ReplicationSlot = std::mem::zeroed();
    let second_slot_contents: ReplicationSlot;
    let src_restart_lsn: XLogRecPtr;
    let src_islogical: bool;
    let mut temporary: bool;
    let mut plugin: *mut c_char;
    let mut values: [Datum; 2] = [0; 2];
    let mut nulls: [bool; 2] = [false; 2];
    let mut tupdesc: TupleDesc = std::ptr::null_mut();

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    CheckSlotPermissions();

    if logical_slot {
        CheckLogicalDecodingRequirements();
    } else {
        CheckSlotRequirements();
    }

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    /*
     * We need to prevent the source slot's reserved WAL from being removed,
     * but we don't want to lock that slot for very long, and it can advance
     * in the meantime.  So obtain the source slot's data, and create a new
     * slot using its restart_lsn.  Afterwards we lock the source slot again
     * and verify that the data we copied (name, type) has not changed
     * incompatibly.  No inconvenient WAL removal can occur once the new slot
     * is created -- but since WAL removal could have occurred before we
     * managed to create the new slot, we advance the new slot's restart_lsn
     * to the source slot's updated restart_lsn the second time we lock it.
     */
    for i in 0..max_replication_slots {
        let s: *mut ReplicationSlot = &mut (*ReplicationSlotCtl).replication_slots[i as usize];

        if (*s).in_use && strcmp(NameStr!((*s).data.name), NameStr!(*src_name)) == 0 {
            /* Copy the slot contents while holding spinlock */
            SpinLockAcquire(&mut (*s).mutex);
            first_slot_contents = core::ptr::read(s); // C: first_slot_contents = *s
            SpinLockRelease(&mut (*s).mutex);
            src = s;
            break;
        }
    }

    LWLockRelease(ReplicationSlotControlLock);

    if src.is_null() {
        elog!(
            ERROR,
            "replication slot \"{}\" does not exist",
            cstr_to_display(NameStr!(*src_name))
        );
    }

    src_islogical = SlotIsLogical(&first_slot_contents);
    src_restart_lsn = first_slot_contents.data.restart_lsn;
    temporary = first_slot_contents.data.persistency == RS_TEMPORARY;
    plugin = if logical_slot {
        NameStr!(first_slot_contents.data.plugin)
    } else {
        std::ptr::null_mut()
    };

    /* Check type of replication slot */
    if src_islogical != logical_slot {
        if src_islogical {
            elog!(
                ERROR,
                "cannot copy physical replication slot \"{}\" as a logical replication slot",
                cstr_to_display(NameStr!(*src_name))
            );
        } else {
            elog!(
                ERROR,
                "cannot copy logical replication slot \"{}\" as a physical replication slot",
                cstr_to_display(NameStr!(*src_name))
            );
        }
    }

    /* Copying non-reserved slot doesn't make sense */
    if XLogRecPtrIsInvalid(src_restart_lsn) {
        ereport!(ERROR, "cannot copy a replication slot that doesn't reserve WAL");
    }

    /* Cannot copy an invalidated replication slot */
    if first_slot_contents.data.invalidated != RS_INVAL_NONE {
        elog!(
            ERROR,
            "cannot copy invalidated replication slot \"{}\"",
            cstr_to_display(NameStr!(*src_name))
        );
    }

    /* Overwrite params from optional arguments */
    if PG_NARGS(fcinfo) >= 3 {
        temporary = PG_GETARG_BOOL(fcinfo, 2);
    }
    if PG_NARGS(fcinfo) >= 4 {
        Assert!(logical_slot);
        plugin = NameStr!(*(PG_GETARG_NAME(fcinfo, 3)));
    }

    /* Create new slot and acquire it */
    if logical_slot {
        /*
         * We must not try to read WAL, since we haven't reserved it yet --
         * hence pass find_startpoint false.  confirmed_flush will be set
         * below, by copying from the source slot.
         *
         * We don't copy the failover option to prevent potential issues with
         * slot synchronization. For instance, if a slot was synchronized to
         * the standby, then dropped on the primary, and immediately recreated
         * by copying from another existing slot with much earlier restart_lsn
         * and confirmed_flush_lsn, the slot synchronization would only
         * observe the LSN of the same slot moving backward. As slot
         * synchronization does not copy the restart_lsn and
         * confirmed_flush_lsn backward (see update_local_synced_slot() for
         * details), if a failover happens before the primary's slot catches
         * up, logical replication cannot continue using the synchronized slot
         * on the promoted standby because the slot retains the restart_lsn
         * and confirmed_flush_lsn that are much later than expected.
         */
        create_logical_replication_slot(
            NameStr!(*dst_name),
            plugin,
            temporary,
            false,
            false,
            src_restart_lsn,
            false,
        );
    } else {
        create_physical_replication_slot(NameStr!(*dst_name), true, temporary, src_restart_lsn);
    }

    /*
     * Update the destination slot to current values of the source slot;
     * recheck that the source slot is still the one we saw previously.
     */
    {
        let copy_effective_xmin: TransactionId;
        let copy_effective_catalog_xmin: TransactionId;
        let copy_xmin: TransactionId;
        let copy_catalog_xmin: TransactionId;
        let copy_restart_lsn: XLogRecPtr;
        let copy_confirmed_flush: XLogRecPtr;
        let copy_islogical: bool;
        let copy_name: *mut c_char;

        /* Copy data of source slot again */
        SpinLockAcquire(&mut (*src).mutex);
        second_slot_contents = core::ptr::read(src); // C: second_slot_contents = *src
        SpinLockRelease(&mut (*src).mutex);

        copy_effective_xmin = second_slot_contents.effective_xmin;
        copy_effective_catalog_xmin = second_slot_contents.effective_catalog_xmin;

        copy_xmin = second_slot_contents.data.xmin;
        copy_catalog_xmin = second_slot_contents.data.catalog_xmin;
        copy_restart_lsn = second_slot_contents.data.restart_lsn;
        copy_confirmed_flush = second_slot_contents.data.confirmed_flush;

        /* for existence check */
        copy_name = NameStr!(second_slot_contents.data.name);
        copy_islogical = SlotIsLogical(&second_slot_contents);

        /*
         * Check if the source slot still exists and is valid. We regard it as
         * invalid if the type of replication slot or name has been changed,
         * or the restart_lsn either is invalid or has gone backward. (The
         * restart_lsn could go backwards if the source slot is dropped and
         * copied from an older slot during installation.)
         *
         * Since erroring out will release and drop the destination slot we
         * don't need to release it here.
         */
        if copy_restart_lsn < src_restart_lsn
            || src_islogical != copy_islogical
            || strcmp(copy_name, NameStr!(*src_name)) != 0
        {
            elog!(
                ERROR,
                "could not copy replication slot \"{}\"",
                cstr_to_display(NameStr!(*src_name))
            );
        }

        /* The source slot must have a consistent snapshot */
        if src_islogical && XLogRecPtrIsInvalid(copy_confirmed_flush) {
            elog!(
                ERROR,
                "cannot copy unfinished logical replication slot \"{}\"",
                cstr_to_display(NameStr!(*src_name))
            );
        }

        /*
         * Copying an invalid slot doesn't make sense. Note that the source
         * slot can become invalid after we create the new slot and copy the
         * data of source slot. This is possible because the operations in
         * InvalidateObsoleteReplicationSlots() are not serialized with this
         * function. Even though we can't detect such a case here, the copied
         * slot will become invalid in the next checkpoint cycle.
         */
        if second_slot_contents.data.invalidated != RS_INVAL_NONE {
            elog!(
                ERROR,
                "cannot copy replication slot \"{}\"",
                cstr_to_display(NameStr!(*src_name))
            );
        }

        /* Install copied values again */
        SpinLockAcquire(&mut (*MyReplicationSlot()).mutex);
        (*MyReplicationSlot()).effective_xmin = copy_effective_xmin;
        (*MyReplicationSlot()).effective_catalog_xmin = copy_effective_catalog_xmin;

        (*MyReplicationSlot()).data.xmin = copy_xmin;
        (*MyReplicationSlot()).data.catalog_xmin = copy_catalog_xmin;
        (*MyReplicationSlot()).data.restart_lsn = copy_restart_lsn;
        (*MyReplicationSlot()).data.confirmed_flush = copy_confirmed_flush;
        SpinLockRelease(&mut (*MyReplicationSlot()).mutex);

        ReplicationSlotMarkDirty();
        ReplicationSlotsComputeRequiredXmin(false);
        ReplicationSlotsComputeRequiredLSN();
        ReplicationSlotSave();

        // #ifdef USE_ASSERT_CHECKING
        /* Check that the restart_lsn is available */
        #[cfg(debug_assertions)]
        {
            let segno: XLogSegNo;

            XLByteToSeg!(copy_restart_lsn, segno, wal_segment_size);
            Assert!(XLogGetLastRemovedSegno() < segno);
        }
        // #endif
    }

    /* target slot fully created, mark as persistent if needed */
    if logical_slot && !temporary {
        ReplicationSlotPersist();
    }

    /* All done.  Set up the return values */
    values[0] = NameGetDatum(dst_name);
    nulls[0] = false;
    if !XLogRecPtrIsInvalid((*MyReplicationSlot()).data.confirmed_flush) {
        values[1] = LSNGetDatum((*MyReplicationSlot()).data.confirmed_flush);
        nulls[1] = false;
    } else {
        nulls[1] = true;
    }

    let tuple: HeapTuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    let result: Datum = HeapTupleGetDatum(tuple);

    ReplicationSlotRelease();

    return result;
}

/* The wrappers below are all to appease opr_sanity */
pub unsafe fn pg_copy_logical_replication_slot_a(fcinfo: FunctionCallInfo) -> Datum {
    return copy_replication_slot(fcinfo, true);
}

pub unsafe fn pg_copy_logical_replication_slot_b(fcinfo: FunctionCallInfo) -> Datum {
    return copy_replication_slot(fcinfo, true);
}

pub unsafe fn pg_copy_logical_replication_slot_c(fcinfo: FunctionCallInfo) -> Datum {
    return copy_replication_slot(fcinfo, true);
}

pub unsafe fn pg_copy_physical_replication_slot_a(fcinfo: FunctionCallInfo) -> Datum {
    return copy_replication_slot(fcinfo, false);
}

pub unsafe fn pg_copy_physical_replication_slot_b(fcinfo: FunctionCallInfo) -> Datum {
    return copy_replication_slot(fcinfo, false);
}

/*
 * Synchronize failover enabled replication slots to a standby server
 * from the primary server.
 */
pub unsafe fn pg_sync_replication_slots(_fcinfo: FunctionCallInfo) -> Datum {
    let wrconn: *mut WalReceiverConn;
    let mut err: *mut c_char = std::ptr::null_mut();
    let mut app_name: StringInfoData = std::mem::zeroed();

    CheckSlotPermissions();

    if !RecoveryInProgress() {
        ereport!(
            ERROR,
            "replication slots can only be synchronized to a standby server"
        );
    }

    ValidateSlotSyncParams(ERROR);

    /* Load the libpq-specific functions */
    load_file(c"libpqwalreceiver".as_ptr(), false);

    let _ = CheckAndGetDbnameFromConninfo();

    initStringInfo(&mut app_name);
    if *cluster_name() != 0 {
        appendStringInfo!(&mut app_name, "{}_slotsync", cstr_to_display(cluster_name()));
    } else {
        appendStringInfoString(&mut app_name, c"slotsync".as_ptr());
    }

    /* Connect to the primary server. */
    wrconn = walrcv_connect(
        PrimaryConnInfo,
        false,
        false,
        false,
        app_name.data,
        &mut err,
    );

    if wrconn.is_null() {
        elog!(
            ERROR,
            "synchronization worker \"{}\" could not connect to the primary server: {}",
            cstr_to_display(app_name.data),
            cstr_to_display(err)
        );
    }

    pfree(app_name.data as *mut std::ffi::c_void);

    SyncReplicationSlots(wrconn);

    walrcv_disconnect(wrconn);

    return PG_RETURN_VOID();
}

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies
// ----------------------------------------------------------------------------

// Types - canonical struct definitions live in slot.rs; re-export here.
pub use crate::replication::slot::{
    ReplicationSlot,
    ReplicationSlotCtlData,
    ReplicationSlotPersistentData,
    ReplicationSlotInvalidationCause,
};
// NameData and slock_t from their canonical crate homes
pub use crate::c::NameData;
pub use crate::storage::lmgr::s_lock::slock_t;
#[allow(non_camel_case_types)]
pub type Name = *mut NameData;
pub enum LogicalDecodingContext {}
#[repr(C)]
pub struct ReturnSetInfo {
    pub setResult: *mut std::ffi::c_void,
    pub setDesc: *mut std::ffi::c_void,
}
pub enum WalReceiverConn {}
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub resultinfo: *mut std::ffi::c_void,
}
#[allow(non_camel_case_types)]
pub type HeapTuple = *mut std::ffi::c_void;
#[allow(non_camel_case_types)]
pub type TupleDesc = *mut std::ffi::c_void;
#[allow(non_camel_case_types)]
pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;
#[allow(non_camel_case_types)]
pub type WALAvailability = c_int;
// ReplicationSlotInvalidationCause is re-exported from slot (see above)
#[allow(non_camel_case_types)]
pub type XLogSegNo = uint64;
#[allow(non_camel_case_types)]
pub type LWLock = *mut std::ffi::c_void;
#[allow(non_camel_case_types)]
pub type LWLockMode = c_int;

// Constants
pub const TYPEFUNC_COMPOSITE: c_int = 1;
pub const LW_SHARED: LWLockMode = 1;
pub const LW_EXCLUSIVE: LWLockMode = 2;
pub const RS_PERSISTENT: c_int = 0;
pub const RS_EPHEMERAL: c_int = 1;
pub const RS_TEMPORARY: c_int = 2;
pub const RS_INVAL_NONE: ReplicationSlotInvalidationCause = 0;
pub const RS_INVAL_WAL_REMOVED: ReplicationSlotInvalidationCause = 1;
pub const RS_INVAL_HORIZON: ReplicationSlotInvalidationCause = 2;
pub const RS_INVAL_WAL_LEVEL: ReplicationSlotInvalidationCause = 3;
pub const WALAVAIL_INVALID_LSN: WALAvailability = 0;
pub const WALAVAIL_RESERVED: WALAvailability = 1;
pub const WALAVAIL_EXTENDED: WALAvailability = 2;
pub const WALAVAIL_UNRESERVED: WALAvailability = 3;
pub const WALAVAIL_REMOVED: WALAvailability = 4;
pub const NIL: *mut std::ffi::c_void = std::ptr::null_mut();
pub const InvalidXLogRecPtr: XLogRecPtr = 0;
pub const InvalidOid: Oid = 0;
pub const InvalidTransactionId: TransactionId = 0;

// Globals (stubs)
#[allow(non_upper_case_globals)]
pub static mut max_replication_slots: c_int = 0;
#[allow(non_upper_case_globals)]
pub static mut max_slot_wal_keep_size_mb: c_int = 0;
#[allow(non_upper_case_globals)]
pub static mut wal_keep_size_mb: c_int = 0;
#[allow(non_upper_case_globals)]
pub static mut wal_segment_size: c_int = 0;
pub use crate::backend_link_shims::ReplicationSlotControlLock;
#[allow(non_upper_case_globals)]
pub static mut ReplicationSlotCtl: *mut ReplicationSlotCtlData = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
pub static mut PrimaryConnInfo: *mut c_char = std::ptr::null_mut();

unsafe fn MyReplicationSlot() -> *mut ReplicationSlot {
    unimplemented!() // TODO: replication/slot.c
}
unsafe fn cluster_name() -> *mut c_char {
    unimplemented!() // TODO: utils/misc/guc_tables.c
}

// Macro-like helpers / display helpers
fn cstr_to_display(_s: *const c_char) -> &'static str {
    "" // TODO: c-string display helper
}
#[allow(non_snake_case)]
fn LSN_FORMAT_HI(_lsn: XLogRecPtr) -> u32 {
    0 // TODO: access/xlogdefs.h LSN_FORMAT_ARGS
}
#[allow(non_snake_case)]
fn LSN_FORMAT_LO(_lsn: XLogRecPtr) -> u32 {
    0 // TODO: access/xlogdefs.h LSN_FORMAT_ARGS
}

// Function stubs
unsafe fn ReplicationSlotCreate(
    name: *mut c_char,
    db_specific: bool,
    persistency: c_int,
    two_phase: bool,
    failover: bool,
    synced: bool,
) { crate::replication::slot::ReplicationSlotCreate(name as _, db_specific, persistency as _, two_phase, failover, synced) }
unsafe fn ReplicationSlotReserveWal() { crate::replication::slot::ReplicationSlotReserveWal() }
unsafe fn ReplicationSlotMarkDirty() { crate::replication::slot::ReplicationSlotMarkDirty() }
unsafe fn ReplicationSlotSave() { crate::replication::slot::ReplicationSlotSave() }
unsafe fn ReplicationSlotRelease() { crate::replication::slot::ReplicationSlotRelease() }
unsafe fn ReplicationSlotPersist() { crate::replication::slot::ReplicationSlotPersist() }
unsafe fn ReplicationSlotDrop(name: *mut c_char, nowait: bool) { crate::replication::slot::ReplicationSlotDrop(name as _, nowait) }
unsafe fn ReplicationSlotAcquire(name: *mut c_char, nowait: bool, error_if_invalid: bool) { crate::replication::slot::ReplicationSlotAcquire(name as _, nowait, error_if_invalid) }
unsafe fn ReplicationSlotsComputeRequiredXmin(already_locked: bool) { crate::replication::slot::ReplicationSlotsComputeRequiredXmin(already_locked) }
unsafe fn ReplicationSlotsComputeRequiredLSN() { crate::replication::slot::ReplicationSlotsComputeRequiredLSN() }
unsafe fn CheckSlotPermissions() { crate::replication::slot::CheckSlotPermissions() }
unsafe fn CheckSlotRequirements() { crate::replication::slot::CheckSlotRequirements() }
unsafe fn CheckLogicalDecodingRequirements() { crate::replication::logical::logical::CheckLogicalDecodingRequirements() }
unsafe fn GetSlotInvalidationCauseName(cause: ReplicationSlotInvalidationCause) -> *const c_char { crate::replication::slot::GetSlotInvalidationCauseName(cause) }
unsafe fn SlotIsPhysical(slot: *const ReplicationSlot) -> bool { crate::replication::slot::SlotIsPhysical(slot as _) }
unsafe fn SlotIsLogical(slot: *const ReplicationSlot) -> bool { crate::replication::slot::SlotIsLogical(slot as _) }
unsafe fn CreateInitDecodingContext(
    plugin: *mut c_char,
    output_plugin_options: *mut std::ffi::c_void,
    need_full_snapshot: bool,
    restart_lsn: XLogRecPtr,
    xl_routine: *mut std::ffi::c_void,
    prepare_write: Option<unsafe extern "C" fn()>,
    do_write: Option<unsafe extern "C" fn()>,
    update_progress: Option<unsafe extern "C" fn()>,
) -> *mut LogicalDecodingContext { unimplemented!() }
unsafe fn DecodingContextFindStartpoint(ctx: *mut LogicalDecodingContext) { crate::replication::logical::logical::DecodingContextFindStartpoint(ctx as _) }
unsafe fn FreeDecodingContext(ctx: *mut LogicalDecodingContext) { crate::replication::logical::logical::FreeDecodingContext(ctx as _) }
unsafe fn LogicalSlotAdvanceAndCheckSnapState(
    moveto: XLogRecPtr,
    found_consistent_snapshot: *mut bool,
) -> XLogRecPtr { crate::replication::logical::logical::LogicalSlotAdvanceAndCheckSnapState(moveto as _, found_consistent_snapshot as _) }
unsafe fn PhysicalWakeupLogicalWalSnd() { crate::replication::walsender::PhysicalWakeupLogicalWalSnd() }
unsafe fn XL_ROUTINE_slotfuncs() -> *mut std::ffi::c_void {
    unimplemented!() // TODO: access/xlogreader.h XL_ROUTINE
}
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn InitMaterializedSRF(fcinfo: FunctionCallInfo, flags: c_int) { crate::utils::fmgr::funcapi::InitMaterializedSRF(fcinfo as _, flags as _) }
unsafe fn GetXLogWriteRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetXLogWriteRecPtr() }
unsafe fn GetWALAvailability(targetLSN: XLogRecPtr) -> WALAvailability { crate::access::transam::xlog::GetWALAvailability(targetLSN as _) }
unsafe fn GetFlushRecPtr(insertTLI: *mut std::ffi::c_void) -> XLogRecPtr { crate::access::transam::xlog::GetFlushRecPtr(insertTLI as _) }
unsafe fn GetXLogReplayRecPtr(replayTLI: *mut std::ffi::c_void) -> XLogRecPtr { crate::access::transam::xlogrecovery::GetXLogReplayRecPtr(replayTLI as _) }
unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }
unsafe fn XLogGetLastRemovedSegno() -> XLogSegNo { crate::access::transam::xlog::XLogGetLastRemovedSegno() }
unsafe fn XLogMBVarToSegs(mb: c_int, wal_segsz: c_int) -> uint64 { crate::access::transam::xlog_internal::XLogMBVarToSegs(mb as _, wal_segsz as _) as _ }
unsafe fn LWLockAcquire(_lock: LWLock, _mode: LWLockMode) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}
unsafe fn LWLockRelease(_lock: LWLock) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockAcquire(_lock as _)
}
unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockRelease(_lock as _)
}
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn tuplestore_putvalues(
    state: *mut std::ffi::c_void,
    tdesc: *mut std::ffi::c_void,
    values: *mut Datum,
    isnull: *mut bool,
) { crate::utils::sort::tuplestore::tuplestore_putvalues(state as _, tdesc as _, values as _, isnull as _) }
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn load_file(filename: *const c_char, restricted: bool) { crate::utils::fmgr::dfmgr::load_file(filename as _, restricted) }
unsafe fn ValidateSlotSyncParams(elevel: c_int) -> bool { crate::replication::logical::slotsync::ValidateSlotSyncParams(elevel as _) }
unsafe fn CheckAndGetDbnameFromConninfo() -> *mut c_char { crate::replication::logical::slotsync::CheckAndGetDbnameFromConninfo() }
unsafe fn SyncReplicationSlots(wrconn: *mut WalReceiverConn) { crate::replication::logical::slotsync::SyncReplicationSlots(wrconn as _) }
unsafe fn walrcv_connect(
    conninfo: *mut c_char,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: *mut c_char,
    err: *mut *mut c_char,
) -> *mut WalReceiverConn { unimplemented!() }
unsafe fn walrcv_disconnect(_wrconn: *mut WalReceiverConn) {
    unimplemented!() // TODO: replication/walreceiver.h
}

// Datum conversion helper stubs (when not provided by prelude)
unsafe fn NameGetDatum(name: *const NameData) -> Datum { crate::postgres::NameGetDatum(name as _) }
unsafe fn LSNGetDatum(lsn: XLogRecPtr) -> Datum { crate::utils::adt::pg_lsn::LSNGetDatum(lsn as _) }
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/builtins.h
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn BoolGetDatum(_b: bool) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int32GetDatum(_i: c_int) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int64GetDatum(i: i64) -> Datum { crate::postgres::Int64GetDatum(i as _) }
unsafe fn TransactionIdGetDatum(_xid: TransactionId) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn TimestampTzGetDatum(_t: i64) -> Datum {
    unimplemented!() // TODO: utils/timestamp.h
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO: funcapi.h
}
unsafe fn PG_GETARG_NAME(_fcinfo: FunctionCallInfo, _n: c_int) -> Name {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_GETARG_BOOL(_fcinfo: FunctionCallInfo, _n: c_int) -> bool {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_GETARG_LSN(_fcinfo: FunctionCallInfo, _n: c_int) -> XLogRecPtr {
    unimplemented!() // TODO: utils/pg_lsn.h
}
unsafe fn PG_NARGS(_fcinfo: FunctionCallInfo) -> c_int {
    unimplemented!() // TODO: fmgr.h
}
fn PG_RETURN_VOID() -> Datum {
    0 as Datum
}
fn XLogRecPtrIsInvalid(lsn: XLogRecPtr) -> bool {
    lsn == InvalidXLogRecPtr
}
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

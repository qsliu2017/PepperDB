//! pg_upgrade_support.c - server-side functions to set backend global
//! variables to control oid and relfilenumber assignment, and do other special
//! hacks needed for pg_upgrade.

use crate::prelude::*;

use crate::{
    PG_ARGISNULL, PG_GETARG_BOOL, PG_GETARG_CHAR, PG_GETARG_DATUM, PG_GETARG_NAME, PG_GETARG_OID,
    PG_GETARG_TEXT_P, PG_GETARG_TEXT_PP, PG_RETURN_BOOL, PG_RETURN_VOID,
};

use crate::access::common::relation::{relation_close, relation_open};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::binary_upgrade::*;
use crate::catalog::catalog_oids::{ReplicationOriginRelationId, SubscriptionRelationId};
use crate::miscadmin::GetUserId;
use crate::nodes::pg_list::{lappend_oid, List, NIL};
use crate::storage::lockdefs::{AccessShareLock, RowExclusiveLock, LOCKMODE};
use crate::utils::adt::pg_lsn::DatumGetLSN;
use crate::utils::builtins::{text_to_cstring, TextDatumGetCString};
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::rel::Relation;

use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, RepOriginId, XLogRecPtr};
use crate::pg_config_manual::NAMEDATALEN;
use crate::utils::array::ArrayType;

// catalog/binary_upgrade.rs aliases RelFileNumber to common::relpath::RelFileNumber (== Oid).
use crate::common::relpath::RelFileNumber;

// IsBinaryUpgrade lives in utils/init/globals.rs.
use crate::utils::init::globals::IsBinaryUpgrade;

// ---------------------------------------------------------------------------
// Local const stubs for generated/external symbols.
// ---------------------------------------------------------------------------

// catalog/pg_type_d.h
const TEXTOID: Oid = 25;

// utils/errcodes.h - ERRCODE_CANT_CHANGE_RUNTIME_PARAM (not yet ported).
// Unused at runtime here because we route the runtime-arg message through elog!.

// ---------------------------------------------------------------------------
// Local stubs for not-yet-ported called functions.
// ---------------------------------------------------------------------------

// catalog/heap.h
unsafe fn SetAttrMissing(_relid: Oid, _attname: *mut c_char, _value: *mut c_char) {
    unimplemented!() // TODO: SetAttrMissing not yet ported
}

// commands/extension.h
unsafe fn get_extension_oid(_extname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: get_extension_oid not yet ported
}

unsafe fn InsertExtensionTuple(
    _extName: *const c_char,
    _extOwner: Oid,
    _schemaOid: Oid,
    _relocatable: bool,
    _extVersion: *const c_char,
    _extConfig: Datum,
    _extCondition: Datum,
    _requiredExtensions: *mut List,
) -> Oid {
    unimplemented!() // TODO: InsertExtensionTuple not yet ported
}

// catalog/namespace.h
unsafe fn get_namespace_oid(_nspname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: get_namespace_oid not yet ported
}

// utils/lsyscache.h
unsafe fn get_subscription_oid(_subname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: get_subscription_oid not yet ported
}

unsafe fn has_rolreplication(_roleid: Oid) -> bool {
    unimplemented!() // TODO: has_rolreplication not yet ported
}

// utils/array.h
unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() // TODO: deconstruct_array_builtin not yet ported
}

// catalog/pg_subscription_rel.h
unsafe fn AddSubscriptionRelState(
    _subid: Oid,
    _relid: Oid,
    _state: c_char,
    _sublsn: XLogRecPtr,
    _retain_lock: bool,
) {
    unimplemented!() // TODO: AddSubscriptionRelState not yet ported
}

// replication/worker_internal.h
unsafe fn ReplicationOriginNameForLogicalRep(
    _suboid: Oid,
    _relid: Oid,
    _originname: *mut c_char,
    _szoriginname: Size,
) {
    unimplemented!() // TODO: ReplicationOriginNameForLogicalRep not yet ported
}

// replication/origin.h
unsafe fn replorigin_by_name(_roname: *mut c_char, _missing_ok: bool) -> RepOriginId {
    unimplemented!() // TODO: replorigin_by_name not yet ported
}

unsafe fn replorigin_advance(
    _node: RepOriginId,
    _remote_commit: XLogRecPtr,
    _local_commit: XLogRecPtr,
    _go_backward: bool,
    _wal_log: bool,
) {
    unimplemented!() // TODO: replorigin_advance not yet ported
}

// replication/logical.h
unsafe fn LogicalReplicationSlotHasPendingWal(_end_of_wal: XLogRecPtr) -> bool {
    unimplemented!() // TODO: LogicalReplicationSlotHasPendingWal not yet ported
}

// access/xlog.h
unsafe fn GetFlushRecPtr(_insertTLI: *mut c_void) -> XLogRecPtr {
    unimplemented!() // TODO: GetFlushRecPtr not yet ported
}

// replication/slot.h
unsafe fn ReplicationSlotAcquire(_name: *const c_char, _nowait: bool, _error_if_invalid: bool) {
    unimplemented!() // TODO: ReplicationSlotAcquire not yet ported
}

unsafe fn ReplicationSlotRelease() {
    unimplemented!() // TODO: ReplicationSlotRelease not yet ported
}

// storage/lmgr.h
unsafe fn LockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: LockRelationOid not yet ported
}

unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: UnlockRelationOid not yet ported
}

// ---------------------------------------------------------------------------
// CHECK_IS_BINARY_UPGRADE - the C macro body, inlined per call site.
// ---------------------------------------------------------------------------
macro_rules! CHECK_IS_BINARY_UPGRADE {
    () => {
        if !IsBinaryUpgrade {
            ereport!(
                ERROR,
                "function can only be called when server is in binary upgrade mode"
            );
        }
    };
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_pg_tablespace_oid(fcinfo: FunctionCallInfo) -> Datum {
    let tbspoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_pg_tablespace_oid = tbspoid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_pg_type_oid(fcinfo: FunctionCallInfo) -> Datum {
    let typoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_pg_type_oid = typoid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_array_pg_type_oid(fcinfo: FunctionCallInfo) -> Datum {
    let typoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_array_pg_type_oid = typoid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_multirange_pg_type_oid(fcinfo: FunctionCallInfo) -> Datum {
    let typoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_mrng_pg_type_oid = typoid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_multirange_array_pg_type_oid(
    fcinfo: FunctionCallInfo,
) -> Datum {
    let typoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_mrng_array_pg_type_oid = typoid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_heap_pg_class_oid(fcinfo: FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_heap_pg_class_oid = reloid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_heap_relfilenode(fcinfo: FunctionCallInfo) -> Datum {
    let relfilenumber: RelFileNumber = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_heap_pg_class_relfilenumber = relfilenumber;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_index_pg_class_oid(fcinfo: FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_index_pg_class_oid = reloid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_index_relfilenode(fcinfo: FunctionCallInfo) -> Datum {
    let relfilenumber: RelFileNumber = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_index_pg_class_relfilenumber = relfilenumber;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_toast_pg_class_oid(fcinfo: FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_toast_pg_class_oid = reloid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_toast_relfilenode(fcinfo: FunctionCallInfo) -> Datum {
    let relfilenumber: RelFileNumber = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_toast_pg_class_relfilenumber = relfilenumber;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_pg_enum_oid(fcinfo: FunctionCallInfo) -> Datum {
    let enumoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_pg_enum_oid = enumoid;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_next_pg_authid_oid(fcinfo: FunctionCallInfo) -> Datum {
    let authoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_next_pg_authid_oid = authoid;
    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_create_empty_extension(fcinfo: FunctionCallInfo) -> Datum {
    let extName: *mut text;
    let schemaName: *mut text;
    let relocatable: bool;
    let extVersion: *mut text;
    let extConfig: Datum;
    let extCondition: Datum;
    let mut requiredExtensions: *mut List;

    CHECK_IS_BINARY_UPGRADE!();

    /* We must check these things before dereferencing the arguments */
    if PG_ARGISNULL!(fcinfo, 0)
        || PG_ARGISNULL!(fcinfo, 1)
        || PG_ARGISNULL!(fcinfo, 2)
        || PG_ARGISNULL!(fcinfo, 3)
    {
        elog!(
            ERROR,
            "null argument to binary_upgrade_create_empty_extension is not allowed"
        );
    }

    extName = PG_GETARG_TEXT_PP!(fcinfo, 0);
    schemaName = PG_GETARG_TEXT_PP!(fcinfo, 1);
    relocatable = PG_GETARG_BOOL!(fcinfo, 2);
    extVersion = PG_GETARG_TEXT_PP!(fcinfo, 3);

    if PG_ARGISNULL!(fcinfo, 4) {
        extConfig = PointerGetDatum(null());
    } else {
        extConfig = PG_GETARG_DATUM!(fcinfo, 4);
    }

    if PG_ARGISNULL!(fcinfo, 5) {
        extCondition = PointerGetDatum(null());
    } else {
        extCondition = PG_GETARG_DATUM!(fcinfo, 5);
    }

    requiredExtensions = NIL;
    if !PG_ARGISNULL!(fcinfo, 6) {
        let textArray: *mut ArrayType = PG_GETARG_DATUM!(fcinfo, 6) as *mut ArrayType;
        let mut textDatums: *mut Datum = null_mut();
        let mut ndatums: c_int = 0;
        let mut i: c_int;

        deconstruct_array_builtin(
            textArray,
            TEXTOID,
            &mut textDatums,
            null_mut(),
            &mut ndatums,
        );
        i = 0;
        while i < ndatums {
            let extName: *mut c_char = TextDatumGetCString(*textDatums.add(i as usize));
            let extOid: Oid = get_extension_oid(extName, false);

            requiredExtensions = lappend_oid(requiredExtensions, extOid);
            i += 1;
        }
    }

    InsertExtensionTuple(
        text_to_cstring(extName),
        GetUserId(),
        get_namespace_oid(text_to_cstring(schemaName), false),
        relocatable,
        text_to_cstring(extVersion),
        extConfig,
        extCondition,
        requiredExtensions,
    );

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_record_init_privs(fcinfo: FunctionCallInfo) -> Datum {
    let record_init_privs: bool = PG_GETARG_BOOL!(fcinfo, 0);

    CHECK_IS_BINARY_UPGRADE!();
    binary_upgrade_record_init_privs = record_init_privs;

    PG_RETURN_VOID!()
}

#[no_mangle]
pub unsafe fn binary_upgrade_set_missing_value(fcinfo: FunctionCallInfo) -> Datum {
    let table_id: Oid = PG_GETARG_OID!(fcinfo, 0);
    let attname: *mut text = PG_GETARG_TEXT_P!(fcinfo, 1);
    let value: *mut text = PG_GETARG_TEXT_P!(fcinfo, 2);
    let cattname: *mut c_char = text_to_cstring(attname);
    let cvalue: *mut c_char = text_to_cstring(value);

    CHECK_IS_BINARY_UPGRADE!();
    SetAttrMissing(table_id, cattname, cvalue);

    PG_RETURN_VOID!()
}

/*
 * Verify the given slot has already consumed all the WAL changes.
 *
 * Returns true if there are no decodable WAL records after the
 * confirmed_flush_lsn. Otherwise false.
 *
 * This is a special purpose function to ensure that the given slot can be
 * upgraded without data loss.
 */
#[no_mangle]
pub unsafe fn binary_upgrade_logical_slot_has_caught_up(fcinfo: FunctionCallInfo) -> Datum {
    let slot_name: Name;
    let end_of_wal: XLogRecPtr;
    let found_pending_wal: bool;

    CHECK_IS_BINARY_UPGRADE!();

    /*
     * Binary upgrades only allowed super-user connections so we must have
     * permission to use replication slots.
     */
    Assert!(has_rolreplication(GetUserId()));

    slot_name = PG_GETARG_NAME!(fcinfo, 0);

    /* Acquire the given slot */
    ReplicationSlotAcquire(NameStr(&*slot_name), true, true);

    Assert!(SlotIsLogical(MyReplicationSlot));

    /* Slots must be valid as otherwise we won't be able to scan the WAL */
    Assert!((*MyReplicationSlot).data.invalidated == RS_INVAL_NONE);

    end_of_wal = GetFlushRecPtr(null_mut());
    found_pending_wal = LogicalReplicationSlotHasPendingWal(end_of_wal);

    /* Clean up */
    ReplicationSlotRelease();

    PG_RETURN_BOOL!(!found_pending_wal)
}

/*
 * binary_upgrade_add_sub_rel_state
 *
 * Add the relation with the specified relation state to pg_subscription_rel
 * catalog.
 */
#[no_mangle]
pub unsafe fn binary_upgrade_add_sub_rel_state(fcinfo: FunctionCallInfo) -> Datum {
    let subrel: Relation;
    let rel: Relation;
    let subid: Oid;
    let subname: *mut c_char;
    let relid: Oid;
    let relstate: c_char;
    let sublsn: XLogRecPtr;

    CHECK_IS_BINARY_UPGRADE!();

    /* We must check these things before dereferencing the arguments */
    if PG_ARGISNULL!(fcinfo, 0) || PG_ARGISNULL!(fcinfo, 1) || PG_ARGISNULL!(fcinfo, 2) {
        elog!(
            ERROR,
            "null argument to binary_upgrade_add_sub_rel_state is not allowed"
        );
    }

    subname = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    relid = PG_GETARG_OID!(fcinfo, 1);
    relstate = PG_GETARG_CHAR!(fcinfo, 2);
    sublsn = if PG_ARGISNULL!(fcinfo, 3) {
        InvalidXLogRecPtr
    } else {
        DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 3))
    };

    subrel = table_open(SubscriptionRelationId, RowExclusiveLock);
    subid = get_subscription_oid(subname, false);
    rel = relation_open(relid, AccessShareLock);

    /*
     * Since there are no concurrent ALTER/DROP SUBSCRIPTION commands during
     * the upgrade process, and the apply worker (which builds cache based on
     * the subscription catalog) is not running, the locks can be released
     * immediately.
     */
    AddSubscriptionRelState(subid, relid, relstate, sublsn, false);
    relation_close(rel, AccessShareLock);
    table_close(subrel, RowExclusiveLock);

    PG_RETURN_VOID!()
}

/*
 * binary_upgrade_replorigin_advance
 *
 * Update the remote_lsn for the subscriber's replication origin.
 */
#[no_mangle]
pub unsafe fn binary_upgrade_replorigin_advance(fcinfo: FunctionCallInfo) -> Datum {
    let rel: Relation;
    let subid: Oid;
    let subname: *mut c_char;
    let mut originname: [c_char; NAMEDATALEN];
    let node: RepOriginId;
    let remote_commit: XLogRecPtr;

    CHECK_IS_BINARY_UPGRADE!();

    /*
     * We must ensure a non-NULL subscription name before dereferencing the
     * arguments.
     */
    if PG_ARGISNULL!(fcinfo, 0) {
        elog!(
            ERROR,
            "null argument to binary_upgrade_replorigin_advance is not allowed"
        );
    }

    subname = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    remote_commit = if PG_ARGISNULL!(fcinfo, 1) {
        InvalidXLogRecPtr
    } else {
        DatumGetLSN(PG_GETARG_DATUM!(fcinfo, 1))
    };

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    subid = get_subscription_oid(subname, false);

    originname = [0; NAMEDATALEN];
    ReplicationOriginNameForLogicalRep(
        subid,
        InvalidOid,
        originname.as_mut_ptr(),
        core::mem::size_of::<[c_char; NAMEDATALEN]>() as Size,
    );

    /* Lock to prevent the replication origin from vanishing */
    LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);
    node = replorigin_by_name(originname.as_mut_ptr(), false);

    /*
     * The server will be stopped after setting up the objects in the new
     * cluster and the origins will be flushed during the shutdown checkpoint.
     * This will ensure that the latest LSN values for origin will be
     * available after the upgrade.
     */
    replorigin_advance(
        node,
        remote_commit,
        InvalidXLogRecPtr,
        false, /* backward */
        false, /* WAL log */
    );

    UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);
    table_close(rel, RowExclusiveLock);

    PG_RETURN_VOID!()
}

// ---------------------------------------------------------------------------
// Replication-slot globals/helpers used by
// binary_upgrade_logical_slot_has_caught_up (not yet ported).
// ---------------------------------------------------------------------------

// replication/slot.h: ReplicationSlot is an opaque struct here.
#[repr(C)]
pub struct ReplicationSlotData {
    pub invalidated: c_int,
}

#[repr(C)]
pub struct ReplicationSlot {
    pub data: ReplicationSlotData,
}

// replication/slot.h: MyReplicationSlot global (not yet ported).
static mut MyReplicationSlot: *mut ReplicationSlot = null_mut();

// replication/slot.h: RS_INVAL_NONE (not yet ported).
const RS_INVAL_NONE: c_int = 0;

// replication/slot.h: SlotIsLogical (not yet ported).
unsafe fn SlotIsLogical(_slot: *mut ReplicationSlot) -> bool {
    unimplemented!() // TODO: SlotIsLogical not yet ported
}

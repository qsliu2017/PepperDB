//! Translation of postgres/src/include/catalog/pg_db_role_setting.h
//!
//! The `FormData_pg_db_role_setting` struct: the fixed-layout part of a
//! pg_db_role_setting catalog row (per-database/per-user GUC settings).  As in
//! the C header, the struct as compiled into the backend stops at the field
//! just before `#ifdef CATALOG_VARLEN`; the trailing variable-length field
//! (setconfig[1], a text[] of GUC settings, guarded by CATALOG_VARLEN) is NOT
//! part of this in-memory struct - it lives only in a real on-disk tuple and is
//! reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

// ---------------------------------------------------------------------------
// Translation of postgres/src/backend/catalog/pg_db_role_setting.c
// Routines to support manipulation of the pg_db_role_setting relation.
// ---------------------------------------------------------------------------

use core::ffi::{c_char, c_int, c_void};
use core::ptr;

use crate::postgres::Datum;
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::common::heaptuple::{heap_form_tuple, heap_modify_tuple};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::{heap_getattr, HeapTuple};
use crate::access::heap::heapam::heap_getnext;
use crate::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::access::relscan::TableScanDesc;
use crate::access::sdir::ForwardScanDirection;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};
use crate::access::table::tableam::table_beginscan_catalog;
use crate::catalog::catalog_oids::DbRoleSettingRelationId;
use crate::catalog::indexing::{CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate};
use crate::storage::lockdefs::{NoLock, RowExclusiveLock};
use crate::utils::rel::Relation;
use crate::utils::misc::guc::{
    GUCArrayAdd, GUCArrayDelete, GUCArrayReset, GucAction, GucContext, GucSource, ProcessGUCArray,
};
use crate::utils::misc::guc_funcs::{ExtractSetVariableArgs, VariableSetStmt, VAR_RESET_ALL};

// utils/snapshot.h - Snapshot.
// TODO(pg-port): real Snapshot lives in utils/snapshot.h.
pub type Snapshot = *mut c_void;
// catalog/pg_db_role_setting.h - the datid/rolid index OID (real const lives in
// catalog/catalog.rs but is private there).
const DbRoleSettingDatidRolidIndexId: Oid = 2965;
// utils/fmgroids.h - regproc OID of oideq().
const F_OIDEQ: Oid = 184;

// catalog/pg_db_role_setting.h column numbers and attribute count.
const Anum_pg_db_role_setting_setdatabase: c_int = 1;
const Anum_pg_db_role_setting_setrole: c_int = 2;
const Anum_pg_db_role_setting_setconfig: c_int = 3;
const Natts_pg_db_role_setting: usize = 3;

// access/htup.h - HeapTupleIsValid.
#[inline]
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

// postgres_ext.h / postgres.h - OidIsValid.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

// postgres.h Datum helpers.
#[inline]
fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}
#[inline]
fn PointerGetDatum(p: *mut c_void) -> Datum {
    p as Datum
}
// utils/array.h - detoast a Datum to an ArrayType pointer.
// TODO(pg-port): real DatumGetArrayTypeP (with detoast) lives in utils/array.h.
#[inline]
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut c_void {
    d as *mut c_void
}

// utils/rel.h - RelationGetDescr.
// TODO(pg-port): real RelationGetDescr lives in utils/rel.h ((*rel).rd_att).
unsafe fn RelationGetDescr(_rel: Relation) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real RelationGetDescr lives in utils/rel.h
}

// access/tableam.h - table_endscan.
// TODO(pg-port): real table_endscan lives in access/tableam.c.
unsafe fn table_endscan(scan: TableScanDesc) {
    let _ = scan;
    /* TODO(pg-port) */
}

// catalog/objectaccess.h - InvokeObjectPostAlterHookArg.
// TODO(pg-port): real macro lives in catalog/objectaccess.h.
unsafe fn InvokeObjectPostAlterHookArg(
    _classId: Oid,
    _objectId: Oid,
    _subId: c_int,
    _auxiliaryId: Oid,
    _is_internal: bool,
) {
    /* TODO(pg-port) */
}

pub unsafe fn AlterSetting(databaseid: Oid, roleid: Oid, setstmt: *mut VariableSetStmt) {
    let valuestr: *mut c_char;
    let tuple: HeapTuple;
    let rel: Relation;
    let mut scankey: [ScanKeyData; 2] = core::mem::zeroed();
    let scan;

    valuestr = ExtractSetVariableArgs(setstmt);

    /* Get the old tuple, if any. */

    rel = table_open(DbRoleSettingRelationId, RowExclusiveLock) as Relation;
    ScanKeyInit(
        &mut scankey[0] as ScanKey,
        Anum_pg_db_role_setting_setdatabase as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(databaseid),
    );
    ScanKeyInit(
        &mut scankey[1] as ScanKey,
        Anum_pg_db_role_setting_setrole as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(roleid),
    );
    scan = systable_beginscan(
        rel,
        DbRoleSettingDatidRolidIndexId,
        true,
        ptr::null_mut(),
        2,
        scankey.as_mut_ptr() as ScanKey,
    );
    tuple = systable_getnext(scan) as HeapTuple;

    /*
     * There are three cases:
     *
     * - in RESET ALL, request GUC to reset the settings array and update the
     * catalog if there's anything left, delete it otherwise
     *
     * - in other commands, if there's a tuple in pg_db_role_setting, update
     * it; if it ends up empty, delete it
     *
     * - otherwise, insert a new pg_db_role_setting tuple, but only if the
     * command is not RESET
     */
    if (*setstmt).kind == VAR_RESET_ALL {
        if HeapTupleIsValid(tuple) {
            let mut new: *mut c_void = ptr::null_mut();
            let datum: Datum;
            let mut isnull: bool = false;

            datum = heap_getattr(
                tuple,
                Anum_pg_db_role_setting_setconfig,
                RelationGetDescr(rel) as TupleDesc,
                &mut isnull,
            );

            if !isnull {
                new = GUCArrayReset(DatumGetArrayTypeP(datum));
            }

            if !new.is_null() {
                let mut repl_val: [Datum; Natts_pg_db_role_setting] =
                    [0 as Datum; Natts_pg_db_role_setting];
                let repl_null: [bool; Natts_pg_db_role_setting] =
                    [false; Natts_pg_db_role_setting];
                let mut repl_repl: [bool; Natts_pg_db_role_setting] =
                    [false; Natts_pg_db_role_setting];
                let newtuple: HeapTuple;

                repl_repl = [false; Natts_pg_db_role_setting];

                repl_val[(Anum_pg_db_role_setting_setconfig - 1) as usize] = PointerGetDatum(new);
                repl_repl[(Anum_pg_db_role_setting_setconfig - 1) as usize] = true;

                newtuple = heap_modify_tuple(
                    tuple,
                    RelationGetDescr(rel) as TupleDesc,
                    repl_val.as_ptr(),
                    repl_null.as_ptr(),
                    repl_repl.as_ptr(),
                );
                CatalogTupleUpdate(
                    rel,
                    &mut (*tuple).t_self,
                    newtuple,
                );
            } else {
                CatalogTupleDelete(
                    rel,
                    &mut (*tuple).t_self,
                );
            }
        }
    } else if HeapTupleIsValid(tuple) {
        let mut repl_val: [Datum; Natts_pg_db_role_setting] =
            [0 as Datum; Natts_pg_db_role_setting];
        let repl_null: [bool; Natts_pg_db_role_setting] = [false; Natts_pg_db_role_setting];
        let mut repl_repl: [bool; Natts_pg_db_role_setting] = [false; Natts_pg_db_role_setting];
        let newtuple: HeapTuple;
        let datum: Datum;
        let mut isnull: bool = false;
        let mut a: *mut c_void;

        repl_repl = [false; Natts_pg_db_role_setting];
        repl_repl[(Anum_pg_db_role_setting_setconfig - 1) as usize] = true;

        /* Extract old value of setconfig */
        datum = heap_getattr(
            tuple,
            Anum_pg_db_role_setting_setconfig,
            RelationGetDescr(rel) as TupleDesc,
            &mut isnull,
        );
        a = if isnull {
            ptr::null_mut()
        } else {
            DatumGetArrayTypeP(datum)
        };

        /* Update (valuestr is NULL in RESET cases) */
        if !valuestr.is_null() {
            a = GUCArrayAdd(a, (*setstmt).name, valuestr);
        } else {
            a = GUCArrayDelete(a, (*setstmt).name);
        }

        if !a.is_null() {
            repl_val[(Anum_pg_db_role_setting_setconfig - 1) as usize] = PointerGetDatum(a);

            newtuple = heap_modify_tuple(
                tuple,
                RelationGetDescr(rel) as TupleDesc,
                repl_val.as_ptr(),
                repl_null.as_ptr(),
                repl_repl.as_ptr(),
            );
            CatalogTupleUpdate(
                rel,
                &mut (*tuple).t_self,
                newtuple,
            );
        } else {
            CatalogTupleDelete(
                rel,
                &mut (*tuple).t_self,
            );
        }
    } else if !valuestr.is_null() {
        /* non-null valuestr means it's not RESET, so insert a new tuple */
        let newtuple: HeapTuple;
        let mut values: [Datum; Natts_pg_db_role_setting] =
            [0 as Datum; Natts_pg_db_role_setting];
        let nulls: [bool; Natts_pg_db_role_setting] = [false; Natts_pg_db_role_setting];
        let a: *mut c_void;

        a = GUCArrayAdd(ptr::null_mut(), (*setstmt).name, valuestr);

        values[(Anum_pg_db_role_setting_setdatabase - 1) as usize] = ObjectIdGetDatum(databaseid);
        values[(Anum_pg_db_role_setting_setrole - 1) as usize] = ObjectIdGetDatum(roleid);
        values[(Anum_pg_db_role_setting_setconfig - 1) as usize] = PointerGetDatum(a);
        newtuple = heap_form_tuple(
            RelationGetDescr(rel) as TupleDesc,
            values.as_ptr(),
            nulls.as_ptr(),
        );

        CatalogTupleInsert(rel, newtuple);
    }

    InvokeObjectPostAlterHookArg(DbRoleSettingRelationId, databaseid, 0, roleid, false);

    systable_endscan(scan);

    /* Close pg_db_role_setting, but keep lock till commit */
    table_close(rel, NoLock);
}

/*
 * Drop some settings from the catalog.  These can be for a particular
 * database, or for a particular role.  (It is of course possible to do both
 * too, but it doesn't make sense for current uses.)
 */
pub unsafe fn DropSetting(databaseid: Oid, roleid: Oid) {
    let relsetting: Relation;
    let scan: TableScanDesc;
    let mut keys: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;
    let mut numkeys: c_int = 0;

    relsetting = table_open(DbRoleSettingRelationId, RowExclusiveLock) as Relation;

    if OidIsValid(databaseid) {
        ScanKeyInit(
            &mut keys[numkeys as usize] as ScanKey,
            Anum_pg_db_role_setting_setdatabase as i16,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(databaseid),
        );
        numkeys += 1;
    }
    if OidIsValid(roleid) {
        ScanKeyInit(
            &mut keys[numkeys as usize] as ScanKey,
            Anum_pg_db_role_setting_setrole as i16,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(roleid),
        );
        numkeys += 1;
    }

    scan = table_beginscan_catalog(
        relsetting,
        numkeys,
        keys.as_mut_ptr(),
    );
    loop {
        tup = heap_getnext(scan, ForwardScanDirection);
        if !HeapTupleIsValid(tup) {
            break;
        }
        CatalogTupleDelete(
            relsetting,
            &mut (*tup).t_self,
        );
    }
    table_endscan(scan);

    table_close(relsetting, RowExclusiveLock);
}

/*
 * Scan pg_db_role_setting looking for applicable settings, and load them on
 * the current process.
 *
 * relsetting is pg_db_role_setting, already opened and locked.
 *
 * Note: we only consider setting for the exact databaseid/roleid combination.
 * This probably needs to be called more than once, with InvalidOid passed as
 * databaseid/roleid.
 */
pub unsafe fn ApplySetting(
    snapshot: Snapshot,
    databaseid: Oid,
    roleid: Oid,
    relsetting: Relation,
    source: GucSource,
) {
    let scan;
    let mut keys: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;

    ScanKeyInit(
        &mut keys[0] as ScanKey,
        Anum_pg_db_role_setting_setdatabase as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(databaseid),
    );
    ScanKeyInit(
        &mut keys[1] as ScanKey,
        Anum_pg_db_role_setting_setrole as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(roleid),
    );

    scan = systable_beginscan(
        relsetting,
        DbRoleSettingDatidRolidIndexId,
        true,
        snapshot,
        2,
        keys.as_mut_ptr() as ScanKey,
    );
    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }

        let mut isnull: bool = false;
        let datum: Datum;

        datum = heap_getattr(
            tup,
            Anum_pg_db_role_setting_setconfig,
            RelationGetDescr(relsetting) as TupleDesc,
            &mut isnull,
        );
        if !isnull {
            let a: *mut c_void = DatumGetArrayTypeP(datum);

            /*
             * We process all the options at SUSET level.  We assume that the
             * right to insert an option into pg_db_role_setting was checked
             * when it was inserted.
             */
            ProcessGUCArray(
                a,
                GucContext::PGC_SUSET,
                source,
                GucAction::GUC_ACTION_SET,
            );
        }
    }

    systable_endscan(scan);
}

/*
 * FormData_pg_db_role_setting - the fixed part of a pg_db_role_setting row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_db_role_setting {
    /* database, or 0 for a role-specific setting */
    pub setdatabase: Oid,
    /* role, or 0 for a database-specific setting */
    pub setrole: Oid,
}

/*
 * Form_pg_db_role_setting corresponds to a pointer to a tuple with the format
 * of the pg_db_role_setting relation.
 */
pub type Form_pg_db_role_setting = *mut FormData_pg_db_role_setting;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (The pg_db_role_setting header exposes no #define constants.)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // setrole sits right after the 4-byte setdatabase Oid (the first key field).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_db_role_setting, setrole),
            core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_db_role_setting>()
                >= core::mem::offset_of!(FormData_pg_db_role_setting, setrole)
                    + core::mem::size_of::<Oid>()
        );
    }
}

//! access/transam/rmgr.c - Resource managers definition.
//!
//! This file owns the master `RmgrTable[]` array of WAL resource-manager method
//! tables and the routines that start up / clean up / register them, plus the
//! `pg_get_wal_resource_managers` SRF.

use crate::prelude::*;

use std::ffi::{c_char, c_int};
use std::ptr;

use crate::access::transam::xlog_internal::{GetRmgr, RmgrData, RmgrIdExists};
use crate::access::transam::xlogreader::{RmgrIdIsBuiltin, RmgrIdIsCustom};
use crate::nodes::execnodes::{ReturnSetInfo, Tuplestorestate};
use crate::access::common::tupdesc::TupleDesc;
use crate::utils::builtins::CStringGetTextDatum;
use crate::utils::fmgr::FunctionCallInfo;
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::miscadmin::process_shared_preload_libraries_in_progress;
use crate::access::rmgrlist::RmgrId;

/*
 * rmgr.h: RM_MAX_ID == UINT8_MAX.  The table has one slot per possible RmgrId
 * (builtin + custom range) so custom rmgrs can be registered into the high ids.
 */
const RM_MAX_ID: usize = u8::MAX as usize;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported called functions.
// ---------------------------------------------------------------------------

// TODO: port InitMaterializedSRF (src/backend/utils/fmgr/funcapi.c)
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!()
}

// TODO: port tuplestore_putvalues (src/backend/utils/sort/tuplestore.c)
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// RmgrTable[]
// ---------------------------------------------------------------------------

/*
 * must be kept in sync with RmgrData definition in xlog_internal.h
 *
 * In C this table is built by including "access/rmgrlist.h" with the PG_RMGR
 * X-macro expanding each entry to
 *   { name, redo, desc, identify, startup, cleanup, mask, decode }
 *
 * The callback function pointers (xlog_redo, xlog_desc, ...) live in the
 * various per-rmgr modules and are NOT yet ported, so each builtin slot below
 * carries its real `rm_name` (so RmgrIdExists / GetRmgr behave correctly) with
 * all callbacks set to None.  The canonical callback identifier names are
 * recorded in crate::access::rmgrlist::RMGR_LIST and must be wired in here once
 * those modules are translated.  All non-builtin slots are zero (NULL name).
 */
// TODO: replace the None callbacks below with the real per-rmgr function
// pointers named in crate::access::rmgrlist::RMGR_LIST once those modules port.
pub static mut RmgrTable: [RmgrData; RM_MAX_ID + 1] = {
    // An all-NULL/None entry, used both as the array initializer and for the
    // unregistered (custom) slots.
    const EMPTY: RmgrData = RmgrData {
        rm_name: ptr::null(),
        rm_redo: None,
        rm_desc: None,
        rm_identify: None,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    };

    // Helper to build a builtin entry carrying just its name.
    macro_rules! named {
        ($n:expr) => {
            RmgrData {
                rm_name: concat!($n, "\0").as_ptr() as *const c_char,
                rm_redo: None,
                rm_desc: None,
                rm_identify: None,
                rm_startup: None,
                rm_cleanup: None,
                rm_mask: None,
                rm_decode: None,
            }
        };
    }

    let mut table = [EMPTY; RM_MAX_ID + 1];

    // Builtin entries, in rmgrlist.h order (index == RmgrId).
    table[0] = named!("XLOG");
    table[1] = named!("Transaction");
    table[2] = named!("Storage");
    table[3] = named!("CLOG");
    table[4] = named!("Database");
    table[5] = named!("Tablespace");
    table[6] = named!("MultiXact");
    table[7] = named!("RelMap");
    table[8] = named!("Standby");
    table[9] = named!("Heap2");
    table[10] = named!("Heap");
    table[11] = named!("Btree");
    table[12] = named!("Hash");
    table[13] = named!("Gin");
    table[14] = named!("Gist");
    table[15] = named!("Sequence");
    table[16] = named!("SPGist");
    table[17] = named!("BRIN");
    table[18] = named!("CommitTs");
    table[19] = named!("ReplicationOrigin");
    table[20] = named!("Generic");
    table[21] = named!("LogicalMessage");

    table
};

/*
 * Start up all resource managers.
 */
pub unsafe fn RmgrStartup() {
    for rmid in 0..=RM_MAX_ID {
        if !RmgrIdExists(rmid as RmgrId) {
            continue;
        }

        if let Some(rm_startup) = RmgrTable[rmid].rm_startup {
            rm_startup();
        }
    }
}

/*
 * Clean up all resource managers.
 */
pub unsafe fn RmgrCleanup() {
    for rmid in 0..=RM_MAX_ID {
        if !RmgrIdExists(rmid as RmgrId) {
            continue;
        }

        if let Some(rm_cleanup) = RmgrTable[rmid].rm_cleanup {
            rm_cleanup();
        }
    }
}

/*
 * Emit ERROR when we encounter a record with an RmgrId we don't
 * recognize.
 */
pub unsafe fn RmgrNotFound(rmid: RmgrId) {
    ereport!(
        ERROR,
        format!("resource manager with ID {} not registered", rmid)
    );
}

/*
 * Register a new custom WAL resource manager.
 *
 * Resource manager IDs must be globally unique across all extensions. Refer
 * to https://wiki.postgresql.org/wiki/CustomWALResourceManagers to reserve a
 * unique RmgrId for your extension, to avoid conflicts with other extension
 * developers. During development, use RM_EXPERIMENTAL_ID to avoid needlessly
 * reserving a new ID.
 */
pub unsafe fn RegisterCustomRmgr(rmid: RmgrId, rmgr: *const RmgrData) {
    if (*rmgr).rm_name.is_null() || strlen((*rmgr).rm_name) == 0 {
        ereport!(ERROR, "custom resource manager name is invalid");
    }

    if !RmgrIdIsCustom(rmid as c_int) {
        ereport!(
            ERROR,
            format!("custom resource manager ID {} is out of range", rmid)
        );
    }

    if !process_shared_preload_libraries_in_progress {
        ereport!(
            ERROR,
            format!(
                "failed to register custom resource manager \"{}\" with ID {}",
                cstr_to_string((*rmgr).rm_name),
                rmid
            )
        );
    }

    if !RmgrTable[rmid as usize].rm_name.is_null() {
        ereport!(
            ERROR,
            format!(
                "failed to register custom resource manager \"{}\" with ID {}",
                cstr_to_string((*rmgr).rm_name),
                rmid
            )
        );
    }

    /* check for existing rmgr with the same name */
    for existing_rmid in 0..=RM_MAX_ID {
        if !RmgrIdExists(existing_rmid as RmgrId) {
            continue;
        }

        if pg_strcasecmp(RmgrTable[existing_rmid].rm_name, (*rmgr).rm_name) == 0 {
            ereport!(
                ERROR,
                format!(
                    "failed to register custom resource manager \"{}\" with ID {}",
                    cstr_to_string((*rmgr).rm_name),
                    rmid
                )
            );
        }
    }

    /* register it */
    RmgrTable[rmid as usize] = *rmgr;
    ereport!(
        LOG,
        format!(
            "registered custom resource manager \"{}\" with ID {}",
            cstr_to_string((*rmgr).rm_name),
            rmid
        )
    );
}

/* SQL SRF showing loaded resource managers */
pub unsafe fn pg_get_wal_resource_managers(fcinfo: FunctionCallInfo) -> Datum {
    const PG_GET_RESOURCE_MANAGERS_COLS: usize = 3;

    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    InitMaterializedSRF(fcinfo, 0);

    for rmid in 0..=RM_MAX_ID {
        if !RmgrIdExists(rmid as RmgrId) {
            continue;
        }

        let mut values: [Datum; PG_GET_RESOURCE_MANAGERS_COLS] =
            [0; PG_GET_RESOURCE_MANAGERS_COLS];
        let mut nulls: [bool; PG_GET_RESOURCE_MANAGERS_COLS] =
            [false; PG_GET_RESOURCE_MANAGERS_COLS];

        values[0] = Int32GetDatum(rmid as int32);
        values[1] = CStringGetTextDatum(GetRmgr(rmid as RmgrId).rm_name);
        values[2] = BoolGetDatum(RmgrIdIsBuiltin(rmid as c_int));
        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    0 as Datum
}

// ---------------------------------------------------------------------------
// Small local helpers (faithful inline equivalents of C library calls).
// ---------------------------------------------------------------------------

/// `strlen()` over a NUL-terminated C string.
#[inline]
unsafe fn strlen(s: *const c_char) -> usize {
    if s.is_null() {
        return 0;
    }
    std::ffi::CStr::from_ptr(s).to_bytes().len()
}

/// Lossy conversion of a C string to a Rust String, for ereport! message
/// formatting only (the C code interpolates the raw `%s` directly).
#[inline]
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

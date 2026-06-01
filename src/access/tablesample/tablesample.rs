//! access/tablesample/tablesample.c - support functions for the TABLESAMPLE feature.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   - "postgres.h"        -> crate::prelude::*
//!   - "access/tsmapi.h"   -> crate::access::tsmapi (TsmRoutine, REAL)
//!
//! NOTE: the C IsA(routine, TsmRoutine) check relies on the T_TsmRoutine NodeTag
//! set by makeNode(TsmRoutine) in the handler. That variant is not yet present in
//! crate::nodes::nodes::NodeTag, so the tag check is degraded to a NULL check.
//! See the TODO in GetTsmRoutine below.

use crate::prelude::*;

use crate::access::tsmapi::TsmRoutine;
use crate::utils::fmgr::OidFunctionCall1Coll;

/*
 * GetTsmRoutine --- get a TsmRoutine struct by invoking the handler.
 *
 * This is a convenience routine that's just meant to check for errors.
 */
pub unsafe fn GetTsmRoutine(tsmhandler: Oid) -> *mut TsmRoutine {
    // C: datum = OidFunctionCall1(tsmhandler, PointerGetDatum(NULL));
    let datum: Datum = OidFunctionCall1Coll(
        tsmhandler,
        crate::postgres_ext::InvalidOid,
        PointerGetDatum(null()),
    );
    let routine = DatumGetPointer(datum) as *mut TsmRoutine;

    // C: if (routine == NULL || !IsA(routine, TsmRoutine))
    //         elog(ERROR, "tablesample handler function %u did not return a
    //              TsmRoutine struct", tsmhandler);
    // TODO: T_TsmRoutine is not yet a NodeTag variant, so the IsA() tag check is
    // omitted; only the NULL check is performed here.
    if routine.is_null() {
        elog!(
            ERROR,
            "tablesample handler function {} did not return a TsmRoutine struct",
            tsmhandler
        );
    }

    routine
}

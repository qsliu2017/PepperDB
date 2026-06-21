//! utils/activity/wait_event_funcs.c - Functions for accessing wait event data.

use crate::prelude::*;

use std::ffi::{c_char, c_int};
use std::ptr;

use crate::nodes::execnodes::ReturnSetInfo;
use crate::utils::builtins::CStringGetTextDatum;
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::wait_classes::{PG_WAIT_EXTENSION, PG_WAIT_INJECTIONPOINT};
use crate::lib::stringinfo::{initStringInfo, StringInfoData};
use crate::appendStringInfo;

/*
 * Each wait event has one corresponding entry in this structure, fed to
 * the SQL function of this file.
 */
#[repr(C)]
struct WaitEventDataEntry {
    r#type: *const c_char,
    name: *const c_char,
    description: *const c_char,
}

// The C source #include's the generated file "utils/wait_event_funcs_data.c",
// which is produced at build time from src/backend/utils/activity/wait_event_names.txt
// by generate-wait_event_types.pl.  That generated table is not part of the port
// yet, so the static array currently holds only the terminating {NULL,NULL,NULL}
// sentinel.  The iteration logic below is faithful and will pick up entries once
// the generated data is wired in.
// TODO: include generated waitEventData entries from wait_event_names.txt.
// `const` not `static`: entries hold *const c_char (not Sync), so it can't be a
// shared static; a const slice of 'static-literal pointers is equivalent.
const waitEventData: &[WaitEventDataEntry] = &[
    /* end of list */
    WaitEventDataEntry {
        r#type: ptr::null(),
        name: ptr::null(),
        description: ptr::null(),
    },
];

// ---- Stubs for not-yet-ported called functions ----

// TODO: port InitMaterializedSRF (src/backend/utils/fmgr/funcapi.c)
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!()
}

// TODO: port GetWaitEventCustomNames (src/backend/utils/activity/wait_event.c)
unsafe fn GetWaitEventCustomNames(_classId: uint32, _nwaitevents: *mut c_int) -> *mut *mut c_char { crate::utils::activity::wait_event::GetWaitEventCustomNames(_classId, _nwaitevents) }

// TODO: port tuplestore_putvalues (src/backend/utils/sort/tuplestore.c)
// Note: an unrelated translation exists in executor/tstoreReceiver.rs (private);
// stub here to match the public C signature used by this file.
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!()
}

// Opaque types used through pointers only.
#[allow(non_camel_case_types)]
pub enum Tuplestorestate {}
#[allow(non_camel_case_types)]
pub type TupleDesc = *mut std::ffi::c_void;

/*
 * pg_get_wait_events
 *
 * List information about wait events (type, name and description).
 */
pub unsafe fn pg_get_wait_events(fcinfo: FunctionCallInfo) -> Datum {
    const PG_GET_WAIT_EVENTS_COLS: usize = 3;

    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let waiteventnames: *mut *mut c_char;
    let mut nbwaitevents: c_int = 0;

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    /* Iterate over the list of wait events */
    let mut idx = 0usize;
    while !waitEventData[idx].r#type.is_null() {
        let mut values: [Datum; PG_GET_WAIT_EVENTS_COLS] = [0; PG_GET_WAIT_EVENTS_COLS];
        let mut nulls: [bool; PG_GET_WAIT_EVENTS_COLS] = [false; PG_GET_WAIT_EVENTS_COLS];

        values[0] = CStringGetTextDatum(waitEventData[idx].r#type);
        values[1] = CStringGetTextDatum(waitEventData[idx].name);
        values[2] = CStringGetTextDatum(waitEventData[idx].description);

        tuplestore_putvalues(
            (*rsinfo).setResult as *mut Tuplestorestate,
            (*rsinfo).setDesc as TupleDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        idx += 1;
    }

    /* Handle custom wait events for extensions */
    let waiteventnames = GetWaitEventCustomNames(PG_WAIT_EXTENSION, &mut nbwaitevents);

    for idx in 0..nbwaitevents {
        let mut buf: StringInfoData = std::mem::zeroed();
        let mut values: [Datum; PG_GET_WAIT_EVENTS_COLS] = [0; PG_GET_WAIT_EVENTS_COLS];
        let mut nulls: [bool; PG_GET_WAIT_EVENTS_COLS] = [false; PG_GET_WAIT_EVENTS_COLS];

        values[0] = CStringGetTextDatum(b"Extension\0".as_ptr() as *const c_char);
        values[1] = CStringGetTextDatum(*waiteventnames.offset(idx as isize));

        initStringInfo(&mut buf);
        appendStringInfo!(
            &mut buf,
            "Waiting for custom wait event \"{}\" defined by extension module",
            cstr_to_str(*waiteventnames.offset(idx as isize))
        );

        values[2] = CStringGetTextDatum(buf.data);

        tuplestore_putvalues(
            (*rsinfo).setResult as *mut Tuplestorestate,
            (*rsinfo).setDesc as TupleDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    /* Likewise for injection points */
    let waiteventnames = GetWaitEventCustomNames(PG_WAIT_INJECTIONPOINT, &mut nbwaitevents);

    for idx in 0..nbwaitevents {
        let mut buf: StringInfoData = std::mem::zeroed();
        let mut values: [Datum; PG_GET_WAIT_EVENTS_COLS] = [0; PG_GET_WAIT_EVENTS_COLS];
        let mut nulls: [bool; PG_GET_WAIT_EVENTS_COLS] = [false; PG_GET_WAIT_EVENTS_COLS];

        values[0] = CStringGetTextDatum(b"InjectionPoint\0".as_ptr() as *const c_char);
        values[1] = CStringGetTextDatum(*waiteventnames.offset(idx as isize));

        initStringInfo(&mut buf);
        appendStringInfo!(
            &mut buf,
            "Waiting for injection point \"{}\"",
            cstr_to_str(*waiteventnames.offset(idx as isize))
        );

        values[2] = CStringGetTextDatum(buf.data);

        tuplestore_putvalues(
            (*rsinfo).setResult as *mut Tuplestorestate,
            (*rsinfo).setDesc as TupleDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    let _ = waiteventnames;

    0 as Datum
}

/// Helper: borrow a NUL-terminated C string as a &str for use in the
/// Rust `appendStringInfo!` format (the C code passes the raw char* to "%s").
unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = std::slice::from_raw_parts(s as *const u8, n);
    std::str::from_utf8_unchecked(bytes)
}

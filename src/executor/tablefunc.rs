//! executor/tablefunc.h - interface for TableFunc executor node

use std::ffi::{c_char, c_int};

use crate::c::int32;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// Forward-declared in the C header to avoid including execnodes.h.
// Defined canonically in nodes/execnodes.rs once that lands.
// TODO: dedup when execnodes.h lands.
use crate::nodes::execnodes::TableFuncScanState;

/*
 * TableFuncRoutine holds function pointers used for generating content of
 * table-producer functions, such as XMLTABLE.
 */
#[repr(C)]
pub struct TableFuncRoutine {
    pub InitOpaque: Option<unsafe extern "C" fn(state: *mut TableFuncScanState, natts: c_int)>,
    pub SetDocument: Option<unsafe extern "C" fn(state: *mut TableFuncScanState, value: Datum)>,
    pub SetNamespace: Option<
        unsafe extern "C" fn(
            state: *mut TableFuncScanState,
            name: *const c_char,
            uri: *const c_char,
        ),
    >,
    pub SetRowFilter:
        Option<unsafe extern "C" fn(state: *mut TableFuncScanState, path: *const c_char)>,
    pub SetColumnFilter: Option<
        unsafe extern "C" fn(state: *mut TableFuncScanState, path: *const c_char, colnum: c_int),
    >,
    pub FetchRow: Option<unsafe extern "C" fn(state: *mut TableFuncScanState) -> bool>,
    pub GetValue: Option<
        unsafe extern "C" fn(
            state: *mut TableFuncScanState,
            colnum: c_int,
            typid: Oid,
            typmod: int32,
            isnull: *mut bool,
        ) -> Datum,
    >,
    pub DestroyOpaque: Option<unsafe extern "C" fn(state: *mut TableFuncScanState)>,
}

//! Miscellaneous SQL-callable functions. Translated from
//! src/backend/utils/adt/misc.c (the subset needed by the type regression
//! tests' tails).
//!
//! `pg_input_is_valid(text, text)` and `pg_input_error_info(text, text)` are the
//! non-error-throwing type-input probes: they run a datatype's input function
//! under an `ErrorSaveContext` (via `InputFunctionCallSafe`) so a bad value is
//! reported as `false` / a detail row instead of raising. `pg_input_is_valid`
//! returns bool; `pg_input_error_info` returns a `(message, detail, hint,
//! sql_error_code)` record and is therefore called as `FROM
//! pg_input_error_info(...)` -- the set/record-returning path.

use crate::backend::utils::adt::varlena::{cstring_to_text, text_to_cstring};
use crate::backend::utils::cache::lsyscache::get_type_input_info;
use crate::backend::utils::fmgr::fmgr::{empty_flinfo, fmgr_info, InputFunctionCallSafe};
use crate::c::text;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::parser::parse_type::parseTypeString;
use crate::postgres::{BoolGetDatum, Datum, PointerGetDatum};
use crate::utils::elog::unpack_sql_state;

/// Read text argument `n` as an owned `String` (C `text_to_cstring(PG_GETARG_TEXT_PP(n))`).
fn pg_getarg_text_str(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = fcinfo.args[n].value.0 as *const text;
    // SAFETY: the arg is a valid non-toasted text varlena kept alive for the call.
    text_to_cstring(unsafe { &*p })
}

/// PG `pg_input_is_valid`: is `txt` a valid input for the type named `typname`?
/// Runs the type's input function under a soft-error context and returns whether
/// it succeeded.
pub fn pg_input_is_valid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let txt = pg_getarg_text_str(fcinfo, 0);
    let typname = pg_getarg_text_str(fcinfo, 1);
    let mut escontext = ErrorSaveContext::new();
    BoolGetDatum(pg_input_is_valid_common(&txt, &typname, &mut escontext))
}

/// PG `pg_input_error_info`: the error info for a failed input of `txt` as
/// `typname`, as a `(message, detail, hint, sql_error_code)` row (all NULL when
/// the input is valid). Returns the composite Datum.
///
/// NOTE: this is a record-returning function invoked as `SELECT * FROM
/// pg_input_error_info(...)`; the function-in-FROM (RangeFunction) machinery and
/// `heap_form_tuple`/`get_call_result_type` composite plumbing it needs are not
/// wired yet (owned by the SRF/funcapi step), so reaching this function raises
/// through that path rather than here. The soft-error extraction below is the
/// faithful body for when that lands.
pub fn pg_input_error_info(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let txt = pg_getarg_text_str(fcinfo, 0);
    let typname = pg_getarg_text_str(fcinfo, 1);

    let mut escontext = ErrorSaveContext::new();
    escontext.details_wanted = true;

    let (message, detail, hint, sqlstate) = if pg_input_is_valid_common(&txt, &typname, &mut escontext)
    {
        (None, None, None, None)
    } else {
        let Some(edata) = escontext.error_data.as_deref() else {
            crate::elog!(crate::utils::elog::ERROR, "soft error without details".to_string());
            unreachable!("elog(ERROR) diverges");
        };
        (
            edata.message.clone(),
            edata.detail.clone(),
            edata.hint.clone(),
            Some(unpack_sql_state(edata.sqlerrcode)),
        )
    };

    // heap_form_tuple over the 4-column result tupdesc (needs get_call_result_type
    // + the composite tuple builder from the funcapi/heaptuple path).
    let _ = (message, detail, hint, sqlstate, cstring_to_text);
    unimplemented!(
        "pg_input_error_info needs get_call_result_type + heap_form_tuple (record-returning fn / funcapi)"
    )
}

/// PG `pg_input_is_valid_common`: shared body of the two probes. Parses the type
/// name, looks up its input function, and runs it under `escontext` via
/// `InputFunctionCallSafe`; returns whether the conversion succeeded.
fn pg_input_is_valid_common(txt: &str, typname: &str, escontext: &mut ErrorSaveContext) -> bool {
    // Parse type-name argument to obtain type OID and encoded typmod. A bad TYPE
    // NAME is a hard error (parseTypeString raises), distinct from a bad VALUE.
    let Some((typoid, typmod)) = parseTypeString(typname, None) else {
        crate::elog!(crate::utils::elog::ERROR, format!("type \"{typname}\" does not exist"));
        unreachable!("elog(ERROR) diverges");
    };

    let (typiofunc, typioparam) = get_type_input_info(typoid);
    let mut inputproc = empty_flinfo();
    fmgr_info(typiofunc, &mut inputproc);

    // Perform the conversion under the soft-error context.
    InputFunctionCallSafe(&mut inputproc, txt, typioparam, typmod, Some(escontext)).is_some()
}

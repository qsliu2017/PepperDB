//! fe_utils/psqlscan.h - lexical scanner for SQL commands

use std::ffi::{c_char, c_int, c_void};

// PQExpBuffer is a libpq/pqexpbuffer frontend type; no real definition exists
// in src/ yet, so stub locally.
// TODO: dedup
pub type PQExpBuffer = *mut c_void;

/* Abstract type for lexer's internal state */
// typedef struct PsqlScanStateData *PsqlScanState;
pub type PsqlScanStateData = c_void;
pub type PsqlScanState = *mut PsqlScanStateData;

/* Termination states for psql_scan() */
pub type PsqlScanResult = c_int;
pub const PSCAN_SEMICOLON: PsqlScanResult = 0; /* found command-ending semicolon */
pub const PSCAN_BACKSLASH: PsqlScanResult = 1; /* found backslash command */
pub const PSCAN_INCOMPLETE: PsqlScanResult = 2; /* end of line, SQL statement incomplete */
pub const PSCAN_EOL: PsqlScanResult = 3; /* end of line, SQL possibly complete */

/* Prompt type returned by psql_scan() */
pub type promptStatus_t = c_int;
pub const PROMPT_READY: promptStatus_t = 0;
pub const PROMPT_CONTINUE: promptStatus_t = 1;
pub const PROMPT_COMMENT: promptStatus_t = 2;
pub const PROMPT_SINGLEQUOTE: promptStatus_t = 3;
pub const PROMPT_DOUBLEQUOTE: promptStatus_t = 4;
pub const PROMPT_DOLLARQUOTE: promptStatus_t = 5;
pub const PROMPT_PAREN: promptStatus_t = 6;
pub const PROMPT_COPY: promptStatus_t = 7;

/* Quoting request types for get_variable() callback */
pub type PsqlScanQuoteType = c_int;
pub const PQUOTE_PLAIN: PsqlScanQuoteType = 0; /* just return the actual value */
pub const PQUOTE_SQL_LITERAL: PsqlScanQuoteType = 1; /* add quotes to make a valid SQL literal */
pub const PQUOTE_SQL_IDENT: PsqlScanQuoteType = 2; /* quote if needed to make a SQL identifier */
pub const PQUOTE_SHELL_ARG: PsqlScanQuoteType = 3; /* quote if needed to be safe in a shell cmd */

/* Callback functions to be used by the lexer */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PsqlScanCallbacks {
    /* Fetch value of a variable, as a free'able string; NULL if unknown */
    /* This pointer can be NULL if no variable substitution is wanted */
    pub get_variable: Option<
        unsafe extern "C" fn(
            varname: *const c_char,
            quote: PsqlScanQuoteType,
            passthrough: *mut c_void,
        ) -> *mut c_char,
    >,
}

pub unsafe fn psql_scan_create(_callbacks: *const PsqlScanCallbacks) -> PsqlScanState {
    unimplemented!()
}

pub unsafe fn psql_scan_destroy(_state: PsqlScanState) {
    unimplemented!()
}

pub unsafe fn psql_scan_set_passthrough(_state: PsqlScanState, _passthrough: *mut c_void) {
    unimplemented!()
}

pub unsafe fn psql_scan_setup(
    _state: PsqlScanState,
    _line: *const c_char,
    _line_len: c_int,
    _encoding: c_int,
    _std_strings: bool,
) {
    unimplemented!()
}

pub unsafe fn psql_scan_finish(_state: PsqlScanState) {
    unimplemented!()
}

pub unsafe fn psql_scan(
    _state: PsqlScanState,
    _query_buf: PQExpBuffer,
    _prompt: *mut promptStatus_t,
) -> PsqlScanResult {
    unimplemented!()
}

pub unsafe fn psql_scan_reset(_state: PsqlScanState) {
    unimplemented!()
}

pub unsafe fn psql_scan_reselect_sql_lexer(_state: PsqlScanState) {
    unimplemented!()
}

pub unsafe fn psql_scan_in_quote(_state: PsqlScanState) -> bool {
    unimplemented!()
}

pub unsafe fn psql_scan_get_location(
    _state: PsqlScanState,
    _lineno: *mut c_int,
    _offset: *mut c_int,
) {
    unimplemented!()
}

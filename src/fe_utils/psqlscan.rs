//! Translated from PostgreSQL src/include/fe_utils/psqlscan.h
//
// Lexical scanner for SQL commands (psql heritage).

pub use crate::fe_utils::string_utils::PQExpBuffer;

/// Termination states for psql_scan().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PsqlScanResult {
    Semicolon,   // found command-ending semicolon
    Backslash,   // found backslash command
    Incomplete,  // end of line, SQL statement incomplete
    Eol,         // end of line, SQL possibly complete
}

/// Prompt type returned by psql_scan().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptStatus {
    Ready,
    Continue,
    Comment,
    SingleQuote,
    DoubleQuote,
    DollarQuote,
    Paren,
    Copy,
}

/// Quoting request types for the get_variable() callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PsqlScanQuoteType {
    Plain,       // just return the actual value
    SqlLiteral,  // add quotes to make a valid SQL literal
    SqlIdent,    // quote if needed to make a SQL identifier
    ShellArg,    // quote if needed to be safe in a shell cmd
}

/// Callbacks used by the lexer. The single `get_variable` hook is a
/// runtime-NULL-checkable fn pointer (C's `void *passthrough` is dropped --
/// function-mapping 6.3); the outer `None` skips substitution entirely.
pub struct PsqlScanCallbacks {
    /// Fetch a variable value; `None` result = unknown variable.
    pub get_variable: Option<fn(&str, PsqlScanQuoteType) -> Option<String>>,
}

/// Opaque lexer state (C: `struct PsqlScanStateData *`).
pub struct PsqlScanState {
    _private: (),
}

pub fn psql_scan_create(_callbacks: PsqlScanCallbacks) -> PsqlScanState {
    unimplemented!()
}

pub fn psql_scan_destroy(_state: PsqlScanState) {
    unimplemented!()
}

pub fn psql_scan_setup(
    _state: &mut PsqlScanState,
    _line: &str,
    _encoding: i32,
    _std_strings: bool,
) {
    unimplemented!()
}

pub fn psql_scan_finish(_state: &mut PsqlScanState) {
    unimplemented!()
}

/// C: `PsqlScanResult psql_scan(state, query_buf, promptStatus_t *prompt)`.
/// Returns the result paired with the prompt status (out-param folded in).
pub fn psql_scan(
    _state: &mut PsqlScanState,
    _query_buf: &mut PQExpBuffer,
) -> (PsqlScanResult, PromptStatus) {
    unimplemented!()
}

pub fn psql_scan_reset(_state: &mut PsqlScanState) {
    unimplemented!()
}

pub fn psql_scan_reselect_sql_lexer(_state: &mut PsqlScanState) {
    unimplemented!()
}

pub fn psql_scan_in_quote(_state: &PsqlScanState) -> bool {
    unimplemented!()
}

/// C uses two int out-params; returns `(lineno, offset)`.
pub fn psql_scan_get_location(_state: &PsqlScanState) -> (i32, i32) {
    unimplemented!()
}

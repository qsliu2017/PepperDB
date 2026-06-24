//! Translated from PostgreSQL src/include/fe_utils/psqlscan_int.h
//
// Internal lexer state shared by psqlscan.l and compatible lexers. The flex
// re-entrant scanner (`yyscan_t`, `YY_BUFFER_STATE`) is opaque C; a Rust lexer
// will not use flex, so these stay as opaque handles. In-memory.

use crate::fe_utils::psqlscan::{PQExpBuffer, PsqlScanCallbacks, PsqlScanQuoteType, PsqlScanState};

/// Opaque flex buffer handle. (C: `struct yy_buffer_state *`.)
pub struct YyBufferState;
/// Opaque flex scanner handle. (C: `void *yyscan_t`.)
pub type YyScanT = *mut core::ffi::c_void; // TODO(ptr): flex opaque scanner

/// A stacked flex buffer used for psql variable substitution. (C: StackElem,
/// an intrusive singly-linked list -> the stack is a `Vec<StackElem>` owner.)
pub struct StackElem {
    pub buf: Option<Box<YyBufferState>>,
    pub bufstring: String,           // data actually scanned by flex
    pub origstring: Option<String>,  // copy of original data, if needed
    pub varname: Option<String>,     // variable providing data, or None
}

/// All working state of the lexer between calls. (C: PsqlScanStateData.)
pub struct PsqlScanStateData {
    pub scanner: YyScanT,
    pub output_buf: PQExpBuffer,
    pub buffer_stack: Vec<StackElem>, // was an intrusive list of StackElem
    pub scanbufhandle: Option<Box<YyBufferState>>,
    pub scanbuf: String,
    pub scanline: Option<String>,
    pub encoding: i32,
    pub safe_encoding: bool,
    pub std_strings: bool,
    pub curline: Option<String>,
    pub refline: Option<String>,
    pub cur_line_no: i32,
    pub cur_line_ptr: Option<String>,
    pub start_state: i32,
    pub state_before_str_stop: i32,
    pub paren_depth: i32,
    pub xcdepth: i32,
    pub dolqstart: Option<String>,
    pub identifier_count: i32,
    pub identifiers: [u8; 4],
    pub begin_depth: i32,
    pub callbacks: PsqlScanCallbacks,
    // C `void *cb_passthrough` -> closure-captured context (see callbacks).
}

// Functions exported by psqlscan.l for use within compatible lexers.
pub fn psqlscan_push_new_buffer(state: &mut PsqlScanState, newstr: &str, varname: Option<&str>) {
    unimplemented!()
}
pub fn psqlscan_pop_buffer_stack(state: &mut PsqlScanState) {
    unimplemented!()
}
pub fn psqlscan_select_top_buffer(state: &mut PsqlScanState) {
    unimplemented!()
}
pub fn psqlscan_var_is_current_source(state: &PsqlScanState, varname: &str) -> bool {
    unimplemented!()
}
pub fn psqlscan_prepare_buffer(state: &mut PsqlScanState, txt: &str) -> (Box<YyBufferState>, String) {
    // C out-param `char **txtcopy` -> returned alongside the buffer handle.
    unimplemented!()
}
pub fn psqlscan_emit(state: &mut PsqlScanState, txt: &str) {
    unimplemented!()
}
pub fn psqlscan_extract_substring(state: &mut PsqlScanState, txt: &str) -> String {
    unimplemented!()
}
pub fn psqlscan_escape_variable(state: &mut PsqlScanState, txt: &str, quote: PsqlScanQuoteType) {
    unimplemented!()
}
pub fn psqlscan_test_variable(state: &mut PsqlScanState, txt: &str) {
    unimplemented!()
}

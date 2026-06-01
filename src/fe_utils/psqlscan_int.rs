//! fe_utils/psqlscan_int.h - lexical scanner internal declarations
//!
//! Declares the PsqlScanStateData structure used by psqlscan.l and shared by
//! other lexers compatible with it, such as psqlscanslash.l.

use std::ffi::{c_char, c_int, c_void};

use crate::fe_utils::psqlscan::{PsqlScanCallbacks, PsqlScanQuoteType, PsqlScanState};

// PQExpBuffer: defined variously across the tree (psqlscan.rs / string_utils.rs /
// recovery_gen.rs). Use a local stub here to avoid import ambiguity.
// TODO: dedup with the canonical PQExpBuffer definition.
pub type PQExpBuffer = *mut c_void;

// Flex re-entrant scanner / buffer-state typedefs. In the C build these come
// from the generated flex header; standalone they are stubbed as below.
// (YY_TYPEDEF_YY_BUFFER_STATE / YY_TYPEDEF_YY_SCANNER_T guards)
// typedef struct yy_buffer_state *YY_BUFFER_STATE;
pub type YY_BUFFER_STATE = *mut c_void;
// typedef void *yyscan_t;
pub type yyscan_t = *mut c_void;

/*
 * We use a stack of flex buffers to handle substitution of psql variables.
 * Each stacked buffer contains the as-yet-unread text from one psql variable.
 * When we pop the stack all the way, we resume reading from the outer buffer
 * identified by scanbufhandle.
 */
#[repr(C)]
pub struct StackElem {
    /// flex input control structure
    pub buf: YY_BUFFER_STATE,
    /// data actually being scanned by flex
    pub bufstring: *mut c_char,
    /// copy of original data, if needed
    pub origstring: *mut c_char,
    /// name of variable providing data, or NULL
    pub varname: *mut c_char,
    pub next: *mut StackElem,
}

/*
 * All working state of the lexer must be stored in PsqlScanStateData
 * between calls.  This allows us to have multiple open lexer operations,
 * which is needed for nested include files.  The lexer itself is not
 * recursive, but it must be re-entrant.
 */
#[repr(C)]
pub struct PsqlScanStateData {
    /// Flex's state for this PsqlScanState
    pub scanner: yyscan_t,

    /// current output buffer
    pub output_buf: PQExpBuffer,

    /// stack of variable expansion buffers
    pub buffer_stack: *mut StackElem,

    /*
     * These variables always refer to the outer buffer, never to any stacked
     * variable-expansion buffer.
     */
    pub scanbufhandle: YY_BUFFER_STATE,
    /// start of outer-level input buffer
    pub scanbuf: *mut c_char,
    /// current input line at outer level
    pub scanline: *const c_char,

    /* safe_encoding, curline, refline are used by emit() to replace FFs */
    /// encoding being used now
    pub encoding: c_int,
    /// is current encoding "safe"?
    pub safe_encoding: bool,
    /// are string literals standard?
    pub std_strings: bool,
    /// actual flex input string for cur buf
    pub curline: *const c_char,
    /// original data for cur buffer
    pub refline: *const c_char,

    /* status for psql_scan_get_location() */
    /// current line#, or 0 if no yylex done
    pub cur_line_no: c_int,
    /// points into cur_line_no'th line in scanbuf
    pub cur_line_ptr: *const c_char,

    /*
     * All this state lives across successive input lines, until explicitly
     * reset by psql_scan_reset.  start_state is adopted by yylex() on entry,
     * and updated with its finishing state on exit.
     */
    /// yylex's starting/finishing state
    pub start_state: c_int,
    /// start cond. before end quote
    pub state_before_str_stop: c_int,
    /// depth of nesting in parentheses
    pub paren_depth: c_int,
    /// depth of nesting in slash-star comments
    pub xcdepth: c_int,
    /// current $foo$ quote start string
    pub dolqstart: *mut c_char,

    /*
     * State to track boundaries of BEGIN ... END blocks in function
     * definitions, so that semicolons do not send query too early.
     */
    /// identifiers since start of statement
    pub identifier_count: c_int,
    /// records the first few identifiers
    pub identifiers: [c_char; 4],
    /// depth of begin/end pairs
    pub begin_depth: c_int,

    /*
     * Callback functions provided by the program making use of the lexer,
     * plus a void* callback passthrough argument.
     */
    pub callbacks: *const PsqlScanCallbacks,
    pub cb_passthrough: *mut c_void,
}

/*
 * Functions exported by psqlscan.l, but only meant for use within
 * compatible lexers.
 */
pub unsafe fn psqlscan_push_new_buffer(
    state: PsqlScanState,
    newstr: *const c_char,
    varname: *const c_char,
) {
    let _ = (state, newstr, varname);
    unimplemented!()
}

pub unsafe fn psqlscan_pop_buffer_stack(state: PsqlScanState) {
    let _ = state;
    unimplemented!()
}

pub unsafe fn psqlscan_select_top_buffer(state: PsqlScanState) {
    let _ = state;
    unimplemented!()
}

pub unsafe fn psqlscan_var_is_current_source(
    state: PsqlScanState,
    varname: *const c_char,
) -> bool {
    let _ = (state, varname);
    unimplemented!()
}

pub unsafe fn psqlscan_prepare_buffer(
    state: PsqlScanState,
    txt: *const c_char,
    len: c_int,
    txtcopy: *mut *mut c_char,
) -> YY_BUFFER_STATE {
    let _ = (state, txt, len, txtcopy);
    unimplemented!()
}

pub unsafe fn psqlscan_emit(state: PsqlScanState, txt: *const c_char, len: c_int) {
    let _ = (state, txt, len);
    unimplemented!()
}

pub unsafe fn psqlscan_extract_substring(
    state: PsqlScanState,
    txt: *const c_char,
    len: c_int,
) -> *mut c_char {
    let _ = (state, txt, len);
    unimplemented!()
}

pub unsafe fn psqlscan_escape_variable(
    state: PsqlScanState,
    txt: *const c_char,
    len: c_int,
    quote: PsqlScanQuoteType,
) {
    let _ = (state, txt, len, quote);
    unimplemented!()
}

pub unsafe fn psqlscan_test_variable(state: PsqlScanState, txt: *const c_char, len: c_int) {
    let _ = (state, txt, len);
    unimplemented!()
}

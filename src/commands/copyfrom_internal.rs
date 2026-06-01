//! commands/copyfrom_internal.h - Internal definitions for COPY FROM command.

use std::ffi::{c_char, c_int, c_void};

use crate::access::attnum::AttrNumber;
use crate::c::uint64;
use crate::commands::copyapi::{CopyFromRoutine, CopyFromState};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::nodes::execnodes::{ExprContext, ExprState, TransitionCaptureState};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::List;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::rel::Relation;

// ---------------------------------------------------------------------------
// Stubs for symbols defined in copy.h, which has not been ported yet.
// TODO: dedup when commands/copy.h lands.
// ---------------------------------------------------------------------------

/// `typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread)`
pub type copy_data_source_cb =
    Option<unsafe extern "C" fn(outbuf: *mut c_void, minread: c_int, maxread: c_int) -> c_int>;

/// `typedef struct CopyFormatOptions { ... } CopyFormatOptions` (from copy.h).
/// Re-exported from commands/copy.rs, which is the canonical definition.
/// on_error (CopyOnErrorChoice) -- copy.h
/// reject_limit (int64) -- copy.h
/// force_notnull_flags (*mut bool) -- copy.h
/// force_null_flags (*mut bool) -- copy.h
pub use crate::commands::copy::CopyFormatOptions;

// `FILE` from <stdio.h>.
pub type FILE = c_void;

// ---------------------------------------------------------------------------
// CopySource: source cases at the bottom level.
// C enum -> pub type + pub const (project convention).
// ---------------------------------------------------------------------------
pub type CopySource = c_int;
pub const COPY_FILE: CopySource = 0; // from file (or a piped program)
pub const COPY_FRONTEND: CopySource = 1; // from frontend
pub const COPY_CALLBACK: CopySource = 2; // from callback function

// ---------------------------------------------------------------------------
// EolType: end-of-line terminator type of the input.
// ---------------------------------------------------------------------------
pub type EolType = c_int;
pub const EOL_UNKNOWN: EolType = 0;
pub const EOL_NL: EolType = 1;
pub const EOL_CR: EolType = 2;
pub const EOL_CRNL: EolType = 3;

// ---------------------------------------------------------------------------
// CopyInsertMethod: insert method to be used during COPY FROM.
// ---------------------------------------------------------------------------
pub type CopyInsertMethod = c_int;
pub const CIM_SINGLE: CopyInsertMethod = 0; // use table_tuple_insert or ExecForeignInsert
pub const CIM_MULTI: CopyInsertMethod = 1; // always use table_multi_insert or ExecForeignBatchInsert
pub const CIM_MULTI_CONDITIONAL: CopyInsertMethod = 2; // use table_multi_insert/ExecForeignBatchInsert only if valid

// ---------------------------------------------------------------------------
// Buffer size #defines.
// ---------------------------------------------------------------------------
pub const INPUT_BUF_SIZE: c_int = 65536; // we palloc INPUT_BUF_SIZE+1 bytes
pub const RAW_BUF_SIZE: c_int = 65536; // we palloc RAW_BUF_SIZE+1 bytes

/// `#define INPUT_BUF_BYTES(cstate) ((cstate)->input_buf_len - (cstate)->input_buf_index)`
#[inline]
pub unsafe fn INPUT_BUF_BYTES(cstate: *const CopyFromStateData) -> c_int {
    (*cstate).input_buf_len - (*cstate).input_buf_index
}

/// `#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)`
#[inline]
pub unsafe fn RAW_BUF_BYTES(cstate: *const CopyFromStateData) -> c_int {
    (*cstate).raw_buf_len - (*cstate).raw_buf_index
}

// ---------------------------------------------------------------------------
// CopyFromStateData: all state for a COPY FROM operation.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct CopyFromStateData {
    /* format routine */
    pub routine: *const CopyFromRoutine,

    /* low-level state data */
    pub copy_src: CopySource,    // type of copy source
    pub copy_file: *mut FILE,    // used if copy_src == COPY_FILE
    pub fe_msgbuf: StringInfo,   // used if copy_src == COPY_FRONTEND

    pub eol_type: EolType,        // EOL type of input
    pub file_encoding: c_int,     // file or remote side's character encoding
    pub need_transcoding: bool,   // file encoding diff from server?
    pub conversion_proc: Oid,     // encoding conversion function

    /* parameters from the COPY command */
    pub rel: Relation,                       // relation to copy from
    pub attnumlist: *mut List,               // integer list of attnums to copy
    pub filename: *mut c_char,               // filename, or NULL for STDIN
    pub is_program: bool,                    // is 'filename' a program to popen?
    pub data_source_cb: copy_data_source_cb, // function for reading data

    pub opts: CopyFormatOptions,
    pub convert_select_flags: *mut bool, // per-column CSV/TEXT CS flags
    pub whereClause: *mut Node,          // WHERE condition (or NULL)

    /* these are just for error messages, see CopyFromErrorCallback */
    pub cur_relname: *const c_char, // table name for error messages
    pub cur_lineno: uint64,         // line number for error messages
    pub cur_attname: *const c_char, // current att for error messages
    pub cur_attval: *const c_char,  // current att value for error messages
    pub relname_only: bool,         // don't output line number, att, etc.

    /*
     * Working state
     */
    pub copycontext: MemoryContext, // per-copy execution context

    pub num_defaults: AttrNumber,      // count of att that are missing and have default value
    pub in_functions: *mut FmgrInfo,   // array of input functions for each attrs
    pub typioparams: *mut Oid,         // array of element types for in_functions
    pub escontext: *mut ErrorSaveContext, // soft error trapped during in_functions execution
    pub num_errors: uint64,            // total number of rows which contained soft errors
    pub defmap: *mut c_int,            // array of default att numbers related to missing att
    pub defexprs: *mut *mut ExprState, // array of default att expressions for all att
    pub defaults: *mut bool,           // if DEFAULT marker was found for corresponding att
    pub volatile_defexprs: bool,       // is any of defexprs volatile?
    pub range_table: *mut List,        // single element list of RangeTblEntry
    pub rteperminfos: *mut List,       // single element list of RTEPermissionInfo
    pub qualexpr: *mut ExprState,

    pub transition_capture: *mut TransitionCaptureState,

    /*
     * These variables are used to reduce overhead in COPY FROM.
     *
     * attribute_buf holds the separated, de-escaped text for each field of the
     * current line.  The CopyReadAttributes functions return arrays of pointers
     * into this buffer.  We avoid palloc/pfree overhead by re-using the buffer
     * on each cycle.
     *
     * In binary COPY FROM, attribute_buf holds the binary data for the current
     * field, but the usage is otherwise similar.
     */
    pub attribute_buf: StringInfoData,

    /* field raw data pointers found by COPY FROM */
    pub max_fields: c_int,
    pub raw_fields: *mut *mut c_char,

    /*
     * Similarly, line_buf holds the whole input line being processed. The input
     * cycle is first to read the whole line into line_buf, and then extract the
     * individual attribute fields into attribute_buf.  line_buf is preserved
     * unmodified so that we can display it in error messages if appropriate.
     * (In binary mode, line_buf is not used.)
     */
    pub line_buf: StringInfoData,
    pub line_buf_valid: bool, // contains the row being processed?

    /*
     * input_buf holds input data, already converted to database encoding.
     *
     * In text mode, CopyReadLine parses this data sufficiently to locate line
     * boundaries, then transfers the data to line_buf. We guarantee that there
     * is a \0 at input_buf[input_buf_len] at all times.  (In binary mode,
     * input_buf is not used.)
     *
     * If encoding conversion is not required, input_buf is not a separate buffer
     * but points directly to raw_buf.  In that case, input_buf_len tracks the
     * number of bytes that have been verified as valid in the database encoding,
     * and raw_buf_len is the total number of bytes stored in the buffer.
     */
    pub input_buf: *mut c_char,
    pub input_buf_index: c_int,    // next byte to process
    pub input_buf_len: c_int,      // total # of bytes stored
    pub input_reached_eof: bool,   // true if we reached EOF
    pub input_reached_error: bool, // true if a conversion error happened

    /*
     * raw_buf holds raw input data read from the data source (file or client
     * connection), not yet converted to the database encoding.  Like with
     * 'input_buf', we guarantee that there is a \0 at raw_buf[raw_buf_len].
     */
    pub raw_buf: *mut c_char,
    pub raw_buf_index: c_int,  // next byte to process
    pub raw_buf_len: c_int,    // total # of bytes stored
    pub raw_reached_eof: bool, // true if we reached EOF

    pub bytes_processed: uint64, // number of bytes processed so far
}

// ---------------------------------------------------------------------------
// Function prototypes.
// ---------------------------------------------------------------------------
pub unsafe fn ReceiveCopyBegin(cstate: CopyFromState) {
    unimplemented!()
}

pub unsafe fn ReceiveCopyBinaryHeader(cstate: CopyFromState) {
    unimplemented!()
}

/* One-row callbacks for built-in formats defined in copyfromparse.c */
pub unsafe fn CopyFromTextOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    unimplemented!()
}

pub unsafe fn CopyFromCSVOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    unimplemented!()
}

pub unsafe fn CopyFromBinaryOneRow(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    unimplemented!()
}

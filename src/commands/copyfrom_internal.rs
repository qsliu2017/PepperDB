//! Translated from PostgreSQL src/include/commands/copyfrom_internal.h

use crate::commands::copyapi::CopyFromRoutine;
use crate::commands::copy::{CopyDataSourceCb, CopyFormatOptions, CopyFromState};
use crate::commands::trigger::TransitionCaptureState;
use crate::nodes::execnodes::{ExprContext, ExprState};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::RangeTblEntry;
use crate::access::attnum::AttrNumber;
use crate::fmgr::FmgrInfo;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;
use std::sync::Arc;
use crate::utils::rel::RelationData;

/// The different source cases at the bottom level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopySource {
    File,     // from file (or a piped program)
    Frontend, // from frontend
    Callback, // from callback function
}

/// End-of-line terminator type of the input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EolType {
    Unknown,
    Nl,
    Cr,
    CrNl,
}

/// Insert method to be used during COPY FROM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyInsertMethod {
    Single,           // use table_tuple_insert or ExecForeignInsert
    Multi,            // always use table_multi_insert / ExecForeignBatchInsert
    MultiConditional, // use multi-insert only if valid
}

pub const INPUT_BUF_SIZE: usize = 65536;
pub const RAW_BUF_SIZE: usize = 65536;

/// All state used throughout a COPY FROM operation. In-memory: idiomatic Rust
/// (no layout contract). This is the real body behind `copy::CopyFromStateData`.
pub struct CopyFromStateData {
    /// Format routine. routine-struct: was `const CopyFromRoutine *`.
    pub routine: &'static dyn CopyFromRoutine,

    // low-level state data
    pub copy_src: CopySource, // type of copy source
    pub copy_file: Option<std::fs::File>, // used if copy_src == File
    pub fe_msgbuf: Vec<u8>,   // used if copy_src == Frontend (was StringInfo)

    pub eol_type: EolType,      // EOL type of input
    pub file_encoding: i32,     // file/remote side's character encoding
    pub need_transcoding: bool, // file encoding diff from server?
    pub conversion_proc: Oid,   // encoding conversion function

    // parameters from the COPY command
    pub rel: Arc<RelationData>,           // relation to copy from
    pub attnumlist: Vec<i32>,    // list of attnums to copy
    pub filename: Option<String>, // None for STDIN
    pub is_program: bool,        // is 'filename' a program to popen?
    pub data_source_cb: Option<CopyDataSourceCb>, // reader callback

    pub opts: CopyFormatOptions,
    pub convert_select_flags: Option<Vec<bool>>, // per-column CSV/TEXT CS flags
    pub where_clause: Option<Node>,         // WHERE condition (or None)

    // just for error messages, see CopyFromErrorCallback
    pub cur_relname: Option<String>, // table name for error messages
    pub cur_lineno: u64,             // line number for error messages
    pub cur_attname: Option<String>, // current att for error messages
    pub cur_attval: Option<String>,  // current att value for error messages
    pub relname_only: bool,          // don't output line number, att, etc.

    // Working state
    pub copycontext: MemoryContext, // per-copy execution context

    pub num_defaults: AttrNumber,        // count of att missing w/ default value
    pub in_functions: Vec<FmgrInfo>,     // input functions per attr
    pub typioparams: Vec<Oid>,           // element types for in_functions
    pub escontext: Option<Box<ErrorSaveContext>>, // soft error trapped in in_functions
    pub num_errors: u64,                 // total rows with soft errors
    pub defmap: Vec<i32>,                // default att numbers for missing att
    pub defexprs: Vec<ExprState>,        // default att expressions
    pub defaults: Vec<bool>,             // DEFAULT marker found per att
    pub volatile_defexprs: bool,         // any of defexprs volatile?
    pub range_table: Vec<RangeTblEntry>, // single element list
    pub rteperminfos: Vec<RangeTblEntry>, // single element list of RTEPermissionInfo
    pub qualexpr: Option<Box<ExprState>>,

    pub transition_capture: Option<Box<TransitionCaptureState>>,

    // attribute_buf holds the de-escaped text for each field of the current line.
    pub attribute_buf: Vec<u8>, // was StringInfoData

    // field raw data pointers found by COPY FROM
    pub max_fields: i32,
    pub raw_fields: Vec<String>,

    // line_buf holds the whole input line being processed (unmodified for errors).
    pub line_buf: Vec<u8>,    // was StringInfoData
    pub line_buf_valid: bool, // contains the row being processed?

    // input_buf holds input data, already converted to database encoding.
    pub input_buf: Vec<u8>,
    pub input_buf_index: i32,    // next byte to process
    pub input_buf_len: i32,      // total # of bytes stored
    pub input_reached_eof: bool, // reached EOF?
    pub input_reached_error: bool, // a conversion error happened?

    // raw_buf holds raw input data not yet converted to the database encoding.
    pub raw_buf: Vec<u8>,
    pub raw_buf_index: i32,   // next byte to process
    pub raw_buf_len: i32,     // total # of bytes stored
    pub raw_reached_eof: bool, // reached EOF?

    pub bytes_processed: u64, // number of bytes processed so far
}

impl CopyFromStateData {
    /// Unconsumed bytes available in input_buf (was INPUT_BUF_BYTES macro).
    pub const fn input_buf_bytes(&self) -> i32 {
        self.input_buf_len - self.input_buf_index
    }

    /// Unconsumed bytes available in raw_buf (was RAW_BUF_BYTES macro).
    pub const fn raw_buf_bytes(&self) -> i32 {
        self.raw_buf_len - self.raw_buf_index
    }
}

pub fn ReceiveCopyBegin(_cstate: CopyFromState) {
    unimplemented!()
}

pub fn ReceiveCopyBinaryHeader(_cstate: CopyFromState) {
    unimplemented!()
}

// One-row callbacks for built-in formats (defined in copyfromparse.c). Return
// false when there are no more tuples.
pub fn CopyFromTextOneRow(_cstate: CopyFromState, _econtext: Option<&mut ExprContext>,
                          _values: &mut [Datum], _nulls: &mut [bool]) -> bool {
    unimplemented!()
}

pub fn CopyFromCSVOneRow(_cstate: CopyFromState, _econtext: Option<&mut ExprContext>,
                         _values: &mut [Datum], _nulls: &mut [bool]) -> bool {
    unimplemented!()
}

pub fn CopyFromBinaryOneRow(_cstate: CopyFromState, _econtext: Option<&mut ExprContext>,
                            _values: &mut [Datum], _nulls: &mut [bool]) -> bool {
    unimplemented!()
}

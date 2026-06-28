//! Translated from PostgreSQL src/include/commands/copy.h

use crate::access::tupdesc::TupleDesc;
use crate::commands::copyfrom_internal::CopyFromStateData;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::ExprContext;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CopyStmt, RawStmt};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;
use crate::utils::rel::Relation;

/// Whether a header line should be present, and whether it must match names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyHeaderChoice {
    FALSE = 0,
    TRUE,
    MATCH,
}

/// Where to save input processing errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyOnErrorChoice {
    /// immediately throw errors, default
    STOP = 0,
    /// ignore errors
    IGNORE,
}

/// Verbosity of logged messages by COPY command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyLogVerbosityChoice {
    /// logs none
    SILENT = -1,
    /// logs no additional messages (default, assigned 0)
    DEFAULT = 0,
    /// logs additional messages
    VERBOSE,
}

/// Parsed COPY options (mostly formatting; `freeze` is parsed alongside).
pub struct CopyFormatOptions {
    /// file/remote-side encoding, -1 if not specified
    pub file_encoding: i32,
    pub binary: bool,
    pub freeze: bool,
    pub csv_mode: bool,
    pub header_line: CopyHeaderChoice,
    /// NULL marker string (server encoding!)
    pub null_print: Option<String>,
    pub null_print_len: i32,
    /// same converted to file encoding
    pub null_print_client: Option<String>,
    pub default_print: Option<String>,
    pub default_print_len: i32,
    /// column delimiter (must be 1 byte)
    pub delim: Option<String>,
    /// CSV quote char (must be 1 byte)
    pub quote: Option<String>,
    /// CSV escape char (must be 1 byte)
    pub escape: Option<String>,
    /// list of column names
    pub force_quote: Vec<Node>,
    /// FORCE_QUOTE *?
    pub force_quote_all: bool,
    /// per-column CSV FQ flags
    pub force_quote_flags: Vec<bool>,
    pub force_notnull: Vec<Node>,
    pub force_notnull_all: bool,
    pub force_notnull_flags: Vec<bool>,
    pub force_null: Vec<Node>,
    pub force_null_all: bool,
    pub force_null_flags: Vec<bool>,
    pub convert_selectively: bool,
    pub on_error: CopyOnErrorChoice,
    pub log_verbosity: CopyLogVerbosityChoice,
    /// maximum tolerable number of errors
    pub reject_limit: i64,
    /// list of column names (can be empty)
    pub convert_select: Vec<Node>,
}

/// Opaque; private state defined in copyto.c, not ported.
pub struct CopyToStateData;
pub type CopyFromState = *mut CopyFromStateData; // TODO(ptr)
pub type CopyToState = *mut CopyToStateData; // TODO(ptr)

// copy_data_source_cb / copy_data_dest_cb: opaque callbacks -> closures (6.3).
pub type CopyDataSourceCb = Box<dyn FnMut(&mut [u8], i32, i32) -> i32>;
pub type CopyDataDestCb = Box<dyn FnMut(&[u8], i32)>;

/// DoCopy: `processed` out-param folded into the return.
pub fn DoCopy(
    _pstate: &mut ParseState,
    _stmt: &CopyStmt,
    _stmt_location: i32,
    _stmt_len: i32,
) -> u64 {
    unimplemented!()
}

pub fn ProcessCopyOptions(
    _pstate: &mut ParseState,
    _opts_out: &mut CopyFormatOptions,
    _is_from: bool,
    _options: Vec<Node>,
) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn BeginCopyFrom(
    _pstate: &mut ParseState,
    _rel: Relation,
    _where_clause: Option<&Node>,
    _filename: Option<&str>,
    _is_program: bool,
    _data_source_cb: Option<CopyDataSourceCb>,
    _attnamelist: Vec<Node>,
    _options: Vec<Node>,
) -> CopyFromState {
    unimplemented!()
}

pub fn EndCopyFrom(_cstate: CopyFromState) {
    unimplemented!()
}

pub fn NextCopyFrom(
    _cstate: CopyFromState,
    _econtext: &mut ExprContext,
    _values: &mut [Datum],
    _nulls: &mut [bool],
) -> bool {
    unimplemented!()
}

/// NextCopyFromRawFields: fields/nfields out-params -> returned Vec when present.
pub fn NextCopyFromRawFields(_cstate: CopyFromState) -> Option<Vec<String>> {
    unimplemented!()
}

// CopyFromErrorCallback(void *arg): error-context callback -> closure later.
pub fn CopyFromErrorCallback(_arg: &mut CopyFromStateData) {
    unimplemented!()
}

pub fn CopyLimitPrintoutLength(_str: &str) -> String {
    unimplemented!()
}

pub fn CopyFrom(_cstate: CopyFromState) -> u64 {
    unimplemented!()
}

pub fn CreateCopyDestReceiver() -> Box<dyn DestReceiver> {
    unimplemented!()
}

// internal prototypes
#[allow(clippy::too_many_arguments)]
pub fn BeginCopyTo(
    _pstate: &mut ParseState,
    _rel: Relation,
    _raw_query: *mut RawStmt,
    _query_rel_id: Oid,
    _filename: Option<&str>,
    _is_program: bool,
    _data_dest_cb: Option<CopyDataDestCb>,
    _attnamelist: Vec<Node>,
    _options: Vec<Node>,
) -> CopyToState {
    unimplemented!()
}

pub fn EndCopyTo(_cstate: CopyToState) {
    unimplemented!()
}

pub fn DoCopyTo(_cstate: CopyToState) -> u64 {
    unimplemented!()
}

pub fn CopyGetAttnums(
    _tup_desc: TupleDesc,
    _rel: Relation,
    _attnamelist: Vec<Node>,
) -> Vec<Node> {
    unimplemented!()
}

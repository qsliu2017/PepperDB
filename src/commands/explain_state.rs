//! Translated from PostgreSQL src/include/commands/explain_state.h

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DefElem;
use crate::nodes::plannodes::PlannedStmt;
use crate::parser::parse_node::ParseState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExplainSerializeOption {
    None,
    Text,
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExplainFormat {
    Text,
    Xml,
    Json,
    Yaml,
}

pub struct ExplainWorkersState {
    pub num_workers: i32,            // # of worker processes the plan used
    pub worker_inited: Vec<bool>,    // per-worker state-initialized flags
    pub worker_str: Vec<String>,     // per-worker transient output buffers
    pub worker_state_save: Vec<i32>, // per-worker grouping state save areas
    pub prev_str: String,            // saved output buffer while redirecting
}

/// Opaque per-extension EXPLAIN state (C `void *`); raw plugin pointer.
pub type ExplainExtensionState = *mut core::ffi::c_void; // TODO(ptr)

pub struct ExplainState {
    pub str: String, // output buffer
    // options
    pub verbose: bool,  // be verbose
    pub analyze: bool,  // print actual times
    pub costs: bool,    // print estimated costs
    pub buffers: bool,  // print buffer usage
    pub wal: bool,      // print WAL usage
    pub timing: bool,   // print detailed node timing
    pub summary: bool,  // print total planning and execution timing
    pub memory: bool,   // print planner's memory usage information
    pub settings: bool, // print modified settings
    pub generic: bool,  // generate a generic plan
    pub serialize: ExplainSerializeOption, // serialize the query's output?
    pub format: ExplainFormat, // output format
    // state for output formatting --- not reset for each new plan tree
    pub indent: i32,                  // current indentation level
    pub grouping_stack: Vec<Box<Node>>, // format-specific grouping state
    // state related to the current plan tree (filled by ExplainPrintPlan)
    pub pstmt: *mut PlannedStmt,      // top of plan // TODO(ptr)
    pub rtable: Vec<Box<Node>>,      // range table
    pub rtable_names: Vec<Box<Node>>, // alias names for RTEs
    pub deparse_cxt: Vec<Box<Node>>, // context list for deparsing expressions
    pub printed_subplans: Bitmapset, // ids of SubPlans we've printed
    pub hide_workers: bool,          // set if we find an invisible Gather
    pub rtable_size: i32,            // length of rtable excluding the RTE_GROUP entry
    // state related to the current plan node
    pub workers_state: Option<Box<ExplainWorkersState>>, // needed if parallel plan
    // extensions
    pub extension_state: Vec<ExplainExtensionState>,
}

/// C: `void (*ExplainOptionHandler)(ExplainState *, DefElem *, ParseState *);`
pub type ExplainOptionHandler =
    fn(es: &mut ExplainState, opt: &mut DefElem, pstate: &mut ParseState);

/// Hook to perform additional EXPLAIN options validation.
/// C: `void (*explain_validate_options_hook_type)(ExplainState *, List *, ParseState *);`
pub type explain_validate_options_hook_type =
    fn(es: &mut ExplainState, options: &[Box<Node>], pstate: &mut ParseState);

pub static mut explain_validate_options_hook: Option<explain_validate_options_hook_type> = None;

pub fn NewExplainState() -> Box<ExplainState> {
    unimplemented!()
}

pub fn ParseExplainOptionList(_es: &mut ExplainState, _options: &[Box<Node>], _pstate: &mut ParseState) {
    unimplemented!()
}

pub fn GetExplainExtensionId(_extension_name: &str) -> i32 {
    unimplemented!()
}

pub fn GetExplainExtensionState(_es: &ExplainState, _extension_id: i32) -> ExplainExtensionState {
    unimplemented!()
}

pub fn SetExplainExtensionState(
    _es: &mut ExplainState,
    _extension_id: i32,
    _opaque: ExplainExtensionState,
) {
    unimplemented!()
}

pub fn RegisterExtensionExplainOption(_option_name: &str, _handler: ExplainOptionHandler) {
    unimplemented!()
}

pub fn ApplyExtensionExplainOption(
    _es: &mut ExplainState,
    _opt: &mut DefElem,
    _pstate: &mut ParseState,
) -> bool {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/nodes/params.h
//! Support for finding the values associated with Param nodes.

use bitflags::bitflags;

// Bitmapset is forward-declared in params.h but unreferenced in 18.4 defs.
#[allow(unused_imports)]
use crate::nodes::bitmapset::Bitmapset;
pub use crate::nodes::execnodes::ExprState;
use crate::nodes::primnodes::Param;
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

bitflags! {
    /// C: `#define PARAM_FLAG_CONST 0x0001` - pflags bits for ParamExternData.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ParamFlags: u16 {
        const CONST = 0x0001; // parameter is constant
    }
}

/// Static per-parameter data for EXTERN parameters.
pub struct ParamExternData {
    pub value: Datum,         // parameter value
    pub isnull: bool,         // is it NULL?
    pub pflags: ParamFlags,   // flag bits, see above
    pub ptype: Oid,           // parameter's datatype, or 0
}

/// C: `typedef void (*ParserSetupHook) (struct ParseState *pstate, void *arg);`
/// The void* arg is captured by the closure.
pub type ParserSetupHook = fn(pstate: &mut ParseState);

/// C: `typedef ParamExternData *(*ParamFetchHook) (ParamListInfo params,
/// int paramid, bool speculative, ParamExternData *workspace);`
/// A runtime-NULL-checkable hook fn pointer; its void* `paramFetchArg` is
/// dropped (function-mapping 6.3, matching the sibling ParserSetupHook style).
pub type ParamFetchHook =
    fn(params: &mut ParamListInfoData, paramid: i32, speculative: bool, workspace: &mut ParamExternData) -> ParamExternData;

/// C: `typedef void (*ParamCompileHook) (ParamListInfo params, struct Param *param,
/// struct ExprState *state, Datum *resv, bool *resnull);`
/// Hook fn pointer; its void* `paramCompileArg` is dropped.
pub type ParamCompileHook =
    fn(params: &mut ParamListInfoData, param: &Param, state: &mut ExprState, resv: &mut Datum, resnull: &mut bool);

/// ParamListInfo: parameters passed into the executor for parameterized plans.
/// In-memory; the C FLEXIBLE_ARRAY_MEMBER `params[]` becomes a `Vec`. The
/// dynamic-access hooks (paramFetch/paramCompile) are runtime-NULL-checkable
/// fn pointers; their void* `arg` fields disappear (function-mapping 6.3).
pub struct ParamListInfoData {
    pub param_fetch: Option<ParamFetchHook>,
    pub param_compile: Option<ParamCompileHook>,
    pub parser_setup: Option<ParserSetupHook>,
    pub param_values_str: Option<String>, // params as a single string for errors
    pub num_params: i32,                   // nominal/maximum # of Params represented
    pub params: Vec<ParamExternData>,      // length 0 if param_fetch supplied
}

/// C: `typedef struct ParamListInfoData *ParamListInfo;`
pub type ParamListInfo = Box<ParamListInfoData>; // TODO(ptr)

/// Executor internal parameters (EXEC).
pub struct ParamExecData {
    pub exec_plan: usize, // should be "SubPlanState *" (void *) TODO(ptr)
    pub value: Datum,
    pub isnull: bool,
}

/// Argument for ParamsErrorCallback.
pub struct ParamsErrorCbData {
    pub portal_name: String,
    pub params: ParamListInfo,
}

// Functions in src/backend/nodes/params.c.
pub fn makeParamList(_num_params: i32) -> ParamListInfo {
    unimplemented!()
}
pub fn copyParamList(_from: &ParamListInfoData) -> ParamListInfo {
    unimplemented!()
}
pub fn EstimateParamListSpace(_param_li: &ParamListInfoData) -> usize {
    unimplemented!()
}
pub fn SerializeParamList(_param_li: &ParamListInfoData, _start_address: &mut [u8]) {
    unimplemented!()
}
pub fn RestoreParamList(_start_address: &mut [u8]) -> ParamListInfo {
    unimplemented!()
}
pub fn BuildParamLogString(_params: &ParamListInfoData, _known_text_values: &[&str], _maxlen: i32) -> String {
    unimplemented!()
}
/// C: `void ParamsErrorCallback(void *arg)` - the opaque arg is the callback context.
pub fn ParamsErrorCallback(_arg: &mut ParamsErrorCbData) {
    unimplemented!()
}

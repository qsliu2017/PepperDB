//! Translation of postgres/src/include/nodes/params.h
//!                + (partial) postgres/src/backend/nodes/params.c
//!
//! Support for finding the values associated with Param nodes (ParamListInfo for
//! parameterized plans, ParamExecData for executor-internal params).
//!
//! Only `makeParamList` is translated from params.c so far; the copy/serialize/
//! log helpers depend on datumCopy / datum (de)serialization that are not yet
//! ported. TODO(pg-port): translate the rest of params.c.

use crate::prelude::*;
use crate::nodes::primnodes::Param;
use core::ffi::{c_char, c_void};

/// parameter is constant
pub const PARAM_FLAG_CONST: c_int = 0x0001;

/// Per-parameter value for the "static" PARAM_EXTERN approach.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParamExternData {
    /// parameter value
    pub value: Datum,
    /// is it NULL?
    pub isnull: bool,
    /// flag bits (PARAM_FLAG_*)
    pub pflags: uint16,
    /// parameter's datatype, or 0
    pub ptype: Oid,
}

pub type ParamListInfo = *mut ParamListInfoData;

// Forward-declared types from headers not yet translated.
/// TODO(pg-port): execnodes.h ExprState.
#[repr(C)]
pub struct ExprState {
    _opaque: [u8; 0],
}
/// TODO(pg-port): parser/parse_node.h ParseState.
#[repr(C)]
pub struct ParseState {
    _opaque: [u8; 0],
}

pub type ParamFetchHook = Option<
    unsafe fn(
        params: ParamListInfo,
        paramid: c_int,
        speculative: bool,
        workspace: *mut ParamExternData,
    ) -> *mut ParamExternData,
>;

pub type ParamCompileHook = Option<
    unsafe fn(
        params: ParamListInfo,
        param: *mut Param,
        state: *mut ExprState,
        resv: *mut Datum,
        resnull: *mut bool,
    ),
>;

pub type ParserSetupHook = Option<unsafe fn(pstate: *mut ParseState, arg: *mut c_void)>;

#[repr(C)]
pub struct ParamListInfoData {
    /// parameter fetch hook
    pub paramFetch: ParamFetchHook,
    pub paramFetchArg: *mut c_void,
    /// parameter compile hook
    pub paramCompile: ParamCompileHook,
    pub paramCompileArg: *mut c_void,
    /// parser setup hook
    pub parserSetup: ParserSetupHook,
    pub parserSetupArg: *mut c_void,
    /// params as a single string for errors
    pub paramValuesStr: *mut c_char,
    /// nominal/maximum # of Params represented
    pub numParams: c_int,
    /// params[] is length 0 if paramFetch is supplied, else length numParams.
    pub params: [ParamExternData; FLEXIBLE_ARRAY_MEMBER],
}

/// Executor-internal parameter (PARAM_EXEC): a value passed into/out of a subquery.
#[repr(C)]
pub struct ParamExecData {
    /// should be "SubPlanState *"
    pub execPlan: *mut c_void,
    pub value: Datum,
    pub isnull: bool,
}

/// Argument for ParamsErrorCallback.
#[repr(C)]
pub struct ParamsErrorCbData {
    pub portalName: *const c_char,
    pub params: ParamListInfo,
}

/// `makeParamList(numParams)`: allocate a ParamListInfo with `numParams` slots,
/// all hooks NULL. (from params.c)
///
/// # Safety
/// Returns a palloc'd ParamListInfo the caller owns.
pub unsafe fn makeParamList(numParams: c_int) -> ParamListInfo {
    let size = core::mem::offset_of!(ParamListInfoData, params)
        + numParams as usize * core::mem::size_of::<ParamExternData>();

    let retval = palloc(size) as ParamListInfo;
    (*retval).paramFetch = None;
    (*retval).paramFetchArg = core::ptr::null_mut();
    (*retval).paramCompile = None;
    (*retval).paramCompileArg = core::ptr::null_mut();
    (*retval).parserSetup = None;
    (*retval).parserSetupArg = core::ptr::null_mut();
    (*retval).paramValuesStr = core::ptr::null_mut();
    (*retval).numParams = numParams;

    retval
}

// TODO(pg-port): copyParamList, EstimateParamListSpace, SerializeParamList,
// RestoreParamList, BuildParamLogString, ParamsErrorCallback - need datumCopy /
// datum (de)serialization (utils/adt/datum.c) which are not yet translated.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_param_list() {
        unsafe {
            let pl = makeParamList(3);
            assert_eq!((*pl).numParams, 3);
            assert!((*pl).paramFetch.is_none());
            // write/read the inline params array
            let p0 = (*pl).params.as_mut_ptr();
            (*p0.add(0)).value = 42;
            (*p0.add(0)).ptype = 23; // int4
            (*p0.add(2)).isnull = true;
            assert_eq!((*p0.add(0)).value, 42);
            assert_eq!((*p0.add(0)).ptype, 23);
            assert!((*p0.add(2)).isnull);
            pfree(pl as *mut c_void);
        }
    }
}

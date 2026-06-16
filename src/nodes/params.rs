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
use crate::nodes::primnodes::{Param, ParamKind};
use crate::nodes::nodes::{Node, ParseLoc};
use crate::nodes::parsenodes::ParamRef;
pub use crate::parser::parse_node::ParseState;
use crate::access::transam::xact::IsAbortedTransactionBlockState;
use crate::makeNode;
use core::ffi::{c_char, c_void};

/// TODO(pg-port): utils/elog.h errcontext(); emits an error-context line.
macro_rules! errcontext {
    ($fmt:expr $(, $arg:expr)* $(,)?) => {{ let _ = ($fmt $(, $arg)*); }};
}

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
    (*retval).parserSetup = Some(paramlist_parser_setup);
    (*retval).parserSetupArg = retval as *mut c_void;
    (*retval).paramValuesStr = core::ptr::null_mut();
    (*retval).numParams = numParams;

    retval
}

/// Copy a ParamListInfo structure.
///
/// The result is allocated in CurrentMemoryContext.
///
/// Note: the intent of this function is to make a static, self-contained
/// set of parameter values.  If dynamic parameter hooks are present, we
/// intentionally do not copy them into the result.  Rather, we forcibly
/// instantiate all available parameter values and copy the datum values.
///
/// paramValuesStr is not copied, either.
pub unsafe fn copyParamList(from: ParamListInfo) -> ParamListInfo {
    if from.is_null() || (*from).numParams <= 0 {
        return core::ptr::null_mut();
    }

    let retval = makeParamList((*from).numParams);

    for i in 0..(*from).numParams {
        let oprm: *mut ParamExternData;
        let nprm: *mut ParamExternData = &raw mut (*retval).params[i as usize];
        let mut prmdata: ParamExternData = core::mem::zeroed();
        let mut typLen: int16 = 0;
        let mut typByVal: bool = false;

        /* give hook a chance in case parameter is dynamic */
        if let Some(paramFetch) = (*from).paramFetch {
            oprm = paramFetch(from, i + 1, false, &raw mut prmdata);
        } else {
            oprm = &raw mut (*from).params[i as usize];
        }

        /* flat-copy the parameter info */
        *nprm = *oprm;

        /* need datumCopy in case it's a pass-by-reference datatype */
        if (*nprm).isnull || !OidIsValid((*nprm).ptype) {
            continue;
        }
        get_typlenbyval((*nprm).ptype, &raw mut typLen, &raw mut typByVal);
        (*nprm).value = datumCopy((*nprm).value, typByVal, typLen as c_int);
    }

    retval
}

/// Set up to parse a query containing references to parameters
/// sourced from a ParamListInfo.
unsafe fn paramlist_parser_setup(pstate: *mut ParseState, arg: *mut c_void) {
    (*pstate).p_paramref_hook = Some(paramlist_param_ref);
    /* no need to use p_coerce_param_hook */
    (*pstate).p_ref_hook_state = arg;
}

/// Transform a ParamRef using parameter type data from a ParamListInfo.
unsafe fn paramlist_param_ref(pstate: *mut ParseState, pref: *mut c_void) -> *mut Node {
    let pref: *mut ParamRef = pref as *mut ParamRef;
    let paramLI: ParamListInfo = (*pstate).p_ref_hook_state as ParamListInfo;
    let paramno: c_int = (*pref).number;
    let prm: *mut ParamExternData;
    let mut prmdata: ParamExternData = core::mem::zeroed();
    let param: *mut Param;

    /* check parameter number is valid */
    if paramno <= 0 || paramno > (*paramLI).numParams {
        return core::ptr::null_mut();
    }

    /* give hook a chance in case parameter is dynamic */
    if let Some(paramFetch) = (*paramLI).paramFetch {
        prm = paramFetch(paramLI, paramno, false, &raw mut prmdata);
    } else {
        prm = &raw mut (*paramLI).params[(paramno - 1) as usize];
    }

    if !OidIsValid((*prm).ptype) {
        return core::ptr::null_mut();
    }

    param = makeNode!(Param, T_Param);
    (*param).paramkind = ParamKind::PARAM_EXTERN;
    (*param).paramid = paramno;
    (*param).paramtype = (*prm).ptype;
    (*param).paramtypmod = -1;
    (*param).paramcollid = get_typcollation((*param).paramtype);
    (*param).location = (*pref).location;

    param as *mut Node
}

/// Estimate the amount of space required to serialize a ParamListInfo.
pub unsafe fn EstimateParamListSpace(paramLI: ParamListInfo) -> Size {
    let mut sz: Size = core::mem::size_of::<c_int>();

    if paramLI.is_null() || (*paramLI).numParams <= 0 {
        return sz;
    }

    for i in 0..(*paramLI).numParams {
        let prm: *mut ParamExternData;
        let mut prmdata: ParamExternData = core::mem::zeroed();
        let typeOid: Oid;
        let mut typLen: int16 = 0;
        let mut typByVal: bool = false;

        /* give hook a chance in case parameter is dynamic */
        if let Some(paramFetch) = (*paramLI).paramFetch {
            prm = paramFetch(paramLI, i + 1, false, &raw mut prmdata);
        } else {
            prm = &raw mut (*paramLI).params[i as usize];
        }

        typeOid = (*prm).ptype;

        sz = add_size(sz, core::mem::size_of::<Oid>()); /* space for type OID */
        sz = add_size(sz, core::mem::size_of::<uint16>()); /* space for pflags */

        /* space for datum/isnull */
        if OidIsValid(typeOid) {
            get_typlenbyval(typeOid, &raw mut typLen, &raw mut typByVal);
        } else {
            /* If no type OID, assume by-value, like copyParamList does. */
            typLen = core::mem::size_of::<Datum>() as int16;
            typByVal = true;
        }
        sz = add_size(
            sz,
            datumEstimateSpace((*prm).value, (*prm).isnull, typByVal, typLen as c_int),
        );
    }

    sz
}

/// Serialize a ParamListInfo structure into caller-provided storage.
///
/// We write the number of parameters first, as a 4-byte integer, and then
/// write details for each parameter in turn.  The details for each parameter
/// consist of a 4-byte type OID, 2 bytes of flags, and then the datum as
/// serialized by datumSerialize().  The caller is responsible for ensuring
/// that there is enough storage to store the number of bytes that will be
/// written; use EstimateParamListSpace to find out how many will be needed.
/// *start_address is updated to point to the byte immediately following those
/// written.
///
/// RestoreParamList can be used to recreate a ParamListInfo based on the
/// serialized representation; this will be a static, self-contained copy
/// just as copyParamList would create.
///
/// paramValuesStr is not included.
pub unsafe fn SerializeParamList(paramLI: ParamListInfo, start_address: *mut *mut c_char) {
    let nparams: c_int;

    /* Write number of parameters. */
    if paramLI.is_null() || (*paramLI).numParams <= 0 {
        nparams = 0;
    } else {
        nparams = (*paramLI).numParams;
    }
    core::ptr::copy_nonoverlapping(
        &raw const nparams as *const u8,
        *start_address as *mut u8,
        core::mem::size_of::<c_int>(),
    );
    *start_address = (*start_address).add(core::mem::size_of::<c_int>());

    /* Write each parameter in turn. */
    for i in 0..nparams {
        let prm: *mut ParamExternData;
        let mut prmdata: ParamExternData = core::mem::zeroed();
        let typeOid: Oid;
        let mut typLen: int16 = 0;
        let mut typByVal: bool = false;

        /* give hook a chance in case parameter is dynamic */
        if let Some(paramFetch) = (*paramLI).paramFetch {
            prm = paramFetch(paramLI, i + 1, false, &raw mut prmdata);
        } else {
            prm = &raw mut (*paramLI).params[i as usize];
        }

        typeOid = (*prm).ptype;

        /* Write type OID. */
        core::ptr::copy_nonoverlapping(
            &raw const typeOid as *const u8,
            *start_address as *mut u8,
            core::mem::size_of::<Oid>(),
        );
        *start_address = (*start_address).add(core::mem::size_of::<Oid>());

        /* Write flags. */
        core::ptr::copy_nonoverlapping(
            &raw const (*prm).pflags as *const u8,
            *start_address as *mut u8,
            core::mem::size_of::<uint16>(),
        );
        *start_address = (*start_address).add(core::mem::size_of::<uint16>());

        /* Write datum/isnull. */
        if OidIsValid(typeOid) {
            get_typlenbyval(typeOid, &raw mut typLen, &raw mut typByVal);
        } else {
            /* If no type OID, assume by-value, like copyParamList does. */
            typLen = core::mem::size_of::<Datum>() as int16;
            typByVal = true;
        }
        datumSerialize(
            (*prm).value,
            (*prm).isnull,
            typByVal,
            typLen as c_int,
            start_address,
        );
    }
}

/// Restore a ParamListInfo structure from serialized form.
///
/// The result is allocated in CurrentMemoryContext.
///
/// Note: the intent of this function is to make a static, self-contained
/// set of parameter values.  If dynamic parameter hooks are present, we
/// intentionally do not copy them into the result.  Rather, we forcibly
/// instantiate all available parameter values and copy the datum values.
pub unsafe fn RestoreParamList(start_address: *mut *mut c_char) -> ParamListInfo {
    let paramLI: ParamListInfo;
    let mut nparams: c_int = 0;

    core::ptr::copy_nonoverlapping(
        *start_address as *const u8,
        &raw mut nparams as *mut u8,
        core::mem::size_of::<c_int>(),
    );
    *start_address = (*start_address).add(core::mem::size_of::<c_int>());

    paramLI = makeParamList(nparams);

    for i in 0..nparams {
        let prm: *mut ParamExternData = &raw mut (*paramLI).params[i as usize];

        /* Read type OID. */
        core::ptr::copy_nonoverlapping(
            *start_address as *const u8,
            &raw mut (*prm).ptype as *mut u8,
            core::mem::size_of::<Oid>(),
        );
        *start_address = (*start_address).add(core::mem::size_of::<Oid>());

        /* Read flags. */
        core::ptr::copy_nonoverlapping(
            *start_address as *const u8,
            &raw mut (*prm).pflags as *mut u8,
            core::mem::size_of::<uint16>(),
        );
        *start_address = (*start_address).add(core::mem::size_of::<uint16>());

        /* Read datum/isnull. */
        (*prm).value = datumRestore(start_address, &raw mut (*prm).isnull);
    }

    paramLI
}

/// BuildParamLogString
///     Return a string that represents the parameter list, for logging.
///
/// If caller already knows textual representations for some parameters, it can
/// pass an array of exactly params->numParams values as knownTextValues, which
/// can contain NULLs for any unknown individual values.  NULL can be given if
/// no parameters are known.
///
/// If maxlen is >= 0, that's the maximum number of bytes of any one
/// parameter value to be printed; an ellipsis is added if the string is
/// longer.  (Added quotes are not considered in this calculation.)
pub unsafe fn BuildParamLogString(
    params: ParamListInfo,
    knownTextValues: *mut *mut c_char,
    maxlen: c_int,
) -> *mut c_char {
    let tmpCxt: MemoryContext;
    let oldCxt: MemoryContext;
    let mut buf: StringInfoData = core::mem::zeroed();

    /*
     * NB: think not of returning params->paramValuesStr!  It may have been
     * generated with a different maxlen, and so be unsuitable.  Besides that,
     * this is the function used to create that string.
     */

    /*
     * No work if the param fetch hook is in use.  Also, it's not possible to
     * do this in an aborted transaction.  (It might be possible to improve on
     * this last point when some knownTextValues exist, but it seems tricky.)
     */
    if (*params).paramFetch.is_some() || IsAbortedTransactionBlockState() {
        return core::ptr::null_mut();
    }

    /* Initialize the output stringinfo, in caller's memory context */
    initStringInfo(&raw mut buf);

    /* Use a temporary context to call output functions, just in case */
    tmpCxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"BuildParamLogString".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    oldCxt = MemoryContextSwitchTo(tmpCxt);

    for paramno in 0..(*params).numParams {
        let param: *mut ParamExternData = &raw mut (*params).params[paramno as usize];

        appendStringInfo(
            &raw mut buf,
            c"%s$%d = ".as_ptr(),
            if paramno > 0 { c", ".as_ptr() } else { c"".as_ptr() },
            paramno + 1,
        );

        if (*param).isnull || !OidIsValid((*param).ptype) {
            appendStringInfoString(&raw mut buf, c"NULL".as_ptr());
        } else {
            if !knownTextValues.is_null() && !(*knownTextValues.add(paramno as usize)).is_null() {
                appendStringInfoStringQuoted(
                    &raw mut buf,
                    *knownTextValues.add(paramno as usize),
                    maxlen,
                );
            } else {
                let mut typoutput: Oid = 0;
                let mut typisvarlena: bool = false;
                let pstring: *mut c_char;

                getTypeOutputInfo((*param).ptype, &raw mut typoutput, &raw mut typisvarlena);
                pstring = OidOutputFunctionCall(typoutput, (*param).value);
                appendStringInfoStringQuoted(&raw mut buf, pstring, maxlen);
            }
        }
    }

    MemoryContextSwitchTo(oldCxt);
    MemoryContextDelete(tmpCxt);

    buf.data
}

/// ParamsErrorCallback - callback for printing parameters in error context
///
/// Note that this is a no-op unless BuildParamLogString has been called
/// beforehand.
pub unsafe fn ParamsErrorCallback(arg: *mut c_void) {
    let data: *mut ParamsErrorCbData = arg as *mut ParamsErrorCbData;

    if data.is_null()
        || (*data).params.is_null()
        || (*(*data).params).paramValuesStr.is_null()
    {
        return;
    }

    if !(*data).portalName.is_null() && *(*data).portalName != 0 {
        errcontext!(
            c"portal \"%s\" with parameters: %s".as_ptr(),
            (*data).portalName,
            (*(*data).params).paramValuesStr
        );
    } else {
        errcontext!(
            c"unnamed portal with parameters: %s".as_ptr(),
            (*(*data).params).paramValuesStr
        );
    }
}

// ---------------------------------------------------------------------------
// Local stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

/// TODO(pg-port): lib/stringinfo.h StringInfoData.
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

extern "C" {
    /// TODO(pg-port): variadic, fmgr.h getTypeOutputInfo etc. handled via local stubs.
    fn appendStringInfo(str: *mut StringInfoData, fmt: *const c_char, ...);
}

/// TODO(pg-port): utils/datum.c datumCopy.
unsafe fn datumCopy(_value: Datum, _typByVal: bool, _typLen: c_int) -> Datum {
    unimplemented!()
}

/// TODO(pg-port): utils/datum.c datumEstimateSpace.
unsafe fn datumEstimateSpace(
    _value: Datum,
    _isnull: bool,
    _typByVal: bool,
    _typLen: c_int,
) -> Size {
    unimplemented!()
}

/// TODO(pg-port): utils/datum.c datumSerialize.
unsafe fn datumSerialize(
    _value: Datum,
    _isnull: bool,
    _typByVal: bool,
    _typLen: c_int,
    _start_address: *mut *mut c_char,
) {
    unimplemented!()
}

/// TODO(pg-port): utils/datum.c datumRestore.
unsafe fn datumRestore(_start_address: *mut *mut c_char, _isnull: *mut bool) -> Datum {
    unimplemented!()
}

/// TODO(pg-port): utils/cache/lsyscache.c get_typlenbyval.
unsafe fn get_typlenbyval(_typid: Oid, _typlen: *mut int16, _typbyval: *mut bool) {
    unimplemented!()
}

/// TODO(pg-port): utils/cache/lsyscache.c get_typcollation.
unsafe fn get_typcollation(_typid: Oid) -> Oid {
    unimplemented!()
}

/// TODO(pg-port): utils/fmgr.c getTypeOutputInfo.
unsafe fn getTypeOutputInfo(_type: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!()
}

/// TODO(pg-port): utils/fmgr/fmgr.c OidOutputFunctionCall.
unsafe fn OidOutputFunctionCall(_functionId: Oid, _val: Datum) -> *mut c_char {
    unimplemented!()
}

/// TODO(pg-port): lib/stringinfo.c initStringInfo.
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!()
}

/// TODO(pg-port): lib/stringinfo.c appendStringInfoString.
unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!()
}

/// TODO(pg-port): mb/stringinfo_mb.c appendStringInfoStringQuoted.
unsafe fn appendStringInfoStringQuoted(_str: *mut StringInfoData, _s: *const c_char, _maxlen: c_int) {
    unimplemented!()
}

/// TODO(pg-port): common/int.h add_size.
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}

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

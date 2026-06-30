//! The Postgres function manager. Translated from
//! src/backend/utils/fmgr/fmgr.c.
//!
//! `fmgr_info`/`fmgr_info_cxt` fill a [`FmgrInfo`] from a function OID; the
//! `FunctionCallN`/`OidFunctionCallN` families invoke a looked-up or named
//! function with a directly-computed argument list; `InputFunctionCall` /
//! `OutputFunctionCall` and friends wrap the datatype I/O functions. Lookup over
//! the generated builtin table (`fmgr_isbuiltin` / `fmgr_lookupByName`) is the
//! fast path; everything else needs pg_proc via syscache.
//!
//! Subsystems not yet translated are reached through their existing
//! `unimplemented!()` stubs (rules.md s4): syscache (`SearchSysCache1`) for
//! non-builtin functions, the C-language dynamic loader (`load_external_function`
//! / `fetch_finfo_record`), the security-definer call handler, detoast
//! (`detoast_attr`), and the expr-tree introspection (`exprType`, the `IsA` node
//! dispatch). For M1 only the builtin path needs to actually work.
//!
//! Memory management is tombstoned: PG's `palloc`/`pfree`/`MemoryContext` become
//! Rust ownership, so `pg_detoast_datum_copy` returns an owned copy and the
//! `fmgr_symbol` two-out-param C signature folds to a returned tuple. The C
//! `CFuncHash` cache for external C functions is part of the deferred C-language
//! path and is not built here.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): the not-yet-translated subsystem reaches \
              (syscache/detoast/dfmgr) use unimplemented!(); FunctionCallInvoke's \
              fn_addr unwrap is guarded by the header allow. New raises use elog!/OrElog."
)]
#![allow(
    clippy::similar_names,
    reason = "fcinfo/flinfo are the PG identifiers (FunctionCallInfo / FmgrInfo)"
)]
#![allow(
    clippy::unnecessary_wraps,
    reason = "the call families' return type is Option<Datum> per the fmgr.h header \
              contract (None == SQL NULL); the NULL path diverges via elog(ERROR), \
              so the happy path always yields Some -- the signature is fixed"
)]

use crate::c::{bytea, varlena};
use crate::{elog, ereport};
use crate::fmgr::{
    FmgrInfo, FunctionCallInfoBaseData, InitFunctionCallInfoData, PGFunction, Pg_finfo_record,
};
use crate::nodes::nodes::Node;
use crate::postgres::{
    CStringGetDatum, Datum, DatumGetCString, Int32GetDatum, ObjectIdGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_FUNCTION};
use crate::utils::fmgrtab::{
    fmgr_builtin_oid_index, fmgr_builtins, fmgr_last_builtin_oid, fmgr_nbuiltins, FmgrBuiltin,
    InvalidOidBuiltinMapping,
};
use crate::utils::palloc::MemoryContext;

// TRACK_FUNC_* (utils/guc.h) gate function-stats tracking. The builtin/internal
// fast path uses ALL ("never track"); the rest are placeholders until the call
// handler / pgstat tracking lands.
const TRACK_FUNC_OFF: u8 = 0;
const TRACK_FUNC_PL: u8 = 1;
const TRACK_FUNC_ALL: u8 = 2;

// pg_language.h OIDs for the prolang switch in fmgr_info_cxt_security.
const INTERNAL_LANGUAGE_ID: Oid = Oid::new(12);
const C_LANGUAGE_ID: Oid = Oid::new(13);
const SQL_LANGUAGE_ID: Oid = Oid::new(14);

// ---------------------------------------------------------------------------
// Lookup routines for the builtin-function table. Search by Oid or by name;
// by Oid is much faster (a direct index).
// ---------------------------------------------------------------------------

/// PG `fmgr_isbuiltin`: builtin-table entry for `id`, or `None`.
fn fmgr_isbuiltin(id: Oid) -> Option<&'static FmgrBuiltin> {
    // Fast lookup only possible if the original OID is still assigned.
    if id > fmgr_last_builtin_oid {
        return None;
    }
    // A miss in range is likely a nonexistent function; None triggers an ERROR
    // later, matching C.
    let index = fmgr_builtin_oid_index[id.get() as usize];
    if index == InvalidOidBuiltinMapping {
        return None;
    }
    Some(&fmgr_builtins[index as usize])
}

/// PG `fmgr_lookupByName`: builtin-table entry whose name matches, or `None`.
/// There can be several entries with the same name; they all point to the same
/// routine.
fn fmgr_lookup_by_name(name: &str) -> Option<&'static FmgrBuiltin> {
    debug_assert_eq!(fmgr_builtins.len(), fmgr_nbuiltins);
    fmgr_builtins.iter().find(|b| b.func_name == name)
}

// ---------------------------------------------------------------------------
// fmgr_info: fill a FmgrInfo from a function OID.
// ---------------------------------------------------------------------------

/// PG `fmgr_info`: fill `finfo` for the function `function_id`.
///
/// The caller's current memory context becomes `fn_mcxt`; subsidiary data lives
/// there, so the caller must keep it at least as long as the info struct.
#[allow(deprecated)]
pub fn fmgr_info(function_id: Oid, finfo: &mut FmgrInfo) {
    fmgr_info_cxt_security(function_id, finfo, current_memory_context(), false);
}

/// PG `fmgr_info_cxt`: like [`fmgr_info`], with an explicit context for the
/// subsidiary data.
#[allow(deprecated)]
pub fn fmgr_info_cxt(function_id: Oid, finfo: &mut FmgrInfo, mcxt: MemoryContext) {
    fmgr_info_cxt_security(function_id, finfo, mcxt, false);
}

/// PG `fmgr_info_cxt_security`: the actual work. `ignore_security` is true only
/// to avoid recursion (from the security-definer handler / other-language path).
#[allow(deprecated)]
fn fmgr_info_cxt_security(
    function_id: Oid,
    finfo: &mut FmgrInfo,
    mcxt: MemoryContext,
    ignore_security: bool,
) {
    // fn_oid *must* be filled in last: some code assumes a valid fn_oid means the
    // whole struct is valid, and some FmgrInfo structs survive elogs.
    finfo.oid = InvalidOid;
    finfo.extra = 0;
    let _ = mcxt; // fn_mcxt tombstoned (see FmgrInfo.mcxt)
    finfo.mcxt = ();
    finfo.expr = None; // caller may set this later

    if let Some(fbp) = fmgr_isbuiltin(function_id) {
        // Fast path for builtin functions: don't bother consulting pg_proc.
        finfo.nargs = fbp.nargs;
        finfo.strict = fbp.strict;
        finfo.retset = fbp.retset;
        finfo.stats = TRACK_FUNC_ALL; // ie, never track
        finfo.fn_addr = fbp.func;
        finfo.oid = function_id;
        return;
    }

    // Otherwise we need the pg_proc entry. Everything below reaches syscache /
    // the call handler / the dynamic loader, none of which is translated yet.
    let _ = (ignore_security, C_LANGUAGE_ID, SQL_LANGUAGE_ID, INTERNAL_LANGUAGE_ID);
    let _ = (TRACK_FUNC_OFF, TRACK_FUNC_PL);
    let _ = fmgr_lookup_by_name; // reached on the CREATE FUNCTION alias path
    unimplemented!("fmgr_info_cxt_security non-builtin path (syscache/pg_proc) deferred")
}

/// PG `fmgr_symbol`: module + C function name implementing `function_id`.
///
/// C returns through two `char **` out-params; here that folds to a `(module,
/// function)` tuple. The non-builtin cases need syscache.
pub fn fmgr_symbol(_function_id: Oid) -> (String, String) {
    unimplemented!("fmgr_symbol needs pg_proc via syscache")
}

/// PG `fetch_finfo_record`: fetch + validate the info record for an external
/// function (C-language path). Reaches the dynamic loader (dfmgr.c).
pub fn fetch_finfo_record(_filehandle: usize, _funcname: &str) -> &'static Pg_finfo_record {
    unimplemented!("fetch_finfo_record needs the dynamic loader (dfmgr)")
}

/// PG `fmgr_info_copy`: shallow-copy an FmgrInfo, retargeting `fn_mcxt` and
/// zeroing `fn_extra` (subsidiary info is recomputed on next use).
#[allow(deprecated)]
pub fn fmgr_info_copy(dstinfo: &mut FmgrInfo, srcinfo: &mut FmgrInfo, destcxt: MemoryContext) {
    dstinfo.fn_addr = srcinfo.fn_addr;
    dstinfo.oid = srcinfo.oid;
    dstinfo.nargs = srcinfo.nargs;
    dstinfo.strict = srcinfo.strict;
    dstinfo.retset = srcinfo.retset;
    dstinfo.stats = srcinfo.stats;
    // fn_expr is None on every path that reaches a copy in this port; the C
    // shallow copy shares the parse-tree pointer, which we never need here.
    dstinfo.expr = None;
    let _ = destcxt; // fn_mcxt tombstoned (see FmgrInfo.mcxt)
    dstinfo.mcxt = ();
    dstinfo.extra = 0;
}

/// PG `fmgr_internal_function`: OID of the internal function named `proname`,
/// or `InvalidOid`. (Used by `fmgr_internal_validator`.) Folds to `Option<Oid>`.
pub fn fmgr_internal_function(proname: &str) -> Option<Oid> {
    fmgr_lookup_by_name(proname).map(|fbp| fbp.foid)
}

// ---------------------------------------------------------------------------
// fcinfo helpers shared by the call families.
// ---------------------------------------------------------------------------

/// Build a fresh fcinfo with `nargs` slots (PG's `LOCAL_FCINFO` is a stack-local
/// flexible-array; here the args are a `Vec`).
fn local_fcinfo(
    flinfo: Option<Box<FmgrInfo>>,
    nargs: i16,
    collation: Oid,
    context: Option<Box<Node>>,
    resultinfo: Option<Box<Node>>,
) -> FunctionCallInfoBaseData {
    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: InvalidOid,
        isnull: false,
        nargs: 0,
        args: vec![
            crate::postgres::NullableDatum { value: Datum(0), isnull: true };
            nargs as usize
        ],
    };
    InitFunctionCallInfoData(&mut fcinfo, flinfo, nargs, collation, context, resultinfo);
    fcinfo
}

/// Set arg `n` to a non-null `value`.
fn set_arg(fcinfo: &mut FunctionCallInfoBaseData, n: usize, value: Datum) {
    fcinfo.args[n].value = value;
    fcinfo.args[n].isnull = false;
}

/// Populate fcinfo args 0.. from `args`, each non-null (the `DirectFunctionCallN`
/// / `FunctionCallN` contract: no NULL arguments).
fn set_args(fcinfo: &mut FunctionCallInfoBaseData, args: &[Datum]) {
    for (n, &value) in args.iter().enumerate() {
        set_arg(fcinfo, n, value);
    }
}

/// Invoke `func` on a fully-populated fcinfo; fold `isnull` into the result.
/// The header's call families return `Option<Datum>` (None == SQL NULL); the C
/// code instead `elog(ERROR)`s on an unexpected NULL, which (raising) is why the
/// happy path always yields `Some`.
fn invoke(func: PGFunction, fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    func(fcinfo)
}

// ---------------------------------------------------------------------------
// DirectFunctionCallN: invoke a specifically-named function with a directly
// computed parameter list (no FmgrInfo). Arguments and result are non-NULL.
// ---------------------------------------------------------------------------

macro_rules! direct_function_call {
    ($name:ident, $n:expr, $($arg:ident),+) => {
        /// PG `
        #[doc = stringify!($name)]
        /// `.
        pub fn $name(func: PGFunction, collation: Oid, $($arg: Datum),+) -> Option<Datum> {
            let mut fcinfo = local_fcinfo(None, $n, collation, None, None);
            set_args(&mut fcinfo, &[$($arg),+]);
            let result = invoke(func, &mut fcinfo);
            if fcinfo.isnull {
                elog!(ERROR, "function returned NULL".to_string());
            }
            Some(result)
        }
    };
}

direct_function_call!(DirectFunctionCall1Coll, 1, arg1);
direct_function_call!(DirectFunctionCall2Coll, 2, arg1, arg2);
direct_function_call!(DirectFunctionCall3Coll, 3, arg1, arg2, arg3);
direct_function_call!(DirectFunctionCall4Coll, 4, arg1, arg2, arg3, arg4);
direct_function_call!(DirectFunctionCall5Coll, 5, arg1, arg2, arg3, arg4, arg5);
direct_function_call!(DirectFunctionCall6Coll, 6, arg1, arg2, arg3, arg4, arg5, arg6);
direct_function_call!(DirectFunctionCall7Coll, 7, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
direct_function_call!(DirectFunctionCall8Coll, 8, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
direct_function_call!(DirectFunctionCall9Coll, 9, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);

// ---------------------------------------------------------------------------
// CallerFInfoFunctionCallN: like Direct*, but initialise the fcinfo from a
// caller-supplied flinfo (the callee should only touch fn_extra / fn_mcxt).
// ---------------------------------------------------------------------------

/// PG `CallerFInfoFunctionCall1`.
#[allow(deprecated)]
pub fn CallerFInfoFunctionCall1(
    func: PGFunction,
    flinfo: &mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
) -> Option<Datum> {
    let mut fcinfo = local_fcinfo(
        Some(Box::new(clone_flinfo(flinfo))),
        1,
        collation,
        None,
        None,
    );
    set_arg(&mut fcinfo, 0, arg1);
    let result = invoke(func, &mut fcinfo);
    if fcinfo.isnull {
        elog!(ERROR, "function returned NULL".to_string());
    }
    Some(result)
}

/// PG `CallerFInfoFunctionCall2`.
#[allow(deprecated)]
pub fn CallerFInfoFunctionCall2(
    func: PGFunction,
    flinfo: &mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
) -> Option<Datum> {
    let mut fcinfo = local_fcinfo(
        Some(Box::new(clone_flinfo(flinfo))),
        2,
        collation,
        None,
        None,
    );
    set_arg(&mut fcinfo, 0, arg1);
    set_arg(&mut fcinfo, 1, arg2);
    let result = invoke(func, &mut fcinfo);
    if fcinfo.isnull {
        elog!(ERROR, "function returned NULL".to_string());
    }
    Some(result)
}

/// Shallow flinfo clone (the fcinfo borrows a copy; C aliases the pointer).
#[allow(deprecated)]
fn clone_flinfo(src: &FmgrInfo) -> FmgrInfo {
    FmgrInfo {
        fn_addr: src.fn_addr,
        oid: src.oid,
        nargs: src.nargs,
        strict: src.strict,
        retset: src.retset,
        stats: src.stats,
        extra: src.extra,
        mcxt: (),
        expr: None,
    }
}

// ---------------------------------------------------------------------------
// FunctionCallNColl: invoke a previously-looked-up function (via its flinfo).
// ---------------------------------------------------------------------------

/// Invoke `flinfo`'s function on a populated fcinfo, ERRORing on a NULL result
/// (the caller is not expecting one). Returns `Some(result)` (the header's
/// `Option<Datum>` fold; the C NULL case diverges via elog(ERROR)).
#[allow(deprecated)]
fn function_call_invoke_checked(fcinfo: &mut FunctionCallInfoBaseData, fn_oid: Oid) -> Option<Datum> {
    let func = fcinfo
        .flinfo
        .as_ref()
        .and_then(|fi| fi.fn_addr)
        .unwrap_or_else(|| panic!("FunctionCallN reached without an installed fn_addr"));
    let result = func(fcinfo);
    if fcinfo.isnull {
        elog!(ERROR, format!("function {} returned NULL", fn_oid.get()));
    }
    Some(result)
}

macro_rules! function_call {
    ($name:ident, $n:expr $(, $arg:ident)*) => {
        /// PG `
        #[doc = stringify!($name)]
        /// `.
        #[allow(deprecated)]
        pub fn $name(flinfo: &mut FmgrInfo, collation: Oid $(, $arg: Datum)*) -> Option<Datum> {
            let fn_oid = flinfo.oid;
            let mut fcinfo = local_fcinfo(
                Some(Box::new(clone_flinfo(flinfo))),
                $n,
                collation,
                None,
                None,
            );
            set_args(&mut fcinfo, &[$($arg),*]);
            function_call_invoke_checked(&mut fcinfo, fn_oid)
        }
    };
}

function_call!(FunctionCall0Coll, 0);
function_call!(FunctionCall1Coll, 1, arg1);
function_call!(FunctionCall2Coll, 2, arg1, arg2);
function_call!(FunctionCall3Coll, 3, arg1, arg2, arg3);
function_call!(FunctionCall4Coll, 4, arg1, arg2, arg3, arg4);
function_call!(FunctionCall5Coll, 5, arg1, arg2, arg3, arg4, arg5);
function_call!(FunctionCall6Coll, 6, arg1, arg2, arg3, arg4, arg5, arg6);
function_call!(FunctionCall7Coll, 7, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
function_call!(FunctionCall8Coll, 8, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
function_call!(FunctionCall9Coll, 9, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);

// ---------------------------------------------------------------------------
// OidFunctionCallNColl: fmgr_info() followed by FunctionCallN().
// ---------------------------------------------------------------------------

macro_rules! oid_function_call {
    ($name:ident, $call:ident $(, $arg:ident)*) => {
        /// PG `
        #[doc = stringify!($name)]
        /// `.
        #[allow(deprecated)]
        pub fn $name(function_id: Oid, collation: Oid $(, $arg: Datum)*) -> Option<Datum> {
            let mut flinfo = empty_flinfo();
            fmgr_info(function_id, &mut flinfo);
            $call(&mut flinfo, collation $(, $arg)*)
        }
    };
}

oid_function_call!(OidFunctionCall0Coll, FunctionCall0Coll);
oid_function_call!(OidFunctionCall1Coll, FunctionCall1Coll, arg1);
oid_function_call!(OidFunctionCall2Coll, FunctionCall2Coll, arg1, arg2);
oid_function_call!(OidFunctionCall3Coll, FunctionCall3Coll, arg1, arg2, arg3);
oid_function_call!(OidFunctionCall4Coll, FunctionCall4Coll, arg1, arg2, arg3, arg4);
oid_function_call!(OidFunctionCall5Coll, FunctionCall5Coll, arg1, arg2, arg3, arg4, arg5);
oid_function_call!(OidFunctionCall6Coll, FunctionCall6Coll, arg1, arg2, arg3, arg4, arg5, arg6);
oid_function_call!(OidFunctionCall7Coll, FunctionCall7Coll, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
oid_function_call!(OidFunctionCall8Coll, FunctionCall8Coll, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
oid_function_call!(OidFunctionCall9Coll, FunctionCall9Coll, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);

/// A zeroed FmgrInfo to be filled by `fmgr_info` (PG's stack `FmgrInfo flinfo;`).
#[allow(deprecated)]
pub fn empty_flinfo() -> FmgrInfo {
    FmgrInfo {
        fn_addr: None,
        oid: InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

// ---------------------------------------------------------------------------
// Datatype I/O convenience invocations.
// ---------------------------------------------------------------------------

/// PG `InputFunctionCall`: call a looked-up input function. `str` may be NULL
/// (reading a SQL NULL); a strict function then short-circuits to a NULL result.
/// `None` arg models C's `str == NULL`.
#[allow(deprecated)]
pub fn InputFunctionCall(
    flinfo: &mut FmgrInfo,
    str: &str,
    typioparam: Oid,
    typmod: i32,
) -> Option<Datum> {
    // `&str` cannot express C's NULL; the dedicated NULL path is exercised through
    // InputFunctionCallSafe / OidInputFunctionCall. Here we always have a string.
    let mut fcinfo = local_fcinfo(
        Some(Box::new(clone_flinfo(flinfo))),
        3,
        InvalidOid,
        None,
        None,
    );
    set_arg(&mut fcinfo, 0, CStringGetDatum(str.as_ptr().cast::<i8>()));
    set_arg(&mut fcinfo, 1, ObjectIdGetDatum(typioparam));
    set_arg(&mut fcinfo, 2, Int32GetDatum(typmod));

    let func = fcinfo
        .flinfo
        .as_ref()
        .and_then(|fi| fi.fn_addr)
        .unwrap_or_else(|| panic!("input function flinfo has no installed fn_addr"));
    let result = func(&mut fcinfo);
    // Should get a null result iff str is NULL; here str is always non-null.
    if fcinfo.isnull {
        elog!(ERROR, format!("input function {} returned NULL", flinfo.oid.get()));
    }
    Some(result)
}

/// PG `InputFunctionCallSafe`: soft-error variant; reaches `SOFT_ERROR_OCCURRED`
/// on the not-yet-translated `ErrorSaveContext`.
#[allow(deprecated)]
pub fn InputFunctionCallSafe(
    _flinfo: &mut FmgrInfo,
    _str: &str,
    _typioparam: Oid,
    _typmod: i32,
    _escontext: *mut Node,
) -> Option<Datum> {
    unimplemented!("InputFunctionCallSafe needs ErrorSaveContext (miscnodes)")
}

/// PG `DirectInputFunctionCallSafe`: soft-error variant by direct fn pointer.
#[allow(deprecated)]
pub fn DirectInputFunctionCallSafe(
    _func: PGFunction,
    _str: &str,
    _typioparam: Oid,
    _typmod: i32,
    _escontext: *mut Node,
) -> Option<Datum> {
    unimplemented!("DirectInputFunctionCallSafe needs ErrorSaveContext (miscnodes)")
}

/// PG `OidInputFunctionCall`: fmgr_info() then InputFunctionCall().
#[allow(deprecated)]
pub fn OidInputFunctionCall(
    function_id: Oid,
    str: &str,
    typioparam: Oid,
    typmod: i32,
) -> Option<Datum> {
    let mut flinfo = empty_flinfo();
    fmgr_info(function_id, &mut flinfo);
    InputFunctionCall(&mut flinfo, str, typioparam, typmod)
}

/// PG `OutputFunctionCall`: call a looked-up output function. Little more than
/// window dressing for `FunctionCall1`; the C result is a `char *`, here a
/// `String`.
#[allow(deprecated)]
pub fn OutputFunctionCall(flinfo: &mut FmgrInfo, val: Datum) -> String {
    let datum = FunctionCall1Coll(flinfo, InvalidOid, val)
        .unwrap_or_else(|| panic!("output function returned NULL"));
    cstring_datum_to_string(datum)
}

/// PG `OidOutputFunctionCall`: fmgr_info() then OutputFunctionCall().
#[allow(deprecated)]
pub fn OidOutputFunctionCall(function_id: Oid, val: Datum) -> String {
    let mut flinfo = empty_flinfo();
    fmgr_info(function_id, &mut flinfo);
    OutputFunctionCall(&mut flinfo, val)
}

/// PG `ReceiveFunctionCall`: call a looked-up binary-input function.
#[allow(deprecated)]
pub fn ReceiveFunctionCall(
    _flinfo: &mut FmgrInfo,
    _buf: *mut crate::lib::stringinfo::StringInfo,
    _typioparam: Oid,
    _typmod: i32,
) -> Option<Datum> {
    unimplemented!("ReceiveFunctionCall needs the wire StringInfo path")
}

/// PG `OidReceiveFunctionCall`: fmgr_info() then ReceiveFunctionCall().
#[allow(deprecated)]
pub fn OidReceiveFunctionCall(
    _function_id: Oid,
    _buf: *mut crate::lib::stringinfo::StringInfo,
    _typioparam: Oid,
    _typmod: i32,
) -> Option<Datum> {
    unimplemented!("OidReceiveFunctionCall needs the wire StringInfo path")
}

/// PG `SendFunctionCall`: call a looked-up binary-output function; the freshly
/// allocated bytea is owned by the caller (`Box`). `DatumGetByteaP` (the
/// non-toasted-result guarantee) is part of the deferred varlena/detoast path.
#[allow(deprecated)]
pub fn SendFunctionCall(_flinfo: &mut FmgrInfo, _val: Datum) -> Box<bytea> {
    unimplemented!("SendFunctionCall needs DatumGetByteaP (varlena/detoast) boxing")
}

/// PG `OidSendFunctionCall`: fmgr_info() then SendFunctionCall().
#[allow(deprecated)]
pub fn OidSendFunctionCall(function_id: Oid, val: Datum) -> Box<bytea> {
    let mut flinfo = empty_flinfo();
    fmgr_info(function_id, &mut flinfo);
    SendFunctionCall(&mut flinfo, val)
}

// Non-Coll convenience wrappers (collation defaults to InvalidOid) live in the
// header (fmgr.h), delegating to the *Coll forms above.

/// Read a C-string Datum into an owned `String` (output-function result).
fn cstring_datum_to_string(datum: Datum) -> String {
    let p = DatumGetCString(datum);
    // SAFETY: an output function returns a palloc'd NUL-terminated C string.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

// ---------------------------------------------------------------------------
// Support routines for toastable datatypes.
// ---------------------------------------------------------------------------

/// PG `pg_detoast_datum`: detoast an extended varlena, else return it as-is.
#[allow(deprecated)]
pub fn pg_detoast_datum(datum: *mut varlena) -> *mut varlena {
    if varatt_is_extended(datum) {
        detoast_attr(datum)
    } else {
        datum
    }
}

/// PG `pg_detoast_datum_copy`: like [`pg_detoast_datum`] but always a fresh copy.
#[allow(deprecated)]
pub fn pg_detoast_datum_copy(datum: *mut varlena) -> *mut varlena {
    if varatt_is_extended(datum) {
        detoast_attr(datum)
    } else {
        // Make a modifiable copy of the (un-toasted) varlena.
        unimplemented!("pg_detoast_datum_copy varlena copy needs VARSIZE/palloc")
    }
}

/// PG `pg_detoast_datum_slice`: fetch only `[first, first+count)` from the toast.
#[allow(deprecated)]
pub fn pg_detoast_datum_slice(datum: *mut varlena, first: i32, count: i32) -> *mut varlena {
    detoast_attr_slice(datum, first, count)
}

/// PG `pg_detoast_datum_packed`: detoast only if compressed/external.
#[allow(deprecated)]
pub fn pg_detoast_datum_packed(datum: *mut varlena) -> *mut varlena {
    if varatt_is_compressed(datum) || varatt_is_external(datum) {
        detoast_attr(datum)
    } else {
        datum
    }
}

// detoast.c + varatt.h are not translated yet; reach their stubs.
fn varatt_is_extended(_datum: *mut varlena) -> bool {
    unimplemented!("VARATT_IS_EXTENDED (varatt) deferred")
}
fn varatt_is_compressed(_datum: *mut varlena) -> bool {
    unimplemented!("VARATT_IS_COMPRESSED (varatt) deferred")
}
fn varatt_is_external(_datum: *mut varlena) -> bool {
    unimplemented!("VARATT_IS_EXTERNAL (varatt) deferred")
}
fn detoast_attr(_datum: *mut varlena) -> *mut varlena {
    unimplemented!("detoast_attr (access/detoast) deferred")
}
fn detoast_attr_slice(_datum: *mut varlena, _first: i32, _count: i32) -> *mut varlena {
    unimplemented!("detoast_attr_slice (access/detoast) deferred")
}

// ---------------------------------------------------------------------------
// Support routines for extracting info from the fn_expr parse tree.
// These reach exprType / the IsA node dispatch (nodeFuncs / primnodes), deferred.
// ---------------------------------------------------------------------------

/// PG `get_fn_expr_rettype`: actual return-type OID, or `InvalidOid`.
#[allow(deprecated)]
pub fn get_fn_expr_rettype(flinfo: &mut FmgrInfo) -> Oid {
    let Some(expr) = flinfo.expr.as_deref() else {
        return InvalidOid;
    };
    expr_type(expr)
}

/// PG `get_fn_expr_argtype`: actual type OID of argument `argnum` (from 0).
#[allow(deprecated)]
pub fn get_fn_expr_argtype(flinfo: &mut FmgrInfo, argnum: i32) -> Oid {
    let Some(expr) = flinfo.expr.as_deref() else {
        return InvalidOid;
    };
    get_call_expr_argtype(expr, argnum)
}

/// PG `get_call_expr_argtype`: like above but from the calling expression tree.
pub fn get_call_expr_argtype(_expr: &Node, _argnum: i32) -> Oid {
    unimplemented!("get_call_expr_argtype needs node IsA dispatch + exprType")
}

/// PG `get_fn_expr_arg_stable`: whether arg `argnum` is constant for the query.
#[allow(deprecated)]
pub fn get_fn_expr_arg_stable(flinfo: &mut FmgrInfo, argnum: i32) -> bool {
    let Some(expr) = flinfo.expr.as_deref() else {
        return false;
    };
    get_call_expr_arg_stable(expr, argnum)
}

/// PG `get_call_expr_arg_stable`: as above, from the calling expression tree.
pub fn get_call_expr_arg_stable(_expr: &Node, _argnum: i32) -> bool {
    unimplemented!("get_call_expr_arg_stable needs node IsA dispatch")
}

/// PG `get_fn_expr_variadic`: the VARIADIC flag from the call, or false.
#[allow(deprecated)]
pub fn get_fn_expr_variadic(flinfo: &mut FmgrInfo) -> bool {
    if flinfo.expr.is_none() {
        return false;
    }
    unimplemented!("get_fn_expr_variadic needs FuncExpr IsA dispatch")
}

/// PG `set_fn_opclass_options`: stash opclass options as a bytea Const in fn_expr.
#[allow(deprecated)]
pub fn set_fn_opclass_options(_flinfo: &mut FmgrInfo, _options: *mut bytea) {
    unimplemented!("set_fn_opclass_options needs makeConst")
}

/// PG `has_fn_opclass_options`: whether opclass options are present.
#[allow(deprecated)]
pub fn has_fn_opclass_options(flinfo: &mut FmgrInfo) -> bool {
    if flinfo.expr.is_none() {
        return false;
    }
    unimplemented!("has_fn_opclass_options needs Const IsA dispatch")
}

/// PG `get_fn_opclass_options`: the cached opclass-options bytea, or ERROR.
#[allow(deprecated)]
pub fn get_fn_opclass_options(flinfo: &mut FmgrInfo) -> *mut bytea {
    if flinfo.expr.is_some() {
        unimplemented!("get_fn_opclass_options needs Const IsA dispatch")
    }
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("operator class options info is absent in function call context");
    });
    unreachable!()
}

/// Tree introspection leaf: PG `exprType(Node*)` (nodeFuncs), deferred.
fn expr_type(_expr: &Node) -> Oid {
    unimplemented!("exprType (nodeFuncs) deferred")
}

// ---------------------------------------------------------------------------
// Support routines for procedural-language implementations.
// ---------------------------------------------------------------------------

/// PG `CheckFunctionValidatorAccess`: verify a validator matches the function's
/// language and the user may use both. Reaches syscache + acl, deferred.
pub fn CheckFunctionValidatorAccess(_validator_oid: Oid, _function_oid: Oid) -> bool {
    let _ = (ERRCODE_UNDEFINED_FUNCTION,);
    unimplemented!("CheckFunctionValidatorAccess needs syscache + acl")
}

// CurrentMemoryContext is tombstoned (Rust ownership). fmgr.c only stores it
// into FmgrInfo.fn_mcxt for a later palloc that no longer exists, so the stored
// context is a null handle until the memory model needs it. (deleted by
// redesign: the live process-global allocator pointer.)
fn current_memory_context() -> MemoryContext {
    core::ptr::null_mut()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `fmgr_isbuiltin` over the generated table: a known builtin OID resolves to
    /// an entry; an out-of-range OID and an unassigned in-range OID return None.
    #[test]
    fn isbuiltin_resolves_and_misses() {
        // The generated table is non-empty (pg_proc.dat has thousands of
        // internal-language builtins).
        assert_eq!(fmgr_builtins.len(), fmgr_nbuiltins);

        // Pick a real builtin OID from the table and look it up by OID.
        let sample = &fmgr_builtins[0];
        let got = fmgr_isbuiltin(sample.foid).expect("sample builtin OID resolves");
        assert_eq!(got.foid, sample.foid);
        assert_eq!(got.func_name, sample.func_name);

        // Out of range: above the last builtin OID -> None.
        assert!(fmgr_isbuiltin(Oid::new(fmgr_last_builtin_oid.get() + 1)).is_none());
        // In range but unassigned (OID 0 is never a builtin) -> None.
        assert!(fmgr_isbuiltin(Oid::new(0)).is_none());
    }

    /// `fmgr_lookup_by_name` finds the same routine `fmgr_isbuiltin` does, and
    /// `fmgr_internal_function` exposes the OID (Option fold of C's InvalidOid).
    #[test]
    fn lookup_by_name_roundtrip() {
        let sample = &fmgr_builtins[0];
        let by_name = fmgr_lookup_by_name(sample.func_name).expect("name resolves");
        assert_eq!(by_name.foid, sample.foid);

        assert_eq!(fmgr_internal_function(sample.func_name), Some(sample.foid));
        assert_eq!(fmgr_internal_function("\0not a real prosrc name\0"), None);
    }

    /// `fmgr_info` on a builtin OID fills the fast-path fields from the table.
    /// For step 01 every row's `func` is None, so `fn_addr` is None too.
    #[test]
    #[allow(deprecated)]
    fn fmgr_info_builtin_fast_path() {
        let sample = &fmgr_builtins[0];
        let mut finfo = empty_flinfo();
        fmgr_info(sample.foid, &mut finfo);

        assert_eq!(finfo.oid, sample.foid);
        assert_eq!(finfo.nargs, sample.nargs);
        assert_eq!(finfo.strict, sample.strict);
        assert_eq!(finfo.retset, sample.retset);
        assert_eq!(finfo.stats, TRACK_FUNC_ALL);
        // Step 01: no builtins bound yet.
        assert_eq!(finfo.fn_addr.is_some(), sample.func.is_some());
    }

    /// FunctionCallInfo arg get/set roundtrip through the fcinfo helpers.
    #[test]
    fn fcinfo_arg_roundtrip() {
        let mut fcinfo = local_fcinfo(None, 2, InvalidOid, None, None);
        assert_eq!(fcinfo.nargs, 2);
        assert!(fcinfo.args[0].isnull);

        set_arg(&mut fcinfo, 0, Datum(42));
        set_arg(&mut fcinfo, 1, Datum(99));
        assert_eq!(fcinfo.args[0].value, Datum(42));
        assert!(!fcinfo.args[0].isnull);
        assert_eq!(fcinfo.args[1].value, Datum(99));
        assert!(!fcinfo.args[1].isnull);
    }

    /// A bound builtin (synthesized here) invokes through DirectFunctionCall1Coll.
    #[test]
    fn direct_call_invokes_bound_fn() {
        fn double_it(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            Datum(fcinfo.args[0].value.0 * 2)
        }
        let out = DirectFunctionCall1Coll(double_it, InvalidOid, Datum(21));
        assert_eq!(out, Some(Datum(42)));
    }
}

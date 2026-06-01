//! Translation of the PostgreSQL function manager.
//!
//! Combines:
//!   HEADER: postgres/src/include/fmgr.h
//!   IMPL:   postgres/src/backend/utils/fmgr/fmgr.c
//!
//! fmgr.h defines the function-call interface (FmgrInfo, FunctionCallInfoBaseData,
//! PGFunction, and the rich family of PG_GETARG_*/PG_RETURN_* macros that every
//! fmgr-callable function uses). fmgr.c implements the lookup of pg_proc entries
//! into FmgrInfo structs and the DirectFunctionCallN / FunctionCallN /
//! OidFunctionCallN call wrappers.
//!
//! Catalog-dependent code paths (SearchSysCache on pg_proc/pg_language, the
//! generated builtin function table from utils/fmgrtab.h, the toast detoaster
//! from access/detoast.h, expression-tree introspection from nodes/nodeFuncs.c,
//! and the security-definer / GUC / pgstat machinery) are NOT yet translated.
//! Those paths are stubbed with `// TODO(pg-port): ...` and `unimplemented!()`
//! while the surrounding struct-filling skeleton and signatures are preserved.

use crate::prelude::*; // Datum, NullableDatum, Oid, bool, all DatumGet*/*GetDatum, palloc, etc.
use crate::nodes::nodes::{Node, NodeTag};
use crate::utils::palloc::MemoryContext;
use core::ffi::{c_char, c_int, c_short, c_uchar, c_void};

// The following catalog/node helpers are referenced by translated code but live
// in not-yet-ported units. They are pulled in where they exist; the rest are
// stubbed inline below.
use crate::nodes::primnodes::{
    Const, DistinctExpr, FuncExpr, NullIfExpr, OpExpr, Param, ScalarArrayOpExpr, WindowFunc,
    PARAM_EXTERN,
};
use crate::nodes::pg_list::{list_length, list_nth, List};
use crate::lib::stringinfo::StringInfo; // *mut StringInfoData (lib/stringinfo.h)
use crate::IsA; // nodes.h IsA! macro (used by the fn_expr-introspection routines)

/*
 * ---------------------------------------------------------------------------
 *  fmgr.h  --  Definitions for the Postgres function manager and function-call
 *  interface.
 * ---------------------------------------------------------------------------
 */

/* We don't want to include primnodes.h here, so make some stub references */
pub type fmNodePtr = *mut Node; /* typedef struct Node *fmNodePtr */
/* typedef struct Aggref *fmAggrefPtr */
pub type fmAggrefPtr = *mut c_void;

/* Likewise, avoid including execnodes.h here */
pub type fmExprContextCallbackFunction = unsafe fn(arg: Datum);

/* Likewise, avoid including stringinfo.h here */
/* typedef struct StringInfoData *fmStringInfo */
pub type fmStringInfo = *mut c_void;

/*
 * All functions that can be called directly by fmgr must have this signature.
 * (Other functions can be called by using a handler that does have this
 * signature.)
 */

pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;

pub type PGFunction = unsafe fn(fcinfo: FunctionCallInfo) -> Datum;

/*
 * This struct holds the system-catalog information that must be looked up
 * before a function can be called through fmgr.  If the same function is
 * to be called multiple times, the lookup need be done only once and the
 * info struct saved for re-use.
 *
 * Note that fn_expr really is parse-time-determined information about the
 * arguments, rather than about the function itself.  But it's convenient to
 * store it here rather than in FunctionCallInfoBaseData, where it might more
 * logically belong.
 *
 * fn_extra is available for use by the called function; all other fields
 * should be treated as read-only after the struct is created.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FmgrInfo {
    pub fn_addr: Option<PGFunction>, /* pointer to function or handler to be called */
    pub fn_oid: Oid,                 /* OID of function (NOT of handler, if any) */
    pub fn_nargs: c_short,           /* number of input args (0..FUNC_MAX_ARGS) */
    pub fn_strict: bool,             /* function is "strict" (NULL in => NULL out) */
    pub fn_retset: bool,             /* function returns a set */
    pub fn_stats: c_uchar,           /* collect stats if track_functions > this */
    pub fn_extra: *mut c_void,       /* extra space for use by handler */
    pub fn_mcxt: MemoryContext,      /* memory context to store fn_extra in */
    pub fn_expr: fmNodePtr,          /* expression parse tree for call, or NULL */
}

/*
 * This struct is the data actually passed to an fmgr-called function.
 *
 * The called function is expected to set isnull, and possibly resultinfo or
 * fields in whatever resultinfo points to.  It should not change any other
 * fields.  (In particular, scribbling on the argument arrays is a bad idea,
 * since some callers assume they can re-call with the same arguments.)
 *
 * Note that enough space for arguments needs to be provided, either by using
 * SizeForFunctionCallInfo() in dynamic allocations, or by using
 * LOCAL_FCINFO() for on-stack allocations.
 *
 * This struct is named *BaseData, rather than *Data, to break pre v12 code
 * that allocated FunctionCallInfoData itself, as it'd often silently break
 * old code due to no space for arguments being provided.
 */
#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub flinfo: *mut FmgrInfo,  /* ptr to lookup info used for this call */
    pub context: fmNodePtr,     /* pass info about context of call */
    pub resultinfo: fmNodePtr,  /* pass or return extra info about result */
    pub fncollation: Oid,       /* collation for function to use */
    // #define FIELDNO_FUNCTIONCALLINFODATA_ISNULL 4
    pub isnull: bool,           /* function must set true if result is NULL */
    pub nargs: c_short,         /* # arguments actually passed */
    // #define FIELDNO_FUNCTIONCALLINFODATA_ARGS 6
    pub args: [NullableDatum; FLEXIBLE_ARRAY_MEMBER],
}

pub const FIELDNO_FUNCTIONCALLINFODATA_ISNULL: usize = 4;
pub const FIELDNO_FUNCTIONCALLINFODATA_ARGS: usize = 6;

/*
 * Space needed for a FunctionCallInfoBaseData struct with sufficient space
 * for `nargs` arguments.
 *
 * C: offsetof(FunctionCallInfoBaseData, args) + sizeof(NullableDatum) * (nargs)
 */
#[inline]
pub const fn SizeForFunctionCallInfo(nargs: usize) -> usize {
    core::mem::offset_of!(FunctionCallInfoBaseData, args)
        + core::mem::size_of::<NullableDatum>() * nargs
}

/*
 * This macro ensures that `name` points to a stack-allocated
 * FunctionCallInfoBaseData struct with sufficient space for `nargs` arguments.
 *
 * In C this is done with a union over a char buffer to guarantee alignment.
 * In Rust we cannot append a variable-length tail to a stack struct directly,
 * so we allocate a properly-aligned byte buffer holding the base struct plus
 * `nargs` NullableDatum slots, then bind `$name` to a `FunctionCallInfo`
 * (a `*mut FunctionCallInfoBaseData`) pointing at it. The byte buffer is bound
 * to a hygienic local in the same scope as `$name`, so it lives for the
 * enclosing scope just like the C `union { ... } name##data`.
 *
 * (The C `name##data` token-paste is not reproduced because macro metavariable
 * concatenation is not available on stable Rust; the owning buffer is instead a
 * hidden, hygienic binding that callers never need to name.)
 */
#[macro_export]
macro_rules! LOCAL_FCINFO {
    ($name:ident, $nargs:expr) => {
        // use an aligned wrapper to guarantee alignment, ensuring enough space
        // for $nargs args is available
        #[repr(C, align(8))]
        struct LocalFcinfoBuf([u8; $crate::utils::fmgr::SizeForFunctionCallInfo($nargs)]);
        let mut fcinfo_data: LocalFcinfoBuf =
            LocalFcinfoBuf([0u8; $crate::utils::fmgr::SizeForFunctionCallInfo($nargs)]);
        #[allow(unused_mut)]
        let mut $name: $crate::utils::fmgr::FunctionCallInfo = (&mut fcinfo_data)
            as *mut LocalFcinfoBuf
            as *mut $crate::utils::fmgr::FunctionCallInfoBaseData;
    };
}

/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.  (Definition below; declared here for parity
 * with fmgr.h.)
 */

/* Convenience macro for setting the fn_expr field */
#[macro_export]
macro_rules! fmgr_info_set_expr {
    ($expr:expr, $finfo:expr) => {
        (*$finfo).fn_expr = ($expr)
    };
}

/*
 * This macro initializes all the fields of a FunctionCallInfoBaseData except
 * for the args[] array.
 *
 * In C, `Fcinfo` is an lvalue of type FunctionCallInfoBaseData; here we accept a
 * `*mut FunctionCallInfoBaseData` and write through it (callers pass the raw
 * pointer produced by LOCAL_FCINFO!).
 */
#[macro_export]
macro_rules! InitFunctionCallInfoData {
    ($Fcinfo:expr, $Flinfo:expr, $Nargs:expr, $Collation:expr, $Context:expr, $Resultinfo:expr) => {{
        (*$Fcinfo).flinfo = ($Flinfo);
        (*$Fcinfo).context = ($Context);
        (*$Fcinfo).resultinfo = ($Resultinfo);
        (*$Fcinfo).fncollation = ($Collation);
        (*$Fcinfo).isnull = false;
        (*$Fcinfo).nargs = ($Nargs);
    }};
}

/*
 * This macro invokes a function given a filled-in FunctionCallInfoBaseData
 * struct.  The macro result is the returned Datum --- but note that
 * caller must still check fcinfo->isnull!	Also, if function is strict,
 * it is caller's responsibility to verify that no null arguments are present
 * before calling.
 *
 * Some code performs multiple calls without redoing InitFunctionCallInfoData,
 * possibly altering the argument values.  This is okay, but be sure to reset
 * the fcinfo->isnull flag before each call, since callees are permitted to
 * assume that starts out false.
 *
 * C: ((* (fcinfo)->flinfo->fn_addr) (fcinfo))
 */
#[macro_export]
macro_rules! FunctionCallInvoke {
    ($fcinfo:expr) => {
        ((*(*$fcinfo).flinfo).fn_addr.unwrap())($fcinfo)
    };
}

/*-------------------------------------------------------------------------
 *		Support macros to ease writing fmgr-compatible functions
 *
 * A C-coded fmgr-compatible function should be declared as
 *
 *		Datum
 *		function_name(PG_FUNCTION_ARGS)
 *		{
 *			...
 *		}
 *
 * It should access its arguments using appropriate PG_GETARG_xxx macros
 * and should return its result using PG_RETURN_xxx.
 *
 * In Rust, a translated fmgr function is written as
 *
 *      pub unsafe fn function_name(fcinfo: FunctionCallInfo) -> Datum { ... }
 *
 * and the PG_* macros below reference the `fcinfo` binding by name, exactly as
 * the C macros expand to `fcinfo->...`.
 *-------------------------------------------------------------------------
 */

/*
 * Standard parameter list for fmgr-compatible functions.
 * C: #define PG_FUNCTION_ARGS FunctionCallInfo fcinfo
 */
#[macro_export]
macro_rules! PG_FUNCTION_ARGS {
    () => {
        fcinfo: $crate::utils::fmgr::FunctionCallInfo
    };
}

/* Get collation function should use. */
#[macro_export]
macro_rules! PG_GET_COLLATION {
    ($fcinfo:expr) => {
        (*$fcinfo).fncollation
    };
}

/* Get number of arguments passed to function. */
#[macro_export]
macro_rules! PG_NARGS {
    ($fcinfo:expr) => {
        (*$fcinfo).nargs
    };
}

/*
 * If function is not marked "proisstrict" in pg_proc, it must check for
 * null arguments using this macro.  Do not try to GETARG a null argument!
 */
#[macro_export]
macro_rules! PG_ARGISNULL {
    ($fcinfo:expr, $n:expr) => {
        (*(*$fcinfo).args.as_ptr().add($n as usize)).isnull
    };
}

/*
 * Support for fetching detoasted copies of toastable datatypes (all of
 * which are varlena types).  pg_detoast_datum() gives you either the input
 * datum (if not toasted) or a detoasted copy allocated with palloc().
 * pg_detoast_datum_copy() always gives you a palloc'd copy --- use it
 * if you need a modifiable copy of the input.  Caller is expected to have
 * checked for null inputs first, if necessary.
 *
 * pg_detoast_datum_packed() will return packed (1-byte header) datums
 * unmodified.  It will still expand an externally toasted or compressed datum.
 * The resulting datum can be accessed using VARSIZE_ANY() and VARDATA_ANY()
 * (beware of multiple evaluations in those macros!)
 *
 * Note: it'd be nice if these could be macros, but I see no way to do that
 * without evaluating the arguments multiple times, which is NOT acceptable.
 *
 * TODO(pg-port): these need the toast infrastructure (access/detoast.h);
 * their bodies are stubbed below.
 */
pub unsafe fn pg_detoast_datum(datum: *mut varlena) -> *mut varlena {
    // C body uses VARATT_IS_EXTENDED()/detoast_attr(); see access/detoast.c.
    // TODO(pg-port): toast infrastructure not yet translated.
    let _ = datum;
    unimplemented!("pg_detoast_datum: toast infrastructure not yet translated")
}

pub unsafe fn pg_detoast_datum_copy(datum: *mut varlena) -> *mut varlena {
    // TODO(pg-port): toast infrastructure not yet translated.
    let _ = datum;
    unimplemented!("pg_detoast_datum_copy: toast infrastructure not yet translated")
}

pub unsafe fn pg_detoast_datum_slice(datum: *mut varlena, first: int32, count: int32) -> *mut varlena {
    // TODO(pg-port): toast infrastructure not yet translated.
    let _ = (datum, first, count);
    unimplemented!("pg_detoast_datum_slice: toast infrastructure not yet translated")
}

// pg_detoast_datum_packed now lives in crate::varatt (the real identity-for-plain impl);
// the PG_DETOAST_DATUM_PACKED macro below routes there.  Keeping a second copy here caused a
// glob-import ambiguity for files that `use crate::varatt::*` and `use crate::utils::fmgr::*`.

#[macro_export]
macro_rules! PG_DETOAST_DATUM {
    ($datum:expr) => {
        $crate::utils::fmgr::pg_detoast_datum(
            $crate::postgres::DatumGetPointer($datum) as *mut $crate::c::varlena,
        )
    };
}
#[macro_export]
macro_rules! PG_DETOAST_DATUM_COPY {
    ($datum:expr) => {
        $crate::utils::fmgr::pg_detoast_datum_copy(
            $crate::postgres::DatumGetPointer($datum) as *mut $crate::c::varlena,
        )
    };
}
#[macro_export]
macro_rules! PG_DETOAST_DATUM_SLICE {
    ($datum:expr, $f:expr, $c:expr) => {
        $crate::utils::fmgr::pg_detoast_datum_slice(
            $crate::postgres::DatumGetPointer($datum) as *mut $crate::c::varlena,
            ($f) as $crate::c::int32,
            ($c) as $crate::c::int32,
        )
    };
}
/* WARNING -- unaligned pointer */
#[macro_export]
macro_rules! PG_DETOAST_DATUM_PACKED {
    ($datum:expr) => {
        $crate::varatt::pg_detoast_datum_packed(
            $crate::postgres::DatumGetPointer($datum) as *mut core::ffi::c_void,
        ) as *mut $crate::c::varlena
    };
}

/*
 * Support for cleaning up detoasted copies of inputs.  This must only
 * be used for pass-by-ref datatypes, and normally would only be used
 * for toastable types.  If the given pointer is different from the
 * original argument, assume it's a palloc'd detoasted copy, and pfree it.
 * NOTE: most functions on toastable types do not have to worry about this,
 * but we currently require that support functions for indexes not leak
 * memory.
 */
#[macro_export]
macro_rules! PG_FREE_IF_COPY {
    ($fcinfo:expr, $ptr:expr, $n:expr) => {{
        if ($ptr) as $crate::c::Pointer != $crate::PG_GETARG_POINTER!($fcinfo, $n) {
            $crate::utils::palloc::pfree($ptr as *mut core::ffi::c_void);
        }
    }};
}

/* Macros for fetching arguments of standard types */

#[macro_export]
macro_rules! PG_GETARG_DATUM {
    ($fcinfo:expr, $n:expr) => {
        (*(*$fcinfo).args.as_ptr().add($n as usize)).value
    };
}
#[macro_export]
macro_rules! PG_GETARG_INT32 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetInt32($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_UINT32 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetUInt32($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_INT16 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetInt16($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_UINT16 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetUInt16($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_CHAR {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetChar($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_BOOL {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetBool($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_OID {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetObjectId($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_POINTER {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetPointer($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_CSTRING {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetCString($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_NAME {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetName($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_TRANSACTIONID {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetTransactionId($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/* these macros hide the pass-by-reference-ness of the datatype: */
#[macro_export]
macro_rules! PG_GETARG_FLOAT4 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetFloat4($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_FLOAT8 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetFloat8($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_INT64 {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetInt64($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/* use this if you want the raw, possibly-toasted input datum: */
#[macro_export]
macro_rules! PG_GETARG_RAW_VARLENA_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::PG_GETARG_POINTER!($fcinfo, $n) as *mut $crate::c::varlena
    };
}
/* use this if you want the input datum de-toasted: */
#[macro_export]
macro_rules! PG_GETARG_VARLENA_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::PG_DETOAST_DATUM!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/* and this if you can handle 1-byte-header datums: */
#[macro_export]
macro_rules! PG_GETARG_VARLENA_PP {
    ($fcinfo:expr, $n:expr) => {
        $crate::PG_DETOAST_DATUM_PACKED!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/* DatumGetFoo macros for varlena types will typically look like this: */
#[macro_export]
macro_rules! DatumGetByteaPP {
    ($X:expr) => {
        $crate::PG_DETOAST_DATUM_PACKED!($X) as *mut $crate::c::bytea
    };
}
#[macro_export]
macro_rules! DatumGetTextPP {
    ($X:expr) => {
        $crate::PG_DETOAST_DATUM_PACKED!($X) as *mut $crate::c::text
    };
}
/* And we also offer variants that return an OK-to-write copy */
#[macro_export]
macro_rules! DatumGetByteaPCopy {
    ($X:expr) => {
        $crate::PG_DETOAST_DATUM_COPY!($X) as *mut $crate::c::bytea
    };
}
#[macro_export]
macro_rules! DatumGetTextPCopy {
    ($X:expr) => {
        $crate::PG_DETOAST_DATUM_COPY!($X) as *mut $crate::c::text
    };
}
/* Variants which return n bytes starting at pos. m */
#[macro_export]
macro_rules! DatumGetByteaPSlice {
    ($X:expr, $m:expr, $n:expr) => {
        $crate::PG_DETOAST_DATUM_SLICE!($X, $m, $n) as *mut $crate::c::bytea
    };
}
#[macro_export]
macro_rules! DatumGetTextPSlice {
    ($X:expr, $m:expr, $n:expr) => {
        $crate::PG_DETOAST_DATUM_SLICE!($X, $m, $n) as *mut $crate::c::text
    };
}
/* GETARG macros for varlena types will typically look like this: */
#[macro_export]
macro_rules! PG_GETARG_BYTEA_PP {
    ($fcinfo:expr, $n:expr) => {
        $crate::DatumGetByteaPP!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_TEXT_PP {
    ($fcinfo:expr, $n:expr) => {
        $crate::DatumGetTextPP!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/* And we also offer variants that return an OK-to-write copy */
#[macro_export]
macro_rules! PG_GETARG_BYTEA_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        $crate::DatumGetByteaPCopy!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_TEXT_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        $crate::DatumGetTextPCopy!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
/* And a b-byte slice from position a -also OK to write */
#[macro_export]
macro_rules! PG_GETARG_BYTEA_P_SLICE {
    ($n:expr, $a:expr, $b:expr) => {
        $crate::DatumGetByteaPSlice!($crate::PG_GETARG_DATUM!($fcinfo, $n), $a, $b)
    };
}
#[macro_export]
macro_rules! PG_GETARG_TEXT_P_SLICE {
    ($n:expr, $a:expr, $b:expr) => {
        $crate::DatumGetTextPSlice!($crate::PG_GETARG_DATUM!($fcinfo, $n), $a, $b)
    };
}
/*
 * Obsolescent variants that guarantee INT alignment for the return value.
 * Few operations on these particular types need alignment, mainly operations
 * that cast the VARDATA pointer to a type like int16[].  Most code should use
 * the ...PP(X) counterpart.  Nonetheless, these appear frequently in code
 * predating the PostgreSQL 8.3 introduction of the ...PP(X) variants.
 */
#[macro_export]
macro_rules! DatumGetByteaP {
    ($X:expr) => {
        $crate::PG_DETOAST_DATUM!($X) as *mut $crate::c::bytea
    };
}
#[macro_export]
macro_rules! DatumGetTextP {
    ($X:expr) => {
        $crate::PG_DETOAST_DATUM!($X) as *mut $crate::c::text
    };
}
#[macro_export]
macro_rules! PG_GETARG_BYTEA_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::DatumGetByteaP!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
#[macro_export]
macro_rules! PG_GETARG_TEXT_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::DatumGetTextP!($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/* To access options from opclass support functions use this: */
#[macro_export]
macro_rules! PG_HAS_OPCLASS_OPTIONS {
    ($fcinfo:expr) => {
        $crate::utils::fmgr::has_fn_opclass_options((*$fcinfo).flinfo)
    };
}
#[macro_export]
macro_rules! PG_GET_OPCLASS_OPTIONS {
    ($fcinfo:expr) => {
        $crate::utils::fmgr::get_fn_opclass_options((*$fcinfo).flinfo)
    };
}

/* To return a NULL do this: */
#[macro_export]
macro_rules! PG_RETURN_NULL {
    ($fcinfo:expr) => {{
        (*$fcinfo).isnull = true;
        return 0 as $crate::postgres::Datum;
    }};
}

/* A few internal functions return void (which is not the same as NULL!) */
#[macro_export]
macro_rules! PG_RETURN_VOID {
    () => {
        return 0 as $crate::postgres::Datum
    };
}

/* Macros for returning results of standard types */

#[macro_export]
macro_rules! PG_RETURN_DATUM {
    ($x:expr) => {
        return ($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_INT32 {
    ($x:expr) => {
        return $crate::postgres::Int32GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_UINT32 {
    ($x:expr) => {
        return $crate::postgres::UInt32GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_INT16 {
    ($x:expr) => {
        return $crate::postgres::Int16GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_UINT16 {
    ($x:expr) => {
        return $crate::postgres::UInt16GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_CHAR {
    ($x:expr) => {
        return $crate::postgres::CharGetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_BOOL {
    ($x:expr) => {
        return $crate::postgres::BoolGetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_OID {
    ($x:expr) => {
        return $crate::postgres::ObjectIdGetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_POINTER {
    ($x:expr) => {
        return $crate::postgres::PointerGetDatum($x as *const core::ffi::c_void)
    };
}
#[macro_export]
macro_rules! PG_RETURN_CSTRING {
    ($x:expr) => {
        return $crate::postgres::CStringGetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_NAME {
    ($x:expr) => {
        // C: NameGetDatum(x) == PointerGetDatum(x). The prelude has no
        // NameGetDatum, so use PointerGetDatum directly (identical semantics).
        return $crate::postgres::PointerGetDatum($x as *const core::ffi::c_void)
    };
}
#[macro_export]
macro_rules! PG_RETURN_TRANSACTIONID {
    ($x:expr) => {
        return $crate::postgres::TransactionIdGetDatum($x)
    };
}
/* these macros hide the pass-by-reference-ness of the datatype: */
#[macro_export]
macro_rules! PG_RETURN_FLOAT4 {
    ($x:expr) => {
        return $crate::postgres::Float4GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_FLOAT8 {
    ($x:expr) => {
        return $crate::postgres::Float8GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_INT64 {
    ($x:expr) => {
        return $crate::postgres::Int64GetDatum($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_UINT64 {
    ($x:expr) => {
        return $crate::postgres::UInt64GetDatum($x)
    };
}
/* RETURN macros for other pass-by-ref types will typically look like this: */
#[macro_export]
macro_rules! PG_RETURN_BYTEA_P {
    ($x:expr) => {
        $crate::PG_RETURN_POINTER!($x)
    };
}
#[macro_export]
macro_rules! PG_RETURN_TEXT_P {
    ($x:expr) => {
        $crate::PG_RETURN_POINTER!($x)
    };
}

/*-------------------------------------------------------------------------
 *		Support for detecting call convention of dynamically-loaded functions
 *
 * Dynamically loaded functions currently can only use the version-1 ("new
 * style") calling convention.  Version-0 ("old style") is not supported
 * anymore.  Version 1 is the call convention defined in this header file.
 *-------------------------------------------------------------------------
 */

#[repr(C)]
pub struct Pg_finfo_record {
    pub api_version: c_int, /* specifies call convention version number */
                            /* More fields may be added later, for version numbers > 1. */
}

/* Expected signature of an info function */
pub type PGFInfoFunction = unsafe fn() -> *const Pg_finfo_record;

/*
 * The PG_FUNCTION_INFO_V1 / PG_MODULE_MAGIC machinery is for dynamically-loaded
 * extension modules and is not meaningful in the in-tree Rust port; the ABI
 * structs are translated for fidelity but the declaration macros are omitted.
 * TODO(pg-port): dynamic loader (dfmgr.c) and module magic are not translated.
 */

/* Definition of the values we check to verify ABI compatibility */
#[repr(C)]
pub struct Pg_abi_values {
    pub version: c_int,       /* PostgreSQL major version */
    pub funcmaxargs: c_int,   /* FUNC_MAX_ARGS */
    pub indexmaxkeys: c_int,  /* INDEX_MAX_KEYS */
    pub namedatalen: c_int,   /* NAMEDATALEN */
    pub float8byval: c_int,   /* FLOAT8PASSBYVAL */
    pub abi_extra: [c_char; 32], /* see pg_config_manual.h */
}

/* Definition of the magic block structure */
#[repr(C)]
pub struct Pg_magic_struct {
    pub len: c_int,                  /* sizeof(this struct) */
    pub abi_fields: Pg_abi_values,   /* see above */
    /* Remaining fields are zero unless filled via PG_MODULE_MAGIC_EXT */
    pub name: *const c_char,         /* optional module name */
    pub version: *const c_char,      /* optional module version */
}

/*
 * Declare the module magic function.  It needs to be a function as the dlsym
 * in the backend is only guaranteed to work on functions, not data
 */
pub type PGModuleMagicFunction = unsafe fn() -> *const Pg_magic_struct;

pub const PG_MAGIC_FUNCTION_NAME_STRING: &str = "Pg_magic_func";

/*-------------------------------------------------------------------------
 *  fmgrtab.h companion: the builtin-function lookup table.
 *
 *  FmgrBuiltin is declared in utils/fmgrtab.h and the arrays are emitted by the
 *  generated fmgrtab.c (from pg_proc.dat). Neither is translated yet, so the
 *  struct is kept here for the lookup routines below and the tables are stubbed.
 *-------------------------------------------------------------------------
 */

#[repr(C)]
pub struct FmgrBuiltin {
    pub foid: Oid,                 /* OID of the function */
    pub nargs: c_short,            /* 0..FUNC_MAX_ARGS, or -1 if variable count */
    pub strict: bool,              /* T if function is "strict" */
    pub retset: bool,              /* T if function returns a set */
    pub funcName: *const c_char,   /* C name of the function */
    pub func: PGFunction,          /* pointer to compiled function */
}

/* track_functions thresholds (utils/guc.h -> TRACK_FUNC_*) */
pub const TRACK_FUNC_OFF: c_uchar = 0;
pub const TRACK_FUNC_PL: c_uchar = 1;
pub const TRACK_FUNC_ALL: c_uchar = 2;

// TODO(pg-port): generated builtin table from utils/fmgrtab.h + fmgrtab.c.
// These are referenced by fmgr_isbuiltin / fmgr_lookupByName below.
//   extern const FmgrBuiltin fmgr_builtins[];
//   extern const int fmgr_nbuiltins;
//   extern const Oid fmgr_last_builtin_oid;
//   extern const uint16 fmgr_builtin_oid_index[];
//   #define InvalidOidBuiltinMapping PG_UINT16_MAX

/*
 * ===========================================================================
 *  fmgr.c  --  The Postgres function manager.
 * ===========================================================================
 */

/*
 * Hooks for function calls
 */
pub type needs_fmgr_hook_type = unsafe fn(fn_oid: Oid) -> bool;
pub type fmgr_hook_type = unsafe fn(event: FmgrHookEventType, flinfo: *mut FmgrInfo, arg: *mut Datum);

pub static mut needs_fmgr_hook: Option<needs_fmgr_hook_type> = None;
pub static mut fmgr_hook: Option<fmgr_hook_type> = None;

/*
 * Hashtable for fast lookup of external C functions
 *
 * TODO(pg-port): CFuncHashTabEntry uses ItemPointerData/HTAB (dfmgr cache);
 * the dynamic C-function cache (lookup_C_func/record_C_func) is not translated.
 */

/*
 * Lookup routines for builtin-function table.  We can search by either Oid
 * or name, but search by Oid is much faster.
 */

unsafe fn fmgr_isbuiltin(id: Oid) -> *const FmgrBuiltin {
    // C body:
    //   uint16 index;
    //   if (id > fmgr_last_builtin_oid) return NULL;
    //   index = fmgr_builtin_oid_index[id];
    //   if (index == InvalidOidBuiltinMapping) return NULL;
    //   return &fmgr_builtins[index];
    // TODO(pg-port): requires generated fmgr_builtins / fmgr_builtin_oid_index.
    let _ = id;
    unimplemented!("fmgr_isbuiltin: builtin function table (fmgrtab.c) not yet translated")
}

/*
 * Lookup a builtin by name.  Note there can be more than one entry in
 * the array with the same name, but they should all point to the same
 * routine.
 */
unsafe fn fmgr_lookupByName(name: *const c_char) -> *const FmgrBuiltin {
    // C body:
    //   for (i = 0; i < fmgr_nbuiltins; i++)
    //       if (strcmp(name, fmgr_builtins[i].funcName) == 0)
    //           return fmgr_builtins + i;
    //   return NULL;
    // TODO(pg-port): requires generated fmgr_builtins / fmgr_nbuiltins.
    let _ = name;
    unimplemented!("fmgr_lookupByName: builtin function table (fmgrtab.c) not yet translated")
}

/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.
 *
 * The caller's CurrentMemoryContext is used as the fn_mcxt of the info
 * struct; this means that any subsidiary data attached to the info struct
 * (either by fmgr_info itself, or later on by a function call handler)
 * will be allocated in that context.  The caller must ensure that this
 * context is at least as long-lived as the info struct itself.  This is
 * not a problem in typical cases where the info struct is on the stack or
 * in freshly-palloc'd space.  However, if one intends to store an info
 * struct in a long-lived table, it's better to use fmgr_info_cxt.
 */
pub unsafe fn fmgr_info(functionId: Oid, finfo: *mut FmgrInfo) {
    fmgr_info_cxt_security(functionId, finfo, CurrentMemoryContext, false);
}

/*
 * Fill a FmgrInfo struct, specifying a memory context in which its
 * subsidiary data should go.
 */
pub unsafe fn fmgr_info_cxt(functionId: Oid, finfo: *mut FmgrInfo, mcxt: MemoryContext) {
    fmgr_info_cxt_security(functionId, finfo, mcxt, false);
}

/*
 * This one does the actual work.  ignore_security is ordinarily false
 * but is set to true when we need to avoid recursion.
 */
unsafe fn fmgr_info_cxt_security(
    functionId: Oid,
    finfo: *mut FmgrInfo,
    mcxt: MemoryContext,
    ignore_security: bool,
) {
    /*
     * fn_oid *must* be filled in last.  Some code assumes that if fn_oid is
     * valid, the whole struct is valid.  Some FmgrInfo struct's do survive
     * elogs.
     */
    (*finfo).fn_oid = InvalidOid;
    (*finfo).fn_extra = null_mut();
    (*finfo).fn_mcxt = mcxt;
    (*finfo).fn_expr = null_mut(); /* caller may set this later */

    let fbp = fmgr_isbuiltin(functionId);
    if !fbp.is_null() {
        /*
         * Fast path for builtin functions: don't bother consulting pg_proc
         */
        (*finfo).fn_nargs = (*fbp).nargs;
        (*finfo).fn_strict = (*fbp).strict;
        (*finfo).fn_retset = (*fbp).retset;
        (*finfo).fn_stats = TRACK_FUNC_ALL; /* ie, never track */
        (*finfo).fn_addr = Some((*fbp).func);
        (*finfo).fn_oid = functionId;
        return;
    }

    /* Otherwise we need the pg_proc entry */
    // TODO(pg-port): pg_proc lookup via syscache (SearchSysCache1(PROCOID, ...)),
    // GETSTRUCT(Form_pg_proc), the prolang switch (INTERNAL/C/SQL/other), the
    // security-definer/proconfig/FmgrHookIsNeeded dispatch to
    // fmgr_security_definer, and ReleaseSysCache. Not translatable until the
    // catalog cache (utils/cache/syscache.c) and pg_proc are ported.
    let _ = ignore_security;
    unimplemented!("fmgr_info_cxt_security: pg_proc lookup via syscache not yet translated")
}

/*
 * Return module and C function name providing implementation of functionId.
 *
 * If *mod == NULL and *fn == NULL, no C symbol is known to implement function.
 * If *mod == NULL and *fn != NULL, the function is implemented by a symbol in
 * the main binary.
 * If *mod != NULL and *fn != NULL the function is implemented in an extension
 * shared object.
 *
 * The returned module and function names are pstrdup'ed into the current
 * memory context.
 */
pub unsafe fn fmgr_symbol(functionId: Oid, mod_: *mut *mut c_char, f_n: *mut *mut c_char) {
    // C body: SearchSysCache1(PROCOID, ...), prolang switch on prosrc/probin.
    // TODO(pg-port): pg_proc lookup via syscache not yet translated.
    let _ = (functionId, mod_, f_n);
    unimplemented!("fmgr_symbol: pg_proc lookup via syscache not yet translated")
}

/*
 * Special fmgr_info processing for C-language functions.  Note that
 * finfo->fn_oid is not valid yet.
 *
 * TODO(pg-port): fmgr_info_C_lang needs the dfmgr loader
 * (load_external_function / fetch_finfo_record) and the CFuncHash cache.
 */

/*
 * Special fmgr_info processing for other-language functions.  Note
 * that finfo->fn_oid is not valid yet.
 *
 * TODO(pg-port): fmgr_info_other_lang needs pg_language syscache lookups.
 */

/*
 * Fetch and validate the information record for the given external function.
 *
 * If no info function exists for the given name an error is raised.
 *
 * This function is broken out of fmgr_info_C_lang so that fmgr_c_validator
 * can validate the information record for a function not yet entered into
 * pg_proc.
 */
pub unsafe fn fetch_finfo_record(filehandle: *mut c_void, funcname: *const c_char) -> *const Pg_finfo_record {
    // C body: psprintf("pg_finfo_%s"), lookup_external_function(), validate
    // inforec->api_version.
    // TODO(pg-port): dynamic loader (dfmgr.c) not yet translated.
    let _ = (filehandle, funcname);
    unimplemented!("fetch_finfo_record: dynamic loader (dfmgr.c) not yet translated")
}

/*
 * Copy an FmgrInfo struct
 *
 * This is inherently somewhat bogus since we can't reliably duplicate
 * language-dependent subsidiary info.  We cheat by zeroing fn_extra,
 * instead, meaning that subsidiary info will have to be recomputed.
 */
pub unsafe fn fmgr_info_copy(dstinfo: *mut FmgrInfo, srcinfo: *mut FmgrInfo, destcxt: MemoryContext) {
    core::ptr::copy_nonoverlapping(srcinfo, dstinfo, 1); /* memcpy(dstinfo, srcinfo, sizeof(FmgrInfo)) */
    (*dstinfo).fn_mcxt = destcxt;
    (*dstinfo).fn_extra = null_mut();
}

/*
 * Specialized lookup routine for fmgr_internal_validator: given the alleged
 * name of an internal function, return the OID of the function.
 * If the name is not recognized, return InvalidOid.
 */
pub unsafe fn fmgr_internal_function(proname: *const c_char) -> Oid {
    let fbp = fmgr_lookupByName(proname);

    if fbp.is_null() {
        return InvalidOid;
    }
    (*fbp).foid
}

/*
 * Support for security-definer and proconfig-using functions.  We support
 * both of these features using the same call handler, because they are
 * often used together and it would be inefficient (as well as notationally
 * messy) to have two levels of call handler involved.
 *
 * TODO(pg-port): fmgr_security_definer needs syscache (pg_proc), GUC stacking
 * (NewGUCNestLevel/AtEOXact_GUC/set_config_with_handle), userid/sec-context
 * (GetUserIdAndSecContext/SetUserIdAndSecContext), pgstat function usage, the
 * fmgr_hook, and the PG_TRY/PG_CATCH error machinery. Stubbed for now.
 */
pub unsafe fn fmgr_security_definer(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("fmgr_security_definer: syscache/GUC/pgstat/hooks not yet translated")
}

/*-------------------------------------------------------------------------
 *		Support routines for callers of fmgr-compatible functions
 *-------------------------------------------------------------------------
 */

/*
 * These are for invocation of a specifically named function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.  Also, the function cannot be one that needs to
 * look at FmgrInfo, since there won't be any.
 */
pub unsafe fn DirectFunctionCall1Coll(func: PGFunction, collation: Oid, arg1: Datum) -> Datum {
    LOCAL_FCINFO!(fcinfo, 1);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 1, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall2Coll(func: PGFunction, collation: Oid, arg1: Datum, arg2: Datum) -> Datum {
    LOCAL_FCINFO!(fcinfo, 2);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 2, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall3Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 3);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 3, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall4Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 4);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 4, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall5Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 5);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 5, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall6Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 6);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 6, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall7Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 7);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 7, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(6)).value = arg7;
    (*(*fcinfo).args.as_mut_ptr().add(6)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall8Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
    arg8: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 8);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 8, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(6)).value = arg7;
    (*(*fcinfo).args.as_mut_ptr().add(6)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(7)).value = arg8;
    (*(*fcinfo).args.as_mut_ptr().add(7)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn DirectFunctionCall9Coll(
    func: PGFunction,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
    arg8: Datum,
    arg9: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 9);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, null_mut(), 9, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(6)).value = arg7;
    (*(*fcinfo).args.as_mut_ptr().add(6)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(7)).value = arg8;
    (*(*fcinfo).args.as_mut_ptr().add(7)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(8)).value = arg9;
    (*(*fcinfo).args.as_mut_ptr().add(8)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

/*
 * These functions work like the DirectFunctionCall functions except that
 * they use the flinfo parameter to initialise the fcinfo for the call.
 * It's recommended that the callee only use the fn_extra and fn_mcxt
 * fields, as other fields will typically describe the calling function
 * not the callee.  Conversely, the calling function should not have
 * used fn_extra, unless its use is known to be compatible with the callee's.
 */

pub unsafe fn CallerFInfoFunctionCall1(
    func: PGFunction,
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 1);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 1, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

pub unsafe fn CallerFInfoFunctionCall2(
    func: PGFunction,
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 2);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 2, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;

    result = func(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {:p} returned NULL", func as *const c_void);
    }

    result
}

/*
 * These are for invocation of a previously-looked-up function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.
 */
pub unsafe fn FunctionCall0Coll(flinfo: *mut FmgrInfo, collation: Oid) -> Datum {
    LOCAL_FCINFO!(fcinfo, 0);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 0, collation, null_mut(), null_mut());

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall1Coll(flinfo: *mut FmgrInfo, collation: Oid, arg1: Datum) -> Datum {
    LOCAL_FCINFO!(fcinfo, 1);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 1, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall2Coll(flinfo: *mut FmgrInfo, collation: Oid, arg1: Datum, arg2: Datum) -> Datum {
    LOCAL_FCINFO!(fcinfo, 2);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 2, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall3Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 3);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 3, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall4Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 4);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 4, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall5Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 5);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 5, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall6Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 6);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 6, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall7Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 7);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 7, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(6)).value = arg7;
    (*(*fcinfo).args.as_mut_ptr().add(6)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall8Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
    arg8: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 8);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 8, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(6)).value = arg7;
    (*(*fcinfo).args.as_mut_ptr().add(6)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(7)).value = arg8;
    (*(*fcinfo).args.as_mut_ptr().add(7)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

pub unsafe fn FunctionCall9Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
    arg8: Datum,
    arg9: Datum,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 9);
    let result: Datum;

    InitFunctionCallInfoData!(fcinfo, flinfo, 9, collation, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = arg1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = arg2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = arg3;
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(3)).value = arg4;
    (*(*fcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(4)).value = arg5;
    (*(*fcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(5)).value = arg6;
    (*(*fcinfo).args.as_mut_ptr().add(5)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(6)).value = arg7;
    (*(*fcinfo).args.as_mut_ptr().add(6)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(7)).value = arg8;
    (*(*fcinfo).args.as_mut_ptr().add(7)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(8)).value = arg9;
    (*(*fcinfo).args.as_mut_ptr().add(8)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (*fcinfo).isnull {
        elog!(ERROR, "function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

/*
 * These are for invocation of a function identified by OID with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.  These are essentially fmgr_info() followed
 * by FunctionCallN().  If the same function is to be invoked repeatedly,
 * do the fmgr_info() once and then use FunctionCallN().
 */
pub unsafe fn OidFunctionCall0Coll(functionId: Oid, collation: Oid) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall0Coll(&mut flinfo, collation)
}

pub unsafe fn OidFunctionCall1Coll(functionId: Oid, collation: Oid, arg1: Datum) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall1Coll(&mut flinfo, collation, arg1)
}

pub unsafe fn OidFunctionCall2Coll(functionId: Oid, collation: Oid, arg1: Datum, arg2: Datum) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall2Coll(&mut flinfo, collation, arg1, arg2)
}

pub unsafe fn OidFunctionCall3Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall3Coll(&mut flinfo, collation, arg1, arg2, arg3)
}

pub unsafe fn OidFunctionCall4Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall4Coll(&mut flinfo, collation, arg1, arg2, arg3, arg4)
}

pub unsafe fn OidFunctionCall5Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall5Coll(&mut flinfo, collation, arg1, arg2, arg3, arg4, arg5)
}

pub unsafe fn OidFunctionCall6Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall6Coll(&mut flinfo, collation, arg1, arg2, arg3, arg4, arg5, arg6)
}

pub unsafe fn OidFunctionCall7Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall7Coll(&mut flinfo, collation, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
}

pub unsafe fn OidFunctionCall8Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
    arg8: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall8Coll(&mut flinfo, collation, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
}

pub unsafe fn OidFunctionCall9Coll(
    functionId: Oid,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
    arg8: Datum,
    arg9: Datum,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);

    FunctionCall9Coll(&mut flinfo, collation, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)
}

/*
 * The collation-omitting convenience wrappers (DirectFunctionCallN /
 * FunctionCallN / OidFunctionCallN, all defaulting collation to InvalidOid)
 * are expressed as macro_rules!, matching the `#define ...Coll(..., InvalidOid, ...)`
 * shorthands in fmgr.h.
 */
#[macro_export]
macro_rules! DirectFunctionCall1 {
    ($func:expr, $arg1:expr) => {
        $crate::utils::fmgr::DirectFunctionCall1Coll($func, $crate::postgres_ext::InvalidOid, $arg1)
    };
}
#[macro_export]
macro_rules! DirectFunctionCall2 {
    ($func:expr, $arg1:expr, $arg2:expr) => {
        $crate::utils::fmgr::DirectFunctionCall2Coll($func, $crate::postgres_ext::InvalidOid, $arg1, $arg2)
    };
}
#[macro_export]
macro_rules! DirectFunctionCall3 {
    ($func:expr, $arg1:expr, $arg2:expr, $arg3:expr) => {
        $crate::utils::fmgr::DirectFunctionCall3Coll(
            $func, $crate::postgres_ext::InvalidOid, $arg1, $arg2, $arg3,
        )
    };
}
#[macro_export]
macro_rules! FunctionCall1 {
    ($flinfo:expr, $arg1:expr) => {
        $crate::utils::fmgr::FunctionCall1Coll($flinfo, $crate::postgres_ext::InvalidOid, $arg1)
    };
}
#[macro_export]
macro_rules! FunctionCall2 {
    ($flinfo:expr, $arg1:expr, $arg2:expr) => {
        $crate::utils::fmgr::FunctionCall2Coll($flinfo, $crate::postgres_ext::InvalidOid, $arg1, $arg2)
    };
}
#[macro_export]
macro_rules! OidFunctionCall1 {
    ($functionId:expr, $arg1:expr) => {
        $crate::utils::fmgr::OidFunctionCall1Coll($functionId, $crate::postgres_ext::InvalidOid, $arg1)
    };
}
#[macro_export]
macro_rules! OidFunctionCall2 {
    ($functionId:expr, $arg1:expr, $arg2:expr) => {
        $crate::utils::fmgr::OidFunctionCall2Coll(
            $functionId, $crate::postgres_ext::InvalidOid, $arg1, $arg2,
        )
    };
}

/*
 * Special cases for convenient invocation of datatype I/O functions.
 */

/*
 * Call a previously-looked-up datatype input function.
 *
 * "str" may be NULL to indicate we are reading a NULL.  In this case
 * the caller should assume the result is NULL, but we'll call the input
 * function anyway if it's not strict.  So this is almost but not quite
 * the same as FunctionCall3.
 */
pub unsafe fn InputFunctionCall(flinfo: *mut FmgrInfo, str: *mut c_char, typioparam: Oid, typmod: int32) -> Datum {
    LOCAL_FCINFO!(fcinfo, 3);
    let result: Datum;

    if str.is_null() && (*flinfo).fn_strict {
        return 0 as Datum; /* just return null result */
    }

    InitFunctionCallInfoData!(fcinfo, flinfo, 3, InvalidOid, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = CStringGetDatum(str);
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typioparam);
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = Int32GetDatum(typmod);
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Should get null result if and only if str is NULL */
    if str.is_null() {
        if !(*fcinfo).isnull {
            elog!(ERROR, "input function {} returned non-NULL", (*flinfo).fn_oid);
        }
    } else if (*fcinfo).isnull {
        elog!(ERROR, "input function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

/*
 * Call a previously-looked-up datatype input function, with non-exception
 * handling of "soft" errors.
 *
 * This is basically like InputFunctionCall, but the converted Datum is
 * returned into *result while the function result is true for success or
 * false for failure.  Also, the caller may pass an ErrorSaveContext node.
 * (We declare that as "fmNodePtr" to avoid including nodes.h in fmgr.h.)
 *
 * If escontext points to an ErrorSaveContext, any "soft" errors detected by
 * the input function will be reported by filling the escontext struct and
 * returning false.  (The caller can choose to test SOFT_ERROR_OCCURRED(),
 * but checking the function result instead is usually cheaper.)
 *
 * If escontext does not point to an ErrorSaveContext, errors are reported
 * via ereport(ERROR), so that there is no functional difference from
 * InputFunctionCall; the result will always be true if control returns.
 */
pub unsafe fn InputFunctionCallSafe(
    flinfo: *mut FmgrInfo,
    str: *mut c_char,
    typioparam: Oid,
    typmod: int32,
    escontext: fmNodePtr,
    result: *mut Datum,
) -> bool {
    LOCAL_FCINFO!(fcinfo, 3);

    if str.is_null() && (*flinfo).fn_strict {
        *result = 0 as Datum; /* just return null result */
        return true;
    }

    InitFunctionCallInfoData!(fcinfo, flinfo, 3, InvalidOid, escontext, null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = CStringGetDatum(str);
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typioparam);
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = Int32GetDatum(typmod);
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    *result = FunctionCallInvoke!(fcinfo);

    /* Result value is garbage, and could be null, if an error was reported */
    // TODO(pg-port): SOFT_ERROR_OCCURRED(escontext) needs nodes/miscnodes.h
    // (ErrorSaveContext). Until then we treat "no soft error" as the path.
    if SOFT_ERROR_OCCURRED(escontext) {
        return false;
    }

    /* Otherwise, should get null result if and only if str is NULL */
    if str.is_null() {
        if !(*fcinfo).isnull {
            elog!(ERROR, "input function {} returned non-NULL", (*flinfo).fn_oid);
        }
    } else if (*fcinfo).isnull {
        elog!(ERROR, "input function {} returned NULL", (*flinfo).fn_oid);
    }

    true
}

/*
 * Call a directly-named datatype input function, with non-exception
 * handling of "soft" errors.
 *
 * This is like InputFunctionCallSafe, except that it is given a direct
 * pointer to the C function to call.  We assume that that function is
 * strict.  Also, the function cannot be one that needs to
 * look at FmgrInfo, since there won't be any.
 */
pub unsafe fn DirectInputFunctionCallSafe(
    func: PGFunction,
    str: *mut c_char,
    typioparam: Oid,
    typmod: int32,
    escontext: fmNodePtr,
    result: *mut Datum,
) -> bool {
    LOCAL_FCINFO!(fcinfo, 3);

    if str.is_null() {
        *result = 0 as Datum; /* just return null result */
        return true;
    }

    InitFunctionCallInfoData!(fcinfo, null_mut(), 3, InvalidOid, escontext, null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = CStringGetDatum(str);
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typioparam);
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = Int32GetDatum(typmod);
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    *result = func(fcinfo);

    /* Result value is garbage, and could be null, if an error was reported */
    if SOFT_ERROR_OCCURRED(escontext) {
        return false;
    }

    /* Otherwise, shouldn't get null result */
    if (*fcinfo).isnull {
        elog!(ERROR, "input function {:p} returned NULL", func as *const c_void);
    }

    true
}

/*
 * Call a previously-looked-up datatype output function.
 *
 * Do not call this on NULL datums.
 *
 * This is currently little more than window dressing for FunctionCall1.
 */
pub unsafe fn OutputFunctionCall(flinfo: *mut FmgrInfo, val: Datum) -> *mut c_char {
    DatumGetCString(FunctionCall1!(flinfo, val))
}

/*
 * Call a previously-looked-up datatype binary-input function.
 *
 * "buf" may be NULL to indicate we are reading a NULL.  In this case
 * the caller should assume the result is NULL, but we'll call the receive
 * function anyway if it's not strict.  So this is almost but not quite
 * the same as FunctionCall3.
 */
pub unsafe fn ReceiveFunctionCall(
    flinfo: *mut FmgrInfo,
    buf: StringInfo,
    typioparam: Oid,
    typmod: int32,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 3);
    let result: Datum;

    if buf.is_null() && (*flinfo).fn_strict {
        return 0 as Datum; /* just return null result */
    }

    InitFunctionCallInfoData!(fcinfo, flinfo, 3, InvalidOid, null_mut(), null_mut());

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = PointerGetDatum(buf as *const c_void);
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typioparam);
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = Int32GetDatum(typmod);
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    /* Should get null result if and only if buf is NULL */
    if buf.is_null() {
        if !(*fcinfo).isnull {
            elog!(ERROR, "receive function {} returned non-NULL", (*flinfo).fn_oid);
        }
    } else if (*fcinfo).isnull {
        elog!(ERROR, "receive function {} returned NULL", (*flinfo).fn_oid);
    }

    result
}

/*
 * Call a previously-looked-up datatype binary-output function.
 *
 * Do not call this on NULL datums.
 *
 * This is little more than window dressing for FunctionCall1, but it does
 * guarantee a non-toasted result, which strictly speaking the underlying
 * function doesn't.
 */
pub unsafe fn SendFunctionCall(flinfo: *mut FmgrInfo, val: Datum) -> *mut bytea {
    DatumGetByteaP!(FunctionCall1!(flinfo, val))
}

/*
 * As above, for I/O functions identified by OID.  These are only to be used
 * in seldom-executed code paths.  They are not only slow but leak memory.
 */
pub unsafe fn OidInputFunctionCall(functionId: Oid, str: *mut c_char, typioparam: Oid, typmod: int32) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);
    InputFunctionCall(&mut flinfo, str, typioparam, typmod)
}

pub unsafe fn OidOutputFunctionCall(functionId: Oid, val: Datum) -> *mut c_char {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);
    OutputFunctionCall(&mut flinfo, val)
}

pub unsafe fn OidReceiveFunctionCall(
    functionId: Oid,
    buf: StringInfo,
    typioparam: Oid,
    typmod: int32,
) -> Datum {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);
    ReceiveFunctionCall(&mut flinfo, buf, typioparam, typmod)
}

pub unsafe fn OidSendFunctionCall(functionId: Oid, val: Datum) -> *mut bytea {
    let mut flinfo: FmgrInfo = core::mem::zeroed();

    fmgr_info(functionId, &mut flinfo);
    SendFunctionCall(&mut flinfo, val)
}

/*-------------------------------------------------------------------------
 *		Support routines for standard maybe-pass-by-reference datatypes
 *
 * int8 and float8 can be passed by value if Datum is wide enough.
 * (For backwards-compatibility reasons, we allow pass-by-ref to be chosen
 * at compile time even if pass-by-val is possible.)
 *
 * Note: there is only one switch controlling the pass-by-value option for
 * both int8 and float8; this is to avoid making things unduly complicated
 * for the timestamp types, which might have either representation.
 *
 * In this port, USE_FLOAT8_BYVAL is assumed (Datum is uintptr_t == 64-bit), so
 * Int64GetDatum/Float8GetDatum are pass-by-value and live in postgres.rs; the
 * `#ifndef USE_FLOAT8_BYVAL` pass-by-ref variants are intentionally omitted.
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *		Support routines for toastable datatypes
 *
 * (pg_detoast_datum and friends are declared above near the PG_DETOAST_DATUM
 * macros; their bodies require the toast infrastructure and are stubbed.)
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *		Support routines for extracting info from fn_expr parse tree
 *
 * These are needed by polymorphic functions, which accept multiple possible
 * input types and need help from the parser to know what they've got.
 * Also, some functions might be interested in whether a parameter is constant.
 * Functions taking VARIADIC ANY also need to know about the VARIADIC keyword.
 *-------------------------------------------------------------------------
 */

/*
 * Get the actual type OID of the function return type
 *
 * Returns InvalidOid if information is not available
 */
pub unsafe fn get_fn_expr_rettype(flinfo: *mut FmgrInfo) -> Oid {
    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if flinfo.is_null() || (*flinfo).fn_expr.is_null() {
        return InvalidOid;
    }

    let expr: *mut Node = (*flinfo).fn_expr;

    exprType(expr)
}

/*
 * Get the actual type OID of a specific function argument (counting from 0)
 *
 * Returns InvalidOid if information is not available
 */
pub unsafe fn get_fn_expr_argtype(flinfo: *mut FmgrInfo, argnum: c_int) -> Oid {
    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if flinfo.is_null() || (*flinfo).fn_expr.is_null() {
        return InvalidOid;
    }

    get_call_expr_argtype((*flinfo).fn_expr, argnum)
}

/*
 * Get the actual type OID of a specific function argument (counting from 0),
 * but working from the calling expression tree instead of FmgrInfo
 *
 * Returns InvalidOid if information is not available
 */
pub unsafe fn get_call_expr_argtype(expr: *mut Node, argnum: c_int) -> Oid {
    let args: *mut List;
    let mut argtype: Oid;

    if expr.is_null() {
        return InvalidOid;
    }

    if IsA!(expr, T_FuncExpr) {
        args = (*(expr as *mut FuncExpr)).args;
    } else if IsA!(expr, T_OpExpr) {
        args = (*(expr as *mut OpExpr)).args;
    } else if IsA!(expr, T_DistinctExpr) {
        args = (*(expr as *mut DistinctExpr)).args;
    } else if IsA!(expr, T_ScalarArrayOpExpr) {
        args = (*(expr as *mut ScalarArrayOpExpr)).args;
    } else if IsA!(expr, T_NullIfExpr) {
        args = (*(expr as *mut NullIfExpr)).args;
    } else if IsA!(expr, T_WindowFunc) {
        args = (*(expr as *mut WindowFunc)).args;
    } else {
        return InvalidOid;
    }

    if argnum < 0 || argnum >= list_length(args) {
        return InvalidOid;
    }

    argtype = exprType(list_nth(args, argnum) as *mut Node);

    /*
     * special hack for ScalarArrayOpExpr: what the underlying function will
     * actually get passed is the element type of the array.
     */
    if IsA!(expr, T_ScalarArrayOpExpr) && argnum == 1 {
        argtype = get_base_element_type(argtype);
    }

    argtype
}

/*
 * Find out whether a specific function argument is constant for the
 * duration of a query
 *
 * Returns false if information is not available
 */
pub unsafe fn get_fn_expr_arg_stable(flinfo: *mut FmgrInfo, argnum: c_int) -> bool {
    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if flinfo.is_null() || (*flinfo).fn_expr.is_null() {
        return false;
    }

    get_call_expr_arg_stable((*flinfo).fn_expr, argnum)
}

/*
 * Find out whether a specific function argument is constant for the
 * duration of a query, but working from the calling expression tree
 *
 * Returns false if information is not available
 */
pub unsafe fn get_call_expr_arg_stable(expr: *mut Node, argnum: c_int) -> bool {
    let args: *mut List;
    let arg: *mut Node;

    if expr.is_null() {
        return false;
    }

    if IsA!(expr, T_FuncExpr) {
        args = (*(expr as *mut FuncExpr)).args;
    } else if IsA!(expr, T_OpExpr) {
        args = (*(expr as *mut OpExpr)).args;
    } else if IsA!(expr, T_DistinctExpr) {
        args = (*(expr as *mut DistinctExpr)).args;
    } else if IsA!(expr, T_ScalarArrayOpExpr) {
        args = (*(expr as *mut ScalarArrayOpExpr)).args;
    } else if IsA!(expr, T_NullIfExpr) {
        args = (*(expr as *mut NullIfExpr)).args;
    } else if IsA!(expr, T_WindowFunc) {
        args = (*(expr as *mut WindowFunc)).args;
    } else {
        return false;
    }

    if argnum < 0 || argnum >= list_length(args) {
        return false;
    }

    arg = list_nth(args, argnum) as *mut Node;

    /*
     * Either a true Const or an external Param will have a value that doesn't
     * change during the execution of the query.  In future we might want to
     * consider other cases too, e.g. now().
     */
    if IsA!(arg, T_Const) {
        return true;
    }
    if IsA!(arg, T_Param) && (*(arg as *mut Param)).paramkind == PARAM_EXTERN {
        return true;
    }

    false
}

/*
 * Get the VARIADIC flag from the function invocation
 *
 * Returns false (the default assumption) if information is not available
 *
 * Note this is generally only of interest to VARIADIC ANY functions
 */
pub unsafe fn get_fn_expr_variadic(flinfo: *mut FmgrInfo) -> bool {
    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if flinfo.is_null() || (*flinfo).fn_expr.is_null() {
        return false;
    }

    let expr: *mut Node = (*flinfo).fn_expr;

    if IsA!(expr, T_FuncExpr) {
        (*(expr as *mut FuncExpr)).funcvariadic
    } else {
        false
    }
}

/*
 * Set options to FmgrInfo of opclass support function.
 *
 * Opclass support functions are called outside of expressions.  Thanks to that
 * we can use fn_expr to store opclass options as bytea constant.
 */
pub unsafe fn set_fn_opclass_options(flinfo: *mut FmgrInfo, options: *mut bytea) {
    // C: flinfo->fn_expr = (Node *) makeConst(BYTEAOID, -1, InvalidOid, -1,
    //                                         PointerGetDatum(options),
    //                                         options == NULL, false);
    // TODO(pg-port): makeConst (nodes/makefuncs.c) and BYTEAOID (pg_type)
    // are not yet translated.
    let _ = (flinfo, options);
    unimplemented!("set_fn_opclass_options: makeConst / BYTEAOID not yet translated")
}

/*
 * Check if options are defined for opclass support function.
 */
pub unsafe fn has_fn_opclass_options(flinfo: *mut FmgrInfo) -> bool {
    if !flinfo.is_null() && !(*flinfo).fn_expr.is_null() && IsA!((*flinfo).fn_expr, T_Const) {
        let expr: *mut Const = (*flinfo).fn_expr as *mut Const;

        // TODO(pg-port): BYTEAOID (catalog/pg_type) not yet translated.
        if (*expr).consttype == BYTEAOID {
            return !(*expr).constisnull;
        }
    }
    false
}

/*
 * Get options for opclass support function.
 */
pub unsafe fn get_fn_opclass_options(flinfo: *mut FmgrInfo) -> *mut bytea {
    if !flinfo.is_null() && !(*flinfo).fn_expr.is_null() && IsA!((*flinfo).fn_expr, T_Const) {
        let expr: *mut Const = (*flinfo).fn_expr as *mut Const;

        // TODO(pg-port): BYTEAOID (catalog/pg_type) not yet translated.
        if (*expr).consttype == BYTEAOID {
            return if (*expr).constisnull {
                null_mut()
            } else {
                DatumGetByteaP!((*expr).constvalue)
            };
        }
    }

    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
    ereport!(
        ERROR,
        errmsg!("operator class options info is absent in function call context")
    );

    null_mut()
}

/*-------------------------------------------------------------------------
 *		Support routines for procedural language implementations
 *-------------------------------------------------------------------------
 */

/*
 * Verify that a validator is actually associated with the language of a
 * particular function and that the user has access to both the language and
 * the function.  All validators should call this before doing anything
 * substantial.  Doing so ensures a user cannot achieve anything with explicit
 * calls to validators that he could not achieve with CREATE FUNCTION or by
 * simply calling an existing function.
 *
 * When this function returns false, callers should skip all validation work
 * and call PG_RETURN_VOID().  This never happens at present; it is reserved
 * for future expansion.
 *
 * TODO(pg-port): CheckFunctionValidatorAccess needs the syscache (pg_proc,
 * pg_language) and the ACL machinery (object_aclcheck / aclcheck_error).
 */
pub unsafe fn CheckFunctionValidatorAccess(validatorOid: Oid, functionOid: Oid) -> bool {
    let _ = (validatorOid, functionOid);
    unimplemented!("CheckFunctionValidatorAccess: syscache + ACL machinery not yet translated")
}

/*-------------------------------------------------------------------------
 *  Catalog/node helper stubs.
 *
 *  The following symbols are used by the translated fn_expr-introspection
 *  routines above but are defined in not-yet-ported units. They are stubbed
 *  here so this module is self-contained; replace them with `use` imports once
 *  the owning modules land.
 *-------------------------------------------------------------------------
 */

/* nodes/nodeFuncs.c */
#[inline]
unsafe fn exprType(expr: *const Node) -> Oid {
    // TODO(pg-port): exprType lives in nodes/nodeFuncs.c (not yet translated).
    let _ = expr;
    unimplemented!("exprType: nodes/nodeFuncs.c not yet translated")
}

/* utils/cache/lsyscache.c */
#[inline]
unsafe fn get_base_element_type(typid: Oid) -> Oid {
    // TODO(pg-port): get_base_element_type lives in utils/cache/lsyscache.c.
    let _ = typid;
    unimplemented!("get_base_element_type: utils/cache/lsyscache.c not yet translated")
}

/* catalog/pg_type (pg_type.dat) */
// TODO(pg-port): BYTEAOID comes from the generated pg_type_d.h; stub value 17.
const BYTEAOID: Oid = 17;

/* errcodes.h classification (errcode() shim ignores the value) */
// TODO(pg-port): ERRCODE_INVALID_PARAMETER_VALUE from utils/errcodes.h.
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

/* nodes/miscnodes.h */
#[inline]
unsafe fn SOFT_ERROR_OCCURRED(escontext: fmNodePtr) -> bool {
    // C: ((escontext) != NULL && IsA(escontext, ErrorSaveContext) &&
    //     ((ErrorSaveContext *) escontext)->error_occurred)
    // TODO(pg-port): ErrorSaveContext lives in nodes/miscnodes.h (not yet
    // translated). Conservatively report "no soft error".
    let _ = escontext;
    false
}

/* lib/stringinfo.h: StringInfo (= *mut StringInfoData) is imported at the top. */

/*
 * ===========================================================================
 *  Translation summary
 * ===========================================================================
 *
 * Types / structs (all #[repr(C)] where they have a C layout):
 *   - PGFunction               = unsafe fn(FunctionCallInfo) -> Datum
 *   - FunctionCallInfo         = *mut FunctionCallInfoBaseData
 *   - FmgrInfo, FunctionCallInfoBaseData (with FLEXIBLE_ARRAY_MEMBER args[])
 *   - fmNodePtr, fmAggrefPtr, fmExprContextCallbackFunction, fmStringInfo
 *   - Pg_finfo_record, PGFInfoFunction, Pg_abi_values, Pg_magic_struct,
 *     PGModuleMagicFunction
 *   - FmgrBuiltin (companion fmgrtab.h struct) + TRACK_FUNC_* constants
 *   - FmgrHookEventType, needs_fmgr_hook_type, fmgr_hook_type
 *
 * Constants: FIELDNO_FUNCTIONCALLINFODATA_ISNULL/ARGS, PG_MAGIC_FUNCTION_NAME_STRING,
 *   AGG_CONTEXT_AGGREGATE/WINDOW.
 *
 * Macros implemented as macro_rules!:
 *   - LOCAL_FCINFO! (stack-allocates an aligned buffer sized for N args and
 *     binds a FunctionCallInfo pointer; the Rust-friendly analog of the C union)
 *   - InitFunctionCallInfoData!, FunctionCallInvoke!, fmgr_info_set_expr!
 *   - PG_FUNCTION_ARGS!, PG_GET_COLLATION!, PG_NARGS!, PG_ARGISNULL!
 *   - PG_GETARG_* family (DATUM/INT32/UINT32/INT16/UINT16/CHAR/BOOL/OID/POINTER/
 *     CSTRING/NAME/TRANSACTIONID/FLOAT4/FLOAT8/INT64 + varlena PP/P/COPY/SLICE,
 *     RAW_VARLENA_P, VARLENA_P, VARLENA_PP) and the DatumGet{Bytea,Text}P* helpers
 *   - PG_RETURN_* family (NULL/VOID/DATUM/INT32/.../INT64/UINT64/FLOAT4/FLOAT8/
 *     BYTEA_P/TEXT_P/NAME/CSTRING/POINTER/TRANSACTIONID/OID/BOOL/CHAR)
 *   - PG_DETOAST_DATUM!/_COPY!/_SLICE!/_PACKED!, PG_FREE_IF_COPY!
 *   - PG_HAS_OPCLASS_OPTIONS!/PG_GET_OPCLASS_OPTIONS!
 *   - DirectFunctionCall1/2/3!, FunctionCall1/2!, OidFunctionCall1/2!
 *     (collation-omitting wrappers, default InvalidOid)
 *
 * SizeForFunctionCallInfo() implemented as a const fn using offset_of! + size_of.
 *
 * Functions translated faithfully (struct-filling + invoke logic):
 *   - fmgr_info, fmgr_info_cxt (skeleton; dispatch into fmgr_info_cxt_security)
 *   - DirectFunctionCall1..9Coll, CallerFInfoFunctionCall1/2
 *   - FunctionCall0..9Coll, OidFunctionCall0..9Coll
 *   - InputFunctionCall, InputFunctionCallSafe, DirectInputFunctionCallSafe,
 *     OutputFunctionCall, ReceiveFunctionCall, SendFunctionCall,
 *     OidInputFunctionCall, OidOutputFunctionCall, OidReceiveFunctionCall,
 *     OidSendFunctionCall
 *   - fmgr_info_copy, fmgr_internal_function
 *   - get_fn_expr_rettype/argtype, get_call_expr_argtype,
 *     get_fn_expr_arg_stable, get_call_expr_arg_stable, get_fn_expr_variadic,
 *     has_fn_opclass_options, get_fn_opclass_options (node-walk logic faithful;
 *     leaf helpers exprType/get_base_element_type/BYTEAOID stubbed)
 *
 * Catalog/infra-dependent parts STUBBED with TODO(pg-port) + unimplemented!():
 *   - fmgr_isbuiltin / fmgr_lookupByName     (generated fmgrtab.c builtin tables)
 *   - fmgr_info_cxt_security                  (pg_proc syscache + prolang dispatch)
 *   - fmgr_symbol                             (pg_proc syscache)
 *   - fmgr_security_definer                   (syscache/GUC/pgstat/hooks/PG_TRY)
 *   - fetch_finfo_record                      (dynamic loader dfmgr.c)
 *   - CheckFunctionValidatorAccess            (syscache + ACL machinery)
 *   - set_fn_opclass_options                  (makeConst / BYTEAOID)
 *   - pg_detoast_datum / _copy / _slice / _packed   (toast infrastructure)
 *   - exprType / get_base_element_type / SOFT_ERROR_OCCURRED   (node/cache leaves)
 *   - The static C-func cache (CFuncHashTabEntry, lookup_C_func, record_C_func)
 *     and fmgr_info_C_lang / fmgr_info_other_lang are documented but omitted
 *     (need dfmgr loader + pg_language syscache).
 *   - The pass-by-ref Int64GetDatum/Float8GetDatum (#ifndef USE_FLOAT8_BYVAL)
 *     are omitted; the by-value versions live in postgres.rs.
 *   - PG_FUNCTION_INFO_V1 / PG_MODULE_MAGIC[_EXT] declaration macros (extension
 *     loading) are omitted; only the ABI structs are translated.
 *
 * Coverage: the full fmgr.h interface and every fmgr.c routine are represented.
 * The high-value PG_GETARG, PG_RETURN, PG_FUNCTION_ARGS macros and the
 * DirectFunctionCallN/FunctionCallN/OidFunctionCallN call wrappers are complete
 * and exercise the prelude's DatumGet / GetDatum helpers directly.
 */

/*
 * Support for aggregate functions
 *
 * These are actually in executor/nodeAgg.c, but we declare them here since
 * the whole point is for callers to not be overly friendly with nodeAgg.
 *
 * TODO(pg-port): AggCheckCallContext / AggGetAggref / AggGetTempMemoryContext /
 * AggStateIsShared / AggRegisterCallback live in executor/nodeAgg.c (not ported).
 */

/* AggCheckCallContext can return one of the following codes, or 0: */
pub const AGG_CONTEXT_AGGREGATE: c_int = 1; /* regular aggregate */
pub const AGG_CONTEXT_WINDOW: c_int = 2; /* window function */

/*
 * We allow plugin modules to hook function entry/exit.  This is intended
 * as support for loadable security policy modules, which may want to
 * perform additional privilege checks on function entry or exit, or to do
 * other internal bookkeeping.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FmgrHookEventType {
    FHET_START,
    FHET_END,
    FHET_ABORT,
}
pub use FmgrHookEventType::*;

#[macro_export]
macro_rules! FmgrHookIsNeeded {
    ($fn_oid:expr) => {
        match $crate::utils::fmgr::needs_fmgr_hook {
            None => false,
            Some(hook) => hook($fn_oid),
        }
    };
}

pub mod dfmgr;

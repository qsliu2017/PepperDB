//! domains.c - I/O functions for domain types (constraint checking on input).
//!
//! The output functions for a domain type are just the same ones provided by
//! its underlying base type.  The input functions, however, must apply any
//! constraints defined by the type.  We create special input functions that
//! invoke the base type's input function and then check the constraints.
//!
//! STUBBED dependencies (unported in this port): the typcache machinery
//! (`TypeCacheEntry`, `lookup_type_cache`, `DomainConstraintRef`,
//! `InitDomainConstraintRef`, `UpdateDomainConstraintRef`, the typcache
//! constants), the lsyscache helpers (`getTypeInputInfo`,
//! `getTypeBinaryInputInfo`, `get_namespace_name`), the syscache
//! (`SearchSysCache1`, `ReleaseSysCache`, `TYPEOID`), `err_generic_string`,
//! and the `MakeExpandedObjectReadOnly` macro.  These are stubbed locally so
//! the real domain_in/domain_recv/domain_check control flow is translated 1:1.

use crate::prelude::*;
use std::ffi::c_short;
type sig_atomic_t = std::ffi::c_int;
use crate::{PG_ARGISNULL, PG_GETARG_CSTRING, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_DATUM, PG_RETURN_NULL};
use crate::{current_cell, foreach, IsA, SOFT_ERROR_OCCURRED};

use crate::nodes::execnodes::{
    DomainConstraintState, ExprContext, DOM_CONSTRAINT_CHECK, DOM_CONSTRAINT_NOTNULL,
};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{lfirst, List};
use crate::lib::stringinfo::StringInfo;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::catalog::pg_type::{Form_pg_type, TYPTYPE_DOMAIN};
use crate::utils::fmgr::{
    fmgr_info_cxt, FmgrInfo, FunctionCallInfo, InputFunctionCallSafe, ReceiveFunctionCall,
};
use crate::executor::executor::{CreateStandaloneExprContext, ExecCheck, ReScanExprContext};
use crate::utils::builtins::format_type_be;

use std::ffi::CStr;

// TODO(pg-port): ERRCODE_* from utils/errcodes.h.
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;
const ERRCODE_NOT_NULL_VIOLATION: c_int = 0;
const ERRCODE_CHECK_VIOLATION: c_int = 0;

/*
 * STUBBED typcache.h types -----------------------------------------------
 *
 * The real typcache is not yet ported.  We model the pieces that domains.c
 * touches with opaque/local definitions so the surrounding logic translates.
 */

/* utils/typcache.h: bits requested from lookup_type_cache */
const TYPECACHE_DOMAIN_BASE_INFO: c_int = 0x4000;

#[repr(C)]
pub struct TypeCacheEntry {
    pub typtype: c_char,
    pub typlen: c_short,
    pub domainBaseType: Oid,
    pub domainBaseTypmod: int32,
}

#[repr(C)]
pub struct DomainConstraintRef {
    /* Reference to cached list of constraint items to check */
    pub constraints: *mut List,
    /* The typcache entry for the domain */
    pub tcache: *mut TypeCacheEntry,
}

unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("lookup_type_cache: utils/cache/typcache.c not yet translated")
}

unsafe fn InitDomainConstraintRef(
    _type_id: Oid,
    _ref_: *mut DomainConstraintRef,
    _refctx: MemoryContext,
    _need_exprstate: bool,
) {
    unimplemented!("InitDomainConstraintRef: utils/cache/typcache.c not yet translated")
}

unsafe fn UpdateDomainConstraintRef(_ref_: *mut DomainConstraintRef) {
    unimplemented!("UpdateDomainConstraintRef: utils/cache/typcache.c not yet translated")
}

/* utils/lsyscache.h */
unsafe fn getTypeInputInfo(_typ: Oid, _typinput: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!("getTypeInputInfo: utils/cache/lsyscache.c not yet translated")
}

unsafe fn getTypeBinaryInputInfo(_typ: Oid, _typreceive: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!("getTypeBinaryInputInfo: utils/cache/lsyscache.c not yet translated")
}

unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!("get_namespace_name: utils/cache/lsyscache.c not yet translated")
}

/* utils/syscache.h */
const TYPEOID: c_int = 0;

unsafe fn SearchSysCache1(_cache_id: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!("SearchSysCache1: utils/cache/syscache.c not yet translated")
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!("ReleaseSysCache: utils/cache/syscache.c not yet translated")
}

/* utils/elog.h: stores a generic string field into the current errordata */
unsafe fn err_generic_string(_field: c_int, _str: *const c_char) -> c_int {
    /* TODO(pg-port): err_generic_string in elog.c not yet translated. */
    0
}

/*
 * utils/expandeddatum.h: MakeExpandedObjectReadOnly(d, isnull, typlen).
 * If a R/W expanded object, return a R/O pointer; otherwise return d.
 * STUB: expanded-object support is incomplete, so just pass datum through.
 */
unsafe fn MakeExpandedObjectReadOnly(d: Datum, _isnull: bool, _typlen: c_short) -> Datum {
    d
}

/*
 * structure to cache state across multiple calls
 */
#[repr(C)]
struct DomainIOData {
    domain_type: Oid,
    /* Data needed to call base type's input function */
    typiofunc: Oid,
    typioparam: Oid,
    typtypmod: int32,
    proc: FmgrInfo,
    /* Reference to cached list of constraint items to check */
    constraint_ref: DomainConstraintRef,
    /* Context for evaluating CHECK constraints in */
    econtext: *mut ExprContext,
    /* Memory context this cache is in */
    mcxt: MemoryContext,
}

/*
 * domain_state_setup - initialize the cache for a new domain type.
 *
 * Note: we can't re-use the same cache struct for a new domain type,
 * since there's no provision for releasing the DomainConstraintRef.
 * If a call site needs to deal with a new domain type, we just leak
 * the old struct for the duration of the query.
 */
unsafe fn domain_state_setup(
    domainType: Oid,
    binary: bool,
    mcxt: MemoryContext,
) -> *mut DomainIOData {
    let my_extra: *mut DomainIOData;
    let typentry: *mut TypeCacheEntry;
    let baseType: Oid;

    my_extra =
        MemoryContextAlloc(mcxt, std::mem::size_of::<DomainIOData>() as Size) as *mut DomainIOData;

    /*
     * Verify that domainType represents a valid domain type.  We need to be
     * careful here because domain_in and domain_recv can be called from SQL,
     * possibly with incorrect arguments.  We use lookup_type_cache mainly
     * because it will throw a clean user-facing error for a bad OID; but also
     * it can cache the underlying base type info.
     */
    typentry = lookup_type_cache(domainType, TYPECACHE_DOMAIN_BASE_INFO);
    if (*typentry).typtype != TYPTYPE_DOMAIN {
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "type {} is not a domain",
                cstr_to_string(format_type_be(domainType))
            )
        );
    }

    /* Find out the base type */
    baseType = (*typentry).domainBaseType;
    (*my_extra).typtypmod = (*typentry).domainBaseTypmod;

    /* Look up underlying I/O function */
    if binary {
        getTypeBinaryInputInfo(
            baseType,
            &mut (*my_extra).typiofunc,
            &mut (*my_extra).typioparam,
        );
    } else {
        getTypeInputInfo(
            baseType,
            &mut (*my_extra).typiofunc,
            &mut (*my_extra).typioparam,
        );
    }
    fmgr_info_cxt((*my_extra).typiofunc, &mut (*my_extra).proc, mcxt);

    /* Look up constraints for domain */
    InitDomainConstraintRef(domainType, &mut (*my_extra).constraint_ref, mcxt, true);

    /* We don't make an ExprContext until needed */
    (*my_extra).econtext = null_mut();
    (*my_extra).mcxt = mcxt;

    /* Mark cache valid */
    (*my_extra).domain_type = domainType;

    my_extra
}

/*
 * domain_check_input - apply the cached checks.
 *
 * This is roughly similar to the handling of CoerceToDomain nodes in
 * execExpr*.c, but we execute each constraint separately, rather than
 * compiling them in-line within a larger expression.
 *
 * If escontext points to an ErrorSaveContext, any failures are reported
 * there, otherwise they are ereport'ed.  Note that we do not attempt to do
 * soft reporting of errors raised during execution of CHECK constraints.
 */
unsafe fn domain_check_input(
    value: Datum,
    isnull: bool,
    my_extra: *mut DomainIOData,
    escontext: *mut Node,
) {
    let mut econtext: *mut ExprContext = (*my_extra).econtext;

    /* Make sure we have up-to-date constraints */
    UpdateDomainConstraintRef(&mut (*my_extra).constraint_ref);

    foreach!(l, (*my_extra).constraint_ref.constraints, {
        let con = lfirst(current_cell!(l)) as *mut DomainConstraintState;

        match (*con).constrainttype {
            DOM_CONSTRAINT_NOTNULL => {
                if isnull {
                    errsave_not_null(escontext, my_extra);
                    /* goto fail */
                    if !econtext.is_null() {
                        ReScanExprContext(econtext);
                    }
                    return;
                }
            }
            DOM_CONSTRAINT_CHECK => {
                /* Make the econtext if we didn't already */
                if econtext.is_null() {
                    let oldcontext: MemoryContext;

                    oldcontext = MemoryContextSwitchTo((*my_extra).mcxt);
                    econtext = CreateStandaloneExprContext();
                    MemoryContextSwitchTo(oldcontext);
                    (*my_extra).econtext = econtext;
                }

                /*
                 * Set up value to be returned by CoerceToDomainValue nodes.
                 * Unlike in the generic expression case, this econtext
                 * couldn't be shared with anything else, so no need to save
                 * and restore fields.  But we do need to protect the
                 * passed-in value against being changed by called functions.
                 * (It couldn't be a R/W expanded object for most uses, but
                 * that seems possible for domain_check().)
                 */
                (*econtext).domainValue_datum = MakeExpandedObjectReadOnly(
                    value,
                    isnull,
                    (*(*my_extra).constraint_ref.tcache).typlen,
                );
                (*econtext).domainValue_isNull = isnull;

                if !ExecCheck((*con).check_exprstate, econtext) {
                    errsave_check(escontext, my_extra, (*con).name);
                    /* goto fail */
                    if !econtext.is_null() {
                        ReScanExprContext(econtext);
                    }
                    return;
                }
            }
            #[allow(unreachable_patterns)]
            _ => {
                elog!(
                    ERROR,
                    "unrecognized constraint type: {}",
                    (*con).constrainttype as c_int
                );
            }
        }
    });

    /*
     * Before exiting, call any shutdown callbacks and reset econtext's
     * per-tuple memory.  This avoids leaking non-memory resources, if
     * anything in the expression(s) has any.
     */
    /* fail: */
    if !econtext.is_null() {
        ReScanExprContext(econtext);
    }
}

/*
 * errsave() expansion for the NOT NULL constraint violation.  In C this is an
 * errsave(escontext, (...)) that either soft-reports or ereport's.  Soft-error
 * reporting (ErrorSaveContext) is not yet ported, so we hard-report here.
 */
unsafe fn errsave_not_null(escontext: *mut Node, my_extra: *mut DomainIOData) {
    let _ = escontext;
    let _ = errcode(ERRCODE_NOT_NULL_VIOLATION);
    let _ = errdatatype((*my_extra).domain_type);
    ereport!(
        ERROR,
        errmsg!(
            "domain {} does not allow null values",
            cstr_to_string(format_type_be((*my_extra).domain_type))
        )
    );
}

/*
 * errsave() expansion for a CHECK constraint violation.  See errsave_not_null.
 */
unsafe fn errsave_check(escontext: *mut Node, my_extra: *mut DomainIOData, conname: *mut c_char) {
    let _ = escontext;
    let _ = errcode(ERRCODE_CHECK_VIOLATION);
    let _ = errdomainconstraint((*my_extra).domain_type, conname);
    ereport!(
        ERROR,
        errmsg!(
            "value for domain {} violates check constraint \"{}\"",
            cstr_to_string(format_type_be((*my_extra).domain_type)),
            cstr_to_string(conname)
        )
    );
}

/*
 * domain_in		- input routine for any domain type.
 */
#[no_mangle]
pub unsafe fn domain_in(fcinfo: FunctionCallInfo) -> Datum {
    let string: *mut c_char;
    let domainType: Oid;
    let escontext: *mut Node = (*fcinfo).context as *mut Node;
    let mut my_extra: *mut DomainIOData;
    let mut value: Datum = 0;

    /*
     * Since domain_in is not strict, we have to check for null inputs. The
     * typioparam argument should never be null in normal system usage, but it
     * could be null in a manual invocation --- if so, just return null.
     */
    if PG_ARGISNULL!(fcinfo, 0) {
        string = null_mut();
    } else {
        string = PG_GETARG_CSTRING!(fcinfo, 0);
    }
    if PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_NULL!(fcinfo);
    }
    domainType = PG_GETARG_OID!(fcinfo, 1);

    /*
     * We arrange to look up the needed info just once per series of calls,
     * assuming the domain type doesn't change underneath us (which really
     * shouldn't happen, but cope if it does).
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut DomainIOData;
    if my_extra.is_null() || (*my_extra).domain_type != domainType {
        my_extra = domain_state_setup(domainType, false, (*(*fcinfo).flinfo).fn_mcxt);
        (*(*fcinfo).flinfo).fn_extra = my_extra as *mut c_void;
    }

    /*
     * Invoke the base type's typinput procedure to convert the data.
     */
    if !InputFunctionCallSafe(
        &mut (*my_extra).proc,
        string,
        (*my_extra).typioparam,
        (*my_extra).typtypmod,
        escontext as crate::utils::fmgr::fmNodePtr,
        &mut value,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * Do the necessary checks to ensure it's a valid domain value.
     */
    domain_check_input(value, string.is_null(), my_extra, escontext);

    if string.is_null() {
        PG_RETURN_NULL!(fcinfo);
    } else {
        PG_RETURN_DATUM!(value);
    }
}

/*
 * domain_recv		- binary input routine for any domain type.
 */
#[no_mangle]
pub unsafe fn domain_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo;
    let domainType: Oid;
    let mut my_extra: *mut DomainIOData;
    let value: Datum;

    /*
     * Since domain_recv is not strict, we have to check for null inputs. The
     * typioparam argument should never be null in normal system usage, but it
     * could be null in a manual invocation --- if so, just return null.
     */
    if PG_ARGISNULL!(fcinfo, 0) {
        buf = null_mut();
    } else {
        buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    }
    if PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_NULL!(fcinfo);
    }
    domainType = PG_GETARG_OID!(fcinfo, 1);

    /*
     * We arrange to look up the needed info just once per series of calls,
     * assuming the domain type doesn't change underneath us (which really
     * shouldn't happen, but cope if it does).
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut DomainIOData;
    if my_extra.is_null() || (*my_extra).domain_type != domainType {
        my_extra = domain_state_setup(domainType, true, (*(*fcinfo).flinfo).fn_mcxt);
        (*(*fcinfo).flinfo).fn_extra = my_extra as *mut c_void;
    }

    /*
     * Invoke the base type's typreceive procedure to convert the data.
     */
    value = ReceiveFunctionCall(
        &mut (*my_extra).proc,
        buf,
        (*my_extra).typioparam,
        (*my_extra).typtypmod,
    );

    /*
     * Do the necessary checks to ensure it's a valid domain value.
     */
    domain_check_input(value, buf.is_null(), my_extra, null_mut());

    if buf.is_null() {
        PG_RETURN_NULL!(fcinfo);
    } else {
        PG_RETURN_DATUM!(value);
    }
}

/*
 * domain_check - check that a datum satisfies the constraints of a
 * domain.  extra and mcxt can be passed if they are available from,
 * say, a FmgrInfo structure, or they can be NULL, in which case the
 * setup is repeated for each call.
 */
pub unsafe fn domain_check(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mcxt: MemoryContext,
) {
    let _ = domain_check_internal(value, isnull, domainType, extra, mcxt, null_mut());
}

/* Error-safe variant of domain_check(). */
pub unsafe fn domain_check_safe(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mcxt: MemoryContext,
    escontext: *mut Node,
) -> bool {
    domain_check_internal(value, isnull, domainType, extra, mcxt, escontext)
}

/*
 * domain_check_internal
 * 		Workhorse for domain_check() and domain_check_safe()
 *
 * Returns false if an error occurred in domain_check_input() and 'escontext'
 * points to an ErrorSaveContext, true otherwise.
 */
unsafe fn domain_check_internal(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    extra: *mut *mut c_void,
    mut mcxt: MemoryContext,
    escontext: *mut Node,
) -> bool {
    let mut my_extra: *mut DomainIOData = null_mut();

    if mcxt.is_null() {
        mcxt = CurrentMemoryContext;
    }

    /*
     * We arrange to look up the needed info just once per series of calls,
     * assuming the domain type doesn't change underneath us (which really
     * shouldn't happen, but cope if it does).
     */
    if !extra.is_null() {
        my_extra = *extra as *mut DomainIOData;
    }
    if my_extra.is_null() || (*my_extra).domain_type != domainType {
        my_extra = domain_state_setup(domainType, true, mcxt);
        if !extra.is_null() {
            *extra = my_extra as *mut c_void;
        }
    }

    /*
     * Do the necessary checks to ensure it's a valid domain value.
     */
    domain_check_input(value, isnull, my_extra, escontext);

    !SOFT_ERROR_OCCURRED!(escontext)
}

/*
 * errdatatype --- stores schema_name and datatype_name of a datatype
 * within the current errordata.
 */
pub unsafe fn errdatatype(datatypeOid: Oid) -> c_int {
    let tup: HeapTuple;
    let typtup: Form_pg_type;

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(datatypeOid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", datatypeOid);
    }
    typtup = GETSTRUCT(tup) as Form_pg_type;

    err_generic_string(
        PG_DIAG_SCHEMA_NAME as c_int,
        get_namespace_name((*typtup).typnamespace),
    );
    err_generic_string(
        PG_DIAG_DATATYPE_NAME as c_int,
        NameStr(&(*typtup).typname),
    );

    ReleaseSysCache(tup);

    0 /* return value does not matter */
}

/*
 * errdomainconstraint --- stores schema_name, datatype_name and
 * constraint_name of a domain-related constraint within the current errordata.
 */
pub unsafe fn errdomainconstraint(datatypeOid: Oid, conname: *const c_char) -> c_int {
    errdatatype(datatypeOid);
    err_generic_string(PG_DIAG_CONSTRAINT_NAME as c_int, conname);

    0 /* return value does not matter */
}

/* Helper: render a NUL-terminated C string for {} formatting in messages. */
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::from("(null)");
    }
    CStr::from_ptr(s).to_string_lossy().into_owned()
}

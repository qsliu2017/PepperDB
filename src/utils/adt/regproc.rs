//! regproc.rs
//!   Functions for the built-in types regproc, regclass, regtype, etc.
//! Translated 1:1 from postgres/src/backend/utils/adt/regproc.c
//!
//! These types are all binary-compatible with type Oid, and rely on Oid
//! for comparison and so forth.  Their only interesting behavior is in
//! special I/O conversion routines.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include <ctype.h>
//!   #include "access/htup_details.h"
//!   #include "catalog/namespace.h"
//!   #include "catalog/pg_class.h"
//!   #include "catalog/pg_collation.h"
//!   #include "catalog/pg_operator.h"
//!   #include "catalog/pg_proc.h"
//!   #include "catalog/pg_ts_config.h"
//!   #include "catalog/pg_ts_dict.h"
//!   #include "catalog/pg_type.h"
//!   #include "lib/stringinfo.h"
//!   #include "mb/pg_wchar.h"
//!   #include "miscadmin.h"
//!   #include "nodes/miscnodes.h"
//!   #include "parser/parse_type.h"
//!   #include "parser/scansup.h"
//!   #include "utils/acl.h"
//!   #include "utils/builtins.h"
//!   #include "utils/lsyscache.h"
//!   #include "utils/regproc.h"
//!   #include "utils/syscache.h"
//!   #include "utils/varlena.h"
//!
//! REAL (dependencies translated):
//!   * SearchSysCache1 / ReleaseSysCache / GETSTRUCT / HeapTupleIsValid
//!     (utils/cache/syscache.c, access/htup_details.h)
//!   * the catalog Form_pg_* structs (catalog/pg_proc.h, pg_operator.h, ...)
//!   * parseTypeString (parser/parse_type.c)
//!   * DirectInputFunctionCallSafe, oidin, oidrecv, oidsend (utils/fmgr.c,
//!     utils/adt/oid.c)
//!   * quote_qualified_identifier / quote_identifier / format_type_be /
//!     format_type_be_qualified (utils/builtins.c, utils/adt/format_type.c)
//!   * scanner_isspace (parser/scansup.c), IsBootstrapProcessingMode (miscadmin.h)
//!   * GetUserNameFromId (miscadmin.h, currently itself a stub)
//!   * StringInfo machinery (lib/stringinfo.c), list/value helpers (nodes/*)
//!
//! Stubbed (dependencies not yet translated -- minimal local stubs with
//! TODO(pg-port) markers):
//!   * catalog/namespace.c: FuncnameGetCandidates, OpernameGetCandidates,
//!     OpernameGetOprid, makeRangeVarFromNameList, RangeVarGetRelid,
//!     NameListToString, get_namespace_name, get_namespace_name_or_temp,
//!     FunctionIsVisible/OperatorIsVisible/RelationIsVisible/
//!     CollationIsVisible/TSConfigIsVisible/TSDictionaryIsVisible
//!   * utils/lsyscache.c: get_collation_oid, get_ts_config_oid, get_ts_dict_oid,
//!     get_namespace_oid
//!   * utils/acl.c: get_role_oid
//!   * mb/pg_wchar.h: GetDatabaseEncodingName
//!   * utils/varlena.c: SplitIdentifierString, textToQualifiedNameList
//!   * the syscache identifier enum (PROCOID/OPEROID/...) from
//!     catalog/syscache_ids.h (generated; declared locally as 0 per the
//!     established port convention)
//!   * pg_proc.proargtypes (CATALOG_VARLEN; not a fixed field of the ported
//!     FormData_pg_proc) -- accessed through a stub helper

use crate::prelude::*; // postgres.h: Datum, Oid, InvalidOid, palloc/pstrdup, ereport!/errmsg!/elog!, OidIsValid, ObjectIdGetDatum, NameStr, NAMEDATALEN, ...
use crate::utils::fmgr::{DirectInputFunctionCallSafe, FunctionCallInfo, PGFunction}; // utils/fmgr.h
// The PG_GETARG_*!/PG_RETURN_*! helpers are #[macro_export] macro_rules! at the
// crate root and must be imported by name.
use crate::{
    PG_GETARG_CSTRING, PG_GETARG_OID, PG_GETARG_TEXT_PP, PG_RETURN_CSTRING, PG_RETURN_DATUM,
    PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_OID,
};

use core::ffi::{c_char, c_int, c_void};

use crate::c::{bits16, int32}; // c.h
use crate::pg_config_manual::{FUNC_MAX_ARGS, NAMEDATALEN}; // pg_config_manual.h

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT}; // access/htup_details.h
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfoData,
}; // lib/stringinfo.h
use crate::nodes::nodes::{Node, NodeTag}; // nodes/nodes.h
use crate::nodes::miscnodes::ErrorSaveContext; // nodes/miscnodes.h
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_free, list_length, List}; // nodes/pg_list.h
use crate::nodes::primnodes::RangeVar; // nodes/primnodes.h
use crate::nodes::value::{makeString, String}; // nodes/value.h
use crate::{current_cell, foreach, list_make1, list_make2, strVal}; // list/value accessor macros
use crate::{appendStringInfo}; // lib/stringinfo.h

use crate::catalog::pg_class::Form_pg_class; // catalog/pg_class.h
use crate::catalog::pg_collation::Form_pg_collation; // catalog/pg_collation.h
use crate::catalog::pg_operator::Form_pg_operator; // catalog/pg_operator.h
use crate::catalog::pg_proc::Form_pg_proc; // catalog/pg_proc.h
use crate::catalog::pg_ts_config::Form_pg_ts_config; // catalog/pg_ts_config.h
use crate::catalog::pg_ts_dict::Form_pg_ts_dict; // catalog/pg_ts_dict.h
use crate::catalog::pg_type::Form_pg_type; // catalog/pg_type.h

use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1}; // utils/syscache.h
use crate::parser::parse_type::parseTypeString; // parser/parse_type.h
use crate::parser::scansup::scanner_isspace; // parser/scansup.h
use crate::miscadmin::{GetUserNameFromId, IsBootstrapProcessingMode}; // miscadmin.h
use crate::utils::builtins::{quote_identifier, quote_qualified_identifier}; // utils/builtins.h
use crate::utils::adt::format_type::{format_type_be, format_type_be_qualified}; // utils/builtins.h (format_type.c)
use crate::utils::adt::oid::{oidin, oidrecv, oidsend}; // utils/adt/oid.c
use crate::storage::lockdefs::NoLock; // storage/lock.h (NoLock)

extern "C" {
    fn snprintf(buf: *mut c_char, size: Size, fmt: *const c_char, ...) -> c_int;
    fn sprintf(buf: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> Size;
    fn strspn(s: *const c_char, accept: *const c_char) -> Size;
    fn memcmp(a: *const c_void, b: *const c_void, n: Size) -> c_int;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int; // port/pgstrcasecmp.c
}

/* RegProcedure is just an Oid alias (access/transam.h / postgres_ext.h). */
type RegProcedure = Oid;

/*
 * SysCacheIdentifier values used here.  These come from the genbki-generated
 * catalog/syscache_ids.h, which is not yet ported; declared locally as 0 per
 * the established convention (see parser/parse_type.rs).
 * TODO(pg-port): catalog/syscache_ids.h (generated).
 */
const PROCOID: c_int = 0;
const OPEROID: c_int = 0;
const RELOID: c_int = 0;
const COLLOID: c_int = 0;
const TYPEOID: c_int = 0;
const TSCONFIGOID: c_int = 0;
const TSDICTOID: c_int = 0;

/* T_ErrorSaveContext node tag (nodes/nodes.h). */
const T_ErrorSaveContext: NodeTag = NodeTag::T_ErrorSaveContext;

/* errcode classifications (utils/errcodes.h).  The errcode() shim ignores the
 * value, so these are placeholders.  Per port convention, ereport! carries only
 * (level, errmsg!), and errcode/errdetail/errhint are dropped. */
// TODO(pg-port): ERRCODE_* from utils/errcodes.h.

/* format_procedure / format_operator flag bits (utils/regproc.h). */
const FORMAT_PROC_INVALID_AS_NULL: bits16 = 0x01;
const FORMAT_PROC_FORCE_QUALIFY: bits16 = 0x02;
const FORMAT_OPERATOR_INVALID_AS_NULL: bits16 = 0x01;
const FORMAT_OPERATOR_FORCE_QUALIFY: bits16 = 0x02;

/*
 * FuncCandidateList (catalog/namespace.h).  The full struct also carries
 * pathpos / nominalnargs / nvargs / ndargs / argnumbers; we only reference the
 * fields used here (next, oid, args).
 * TODO(pg-port): real _FuncCandidateList from catalog/namespace.h.
 */
#[repr(C)]
pub struct _FuncCandidateList {
    pub next: *mut _FuncCandidateList,
    pub oid: Oid,
    pub args: [Oid; 1], /* FLEXIBLE_ARRAY_MEMBER */
}
pub type FuncCandidateList = *mut _FuncCandidateList;

/*
 * Local stubs for catalog/namespace.c, utils/lsyscache.c, utils/acl.c,
 * utils/varlena.c, mb/pg_wchar.h, and the pg_proc.proargtypes accessor.
 * These dependencies are not yet translated.
 */

/* catalog/namespace.c */
// TODO(pg-port): FuncnameGetCandidates lives in catalog/namespace.c
unsafe fn FuncnameGetCandidates(
    _names: *mut List,
    _nargs: c_int,
    _argnames: *mut List,
    _expand_variadic: bool,
    _expand_defaults: bool,
    _include_out_arguments: bool,
    _missing_ok: bool,
) -> FuncCandidateList {
    unimplemented!("FuncnameGetCandidates: catalog/namespace.c not yet translated")
}
// TODO(pg-port): OpernameGetCandidates lives in catalog/namespace.c
unsafe fn OpernameGetCandidates(
    _names: *mut List,
    _oprkind: c_char,
    _missing_schema_ok: bool,
) -> FuncCandidateList {
    unimplemented!("OpernameGetCandidates: catalog/namespace.c not yet translated")
}
// TODO(pg-port): OpernameGetOprid lives in catalog/namespace.c
unsafe fn OpernameGetOprid(_names: *mut List, _oprleft: Oid, _oprright: Oid) -> Oid {
    unimplemented!("OpernameGetOprid: catalog/namespace.c not yet translated")
}
// TODO(pg-port): makeRangeVarFromNameList lives in catalog/namespace.c
unsafe fn makeRangeVarFromNameList(_names: *mut List) -> *mut RangeVar {
    unimplemented!("makeRangeVarFromNameList: catalog/namespace.c not yet translated")
}
// TODO(pg-port): RangeVarGetRelid is RangeVarGetRelidExtended(...) in catalog/namespace.c
unsafe fn RangeVarGetRelid(_relation: *mut RangeVar, _lockmode: c_int, _missing_ok: bool) -> Oid {
    unimplemented!("RangeVarGetRelid: catalog/namespace.c not yet translated")
}
// TODO(pg-port): NameListToString lives in catalog/namespace.c
unsafe fn NameListToString(_names: *mut List) -> *mut c_char {
    unimplemented!("NameListToString: catalog/namespace.c not yet translated")
}
// TODO(pg-port): get_namespace_name lives in utils/lsyscache.c
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!("get_namespace_name: utils/lsyscache.c not yet translated")
}
// TODO(pg-port): get_namespace_name_or_temp lives in catalog/namespace.c
unsafe fn get_namespace_name_or_temp(_nspid: Oid) -> *mut c_char {
    unimplemented!("get_namespace_name_or_temp: catalog/namespace.c not yet translated")
}
// TODO(pg-port): *IsVisible lives in catalog/namespace.c
unsafe fn FunctionIsVisible(_funcid: Oid) -> bool {
    unimplemented!("FunctionIsVisible: catalog/namespace.c not yet translated")
}
unsafe fn OperatorIsVisible(_oprid: Oid) -> bool {
    unimplemented!("OperatorIsVisible: catalog/namespace.c not yet translated")
}
unsafe fn RelationIsVisible(_relid: Oid) -> bool {
    unimplemented!("RelationIsVisible: catalog/namespace.c not yet translated")
}
unsafe fn CollationIsVisible(_collid: Oid) -> bool {
    unimplemented!("CollationIsVisible: catalog/namespace.c not yet translated")
}
unsafe fn TSConfigIsVisible(_cfgid: Oid) -> bool {
    unimplemented!("TSConfigIsVisible: catalog/namespace.c not yet translated")
}
unsafe fn TSDictionaryIsVisible(_dictid: Oid) -> bool {
    unimplemented!("TSDictionaryIsVisible: catalog/namespace.c not yet translated")
}

/* utils/lsyscache.c */
// TODO(pg-port): get_collation_oid lives in catalog/namespace.c (declared in lsyscache.h area)
unsafe fn get_collation_oid(_name: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!("get_collation_oid: catalog/namespace.c not yet translated")
}
// TODO(pg-port): get_ts_config_oid lives in catalog/namespace.c
unsafe fn get_ts_config_oid(_names: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!("get_ts_config_oid: catalog/namespace.c not yet translated")
}
// TODO(pg-port): get_ts_dict_oid lives in catalog/namespace.c
unsafe fn get_ts_dict_oid(_names: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!("get_ts_dict_oid: catalog/namespace.c not yet translated")
}
// TODO(pg-port): get_namespace_oid lives in catalog/namespace.c
unsafe fn get_namespace_oid(_nspname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!("get_namespace_oid: catalog/namespace.c not yet translated")
}

/* utils/acl.c */
// TODO(pg-port): get_role_oid lives in utils/adt/acl.c
unsafe fn get_role_oid(_rolename: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!("get_role_oid: utils/adt/acl.c not yet translated")
}

/* mb/pg_wchar.h */
// TODO(pg-port): GetDatabaseEncodingName lives in mb/mbutils.c
unsafe fn GetDatabaseEncodingName() -> *const c_char {
    unimplemented!("GetDatabaseEncodingName: mb/mbutils.c not yet translated")
}

/* utils/varlena.c */
// TODO(pg-port): SplitIdentifierString lives in utils/adt/varlena.c
unsafe fn SplitIdentifierString(
    _rawstring: *mut c_char,
    _separator: c_char,
    _namelist: *mut *mut List,
) -> bool {
    unimplemented!("SplitIdentifierString: utils/adt/varlena.c not yet translated")
}
// TODO(pg-port): text_to_cstring lives in utils/adt/varlena.c
unsafe fn text_to_cstring(_t: *const crate::c::text) -> *mut c_char {
    unimplemented!("text_to_cstring: utils/adt/varlena.c not yet translated")
}
// TODO(pg-port): textToQualifiedNameList lives in utils/adt/varlena.c
unsafe fn textToQualifiedNameList(_textval: *const crate::c::text) -> *mut List {
    unimplemented!("textToQualifiedNameList: utils/adt/varlena.c not yet translated")
}

/*
 * pg_proc.proargtypes is an oidvector that lives beyond the CATALOG_VARLEN
 * cutoff of FormData_pg_proc, so it is not a fixed field of the ported struct.
 * This helper stands in for `procform->proargtypes.values`.
 * (oidvector layout: ArrayType header followed by Oid values[].)
 * TODO(pg-port): pg_proc.proargtypes (CATALOG_VARLEN) accessor not ported.
 */
unsafe fn pg_proc_proargtypes_values(_procform: Form_pg_proc) -> *const Oid {
    unimplemented!("pg_proc.proargtypes (CATALOG_VARLEN) accessor not ported")
}

/*
 * ereturn(escontext, dummy, ...) - report a soft error if escontext is an
 * ErrorSaveContext, else throw.  The current elog shim has no soft-error path
 * (errcode/errdetail/errhint are dropped per port convention), so this always
 * reports a hard ERROR via ereport!.  The `dummy` return value is what C would
 * return on the soft path; we keep the same shape for faithfulness.
 *
 * Callers invoke this then return the dummy themselves (matching the C macro,
 * which expands to a `return dummy;`).
 */

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 * regprocin		- converts "proname" to proc OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_proc entry.
 */
pub unsafe fn regprocin(fcinfo: FunctionCallInfo) -> Datum {
    let pro_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: RegProcedure = InvalidOid;
    let names: *mut List;
    let clist: FuncCandidateList;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(pro_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* Else it's a name, possibly schema-qualified */

    /*
     * We should never get here in bootstrap mode, as all references should
     * have been resolved by genbki.pl.
     */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regproc values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_proc entries in the current search path.
     */
    names = stringToQualifiedNameList(pro_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    clist = FuncnameGetCandidates(names, -1, null_mut(), false, false, false, true);

    if clist.is_null() {
        /* ereturn(escontext, (Datum) 0, errmsg("function \"%s\" does not exist", ...)) */
        ereport!(
            ERROR,
            errmsg!(
                "function \"{}\" does not exist",
                std::ffi::CStr::from_ptr(pro_name_or_oid).to_string_lossy()
            )
        );
        return 0 as Datum;
    } else if !(*clist).next.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "more than one function named \"{}\"",
                std::ffi::CStr::from_ptr(pro_name_or_oid).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    result = (*clist).oid;

    PG_RETURN_OID!(result);
}

/*
 * to_regproc	- converts "proname" to proc OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regproc(fcinfo: FunctionCallInfo) -> Datum {
    let pro_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regprocin,
        pro_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * regprocout		- converts proc OID to "pro_name"
 */
pub unsafe fn regprocout(fcinfo: FunctionCallInfo) -> Datum {
    let proid: RegProcedure = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let proctup: HeapTuple;

    if proid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(proid));

    if HeapTupleIsValid(proctup) {
        let procform: Form_pg_proc = GETSTRUCT(proctup) as Form_pg_proc;
        let proname: *mut c_char = NameStr(&(*procform).proname) as *mut c_char;

        /*
         * In bootstrap mode, skip the fancy namespace stuff and just return
         * the proc name.  (This path is only needed for debugging output
         * anyway.)
         */
        if IsBootstrapProcessingMode() {
            result = pstrdup(proname);
        } else {
            let nspname: *mut c_char;
            let clist: FuncCandidateList;

            /*
             * Would this proc be found (uniquely!) by regprocin? If not,
             * qualify it.
             */
            clist = FuncnameGetCandidates(
                list_make1!(makeString(proname) as *mut c_void),
                -1,
                null_mut(),
                false,
                false,
                false,
                false,
            );
            if !clist.is_null() && (*clist).next.is_null() && (*clist).oid == proid {
                nspname = null_mut();
            } else {
                nspname = get_namespace_name((*procform).pronamespace);
            }

            result = quote_qualified_identifier(nspname, proname) as *mut c_char;
        }

        ReleaseSysCache(proctup);
    } else {
        /* If OID doesn't match any pg_proc entry, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), proid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regprocrecv			- converts external binary format to regproc
 */
pub unsafe fn regprocrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regprocsend			- converts regproc to binary format
 */
pub unsafe fn regprocsend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regprocedurein		- converts "proname(args)" to proc OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_proc entry.
 */
pub unsafe fn regprocedurein(fcinfo: FunctionCallInfo) -> Datum {
    let pro_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: RegProcedure = InvalidOid;
    let mut names: *mut List = null_mut();
    let mut nargs: c_int = 0;
    let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
    let mut clist: FuncCandidateList;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(pro_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regprocedure values must be OIDs in bootstrap mode");
    }

    /*
     * Else it's a name and arguments.  Parse the name and arguments, look up
     * potential matches in the current namespace search list, and scan to see
     * which one exactly matches the given argument types.  (There will not be
     * more than one match.)
     */
    if !parseNameAndArgTypes(
        pro_name_or_oid,
        false,
        &mut names,
        &mut nargs,
        argtypes.as_mut_ptr(),
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    clist = FuncnameGetCandidates(names, nargs, null_mut(), false, false, false, true);

    while !clist.is_null() {
        if memcmp(
            (*clist).args.as_ptr() as *const c_void,
            argtypes.as_ptr() as *const c_void,
            (nargs as Size) * core::mem::size_of::<Oid>(),
        ) == 0
        {
            break;
        }
        clist = (*clist).next;
    }

    if clist.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "function \"{}\" does not exist",
                std::ffi::CStr::from_ptr(pro_name_or_oid).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    result = (*clist).oid;

    PG_RETURN_OID!(result);
}

/*
 * to_regprocedure	- converts "proname(args)" to proc OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regprocedure(fcinfo: FunctionCallInfo) -> Datum {
    let pro_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regprocedurein,
        pro_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * format_procedure		- converts proc OID to "pro_name(args)"
 *
 * This exports the useful functionality of regprocedureout for use
 * in other backend modules.  The result is a palloc'd string.
 */
pub unsafe fn format_procedure(procedure_oid: Oid) -> *mut c_char {
    format_procedure_extended(procedure_oid, 0)
}

pub unsafe fn format_procedure_qualified(procedure_oid: Oid) -> *mut c_char {
    format_procedure_extended(procedure_oid, FORMAT_PROC_FORCE_QUALIFY)
}

/*
 * format_procedure_extended - converts procedure OID to "pro_name(args)"
 *
 * This exports the useful functionality of regprocedureout for use
 * in other backend modules.  The result is a palloc'd string, or NULL.
 *
 * Routine to produce regprocedure names; see format_procedure above.
 *
 * The following bits in 'flags' modify the behavior:
 * - FORMAT_PROC_INVALID_AS_NULL
 *			if the procedure OID is invalid or unknown, return NULL instead
 *			of the numeric OID.
 * - FORMAT_PROC_FORCE_QUALIFY
 *			always schema-qualify procedure names, regardless of search_path
 */
pub unsafe fn format_procedure_extended(procedure_oid: Oid, flags: bits16) -> *mut c_char {
    let result: *mut c_char;
    let proctup: HeapTuple;

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procedure_oid));

    if HeapTupleIsValid(proctup) {
        let procform: Form_pg_proc = GETSTRUCT(proctup) as Form_pg_proc;
        let proname: *mut c_char = NameStr(&(*procform).proname) as *mut c_char;
        let nargs: c_int = (*procform).pronargs as c_int;
        let mut i: c_int;
        let nspname: *mut c_char;
        let mut buf: StringInfoData = std::mem::zeroed();

        /* XXX no support here for bootstrap mode */
        Assert!(!IsBootstrapProcessingMode());

        initStringInfo(&mut buf);

        /*
         * Would this proc be found (given the right args) by regprocedurein?
         * If not, or if caller requests it, we need to qualify it.
         */
        if (flags & FORMAT_PROC_FORCE_QUALIFY) == 0 && FunctionIsVisible(procedure_oid) {
            nspname = null_mut();
        } else {
            nspname = get_namespace_name((*procform).pronamespace);
        }

        appendStringInfo!(
            &mut buf,
            "{}(",
            std::ffi::CStr::from_ptr(quote_qualified_identifier(nspname, proname)).to_string_lossy()
        );
        i = 0;
        while i < nargs {
            let thisargtype: Oid = *pg_proc_proargtypes_values(procform).add(i as usize);

            if i > 0 {
                appendStringInfoChar(&mut buf, b',' as c_char);
            }
            appendStringInfoString(
                &mut buf,
                if (flags & FORMAT_PROC_FORCE_QUALIFY) != 0 {
                    format_type_be_qualified(thisargtype)
                } else {
                    format_type_be(thisargtype)
                },
            );
            i += 1;
        }
        appendStringInfoChar(&mut buf, b')' as c_char);

        result = buf.data;

        ReleaseSysCache(proctup);
    } else if (flags & FORMAT_PROC_INVALID_AS_NULL) != 0 {
        /* If object is undefined, return NULL as wanted by caller */
        result = null_mut();
    } else {
        /* If OID doesn't match any pg_proc entry, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), procedure_oid);
    }

    result
}

/*
 * Output an objname/objargs representation for the procedure with the
 * given OID.  If it doesn't exist, an error is thrown.
 *
 * This can be used to feed get_object_address.
 */
pub unsafe fn format_procedure_parts(
    procedure_oid: Oid,
    objnames: *mut *mut List,
    objargs: *mut *mut List,
    missing_ok: bool,
) {
    let proctup: HeapTuple;
    let procform: Form_pg_proc;
    let nargs: c_int;
    let mut i: c_int;

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procedure_oid));

    if !HeapTupleIsValid(proctup) {
        if !missing_ok {
            elog!(
                ERROR,
                "cache lookup failed for procedure with OID {}",
                procedure_oid
            );
        }
        return;
    }

    procform = GETSTRUCT(proctup) as Form_pg_proc;
    nargs = (*procform).pronargs as c_int;

    *objnames = list_make2!(
        get_namespace_name_or_temp((*procform).pronamespace) as *mut c_void,
        pstrdup(NameStr(&(*procform).proname)) as *mut c_void
    );
    *objargs = null_mut();
    i = 0;
    while i < nargs {
        let thisargtype: Oid = *pg_proc_proargtypes_values(procform).add(i as usize);

        *objargs = lappend(*objargs, format_type_be_qualified(thisargtype) as *mut c_void);
        i += 1;
    }

    ReleaseSysCache(proctup);
}

/*
 * regprocedureout		- converts proc OID to "pro_name(args)"
 */
pub unsafe fn regprocedureout(fcinfo: FunctionCallInfo) -> Datum {
    let proid: RegProcedure = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;

    if proid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
    } else {
        result = format_procedure(proid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regprocedurerecv			- converts external binary format to regprocedure
 */
pub unsafe fn regprocedurerecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regproceduresend			- converts regprocedure to binary format
 */
pub unsafe fn regproceduresend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regoperin		- converts "oprname" to operator OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '0' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_operator entry.
 */
pub unsafe fn regoperin(fcinfo: FunctionCallInfo) -> Datum {
    let opr_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;
    let clist: FuncCandidateList;

    /* Handle "0" or numeric OID */
    if parseNumericOid(opr_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* Else it's a name, possibly schema-qualified */

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regoper values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_operator entries in the current search path.
     */
    names = stringToQualifiedNameList(opr_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    clist = OpernameGetCandidates(names, b'\0' as c_char, true);

    if clist.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "operator does not exist: {}",
                std::ffi::CStr::from_ptr(opr_name_or_oid).to_string_lossy()
            )
        );
        return 0 as Datum;
    } else if !(*clist).next.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "more than one operator named {}",
                std::ffi::CStr::from_ptr(opr_name_or_oid).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    result = (*clist).oid;

    PG_RETURN_OID!(result);
}

/*
 * to_regoper		- converts "oprname" to operator OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regoper(fcinfo: FunctionCallInfo) -> Datum {
    let opr_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regoperin,
        opr_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * regoperout		- converts operator OID to "opr_name"
 */
pub unsafe fn regoperout(fcinfo: FunctionCallInfo) -> Datum {
    let oprid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let opertup: HeapTuple;

    if oprid == InvalidOid {
        result = pstrdup(c"0".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(oprid));

    if HeapTupleIsValid(opertup) {
        let operform: Form_pg_operator = GETSTRUCT(opertup) as Form_pg_operator;
        let oprname: *mut c_char = NameStr(&(*operform).oprname) as *mut c_char;

        /*
         * In bootstrap mode, skip the fancy namespace stuff and just return
         * the oper name.  (This path is only needed for debugging output
         * anyway.)
         */
        if IsBootstrapProcessingMode() {
            result = pstrdup(oprname);
        } else {
            let clist: FuncCandidateList;

            /*
             * Would this oper be found (uniquely!) by regoperin? If not,
             * qualify it.
             */
            clist = OpernameGetCandidates(
                list_make1!(makeString(oprname) as *mut c_void),
                b'\0' as c_char,
                false,
            );
            if !clist.is_null() && (*clist).next.is_null() && (*clist).oid == oprid {
                result = pstrdup(oprname);
            } else {
                let mut nspname: *const c_char;

                nspname = get_namespace_name((*operform).oprnamespace);
                nspname = quote_identifier(nspname);
                result = palloc(strlen(nspname) + strlen(oprname) + 2) as *mut c_char;
                sprintf(result, c"%s.%s".as_ptr(), nspname, oprname);
            }
        }

        ReleaseSysCache(opertup);
    } else {
        /*
         * If OID doesn't match any pg_operator entry, return it numerically
         */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), oprid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regoperrecv			- converts external binary format to regoper
 */
pub unsafe fn regoperrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regopersend			- converts regoper to binary format
 */
pub unsafe fn regopersend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regoperatorin		- converts "oprname(args)" to operator OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '0' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_operator entry.
 */
pub unsafe fn regoperatorin(fcinfo: FunctionCallInfo) -> Datum {
    let opr_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let mut names: *mut List = null_mut();
    let mut nargs: c_int = 0;
    let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];

    /* Handle "0" or numeric OID */
    if parseNumericOid(opr_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regoperator values must be OIDs in bootstrap mode");
    }

    /*
     * Else it's a name and arguments.  Parse the name and arguments, look up
     * potential matches in the current namespace search list, and scan to see
     * which one exactly matches the given argument types.  (There will not be
     * more than one match.)
     */
    if !parseNameAndArgTypes(
        opr_name_or_oid,
        true,
        &mut names,
        &mut nargs,
        argtypes.as_mut_ptr(),
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    if nargs == 1 {
        /* errhint dropped per port convention */
        ereport!(ERROR, errmsg!("missing argument"));
        return 0 as Datum;
    }
    if nargs != 2 {
        /* errhint dropped per port convention */
        ereport!(ERROR, errmsg!("too many arguments"));
        return 0 as Datum;
    }

    result = OpernameGetOprid(names, argtypes[0], argtypes[1]);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "operator does not exist: {}",
                std::ffi::CStr::from_ptr(opr_name_or_oid).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * to_regoperator	- converts "oprname(args)" to operator OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regoperator(fcinfo: FunctionCallInfo) -> Datum {
    let opr_name_or_oid: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regoperatorin,
        opr_name_or_oid,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * format_operator_extended - converts operator OID to "opr_name(args)"
 *
 * This exports the useful functionality of regoperatorout for use
 * in other backend modules.  The result is a palloc'd string, or NULL.
 *
 * The following bits in 'flags' modify the behavior:
 * - FORMAT_OPERATOR_INVALID_AS_NULL
 *			if the operator OID is invalid or unknown, return NULL instead
 *			of the numeric OID.
 * - FORMAT_OPERATOR_FORCE_QUALIFY
 *			always schema-qualify operator names, regardless of search_path
 */
pub unsafe fn format_operator_extended(operator_oid: Oid, flags: bits16) -> *mut c_char {
    let result: *mut c_char;
    let opertup: HeapTuple;

    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operator_oid));

    if HeapTupleIsValid(opertup) {
        let operform: Form_pg_operator = GETSTRUCT(opertup) as Form_pg_operator;
        let oprname: *mut c_char = NameStr(&(*operform).oprname) as *mut c_char;
        let nspname: *mut c_char;
        let mut buf: StringInfoData = std::mem::zeroed();

        /* XXX no support here for bootstrap mode */
        Assert!(!IsBootstrapProcessingMode());

        initStringInfo(&mut buf);

        /*
         * Would this oper be found (given the right args) by regoperatorin?
         * If not, or if caller explicitly requests it, we need to qualify it.
         */
        if (flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0 || !OperatorIsVisible(operator_oid) {
            nspname = get_namespace_name((*operform).oprnamespace);
            appendStringInfo!(
                &mut buf,
                "{}.",
                std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy()
            );
        }

        appendStringInfo!(
            &mut buf,
            "{}(",
            std::ffi::CStr::from_ptr(oprname).to_string_lossy()
        );

        if (*operform).oprleft != InvalidOid {
            appendStringInfo!(
                &mut buf,
                "{},",
                std::ffi::CStr::from_ptr(if (flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0 {
                    format_type_be_qualified((*operform).oprleft)
                } else {
                    format_type_be((*operform).oprleft)
                })
                .to_string_lossy()
            );
        } else {
            appendStringInfoString(&mut buf, c"NONE,".as_ptr());
        }

        if (*operform).oprright != InvalidOid {
            appendStringInfo!(
                &mut buf,
                "{})",
                std::ffi::CStr::from_ptr(if (flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0 {
                    format_type_be_qualified((*operform).oprright)
                } else {
                    format_type_be((*operform).oprright)
                })
                .to_string_lossy()
            );
        } else {
            appendStringInfoString(&mut buf, c"NONE)".as_ptr());
        }

        result = buf.data;

        ReleaseSysCache(opertup);
    } else if (flags & FORMAT_OPERATOR_INVALID_AS_NULL) != 0 {
        /* If object is undefined, return NULL as wanted by caller */
        result = null_mut();
    } else {
        /*
         * If OID doesn't match any pg_operator entry, return it numerically
         */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), operator_oid);
    }

    result
}

pub unsafe fn format_operator(operator_oid: Oid) -> *mut c_char {
    format_operator_extended(operator_oid, 0)
}

pub unsafe fn format_operator_qualified(operator_oid: Oid) -> *mut c_char {
    format_operator_extended(operator_oid, FORMAT_OPERATOR_FORCE_QUALIFY)
}

pub unsafe fn format_operator_parts(
    operator_oid: Oid,
    objnames: *mut *mut List,
    objargs: *mut *mut List,
    missing_ok: bool,
) {
    let opertup: HeapTuple;
    let oprForm: Form_pg_operator;

    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operator_oid));
    if !HeapTupleIsValid(opertup) {
        if !missing_ok {
            elog!(
                ERROR,
                "cache lookup failed for operator with OID {}",
                operator_oid
            );
        }
        return;
    }

    oprForm = GETSTRUCT(opertup) as Form_pg_operator;
    *objnames = list_make2!(
        get_namespace_name_or_temp((*oprForm).oprnamespace) as *mut c_void,
        pstrdup(NameStr(&(*oprForm).oprname)) as *mut c_void
    );
    *objargs = null_mut();
    if (*oprForm).oprleft != InvalidOid {
        *objargs = lappend(
            *objargs,
            format_type_be_qualified((*oprForm).oprleft) as *mut c_void,
        );
    }
    if (*oprForm).oprright != InvalidOid {
        *objargs = lappend(
            *objargs,
            format_type_be_qualified((*oprForm).oprright) as *mut c_void,
        );
    }

    ReleaseSysCache(opertup);
}

/*
 * regoperatorout		- converts operator OID to "opr_name(args)"
 */
pub unsafe fn regoperatorout(fcinfo: FunctionCallInfo) -> Datum {
    let oprid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;

    if oprid == InvalidOid {
        result = pstrdup(c"0".as_ptr());
    } else {
        result = format_operator(oprid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regoperatorrecv			- converts external binary format to regoperator
 */
pub unsafe fn regoperatorrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regoperatorsend			- converts regoperator to binary format
 */
pub unsafe fn regoperatorsend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regclassin		- converts "classname" to class OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_class entry.
 */
pub unsafe fn regclassin(fcinfo: FunctionCallInfo) -> Datum {
    let class_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(class_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* Else it's a name, possibly schema-qualified */

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regclass values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_class entries in the current search path.
     */
    names = stringToQualifiedNameList(class_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    /* We might not even have permissions on this relation; don't lock it. */
    result = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * to_regclass		- converts "classname" to class OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regclass(fcinfo: FunctionCallInfo) -> Datum {
    let class_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regclassin,
        class_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * regclassout		- converts class OID to "class_name"
 */
pub unsafe fn regclassout(fcinfo: FunctionCallInfo) -> Datum {
    let classid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let classtup: HeapTuple;

    if classid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(classid));

    if HeapTupleIsValid(classtup) {
        let classform: Form_pg_class = GETSTRUCT(classtup) as Form_pg_class;
        let classname: *mut c_char = NameStr(&(*classform).relname) as *mut c_char;

        /*
         * In bootstrap mode, skip the fancy namespace stuff and just return
         * the class name.  (This path is only needed for debugging output
         * anyway.)
         */
        if IsBootstrapProcessingMode() {
            result = pstrdup(classname);
        } else {
            let nspname: *mut c_char;

            /*
             * Would this class be found by regclassin? If not, qualify it.
             */
            if RelationIsVisible(classid) {
                nspname = null_mut();
            } else {
                nspname = get_namespace_name((*classform).relnamespace);
            }

            result = quote_qualified_identifier(nspname, classname) as *mut c_char;
        }

        ReleaseSysCache(classtup);
    } else {
        /* If OID doesn't match any pg_class entry, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), classid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regclassrecv			- converts external binary format to regclass
 */
pub unsafe fn regclassrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regclasssend			- converts regclass to binary format
 */
pub unsafe fn regclasssend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regcollationin		- converts "collationname" to collation OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_collation entry.
 */
pub unsafe fn regcollationin(fcinfo: FunctionCallInfo) -> Datum {
    let collation_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(collation_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* Else it's a name, possibly schema-qualified */

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regcollation values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_collation entries in the current search path.
     */
    names = stringToQualifiedNameList(collation_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    result = get_collation_oid(names, true);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "collation \"{}\" for encoding \"{}\" does not exist",
                std::ffi::CStr::from_ptr(NameListToString(names)).to_string_lossy(),
                std::ffi::CStr::from_ptr(GetDatabaseEncodingName()).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * to_regcollation		- converts "collationname" to collation OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regcollation(fcinfo: FunctionCallInfo) -> Datum {
    let collation_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regcollationin,
        collation_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * regcollationout		- converts collation OID to "collation_name"
 */
pub unsafe fn regcollationout(fcinfo: FunctionCallInfo) -> Datum {
    let collationid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let collationtup: HeapTuple;

    if collationid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    collationtup = SearchSysCache1(COLLOID, ObjectIdGetDatum(collationid));

    if HeapTupleIsValid(collationtup) {
        let collationform: Form_pg_collation = GETSTRUCT(collationtup) as Form_pg_collation;
        let collationname: *mut c_char = NameStr(&(*collationform).collname) as *mut c_char;

        /*
         * In bootstrap mode, skip the fancy namespace stuff and just return
         * the collation name.  (This path is only needed for debugging output
         * anyway.)
         */
        if IsBootstrapProcessingMode() {
            result = pstrdup(collationname);
        } else {
            let nspname: *mut c_char;

            /*
             * Would this collation be found by regcollationin? If not,
             * qualify it.
             */
            if CollationIsVisible(collationid) {
                nspname = null_mut();
            } else {
                nspname = get_namespace_name((*collationform).collnamespace);
            }

            result = quote_qualified_identifier(nspname, collationname) as *mut c_char;
        }

        ReleaseSysCache(collationtup);
    } else {
        /* If OID doesn't match any pg_collation entry, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), collationid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regcollationrecv			- converts external binary format to regcollation
 */
pub unsafe fn regcollationrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regcollationsend			- converts regcollation to binary format
 */
pub unsafe fn regcollationsend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regtypein		- converts "typename" to type OID
 *
 * The type name can be specified using the full type syntax recognized by
 * the parser; for example, DOUBLE PRECISION and INTEGER[] will work and be
 * translated to the correct type names.  (We ignore any typmod info
 * generated by the parser, however.)
 *
 * We also accept a numeric OID, for symmetry with the output routine,
 * and for possible use in bootstrap mode.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_type entry.
 */
pub unsafe fn regtypein(fcinfo: FunctionCallInfo) -> Datum {
    let typ_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let mut typmod: int32 = 0;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(typ_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* Else it's a type name, possibly schema-qualified or decorated */

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regtype values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: invoke the full parser to deal with special cases such as
     * array syntax.  We don't need to check for parseTypeString failure,
     * since we'll just return anyway.
     */
    let _ = parseTypeString(typ_name_or_oid, &mut result, &mut typmod, escontext);

    PG_RETURN_OID!(result);
}

/*
 * to_regtype		- converts "typename" to type OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regtype(fcinfo: FunctionCallInfo) -> Datum {
    let typ_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regtypein,
        typ_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * to_regtypemod	- converts "typename" to type modifier
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regtypemod(fcinfo: FunctionCallInfo) -> Datum {
    let typ_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut typid: Oid = InvalidOid;
    let mut typmod: int32 = 0;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    /* We rely on parseTypeString to parse the input. */
    if !parseTypeString(
        typ_name,
        &mut typid,
        &mut typmod,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32!(typmod);
}

/*
 * regtypeout		- converts type OID to "typ_name"
 */
pub unsafe fn regtypeout(fcinfo: FunctionCallInfo) -> Datum {
    let typid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let typetup: HeapTuple;

    if typid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if HeapTupleIsValid(typetup) {
        let typeform: Form_pg_type = GETSTRUCT(typetup) as Form_pg_type;

        /*
         * In bootstrap mode, skip the fancy namespace stuff and just return
         * the type name.  (This path is only needed for debugging output
         * anyway.)
         */
        if IsBootstrapProcessingMode() {
            let typname: *mut c_char = NameStr(&(*typeform).typname) as *mut c_char;

            result = pstrdup(typname);
        } else {
            result = format_type_be(typid);
        }

        ReleaseSysCache(typetup);
    } else {
        /* If OID doesn't match any pg_type entry, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), typid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regtyperecv			- converts external binary format to regtype
 */
pub unsafe fn regtyperecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regtypesend			- converts regtype to binary format
 */
pub unsafe fn regtypesend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regconfigin		- converts "tsconfigname" to tsconfig OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_ts_config entry.
 */
pub unsafe fn regconfigin(fcinfo: FunctionCallInfo) -> Datum {
    let cfg_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(cfg_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regconfig values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_ts_config entries in the current search path.
     */
    names = stringToQualifiedNameList(cfg_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    result = get_ts_config_oid(names, true);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "text search configuration \"{}\" does not exist",
                std::ffi::CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * regconfigout		- converts tsconfig OID to "tsconfigname"
 */
pub unsafe fn regconfigout(fcinfo: FunctionCallInfo) -> Datum {
    let cfgid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let cfgtup: HeapTuple;

    if cfgid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    cfgtup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgid));

    if HeapTupleIsValid(cfgtup) {
        let cfgform: Form_pg_ts_config = GETSTRUCT(cfgtup) as Form_pg_ts_config;
        let cfgname: *mut c_char = NameStr(&(*cfgform).cfgname) as *mut c_char;
        let nspname: *mut c_char;

        /*
         * Would this config be found by regconfigin? If not, qualify it.
         */
        if TSConfigIsVisible(cfgid) {
            nspname = null_mut();
        } else {
            nspname = get_namespace_name((*cfgform).cfgnamespace);
        }

        result = quote_qualified_identifier(nspname, cfgname) as *mut c_char;

        ReleaseSysCache(cfgtup);
    } else {
        /* If OID doesn't match any pg_ts_config row, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), cfgid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regconfigrecv			- converts external binary format to regconfig
 */
pub unsafe fn regconfigrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regconfigsend			- converts regconfig to binary format
 */
pub unsafe fn regconfigsend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regdictionaryin		- converts "tsdictionaryname" to tsdictionary OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_ts_dict entry.
 */
pub unsafe fn regdictionaryin(fcinfo: FunctionCallInfo) -> Datum {
    let dict_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(dict_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regdictionary values must be OIDs in bootstrap mode");
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_ts_dict entries in the current search path.
     */
    names = stringToQualifiedNameList(dict_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    result = get_ts_dict_oid(names, true);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "text search dictionary \"{}\" does not exist",
                std::ffi::CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * regdictionaryout		- converts tsdictionary OID to "tsdictionaryname"
 */
pub unsafe fn regdictionaryout(fcinfo: FunctionCallInfo) -> Datum {
    let dictid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let dicttup: HeapTuple;

    if dictid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    dicttup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictid));

    if HeapTupleIsValid(dicttup) {
        let dictform: Form_pg_ts_dict = GETSTRUCT(dicttup) as Form_pg_ts_dict;
        let dictname: *mut c_char = NameStr(&(*dictform).dictname) as *mut c_char;
        let nspname: *mut c_char;

        /*
         * Would this dictionary be found by regdictionaryin? If not, qualify
         * it.
         */
        if TSDictionaryIsVisible(dictid) {
            nspname = null_mut();
        } else {
            nspname = get_namespace_name((*dictform).dictnamespace);
        }

        result = quote_qualified_identifier(nspname, dictname) as *mut c_char;

        ReleaseSysCache(dicttup);
    } else {
        /* If OID doesn't match any pg_ts_dict row, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), dictid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regdictionaryrecv	- converts external binary format to regdictionary
 */
pub unsafe fn regdictionaryrecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regdictionarysend	- converts regdictionary to binary format
 */
pub unsafe fn regdictionarysend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regrolein	- converts "rolename" to role OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_authid entry.
 */
pub unsafe fn regrolein(fcinfo: FunctionCallInfo) -> Datum {
    let role_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(role_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regrole values must be OIDs in bootstrap mode");
    }

    /* Normal case: see if the name matches any pg_authid entry. */
    names = stringToQualifiedNameList(role_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    if list_length(names) != 1 {
        ereport!(ERROR, errmsg!("invalid name syntax"));
        return 0 as Datum;
    }

    result = get_role_oid(strVal!(linitial(names)), true);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "role \"{}\" does not exist",
                std::ffi::CStr::from_ptr(strVal!(linitial(names))).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * to_regrole		- converts "rolename" to role OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regrole(fcinfo: FunctionCallInfo) -> Datum {
    let role_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regrolein,
        role_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * regroleout		- converts role OID to "role_name"
 */
pub unsafe fn regroleout(fcinfo: FunctionCallInfo) -> Datum {
    let roleoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut result: *mut c_char;

    if roleoid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    result = GetUserNameFromId(roleoid, true);

    if !result.is_null() {
        /* pstrdup is not really necessary, but it avoids a compiler warning */
        result = pstrdup(quote_identifier(result));
    } else {
        /* If OID doesn't match any role, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), roleoid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regrolerecv - converts external binary format to regrole
 */
pub unsafe fn regrolerecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regrolesend - converts regrole to binary format
 */
pub unsafe fn regrolesend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * regnamespacein		- converts "nspname" to namespace OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_namespace entry.
 */
pub unsafe fn regnamespacein(fcinfo: FunctionCallInfo) -> Datum {
    let nsp_name_or_oid: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: Oid = InvalidOid;
    let names: *mut List;

    /* Handle "-" or numeric OID */
    if parseDashOrOid(nsp_name_or_oid, &mut result, escontext) {
        PG_RETURN_OID!(result);
    }

    /* The rest of this wouldn't work in bootstrap mode */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "regnamespace values must be OIDs in bootstrap mode");
    }

    /* Normal case: see if the name matches any pg_namespace entry. */
    names = stringToQualifiedNameList(nsp_name_or_oid, escontext);
    if names.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    if list_length(names) != 1 {
        ereport!(ERROR, errmsg!("invalid name syntax"));
        return 0 as Datum;
    }

    result = get_namespace_oid(strVal!(linitial(names)), true);

    if !OidIsValid(result) {
        ereport!(
            ERROR,
            errmsg!(
                "schema \"{}\" does not exist",
                std::ffi::CStr::from_ptr(strVal!(linitial(names))).to_string_lossy()
            )
        );
        return 0 as Datum;
    }

    PG_RETURN_OID!(result);
}

/*
 * to_regnamespace		- converts "nspname" to namespace OID
 *
 * If the name is not found, we return NULL.
 */
pub unsafe fn to_regnamespace(fcinfo: FunctionCallInfo) -> Datum {
    let nsp_name: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));
    let mut result: Datum = 0 as Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = T_ErrorSaveContext;

    if !DirectInputFunctionCallSafe(
        regnamespacein,
        nsp_name,
        InvalidOid,
        -1,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        &mut result,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_DATUM!(result);
}

/*
 * regnamespaceout		- converts namespace OID to "nsp_name"
 */
pub unsafe fn regnamespaceout(fcinfo: FunctionCallInfo) -> Datum {
    let nspid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut result: *mut c_char;

    if nspid == InvalidOid {
        result = pstrdup(c"-".as_ptr());
        PG_RETURN_CSTRING!(result);
    }

    result = get_namespace_name(nspid);

    if !result.is_null() {
        /* pstrdup is not really necessary, but it avoids a compiler warning */
        result = pstrdup(quote_identifier(result));
    } else {
        /* If OID doesn't match any namespace, return it numerically */
        result = palloc(NAMEDATALEN) as *mut c_char;
        snprintf(result, NAMEDATALEN as Size, c"%u".as_ptr(), nspid);
    }

    PG_RETURN_CSTRING!(result);
}

/*
 *		regnamespacerecv	- converts external binary format to regnamespace
 */
pub unsafe fn regnamespacerecv(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidrecv, so share code */
    oidrecv(fcinfo)
}

/*
 *		regnamespacesend		- converts regnamespace to binary format
 */
pub unsafe fn regnamespacesend(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as oidsend, so share code */
    oidsend(fcinfo)
}

/*
 * text_regclass: convert text to regclass
 *
 * This could be replaced by CoerceViaIO, except that we need to treat
 * text-to-regclass as an implicit cast to support legacy forms of nextval()
 * and related functions.
 */
pub unsafe fn text_regclass(fcinfo: FunctionCallInfo) -> Datum {
    let relname: *mut crate::c::text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let result: Oid;
    let rv: *mut RangeVar;

    rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));

    /* We might not even have permissions on this relation; don't lock it. */
    result = RangeVarGetRelid(rv, NoLock, false);

    PG_RETURN_OID!(result);
}

/*
 * Given a C string, parse it into a qualified-name list.
 *
 * If escontext is an ErrorSaveContext node, invalid input will be
 * reported there instead of being thrown, and we return NIL.
 * (NIL is not possible as a success return, since empty-input is an error.)
 */
pub unsafe fn stringToQualifiedNameList(string: *const c_char, escontext: *mut Node) -> *mut List {
    let rawname: *mut c_char;
    let mut result: *mut List = null_mut();
    let mut namelist: *mut List = null_mut();

    /* We need a modifiable copy of the input string. */
    rawname = pstrdup(string);

    if !SplitIdentifierString(rawname, b'.' as c_char, &mut namelist) {
        let _ = escontext;
        ereport!(ERROR, errmsg!("invalid name syntax"));
        return null_mut();
    }

    if namelist.is_null() {
        ereport!(ERROR, errmsg!("invalid name syntax"));
        return null_mut();
    }

    foreach!(l, namelist, {
        let curname: *mut c_char = lfirst(current_cell!(l)) as *mut c_char;

        result = lappend(result, makeString(pstrdup(curname)) as *mut c_void);
    });

    pfree(rawname as *mut c_void);
    list_free(namelist);

    result
}

/*****************************************************************************
 *	 SUPPORT ROUTINES														 *
 *****************************************************************************/

/*
 * Given a C string, see if it is all-digits (and not empty).
 * If so, convert directly to OID and return true.
 * If it is not all-digits, return false.
 *
 * If escontext is an ErrorSaveContext node, any error in oidin() will be
 * reported there instead of being thrown (but we still return true).
 */
unsafe fn parseNumericOid(string: *mut c_char, result: *mut Oid, escontext: *mut Node) -> bool {
    if *string >= b'0' as c_char
        && *string <= b'9' as c_char
        && strspn(string, c"0123456789".as_ptr()) == strlen(string)
    {
        let mut oid_datum: Datum = 0 as Datum;

        /* We need not care here whether oidin() fails or not. */
        let _ = DirectInputFunctionCallSafe(oidin, string, InvalidOid, -1, escontext, &mut oid_datum);
        *result = DatumGetObjectId(oid_datum);
        return true;
    }

    /* Prevent uninitialized-variable warnings from stupider compilers. */
    *result = InvalidOid;
    false
}

/*
 * As above, but also accept "-" as meaning 0 (InvalidOid).
 */
unsafe fn parseDashOrOid(string: *mut c_char, result: *mut Oid, escontext: *mut Node) -> bool {
    /* '-' ? */
    if strcmp(string, c"-".as_ptr()) == 0 {
        *result = InvalidOid;
        return true;
    }

    /* Numeric OID? */
    parseNumericOid(string, result, escontext)
}

/*
 * Given a C string, parse it into a qualified function or operator name
 * followed by a parenthesized list of type names.  Reduce the
 * type names to an array of OIDs (returned into *nargs and *argtypes;
 * the argtypes array should be of size FUNC_MAX_ARGS).  The function or
 * operator name is returned to *names as a List of Strings.
 *
 * If allowNone is true, accept "NONE" and return it as InvalidOid (this is
 * for unary operators).
 *
 * Returns true on success, false on failure (the latter only possible
 * if escontext is an ErrorSaveContext node).
 */
unsafe fn parseNameAndArgTypes(
    string: *const c_char,
    allowNone: bool,
    names: *mut *mut List,
    nargs: *mut c_int,
    argtypes: *mut Oid,
    escontext: *mut Node,
) -> bool {
    let rawname: *mut c_char;
    let mut ptr: *mut c_char;
    let mut ptr2: *mut c_char;
    let mut typename: *mut c_char;
    let mut in_quote: bool;
    let mut had_comma: bool;
    let mut paren_count: c_int;
    let mut typeid: Oid = 0;
    let mut typmod: int32 = 0;

    /* We need a modifiable copy of the input string. */
    rawname = pstrdup(string);

    /* Scan to find the expected left paren; mustn't be quoted */
    in_quote = false;
    ptr = rawname;
    while *ptr != 0 {
        if *ptr == b'"' as c_char {
            in_quote = !in_quote;
        } else if *ptr == b'(' as c_char && !in_quote {
            break;
        }
        ptr = ptr.add(1);
    }
    if *ptr == b'\0' as c_char {
        ereport!(ERROR, errmsg!("expected a left parenthesis"));
        return false;
    }

    /* Separate the name and parse it into a list */
    *ptr = b'\0' as c_char;
    ptr = ptr.add(1);
    *names = stringToQualifiedNameList(rawname, escontext);
    if (*names).is_null() {
        return false;
    }

    /* Check for the trailing right parenthesis and remove it */
    ptr2 = ptr.add(strlen(ptr) as usize);
    loop {
        ptr2 = ptr2.offset(-1);
        if !(ptr2 > ptr) {
            break;
        }
        if !scanner_isspace(*ptr2) {
            break;
        }
    }
    if *ptr2 != b')' as c_char {
        ereport!(ERROR, errmsg!("expected a right parenthesis"));
        return false;
    }

    *ptr2 = b'\0' as c_char;

    /* Separate the remaining string into comma-separated type names */
    *nargs = 0;
    had_comma = false;

    loop {
        /* allow leading whitespace */
        while scanner_isspace(*ptr) {
            ptr = ptr.add(1);
        }
        if *ptr == b'\0' as c_char {
            /* End of string.  Okay unless we had a comma before. */
            if had_comma {
                ereport!(ERROR, errmsg!("expected a type name"));
                return false;
            }
            break;
        }
        typename = ptr;
        /* Find end of type name --- end of string or comma */
        /* ... but not a quoted or parenthesized comma */
        in_quote = false;
        paren_count = 0;
        while *ptr != 0 {
            if *ptr == b'"' as c_char {
                in_quote = !in_quote;
            } else if *ptr == b',' as c_char && !in_quote && paren_count == 0 {
                break;
            } else if !in_quote {
                match *ptr as u8 {
                    b'(' | b'[' => {
                        paren_count += 1;
                    }
                    b')' | b']' => {
                        paren_count -= 1;
                    }
                    _ => {}
                }
            }
            ptr = ptr.add(1);
        }
        if in_quote || paren_count != 0 {
            ereport!(ERROR, errmsg!("improper type name"));
            return false;
        }

        ptr2 = ptr;
        if *ptr == b',' as c_char {
            had_comma = true;
            *ptr = b'\0' as c_char;
            ptr = ptr.add(1);
        } else {
            had_comma = false;
            Assert!(*ptr == b'\0' as c_char);
        }
        /* Lop off trailing whitespace */
        loop {
            ptr2 = ptr2.offset(-1);
            if !(ptr2 >= typename) {
                break;
            }
            if !scanner_isspace(*ptr2) {
                break;
            }
            *ptr2 = b'\0' as c_char;
        }

        if allowNone && pg_strcasecmp(typename, c"none".as_ptr()) == 0 {
            /* Special case for NONE */
            typeid = InvalidOid;
            typmod = -1;
        } else {
            /* Use full parser to resolve the type name */
            if !parseTypeString(typename, &mut typeid, &mut typmod, escontext) {
                return false;
            }
        }
        if *nargs >= FUNC_MAX_ARGS as c_int {
            ereport!(ERROR, errmsg!("too many arguments"));
            return false;
        }

        *argtypes.add(*nargs as usize) = typeid;
        *nargs += 1;
    }

    pfree(rawname as *mut c_void);

    true
}

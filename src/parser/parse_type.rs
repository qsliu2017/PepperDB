//! handle type operations for parser
//!
//! src/backend/parser/parse_type.c
//! src/include/parser/parse_type.h
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid};
use crate::nodes::makefuncs::makeRangeVar;
use crate::nodes::pg_list::lfirst;
use crate::nodes::primnodes::RangeVar;
use crate::utils::array::ArrayType;


use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::parsenodes::{A_Const, ColumnDef, ColumnRef, TypeName};
use crate::nodes::pg_list::{
    linitial, list_head, list_length, lsecond, lthird, lfourth, List, ListCell, NIL,
};
use crate::nodes::value::Float as PgFloat;
use crate::nodes::value::Integer as PgInteger;
use crate::nodes::value::String as PgString;
use crate::parser::parser::{raw_parser, RawParseMode};
use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition, setup_parser_errposition_callback,
    ParseCallbackState, ParseState,
};
use crate::{
    castNode, current_cell, foreach, intVal, IsA, lfirst_node, linitial_node,
    strVal,
};

// ----------------------------------------------------------------
// parse_type.h
//
//   typedef HeapTuple Type;
// ----------------------------------------------------------------

pub type Type = HeapTuple;

/* true if typeid is composite, or domain over composite, but not RECORD */
/* #define ISCOMPLEX(typeid) (typeOrDomainTypeRelid(typeid) != InvalidOid) */
#[macro_export]
macro_rules! ISCOMPLEX {
    ($typeid:expr) => {
        $crate::parser::parse_type::typeOrDomainTypeRelid($typeid) != $crate::postgres_ext::InvalidOid
    };
}

// ----------------------------------------------------------------
// error context callback support (utils/elog.h)
// ----------------------------------------------------------------

#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub arg: *mut c_void,
}

// ----------------------------------------------------------------
// local stubs for as-yet-unported dependencies
// ----------------------------------------------------------------

unsafe fn makeRangeVar_stub(_s: *mut c_char, _r: *mut c_char, _location: c_int) -> *mut RangeVar {
    crate::nodes::makefuncs::makeRangeVar(_s as _, _r as _, _location) as _
}

extern "C" {
    static mut error_context_stack: *mut ErrorContextCallback;
    fn strspn(s: *const c_char, accept: *const c_char) -> Size;
    fn strlen(s: *const c_char) -> Size;
}

unsafe fn NameListToString(_names: *mut List) -> *mut c_char {
    crate::catalog::namespace::NameListToString(_names as _) as _
}

unsafe fn RangeVarGetRelid(_relation: *mut RangeVar, _lockmode: LOCKMODE, _missing_ok: bool) -> Oid {
    crate::catalog::namespace::RangeVarGetRelid(_relation as _, _lockmode as _, _missing_ok)
}

unsafe fn get_attnum(_relid: Oid, _attname: *const c_char) -> AttrNumber {
    crate::utils::cache::lsyscache::get_attnum(_relid, _attname)
}

unsafe fn get_atttype(_relid: Oid, _attnum: AttrNumber) -> Oid {
    crate::utils::cache::lsyscache::get_atttype(_relid, _attnum)
}

unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(_type_oid)
}

unsafe fn DeconstructQualifiedName(
    _names: *mut List,
    _nspname_p: *mut *mut c_char,
    _objname_p: *mut *mut c_char,
) {
    crate::catalog::namespace::DeconstructQualifiedName(_names, _nspname_p, _objname_p)
}

unsafe fn LookupExplicitNamespace(_nspname: *const c_char, _missing_ok: bool) -> Oid {
    crate::catalog::namespace::LookupExplicitNamespace(_nspname, _missing_ok)
}

unsafe fn GetSysCacheOid2(
    _cache_id: c_int,
    _oid_col: AttrNumber,
    _key1: Datum,
    _key2: Datum,
) -> Oid {
    crate::utils::cache::lsyscache::GetSysCacheOid2(_cache_id, _oid_col, _key1, _key2)
}

unsafe fn TypenameGetTypidExtended(_typname: *const c_char, _temp_ok: bool) -> Oid {
    crate::catalog::namespace::TypenameGetTypidExtended(_typname, _temp_ok)
}

unsafe fn get_array_type(_typid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_array_type(_typid)
}

unsafe fn SearchSysCache1(_cache_id: c_int, _key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(_cache_id, _key1)
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(_tuple)
}

unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut ArrayType {
    crate::utils::adt::arrayfuncs::construct_array_builtin(_elems, _nelems, _elmtype)
}

unsafe fn OidFunctionCall1(_functionId: Oid, _arg1: Datum) -> Datum {
    crate::utils::fmgr::OidFunctionCall1Coll(_functionId, InvalidOid, _arg1)
}

unsafe fn get_collation_oid(_collname: *mut List, _missing_ok: bool) -> Oid {
    crate::catalog::namespace::get_collation_oid(_collname, _missing_ok)
}

unsafe fn get_typcollation(_typid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_typcollation(_typid)
}

unsafe fn pstrdup(_str: *const c_char) -> *mut c_char {
    crate::utils::palloc::pstrdup(_str)
}

unsafe fn getTypeIOParam(_typeTuple: HeapTuple) -> Oid {
    crate::utils::cache::lsyscache::getTypeIOParam(_typeTuple)
}

unsafe fn OidInputFunctionCall(
    _functionId: Oid,
    _str: *mut c_char,
    _typioparam: Oid,
    _typmod: int32,
) -> Datum {
    crate::utils::fmgr::OidInputFunctionCall(_functionId, _str, _typioparam, _typmod)
}

unsafe fn psprintf_long(_v: c_long) -> *mut c_char {
    crate::utils::palloc::pstrdup(format!("{}\0", _v).as_ptr() as *const c_char)
}

pub use crate::lib::stringinfo::{StringInfoData, StringInfo, initStringInfo, appendStringInfoChar, appendStringInfoString};

pub use crate::catalog::pg_type::{FormData_pg_type, Form_pg_type};

unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void {
    crate::access::htup_details::GETSTRUCT(tup as *const _)
}

unsafe fn NameStr(name: *mut c_char) -> *mut c_char {
    name
}

// constants
const NoLock: LOCKMODE = 0;
const TYPTYPE_DOMAIN: c_char = b'd' as c_char;

const TYPENAMENSP: c_int = 81; // SysCacheIdentifier
const TYPEOID: c_int = 82; // SysCacheIdentifier
const Anum_pg_type_oid: AttrNumber = 1;

const CSTRINGOID: Oid = 2275;

type LOCKMODE = c_int;

/*
 * LookupTypeName
 *		Wrapper for typical case.
 */
pub unsafe fn LookupTypeName(
    pstate: *mut ParseState,
    typeName: *const TypeName,
    typmod_p: *mut int32,
    missing_ok: bool,
) -> Type {
    return LookupTypeNameExtended(pstate, typeName, typmod_p, true, missing_ok);
}

/*
 * LookupTypeNameExtended
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns NULL if no such type can be found.  If the type is found,
 *		the typmod value represented in the TypeName struct is computed and
 *		stored into *typmod_p.
 *
 * NB: on success, the caller must ReleaseSysCache the type tuple when done
 * with it.
 *
 * NB: direct callers of this function MUST check typisdefined before assuming
 * that the type is fully valid.  Most code should go through typenameType
 * or typenameTypeId instead.
 *
 * typmod_p can be passed as NULL if the caller does not care to know the
 * typmod value, but the typmod decoration (if any) will be validated anyway,
 * except in the case where the type is not found.  Note that if the type is
 * found but is a shell, and there is typmod decoration, an error will be
 * thrown --- this is intentional.
 *
 * If temp_ok is false, ignore types in the temporary namespace.  Pass false
 * when the caller will decide, using goodness of fit criteria, whether the
 * typeName is actually a type or something else.  If typeName always denotes
 * a type (or denotes nothing), pass true.
 *
 * pstate is only used for error location info, and may be NULL.
 */
pub unsafe fn LookupTypeNameExtended(
    pstate: *mut ParseState,
    typeName: *const TypeName,
    typmod_p: *mut int32,
    temp_ok: bool,
    missing_ok: bool,
) -> Type {
    let typoid: Oid;
    let tup: HeapTuple;
    let typmod: int32;

    if (*typeName).names == NIL {
        /* We have the OID already if it's an internally generated TypeName */
        typoid = (*typeName).typeOid;
    } else if (*typeName).pct_type {
        /* Handle %TYPE reference to type of an existing field */
        let rel: *mut RangeVar = makeRangeVar_stub(null_mut(), null_mut(), (*typeName).location);
        let mut field: *mut c_char = null_mut();
        let relid: Oid;
        let attnum: AttrNumber;

        /* deconstruct the name list */
        match list_length((*typeName).names) {
            1 => {
                ereport!(
                    ERROR,
                    "improper %TYPE reference (too few dotted names)"
                );
                parser_errposition(pstate, (*typeName).location);
                unreachable!()
            }
            2 => {
                (*rel).relname = strVal!(linitial((*typeName).names));
                field = strVal!(lsecond((*typeName).names));
            }
            3 => {
                (*rel).schemaname = strVal!(linitial((*typeName).names));
                (*rel).relname = strVal!(lsecond((*typeName).names));
                field = strVal!(lthird((*typeName).names));
            }
            4 => {
                (*rel).catalogname = strVal!(linitial((*typeName).names));
                (*rel).schemaname = strVal!(lsecond((*typeName).names));
                (*rel).relname = strVal!(lthird((*typeName).names));
                field = strVal!(lfourth((*typeName).names));
            }
            _ => {
                ereport!(
                    ERROR,
                    "improper %TYPE reference (too many dotted names)"
                );
                parser_errposition(pstate, (*typeName).location);
                unreachable!()
            }
        }

        /*
         * Look up the field.
         *
         * XXX: As no lock is taken here, this might fail in the presence of
         * concurrent DDL.  But taking a lock would carry a performance
         * penalty and would also require a permissions check.
         */
        relid = RangeVarGetRelid(rel, NoLock, missing_ok);
        attnum = get_attnum(relid, field);
        if attnum == InvalidAttrNumber {
            if missing_ok {
                typoid = InvalidOid;
            } else {
                ereport!(
                    ERROR,
                    "column of relation does not exist"
                );
                parser_errposition(pstate, (*typeName).location);
                unreachable!()
            }
        } else {
            typoid = get_atttype(relid, attnum);

            /* this construct should never have an array indicator */
            Assert!((*typeName).arrayBounds == NIL);

            /* emit nuisance notice (intentionally not errposition'd) */
            elog!(
                NOTICE,
                "type reference {} converted to {}",
                TypeNameToString(typeName) as usize,
                format_type_be(typoid) as usize
            );
        }
    } else {
        /* Normal reference to a type name */
        let mut schemaname: *mut c_char = null_mut();
        let mut typname: *mut c_char = null_mut();

        /* deconstruct the name list */
        DeconstructQualifiedName((*typeName).names, &mut schemaname, &mut typname);

        if !schemaname.is_null() {
            /* Look in specific schema only */
            let namespaceId: Oid;
            let mut pcbstate: ParseCallbackState = core::mem::zeroed();

            setup_parser_errposition_callback(&mut pcbstate, pstate, (*typeName).location);

            namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
            if OidIsValid(namespaceId) {
                typoid = GetSysCacheOid2(
                    TYPENAMENSP,
                    Anum_pg_type_oid,
                    PointerGetDatum(typname as *const c_void),
                    ObjectIdGetDatum(namespaceId),
                );
            } else {
                typoid = InvalidOid;
            }

            cancel_parser_errposition_callback(&mut pcbstate);
        } else {
            /* Unqualified type name, so search the search path */
            typoid = TypenameGetTypidExtended(typname, temp_ok);
        }

        /* If an array reference, return the array type instead */
        if (*typeName).arrayBounds != NIL {
            return_through_array(&typoid, get_array_type(typoid));
        }
    }

    if !OidIsValid(typoid) {
        if !typmod_p.is_null() {
            *typmod_p = -1;
        }
        return null_mut();
    }

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for type {}", typoid);
        unreachable!()
    }

    typmod = typenameTypeMod(pstate, typeName, tup as Type);

    if !typmod_p.is_null() {
        *typmod_p = typmod;
    }

    return tup as Type;
}

// Helper: faithful translation of the in-place reassignment
// `typoid = get_array_type(typoid)` (typoid is logically `let` but reassigned
// only in this branch in C). We model it by mutating through a pointer.
#[inline]
unsafe fn return_through_array(slot: *const Oid, newval: Oid) {
    *(slot as *mut Oid) = newval;
}

/*
 * LookupTypeNameOid
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns InvalidOid if no such type can be found.  If the type is found,
 *		return its Oid.
 *
 * NB: direct callers of this function need to be aware that the type OID
 * returned may correspond to a shell type.  Most code should go through
 * typenameTypeId instead.
 *
 * pstate is only used for error location info, and may be NULL.
 */
pub unsafe fn LookupTypeNameOid(
    pstate: *mut ParseState,
    typeName: *const TypeName,
    missing_ok: bool,
) -> Oid {
    let typoid: Oid;
    let tup: Type;

    tup = LookupTypeName(pstate, typeName, null_mut(), missing_ok);
    if tup.is_null() {
        if !missing_ok {
            ereport!(ERROR, "type does not exist");
            parser_errposition(pstate, (*typeName).location);
            unreachable!()
        }

        return InvalidOid;
    }

    typoid = (*(GETSTRUCT(tup) as Form_pg_type)).oid;
    ReleaseSysCache(tup);

    return typoid;
}

/*
 * typenameType - given a TypeName, return a Type structure and typmod
 *
 * This is equivalent to LookupTypeName, except that this will report
 * a suitable error message if the type cannot be found or is not defined.
 * Callers of this can therefore assume the result is a fully valid type.
 */
pub unsafe fn typenameType(
    pstate: *mut ParseState,
    typeName: *const TypeName,
    typmod_p: *mut int32,
) -> Type {
    let tup: Type;

    tup = LookupTypeName(pstate, typeName, typmod_p, false);
    if tup.is_null() {
        ereport!(ERROR, "type does not exist");
        parser_errposition(pstate, (*typeName).location);
        unreachable!()
    }
    if !(*(GETSTRUCT(tup) as Form_pg_type)).typisdefined {
        ereport!(ERROR, "type is only a shell");
        parser_errposition(pstate, (*typeName).location);
        unreachable!()
    }
    return tup;
}

/*
 * typenameTypeId - given a TypeName, return the type's OID
 *
 * This is similar to typenameType, but we only hand back the type OID
 * not the syscache entry.
 */
pub unsafe fn typenameTypeId(pstate: *mut ParseState, typeName: *const TypeName) -> Oid {
    let typoid: Oid;
    let tup: Type;

    tup = typenameType(pstate, typeName, null_mut());
    typoid = (*(GETSTRUCT(tup) as Form_pg_type)).oid;
    ReleaseSysCache(tup);

    return typoid;
}

/*
 * typenameTypeIdAndMod - given a TypeName, return the type's OID and typmod
 *
 * This is equivalent to typenameType, but we only hand back the type OID
 * and typmod, not the syscache entry.
 */
#[no_mangle]
pub unsafe fn typenameTypeIdAndMod(
    pstate: *mut ParseState,
    typeName: *const TypeName,
    typeid_p: *mut Oid,
    typmod_p: *mut int32,
) {
    let tup: Type;

    tup = typenameType(pstate, typeName, typmod_p);
    *typeid_p = (*(GETSTRUCT(tup) as Form_pg_type)).oid;
    ReleaseSysCache(tup);
}

/*
 * typenameTypeMod - given a TypeName, return the internal typmod value
 *
 * This will throw an error if the TypeName includes type modifiers that are
 * illegal for the data type.
 *
 * The actual type OID represented by the TypeName must already have been
 * looked up, and is passed as "typ".
 *
 * pstate is only used for error location info, and may be NULL.
 */
unsafe fn typenameTypeMod(pstate: *mut ParseState, typeName: *const TypeName, typ: Type) -> int32 {
    let result: int32;
    let typmodin: Oid;
    let datums: *mut Datum;
    let mut n: c_int;
    let arrtypmod: *mut ArrayType;
    let mut pcbstate: ParseCallbackState = core::mem::zeroed();

    /* Return prespecified typmod if no typmod expressions */
    if (*typeName).typmods == NIL {
        return (*typeName).typemod;
    }

    /*
     * Else, type had better accept typmods.  We give a special error message
     * for the shell-type case, since a shell couldn't possibly have a
     * typmodin function.
     */
    if !(*(GETSTRUCT(typ) as Form_pg_type)).typisdefined {
        ereport!(ERROR, "type modifier cannot be specified for shell type");
        parser_errposition(pstate, (*typeName).location);
        unreachable!()
    }

    typmodin = (*(GETSTRUCT(typ) as Form_pg_type)).typmodin;

    if typmodin == InvalidOid {
        ereport!(ERROR, "type modifier is not allowed for type");
        parser_errposition(pstate, (*typeName).location);
        unreachable!()
    }

    /*
     * Convert the list of raw-grammar-output expressions to a cstring array.
     * Currently, we allow simple numeric constants, string literals, and
     * identifiers; possibly this list could be extended.
     */
    datums = palloc(list_length((*typeName).typmods) as usize * size_of::<Datum>()) as *mut Datum;
    n = 0;
    foreach!(l, (*typeName).typmods, {
        let tm: *mut Node = lfirst(current_cell!(l)) as *mut Node;
        let mut cstr: *mut c_char = null_mut();

        if IsA!(tm, T_A_Const) {
            let ac: *mut A_Const = tm as *mut A_Const;

            if IsA!(&mut (*ac).val, T_Integer) {
                cstr = psprintf_long(intVal!(&raw mut (*ac).val) as c_long);
            } else if IsA!(&mut (*ac).val, T_Float) {
                /* we can just use the string representation directly. */
                cstr = (*(core::ptr::addr_of!((*ac).val.fval) as *const crate::nodes::value::Float)).fval;
            } else if IsA!(&mut (*ac).val, T_String) {
                /* we can just use the string representation directly. */
                cstr = strVal!(&raw mut (*ac).val);
            }
        } else if IsA!(tm, T_ColumnRef) {
            let cr: *mut ColumnRef = tm as *mut ColumnRef;

            if list_length((*cr).fields) == 1 && IsA!(linitial((*cr).fields), T_String) {
                cstr = strVal!(linitial((*cr).fields));
            }
        }
        if cstr.is_null() {
            ereport!(ERROR, "type modifiers must be simple constants or identifiers");
            parser_errposition(pstate, (*typeName).location);
            unreachable!()
        }
        *datums.offset(n as isize) = CStringGetDatum(cstr);
        n += 1;
    });

    arrtypmod = construct_array_builtin(datums, n, CSTRINGOID);

    /* arrange to report location if type's typmodin function fails */
    setup_parser_errposition_callback(&mut pcbstate, pstate, (*typeName).location);

    result = DatumGetInt32(OidFunctionCall1(
        typmodin,
        PointerGetDatum(arrtypmod as *const c_void),
    ));

    cancel_parser_errposition_callback(&mut pcbstate);

    pfree(datums as *mut c_void);
    pfree(arrtypmod as *mut c_void);

    return result;
}

/*
 * appendTypeNameToBuffer
 *		Append a string representing the name of a TypeName to a StringInfo.
 *		This is the shared guts of TypeNameToString and TypeNameListToString.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
unsafe fn appendTypeNameToBuffer(typeName: *const TypeName, string: StringInfo) {
    if (*typeName).names != NIL {
        /* Emit possibly-qualified name as-is */
        foreach!(l, (*typeName).names, {
            if current_cell!(l) != list_head((*typeName).names) {
                appendStringInfoChar(string, b'.' as c_char);
            }
            appendStringInfoString(string, strVal!(lfirst(current_cell!(l))));
        });
    } else {
        /* Look up internally-specified type */
        appendStringInfoString(string, format_type_be((*typeName).typeOid));
    }

    /*
     * Add decoration as needed, but only for fields considered by
     * LookupTypeName
     */
    if (*typeName).pct_type {
        appendStringInfoString(string, c"%TYPE".as_ptr());
    }

    if (*typeName).arrayBounds != NIL {
        appendStringInfoString(string, c"[]".as_ptr());
    }
}

/*
 * TypeNameToString
 *		Produce a string representing the name of a TypeName.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
pub unsafe fn TypeNameToString(typeName: *const TypeName) -> *mut c_char {
    let mut string: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut string);
    appendTypeNameToBuffer(typeName, &mut string);
    return string.data;
}

/*
 * TypeNameListToString
 *		Produce a string representing the name(s) of a List of TypeNames
 */
pub unsafe fn TypeNameListToString(typenames: *mut List) -> *mut c_char {
    let mut string: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut string);
    foreach!(l, typenames, {
        let typeName: *mut TypeName = lfirst_node!(TypeName, T_TypeName, current_cell!(l));

        if current_cell!(l) != list_head(typenames) {
            appendStringInfoChar(&mut string, b',' as c_char);
        }
        appendTypeNameToBuffer(typeName, &mut string);
    });
    return string.data;
}

/*
 * LookupCollation
 *
 * Look up collation by name, return OID, with support for error location.
 */
pub unsafe fn LookupCollation(
    pstate: *mut ParseState,
    collnames: *mut List,
    location: c_int,
) -> Oid {
    let colloid: Oid;
    let mut pcbstate: ParseCallbackState = core::mem::zeroed();

    if !pstate.is_null() {
        setup_parser_errposition_callback(&mut pcbstate, pstate, location);
    }

    colloid = get_collation_oid(collnames, false);

    if !pstate.is_null() {
        cancel_parser_errposition_callback(&mut pcbstate);
    }

    return colloid;
}

/*
 * GetColumnDefCollation
 *
 * Get the collation to be used for a column being defined, given the
 * ColumnDef node and the previously-determined column type OID.
 *
 * pstate is only used for error location purposes, and can be NULL.
 */
pub unsafe fn GetColumnDefCollation(
    pstate: *mut ParseState,
    coldef: *const ColumnDef,
    typeOid: Oid,
) -> Oid {
    let result: Oid;
    let typcollation: Oid = get_typcollation(typeOid);
    let mut location: c_int = (*coldef).location;

    if !(*coldef).collClause.is_null() {
        /* We have a raw COLLATE clause, so look up the collation */
        location = (*(*coldef).collClause).location;
        result = LookupCollation(pstate, (*(*coldef).collClause).collname, location);
    } else if OidIsValid((*coldef).collOid) {
        /* Precooked collation spec, use that */
        result = (*coldef).collOid;
    } else {
        /* Use the type's default collation if any */
        result = typcollation;
    }

    /* Complain if COLLATE is applied to an uncollatable type */
    if OidIsValid(result) && !OidIsValid(typcollation) {
        ereport!(ERROR, "collations are not supported by type");
        parser_errposition(pstate, location);
        unreachable!()
    }

    return result;
}

/* return a Type structure, given a type id */
/* NB: caller must ReleaseSysCache the type tuple when done with it */
pub unsafe fn typeidType(id: Oid) -> Type {
    let tup: HeapTuple;

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(id));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", id);
        unreachable!()
    }
    return tup as Type;
}

/* given type (as type struct), return the type OID */
pub unsafe fn typeTypeId(tp: Type) -> Oid {
    if tp.is_null() {
        /* probably useless */
        elog!(ERROR, "typeTypeId() called with NULL type struct");
        unreachable!()
    }
    return (*(GETSTRUCT(tp) as Form_pg_type)).oid;
}

/* given type (as type struct), return the length of type */
pub unsafe fn typeLen(t: Type) -> int16 {
    let typ: Form_pg_type;

    typ = GETSTRUCT(t) as Form_pg_type;
    return (*typ).typlen;
}

/* given type (as type struct), return its 'byval' attribute */
pub unsafe fn typeByVal(t: Type) -> bool {
    let typ: Form_pg_type;

    typ = GETSTRUCT(t) as Form_pg_type;
    return (*typ).typbyval;
}

/* given type (as type struct), return the type's name */
pub unsafe fn typeTypeName(t: Type) -> *mut c_char {
    let typ: Form_pg_type;

    typ = GETSTRUCT(t) as Form_pg_type;
    /* pstrdup here because result may need to outlive the syscache entry */
    return pstrdup(NameStr((*typ).typname.data.as_mut_ptr()));
}

/* given type (as type struct), return its 'typrelid' attribute */
pub unsafe fn typeTypeRelid(typ: Type) -> Oid {
    let typtup: Form_pg_type;

    typtup = GETSTRUCT(typ) as Form_pg_type;
    return (*typtup).typrelid;
}

/* given type (as type struct), return its 'typcollation' attribute */
pub unsafe fn typeTypeCollation(typ: Type) -> Oid {
    let typtup: Form_pg_type;

    typtup = GETSTRUCT(typ) as Form_pg_type;
    return (*typtup).typcollation;
}

/*
 * Given a type structure and a string, returns the internal representation
 * of that string.  The "string" can be NULL to perform conversion of a NULL
 * (which might result in failure, if the input function rejects NULLs).
 */
pub unsafe fn stringTypeDatum(tp: Type, string: *mut c_char, atttypmod: int32) -> Datum {
    let typform: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
    let typinput: Oid = (*typform).typinput;
    let typioparam: Oid = getTypeIOParam(tp);

    return OidInputFunctionCall(typinput, string, typioparam, atttypmod);
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type.
 */
pub unsafe fn typeidTypeRelid(type_id: Oid) -> Oid {
    let typeTuple: HeapTuple;
    let type_: Form_pg_type;
    let result: Oid;

    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
    if !HeapTupleIsValid(typeTuple) {
        elog!(ERROR, "cache lookup failed for type {}", type_id);
        unreachable!()
    }
    type_ = GETSTRUCT(typeTuple) as Form_pg_type;
    result = (*type_).typrelid;
    ReleaseSysCache(typeTuple);
    return result;
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type or a domain over one.
 * This is the same as typeidTypeRelid(getBaseType(type_id)), but faster.
 */
pub unsafe fn typeOrDomainTypeRelid(mut type_id: Oid) -> Oid {
    let mut typeTuple: HeapTuple;
    let mut type_: Form_pg_type;
    let result: Oid;

    loop {
        typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
        if !HeapTupleIsValid(typeTuple) {
            elog!(ERROR, "cache lookup failed for type {}", type_id);
            unreachable!()
        }
        type_ = GETSTRUCT(typeTuple) as Form_pg_type;
        if (*type_).typtype != TYPTYPE_DOMAIN {
            /* Not a domain, so done looking through domains */
            break;
        }
        /* It is a domain, so examine the base type instead */
        type_id = (*type_).typbasetype;
        ReleaseSysCache(typeTuple);
    }
    result = (*type_).typrelid;
    ReleaseSysCache(typeTuple);
    return result;
}

/*
 * error context callback for parse failure during parseTypeString()
 */
unsafe extern "C" fn pts_error_callback(arg: *mut c_void) {
    let _str: *const c_char = arg as *const c_char;

    // errcontext("invalid type name \"%s\"", str);
    // TODO: utils/error/elog.c errcontext()
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and return the result as a TypeName.
 *
 * If the string cannot be parsed as a type, an error is raised,
 * unless escontext is an ErrorSaveContext node, in which case we may
 * fill that and return NULL.  But note that the ErrorSaveContext option
 * is mostly aspirational at present: errors detected by the main
 * grammar, rather than here, will still be thrown.
 */
#[no_mangle]
pub unsafe fn typeStringToTypeName(str: *const c_char, escontext: *mut Node) -> *mut TypeName {
    let raw_parsetree_list: *mut List;
    let typeName: *mut TypeName;
    let mut ptserrcontext: ErrorContextCallback = core::mem::zeroed();

    /* make sure we give useful error for empty input */
    if strspn(str, c" \t\n\r\u{0c}\u{0b}".as_ptr()) == strlen(str) {
        return typeStringToTypeName_fail(str, escontext);
    }

    /*
     * Setup error traceback support in case of ereport() during parse
     */
    ptserrcontext.callback = Some(pts_error_callback);
    ptserrcontext.arg = str as *mut c_void; /* unconstify(char *, str) */
    ptserrcontext.previous = error_context_stack;
    error_context_stack = &mut ptserrcontext;

    raw_parsetree_list = raw_parser(str, RawParseMode::RAW_PARSE_TYPE_NAME);

    error_context_stack = ptserrcontext.previous;

    /* We should get back exactly one TypeName node. */
    Assert!(list_length(raw_parsetree_list) == 1);
    typeName = linitial_node!(TypeName, T_TypeName, raw_parsetree_list);

    /* The grammar allows SETOF in TypeName, but we don't want that here. */
    if (*typeName).setof {
        return typeStringToTypeName_fail(str, escontext);
    }

    return typeName;
}

// fail: label of typeStringToTypeName
//
//   ereturn(escontext, NULL,
//           (errcode(ERRCODE_SYNTAX_ERROR),
//            errmsg("invalid type name \"%s\"", str)));
unsafe fn typeStringToTypeName_fail(str: *const c_char, escontext: *mut Node) -> *mut TypeName {
    if SOFT_ERROR_OCCURRED_INTO(escontext, str) {
        return null_mut();
    }
    elog!(ERROR, "invalid type name");
    unreachable!()
}

// Helper modeling ereturn(escontext, NULL, ...): if escontext is a soft
// ErrorSaveContext, fill it and signal a soft return; otherwise throw.
unsafe fn SOFT_ERROR_OCCURRED_INTO(escontext: *mut Node, _str: *const c_char) -> bool {
    if !escontext.is_null() && IsA!(escontext, T_ErrorSaveContext) {
        let esc = escontext as *mut ErrorSaveContext;
        (*esc).error_occurred = true;
        // TODO: utils/error/elog.c - record errmsg("invalid type name \"%s\"", str)
        return true;
    }
    false
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and convert it to a type OID and type modifier.
 *
 * If escontext is an ErrorSaveContext node, then errors are reported by
 * filling escontext and returning false, instead of throwing them.
 */
pub unsafe fn parseTypeString(
    str: *const c_char,
    typeid_p: *mut Oid,
    typmod_p: *mut int32,
    escontext: *mut Node,
) -> bool {
    let typeName: *mut TypeName;
    let tup: Type;

    typeName = typeStringToTypeName(str, escontext);
    if typeName.is_null() {
        return false;
    }

    tup = LookupTypeName(
        null_mut(),
        typeName,
        typmod_p,
        !escontext.is_null() && IsA!(escontext, T_ErrorSaveContext),
    );
    if tup.is_null() {
        // ereturn(escontext, false, errmsg("type \"%s\" does not exist", ...))
        if !escontext.is_null() && IsA!(escontext, T_ErrorSaveContext) {
            (*(escontext as *mut ErrorSaveContext)).error_occurred = true;
            return false;
        }
        ereport!(ERROR, "type does not exist");
        unreachable!()
    } else {
        let typ: Form_pg_type = GETSTRUCT(tup) as Form_pg_type;

        if !(*typ).typisdefined {
            ReleaseSysCache(tup);
            // ereturn(escontext, false, errmsg("type \"%s\" is only a shell", ...))
            if !escontext.is_null() && IsA!(escontext, T_ErrorSaveContext) {
                (*(escontext as *mut ErrorSaveContext)).error_occurred = true;
                return false;
            }
            ereport!(ERROR, "type is only a shell");
            unreachable!()
        }
        *typeid_p = (*typ).oid;
        ReleaseSysCache(tup);
    }

    return true;
}

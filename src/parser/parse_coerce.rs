/*-------------------------------------------------------------------------
 *
 * parse_coerce.rs
 *   handle type coercions/conversions for parser
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/parser/parse_coerce.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]

use crate::prelude::*;
use crate::{IsA, makeNode, list_make1, PG_DETOAST_DATUM};

use std::ffi::{c_char, c_int, c_void};

use crate::c::{OidIsValid, int32, int16};
use crate::postgres_ext::{Oid, InvalidOid};
use crate::postgres::{Datum, Int32GetDatum, PointerGetDatum, DatumGetCString};

/* ---- Node/list infrastructure ---- */
use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst,
    linitial, list_head,
    lappend, list_length, list_make1_impl,
    list_second_cell,
    lnext, ListCell,
};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
    expression_returns_set, applyRelabelType,
};
use crate::nodes::makefuncs::{
    makeConst, makeFuncExpr, makeNullConst, makeRelabelType,
};
use crate::nodes::primnodes::{
    Expr, Var, Param, CollateExpr,
    FuncExpr, CoercionForm, CoercionForm::*, CoercionContext, CoercionContext::*,
    RelabelType, CoerceViaIO, ArrayCoerceExpr, ConvertRowtypeExpr,
    CoerceToDomain, CaseTestExpr, RowExpr, Const,
};

/* ---- Catalog OIDs ---- */
use crate::catalog::pg_type_d::{
    BOOLOID, INT4OID, TEXTOID, TEXTARRAYOID, UNKNOWNOID, INTERVALOID,
    RECORDOID, ANYOID, ANYELEMENTOID, ANYNONARRAYOID, ANYCOMPATIBLEOID,
    ANYCOMPATIBLENONARRAYOID, ANYARRAYOID, ANYENUMOID, ANYRANGEOID,
    ANYMULTIRANGEOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLERANGEOID,
    ANYCOMPATIBLEMULTIRANGEOID, INT2VECTOROID, OIDVECTOROID,
    ANYELEMENTOID as _ANYELEMENTOID,
};
/* RECORDARRAYOID = _RECORDOID = 2287 */
const RECORDARRAYOID: Oid = 2287;
/* INTERNALOID */
const INTERNALOID: Oid = 2281;

/* ---- pg_type category constants ---- */
use crate::catalog::pg_type::{
    TYPCATEGORY_INVALID, TYPCATEGORY_STRING,
};
pub type TYPCATEGORY = c_char;

/* ---- pg_cast / pg_proc structs ---- */
use crate::catalog::pg_cast::{
    Form_pg_cast, FormData_pg_cast,
    COERCION_CODE_IMPLICIT, COERCION_CODE_ASSIGNMENT, COERCION_CODE_EXPLICIT,
    COERCION_METHOD_FUNCTION, COERCION_METHOD_BINARY, COERCION_METHOD_INOUT,
};
use crate::catalog::pg_proc::{Form_pg_proc, FormData_pg_proc, PROKIND_FUNCTION};
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_type::{Form_pg_type, FormData_pg_type};

/* ---- HeapTuple / syscache ---- */
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::utils::cache::syscache::{
    SearchSysCache1, SearchSysCache2, ReleaseSysCache,
};
use crate::utils::cache::syscache_ids_gen::{CASTSOURCETARGET, PROCOID, RELOID};

/* ---- lsyscache utilities ---- */
use crate::utils::cache::lsyscache::{
    format_type_be,
    get_array_type, get_element_type, get_base_element_type,
    getBaseType, getBaseTypeAndTypmod,
    get_typtype, type_is_collatable,
    get_type_category_preferred,
    ObjectIdGetDatum, Int32GetDatum as Int32GD,
    BoolGetDatum, pstrdup, palloc,
    type_is_enum, type_is_range, type_is_multirange,
    get_range_subtype, get_multirange_range, get_range_multirange,
    IsTrueArrayType,
};
use crate::utils::cache::typcache::{
    lookup_rowtype_tupdesc, DomainHasConstraints,
};
use crate::access::common::tupdesc::{TupleDescAttr, TupleDesc, ReleaseTupleDesc};

/* ---- parse_type helpers ---- */
use crate::parser::parse_type::{
    typeidType, typeLen, typeByVal, typeTypeCollation, stringTypeDatum,
    typeOrDomainTypeRelid, Type,
};
use crate::ISCOMPLEX;

/* ---- parse_node / error ---- */
use crate::parser::parse_node::{
    ParseState, parser_errposition,
    setup_parser_errposition_callback, cancel_parser_errposition_callback,
    ParseCallbackState,
};

/* ---- parse_relation: GetNSItemByRangeTablePosn, expandNSItemVars ---- */
use crate::parser::parse_relation::{GetNSItemByRangeTablePosn, expandNSItemVars};

/* ---- error macros ---- */
use crate::utils::elog::{emit_log, ERROR, WARNING};

/* ---- misc ---- */
use crate::pg_config_manual::FUNC_MAX_ARGS;
use crate::c::uint32;
use crate::postgres::ObjectIdGetDatum as PgObjectIdGetDatum;

/* ---- CoercionPathType (enum lives in utils/adt/ri_triggers.rs; re-declare locally) ---- */
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CoercionPathType {
    COERCION_PATH_NONE        = 0, /* not found */
    COERCION_PATH_FUNC        = 1, /* apply the coercion function */
    COERCION_PATH_RELABELTYPE = 2, /* binary-compatible cast, no function */
    COERCION_PATH_ARRAYCOERCE = 3, /* need an ArrayCoerceExpr node */
    COERCION_PATH_COERCEVIAIO = 4, /* need a CoerceViaIO node */
}
pub use CoercionPathType::*;

/* ---- ERRCODE stubs (parser uses 0 everywhere as placeholder) ---- */
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;
const ERRCODE_CANNOT_COERCE: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_INTERNAL_ERROR: c_int = 0;

#[inline]
fn errcode(_code: c_int) {}
#[inline]
fn errdetail(_s: &str) {}

/* ---- psprintf stub ---- */
#[allow(clippy::not_unsafe_ptr_arg_deref)]
unsafe fn psprintf(fmt: *const c_char, _arg: *const c_char) -> *mut c_char {
    /* TODO(pg-port): real psprintf from utils/misc/psprintf.c */
    pstrdup(fmt)
}
unsafe fn psprintf1(msg: *const c_char) -> *mut c_char {
    pstrdup(msg)
}

/* ---- typeInheritsFrom: catalog/pg_inherits.c ---- */
/* TODO(pg-port): real fn; needs pg_inherits syscache lookup */
unsafe fn typeInheritsFrom(_subclassTypeId: Oid, _superclassTypeId: Oid) -> bool {
    crate::catalog::pg_inherits::typeInheritsFrom(_subclassTypeId, _superclassTypeId)
}

/* ---- type_is_array: utils/cache/lsyscache.c ---- */
/* TODO(pg-port): real fn */
unsafe fn type_is_array(typid: Oid) -> bool {
    OidIsValid(get_element_type(typid))
}

/* ---- type_is_array_domain: utils/cache/lsyscache.c ---- */
/* TODO(pg-port): real fn; checks domain chain for array base type */
unsafe fn type_is_array_domain(typid: Oid) -> bool {
    get_base_element_type(typid) != InvalidOid
}

/* ---- IsPolymorphicType / IsPolymorphicTypeFamily1 / IsPolymorphicTypeFamily2 ---- */
/* TODO(pg-port): real macros from include/catalog/pg_proc.h */
unsafe fn IsPolymorphicType(typid: Oid) -> bool {
    IsPolymorphicTypeFamily1(typid) || IsPolymorphicTypeFamily2(typid)
}
unsafe fn IsPolymorphicTypeFamily1(typid: Oid) -> bool {
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
}
unsafe fn IsPolymorphicTypeFamily2(typid: Oid) -> bool {
    typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/* ---------- forward declarations ---------- */
/* (Rust does not need forward decls; functions are defined later in this file.) */

/* =========================================================================
 * coerce_to_target_type()
 *   Convert an expression to a target type and typmod.
 *
 * This is the general-purpose entry point for arbitrary type coercion
 * operations.  Direct use of the component operations can_coerce_type,
 * coerce_type, and coerce_type_typmod should be restricted to special
 * cases (eg, when the conversion is expected to succeed).
 *
 * Returns the possibly-transformed expression tree, or NULL if the type
 * conversion is not possible.  (We do this, rather than ereport'ing directly,
 * so that callers can generate custom error messages indicating context.)
 * =========================================================================
 */
#[no_mangle]
pub unsafe fn coerce_to_target_type(
    pstate: *mut ParseState,
    expr: *mut Node,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: int32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
) -> *mut Node {
    let result: *mut Node;
    let origexpr: *mut Node;

    if !can_coerce_type(1, &exprtype, &targettype, ccontext) {
        return std::ptr::null_mut();
    }

    /*
     * If the input has a CollateExpr at the top, strip it off, perform the
     * coercion, and put a new one back on.  This is annoying since it
     * duplicates logic in coerce_type, but if we don't do this then it's too
     * hard to tell whether coerce_type actually changed anything, and we
     * *must* know that to avoid possibly calling hide_coercion_node on
     * something that wasn't generated by coerce_type.  Note that if there are
     * multiple stacked CollateExprs, we just discard all but the topmost.
     * Also, if the target type isn't collatable, we discard the CollateExpr.
     */
    origexpr = expr;
    let mut expr = expr;
    while !expr.is_null() && IsA!(expr, T_CollateExpr) {
        expr = (*(expr as *mut CollateExpr)).arg as *mut Node;
    }

    let mut result = coerce_type(
        pstate, expr, exprtype,
        targettype, targettypmod,
        ccontext, cformat, location,
    );

    /*
     * If the target is a fixed-length type, it may need a length coercion as
     * well as a type coercion.  If we find ourselves adding both, force the
     * inner coercion node to implicit display form.
     */
    result = coerce_type_typmod(
        result,
        targettype, targettypmod,
        ccontext, cformat, location,
        (result != expr && !IsA!(result, T_Const)),
    );

    if expr != origexpr && type_is_collatable(targettype) {
        /* Reinstall top CollateExpr */
        let coll = origexpr as *mut CollateExpr;
        let newcoll = makeNode!(CollateExpr, T_CollateExpr);
        (*newcoll).arg = result as *mut Expr;
        (*newcoll).collOid = (*coll).collOid;
        (*newcoll).location = (*coll).location;
        result = newcoll as *mut Node;
    }

    result
}


/*
 * coerce_type()
 *   Convert an expression to a different type.
 *
 * The caller should already have determined that the coercion is possible;
 * see can_coerce_type.
 *
 * Normally, no coercion to a typmod (length) is performed here.  The caller
 * must call coerce_type_typmod as well, if a typmod constraint is wanted.
 * (But if the target type is a domain, it may internally contain a
 * typmod constraint, which will be applied inside coerce_to_domain.)
 * In some cases pg_cast specifies a type coercion function that also
 * applies length conversion, and in those cases only, the result will
 * already be properly coerced to the specified typmod.
 *
 * pstate is only used in the case that we are able to resolve the type of
 * a previously UNKNOWN Param.  It is okay to pass pstate = NULL if the
 * caller does not want type information updated for Params.
 *
 * Note: this function must not modify the given expression tree, only add
 * decoration on top of it.  See transformSetOperationTree, for example.
 */
pub unsafe fn coerce_type(
    pstate: *mut ParseState,
    node: *mut Node,
    inputTypeId: Oid,
    targetTypeId: Oid,
    targetTypeMod: int32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
) -> *mut Node {
    let mut result: *mut Node;
    let pathtype: CoercionPathType;
    let mut funcId: Oid = InvalidOid;

    if targetTypeId == inputTypeId || node.is_null() {
        /* no conversion needed */
        return node;
    }
    if targetTypeId == ANYOID
        || targetTypeId == ANYELEMENTOID
        || targetTypeId == ANYNONARRAYOID
        || targetTypeId == ANYCOMPATIBLEOID
        || targetTypeId == ANYCOMPATIBLENONARRAYOID
    {
        /*
         * Assume can_coerce_type verified that implicit coercion is okay.
         *
         * Note: by returning the unmodified node here, we are saying that
         * it's OK to treat an UNKNOWN constant as a valid input for a
         * function accepting one of these pseudotypes.  This should be all
         * right, since an UNKNOWN value is still a perfectly valid Datum.
         *
         * NB: we do NOT want a RelabelType here: the exposed type of the
         * function argument must be its actual type, not the polymorphic
         * pseudotype.
         */
        return node;
    }
    if targetTypeId == ANYARRAYOID
        || targetTypeId == ANYENUMOID
        || targetTypeId == ANYRANGEOID
        || targetTypeId == ANYMULTIRANGEOID
        || targetTypeId == ANYCOMPATIBLEARRAYOID
        || targetTypeId == ANYCOMPATIBLERANGEOID
        || targetTypeId == ANYCOMPATIBLEMULTIRANGEOID
    {
        /*
         * Assume can_coerce_type verified that implicit coercion is okay.
         *
         * These cases are unlike the ones above because the exposed type of
         * the argument must be an actual array, enum, range, or multirange
         * type.  In particular the argument must *not* be an UNKNOWN
         * constant.  If it is, we just fall through; below, we'll call the
         * pseudotype's input function, which will produce an error.  Also, if
         * what we have is a domain over array, enum, range, or multirange, we
         * have to relabel it to its base type.
         *
         * Note: currently, we can't actually see a domain-over-enum here,
         * since the other functions in this file will not match such a
         * parameter to ANYENUM.  But that should get changed eventually.
         */
        if inputTypeId != UNKNOWNOID {
            let baseTypeId = getBaseType(inputTypeId);
            if baseTypeId != inputTypeId {
                let r = makeRelabelType(
                    node as *mut Expr,
                    baseTypeId, -1,
                    InvalidOid,
                    cformat,
                );
                (*r).location = location;
                return r as *mut Node;
            }
            /* Not a domain type, so return it as-is */
            return node;
        }
    }
    if inputTypeId == UNKNOWNOID && IsA!(node, T_Const) {
        /*
         * Input is a string constant with previously undetermined type. Apply
         * the target type's typinput function to it to produce a constant of
         * the target type.
         *
         * NOTE: this case cannot be folded together with the other
         * constant-input case, since the typinput function does not
         * necessarily behave the same as a type conversion function. For
         * example, int4's typinput function will reject "1.2", whereas
         * float-to-int type conversion will round to integer.
         */
        let con = node as *mut Const;
        let newcon = makeNode!(Const, T_Const);
        let mut baseTypeMod: int32 = targetTypeMod;
        let baseTypeId = getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod);
        let inputTypeMod: int32;

        /*
         * For most types we pass typmod -1 to the input routine, because
         * existing input routines follow implicit-coercion semantics for
         * length checks, which is not always what we want here.  Any length
         * constraint will be applied later by our caller.  An exception
         * however is the INTERVAL type, for which we *must* pass the typmod
         * or it won't be able to obey the bizarre SQL-spec input rules. (Ugly
         * as sin, but so is this part of the spec...)
         */
        if baseTypeId == INTERVALOID {
            inputTypeMod = baseTypeMod;
        } else {
            inputTypeMod = -1;
        }

        let baseType = typeidType(baseTypeId);

        (*newcon).consttype = baseTypeId;
        (*newcon).consttypmod = inputTypeMod;
        (*newcon).constcollid = typeTypeCollation(baseType);
        (*newcon).constlen = typeLen(baseType) as i32;
        (*newcon).constbyval = typeByVal(baseType);
        (*newcon).constisnull = (*con).constisnull;

        /*
         * We use the original literal's location regardless of the position
         * of the coercion.  This is a change from pre-9.2 behavior, meant to
         * simplify life for pg_stat_statements.
         */
        (*newcon).location = (*con).location;

        /*
         * Set up to point at the constant's text if the input routine throws
         * an error.
         */
        let mut pcbstate: ParseCallbackState = std::mem::zeroed();
        setup_parser_errposition_callback(&mut pcbstate, pstate, (*con).location);

        /*
         * We assume here that UNKNOWN's internal representation is the same
         * as CSTRING.
         */
        if !(*con).constisnull {
            (*newcon).constvalue = stringTypeDatum(
                baseType,
                DatumGetCString((*con).constvalue),
                inputTypeMod,
            );
        } else {
            (*newcon).constvalue = stringTypeDatum(baseType, std::ptr::null_mut(), inputTypeMod);
        }

        /*
         * If it's a varlena value, force it to be in non-expanded
         * (non-toasted) format; this avoids any possible dependency on
         * external values and improves consistency of representation.
         */
        if !(*con).constisnull && (*newcon).constlen == -1 {
            (*newcon).constvalue =
                PointerGetDatum(PG_DETOAST_DATUM!((*newcon).constvalue) as *const c_void);
        }

        /* (RANDOMIZE_ALLOCATED_MEMORY check omitted - debug-only) */

        cancel_parser_errposition_callback(&mut pcbstate);

        result = newcon as *mut Node;

        /* If target is a domain, apply constraints. */
        if baseTypeId != targetTypeId {
            result = coerce_to_domain(
                result,
                baseTypeId, baseTypeMod,
                targetTypeId,
                ccontext, cformat, location,
                false,
            );
        }

        ReleaseSysCache(baseType);

        return result;
    }
    if IsA!(node, T_Param)
        && !pstate.is_null()
        && (*pstate).p_coerce_param_hook.is_some()
    {
        /*
         * Allow the CoerceParamHook to decide what happens.  It can return a
         * transformed node (very possibly the same Param node), or return
         * NULL to indicate we should proceed with normal coercion.
         */
        result = (*pstate).p_coerce_param_hook.unwrap()(
            pstate,
            node as *mut Param as *mut c_void,
            targetTypeId,
            targetTypeMod,
            location,
        );
        if !result.is_null() {
            return result;
        }
    }
    if IsA!(node, T_CollateExpr) {
        /*
         * If we have a COLLATE clause, we have to push the coercion
         * underneath the COLLATE; or discard the COLLATE if the target type
         * isn't collatable.  This is really ugly, but there is little choice
         * because the above hacks on Consts and Params wouldn't happen
         * otherwise.  This kluge has consequences in coerce_to_target_type.
         */
        let coll = node as *mut CollateExpr;
        result = coerce_type(
            pstate, (*coll).arg as *mut Node,
            inputTypeId, targetTypeId, targetTypeMod,
            ccontext, cformat, location,
        );
        if type_is_collatable(targetTypeId) {
            let newcoll = makeNode!(CollateExpr, T_CollateExpr);
            (*newcoll).arg = result as *mut Expr;
            (*newcoll).collOid = (*coll).collOid;
            (*newcoll).location = (*coll).location;
            result = newcoll as *mut Node;
        }
        return result;
    }
    let pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &mut funcId);
    if pathtype != COERCION_PATH_NONE {
        let mut baseTypeMod: int32 = targetTypeMod;
        let baseTypeId = getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod);

        if pathtype != COERCION_PATH_RELABELTYPE {
            /*
             * Generate an expression tree representing run-time application
             * of the conversion function.  If we are dealing with a domain
             * target type, the conversion function will yield the base type,
             * and we need to extract the correct typmod to use from the
             * domain's typtypmod.
             */
            result = build_coercion_expression(
                node, pathtype, funcId,
                baseTypeId, baseTypeMod,
                ccontext, cformat, location,
            );

            /*
             * If domain, coerce to the domain type and relabel with domain
             * type ID, hiding the previous coercion node.
             */
            if targetTypeId != baseTypeId {
                result = coerce_to_domain(
                    result, baseTypeId, baseTypeMod,
                    targetTypeId,
                    ccontext, cformat, location,
                    true,
                );
            }
        } else {
            /*
             * We don't need to do a physical conversion, but we do need to
             * attach a RelabelType node so that the expression will be seen
             * to have the intended type when inspected by higher-level code.
             *
             * Also, domains may have value restrictions beyond the base type
             * that must be accounted for.  If the destination is a domain
             * then we won't need a RelabelType node.
             */
            result = coerce_to_domain(
                node, baseTypeId, baseTypeMod,
                targetTypeId,
                ccontext, cformat, location,
                false,
            );
            if result == node {
                /*
                 * XXX could we label result with exprTypmod(node) instead of
                 * default -1 typmod, to save a possible length-coercion
                 * later? Would work if both types have same interpretation of
                 * typmod, which is likely but not certain.
                 */
                let r = makeRelabelType(
                    result as *mut Expr,
                    targetTypeId, -1,
                    InvalidOid,
                    cformat,
                );
                (*r).location = location;
                result = r as *mut Node;
            }
        }
        return result;
    }
    if inputTypeId == RECORDOID && ISCOMPLEX!(targetTypeId) {
        /* Coerce a RECORD to a specific complex type */
        return coerce_record_to_complex(pstate, node, targetTypeId, ccontext, cformat, location);
    }
    if targetTypeId == RECORDOID && ISCOMPLEX!(inputTypeId) {
        /* Coerce a specific complex type to RECORD */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
    /* #ifdef NOT_USED: RECORDARRAYOID -> complex array coerce; not implemented */
    if targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId) {
        /* Coerce a specific complex array type to record[] */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
    if typeInheritsFrom(inputTypeId, targetTypeId)
        || typeIsOfTypedTable(inputTypeId, targetTypeId)
    {
        /*
         * Input class type is a subclass of target, so generate an
         * appropriate runtime conversion (removing unneeded columns and
         * possibly rearranging the ones that are wanted).
         *
         * We will also get here when the input is a domain over a subclass of
         * the target type.  To keep life simple for the executor, we define
         * ConvertRowtypeExpr as only working between regular composite types;
         * therefore, in such cases insert a RelabelType to smash the input
         * expression down to its base type.
         */
        let baseTypeId = getBaseType(inputTypeId);
        let r = makeNode!(ConvertRowtypeExpr, T_ConvertRowtypeExpr);
        let mut node_inner = node;
        if baseTypeId != inputTypeId {
            let rt = makeRelabelType(
                node_inner as *mut Expr,
                baseTypeId, -1,
                InvalidOid,
                COERCE_IMPLICIT_CAST,
            );
            (*rt).location = location;
            node_inner = rt as *mut Node;
        }
        (*r).arg = node_inner as *mut Expr;
        (*r).resulttype = targetTypeId;
        (*r).convertformat = cformat;
        (*r).location = location;
        return r as *mut Node;
    }
    /* If we get here, caller blew it */
    elog!(ERROR, "failed to find conversion function from {} to {}",
          std::ffi::CStr::from_ptr(format_type_be(inputTypeId)).to_string_lossy(),
          std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy());
    std::ptr::null_mut() /* keep compiler quiet */
}

/*
 * can_coerce_type()
 *   Can input_typeids be coerced to target_typeids?
 *
 * We must be told the context (CAST construct, assignment, implicit coercion)
 * as this determines the set of available casts.
 */
pub unsafe fn can_coerce_type(
    nargs: c_int,
    input_typeids: *const Oid,
    target_typeids: *const Oid,
    ccontext: CoercionContext,
) -> bool {
    let mut have_generics = false;

    /* run through argument list... */
    for i in 0..nargs as usize {
        let inputTypeId = *input_typeids.add(i);
        let targetTypeId = *target_typeids.add(i);
        let mut funcId: Oid = InvalidOid;

        /* no problem if same type */
        if inputTypeId == targetTypeId {
            continue;
        }

        /* accept if target is ANY */
        if targetTypeId == ANYOID {
            continue;
        }

        /* accept if target is polymorphic, for now */
        if IsPolymorphicType(targetTypeId) {
            have_generics = true; /* do more checking later */
            continue;
        }

        /*
         * If input is an untyped string constant, assume we can convert it to
         * anything.
         */
        if inputTypeId == UNKNOWNOID {
            continue;
        }

        /*
         * If pg_cast shows that we can coerce, accept.  This test now covers
         * both binary-compatible and coercion-function cases.
         */
        let pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &mut funcId);
        if pathtype != COERCION_PATH_NONE {
            continue;
        }

        /*
         * If input is RECORD and target is a composite type, assume we can
         * coerce (may need tighter checking here)
         */
        if inputTypeId == RECORDOID && ISCOMPLEX!(targetTypeId) {
            continue;
        }

        /*
         * If input is a composite type and target is RECORD, accept
         */
        if targetTypeId == RECORDOID && ISCOMPLEX!(inputTypeId) {
            continue;
        }

        /* #ifdef NOT_USED: RECORDARRAYOID check not implemented */

        /*
         * If input is a composite array type and target is record[], accept
         */
        if targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId) {
            continue;
        }

        /*
         * If input is a class type that inherits from target, accept
         */
        if typeInheritsFrom(inputTypeId, targetTypeId)
            || typeIsOfTypedTable(inputTypeId, targetTypeId)
        {
            continue;
        }

        /*
         * Else, cannot coerce at this argument position
         */
        return false;
    }

    /* If we found any generic argument types, cross-check them */
    if have_generics {
        if !check_generic_type_consistency(input_typeids, target_typeids, nargs) {
            return false;
        }
    }

    true
}


/*
 * Create an expression tree to represent coercion to a domain type.
 *
 * 'arg': input expression
 * 'baseTypeId': base type of domain
 * 'baseTypeMod': base type typmod of domain
 * 'typeId': target type to coerce to
 * 'ccontext': context indicator to control coercions
 * 'cformat': coercion display format
 * 'location': coercion request location
 * 'hideInputCoercion': if true, hide the input coercion under this one.
 *
 * If the target type isn't a domain, the given 'arg' is returned as-is.
 */
pub unsafe fn coerce_to_domain(
    arg: *mut Node,
    baseTypeId: Oid,
    baseTypeMod: int32,
    typeId: Oid,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
    hideInputCoercion: bool,
) -> *mut Node {
    /* We now require the caller to supply correct baseTypeId/baseTypeMod */
    assert!(OidIsValid(baseTypeId));

    /* If it isn't a domain, return the node as it was passed in */
    if baseTypeId == typeId {
        return arg;
    }

    /* Suppress display of nested coercion steps */
    if hideInputCoercion {
        hide_coercion_node(arg);
    }

    /*
     * If the domain applies a typmod to its base type, build the appropriate
     * coercion step.  Mark it implicit for display purposes, because we don't
     * want it shown separately by ruleutils.c; but the isExplicit flag passed
     * to the conversion function depends on the manner in which the domain
     * coercion is invoked, so that the semantics of implicit and explicit
     * coercion differ.  (Is that really the behavior we want?)
     *
     * NOTE: because we apply this as part of the fixed expression structure,
     * ALTER DOMAIN cannot alter the typtypmod.  But it's unclear that that
     * would be safe to do anyway, without lots of knowledge about what the
     * base type thinks the typmod means.
     */
    let arg = coerce_type_typmod(arg, baseTypeId, baseTypeMod,
                                  ccontext, COERCE_IMPLICIT_CAST, location, false);

    /*
     * Now build the domain coercion node.  This represents run-time checking
     * of any constraints currently attached to the domain.  This also ensures
     * that the expression is properly labeled as to result type.
     */
    let result = makeNode!(CoerceToDomain, T_CoerceToDomain);
    (*result).arg = arg as *mut Expr;
    (*result).resulttype = typeId;
    (*result).resulttypmod = -1; /* currently, always -1 for domains */
    /* resultcollid will be set by parse_collate.c */
    (*result).coercionformat = cformat;
    (*result).location = location;

    result as *mut Node
}


/*
 * coerce_type_typmod()
 *   Force a value to a particular typmod, if meaningful and possible.
 *
 * This is applied to values that are going to be stored in a relation
 * (where we have an atttypmod for the column) as well as values being
 * explicitly CASTed (where the typmod comes from the target type spec).
 *
 * The caller must have already ensured that the value is of the correct
 * type, typically by applying coerce_type.
 *
 * ccontext may affect semantics, depending on whether the length coercion
 * function pays attention to the isExplicit flag it's passed.
 *
 * cformat determines the display properties of the generated node (if any).
 *
 * If hideInputCoercion is true *and* we generate a node, the input node is
 * forced to IMPLICIT display form, so that only the typmod coercion node will
 * be visible when displaying the expression.
 *
 * NOTE: this does not need to work on domain types, because any typmod
 * coercion for a domain is considered to be part of the type coercion
 * needed to produce the domain value in the first place.  So, no getBaseType.
 */
unsafe fn coerce_type_typmod(
    node: *mut Node,
    targetTypeId: Oid,
    targetTypMod: int32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
    hideInputCoercion: bool,
) -> *mut Node {
    /* Skip coercion if already done */
    if targetTypMod == exprTypmod(node) {
        return node;
    }

    /* Suppress display of nested coercion steps */
    if hideInputCoercion {
        hide_coercion_node(node);
    }

    /*
     * A negative typmod means that no actual coercion is needed, but we still
     * want a RelabelType to ensure that the expression exposes the intended
     * typmod.
     */
    let pathtype: CoercionPathType;
    let mut funcId: Oid = InvalidOid;
    if targetTypMod < 0 {
        pathtype = COERCION_PATH_NONE;
    } else {
        pathtype = find_typmod_coercion_function(targetTypeId, &mut funcId);
    }

    if pathtype != COERCION_PATH_NONE {
        build_coercion_expression(node, pathtype, funcId,
                                   targetTypeId, targetTypMod,
                                   ccontext, cformat, location)
    } else {
        /*
         * We don't need to perform any actual coercion step, but we should
         * apply a RelabelType to ensure that the expression exposes the
         * intended typmod.
         */
        applyRelabelType(node, targetTypeId, targetTypMod,
                          exprCollation(node),
                          cformat, location, false)
    }
}

/*
 * Mark a coercion node as IMPLICIT so it will never be displayed by
 * ruleutils.c.  We use this when we generate a nest of coercion nodes
 * to implement what is logically one conversion; the inner nodes are
 * forced to IMPLICIT_CAST format.  This does not change their semantics,
 * only display behavior.
 *
 * It is caller error to call this on something that doesn't have a
 * CoercionForm field.
 */
unsafe fn hide_coercion_node(node: *mut Node) {
    if IsA!(node, T_FuncExpr) {
        (*(node as *mut FuncExpr)).funcformat = COERCE_IMPLICIT_CAST;
    } else if IsA!(node, T_RelabelType) {
        (*(node as *mut RelabelType)).relabelformat = COERCE_IMPLICIT_CAST;
    } else if IsA!(node, T_CoerceViaIO) {
        (*(node as *mut CoerceViaIO)).coerceformat = COERCE_IMPLICIT_CAST;
    } else if IsA!(node, T_ArrayCoerceExpr) {
        (*(node as *mut ArrayCoerceExpr)).coerceformat = COERCE_IMPLICIT_CAST;
    } else if IsA!(node, T_ConvertRowtypeExpr) {
        (*(node as *mut ConvertRowtypeExpr)).convertformat = COERCE_IMPLICIT_CAST;
    } else if IsA!(node, T_RowExpr) {
        (*(node as *mut RowExpr)).row_format = COERCE_IMPLICIT_CAST;
    } else if IsA!(node, T_CoerceToDomain) {
        (*(node as *mut CoerceToDomain)).coercionformat = COERCE_IMPLICIT_CAST;
    } else {
        elog!(ERROR, "unsupported node type: {}", nodeTag(node) as i32);
    }
}

/*
 * build_coercion_expression()
 *   Construct an expression tree for applying a pg_cast entry.
 *
 * This is used for both type-coercion and length-coercion operations,
 * since there is no difference in terms of the calling convention.
 */
unsafe fn build_coercion_expression(
    node: *mut Node,
    pathtype: CoercionPathType,
    funcId: Oid,
    targetTypeId: Oid,
    targetTypMod: int32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
) -> *mut Node {
    let mut nargs: c_int = 0;

    if OidIsValid(funcId) {
        let tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for function {}", funcId);
        }
        let procstruct = GETSTRUCT(tp) as Form_pg_proc;

        /*
         * These Asserts essentially check that function is a legal coercion
         * function.  We can't make the seemingly obvious tests on prorettype
         * and proargtypes[0], even in the COERCION_PATH_FUNC case, because of
         * various binary-compatibility cases.
         */
        /* Assert(targetTypeId == procstruct->prorettype); */
        assert!(!(*procstruct).proretset);
        assert!((*procstruct).prokind == PROKIND_FUNCTION);
        nargs = (*procstruct).pronargs as c_int;
        assert!(nargs >= 1 && nargs <= 3);
        /* Assert(procstruct->proargtypes.values[0] == exprType(node)); */
        /* TODO(pg-port): proargtypes is CATALOG_VARLEN and omitted from FormData_pg_proc;
         * skip the proargtypes[1]/[2] assertions for now (zero-fill / unavailable). */
        let _ = (nargs, INT4OID, BOOLOID); /* suppress unused-variable warnings */

        ReleaseSysCache(tp);
    }

    if pathtype == COERCION_PATH_FUNC {
        /* We build an ordinary FuncExpr with special arguments */
        assert!(OidIsValid(funcId));

        let mut args = list_make1!(node);

        if nargs >= 2 {
            /* Pass target typmod as an int4 constant */
            let cons = makeConst(
                INT4OID,
                -1,
                InvalidOid,
                std::mem::size_of::<int32>() as i32,
                Int32GetDatum(targetTypMod),
                false,
                true,
            );
            args = lappend(args, cons as *mut c_void);
        }

        if nargs == 3 {
            /* Pass it a boolean isExplicit parameter, too */
            let cons = makeConst(
                BOOLOID,
                -1,
                InvalidOid,
                std::mem::size_of::<bool>() as i32,
                BoolGetDatum(ccontext == COERCION_EXPLICIT),
                false,
                true,
            );
            args = lappend(args, cons as *mut c_void);
        }

        let fexpr = makeFuncExpr(funcId, targetTypeId, args, InvalidOid, InvalidOid, cformat);
        (*fexpr).location = location;
        return fexpr as *mut Node;
    } else if pathtype == COERCION_PATH_ARRAYCOERCE {
        /* We need to build an ArrayCoerceExpr */
        let acoerce = makeNode!(ArrayCoerceExpr, T_ArrayCoerceExpr);
        let ctest = makeNode!(CaseTestExpr, T_CaseTestExpr);
        let mut sourceBaseTypeMod = exprTypmod(node);
        let sourceBaseTypeId = getBaseTypeAndTypmod(exprType(node), &mut sourceBaseTypeMod);

        /*
         * Set up a CaseTestExpr representing one element of the source array.
         * This is an abuse of CaseTestExpr, but it's OK as long as there
         * can't be any CaseExpr or ArrayCoerceExpr within the completed
         * elemexpr.
         */
        (*ctest).typeId = get_element_type(sourceBaseTypeId);
        assert!(OidIsValid((*ctest).typeId));
        (*ctest).typeMod = sourceBaseTypeMod;
        (*ctest).collation = InvalidOid; /* Assume coercions don't care */

        /* And coerce it to the target element type */
        let targetElementType = get_element_type(targetTypeId);
        assert!(OidIsValid(targetElementType));

        let elemexpr = coerce_to_target_type(
            std::ptr::null_mut(),
            ctest as *mut Node,
            (*ctest).typeId,
            targetElementType,
            targetTypMod,
            ccontext,
            cformat,
            location,
        );
        if elemexpr.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "failed to coerce array element type as expected");
        }

        (*acoerce).arg = node as *mut Expr;
        (*acoerce).elemexpr = elemexpr as *mut Expr;
        (*acoerce).resulttype = targetTypeId;

        /*
         * Label the output as having a particular element typmod only if we
         * ended up with a per-element expression that is labeled that way.
         */
        (*acoerce).resulttypmod = exprTypmod(elemexpr);
        /* resultcollid will be set by parse_collate.c */
        (*acoerce).coerceformat = cformat;
        (*acoerce).location = location;

        return acoerce as *mut Node;
    } else if pathtype == COERCION_PATH_COERCEVIAIO {
        /* We need to build a CoerceViaIO node */
        let iocoerce = makeNode!(CoerceViaIO, T_CoerceViaIO);

        assert!(!OidIsValid(funcId));

        (*iocoerce).arg = node as *mut Expr;
        (*iocoerce).resulttype = targetTypeId;
        /* resultcollid will be set by parse_collate.c */
        (*iocoerce).coerceformat = cformat;
        (*iocoerce).location = location;

        return iocoerce as *mut Node;
    } else {
        elog!(ERROR, "unsupported pathtype {} in build_coercion_expression", pathtype as i32);
        std::ptr::null_mut() /* keep compiler quiet */
    }
}


/*
 * coerce_record_to_complex
 *   Coerce a RECORD to a specific composite type.
 *
 * Currently we only support this for inputs that are RowExprs or whole-row
 * Vars.
 */
unsafe fn coerce_record_to_complex(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
) -> *mut Node {
    let rowexpr: *mut RowExpr;
    let mut baseTypeMod: int32 = -1;
    let tupdesc: TupleDesc;
    let mut args: *mut List = NIL;
    let mut newargs: *mut List = NIL;
    let mut ucolno: c_int;
    let mut arg: *mut ListCell;

    if !node.is_null() && IsA!(node, T_RowExpr) {
        /*
         * Since the RowExpr must be of type RECORD, we needn't worry about it
         * containing any dropped columns.
         */
        args = (*(node as *mut RowExpr)).args;
    } else if !node.is_null()
        && IsA!(node, T_Var)
        && (*(node as *mut Var)).varattno == 0 /* InvalidAttrNumber */
    {
        let rtindex = (*(node as *mut Var)).varno as c_int;
        let sublevels_up = (*(node as *mut Var)).varlevelsup as c_int;
        let vlocation = (*(node as *mut Var)).location;
        let nsitem = GetNSItemByRangeTablePosn(pstate, rtindex, sublevels_up);
        args = expandNSItemVars(pstate, nsitem, sublevels_up, vlocation, std::ptr::null_mut());
    } else {
        ereport!(ERROR,
            errmsg!("cannot cast type {} to {}",
                std::ffi::CStr::from_ptr(format_type_be(RECORDOID)).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
            /* errcode(ERRCODE_CANNOT_COERCE), errposition from parser_coercion_errposition */
        );
        /* parser_coercion_errposition(pstate, location, node) - omitted from ereport shim */
        unreachable!()
    }

    /*
     * Look up the composite type, accounting for possibility that what we are
     * given is a domain over composite.
     */
    let baseTypeId = getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod);
    tupdesc = lookup_rowtype_tupdesc(baseTypeId, baseTypeMod);

    /* Process the fields */
    newargs = NIL;
    ucolno = 1;
    arg = list_head(args);
    for i in 0..(*tupdesc).natts {
        let attr = TupleDescAttr(tupdesc, i as c_int);

        /* Fill in NULLs for dropped columns in rowtype */
        if (*attr).attisdropped {
            /*
             * can't use atttypid here, but it doesn't really matter what type
             * the Const claims to be.
             */
            newargs = lappend(newargs, makeNullConst(INT4OID, -1, InvalidOid) as *mut c_void);
            continue;
        }

        if arg.is_null() {
            ereport!(ERROR,
                errmsg!("cannot cast type {} to {}: Input has too few columns.",
                    std::ffi::CStr::from_ptr(format_type_be(RECORDOID)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
                /* errcode(ERRCODE_CANNOT_COERCE), errdetail, errposition omitted */
            );
            unreachable!()
        }
        let expr = lfirst(arg) as *mut Node;
        let exprtype = exprType(expr);

        let cexpr = coerce_to_target_type(
            pstate,
            expr, exprtype,
            (*attr).atttypid,
            (*attr).atttypmod,
            ccontext,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if cexpr.is_null() {
            ereport!(ERROR,
                errmsg!("cannot cast type {} to {}: Cannot cast type {} to {} in column {}.",
                    std::ffi::CStr::from_ptr(format_type_be(RECORDOID)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(exprtype)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be((*attr).atttypid)).to_string_lossy(),
                    ucolno)
                /* errcode(ERRCODE_CANNOT_COERCE), errdetail, errposition omitted */
            );
            unreachable!()
        }
        newargs = lappend(newargs, cexpr as *mut c_void);
        ucolno += 1;
        arg = lnext(args, arg);
    }
    if !arg.is_null() {
        ereport!(ERROR,
            errmsg!("cannot cast type {} to {}: Input has too many columns.",
                std::ffi::CStr::from_ptr(format_type_be(RECORDOID)).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
            /* errcode(ERRCODE_CANNOT_COERCE), errdetail, errposition omitted */
        );
        unreachable!()
    }

    ReleaseTupleDesc(tupdesc);

    let rowexpr = makeNode!(RowExpr, T_RowExpr);
    (*rowexpr).args = newargs;
    (*rowexpr).row_typeid = baseTypeId;
    (*rowexpr).row_format = cformat;
    (*rowexpr).colnames = NIL; /* not needed for named target type */
    (*rowexpr).location = location;

    /* If target is a domain, apply constraints */
    if baseTypeId != targetTypeId {
        (*rowexpr).row_format = COERCE_IMPLICIT_CAST;
        return coerce_to_domain(
            rowexpr as *mut Node,
            baseTypeId, baseTypeMod,
            targetTypeId,
            ccontext, cformat, location,
            false,
        );
    }

    rowexpr as *mut Node
}


/*
 * coerce_to_boolean()
 *		Coerce an argument of a construct that requires boolean input
 *		(AND, OR, NOT, etc.).  Also check that input is not a set.
 *
 * Returns the possibly-transformed node tree.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
pub unsafe fn coerce_to_boolean(
    pstate: *mut ParseState,
    node: *mut Node,
    constructName: *const c_char,
) -> *mut Node {
    let mut node = node;
    let inputTypeId = exprType(node);

    if inputTypeId != BOOLOID {
        let newnode = coerce_to_target_type(
            pstate, node, inputTypeId,
            BOOLOID, -1,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if newnode.is_null() {
            let cname = std::ffi::CStr::from_ptr(constructName).to_string_lossy();
            ereport!(ERROR,
                errmsg!("argument of {} must be type {}, not type {}",
                    cname,
                    "boolean",
                    std::ffi::CStr::from_ptr(format_type_be(inputTypeId)).to_string_lossy())
                /* errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition omitted */
            );
            unreachable!()
        }
        node = newnode;
    }

    if expression_returns_set(node) {
        let cname = std::ffi::CStr::from_ptr(constructName).to_string_lossy();
        ereport!(ERROR,
            errmsg!("argument of {} must not return a set", cname)
        );
        unreachable!()
    }

    node
}

/*
 * coerce_to_specific_type_typmod()
 *		Coerce an argument of a construct that requires a specific data type,
 *		with a specific typmod.  Also check that input is not a set.
 *
 * Returns the possibly-transformed node tree.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
pub unsafe fn coerce_to_specific_type_typmod(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    targetTypmod: int32,
    constructName: *const c_char,
) -> *mut Node {
    let mut node = node;
    let inputTypeId = exprType(node);

    if inputTypeId != targetTypeId {
        let newnode = coerce_to_target_type(
            pstate, node, inputTypeId,
            targetTypeId, targetTypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if newnode.is_null() {
            let cname = std::ffi::CStr::from_ptr(constructName).to_string_lossy();
            ereport!(ERROR,
                errmsg!("argument of {} must be type {}, not type {}",
                    cname,
                    std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(inputTypeId)).to_string_lossy())
                /* errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition omitted */
            );
            unreachable!()
        }
        node = newnode;
    }

    if expression_returns_set(node) {
        let cname = std::ffi::CStr::from_ptr(constructName).to_string_lossy();
        ereport!(ERROR,
            errmsg!("argument of {} must not return a set", cname)
        );
        unreachable!()
    }

    node
}

/*
 * coerce_to_specific_type()
 *		Coerce an argument of a construct that requires a specific data type.
 *		Also check that input is not a set.
 *
 * Returns the possibly-transformed node tree.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
pub unsafe fn coerce_to_specific_type(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    constructName: *const c_char,
) -> *mut Node {
    coerce_to_specific_type_typmod(pstate, node, targetTypeId, -1, constructName)
}

/*
 * coerce_null_to_domain()
 *		Build a NULL constant, then wrap it in CoerceToDomain
 *		if the desired type is a domain type.
 */
pub unsafe fn coerce_null_to_domain(
    typid: Oid,
    typmod: int32,
    collation: Oid,
    typlen: c_int,
    typbyval: bool,
) -> *mut Node {
    let mut baseTypeMod: int32 = typmod;
    let baseTypeId = getBaseTypeAndTypmod(typid, &mut baseTypeMod);
    let mut result: *mut Node = makeConst(
        baseTypeId,
        baseTypeMod,
        collation,
        typlen,
        0 as crate::postgres::Datum, /* (Datum) 0 */
        true,  /* isnull */
        typbyval,
    ) as *mut Node;
    if typid != baseTypeId {
        result = coerce_to_domain(
            result,
            baseTypeId, baseTypeMod,
            typid,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
            false,
        );
    }
    result
}

/*
 * parser_coercion_errposition - report coercion error location, if possible
 *
 * We prefer to point at the coercion request (CAST, ::, etc) if possible;
 * but there may be no such location in the case of an implicit coercion.
 * In that case point at the input expression.
 */
pub unsafe fn parser_coercion_errposition(
    pstate: *mut ParseState,
    coerce_location: c_int,
    input_expr: *mut Node,
) -> c_int {
    if coerce_location >= 0 {
        parser_errposition(pstate, coerce_location)
    } else {
        parser_errposition(pstate, exprLocation(input_expr))
    }
}

/*
 * select_common_type()
 *		Determine the common supertype of a list of input expressions.
 *		This is used for determining the output type of CASE, UNION,
 *		and similar constructs.
 *
 * 'exprs' is a *nonempty* list of expressions.  Note that earlier items
 * in the list will be preferred if there is doubt.
 * 'context' is a phrase to use in the error message if we fail to select
 * a usable type.  Pass NULL to have the routine return InvalidOid
 * rather than throwing an error on failure.
 * 'which_expr': if not NULL, receives a pointer to the particular input
 * expression from which the result type was taken.
 *
 * Caution: "failure" just means that there were inputs of different type
 * categories.  It is not guaranteed that all the inputs are coercible to the
 * selected type; caller must check that (see verify_common_type).
 */
pub unsafe fn select_common_type(
    pstate: *mut ParseState,
    exprs: *mut List,
    context: *const c_char,
    which_expr: *mut *mut Node,
) -> Oid {
    let pexpr = linitial(exprs) as *mut Node;
    let mut lc = list_second_cell(exprs);
    let mut ptype = exprType(pexpr);
    let mut pexpr_cur = pexpr;

    /*
     * If all input types are valid and exactly the same, just pick that type.
     * This is the only way that we will resolve the result as being a domain
     * type; otherwise domains are smashed to their base types for comparison.
     */
    if ptype != UNKNOWNOID {
        while !lc.is_null() {
            let nexpr = lfirst(lc) as *mut Node;
            let ntype = exprType(nexpr);
            if ntype != ptype {
                break;
            }
            lc = lnext(exprs, lc);
        }
        if lc.is_null() {
            /* got to the end of the list */
            if !which_expr.is_null() {
                *which_expr = pexpr_cur;
            }
            return ptype;
        }
    }

    /*
     * Nope, so set up for the full algorithm.
     */
    ptype = getBaseType(ptype);
    let mut pcategory: TYPCATEGORY = 0;
    let mut pispreferred: bool = false;
    get_type_category_preferred(ptype, &mut pcategory, &mut pispreferred);

    while !lc.is_null() {
        let nexpr = lfirst(lc) as *mut Node;
        let ntype = getBaseType(exprType(nexpr));

        /* move on to next one if no new information... */
        if ntype != UNKNOWNOID && ntype != ptype {
            let mut ncategory: TYPCATEGORY = 0;
            let mut nispreferred: bool = false;
            get_type_category_preferred(ntype, &mut ncategory, &mut nispreferred);
            if ptype == UNKNOWNOID {
                /* so far, only unknowns so take anything... */
                pexpr_cur = nexpr;
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                /*
                 * both types in different categories? then not much hope...
                 */
                if context.is_null() {
                    return InvalidOid;
                }
                ereport!(ERROR,
                    errmsg!("{} types {} and {} cannot be matched",
                        std::ffi::CStr::from_ptr(context).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(ptype)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(ntype)).to_string_lossy())
                    /* errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition omitted */
                );
                unreachable!()
            } else if !pispreferred
                && can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)
                && !can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT)
            {
                /*
                 * take new type if can coerce to it implicitly but not the
                 * other way; but if we have a preferred type, stay on it.
                 */
                pexpr_cur = nexpr;
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
        lc = lnext(exprs, lc);
    }

    /*
     * If all the inputs were UNKNOWN type --- ie, unknown-type literals ---
     * then resolve as type TEXT.
     */
    if ptype == UNKNOWNOID {
        ptype = TEXTOID;
    }

    if !which_expr.is_null() {
        *which_expr = pexpr_cur;
    }
    ptype
}

/*
 * select_common_type_from_oids()
 *		Determine the common supertype of an array of type OIDs.
 *
 * This is the same logic as select_common_type(), but working from
 * an array of type OIDs not a list of expressions.  On failure, return
 * InvalidOid if noerror is true, else throw an error.
 */
unsafe fn select_common_type_from_oids(
    nargs: c_int,
    typeids: *const Oid,
    noerror: bool,
) -> Oid {
    let nargs = nargs as usize;
    assert!(nargs > 0);
    let mut ptype = *typeids.add(0);
    let mut i: usize = 1;

    /* If all input types are valid and exactly the same, pick that type. */
    if ptype != UNKNOWNOID {
        while i < nargs {
            if *typeids.add(i) != ptype {
                break;
            }
            i += 1;
        }
        if i == nargs {
            return ptype;
        }
    }

    ptype = getBaseType(ptype);
    let mut pcategory: TYPCATEGORY = 0;
    let mut pispreferred: bool = false;
    get_type_category_preferred(ptype, &mut pcategory, &mut pispreferred);

    while i < nargs {
        let ntype = getBaseType(*typeids.add(i));

        /* move on to next one if no new information... */
        if ntype != UNKNOWNOID && ntype != ptype {
            let mut ncategory: TYPCATEGORY = 0;
            let mut nispreferred: bool = false;
            get_type_category_preferred(ntype, &mut ncategory, &mut nispreferred);
            if ptype == UNKNOWNOID {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if ncategory != pcategory {
                if noerror {
                    return InvalidOid;
                }
                ereport!(ERROR,
                    errmsg!("argument types {} and {} cannot be matched",
                        std::ffi::CStr::from_ptr(format_type_be(ptype)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(ntype)).to_string_lossy())
                );
                unreachable!()
            } else if !pispreferred
                && can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)
                && !can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT)
            {
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            }
        }
        i += 1;
    }

    /* Like select_common_type(), choose TEXT if all inputs were UNKNOWN */
    if ptype == UNKNOWNOID {
        ptype = TEXTOID;
    }

    ptype
}

/*
 * coerce_to_common_type()
 *		Coerce an expression to the given type.
 *
 * This is used following select_common_type() to coerce the individual
 * expressions to the desired type.
 */
pub unsafe fn coerce_to_common_type(
    pstate: *mut ParseState,
    node: *mut Node,
    targetTypeId: Oid,
    context: *const c_char,
) -> *mut Node {
    let inputTypeId = exprType(node);

    if inputTypeId == targetTypeId {
        return node; /* no work */
    }
    if can_coerce_type(1, &inputTypeId, &targetTypeId, COERCION_IMPLICIT) {
        coerce_type(pstate, node, inputTypeId, targetTypeId, -1,
                    COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1)
    } else {
        ereport!(ERROR,
            errmsg!("{} could not convert type {} to {}",
                std::ffi::CStr::from_ptr(context).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(inputTypeId)).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
            /* errcode(ERRCODE_CANNOT_COERCE), parser_errposition omitted */
        );
        unreachable!()
    }
}

/*
 * verify_common_type()
 *		Verify that all input types can be coerced to a proposed common type.
 *		Return true if so, false if not all coercions are possible.
 */
pub unsafe fn verify_common_type(common_type: Oid, exprs: *mut List) -> bool {
    let mut lc = list_head(exprs);
    while !lc.is_null() {
        let nexpr = lfirst(lc) as *mut Node;
        let ntype = exprType(nexpr);
        if !can_coerce_type(1, &ntype, &common_type, COERCION_IMPLICIT) {
            return false;
        }
        lc = lnext(exprs, lc);
    }
    true
}

/*
 * verify_common_type_from_oids()
 *		As above, but work from an array of type OIDs.
 */
unsafe fn verify_common_type_from_oids(
    common_type: Oid,
    nargs: c_int,
    typeids: *const Oid,
) -> bool {
    for i in 0..(nargs as usize) {
        if !can_coerce_type(1, typeids.add(i), &common_type, COERCION_IMPLICIT) {
            return false;
        }
    }
    true
}

/*
 * select_common_typmod()
 *		Determine the common typmod of a list of input expressions.
 *
 * common_type is the selected common type of the expressions, typically
 * computed using select_common_type().
 */
pub unsafe fn select_common_typmod(
    _pstate: *mut ParseState,
    exprs: *mut List,
    common_type: Oid,
) -> int32 {
    let mut first = true;
    let mut result: int32 = -1;
    let mut lc = list_head(exprs);
    while !lc.is_null() {
        let expr = lfirst(lc) as *mut Node;
        /* Types must match */
        if exprType(expr) != common_type {
            return -1;
        } else if first {
            result = exprTypmod(expr);
            first = false;
        } else {
            /* As soon as we see a non-matching typmod, fall back to -1 */
            if result != exprTypmod(expr) {
                return -1;
            }
        }
        lc = lnext(exprs, lc);
    }
    result
}

/*
 * check_generic_type_consistency()
 *		Are the actual arguments potentially compatible with a
 *		polymorphic function?
 *
 * See function header comment in C source for full rule descriptions.
 *
 * We do not ereport here, but just return false if a rule is violated.
 */
pub unsafe fn check_generic_type_consistency(
    actual_arg_types: *const Oid,
    declared_arg_types: *const Oid,
    nargs: c_int,
) -> bool {
    let nargs = nargs as usize;
    let mut elem_typeid: Oid = InvalidOid;
    let mut array_typeid: Oid = InvalidOid;
    let mut range_typeid: Oid = InvalidOid;
    let mut multirange_typeid: Oid = InvalidOid;
    let mut anycompatible_range_typeid: Oid = InvalidOid;
    let mut anycompatible_range_typelem: Oid = InvalidOid;
    let mut anycompatible_multirange_typeid: Oid = InvalidOid;
    let mut anycompatible_multirange_typelem: Oid = InvalidOid;
    let mut _range_typelem: Oid = InvalidOid;
    let mut have_anynonarray = false;
    let mut have_anyenum = false;
    let mut have_anycompatible_nonarray = false;
    let mut n_anycompatible_args: usize = 0;
    let mut anycompatible_actual_types: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];

    assert!(nargs <= FUNC_MAX_ARGS);
    for j in 0..nargs {
        let decl_type = *declared_arg_types.add(j);
        let mut actual_type = *actual_arg_types.add(j);

        if decl_type == ANYELEMENTOID
            || decl_type == ANYNONARRAYOID
            || decl_type == ANYENUMOID
        {
            if decl_type == ANYNONARRAYOID {
                have_anynonarray = true;
            } else if decl_type == ANYENUMOID {
                have_anyenum = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            if OidIsValid(elem_typeid) && actual_type != elem_typeid {
                return false;
            }
            elem_typeid = actual_type;
        } else if decl_type == ANYARRAYOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(array_typeid) && actual_type != array_typeid {
                return false;
            }
            array_typeid = actual_type;
        } else if decl_type == ANYRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(range_typeid) && actual_type != range_typeid {
                return false;
            }
            range_typeid = actual_type;
        } else if decl_type == ANYMULTIRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(multirange_typeid) && actual_type != multirange_typeid {
                return false;
            }
            multirange_typeid = actual_type;
        } else if decl_type == ANYCOMPATIBLEOID
            || decl_type == ANYCOMPATIBLENONARRAYOID
        {
            if decl_type == ANYCOMPATIBLENONARRAYOID {
                have_anycompatible_nonarray = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            /* collect the actual types of non-unknown COMPATIBLE args */
            anycompatible_actual_types[n_anycompatible_args] = actual_type;
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLEARRAYOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            let elem_type = get_element_type(actual_type);
            if !OidIsValid(elem_type) {
                return false; /* not an array */
            }
            /* collect the element type for common-supertype choice */
            anycompatible_actual_types[n_anycompatible_args] = elem_type;
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLERANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(anycompatible_range_typeid) {
                /* All ANYCOMPATIBLERANGE arguments must be the same type */
                if anycompatible_range_typeid != actual_type {
                    return false;
                }
            } else {
                anycompatible_range_typeid = actual_type;
                anycompatible_range_typelem = get_range_subtype(actual_type);
                if !OidIsValid(anycompatible_range_typelem) {
                    return false; /* not a range type */
                }
                /* collect the subtype for common-supertype choice */
                anycompatible_actual_types[n_anycompatible_args] =
                    anycompatible_range_typelem;
                n_anycompatible_args += 1;
            }
        } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
            if actual_type == UNKNOWNOID {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(anycompatible_multirange_typeid) {
                /* All ANYCOMPATIBLEMULTIRANGE arguments must be the same type */
                if anycompatible_multirange_typeid != actual_type {
                    return false;
                }
            } else {
                anycompatible_multirange_typeid = actual_type;
                anycompatible_multirange_typelem =
                    get_multirange_range(actual_type);
                if !OidIsValid(anycompatible_multirange_typelem) {
                    return false; /* not a multirange type */
                }
                /* we'll consider the subtype below */
            }
        }
    }

    /* Get the element type based on the array type, if we have one */
    if OidIsValid(array_typeid) {
        if array_typeid != ANYARRAYOID {
            let array_typelem = get_element_type(array_typeid);
            if !OidIsValid(array_typelem) {
                return false; /* should be an array, but isn't */
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = array_typelem;
            } else if array_typelem != elem_typeid {
                return false;
            }
        }
        /* else: ANYARRAYOID input -- allow for now */
    }

    /* Deduce range type from multirange type, or check that they agree */
    if OidIsValid(multirange_typeid) {
        let multirange_typelem = get_multirange_range(multirange_typeid);
        if !OidIsValid(multirange_typelem) {
            return false; /* should be a multirange, but isn't */
        }
        if !OidIsValid(range_typeid) {
            range_typeid = multirange_typelem;
            _range_typelem = get_range_subtype(multirange_typelem);
            if !OidIsValid(_range_typelem) {
                return false; /* should be a range, but isn't */
            }
        } else if multirange_typelem != range_typeid {
            return false;
        }
    }

    /* Get the element type based on the range type, if we have one */
    if OidIsValid(range_typeid) {
        _range_typelem = get_range_subtype(range_typeid);
        if !OidIsValid(_range_typelem) {
            return false; /* should be a range, but isn't */
        }
        if !OidIsValid(elem_typeid) {
            elem_typeid = _range_typelem;
        } else if _range_typelem != elem_typeid {
            return false;
        }
    }

    if have_anynonarray {
        /* require the element type to not be an array or domain over array */
        if type_is_array_domain(elem_typeid) {
            return false;
        }
    }

    if have_anyenum {
        /* require the element type to be an enum */
        if !type_is_enum(elem_typeid) {
            return false;
        }
    }

    /* Deduce range type from multirange type, or check that they agree */
    if OidIsValid(anycompatible_multirange_typeid) {
        if OidIsValid(anycompatible_range_typeid) {
            if anycompatible_multirange_typelem != anycompatible_range_typeid {
                return false;
            }
        } else {
            anycompatible_range_typeid = anycompatible_multirange_typelem;
            anycompatible_range_typelem =
                get_range_subtype(anycompatible_range_typeid);
            if !OidIsValid(anycompatible_range_typelem) {
                return false; /* not a range type */
            }
            /* collect the subtype for common-supertype choice */
            anycompatible_actual_types[n_anycompatible_args] =
                anycompatible_range_typelem;
            n_anycompatible_args += 1;
        }
    }

    /* Check matching of ANYCOMPATIBLE-family arguments, if any */
    if n_anycompatible_args > 0 {
        let anycompatible_typeid = select_common_type_from_oids(
            n_anycompatible_args as c_int,
            anycompatible_actual_types.as_ptr(),
            true,
        );

        if !OidIsValid(anycompatible_typeid) {
            return false; /* there's definitely no common supertype */
        }

        /* We have to verify that the selected type actually works */
        if !verify_common_type_from_oids(
            anycompatible_typeid,
            n_anycompatible_args as c_int,
            anycompatible_actual_types.as_ptr(),
        ) {
            return false;
        }

        if have_anycompatible_nonarray {
            if type_is_array_domain(anycompatible_typeid) {
                return false;
            }
        }

        /*
         * The anycompatible type must exactly match the range element type,
         * if we were able to identify one.
         */
        if OidIsValid(anycompatible_range_typelem)
            && anycompatible_range_typelem != anycompatible_typeid
        {
            return false;
        }
    }

    /* Looks valid */
    true
}

/*
 * enforce_generic_type_consistency()
 *		Make sure a polymorphic function is legally callable, and
 *		deduce actual argument and result types.
 *
 * See function header comment in C source for full rule descriptions.
 */
pub unsafe fn enforce_generic_type_consistency(
    actual_arg_types: *const Oid,
    declared_arg_types: *mut Oid,
    nargs: c_int,
    rettype: Oid,
    allow_poly: bool,
) -> Oid {
    let nargs = nargs as usize;
    let mut have_poly_anycompatible = false;
    let mut have_poly_unknowns = false;
    let mut elem_typeid: Oid = InvalidOid;
    let mut array_typeid: Oid = InvalidOid;
    let mut range_typeid: Oid = InvalidOid;
    let mut multirange_typeid: Oid = InvalidOid;
    let mut anycompatible_typeid: Oid = InvalidOid;
    let mut anycompatible_array_typeid: Oid = InvalidOid;
    let mut anycompatible_range_typeid: Oid = InvalidOid;
    let mut anycompatible_range_typelem: Oid = InvalidOid;
    let mut anycompatible_multirange_typeid: Oid = InvalidOid;
    let mut anycompatible_multirange_typelem: Oid = InvalidOid;
    let mut have_anynonarray = (rettype == ANYNONARRAYOID);
    let mut have_anyenum = (rettype == ANYENUMOID);
    let mut have_anymultirange = (rettype == ANYMULTIRANGEOID);
    let mut have_anycompatible_nonarray = (rettype == ANYCOMPATIBLENONARRAYOID);
    let mut have_anycompatible_array = (rettype == ANYCOMPATIBLEARRAYOID);
    let mut have_anycompatible_range = (rettype == ANYCOMPATIBLERANGEOID);
    let mut have_anycompatible_multirange = (rettype == ANYCOMPATIBLEMULTIRANGEOID);
    let mut n_poly_args: usize = 0; /* counts all family-1 arguments */
    let mut n_anycompatible_args: usize = 0; /* counts only non-unknowns */
    let mut anycompatible_actual_types: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];

    assert!(nargs <= FUNC_MAX_ARGS);
    for j in 0..nargs {
        let decl_type = *declared_arg_types.add(j);
        let mut actual_type = *actual_arg_types.add(j);

        if decl_type == ANYELEMENTOID
            || decl_type == ANYNONARRAYOID
            || decl_type == ANYENUMOID
        {
            n_poly_args += 1;
            if decl_type == ANYNONARRAYOID {
                have_anynonarray = true;
            } else if decl_type == ANYENUMOID {
                have_anyenum = true;
            }
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue; /* no new information here */
            }
            if OidIsValid(elem_typeid) && actual_type != elem_typeid {
                ereport!(ERROR,
                    errmsg!("arguments declared \"{}\" are not all alike",
                        "anyelement")
                    /* errdetail omitted */
                );
                unreachable!()
            }
            elem_typeid = actual_type;
        } else if decl_type == ANYARRAYOID {
            n_poly_args += 1;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(array_typeid) && actual_type != array_typeid {
                ereport!(ERROR,
                    errmsg!("arguments declared \"{}\" are not all alike",
                        "anyarray")
                );
                unreachable!()
            }
            array_typeid = actual_type;
        } else if decl_type == ANYRANGEOID {
            n_poly_args += 1;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(range_typeid) && actual_type != range_typeid {
                ereport!(ERROR,
                    errmsg!("arguments declared \"{}\" are not all alike",
                        "anyrange")
                );
                unreachable!()
            }
            range_typeid = actual_type;
        } else if decl_type == ANYMULTIRANGEOID {
            n_poly_args += 1;
            have_anymultirange = true;
            if actual_type == UNKNOWNOID {
                have_poly_unknowns = true;
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(multirange_typeid) && actual_type != multirange_typeid {
                ereport!(ERROR,
                    errmsg!("arguments declared \"{}\" are not all alike",
                        "anymultirange")
                );
                unreachable!()
            }
            multirange_typeid = actual_type;
        } else if decl_type == ANYCOMPATIBLEOID
            || decl_type == ANYCOMPATIBLENONARRAYOID
        {
            have_poly_anycompatible = true;
            if decl_type == ANYCOMPATIBLENONARRAYOID {
                have_anycompatible_nonarray = true;
            }
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            /* collect the actual types of non-unknown COMPATIBLE args */
            anycompatible_actual_types[n_anycompatible_args] = actual_type;
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLEARRAYOID {
            have_poly_anycompatible = true;
            have_anycompatible_array = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            let anycompatible_elem_type = get_element_type(actual_type);
            if !OidIsValid(anycompatible_elem_type) {
                ereport!(ERROR,
                    errmsg!("argument declared {} is not an array but type {}",
                        "anycompatiblearray",
                        std::ffi::CStr::from_ptr(format_type_be(actual_type)).to_string_lossy())
                );
                unreachable!()
            }
            /* collect the element type for common-supertype choice */
            anycompatible_actual_types[n_anycompatible_args] = anycompatible_elem_type;
            n_anycompatible_args += 1;
        } else if decl_type == ANYCOMPATIBLERANGEOID {
            have_poly_anycompatible = true;
            have_anycompatible_range = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(anycompatible_range_typeid) {
                /* All ANYCOMPATIBLERANGE arguments must be the same type */
                if anycompatible_range_typeid != actual_type {
                    ereport!(ERROR,
                        errmsg!("arguments declared \"{}\" are not all alike",
                            "anycompatiblerange")
                        /* errdetail omitted */
                    );
                    unreachable!()
                }
            } else {
                anycompatible_range_typeid = actual_type;
                anycompatible_range_typelem = get_range_subtype(actual_type);
                if !OidIsValid(anycompatible_range_typelem) {
                    ereport!(ERROR,
                        errmsg!("argument declared {} is not a range type but type {}",
                            "anycompatiblerange",
                            std::ffi::CStr::from_ptr(format_type_be(actual_type)).to_string_lossy())
                    );
                    unreachable!()
                }
                /* collect the subtype for common-supertype choice */
                anycompatible_actual_types[n_anycompatible_args] =
                    anycompatible_range_typelem;
                n_anycompatible_args += 1;
            }
        } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
            have_poly_anycompatible = true;
            have_anycompatible_multirange = true;
            if actual_type == UNKNOWNOID {
                continue;
            }
            if allow_poly && decl_type == actual_type {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if OidIsValid(anycompatible_multirange_typeid) {
                /* All ANYCOMPATIBLEMULTIRANGE arguments must be the same type */
                if anycompatible_multirange_typeid != actual_type {
                    ereport!(ERROR,
                        errmsg!("arguments declared \"{}\" are not all alike",
                            "anycompatiblemultirange")
                        /* errdetail omitted */
                    );
                    unreachable!()
                }
            } else {
                anycompatible_multirange_typeid = actual_type;
                anycompatible_multirange_typelem =
                    get_multirange_range(actual_type);
                if !OidIsValid(anycompatible_multirange_typelem) {
                    ereport!(ERROR,
                        errmsg!("argument declared {} is not a multirange type but type {}",
                            "anycompatiblemultirange",
                            std::ffi::CStr::from_ptr(format_type_be(actual_type)).to_string_lossy())
                    );
                    unreachable!()
                }
                /* we'll consider the subtype below */
            }
        }
    }

    /*
     * Fast Track: if none of the arguments are polymorphic, return the
     * unmodified rettype.
     */
    if n_poly_args == 0 && !have_poly_anycompatible {
        return rettype;
    }

    /* Check matching of family-1 polymorphic arguments, if any */
    if n_poly_args > 0 {
        /* Get the element type based on the array type, if we have one */
        if OidIsValid(array_typeid) {
            let array_typelem;
            if array_typeid == ANYARRAYOID {
                /*
                 * Special case: allow ANYARRAY input to ANYARRAY argument iff
                 * it's the only poly arg and result doesn't need element type.
                 */
                if n_poly_args != 1
                    || (rettype != ANYARRAYOID
                        && IsPolymorphicTypeFamily1(rettype))
                {
                    ereport!(ERROR,
                        errmsg!("cannot determine element type of \"anyarray\" argument")
                    );
                    unreachable!()
                }
                array_typelem = ANYELEMENTOID;
            } else {
                array_typelem = get_element_type(array_typeid);
                if !OidIsValid(array_typelem) {
                    ereport!(ERROR,
                        errmsg!("argument declared {} is not an array but type {}",
                            "anyarray",
                            std::ffi::CStr::from_ptr(format_type_be(array_typeid)).to_string_lossy())
                    );
                    unreachable!()
                }
            }

            if !OidIsValid(elem_typeid) {
                elem_typeid = array_typelem;
            } else if array_typelem != elem_typeid {
                ereport!(ERROR,
                    errmsg!("argument declared {} is not consistent with argument declared {}",
                        "anyarray", "anyelement")
                    /* errdetail omitted */
                );
                unreachable!()
            }
        }

        /* Deduce range type from multirange type, or vice versa */
        if OidIsValid(multirange_typeid) {
            let multirange_typelem = get_multirange_range(multirange_typeid);
            if !OidIsValid(multirange_typelem) {
                ereport!(ERROR,
                    errmsg!("argument declared {} is not a multirange type but type {}",
                        "anymultirange",
                        std::ffi::CStr::from_ptr(format_type_be(multirange_typeid)).to_string_lossy())
                );
                unreachable!()
            }
            if !OidIsValid(range_typeid) {
                range_typeid = multirange_typelem;
            } else if multirange_typelem != range_typeid {
                ereport!(ERROR,
                    errmsg!("argument declared {} is not consistent with argument declared {}",
                        "anymultirange", "anyrange")
                    /* errdetail omitted */
                );
                unreachable!()
            }
        } else if have_anymultirange && OidIsValid(range_typeid) {
            multirange_typeid = get_range_multirange(range_typeid);
            /* We'll complain below if that didn't work */
        }

        /* Get the element type based on the range type, if we have one */
        if OidIsValid(range_typeid) {
            let range_typelem = get_range_subtype(range_typeid);
            if !OidIsValid(range_typelem) {
                ereport!(ERROR,
                    errmsg!("argument declared {} is not a range type but type {}",
                        "anyrange",
                        std::ffi::CStr::from_ptr(format_type_be(range_typeid)).to_string_lossy())
                );
                unreachable!()
            }
            if !OidIsValid(elem_typeid) {
                elem_typeid = range_typelem;
            } else if range_typelem != elem_typeid {
                ereport!(ERROR,
                    errmsg!("argument declared {} is not consistent with argument declared {}",
                        "anyrange", "anyelement")
                    /* errdetail omitted */
                );
                unreachable!()
            }
        }

        if !OidIsValid(elem_typeid) {
            if allow_poly {
                elem_typeid = ANYELEMENTOID;
                array_typeid = ANYARRAYOID;
                range_typeid = ANYRANGEOID;
                multirange_typeid = ANYMULTIRANGEOID;
            } else {
                ereport!(ERROR,
                    errmsg!("could not determine polymorphic type because input has type {}",
                        "unknown")
                );
                unreachable!()
            }
        }

        if have_anynonarray && elem_typeid != ANYELEMENTOID {
            if type_is_array_domain(elem_typeid) {
                ereport!(ERROR,
                    errmsg!("type matched to anynonarray is an array type: {}",
                        std::ffi::CStr::from_ptr(format_type_be(elem_typeid)).to_string_lossy())
                );
                unreachable!()
            }
        }

        if have_anyenum && elem_typeid != ANYELEMENTOID {
            if !type_is_enum(elem_typeid) {
                ereport!(ERROR,
                    errmsg!("type matched to anyenum is not an enum type: {}",
                        std::ffi::CStr::from_ptr(format_type_be(elem_typeid)).to_string_lossy())
                );
                unreachable!()
            }
        }
    }

    /* Check matching of family-2 polymorphic arguments, if any */
    if have_poly_anycompatible {
        /* Deduce range type from multirange type, or vice versa */
        if OidIsValid(anycompatible_multirange_typeid) {
            if OidIsValid(anycompatible_range_typeid) {
                if anycompatible_multirange_typelem != anycompatible_range_typeid {
                    ereport!(ERROR,
                        errmsg!("argument declared {} is not consistent with argument declared {}",
                            "anycompatiblemultirange", "anycompatiblerange")
                        /* errdetail omitted */
                    );
                    unreachable!()
                }
            } else {
                anycompatible_range_typeid = anycompatible_multirange_typelem;
                anycompatible_range_typelem =
                    get_range_subtype(anycompatible_range_typeid);
                if !OidIsValid(anycompatible_range_typelem) {
                    ereport!(ERROR,
                        errmsg!("argument declared {} is not a multirange type but type {}",
                            "anycompatiblemultirange",
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_multirange_typeid)).to_string_lossy())
                    );
                    unreachable!()
                }
                have_anycompatible_range = true;
                /* collect the subtype for common-supertype choice */
                anycompatible_actual_types[n_anycompatible_args] =
                    anycompatible_range_typelem;
                n_anycompatible_args += 1;
            }
        } else if have_anycompatible_multirange
            && OidIsValid(anycompatible_range_typeid)
        {
            anycompatible_multirange_typeid =
                get_range_multirange(anycompatible_range_typeid);
            /* We'll complain below if that didn't work */
        }

        if n_anycompatible_args > 0 {
            anycompatible_typeid = select_common_type_from_oids(
                n_anycompatible_args as c_int,
                anycompatible_actual_types.as_ptr(),
                false,
            );

            /* We have to verify that the selected type actually works */
            if !verify_common_type_from_oids(
                anycompatible_typeid,
                n_anycompatible_args as c_int,
                anycompatible_actual_types.as_ptr(),
            ) {
                ereport!(ERROR,
                    errmsg!("arguments of anycompatible family cannot be cast to a common type")
                );
                unreachable!()
            }

            if have_anycompatible_array {
                anycompatible_array_typeid = get_array_type(anycompatible_typeid);
                if !OidIsValid(anycompatible_array_typeid) {
                    ereport!(ERROR,
                        errmsg!("could not find array type for data type {}",
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_typeid)).to_string_lossy())
                    );
                    unreachable!()
                }
            }

            if have_anycompatible_range {
                /* we can't infer a range type from the others */
                if !OidIsValid(anycompatible_range_typeid) {
                    ereport!(ERROR,
                        errmsg!("could not determine polymorphic type {} because input has type {}",
                            "anycompatiblerange", "unknown")
                    );
                    unreachable!()
                }
                /* the anycompatible type must exactly match the range element type */
                if anycompatible_range_typelem != anycompatible_typeid {
                    ereport!(ERROR,
                        errmsg!("anycompatiblerange type {} does not match anycompatible type {}",
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_range_typeid)).to_string_lossy(),
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_typeid)).to_string_lossy())
                    );
                    unreachable!()
                }
            }

            if have_anycompatible_multirange {
                /* we can't infer a multirange type from the others */
                if !OidIsValid(anycompatible_multirange_typeid) {
                    ereport!(ERROR,
                        errmsg!("could not determine polymorphic type {} because input has type {}",
                            "anycompatiblemultirange", "unknown")
                    );
                    unreachable!()
                }
                /* the anycompatible type must exactly match the multirange element type */
                if anycompatible_range_typelem != anycompatible_typeid {
                    ereport!(ERROR,
                        errmsg!("anycompatiblemultirange type {} does not match anycompatible type {}",
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_multirange_typeid)).to_string_lossy(),
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_typeid)).to_string_lossy())
                    );
                    unreachable!()
                }
            }

            if have_anycompatible_nonarray {
                if type_is_array_domain(anycompatible_typeid) {
                    ereport!(ERROR,
                        errmsg!("type matched to anycompatiblenonarray is an array type: {}",
                            std::ffi::CStr::from_ptr(format_type_be(anycompatible_typeid)).to_string_lossy())
                    );
                    unreachable!()
                }
            }
        } else {
            if allow_poly {
                anycompatible_typeid = ANYCOMPATIBLEOID;
                anycompatible_array_typeid = ANYCOMPATIBLEARRAYOID;
                anycompatible_range_typeid = ANYCOMPATIBLERANGEOID;
                anycompatible_multirange_typeid = ANYCOMPATIBLEMULTIRANGEOID;
            } else {
                /*
                 * All family-2 polymorphic arguments have UNKNOWN inputs.
                 * Resolve to TEXT as select_common_type() would do.
                 */
                anycompatible_typeid = TEXTOID;
                anycompatible_array_typeid = TEXTARRAYOID;
                if have_anycompatible_range {
                    ereport!(ERROR,
                        errmsg!("could not determine polymorphic type {} because input has type {}",
                            "anycompatiblerange", "unknown")
                    );
                    unreachable!()
                }
                if have_anycompatible_multirange {
                    ereport!(ERROR,
                        errmsg!("could not determine polymorphic type {} because input has type {}",
                            "anycompatiblemultirange", "unknown")
                    );
                    unreachable!()
                }
            }
        }

        /* replace family-2 polymorphic types by selected types */
        for j in 0..nargs {
            let decl_type = *declared_arg_types.add(j);
            if decl_type == ANYCOMPATIBLEOID || decl_type == ANYCOMPATIBLENONARRAYOID {
                *declared_arg_types.add(j) = anycompatible_typeid;
            } else if decl_type == ANYCOMPATIBLEARRAYOID {
                *declared_arg_types.add(j) = anycompatible_array_typeid;
            } else if decl_type == ANYCOMPATIBLERANGEOID {
                *declared_arg_types.add(j) = anycompatible_range_typeid;
            } else if decl_type == ANYCOMPATIBLEMULTIRANGEOID {
                *declared_arg_types.add(j) = anycompatible_multirange_typeid;
            }
        }
    }

    /*
     * If we had any UNKNOWN inputs for family-1 polymorphic arguments,
     * re-scan to assign correct types to them.
     */
    if have_poly_unknowns {
        for j in 0..nargs {
            let decl_type = *declared_arg_types.add(j);
            let actual_type = *actual_arg_types.add(j);

            if actual_type != UNKNOWNOID {
                continue;
            }

            if decl_type == ANYELEMENTOID
                || decl_type == ANYNONARRAYOID
                || decl_type == ANYENUMOID
            {
                *declared_arg_types.add(j) = elem_typeid;
            } else if decl_type == ANYARRAYOID {
                if !OidIsValid(array_typeid) {
                    array_typeid = get_array_type(elem_typeid);
                    if !OidIsValid(array_typeid) {
                        ereport!(ERROR,
                            errmsg!("could not find array type for data type {}",
                                std::ffi::CStr::from_ptr(format_type_be(elem_typeid)).to_string_lossy())
                        );
                        unreachable!()
                    }
                }
                *declared_arg_types.add(j) = array_typeid;
            } else if decl_type == ANYRANGEOID {
                if !OidIsValid(range_typeid) {
                    ereport!(ERROR,
                        errmsg!("could not determine polymorphic type {} because input has type {}",
                            "anyrange", "unknown")
                    );
                    unreachable!()
                }
                *declared_arg_types.add(j) = range_typeid;
            } else if decl_type == ANYMULTIRANGEOID {
                if !OidIsValid(multirange_typeid) {
                    ereport!(ERROR,
                        errmsg!("could not determine polymorphic type {} because input has type {}",
                            "anymultirange", "unknown")
                    );
                    unreachable!()
                }
                *declared_arg_types.add(j) = multirange_typeid;
            }
        }
    }

    /* if we return ANYELEMENT use the appropriate argument type */
    if rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID {
        return elem_typeid;
    }

    /* if we return ANYARRAY use the appropriate argument type */
    if rettype == ANYARRAYOID {
        if !OidIsValid(array_typeid) {
            array_typeid = get_array_type(elem_typeid);
            if !OidIsValid(array_typeid) {
                ereport!(ERROR,
                    errmsg!("could not find array type for data type {}",
                        std::ffi::CStr::from_ptr(format_type_be(elem_typeid)).to_string_lossy())
                );
                unreachable!()
            }
        }
        return array_typeid;
    }

    /* if we return ANYRANGE use the appropriate argument type */
    if rettype == ANYRANGEOID {
        /* this error is unreachable if the function signature is valid: */
        if !OidIsValid(range_typeid) {
            ereport!(ERROR,
                errmsg!("could not determine polymorphic type {} because input has type {}",
                    "anyrange", "unknown")
            );
            unreachable!()
        }
        return range_typeid;
    }

    /* if we return ANYMULTIRANGE use the appropriate argument type */
    if rettype == ANYMULTIRANGEOID {
        if !OidIsValid(multirange_typeid) {
            ereport!(ERROR,
                errmsg!("could not determine polymorphic type {} because input has type {}",
                    "anymultirange", "unknown")
            );
            unreachable!()
        }
        return multirange_typeid;
    }

    /* if we return ANYCOMPATIBLE use the appropriate type */
    if rettype == ANYCOMPATIBLEOID || rettype == ANYCOMPATIBLENONARRAYOID {
        if !OidIsValid(anycompatible_typeid) {
            ereport!(ERROR,
                errmsg!("could not identify anycompatible type")
            );
            unreachable!()
        }
        return anycompatible_typeid;
    }

    /* if we return ANYCOMPATIBLEARRAY use the appropriate type */
    if rettype == ANYCOMPATIBLEARRAYOID {
        if !OidIsValid(anycompatible_array_typeid) {
            ereport!(ERROR,
                errmsg!("could not identify anycompatiblearray type")
            );
            unreachable!()
        }
        return anycompatible_array_typeid;
    }

    /* if we return ANYCOMPATIBLERANGE use the appropriate argument type */
    if rettype == ANYCOMPATIBLERANGEOID {
        if !OidIsValid(anycompatible_range_typeid) {
            ereport!(ERROR,
                errmsg!("could not identify anycompatiblerange type")
            );
            unreachable!()
        }
        return anycompatible_range_typeid;
    }

    /* if we return ANYCOMPATIBLEMULTIRANGE use the appropriate argument type */
    if rettype == ANYCOMPATIBLEMULTIRANGEOID {
        if !OidIsValid(anycompatible_multirange_typeid) {
            ereport!(ERROR,
                errmsg!("could not identify anycompatiblemultirange type")
            );
            unreachable!()
        }
        return anycompatible_multirange_typeid;
    }

    /* we don't return a generic type; send back the original return type */
    rettype
}

/*
 * check_valid_polymorphic_signature()
 *		Is a proposed function signature valid per polymorphism rules?
 *
 * Returns NULL if the signature is valid.  Otherwise, returns a palloc'd,
 * already translated errdetail string saying why not.
 */
pub unsafe fn check_valid_polymorphic_signature(
    ret_type: Oid,
    declared_arg_types: *const Oid,
    nargs: c_int,
) -> *mut c_char {
    let nargs = nargs as usize;
    if ret_type == ANYRANGEOID || ret_type == ANYMULTIRANGEOID {
        /*
         * ANYRANGE and ANYMULTIRANGE require an ANYRANGE or ANYMULTIRANGE
         * input, else we can't tell which of several range types with the
         * same element type to use.
         */
        for i in 0..nargs {
            let dt = *declared_arg_types.add(i);
            if dt == ANYRANGEOID || dt == ANYMULTIRANGEOID {
                return std::ptr::null_mut(); /* OK */
            }
        }
        return psprintf(
            b"A result of type %s requires at least one input of type anyrange or anymultirange.\0".as_ptr() as *const c_char,
            format_type_be(ret_type),
        );
    } else if ret_type == ANYCOMPATIBLERANGEOID || ret_type == ANYCOMPATIBLEMULTIRANGEOID {
        for i in 0..nargs {
            let dt = *declared_arg_types.add(i);
            if dt == ANYCOMPATIBLERANGEOID || dt == ANYCOMPATIBLEMULTIRANGEOID {
                return std::ptr::null_mut(); /* OK */
            }
        }
        return psprintf(
            b"A result of type %s requires at least one input of type anycompatiblerange or anycompatiblemultirange.\0".as_ptr() as *const c_char,
            format_type_be(ret_type),
        );
    } else if IsPolymorphicTypeFamily1(ret_type) {
        /* Otherwise, any family-1 type can be deduced from any other */
        for i in 0..nargs {
            if IsPolymorphicTypeFamily1(*declared_arg_types.add(i)) {
                return std::ptr::null_mut(); /* OK */
            }
        }
        /* Keep this list in sync with IsPolymorphicTypeFamily1! */
        return psprintf1(
            b"A result of type %s requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange.\0".as_ptr() as *const c_char,
        );
    } else if IsPolymorphicTypeFamily2(ret_type) {
        /* Otherwise, any family-2 type can be deduced from any other */
        for i in 0..nargs {
            if IsPolymorphicTypeFamily2(*declared_arg_types.add(i)) {
                return std::ptr::null_mut(); /* OK */
            }
        }
        /* Keep this list in sync with IsPolymorphicTypeFamily2! */
        return psprintf1(
            b"A result of type %s requires at least one input of type anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange.\0".as_ptr() as *const c_char,
        );
    } else {
        std::ptr::null_mut() /* OK, ret_type is not polymorphic */
    }
}

/*
 * check_valid_internal_signature()
 *		Is a proposed function signature valid per INTERNAL safety rules?
 *
 * Returns NULL if OK, or a suitable error message if ret_type is INTERNAL but
 * none of the declared arg types are.
 */
pub unsafe fn check_valid_internal_signature(
    ret_type: Oid,
    declared_arg_types: *const Oid,
    nargs: c_int,
) -> *mut c_char {
    if ret_type == INTERNALOID {
        for i in 0..(nargs as usize) {
            if *declared_arg_types.add(i) == ret_type {
                return std::ptr::null_mut(); /* OK */
            }
        }
        return pstrdup(
            b"A result of type internal requires at least one input of type internal.\0"
                .as_ptr() as *const c_char,
        );
    }
    std::ptr::null_mut() /* OK, ret_type is not INTERNAL */
}


/* TypeCategory()
 *		Assign a category to the specified type OID.
 *
 * NB: this must not return TYPCATEGORY_INVALID.
 */
pub unsafe fn TypeCategory(r#type: Oid) -> TYPCATEGORY {
    let mut typcategory: TYPCATEGORY = 0;
    let mut typispreferred: bool = false;
    get_type_category_preferred(r#type, &mut typcategory, &mut typispreferred);
    assert!(typcategory != TYPCATEGORY_INVALID);
    typcategory as TYPCATEGORY
}


/* IsPreferredType()
 *		Check if this type is a preferred type for the given category.
 *
 * If category is TYPCATEGORY_INVALID, then we'll return true for preferred
 * types of any category; otherwise, only for preferred types of that
 * category.
 */
pub unsafe fn IsPreferredType(category: TYPCATEGORY, r#type: Oid) -> bool {
    let mut typcategory: TYPCATEGORY = 0;
    let mut typispreferred: bool = false;
    get_type_category_preferred(r#type, &mut typcategory, &mut typispreferred);
    if category == typcategory || category == TYPCATEGORY_INVALID {
        typispreferred
    } else {
        false
    }
}


/* IsBinaryCoercible()
 *		Check if srctype is binary-coercible to targettype.
 */
#[no_mangle]
pub unsafe fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> bool {
    let mut castoid: Oid = InvalidOid;
    IsBinaryCoercibleWithCast(srctype, targettype, &mut castoid)
}

/* IsBinaryCoercibleWithCast()
 *		Check if srctype is binary-coercible to targettype.
 *
 * This variant also returns the OID of the pg_cast entry if one is involved.
 * *castoid is set to InvalidOid if no binary-coercible cast exists, or if
 * there is a hard-wired rule for it rather than a pg_cast entry.
 */
pub unsafe fn IsBinaryCoercibleWithCast(
    mut srctype: Oid,
    targettype: Oid,
    castoid: *mut Oid,
) -> bool {
    *castoid = InvalidOid;

    /* Fast path if same type */
    if srctype == targettype {
        return true;
    }

    /* Anything is coercible to ANY or ANYELEMENT or ANYCOMPATIBLE */
    if targettype == ANYOID
        || targettype == ANYELEMENTOID
        || targettype == ANYCOMPATIBLEOID
    {
        return true;
    }

    /* If srctype is a domain, reduce to its base type */
    if OidIsValid(srctype) {
        srctype = getBaseType(srctype);
    }

    /* Somewhat-fast path for domain -> base type case */
    if srctype == targettype {
        return true;
    }

    /* Also accept any array type as coercible to ANY[COMPATIBLE]ARRAY */
    if targettype == ANYARRAYOID || targettype == ANYCOMPATIBLEARRAYOID {
        if type_is_array(srctype) {
            return true;
        }
    }

    /* Also accept any non-array type as coercible to ANY[COMPATIBLE]NONARRAY */
    if targettype == ANYNONARRAYOID || targettype == ANYCOMPATIBLENONARRAYOID {
        if !type_is_array(srctype) {
            return true;
        }
    }

    /* Also accept any enum type as coercible to ANYENUM */
    if targettype == ANYENUMOID {
        if type_is_enum(srctype) {
            return true;
        }
    }

    /* Also accept any range type as coercible to ANY[COMPATIBLE]RANGE */
    if targettype == ANYRANGEOID || targettype == ANYCOMPATIBLERANGEOID {
        if type_is_range(srctype) {
            return true;
        }
    }

    /* Also, any multirange type is coercible to ANY[COMPATIBLE]MULTIRANGE */
    if targettype == ANYMULTIRANGEOID || targettype == ANYCOMPATIBLEMULTIRANGEOID {
        if type_is_multirange(srctype) {
            return true;
        }
    }

    /* Also accept any composite type as coercible to RECORD */
    if targettype == RECORDOID {
        if ISCOMPLEX!(srctype) {
            return true;
        }
    }

    /* Also accept any composite array type as coercible to RECORD[] */
    if targettype == RECORDARRAYOID {
        if is_complex_array(srctype) {
            return true;
        }
    }

    /* Else look in pg_cast */
    let tuple = SearchSysCache2(
        CASTSOURCETARGET,
        ObjectIdGetDatum(srctype),
        ObjectIdGetDatum(targettype),
    );
    if !HeapTupleIsValid(tuple) {
        return false; /* no cast */
    }
    let castForm = GETSTRUCT(tuple) as *mut FormData_pg_cast;

    let result = (*castForm).castmethod == COERCION_METHOD_BINARY as i8
        && (*castForm).castcontext == COERCION_CODE_IMPLICIT as i8;

    if result {
        *castoid = (*castForm).oid;
    }

    ReleaseSysCache(tuple);

    result
}


/*
 * find_coercion_pathway
 *		Look for a coercion pathway between two types.
 *
 * Currently, this deals only with scalar-type cases; it does not consider
 * polymorphic types nor casts between composite types.
 *
 * ccontext determines the set of available casts.
 */
pub unsafe fn find_coercion_pathway(
    mut targetTypeId: Oid,
    mut sourceTypeId: Oid,
    ccontext: CoercionContext,
    funcid: *mut Oid,
) -> CoercionPathType {
    let mut result = COERCION_PATH_NONE;

    *funcid = InvalidOid;

    /* Perhaps the types are domains; if so, look at their base types */
    if OidIsValid(sourceTypeId) {
        sourceTypeId = getBaseType(sourceTypeId);
    }
    if OidIsValid(targetTypeId) {
        targetTypeId = getBaseType(targetTypeId);
    }

    /* Domains are always coercible to and from their base type */
    if sourceTypeId == targetTypeId {
        return COERCION_PATH_RELABELTYPE;
    }

    /* Look in pg_cast */
    let tuple = SearchSysCache2(
        CASTSOURCETARGET,
        ObjectIdGetDatum(sourceTypeId),
        ObjectIdGetDatum(targetTypeId),
    );

    if HeapTupleIsValid(tuple) {
        let castForm = GETSTRUCT(tuple) as *mut FormData_pg_cast;

        /* convert char value for castcontext to CoercionContext enum */
        let castcontext: CoercionContext = match (*castForm).castcontext as u8 {
            x if x == COERCION_CODE_IMPLICIT as u8 => COERCION_IMPLICIT,
            x if x == COERCION_CODE_ASSIGNMENT as u8 => COERCION_ASSIGNMENT,
            x if x == COERCION_CODE_EXPLICIT as u8 => COERCION_EXPLICIT,
            x => {
                ereport!(ERROR,
                    errmsg!("unrecognized castcontext: {}", x as c_int)
                );
                unreachable!()
            }
        };

        /* Rely on ordering of enum for correct behavior here */
        if (ccontext as i32) >= (castcontext as i32) {
            match (*castForm).castmethod as u8 {
                x if x == COERCION_METHOD_FUNCTION as u8 => {
                    result = COERCION_PATH_FUNC;
                    *funcid = (*castForm).castfunc;
                }
                x if x == COERCION_METHOD_INOUT as u8 => {
                    result = COERCION_PATH_COERCEVIAIO;
                }
                x if x == COERCION_METHOD_BINARY as u8 => {
                    result = COERCION_PATH_RELABELTYPE;
                }
                x => {
                    ereport!(ERROR,
                        errmsg!("unrecognized castmethod: {}", x as c_int)
                    );
                    unreachable!()
                }
            }
        }

        ReleaseSysCache(tuple);
    } else {
        /*
         * If there's no pg_cast entry, perhaps we are dealing with a pair of
         * array types.  If so, and if their element types have a conversion
         * pathway, report that we can coerce with an ArrayCoerceExpr.
         *
         * Hack: disallow coercions to oidvector and int2vector.
         */
        if targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID {
            let targetElem = get_element_type(targetTypeId);
            let sourceElem = get_element_type(sourceTypeId);

            if OidIsValid(targetElem) && OidIsValid(sourceElem) {
                let mut elemfuncid: Oid = InvalidOid;
                let elempathtype = find_coercion_pathway(
                    targetElem,
                    sourceElem,
                    ccontext,
                    &mut elemfuncid,
                );
                if elempathtype != COERCION_PATH_NONE {
                    result = COERCION_PATH_ARRAYCOERCE;
                }
            }
        }

        /*
         * If we still haven't found a possibility, consider automatic casting
         * using I/O functions.
         */
        if result == COERCION_PATH_NONE {
            if (ccontext as i32) >= (COERCION_ASSIGNMENT as i32)
                && TypeCategory(targetTypeId) == TYPCATEGORY_STRING
            {
                result = COERCION_PATH_COERCEVIAIO;
            } else if (ccontext as i32) >= (COERCION_EXPLICIT as i32)
                && TypeCategory(sourceTypeId) == TYPCATEGORY_STRING
            {
                result = COERCION_PATH_COERCEVIAIO;
            }
        }
    }

    /*
     * When parsing PL/pgSQL assignments, allow an I/O cast to be used
     * whenever no normal coercion is available.
     */
    if result == COERCION_PATH_NONE && ccontext == COERCION_PLPGSQL {
        result = COERCION_PATH_COERCEVIAIO;
    }

    result
}


/*
 * find_typmod_coercion_function -- does the given type need length coercion?
 *
 * If the target type possesses a pg_cast function from itself to itself,
 * it must need length coercion.
 *
 * We use the same result enum as find_coercion_pathway, but the only possible
 * result codes are:
 *	COERCION_PATH_NONE: no length coercion needed
 *	COERCION_PATH_FUNC: apply the function returned in *funcid
 *	COERCION_PATH_ARRAYCOERCE: apply the function using ArrayCoerceExpr
 */
pub unsafe fn find_typmod_coercion_function(
    mut typeId: Oid,
    funcid: *mut Oid,
) -> CoercionPathType {
    *funcid = InvalidOid;
    let mut result = COERCION_PATH_FUNC;

    let targetType = typeidType(typeId);
    let typeForm = GETSTRUCT(targetType) as *mut FormData_pg_type;

    /* Check for a "true" array type */
    if IsTrueArrayType(typeForm) {
        /* Yes, switch our attention to the element type */
        typeId = (*typeForm).typelem;
        result = COERCION_PATH_ARRAYCOERCE;
    }
    ReleaseSysCache(targetType);

    /* Look in pg_cast */
    let tuple = SearchSysCache2(
        CASTSOURCETARGET,
        ObjectIdGetDatum(typeId),
        ObjectIdGetDatum(typeId),
    );

    if HeapTupleIsValid(tuple) {
        let castForm = GETSTRUCT(tuple) as *mut FormData_pg_cast;
        *funcid = (*castForm).castfunc;
        ReleaseSysCache(tuple);
    }

    if !OidIsValid(*funcid) {
        result = COERCION_PATH_NONE;
    }

    result
}

/*
 * is_complex_array
 *		Is this type an array of composite?
 *
 * Note: this will not return true for record[]; check for RECORDARRAYOID
 * separately if needed.
 */
unsafe fn is_complex_array(typid: Oid) -> bool {
    let elemtype = get_element_type(typid);
    OidIsValid(elemtype) && ISCOMPLEX!(elemtype)
}


/*
 * Check whether reltypeId is the row type of a typed table of type
 * reloftypeId, or is a domain over such a row type.
 */
unsafe fn typeIsOfTypedTable(reltypeId: Oid, reloftypeId: Oid) -> bool {
    let relid = typeOrDomainTypeRelid(reltypeId);
    let mut result = false;

    if relid != InvalidOid {
        let tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid(tp) {
            ereport!(ERROR,
                errmsg!("cache lookup failed for relation {}", relid)
            );
            unreachable!()
        }

        let reltup = GETSTRUCT(tp) as *mut crate::catalog::pg_class::FormData_pg_class;
        if (*reltup).reloftype == reloftypeId {
            result = true;
        }

        ReleaseSysCache(tp);
    }

    result
}

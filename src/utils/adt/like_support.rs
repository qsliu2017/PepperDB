//! Translation of postgres/src/backend/utils/adt/like_support.c
//!
//! Planner support functions for LIKE, regex, and related operators.
//!
//! These routines handle special optimization of operators that can be
//! used with index scans even though they are not known to the executor's
//! indexscan machinery.  The key idea is that these operators allow us
//! to derive approximate indexscan qual clauses, such that any tuples
//! that pass the operator clause itself must also satisfy the simpler
//! indexscan condition(s).  Then we can use the indexscan machinery
//! to avoid scanning as much of the table as we'd otherwise have to,
//! while applying the original operator as a qpqual condition to ensure
//! we deliver only the tuples we want.  (In essence, we're using a regular
//! index as if it were a lossy index.)
//!
//! An example of what we're doing is
//!			textfield LIKE 'abc%def'
//! from which we can generate the indexscanable conditions
//!			textfield >= 'abc' AND textfield < 'abd'
//! which allow efficient scanning of an index on textfield.
//! (In reality, character set and collation issues make the transformation
//! from LIKE to indexscan limits rather harder than one might think ...
//! but that's the basic idea.)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include <math.h>
//!   #include "access/htup_details.h"
//!   #include "catalog/pg_collation.h"
//!   #include "catalog/pg_operator.h"
//!   #include "catalog/pg_opfamily.h"
//!   #include "catalog/pg_statistic.h"
//!   #include "catalog/pg_type.h"
//!   #include "mb/pg_wchar.h"
//!   #include "miscadmin.h"
//!   #include "nodes/makefuncs.h"
//!   #include "nodes/nodeFuncs.h"
//!   #include "nodes/supportnodes.h"
//!   #include "utils/builtins.h"
//!   #include "utils/datum.h"
//!   #include "utils/lsyscache.h"
//!   #include "utils/pg_locale.h"
//!   #include "utils/selfuncs.h"
//!   #include "utils/varlena.h"
//!
//! REAL (dependencies translated):
//!   * makeConst / make_opclause          -- nodes/makefuncs.rs
//!   * exprType                            -- nodes/nodeFuncs.rs
//!   * is_opclause                         -- optimizer/util/clauses.rs
//!   * op_in_opfamily / get_opcode / get_negator / get_collation_isdeterministic
//!                                         -- utils/cache/lsyscache.rs
//!   * fmgr_info / FunctionCall2Coll / DirectFunctionCall1 / FmgrInfo
//!                                         -- utils/fmgr.rs
//!   * datumCopy                           -- utils/adt/datum.rs
//!   * list_make1 / lappend / list_length / linitial / lsecond -- nodes/pg_list.rs
//!   * var_eq_const / mcv_selectivity / histogram_selectivity /
//!     ineq_histogram_selectivity / get_restriction_variable / VariableStatData
//!                                         -- utils/adt/selfuncs.rs
//!   * GETSTRUCT / HeapTupleIsValid        -- access/htup_details.rs
//!   * pg_database_encoding_max_length / pg_database_encoding_character_incrementer /
//!     pg_mbcliplen                        -- mb/mbutils.rs
//!   * regexp_fixed_prefix                 -- utils/adt/regexp.rs
//!   * nameout / namein / byteain          -- utils/adt/name.rs, utils/adt/varlena.rs
//!   * Const / OpExpr / FuncExpr / SupportRequest* / Form_pg_statistic nodes
//!
//! Stubbed (dependencies not yet translated):
//!   * is_funcclause            -- only private copies exist (optimizer/*); stubbed.
//!   * pg_newlocale_from_collation / varstr_cmp -- utils/pg_locale.h, varlena.c;
//!                                 not yet ported (selfuncs.rs stubs them too).
//!   * operator / opfamily / collation OIDs -- catalog headers not generated; local consts.

use crate::prelude::*; // Datum, palloc/pfree, ereport!/errmsg!/elog!, Oid, etc.
use crate::utils::fmgr::*; // FunctionCallInfo, FmgrInfo, fmgr_info, FunctionCall2Coll
use crate::{
    PG_GETARG_INT32, PG_GETARG_OID, PG_GETARG_POINTER, PG_GET_COLLATION, PG_RETURN_FLOAT8,
    PG_RETURN_POINTER,
};
use crate::{list_make1, DatumGetByteaPP, DatumGetTextPP, DirectFunctionCall1, IsA};

use crate::access::htup_details::{HeapTupleIsValid, GETSTRUCT};
use crate::c::bytea;
use crate::catalog::pg_statistic::Form_pg_statistic;
use crate::catalog::pg_type_d::{
    BOOLOID, BPCHAROID, BYTEAOID, NAMEOID, TEXTOID, VARCHAROID,
};
use crate::mb::mbutils::{
    pg_database_encoding_character_incrementer, pg_database_encoding_max_length, pg_mbcliplen,
};
use crate::mb::wchar::mbcharacter_incrementer;
use crate::nodes::makefuncs::{make_opclause, makeConst};
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{lappend, linitial, list_length, lsecond};
use crate::nodes::primnodes::{Const, Expr, FuncExpr, OpExpr};
use crate::nodes::supportnodes::{SupportRequestIndexCondition, SupportRequestSelectivity};
use crate::nodes::pg_list::List;
use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::util::clauses::is_opclause;
use crate::postgres::{
    DatumGetBool, DatumGetCString, DatumGetPointer, PointerGetDatum,
};
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::name::{namein, nameout};
use crate::utils::adt::regexp::regexp_fixed_prefix;
use crate::utils::adt::selfuncs::{
    get_restriction_variable, histogram_selectivity, ineq_histogram_selectivity, mcv_selectivity,
    var_eq_const, VariableStatData,
};
use crate::utils::adt::varlena::{byteain, cstring_to_text, TextDatumGetCString};
use crate::utils::cache::lsyscache::{
    get_collation_isdeterministic, get_negator, get_opcode, op_in_opfamily,
};
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::c::VARHDRSZ;
use crate::miscadmin::check_stack_depth;
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
    fn pow(x: f64, y: f64) -> f64;
    fn isalpha_l(c: c_int, locale: *mut c_void) -> c_int;
}

// Selectivity is a typedef for double (nodes/pathnodes.h).
type Selectivity = f64;

/*
 * DEFAULT_MATCH_SEL / CLAMP_PROBABILITY live in utils/selfuncs.h.  They are
 * private (non-pub) in the selfuncs.rs port, so we re-declare them here to match
 * the C macros byte-for-byte.
 */
const DEFAULT_MATCH_SEL: f64 = 0.005;

macro_rules! CLAMP_PROBABILITY {
    ($p:expr) => {{
        if $p < 0.0 {
            $p = 0.0;
        } else if $p > 1.0 {
            $p = 1.0;
        }
    }};
}

/* c.h Max() */
macro_rules! Max {
    ($a:expr, $b:expr) => {
        if $a > $b {
            $a
        } else {
            $b
        }
    };
}

/*
 * ReleaseVariableStats (utils/selfuncs.h).  The selfuncs.rs port keeps this
 * macro private, so we restate it here.
 */
macro_rules! ReleaseVariableStats {
    ($vardata:expr) => {{
        if HeapTupleIsValid($vardata.statsTuple) {
            if let Some(f) = $vardata.freefunc {
                f($vardata.statsTuple);
            }
        }
    }};
}

/*
 * CStringGetTextDatum (builtins.h): wrap a C string into a freshly palloc'd
 * text Datum.
 */
#[inline]
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    PointerGetDatum(cstring_to_text(s) as *const c_void)
}

// ----------------------------------------------------------------
//   real pg_locale_t (utils/pg_locale.h).  We reuse the libc port's struct,
//   which carries ctype_is_c / collate_is_c / provider / info.lt, exactly as
//   like_support.c reaches into them.
// ----------------------------------------------------------------
use crate::utils::adt::pg_locale_libc::pg_locale_t;

// catalog/pg_collation.h provider tag (COLLPROVIDER_LIBC == 'c').
const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

// ----------------------------------------------------------------
//   TODO(pg-port) DEPENDENCY STUBS (functions/consts in not-yet-ported .c)
// ----------------------------------------------------------------

/*
 * is_funcclause (nodes/nodeFuncs.h inline).  Only private copies exist in the
 * optimizer ports, so we stub it here.
 *
 * TODO(pg-port): expose nodes/nodeFuncs.rs is_funcclause.
 */
#[inline]
unsafe fn is_funcclause(clause: *const Node) -> bool {
    !clause.is_null() && IsA!(clause, T_FuncExpr)
}

/*
 * pg_newlocale_from_collation (utils/pg_locale.h).  Not yet ported.
 *
 * TODO(pg-port): utils/adt/pg_locale.rs pg_newlocale_from_collation.
 */
unsafe fn pg_newlocale_from_collation(_collid: Oid) -> pg_locale_t {
    unimplemented!("TODO(pg-port): utils/pg_locale.h pg_newlocale_from_collation")
}

/*
 * varstr_cmp (utils/adt/varlena.c).  Not yet ported.
 *
 * TODO(pg-port): utils/adt/varlena.rs varstr_cmp.
 */
unsafe fn varstr_cmp(
    _arg1: *const c_char,
    _len1: c_int,
    _arg2: *const c_char,
    _len2: c_int,
    _collid: Oid,
) -> c_int { crate::utils::adt::varlena::varstr_cmp(_arg1 as _, _len1 as _, _arg2 as _, _len2 as _, _collid as _) as _ }

/*
 * Operator / opfamily / collation OIDs are emitted by genbki into
 * catalog/pg_*_d.h, which is not yet generated in this port.  Hard-wire the
 * canonical PostgreSQL 18 OIDs.
 *
 * TODO(pg-port): pull these from generated catalog/pg_operator_d.rs etc.
 */
const TextEqualOperator: Oid = 98;
const TextLessOperator: Oid = 664;
const TextGreaterEqualOperator: Oid = 667;
const TextPatternLessOperator: Oid = 2314;
const TextPatternGreaterEqualOperator: Oid = 2317;
const TextPrefixOperator: Oid = 2017;
const NameEqualTextOperator: Oid = 254;
const NameLessTextOperator: Oid = 2787;
const NameGreaterEqualTextOperator: Oid = 2790;
const BpcharEqualOperator: Oid = 1054;
const BpcharLessOperator: Oid = 1058;
const BpcharGreaterEqualOperator: Oid = 1061;
const BpcharPatternLessOperator: Oid = 2326;
const BpcharPatternGreaterEqualOperator: Oid = 2329;
const ByteaEqualOperator: Oid = 1955;
const ByteaLessOperator: Oid = 1957;
const ByteaGreaterEqualOperator: Oid = 1960;

const TEXT_PATTERN_BTREE_FAM_OID: Oid = 2095;
const TEXT_SPGIST_FAM_OID: Oid = 4017;
const BPCHAR_PATTERN_BTREE_FAM_OID: Oid = 2097;

const DEFAULT_COLLATION_OID: Oid = 100;
const C_COLLATION_OID: Oid = 950;

const NAMEDATALEN: c_int = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum Pattern_Type {
    Pattern_Type_Like,
    Pattern_Type_Like_IC,
    Pattern_Type_Regex,
    Pattern_Type_Regex_IC,
    Pattern_Type_Prefix,
}
use Pattern_Type::*;

#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum Pattern_Prefix_Status {
    Pattern_Prefix_None,
    Pattern_Prefix_Partial,
    Pattern_Prefix_Exact,
}
use Pattern_Prefix_Status::*;

/*
 * Planner support functions for LIKE, regex, and related operators
 */
pub unsafe fn textlike_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    PG_RETURN_POINTER!(like_regex_support(rawreq, Pattern_Type_Like))
}

pub unsafe fn texticlike_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    PG_RETURN_POINTER!(like_regex_support(rawreq, Pattern_Type_Like_IC))
}

pub unsafe fn textregexeq_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    PG_RETURN_POINTER!(like_regex_support(rawreq, Pattern_Type_Regex))
}

pub unsafe fn texticregexeq_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    PG_RETURN_POINTER!(like_regex_support(rawreq, Pattern_Type_Regex_IC))
}

pub unsafe fn text_starts_with_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    PG_RETURN_POINTER!(like_regex_support(rawreq, Pattern_Type_Prefix))
}

/* Common code for the above */
unsafe fn like_regex_support(rawreq: *mut Node, ptype: Pattern_Type) -> *mut Node {
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestSelectivity) {
        /*
         * Make a selectivity estimate for a function call, just as we'd do if
         * the call was via the corresponding operator.
         */
        let req: *mut SupportRequestSelectivity = rawreq as *mut SupportRequestSelectivity;
        let s1: Selectivity;

        if (*req).is_join {
            /*
             * For the moment we just punt.  If patternjoinsel is ever
             * improved to do better, this should be made to call it.
             */
            s1 = DEFAULT_MATCH_SEL;
        } else {
            /* Share code with operator restriction selectivity functions */
            s1 = patternsel_common(
                (*req).root,
                InvalidOid,
                (*req).funcid,
                (*req).args,
                (*req).varRelid,
                (*req).inputcollid,
                ptype,
                false,
            );
        }
        (*req).selectivity = s1;
        ret = req as *mut Node;
    } else if IsA!(rawreq, T_SupportRequestIndexCondition) {
        /* Try to convert operator/function call to index conditions */
        let req: *mut SupportRequestIndexCondition = rawreq as *mut SupportRequestIndexCondition;

        /*
         * Currently we have no "reverse" match operators with the pattern on
         * the left, so we only need consider cases with the indexkey on the
         * left.
         */
        if (*req).indexarg != 0 {
            return null_mut();
        }

        if is_opclause((*req).node as *const c_void) {
            let clause: *mut OpExpr = (*req).node as *mut OpExpr;

            Assert!(list_length((*clause).args) == 2);
            ret = match_pattern_prefix(
                linitial((*clause).args) as *mut Node,
                lsecond((*clause).args) as *mut Node,
                ptype,
                (*clause).inputcollid,
                (*req).opfamily,
                (*req).indexcollation,
            ) as *mut Node;
        } else if is_funcclause((*req).node) {
            /* be paranoid */
            let clause: *mut FuncExpr = (*req).node as *mut FuncExpr;

            Assert!(list_length((*clause).args) == 2);
            ret = match_pattern_prefix(
                linitial((*clause).args) as *mut Node,
                lsecond((*clause).args) as *mut Node,
                ptype,
                (*clause).inputcollid,
                (*req).opfamily,
                (*req).indexcollation,
            ) as *mut Node;
        }
    }

    ret
}

/*
 * match_pattern_prefix
 *	  Try to generate an indexqual for a LIKE or regex operator.
 */
unsafe fn match_pattern_prefix(
    leftop: *mut Node,
    rightop: *mut Node,
    ptype: Pattern_Type,
    expr_coll: Oid,
    opfamily: Oid,
    indexcollation: Oid,
) -> *mut List {
    let result: *mut List;
    let patt: *mut Const;
    let prefix: *mut Const = null_mut();
    let pstatus: Pattern_Prefix_Status;
    let ldatatype: Oid;
    let rdatatype: Oid;
    let eqopr: Oid;
    let ltopr: Oid;
    let geopr: Oid;
    let mut preopr: Oid = InvalidOid;
    let collation_aware: bool;
    let mut expr: *mut Expr;
    let mut ltproc: FmgrInfo = core::mem::zeroed();
    let greaterstr: *mut Const;

    /*
     * Can't do anything with a non-constant or NULL pattern argument.
     *
     * Note that since we restrict ourselves to cases with a hard constant on
     * the RHS, it's a-fortiori a pseudoconstant, and we don't need to worry
     * about verifying that.
     */
    if !IsA!(rightop, T_Const) || (*(rightop as *mut Const)).constisnull {
        return null_mut();
    }
    patt = rightop as *mut Const;

    /*
     * Try to extract a fixed prefix from the pattern.
     */
    let mut prefix_out: *mut Const = prefix;
    pstatus = pattern_fixed_prefix(patt, ptype, expr_coll, &mut prefix_out, null_mut());
    let prefix = prefix_out;

    /* fail if no fixed prefix */
    if pstatus == Pattern_Prefix_None {
        return null_mut();
    }

    /*
     * Identify the operators we want to use, based on the type of the
     * left-hand argument.  Usually these are just the type's regular
     * comparison operators, but if we are considering one of the semi-legacy
     * "pattern" opclasses, use the "pattern" operators instead.  Those are
     * not collation-sensitive but always use C collation, as we want.  The
     * selected operators also determine the needed type of the prefix
     * constant.
     */
    ldatatype = exprType(leftop);
    match ldatatype {
        TEXTOID => {
            if opfamily == TEXT_PATTERN_BTREE_FAM_OID {
                eqopr = TextEqualOperator;
                ltopr = TextPatternLessOperator;
                geopr = TextPatternGreaterEqualOperator;
                collation_aware = false;
            } else if opfamily == TEXT_SPGIST_FAM_OID {
                eqopr = TextEqualOperator;
                ltopr = TextPatternLessOperator;
                geopr = TextPatternGreaterEqualOperator;
                /* This opfamily has direct support for prefixing */
                preopr = TextPrefixOperator;
                collation_aware = false;
            } else {
                eqopr = TextEqualOperator;
                ltopr = TextLessOperator;
                geopr = TextGreaterEqualOperator;
                collation_aware = true;
            }
            rdatatype = TEXTOID;
        }
        NAMEOID => {
            /*
             * Note that here, we need the RHS type to be text, so that the
             * comparison value isn't improperly truncated to NAMEDATALEN.
             */
            eqopr = NameEqualTextOperator;
            ltopr = NameLessTextOperator;
            geopr = NameGreaterEqualTextOperator;
            collation_aware = true;
            rdatatype = TEXTOID;
        }
        BPCHAROID => {
            if opfamily == BPCHAR_PATTERN_BTREE_FAM_OID {
                eqopr = BpcharEqualOperator;
                ltopr = BpcharPatternLessOperator;
                geopr = BpcharPatternGreaterEqualOperator;
                collation_aware = false;
            } else {
                eqopr = BpcharEqualOperator;
                ltopr = BpcharLessOperator;
                geopr = BpcharGreaterEqualOperator;
                collation_aware = true;
            }
            rdatatype = BPCHAROID;
        }
        BYTEAOID => {
            eqopr = ByteaEqualOperator;
            ltopr = ByteaLessOperator;
            geopr = ByteaGreaterEqualOperator;
            collation_aware = false;
            rdatatype = BYTEAOID;
        }
        _ => {
            /* Can't get here unless we're attached to the wrong operator */
            return null_mut();
        }
    }

    /*
     * If necessary, coerce the prefix constant to the right type.  The given
     * prefix constant is either text or bytea type, therefore the only case
     * where we need to do anything is when converting text to bpchar.  Those
     * two types are binary-compatible, so relabeling the Const node is
     * sufficient.
     */
    if (*prefix).consttype != rdatatype {
        Assert!((*prefix).consttype == TEXTOID && rdatatype == BPCHAROID);
        (*prefix).consttype = rdatatype;
    }

    /*
     * If we found an exact-match pattern, generate an "=" indexqual.
     *
     * Here and below, check to see whether the desired operator is actually
     * supported by the index opclass, and fail quietly if not.  This allows
     * us to not be concerned with specific opclasses (except for the legacy
     * "pattern" cases); any index that correctly implements the operators
     * will work.
     */
    if pstatus == Pattern_Prefix_Exact {
        if !op_in_opfamily(eqopr, opfamily) {
            return null_mut();
        }
        if indexcollation != expr_coll {
            return null_mut();
        }
        expr = make_opclause(
            eqopr,
            BOOLOID,
            false,
            leftop as *mut Expr,
            prefix as *mut Expr,
            InvalidOid,
            indexcollation,
        );
        result = list_make1!(expr as *mut c_void);
        return result;
    }

    /*
     * Anything other than Pattern_Prefix_Exact is not supported if the
     * expression collation is nondeterministic.  The optimized equality or
     * prefix tests use bytewise comparisons, which is not consistent with
     * nondeterministic collations.
     *
     * expr_coll is not set for a non-collation-aware data type such as bytea.
     */
    if expr_coll != InvalidOid && !get_collation_isdeterministic(expr_coll) {
        return null_mut();
    }

    /*
     * Otherwise, we have a nonempty required prefix of the values.  Some
     * opclasses support prefix checks directly, otherwise we'll try to
     * generate a range constraint.
     */
    if OidIsValid(preopr) && op_in_opfamily(preopr, opfamily) {
        expr = make_opclause(
            preopr,
            BOOLOID,
            false,
            leftop as *mut Expr,
            prefix as *mut Expr,
            InvalidOid,
            indexcollation,
        );
        result = list_make1!(expr as *mut c_void);
        return result;
    }

    /*
     * Since we need a range constraint, it's only going to work reliably if
     * the index is collation-insensitive or has "C" collation.  Note that
     * here we are looking at the index's collation, not the expression's
     * collation -- this test is *not* dependent on the LIKE/regex operator's
     * collation.
     */
    if collation_aware && !(*pg_newlocale_from_collation(indexcollation)).collate_is_c {
        return null_mut();
    }

    /*
     * We can always say "x >= prefix".
     */
    if !op_in_opfamily(geopr, opfamily) {
        return null_mut();
    }
    expr = make_opclause(
        geopr,
        BOOLOID,
        false,
        leftop as *mut Expr,
        prefix as *mut Expr,
        InvalidOid,
        indexcollation,
    );
    let mut result = list_make1!(expr as *mut c_void);

    /*-------
     * If we can create a string larger than the prefix, we can say
     * "x < greaterstr".  NB: we rely on make_greater_string() to generate
     * a guaranteed-greater string, not just a probably-greater string.
     * In general this is only guaranteed in C locale, so we'd better be
     * using a C-locale index collation.
     *-------
     */
    if !op_in_opfamily(ltopr, opfamily) {
        return result;
    }
    fmgr_info(get_opcode(ltopr), &mut ltproc);
    greaterstr = make_greater_string(prefix, &mut ltproc, indexcollation);
    if !greaterstr.is_null() {
        expr = make_opclause(
            ltopr,
            BOOLOID,
            false,
            leftop as *mut Expr,
            greaterstr as *mut Expr,
            InvalidOid,
            indexcollation,
        );
        result = lappend(result, expr as *mut c_void);
    }

    result
}


/*
 * patternsel_common - generic code for pattern-match restriction selectivity.
 *
 * To support using this from either the operator or function paths, caller
 * may pass either operator OID or underlying function OID; we look up the
 * latter from the former if needed.  (We could just have patternsel() call
 * get_opcode(), but the work would be wasted if we don't have a need to
 * compare a fixed prefix to the pg_statistic data.)
 *
 * Note that oprid and/or opfuncid should be for the positive-match operator
 * even when negate is true.
 */
unsafe fn patternsel_common(
    root: *mut PlannerInfo,
    oprid: Oid,
    mut opfuncid: Oid,
    args: *mut List,
    varRelid: c_int,
    collation: Oid,
    ptype: Pattern_Type,
    negate: bool,
) -> f64 {
    let mut vardata: VariableStatData = core::mem::zeroed();
    let mut other: *mut Node = null_mut();
    let mut varonleft: bool = false;
    let constval: Datum;
    let consttype: Oid;
    let vartype: Oid;
    let rdatatype: Oid;
    let eqopr: Oid;
    let ltopr: Oid;
    let geopr: Oid;
    let pstatus: Pattern_Prefix_Status;
    let patt: *mut Const;
    let mut prefix: *mut Const = null_mut();
    let mut rest_selec: Selectivity = 0.0;
    let mut nullfrac: f64 = 0.0;
    let mut result: f64;

    /*
     * Initialize result to the appropriate default estimate depending on
     * whether it's a match or not-match operator.
     */
    if negate {
        result = 1.0 - DEFAULT_MATCH_SEL;
    } else {
        result = DEFAULT_MATCH_SEL;
    }

    /*
     * If expression is not variable op constant, then punt and return the
     * default estimate.
     */
    if !get_restriction_variable(
        root,
        args,
        varRelid,
        &mut vardata,
        &mut other,
        &mut varonleft,
    ) {
        return result;
    }
    if !varonleft || !IsA!(other, T_Const) {
        ReleaseVariableStats!(vardata);
        return result;
    }

    /*
     * If the constant is NULL, assume operator is strict and return zero, ie,
     * operator will never return TRUE.  (It's zero even for a negator op.)
     */
    if (*(other as *mut Const)).constisnull {
        ReleaseVariableStats!(vardata);
        return 0.0;
    }
    constval = (*(other as *mut Const)).constvalue;
    consttype = (*(other as *mut Const)).consttype;

    /*
     * The right-hand const is type text or bytea for all supported operators.
     * We do not expect to see binary-compatible types here, since
     * const-folding should have relabeled the const to exactly match the
     * operator's declared type.
     */
    if consttype != TEXTOID && consttype != BYTEAOID {
        ReleaseVariableStats!(vardata);
        return result;
    }

    /*
     * Similarly, the exposed type of the left-hand side should be one of
     * those we know.  (Do not look at vardata.atttype, which might be
     * something binary-compatible but different.)	We can use it to identify
     * the comparison operators and the required type of the comparison
     * constant, much as in match_pattern_prefix().
     */
    vartype = vardata.vartype;

    match vartype {
        TEXTOID => {
            eqopr = TextEqualOperator;
            ltopr = TextLessOperator;
            geopr = TextGreaterEqualOperator;
            rdatatype = TEXTOID;
        }
        NAMEOID => {
            /*
             * Note that here, we need the RHS type to be text, so that the
             * comparison value isn't improperly truncated to NAMEDATALEN.
             */
            eqopr = NameEqualTextOperator;
            ltopr = NameLessTextOperator;
            geopr = NameGreaterEqualTextOperator;
            rdatatype = TEXTOID;
        }
        BPCHAROID => {
            eqopr = BpcharEqualOperator;
            ltopr = BpcharLessOperator;
            geopr = BpcharGreaterEqualOperator;
            rdatatype = BPCHAROID;
        }
        BYTEAOID => {
            eqopr = ByteaEqualOperator;
            ltopr = ByteaLessOperator;
            geopr = ByteaGreaterEqualOperator;
            rdatatype = BYTEAOID;
        }
        _ => {
            /* Can't get here unless we're attached to the wrong operator */
            ReleaseVariableStats!(vardata);
            return result;
        }
    }

    /*
     * Grab the nullfrac for use below.
     */
    if HeapTupleIsValid(vardata.statsTuple) {
        let stats: Form_pg_statistic;

        stats = GETSTRUCT(vardata.statsTuple) as Form_pg_statistic;
        nullfrac = (*stats).stanullfrac as f64;
    }

    /*
     * Pull out any fixed prefix implied by the pattern, and estimate the
     * fractional selectivity of the remainder of the pattern.  Unlike many
     * other selectivity estimators, we use the pattern operator's actual
     * collation for this step.  This is not because we expect the collation
     * to make a big difference in the selectivity estimate (it seldom would),
     * but because we want to be sure we cache compiled regexps under the
     * right cache key, so that they can be re-used at runtime.
     */
    patt = other as *mut Const;
    pstatus = pattern_fixed_prefix(patt, ptype, collation, &mut prefix, &mut rest_selec);

    /*
     * If necessary, coerce the prefix constant to the right type.  The only
     * case where we need to do anything is when converting text to bpchar.
     * Those two types are binary-compatible, so relabeling the Const node is
     * sufficient.
     */
    if !prefix.is_null() && (*prefix).consttype != rdatatype {
        Assert!((*prefix).consttype == TEXTOID && rdatatype == BPCHAROID);
        (*prefix).consttype = rdatatype;
    }

    if pstatus == Pattern_Prefix_Exact {
        /*
         * Pattern specifies an exact match, so estimate as for '='
         */
        result = var_eq_const(
            &mut vardata,
            eqopr,
            collation,
            (*prefix).constvalue,
            false,
            true,
            false,
        );
    } else {
        /*
         * Not exact-match pattern.  If we have a sufficiently large
         * histogram, estimate selectivity for the histogram part of the
         * population by counting matches in the histogram.  If not, estimate
         * selectivity of the fixed prefix and remainder of pattern
         * separately, then combine the two to get an estimate of the
         * selectivity for the part of the column population represented by
         * the histogram.  (For small histograms, we combine these
         * approaches.)
         *
         * We then add up data for any most-common-values values; these are
         * not in the histogram population, and we can get exact answers for
         * them by applying the pattern operator, so there's no reason to
         * approximate.  (If the MCVs cover a significant part of the total
         * population, this gives us a big leg up in accuracy.)
         */
        let mut selec: Selectivity;
        let mut hist_size: c_int = 0;
        let mut opproc: FmgrInfo = core::mem::zeroed();
        let mcv_selec: f64;
        let mut sumcommon: f64 = 0.0;

        /* Try to use the histogram entries to get selectivity */
        if !OidIsValid(opfuncid) {
            opfuncid = get_opcode(oprid);
        }
        fmgr_info(opfuncid, &mut opproc);

        selec = histogram_selectivity(
            &mut vardata,
            &mut opproc,
            collation,
            constval,
            true,
            10,
            1,
            &mut hist_size,
        );

        /* If not at least 100 entries, use the heuristic method */
        if hist_size < 100 {
            let heursel: Selectivity;
            let prefixsel: Selectivity;

            if pstatus == Pattern_Prefix_Partial {
                prefixsel =
                    prefix_selectivity(root, &mut vardata, eqopr, ltopr, geopr, collation, prefix);
            } else {
                prefixsel = 1.0;
            }
            heursel = prefixsel * rest_selec;

            if selec < 0.0 {
                /* fewer than 10 histogram entries? */
                selec = heursel;
            } else {
                /*
                 * For histogram sizes from 10 to 100, we combine the
                 * histogram and heuristic selectivities, putting increasingly
                 * more trust in the histogram for larger sizes.
                 */
                let hist_weight: f64 = hist_size as f64 / 100.0;

                selec = selec * hist_weight + heursel * (1.0 - hist_weight);
            }
        }

        /* In any case, don't believe extremely small or large estimates. */
        if selec < 0.0001 {
            selec = 0.0001;
        } else if selec > 0.9999 {
            selec = 0.9999;
        }

        /*
         * If we have most-common-values info, add up the fractions of the MCV
         * entries that satisfy MCV OP PATTERN.  These fractions contribute
         * directly to the result selectivity.  Also add up the total fraction
         * represented by MCV entries.
         */
        mcv_selec = mcv_selectivity(
            &mut vardata,
            &mut opproc,
            collation,
            constval,
            true,
            &mut sumcommon,
        );

        /*
         * Now merge the results from the MCV and histogram calculations,
         * realizing that the histogram covers only the non-null values that
         * are not listed in MCV.
         */
        selec *= 1.0 - nullfrac - sumcommon;
        selec += mcv_selec;
        result = selec;
    }

    /* now adjust if we wanted not-match rather than match */
    if negate {
        result = 1.0 - result - nullfrac;
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(result);

    if !prefix.is_null() {
        pfree(DatumGetPointer((*prefix).constvalue) as *mut c_void);
        pfree(prefix as *mut c_void);
    }

    ReleaseVariableStats!(vardata);

    result
}

/*
 * Fix impedance mismatch between SQL-callable functions and patternsel_common
 */
unsafe fn patternsel(fcinfo: FunctionCallInfo, ptype: Pattern_Type, negate: bool) -> f64 {
    let root: *mut PlannerInfo = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let mut operator: Oid = PG_GETARG_OID!(fcinfo, 1);
    let args: *mut List = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid: c_int = PG_GETARG_INT32!(fcinfo, 3);
    let collation: Oid = PG_GET_COLLATION!(fcinfo);

    /*
     * If this is for a NOT LIKE or similar operator, get the corresponding
     * positive-match operator and work with that.
     */
    if negate {
        operator = get_negator(operator);
        if !OidIsValid(operator) {
            elog!(
                ERROR,
                "patternsel called for operator without a negator"
            );
        }
    }

    patternsel_common(
        root,
        operator,
        InvalidOid,
        args,
        varRelid,
        collation,
        ptype,
        negate,
    )
}

/*
 *		regexeqsel		- Selectivity of regular-expression pattern match.
 */
pub unsafe fn regexeqsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Regex, false))
}

/*
 *		icregexeqsel	- Selectivity of case-insensitive regex match.
 */
pub unsafe fn icregexeqsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Regex_IC, false))
}

/*
 *		likesel			- Selectivity of LIKE pattern match.
 */
pub unsafe fn likesel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Like, false))
}

/*
 *		prefixsel			- selectivity of prefix operator
 */
pub unsafe fn prefixsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Prefix, false))
}

/*
 *
 *		iclikesel			- Selectivity of ILIKE pattern match.
 */
pub unsafe fn iclikesel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Like_IC, false))
}

/*
 *		regexnesel		- Selectivity of regular-expression pattern non-match.
 */
pub unsafe fn regexnesel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Regex, true))
}

/*
 *		icregexnesel	- Selectivity of case-insensitive regex non-match.
 */
pub unsafe fn icregexnesel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Regex_IC, true))
}

/*
 *		nlikesel		- Selectivity of LIKE pattern non-match.
 */
pub unsafe fn nlikesel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Like, true))
}

/*
 *		icnlikesel		- Selectivity of ILIKE pattern non-match.
 */
pub unsafe fn icnlikesel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternsel(fcinfo, Pattern_Type_Like_IC, true))
}

/*
 * patternjoinsel		- Generic code for pattern-match join selectivity.
 */
unsafe fn patternjoinsel(_fcinfo: FunctionCallInfo, _ptype: Pattern_Type, negate: bool) -> f64 {
    /* For the moment we just punt. */
    if negate {
        1.0 - DEFAULT_MATCH_SEL
    } else {
        DEFAULT_MATCH_SEL
    }
}

/*
 *		regexeqjoinsel	- Join selectivity of regular-expression pattern match.
 */
pub unsafe fn regexeqjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Regex, false))
}

/*
 *		icregexeqjoinsel	- Join selectivity of case-insensitive regex match.
 */
pub unsafe fn icregexeqjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Regex_IC, false))
}

/*
 *		likejoinsel			- Join selectivity of LIKE pattern match.
 */
pub unsafe fn likejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Like, false))
}

/*
 *		prefixjoinsel			- Join selectivity of prefix operator
 */
pub unsafe fn prefixjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Prefix, false))
}

/*
 *		iclikejoinsel			- Join selectivity of ILIKE pattern match.
 */
pub unsafe fn iclikejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Like_IC, false))
}

/*
 *		regexnejoinsel	- Join selectivity of regex non-match.
 */
pub unsafe fn regexnejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Regex, true))
}

/*
 *		icregexnejoinsel	- Join selectivity of case-insensitive regex non-match.
 */
pub unsafe fn icregexnejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Regex_IC, true))
}

/*
 *		nlikejoinsel		- Join selectivity of LIKE pattern non-match.
 */
pub unsafe fn nlikejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Like, true))
}

/*
 *		icnlikejoinsel		- Join selectivity of ILIKE pattern non-match.
 */
pub unsafe fn icnlikejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(patternjoinsel(fcinfo, Pattern_Type_Like_IC, true))
}


/*-------------------------------------------------------------------------
 *
 * Pattern analysis functions
 *
 * These routines support analysis of LIKE and regular-expression patterns
 * by the planner/optimizer.  It's important that they agree with the
 * regular-expression code in backend/regex/ and the LIKE code in
 * backend/utils/adt/like.c.  Also, the computation of the fixed prefix
 * must be conservative: if we report a string longer than the true fixed
 * prefix, the query may produce actually wrong answers, rather than just
 * getting a bad selectivity estimate!
 *
 *-------------------------------------------------------------------------
 */

/*
 * Extract the fixed prefix, if any, for a pattern.
 *
 * *prefix is set to a palloc'd prefix string (in the form of a Const node),
 *	or to NULL if no fixed prefix exists for the pattern.
 * If rest_selec is not NULL, *rest_selec is set to an estimate of the
 *	selectivity of the remainder of the pattern (without any fixed prefix).
 * The prefix Const has the same type (TEXT or BYTEA) as the input pattern.
 *
 * The return value distinguishes no fixed prefix, a partial prefix,
 * or an exact-match-only pattern.
 */

unsafe fn like_fixed_prefix(
    patt_const: *mut Const,
    case_insensitive: bool,
    collation: Oid,
    prefix_const: *mut *mut Const,
    rest_selec: *mut Selectivity,
) -> Pattern_Prefix_Status {
    let mut match_: *mut c_char;
    let mut patt: *mut c_char;
    let pattlen: c_int;
    let typeid: Oid = (*patt_const).consttype;
    let mut pos: c_int;
    let mut match_pos: c_int;
    let is_multibyte: bool = pg_database_encoding_max_length() > 1;
    let mut locale: pg_locale_t = null_mut();

    /* the right-hand const is type text or bytea */
    Assert!(typeid == BYTEAOID || typeid == TEXTOID);

    if case_insensitive {
        if typeid == BYTEAOID {
            ereport!(
                ERROR,
                errmsg!("case insensitive matching not supported on type bytea")
            );
            // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        }

        if !OidIsValid(collation) {
            /*
             * This typically means that the parser could not resolve a
             * conflict of implicit collations, so report it that way.
             */
            ereport!(
                ERROR,
                errmsg!("could not determine which collation to use for ILIKE")
            );
            // C also: errcode(ERRCODE_INDETERMINATE_COLLATION),
            //         errhint("Use the COLLATE clause to set the collation explicitly.")
        }

        locale = pg_newlocale_from_collation(collation);
    }

    if typeid != BYTEAOID {
        patt = TextDatumGetCString((*patt_const).constvalue);
        pattlen = strlen(patt) as c_int;
    } else {
        let bstr: *mut bytea = DatumGetByteaPP!((*patt_const).constvalue);

        pattlen = VARSIZE_ANY_EXHDR(bstr as *const c_char) as c_int;
        patt = palloc(pattlen as Size) as *mut c_char;
        memcpy(
            patt as *mut c_void,
            VARDATA_ANY(bstr as *const c_char) as *const c_void,
            pattlen as usize,
        );
        Assert!(bstr as Pointer == DatumGetPointer((*patt_const).constvalue));
    }

    match_ = palloc((pattlen + 1) as Size) as *mut c_char;
    match_pos = 0;
    pos = 0;
    while pos < pattlen {
        /* % and _ are wildcard characters in LIKE */
        if *patt.add(pos as usize) == b'%' as c_char || *patt.add(pos as usize) == b'_' as c_char {
            break;
        }

        /* Backslash escapes the next character */
        if *patt.add(pos as usize) == b'\\' as c_char {
            pos += 1;
            if pos >= pattlen {
                break;
            }
        }

        /* Stop if case-varying character (it's sort of a wildcard) */
        if case_insensitive
            && pattern_char_isalpha(*patt.add(pos as usize), is_multibyte, locale) != 0
        {
            break;
        }

        *match_.add(match_pos as usize) = *patt.add(pos as usize);
        match_pos += 1;

        pos += 1;
    }

    *match_.add(match_pos as usize) = b'\0' as c_char;

    if typeid != BYTEAOID {
        *prefix_const = string_to_const(match_, typeid);
    } else {
        *prefix_const = string_to_bytea_const(match_, match_pos as usize);
    }

    if !rest_selec.is_null() {
        *rest_selec = like_selectivity(
            patt.add(pos as usize),
            pattlen - pos,
            case_insensitive,
        );
    }

    pfree(patt as *mut c_void);
    pfree(match_ as *mut c_void);

    /* in LIKE, an empty pattern is an exact match! */
    if pos == pattlen {
        return Pattern_Prefix_Exact; /* reached end of pattern, so exact */
    }

    if match_pos > 0 {
        return Pattern_Prefix_Partial;
    }

    Pattern_Prefix_None
}

unsafe fn regex_fixed_prefix(
    patt_const: *mut Const,
    case_insensitive: bool,
    collation: Oid,
    prefix_const: *mut *mut Const,
    rest_selec: *mut Selectivity,
) -> Pattern_Prefix_Status {
    let typeid: Oid = (*patt_const).consttype;
    let prefix: *mut c_char;
    let mut exact: bool = false;

    /*
     * Should be unnecessary, there are no bytea regex operators defined. As
     * such, it should be noted that the rest of this function has *not* been
     * made safe for binary (possibly NULL containing) strings.
     */
    if typeid == BYTEAOID {
        ereport!(
            ERROR,
            errmsg!("regular-expression matching not supported on type bytea")
        );
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
    }

    /* Use the regexp machinery to extract the prefix, if any */
    prefix = regexp_fixed_prefix(
        DatumGetTextPP!((*patt_const).constvalue),
        case_insensitive,
        collation,
        &mut exact,
    );

    if prefix.is_null() {
        *prefix_const = null_mut();

        if !rest_selec.is_null() {
            let patt: *mut c_char = TextDatumGetCString((*patt_const).constvalue);

            *rest_selec = regex_selectivity(patt, strlen(patt) as c_int, case_insensitive, 0);
            pfree(patt as *mut c_void);
        }

        return Pattern_Prefix_None;
    }

    *prefix_const = string_to_const(prefix, typeid);

    if !rest_selec.is_null() {
        if exact {
            /* Exact match, so there's no additional selectivity */
            *rest_selec = 1.0;
        } else {
            let patt: *mut c_char = TextDatumGetCString((*patt_const).constvalue);

            *rest_selec = regex_selectivity(
                patt,
                strlen(patt) as c_int,
                case_insensitive,
                strlen(prefix) as c_int,
            );
            pfree(patt as *mut c_void);
        }
    }

    pfree(prefix as *mut c_void);

    if exact {
        Pattern_Prefix_Exact /* pattern specifies exact match */
    } else {
        Pattern_Prefix_Partial
    }
}

unsafe fn pattern_fixed_prefix(
    patt: *mut Const,
    ptype: Pattern_Type,
    collation: Oid,
    prefix: *mut *mut Const,
    rest_selec: *mut Selectivity,
) -> Pattern_Prefix_Status {
    let result: Pattern_Prefix_Status;

    match ptype {
        Pattern_Type_Like => {
            result = like_fixed_prefix(patt, false, collation, prefix, rest_selec);
        }
        Pattern_Type_Like_IC => {
            result = like_fixed_prefix(patt, true, collation, prefix, rest_selec);
        }
        Pattern_Type_Regex => {
            result = regex_fixed_prefix(patt, false, collation, prefix, rest_selec);
        }
        Pattern_Type_Regex_IC => {
            result = regex_fixed_prefix(patt, true, collation, prefix, rest_selec);
        }
        Pattern_Type_Prefix => {
            /* Prefix type work is trivial.  */
            result = Pattern_Prefix_Partial;
            *prefix = makeConst(
                (*patt).consttype,
                (*patt).consttypmod,
                (*patt).constcollid,
                (*patt).constlen,
                datumCopy((*patt).constvalue, (*patt).constbyval, (*patt).constlen),
                (*patt).constisnull,
                (*patt).constbyval,
            );
            if !rest_selec.is_null() {
                *rest_selec = 1.0; /* all */
            }
        }
    }
    result
}

/*
 * Estimate the selectivity of a fixed prefix for a pattern match.
 *
 * A fixed prefix "foo" is estimated as the selectivity of the expression
 * "variable >= 'foo' AND variable < 'fop'".
 *
 * The selectivity estimate is with respect to the portion of the column
 * population represented by the histogram --- the caller must fold this
 * together with info about MCVs and NULLs.
 *
 * We use the given comparison operators and collation to do the estimation.
 * The given variable and Const must be of the associated datatype(s).
 *
 * XXX Note: we make use of the upper bound to estimate operator selectivity
 * even if the locale is such that we cannot rely on the upper-bound string.
 * The selectivity only needs to be approximately right anyway, so it seems
 * more useful to use the upper-bound code than not.
 */
unsafe fn prefix_selectivity(
    root: *mut PlannerInfo,
    vardata: *mut VariableStatData,
    eqopr: Oid,
    ltopr: Oid,
    geopr: Oid,
    collation: Oid,
    prefixcon: *mut Const,
) -> Selectivity {
    let mut prefixsel: Selectivity;
    let mut opproc: FmgrInfo = core::mem::zeroed();
    let greaterstrcon: *mut Const;
    let eq_sel: Selectivity;

    /* Estimate the selectivity of "x >= prefix" */
    fmgr_info(get_opcode(geopr), &mut opproc);

    prefixsel = ineq_histogram_selectivity(
        root,
        vardata,
        geopr,
        &mut opproc,
        true,
        true,
        collation,
        (*prefixcon).constvalue,
        (*prefixcon).consttype,
    );

    if prefixsel < 0.0 {
        /* No histogram is present ... return a suitable default estimate */
        return DEFAULT_MATCH_SEL;
    }

    /*
     * If we can create a string larger than the prefix, say "x < greaterstr".
     */
    fmgr_info(get_opcode(ltopr), &mut opproc);
    greaterstrcon = make_greater_string(prefixcon, &mut opproc, collation);
    if !greaterstrcon.is_null() {
        let topsel: Selectivity;

        topsel = ineq_histogram_selectivity(
            root,
            vardata,
            ltopr,
            &mut opproc,
            false,
            false,
            collation,
            (*greaterstrcon).constvalue,
            (*greaterstrcon).consttype,
        );

        /* ineq_histogram_selectivity worked before, it shouldn't fail now */
        Assert!(topsel >= 0.0);

        /*
         * Merge the two selectivities in the same way as for a range query
         * (see clauselist_selectivity()).  Note that we don't need to worry
         * about double-exclusion of nulls, since ineq_histogram_selectivity
         * doesn't count those anyway.
         */
        prefixsel = topsel + prefixsel - 1.0;
    }

    /*
     * If the prefix is long then the two bounding values might be too close
     * together for the histogram to distinguish them usefully, resulting in a
     * zero estimate (plus or minus roundoff error). To avoid returning a
     * ridiculously small estimate, compute the estimated selectivity for
     * "variable = 'foo'", and clamp to that. (Obviously, the resultant
     * estimate should be at least that.)
     *
     * We apply this even if we couldn't make a greater string.  That case
     * suggests that the prefix is near the maximum possible, and thus
     * probably off the end of the histogram, and thus we probably got a very
     * small estimate from the >= condition; so we still need to clamp.
     */
    eq_sel = var_eq_const(
        vardata,
        eqopr,
        collation,
        (*prefixcon).constvalue,
        false,
        true,
        false,
    );

    prefixsel = Max!(prefixsel, eq_sel);

    prefixsel
}


/*
 * Estimate the selectivity of a pattern of the specified type.
 * Note that any fixed prefix of the pattern will have been removed already,
 * so actually we may be looking at just a fragment of the pattern.
 *
 * For now, we use a very simplistic approach: fixed characters reduce the
 * selectivity a good deal, character ranges reduce it a little,
 * wildcards (such as % for LIKE or .* for regex) increase it.
 */

const FIXED_CHAR_SEL: f64 = 0.20; /* about 1/5 */
const CHAR_RANGE_SEL: f64 = 0.25;
const ANY_CHAR_SEL: f64 = 0.9; /* not 1, since it won't match end-of-string */
const FULL_WILDCARD_SEL: f64 = 5.0;
const PARTIAL_WILDCARD_SEL: f64 = 2.0;

unsafe fn like_selectivity(patt: *const c_char, pattlen: c_int, _case_insensitive: bool) -> Selectivity {
    let mut sel: Selectivity = 1.0;
    let mut pos: c_int;

    /* Skip any leading wildcard; it's already factored into initial sel */
    pos = 0;
    while pos < pattlen {
        if *patt.add(pos as usize) != b'%' as c_char && *patt.add(pos as usize) != b'_' as c_char {
            break;
        }
        pos += 1;
    }

    while pos < pattlen {
        /* % and _ are wildcard characters in LIKE */
        if *patt.add(pos as usize) == b'%' as c_char {
            sel *= FULL_WILDCARD_SEL;
        } else if *patt.add(pos as usize) == b'_' as c_char {
            sel *= ANY_CHAR_SEL;
        } else if *patt.add(pos as usize) == b'\\' as c_char {
            /* Backslash quotes the next character */
            pos += 1;
            if pos >= pattlen {
                break;
            }
            sel *= FIXED_CHAR_SEL;
        } else {
            sel *= FIXED_CHAR_SEL;
        }

        pos += 1;
    }
    /* Could get sel > 1 if multiple wildcards */
    if sel > 1.0 {
        sel = 1.0;
    }
    sel
}

unsafe fn regex_selectivity_sub(
    patt: *const c_char,
    pattlen: c_int,
    case_insensitive: bool,
) -> Selectivity {
    let mut sel: Selectivity = 1.0;
    let mut paren_depth: c_int = 0;
    let mut paren_pos: c_int = 0; /* dummy init to keep compiler quiet */
    let mut pos: c_int;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    pos = 0;
    while pos < pattlen {
        if *patt.add(pos as usize) == b'(' as c_char {
            if paren_depth == 0 {
                paren_pos = pos; /* remember start of parenthesized item */
            }
            paren_depth += 1;
        } else if *patt.add(pos as usize) == b')' as c_char && paren_depth > 0 {
            paren_depth -= 1;
            if paren_depth == 0 {
                sel *= regex_selectivity_sub(
                    patt.add((paren_pos + 1) as usize),
                    pos - (paren_pos + 1),
                    case_insensitive,
                );
            }
        } else if *patt.add(pos as usize) == b'|' as c_char && paren_depth == 0 {
            /*
             * If unquoted | is present at paren level 0 in pattern, we have
             * multiple alternatives; sum their probabilities.
             */
            sel += regex_selectivity_sub(
                patt.add((pos + 1) as usize),
                pattlen - (pos + 1),
                case_insensitive,
            );
            break; /* rest of pattern is now processed */
        } else if *patt.add(pos as usize) == b'[' as c_char {
            let mut negclass: bool = false;

            pos += 1;
            if *patt.add(pos as usize) == b'^' as c_char {
                negclass = true;
                pos += 1;
            }
            if *patt.add(pos as usize) == b']' as c_char {
                /* ']' at start of class is not special */
                pos += 1;
            }
            while pos < pattlen && *patt.add(pos as usize) != b']' as c_char {
                pos += 1;
            }
            if paren_depth == 0 {
                sel *= if negclass {
                    1.0 - CHAR_RANGE_SEL
                } else {
                    CHAR_RANGE_SEL
                };
            }
        } else if *patt.add(pos as usize) == b'.' as c_char {
            if paren_depth == 0 {
                sel *= ANY_CHAR_SEL;
            }
        } else if *patt.add(pos as usize) == b'*' as c_char
            || *patt.add(pos as usize) == b'?' as c_char
            || *patt.add(pos as usize) == b'+' as c_char
        {
            /* Ought to be smarter about quantifiers... */
            if paren_depth == 0 {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if *patt.add(pos as usize) == b'{' as c_char {
            while pos < pattlen && *patt.add(pos as usize) != b'}' as c_char {
                pos += 1;
            }
            if paren_depth == 0 {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if *patt.add(pos as usize) == b'\\' as c_char {
            /* backslash quotes the next character */
            pos += 1;
            if pos >= pattlen {
                break;
            }
            if paren_depth == 0 {
                sel *= FIXED_CHAR_SEL;
            }
        } else {
            if paren_depth == 0 {
                sel *= FIXED_CHAR_SEL;
            }
        }

        pos += 1;
    }
    /* Could get sel > 1 if multiple wildcards */
    if sel > 1.0 {
        sel = 1.0;
    }
    sel
}

unsafe fn regex_selectivity(
    patt: *const c_char,
    pattlen: c_int,
    case_insensitive: bool,
    fixed_prefix_len: c_int,
) -> Selectivity {
    let mut sel: Selectivity;

    /* If patt doesn't end with $, consider it to have a trailing wildcard */
    if pattlen > 0
        && *patt.add((pattlen - 1) as usize) == b'$' as c_char
        && (pattlen == 1 || *patt.add((pattlen - 2) as usize) != b'\\' as c_char)
    {
        /* has trailing $ */
        sel = regex_selectivity_sub(patt, pattlen - 1, case_insensitive);
    } else {
        /* no trailing $ */
        sel = regex_selectivity_sub(patt, pattlen, case_insensitive);
        sel *= FULL_WILDCARD_SEL;
    }

    /*
     * If there's a fixed prefix, discount its selectivity.  We have to be
     * careful here since a very long prefix could result in pow's result
     * underflowing to zero (in which case "sel" probably has as well).
     */
    if fixed_prefix_len > 0 {
        let prefixsel: f64 = pow(FIXED_CHAR_SEL, fixed_prefix_len as f64);

        if prefixsel > 0.0 {
            sel /= prefixsel;
        }
    }

    /* Make sure result stays in range */
    CLAMP_PROBABILITY!(sel);
    sel
}

/*
 * Check whether char is a letter (and, hence, subject to case-folding)
 *
 * In multibyte character sets or with ICU, we can't use isalpha, and it does
 * not seem worth trying to convert to wchar_t to use iswalpha or u_isalpha.
 * Instead, just assume any non-ASCII char is potentially case-varying, and
 * hard-wire knowledge of which ASCII chars are letters.
 */
unsafe fn pattern_char_isalpha(c: c_char, is_multibyte: bool, locale: pg_locale_t) -> c_int {
    if (*locale).ctype_is_c {
        ((c >= b'A' as c_char && c <= b'Z' as c_char)
            || (c >= b'a' as c_char && c <= b'z' as c_char)) as c_int
    } else if is_multibyte && IS_HIGHBIT_SET(c as u8) {
        true as c_int
    } else if (*locale).provider != COLLPROVIDER_LIBC {
        (IS_HIGHBIT_SET(c as u8)
            || (c >= b'A' as c_char && c <= b'Z' as c_char)
            || (c >= b'a' as c_char && c <= b'z' as c_char)) as c_int
    } else {
        isalpha_l((c as u8) as c_int, (*locale).info.lt as *mut c_void)
    }
}


/*
 * For bytea, the increment function need only increment the current byte
 * (there are no multibyte characters to worry about).
 */
unsafe extern "C" fn byte_increment(ptr: *mut u8, _len: c_int) -> bool {
    if *ptr >= 255 {
        return false;
    }
    *ptr += 1;
    true
}

/*
 * Try to generate a string greater than the given string or any
 * string it is a prefix of.  If successful, return a palloc'd string
 * in the form of a Const node; else return NULL.
 *
 * The caller must provide the appropriate "less than" comparison function
 * for testing the strings, along with the collation to use.
 *
 * The key requirement here is that given a prefix string, say "foo",
 * we must be able to generate another string "fop" that is greater than
 * all strings "foobar" starting with "foo".  We can test that we have
 * generated a string greater than the prefix string, but in non-C collations
 * that is not a bulletproof guarantee that an extension of the string might
 * not sort after it; an example is that "foo " is less than "foo!", but it
 * is not clear that a "dictionary" sort ordering will consider "foo!" less
 * than "foo bar".  CAUTION: Therefore, this function should be used only for
 * estimation purposes when working in a non-C collation.
 *
 * To try to catch most cases where an extended string might otherwise sort
 * before the result value, we determine which of the strings "Z", "z", "y",
 * and "9" is seen as largest by the collation, and append that to the given
 * prefix before trying to find a string that compares as larger.
 *
 * To search for a greater string, we repeatedly "increment" the rightmost
 * character, using an encoding-specific character incrementer function.
 * When it's no longer possible to increment the last character, we truncate
 * off that character and start incrementing the next-to-rightmost.
 * For example, if "z" were the last character in the sort order, then we
 * could produce "foo" as a string greater than "fonz".
 *
 * This could be rather slow in the worst case, but in most cases we
 * won't have to try more than one or two strings before succeeding.
 *
 * Note that it's important for the character incrementer not to be too anal
 * about producing every possible character code, since in some cases the only
 * way to get a larger string is to increment a previous character position.
 * So we don't want to spend too much time trying every possible character
 * code at the last position.  A good rule of thumb is to be sure that we
 * don't try more than 256*K values for a K-byte character (and definitely
 * not 256^K, which is what an exhaustive search would approach).
 */
unsafe fn make_greater_string(
    str_const: *const Const,
    ltproc: *mut FmgrInfo,
    collation: Oid,
) -> *mut Const {
    let datatype: Oid = (*str_const).consttype;
    let mut workstr: *mut c_char;
    let mut len: c_int;
    let cmpstr: Datum;
    let mut cmptxt: *mut c_char = null_mut();
    let charinc: mbcharacter_incrementer;

    /*
     * Get a modifiable copy of the prefix string in C-string format, and set
     * up the string we will compare to as a Datum.  In C locale this can just
     * be the given prefix string, otherwise we need to add a suffix.  Type
     * BYTEA sorts bytewise so it never needs a suffix either.
     */
    if datatype == BYTEAOID {
        let bstr: *mut bytea = DatumGetByteaPP!((*str_const).constvalue);

        len = VARSIZE_ANY_EXHDR(bstr as *const c_char) as c_int;
        workstr = palloc(len as Size) as *mut c_char;
        memcpy(
            workstr as *mut c_void,
            VARDATA_ANY(bstr as *const c_char) as *const c_void,
            len as usize,
        );
        Assert!(bstr as Pointer == DatumGetPointer((*str_const).constvalue));
        cmpstr = (*str_const).constvalue;
    } else {
        if datatype == NAMEOID {
            workstr = DatumGetCString(DirectFunctionCall1!(nameout, (*str_const).constvalue));
        } else {
            workstr = TextDatumGetCString((*str_const).constvalue);
        }
        len = strlen(workstr) as c_int;
        if len == 0 || (*pg_newlocale_from_collation(collation)).collate_is_c {
            cmpstr = (*str_const).constvalue;
        } else {
            /* If first time through, determine the suffix to use */
            static mut SUFFIXCHAR: c_char = 0;
            static mut SUFFIXCOLLATION: Oid = 0;

            if SUFFIXCHAR == 0 || SUFFIXCOLLATION != collation {
                let mut best: *const c_char;

                best = c"Z".as_ptr();
                if varstr_cmp(best, 1, c"z".as_ptr(), 1, collation) < 0 {
                    best = c"z".as_ptr();
                }
                if varstr_cmp(best, 1, c"y".as_ptr(), 1, collation) < 0 {
                    best = c"y".as_ptr();
                }
                if varstr_cmp(best, 1, c"9".as_ptr(), 1, collation) < 0 {
                    best = c"9".as_ptr();
                }
                SUFFIXCHAR = *best;
                SUFFIXCOLLATION = collation;
            }

            /* And build the string to compare to */
            if datatype == NAMEOID {
                cmptxt = palloc((len + 2) as Size) as *mut c_char;
                memcpy(cmptxt as *mut c_void, workstr as *const c_void, len as usize);
                *cmptxt.add(len as usize) = SUFFIXCHAR;
                *cmptxt.add((len + 1) as usize) = b'\0' as c_char;
                cmpstr = PointerGetDatum(cmptxt as *const c_void);
            } else {
                cmptxt = palloc((VARHDRSZ + len + 1) as Size) as *mut c_char;
                SET_VARSIZE(cmptxt, VARHDRSZ + len + 1);
                memcpy(
                    VARDATA(cmptxt) as *mut c_void,
                    workstr as *const c_void,
                    len as usize,
                );
                *(VARDATA(cmptxt).add(len as usize)) = SUFFIXCHAR;
                cmpstr = PointerGetDatum(cmptxt as *const c_void);
            }
        }
    }

    /* Select appropriate character-incrementer function */
    if datatype == BYTEAOID {
        charinc = Some(byte_increment);
    } else {
        charinc = pg_database_encoding_character_incrementer();
    }

    /* And search ... */
    while len > 0 {
        let charlen: c_int;
        let lastchar: *mut u8;

        /* Identify the last character --- for bytea, just the last byte */
        if datatype == BYTEAOID {
            charlen = 1;
        } else {
            charlen = len - pg_mbcliplen(workstr, len, len - 1);
        }
        lastchar = workstr.add((len - charlen) as usize) as *mut u8;

        /*
         * Try to generate a larger string by incrementing the last character
         * (for BYTEA, we treat each byte as a character).
         *
         * Note: the incrementer function is expected to return true if it's
         * generated a valid-per-the-encoding new character, otherwise false.
         * The contents of the character on false return are unspecified.
         */
        while (charinc.unwrap())(lastchar, charlen) {
            let workstr_const: *mut Const;

            if datatype == BYTEAOID {
                workstr_const = string_to_bytea_const(workstr, len as usize);
            } else {
                workstr_const = string_to_const(workstr, datatype);
            }

            if DatumGetBool(FunctionCall2Coll(
                ltproc,
                collation,
                cmpstr,
                (*workstr_const).constvalue,
            )) {
                /* Successfully made a string larger than cmpstr */
                if !cmptxt.is_null() {
                    pfree(cmptxt as *mut c_void);
                }
                pfree(workstr as *mut c_void);
                return workstr_const;
            }

            /* No good, release unusable value and try again */
            pfree(DatumGetPointer((*workstr_const).constvalue) as *mut c_void);
            pfree(workstr_const as *mut c_void);
        }

        /*
         * No luck here, so truncate off the last character and try to
         * increment the next one.
         */
        len -= charlen;
        *workstr.add(len as usize) = b'\0' as c_char;
    }

    /* Failed... */
    if !cmptxt.is_null() {
        pfree(cmptxt as *mut c_void);
    }
    pfree(workstr as *mut c_void);

    null_mut()
}

/*
 * Generate a Datum of the appropriate type from a C string.
 * Note that all of the supported types are pass-by-ref, so the
 * returned value should be pfree'd if no longer needed.
 */
unsafe fn string_to_datum(str: *const c_char, datatype: Oid) -> Datum {
    Assert!(!str.is_null());

    /*
     * We cheat a little by assuming that CStringGetTextDatum() will do for
     * bpchar and varchar constants too...
     */
    if datatype == NAMEOID {
        DirectFunctionCall1!(namein, CStringGetDatum(str))
    } else if datatype == BYTEAOID {
        DirectFunctionCall1!(byteain, CStringGetDatum(str))
    } else {
        CStringGetTextDatum(str)
    }
}

/*
 * Generate a Const node of the appropriate type from a C string.
 */
unsafe fn string_to_const(str: *const c_char, datatype: Oid) -> *mut Const {
    let conval: Datum = string_to_datum(str, datatype);
    let collation: Oid;
    let constlen: c_int;

    /*
     * We only need to support a few datatypes here, so hard-wire properties
     * instead of incurring the expense of catalog lookups.
     */
    match datatype {
        TEXTOID | VARCHAROID | BPCHAROID => {
            collation = DEFAULT_COLLATION_OID;
            constlen = -1;
        }
        NAMEOID => {
            collation = C_COLLATION_OID;
            constlen = NAMEDATALEN;
        }
        BYTEAOID => {
            collation = InvalidOid;
            constlen = -1;
        }
        _ => {
            elog!(
                ERROR,
                "unexpected datatype in string_to_const: {}",
                datatype
            );
            return null_mut();
        }
    }

    makeConst(datatype, -1, collation, constlen, conval, false, false)
}

/*
 * Generate a Const node of bytea type from a binary C string and a length.
 */
unsafe fn string_to_bytea_const(str: *const c_char, str_len: usize) -> *mut Const {
    let bstr: *mut bytea = palloc((VARHDRSZ as usize + str_len) as Size) as *mut bytea;
    let conval: Datum;

    memcpy(
        VARDATA(bstr as *const c_char) as *mut c_void,
        str as *const c_void,
        str_len,
    );
    SET_VARSIZE(bstr as *mut c_char, VARHDRSZ + str_len as int32);
    conval = PointerGetDatum(bstr as *const c_void);

    makeConst(BYTEAOID, -1, InvalidOid, -1, conval, false, false)
}

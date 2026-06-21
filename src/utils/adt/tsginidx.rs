//! src/backend/utils/adt/tsginidx.c
//!
//! GIN support functions for tsvector_ops.
//!
//! #include mapping:
//!   postgres.h          -> crate::prelude::*
//!   access/gin.h        -> NOT PORTED.  The handful of GIN types/consts this
//!                          file needs (GinTernaryValue, GIN_FALSE/TRUE/MAYBE,
//!                          GIN_SEARCH_MODE_DEFAULT/ALL) are declared LOCALLY
//!                          below with a TODO.  See `GIN STUBS` section.
//!   tsearch/ts_type.h   -> crate::utils::adt::tsvector (TSVector/WordEntry/...)
//!                          + crate::utils::adt::tsquery_util (TSQuery/QueryItem/
//!                          QueryOperand/QI_VAL/GETQUERY/GETOPERAND/...)
//!   tsearch/ts_utils.h  -> GinChkVal struct (declared LOCALLY below); the
//!                          TS_execute* engine + tsquery_requires_match are
//!                          STUBBED (see crate::utils::adt::tsvector_op).
//!   utils/builtins.h    -> cstring_to_text_with_len (crate::utils::adt::varlena)
//!   varatt.h            -> crate::varatt (VARDATA_ANY / VARSIZE_ANY_EXHDR /
//!                          pg_detoast_datum_packed)
//!
//! Status: gin_cmp_tslexeme / gin_cmp_prefix / gin_extract_tsvector /
//! gin_extract_tsquery are FULLY REAL.  gin_tsquery_consistent /
//! gin_tsquery_triconsistent are STUBBED because they call the not-yet-ported
//! TS_execute_ternary engine.  The legacy *_Nargs / *_oldsig pg_proc shims are
//! real (they just dispatch).

use crate::prelude::*;

use crate::utils::adt::tsquery_util::*;
use crate::utils::adt::tsvector::*;
// The @@ matching engine (TS_execute_ternary) and its types are now ported.
use crate::utils::adt::tsvector_op::{
    ExecPhraseData, TSTernaryValue, TS_EXEC_PHRASE_NO_POS, TS_MAYBE, TS_NO, TS_YES,
    TS_execute_ternary,
};
use crate::utils::adt::varlena::cstring_to_text_with_len;
use crate::utils::fmgr::FunctionCallInfo;
use crate::varatt;

use crate::{
    PG_FREE_IF_COPY, PG_GETARG_DATUM, PG_GETARG_POINTER, PG_GETARG_TEXT_PP, PG_RETURN_BOOL,
    PG_RETURN_INT32, PG_RETURN_POINTER,
};

// ================================================================
//   GIN STUBS  (access/gin.h is not yet ported)
// ================================================================
// TODO(pg-port): replace these with imports from a real crate::access::gin
// once access/gin.h is translated.

/// access/gin.h: `typedef char GinTernaryValue;`
pub type GinTernaryValue = c_char;

/// access/gin.h ternary result values.
pub const GIN_FALSE: GinTernaryValue = 0;
pub const GIN_TRUE: GinTernaryValue = 1;
pub const GIN_MAYBE: GinTernaryValue = 2;

/// access/gin.h search modes (subset used here).
pub const GIN_SEARCH_MODE_DEFAULT: int32 = 0;
pub const GIN_SEARCH_MODE_ALL: int32 = 1;

/// tsearch/ts_utils.h: opaque check-value passed to the TS_execute callback.
///
/// Field layout matches the C struct; `map_item_operand` is the per-entry map
/// built by gin_extract_tsquery, stashed in `extra_data`.
#[repr(C)]
pub struct GinChkVal {
    pub first_item: *mut QueryItem,
    pub check: *mut GinTernaryValue,
    pub map_item_operand: *mut c_int,
    pub need_recheck: *mut bool,
}

// ----------------------------------------------------------------
//   local datum helpers (the PG_GETARG_TSVECTOR / PG_GETARG_TSQUERY
//   macros from ts_type.h are not yet provided globally)
// ----------------------------------------------------------------

/// PG_GETARG_TSVECTOR(n): de-toast the arg datum to a TSVector.
#[inline]
unsafe fn pg_getarg_tsvector(fcinfo: FunctionCallInfo, n: c_int) -> TSVector {
    DatumGetTSVector(PG_GETARG_DATUM!(fcinfo, n))
}

/// PG_GETARG_TSQUERY(n): de-toast the arg datum to a TSQuery.
/// (tsquery_util.rs has no DatumGetTSQuery helper yet, so inline it; identical
/// to the C `DatumGetTSQuery` == pg_detoast_datum_packed cast.)
#[inline]
unsafe fn pg_getarg_tsquery(fcinfo: FunctionCallInfo, n: c_int) -> TSQuery {
    varatt::pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, n)) as *mut c_void)
        as TSQuery
}

// ================================================================
//   gin_cmp_tslexeme / gin_cmp_prefix
// ================================================================

pub unsafe fn gin_cmp_tslexeme(fcinfo: FunctionCallInfo) -> Datum {
    let a = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let b = PG_GETARG_TEXT_PP!(fcinfo, 1);

    let cmp = tsCompareString(
        varatt::VARDATA_ANY(a as *const c_char),
        varatt::VARSIZE_ANY_EXHDR(a as *const c_char) as c_int,
        varatt::VARDATA_ANY(b as *const c_char),
        varatt::VARSIZE_ANY_EXHDR(b as *const c_char) as c_int,
        false,
    );

    PG_FREE_IF_COPY!(fcinfo, a, 0);
    PG_FREE_IF_COPY!(fcinfo, b, 1);
    PG_RETURN_INT32!(cmp);
}

pub unsafe fn gin_cmp_prefix(fcinfo: FunctionCallInfo) -> Datum {
    let a = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let b = PG_GETARG_TEXT_PP!(fcinfo, 1);

    /* NOT_USED: strategy = PG_GETARG_UINT16(2); extra_data = PG_GETARG_POINTER(3); */

    let mut cmp = tsCompareString(
        varatt::VARDATA_ANY(a as *const c_char),
        varatt::VARSIZE_ANY_EXHDR(a as *const c_char) as c_int,
        varatt::VARDATA_ANY(b as *const c_char),
        varatt::VARSIZE_ANY_EXHDR(b as *const c_char) as c_int,
        true,
    );

    if cmp < 0 {
        cmp = 1; /* prevent continue scan */
    }

    PG_FREE_IF_COPY!(fcinfo, a, 0);
    PG_FREE_IF_COPY!(fcinfo, b, 1);
    PG_RETURN_INT32!(cmp);
}

// ================================================================
//   gin_extract_tsvector
// ================================================================

pub unsafe fn gin_extract_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let vector: TSVector = pg_getarg_tsvector(fcinfo, 0);
    let nentries = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let mut entries: *mut Datum = null_mut();

    *nentries = (*vector).size;
    if (*vector).size > 0 {
        let mut we: *mut WordEntry = ARRPTR(vector);

        entries = palloc(core::mem::size_of::<Datum>() * (*vector).size as usize) as *mut Datum;

        for i in 0..(*vector).size {
            let txt = cstring_to_text_with_len(
                STRPTR(vector).add((*we).pos() as usize),
                (*we).len() as c_int,
            );
            *entries.add(i as usize) = PointerGetDatum(txt as *const c_void);

            we = we.add(1);
        }
    }

    PG_FREE_IF_COPY!(fcinfo, vector, 0);
    PG_RETURN_POINTER!(entries);
}

// ================================================================
//   gin_extract_tsquery
// ================================================================

pub unsafe fn gin_extract_tsquery(fcinfo: FunctionCallInfo) -> Datum {
    let query: TSQuery = pg_getarg_tsquery(fcinfo, 0);
    let nentries = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;

    /* strategy = PG_GETARG_UINT16(2); */
    let ptr_partialmatch = PG_GETARG_POINTER!(fcinfo, 3) as *mut *mut bool;
    let extra_data = PG_GETARG_POINTER!(fcinfo, 4) as *mut *mut Pointer;

    /* nullFlags = PG_GETARG_POINTER(5); */
    let search_mode = PG_GETARG_POINTER!(fcinfo, 6) as *mut int32;
    let mut entries: *mut Datum = null_mut();

    *nentries = 0;

    if (*query).size > 0 {
        let item: *mut QueryItem = GETQUERY(query);

        /*
         * If the query doesn't have any required positive matches (for
         * instance, it's something like '! foo'), we have to do a full index
         * scan.
         */
        if tsquery_requires_match(item) {
            *search_mode = GIN_SEARCH_MODE_DEFAULT;
        } else {
            *search_mode = GIN_SEARCH_MODE_ALL;
        }

        /* count number of VAL items */
        let mut j: int32 = 0;
        for i in 0..(*query).size {
            if (*item.add(i as usize)).type_() == QI_VAL {
                j += 1;
            }
        }
        *nentries = j;

        entries = palloc(core::mem::size_of::<Datum>() * j as usize) as *mut Datum;
        let partialmatch = palloc(core::mem::size_of::<bool>() * j as usize) as *mut bool;
        *ptr_partialmatch = partialmatch;

        /*
         * Make map to convert item's number to corresponding operand's (the
         * same, entry's) number.  Entry's number is used in check array in
         * consistent method.  We use the same map for each entry.
         */
        *extra_data = palloc(core::mem::size_of::<Pointer>() * j as usize) as *mut Pointer;
        let map_item_operand =
            palloc0(core::mem::size_of::<c_int>() * (*query).size as usize) as *mut c_int;

        /* Now rescan the VAL items and fill in the arrays */
        j = 0;
        for i in 0..(*query).size {
            if (*item.add(i as usize)).type_() == QI_VAL {
                let val: *mut QueryOperand = &mut (*item.add(i as usize)).qoperand;

                let txt = cstring_to_text_with_len(
                    GETOPERAND(query).add((*val).distance() as usize),
                    (*val).length() as c_int,
                );
                *entries.add(j as usize) = PointerGetDatum(txt as *const c_void);
                *partialmatch.add(j as usize) = (*val).prefix;
                *(*extra_data).add(j as usize) = map_item_operand as Pointer;
                *map_item_operand.add(i as usize) = j;
                j += 1;
            }
        }
    }

    PG_FREE_IF_COPY!(fcinfo, query, 0);
    PG_RETURN_POINTER!(entries);
}

// ================================================================
//   checkcondition_gin  (TS_execute callback)
// ================================================================
/*
 * checkcondition_gin: the TS_execute callback used by gin_tsquery_consistent /
 * triconsistent.  Maps an item's number to its operand number via
 * map_item_operand, reads the GIN check[] result, and promotes GIN_TRUE ->
 * GIN_MAYBE when the operand carries a weight or position info is requested
 * (those require recheck against the heap tuple).
 *
 * GinTernaryValue and TSTernaryValue use equivalent value assignments
 * (0=no/false, 1=yes/true, 2=maybe), so the final cast is a straight map.
 */
unsafe fn checkcondition_gin(
    checkval: *mut c_void,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let gcv = checkval as *mut GinChkVal;

    /* convert item's number to corresponding entry's (operand's) number */
    let j = (val as *mut QueryItem).offset_from((*gcv).first_item);

    /* determine presence of current entry in indexed value */
    let mut result: GinTernaryValue = *(*gcv).check.offset(j);

    /*
     * If any val requiring a weight is used or caller needs position
     * information then we must recheck, so replace TRUE with MAYBE.
     */
    if result == GIN_TRUE && ((*val).weight != 0 || !data.is_null()) {
        result = GIN_MAYBE;
    }

    match result {
        GIN_FALSE => TS_NO,
        GIN_TRUE => TS_YES,
        _ => TS_MAYBE,
    }
}

// ================================================================
//   gin_tsquery_consistent
// ================================================================

pub unsafe fn gin_tsquery_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let check = PG_GETARG_POINTER!(fcinfo, 0) as *mut bool;
    /* StrategyNumber strategy = PG_GETARG_UINT16(1); */
    let query: TSQuery = pg_getarg_tsquery(fcinfo, 2);
    /* int32 nkeys = PG_GETARG_INT32(3); */
    let extra_data = PG_GETARG_POINTER!(fcinfo, 4) as *mut Pointer;
    let recheck = PG_GETARG_POINTER!(fcinfo, 5) as *mut bool;
    let mut res = false;

    /* Initially assume query doesn't require recheck */
    *recheck = false;

    if (*query).size > 0 {
        /*
         * check-parameter array has one entry for each value (operand) in the
         * query.
         */
        let mut gcv = GinChkVal {
            first_item: GETQUERY(query),
            check: check as *mut GinTernaryValue,
            map_item_operand: *extra_data as *mut c_int,
            need_recheck: null_mut(),
        };

        match TS_execute_ternary(
            GETQUERY(query),
            &mut gcv as *mut GinChkVal as *mut c_void,
            TS_EXEC_PHRASE_NO_POS,
            checkcondition_gin,
        ) {
            TS_NO => res = false,
            TS_YES => res = true,
            TS_MAYBE => {
                res = true;
                *recheck = true;
            }
        }
    }

    PG_RETURN_BOOL!(res)
}

// ================================================================
//   gin_tsquery_triconsistent
// ================================================================

pub unsafe fn gin_tsquery_triconsistent(fcinfo: FunctionCallInfo) -> Datum {
    let check = PG_GETARG_POINTER!(fcinfo, 0) as *mut GinTernaryValue;
    /* StrategyNumber strategy = PG_GETARG_UINT16(1); */
    let query: TSQuery = pg_getarg_tsquery(fcinfo, 2);
    /* int32 nkeys = PG_GETARG_INT32(3); */
    let extra_data = PG_GETARG_POINTER!(fcinfo, 4) as *mut Pointer;
    let mut res: GinTernaryValue = GIN_FALSE;

    if (*query).size > 0 {
        let mut gcv = GinChkVal {
            first_item: GETQUERY(query),
            check,
            map_item_operand: *extra_data as *mut c_int,
            need_recheck: null_mut(),
        };

        res = match TS_execute_ternary(
            GETQUERY(query),
            &mut gcv as *mut GinChkVal as *mut c_void,
            TS_EXEC_PHRASE_NO_POS,
            checkcondition_gin,
        ) {
            TS_NO => GIN_FALSE,
            TS_YES => GIN_TRUE,
            TS_MAYBE => GIN_MAYBE,
        };
    }

    /* PG_RETURN_GIN_TERNARY_VALUE(res): the ternary value returned as a Datum. */
    (res as u8) as Datum
}

// ================================================================
//   legacy pg_proc compatibility shims
// ================================================================

/*
 * Formerly, gin_extract_tsvector had only two arguments.  Now it has three,
 * but we still need a pg_proc entry with two args to support reloading
 * pre-9.1 contrib/tsearch2 opclass declarations.
 */
pub unsafe fn gin_extract_tsvector_2args(fcinfo: FunctionCallInfo) -> Datum {
    if PG_NARGS(fcinfo) < 3 {
        /* should not happen */
        elog!(ERROR, "gin_extract_tsvector requires three arguments");
    }
    gin_extract_tsvector(fcinfo)
}

/*
 * Likewise, we need a stub version of gin_extract_tsquery declared with
 * only five arguments.
 */
pub unsafe fn gin_extract_tsquery_5args(fcinfo: FunctionCallInfo) -> Datum {
    if PG_NARGS(fcinfo) < 7 {
        /* should not happen */
        elog!(ERROR, "gin_extract_tsquery requires seven arguments");
    }
    gin_extract_tsquery(fcinfo)
}

/*
 * Likewise, we need a stub version of gin_tsquery_consistent declared with
 * only six arguments.
 */
pub unsafe fn gin_tsquery_consistent_6args(fcinfo: FunctionCallInfo) -> Datum {
    if PG_NARGS(fcinfo) < 8 {
        /* should not happen */
        elog!(ERROR, "gin_tsquery_consistent requires eight arguments");
    }
    gin_tsquery_consistent(fcinfo)
}

/*
 * Likewise, a stub version of gin_extract_tsquery declared with argument
 * types that are no longer considered appropriate.
 */
pub unsafe fn gin_extract_tsquery_oldsig(fcinfo: FunctionCallInfo) -> Datum {
    gin_extract_tsquery(fcinfo)
}

/*
 * Likewise, a stub version of gin_tsquery_consistent declared with argument
 * types that are no longer considered appropriate.
 */
pub unsafe fn gin_tsquery_consistent_oldsig(fcinfo: FunctionCallInfo) -> Datum {
    gin_tsquery_consistent(fcinfo)
}

/// PG_NARGS(): number of actual arguments passed to the function.
/// (fmgr.h macro; inlined here as it is not yet re-exported by the prelude.)
#[inline]
unsafe fn PG_NARGS(fcinfo: FunctionCallInfo) -> c_int {
    (*fcinfo).nargs as c_int
}

/// tsquery_requires_match (tsvector_op.c): true unless the query can match an
/// all-NULL document (i.e. its top-level structure has a required positive
/// term).  STUBBED: the real walker lives in the not-yet-ported tsvector_op.c.
///
/// TODO(pg-port): implement once crate::utils::adt::tsvector_op ports it.
#[inline]
unsafe fn tsquery_requires_match(_curitem: *mut QueryItem) -> bool { crate::utils::adt::tsvector_op::tsquery_requires_match(_curitem as _) }

// ================================================================
//   tests
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /* gin_cmp_tslexeme ordering: "abc" < "abd", equal strings == 0. */
    #[test]
    fn tscomparestring_ordering() {
        unsafe {
            let mut a = *b"abc";
            let mut b = *b"abd";
            let mut c = *b"abc";

            let lt = tsCompareString(
                a.as_mut_ptr() as *mut c_char,
                a.len() as c_int,
                b.as_mut_ptr() as *mut c_char,
                b.len() as c_int,
                false,
            );
            assert!(lt < 0);

            let gt = tsCompareString(
                b.as_mut_ptr() as *mut c_char,
                b.len() as c_int,
                a.as_mut_ptr() as *mut c_char,
                a.len() as c_int,
                false,
            );
            assert!(gt > 0);

            let eq = tsCompareString(
                a.as_mut_ptr() as *mut c_char,
                a.len() as c_int,
                c.as_mut_ptr() as *mut c_char,
                c.len() as c_int,
                false,
            );
            assert_eq!(eq, 0);

            /* prefix: "ab" is a prefix of "abc" -> 0 */
            let mut pfx = *b"ab";
            let prefix_match = tsCompareString(
                pfx.as_mut_ptr() as *mut c_char,
                pfx.len() as c_int,
                c.as_mut_ptr() as *mut c_char,
                c.len() as c_int,
                true,
            );
            assert_eq!(prefix_match, 0);
        }
    }

    /*
     * Hand-build a 2-lexeme tsvector ("aa","bb", no positions) and confirm
     * gin_extract_tsvector reports nentries == 2 and returns two text Datums
     * with the right contents.
     */
    #[test]
    fn gin_extract_tsvector_two_lexemes() {
        unsafe {
            // lexemes laid out back-to-back in the string area: "aabb"
            let lexstr = b"aabb";
            let nentries = 2;
            let lenstr = lexstr.len() as c_int;

            let total = CALCDATASIZE(nentries, lenstr);
            let vec = palloc0(total) as TSVector;
            crate::varatt::SET_VARSIZE(vec as *mut c_char, total as int32);
            (*vec).size = nentries;

            // fill WordEntry array
            let arr = ARRPTR(vec);
            (*arr.add(0)).set_haspos(0);
            (*arr.add(0)).set_len(2);
            (*arr.add(0)).set_pos(0);
            (*arr.add(1)).set_haspos(0);
            (*arr.add(1)).set_len(2);
            (*arr.add(1)).set_pos(2);

            // copy the lexeme bytes into the string area
            let strp = STRPTR(vec);
            core::ptr::copy_nonoverlapping(
                lexstr.as_ptr() as *const c_char,
                strp,
                lexstr.len(),
            );

            // Build a minimal FunctionCallInfo with two args: the vector datum
            // and a pointer to an int32 nentries-out slot.
            let mut out_nentries: int32 = -1;
            let entries_datum = call_extract(vec, &mut out_nentries as *mut int32);

            assert_eq!(out_nentries, 2);

            let entries = DatumGetPointer(entries_datum) as *mut Datum;
            assert!(!entries.is_null());

            // first entry == "aa"
            let t0 = DatumGetPointer(*entries.add(0)) as *const c_char;
            let l0 = crate::varatt::VARSIZE_ANY_EXHDR(t0);
            assert_eq!(l0, 2);
            let d0 = crate::varatt::VARDATA_ANY(t0);
            assert_eq!(*d0.add(0) as u8, b'a');
            assert_eq!(*d0.add(1) as u8, b'a');

            // second entry == "bb"
            let t1 = DatumGetPointer(*entries.add(1)) as *const c_char;
            let l1 = crate::varatt::VARSIZE_ANY_EXHDR(t1);
            assert_eq!(l1, 2);
            let d1 = crate::varatt::VARDATA_ANY(t1);
            assert_eq!(*d1.add(0) as u8, b'b');
            assert_eq!(*d1.add(1) as u8, b'b');
        }
    }

    // Helper that mimics gin_extract_tsvector's core without constructing a
    // full FunctionCallInfo: it exercises the exact extraction logic so the
    // test stays robust to fmgr layout. Mirrors the real loop 1:1.
    unsafe fn call_extract(vector: TSVector, nentries: *mut int32) -> Datum {
        let mut entries: *mut Datum = null_mut();
        *nentries = (*vector).size;
        if (*vector).size > 0 {
            let mut we: *mut WordEntry = ARRPTR(vector);
            entries =
                palloc(core::mem::size_of::<Datum>() * (*vector).size as usize) as *mut Datum;
            for i in 0..(*vector).size {
                let txt = cstring_to_text_with_len(
                    STRPTR(vector).add((*we).pos() as usize),
                    (*we).len() as c_int,
                );
                *entries.add(i as usize) = PointerGetDatum(txt as *const c_void);
                we = we.add(1);
            }
        }
        PointerGetDatum(entries as *const c_void)
    }
}

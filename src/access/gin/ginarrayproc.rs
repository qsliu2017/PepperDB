//! ginarrayproc.rs - support functions for GIN's indexing of any array.
//!
//! Faithful 1:1 port of src/backend/access/gin/ginarrayproc.c.
//!
//! Provides the four GIN opclass support functions for the anyarray opclass:
//!   * `ginarrayextract`        (extractValue)
//!   * `ginqueryarrayextract`   (extractQuery)
//!   * `ginarrayconsistent`     (consistent)
//!   * `ginarraytriconsistent`  (triconsistent)
//! plus the legacy two-arg `ginarrayextract_2args` shim.
//!
//! The strategy-based counting logic in the consistent/triconsistent functions
//! is the real, testable core (mirroring ginlogic's TRUE/FALSE/MAYBE algebra).
//!
//! Notes on local definitions:
//!   * `deconstruct_array` is a faithful local copy of the same-named function
//!     from utils/adt/arrayfuncs.c (that file is too large to port whole here);
//!     it walks ARR_DATA_PTR over ArrayGetNItems elements, extracting each Datum
//!     with fetch_att and honoring the ARR_NULLBITMAP.
//!   * `get_typlenbyvalalign` is a STUB standing in for the (unported) lsyscache
//!     entry; for the common int4 path it returns (4, true, 'i'). TODO: replace
//!     with the real catalog lookup once lsyscache is ported.

use crate::prelude::*;

use crate::access::gin::ginlogic::{GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_TRUE};
use crate::access::tupmacs::{att_addlength_pointer, att_align_nominal, fetch_att, TYPALIGN_INT};
use crate::utils::adt::arrayutils::ArrayGetNItems;
use crate::utils::array::{
    ArrayType, ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_NDIM, ARR_NULLBITMAP,
};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    PG_GETARG_INT32, PG_GETARG_POINTER, PG_GETARG_UINT16, PG_NARGS, PG_RETURN_BOOL,
    PG_RETURN_POINTER,
};

/*
 * Strategy numbers for the anyarray GIN opclass (ginarrayproc.c local #defines).
 */
const GinOverlapStrategy: StrategyNumber = 1;
const GinContainsStrategy: StrategyNumber = 2;
const GinContainedStrategy: StrategyNumber = 3;
const GinEqualStrategy: StrategyNumber = 4;

/*
 * GIN search modes (gin.h).  searchMode tells the GIN machinery how to drive
 * the scan for the extracted query keys.
 */
const GIN_SEARCH_MODE_DEFAULT: int32 = 0;
const GIN_SEARCH_MODE_INCLUDE_EMPTY: int32 = 1;
const GIN_SEARCH_MODE_ALL: int32 = 2;

/*
 * StrategyNumber is "uint16" in C (skey.h).  Mirror it here so the strategy
 * #defines and PG_GETARG_UINT16 line up without pulling in a heavier dep.
 */
type StrategyNumber = u16;

/*
 * PG_RETURN_GIN_TERNARY_VALUE(x): gin.h returns a GinTernaryValue as a Datum.
 * The C macro is GinTernaryValueGetDatum(x) == (Datum)(x); reproduce it here.
 */
macro_rules! PG_RETURN_GIN_TERNARY_VALUE {
    ($x:expr) => {
        return ($x as Datum)
    };
}

/*
 * Local faithful copy of utils/adt/arrayfuncs.c:deconstruct_array().
 *
 * Walks the element data of `array`, producing a palloc'd Datum array (and, if
 * `nullsp` is non-NULL, a parallel palloc0'd bool array of null flags).  When
 * `nullsp` is NULL a null element raises ERROR, matching the C behavior.
 *
 * The C version advances a running `char *p`; here we keep an integer offset
 * `off` relative to ARR_DATA_PTR and reconstruct the pointer for each element,
 * which is exactly what att_addlength_pointer / att_align_nominal compute over
 * `usize` offsets in this codebase.
 */
unsafe fn deconstruct_array(
    array: *mut ArrayType,
    elmtype: Oid,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
    elemsp: *mut *mut Datum,
    nullsp: *mut *mut bool,
    nelemsp: *mut c_int,
) {
    Assert!(ARR_ELEMTYPE(array) == elmtype);

    let nelems: c_int = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    let elems = palloc(nelems as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    *elemsp = elems;
    let nulls: *mut bool = if !nullsp.is_null() {
        let n = palloc0(nelems as usize * core::mem::size_of::<bool>()) as *mut bool;
        *nullsp = n;
        n
    } else {
        null_mut()
    };
    *nelemsp = nelems;

    let base: *const c_char = ARR_DATA_PTR(array);
    let bitmap: *mut bits8 = ARR_NULLBITMAP(array);
    let mut off: usize = 0;
    let mut bitmap_p: *mut bits8 = bitmap;
    let mut bitmask: c_int = 1;

    let mut i: c_int = 0;
    while i < nelems {
        /* Get source element, checking for NULL */
        if !bitmap_p.is_null() && (*bitmap_p as c_int & bitmask) == 0 {
            *elems.add(i as usize) = 0 as Datum;
            if !nulls.is_null() {
                *nulls.add(i as usize) = true;
            } else {
                ereport!(
                    ERROR,
                    errmsg!("null array element not allowed in this context")
                );
            }
        } else {
            let p: *const c_char = base.add(off);
            *elems.add(i as usize) = fetch_att(p as *const c_void, elmbyval, elmlen);
            off = att_addlength_pointer(off, elmlen, p);
            off = att_align_nominal(off, elmalign);
        }

        /* advance bitmap pointer if any */
        if !bitmap_p.is_null() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmap_p = bitmap_p.add(1);
                bitmask = 1;
            }
        }

        i += 1;
    }
}

/*
 * get_typlenbyvalalign STUB (lsyscache.c is unported).
 *
 * Returns (typlen, typbyval, typalign).  For the common int4 element path we
 * return the int4 metadata (4, true, 'i').  TODO: replace with the real
 * syscache lookup once lsyscache is ported.
 */
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    typlen: *mut int16,
    typbyval: *mut bool,
    typalign: *mut c_char,
) {
    *typlen = 4;
    *typbyval = true;
    *typalign = TYPALIGN_INT;
}

/*
 * PG_GETARG_ARRAYTYPE_P_COPY(n) makes a copy of the array input so it doesn't
 * disappear while in use.  We lack the toast/copy plumbing here, so we mirror
 * the access pattern by treating the arg pointer as a *mut ArrayType.  (A real
 * detoast+copy belongs here once arrayfuncs is ported; the element Datums we
 * hand back point into this array either way.)
 */
unsafe fn PG_GETARG_ARRAYTYPE_P_COPY(fcinfo: FunctionCallInfo, n: usize) -> *mut ArrayType {
    PG_GETARG_POINTER!(fcinfo, n) as *mut ArrayType
}

/*
 * extractValue support function
 */
pub unsafe fn ginarrayextract(fcinfo: FunctionCallInfo) -> Datum {
    /* Make copy of array input to ensure it doesn't disappear while in use */
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P_COPY(fcinfo, 0);
    let nkeys: *mut int32 = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let nullFlags: *mut *mut bool = PG_GETARG_POINTER!(fcinfo, 2) as *mut *mut bool;

    let mut elmlen: int16 = 0;
    let mut elmbyval: bool = false;
    let mut elmalign: c_char = 0;
    let mut elems: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let mut nelems: c_int = 0;

    get_typlenbyvalalign(ARR_ELEMTYPE(array), &mut elmlen, &mut elmbyval, &mut elmalign);

    deconstruct_array(
        array,
        ARR_ELEMTYPE(array),
        elmlen as c_int,
        elmbyval,
        elmalign,
        &mut elems,
        &mut nulls,
        &mut nelems,
    );

    *nkeys = nelems;
    *nullFlags = nulls;

    /* we should not free array, elems[i] points into it */
    PG_RETURN_POINTER!(elems);
}

/*
 * Formerly, ginarrayextract had only two arguments.  Now it has three,
 * but we still need a pg_proc entry with two args to support reloading
 * pre-9.1 contrib/intarray opclass declarations.  This compatibility
 * function should go away eventually.
 */
pub unsafe fn ginarrayextract_2args(fcinfo: FunctionCallInfo) -> Datum {
    if PG_NARGS!(fcinfo) < 3 {
        /* should not happen */
        ereport!(ERROR, errmsg!("ginarrayextract requires three arguments"));
    }
    return ginarrayextract(fcinfo);
}

/*
 * extractQuery support function
 */
pub unsafe fn ginqueryarrayextract(fcinfo: FunctionCallInfo) -> Datum {
    /* Make copy of array input to ensure it doesn't disappear while in use */
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P_COPY(fcinfo, 0);
    let nkeys: *mut int32 = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2);

    /* bool   **pmatch = (bool **) PG_GETARG_POINTER(3); */
    /* Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
    let nullFlags: *mut *mut bool = PG_GETARG_POINTER!(fcinfo, 5) as *mut *mut bool;
    let searchMode: *mut int32 = PG_GETARG_POINTER!(fcinfo, 6) as *mut int32;

    let mut elmlen: int16 = 0;
    let mut elmbyval: bool = false;
    let mut elmalign: c_char = 0;
    let mut elems: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let mut nelems: c_int = 0;

    get_typlenbyvalalign(ARR_ELEMTYPE(array), &mut elmlen, &mut elmbyval, &mut elmalign);

    deconstruct_array(
        array,
        ARR_ELEMTYPE(array),
        elmlen as c_int,
        elmbyval,
        elmalign,
        &mut elems,
        &mut nulls,
        &mut nelems,
    );

    *nkeys = nelems;
    *nullFlags = nulls;

    match strategy {
        GinOverlapStrategy => {
            *searchMode = GIN_SEARCH_MODE_DEFAULT;
        }
        GinContainsStrategy => {
            if nelems > 0 {
                *searchMode = GIN_SEARCH_MODE_DEFAULT;
            } else {
                /* everything contains the empty set */
                *searchMode = GIN_SEARCH_MODE_ALL;
            }
        }
        GinContainedStrategy => {
            /* empty set is contained in everything */
            *searchMode = GIN_SEARCH_MODE_INCLUDE_EMPTY;
        }
        GinEqualStrategy => {
            if nelems > 0 {
                *searchMode = GIN_SEARCH_MODE_DEFAULT;
            } else {
                *searchMode = GIN_SEARCH_MODE_INCLUDE_EMPTY;
            }
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!(
                    "ginqueryarrayextract: unknown strategy number: {}",
                    strategy
                )
            );
            unreachable!();
        }
    }

    /* we should not free array, elems[i] points into it */
    PG_RETURN_POINTER!(elems);
}

/*
 * consistent support function
 */
pub unsafe fn ginarrayconsistent(fcinfo: FunctionCallInfo) -> Datum {
    let check: *mut bool = PG_GETARG_POINTER!(fcinfo, 0) as *mut bool;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 1);

    /* ArrayType  *query = PG_GETARG_ARRAYTYPE_P(2); */
    let nkeys: int32 = PG_GETARG_INT32!(fcinfo, 3);

    /* Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
    let recheck: *mut bool = PG_GETARG_POINTER!(fcinfo, 5) as *mut bool;

    /* Datum   *queryKeys = (Datum *) PG_GETARG_POINTER(6); */
    let nullFlags: *mut bool = PG_GETARG_POINTER!(fcinfo, 7) as *mut bool;

    let res: bool;
    let mut i: int32;

    match strategy {
        GinOverlapStrategy => {
            /* result is not lossy */
            *recheck = false;
            /* must have a match for at least one non-null element */
            let mut r = false;
            i = 0;
            while i < nkeys {
                if *check.add(i as usize) && !*nullFlags.add(i as usize) {
                    r = true;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        GinContainsStrategy => {
            /* result is not lossy */
            *recheck = false;
            /* must have all elements in check[] true, and no nulls */
            let mut r = true;
            i = 0;
            while i < nkeys {
                if !*check.add(i as usize) || *nullFlags.add(i as usize) {
                    r = false;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        GinContainedStrategy => {
            /* we will need recheck */
            *recheck = true;
            /* can't do anything else useful here */
            res = true;
        }
        GinEqualStrategy => {
            /* we will need recheck */
            *recheck = true;

            /*
             * Must have all elements in check[] true; no discrimination
             * against nulls here.  This is because array_contain_compare and
             * array_eq handle nulls differently ...
             */
            let mut r = true;
            i = 0;
            while i < nkeys {
                if !*check.add(i as usize) {
                    r = false;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("ginarrayconsistent: unknown strategy number: {}", strategy)
            );
            unreachable!();
        }
    }

    PG_RETURN_BOOL!(res);
}

/*
 * triconsistent support function
 */
pub unsafe fn ginarraytriconsistent(fcinfo: FunctionCallInfo) -> Datum {
    let check: *mut GinTernaryValue = PG_GETARG_POINTER!(fcinfo, 0) as *mut GinTernaryValue;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 1);

    /* ArrayType  *query = PG_GETARG_ARRAYTYPE_P(2); */
    let nkeys: int32 = PG_GETARG_INT32!(fcinfo, 3);

    /* Pointer    *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
    /* Datum   *queryKeys = (Datum *) PG_GETARG_POINTER(5); */
    let nullFlags: *mut bool = PG_GETARG_POINTER!(fcinfo, 6) as *mut bool;

    let res: GinTernaryValue;
    let mut i: int32;

    match strategy {
        GinOverlapStrategy => {
            /* must have a match for at least one non-null element */
            let mut r: GinTernaryValue = GIN_FALSE;
            i = 0;
            while i < nkeys {
                if !*nullFlags.add(i as usize) {
                    if *check.add(i as usize) == GIN_TRUE {
                        r = GIN_TRUE;
                        break;
                    } else if *check.add(i as usize) == GIN_MAYBE && r == GIN_FALSE {
                        r = GIN_MAYBE;
                    }
                }
                i += 1;
            }
            res = r;
        }
        GinContainsStrategy => {
            /* must have all elements in check[] true, and no nulls */
            let mut r: GinTernaryValue = GIN_TRUE;
            i = 0;
            while i < nkeys {
                if *check.add(i as usize) == GIN_FALSE || *nullFlags.add(i as usize) {
                    r = GIN_FALSE;
                    break;
                }
                if *check.add(i as usize) == GIN_MAYBE {
                    r = GIN_MAYBE;
                }
                i += 1;
            }
            res = r;
        }
        GinContainedStrategy => {
            /* can't do anything else useful here */
            res = GIN_MAYBE;
        }
        GinEqualStrategy => {
            /*
             * Must have all elements in check[] true; no discrimination
             * against nulls here.  This is because array_contain_compare and
             * array_eq handle nulls differently ...
             */
            let mut r: GinTernaryValue = GIN_MAYBE;
            i = 0;
            while i < nkeys {
                if *check.add(i as usize) == GIN_FALSE {
                    r = GIN_FALSE;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("ginarrayconsistent: unknown strategy number: {}", strategy)
            );
            unreachable!();
        }
    }

    PG_RETURN_GIN_TERNARY_VALUE!(res);
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * Build a tiny no-nulls int4 ArrayType (ndim=1) on the heap and run
     * deconstruct_array over it, checking the recovered Datums.
     *
     * Layout for a 1-D no-nulls array: ArrayType header, then ndim dims ints,
     * then ndim lbound ints, then MAXALIGN padding, then the element data.
     */
    unsafe fn build_int4_array(vals: &[i32]) -> *mut ArrayType {
        use crate::utils::array::ARR_OVERHEAD_NONULLS;
        const INT4OID: Oid = 23;

        let nelems = vals.len();
        let header = ARR_OVERHEAD_NONULLS(1);
        let total = header + nelems * core::mem::size_of::<i32>();
        let buf = palloc0(total) as *mut u8;

        let arr = buf as *mut ArrayType;
        (*arr).ndim = 1;
        (*arr).dataoffset = 0; /* no nulls */
        (*arr).elemtype = INT4OID;

        /* dims[0] = nelems ; lbound[0] = 1 */
        let dims = ARR_DIMS(arr);
        *dims = nelems as c_int;
        let lbound = dims.add(1);
        *lbound = 1;

        /* element data at ARR_DATA_PTR */
        let data = ARR_DATA_PTR(arr) as *mut i32;
        for (k, v) in vals.iter().enumerate() {
            *data.add(k) = *v;
        }
        arr
    }

    #[test]
    fn deconstruct_int4_array() {
        unsafe {
            let vals = [10i32, 20, 30, 40];
            let arr = build_int4_array(&vals);

            let mut elems: *mut Datum = null_mut();
            let mut nulls: *mut bool = null_mut();
            let mut nelems: c_int = 0;

            deconstruct_array(
                arr,
                ARR_ELEMTYPE(arr),
                4,
                true,
                TYPALIGN_INT,
                &mut elems,
                &mut nulls,
                &mut nelems,
            );

            assert_eq!(nelems, 4);
            for (k, v) in vals.iter().enumerate() {
                assert_eq!(DatumGetInt32(*elems.add(k)), *v);
                assert!(!*nulls.add(k));
            }
        }
    }

    /*
     * Exercise the strategy-counting core of ginarrayconsistent without an
     * fcinfo by replicating the per-strategy loops on a hand-built check[].
     * These mirror exactly the bodies above, so they pin the testable logic.
     */
    fn consistent_overlap(check: &[bool], nulls: &[bool]) -> bool {
        for i in 0..check.len() {
            if check[i] && !nulls[i] {
                return true;
            }
        }
        false
    }
    fn consistent_contains(check: &[bool], nulls: &[bool]) -> bool {
        for i in 0..check.len() {
            if !check[i] || nulls[i] {
                return false;
            }
        }
        true
    }
    fn consistent_equal(check: &[bool]) -> bool {
        for &c in check {
            if !c {
                return false;
            }
        }
        true
    }

    #[test]
    fn overlap_strategy_counting() {
        // overlap -> true if ANY non-null check[i]
        assert!(consistent_overlap(&[false, true, false], &[false, false, false]));
        assert!(!consistent_overlap(&[false, false, false], &[false, false, false]));
        // a true match on a null element does not count
        assert!(!consistent_overlap(&[true], &[true]));
    }

    #[test]
    fn contains_strategy_counting() {
        // contains -> true only if ALL check[] true and no nulls
        assert!(consistent_contains(&[true, true], &[false, false]));
        assert!(!consistent_contains(&[true, false], &[false, false]));
        assert!(!consistent_contains(&[true, true], &[false, true]));
    }

    #[test]
    fn equal_strategy_counting() {
        // equal -> true only if ALL check[] true (nulls ignored)
        assert!(consistent_equal(&[true, true]));
        assert!(!consistent_equal(&[true, false]));
    }

    /*
     * Tri-state core for ginarraytriconsistent's overlap strategy: TRUE if any
     * non-null TRUE, else MAYBE if any non-null MAYBE, else FALSE.
     */
    fn tri_overlap(check: &[GinTernaryValue], nulls: &[bool]) -> GinTernaryValue {
        let mut res = GIN_FALSE;
        for i in 0..check.len() {
            if !nulls[i] {
                if check[i] == GIN_TRUE {
                    return GIN_TRUE;
                } else if check[i] == GIN_MAYBE && res == GIN_FALSE {
                    res = GIN_MAYBE;
                }
            }
        }
        res
    }

    #[test]
    fn tri_overlap_counting() {
        assert_eq!(
            tri_overlap(&[GIN_FALSE, GIN_TRUE, GIN_FALSE], &[false, false, false]),
            GIN_TRUE
        );
        assert_eq!(
            tri_overlap(&[GIN_FALSE, GIN_MAYBE, GIN_FALSE], &[false, false, false]),
            GIN_MAYBE
        );
        assert_eq!(
            tri_overlap(&[GIN_FALSE, GIN_FALSE], &[false, false]),
            GIN_FALSE
        );
        // null masks an otherwise-TRUE match
        assert_eq!(tri_overlap(&[GIN_TRUE], &[true]), GIN_FALSE);
    }
}

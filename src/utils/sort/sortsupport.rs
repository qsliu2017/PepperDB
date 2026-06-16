//! Support routines for accelerated sorting.
//!
//! Combines:
//!   IMPL:   postgres/src/backend/utils/sort/sortsupport.c
//!   HEADER: postgres/src/include/utils/sortsupport.h
//!           (SortSupportData struct, SortSupport typedef, and the inline
//!            Apply*SortComparator / ApplySortAbbrevFullComparator helpers)
//!
//! #include mapping:
//!   "postgres.h"            -> use crate::prelude::*
//!   "access/gist.h"         -> GIST_AM_OID / GIST_SORTSUPPORT_PROC (STUB consts below)
//!   "access/nbtree.h"       -> BTORDER_PROC / BTSORTSUPPORT_PROC (STUB consts below)
//!   "fmgr.h"                -> crate::utils::fmgr (FmgrInfo, FunctionCallInfoBaseData,
//!                              fmgr_info_cxt, InitFunctionCallInfoData!, FunctionCallInvoke!,
//!                              SizeForFunctionCallInfo, OidFunctionCall1)
//!   "utils/lsyscache.h"     -> get_ordering_op_properties / get_opfamily_proc (STUB)
//!   "utils/rel.h"           -> Relation accessors (STUB)
//!   "access/attnum.h"       -> AttrNumber (crate::nodes::primnodes::AttrNumber)
//!   "utils/relcache.h"      -> Relation (STUB opaque type)

use crate::prelude::*;
use crate::{FunctionCallInvoke, InitFunctionCallInfoData};
use crate::nodes::primnodes::AttrNumber;
use crate::utils::fmgr::{
    FmgrInfo, FunctionCallInfo, FunctionCallInfoBaseData, SizeForFunctionCallInfo, fmgr_info_cxt,
};
use crate::utils::palloc::MemoryContext;
use core::ffi::{c_int, c_void};

// ---------------------------------------------------------------------------
// Stubbed external dependencies (not yet ported).
// ---------------------------------------------------------------------------

/// `Relation` from utils/relcache.h - opaque RelationData pointer.
// TODO(pg-port): replace with the real Relation type once relcache is ported.
pub type Relation = *mut c_void;

/// `CompareType` from access/cmptype.h. Only COMPARE_GT is consulted here.
// TODO(pg-port): replace with the real CompareType enum once cmptype is ported.
pub type CompareType = c_int;
pub const COMPARE_GT: CompareType = 5; // matches PostgreSQL's COMPARE_GT ordinal

// pg_amproc support-function "strategy" numbers.
// TODO(pg-port): pull these from access/nbtree.h / access/gist.h once ported.
pub const BTORDER_PROC: c_int = 1; // access/nbtree.h
pub const BTSORTSUPPORT_PROC: c_int = 2; // access/nbtree.h
pub const GIST_SORTSUPPORT_PROC: c_int = 11; // access/gist.h
pub const GIST_AM_OID: Oid = 783; // access/gist.h (pg_am OID of the gist AM)

/// `get_ordering_op_properties` (utils/lsyscache.c). STUB.
// TODO(pg-port): port lsyscache.c; resolves an ordering operator into its
// opfamily / opcintype / compare-type.
#[allow(unused_variables)]
unsafe fn get_ordering_op_properties(
    opno: Oid,
    opfamily: *mut Oid,
    opcintype: *mut Oid,
    cmptype: *mut CompareType,
) -> bool {
    unimplemented!("get_ordering_op_properties: lsyscache not yet ported")
}

/// `get_opfamily_proc` (utils/lsyscache.c). STUB.
// TODO(pg-port): port lsyscache.c; looks up a support proc OID in pg_amproc.
#[allow(unused_variables)]
unsafe fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: c_int) -> Oid {
    unimplemented!("get_opfamily_proc: lsyscache not yet ported")
}

/// `OidFunctionCall1` (fmgr.h convenience for PointerGetDatum(ssup)). STUB.
// TODO(pg-port): the real OidFunctionCall1 lives in crate::utils::fmgr but
// depends on fmgr_info catalog lookups not yet wired; stubbed here for the
// sortsupport-setup paths that are themselves stubbed.
#[allow(unused_variables)]
unsafe fn OidFunctionCall1(functionId: Oid, arg1: Datum) -> Datum {
    unimplemented!("OidFunctionCall1: fmgr catalog path not yet ported")
}

// ---------------------------------------------------------------------------
// sortsupport.h -- SortSupportData / SortSupport
// ---------------------------------------------------------------------------

pub type SortSupport = *mut SortSupportData;

/// `SortSupportData` (utils/sortsupport.h).
#[repr(C)]
pub struct SortSupportData {
    // Initialized before BTSORTSUPPORT and not changed later.
    pub ssup_cxt: MemoryContext, // Context containing sort info
    pub ssup_collation: Oid,     // Collation to use, or InvalidOid

    // May be changed after BTSORTSUPPORT is called.
    pub ssup_reverse: bool,      // descending-order sort?
    pub ssup_nulls_first: bool,  // sort nulls first?

    // Workspace for callers; not touched by opclass functions.
    pub ssup_attno: AttrNumber,  // column number to sort

    // Zeroed before BTSORTSUPPORT; workspace for opclass functions.
    pub ssup_extra: *mut c_void,

    /*
     * Comparator: same API as a traditional btree comparison function, ie,
     * return <0, 0, or >0 according as x is less than, equal to, or greater
     * than y.  x and y are guaranteed not null.  May be the authoritative or
     * the abbreviated comparator.
     */
    pub comparator: Option<unsafe fn(x: Datum, y: Datum, ssup: SortSupport) -> c_int>,

    // "Abbreviated key" infrastructure.
    pub abbreviate: bool,

    pub abbrev_converter: Option<unsafe fn(original: Datum, ssup: SortSupport) -> Datum>,

    pub abbrev_abort: Option<unsafe fn(memtupcount: c_int, ssup: SortSupport) -> bool>,

    pub abbrev_full_comparator:
        Option<unsafe fn(x: Datum, y: Datum, ssup: SortSupport) -> c_int>,
}

/*
 * Apply a sort comparator function and return a 3-way comparison result.
 * This takes care of handling reverse-sort and NULLs-ordering properly.
 */
#[inline]
pub unsafe fn ApplySortComparator(
    datum1: Datum,
    isNull1: bool,
    datum2: Datum,
    isNull2: bool,
    ssup: SortSupport,
) -> c_int {
    let mut compare: c_int;

    if isNull1 {
        if isNull2 {
            compare = 0; // NULL "=" NULL
        } else if (*ssup).ssup_nulls_first {
            compare = -1; // NULL "<" NOT_NULL
        } else {
            compare = 1; // NULL ">" NOT_NULL
        }
    } else if isNull2 {
        if (*ssup).ssup_nulls_first {
            compare = 1; // NOT_NULL ">" NULL
        } else {
            compare = -1; // NOT_NULL "<" NULL
        }
    } else {
        compare = ((*ssup).comparator.unwrap())(datum1, datum2, ssup);
        if (*ssup).ssup_reverse {
            INVERT_COMPARE_RESULT(&mut compare);
        }
    }

    compare
}

/*
 * Apply a sort comparator function and return a 3-way comparison using full,
 * authoritative comparator.  This takes care of handling reverse-sort and
 * NULLs-ordering properly.
 */
#[inline]
pub unsafe fn ApplySortAbbrevFullComparator(
    datum1: Datum,
    isNull1: bool,
    datum2: Datum,
    isNull2: bool,
    ssup: SortSupport,
) -> c_int {
    let mut compare: c_int;

    if isNull1 {
        if isNull2 {
            compare = 0; // NULL "=" NULL
        } else if (*ssup).ssup_nulls_first {
            compare = -1; // NULL "<" NOT_NULL
        } else {
            compare = 1; // NULL ">" NOT_NULL
        }
    } else if isNull2 {
        if (*ssup).ssup_nulls_first {
            compare = 1; // NOT_NULL ">" NULL
        } else {
            compare = -1; // NOT_NULL "<" NULL
        }
    } else {
        compare = ((*ssup).abbrev_full_comparator.unwrap())(datum1, datum2, ssup);
        if (*ssup).ssup_reverse {
            INVERT_COMPARE_RESULT(&mut compare);
        }
    }

    compare
}

/// `INVERT_COMPARE_RESULT` (c.h): swaps the sign of a 3-way comparison.
/// C macro: `((var) = ((var) < 0) ? 1 : -(var))`
#[inline]
fn INVERT_COMPARE_RESULT(var: &mut c_int) {
    *var = if *var < 0 { 1 } else { -*var };
}

// ---------------------------------------------------------------------------
// sortsupport.c
// ---------------------------------------------------------------------------

/// Info needed to use an old-style comparison function as a sort comparator.
///
/// C lays this out as `{ FmgrInfo flinfo; FunctionCallInfoBaseData fcinfo; }`
/// with a flexible args[] tail on fcinfo, sized via SizeForSortShimExtra.  We
/// reproduce the same byte layout with #[repr(C)] so the trailing arg slots
/// allocated by SizeForSortShimExtra() land right after `fcinfo`.
#[repr(C)]
pub struct SortShimExtra {
    pub flinfo: FmgrInfo,                 // lookup data for comparison function
    pub fcinfo: FunctionCallInfoBaseData, // reusable callinfo structure (FAM tail)
}

/// C: `offsetof(SortShimExtra, fcinfo) + SizeForFunctionCallInfo(nargs)`
#[inline]
pub const fn SizeForSortShimExtra(nargs: usize) -> usize {
    core::mem::offset_of!(SortShimExtra, fcinfo) + SizeForFunctionCallInfo(nargs)
}

/*
 * Shim function for calling an old-style comparator
 *
 * This is essentially an inlined version of FunctionCall2Coll(), except
 * we assume that the FunctionCallInfoBaseData was already mostly set up by
 * PrepareSortSupportComparisonShim.
 */
unsafe fn comparison_shim(x: Datum, y: Datum, ssup: SortSupport) -> c_int {
    let extra = (*ssup).ssup_extra as *mut SortShimExtra;

    // extra->fcinfo.args[0].value = x; extra->fcinfo.args[1].value = y;
    let args = (*extra).fcinfo.args.as_mut_ptr();
    (*args.add(0)).value = x;
    (*args.add(1)).value = y;

    // just for paranoia's sake, we reset isnull each time
    (*extra).fcinfo.isnull = false;

    let fcinfo: FunctionCallInfo = &mut (*extra).fcinfo;
    let result = FunctionCallInvoke!(fcinfo);

    // Check for null result, since caller is clearly not expecting one
    if (*extra).fcinfo.isnull {
        elog!(ERROR, "function {} returned NULL", (*extra).flinfo.fn_oid);
    }

    DatumGetInt32(result)
}

/*
 * Set up a shim function to allow use of an old-style btree comparison
 * function as if it were a sort support comparator.
 */
pub unsafe fn PrepareSortSupportComparisonShim(cmpFunc: Oid, ssup: SortSupport) {
    let extra =
        MemoryContextAlloc((*ssup).ssup_cxt, SizeForSortShimExtra(2)) as *mut SortShimExtra;

    // Lookup the comparison function
    fmgr_info_cxt(cmpFunc, &mut (*extra).flinfo, (*ssup).ssup_cxt);

    // We can initialize the callinfo just once and re-use it
    let fcinfo: FunctionCallInfo = &mut (*extra).fcinfo;
    InitFunctionCallInfoData!(
        fcinfo,
        &mut (*extra).flinfo,
        2i16,
        (*ssup).ssup_collation,
        null_mut(),
        null_mut()
    );
    let args = (*extra).fcinfo.args.as_mut_ptr();
    (*args.add(0)).isnull = false;
    (*args.add(1)).isnull = false;

    (*ssup).ssup_extra = extra as *mut c_void;
    (*ssup).comparator = Some(comparison_shim);
}

/*
 * Look up and call sortsupport function to setup SortSupport comparator;
 * or if no such function exists or it declines to set up the appropriate
 * state, prepare a suitable shim.
 */
unsafe fn FinishSortSupportFunction(opfamily: Oid, opcintype: Oid, ssup: SortSupport) {
    // Look for a sort support function
    let sortSupportFunction =
        get_opfamily_proc(opfamily, opcintype, opcintype, BTSORTSUPPORT_PROC);
    if OidIsValid(sortSupportFunction) {
        /*
         * The sort support function can provide a comparator, but it can also
         * choose not to so (e.g. based on the selected collation).
         */
        OidFunctionCall1(sortSupportFunction, PointerGetDatum(ssup as *const c_void));
    }

    if (*ssup).comparator.is_none() {
        let sortFunction = get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC);

        if !OidIsValid(sortFunction) {
            elog!(
                ERROR,
                "missing support function {}({},{}) in opfamily {}",
                BTORDER_PROC,
                opcintype,
                opcintype,
                opfamily
            );
        }

        // We'll use a shim to call the old-style btree comparator
        PrepareSortSupportComparisonShim(sortFunction, ssup);
    }
}

/*
 * Fill in SortSupport given an ordering operator (btree "<" or ">" operator).
 *
 * Caller must previously have zeroed the SortSupportData structure and then
 * filled in ssup_cxt, ssup_collation, and ssup_nulls_first.  This will fill
 * in ssup_reverse as well as the comparator function pointer.
 */
#[allow(unused_variables)]
pub unsafe fn PrepareSortSupportFromOrderingOp(orderingOp: Oid, ssup: SortSupport) {
    // TODO(pg-port): needs get_ordering_op_properties (lsyscache).
    unimplemented!("PrepareSortSupportFromOrderingOp: lsyscache not yet ported")
}

/*
 * Fill in SortSupport given an index relation and attribute.
 *
 * Caller must previously have zeroed the SortSupportData structure and then
 * filled in ssup_cxt, ssup_attno, ssup_collation, and ssup_nulls_first.  This
 * will fill in ssup_reverse (based on the supplied argument), as well as the
 * comparator function pointer.
 */
#[allow(unused_variables)]
pub unsafe fn PrepareSortSupportFromIndexRel(indexRel: Relation, reverse: bool, ssup: SortSupport) {
    // TODO(pg-port): needs relcache Relation accessors (rd_opfamily/rd_opcintype/
    // rd_indam/rd_rel) plus FinishSortSupportFunction.
    unimplemented!("PrepareSortSupportFromIndexRel: relcache not yet ported")
}

/*
 * Fill in SortSupport given a GiST index relation
 *
 * Caller must previously have zeroed the SortSupportData structure and then
 * filled in ssup_cxt, ssup_attno, ssup_collation, and ssup_nulls_first.  This
 * will fill in ssup_reverse (always false for GiST index build), as well as
 * the comparator function pointer.
 */
#[allow(unused_variables)]
pub unsafe fn PrepareSortSupportFromGistIndexRel(indexRel: Relation, ssup: SortSupport) {
    // TODO(pg-port): needs relcache Relation accessors plus get_opfamily_proc/
    // OidFunctionCall1.
    unimplemented!("PrepareSortSupportFromGistIndexRel: relcache not yet ported")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple authoritative int comparator over Datum-encoded i32 values.
    unsafe fn int_cmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
        let a = DatumGetInt32(x);
        let b = DatumGetInt32(y);
        if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }
    }

    fn make_ssup(reverse: bool, nulls_first: bool) -> SortSupportData {
        SortSupportData {
            ssup_cxt: null_mut(),
            ssup_collation: 0,
            ssup_reverse: reverse,
            ssup_nulls_first: nulls_first,
            ssup_attno: 0,
            ssup_extra: null_mut(),
            comparator: Some(int_cmp),
            abbreviate: false,
            abbrev_converter: None,
            abbrev_abort: None,
            abbrev_full_comparator: None,
        }
    }

    fn d(v: i32) -> Datum {
        Int32GetDatum(v)
    }

    #[test]
    fn invert_compare_result() {
        let mut c = -5;
        INVERT_COMPARE_RESULT(&mut c);
        assert_eq!(c, 1);
        let mut c = 5;
        INVERT_COMPARE_RESULT(&mut c);
        assert_eq!(c, -5);
        let mut c = 0;
        INVERT_COMPARE_RESULT(&mut c);
        assert_eq!(c, 0);
    }

    #[test]
    fn forward_ordering() {
        let mut s = make_ssup(false, false);
        let ssup: SortSupport = &mut s;
        unsafe {
            assert!(ApplySortComparator(d(1), false, d(2), false, ssup) < 0);
            assert!(ApplySortComparator(d(2), false, d(1), false, ssup) > 0);
            assert_eq!(ApplySortComparator(d(7), false, d(7), false, ssup), 0);
        }
    }

    #[test]
    fn reverse_ordering() {
        let mut s = make_ssup(true, false);
        let ssup: SortSupport = &mut s;
        unsafe {
            // reverse: 1 vs 2 -> would be -1, inverted to +1
            assert!(ApplySortComparator(d(1), false, d(2), false, ssup) > 0);
            assert!(ApplySortComparator(d(2), false, d(1), false, ssup) < 0);
            assert_eq!(ApplySortComparator(d(7), false, d(7), false, ssup), 0);
        }
    }

    #[test]
    fn nulls_first_ordering() {
        let mut s = make_ssup(false, true);
        let ssup: SortSupport = &mut s;
        unsafe {
            // NULL "<" NOT_NULL when nulls_first
            assert_eq!(ApplySortComparator(d(0), true, d(5), false, ssup), -1);
            // NOT_NULL ">" NULL when nulls_first
            assert_eq!(ApplySortComparator(d(5), false, d(0), true, ssup), 1);
            // NULL "=" NULL
            assert_eq!(ApplySortComparator(d(0), true, d(0), true, ssup), 0);
        }
    }

    #[test]
    fn nulls_last_ordering() {
        let mut s = make_ssup(false, false);
        let ssup: SortSupport = &mut s;
        unsafe {
            // NULL ">" NOT_NULL when nulls last
            assert_eq!(ApplySortComparator(d(0), true, d(5), false, ssup), 1);
            // NOT_NULL "<" NULL when nulls last
            assert_eq!(ApplySortComparator(d(5), false, d(0), true, ssup), -1);
        }
    }

    #[test]
    fn nulls_first_independent_of_reverse() {
        // ssup_reverse does not flip NULL ordering: that is governed solely by
        // ssup_nulls_first in ApplySortComparator.
        let mut s = make_ssup(true, true);
        let ssup: SortSupport = &mut s;
        unsafe {
            assert_eq!(ApplySortComparator(d(0), true, d(5), false, ssup), -1);
            assert_eq!(ApplySortComparator(d(5), false, d(0), true, ssup), 1);
        }
    }

    #[test]
    fn abbrev_full_comparator_path() {
        let mut s = make_ssup(false, false);
        s.abbrev_full_comparator = Some(int_cmp);
        let ssup: SortSupport = &mut s;
        unsafe {
            assert!(ApplySortAbbrevFullComparator(d(1), false, d(2), false, ssup) < 0);
            assert_eq!(ApplySortAbbrevFullComparator(d(0), true, d(0), true, ssup), 0);
        }
    }
}

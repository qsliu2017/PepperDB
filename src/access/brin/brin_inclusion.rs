//! Translation of postgres/src/backend/access/brin/brin_inclusion.c
//!
//!     Implementation of inclusion opclasses for BRIN
//!
//! The "inclusion" opclass summarizes a heap page range by the R-Tree "union"
//! (bounding element) of all indexed values seen in that range, plus two boolean
//! flags.  Each BRIN index tuple therefore stores exactly three Datums per
//! column:
//!
//!   INCLUSION_UNION           the union of the values in the block range
//!   INCLUSION_UNMERGEABLE     whether the values cannot be merged (e.g. an IPv6
//!                             address amidst IPv4 addresses)
//!   INCLUSION_CONTAINS_EMPTY  whether an empty value is present in any tuple
//!
//! Four mandatory BRIN support procedures are implemented here:
//!
//!   brin_inclusion_opcinfo     (BRIN_PROCNUM_OPCINFO)    describe the shape
//!   brin_inclusion_add_value   (BRIN_PROCNUM_ADDVALUE)   fold a new value in
//!   brin_inclusion_consistent  (BRIN_PROCNUM_CONSISTENT) test a scan key
//!   brin_inclusion_union       (BRIN_PROCNUM_UNION)      merge two summaries
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! PORTING NOTES
//! -------------
//! The core add_value / consistent / union logic is translated 1:1 and is
//! REAL: given valid merge/mergeable/contains/empty procinfos and the R-Tree
//! strategy procinfos, it tracks the running union and answers scan keys exactly
//! like upstream.  The strategy dispatch (left-of, over-right, overlap, contains,
//! adjacent, ...) is done via FunctionCall2Coll over the ported fmgr; the
//! geometric/range operators themselves are NOT reimplemented here.
//!
//! The catalog-dependent plumbing is NOT yet ported and is stubbed:
//!   * `inclusion_get_procinfo`'s cache-FILL path (index_getprocid /
//!     index_getprocinfo / fmgr_info_copy against bdesc->bd_index) is
//!     `unimplemented!()`.  The cache LOOKUP path -- reading an already-populated
//!     FmgrInfo (or the `extra_proc_missing` flag) out of the per-column
//!     `InclusionOpaque` stored in `oi_opaque` -- is real, so callers/tests that
//!     pre-populate the opaque can exercise the logic.
//!   * `inclusion_get_strategy_procinfo`'s cache-FILL path (SearchSysCache4 on
//!     pg_amop + get_opcode + fmgr_info_cxt) is `unimplemented!()`.  The LOOKUP
//!     path is real, like in brin_minmax.
//!   * `lookup_type_cache` (the TypeCacheEntry build in opcinfo) is stubbed.
//!   * The `bd_tupdesc` CompactAttribute lookup (attbyval/attlen) is funneled
//!     through `inclusion_get_attr`, a stub returning a caller-provided
//!     `InclusionAttr` -- the real `TupleDescCompactAttr(bd_tupdesc, ..)` decode
//!     is unported.
//!
//! The locally-defined #[repr(C)] structs (BrinValues, BrinOpcInfo, BrinDesc,
//! InclusionScanKey) mirror only the subset of fields these four functions
//! touch; see access/brin_tuple.h and access/brin_internal.h for the full
//! definitions.  They intentionally duplicate the mirrors in brin_minmax.rs to
//! keep each opclass file self-contained while the framework headers are
//! unported.

use crate::prelude::*; // Datum, c_int, uint16, Oid, bool, palloc0, null_mut, DatumGet*/*GetDatum
use crate::utils::fmgr::{FmgrInfo, FunctionCall1Coll, FunctionCall2Coll, FunctionCallInfo};
use crate::utils::adt::datum::datumCopy;
use crate::access::stratnum::{
    RTAboveStrategyNumber, RTAdjacentStrategyNumber, RTBelowStrategyNumber,
    RTContainedByStrategyNumber, RTContainsElemStrategyNumber, RTContainsStrategyNumber,
    RTEqualStrategyNumber, RTGreaterEqualStrategyNumber, RTGreaterStrategyNumber,
    RTLeftStrategyNumber, RTLessEqualStrategyNumber, RTLessStrategyNumber, RTMaxStrategyNumber,
    RTOverAboveStrategyNumber, RTOverBelowStrategyNumber, RTOverLeftStrategyNumber,
    RTOverRightStrategyNumber, RTOverlapStrategyNumber, RTRightStrategyNumber,
    RTSameStrategyNumber, RTSubEqualStrategyNumber, RTSubStrategyNumber, RTSuperEqualStrategyNumber,
    RTSuperStrategyNumber,
};
use crate::{
    PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_OID, PG_GETARG_POINTER, PG_GET_COLLATION, PG_NARGS,
    PG_RETURN_BOOL, PG_RETURN_DATUM, PG_RETURN_POINTER, PG_RETURN_VOID,
};

/* AttrNumber is `int16` (primnodes.h / c.h); mirror it locally for clarity. */
pub type AttrNumber = int16;

pub const InvalidOid: Oid = 0;

/*
 * Additional SQL level support functions.  Procedure numbers must not use
 * values reserved for BRIN itself; see brin_internal.h.
 */
pub const INCLUSION_MAX_PROCNUMS: usize = 4; /* maximum support procs we need */
pub const PROCNUM_MERGE: uint16 = 11; /* required */
pub const PROCNUM_MERGEABLE: uint16 = 12; /* optional */
pub const PROCNUM_CONTAINS: uint16 = 13; /* optional */
pub const PROCNUM_EMPTY: uint16 = 14; /* optional */

/*
 * Subtract this from procnum to obtain index in InclusionOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
pub const PROCNUM_BASE: uint16 = 11;

/*
 * The values stored in the bv_values arrays correspond to:
 *   INCLUSION_UNION             the union of the values in the block range
 *   INCLUSION_UNMERGEABLE       whether the values cannot be merged
 *   INCLUSION_CONTAINS_EMPTY    whether an empty value is present
 */
pub const INCLUSION_UNION: usize = 0;
pub const INCLUSION_UNMERGEABLE: usize = 1;
pub const INCLUSION_CONTAINS_EMPTY: usize = 2;

/*
 * ScanKey strategy fields touched by brin_inclusion_consistent.  This is the
 * subset of access/skey.h's ScanKeyData that the consistent function reads.
 * (The crate's full ScanKeyData lives in access/common/scankey.rs; we mirror
 * only the read fields here to keep the port self-contained.)
 */
#[repr(C)]
pub struct InclusionScanKey {
    pub sk_attno: AttrNumber,
    pub sk_strategy: uint16,
    pub sk_subtype: Oid,
    pub sk_argument: Datum,
}

/*
 * BrinValues -- one per indexed column in an in-memory BRIN tuple.
 * From access/brin_tuple.h; the serialize callback is opaque here.
 */
#[repr(C)]
pub struct BrinValues {
    pub bv_attno: AttrNumber,      /* index attribute number */
    pub bv_hasnulls: bool,         /* are there any nulls in the page range? */
    pub bv_allnulls: bool,         /* are all values nulls in the page range? */
    pub bv_values: *mut Datum,     /* current accumulated values (3 stored) */
    pub bv_mem_value: Datum,       /* expanded accumulated values */
    pub bv_context: *mut c_void,   /* MemoryContext */
    pub bv_serialize: *mut c_void, /* brin_serialize_callback_type */
}

/*
 * Number of stored Datums the opclass type-cache flexible array is sized for in
 * this port.  Real BRIN sizes oi_typcache[] to oi_nstored; inclusion uses 3.
 */
pub const INCLUSION_NSTORED: usize = 3;

/*
 * BrinOpcInfo -- result of the OpcInfo amproc; describes the on-disk shape of
 * one index column.  From access/brin_internal.h.  oi_typcache is a flexible
 * array of TypeCacheEntry* in C; we fix it at INCLUSION_NSTORED entries and keep
 * the entries opaque (TypeCacheEntry is unported).
 */
#[repr(C)]
pub struct BrinOpcInfo {
    pub oi_nstored: uint16,                            /* # Datums stored per column */
    pub oi_regular_nulls: bool,                        /* regular NULL handling? */
    pub oi_opaque: *mut c_void,                        /* opclass private (InclusionOpaque) */
    pub oi_typcache: [*mut c_void; INCLUSION_NSTORED], /* TypeCacheEntry* per column */
}

/*
 * BrinDesc -- decodes BRIN tuples to/from disk.  From access/brin_internal.h.
 * bd_info is the per-column BrinOpcInfo array (natts long); the remaining
 * pointer fields (context/index/tupdescs) are opaque in this port.
 */
#[repr(C)]
pub struct BrinDesc {
    pub bd_context: *mut c_void,        /* MemoryContext */
    pub bd_index: *mut c_void,          /* Relation */
    pub bd_tupdesc: *mut c_void,        /* TupleDesc */
    pub bd_disktdesc: *mut c_void,      /* TupleDesc */
    pub bd_totalstored: c_int,          /* total stored Datums across columns */
    pub bd_info: *mut *mut BrinOpcInfo, /* per-column info; bd_tupdesc->natts long */
}

/*
 * Per-column opclass private area, stashed in BrinOpcInfo.oi_opaque.
 * From the C InclusionOpaque: a per-subtype cache of strategy procedures plus a
 * cache of the (up to four) opclass support procedures.
 */
#[repr(C)]
pub struct InclusionOpaque {
    pub extra_procinfos: [FmgrInfo; INCLUSION_MAX_PROCNUMS],
    pub extra_proc_missing: [bool; INCLUSION_MAX_PROCNUMS],
    pub cached_subtype: Oid,
    pub strategy_procinfos: [FmgrInfo; RTMaxStrategyNumber as usize],
}

/*
 * Stub for the attribute info brin_inclusion needs out of bd_tupdesc.  In C this
 * is `TupleDescCompactAttr(bdesc->bd_tupdesc, attno - 1)` yielding a
 * CompactAttribute from which attbyval/attlen are read.  The tupdesc decode is
 * unported; `inclusion_get_attr` returns one of these instead.
 */
#[repr(C)]
pub struct InclusionAttr {
    pub attlen: c_int,
    pub attbyval: bool,
}

/* --------------------------------------------------------------------------
 * brin_inclusion_opcinfo  (BRIN_PROCNUM_OPCINFO)
 *
 * palloc0 a BrinOpcInfo describing a three-Datum [union, unmergeable,
 * contains_empty] column and wire up its opaque area.  In C the allocation
 * packs InclusionOpaque immediately after the BrinOpcInfo (a single palloc);
 * here we allocate the opaque separately to keep the layout safe in Rust, then
 * point oi_opaque at it.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_inclusion_opcinfo(fcinfo: FunctionCallInfo) -> Datum {
    let typoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    /*
     * All members of opaque are initialized lazily; both procinfo arrays start
     * out non-initialized by having fn_oid be InvalidOid, and "missing" false,
     * via the zeroing palloc0.
     */
    let result = palloc0(core::mem::size_of::<BrinOpcInfo>()) as *mut BrinOpcInfo;

    (*result).oi_nstored = 3;
    (*result).oi_regular_nulls = true;

    /* InclusionOpaque (packed after the struct in C); allocate it zeroed. */
    let opaque = palloc0(core::mem::size_of::<InclusionOpaque>()) as *mut InclusionOpaque;
    (*result).oi_opaque = opaque as *mut c_void;

    /*
     * oi_typcache[UNION] = lookup_type_cache(typoid, 0), and the two boolean
     * flags use lookup_type_cache(BOOLOID, 0).
     * STUB: lookup_type_cache / TypeCacheEntry are unported; leave the slots
     * null.  Recorded so callers know the type cache is not populated yet.
     */
    let _ = typoid; // TODO(pg-port): oi_typcache[UNION] = lookup_type_cache(typoid, 0)
    (*result).oi_typcache[INCLUSION_UNION] = null_mut();
    (*result).oi_typcache[INCLUSION_UNMERGEABLE] = null_mut(); // bool_typcache
    (*result).oi_typcache[INCLUSION_CONTAINS_EMPTY] = null_mut(); // bool_typcache

    PG_RETURN_POINTER!(result);
}

/* --------------------------------------------------------------------------
 * brin_inclusion_add_value  (BRIN_PROCNUM_ADDVALUE)
 *
 * Fold a not-null heap value into the page-range summary.  If the new value is
 * outside the union specified by the existing tuple values, update the index
 * tuple and return true.  Otherwise return false and leave it unmodified.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_inclusion_add_value(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let newval: Datum = PG_GETARG_DATUM!(fcinfo, 2);
    let isnull: bool = PG_GETARG_BOOL!(fcinfo, 3); /* asserts-only */
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let mut new = false;

    Assert!(!isnull);

    let attno: AttrNumber = (*column).bv_attno;
    let attr = inclusion_get_attr(bdesc, attno);

    /*
     * If the recorded value is null, copy the new value (which we know to be
     * not null), and we're almost done.
     */
    if (*column).bv_allnulls {
        *(*column).bv_values.add(INCLUSION_UNION) =
            datumCopy(newval, (*attr).attbyval, (*attr).attlen);
        *(*column).bv_values.add(INCLUSION_UNMERGEABLE) = BoolGetDatum(false);
        *(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY) = BoolGetDatum(false);
        (*column).bv_allnulls = false;
        new = true;
    }

    /*
     * No need for further processing if the block range is marked as containing
     * unmergeable values.
     */
    if DatumGetBool(*(*column).bv_values.add(INCLUSION_UNMERGEABLE)) {
        PG_RETURN_BOOL!(false);
    }

    /*
     * If the opclass supports the concept of empty values, test the passed new
     * value for emptiness; if it returns true, set the "contains empty" flag in
     * the element (unless already set).
     */
    let mut finfo = inclusion_get_procinfo(bdesc, attno as uint16, PROCNUM_EMPTY, true);
    if !finfo.is_null() && DatumGetBool(FunctionCall1Coll(finfo, colloid, newval)) {
        if !DatumGetBool(*(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY)) {
            *(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY) = BoolGetDatum(true);
            PG_RETURN_BOOL!(true);
        }

        PG_RETURN_BOOL!(false);
    }

    if new {
        PG_RETURN_BOOL!(true);
    }

    /* Check if the new value is already contained. */
    finfo = inclusion_get_procinfo(bdesc, attno as uint16, PROCNUM_CONTAINS, true);
    if !finfo.is_null()
        && DatumGetBool(FunctionCall2Coll(
            finfo,
            colloid,
            *(*column).bv_values.add(INCLUSION_UNION),
            newval,
        ))
    {
        PG_RETURN_BOOL!(false);
    }

    /*
     * Check if the new value is mergeable to the existing union.  If it is not,
     * mark the value as containing unmergeable elements and get out.
     */
    finfo = inclusion_get_procinfo(bdesc, attno as uint16, PROCNUM_MERGEABLE, true);
    if !finfo.is_null()
        && !DatumGetBool(FunctionCall2Coll(
            finfo,
            colloid,
            *(*column).bv_values.add(INCLUSION_UNION),
            newval,
        ))
    {
        *(*column).bv_values.add(INCLUSION_UNMERGEABLE) = BoolGetDatum(true);
        PG_RETURN_BOOL!(true);
    }

    /* Finally, merge the new value to the existing union. */
    finfo = inclusion_get_procinfo(bdesc, attno as uint16, PROCNUM_MERGE, false);
    let mut result = FunctionCall2Coll(
        finfo,
        colloid,
        *(*column).bv_values.add(INCLUSION_UNION),
        newval,
    );
    if !(*attr).attbyval
        && DatumGetPointer(result) != DatumGetPointer(*(*column).bv_values.add(INCLUSION_UNION))
    {
        pfree(DatumGetPointer(*(*column).bv_values.add(INCLUSION_UNION)) as *mut c_void);

        if result == newval {
            result = datumCopy(result, (*attr).attbyval, (*attr).attlen);
        }
    }
    *(*column).bv_values.add(INCLUSION_UNION) = result;

    PG_RETURN_BOOL!(true);
}

/* --------------------------------------------------------------------------
 * brin_inclusion_consistent  (BRIN_PROCNUM_CONSISTENT)
 *
 * Decide whether a scan key is consistent with the union summary.  NULL keys
 * and all-NULL ranges are filtered out by the AM before we are reached.  All
 * strategies are optional.  Placement strategies are answered by logically
 * negating the converse placement operator; overlap/contains by calling the
 * operator directly; contained-by/comparison strategies by combining the
 * overlap/contains/left-of operators with the "contains empty" flag.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_inclusion_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let key = PG_GETARG_POINTER!(fcinfo, 2) as *mut InclusionScanKey;
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);

    /* This opclass uses the old signature with only three arguments. */
    Assert!(PG_NARGS!(fcinfo) == 3);
    /* Should not be dealing with all-NULL ranges. */
    Assert!(!(*column).bv_allnulls);

    /* It has to be checked, if it contains elements that are not mergeable. */
    if DatumGetBool(*(*column).bv_values.add(INCLUSION_UNMERGEABLE)) {
        PG_RETURN_BOOL!(true);
    }

    let attno: AttrNumber = (*key).sk_attno;
    let subtype: Oid = (*key).sk_subtype;
    let query: Datum = (*key).sk_argument;
    let unionval: Datum = *(*column).bv_values.add(INCLUSION_UNION);

    match (*key).sk_strategy {
        /*
         * Placement strategies: implemented by logically negating the result of
         * the converse placement operator.  These all return false if either
         * argument is empty, so there is no need to check for empty elements.
         */
        RTLeftStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTOverRightStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTOverLeftStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTRightStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTOverRightStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTLeftStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTRightStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTOverLeftStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTBelowStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTOverAboveStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTOverBelowStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTAboveStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTOverAboveStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTBelowStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }
        RTAboveStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTOverBelowStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }

        /*
         * Overlap and contains strategies: call the operator and return its
         * result.  Empty elements don't change the result.
         */
        RTOverlapStrategyNumber
        | RTContainsStrategyNumber
        | RTContainsElemStrategyNumber
        | RTSubStrategyNumber
        | RTSubEqualStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, (*key).sk_strategy);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_DATUM!(result);
        }

        /*
         * Contained by strategies: we cannot just call the original operator
         * because some elements can be contained even though the union is not;
         * instead we use the overlap operator, and check empties separately
         * (empties are contained by everything).
         */
        RTContainedByStrategyNumber | RTSuperStrategyNumber | RTSuperEqualStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTOverlapStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if DatumGetBool(result) {
                PG_RETURN_BOOL!(true);
            }

            PG_RETURN_DATUM!(*(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY));
        }

        /*
         * Adjacent strategy: test for overlap first but, to be safe, also call
         * the actual adjacent operator.  An empty element cannot be adjacent to
         * any other, so there is no need to check for it.
         */
        RTAdjacentStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTOverlapStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if DatumGetBool(result) {
                PG_RETURN_BOOL!(true);
            }

            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTAdjacentStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_DATUM!(result);
        }

        /*
         * Basic comparison strategies.  Empty elements are considered to be less
         * than the others, so when there is a possibility that empty elements
         * change the result we return the "contains empty" flag.
         */
        RTLessStrategyNumber | RTLessEqualStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTRightStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if !DatumGetBool(result) {
                PG_RETURN_BOOL!(true);
            }

            PG_RETURN_DATUM!(*(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY));
        }

        RTSameStrategyNumber | RTEqualStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTContainsStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if DatumGetBool(result) {
                PG_RETURN_BOOL!(true);
            }

            PG_RETURN_DATUM!(*(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY));
        }

        RTGreaterEqualStrategyNumber => {
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTLeftStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if !DatumGetBool(result) {
                PG_RETURN_BOOL!(true);
            }

            PG_RETURN_DATUM!(*(*column).bv_values.add(INCLUSION_CONTAINS_EMPTY));
        }

        RTGreaterStrategyNumber => {
            /* no need to check for empty elements */
            let finfo = inclusion_get_strategy_procinfo(bdesc, attno as uint16, subtype, RTLeftStrategyNumber);
            let result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL!(!DatumGetBool(result));
        }

        _ => {
            /* shouldn't happen */
            ereport!(ERROR, errmsg!("invalid strategy number {}", (*key).sk_strategy));
            unreachable!();
        }
    }
}

/* --------------------------------------------------------------------------
 * brin_inclusion_union  (BRIN_PROCNUM_UNION)
 *
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_inclusion_union(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let col_a = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let col_b = PG_GETARG_POINTER!(fcinfo, 2) as *mut BrinValues;
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);

    Assert!((*col_a).bv_attno == (*col_b).bv_attno);
    Assert!(!(*col_a).bv_allnulls && !(*col_b).bv_allnulls);

    let attno: AttrNumber = (*col_a).bv_attno;
    let attr = inclusion_get_attr(bdesc, attno);

    /* If B includes empty elements, mark A similarly, if needed. */
    if !DatumGetBool(*(*col_a).bv_values.add(INCLUSION_CONTAINS_EMPTY))
        && DatumGetBool(*(*col_b).bv_values.add(INCLUSION_CONTAINS_EMPTY))
    {
        *(*col_a).bv_values.add(INCLUSION_CONTAINS_EMPTY) = BoolGetDatum(true);
    }

    /* Check if A includes elements that are not mergeable. */
    if DatumGetBool(*(*col_a).bv_values.add(INCLUSION_UNMERGEABLE)) {
        PG_RETURN_VOID!();
    }

    /* If B includes elements that are not mergeable, mark A similarly. */
    if DatumGetBool(*(*col_b).bv_values.add(INCLUSION_UNMERGEABLE)) {
        *(*col_a).bv_values.add(INCLUSION_UNMERGEABLE) = BoolGetDatum(true);
        PG_RETURN_VOID!();
    }

    /* Check if A and B are mergeable; if not, mark A unmergeable. */
    let mut finfo = inclusion_get_procinfo(bdesc, attno as uint16, PROCNUM_MERGEABLE, true);
    if !finfo.is_null()
        && !DatumGetBool(FunctionCall2Coll(
            finfo,
            colloid,
            *(*col_a).bv_values.add(INCLUSION_UNION),
            *(*col_b).bv_values.add(INCLUSION_UNION),
        ))
    {
        *(*col_a).bv_values.add(INCLUSION_UNMERGEABLE) = BoolGetDatum(true);
        PG_RETURN_VOID!();
    }

    /* Finally, merge B to A. */
    finfo = inclusion_get_procinfo(bdesc, attno as uint16, PROCNUM_MERGE, false);
    let mut result = FunctionCall2Coll(
        finfo,
        colloid,
        *(*col_a).bv_values.add(INCLUSION_UNION),
        *(*col_b).bv_values.add(INCLUSION_UNION),
    );
    if !(*attr).attbyval
        && DatumGetPointer(result) != DatumGetPointer(*(*col_a).bv_values.add(INCLUSION_UNION))
    {
        pfree(DatumGetPointer(*(*col_a).bv_values.add(INCLUSION_UNION)) as *mut c_void);

        if result == *(*col_b).bv_values.add(INCLUSION_UNION) {
            result = datumCopy(result, (*attr).attbyval, (*attr).attlen);
        }
    }
    *(*col_a).bv_values.add(INCLUSION_UNION) = result;

    PG_RETURN_VOID!();
}

/* --------------------------------------------------------------------------
 * inclusion_get_attr -- STUB for TupleDescCompactAttr(bdesc->bd_tupdesc,
 * attno - 1).
 *
 * brin_inclusion reads attbyval/attlen off the indexed column.  The tupdesc
 * decode is unported; here the caller is expected to have stashed an
 * `InclusionAttr` pointer in bd_disktdesc for the (single) column under test.
 * Real port: walk bdesc->bd_tupdesc with TupleDescCompactAttr.
 * -------------------------------------------------------------------------- */
unsafe fn inclusion_get_attr(bdesc: *mut BrinDesc, _attno: AttrNumber) -> *mut InclusionAttr {
    // TODO(pg-port): return TupleDescCompactAttr(bdesc->bd_tupdesc, attno - 1).
    let p = (*bdesc).bd_disktdesc as *mut InclusionAttr;
    Assert!(!p.is_null());
    p
}

/* --------------------------------------------------------------------------
 * inclusion_get_procinfo
 *
 * Cache and return the inclusion opclass support procedure for `procnum`, or
 * null if it does not exist.  If missing_ok is true and the proc isn't set up,
 * return null instead of raising an error.
 *
 * The cache LOOKUP (reading an already-populated FmgrInfo, or the
 * `extra_proc_missing` flag) is real.  The cache FILL path -- the
 * index_getprocid / index_getprocinfo / fmgr_info_copy that upstream uses to
 * populate a slot on miss -- depends on the unported relcache/index AM
 * machinery and is `unimplemented!()`.  Tests and callers must therefore
 * pre-populate extra_procinfos[procnum - PROCNUM_BASE] (fn_oid != InvalidOid)
 * for the procs they exercise, or set extra_proc_missing for ones they want
 * treated as absent.
 * -------------------------------------------------------------------------- */
unsafe fn inclusion_get_procinfo(
    bdesc: *mut BrinDesc,
    attno: uint16,
    procnum: uint16,
    missing_ok: bool,
) -> *mut FmgrInfo {
    let basenum = (procnum - PROCNUM_BASE) as usize;

    /*
     * We cache these in the opaque struct, to avoid repetitive syscache
     * lookups.
     */
    let opaque = (*(*(*bdesc).bd_info.add((attno - 1) as usize))).oi_opaque as *mut InclusionOpaque;

    /*
     * If we already searched for this proc and didn't find it, don't bother
     * searching again.
     */
    if (*opaque).extra_proc_missing[basenum] {
        return null_mut();
    }

    if (*opaque).extra_procinfos[basenum].fn_oid == InvalidOid {
        /*
         * Cache miss.  Upstream checks index_getprocid(bd_index, attno, procnum)
         * and, if valid, fmgr_info_copy's index_getprocinfo into the slot;
         * otherwise it either errors (missing_ok == false) or records the proc
         * as missing.  The relcache/index AM path is unported.
         */
        let _ = missing_ok;
        // TODO(pg-port): if RegProcedureIsValid(index_getprocid(bd_index, attno,
        //                procnum)) { fmgr_info_copy(slot, index_getprocinfo(..),
        //                bd_context) } else if !missing_ok { ereport(ERROR,
        //                "invalid opclass definition") } else { mark missing }.
        unimplemented!("inclusion_get_procinfo cache fill: index_getproc* relcache lookup unported");
    }

    &mut (*opaque).extra_procinfos[basenum]
}

/* --------------------------------------------------------------------------
 * inclusion_get_strategy_procinfo
 *
 * Cache and return the procedure of the given strategy, out of the per-column
 * InclusionOpaque stored in bdesc->bd_info[attno-1]->oi_opaque.  The data type
 * of the index is the left-hand side of the operator and `subtype` the right.
 *
 * The cache LOOKUP (reading an already-populated FmgrInfo) is real.  The cache
 * FILL path -- the SearchSysCache4(AMOPSTRATEGY, ...) + get_opcode +
 * fmgr_info_cxt that upstream uses on miss -- depends on the unported
 * syscache/opclass machinery and is `unimplemented!()`.  Mirrors
 * minmax_get_strategy_procinfo; tests pre-populate strategy_procinfos.
 * -------------------------------------------------------------------------- */
unsafe fn inclusion_get_strategy_procinfo(
    bdesc: *mut BrinDesc,
    attno: uint16,
    subtype: Oid,
    strategynum: uint16,
) -> *mut FmgrInfo {
    Assert!(strategynum >= 1 && strategynum <= RTMaxStrategyNumber);

    let opaque = (*(*(*bdesc).bd_info.add((attno - 1) as usize))).oi_opaque as *mut InclusionOpaque;

    /*
     * We cache the procedures for the last sub-type in the opaque struct, to
     * avoid repetitive syscache lookups.  If the sub-type is changed,
     * invalidate all the cached entries.
     */
    if (*opaque).cached_subtype != subtype {
        let mut i: uint16 = 1;
        while i <= RTMaxStrategyNumber {
            (*opaque).strategy_procinfos[(i - 1) as usize].fn_oid = InvalidOid;
            i += 1;
        }
        (*opaque).cached_subtype = subtype;
    }

    if (*opaque).strategy_procinfos[(strategynum - 1) as usize].fn_oid == InvalidOid {
        /*
         * Cache miss.  Upstream looks up the operator in the opclass' opfamily
         * via SearchSysCache4(AMOPSTRATEGY, opfamily, atttypid, subtype,
         * strategynum), takes its opcode, and fmgr_info_cxt's it into the slot.
         * The syscache / pg_amop / get_opcode / fmgr_info_cxt path is unported.
         */
        // TODO(pg-port): SearchSysCache4(AMOPSTRATEGY, ...) -> get_opcode ->
        //                fmgr_info_cxt(opcode, &slot, bdesc->bd_context).
        unimplemented!("inclusion_get_strategy_procinfo cache fill: pg_amop syscache lookup unported");
    }

    &mut (*opaque).strategy_procinfos[(strategynum - 1) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;
    use crate::utils::fmgr::PGFunction;

    /*
     * Fake R-Tree operators over a 1-D "interval" union represented as two i32
     * packed into the low/high halves of a Datum: union = (lo as u32) |
     * ((hi as u32) << 32).  The query is a single point (i32) for placement
     * strategies, or another packed interval for overlap/contains.  These are
     * just enough to exercise the consistent dispatch deterministically.
     */
    fn pack(lo: i32, hi: i32) -> Datum {
        ((lo as u32 as u64) | ((hi as u32 as u64) << 32)) as Datum
    }
    fn unpack(d: Datum) -> (i32, i32) {
        let v = d as u64;
        ((v & 0xffff_ffff) as u32 as i32, (v >> 32) as u32 as i32)
    }

    /* "over-right": union's right edge is at/right of point -> union &> point. */
    unsafe fn fake_overright(fcinfo: FunctionCallInfo) -> Datum {
        let (_lo, hi) = unpack(PG_GETARG_DATUM!(fcinfo, 0));
        let q = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(hi >= q)
    }

    /* "overlap": packed interval args overlap. */
    unsafe fn fake_overlap(fcinfo: FunctionCallInfo) -> Datum {
        let (alo, ahi) = unpack(PG_GETARG_DATUM!(fcinfo, 0));
        let (blo, bhi) = unpack(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(alo <= bhi && blo <= ahi)
    }

    /* "contains": union contains the query interval. */
    unsafe fn fake_contains(fcinfo: FunctionCallInfo) -> Datum {
        let (alo, ahi) = unpack(PG_GETARG_DATUM!(fcinfo, 0));
        let (blo, bhi) = unpack(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(alo <= blo && bhi <= ahi)
    }

    unsafe fn fake_flinfo(proc: PGFunction) -> FmgrInfo {
        let mut fi: FmgrInfo = MaybeUninit::zeroed().assume_init();
        fi.fn_addr = Some(proc);
        fi.fn_oid = 1; /* any valid-looking, non-Invalid oid -> cache treats as filled */
        fi.fn_nargs = 2;
        fi
    }

    /*
     * Build an InclusionOpaque whose relevant R-Tree strategy slots point at the
     * fake operators, with cached_subtype matching so the cache LOOKUP path is
     * taken (never the unimplemented FILL path).
     */
    unsafe fn make_opaque(subtype: Oid) -> Box<InclusionOpaque> {
        let mut op: InclusionOpaque = MaybeUninit::zeroed().assume_init();
        op.cached_subtype = subtype;
        op.strategy_procinfos[(RTOverRightStrategyNumber - 1) as usize] = fake_flinfo(fake_overright);
        op.strategy_procinfos[(RTOverlapStrategyNumber - 1) as usize] = fake_flinfo(fake_overlap);
        op.strategy_procinfos[(RTContainsStrategyNumber - 1) as usize] = fake_flinfo(fake_contains);
        Box::new(op)
    }

    unsafe fn make_bdesc(
        opaque: *mut InclusionOpaque,
        attr: *mut InclusionAttr,
    ) -> (Box<BrinDesc>, Box<BrinOpcInfo>, Box<*mut BrinOpcInfo>) {
        let mut opc: BrinOpcInfo = MaybeUninit::zeroed().assume_init();
        opc.oi_nstored = 3;
        opc.oi_regular_nulls = true;
        opc.oi_opaque = opaque as *mut c_void;
        let mut opc = Box::new(opc);

        let mut info_slot: Box<*mut BrinOpcInfo> = Box::new(&mut *opc as *mut BrinOpcInfo);

        let mut bd: BrinDesc = MaybeUninit::zeroed().assume_init();
        bd.bd_info = &mut *info_slot as *mut *mut BrinOpcInfo;
        bd.bd_disktdesc = attr as *mut c_void;
        let bd = Box::new(bd);
        (bd, opc, info_slot)
    }

    /* Build a fcinfo with `nargs` slots, all zero, collation Invalid. */
    unsafe fn make_fcinfo(nargs: usize) -> Vec<u8> {
        let sz = core::mem::size_of::<crate::utils::fmgr::FunctionCallInfoBaseData>()
            + nargs * core::mem::size_of::<crate::postgres::NullableDatum>();
        let mut buf = vec![0u8; sz];
        let fc = buf.as_mut_ptr() as FunctionCallInfo;
        (*fc).fncollation = InvalidOid;
        (*fc).nargs = nargs as i16;
        buf
    }

    unsafe fn set_arg(fc: FunctionCallInfo, n: usize, v: Datum) {
        (*(*fc).args.as_mut_ptr().add(n)).value = v;
        (*(*fc).args.as_mut_ptr().add(n)).isnull = false;
    }

    /* Run brin_inclusion_consistent for one packed-union summary and key. */
    unsafe fn run_consistent(
        bd: *mut BrinDesc,
        unionval: Datum,
        unmergeable: bool,
        contains_empty: bool,
        strategy: uint16,
        subtype: Oid,
        query: Datum,
    ) -> bool {
        let mut vals: [Datum; 3] = [unionval, BoolGetDatum(unmergeable), BoolGetDatum(contains_empty)];
        let mut col: BrinValues = MaybeUninit::zeroed().assume_init();
        col.bv_attno = 1;
        col.bv_allnulls = false;
        col.bv_values = vals.as_mut_ptr();

        let mut key: InclusionScanKey = MaybeUninit::zeroed().assume_init();
        key.sk_attno = 1;
        key.sk_strategy = strategy;
        key.sk_subtype = subtype;
        key.sk_argument = query;

        let mut buf = make_fcinfo(3);
        let fc = buf.as_mut_ptr() as FunctionCallInfo;
        set_arg(fc, 0, bd as Datum);
        set_arg(fc, 1, &mut col as *mut BrinValues as Datum);
        set_arg(fc, 2, &mut key as *mut InclusionScanKey as Datum);

        DatumGetBool(brin_inclusion_consistent(fc))
    }

    /*
     * Placement strategy RTLeftStrategyNumber is answered as
     * !(union over-right query).  With our fake_overright (hi >= q), "union is
     * strictly left of point" must be true iff hi < q.
     */
    #[test]
    fn consistent_left_negates_overright() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            let mut attr = Box::new(InclusionAttr { attlen: 8, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            let unionval = pack(10, 20);
            /* point below hi -> overright true -> left false */
            assert!(!run_consistent(&mut *bd, unionval, false, false, RTLeftStrategyNumber, InvalidOid, Int32GetDatum(15)));
            /* point above hi -> overright false -> left true */
            assert!(run_consistent(&mut *bd, unionval, false, false, RTLeftStrategyNumber, InvalidOid, Int32GetDatum(25)));
            /* point == hi -> hi>=q true -> overright true -> left false */
            assert!(!run_consistent(&mut *bd, unionval, false, false, RTLeftStrategyNumber, InvalidOid, Int32GetDatum(20)));
        }
    }

    /*
     * Unmergeable ranges are always consistent (the AM must re-check).  This is
     * the early-out at the top of consistent and does not consult any procinfo.
     */
    #[test]
    fn consistent_unmergeable_always_true() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            let mut attr = Box::new(InclusionAttr { attlen: 8, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            /* even a "left" key against an unmergeable range returns true */
            assert!(run_consistent(&mut *bd, pack(10, 20), true, false, RTLeftStrategyNumber, InvalidOid, Int32GetDatum(15)));
        }
    }

    /*
     * RTContainedByStrategyNumber: true if union overlaps query OR the range
     * contains an empty element.  Exercises both the overlap branch and the
     * "contains empty" fallback.
     */
    #[test]
    fn consistent_contained_by_uses_overlap_then_empty() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            let mut attr = Box::new(InclusionAttr { attlen: 8, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            let unionval = pack(10, 20);
            /* query [15,30] overlaps [10,20] -> true */
            assert!(run_consistent(&mut *bd, unionval, false, false, RTContainedByStrategyNumber, InvalidOid, pack(15, 30)));
            /* query [30,40] disjoint, no empty -> false */
            assert!(!run_consistent(&mut *bd, unionval, false, false, RTContainedByStrategyNumber, InvalidOid, pack(30, 40)));
            /* query [30,40] disjoint, but contains_empty -> true */
            assert!(run_consistent(&mut *bd, unionval, false, true, RTContainedByStrategyNumber, InvalidOid, pack(30, 40)));
        }
    }

    /*
     * RTContainsStrategyNumber returns the contains result directly.  union
     * [0,100] contains [10,20] but not [50,150].
     */
    #[test]
    fn consistent_contains_returns_operator() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            let mut attr = Box::new(InclusionAttr { attlen: 8, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            let unionval = pack(0, 100);
            assert!(run_consistent(&mut *bd, unionval, false, false, RTContainsStrategyNumber, InvalidOid, pack(10, 20)));
            assert!(!run_consistent(&mut *bd, unionval, false, false, RTContainsStrategyNumber, InvalidOid, pack(50, 150)));
        }
    }

    /*
     * add_value bookkeeping on the first (all-null -> first value) insertion:
     * uses no procinfo at all (the empty/contains/mergeable lookups are skipped
     * because `new` short-circuits after the empty check returns null... but our
     * opaque leaves PROCNUM_EMPTY missing).  Mark all extra procs missing so the
     * optional-proc paths are exercised as "absent", landing on the `new` early
     * return.  Asserts the union/flags were initialized and true was returned.
     */
    #[test]
    fn add_value_first_insert_initializes_union() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            /* mark all four optional/required extra procs as missing so the
             * empty test is skipped and we hit the `if new { return true }`. */
            for i in 0..INCLUSION_MAX_PROCNUMS {
                opaque.extra_proc_missing[i] = true;
            }
            let mut attr = Box::new(InclusionAttr { attlen: 8, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            let mut vals: [Datum; 3] = [0, 0, 0];
            let mut col: BrinValues = MaybeUninit::zeroed().assume_init();
            col.bv_attno = 1;
            col.bv_allnulls = true;
            col.bv_values = vals.as_mut_ptr();

            let mut buf = make_fcinfo(4);
            let fc = buf.as_mut_ptr() as FunctionCallInfo;
            set_arg(fc, 0, &mut *bd as *mut BrinDesc as Datum);
            set_arg(fc, 1, &mut col as *mut BrinValues as Datum);
            set_arg(fc, 2, pack(5, 5));
            set_arg(fc, 3, BoolGetDatum(false));

            let ret = DatumGetBool(brin_inclusion_add_value(fc));
            assert!(ret, "first insert must report updated");
            assert!(!col.bv_allnulls);
            assert_eq!(unpack(vals[INCLUSION_UNION]), (5, 5));
            assert!(!DatumGetBool(vals[INCLUSION_UNMERGEABLE]));
            assert!(!DatumGetBool(vals[INCLUSION_CONTAINS_EMPTY]));
        }
    }

    /*
     * add_value on an already-unmergeable range short-circuits and returns false
     * without touching any procinfo.
     */
    #[test]
    fn add_value_unmergeable_short_circuits() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            let mut attr = Box::new(InclusionAttr { attlen: 8, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            let mut vals: [Datum; 3] =
                [pack(0, 100), BoolGetDatum(true), BoolGetDatum(false)];
            let mut col: BrinValues = MaybeUninit::zeroed().assume_init();
            col.bv_attno = 1;
            col.bv_allnulls = false;
            col.bv_values = vals.as_mut_ptr();

            let mut buf = make_fcinfo(4);
            let fc = buf.as_mut_ptr() as FunctionCallInfo;
            set_arg(fc, 0, &mut *bd as *mut BrinDesc as Datum);
            set_arg(fc, 1, &mut col as *mut BrinValues as Datum);
            set_arg(fc, 2, pack(5, 5));
            set_arg(fc, 3, BoolGetDatum(false));

            let ret = DatumGetBool(brin_inclusion_add_value(fc));
            assert!(!ret, "unmergeable range must report not-updated");
            /* union unchanged */
            assert_eq!(unpack(vals[INCLUSION_UNION]), (0, 100));
        }
    }
}

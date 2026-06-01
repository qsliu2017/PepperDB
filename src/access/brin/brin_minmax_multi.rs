//! Translation of postgres/src/backend/access/brin/brin_minmax_multi.c
//!
//!     Implementation of Multi Min/Max opclass for BRIN
//!
//! Implements a variant of minmax opclass, where the summary is composed of
//! multiple smaller intervals. This allows us to handle outliers, which
//! usually make the simple minmax opclass inefficient.
//!
//! Consider for example page range with simple minmax interval [1000,2000],
//! and assume a new row gets inserted into the range with value 1000000.
//! Due to that the interval gets [1000,1000000]. I.e. the minmax interval
//! got 1000x wider and won't be useful to eliminate scan keys between 2001
//! and 1000000.
//!
//! With minmax-multi opclass, we may have [1000,2000] interval initially,
//! but after adding the new row we start tracking it as two interval:
//!
//!   [1000,2000] and [1000000,1000000]
//!
//! This allows us to still eliminate the page range when the scan keys hit
//! the gap between 2000 and 1000000, making it useful in cases when the
//! simple minmax opclass gets inefficient.
//!
//! The number of intervals tracked per page range is somewhat flexible.
//! What is restricted is the number of values per page range, and the limit
//! is currently 32 (see values_per_range reloption). Collapsed intervals
//! (with equal minimum and maximum value) are stored as a single value,
//! while regular intervals require two values.
//!
//! When the number of values gets too high (by adding new values to the
//! summary), we merge some of the intervals to free space for more values.
//! This is done in a greedy way - we simply pick the two closest intervals,
//! merge them, and repeat this until the number of values to store gets
//! sufficiently low (below 50% of maximum values), but that is mostly
//! arbitrary threshold and may be changed easily).
//!
//! To pick the closest intervals we use the "distance" support procedure,
//! which measures space between two ranges (i.e. the length of an interval).
//! The computed value may be an approximation - in the worst case we will
//! merge two ranges that are slightly less optimal at that step, but the
//! index should still produce correct results.
//!
//! The compactions (reducing the number of values) is fairly expensive, as
//! it requires calling the distance functions, sorting etc. So when building
//! the summary, we use a significantly larger buffer, and only enforce the
//! exact limit at the very end. This improves performance, and it also helps
//! with building better ranges (due to the greedy approach).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/brin/brin_minmax_multi.c
//!
//! PORTING NOTES
//! -------------
//! The core Ranges / ExpandedRange logic (init, serialize/deserialize, add,
//! compact, union, consistent) is translated 1:1.  Catalog/syscache plumbing
//! and the type-specific adt helpers used by the distance functions live in
//! other .c files; those are mirrored as TODO(pg-port) stubs at the end of the
//! file, matching the convention in brin_bloom.rs / brin_minmax.rs.

use crate::prelude::*;
use crate::access::attnum::AttrNumber;
use crate::storage::block::BlockNumber;

use core::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, int64, uint16, Size};
use crate::utils::fmgr::{
    FmgrInfo, FunctionCall1Coll, FunctionCall2Coll, FunctionCallInfo,
    OidOutputFunctionCall, OutputFunctionCall, fmgr_info, fmgr_info_copy, fmgr_info_cxt,
};
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::float::get_float8_infinity;
use crate::lib::stringinfo::StringInfoData;
use crate::lib::stringinfo::{initStringInfo, appendStringInfoChar};
use crate::port::qsort::{qsort_arg, pg_qsort};
use crate::port::bsearch_arg::bsearch_arg;
use libc::{memcpy, memmove, strlen};
use crate::access::tupmacs::{fetch_att, store_att_byval};
use crate::varatt::{SET_VARSIZE, VARSIZE_ANY, VARDATA_ANY};

/* Real homes for the catalog/syscache and type-specific helpers. */
use crate::access::brin::brin_internal::{BrinDesc, BrinOpcInfo, SizeofBrinOpcInfo};
use crate::access::brin::brin::{BrinOptions, BRIN_DEFAULT_PAGES_PER_RANGE};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::access::common::scankey::{ScanKey, SK_ISNULL};
use crate::access::common::tupdesc::TupleDescAttr;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, MaxHeapTuplesPerPage};
use crate::access::index::indexam::{index_getprocid, index_getprocinfo};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerCompare, ItemPointerGetBlockNumberNoCheck,
    ItemPointerGetOffsetNumberNoCheck,
};
use crate::utils::cache::lsyscache::{get_typbyval, get_typlen, get_opcode, getTypeOutputInfo};
use crate::utils::cache::typcache::lookup_type_cache;
use crate::utils::cache::syscache::{
    SearchSysCache4, ReleaseSysCache, SysCacheGetAttrNotNull,
};
use crate::utils::builtins::{cstring_to_text, cstring_to_text_with_len};
use crate::utils::adt::arrayfuncs::{ArrayBuildState, accumArrayResult, makeArrayResult};
use crate::utils::adt::numeric::{numeric_le, numeric_sub, numeric_float8};
use crate::utils::adt::uuid::{pg_uuid_t, DatumGetUUIDP, uuid_le, UUID_LEN};
use crate::utils::adt::mac::{macaddr, DatumGetMacaddrP};
use crate::utils::adt::mac8::{macaddr8, DatumGetMacaddr8P};
use crate::utils::adt::network::inet;
use crate::utils::adt::date::{
    Interval, Timestamp, TimeADT, TimeTzADT, DateADT, USECS_PER_DAY, USECS_PER_SEC,
    DatumGetDateADT, DatumGetTimeADT, DatumGetTimeTzADTP,
};
use crate::utils::adt::pg_lsn::DatumGetLSN;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::utils::rel::Relation;
use crate::catalog::pg_type_d::{ANYARRAYOID, TEXTOID, PG_BRIN_MINMAX_MULTI_SUMMARYOID};
use crate::{
    PG_DETOAST_DATUM, PG_GETARG_DATUM, PG_GETARG_FLOAT4, PG_GETARG_FLOAT8, PG_GETARG_INT16,
    PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_POINTER, PG_GET_COLLATION, PG_GET_OPCLASS_OPTIONS,
    PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_DATUM, PG_RETURN_FLOAT8, PG_RETURN_POINTER,
    PG_RETURN_VOID, DirectFunctionCall1, DirectFunctionCall2, FunctionCall1,
};

/*
 * pg_amop catcache id and amopopr attribute number (utils/syscache.h,
 * catalog/pg_amop.h).  These catalog identifiers are not yet centralized in
 * this port, so mirror the constants locally.
 */
pub const AMOPSTRATEGY: c_int = 4;
pub const Anum_pg_amop_amopopr: AttrNumber = 7;

/*
 * BrinGetPagesPerRange(relation) - mirrors the macro in access/brin.h.  Reads
 * pagesPerRange from the parsed reloptions, falling back to the default.
 */
#[inline]
unsafe fn BrinGetPagesPerRange(relation: Relation) -> BlockNumber {
    if !(*relation).rd_options.is_null() {
        (*((*relation).rd_options as *mut BrinOptions)).pagesPerRange
    } else {
        BRIN_DEFAULT_PAGES_PER_RANGE
    }
}

/*
 * inet access helpers (utils/inet.h).  The canonical versions in
 * utils/adt/network.rs are module-private, so mirror them here.
 */
#[inline]
unsafe fn ip_family(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).family
}
#[inline]
unsafe fn ip_bits(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).bits
}
#[inline]
unsafe fn ip_addr(inetptr: *const inet) -> *mut u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct)).ipaddr.as_mut_ptr()
}
#[inline]
unsafe fn ip_addrsize(inetptr: *const inet) -> c_int {
    if ip_family(inetptr) == PGSQL_AF_INET {
        4
    } else {
        16
    }
}
const PGSQL_AF_INET: u8 = 2;
#[repr(C)]
struct inet_struct {
    family: u8,
    bits: u8,
    ipaddr: [u8; 16],
}

/*
 * local_relopts plumbing (access/reloptions.h).  The reloptions framework is
 * not ported yet, so these are stubs matching the convention in brin_bloom.rs.
 */
#[repr(C)]
pub struct local_relopts {
    _opaque: [u8; 0],
}
unsafe fn init_local_reloptions(_relopts: *mut local_relopts, _relopt_struct_size: Size) {
    unimplemented!() // TODO(pg-port): access/reloptions.h
}
unsafe fn add_local_int_reloption(
    _relopts: *mut local_relopts,
    _name: *const c_char,
    _desc: *const c_char,
    _default_val: c_int,
    _min_val: c_int,
    _max_val: c_int,
    _offset: c_int,
) {
    unimplemented!() // TODO(pg-port): access/reloptions.h
}
unsafe fn byteasend(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): utils/varlena.h
}

/*
 * appendStringInfo variadic shims (lib/stringinfo.h).  The C source calls the
 * variadic appendStringInfo; mirror the fixed arities used here.
 */
unsafe fn appendStringInfo1(_str: *mut StringInfoData, _fmt: *const c_char, _a: *mut c_char) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.h
}
unsafe fn appendStringInfo2(
    _str: *mut StringInfoData,
    _fmt: *const c_char,
    _a: *mut c_char,
    _b: *mut c_char,
) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.h
}
unsafe fn appendStringInfo3(
    _str: *mut StringInfoData,
    _fmt: *const c_char,
    _a: c_int,
    _b: c_int,
    _c: c_int,
) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.h
}

/*
 * PG_GETARG_* shims for types whose getarg macros are module-private in this
 * port (utils/inet.h).  Expand to DatumGet*P(PG_GETARG_DATUM(n)).
 */
macro_rules! PG_GETARG_MACADDR_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetMacaddrP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_MACADDR8_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetMacaddr8P(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_INET_PP {
    ($fcinfo:expr, $n:expr) => {
        DatumGetInetPP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/*
 * Date/time/lsn getarg shims (utils/date.h, utils/timestamp.h, utils/pg_lsn.h).
 * Their canonical macros are module-private in this port; expand to the
 * importable DatumGet* helpers.
 */
macro_rules! PG_GETARG_DATEADT {
    ($fcinfo:expr, $n:expr) => {
        DatumGetDateADT(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMEADT {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTimeADT(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMETZADT_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTimeTzADTP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMESTAMP {
    ($fcinfo:expr, $n:expr) => {
        DatumGetInt64(PG_GETARG_DATUM!($fcinfo, $n)) as Timestamp
    };
}
macro_rules! PG_GETARG_INTERVAL_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Interval
    };
}
macro_rules! PG_GETARG_LSN {
    ($fcinfo:expr, $n:expr) => {
        DatumGetLSN(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/* DatumGetInetPP (utils/inet.h) -- network.rs's copy is module-private. */
#[inline]
unsafe fn DatumGetInetPP(x: Datum) -> *mut inet {
    PG_DETOAST_DATUM!(x) as *mut inet
}

/*
 * BrinValues -- one per indexed column in an in-memory BRIN tuple.
 * From access/brin_tuple.h (brin_tuple is not wired into the brin module in
 * this port, so mirror the struct locally; the serialize callback is opaque).
 */
#[repr(C)]
pub struct BrinValues {
    pub bv_attno: AttrNumber,      /* index attribute number */
    pub bv_hasnulls: bool,         /* are there any nulls in the page range? */
    pub bv_allnulls: bool,         /* are all values nulls in the page range? */
    pub bv_values: *mut Datum,     /* current accumulated values */
    pub bv_mem_value: Datum,       /* expanded accumulated values */
    pub bv_context: *mut c_void,   /* MemoryContext */
    pub bv_serialize: *mut c_void, /* brin_serialize_callback_type */
}

pub const InvalidOid: Oid = 0;

/*
 * Additional SQL level support functions
 *
 * Procedure numbers must not use values reserved for BRIN itself; see
 * brin_internal.h.
 */
pub const MINMAX_MAX_PROCNUMS: usize = 1; /* maximum support procs we need */
pub const PROCNUM_DISTANCE: uint16 = 11; /* required, distance between values */

/*
 * Subtract this from procnum to obtain index in MinmaxMultiOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
pub const PROCNUM_BASE: uint16 = 11;

/*
 * Sizing the insert buffer - we use 10x the number of values specified
 * in the reloption, but we cap it to 8192 not to get too large. When
 * the buffer gets full, we reduce the number of values by half.
 */
pub const MINMAX_BUFFER_FACTOR: c_int = 10;
pub const MINMAX_BUFFER_MIN: c_int = 256;
pub const MINMAX_BUFFER_MAX: c_int = 8192;
pub const MINMAX_BUFFER_LOAD_FACTOR: f64 = 0.5;

/* B-tree strategy numbers, from access/stratnum.h. */
pub const BTLessStrategyNumber: uint16 = 1;
pub const BTLessEqualStrategyNumber: uint16 = 2;
pub const BTEqualStrategyNumber: uint16 = 3;
pub const BTGreaterEqualStrategyNumber: uint16 = 4;
pub const BTGreaterStrategyNumber: uint16 = 5;
pub const BTMaxStrategyNumber: uint16 = 5;

#[repr(C)]
pub struct MinmaxMultiOpaque {
    pub extra_procinfos: [FmgrInfo; MINMAX_MAX_PROCNUMS],
    pub cached_subtype: Oid,
    pub strategy_procinfos: [FmgrInfo; BTMaxStrategyNumber as usize],
}

/*
 * Storage type for BRIN's minmax reloptions
 */
#[repr(C)]
pub struct MinMaxMultiOptions {
    pub vl_len_: int32,         /* varlena header (do not touch directly!) */
    pub valuesPerRange: c_int,  /* number of values per range */
}

pub const MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE: c_int = 32;

#[inline]
unsafe fn MinMaxMultiGetValuesPerRange(opts: *const MinMaxMultiOptions) -> c_int {
    if !opts.is_null() && (*opts).valuesPerRange != 0 {
        (*opts).valuesPerRange
    } else {
        MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE
    }
}

/* SAMESIGN(a,b) -- kept inline at call sites where needed; unused here. */

/*
 * The summary of minmax-multi indexes has two representations - Ranges for
 * convenient processing, and SerializedRanges for storage in bytea value.
 *
 * The Ranges struct stores the boundary values in a single array, but we
 * treat regular and single-point ranges differently to save space. For
 * regular ranges (with different boundary values) we have to store both
 * the lower and upper bound of the range, while for "single-point ranges"
 * we only need to store a single value.
 *
 * The 'values' array stores boundary values for regular ranges first (there
 * are 2*nranges values to store), and then the nvalues boundary values for
 * single-point ranges. That is, we have (2*nranges + nvalues) boundary
 * values in the array.
 *
 * +-------------------------+----------------------------------+
 * | ranges (2 * nranges of) | single point values (nvalues of) |
 * +-------------------------+----------------------------------+
 *
 * This allows us to quickly add new values, and store outliers without
 * having to widen any of the existing range values.
 *
 * 'nsorted' denotes how many of 'nvalues' in the values[] array are sorted.
 * When nsorted == nvalues, all single point values are sorted.
 *
 * We never store more than maxvalues values (as set by values_per_range
 * reloption). If needed we merge some of the ranges.
 *
 * To minimize palloc overhead, we always allocate the full array with
 * space for maxvalues elements. This should be fine as long as the
 * maxvalues is reasonably small (64 seems fine), which is the case
 * thanks to values_per_range reloption being limited to 256.
 */
#[repr(C)]
pub struct Ranges {
    /* Cache information that we need quite often. */
    pub typid: Oid,
    pub colloid: Oid,
    pub attno: AttrNumber,
    pub cmp: *mut FmgrInfo,

    /* (2*nranges + nvalues) <= maxvalues */
    pub nranges: c_int,   /* number of ranges in the values[] array */
    pub nsorted: c_int,   /* number of nvalues which are sorted */
    pub nvalues: c_int,   /* number of point values in values[] array */
    pub maxvalues: c_int, /* number of elements in the values[] array */

    /*
     * We simply add the values into a large buffer, without any expensive
     * steps (sorting, deduplication, ...). The buffer is a multiple of the
     * target number of values, so the compaction happens less often,
     * amortizing the costs. We keep the actual target and compact to the
     * requested number of values at the very end, before serializing to
     * on-disk representation.
     */
    /* requested number of values */
    pub target_maxvalues: c_int,

    /* values stored for this range - either raw values, or ranges */
    pub values: [Datum; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * On-disk the summary is stored as a bytea value, with a simple header
 * with basic metadata, followed by the boundary values. It has a varlena
 * header, so can be treated as varlena directly.
 *
 * See brin_range_serialize/brin_range_deserialize for serialization details.
 */
#[repr(C)]
pub struct SerializedRanges {
    /* varlena header (do not touch directly!) */
    pub vl_len_: int32,

    /* type of values stored in the data array */
    pub typid: Oid,

    /* (2*nranges + nvalues) <= maxvalues */
    pub nranges: c_int,   /* number of ranges in the array (stored) */
    pub nvalues: c_int,   /* number of values in the data array (all) */
    pub maxvalues: c_int, /* maximum number of values (reloption) */

    /* contains the actual data */
    pub data: [c_char; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * Used to represent ranges expanded to make merging and combining easier.
 *
 * Each expanded range is essentially an interval, represented by min/max
 * values, along with a flag whether it's a collapsed range (in which case
 * the min and max values are equal). We have the flag to handle by-ref
 * data types - we can't simply compare the datums, and this saves some
 * calls to the type-specific comparator function.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExpandedRange {
    pub minval: Datum,   /* lower boundary */
    pub maxval: Datum,   /* upper boundary */
    pub collapsed: bool, /* true if minval==maxval */
}

/*
 * Represents a distance between two ranges (identified by index into
 * an array of extended ranges).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DistanceValue {
    pub index: c_int,
    pub value: f64,
}

/* Cache for support and strategy procedures. */

#[repr(C)]
pub struct compare_context {
    pub cmpFn: *mut FmgrInfo,
    pub colloid: Oid,
}

/*
 * Check that the order of the array values is correct, using the cmp
 * function (which should be BTLessStrategyNumber).
 */
#[cfg(debug_assertions)]
unsafe fn AssertArrayOrder(cmp: *mut FmgrInfo, colloid: Oid, values: *mut Datum, nvalues: c_int) {
    let mut i: c_int = 0;
    while i < (nvalues - 1) {
        let lt = FunctionCall2Coll(cmp, colloid, *values.add(i as usize), *values.add((i + 1) as usize));
        Assert!(DatumGetBool(lt));
        i += 1;
    }
}

/*
 * Comprehensive check of the Ranges structure.
 */
unsafe fn AssertCheckRanges(ranges: *mut Ranges, cmpFn: *mut FmgrInfo, colloid: Oid) {
    #[cfg(debug_assertions)]
    {
        /* some basic sanity checks */
        Assert!((*ranges).nranges >= 0);
        Assert!((*ranges).nsorted >= 0);
        Assert!((*ranges).nvalues >= (*ranges).nsorted);
        Assert!((*ranges).maxvalues >= 2 * (*ranges).nranges + (*ranges).nvalues);
        Assert!((*ranges).typid != InvalidOid);

        let values = (*ranges).values.as_ptr() as *mut Datum;

        /*
         * First the ranges - there are 2*nranges boundary values, and the values
         * have to be strictly ordered (equal values would mean the range is
         * collapsed, and should be stored as a point). This also guarantees that
         * the ranges do not overlap.
         */
        AssertArrayOrder(cmpFn, colloid, values, 2 * (*ranges).nranges);

        /* then the single-point ranges (with nvalues boundary values ) */
        AssertArrayOrder(cmpFn, colloid, values.add((2 * (*ranges).nranges) as usize), (*ranges).nsorted);

        /*
         * Check that none of the values are not covered by ranges (both sorted
         * and unsorted)
         */
        if (*ranges).nranges > 0 {
            let mut i: c_int = 0;
            while i < (*ranges).nvalues {
                let mut compar: Datum;
                let mut start: c_int;
                let mut end: c_int;
                let mut minvalue: Datum = *values.add(0);
                let mut maxvalue: Datum = *values.add((2 * (*ranges).nranges - 1) as usize);
                let value: Datum = *values.add((2 * (*ranges).nranges + i) as usize);

                compar = FunctionCall2Coll(cmpFn, colloid, value, minvalue);

                /*
                 * If the value is smaller than the lower bound in the first range
                 * then it cannot possibly be in any of the ranges.
                 */
                if DatumGetBool(compar) {
                    i += 1;
                    continue;
                }

                compar = FunctionCall2Coll(cmpFn, colloid, maxvalue, value);

                /*
                 * Likewise, if the value is larger than the upper bound of the
                 * final range, then it cannot possibly be inside any of the
                 * ranges.
                 */
                if DatumGetBool(compar) {
                    i += 1;
                    continue;
                }

                /* bsearch the ranges to see if 'value' fits within any of them */
                start = 0; /* first range */
                end = (*ranges).nranges - 1; /* last range */
                loop {
                    let midpoint: c_int = (start + end) / 2;

                    /* this means we ran out of ranges in the last step */
                    if start > end {
                        break;
                    }

                    /* copy the min/max values from the ranges */
                    minvalue = *values.add((2 * midpoint) as usize);
                    maxvalue = *values.add((2 * midpoint + 1) as usize);

                    /*
                     * Is the value smaller than the minval? If yes, we'll recurse
                     * to the left side of range array.
                     */
                    compar = FunctionCall2Coll(cmpFn, colloid, value, minvalue);

                    /* smaller than the smallest value in this range */
                    if DatumGetBool(compar) {
                        end = midpoint - 1;
                        continue;
                    }

                    /*
                     * Is the value greater than the minval? If yes, we'll recurse
                     * to the right side of range array.
                     */
                    compar = FunctionCall2Coll(cmpFn, colloid, maxvalue, value);

                    /* larger than the largest value in this range */
                    if DatumGetBool(compar) {
                        start = midpoint + 1;
                        continue;
                    }

                    /* hey, we found a matching range */
                    Assert!(false);
                }

                i += 1;
            }
        }

        /* and values in the unsorted part must not be in the sorted part */
        if (*ranges).nsorted > 0 {
            let mut cxt = compare_context {
                colloid: (*ranges).colloid,
                cmpFn: (*ranges).cmp,
            };

            let mut i: c_int = (*ranges).nsorted;
            while i < (*ranges).nvalues {
                let value: Datum = *values.add((2 * (*ranges).nranges + i) as usize);

                Assert!(bsearch_arg(
                    &value as *const Datum as *const c_void,
                    values.add((2 * (*ranges).nranges) as usize) as *const c_void,
                    (*ranges).nsorted as Size,
                    core::mem::size_of::<Datum>(),
                    compare_values_c,
                    &mut cxt as *mut compare_context as *mut c_void,
                )
                .is_null());
                i += 1;
            }
        }
    }
    let _ = (ranges, cmpFn, colloid);
}

/*
 * Check that the expanded ranges (built when reducing the number of ranges
 * by combining some of them) are correctly sorted and do not overlap.
 */
unsafe fn AssertCheckExpandedRanges(
    bdesc: *mut BrinDesc,
    colloid: Oid,
    attno: AttrNumber,
    attr: Form_pg_attribute,
    ranges: *mut ExpandedRange,
    nranges: c_int,
) {
    #[cfg(debug_assertions)]
    {
        let eq = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTEqualStrategyNumber);
        let lt = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);

        /*
         * Each range independently should be valid, i.e. that for the boundary
         * values (lower <= upper).
         */
        let mut i: c_int = 0;
        while i < nranges {
            let r: Datum;
            let minval = (*ranges.add(i as usize)).minval;
            let maxval = (*ranges.add(i as usize)).maxval;

            if (*ranges.add(i as usize)).collapsed {
                /* collapsed: minval == maxval */
                r = FunctionCall2Coll(eq, colloid, minval, maxval);
            } else {
                /* non-collapsed: minval < maxval */
                r = FunctionCall2Coll(lt, colloid, minval, maxval);
            }

            Assert!(DatumGetBool(r));
            i += 1;
        }

        /*
         * And the ranges should be ordered and must not overlap, i.e. upper <
         * lower for boundaries of consecutive ranges.
         */
        let mut i: c_int = 0;
        while i < nranges - 1 {
            let maxval = (*ranges.add(i as usize)).maxval;
            let minval = (*ranges.add((i + 1) as usize)).minval;

            let r = FunctionCall2Coll(lt, colloid, maxval, minval);

            Assert!(DatumGetBool(r));
            i += 1;
        }
    }
    let _ = (bdesc, colloid, attno, attr, ranges, nranges);
}

/*
 * minmax_multi_init
 *      Initialize the deserialized range list, allocate all the memory.
 *
 * This is only in-memory representation of the ranges, so we allocate
 * enough space for the maximum number of values (so as not to have to do
 * repallocs as the ranges grow).
 */
unsafe fn minmax_multi_init(maxvalues: c_int) -> *mut Ranges {
    let len: Size;
    let ranges: *mut Ranges;

    Assert!(maxvalues > 0);

    len = core::mem::offset_of!(Ranges, values) /* fixed header */
        + (maxvalues as usize) * core::mem::size_of::<Datum>(); /* Datum values */

    ranges = palloc0(len) as *mut Ranges;

    (*ranges).maxvalues = maxvalues;

    ranges
}

/*
 * range_deduplicate_values
 *      Deduplicate the part with values in the simple points.
 *
 * This is meant to be a cheaper way of reducing the size of the ranges. It
 * does not touch the ranges, and only sorts the other values - it does not
 * call the distance functions, which may be quite expensive, etc.
 *
 * We do know the values are not duplicate with the ranges, because we check
 * that before adding a new value. Same for the sorted part of values.
 */
unsafe fn range_deduplicate_values(range: *mut Ranges) {
    let mut i: c_int;
    let mut n: c_int;
    let start: c_int;
    let mut cxt: compare_context;

    /*
     * If there are no unsorted values, we're done (this probably can't
     * happen, as we're adding values to unsorted part).
     */
    if (*range).nsorted == (*range).nvalues {
        return;
    }

    /* sort the values */
    cxt = compare_context {
        colloid: (*range).colloid,
        cmpFn: (*range).cmp,
    };

    /* the values start right after the ranges (which are always sorted) */
    start = 2 * (*range).nranges;

    let values = (*range).values.as_ptr() as *mut Datum;

    /*
     * XXX This might do a merge sort, to leverage that the first part of the
     * array is already sorted. If the sorted part is large, it might be quite
     * a bit faster.
     */
    qsort_arg(
        values.add(start as usize) as *mut c_void,
        (*range).nvalues as usize,
        core::mem::size_of::<Datum>(),
        compare_values,
        &mut cxt as *mut compare_context as *mut c_void,
    );

    n = 1;
    i = 1;
    while i < (*range).nvalues {
        /* same as preceding value, so store it */
        if compare_values(
            values.add((start + i - 1) as usize) as *const c_void,
            values.add((start + i) as usize) as *const c_void,
            &mut cxt as *mut compare_context as *mut c_void,
        ) == 0
        {
            i += 1;
            continue;
        }

        *values.add((start + n) as usize) = *values.add((start + i) as usize);

        n += 1;
        i += 1;
    }

    /* now all the values are sorted */
    (*range).nvalues = n;
    (*range).nsorted = n;

    AssertCheckRanges(range, (*range).cmp, (*range).colloid);
}

/*
 * compare_expanded_ranges
 *    Compare the expanded ranges - first by minimum, then by maximum.
 *
 * We do guarantee that ranges in a single Ranges object do not overlap, so it
 * may seem strange that we don't order just by minimum. But when merging two
 * Ranges (which happens in the union function), the ranges may in fact
 * overlap. So we do compare both.
 */
unsafe fn compare_expanded_ranges(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let ra = a as *const ExpandedRange;
    let rb = b as *const ExpandedRange;
    let mut r: Datum;

    let cxt = arg as *mut compare_context;

    /* first compare minvals */
    r = FunctionCall2Coll((*cxt).cmpFn, (*cxt).colloid, (*ra).minval, (*rb).minval);

    if DatumGetBool(r) {
        return -1;
    }

    r = FunctionCall2Coll((*cxt).cmpFn, (*cxt).colloid, (*rb).minval, (*ra).minval);

    if DatumGetBool(r) {
        return 1;
    }

    /* then compare maxvals */
    r = FunctionCall2Coll((*cxt).cmpFn, (*cxt).colloid, (*ra).maxval, (*rb).maxval);

    if DatumGetBool(r) {
        return -1;
    }

    r = FunctionCall2Coll((*cxt).cmpFn, (*cxt).colloid, (*rb).maxval, (*ra).maxval);

    if DatumGetBool(r) {
        return 1;
    }

    0
}

/*
 * compare_values
 *    Compare the values.
 */
unsafe fn compare_values(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let da = a as *const Datum;
    let db = b as *const Datum;
    let mut r: Datum;

    let cxt = arg as *mut compare_context;

    r = FunctionCall2Coll((*cxt).cmpFn, (*cxt).colloid, *da, *db);

    if DatumGetBool(r) {
        return -1;
    }

    r = FunctionCall2Coll((*cxt).cmpFn, (*cxt).colloid, *db, *da);

    if DatumGetBool(r) {
        return 1;
    }

    0
}

/*
 * C-ABI wrapper for compare_values, needed where the comparator is passed to
 * bsearch_arg (whose comparator type is `extern "C"` in this port).
 */
unsafe extern "C" fn compare_values_c(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    compare_values(a, b, arg)
}

/*
 * brin_range_serialize
 *    Serialize the in-memory representation into a compact varlena value.
 *
 * Simply copy the header and then also the individual values, as stored
 * in the in-memory value array.
 */
unsafe fn brin_range_serialize(range: *mut Ranges) -> *mut SerializedRanges {
    let mut len: Size;
    let nvalues: c_int;
    let serialized: *mut SerializedRanges;
    let typid: Oid;
    let typlen: c_int;
    let typbyval: bool;

    let mut ptr: *mut c_char;

    /* simple sanity checks */
    Assert!((*range).nranges >= 0);
    Assert!((*range).nsorted >= 0);
    Assert!((*range).nvalues >= 0);
    Assert!((*range).maxvalues > 0);
    Assert!((*range).target_maxvalues > 0);

    /* at this point the range should be compacted to the target size */
    Assert!(2 * (*range).nranges + (*range).nvalues <= (*range).target_maxvalues);

    Assert!((*range).target_maxvalues <= (*range).maxvalues);

    /* range boundaries are always sorted */
    Assert!((*range).nvalues >= (*range).nsorted);

    /* deduplicate values, if there's unsorted part */
    range_deduplicate_values(range);

    /* see how many Datum values we actually have */
    nvalues = 2 * (*range).nranges + (*range).nvalues;

    typid = (*range).typid;
    typbyval = get_typbyval(typid);
    typlen = get_typlen(typid) as c_int;

    let values = (*range).values.as_ptr() as *mut Datum;

    /* header is always needed */
    len = core::mem::offset_of!(SerializedRanges, data);

    /*
     * The space needed depends on data type - for fixed-length data types
     * (by-value and some by-reference) it's pretty simple, just multiply
     * (attlen * nvalues) and we're done. For variable-length by-reference
     * types we need to actually walk all the values and sum the lengths.
     */
    if typlen == -1 {
        /* varlena */
        let mut i: c_int = 0;
        while i < nvalues {
            len += VARSIZE_ANY(*values.add(i as usize) as *const c_char) as Size;
            i += 1;
        }
    } else if typlen == -2 {
        /* cstring */
        let mut i: c_int = 0;
        while i < nvalues {
            /* don't forget to include the null terminator ;-) */
            len += strlen(DatumGetCString(*values.add(i as usize))) + 1;
            i += 1;
        }
    } else {
        /* fixed-length types (even by-reference) */
        Assert!(typlen > 0);
        len += (nvalues as Size) * (typlen as Size);
    }

    /*
     * Allocate the serialized object, copy the basic information. The
     * serialized object is a varlena, so update the header.
     */
    serialized = palloc0(len) as *mut SerializedRanges;
    SET_VARSIZE(serialized as *mut c_char, len as int32);

    (*serialized).typid = typid;
    (*serialized).nranges = (*range).nranges;
    (*serialized).nvalues = (*range).nvalues;
    (*serialized).maxvalues = (*range).target_maxvalues;

    /*
     * And now copy also the boundary values (like the length calculation this
     * depends on the particular data type).
     */
    ptr = (*serialized).data.as_ptr() as *mut c_char; /* start of the serialized data */

    let mut i: c_int = 0;
    while i < nvalues {
        if typbyval {
            /* simple by-value data types */
            let mut tmp: Datum = 0;

            /*
             * For byval types, we need to copy just the significant bytes -
             * we can't use memcpy directly, as that assumes little-endian
             * behavior.  store_att_byval does almost what we need, but it
             * requires a properly aligned buffer - the output buffer does not
             * guarantee that. So we simply use a local Datum variable (which
             * guarantees proper alignment), and then copy the value from it.
             */
            store_att_byval(&mut tmp as *mut Datum as *mut c_void, *values.add(i as usize), typlen);

            memcpy(ptr as *mut c_void, &tmp as *const Datum as *const c_void, typlen as Size);
            ptr = ptr.add(typlen as usize);
        } else if typlen > 0 {
            /* fixed-length by-ref types */
            memcpy(ptr as *mut c_void, DatumGetPointer(*values.add(i as usize)) as *const c_void, typlen as Size);
            ptr = ptr.add(typlen as usize);
        } else if typlen == -1 {
            /* varlena */
            let tmp: c_int = VARSIZE_ANY(DatumGetPointer(*values.add(i as usize)) as *const c_char) as c_int;

            memcpy(ptr as *mut c_void, DatumGetPointer(*values.add(i as usize)) as *const c_void, tmp as Size);
            ptr = ptr.add(tmp as usize);
        } else if typlen == -2 {
            /* cstring */
            let tmp: Size = strlen(DatumGetCString(*values.add(i as usize))) + 1;

            memcpy(ptr as *mut c_void, DatumGetCString(*values.add(i as usize)) as *const c_void, tmp);
            ptr = ptr.add(tmp as usize);
        }

        /* make sure we haven't overflown the buffer end */
        Assert!(ptr <= (serialized as *mut c_char).add(len as usize));
        i += 1;
    }

    /* exact size */
    Assert!(ptr == (serialized as *mut c_char).add(len as usize));

    serialized
}

/*
 * brin_range_deserialize
 *    Serialize the in-memory representation into a compact varlena value.
 *
 * Simply copy the header and then also the individual values, as stored
 * in the in-memory value array.
 */
unsafe fn brin_range_deserialize(maxvalues: c_int, serialized: *mut SerializedRanges) -> *mut Ranges {
    let mut i: c_int;
    let nvalues: c_int;
    let mut ptr: *mut c_char;
    let mut dataptr: *mut c_char;
    let typbyval: bool;
    let typlen: c_int;
    let mut datalen: Size;

    let range: *mut Ranges;

    Assert!((*serialized).nranges >= 0);
    Assert!((*serialized).nvalues >= 0);
    Assert!((*serialized).maxvalues > 0);

    nvalues = 2 * (*serialized).nranges + (*serialized).nvalues;

    Assert!(nvalues <= (*serialized).maxvalues);
    Assert!((*serialized).maxvalues <= maxvalues);

    range = minmax_multi_init(maxvalues);

    /* copy the header info */
    (*range).nranges = (*serialized).nranges;
    (*range).nvalues = (*serialized).nvalues;
    (*range).nsorted = (*serialized).nvalues;
    (*range).maxvalues = maxvalues;
    (*range).target_maxvalues = (*serialized).maxvalues;

    (*range).typid = (*serialized).typid;

    typbyval = get_typbyval((*serialized).typid);
    typlen = get_typlen((*serialized).typid) as c_int;

    let values = (*range).values.as_ptr() as *mut Datum;

    /*
     * And now deconstruct the values into Datum array. We have to copy the
     * data because the serialized representation ignores alignment, and we
     * don't want to rely on it being kept around anyway.
     */
    ptr = (*serialized).data.as_ptr() as *mut c_char;

    /*
     * We don't want to allocate many pieces, so we just allocate everything
     * in one chunk. How much space will we need?
     *
     * XXX We don't need to copy simple by-value data types.
     */
    datalen = 0;
    dataptr = null_mut();
    i = 0;
    while (i < nvalues) && !typbyval {
        if typlen > 0 {
            /* fixed-length by-ref types */
            datalen += MAXALIGN(typlen as Size);
        } else if typlen == -1 {
            /* varlena */
            datalen += MAXALIGN(VARSIZE_ANY(ptr) as Size);
            ptr = ptr.add(VARSIZE_ANY(ptr) as usize);
        } else if typlen == -2 {
            /* cstring */
            let slen: Size = strlen(ptr) + 1;

            datalen += MAXALIGN(slen);
            ptr = ptr.add(slen as usize);
        }
        i += 1;
    }

    if datalen > 0 {
        dataptr = palloc(datalen) as *mut c_char;
    }

    /*
     * Restore the source pointer (might have been modified when calculating
     * the space we need to allocate).
     */
    ptr = (*serialized).data.as_ptr() as *mut c_char;

    i = 0;
    while i < nvalues {
        if typbyval {
            /* simple by-value data types */
            let mut v: Datum = 0;

            memcpy(&mut v as *mut Datum as *mut c_void, ptr as *const c_void, typlen as Size);

            *values.add(i as usize) = fetch_att(&v as *const Datum as *const c_void, true, typlen);
            ptr = ptr.add(typlen as usize);
        } else if typlen > 0 {
            /* fixed-length by-ref types */
            *values.add(i as usize) = PointerGetDatum(dataptr as *const c_void);

            memcpy(dataptr as *mut c_void, ptr as *const c_void, typlen as Size);
            dataptr = dataptr.add(MAXALIGN(typlen as Size) as usize);

            ptr = ptr.add(typlen as usize);
        } else if typlen == -1 {
            /* varlena */
            *values.add(i as usize) = PointerGetDatum(dataptr as *const c_void);

            memcpy(dataptr as *mut c_void, ptr as *const c_void, VARSIZE_ANY(ptr) as Size);
            dataptr = dataptr.add(MAXALIGN(VARSIZE_ANY(ptr) as Size) as usize);
            ptr = ptr.add(VARSIZE_ANY(ptr) as usize);
        } else if typlen == -2 {
            /* cstring */
            let slen: Size = strlen(ptr) + 1;

            *values.add(i as usize) = PointerGetDatum(dataptr as *const c_void);

            memcpy(dataptr as *mut c_void, ptr as *const c_void, slen);
            dataptr = dataptr.add(MAXALIGN(slen) as usize);
            ptr = ptr.add(slen as usize);
        }

        /* make sure we haven't overflown the buffer end */
        Assert!(ptr <= (serialized as *mut c_char).add(VARSIZE_ANY(serialized as *const c_char) as usize));
        i += 1;
    }

    /* should have consumed the whole input value exactly */
    Assert!(ptr == (serialized as *mut c_char).add(VARSIZE_ANY(serialized as *const c_char) as usize));

    /* return the deserialized value */
    range
}

/*
 * Check if the new value matches one of the existing ranges.
 */
unsafe fn has_matching_range(
    bdesc: *mut BrinDesc,
    colloid: Oid,
    ranges: *mut Ranges,
    newval: Datum,
    attno: AttrNumber,
    typid: Oid,
) -> bool {
    let mut compar: Datum;

    let mut minvalue: Datum;
    let mut maxvalue: Datum;

    let cmpLessFn: *mut FmgrInfo;
    let cmpGreaterFn: *mut FmgrInfo;

    /* binary search on ranges */
    let mut start: c_int;
    let mut end: c_int;

    if (*ranges).nranges == 0 {
        return false;
    }

    let values = (*ranges).values.as_ptr() as *mut Datum;

    minvalue = *values.add(0);
    maxvalue = *values.add((2 * (*ranges).nranges - 1) as usize);

    /*
     * Otherwise, need to compare the new value with boundaries of all the
     * ranges. First check if it's less than the absolute minimum, which is
     * the first value in the array.
     */
    cmpLessFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, typid, BTLessStrategyNumber);
    compar = FunctionCall2Coll(cmpLessFn, colloid, newval, minvalue);

    /* smaller than the smallest value in the range list */
    if DatumGetBool(compar) {
        return false;
    }

    /*
     * And now compare it to the existing maximum (last value in the data
     * array). But only if we haven't already ruled out a possible match in
     * the minvalue check.
     */
    cmpGreaterFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, typid, BTGreaterStrategyNumber);
    compar = FunctionCall2Coll(cmpGreaterFn, colloid, newval, maxvalue);

    if DatumGetBool(compar) {
        return false;
    }

    /*
     * So we know it's in the general min/max, the question is whether it
     * falls in one of the ranges or gaps. We'll do a binary search on
     * individual ranges - for each range we check equality (value falls into
     * the range), and then check ranges either above or below the current
     * range.
     */
    start = 0; /* first range */
    end = (*ranges).nranges - 1; /* last range */
    loop {
        let midpoint: c_int = (start + end) / 2;

        /* this means we ran out of ranges in the last step */
        if start > end {
            return false;
        }

        /* copy the min/max values from the ranges */
        minvalue = *values.add((2 * midpoint) as usize);
        maxvalue = *values.add((2 * midpoint + 1) as usize);

        /*
         * Is the value smaller than the minval? If yes, we'll recurse to the
         * left side of range array.
         */
        compar = FunctionCall2Coll(cmpLessFn, colloid, newval, minvalue);

        /* smaller than the smallest value in this range */
        if DatumGetBool(compar) {
            end = midpoint - 1;
            continue;
        }

        /*
         * Is the value greater than the minval? If yes, we'll recurse to the
         * right side of range array.
         */
        compar = FunctionCall2Coll(cmpGreaterFn, colloid, newval, maxvalue);

        /* larger than the largest value in this range */
        if DatumGetBool(compar) {
            start = midpoint + 1;
            continue;
        }

        /* hey, we found a matching range */
        return true;
    }
}

/*
 * range_contains_value
 *      See if the new value is already contained in the range list.
 *
 * We first inspect the list of intervals. We use a small trick - we check
 * the value against min/max of the whole range (min of the first interval,
 * max of the last one) first, and only inspect the individual intervals if
 * this passes.
 *
 * If the value matches none of the intervals, we check the exact values.
 * We simply loop through them and invoke equality operator on them.
 *
 * The last parameter (full) determines whether we need to search all the
 * values, including the unsorted part. With full=false, the unsorted part
 * is not searched, which may produce false negatives and duplicate values
 * (in the unsorted part only), but when we're building the range that's
 * fine - we'll deduplicate before serialization, and it can only happen
 * if there already are unsorted values (so it was already modified).
 *
 * Serialized ranges don't have any unsorted values, so this can't cause
 * false negatives during querying.
 */
unsafe fn range_contains_value(
    bdesc: *mut BrinDesc,
    colloid: Oid,
    attno: AttrNumber,
    attr: Form_pg_attribute,
    ranges: *mut Ranges,
    newval: Datum,
    full: bool,
) -> bool {
    let mut i: c_int;
    let cmpEqualFn: *mut FmgrInfo;
    let typid: Oid = (*attr).atttypid;

    /*
     * First inspect the ranges, if there are any. We first check the whole
     * range, and only when there's still a chance of getting a match we
     * inspect the individual ranges.
     */
    if has_matching_range(bdesc, colloid, ranges, newval, attno, typid) {
        return true;
    }

    cmpEqualFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, typid, BTEqualStrategyNumber);

    let values = (*ranges).values.as_ptr() as *mut Datum;

    /*
     * There is no matching range, so let's inspect the sorted values.
     *
     * We do a sequential search for small numbers of values, and binary
     * search once we have more than 16 values. This threshold is somewhat
     * arbitrary, as it depends on how expensive the comparison function is.
     *
     * XXX If we use the threshold here, maybe we should do the same thing in
     * has_matching_range? Or maybe we should do the bin search all the time?
     *
     * XXX We could use the same optimization as for ranges, to check if the
     * value is between min/max, to maybe rule out all sorted values without
     * having to inspect all of them.
     */
    if (*ranges).nsorted >= 16 {
        let mut cxt = compare_context {
            colloid: (*ranges).colloid,
            cmpFn: (*ranges).cmp,
        };

        if !bsearch_arg(
            &newval as *const Datum as *const c_void,
            values.add((2 * (*ranges).nranges) as usize) as *const c_void,
            (*ranges).nsorted as Size,
            core::mem::size_of::<Datum>(),
            compare_values_c,
            &mut cxt as *mut compare_context as *mut c_void,
        )
        .is_null()
        {
            return true;
        }
    } else {
        i = 2 * (*ranges).nranges;
        while i < 2 * (*ranges).nranges + (*ranges).nsorted {
            let compar: Datum;

            compar = FunctionCall2Coll(cmpEqualFn, colloid, newval, *values.add(i as usize));

            /* found an exact match */
            if DatumGetBool(compar) {
                return true;
            }
            i += 1;
        }
    }

    /* If not asked to inspect the unsorted part, we're done. */
    if !full {
        return false;
    }

    /* Inspect the unsorted part. */
    i = 2 * (*ranges).nranges + (*ranges).nsorted;
    while i < 2 * (*ranges).nranges + (*ranges).nvalues {
        let compar: Datum;

        compar = FunctionCall2Coll(cmpEqualFn, colloid, newval, *values.add(i as usize));

        /* found an exact match */
        if DatumGetBool(compar) {
            return true;
        }
        i += 1;
    }

    /* the value is not covered by this BRIN tuple */
    false
}

/*
 * Expand ranges from Ranges into ExpandedRange array. This expects the
 * eranges to be pre-allocated and with the correct size - there needs to be
 * (nranges + nvalues) elements.
 *
 * The order of expanded ranges is arbitrary. We do expand the ranges first,
 * and this part is sorted. But then we expand the values, and this part may
 * be unsorted.
 */
unsafe fn fill_expanded_ranges(eranges: *mut ExpandedRange, neranges: c_int, ranges: *mut Ranges) {
    let mut idx: c_int;
    let mut i: c_int;

    /* Check that the output array has the right size. */
    Assert!(neranges == ((*ranges).nranges + (*ranges).nvalues));

    let values = (*ranges).values.as_ptr() as *mut Datum;

    idx = 0;
    i = 0;
    while i < (*ranges).nranges {
        (*eranges.add(idx as usize)).minval = *values.add((2 * i) as usize);
        (*eranges.add(idx as usize)).maxval = *values.add((2 * i + 1) as usize);
        (*eranges.add(idx as usize)).collapsed = false;
        idx += 1;

        Assert!(idx <= neranges);
        i += 1;
    }

    i = 0;
    while i < (*ranges).nvalues {
        (*eranges.add(idx as usize)).minval = *values.add((2 * (*ranges).nranges + i) as usize);
        (*eranges.add(idx as usize)).maxval = *values.add((2 * (*ranges).nranges + i) as usize);
        (*eranges.add(idx as usize)).collapsed = true;
        idx += 1;

        Assert!(idx <= neranges);
        i += 1;
    }

    /* Did we produce the expected number of elements? */
    Assert!(idx == neranges);
}

/*
 * Sort and deduplicate expanded ranges.
 *
 * The ranges may be deduplicated - we're simply appending values, without
 * checking for duplicates etc. So maybe the deduplication will reduce the
 * number of ranges enough, and we won't have to compute the distances etc.
 *
 * Returns the number of expanded ranges.
 */
unsafe fn sort_expanded_ranges(
    cmp: *mut FmgrInfo,
    colloid: Oid,
    eranges: *mut ExpandedRange,
    neranges: c_int,
) -> c_int {
    let mut n: c_int;
    let mut i: c_int;
    let mut cxt: compare_context;

    Assert!(neranges > 0);

    /* sort the values */
    cxt = compare_context {
        colloid,
        cmpFn: cmp,
    };

    /*
     * XXX We do qsort on all the values, but we could also leverage the fact
     * that some of the input data is already sorted (all the ranges and maybe
     * some of the points) and do merge sort.
     */
    qsort_arg(
        eranges as *mut c_void,
        neranges as usize,
        core::mem::size_of::<ExpandedRange>(),
        compare_expanded_ranges,
        &mut cxt as *mut compare_context as *mut c_void,
    );

    /*
     * Deduplicate the ranges - simply compare each range to the preceding
     * one, and skip the duplicate ones.
     */
    n = 1;
    i = 1;
    while i < neranges {
        /* if the current range is equal to the preceding one, do nothing */
        if compare_expanded_ranges(
            eranges.add((i - 1) as usize) as *const c_void,
            eranges.add(i as usize) as *const c_void,
            &mut cxt as *mut compare_context as *mut c_void,
        ) == 0
        {
            i += 1;
            continue;
        }

        /* otherwise, copy it to n-th place (if not already there) */
        if i != n {
            memcpy(
                eranges.add(n as usize) as *mut c_void,
                eranges.add(i as usize) as *const c_void,
                core::mem::size_of::<ExpandedRange>(),
            );
        }

        n += 1;
        i += 1;
    }

    Assert!((n > 0) && (n <= neranges));

    n
}

/*
 * When combining multiple Range values (in union function), some of the
 * ranges may overlap. We simply merge the overlapping ranges to fix that.
 *
 * XXX This assumes the expanded ranges were previously sorted (by minval
 * and then maxval). We leverage this when detecting overlap.
 */
unsafe fn merge_overlapping_ranges(
    cmp: *mut FmgrInfo,
    colloid: Oid,
    eranges: *mut ExpandedRange,
    mut neranges: c_int,
) -> c_int {
    let mut idx: c_int;

    /* Merge ranges (idx) and (idx+1) if they overlap. */
    idx = 0;
    while idx < (neranges - 1) {
        let mut r: Datum;

        /*
         * comparing [?,maxval] vs. [minval,?] - the ranges overlap if (minval
         * < maxval)
         */
        r = FunctionCall2Coll(
            cmp,
            colloid,
            (*eranges.add(idx as usize)).maxval,
            (*eranges.add((idx + 1) as usize)).minval,
        );

        /*
         * Nope, maxval < minval, so no overlap. And we know the ranges are
         * ordered, so there are no more overlaps, because all the remaining
         * ranges have greater or equal minval.
         */
        if DatumGetBool(r) {
            /* proceed to the next range */
            idx += 1;
            continue;
        }

        /*
         * So ranges 'idx' and 'idx+1' do overlap, but we don't know if
         * 'idx+1' is contained in 'idx', or if they overlap only partially.
         * So compare the upper bounds and keep the larger one.
         */
        r = FunctionCall2Coll(
            cmp,
            colloid,
            (*eranges.add(idx as usize)).maxval,
            (*eranges.add((idx + 1) as usize)).maxval,
        );

        if DatumGetBool(r) {
            (*eranges.add(idx as usize)).maxval = (*eranges.add((idx + 1) as usize)).maxval;
        }

        /*
         * The range certainly is no longer collapsed (irrespectively of the
         * previous state).
         */
        (*eranges.add(idx as usize)).collapsed = false;

        /*
         * Now get rid of the (idx+1) range entirely by shifting the remaining
         * ranges by 1. There are neranges elements, and we need to move
         * elements from (idx+2). That means the number of elements to move is
         * [ncranges - (idx+2)].
         */
        memmove(
            eranges.add((idx + 1) as usize) as *mut c_void,
            eranges.add((idx + 2) as usize) as *const c_void,
            ((neranges - (idx + 2)) as Size) * core::mem::size_of::<ExpandedRange>(),
        );

        /*
         * Decrease the number of ranges, and repeat (with the same range, as
         * it might overlap with additional ranges thanks to the merge).
         */
        neranges -= 1;
    }

    neranges
}

/*
 * Simple comparator for distance values, comparing the double value.
 * This is intentionally sorting the distances in descending order, i.e.
 * the longer gaps will be at the front.
 */
unsafe fn compare_distances(a: *const c_void, b: *const c_void) -> c_int {
    let da = a as *const DistanceValue;
    let db = b as *const DistanceValue;

    if (*da).value < (*db).value {
        1
    } else if (*da).value > (*db).value {
        -1
    } else {
        0
    }
}

/*
 * Given an array of expanded ranges, compute size of the gaps between each
 * range.  For neranges there are (neranges-1) gaps.
 *
 * We simply call the "distance" function to compute the (max-min) for pairs
 * of consecutive ranges. The function may be fairly expensive, so we do that
 * just once (and then use it to pick as many ranges to merge as possible).
 *
 * See reduce_expanded_ranges for details.
 */
unsafe fn build_distances(
    distanceFn: *mut FmgrInfo,
    colloid: Oid,
    eranges: *mut ExpandedRange,
    neranges: c_int,
) -> *mut DistanceValue {
    let mut i: c_int;
    let ndistances: c_int;
    let distances: *mut DistanceValue;

    Assert!(neranges > 0);

    /* If there's only a single range, there's no distance to calculate. */
    if neranges == 1 {
        return null_mut();
    }

    ndistances = neranges - 1;
    distances = palloc0(core::mem::size_of::<DistanceValue>() * ndistances as usize) as *mut DistanceValue;

    /*
     * Walk through the ranges once and compute the distance between the
     * ranges so that we can sort them once.
     */
    i = 0;
    while i < ndistances {
        let a1: Datum;
        let a2: Datum;
        let r: Datum;

        a1 = (*eranges.add(i as usize)).maxval;
        a2 = (*eranges.add((i + 1) as usize)).minval;

        /* compute length of the gap (between max/min) */
        r = FunctionCall2Coll(distanceFn, colloid, a1, a2);

        /* remember the index of the gap the distance is for */
        (*distances.add(i as usize)).index = i;
        (*distances.add(i as usize)).value = DatumGetFloat8(r);
        i += 1;
    }

    /*
     * Sort the distances in descending order, so that the longest gaps are at
     * the front.
     */
    pg_qsort(
        distances as *mut c_void,
        ndistances as usize,
        core::mem::size_of::<DistanceValue>(),
        compare_distances,
    );

    distances
}

/*
 * Builds expanded ranges for the existing ranges (and single-point ranges),
 * and also the new value (which did not fit into the array).  This expanded
 * representation makes the processing a bit easier, as it allows handling
 * ranges and points the same way.
 *
 * We sort and deduplicate the expanded ranges - this is necessary, because
 * the points may be unsorted. And moreover the two parts (ranges and
 * points) are sorted on their own.
 */
unsafe fn build_expanded_ranges(
    cmp: *mut FmgrInfo,
    colloid: Oid,
    ranges: *mut Ranges,
    nranges: *mut c_int,
) -> *mut ExpandedRange {
    let mut neranges: c_int;
    let eranges: *mut ExpandedRange;

    /* both ranges and points are expanded into a separate element */
    neranges = (*ranges).nranges + (*ranges).nvalues;

    eranges = palloc0(neranges as usize * core::mem::size_of::<ExpandedRange>()) as *mut ExpandedRange;

    /* fill the expanded ranges */
    fill_expanded_ranges(eranges, neranges, ranges);

    /* sort and deduplicate the expanded ranges */
    neranges = sort_expanded_ranges(cmp, colloid, eranges, neranges);

    /* remember how many ranges we built */
    *nranges = neranges;

    eranges
}

/*
 * Counts boundary values needed to store the ranges. Each single-point
 * range is stored using a single value, each regular range needs two.
 */
#[cfg(debug_assertions)]
unsafe fn count_values(cranges: *mut ExpandedRange, ncranges: c_int) -> c_int {
    let mut i: c_int;
    let mut count: c_int;

    count = 0;
    i = 0;
    while i < ncranges {
        if (*cranges.add(i as usize)).collapsed {
            count += 1;
        } else {
            count += 2;
        }
        i += 1;
    }

    count
}

/*
 * reduce_expanded_ranges
 *      reduce the ranges until the number of values is low enough
 *
 * Combines ranges until the number of boundary values drops below the
 * threshold specified by max_values. This happens by merging enough
 * ranges by the distance between them.
 *
 * Returns the number of result ranges.
 *
 * We simply use the global min/max and then add boundaries for enough
 * largest gaps. Each gap adds 2 values, so we simply use (target/2-1)
 * distances. Then we simply sort all the values - each two values are
 * a boundary of a range (possibly collapsed).
 *
 * XXX Some of the ranges may be collapsed (i.e. the min/max values are
 * equal), but we ignore that for now. We could repeat the process,
 * adding a couple more gaps recursively.
 *
 * XXX The ranges to merge are selected solely using the distance. But
 * that may not be the best strategy, for example when multiple gaps
 * are of equal (or very similar) length.
 *
 * Consider for example points 1, 2, 3, .., 64, which have gaps of the
 * same length 1 of course. In that case, we tend to pick the first
 * gap of that length, which leads to this:
 *
 *    step 1:  [1, 2], 3, 4, 5, .., 64
 *    step 2:  [1, 3], 4, 5,    .., 64
 *    step 3:  [1, 4], 5,       .., 64
 *    ...
 *
 * So in the end we'll have one "large" range and multiple small points.
 * That may be fine, but it seems a bit strange and non-optimal. Maybe
 * we should consider other things when picking ranges to merge - e.g.
 * length of the ranges? Or perhaps randomize the choice of ranges, with
 * probability inversely proportional to the distance (the gap lengths
 * may be very close, but not exactly the same).
 *
 * XXX Or maybe we could just handle this by using random value as a
 * tie-break, or by adding random noise to the actual distance.
 */
unsafe fn reduce_expanded_ranges(
    eranges: *mut ExpandedRange,
    neranges: c_int,
    distances: *mut DistanceValue,
    max_values: c_int,
    cmp: *mut FmgrInfo,
    colloid: Oid,
) -> c_int {
    let mut i: c_int;
    let mut nvalues: c_int;
    let values: *mut Datum;

    let mut cxt: compare_context;

    /* total number of gaps between ranges */
    let ndistances: c_int = neranges - 1;

    /* number of gaps to keep */
    let keep: c_int = max_values / 2 - 1;

    /*
     * Maybe we have a sufficiently low number of ranges already?
     *
     * XXX This should happen before we actually do the expensive stuff like
     * sorting, so maybe this should be just an assert.
     */
    if keep >= ndistances {
        return neranges;
    }

    /* sort the values */
    cxt = compare_context {
        colloid,
        cmpFn: cmp,
    };

    /* allocate space for the boundary values */
    nvalues = 0;
    values = palloc(core::mem::size_of::<Datum>() * max_values as usize) as *mut Datum;

    /* add the global min/max values, from the first/last range */
    *values.add(nvalues as usize) = (*eranges.add(0)).minval;
    nvalues += 1;
    *values.add(nvalues as usize) = (*eranges.add((neranges - 1) as usize)).maxval;
    nvalues += 1;

    /* add boundary values for enough gaps */
    i = 0;
    while i < keep {
        /* index of the gap between (index) and (index+1) ranges */
        let index: c_int = (*distances.add(i as usize)).index;

        Assert!((index >= 0) && ((index + 1) < neranges));

        /* add max from the preceding range, minval from the next one */
        *values.add(nvalues as usize) = (*eranges.add(index as usize)).maxval;
        nvalues += 1;
        *values.add(nvalues as usize) = (*eranges.add((index + 1) as usize)).minval;
        nvalues += 1;

        Assert!(nvalues <= max_values);
        i += 1;
    }

    /* We should have an even number of range values. */
    Assert!(nvalues % 2 == 0);

    /*
     * Sort the values using the comparator function, and form ranges from the
     * sorted result.
     */
    qsort_arg(
        values as *mut c_void,
        nvalues as usize,
        core::mem::size_of::<Datum>(),
        compare_values,
        &mut cxt as *mut compare_context as *mut c_void,
    );

    /* We have nvalues boundary values, which means nvalues/2 ranges. */
    i = 0;
    while i < (nvalues / 2) {
        (*eranges.add(i as usize)).minval = *values.add((2 * i) as usize);
        (*eranges.add(i as usize)).maxval = *values.add((2 * i + 1) as usize);

        /* if the boundary values are the same, it's a collapsed range */
        (*eranges.add(i as usize)).collapsed = compare_values(
            values.add((2 * i) as usize) as *const c_void,
            values.add((2 * i + 1) as usize) as *const c_void,
            &mut cxt as *mut compare_context as *mut c_void,
        ) == 0;
        i += 1;
    }

    nvalues / 2
}

/*
 * Store the boundary values from ExpandedRanges back into 'ranges' (using
 * only the minimal number of values needed).
 */
unsafe fn store_expanded_ranges(ranges: *mut Ranges, eranges: *mut ExpandedRange, neranges: c_int) {
    let mut i: c_int;
    let mut idx: c_int = 0;

    let values = (*ranges).values.as_ptr() as *mut Datum;

    /* first copy in the regular ranges */
    (*ranges).nranges = 0;
    i = 0;
    while i < neranges {
        if !(*eranges.add(i as usize)).collapsed {
            *values.add(idx as usize) = (*eranges.add(i as usize)).minval;
            idx += 1;
            *values.add(idx as usize) = (*eranges.add(i as usize)).maxval;
            idx += 1;
            (*ranges).nranges += 1;
        }
        i += 1;
    }

    /* now copy in the collapsed ones */
    (*ranges).nvalues = 0;
    i = 0;
    while i < neranges {
        if (*eranges.add(i as usize)).collapsed {
            *values.add(idx as usize) = (*eranges.add(i as usize)).minval;
            idx += 1;
            (*ranges).nvalues += 1;
        }
        i += 1;
    }

    /* all the values are sorted */
    (*ranges).nsorted = (*ranges).nvalues;

    #[cfg(debug_assertions)]
    {
        Assert!(count_values(eranges, neranges) == 2 * (*ranges).nranges + (*ranges).nvalues);
    }
    Assert!(2 * (*ranges).nranges + (*ranges).nvalues <= (*ranges).maxvalues);
}

/*
 * Consider freeing space in the ranges. Checks if there's space for at least
 * one new value, and performs compaction if needed.
 *
 * Returns true if the value was actually modified.
 */
unsafe fn ensure_free_space_in_buffer(
    bdesc: *mut BrinDesc,
    colloid: Oid,
    attno: AttrNumber,
    attr: Form_pg_attribute,
    range: *mut Ranges,
) -> bool {
    let ctx: MemoryContext;
    let oldctx: MemoryContext;

    let cmpFn: *mut FmgrInfo;
    let distanceFn: *mut FmgrInfo;

    /* expanded ranges */
    let eranges: *mut ExpandedRange;
    let mut neranges: c_int = 0;
    let distances: *mut DistanceValue;

    /*
     * If there is free space in the buffer, we're done without having to
     * modify anything.
     */
    if 2 * (*range).nranges + (*range).nvalues < (*range).maxvalues {
        return false;
    }

    /* we'll certainly need the comparator, so just look it up now */
    cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);

    /* deduplicate values, if there's an unsorted part */
    range_deduplicate_values(range);

    /*
     * Did we reduce enough free space by just the deduplication?
     *
     * We don't simply check against range->maxvalues again. The deduplication
     * might have freed very little space (e.g. just one value), forcing us to
     * do deduplication very often. In that case, it's better to do the
     * compaction and reduce more space.
     */
    if (2 * (*range).nranges + (*range).nvalues) as f64
        <= (*range).maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR
    {
        return true;
    }

    /*
     * We need to combine some of the existing ranges, to reduce the number of
     * values we have to store.
     *
     * The distanceFn calls (which may internally call e.g. numeric_le) may
     * allocate quite a bit of memory, and we must not leak it (we might have
     * to do this repeatedly, even for a single BRIN page range). Otherwise
     * we'd have problems e.g. when building new indexes. So we use a memory
     * context and make sure we free the memory at the end (so if we call the
     * distance function many times, it might be an issue, but meh).
     */
    ctx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"minmax-multi context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    oldctx = MemoryContextSwitchTo(ctx);

    /* build the expanded ranges */
    eranges = build_expanded_ranges(cmpFn, colloid, range, &mut neranges);

    /* Is the expanded representation of ranges correct? */
    AssertCheckExpandedRanges(bdesc, colloid, attno, attr, eranges, neranges);

    /* and we'll also need the 'distance' procedure */
    distanceFn = minmax_multi_get_procinfo(bdesc, attno as uint16, PROCNUM_DISTANCE);

    /* build array of gap distances and sort them in ascending order */
    distances = build_distances(distanceFn, colloid, eranges, neranges);

    /*
     * Combine ranges until we release at least 50% of the space. This
     * threshold is somewhat arbitrary, perhaps needs tuning. We must not use
     * too low or high value.
     */
    neranges = reduce_expanded_ranges(
        eranges,
        neranges,
        distances,
        ((*range).maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR) as c_int,
        cmpFn,
        colloid,
    );

    /* Is the result of reducing expanded ranges correct? */
    AssertCheckExpandedRanges(bdesc, colloid, attno, attr, eranges, neranges);

    /* Make sure we've sufficiently reduced the number of ranges. */
    #[cfg(debug_assertions)]
    {
        Assert!(count_values(eranges, neranges) as f64 <= (*range).maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR);
    }

    /* decompose the expanded ranges into regular ranges and single values */
    store_expanded_ranges(range, eranges, neranges);

    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(ctx);

    /* Did we break the ranges somehow? */
    AssertCheckRanges(range, cmpFn, colloid);

    true
}

/*
 * range_add_value
 *      Add the new value to the minmax-multi range.
 */
unsafe fn range_add_value(
    bdesc: *mut BrinDesc,
    colloid: Oid,
    attno: AttrNumber,
    attr: Form_pg_attribute,
    ranges: *mut Ranges,
    mut newval: Datum,
) -> bool {
    let cmpFn: *mut FmgrInfo;
    let mut modified: bool;

    /* we'll certainly need the comparator, so just look it up now */
    cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);

    /* comprehensive checks of the input ranges */
    AssertCheckRanges(ranges, cmpFn, colloid);

    /*
     * Make sure there's enough free space in the buffer. We only trigger this
     * when the buffer is full, which means it had to be modified as we size
     * it to be larger than what is stored on disk.
     *
     * This needs to happen before we check if the value is contained in the
     * range, because the value might be in the unsorted part, and we don't
     * check that in range_contains_value. The deduplication would then move
     * it to the sorted part, and we'd add the value too, which violates the
     * rule that we never have duplicates with the ranges or sorted values.
     *
     * We might also deduplicate and recheck if the value is contained, but
     * that seems like overkill. We'd need to deduplicate anyway, so why not
     * do it now.
     */
    modified = ensure_free_space_in_buffer(bdesc, colloid, attno, attr, ranges);

    /*
     * Bail out if the value already is covered by the range.
     *
     * We could also add values until we hit values_per_range, and then do the
     * deduplication in a batch, hoping for better efficiency. But that would
     * mean we actually modify the range every time, which means having to
     * serialize the value, which does palloc, walks the values, copies them,
     * etc. Not exactly cheap.
     *
     * So instead we do the check, which should be fairly cheap - assuming the
     * comparator function is not very expensive.
     *
     * This also implies the values array can't contain duplicate values.
     */
    if range_contains_value(bdesc, colloid, attno, attr, ranges, newval, false) {
        return modified;
    }

    /* Make a copy of the value, if needed. */
    newval = datumCopy(newval, (*attr).attbyval, (*attr).attlen as c_int);

    let values = (*ranges).values.as_ptr() as *mut Datum;

    /*
     * If there's space in the values array, copy it in and we're done.
     *
     * We do want to keep the values sorted (to speed up searches), so we do a
     * simple insertion sort. We could do something more elaborate, e.g. by
     * sorting the values only now and then, but for small counts (e.g. when
     * maxvalues is 64) this should be fine.
     */
    *values.add((2 * (*ranges).nranges + (*ranges).nvalues) as usize) = newval;
    (*ranges).nvalues += 1;

    /* If we added the first value, we can consider it as sorted. */
    if (*ranges).nvalues == 1 {
        (*ranges).nsorted = 1;
    }

    /*
     * Check we haven't broken the ordering of boundary values (checks both
     * parts, but that doesn't hurt).
     */
    AssertCheckRanges(ranges, cmpFn, colloid);

    /* Check the range contains the value we just added. */
    Assert!(range_contains_value(bdesc, colloid, attno, attr, ranges, newval, true));

    /* yep, we've modified the range */
    modified = true;
    modified
}

/*
 * Generate range representation of data collected during "batch mode".
 * This is similar to reduce_expanded_ranges, except that we can't assume
 * the values are sorted and there may be duplicate values.
 */
unsafe fn compactify_ranges(bdesc: *mut BrinDesc, ranges: *mut Ranges, max_values: c_int) {
    let cmpFn: *mut FmgrInfo;
    let distanceFn: *mut FmgrInfo;

    /* expanded ranges */
    let eranges: *mut ExpandedRange;
    let mut neranges: c_int = 0;
    let distances: *mut DistanceValue;

    let ctx: MemoryContext;
    let oldctx: MemoryContext;

    /*
     * Do we need to actually compactify anything?
     *
     * There are two reasons why compaction may be needed - firstly, there may
     * be too many values, or some of the values may be unsorted.
     */
    if ((*ranges).nranges * 2 + (*ranges).nvalues <= max_values)
        && ((*ranges).nsorted == (*ranges).nvalues)
    {
        return;
    }

    /* we'll certainly need the comparator, so just look it up now */
    cmpFn = minmax_multi_get_strategy_procinfo(bdesc, (*ranges).attno as uint16, (*ranges).typid, BTLessStrategyNumber);

    /* and we'll also need the 'distance' procedure */
    distanceFn = minmax_multi_get_procinfo(bdesc, (*ranges).attno as uint16, PROCNUM_DISTANCE);

    /*
     * The distanceFn calls (which may internally call e.g. numeric_le) may
     * allocate quite a bit of memory, and we must not leak it. Otherwise,
     * we'd have problems e.g. when building indexes. So we create a local
     * memory context and make sure we free the memory before leaving this
     * function (not after every call).
     */
    ctx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"minmax-multi context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    oldctx = MemoryContextSwitchTo(ctx);

    /* build the expanded ranges */
    eranges = build_expanded_ranges(cmpFn, (*ranges).colloid, ranges, &mut neranges);

    /* build array of gap distances and sort them in ascending order */
    distances = build_distances(distanceFn, (*ranges).colloid, eranges, neranges);

    /*
     * Combine ranges until we get below max_values. We don't use any scale
     * factor, because this is used during serialization, and we don't expect
     * more tuples to be inserted anytime soon.
     */
    neranges = reduce_expanded_ranges(eranges, neranges, distances, max_values, cmpFn, (*ranges).colloid);

    #[cfg(debug_assertions)]
    {
        Assert!(count_values(eranges, neranges) <= max_values);
    }

    /* transform back into regular ranges and single values */
    store_expanded_ranges(ranges, eranges, neranges);

    /* check all the range invariants */
    AssertCheckRanges(ranges, cmpFn, (*ranges).colloid);

    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(ctx);
}

pub unsafe fn brin_minmax_multi_opcinfo(fcinfo: FunctionCallInfo) -> Datum {
    let result: *mut BrinOpcInfo;

    /*
     * opaque->strategy_procinfos is initialized lazily; here it is set to
     * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
     */

    result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) + core::mem::size_of::<MinmaxMultiOpaque>())
        as *mut BrinOpcInfo;
    (*result).oi_nstored = 1;
    (*result).oi_regular_nulls = true;
    (*result).oi_opaque = ((result as *mut c_char).add(MAXALIGN(SizeofBrinOpcInfo(1)) as usize)) as *mut MinmaxMultiOpaque as *mut c_void;
    *(*result).oi_typcache.as_mut_ptr().add(0) =
        lookup_type_cache(PG_BRIN_MINMAX_MULTI_SUMMARYOID, 0) as *mut _;

    PG_RETURN_POINTER!(result)
}

/*
 * Compute the distance between two float4 values (plain subtraction).
 */
pub unsafe fn brin_minmax_multi_distance_float4(fcinfo: FunctionCallInfo) -> Datum {
    let a1: f32 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let a2: f32 = PG_GETARG_FLOAT4!(fcinfo, 1);

    /* if both values are NaN, then we consider them the same */
    if a1.is_nan() && a2.is_nan() {
        PG_RETURN_FLOAT8!(0.0);
    }

    /* if one value is NaN, use infinite distance */
    if a1.is_nan() || a2.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_infinity());
    }

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(a1 <= a2);

    PG_RETURN_FLOAT8!(a2 as f64 - a1 as f64)
}

/*
 * Compute the distance between two float8 values (plain subtraction).
 */
pub unsafe fn brin_minmax_multi_distance_float8(fcinfo: FunctionCallInfo) -> Datum {
    let a1: f64 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let a2: f64 = PG_GETARG_FLOAT8!(fcinfo, 1);

    /* if both values are NaN, then we consider them the same */
    if a1.is_nan() && a2.is_nan() {
        PG_RETURN_FLOAT8!(0.0);
    }

    /* if one value is NaN, use infinite distance */
    if a1.is_nan() || a2.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_infinity());
    }

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(a1 <= a2);

    PG_RETURN_FLOAT8!(a2 - a1)
}

/*
 * Compute the distance between two int2 values (plain subtraction).
 */
pub unsafe fn brin_minmax_multi_distance_int2(fcinfo: FunctionCallInfo) -> Datum {
    let a1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let a2: int16 = PG_GETARG_INT16!(fcinfo, 1);

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(a1 <= a2);

    PG_RETURN_FLOAT8!(a2 as f64 - a1 as f64)
}

/*
 * Compute the distance between two int4 values (plain subtraction).
 */
pub unsafe fn brin_minmax_multi_distance_int4(fcinfo: FunctionCallInfo) -> Datum {
    let a1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let a2: int32 = PG_GETARG_INT32!(fcinfo, 1);

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(a1 <= a2);

    PG_RETURN_FLOAT8!(a2 as f64 - a1 as f64)
}

/*
 * Compute the distance between two int8 values (plain subtraction).
 */
pub unsafe fn brin_minmax_multi_distance_int8(fcinfo: FunctionCallInfo) -> Datum {
    let a1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let a2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(a1 <= a2);

    PG_RETURN_FLOAT8!(a2 as f64 - a1 as f64)
}

/*
 * Compute the distance between two tid values (by mapping them to float8 and
 * then subtracting them).
 */
pub unsafe fn brin_minmax_multi_distance_tid(fcinfo: FunctionCallInfo) -> Datum {
    let da1: f64;
    let da2: f64;

    let pa1 = PG_GETARG_DATUM!(fcinfo, 0) as ItemPointer;
    let pa2 = PG_GETARG_DATUM!(fcinfo, 1) as ItemPointer;

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(ItemPointerCompare(pa1, pa2) <= 0);

    /*
     * We use the no-check variants here, because user-supplied values may
     * have (ip_posid == 0). See ItemPointerCompare.
     */
    da1 = ItemPointerGetBlockNumberNoCheck(pa1) as f64 * MaxHeapTuplesPerPage as f64
        + ItemPointerGetOffsetNumberNoCheck(pa1) as f64;

    da2 = ItemPointerGetBlockNumberNoCheck(pa2) as f64 * MaxHeapTuplesPerPage as f64
        + ItemPointerGetOffsetNumberNoCheck(pa2) as f64;

    PG_RETURN_FLOAT8!(da2 - da1)
}

/*
 * Compute the distance between two numeric values (plain subtraction).
 */
pub unsafe fn brin_minmax_multi_distance_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let d: Datum;
    let a1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let a2: Datum = PG_GETARG_DATUM!(fcinfo, 1);

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(DatumGetBool(DirectFunctionCall2!(numeric_le, a1, a2)));

    d = DirectFunctionCall2!(numeric_sub, a2, a1); /* a2 - a1 */

    PG_RETURN_DATUM!(DirectFunctionCall1!(numeric_float8, d))
}

/*
 * Compute the approximate distance between two UUID values.
 *
 * XXX We do not need a perfectly accurate value, so we approximate the
 * deltas (which would have to be 128-bit integers) with a 64-bit float.
 * The small inaccuracies do not matter in practice, in the worst case
 * we'll decide to merge ranges that are not the closest ones.
 */
pub unsafe fn brin_minmax_multi_distance_uuid(fcinfo: FunctionCallInfo) -> Datum {
    let mut i: c_int;
    let mut delta: f64 = 0.0;

    let a1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let a2: Datum = PG_GETARG_DATUM!(fcinfo, 1);

    let u1: *mut pg_uuid_t = DatumGetUUIDP(a1);
    let u2: *mut pg_uuid_t = DatumGetUUIDP(a2);

    /*
     * We know the values are range boundaries, but the range may be collapsed
     * (i.e. single points), with equal values.
     */
    Assert!(DatumGetBool(DirectFunctionCall2!(uuid_le, a1, a2)));

    /* compute approximate delta as a double precision value */
    i = UUID_LEN as c_int - 1;
    while i >= 0 {
        delta += (*u2).data[i as usize] as c_int as f64 - (*u1).data[i as usize] as c_int as f64;
        delta /= 256.0;
        i -= 1;
    }

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the approximate distance between two dates.
 */
pub unsafe fn brin_minmax_multi_distance_date(fcinfo: FunctionCallInfo) -> Datum {
    let delta: f64;
    let dateVal1: DateADT = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2: DateADT = PG_GETARG_DATEADT!(fcinfo, 1);

    delta = dateVal2 as f64 - dateVal1 as f64;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the approximate distance between two time (without tz) values.
 *
 * TimeADT is just an int64, so we simply subtract the values directly.
 */
pub unsafe fn brin_minmax_multi_distance_time(fcinfo: FunctionCallInfo) -> Datum {
    let delta: f64;

    let ta: TimeADT = PG_GETARG_TIMEADT!(fcinfo, 0);
    let tb: TimeADT = PG_GETARG_TIMEADT!(fcinfo, 1);

    delta = (tb - ta) as f64;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the approximate distance between two timetz values.
 *
 * Simply subtracts the TimeADT (int64) values embedded in TimeTzADT.
 */
pub unsafe fn brin_minmax_multi_distance_timetz(fcinfo: FunctionCallInfo) -> Datum {
    let delta: f64;

    let ta: *mut TimeTzADT = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let tb: *mut TimeTzADT = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    delta = ((*tb).time - (*ta).time) as f64 + ((*tb).zone - (*ta).zone) as f64 * USECS_PER_SEC as f64;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the distance between two timestamp values.
 */
pub unsafe fn brin_minmax_multi_distance_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let delta: f64;

    let dt1: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    delta = dt2 as f64 - dt1 as f64;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the distance between two interval values.
 */
pub unsafe fn brin_minmax_multi_distance_interval(fcinfo: FunctionCallInfo) -> Datum {
    let delta: f64;

    let ia: *mut Interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let ib: *mut Interval = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    let dayfraction: int64;
    let mut days: int64;

    /*
     * Delta is (fractional) number of days between the intervals. Assume
     * months have 30 days for consistency with interval_cmp_internal. We
     * don't need to be exact, in the worst case we'll build a bit less
     * efficient ranges. But we should not contradict interval_cmp.
     */
    dayfraction = ((*ib).time % USECS_PER_DAY) - ((*ia).time % USECS_PER_DAY);
    days = ((*ib).time / USECS_PER_DAY) - ((*ia).time / USECS_PER_DAY);
    days += (*ib).day as int64 - (*ia).day as int64;
    days += ((*ib).month as int64 - (*ia).month as int64) * 30i64;

    /* convert to double precision */
    delta = days as f64 + dayfraction as f64 / USECS_PER_DAY as f64;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the distance between two pg_lsn values.
 *
 * LSN is just an int64 encoding position in the stream, so just subtract
 * those int64 values directly.
 */
pub unsafe fn brin_minmax_multi_distance_pg_lsn(fcinfo: FunctionCallInfo) -> Datum {
    let delta: f64;

    let lsna: XLogRecPtr = PG_GETARG_LSN!(fcinfo, 0);
    let lsnb: XLogRecPtr = PG_GETARG_LSN!(fcinfo, 1);

    delta = (lsnb - lsna) as f64;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the distance between two macaddr values.
 *
 * mac addresses are treated as 6 unsigned chars, so do the same thing we
 * already do for UUID values.
 */
pub unsafe fn brin_minmax_multi_distance_macaddr(fcinfo: FunctionCallInfo) -> Datum {
    let mut delta: f64;

    let a: *mut macaddr = PG_GETARG_MACADDR_P!(fcinfo, 0);
    let b: *mut macaddr = PG_GETARG_MACADDR_P!(fcinfo, 1);

    delta = (*b).f as f64 - (*a).f as f64;
    delta /= 256.0;

    delta += (*b).e as f64 - (*a).e as f64;
    delta /= 256.0;

    delta += (*b).d as f64 - (*a).d as f64;
    delta /= 256.0;

    delta += (*b).c as f64 - (*a).c as f64;
    delta /= 256.0;

    delta += (*b).b as f64 - (*a).b as f64;
    delta /= 256.0;

    delta += (*b).a as f64 - (*a).a as f64;
    delta /= 256.0;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the distance between two macaddr8 values.
 *
 * macaddr8 addresses are 8 unsigned chars, so do the same thing we
 * already do for UUID values.
 */
pub unsafe fn brin_minmax_multi_distance_macaddr8(fcinfo: FunctionCallInfo) -> Datum {
    let mut delta: f64;

    let a: *mut macaddr8 = PG_GETARG_MACADDR8_P!(fcinfo, 0);
    let b: *mut macaddr8 = PG_GETARG_MACADDR8_P!(fcinfo, 1);

    delta = (*b).h as f64 - (*a).h as f64;
    delta /= 256.0;

    delta += (*b).g as f64 - (*a).g as f64;
    delta /= 256.0;

    delta += (*b).f as f64 - (*a).f as f64;
    delta /= 256.0;

    delta += (*b).e as f64 - (*a).e as f64;
    delta /= 256.0;

    delta += (*b).d as f64 - (*a).d as f64;
    delta /= 256.0;

    delta += (*b).c as f64 - (*a).c as f64;
    delta /= 256.0;

    delta += (*b).b as f64 - (*a).b as f64;
    delta /= 256.0;

    delta += (*b).a as f64 - (*a).a as f64;
    delta /= 256.0;

    Assert!(delta >= 0.0);

    PG_RETURN_FLOAT8!(delta)
}

/*
 * Compute the distance between two inet values.
 *
 * The distance is defined as the difference between 32-bit/128-bit values,
 * depending on the IP version. The distance is computed by subtracting
 * the bytes and normalizing it to [0,1] range for each IP family.
 * Addresses from different families are considered to be in maximum
 * distance, which is 1.0.
 *
 * XXX Does this need to consider the mask (bits)?  For now, it's ignored.
 */
pub unsafe fn brin_minmax_multi_distance_inet(fcinfo: FunctionCallInfo) -> Datum {
    let mut delta: f64;
    let mut i: c_int;
    let len: c_int;
    let addra: *mut u8;
    let addrb: *mut u8;

    let ipa: *mut inet = PG_GETARG_INET_PP!(fcinfo, 0);
    let ipb: *mut inet = PG_GETARG_INET_PP!(fcinfo, 1);

    let lena: c_int;
    let lenb: c_int;

    /*
     * If the addresses are from different families, consider them to be in
     * maximal possible distance (which is 1.0).
     */
    if ip_family(ipa) != ip_family(ipb) {
        PG_RETURN_FLOAT8!(1.0);
    }

    addra = palloc(ip_addrsize(ipa) as Size) as *mut u8;
    memcpy(addra as *mut c_void, ip_addr(ipa) as *const c_void, ip_addrsize(ipa) as Size);

    addrb = palloc(ip_addrsize(ipb) as Size) as *mut u8;
    memcpy(addrb as *mut c_void, ip_addr(ipb) as *const c_void, ip_addrsize(ipb) as Size);

    /*
     * The length is calculated from the mask length, because we sort the
     * addresses by first address in the range, so A.B.C.D/24 < A.B.C.1 (the
     * first range starts at A.B.C.0, which is before A.B.C.1). We don't want
     * to produce a negative delta in this case, so we just cut the extra
     * bytes.
     *
     * XXX Maybe this should be a bit more careful and cut the bits, not just
     * whole bytes.
     */
    lena = ip_bits(ipa) as c_int;
    lenb = ip_bits(ipb) as c_int;

    len = ip_addrsize(ipa);

    /* apply the network mask to both addresses */
    i = 0;
    while i < len {
        let mut mask: u8;
        let mut nbits: c_int;

        nbits = Max(0, lena - (i * 8));
        if nbits < 8 {
            mask = (0xFFu32 << (8 - nbits)) as u8;
            *addra.add(i as usize) &= mask;
        }

        nbits = Max(0, lenb - (i * 8));
        if nbits < 8 {
            mask = (0xFFu32 << (8 - nbits)) as u8;
            *addrb.add(i as usize) &= mask;
        }
        i += 1;
    }

    /* Calculate the difference between the addresses. */
    delta = 0.0;
    i = len - 1;
    while i >= 0 {
        let a: u8 = *addra.add(i as usize);
        let b: u8 = *addrb.add(i as usize);

        delta += b as f64 - a as f64;
        delta /= 256.0;
        i -= 1;
    }

    Assert!((delta >= 0.0) && (delta <= 1.0));

    pfree(addra as *mut c_void);
    pfree(addrb as *mut c_void);

    PG_RETURN_FLOAT8!(delta)
}

unsafe fn brin_minmax_multi_serialize(bdesc: *mut BrinDesc, src: Datum, dst: *mut Datum) {
    let ranges: *mut Ranges = DatumGetPointer(src) as *mut Ranges;
    let s: *mut SerializedRanges;

    /*
     * In batch mode, we need to compress the accumulated values to the
     * actually requested number of values/ranges.
     */
    compactify_ranges(bdesc, ranges, (*ranges).target_maxvalues);

    /* At this point everything has to be fully sorted. */
    Assert!((*ranges).nsorted == (*ranges).nvalues);

    s = brin_range_serialize(ranges);
    *dst.add(0) = PointerGetDatum(s as *const c_void);
}

unsafe fn brin_minmax_multi_get_values(_bdesc: *mut BrinDesc, opts: *mut MinMaxMultiOptions) -> c_int {
    MinMaxMultiGetValuesPerRange(opts)
}

/*
 * Examine the given index tuple (which contains the partial status of a
 * certain page range) by comparing it to the given value that comes from
 * another heap tuple.  If the new value is outside the min/max range
 * specified by the existing tuple values, update the index tuple and return
 * true.  Otherwise, return false and do not modify in this case.
 */
pub unsafe fn brin_minmax_multi_add_value(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let newval: Datum = PG_GETARG_DATUM!(fcinfo, 2);
    /* isnull (arg 3) is asserted-not-null only. */
    let opts = PG_GET_OPCLASS_OPTIONS!(fcinfo) as *mut MinMaxMultiOptions;
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let mut modified: bool = false;
    let attr: Form_pg_attribute;
    let attno: AttrNumber;
    let mut ranges: *mut Ranges;
    #[allow(unused_assignments)]
    let mut serialized: *mut SerializedRanges = null_mut();

    Assert!(!DatumGetBool(PG_GETARG_DATUM!(fcinfo, 3)));

    attno = (*column).bv_attno;
    attr = TupleDescAttr((*bdesc).bd_tupdesc, (attno - 1) as c_int);

    /* use the already deserialized value, if possible */
    ranges = DatumGetPointer((*column).bv_mem_value) as *mut Ranges;

    /*
     * If this is the first non-null value, we need to initialize the range
     * list. Otherwise, just extract the existing range list from BrinValues.
     *
     * When starting with an empty range, we assume this is a batch mode and
     * we use a larger buffer. The buffer size is derived from the BRIN range
     * size, number of rows per page, with some sensible min/max values. A
     * small buffer would be bad for performance, but a large buffer might
     * require a lot of memory (because of keeping all the values).
     */
    if (*column).bv_allnulls {
        let oldctx: MemoryContext;

        let target_maxvalues: c_int;
        let mut maxvalues: c_int;
        let pagesPerRange: BlockNumber = BrinGetPagesPerRange((*bdesc).bd_index);

        /* what was specified as a reloption? */
        target_maxvalues = brin_minmax_multi_get_values(bdesc, opts);

        /*
         * Determine the insert buffer size - we use 10x the target, capped to
         * the maximum number of values in the heap range. This is more than
         * enough, considering the actual number of rows per page is likely
         * much lower, but meh.
         */
        maxvalues = Min(target_maxvalues * MINMAX_BUFFER_FACTOR, MaxHeapTuplesPerPage * pagesPerRange as c_int);

        /* but always at least the original value */
        maxvalues = Max(maxvalues, target_maxvalues);

        /* always cap by MIN/MAX */
        maxvalues = Max(maxvalues, MINMAX_BUFFER_MIN);
        maxvalues = Min(maxvalues, MINMAX_BUFFER_MAX);

        oldctx = MemoryContextSwitchTo((*column).bv_context as MemoryContext);
        ranges = minmax_multi_init(maxvalues);
        (*ranges).attno = attno;
        (*ranges).colloid = colloid;
        (*ranges).typid = (*attr).atttypid;
        (*ranges).target_maxvalues = target_maxvalues;

        /* we'll certainly need the comparator, so just look it up now */
        (*ranges).cmp = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);

        MemoryContextSwitchTo(oldctx);

        (*column).bv_allnulls = false;
        modified = true;

        (*column).bv_mem_value = PointerGetDatum(ranges as *const c_void);
        (*column).bv_serialize = brin_minmax_multi_serialize as *mut c_void;
    } else if ranges.is_null() {
        let oldctx: MemoryContext;

        let mut maxvalues: c_int;
        let pagesPerRange: BlockNumber = BrinGetPagesPerRange((*bdesc).bd_index);

        oldctx = MemoryContextSwitchTo((*column).bv_context as MemoryContext);

        serialized = PG_DETOAST_DATUM!(*(*column).bv_values.add(0)) as *mut SerializedRanges;

        /*
         * Determine the insert buffer size - we use 10x the target, capped to
         * the maximum number of values in the heap range. This is more than
         * enough, considering the actual number of rows per page is likely
         * much lower, but meh.
         */
        maxvalues = Min((*serialized).maxvalues * MINMAX_BUFFER_FACTOR, MaxHeapTuplesPerPage * pagesPerRange as c_int);

        /* but always at least the original value */
        maxvalues = Max(maxvalues, (*serialized).maxvalues);

        /* always cap by MIN/MAX */
        maxvalues = Max(maxvalues, MINMAX_BUFFER_MIN);
        maxvalues = Min(maxvalues, MINMAX_BUFFER_MAX);

        ranges = brin_range_deserialize(maxvalues, serialized);

        (*ranges).attno = attno;
        (*ranges).colloid = colloid;
        (*ranges).typid = (*attr).atttypid;

        /* we'll certainly need the comparator, so just look it up now */
        (*ranges).cmp = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);

        (*column).bv_mem_value = PointerGetDatum(ranges as *const c_void);
        (*column).bv_serialize = brin_minmax_multi_serialize as *mut c_void;

        MemoryContextSwitchTo(oldctx);
    }

    /*
     * Try to add the new value to the range. We need to update the modified
     * flag, so that we serialize the updated summary later.
     */
    modified |= range_add_value(bdesc, colloid, attno, attr, ranges, newval);

    PG_RETURN_BOOL!(modified)
}

/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's min/max
 * values.  Return true if so, false otherwise.
 */
pub unsafe fn brin_minmax_multi_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let keys = PG_GETARG_POINTER!(fcinfo, 2) as *mut ScanKey;
    let nkeys: c_int = PG_GETARG_INT32!(fcinfo, 3);

    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let mut subtype: Oid;
    let mut attno: AttrNumber;
    let mut value: Datum;
    let mut finfo: *mut FmgrInfo;
    let serialized: *mut SerializedRanges;
    let ranges: *mut Ranges;
    let mut keyno: c_int;
    let mut rangeno: c_int;
    let mut i: c_int;

    attno = (*column).bv_attno;

    serialized = PG_DETOAST_DATUM!(*(*column).bv_values.add(0)) as *mut SerializedRanges;
    ranges = brin_range_deserialize((*serialized).maxvalues, serialized);

    let values = (*ranges).values.as_ptr() as *mut Datum;

    /* inspect the ranges, and for each one evaluate the scan keys */
    rangeno = 0;
    while rangeno < (*ranges).nranges {
        let minval: Datum = *values.add((2 * rangeno) as usize);
        let maxval: Datum = *values.add((2 * rangeno + 1) as usize);

        /* assume the range is matching, and we'll try to prove otherwise */
        let mut matching: bool = true;

        keyno = 0;
        while keyno < nkeys {
            let mut matches: bool;
            let key: ScanKey = *keys.add(keyno as usize);

            /* NULL keys are handled and filtered-out in bringetbitmap */
            Assert!(((*key).sk_flags & SK_ISNULL) == 0);

            attno = (*key).sk_attno;
            subtype = (*key).sk_subtype;
            value = (*key).sk_argument;
            match (*key).sk_strategy {
                x if x == BTLessStrategyNumber || x == BTLessEqualStrategyNumber => {
                    finfo = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, subtype, (*key).sk_strategy);
                    /* first value from the array */
                    matches = DatumGetBool(FunctionCall2Coll(finfo, colloid, minval, value));
                }
                x if x == BTEqualStrategyNumber => {
                    let mut compar: Datum;
                    let mut cmpFn: *mut FmgrInfo;

                    /* by default this range does not match */
                    matches = false;

                    'eqblock: {
                        /*
                         * Otherwise, need to compare the new value with
                         * boundaries of all the ranges. First check if it's
                         * less than the absolute minimum, which is the first
                         * value in the array.
                         */
                        cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, subtype, BTGreaterStrategyNumber);
                        compar = FunctionCall2Coll(cmpFn, colloid, minval, value);

                        /* smaller than the smallest value in this range */
                        if DatumGetBool(compar) {
                            break 'eqblock;
                        }

                        cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, subtype, BTLessStrategyNumber);
                        compar = FunctionCall2Coll(cmpFn, colloid, maxval, value);

                        /* larger than the largest value in this range */
                        if DatumGetBool(compar) {
                            break 'eqblock;
                        }

                        /*
                         * We haven't managed to eliminate this range, so
                         * consider it matching.
                         */
                        matches = true;
                    }
                }
                x if x == BTGreaterEqualStrategyNumber || x == BTGreaterStrategyNumber => {
                    finfo = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, subtype, (*key).sk_strategy);
                    /* last value from the array */
                    matches = DatumGetBool(FunctionCall2Coll(finfo, colloid, maxval, value));
                }
                _ => {
                    /* shouldn't happen */
                    elog!(ERROR, "invalid strategy number {}", (*key).sk_strategy);
                    matches = false;
                }
            }

            /* the range has to match all the scan keys */
            matching &= matches;

            /* once we find a non-matching key, we're done */
            if !matching {
                break;
            }
            keyno += 1;
        }

        /*
         * have we found a range matching all scan keys? if yes, we're done
         */
        if matching {
            PG_RETURN_BOOL!(true);
        }
        rangeno += 1;
    }

    /*
     * And now inspect the values. We don't bother with doing a binary search
     * here, because we're dealing with serialized / fully compacted ranges,
     * so there should be only very few values.
     */
    i = 0;
    while i < (*ranges).nvalues {
        let val: Datum = *values.add((2 * (*ranges).nranges + i) as usize);

        /* assume the range is matching, and we'll try to prove otherwise */
        let mut matching: bool = true;

        keyno = 0;
        while keyno < nkeys {
            let matches: bool;
            let key: ScanKey = *keys.add(keyno as usize);

            /* we've already dealt with NULL keys at the beginning */
            if (*key).sk_flags & SK_ISNULL != 0 {
                keyno += 1;
                continue;
            }

            attno = (*key).sk_attno;
            subtype = (*key).sk_subtype;
            value = (*key).sk_argument;
            match (*key).sk_strategy {
                x if x == BTLessStrategyNumber
                    || x == BTLessEqualStrategyNumber
                    || x == BTEqualStrategyNumber
                    || x == BTGreaterEqualStrategyNumber
                    || x == BTGreaterStrategyNumber =>
                {
                    finfo = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, subtype, (*key).sk_strategy);
                    matches = DatumGetBool(FunctionCall2Coll(finfo, colloid, val, value));
                }
                _ => {
                    /* shouldn't happen */
                    elog!(ERROR, "invalid strategy number {}", (*key).sk_strategy);
                    matches = false;
                }
            }

            /* the range has to match all the scan keys */
            matching &= matches;

            /* once we find a non-matching key, we're done */
            if !matching {
                break;
            }
            keyno += 1;
        }

        /* have we found a range matching all scan keys? if yes, we're done */
        if matching {
            PG_RETURN_BOOL!(true);
        }
        i += 1;
    }

    PG_RETURN_BOOL!(false)
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 */
pub unsafe fn brin_minmax_multi_union(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let col_a = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let col_b = PG_GETARG_POINTER!(fcinfo, 2) as *mut BrinValues;

    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let serialized_a: *mut SerializedRanges;
    let serialized_b: *mut SerializedRanges;
    let ranges_a: *mut Ranges;
    let ranges_b: *mut Ranges;
    let attno: AttrNumber;
    let attr: Form_pg_attribute;
    let eranges: *mut ExpandedRange;
    let mut neranges: c_int;
    let cmpFn: *mut FmgrInfo;
    let distanceFn: *mut FmgrInfo;
    let distances: *mut DistanceValue;
    let ctx: MemoryContext;
    let oldctx: MemoryContext;

    Assert!((*col_a).bv_attno == (*col_b).bv_attno);
    Assert!(!(*col_a).bv_allnulls && !(*col_b).bv_allnulls);

    attno = (*col_a).bv_attno;
    attr = TupleDescAttr((*bdesc).bd_tupdesc, (attno - 1) as c_int);

    serialized_a = PG_DETOAST_DATUM!(*(*col_a).bv_values.add(0)) as *mut SerializedRanges;
    serialized_b = PG_DETOAST_DATUM!(*(*col_b).bv_values.add(0)) as *mut SerializedRanges;

    ranges_a = brin_range_deserialize((*serialized_a).maxvalues, serialized_a);
    ranges_b = brin_range_deserialize((*serialized_b).maxvalues, serialized_b);

    /* make sure neither of the ranges is NULL */
    Assert!(!ranges_a.is_null() && !ranges_b.is_null());

    neranges = ((*ranges_a).nranges + (*ranges_a).nvalues) + ((*ranges_b).nranges + (*ranges_b).nvalues);

    /*
     * The distanceFn calls (which may internally call e.g. numeric_le) may
     * allocate quite a bit of memory, and we must not leak it. Otherwise,
     * we'd have problems e.g. when building indexes. So we create a local
     * memory context and make sure we free the memory before leaving this
     * function (not after every call).
     */
    ctx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"minmax-multi context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    oldctx = MemoryContextSwitchTo(ctx);

    /* allocate and fill */
    eranges = palloc0(neranges as usize * core::mem::size_of::<ExpandedRange>()) as *mut ExpandedRange;

    /* fill the expanded ranges with entries for the first range */
    fill_expanded_ranges(eranges, (*ranges_a).nranges + (*ranges_a).nvalues, ranges_a);

    /* and now add combine ranges for the second range */
    fill_expanded_ranges(
        eranges.add(((*ranges_a).nranges + (*ranges_a).nvalues) as usize),
        (*ranges_b).nranges + (*ranges_b).nvalues,
        ranges_b,
    );

    cmpFn = minmax_multi_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);

    /* sort the expanded ranges */
    neranges = sort_expanded_ranges(cmpFn, colloid, eranges, neranges);

    /*
     * We've loaded two different lists of expanded ranges, so some of them
     * may be overlapping. So walk through them and merge them.
     */
    neranges = merge_overlapping_ranges(cmpFn, colloid, eranges, neranges);

    /* check that the combine ranges are correct (no overlaps, ordering) */
    AssertCheckExpandedRanges(bdesc, colloid, attno, attr, eranges, neranges);

    /*
     * If needed, reduce some of the ranges.
     *
     * XXX This may be fairly expensive, so maybe we should do it only when
     * it's actually needed (when we have too many ranges).
     */

    /* build array of gap distances and sort them in ascending order */
    distanceFn = minmax_multi_get_procinfo(bdesc, attno as uint16, PROCNUM_DISTANCE);
    distances = build_distances(distanceFn, colloid, eranges, neranges);

    /*
     * See how many values would be needed to store the current ranges, and if
     * needed combine as many of them to get below the threshold. The
     * collapsed ranges will be stored as a single value.
     *
     * XXX This does not apply the load factor, as we don't expect to add more
     * values to the range, so we prefer to keep as many ranges as possible.
     *
     * XXX Can the maxvalues be different in the two ranges? Perhaps we should
     * use maximum of those?
     */
    neranges = reduce_expanded_ranges(eranges, neranges, distances, (*ranges_a).maxvalues, cmpFn, colloid);

    /* Is the result of reducing expanded ranges correct? */
    AssertCheckExpandedRanges(bdesc, colloid, attno, attr, eranges, neranges);

    /* update the first range summary */
    store_expanded_ranges(ranges_a, eranges, neranges);

    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(ctx);

    /* cleanup and update the serialized value */
    pfree(serialized_a as *mut c_void);
    *(*col_a).bv_values.add(0) = PointerGetDatum(brin_range_serialize(ranges_a) as *const c_void);

    PG_RETURN_VOID!()
}

/*
 * Cache and return minmax multi opclass support procedure
 *
 * Return the procedure corresponding to the given function support number
 * or null if it does not exist.
 */
unsafe fn minmax_multi_get_procinfo(bdesc: *mut BrinDesc, attno: uint16, procnum: uint16) -> *mut FmgrInfo {
    let opaque: *mut MinmaxMultiOpaque;
    let basenum: uint16 = procnum - PROCNUM_BASE;

    /*
     * We cache these in the opaque struct, to avoid repetitive syscache
     * lookups.
     */
    opaque = (*(*(*bdesc).bd_info.as_ptr().add((attno - 1) as usize))).oi_opaque as *mut MinmaxMultiOpaque;

    if (*opaque).extra_procinfos[basenum as usize].fn_oid == InvalidOid {
        if RegProcedureIsValid(index_getprocid((*bdesc).bd_index, attno as AttrNumber, procnum)) {
            fmgr_info_copy(
                &mut (*opaque).extra_procinfos[basenum as usize],
                index_getprocinfo((*bdesc).bd_index, attno as AttrNumber, procnum),
                (*bdesc).bd_context,
            );
        } else {
            /*
             * C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
             *         errdetail_internal("The operator class is missing support
             *         function %d for column %d.", procnum, attno).
             */
            ereport!(ERROR, errmsg!("invalid opclass definition"));
        }
    }

    &mut (*opaque).extra_procinfos[basenum as usize]
}

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors minmax_multi_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
unsafe fn minmax_multi_get_strategy_procinfo(
    bdesc: *mut BrinDesc,
    attno: uint16,
    subtype: Oid,
    strategynum: uint16,
) -> *mut FmgrInfo {
    let opaque: *mut MinmaxMultiOpaque;

    Assert!(strategynum >= 1 && strategynum <= BTMaxStrategyNumber);

    opaque = (*(*(*bdesc).bd_info.as_ptr().add((attno - 1) as usize))).oi_opaque as *mut MinmaxMultiOpaque;

    /*
     * We cache the procedures for the previous subtype in the opaque struct,
     * to avoid repetitive syscache lookups.  If the subtype changed,
     * invalidate all the cached entries.
     */
    if (*opaque).cached_subtype != subtype {
        let mut i: uint16 = 1;
        while i <= BTMaxStrategyNumber {
            (*opaque).strategy_procinfos[(i - 1) as usize].fn_oid = InvalidOid;
            i += 1;
        }
        (*opaque).cached_subtype = subtype;
    }

    if (*opaque).strategy_procinfos[(strategynum - 1) as usize].fn_oid == InvalidOid {
        let attr: Form_pg_attribute;
        let tuple: HeapTuple;
        let opfamily: Oid;
        let oprid: Oid;

        opfamily = *(*(*bdesc).bd_index).rd_opfamily.add((attno - 1) as usize);
        attr = TupleDescAttr((*bdesc).bd_tupdesc, (attno - 1) as c_int);
        tuple = SearchSysCache4(
            AMOPSTRATEGY,
            ObjectIdGetDatum(opfamily),
            ObjectIdGetDatum((*attr).atttypid),
            ObjectIdGetDatum(subtype),
            Int16GetDatum(strategynum as int16),
        );
        if !HeapTupleIsValid(tuple) {
            elog!(
                ERROR,
                "missing operator {}({},{}) in opfamily {}",
                strategynum,
                (*attr).atttypid,
                subtype,
                opfamily
            );
        }

        oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple, Anum_pg_amop_amopopr));
        ReleaseSysCache(tuple);
        Assert!(RegProcedureIsValid(oprid));

        fmgr_info_cxt(
            get_opcode(oprid),
            &mut (*opaque).strategy_procinfos[(strategynum - 1) as usize],
            (*bdesc).bd_context,
        );
    }

    &mut (*opaque).strategy_procinfos[(strategynum - 1) as usize]
}

pub unsafe fn brin_minmax_multi_options(fcinfo: FunctionCallInfo) -> Datum {
    let relopts = PG_GETARG_POINTER!(fcinfo, 0) as *mut local_relopts;

    init_local_reloptions(relopts, core::mem::size_of::<MinMaxMultiOptions>());

    add_local_int_reloption(
        relopts,
        c"values_per_range".as_ptr(),
        c"desc".as_ptr(),
        MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE,
        8,
        256,
        core::mem::offset_of!(MinMaxMultiOptions, valuesPerRange) as c_int,
    );

    PG_RETURN_VOID!()
}

/*
 * brin_minmax_multi_summary_in
 *      - input routine for type brin_minmax_multi_summary.
 *
 * brin_minmax_multi_summary is only used internally to represent summaries
 * in BRIN minmax-multi indexes, so it has no operations of its own, and we
 * disallow input too.
 */
pub unsafe fn brin_minmax_multi_summary_in(_fcinfo: FunctionCallInfo) -> Datum {
    /*
     * brin_minmax_multi_summary stores the data in binary form and parsing
     * text input is not needed, so disallow this.
     *
     * C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED).
     */
    ereport!(ERROR, errmsg!("cannot accept a value of type {}", "brin_minmax_multi_summary"));

    PG_RETURN_VOID!() /* keep compiler quiet */
}

/*
 * brin_minmax_multi_summary_out
 *      - output routine for type brin_minmax_multi_summary.
 *
 * BRIN minmax-multi summaries are serialized into a bytea value, but we
 * want to output something nicer humans can understand.
 */
pub unsafe fn brin_minmax_multi_summary_out(fcinfo: FunctionCallInfo) -> Datum {
    let mut i: c_int;
    let mut idx: c_int;
    let ranges: *mut SerializedRanges;
    let ranges_deserialized: *mut Ranges;
    let mut str: StringInfoData = core::mem::zeroed();
    let mut isvarlena: bool = false;
    let mut outfunc: Oid = InvalidOid;
    let mut fmgrinfo: FmgrInfo = core::mem::zeroed();
    let mut astate_values: *mut ArrayBuildState = null_mut();

    initStringInfo(&mut str);
    appendStringInfoChar(&mut str, b'{' as c_char);

    /*
     * Detoast to get value with full 4B header (can't be stored in a toast
     * table, but can use 1B header).
     */
    ranges = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut SerializedRanges;

    /* lookup output func for the type */
    getTypeOutputInfo((*ranges).typid, &mut outfunc, &mut isvarlena);
    fmgr_info(outfunc, &mut fmgrinfo);

    /* deserialize the range info easy-to-process pieces */
    ranges_deserialized = brin_range_deserialize((*ranges).maxvalues, ranges);

    let values = (*ranges_deserialized).values.as_ptr() as *mut Datum;

    appendStringInfo3(
        &mut str,
        c"nranges: %d  nvalues: %d  maxvalues: %d".as_ptr(),
        (*ranges_deserialized).nranges,
        (*ranges_deserialized).nvalues,
        (*ranges_deserialized).maxvalues,
    );

    /* serialize ranges */
    idx = 0;
    i = 0;
    while i < (*ranges_deserialized).nranges {
        let a: *mut c_char;
        let b: *mut c_char;
        let c: *mut text;
        let mut buf: StringInfoData = core::mem::zeroed();

        initStringInfo(&mut buf);

        a = OutputFunctionCall(&mut fmgrinfo, *values.add(idx as usize));
        idx += 1;
        b = OutputFunctionCall(&mut fmgrinfo, *values.add(idx as usize));
        idx += 1;

        appendStringInfo2(&mut buf, c"%s ... %s".as_ptr(), a, b);

        c = cstring_to_text_with_len(buf.data, buf.len);

        astate_values = accumArrayResult(
            astate_values,
            PointerGetDatum(c as *const c_void),
            false,
            TEXTOID,
            CurrentMemoryContext,
        );
        i += 1;
    }

    if (*ranges_deserialized).nranges > 0 {
        let mut typoutput: Oid = InvalidOid;
        let mut typIsVarlena: bool = false;
        let val: Datum;
        let extval: *mut c_char;

        getTypeOutputInfo(ANYARRAYOID, &mut typoutput, &mut typIsVarlena);

        val = makeArrayResult(astate_values, CurrentMemoryContext);

        extval = OidOutputFunctionCall(typoutput, val);

        appendStringInfo1(&mut str, c" ranges: %s".as_ptr(), extval);
    }

    /* serialize individual values */
    astate_values = null_mut();

    i = 0;
    while i < (*ranges_deserialized).nvalues {
        let a: Datum;
        let b: *mut text;

        a = FunctionCall1!(&mut fmgrinfo, *values.add(idx as usize));
        idx += 1;
        b = cstring_to_text(DatumGetCString(a));

        astate_values = accumArrayResult(
            astate_values,
            PointerGetDatum(b as *const c_void),
            false,
            TEXTOID,
            CurrentMemoryContext,
        );
        i += 1;
    }

    if (*ranges_deserialized).nvalues > 0 {
        let mut typoutput: Oid = InvalidOid;
        let mut typIsVarlena: bool = false;
        let val: Datum;
        let extval: *mut c_char;

        getTypeOutputInfo(ANYARRAYOID, &mut typoutput, &mut typIsVarlena);

        val = makeArrayResult(astate_values, CurrentMemoryContext);

        extval = OidOutputFunctionCall(typoutput, val);

        appendStringInfo1(&mut str, c" values: %s".as_ptr(), extval);
    }

    appendStringInfoChar(&mut str, b'}' as c_char);

    PG_RETURN_CSTRING!(str.data)
}

/*
 * brin_minmax_multi_summary_recv
 *      - binary input routine for type brin_minmax_multi_summary.
 */
pub unsafe fn brin_minmax_multi_summary_recv(_fcinfo: FunctionCallInfo) -> Datum {
    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED). */
    ereport!(ERROR, errmsg!("cannot accept a value of type {}", "brin_minmax_multi_summary"));

    PG_RETURN_VOID!() /* keep compiler quiet */
}

/*
 * brin_minmax_multi_summary_send
 *      - binary output routine for type brin_minmax_multi_summary.
 *
 * BRIN minmax-multi summaries are serialized in a bytea value (although
 * the type is named differently), so let's just send that.
 */
pub unsafe fn brin_minmax_multi_summary_send(fcinfo: FunctionCallInfo) -> Datum {
    byteasend(fcinfo)
}

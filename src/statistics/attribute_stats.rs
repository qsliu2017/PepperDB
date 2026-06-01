//! attribute_stats.c - PostgreSQL relation attribute statistics manipulation.
//!
//! Code supporting the direct import of relation attribute statistics, similar
//! to what is done by the ANALYZE command.
//!
//! src/backend/statistics/attribute_stats.c

use crate::prelude::*;

use core::ffi::c_short;

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple, heap_freetuple, heap_modify_tuple};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::StatisticRelationId;
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_statistic::{
    STATISTIC_KIND_BOUNDS_HISTOGRAM, STATISTIC_KIND_CORRELATION, STATISTIC_KIND_DECHIST,
    STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCELEM, STATISTIC_KIND_MCV,
    STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM, STATISTIC_NUM_SLOTS,
};
use crate::nodes::makefuncs::makeRangeVar;
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{lfirst, list_head, lnext, List, ListCell, NIL};
use crate::nodes::primnodes::RangeVar;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lockdefs::{
    AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use crate::utils::array::ArrayType;
use crate::nodes::miscnodes::ErrorData;
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};
use crate::utils::rel::{Relation, RelationGetDescr, RelationGetRelationName};

use crate::postgres::{
    BoolGetDatum, CStringGetDatum, DatumGetInt16, DatumGetObjectId, Int16GetDatum, Int32GetDatum,
    ObjectIdGetDatum, PointerGetDatum,
};
use crate::{
    FunctionCallInvoke, InitFunctionCallInfoData, LOCAL_FCINFO, PG_ARGISNULL, PG_GETARG_BOOL,
    PG_GETARG_DATUM, PG_GETARG_INT16, PG_RETURN_BOOL, PG_RETURN_VOID,
};

/* OID constants from fmgroids / catalog headers. */
const TEXTOID: Oid = 25;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;
const FLOAT4ARRAYOID: Oid = 1021;
const TSVECTOROID: Oid = 3614;
const DEFAULT_COLLATION_OID: Oid = 100;

/* relkind values from pg_class.h */
const RELKIND_INDEX: c_char = b'i' as c_char;
const RELKIND_PARTITIONED_INDEX: c_char = b'I' as c_char;

/* typtype values from pg_type.h */
const TYPTYPE_RANGE: c_char = b'r' as c_char;
const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char;

/* InvalidAttrNumber from access/attnum.h */
const InvalidAttrNumber: AttrNumber = 0;

/* fmgr OID for array_in (from fmgroids.h) */
const F_ARRAY_IN: Oid = 750;
unsafe fn fmgr_info(_oid: Oid, _info: *mut crate::utils::fmgr::FmgrInfo) { unimplemented!() /* TODO: utils/fmgr/fmgr.c */ }

/* Float8LessOperator from pg_operator.h */
const Float8LessOperator: Oid = 672;

/* lookup_type_cache flags from typcache.h */
const TYPECACHE_EQ_OPR: c_int = 0x0001;
const TYPECACHE_LT_OPR: c_int = 0x0002;

/* syscache ids (utils/syscache.h, not yet ported) */
const ATTNUM: c_int = 0;
const STATRELATTINH: c_int = 0;

/*
 * Attribute numbers and Natts for pg_statistic (catalog/pg_statistic.h, not
 * yet ported with these symbols).
 */
const Anum_pg_statistic_starelid: c_int = 1;
const Anum_pg_statistic_staattnum: c_int = 2;
const Anum_pg_statistic_stainherit: c_int = 3;
const Anum_pg_statistic_stanullfrac: c_int = 4;
const Anum_pg_statistic_stawidth: c_int = 5;
const Anum_pg_statistic_stadistinct: c_int = 6;
const Anum_pg_statistic_stakind1: c_int = 7;
const Anum_pg_statistic_staop1: c_int = 12;
const Anum_pg_statistic_stacoll1: c_int = 17;
const Anum_pg_statistic_stanumbers1: c_int = 22;
const Anum_pg_statistic_stavalues1: c_int = 27;
const Natts_pg_statistic: usize = 31;

const DEFAULT_NULL_FRAC: Datum = 0; /* Float4GetDatum(0.0) is 0 */
const DEFAULT_AVG_WIDTH: Datum = 0; /* Int32GetDatum(0), unknown */
const DEFAULT_N_DISTINCT: Datum = 0; /* Float4GetDatum(0.0), unknown */

/*
 * struct StatsArgInfo from statistics/stat_utils.h.  Not yet ported there;
 * defined locally so attarginfo can be expressed.
 */
#[repr(C)]
pub struct StatsArgInfo {
    pub argname: *const c_char,
    pub argtype: Oid,
}

/* enum attribute_stats_argnum */
const ATTRELSCHEMA_ARG: usize = 0;
const ATTRELNAME_ARG: usize = 1;
const ATTNAME_ARG: usize = 2;
const ATTNUM_ARG: usize = 3;
const INHERITED_ARG: usize = 4;
const NULL_FRAC_ARG: usize = 5;
const AVG_WIDTH_ARG: usize = 6;
const N_DISTINCT_ARG: usize = 7;
const MOST_COMMON_VALS_ARG: usize = 8;
const MOST_COMMON_FREQS_ARG: usize = 9;
const HISTOGRAM_BOUNDS_ARG: usize = 10;
const CORRELATION_ARG: usize = 11;
const MOST_COMMON_ELEMS_ARG: usize = 12;
const MOST_COMMON_ELEM_FREQS_ARG: usize = 13;
const ELEM_COUNT_HISTOGRAM_ARG: usize = 14;
const RANGE_LENGTH_HISTOGRAM_ARG: usize = 15;
const RANGE_EMPTY_FRAC_ARG: usize = 16;
const RANGE_BOUNDS_HISTOGRAM_ARG: usize = 17;
const NUM_ATTRIBUTE_STATS_ARGS: usize = 18;

const attarginfo: [StatsArgInfo; NUM_ATTRIBUTE_STATS_ARGS + 1] = [
    StatsArgInfo { argname: c"schemaname".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"relname".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"attname".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"attnum".as_ptr(), argtype: INT2OID },
    StatsArgInfo { argname: c"inherited".as_ptr(), argtype: BOOLOID },
    StatsArgInfo { argname: c"null_frac".as_ptr(), argtype: FLOAT4OID },
    StatsArgInfo { argname: c"avg_width".as_ptr(), argtype: INT4OID },
    StatsArgInfo { argname: c"n_distinct".as_ptr(), argtype: FLOAT4OID },
    StatsArgInfo { argname: c"most_common_vals".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"most_common_freqs".as_ptr(), argtype: FLOAT4ARRAYOID },
    StatsArgInfo { argname: c"histogram_bounds".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"correlation".as_ptr(), argtype: FLOAT4OID },
    StatsArgInfo { argname: c"most_common_elems".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"most_common_elem_freqs".as_ptr(), argtype: FLOAT4ARRAYOID },
    StatsArgInfo { argname: c"elem_count_histogram".as_ptr(), argtype: FLOAT4ARRAYOID },
    StatsArgInfo { argname: c"range_length_histogram".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"range_empty_frac".as_ptr(), argtype: FLOAT4OID },
    StatsArgInfo { argname: c"range_bounds_histogram".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: null(), argtype: 0 },
];

/* enum clear_attribute_stats_argnum */
const C_ATTRELSCHEMA_ARG: usize = 0;
const C_ATTRELNAME_ARG: usize = 1;
const C_ATTNAME_ARG: usize = 2;
const C_INHERITED_ARG: usize = 3;
const C_NUM_ATTRIBUTE_STATS_ARGS: usize = 4;

const cleararginfo: [StatsArgInfo; C_NUM_ATTRIBUTE_STATS_ARGS + 1] = [
    StatsArgInfo { argname: c"relation".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"relation".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"attname".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"inherited".as_ptr(), argtype: BOOLOID },
    StatsArgInfo { argname: null(), argtype: 0 },
];

/*
 * ErrorData mirror exposing just the `elevel` field (utils/elog.h not yet
 * ported; nodes/miscnodes aliases ErrorData to c_void).  escontext.error_data
 * is cast to this to set the downgraded elevel before re-throwing.
 */
#[repr(C)]
pub struct ErrorDataStub {
    pub elevel: c_int,
}

/* TypeCacheEntry partial mirror (utils/typcache.h, not yet ported) */
#[repr(C)]
pub struct TypeCacheEntry {
    pub typtype: c_char,
    pub eq_opr: Oid,
    pub lt_opr: Oid,
}

/*
 * FormData_pg_index mirror exposing the variable-length `indkey` int2vector
 * trailer that the catalog/pg_index.rs fixed-part struct omits.  rd_index is
 * cast to this so we can read indkey.values[] as the C code does.
 */
#[repr(C)]
pub struct int2vector_stub {
    pub values: [int16; FLEXIBLE_ARRAY_MEMBER],
}

#[repr(C)]
pub struct FormData_pg_index_stub {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indkey: int2vector_stub,
}

/* ---- not-yet-ported callees, stubbed locally ---- */

// TODO: port statistics/stat_utils.c
unsafe fn stats_check_required_arg(
    _fcinfo: FunctionCallInfo,
    _arginfo: *const StatsArgInfo,
    _argnum: c_int,
) {
    unimplemented!()
}

// TODO: port statistics/stat_utils.c
unsafe fn stats_check_arg_array(
    _fcinfo: FunctionCallInfo,
    _arginfo: *const StatsArgInfo,
    _argnum: c_int,
) -> bool {
    unimplemented!()
}

// TODO: port statistics/stat_utils.c
unsafe fn stats_check_arg_pair(
    _fcinfo: FunctionCallInfo,
    _arginfo: *const StatsArgInfo,
    _argnum1: c_int,
    _argnum2: c_int,
) -> bool {
    unimplemented!()
}

// TODO: port statistics/stat_utils.c
unsafe fn stats_fill_fcinfo_from_arg_pairs(
    _pairs_fcinfo: FunctionCallInfo,
    _positional_fcinfo: FunctionCallInfo,
    _arginfo: *const StatsArgInfo,
) -> bool {
    unimplemented!()
}

// TODO: port statistics/stat_utils.c
unsafe fn RangeVarCallbackForStats(
    _relation: *const RangeVar,
    _rel_id: Oid,
    _old_relid: Oid,
    _arg: *mut c_void,
) {
    unimplemented!()
}

type RangeVarGetRelidCallback =
    unsafe fn(relation: *const RangeVar, relId: Oid, oldRelid: Oid, arg: *mut c_void);

// TODO: port catalog/namespace.c
unsafe fn RangeVarGetRelidExtended(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _flags: u32,
    _callback: RangeVarGetRelidCallback,
    _callback_arg: *mut c_void,
) -> Oid {
    unimplemented!()
}

// TODO: port access/transam/xlog.c
unsafe fn RecoveryInProgress() -> bool {
    unimplemented!()
}

// TODO: port utils/cache/lsyscache.c
unsafe fn get_attnum(_relid: Oid, _attname: *const c_char) -> AttrNumber {
    unimplemented!()
}

// TODO: port utils/cache/lsyscache.c
unsafe fn get_attname(_relid: Oid, _attnum: AttrNumber, _missing_ok: bool) -> *mut c_char {
    unimplemented!()
}

// TODO: port utils/cache/lsyscache.c
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    unimplemented!()
}

// TODO: port utils/cache/lsyscache.c
unsafe fn get_base_element_type(_typid: Oid) -> Oid {
    unimplemented!()
}

// TODO: port utils/cache/lsyscache.c
unsafe fn type_is_multirange(_typid: Oid) -> bool {
    unimplemented!()
}

// TODO: port utils/cache/lsyscache.c
unsafe fn get_multirange_range(_multirangeOid: Oid) -> Oid {
    unimplemented!()
}

// TODO: port utils/cache/typcache.c
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!()
}

// TODO: port utils/cache/syscache.c
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO: port utils/cache/syscache.c
unsafe fn SearchSysCache3(_cacheId: c_int, _key1: Datum, _key2: Datum, _key3: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO: port utils/cache/syscache.c
unsafe fn SearchSysCacheExistsAttName(_relid: Oid, _attname: *const c_char) -> bool {
    unimplemented!()
}

// TODO: port utils/cache/syscache.c
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

// TODO: port access/common/relation.c
unsafe fn relation_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!()
}

// TODO: port access/common/relation.c
unsafe fn relation_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!()
}

// TODO: port utils/cache/relcache.c
unsafe fn RelationGetIndexExpressions(_relation: Relation) -> *mut List {
    unimplemented!()
}

// TODO: port catalog/indexing.c
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) {
    unimplemented!()
}

// TODO: port catalog/indexing.c
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {
    unimplemented!()
}

// TODO: port catalog/indexing.c
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut ItemPointerData) {
    unimplemented!()
}

// TODO: port access/transam/xact.c
unsafe fn CommandCounterIncrement() {
    unimplemented!()
}

// TODO: port nodes/nodeFuncs.c
unsafe fn exprType(_expr: *const Node) -> Oid {
    unimplemented!()
}

// TODO: port nodes/nodeFuncs.c
unsafe fn exprTypmod(_expr: *const Node) -> i32 {
    unimplemented!()
}

// TODO: port nodes/nodeFuncs.c
unsafe fn exprCollation(_expr: *const Node) -> Oid {
    unimplemented!()
}

// TODO: port utils/adt/arrayfuncs.c
unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut ArrayType {
    unimplemented!()
}

// TODO: port utils/adt/arrayfuncs.c
unsafe fn array_contains_nulls(_array: *mut ArrayType) -> bool {
    unimplemented!()
}

// TODO: port utils/array.h
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!()
}

// TODO: port utils/error/elog.c
unsafe fn ThrowErrorData(_edata: *mut ErrorData) {
    unimplemented!()
}

/*
 * Insert or Update Attribute Statistics
 *
 * See pg_statistic.h for an explanation of how each statistic kind is
 * stored. Custom statistics kinds are not supported.
 *
 * Depending on the statistics kind, we need to derive information from the
 * attribute for which we're storing the stats. For instance, the MCVs are
 * stored as an anyarray, and the representation of the array needs to store
 * the correct element type, which must be derived from the attribute.
 *
 * Major errors, such as the table not existing, the attribute not existing,
 * or a permissions failure are always reported at ERROR. Other errors, such
 * as a conversion failure on one statistic kind, are reported as a WARNING
 * and other statistic kinds may still be updated.
 */
unsafe fn attribute_statistics_update(fcinfo: FunctionCallInfo) -> bool {
    let nspname: *mut c_char;
    let relname: *mut c_char;
    let reloid: Oid;
    let attname: *mut c_char;
    let attnum: AttrNumber;
    let inherited: bool;
    let mut locked_table: Oid = InvalidOid;

    let starel: Relation;
    let statup: HeapTuple;

    let mut atttypid: Oid = InvalidOid;
    let mut atttypmod: i32 = 0;
    let mut atttyptype: c_char = 0;
    let mut atttypcoll: Oid = InvalidOid;
    let mut eq_opr: Oid = InvalidOid;
    let mut lt_opr: Oid = InvalidOid;

    let mut elemtypid: Oid = InvalidOid;
    let mut elem_eq_opr: Oid = InvalidOid;

    let mut array_in_fn: FmgrInfo = std::mem::zeroed();

    let mut do_mcv: bool = !PG_ARGISNULL!(fcinfo, MOST_COMMON_FREQS_ARG)
        && !PG_ARGISNULL!(fcinfo, MOST_COMMON_VALS_ARG);
    let mut do_histogram: bool = !PG_ARGISNULL!(fcinfo, HISTOGRAM_BOUNDS_ARG);
    let mut do_correlation: bool = !PG_ARGISNULL!(fcinfo, CORRELATION_ARG);
    let mut do_mcelem: bool = !PG_ARGISNULL!(fcinfo, MOST_COMMON_ELEMS_ARG)
        && !PG_ARGISNULL!(fcinfo, MOST_COMMON_ELEM_FREQS_ARG);
    let mut do_dechist: bool = !PG_ARGISNULL!(fcinfo, ELEM_COUNT_HISTOGRAM_ARG);
    let mut do_bounds_histogram: bool = !PG_ARGISNULL!(fcinfo, RANGE_BOUNDS_HISTOGRAM_ARG);
    let mut do_range_length_histogram: bool = !PG_ARGISNULL!(fcinfo, RANGE_LENGTH_HISTOGRAM_ARG)
        && !PG_ARGISNULL!(fcinfo, RANGE_EMPTY_FRAC_ARG);

    let mut values: [Datum; Natts_pg_statistic] = [0; Natts_pg_statistic];
    let mut nulls: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];
    let mut replaces: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];

    let mut result: bool = true;

    stats_check_required_arg(fcinfo, attarginfo.as_ptr(), ATTRELSCHEMA_ARG as c_int);
    stats_check_required_arg(fcinfo, attarginfo.as_ptr(), ATTRELNAME_ARG as c_int);

    nspname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, ATTRELSCHEMA_ARG));
    relname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, ATTRELNAME_ARG));

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    /* lock before looking up attribute */
    reloid = RangeVarGetRelidExtended(
        makeRangeVar(nspname, relname, -1),
        ShareUpdateExclusiveLock,
        0,
        RangeVarCallbackForStats,
        &mut locked_table as *mut Oid as *mut c_void,
    );

    /* user can specify either attname or attnum, but not both */
    if !PG_ARGISNULL!(fcinfo, ATTNAME_ARG) {
        if !PG_ARGISNULL!(fcinfo, ATTNUM_ARG) {
            elog!(ERROR, "cannot specify both \"{}\" and \"{}\"", "attname", "attnum");
        }
        attname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, ATTNAME_ARG));
        attnum = get_attnum(reloid, attname);
        /* note that this test covers attisdropped cases too: */
        if attnum == InvalidAttrNumber {
            elog!(
                ERROR,
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(attname).to_string_lossy(),
                std::ffi::CStr::from_ptr(relname).to_string_lossy()
            );
        }
    } else if !PG_ARGISNULL!(fcinfo, ATTNUM_ARG) {
        attnum = PG_GETARG_INT16!(fcinfo, ATTNUM_ARG);
        attname = get_attname(reloid, attnum, true);
        /* annoyingly, get_attname doesn't check attisdropped */
        if attname.is_null() || !SearchSysCacheExistsAttName(reloid, attname) {
            elog!(
                ERROR,
                "column {} of relation \"{}\" does not exist",
                attnum,
                std::ffi::CStr::from_ptr(relname).to_string_lossy()
            );
        }
    } else {
        ereport!(ERROR, "must specify either \"attname\" or \"attnum\"");
        unreachable!();
    }

    if attnum < 0 {
        elog!(
            ERROR,
            "cannot modify statistics on system column \"{}\"",
            std::ffi::CStr::from_ptr(attname).to_string_lossy()
        );
    }

    stats_check_required_arg(fcinfo, attarginfo.as_ptr(), INHERITED_ARG as c_int);
    inherited = PG_GETARG_BOOL!(fcinfo, INHERITED_ARG);

    /*
     * Check argument sanity. If some arguments are unusable, emit a WARNING
     * and set the corresponding argument to NULL in fcinfo.
     */

    if !stats_check_arg_array(fcinfo, attarginfo.as_ptr(), MOST_COMMON_FREQS_ARG as c_int) {
        do_mcv = false;
        result = false;
    }

    if !stats_check_arg_array(fcinfo, attarginfo.as_ptr(), MOST_COMMON_ELEM_FREQS_ARG as c_int) {
        do_mcelem = false;
        result = false;
    }
    if !stats_check_arg_array(fcinfo, attarginfo.as_ptr(), ELEM_COUNT_HISTOGRAM_ARG as c_int) {
        do_dechist = false;
        result = false;
    }

    if !stats_check_arg_pair(
        fcinfo,
        attarginfo.as_ptr(),
        MOST_COMMON_VALS_ARG as c_int,
        MOST_COMMON_FREQS_ARG as c_int,
    ) {
        do_mcv = false;
        result = false;
    }

    if !stats_check_arg_pair(
        fcinfo,
        attarginfo.as_ptr(),
        MOST_COMMON_ELEMS_ARG as c_int,
        MOST_COMMON_ELEM_FREQS_ARG as c_int,
    ) {
        do_mcelem = false;
        result = false;
    }

    if !stats_check_arg_pair(
        fcinfo,
        attarginfo.as_ptr(),
        RANGE_LENGTH_HISTOGRAM_ARG as c_int,
        RANGE_EMPTY_FRAC_ARG as c_int,
    ) {
        do_range_length_histogram = false;
        result = false;
    }

    /* derive information from attribute */
    get_attr_stat_type(
        reloid,
        attnum,
        &mut atttypid,
        &mut atttypmod,
        &mut atttyptype,
        &mut atttypcoll,
        &mut eq_opr,
        &mut lt_opr,
    );

    /* if needed, derive element type */
    if do_mcelem || do_dechist {
        if !get_elem_stat_type(atttypid, atttyptype, &mut elemtypid, &mut elem_eq_opr) {
            elog!(
                WARNING,
                "could not determine element type of column \"{}\"",
                std::ffi::CStr::from_ptr(attname).to_string_lossy()
            );
            elemtypid = InvalidOid;
            elem_eq_opr = InvalidOid;

            do_mcelem = false;
            do_dechist = false;
            result = false;
        }
    }

    /* histogram and correlation require less-than operator */
    if (do_histogram || do_correlation) && !OidIsValid(lt_opr) {
        elog!(
            WARNING,
            "could not determine less-than operator for column \"{}\"",
            std::ffi::CStr::from_ptr(attname).to_string_lossy()
        );

        do_histogram = false;
        do_correlation = false;
        result = false;
    }

    /* only range types can have range stats */
    if (do_range_length_histogram || do_bounds_histogram)
        && !(atttyptype == TYPTYPE_RANGE || atttyptype == TYPTYPE_MULTIRANGE)
    {
        elog!(
            WARNING,
            "column \"{}\" is not a range type",
            std::ffi::CStr::from_ptr(attname).to_string_lossy()
        );

        do_bounds_histogram = false;
        do_range_length_histogram = false;
        result = false;
    }

    fmgr_info(F_ARRAY_IN, &mut array_in_fn);

    starel = table_open(StatisticRelationId, RowExclusiveLock);

    statup = SearchSysCache3(
        STATRELATTINH,
        reloid as Datum,
        attnum as Datum,
        inherited as Datum,
    );

    /* initialize from existing tuple if exists */
    if HeapTupleIsValid(statup) {
        heap_deform_tuple(
            statup,
            RelationGetDescr(starel),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    } else {
        init_empty_stats_tuple(
            reloid,
            attnum,
            inherited,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
        );
    }

    /* if specified, set to argument values */
    if !PG_ARGISNULL!(fcinfo, NULL_FRAC_ARG) {
        values[(Anum_pg_statistic_stanullfrac - 1) as usize] = PG_GETARG_DATUM!(fcinfo, NULL_FRAC_ARG);
        replaces[(Anum_pg_statistic_stanullfrac - 1) as usize] = true;
    }
    if !PG_ARGISNULL!(fcinfo, AVG_WIDTH_ARG) {
        values[(Anum_pg_statistic_stawidth - 1) as usize] = PG_GETARG_DATUM!(fcinfo, AVG_WIDTH_ARG);
        replaces[(Anum_pg_statistic_stawidth - 1) as usize] = true;
    }
    if !PG_ARGISNULL!(fcinfo, N_DISTINCT_ARG) {
        values[(Anum_pg_statistic_stadistinct - 1) as usize] = PG_GETARG_DATUM!(fcinfo, N_DISTINCT_ARG);
        replaces[(Anum_pg_statistic_stadistinct - 1) as usize] = true;
    }

    /* STATISTIC_KIND_MCV */
    if do_mcv {
        let converted: bool;
        let mut converted_flag: bool = false;
        let stanumbers: Datum = PG_GETARG_DATUM!(fcinfo, MOST_COMMON_FREQS_ARG);
        let stavalues: Datum = text_to_stavalues(
            c"most_common_vals".as_ptr(),
            &mut array_in_fn,
            PG_GETARG_DATUM!(fcinfo, MOST_COMMON_VALS_ARG),
            atttypid,
            atttypmod,
            &mut converted_flag,
        );
        converted = converted_flag;

        if converted {
            set_stats_slot(
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
                STATISTIC_KIND_MCV,
                eq_opr,
                atttypcoll,
                stanumbers,
                false,
                stavalues,
                false,
            );
        } else {
            result = false;
        }
    }

    /* STATISTIC_KIND_HISTOGRAM */
    if do_histogram {
        let stavalues: Datum;
        let mut converted: bool = false;

        stavalues = text_to_stavalues(
            c"histogram_bounds".as_ptr(),
            &mut array_in_fn,
            PG_GETARG_DATUM!(fcinfo, HISTOGRAM_BOUNDS_ARG),
            atttypid,
            atttypmod,
            &mut converted,
        );

        if converted {
            set_stats_slot(
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
                STATISTIC_KIND_HISTOGRAM,
                lt_opr,
                atttypcoll,
                0,
                true,
                stavalues,
                false,
            );
        } else {
            result = false;
        }
    }

    /* STATISTIC_KIND_CORRELATION */
    if do_correlation {
        let mut elems: [Datum; 1] = [PG_GETARG_DATUM!(fcinfo, CORRELATION_ARG)];
        let arry: *mut ArrayType = construct_array_builtin(elems.as_mut_ptr(), 1, FLOAT4OID);
        let stanumbers: Datum = PointerGetDatum(arry as *mut c_void);

        set_stats_slot(
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
            STATISTIC_KIND_CORRELATION,
            lt_opr,
            atttypcoll,
            stanumbers,
            false,
            0,
            true,
        );
    }

    /* STATISTIC_KIND_MCELEM */
    if do_mcelem {
        let stanumbers: Datum = PG_GETARG_DATUM!(fcinfo, MOST_COMMON_ELEM_FREQS_ARG);
        let mut converted: bool = false;
        let stavalues: Datum;

        stavalues = text_to_stavalues(
            c"most_common_elems".as_ptr(),
            &mut array_in_fn,
            PG_GETARG_DATUM!(fcinfo, MOST_COMMON_ELEMS_ARG),
            elemtypid,
            atttypmod,
            &mut converted,
        );

        if converted {
            set_stats_slot(
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
                STATISTIC_KIND_MCELEM,
                elem_eq_opr,
                atttypcoll,
                stanumbers,
                false,
                stavalues,
                false,
            );
        } else {
            result = false;
        }
    }

    /* STATISTIC_KIND_DECHIST */
    if do_dechist {
        let stanumbers: Datum = PG_GETARG_DATUM!(fcinfo, ELEM_COUNT_HISTOGRAM_ARG);

        set_stats_slot(
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
            STATISTIC_KIND_DECHIST,
            elem_eq_opr,
            atttypcoll,
            stanumbers,
            false,
            0,
            true,
        );
    }

    /*
     * STATISTIC_KIND_BOUNDS_HISTOGRAM
     *
     * This stakind appears before STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM even
     * though it is numerically greater, and all other stakinds appear in
     * numerical order. We duplicate this quirk for consistency.
     */
    if do_bounds_histogram {
        let mut converted: bool = false;
        let stavalues: Datum;

        stavalues = text_to_stavalues(
            c"range_bounds_histogram".as_ptr(),
            &mut array_in_fn,
            PG_GETARG_DATUM!(fcinfo, RANGE_BOUNDS_HISTOGRAM_ARG),
            atttypid,
            atttypmod,
            &mut converted,
        );

        if converted {
            set_stats_slot(
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
                STATISTIC_KIND_BOUNDS_HISTOGRAM,
                InvalidOid,
                InvalidOid,
                0,
                true,
                stavalues,
                false,
            );
        } else {
            result = false;
        }
    }

    /* STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM */
    if do_range_length_histogram {
        /* The anyarray is always a float8[] for this stakind */
        let mut elems: [Datum; 1] = [PG_GETARG_DATUM!(fcinfo, RANGE_EMPTY_FRAC_ARG)];
        let arry: *mut ArrayType = construct_array_builtin(elems.as_mut_ptr(), 1, FLOAT4OID);
        let stanumbers: Datum = PointerGetDatum(arry as *mut c_void);

        let mut converted: bool = false;
        let stavalues: Datum;

        stavalues = text_to_stavalues(
            c"range_length_histogram".as_ptr(),
            &mut array_in_fn,
            PG_GETARG_DATUM!(fcinfo, RANGE_LENGTH_HISTOGRAM_ARG),
            FLOAT8OID,
            0,
            &mut converted,
        );

        if converted {
            set_stats_slot(
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
                STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                Float8LessOperator,
                InvalidOid,
                stanumbers,
                false,
                stavalues,
                false,
            );
        } else {
            result = false;
        }
    }

    upsert_pg_statistic(
        starel,
        statup,
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
        replaces.as_mut_ptr(),
    );

    if HeapTupleIsValid(statup) {
        ReleaseSysCache(statup);
    }
    table_close(starel, RowExclusiveLock);

    result
}

/*
 * If this relation is an index and that index has expressions in it, and
 * the attnum specified is known to be an expression, then we must walk
 * the list attributes up to the specified attnum to get the right
 * expression.
 */
unsafe fn get_attr_expr(rel: Relation, attnum: c_int) -> *mut Node {
    let index_exprs: *mut List;
    let mut indexpr_item: *mut ListCell;

    /* relation is not an index */
    if (*(*rel).rd_rel).relkind != RELKIND_INDEX
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_INDEX
    {
        return null_mut();
    }

    index_exprs = RelationGetIndexExpressions(rel);

    /* index has no expressions to give */
    if index_exprs == NIL {
        return null_mut();
    }

    /*
     * The index attnum points directly to a relation attnum, then it's not an
     * expression attribute.
     */
    let rd_index = (*rel).rd_index as *mut FormData_pg_index_stub;
    if *(*rd_index).indkey.values.as_ptr().add((attnum - 1) as usize) != 0 {
        return null_mut();
    }

    indexpr_item = list_head((*rel).rd_indexprs);

    for i in 0..(attnum - 1) {
        if *(*rd_index).indkey.values.as_ptr().add(i as usize) == 0 {
            indexpr_item = lnext((*rel).rd_indexprs, indexpr_item);
        }
    }

    if indexpr_item.is_null() {
        /* shouldn't happen */
        elog!(ERROR, "too few entries in indexprs list");
    }

    lfirst(indexpr_item) as *mut Node
}

/*
 * Derive type information from the attribute.
 */
unsafe fn get_attr_stat_type(
    reloid: Oid,
    attnum: AttrNumber,
    atttypid: *mut Oid,
    atttypmod: *mut i32,
    atttyptype: *mut c_char,
    atttypcoll: *mut Oid,
    eq_opr: *mut Oid,
    lt_opr: *mut Oid,
) {
    let rel: Relation = relation_open(reloid, AccessShareLock);
    let attr: Form_pg_attribute;
    let atup: HeapTuple;
    let expr: *mut Node;
    let typcache: *mut TypeCacheEntry;

    atup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(reloid), Int16GetDatum(attnum));

    /* Attribute not found */
    if !HeapTupleIsValid(atup) {
        elog!(
            ERROR,
            "column {} of relation \"{}\" does not exist",
            attnum,
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    attr = GETSTRUCT(atup) as Form_pg_attribute;

    if (*attr).attisdropped {
        elog!(
            ERROR,
            "column {} of relation \"{}\" does not exist",
            attnum,
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    expr = get_attr_expr(rel, (*attr).attnum as c_int);

    /*
     * When analyzing an expression index, believe the expression tree's type
     * not the column datatype --- the latter might be the opckeytype storage
     * type of the opclass, which is not interesting for our purposes. This
     * mimics the behavior of examine_attribute().
     */
    if expr.is_null() {
        *atttypid = (*attr).atttypid;
        *atttypmod = (*attr).atttypmod;
        *atttypcoll = (*attr).attcollation;
    } else {
        *atttypid = exprType(expr);
        *atttypmod = exprTypmod(expr);

        if OidIsValid((*attr).attcollation) {
            *atttypcoll = (*attr).attcollation;
        } else {
            *atttypcoll = exprCollation(expr);
        }
    }
    ReleaseSysCache(atup);

    /*
     * If it's a multirange, step down to the range type, as is done by
     * multirange_typanalyze().
     */
    if type_is_multirange(*atttypid) {
        *atttypid = get_multirange_range(*atttypid);
    }

    /* finds the right operators even if atttypid is a domain */
    typcache = lookup_type_cache(*atttypid, TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR);
    *atttyptype = (*typcache).typtype;
    *eq_opr = (*typcache).eq_opr;
    *lt_opr = (*typcache).lt_opr;

    /*
     * Special case: collation for tsvector is DEFAULT_COLLATION_OID. See
     * compute_tsvector_stats().
     */
    if *atttypid == TSVECTOROID {
        *atttypcoll = DEFAULT_COLLATION_OID;
    }

    relation_close(rel, NoLock);
}

/*
 * Derive element type information from the attribute type.
 */
unsafe fn get_elem_stat_type(
    atttypid: Oid,
    _atttyptype: c_char,
    elemtypid: *mut Oid,
    elem_eq_opr: *mut Oid,
) -> bool {
    let elemtypcache: *mut TypeCacheEntry;

    if atttypid == TSVECTOROID {
        /*
         * Special case: element type for tsvector is text. See
         * compute_tsvector_stats().
         */
        *elemtypid = TEXTOID;
    } else {
        /* find underlying element type through any domain */
        *elemtypid = get_base_element_type(atttypid);
    }

    if !OidIsValid(*elemtypid) {
        return false;
    }

    /* finds the right operator even if elemtypid is a domain */
    elemtypcache = lookup_type_cache(*elemtypid, TYPECACHE_EQ_OPR);
    if !OidIsValid((*elemtypcache).eq_opr) {
        return false;
    }

    *elem_eq_opr = (*elemtypcache).eq_opr;

    true
}

/*
 * Cast a text datum into an array with element type elemtypid.
 *
 * If an error is encountered, capture it and re-throw a WARNING, and set ok
 * to false. If the resulting array contains NULLs, raise a WARNING and set ok
 * to false. Otherwise, set ok to true.
 */
unsafe fn text_to_stavalues(
    staname: *const c_char,
    array_in: *mut FmgrInfo,
    d: Datum,
    typid: Oid,
    typmod: i32,
    ok: *mut bool,
) -> Datum {
    LOCAL_FCINFO!(fcinfo, 8);
    let s: *mut c_char;
    let result: Datum;
    let mut escontext: ErrorSaveContext = std::mem::zeroed();
    escontext.r#type = NodeTag::T_ErrorSaveContext;

    escontext.details_wanted = true;

    s = TextDatumGetCString(d);

    InitFunctionCallInfoData!(
        fcinfo,
        array_in,
        3 as c_short,
        InvalidOid,
        &mut escontext as *mut ErrorSaveContext as *mut Node,
        null_mut()
    );

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = CStringGetDatum(s);
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typid);
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(2)).value = Int32GetDatum(typmod);
    (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;

    result = FunctionCallInvoke!(fcinfo);

    pfree(s as *mut c_void);

    if escontext.error_occurred {
        (*(escontext.error_data as *mut ErrorDataStub)).elevel = WARNING;
        ThrowErrorData(escontext.error_data);
        *ok = false;
        return 0;
    }

    if array_contains_nulls(DatumGetArrayTypeP(result)) {
        elog!(
            WARNING,
            "\"{}\" array must not contain null values",
            std::ffi::CStr::from_ptr(staname).to_string_lossy()
        );
        *ok = false;
        return 0;
    }

    *ok = true;

    result
}

/*
 * Find and update the slot with the given stakind, or use the first empty
 * slot.
 */
unsafe fn set_stats_slot(
    values: *mut Datum,
    nulls: *mut bool,
    replaces: *mut bool,
    stakind: int16,
    staop: Oid,
    stacoll: Oid,
    stanumbers: Datum,
    stanumbers_isnull: bool,
    stavalues: Datum,
    stavalues_isnull: bool,
) {
    let mut slotidx: c_int;
    let mut first_empty: c_int = -1;
    let mut stakind_attnum: AttrNumber;
    let staop_attnum: AttrNumber;
    let stacoll_attnum: AttrNumber;

    /* find existing slot with given stakind */
    slotidx = 0;
    while (slotidx as usize) < STATISTIC_NUM_SLOTS {
        stakind_attnum = (Anum_pg_statistic_stakind1 - 1 + slotidx) as AttrNumber;

        if first_empty < 0 && DatumGetInt16(*values.add(stakind_attnum as usize)) == 0 {
            first_empty = slotidx;
        }
        if DatumGetInt16(*values.add(stakind_attnum as usize)) == stakind {
            break;
        }
        slotidx += 1;
    }

    if (slotidx as usize) >= STATISTIC_NUM_SLOTS && first_empty >= 0 {
        slotidx = first_empty;
    }

    if (slotidx as usize) >= STATISTIC_NUM_SLOTS {
        elog!(ERROR, "maximum number of statistics slots exceeded: {}", slotidx + 1);
    }

    stakind_attnum = (Anum_pg_statistic_stakind1 - 1 + slotidx) as AttrNumber;
    staop_attnum = (Anum_pg_statistic_staop1 - 1 + slotidx) as AttrNumber;
    stacoll_attnum = (Anum_pg_statistic_stacoll1 - 1 + slotidx) as AttrNumber;

    if DatumGetInt16(*values.add(stakind_attnum as usize)) != stakind {
        *values.add(stakind_attnum as usize) = Int16GetDatum(stakind);
        *replaces.add(stakind_attnum as usize) = true;
    }
    if DatumGetObjectId(*values.add(staop_attnum as usize)) != staop {
        *values.add(staop_attnum as usize) = ObjectIdGetDatum(staop);
        *replaces.add(staop_attnum as usize) = true;
    }
    if DatumGetObjectId(*values.add(stacoll_attnum as usize)) != stacoll {
        *values.add(stacoll_attnum as usize) = ObjectIdGetDatum(stacoll);
        *replaces.add(stacoll_attnum as usize) = true;
    }
    if !stanumbers_isnull {
        *values.add((Anum_pg_statistic_stanumbers1 - 1 + slotidx) as usize) = stanumbers;
        *nulls.add((Anum_pg_statistic_stanumbers1 - 1 + slotidx) as usize) = false;
        *replaces.add((Anum_pg_statistic_stanumbers1 - 1 + slotidx) as usize) = true;
    }
    if !stavalues_isnull {
        *values.add((Anum_pg_statistic_stavalues1 - 1 + slotidx) as usize) = stavalues;
        *nulls.add((Anum_pg_statistic_stavalues1 - 1 + slotidx) as usize) = false;
        *replaces.add((Anum_pg_statistic_stavalues1 - 1 + slotidx) as usize) = true;
    }
}

/*
 * Upsert the pg_statistic record.
 */
unsafe fn upsert_pg_statistic(
    starel: Relation,
    oldtup: HeapTuple,
    values: *mut Datum,
    nulls: *mut bool,
    replaces: *mut bool,
) {
    let newtup: HeapTuple;

    if HeapTupleIsValid(oldtup) {
        newtup = heap_modify_tuple(oldtup, RelationGetDescr(starel), values, nulls, replaces);
        CatalogTupleUpdate(starel, &mut (*newtup).t_self, newtup);
    } else {
        newtup = heap_form_tuple(RelationGetDescr(starel), values, nulls);
        CatalogTupleInsert(starel, newtup);
    }

    heap_freetuple(newtup);

    CommandCounterIncrement();
}

/*
 * Delete pg_statistic record.
 */
unsafe fn delete_pg_statistic(reloid: Oid, attnum: AttrNumber, stainherit: bool) -> bool {
    let sd: Relation = table_open(StatisticRelationId, RowExclusiveLock);
    let oldtup: HeapTuple;
    let mut result: bool = false;

    /* Is there already a pg_statistic tuple for this attribute? */
    oldtup = SearchSysCache3(
        STATRELATTINH,
        ObjectIdGetDatum(reloid),
        Int16GetDatum(attnum),
        BoolGetDatum(stainherit),
    );

    if HeapTupleIsValid(oldtup) {
        CatalogTupleDelete(sd, &mut (*oldtup).t_self);
        ReleaseSysCache(oldtup);
        result = true;
    }

    table_close(sd, RowExclusiveLock);

    CommandCounterIncrement();

    result
}

/*
 * Initialize values and nulls for a new stats tuple.
 */
unsafe fn init_empty_stats_tuple(
    reloid: Oid,
    attnum: int16,
    inherited: bool,
    values: *mut Datum,
    nulls: *mut bool,
    replaces: *mut bool,
) {
    std::ptr::write_bytes(nulls, 1u8, Natts_pg_statistic);
    std::ptr::write_bytes(replaces, 1u8, Natts_pg_statistic);

    /* must initialize non-NULL attributes */

    *values.add((Anum_pg_statistic_starelid - 1) as usize) = ObjectIdGetDatum(reloid);
    *nulls.add((Anum_pg_statistic_starelid - 1) as usize) = false;
    *values.add((Anum_pg_statistic_staattnum - 1) as usize) = Int16GetDatum(attnum);
    *nulls.add((Anum_pg_statistic_staattnum - 1) as usize) = false;
    *values.add((Anum_pg_statistic_stainherit - 1) as usize) = BoolGetDatum(inherited);
    *nulls.add((Anum_pg_statistic_stainherit - 1) as usize) = false;

    *values.add((Anum_pg_statistic_stanullfrac - 1) as usize) = DEFAULT_NULL_FRAC;
    *nulls.add((Anum_pg_statistic_stanullfrac - 1) as usize) = false;
    *values.add((Anum_pg_statistic_stawidth - 1) as usize) = DEFAULT_AVG_WIDTH;
    *nulls.add((Anum_pg_statistic_stawidth - 1) as usize) = false;
    *values.add((Anum_pg_statistic_stadistinct - 1) as usize) = DEFAULT_N_DISTINCT;
    *nulls.add((Anum_pg_statistic_stadistinct - 1) as usize) = false;

    /* initialize stakind, staop, and stacoll slots */
    for slotnum in 0..STATISTIC_NUM_SLOTS as c_int {
        *values.add((Anum_pg_statistic_stakind1 + slotnum - 1) as usize) = 0;
        *nulls.add((Anum_pg_statistic_stakind1 + slotnum - 1) as usize) = false;
        *values.add((Anum_pg_statistic_staop1 + slotnum - 1) as usize) = InvalidOid as Datum;
        *nulls.add((Anum_pg_statistic_staop1 + slotnum - 1) as usize) = false;
        *values.add((Anum_pg_statistic_stacoll1 + slotnum - 1) as usize) = InvalidOid as Datum;
        *nulls.add((Anum_pg_statistic_stacoll1 + slotnum - 1) as usize) = false;
    }
}

/*
 * Delete statistics for the given attribute.
 */
pub unsafe fn pg_clear_attribute_stats(fcinfo: FunctionCallInfo) -> Datum {
    let nspname: *mut c_char;
    let relname: *mut c_char;
    let reloid: Oid;
    let attname: *mut c_char;
    let attnum: AttrNumber;
    let inherited: bool;
    let mut locked_table: Oid = InvalidOid;

    stats_check_required_arg(fcinfo, cleararginfo.as_ptr(), C_ATTRELSCHEMA_ARG as c_int);
    stats_check_required_arg(fcinfo, cleararginfo.as_ptr(), C_ATTRELNAME_ARG as c_int);
    stats_check_required_arg(fcinfo, cleararginfo.as_ptr(), C_ATTNAME_ARG as c_int);
    stats_check_required_arg(fcinfo, cleararginfo.as_ptr(), C_INHERITED_ARG as c_int);

    nspname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, C_ATTRELSCHEMA_ARG));
    relname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, C_ATTRELNAME_ARG));

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    reloid = RangeVarGetRelidExtended(
        makeRangeVar(nspname, relname, -1),
        ShareUpdateExclusiveLock,
        0,
        RangeVarCallbackForStats,
        &mut locked_table as *mut Oid as *mut c_void,
    );

    attname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, C_ATTNAME_ARG));
    attnum = get_attnum(reloid, attname);

    if attnum < 0 {
        elog!(
            ERROR,
            "cannot clear statistics on system column \"{}\"",
            std::ffi::CStr::from_ptr(attname).to_string_lossy()
        );
    }

    if attnum == InvalidAttrNumber {
        elog!(
            ERROR,
            "column \"{}\" of relation \"{}\" does not exist",
            std::ffi::CStr::from_ptr(attname).to_string_lossy(),
            std::ffi::CStr::from_ptr(get_rel_name(reloid)).to_string_lossy()
        );
    }

    inherited = PG_GETARG_BOOL!(fcinfo, C_INHERITED_ARG);

    delete_pg_statistic(reloid, attnum, inherited);
    PG_RETURN_VOID!();
}

/*
 * Import statistics for a given relation attribute.
 *
 * Inserts or replaces a row in pg_statistic for the given relation and
 * attribute name or number. It takes input parameters that correspond to
 * columns in the view pg_stats.
 *
 * Parameters are given in a pseudo named-attribute style: they must be
 * pairs of parameter names (as text) and values (of appropriate types).
 * We do that, rather than using regular named-parameter notation, so
 * that we can add or change parameters without fear of breaking
 * carelessly-written calls.
 *
 * Parameters null_frac, avg_width, and n_distinct all correspond to NOT NULL
 * columns in pg_statistic. The remaining parameters all belong to a specific
 * stakind. Some stakinds require multiple parameters, which must be specified
 * together (or neither specified).
 *
 * Parameters are only superficially validated. Omitting a parameter or
 * passing NULL leaves the statistic unchanged.
 *
 * Parameters corresponding to ANYARRAY columns are instead passed in as text
 * values, which is a valid input string for an array of the type or element
 * type of the attribute. Any error generated by the array_in() function will
 * in turn fail the function.
 */
pub unsafe fn pg_restore_attribute_stats(fcinfo: FunctionCallInfo) -> Datum {
    LOCAL_FCINFO!(positional_fcinfo, NUM_ATTRIBUTE_STATS_ARGS);
    let mut result: bool = true;

    InitFunctionCallInfoData!(
        positional_fcinfo,
        null_mut(),
        NUM_ATTRIBUTE_STATS_ARGS as c_short,
        InvalidOid,
        null_mut(),
        null_mut()
    );

    if !stats_fill_fcinfo_from_arg_pairs(fcinfo, positional_fcinfo, attarginfo.as_ptr()) {
        result = false;
    }

    if !attribute_statistics_update(positional_fcinfo) {
        result = false;
    }

    PG_RETURN_BOOL!(result);
}

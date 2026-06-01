/*-------------------------------------------------------------------------
 *
 * enum.c
 *	  I/O functions, operators, aggregates etc for enum types
 *
 * Copyright (c) 2006-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/enum.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;

use std::ffi::{c_char, c_int, c_short};

use crate::nodes::nodes::Node;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};

// fmgr.h argument/return helper macros (declared #[macro_export], so they live at
// the crate root).
use crate::{
    PG_ARGISNULL, PG_GETARG_CSTRING, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_INT32, PG_RETURN_OID,
};

// PG_RETURN_ARRAYTYPE_P has no definition anywhere in src/ yet; provide a faithful
// local stand-in. It returns the ArrayType pointer as a Datum (PG_RETURN_POINTER).
unsafe fn PG_RETURN_ARRAYTYPE_P(x: *mut ArrayType) -> Datum {
    x as Datum
}

// ---------------------------------------------------------------------------
// Local stub types / helpers for unported dependencies.
// ---------------------------------------------------------------------------

type HeapTuple = *mut HeapTupleData;
#[repr(C)]
pub struct HeapTupleData {
    pub t_data: *mut HeapTupleHeaderData,
}
#[repr(C)]
pub struct HeapTupleHeaderData {
    _private: [u8; 0],
}

type Form_pg_enum = *mut FormData_pg_enum;
#[repr(C)]
pub struct FormData_pg_enum {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumsortorder: f32,
    pub enumlabel: NameData,
}
#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}

type Relation = *mut RelationData;
#[repr(C)]
pub struct RelationData {
    _private: [u8; 0],
}

type SysScanDesc = *mut SysScanDescData;
#[repr(C)]
pub struct SysScanDescData {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ScanKeyData {
    _private: [u8; 0],
}

type TypeCacheEntry = TypeCacheEntryData;
#[repr(C)]
pub struct TypeCacheEntryData {
    _private: [u8; 0],
}

type ArrayType = ArrayTypeData;
#[repr(C)]
pub struct ArrayTypeData {
    _private: [u8; 0],
}

#[repr(C)]
pub struct StringInfoData {
    _private: [u8; 0],
}
type StringInfo = *mut StringInfoData;

type ScanDirection = c_int;
const ForwardScanDirection: ScanDirection = 1;
const BackwardScanDirection: ScanDirection = -1;

const NAMEDATALEN: usize = 64;
const InvalidOid: Oid = 0;

const AccessShareLock: c_int = 1;

const TYPALIGN_INT: c_char = b'i' as c_char;

// syscache ids
const ENUMOID: c_int = 0;
const ENUMTYPOIDNAME: c_int = 0;

// pg_enum index/relation oids and attribute numbers
const EnumRelationId: Oid = 3501;
const EnumTypIdSortOrderIndexId: Oid = 3534;
const Anum_pg_enum_enumtypid: c_int = 2;
const BTEqualStrategyNumber: c_short = 3;
const F_OIDEQ: Oid = 184;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: access/htup_details.h
}
unsafe fn HeapTupleHeaderXminCommitted(_td: *mut HeapTupleHeaderData) -> bool {
    unimplemented!() // TODO: access/htup_details.h
}
unsafe fn HeapTupleHeaderGetXmin(_td: *mut HeapTupleHeaderData) -> TransactionId {
    unimplemented!() // TODO: access/htup_details.h
}
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}
unsafe fn TransactionIdIsInProgress(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: storage/procarray.h
}
unsafe fn TransactionIdDidCommit(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: access/transam.h
}
unsafe fn EnumUncommitted(_oid: Oid) -> bool {
    unimplemented!() // TODO: catalog/pg_enum.h
}
unsafe fn NameStr(name: *mut NameData) -> *mut c_char {
    (*name).data.as_mut_ptr()
}
unsafe fn format_type_be(_typoid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/builtins.h
}
unsafe fn SearchSysCache1(_cacheid: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/syscache.h
}
unsafe fn SearchSysCache2(_cacheid: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/syscache.h
}
unsafe fn ReleaseSysCache(_tup: HeapTuple) {
    unimplemented!() // TODO: utils/syscache.h
}
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/typcache.h
}
unsafe fn compare_values_of_enum(_tcache: *mut TypeCacheEntry, _arg1: Oid, _arg2: Oid) -> c_int {
    unimplemented!() // TODO: utils/typcache.h
}
unsafe fn get_fn_expr_argtype(_flinfo: *mut FmgrInfo, _argnum: c_int) -> Oid {
    unimplemented!() // TODO: utils/fmgr.h
}
unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}
unsafe fn pstrdup(_in: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/palloc.h
}
unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attno: c_int,
    _strategy: c_short,
    _procedure: Oid,
    _argument: Datum,
) {
    unimplemented!() // TODO: access/skey.h
}
unsafe fn table_open(_relid: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table.h
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/table.h
}
unsafe fn index_open(_relid: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn systable_beginscan_ordered(
    _heap_rel: Relation,
    _index_rel: Relation,
    _snapshot: *mut std::ffi::c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn systable_getnext_ordered(_sysscan: SysScanDesc, _direction: ScanDirection) -> HeapTuple {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn systable_endscan_ordered(_sysscan: SysScanDesc) {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn construct_array(
    _elems: *mut Datum,
    _nelems: c_int,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
) -> *mut ArrayType {
    unimplemented!() // TODO: utils/array.h
}
unsafe fn pq_getmsgtext(_msg: StringInfo, _rawbytes: c_int, _nbytes: *mut c_int) -> *mut c_char {
    unimplemented!() // TODO: libpq/pqformat.h
}
unsafe fn pq_begintypsend(_buf: StringInfo) {
    unimplemented!() // TODO: libpq/pqformat.h
}
unsafe fn pq_sendtext(_buf: StringInfo, _str: *const c_char, _slen: c_int) {
    unimplemented!() // TODO: libpq/pqformat.h
}
unsafe fn pq_endtypsend(_buf: StringInfo) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: libpq/pqformat.h
}

// ---------------------------------------------------------------------------

/*
 * Disallow use of an uncommitted pg_enum tuple.
 *
 * We need to make sure that uncommitted enum values don't get into indexes.
 * If they did, and if we then rolled back the pg_enum addition, we'd have
 * broken the index because value comparisons will not work reliably without
 * an underlying pg_enum entry.  (Note that removal of the heap entry
 * containing an enum value is not sufficient to ensure that it doesn't appear
 * in upper levels of indexes.)  To do this we prevent an uncommitted row from
 * being used for any SQL-level purpose.  This is stronger than necessary,
 * since the value might not be getting inserted into a table or there might
 * be no index on its column, but it's easy to enforce centrally.
 *
 * However, it's okay to allow use of uncommitted values belonging to enum
 * types that were themselves created in the same transaction, because then
 * any such index would also be new and would go away altogether on rollback.
 * We don't implement that fully right now, but we do allow free use of enum
 * values created during CREATE TYPE AS ENUM, which are surely of the same
 * lifespan as the enum type.  (This case is required by "pg_restore -1".)
 * Values added by ALTER TYPE ADD VALUE are also allowed if the enum type
 * is known to have been created earlier in the same transaction.  (Note that
 * we have to track that explicitly; comparing tuple xmins is insufficient,
 * because the type tuple might have been updated in the current transaction.
 * Subtransactions also create hazards to be accounted for; currently,
 * pg_enum.c only handles ADD VALUE at the outermost transaction level.)
 *
 * This function needs to be called (directly or indirectly) in any of the
 * functions below that could return an enum value to SQL operations.
 */
unsafe fn check_safe_enum_use(enumval_tup: HeapTuple) {
    let xmin: TransactionId;
    let en: Form_pg_enum = GETSTRUCT(enumval_tup) as Form_pg_enum;

    /*
     * If the row is hinted as committed, it's surely safe.  This provides a
     * fast path for all normal use-cases.
     */
    if HeapTupleHeaderXminCommitted((*enumval_tup).t_data) {
        return;
    }

    /*
     * Usually, a row would get hinted as committed when it's read or loaded
     * into syscache; but just in case not, let's check the xmin directly.
     */
    xmin = HeapTupleHeaderGetXmin((*enumval_tup).t_data);
    if !TransactionIdIsInProgress(xmin) && TransactionIdDidCommit(xmin) {
        return;
    }

    /*
     * Check if the enum value is listed as uncommitted.  If not, it's safe,
     * because it can't be shorter-lived than its owning type.  (This'd also
     * be false for values made by other transactions; but the previous tests
     * should have handled all of those.)
     */
    if !EnumUncommitted((*en).oid) {
        return;
    }

    /*
     * There might well be other tests we could do here to narrow down the
     * unsafe conditions, but for now just raise an exception.
     */
    elog!(
        ERROR,
        "unsafe use of new value \"{}\" of enum type {}",
        cstr_to_display(NameStr(&mut (*en).enumlabel)),
        cstr_to_display(format_type_be((*en).enumtypid))
    );
    // errhint: New enum values must be committed before they can be used.
}

// Helper to render a C string for elog! formatting.
unsafe fn cstr_to_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() {
        return std::borrow::Cow::Borrowed("");
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy()
}

/* Basic I/O support */

#[no_mangle]
pub unsafe extern "C" fn enum_in(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let enumtypoid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let escontext: *mut Node = (*fcinfo).context;
    let enumoid: Oid;
    let tup: HeapTuple;

    /* must check length to prevent Assert failure within SearchSysCache */
    if strlen(name) >= NAMEDATALEN {
        // ereturn(escontext, (Datum) 0, ERRCODE_INVALID_TEXT_REPRESENTATION,
        //   "invalid input value for enum %s: \"%s\"", format_type_be(enumtypoid), name)
        if errsave_start(escontext) {
            elog!(
                ERROR,
                "invalid input value for enum {}: \"{}\"",
                cstr_to_display(format_type_be(enumtypoid)),
                cstr_to_display(name)
            );
        }
        return 0 as Datum;
    }

    tup = SearchSysCache2(
        ENUMTYPOIDNAME,
        ObjectIdGetDatum(enumtypoid),
        CStringGetDatum(name),
    );
    if !HeapTupleIsValid(tup) {
        if errsave_start(escontext) {
            elog!(
                ERROR,
                "invalid input value for enum {}: \"{}\"",
                cstr_to_display(format_type_be(enumtypoid)),
                cstr_to_display(name)
            );
        }
        return 0 as Datum;
    }

    /*
     * Check it's safe to use in SQL.  Perhaps we should take the trouble to
     * report "unsafe use" softly; but it's unclear that it's worth the
     * trouble, or indeed that that is a legitimate bad-input case at all
     * rather than an implementation shortcoming.
     */
    check_safe_enum_use(tup);

    /*
     * This comes from pg_enum.oid and stores system oids in user tables. This
     * oid must be preserved by binary upgrades.
     */
    enumoid = (*(GETSTRUCT(tup) as Form_pg_enum)).oid;

    ReleaseSysCache(tup);

    PG_RETURN_OID!(enumoid)
}

#[no_mangle]
pub unsafe extern "C" fn enum_out(fcinfo: FunctionCallInfo) -> Datum {
    let enumval: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char;
    let tup: HeapTuple;
    let en: Form_pg_enum;

    tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumval));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "invalid internal value for enum: {}", enumval);
        unreachable!();
    }
    en = GETSTRUCT(tup) as Form_pg_enum;

    result = pstrdup(NameStr(&mut (*en).enumlabel));

    ReleaseSysCache(tup);

    PG_RETURN_CSTRING!(result)
}

/* Binary I/O support */
#[no_mangle]
pub unsafe extern "C" fn enum_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let enumtypoid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let enumoid: Oid;
    let tup: HeapTuple;
    let name: *mut c_char;
    let mut nbytes: c_int = 0;

    name = pq_getmsgtext(buf, stringinfo_remaining(buf), &mut nbytes);

    /* must check length to prevent Assert failure within SearchSysCache */
    if strlen(name) >= NAMEDATALEN {
        elog!(
            ERROR,
            "invalid input value for enum {}: \"{}\"",
            cstr_to_display(format_type_be(enumtypoid)),
            cstr_to_display(name)
        );
        unreachable!();
    }

    tup = SearchSysCache2(
        ENUMTYPOIDNAME,
        ObjectIdGetDatum(enumtypoid),
        CStringGetDatum(name),
    );
    if !HeapTupleIsValid(tup) {
        elog!(
            ERROR,
            "invalid input value for enum {}: \"{}\"",
            cstr_to_display(format_type_be(enumtypoid)),
            cstr_to_display(name)
        );
        unreachable!();
    }

    /* check it's safe to use in SQL */
    check_safe_enum_use(tup);

    enumoid = (*(GETSTRUCT(tup) as Form_pg_enum)).oid;

    ReleaseSysCache(tup);

    pfree(name as *mut std::ffi::c_void);

    PG_RETURN_OID!(enumoid)
}

#[no_mangle]
pub unsafe extern "C" fn enum_send(fcinfo: FunctionCallInfo) -> Datum {
    let enumval: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let tup: HeapTuple;
    let en: Form_pg_enum;

    tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumval));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "invalid internal value for enum: {}", enumval);
        unreachable!();
    }
    en = GETSTRUCT(tup) as Form_pg_enum;

    pq_begintypsend(&mut buf);
    pq_sendtext(
        &mut buf,
        NameStr(&mut (*en).enumlabel),
        strlen(NameStr(&mut (*en).enumlabel)) as c_int,
    );

    ReleaseSysCache(tup);

    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf))
}

/* Comparison functions and related */

/*
 * enum_cmp_internal is the common engine for all the visible comparison
 * functions, except for enum_eq and enum_ne which can just check for OID
 * equality directly.
 */
unsafe fn enum_cmp_internal(arg1: Oid, arg2: Oid, fcinfo: FunctionCallInfo) -> c_int {
    let tcache: *mut TypeCacheEntry;

    /*
     * We don't need the typcache except in the hopefully-uncommon case that
     * one or both Oids are odd.  This means that cursory testing of code that
     * fails to pass flinfo to an enum comparison function might not disclose
     * the oversight.  To make such errors more obvious, Assert that we have a
     * place to cache even when we take a fast-path exit.
     */
    Assert!(!(*fcinfo).flinfo.is_null());

    /* Equal OIDs are equal no matter what */
    if arg1 == arg2 {
        return 0;
    }

    /* Fast path: even-numbered Oids are known to compare correctly */
    if (arg1 & 1) == 0 && (arg2 & 1) == 0 {
        if arg1 < arg2 {
            return -1;
        } else {
            return 1;
        }
    }

    /* Locate the typcache entry for the enum type */
    let mut tcache = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if tcache.is_null() {
        let enum_tup: HeapTuple;
        let en: Form_pg_enum;
        let typeoid: Oid;

        /* Get the OID of the enum type containing arg1 */
        enum_tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(arg1));
        if !HeapTupleIsValid(enum_tup) {
            elog!(ERROR, "invalid internal value for enum: {}", arg1);
            unreachable!();
        }
        en = GETSTRUCT(enum_tup) as Form_pg_enum;
        typeoid = (*en).enumtypid;
        ReleaseSysCache(enum_tup);
        /* Now locate and remember the typcache entry */
        tcache = lookup_type_cache(typeoid, 0);
        (*(*fcinfo).flinfo).fn_extra = tcache as *mut std::ffi::c_void;
    }

    /* The remaining comparison logic is in typcache.c */
    compare_values_of_enum(tcache, arg1, arg2)
}

#[no_mangle]
pub unsafe extern "C" fn enum_lt(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(enum_cmp_internal(a, b, fcinfo) < 0)
}

#[no_mangle]
pub unsafe extern "C" fn enum_le(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(enum_cmp_internal(a, b, fcinfo) <= 0)
}

#[no_mangle]
pub unsafe extern "C" fn enum_eq(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(a == b)
}

#[no_mangle]
pub unsafe extern "C" fn enum_ne(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(a != b)
}

#[no_mangle]
pub unsafe extern "C" fn enum_ge(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(enum_cmp_internal(a, b, fcinfo) >= 0)
}

#[no_mangle]
pub unsafe extern "C" fn enum_gt(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(enum_cmp_internal(a, b, fcinfo) > 0)
}

#[no_mangle]
pub unsafe extern "C" fn enum_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_OID!(if enum_cmp_internal(a, b, fcinfo) < 0 { a } else { b })
}

#[no_mangle]
pub unsafe extern "C" fn enum_larger(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_OID!(if enum_cmp_internal(a, b, fcinfo) > 0 { a } else { b })
}

#[no_mangle]
pub unsafe extern "C" fn enum_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_INT32!(enum_cmp_internal(a, b, fcinfo))
}

/* Enum programming support functions */

/*
 * enum_endpoint: common code for enum_first/enum_last
 */
unsafe fn enum_endpoint(enumtypoid: Oid, direction: ScanDirection) -> Oid {
    let enum_rel: Relation;
    let enum_idx: Relation;
    let enum_scan: SysScanDesc;
    let enum_tuple: HeapTuple;
    let mut skey: ScanKeyData = std::mem::zeroed();
    let minmax: Oid;

    /*
     * Find the first/last enum member using pg_enum_typid_sortorder_index.
     * Note we must not use the syscache.  See comments for RenumberEnumType
     * in catalog/pg_enum.c for more info.
     */
    ScanKeyInit(
        &mut skey,
        Anum_pg_enum_enumtypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(enumtypoid),
    );

    enum_rel = table_open(EnumRelationId, AccessShareLock);
    enum_idx = index_open(EnumTypIdSortOrderIndexId, AccessShareLock);
    enum_scan = systable_beginscan_ordered(
        enum_rel,
        enum_idx,
        std::ptr::null_mut(),
        1,
        &mut skey,
    );

    enum_tuple = systable_getnext_ordered(enum_scan, direction);
    if HeapTupleIsValid(enum_tuple) {
        /* check it's safe to use in SQL */
        check_safe_enum_use(enum_tuple);
        minmax = (*(GETSTRUCT(enum_tuple) as Form_pg_enum)).oid;
    } else {
        /* should only happen with an empty enum */
        minmax = InvalidOid;
    }

    systable_endscan_ordered(enum_scan);
    index_close(enum_idx, AccessShareLock);
    table_close(enum_rel, AccessShareLock);

    minmax
}

#[no_mangle]
pub unsafe extern "C" fn enum_first(fcinfo: FunctionCallInfo) -> Datum {
    let enumtypoid: Oid;
    let min: Oid;

    /*
     * We rely on being able to get the specific enum type from the calling
     * expression tree.  Notice that the actual value of the argument isn't
     * examined at all; in particular it might be NULL.
     */
    enumtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    if enumtypoid == InvalidOid {
        elog!(ERROR, "could not determine actual enum type");
        unreachable!();
    }

    /* Get the OID using the index */
    min = enum_endpoint(enumtypoid, ForwardScanDirection);

    if !OidIsValid(min) {
        elog!(
            ERROR,
            "enum {} contains no values",
            cstr_to_display(format_type_be(enumtypoid))
        );
        unreachable!();
    }

    PG_RETURN_OID!(min)
}

#[no_mangle]
pub unsafe extern "C" fn enum_last(fcinfo: FunctionCallInfo) -> Datum {
    let enumtypoid: Oid;
    let max: Oid;

    /*
     * We rely on being able to get the specific enum type from the calling
     * expression tree.  Notice that the actual value of the argument isn't
     * examined at all; in particular it might be NULL.
     */
    enumtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    if enumtypoid == InvalidOid {
        elog!(ERROR, "could not determine actual enum type");
        unreachable!();
    }

    /* Get the OID using the index */
    max = enum_endpoint(enumtypoid, BackwardScanDirection);

    if !OidIsValid(max) {
        elog!(
            ERROR,
            "enum {} contains no values",
            cstr_to_display(format_type_be(enumtypoid))
        );
        unreachable!();
    }

    PG_RETURN_OID!(max)
}

/* 2-argument variant of enum_range */
#[no_mangle]
pub unsafe extern "C" fn enum_range_bounds(fcinfo: FunctionCallInfo) -> Datum {
    let lower: Oid;
    let upper: Oid;
    let enumtypoid: Oid;

    if PG_ARGISNULL!(fcinfo, 0) {
        lower = InvalidOid;
    } else {
        lower = PG_GETARG_OID!(fcinfo, 0);
    }
    if PG_ARGISNULL!(fcinfo, 1) {
        upper = InvalidOid;
    } else {
        upper = PG_GETARG_OID!(fcinfo, 1);
    }

    /*
     * We rely on being able to get the specific enum type from the calling
     * expression tree.  The generic type mechanism should have ensured that
     * both are of the same type.
     */
    enumtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    if enumtypoid == InvalidOid {
        elog!(ERROR, "could not determine actual enum type");
        unreachable!();
    }

    PG_RETURN_ARRAYTYPE_P(enum_range_internal(enumtypoid, lower, upper))
}

/* 1-argument variant of enum_range */
#[no_mangle]
pub unsafe extern "C" fn enum_range_all(fcinfo: FunctionCallInfo) -> Datum {
    let enumtypoid: Oid;

    /*
     * We rely on being able to get the specific enum type from the calling
     * expression tree.  Notice that the actual value of the argument isn't
     * examined at all; in particular it might be NULL.
     */
    enumtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    if enumtypoid == InvalidOid {
        elog!(ERROR, "could not determine actual enum type");
        unreachable!();
    }

    PG_RETURN_ARRAYTYPE_P(enum_range_internal(enumtypoid, InvalidOid, InvalidOid))
}

unsafe fn enum_range_internal(enumtypoid: Oid, lower: Oid, upper: Oid) -> *mut ArrayType {
    let result: *mut ArrayType;
    let enum_rel: Relation;
    let enum_idx: Relation;
    let enum_scan: SysScanDesc;
    let mut enum_tuple: HeapTuple;
    let mut skey: ScanKeyData = std::mem::zeroed();
    let mut elems: *mut Datum;
    let mut max: c_int;
    let mut cnt: c_int;
    let mut left_found: bool;

    /*
     * Scan the enum members in order using pg_enum_typid_sortorder_index.
     * Note we must not use the syscache.  See comments for RenumberEnumType
     * in catalog/pg_enum.c for more info.
     */
    ScanKeyInit(
        &mut skey,
        Anum_pg_enum_enumtypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(enumtypoid),
    );

    enum_rel = table_open(EnumRelationId, AccessShareLock);
    enum_idx = index_open(EnumTypIdSortOrderIndexId, AccessShareLock);
    enum_scan = systable_beginscan_ordered(
        enum_rel,
        enum_idx,
        std::ptr::null_mut(),
        1,
        &mut skey,
    );

    max = 64;
    elems = palloc(max as Size * std::mem::size_of::<Datum>()) as *mut Datum;
    cnt = 0;
    left_found = !OidIsValid(lower);

    loop {
        enum_tuple = systable_getnext_ordered(enum_scan, ForwardScanDirection);
        if !HeapTupleIsValid(enum_tuple) {
            break;
        }

        let enum_oid: Oid = (*(GETSTRUCT(enum_tuple) as Form_pg_enum)).oid;

        if !left_found && lower == enum_oid {
            left_found = true;
        }

        if left_found {
            /* check it's safe to use in SQL */
            check_safe_enum_use(enum_tuple);

            if cnt >= max {
                max *= 2;
                elems = repalloc(
                    elems as *mut std::ffi::c_void,
                    max as Size * std::mem::size_of::<Datum>(),
                ) as *mut Datum;
            }

            *elems.offset(cnt as isize) = ObjectIdGetDatum(enum_oid);
            cnt += 1;
        }

        if OidIsValid(upper) && upper == enum_oid {
            break;
        }
    }

    systable_endscan_ordered(enum_scan);
    index_close(enum_idx, AccessShareLock);
    table_close(enum_rel, AccessShareLock);

    /* and build the result array */
    /* note this hardwires some details about the representation of Oid */
    result = construct_array(
        elems,
        cnt,
        enumtypoid,
        std::mem::size_of::<Oid>() as c_int,
        true,
        TYPALIGN_INT,
    );

    pfree(elems as *mut std::ffi::c_void);

    result
}

// ---------------------------------------------------------------------------
// Additional local stubs for soft-error / pqformat helpers used above.
// ---------------------------------------------------------------------------

// errsave/ereturn machinery: returns whether a hard error should be raised.
unsafe fn errsave_start(_escontext: *mut Node) -> bool {
    unimplemented!() // TODO: utils/elog.h (errsave/ereturn)
}

// buf->len - buf->cursor in pq_getmsgtext call.
unsafe fn stringinfo_remaining(_buf: StringInfo) -> c_int {
    unimplemented!() // TODO: lib/stringinfo.h
}

unsafe fn repalloc(_pointer: *mut std::ffi::c_void, _size: Size) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: utils/palloc.h
}

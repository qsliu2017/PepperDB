//! src/backend/utils/cache/lsyscache.c
//!
//! Convenience routines for common queries in the system catalog cache.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES
//!   Eventually, the index information should go through here, too.

use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::catalog::pg_amop::{Form_pg_amop, FormData_pg_amop};
use crate::catalog::pg_amproc::{Form_pg_amproc, FormData_pg_amproc};
use crate::catalog::pg_attribute::{Form_pg_attribute, FormData_pg_attribute};
use crate::catalog::pg_cast::{Form_pg_cast, FormData_pg_cast};
use crate::catalog::pg_class::{Form_pg_class, FormData_pg_class};
use crate::catalog::pg_collation::{Form_pg_collation, FormData_pg_collation};
use crate::catalog::pg_constraint::{
    Form_pg_constraint, FormData_pg_constraint, CONSTRAINT_EXCLUSION, CONSTRAINT_PRIMARY,
    CONSTRAINT_UNIQUE,
};
use crate::catalog::pg_index::{Form_pg_index, FormData_pg_index};
use crate::catalog::pg_language::{Form_pg_language, FormData_pg_language};
use crate::catalog::pg_namespace::{Form_pg_namespace, FormData_pg_namespace};
use crate::catalog::pg_opclass::{Form_pg_opclass, FormData_pg_opclass};
use crate::catalog::pg_opfamily::{Form_pg_opfamily, FormData_pg_opfamily};
use crate::catalog::pg_operator::{Form_pg_operator, FormData_pg_operator};
use crate::catalog::pg_proc::{Form_pg_proc, FormData_pg_proc};
use crate::catalog::pg_publication::{Form_pg_publication, FormData_pg_publication};
use crate::catalog::pg_range::{Form_pg_range, FormData_pg_range};
use crate::catalog::pg_statistic::{Form_pg_statistic, FormData_pg_statistic};
use crate::catalog::pg_subscription::{Form_pg_subscription, FormData_pg_subscription};
use crate::catalog::pg_transform::{Form_pg_transform, FormData_pg_transform};
use crate::catalog::pg_type::{Form_pg_type, FormData_pg_type};
use crate::postgres_ext::Oid;
use crate::utils::cache::syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache, SearchSysCache1, SearchSysCache2,
    SearchSysCache3, SearchSysCache4, SearchSysCacheAttName, SearchSysCacheExists,
    SearchSysCacheList, SysCacheGetAttr, SysCacheGetAttrNotNull,
};

/* Hook for plugins to get control in get_attavgwidth() */
pub type get_attavgwidth_hook_type =
    Option<unsafe fn(relid: Oid, attnum: AttrNumber) -> i32>;

pub static mut get_attavgwidth_hook: get_attavgwidth_hook_type = None;

/* ---------- Syscache ID stubs (catalog/syscache_ids.h, generated) ---------- */
/* TODO(pg-port): replace with generated constants once syscache_ids.h is ported */
const AMOPOPID: c_int = 0;
const AMOPSTRATEGY: c_int = 0;
const AMPROCNUM: c_int = 0;
const ATTNAME: c_int = 0;
const ATTNUM: c_int = 0;
const CASTSOURCETARGET: c_int = 0;
const CLAOID: c_int = 0;
const COLLOID: c_int = 0;
const CONSTROID: c_int = 0;
const INDEXRELID: c_int = 0;
const LANGOID: c_int = 0;
const NAMESPACEOID: c_int = 0;
const OPEROID: c_int = 0;
const OPFAMILYOID: c_int = 0;
const PROCOID: c_int = 0;
const PUBLICATIONNAME: c_int = 0;
const PUBLICATIONOID: c_int = 0;
const RANGETYPE: c_int = 0;
const RANGEMULTIRANGE: c_int = 0;
const RELOID: c_int = 0;
const RELNAMENSP: c_int = 0;
const STATRELATTINH: c_int = 0;
const SUBSCRIPTIONNAME: c_int = 0;
const SUBSCRIPTIONOID: c_int = 0;
const TRFTYPELANG: c_int = 0;
const TYPEOID: c_int = 0;

/* ---------- catcache list type stub ---------- */
/* TODO(pg-port): real CatCList from utils/cache/catcache.h */
#[repr(C)]
pub struct CatCList {
    _private: [u8; 0],
}

/* ---------- local stubs ---------- */

/// TODO(pg-port): SearchSysCacheExists3 -- see utils/cache/syscache.h
#[inline]
pub unsafe fn SearchSysCacheExists3(
    cache_id: c_int,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> bool {
    SearchSysCacheExists(cache_id, key1, key2, key3, 0 as Datum)
}

/// TODO(pg-port): SearchSysCacheList1 -- see utils/cache/syscache.h
#[inline]
pub unsafe fn SearchSysCacheList1(cache_id: c_int, key1: Datum) -> *mut CatCList {
    SearchSysCacheList(cache_id, 1, key1, 0 as Datum, 0 as Datum)
        as *mut CatCList
}

/// TODO(pg-port): ReleaseSysCacheList -- ReleaseCatCacheList in utils/cache/catcache.h
#[inline]
pub unsafe fn ReleaseSysCacheList(_list: *mut CatCList) {
    /* TODO(pg-port): call ReleaseCatCacheList */
}

/// TODO(pg-port): GetSysCacheOid1 -- see utils/cache/syscache.h
#[inline]
pub unsafe fn GetSysCacheOid1(
    cache_id: c_int,
    oid_col: AttrNumber,
    key1: Datum,
) -> Oid {
    GetSysCacheOid(
        cache_id,
        oid_col,
        key1,
        0 as Datum,
        0 as Datum,
        0 as Datum,
    )
}

/// TODO(pg-port): GetSysCacheOid2 -- see utils/cache/syscache.h
#[inline]
pub unsafe fn GetSysCacheOid2(
    cache_id: c_int,
    oid_col: AttrNumber,
    key1: Datum,
    key2: Datum,
) -> Oid {
    GetSysCacheOid(
        cache_id,
        oid_col,
        key1,
        key2,
        0 as Datum,
        0 as Datum,
    )
}

/* ---------- catclist member access helpers (stubs) ---------- */
/* TODO(pg-port): real catclist layout from utils/cache/catcache.h */

/// Returns the number of members in a CatCList.
/// TODO(pg-port): read from real catclist.n_members field.
#[inline]
unsafe fn catclist_n_members(_list: *mut CatCList) -> c_int {
    0
}

/// Returns the i-th member tuple of a CatCList.
/// TODO(pg-port): index into real catclist.members[] array.
#[inline]
unsafe fn catclist_member(_list: *mut CatCList, _i: c_int) -> HeapTuple {
    core::ptr::null_mut()
}

/* ---------- stubs for functions called from this file ---------- */

/// TODO(pg-port): access/index/amapi.h / GetIndexAmRoutineByAmId
pub unsafe fn GetIndexAmRoutineByAmId(
    _amoid: Oid,
    _noerror: bool,
) -> *mut IndexAmRoutine {
    core::ptr::null_mut()
}

/// Stub for IndexAmRoutine.  TODO(pg-port): access/amapi.h
#[repr(C)]
pub struct IndexAmRoutine {
    pub amcanorder: bool,
    pub amconsistentequality: bool,
    pub amconsistentordering: bool,
}

/// TODO(pg-port): access/cmptype.h
pub type CompareType = c_int;
pub const COMPARE_INVALID: CompareType = 0;
pub const COMPARE_LT: CompareType = 1;
pub const COMPARE_GT: CompareType = 2;
pub const COMPARE_EQ: CompareType = 3;
pub const COMPARE_NE: CompareType = 4;

/// TODO(pg-port): IndexAmTranslateStrategy (access/amapi.h / nbtree)
pub unsafe fn IndexAmTranslateStrategy(
    _strategy: i16,
    _amoid: Oid,
    _opfamily: Oid,
    _missing_ok: bool,
) -> CompareType {
    COMPARE_INVALID
}

/// TODO(pg-port): IndexAmTranslateCompareType (access/amapi.h)
pub unsafe fn IndexAmTranslateCompareType(
    _cmptype: CompareType,
    _amoid: Oid,
    _opfamily: Oid,
    _missing_ok: bool,
) -> i16 {
    0
}

/// AM OID constants.  TODO(pg-port): catalog/pg_am_d.h (generated)
pub const BTREE_AM_OID: Oid = 403;
pub const HASH_AM_OID: Oid = 405;
pub const GIST_AM_OID: Oid = 783;
pub const GIN_AM_OID: Oid = 2742;
pub const SPGIST_AM_OID: Oid = 4000;
pub const BRIN_AM_OID: Oid = 3580;

/// HTEqualStrategyNumber.  TODO(pg-port): access/stratnum.h
pub const HTEqualStrategyNumber: i16 = 1;

/// HASHSTANDARD_PROC / HASHEXTENDED_PROC.  TODO(pg-port): access/hash/hashutil.h
pub const HASHSTANDARD_PROC: i16 = 1;

/// OpIndexInterpretation.  TODO(pg-port): utils/lsyscache.h
#[repr(C)]
pub struct OpIndexInterpretation {
    pub opfamily_id: Oid,
    pub cmptype: CompareType,
    pub oplefttype: Oid,
    pub oprighttype: Oid,
}

/// List type stub.  TODO(pg-port): nodes/pg_list.h
pub type List = c_void;
pub const NIL: *mut List = core::ptr::null_mut();

/// lappend_oid stub.  TODO(pg-port): nodes/pg_list.h
pub unsafe fn lappend_oid(list: *mut List, datum: Oid) -> *mut List {
    let _ = datum;
    list
}

/// lappend stub.  TODO(pg-port): nodes/pg_list.h
pub unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List {
    let _ = datum;
    list
}

/// list_member_oid stub.  TODO(pg-port): nodes/pg_list.h
pub unsafe fn list_member_oid(_list: *const List, _oid: Oid) -> bool {
    false
}

/// palloc stub.  TODO(pg-port): utils/palloc.h
pub unsafe fn palloc(size: usize) -> *mut c_void {
    libc_malloc(size)
}

unsafe fn libc_malloc(size: usize) -> *mut c_void {
    extern "C" {
        fn malloc(size: usize) -> *mut c_void;
    }
    malloc(size)
}

/// pfree stub.  TODO(pg-port): utils/palloc.h
pub unsafe fn pfree(ptr: *mut c_void) {
    extern "C" {
        fn free(ptr: *mut c_void);
    }
    free(ptr);
}

/// pstrdup stub.  TODO(pg-port): utils/palloc.h
pub unsafe fn pstrdup(s: *const c_char) -> *mut c_char {
    extern "C" {
        fn strlen(s: *const c_char) -> usize;
        fn malloc(n: usize) -> *mut c_void;
        fn memcpy(d: *mut c_void, s: *const c_void, n: usize) -> *mut c_void;
    }
    let n = strlen(s) + 1;
    let d = malloc(n) as *mut c_char;
    memcpy(d as *mut c_void, s as *const c_void, n);
    d
}

/// IOFuncSelector.  TODO(pg-port): utils/lsyscache.h
pub type IOFuncSelector = c_int;
pub const IOFunc_input: IOFuncSelector = 0;
pub const IOFunc_output: IOFuncSelector = 1;
pub const IOFunc_receive: IOFuncSelector = 2;
pub const IOFunc_send: IOFuncSelector = 3;

/// AttStatsSlot.  TODO(pg-port): utils/lsyscache.h
#[repr(C)]
pub struct AttStatsSlot {
    pub staop: Oid,
    pub stacoll: Oid,
    pub valuetype: Oid,
    pub values: *mut Datum,
    pub nvalues: c_int,
    pub numbers: *mut f32,
    pub nnumbers: c_int,
    pub values_arr: *mut ArrayType,
    pub numbers_arr: *mut ArrayType,
}

/// ArrayType stub.  TODO(pg-port): utils/array.h
pub type ArrayType = c_void;

/// AttrNumber column stubs for catalog tables used here.
/// TODO(pg-port): catalog/*_d.h (generated)
pub const Anum_pg_cast_oid: AttrNumber = 1;
pub const Anum_pg_class_oid: AttrNumber = 1;
pub const Anum_pg_publication_oid: AttrNumber = 1;
pub const Anum_pg_subscription_oid: AttrNumber = 1;
pub const Anum_pg_attribute_attoptions: AttrNumber = 1;
pub const Anum_pg_type_typdefaultbin: AttrNumber = 1;
pub const Anum_pg_type_typdefault: AttrNumber = 2;
pub const Anum_pg_statistic_stavalues1: AttrNumber = 1;
pub const Anum_pg_statistic_stanumbers1: AttrNumber = 1;
pub const Anum_pg_index_indclass: AttrNumber = 1;

pub const STATISTIC_NUM_SLOTS: c_int = 5;
pub const ATTSTATSSLOT_VALUES: c_int = 0x01;
pub const ATTSTATSSLOT_NUMBERS: c_int = 0x02;

/* Inline helpers for Datum conversions (partial stubs) */
/* TODO(pg-port): real implementations in postgres.h / fmgr.h */

#[inline]
pub unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum {
    (oid as Datum)
}

#[inline]
pub unsafe fn CharGetDatum(c: c_char) -> Datum {
    (c as Datum)
}

#[inline]
pub unsafe fn Int16GetDatum(v: i16) -> Datum {
    (v as Datum)
}

#[inline]
pub unsafe fn Int32GetDatum(v: i32) -> Datum {
    (v as Datum)
}

#[inline]
pub unsafe fn BoolGetDatum(v: bool) -> Datum {
    (v as Datum)
}

#[inline]
pub unsafe fn PointerGetDatum(p: *const c_void) -> Datum {
    (p as Datum)
}

#[inline]
pub unsafe fn CStringGetDatum(s: *const c_char) -> Datum {
    (s as Datum)
}

// DatumGetPointer/DatumGetObjectId come from the prelude.

#[inline]
pub unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

pub const InvalidOid: Oid = 0;

/// Node / Const stubs.  TODO(pg-port): nodes/nodes.h
pub type Node = c_void;

/// oidvector stub.  TODO(pg-port): catalog/pg_type.h
#[repr(C)]
pub struct oidvector {
    pub dim1: c_int,
    pub values: [Oid; 1], /* variable length */
}

/* Misc stubs */

/// TODO(pg-port): utils/builtins.h
pub unsafe fn format_type_be(_typid: Oid) -> *const c_char {
    b"?\0".as_ptr() as *const c_char
}

/// TODO(pg-port): utils/builtins.h
pub unsafe fn stringToNode(_str: *const c_char) -> *mut Node {
    core::ptr::null_mut()
}

/// TODO(pg-port): utils/builtins.h
pub unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    core::ptr::null_mut()
}

/// TODO(pg-port): utils/array.h
pub unsafe fn DatumGetArrayTypePCopy(_d: Datum) -> *mut ArrayType {
    core::ptr::null_mut()
}

/// TODO(pg-port): utils/array.h
pub unsafe fn ARR_ELEMTYPE(_arr: *const ArrayType) -> Oid {
    0
}

/// TODO(pg-port): utils/array.h
pub unsafe fn ARR_NDIM(_arr: *const ArrayType) -> c_int {
    0
}

/// TODO(pg-port): utils/array.h
pub unsafe fn ARR_DIMS(_arr: *const ArrayType) -> *const c_int {
    core::ptr::null()
}

/// TODO(pg-port): utils/array.h
pub unsafe fn ARR_HASNULL(_arr: *const ArrayType) -> bool {
    false
}

/// TODO(pg-port): utils/array.h
pub unsafe fn ARR_DATA_PTR(_arr: *mut ArrayType) -> *mut u8 {
    core::ptr::null_mut()
}

/// TODO(pg-port): utils/array.h
pub unsafe fn deconstruct_array(
    _arr: *mut ArrayType,
    _elemtype: Oid,
    _elemlen: i16,
    _elembyval: bool,
    _elemalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
}

/// TODO(pg-port): utils/datum.h
pub unsafe fn datumCopy(_val: Datum, _typbyval: bool, _typlen: i32) -> Datum {
    0 as Datum
}

/// TODO(pg-port): utils/fmgrprotos.h
pub unsafe fn OidInputFunctionCall(
    _func: Oid,
    _str: *mut c_char,
    _typioparam: Oid,
    _typmod: i32,
) -> Datum {
    0 as Datum
}

/// TODO(pg-port): nodes/makefuncs.h
pub unsafe fn makeConst(
    _consttype: Oid,
    _consttypmod: i32,
    _constcollid: Oid,
    _constlen: i16,
    _constvalue: Datum,
    _constisnull: bool,
    _constbyval: bool,
) -> *mut Node {
    core::ptr::null_mut()
}

/// TODO(pg-port): utils/builtins.h
pub unsafe fn type_maximum_size(_typid: Oid, _typmod: i32) -> i32 {
    0
}

/// TODO(pg-port): utils/typcache.h
pub unsafe fn lookup_type_cache(_typid: Oid, _flags: c_int) -> *mut crate::utils::cache::typcache::TypeCacheEntry {
    core::ptr::null_mut()
}

pub const TYPECACHE_CMP_PROC: c_int = 0x0010;
pub const TYPECACHE_HASH_PROC: c_int = 0x0020;

/// TODO(pg-port): catalog/pg_type.h - TYPTYPE_* constants
pub const TYPTYPE_COMPOSITE: c_char = b'c' as c_char;
pub const TYPTYPE_DOMAIN: c_char = b'd' as c_char;
pub const TYPTYPE_ENUM: c_char = b'e' as c_char;
pub const TYPTYPE_RANGE: c_char = b'r' as c_char;
pub const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char;

/// TODO(pg-port): catalog/pg_type.h - TYPSTORAGE_PLAIN
pub const TYPSTORAGE_PLAIN: c_char = b'p' as c_char;

/// TODO(pg-port): catalog/pg_type.h - TYPALIGN_INT
pub const TYPALIGN_INT: c_char = b'i' as c_char;

/// TODO(pg-port): catalog/pg_type.h
pub unsafe fn IsTrueArrayType(_typ: *const FormData_pg_type) -> bool {
    false
}

/// TODO(pg-port): catalog/pg_known_oids.h / pg_type_d.h
pub const BPCHAROID: Oid = 1042;
pub const RECORDOID: Oid = 2249;
pub const FLOAT4OID: Oid = 700;
pub const ARRAY_EQ_OP: Oid = 375;
pub const RECORD_EQ_OP: Oid = 2988;

/// F_BTARRAYCMP etc.  TODO(pg-port): utils/fmgroids.h (generated)
pub const F_BTARRAYCMP: Oid = 0;
pub const F_BTRECORDCMP: Oid = 0;
pub const F_HASH_ARRAY: Oid = 0;
pub const F_HASH_RECORD: Oid = 0;

/// TODO(pg-port): miscadmin.h
pub unsafe fn IsBootstrapProcessingMode() -> bool {
    false
}

/// TODO(pg-port): bootstrap/bootstrap.h
pub unsafe fn boot_get_type_io_data(
    _typid: Oid,
    _typlen: *mut i16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
    _typdelim: *mut c_char,
    _typioparam: *mut Oid,
    _typinput: *mut Oid,
    _typoutput: *mut Oid,
) {
}

/// TODO(pg-port): catalog/namespace.h
pub unsafe fn isTempNamespace(_nspid: Oid) -> bool {
    false
}

/// TODO(pg-port): utils/SubscriptRoutines (utils/array.h / executor/execExpr.h)
pub type SubscriptRoutines = c_void;

/// TODO(pg-port): utils/fmgr.h
pub unsafe fn OidFunctionCall0(_funcid: Oid) -> Datum {
    0 as Datum
}

// NameStr comes from crate::c (takes &NameData).
use crate::c::NameStr;

/// InvalidAttrNumber.  TODO(pg-port): access/attnum.h
pub const InvalidAttrNumber: AttrNumber = 0;

/// MyDatabaseId.  TODO(pg-port): miscadmin.h
pub unsafe fn MyDatabaseId_datum() -> Datum {
    ((crate::miscadmin::MyDatabaseId as u64) as Datum)
}

/*              ---------- AMOP CACHES ----------                             */

/*
 * op_in_opfamily
 *
 *      Return t iff operator 'opno' is in operator family 'opfamily'.
 *
 * This function only considers search operators, not ordering operators.
 */
pub unsafe fn op_in_opfamily(opno: Oid, opfamily: Oid) -> bool {
    SearchSysCacheExists3(
        AMOPOPID,
        ObjectIdGetDatum(opno),
        CharGetDatum(AMOP_SEARCH),
        ObjectIdGetDatum(opfamily),
    )
}

/// AMOP_SEARCH / AMOP_ORDER constants.  TODO(pg-port): catalog/pg_amop.h
pub const AMOP_SEARCH: c_char = b's' as c_char;
pub const AMOP_ORDER: c_char = b'o' as c_char;

/*
 * get_op_opfamily_strategy
 *
 *      Get the operator's strategy number within the specified opfamily,
 *      or 0 if it's not a member of the opfamily.
 *
 * This function only considers search operators, not ordering operators.
 */
pub unsafe fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> c_int {
    let tp: HeapTuple;
    let amop_tup: Form_pg_amop;
    let result: c_int;

    tp = SearchSysCache3(
        AMOPOPID,
        ObjectIdGetDatum(opno),
        CharGetDatum(AMOP_SEARCH),
        ObjectIdGetDatum(opfamily),
    );
    if !HeapTupleIsValid(tp) {
        return 0;
    }
    amop_tup = GETSTRUCT(tp) as Form_pg_amop;
    result = (*amop_tup).amopstrategy as c_int;
    ReleaseSysCache(tp);
    result
}

/*
 * get_op_opfamily_sortfamily
 *
 *      If the operator is an ordering operator within the specified opfamily,
 *      return its amopsortfamily OID; else return InvalidOid.
 */
pub unsafe fn get_op_opfamily_sortfamily(opno: Oid, opfamily: Oid) -> Oid {
    let tp: HeapTuple;
    let amop_tup: Form_pg_amop;
    let result: Oid;

    tp = SearchSysCache3(
        AMOPOPID,
        ObjectIdGetDatum(opno),
        CharGetDatum(AMOP_ORDER),
        ObjectIdGetDatum(opfamily),
    );
    if !HeapTupleIsValid(tp) {
        return InvalidOid;
    }
    amop_tup = GETSTRUCT(tp) as Form_pg_amop;
    result = (*amop_tup).amopsortfamily;
    ReleaseSysCache(tp);
    result
}

/*
 * get_op_opfamily_properties
 *
 *      Get the operator's strategy number and declared input data types
 *      within the specified opfamily.
 *
 * Caller should already have verified that opno is a member of opfamily,
 * therefore we raise an error if the tuple is not found.
 */
pub unsafe fn get_op_opfamily_properties(
    opno: Oid,
    opfamily: Oid,
    ordering_op: bool,
    strategy: *mut c_int,
    lefttype: *mut Oid,
    righttype: *mut Oid,
) {
    let tp: HeapTuple;
    let amop_tup: Form_pg_amop;

    tp = SearchSysCache3(
        AMOPOPID,
        ObjectIdGetDatum(opno),
        CharGetDatum(if ordering_op { AMOP_ORDER } else { AMOP_SEARCH }),
        ObjectIdGetDatum(opfamily),
    );
    if !HeapTupleIsValid(tp) {
        elog!(
            ERROR,
            "operator {} is not a member of opfamily {}",
            opno,
            opfamily
        );
    }
    amop_tup = GETSTRUCT(tp) as Form_pg_amop;
    *strategy = (*amop_tup).amopstrategy as c_int;
    *lefttype = (*amop_tup).amoplefttype;
    *righttype = (*amop_tup).amoprighttype;
    ReleaseSysCache(tp);
}

/*
 * get_opfamily_member
 *      Get the OID of the operator that implements the specified strategy
 *      with the specified datatypes for the specified opfamily.
 *
 * Returns InvalidOid if there is no pg_amop entry for the given keys.
 */
pub unsafe fn get_opfamily_member(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> Oid {
    let tp: HeapTuple;
    let amop_tup: Form_pg_amop;
    let result: Oid;

    tp = SearchSysCache4(
        AMOPSTRATEGY,
        ObjectIdGetDatum(opfamily),
        ObjectIdGetDatum(lefttype),
        ObjectIdGetDatum(righttype),
        Int16GetDatum(strategy),
    );
    if !HeapTupleIsValid(tp) {
        return InvalidOid;
    }
    amop_tup = GETSTRUCT(tp) as Form_pg_amop;
    result = (*amop_tup).amopopr;
    ReleaseSysCache(tp);
    result
}

/*
 * get_opfamily_member_for_cmptype
 *      Get the OID of the operator that implements the specified comparison
 *      type with the specified datatypes for the specified opfamily.
 *
 * Returns InvalidOid if there is no mapping for the comparison type or no
 * pg_amop entry for the given keys.
 */
pub unsafe fn get_opfamily_member_for_cmptype(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    cmptype: CompareType,
) -> Oid {
    let opmethod: Oid;
    let strategy: i16;

    opmethod = get_opfamily_method(opfamily);
    strategy = IndexAmTranslateCompareType(cmptype, opmethod, opfamily, true);
    if strategy == 0 {
        return InvalidOid;
    }
    get_opfamily_member(opfamily, lefttype, righttype, strategy)
}

/*
 * get_opmethod_canorder
 *      Return amcanorder field for given index AM.
 *
 * To speed things up in the common cases, we're hardcoding the results from
 * the built-in index types.
 */
unsafe fn get_opmethod_canorder(amoid: Oid) -> bool {
    match amoid {
        x if x == BTREE_AM_OID => true,
        x if x == HASH_AM_OID || x == GIST_AM_OID || x == GIN_AM_OID
            || x == SPGIST_AM_OID || x == BRIN_AM_OID => false,
        _ => {
            let result: bool;
            let amroutine: *mut IndexAmRoutine = GetIndexAmRoutineByAmId(amoid, false);
            result = (*amroutine).amcanorder;
            pfree(amroutine as *mut c_void);
            result
        }
    }
}

/*
 * get_ordering_op_properties
 *      Given the OID of an ordering operator (a "<" or ">" operator),
 *      determine its opfamily, its declared input datatype, and its
 *      comparison type.
 *
 * Returns true if successful, false if no matching pg_amop entry exists.
 */
pub unsafe fn get_ordering_op_properties(
    opno: Oid,
    opfamily: *mut Oid,
    opcintype: *mut Oid,
    cmptype: *mut CompareType,
) -> bool {
    let mut result: bool = false;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* ensure outputs are initialized on failure */
    *opfamily = InvalidOid;
    *opcintype = InvalidOid;
    *cmptype = COMPARE_INVALID;

    /*
     * Search pg_amop to see if the target operator is registered as the "<"
     * or ">" operator of any btree opfamily.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    i = 0;
    while i < catclist_n_members(catlist) {
        let tuple: HeapTuple = catclist_member(catlist, i);
        let aform: Form_pg_amop = GETSTRUCT(tuple) as Form_pg_amop;
        let am_cmptype: CompareType;

        /* must be ordering index */
        if !get_opmethod_canorder((*aform).amopmethod) {
            i += 1;
            continue;
        }

        am_cmptype = IndexAmTranslateStrategy(
            (*aform).amopstrategy,
            (*aform).amopmethod,
            (*aform).amopfamily,
            true,
        );

        if am_cmptype == COMPARE_LT || am_cmptype == COMPARE_GT {
            /* Found it ... should have consistent input types */
            if (*aform).amoplefttype == (*aform).amoprighttype {
                /* Found a suitable opfamily, return info */
                *opfamily = (*aform).amopfamily;
                *opcintype = (*aform).amoplefttype;
                *cmptype = am_cmptype;
                result = true;
                break;
            }
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result
}

/*
 * get_equality_op_for_ordering_op
 *      Get the OID of the datatype-specific equality operator
 *      associated with an ordering operator (a "<" or ">" operator).
 *
 * If "reverse" isn't NULL, also set *reverse to false if the operator is "<",
 * true if it's ">"
 *
 * Returns InvalidOid if no matching equality operator can be found.
 */
pub unsafe fn get_equality_op_for_ordering_op(opno: Oid, reverse: *mut bool) -> Oid {
    let mut result: Oid = InvalidOid;
    let mut opfamily: Oid = InvalidOid;
    let mut opcintype: Oid = InvalidOid;
    let mut cmptype: CompareType = COMPARE_INVALID;

    /* Find the operator in pg_amop */
    if get_ordering_op_properties(opno, &mut opfamily, &mut opcintype, &mut cmptype) {
        /* Found a suitable opfamily, get matching equality operator */
        result = get_opfamily_member_for_cmptype(
            opfamily,
            opcintype,
            opcintype,
            COMPARE_EQ,
        );
        if !reverse.is_null() {
            *reverse = cmptype == COMPARE_GT;
        }
    }

    result
}

/*
 * get_ordering_op_for_equality_op
 *      Get the OID of a datatype-specific "less than" ordering operator
 *      associated with an equality operator.
 *
 * Returns InvalidOid if no matching ordering operator can be found.
 */
pub unsafe fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> Oid {
    let mut result: Oid = InvalidOid;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any btree opfamily.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    i = 0;
    while i < catclist_n_members(catlist) {
        let tuple: HeapTuple = catclist_member(catlist, i);
        let aform: Form_pg_amop = GETSTRUCT(tuple) as Form_pg_amop;
        let cmptype: CompareType;

        /* must be ordering index */
        if !get_opmethod_canorder((*aform).amopmethod) {
            i += 1;
            continue;
        }

        cmptype = IndexAmTranslateStrategy(
            (*aform).amopstrategy,
            (*aform).amopmethod,
            (*aform).amopfamily,
            true,
        );
        if cmptype == COMPARE_EQ {
            /* Found a suitable opfamily, get matching ordering operator */
            let typid: Oid;

            typid = if use_lhs_type {
                (*aform).amoplefttype
            } else {
                (*aform).amoprighttype
            };
            result = get_opfamily_member_for_cmptype(
                (*aform).amopfamily,
                typid,
                typid,
                COMPARE_LT,
            );
            if OidIsValid(result) {
                break;
            }
            /* failure probably shouldn't happen, but keep looking if so */
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result
}

/*
 * get_mergejoin_opfamilies
 *      Given a putatively mergejoinable operator, return a list of the OIDs
 *      of the amcanorder opfamilies in which it represents equality.
 *
 * It is possible (though at present unusual) for an operator to be equality
 * in more than one opfamily, hence the result is a list.
 */
pub unsafe fn get_mergejoin_opfamilies(opno: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any opfamily of an ordering index type.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    i = 0;
    while i < catclist_n_members(catlist) {
        let tuple: HeapTuple = catclist_member(catlist, i);
        let aform: Form_pg_amop = GETSTRUCT(tuple) as Form_pg_amop;

        /* must be ordering index equality */
        if get_opmethod_canorder((*aform).amopmethod)
            && IndexAmTranslateStrategy(
                (*aform).amopstrategy,
                (*aform).amopmethod,
                (*aform).amopfamily,
                true,
            ) == COMPARE_EQ
        {
            result = lappend_oid(result, (*aform).amopfamily);
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result
}

/*
 * get_compatible_hash_operators
 *      Get the OID(s) of hash equality operator(s) compatible with the given
 *      operator, but operating on its LHS and/or RHS datatype.
 *
 * Returns true if able to find the requested operator(s), false if not.
 */
pub unsafe fn get_compatible_hash_operators(
    opno: Oid,
    lhs_opno: *mut Oid,
    rhs_opno: *mut Oid,
) -> bool {
    let mut result: bool = false;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* Ensure output args are initialized on failure */
    if !lhs_opno.is_null() {
        *lhs_opno = InvalidOid;
    }
    if !rhs_opno.is_null() {
        *rhs_opno = InvalidOid;
    }

    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any hash opfamily.  If the operator is registered in
     * multiple opfamilies, assume we can use any one.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    i = 0;
    while i < catclist_n_members(catlist) {
        let tuple: HeapTuple = catclist_member(catlist, i);
        let aform: Form_pg_amop = GETSTRUCT(tuple) as Form_pg_amop;

        if (*aform).amopmethod == HASH_AM_OID
            && (*aform).amopstrategy == HTEqualStrategyNumber
        {
            /* No extra lookup needed if given operator is single-type */
            if (*aform).amoplefttype == (*aform).amoprighttype {
                if !lhs_opno.is_null() {
                    *lhs_opno = opno;
                }
                if !rhs_opno.is_null() {
                    *rhs_opno = opno;
                }
                result = true;
                break;
            }

            /*
             * Get the matching single-type operator(s).  Failure probably
             * shouldn't happen --- it implies a bogus opfamily --- but
             * continue looking if so.
             */
            if !lhs_opno.is_null() {
                *lhs_opno = get_opfamily_member(
                    (*aform).amopfamily,
                    (*aform).amoplefttype,
                    (*aform).amoplefttype,
                    HTEqualStrategyNumber,
                );
                if !OidIsValid(*lhs_opno) {
                    i += 1;
                    continue;
                }
                /* Matching LHS found, done if caller doesn't want RHS */
                if rhs_opno.is_null() {
                    result = true;
                    break;
                }
            }
            if !rhs_opno.is_null() {
                *rhs_opno = get_opfamily_member(
                    (*aform).amopfamily,
                    (*aform).amoprighttype,
                    (*aform).amoprighttype,
                    HTEqualStrategyNumber,
                );
                if !OidIsValid(*rhs_opno) {
                    /* Forget any LHS operator from this opfamily */
                    if !lhs_opno.is_null() {
                        *lhs_opno = InvalidOid;
                    }
                    i += 1;
                    continue;
                }
                /* Matching RHS found, so done */
                result = true;
                break;
            }
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result
}

/*
 * get_op_hash_functions
 *      Get the OID(s) of the standard hash support function(s) compatible with
 *      the given operator, operating on its LHS and/or RHS datatype as required.
 *
 * Returns true if able to find the requested function(s), false if not.
 */
pub unsafe fn get_op_hash_functions(
    opno: Oid,
    lhs_procno: *mut Oid,
    rhs_procno: *mut Oid,
) -> bool {
    let mut result: bool = false;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* Ensure output args are initialized on failure */
    if !lhs_procno.is_null() {
        *lhs_procno = InvalidOid;
    }
    if !rhs_procno.is_null() {
        *rhs_procno = InvalidOid;
    }

    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any hash opfamily.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    i = 0;
    while i < catclist_n_members(catlist) {
        let tuple: HeapTuple = catclist_member(catlist, i);
        let aform: Form_pg_amop = GETSTRUCT(tuple) as Form_pg_amop;

        if (*aform).amopmethod == HASH_AM_OID
            && (*aform).amopstrategy == HTEqualStrategyNumber
        {
            /*
             * Get the matching support function(s).  Failure probably
             * shouldn't happen --- it implies a bogus opfamily --- but
             * continue looking if so.
             */
            if !lhs_procno.is_null() {
                *lhs_procno = get_opfamily_proc(
                    (*aform).amopfamily,
                    (*aform).amoplefttype,
                    (*aform).amoplefttype,
                    HASHSTANDARD_PROC,
                );
                if !OidIsValid(*lhs_procno) {
                    i += 1;
                    continue;
                }
                /* Matching LHS found, done if caller doesn't want RHS */
                if rhs_procno.is_null() {
                    result = true;
                    break;
                }
                /* Only one lookup needed if given operator is single-type */
                if (*aform).amoplefttype == (*aform).amoprighttype {
                    *rhs_procno = *lhs_procno;
                    result = true;
                    break;
                }
            }
            if !rhs_procno.is_null() {
                *rhs_procno = get_opfamily_proc(
                    (*aform).amopfamily,
                    (*aform).amoprighttype,
                    (*aform).amoprighttype,
                    HASHSTANDARD_PROC,
                );
                if !OidIsValid(*rhs_procno) {
                    /* Forget any LHS function from this opfamily */
                    if !lhs_procno.is_null() {
                        *lhs_procno = InvalidOid;
                    }
                    i += 1;
                    continue;
                }
                /* Matching RHS found, so done */
                result = true;
                break;
            }
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result
}

/*
 * get_op_index_interpretation
 *      Given an operator's OID, find out which amcanorder opfamilies it belongs
 *      to, and what properties it has within each one.  The results are returned
 *      as a palloc'd list of OpIndexInterpretation structs.
 *
 * In addition to the normal btree operators, we consider a <> operator to be
 * a "member" of an opfamily if its negator is an equality operator of the
 * opfamily.  COMPARE_NE is returned as the strategy number for this case.
 */
pub unsafe fn get_op_index_interpretation(opno: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let mut thisresult: *mut OpIndexInterpretation;
    let mut catlist: *mut CatCList;
    let mut i: c_int;

    /*
     * Find all the pg_amop entries containing the operator.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    i = 0;
    while i < catclist_n_members(catlist) {
        let op_tuple: HeapTuple = catclist_member(catlist, i);
        let op_form: Form_pg_amop = GETSTRUCT(op_tuple) as Form_pg_amop;
        let cmptype: CompareType;

        /* must be ordering index */
        if !get_opmethod_canorder((*op_form).amopmethod) {
            i += 1;
            continue;
        }

        /* Get the operator's comparison type */
        cmptype = IndexAmTranslateStrategy(
            (*op_form).amopstrategy,
            (*op_form).amopmethod,
            (*op_form).amopfamily,
            true,
        );

        /* should not happen */
        if cmptype == COMPARE_INVALID {
            i += 1;
            continue;
        }

        thisresult =
            palloc(core::mem::size_of::<OpIndexInterpretation>()) as *mut OpIndexInterpretation;
        (*thisresult).opfamily_id = (*op_form).amopfamily;
        (*thisresult).cmptype = cmptype;
        (*thisresult).oplefttype = (*op_form).amoplefttype;
        (*thisresult).oprighttype = (*op_form).amoprighttype;
        result = lappend(result, thisresult as *mut c_void);
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    /*
     * If we didn't find any btree opfamily containing the operator, perhaps
     * it is a <> operator.  See if it has a negator that is in an opfamily.
     */
    if result.is_null() {
        let op_negator: Oid = get_negator(opno);

        if OidIsValid(op_negator) {
            catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(op_negator));

            i = 0;
            while i < catclist_n_members(catlist) {
                let op_tuple: HeapTuple = catclist_member(catlist, i);
                let op_form: Form_pg_amop = GETSTRUCT(op_tuple) as Form_pg_amop;
                let amroutine: *mut IndexAmRoutine =
                    GetIndexAmRoutineByAmId((*op_form).amopmethod, false);
                let cmptype: CompareType;

                /* must be ordering index */
                if !(*amroutine).amcanorder {
                    i += 1;
                    continue;
                }

                /* Get the operator's comparison type */
                cmptype = IndexAmTranslateStrategy(
                    (*op_form).amopstrategy,
                    (*op_form).amopmethod,
                    (*op_form).amopfamily,
                    true,
                );

                /* Only consider negators that are = */
                if cmptype != COMPARE_EQ {
                    i += 1;
                    continue;
                }

                /* OK, report it as COMPARE_NE */
                thisresult = palloc(core::mem::size_of::<OpIndexInterpretation>())
                    as *mut OpIndexInterpretation;
                (*thisresult).opfamily_id = (*op_form).amopfamily;
                (*thisresult).cmptype = COMPARE_NE;
                (*thisresult).oplefttype = (*op_form).amoplefttype;
                (*thisresult).oprighttype = (*op_form).amoprighttype;
                result = lappend(result, thisresult as *mut c_void);
                i += 1;
            }

            ReleaseSysCacheList(catlist);
        }
    }

    result
}

/*
 * equality_ops_are_compatible
 *      Return true if the two given equality operators have compatible
 *      semantics.
 */
pub unsafe fn equality_ops_are_compatible(opno1: Oid, opno2: Oid) -> bool {
    let result: bool;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* Easy if they're the same operator */
    if opno1 == opno2 {
        return true;
    }

    /*
     * We search through all the pg_amop entries for opno1.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno1));

    let mut found = false;
    i = 0;
    while i < catclist_n_members(catlist) {
        let op_tuple: HeapTuple = catclist_member(catlist, i);
        let op_form: Form_pg_amop = GETSTRUCT(op_tuple) as Form_pg_amop;

        /*
         * op_in_opfamily() is cheaper than GetIndexAmRoutineByAmId(), so
         * check it first
         */
        if op_in_opfamily(opno2, (*op_form).amopfamily) {
            let amroutine: *mut IndexAmRoutine =
                GetIndexAmRoutineByAmId((*op_form).amopmethod, false);

            if (*amroutine).amconsistentequality {
                found = true;
                break;
            }
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result = found;
    result
}

/*
 * comparison_ops_are_compatible
 *      Return true if the two given comparison operators have compatible
 *      semantics.
 *
 * (This is identical to equality_ops_are_compatible(), except that we check
 * amconsistentordering instead of amconsistentequality.)
 */
pub unsafe fn comparison_ops_are_compatible(opno1: Oid, opno2: Oid) -> bool {
    let result: bool;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* Easy if they're the same operator */
    if opno1 == opno2 {
        return true;
    }

    /*
     * We search through all the pg_amop entries for opno1.
     */
    catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno1));

    let mut found = false;
    i = 0;
    while i < catclist_n_members(catlist) {
        let op_tuple: HeapTuple = catclist_member(catlist, i);
        let op_form: Form_pg_amop = GETSTRUCT(op_tuple) as Form_pg_amop;

        /*
         * op_in_opfamily() is cheaper than GetIndexAmRoutineByAmId(), so
         * check it first
         */
        if op_in_opfamily(opno2, (*op_form).amopfamily) {
            let amroutine: *mut IndexAmRoutine =
                GetIndexAmRoutineByAmId((*op_form).amopmethod, false);

            if (*amroutine).amconsistentordering {
                found = true;
                break;
            }
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    result = found;
    result
}


/*              ---------- AMPROC CACHES ----------                            */

/*
 * get_opfamily_proc
 *      Get the OID of the specified support function
 *      for the specified opfamily and datatypes.
 *
 * Returns InvalidOid if there is no pg_amproc entry for the given keys.
 */
pub unsafe fn get_opfamily_proc(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    procnum: i16,
) -> Oid {
    let tp: HeapTuple;
    let amproc_tup: Form_pg_amproc;
    let result: Oid;

    tp = SearchSysCache4(
        AMPROCNUM,
        ObjectIdGetDatum(opfamily),
        ObjectIdGetDatum(lefttype),
        ObjectIdGetDatum(righttype),
        Int16GetDatum(procnum),
    );
    if !HeapTupleIsValid(tp) {
        return InvalidOid;
    }
    amproc_tup = GETSTRUCT(tp) as Form_pg_amproc;
    result = (*amproc_tup).amproc;
    ReleaseSysCache(tp);
    result
}


/*              ---------- ATTRIBUTE CACHES ----------                         */

/*
 * get_attname
 *      Given the relation id and the attribute number, return the "attname"
 *      field from the attribute relation as a palloc'ed string.
 *
 * If no such attribute exists and missing_ok is true, NULL is returned;
 * otherwise a not-intended-for-user-consumption error is thrown.
 */
pub unsafe fn get_attname(
    relid: Oid,
    attnum: AttrNumber,
    missing_ok: bool,
) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if HeapTupleIsValid(tp) {
        let att_tup: Form_pg_attribute = GETSTRUCT(tp) as Form_pg_attribute;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*att_tup).attname));
        ReleaseSysCache(tp);
        return result;
    }

    if !missing_ok {
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            relid
        );
    }
    core::ptr::null_mut()
}

/*
 * get_attnum
 *
 *      Given the relation id and the attribute name,
 *      return the "attnum" field from the attribute relation.
 *
 *      Returns InvalidAttrNumber if the attr doesn't exist (or is dropped).
 */
pub unsafe fn get_attnum(relid: Oid, attname: *const c_char) -> AttrNumber {
    let tp: HeapTuple;

    tp = SearchSysCacheAttName(relid, attname);
    if HeapTupleIsValid(tp) {
        let att_tup: Form_pg_attribute = GETSTRUCT(tp) as Form_pg_attribute;
        let result: AttrNumber;

        result = (*att_tup).attnum;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidAttrNumber;
    }
}

/*
 * get_attgenerated
 *
 *      Given the relation id and the attribute number,
 *      return the "attgenerated" field from the attribute relation.
 *
 *      Errors if not found.
 *
 *      Since not generated is represented by '\0', this can also be used as a
 *      Boolean test.
 */
pub unsafe fn get_attgenerated(relid: Oid, attnum: AttrNumber) -> c_char {
    let tp: HeapTuple;
    let att_tup: Form_pg_attribute;
    let result: c_char;

    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if !HeapTupleIsValid(tp) {
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            relid
        );
    }
    att_tup = GETSTRUCT(tp) as Form_pg_attribute;
    result = (*att_tup).attgenerated;
    ReleaseSysCache(tp);
    result
}

/*
 * get_atttype
 *
 *      Given the relation OID and the attribute number with the relation,
 *      return the attribute type OID.
 */
pub unsafe fn get_atttype(relid: Oid, attnum: AttrNumber) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if HeapTupleIsValid(tp) {
        let att_tup: Form_pg_attribute = GETSTRUCT(tp) as Form_pg_attribute;
        let result: Oid;

        result = (*att_tup).atttypid;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_atttypetypmodcoll
 *
 *      A three-fer: given the relation id and the attribute number,
 *      fetch atttypid, atttypmod, and attcollation in a single cache lookup.
 *
 * Unlike the otherwise-similar get_atttype, this routine
 * raises an error if it can't obtain the information.
 */
pub unsafe fn get_atttypetypmodcoll(
    relid: Oid,
    attnum: AttrNumber,
    typid: *mut Oid,
    typmod: *mut i32,
    collid: *mut Oid,
) {
    let tp: HeapTuple;
    let att_tup: Form_pg_attribute;

    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if !HeapTupleIsValid(tp) {
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            relid
        );
    }
    att_tup = GETSTRUCT(tp) as Form_pg_attribute;

    *typid = (*att_tup).atttypid;
    *typmod = (*att_tup).atttypmod;
    *collid = (*att_tup).attcollation;
    ReleaseSysCache(tp);
}

/*
 * get_attoptions
 *
 *      Given the relation id and the attribute number,
 *      return the attribute options text[] datum, if any.
 */
pub unsafe fn get_attoptions(relid: Oid, attnum: i16) -> Datum {
    let tuple: HeapTuple;
    let attopts: Datum;
    let result: Datum;
    let mut isnull: bool = false;

    tuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));

    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            relid
        );
    }

    attopts = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions, &mut isnull);

    if isnull {
        result = 0 as Datum;
    } else {
        result = datumCopy(attopts, false, -1); /* text[] */
    }

    ReleaseSysCache(tuple);

    result
}

/*              ---------- PG_CAST CACHE ----------                            */

/*
 * get_cast_oid - given two type OIDs, look up a cast OID
 *
 * If missing_ok is false, throw an error if the cast is not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_cast_oid(
    sourcetypeid: Oid,
    targettypeid: Oid,
    missing_ok: bool,
) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid2(
        CASTSOURCETARGET,
        Anum_pg_cast_oid,
        ObjectIdGetDatum(sourcetypeid),
        ObjectIdGetDatum(targettypeid),
    );
    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "cast from type {} to type {} does not exist",
                std::ffi::CStr::from_ptr(format_type_be(sourcetypeid)).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(targettypeid)).to_string_lossy()
            )) /* C also: errcode */;
    }
    oid
}

/*              ---------- COLLATION CACHE ----------                          */

/*
 * get_collation_name
 *      Returns the name of a given pg_collation entry.
 *
 * Returns a palloc'd copy of the string, or NULL if no such collation.
 *
 * NOTE: since collation name is not unique, be wary of code that uses this
 * for anything except preparing error messages.
 */
pub unsafe fn get_collation_name(colloid: Oid) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(colloid));
    if HeapTupleIsValid(tp) {
        let colltup: Form_pg_collation = GETSTRUCT(tp) as Form_pg_collation;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*colltup).collname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return core::ptr::null_mut();
    }
}

pub unsafe fn get_collation_isdeterministic(colloid: Oid) -> bool {
    let tp: HeapTuple;
    let colltup: Form_pg_collation;
    let result: bool;

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(colloid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for collation {}", colloid);
    }
    colltup = GETSTRUCT(tp) as Form_pg_collation;
    result = (*colltup).collisdeterministic;
    ReleaseSysCache(tp);
    result
}

/*              ---------- CONSTRAINT CACHE ----------                         */

/*
 * get_constraint_name
 *      Returns the name of a given pg_constraint entry.
 *
 * Returns a palloc'd copy of the string, or NULL if no such constraint.
 *
 * NOTE: since constraint name is not unique, be wary of code that uses this
 * for anything except preparing error messages.
 */
pub unsafe fn get_constraint_name(conoid: Oid) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
    if HeapTupleIsValid(tp) {
        let contup: Form_pg_constraint = GETSTRUCT(tp) as Form_pg_constraint;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*contup).conname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return core::ptr::null_mut();
    }
}

/*
 * get_constraint_index
 *      Given the OID of a unique, primary-key, or exclusion constraint,
 *      return the OID of the underlying index.
 *
 * Returns InvalidOid if the constraint could not be found or is of
 * the wrong type.
 */
pub unsafe fn get_constraint_index(conoid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
    if HeapTupleIsValid(tp) {
        let contup: Form_pg_constraint = GETSTRUCT(tp) as Form_pg_constraint;
        let result: Oid;

        if (*contup).contype == CONSTRAINT_UNIQUE
            || (*contup).contype == CONSTRAINT_PRIMARY
            || (*contup).contype == CONSTRAINT_EXCLUSION
        {
            result = (*contup).conindid;
        } else {
            result = InvalidOid;
        }
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_constraint_type
 *      Return the pg_constraint.contype value for the given constraint.
 *
 * No frills.
 */
pub unsafe fn get_constraint_type(conoid: Oid) -> c_char {
    let tp: HeapTuple;
    let contype: c_char;

    tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for constraint {}", conoid);
    }

    contype = (*(GETSTRUCT(tp) as Form_pg_constraint)).contype;
    ReleaseSysCache(tp);

    contype
}

/*              ---------- LANGUAGE CACHE ----------                           */

pub unsafe fn get_language_name(langoid: Oid, missing_ok: bool) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(LANGOID, ObjectIdGetDatum(langoid));
    if HeapTupleIsValid(tp) {
        let lantup: Form_pg_language = GETSTRUCT(tp) as Form_pg_language;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*lantup).lanname));
        ReleaseSysCache(tp);
        return result;
    }

    if !missing_ok {
        elog!(ERROR, "cache lookup failed for language {}", langoid);
    }
    core::ptr::null_mut()
}

/*              ---------- OPCLASS CACHE ----------                            */

/*
 * get_opclass_family
 *
 *      Returns the OID of the operator family the opclass belongs to.
 */
pub unsafe fn get_opclass_family(opclass: Oid) -> Oid {
    let tp: HeapTuple;
    let cla_tup: Form_pg_opclass;
    let result: Oid;

    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for opclass {}", opclass);
    }
    cla_tup = GETSTRUCT(tp) as Form_pg_opclass;

    result = (*cla_tup).opcfamily;
    ReleaseSysCache(tp);
    result
}

/*
 * get_opclass_input_type
 *
 *      Returns the OID of the datatype the opclass indexes.
 */
pub unsafe fn get_opclass_input_type(opclass: Oid) -> Oid {
    let tp: HeapTuple;
    let cla_tup: Form_pg_opclass;
    let result: Oid;

    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for opclass {}", opclass);
    }
    cla_tup = GETSTRUCT(tp) as Form_pg_opclass;

    result = (*cla_tup).opcintype;
    ReleaseSysCache(tp);
    result
}

/*
 * get_opclass_opfamily_and_input_type
 *
 *      Returns the OID of the operator family the opclass belongs to,
 *              the OID of the datatype the opclass indexes
 */
pub unsafe fn get_opclass_opfamily_and_input_type(
    opclass: Oid,
    opfamily: *mut Oid,
    opcintype: *mut Oid,
) -> bool {
    let tp: HeapTuple;
    let cla_tup: Form_pg_opclass;

    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if !HeapTupleIsValid(tp) {
        return false;
    }

    cla_tup = GETSTRUCT(tp) as Form_pg_opclass;

    *opfamily = (*cla_tup).opcfamily;
    *opcintype = (*cla_tup).opcintype;

    ReleaseSysCache(tp);

    true
}

/*
 * get_opclass_method
 *
 *      Returns the OID of the index access method the opclass belongs to.
 */
pub unsafe fn get_opclass_method(opclass: Oid) -> Oid {
    let tp: HeapTuple;
    let cla_tup: Form_pg_opclass;
    let result: Oid;

    tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for opclass {}", opclass);
    }
    cla_tup = GETSTRUCT(tp) as Form_pg_opclass;

    result = (*cla_tup).opcmethod;
    ReleaseSysCache(tp);
    result
}

/*              ---------- OPFAMILY CACHE ----------                           */

/*
 * get_opfamily_method
 *
 *      Returns the OID of the index access method the opfamily is for.
 */
pub unsafe fn get_opfamily_method(opfid: Oid) -> Oid {
    let tp: HeapTuple;
    let opfform: Form_pg_opfamily;
    let result: Oid;

    tp = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for operator family {}", opfid);
    }
    opfform = GETSTRUCT(tp) as Form_pg_opfamily;

    result = (*opfform).opfmethod;
    ReleaseSysCache(tp);
    result
}

pub unsafe fn get_opfamily_name(opfid: Oid, missing_ok: bool) -> *mut c_char {
    let tup: HeapTuple;
    let opfname: *mut c_char;
    let opfform: Form_pg_opfamily;

    tup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));

    if !HeapTupleIsValid(tup) {
        if !missing_ok {
            elog!(ERROR, "cache lookup failed for operator family {}", opfid);
        }
        return core::ptr::null_mut();
    }

    opfform = GETSTRUCT(tup) as Form_pg_opfamily;
    opfname = pstrdup(NameStr(&(*opfform).opfname));

    ReleaseSysCache(tup);

    opfname
}

/*              ---------- OPERATOR CACHE ----------                           */

/*
 * get_opcode
 *
 *      Returns the regproc id of the routine used to implement an
 *      operator given the operator oid.
 */
pub unsafe fn get_opcode(opno: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: Oid;

        result = (*optup).oprcode;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_opname
 *    returns the name of the operator with the given opno
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such operator.
 */
pub unsafe fn get_opname(opno: Oid) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*optup).oprname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return core::ptr::null_mut();
    }
}

/*
 * get_op_rettype
 *      Given operator oid, return the operator's result type.
 */
pub unsafe fn get_op_rettype(opno: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: Oid;

        result = (*optup).oprresult;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * op_input_types
 *
 *      Returns the left and right input datatypes for an operator
 *      (InvalidOid if not relevant).
 */
pub unsafe fn op_input_types(opno: Oid, lefttype: *mut Oid, righttype: *mut Oid) {
    let tp: HeapTuple;
    let optup: Form_pg_operator;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if !HeapTupleIsValid(tp) {
        /* shouldn't happen */
        elog!(ERROR, "cache lookup failed for operator {}", opno);
    }
    optup = GETSTRUCT(tp) as Form_pg_operator;
    *lefttype = (*optup).oprleft;
    *righttype = (*optup).oprright;
    ReleaseSysCache(tp);
}

/*
 * op_mergejoinable
 *
 * Returns true if the operator is potentially mergejoinable.
 *
 * In some cases (currently only array_eq and record_eq), mergejoinability
 * depends on the specific input data type the operator is invoked for, so
 * that must be passed as well.
 */
pub unsafe fn op_mergejoinable(opno: Oid, inputtype: Oid) -> bool {
    let mut result: bool = false;
    let tp: HeapTuple;

    /*
     * For array_eq or record_eq, we can sort if the element or field types
     * are all sortable.  We could implement all the checks for that here, but
     * the typcache already does that and caches the results too, so let's
     * rely on the typcache.
     */
    if opno == ARRAY_EQ_OP {
        let typentry = lookup_type_cache(inputtype, TYPECACHE_CMP_PROC);
        if !typentry.is_null() && (*typentry).cmp_proc == F_BTARRAYCMP {
            result = true;
        }
    } else if opno == RECORD_EQ_OP {
        let typentry = lookup_type_cache(inputtype, TYPECACHE_CMP_PROC);
        if !typentry.is_null() && (*typentry).cmp_proc == F_BTRECORDCMP {
            result = true;
        }
    } else {
        /* For all other operators, rely on pg_operator.oprcanmerge */
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if HeapTupleIsValid(tp) {
            let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;

            result = (*optup).oprcanmerge;
            ReleaseSysCache(tp);
        }
    }
    result
}

/*
 * op_hashjoinable
 *
 * Returns true if the operator is hashjoinable.
 *
 * In some cases (currently only array_eq), hashjoinability depends on the
 * specific input data type the operator is invoked for.
 */
pub unsafe fn op_hashjoinable(opno: Oid, inputtype: Oid) -> bool {
    let mut result: bool = false;
    let tp: HeapTuple;

    /* As in op_mergejoinable, let the typcache handle the hard cases */
    if opno == ARRAY_EQ_OP {
        let typentry = lookup_type_cache(inputtype, TYPECACHE_HASH_PROC);
        if !typentry.is_null() && (*typentry).hash_proc == F_HASH_ARRAY {
            result = true;
        }
    } else if opno == RECORD_EQ_OP {
        let typentry = lookup_type_cache(inputtype, TYPECACHE_HASH_PROC);
        if !typentry.is_null() && (*typentry).hash_proc == F_HASH_RECORD {
            result = true;
        }
    } else {
        /* For all other operators, rely on pg_operator.oprcanhash */
        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if HeapTupleIsValid(tp) {
            let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;

            result = (*optup).oprcanhash;
            ReleaseSysCache(tp);
        }
    }
    result
}

/*
 * op_strict
 *
 * Get the proisstrict flag for the operator's underlying function.
 */
pub unsafe fn op_strict(opno: Oid) -> bool {
    let funcid: Oid = get_opcode(opno);

    if funcid == InvalidOid {
        elog!(ERROR, "operator {} does not exist", opno);
    }

    func_strict(funcid)
}

/*
 * op_volatile
 *
 * Get the provolatile flag for the operator's underlying function.
 */
pub unsafe fn op_volatile(opno: Oid) -> c_char {
    let funcid: Oid = get_opcode(opno);

    if funcid == InvalidOid {
        elog!(ERROR, "operator {} does not exist", opno);
    }

    func_volatile(funcid)
}

/*
 * get_commutator
 *
 *      Returns the corresponding commutator of an operator.
 */
pub unsafe fn get_commutator(opno: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: Oid;

        result = (*optup).oprcom;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_negator
 *
 *      Returns the corresponding negator of an operator.
 */
pub unsafe fn get_negator(opno: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: Oid;

        result = (*optup).oprnegate;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_oprrest
 *
 *      Returns procedure id for computing selectivity of an operator.
 */
pub unsafe fn get_oprrest(opno: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: Oid;

        result = (*optup).oprrest;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_oprjoin
 *
 *      Returns procedure id for computing selectivity of a join.
 */
pub unsafe fn get_oprjoin(opno: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if HeapTupleIsValid(tp) {
        let optup: Form_pg_operator = GETSTRUCT(tp) as Form_pg_operator;
        let result: Oid;

        result = (*optup).oprjoin;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}


/*              ---------- FUNCTION CACHE ----------                           */

/*
 * get_func_name
 *    returns the name of the function with the given funcid
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such function.
 */
pub unsafe fn get_func_name(funcid: Oid) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if HeapTupleIsValid(tp) {
        let functup: Form_pg_proc = GETSTRUCT(tp) as Form_pg_proc;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*functup).proname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return core::ptr::null_mut();
    }
}

/*
 * get_func_namespace
 *
 *      Returns the pg_namespace OID associated with a given function.
 */
pub unsafe fn get_func_namespace(funcid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if HeapTupleIsValid(tp) {
        let functup: Form_pg_proc = GETSTRUCT(tp) as Form_pg_proc;
        let result: Oid;

        result = (*functup).pronamespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_func_rettype
 *      Given procedure id, return the function's result type.
 */
pub unsafe fn get_func_rettype(funcid: Oid) -> Oid {
    let tp: HeapTuple;
    let result: Oid;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).prorettype;
    ReleaseSysCache(tp);
    result
}

/*
 * get_func_nargs
 *      Given procedure id, return the number of arguments.
 */
pub unsafe fn get_func_nargs(funcid: Oid) -> c_int {
    let tp: HeapTuple;
    let result: c_int;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).pronargs as c_int;
    ReleaseSysCache(tp);
    result
}

/*
 * get_func_signature
 *      Given procedure id, return the function's argument and result types.
 *      (The return value is the result type.)
 *
 * The arguments are returned as a palloc'd array.
 */
pub unsafe fn get_func_signature(
    funcid: Oid,
    argtypes: *mut *mut Oid,
    nargs: *mut c_int,
) -> Oid {
    let tp: HeapTuple;
    let procstruct: Form_pg_proc;
    let result: Oid;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    procstruct = GETSTRUCT(tp) as Form_pg_proc;

    result = (*procstruct).prorettype;
    *nargs = (*procstruct).pronargs as c_int;
    /* Assert(*nargs == procstruct->proargtypes.dim1); */
    *argtypes = palloc((*nargs as usize) * core::mem::size_of::<Oid>()) as *mut Oid;
    /* TODO(pg-port): pg_proc.proargtypes (oidvector CATALOG_VARLEN) is omitted
     * from the ported FormData_pg_proc, so the argtype copy is unavailable;
     * zero-fill instead (palloc gave us the buffer). */
    core::ptr::write_bytes(*argtypes, 0, *nargs as usize);

    ReleaseSysCache(tp);
    result
}

/*
 * get_func_variadictype
 *      Given procedure id, return the function's provariadic field.
 */
pub unsafe fn get_func_variadictype(funcid: Oid) -> Oid {
    let tp: HeapTuple;
    let result: Oid;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).provariadic;
    ReleaseSysCache(tp);
    result
}

/*
 * get_func_retset
 *      Given procedure id, return the function's proretset flag.
 */
pub unsafe fn get_func_retset(funcid: Oid) -> bool {
    let tp: HeapTuple;
    let result: bool;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).proretset;
    ReleaseSysCache(tp);
    result
}

/*
 * func_strict
 *      Given procedure id, return the function's proisstrict flag.
 */
pub unsafe fn func_strict(funcid: Oid) -> bool {
    let tp: HeapTuple;
    let result: bool;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).proisstrict;
    ReleaseSysCache(tp);
    result
}

/*
 * func_volatile
 *      Given procedure id, return the function's provolatile flag.
 */
pub unsafe fn func_volatile(funcid: Oid) -> c_char {
    let tp: HeapTuple;
    let result: c_char;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).provolatile;
    ReleaseSysCache(tp);
    result
}

/*
 * func_parallel
 *      Given procedure id, return the function's proparallel flag.
 */
pub unsafe fn func_parallel(funcid: Oid) -> c_char {
    let tp: HeapTuple;
    let result: c_char;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).proparallel;
    ReleaseSysCache(tp);
    result
}

/*
 * get_func_prokind
 *     Given procedure id, return the routine kind.
 */
pub unsafe fn get_func_prokind(funcid: Oid) -> c_char {
    let tp: HeapTuple;
    let result: c_char;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).prokind;
    ReleaseSysCache(tp);
    result
}

/*
 * get_func_leakproof
 *     Given procedure id, return the function's leakproof field.
 */
pub unsafe fn get_func_leakproof(funcid: Oid) -> bool {
    let tp: HeapTuple;
    let result: bool;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }

    result = (*(GETSTRUCT(tp) as Form_pg_proc)).proleakproof;
    ReleaseSysCache(tp);
    result
}

/*
 * get_func_support
 *
 *      Returns the support function OID associated with a given function,
 *      or InvalidOid if there is none.
 */
pub unsafe fn get_func_support(funcid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if HeapTupleIsValid(tp) {
        let functup: Form_pg_proc = GETSTRUCT(tp) as Form_pg_proc;
        let result: Oid;

        result = (*functup).prosupport;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*              ---------- RELATION CACHE ----------                           */

/*
 * get_relname_relid
 *      Given name and namespace of a relation, look up the OID.
 *
 * Returns InvalidOid if there is no such relation.
 */
pub unsafe fn get_relname_relid(relname: *const c_char, relnamespace: Oid) -> Oid {
    GetSysCacheOid2(
        RELNAMENSP,
        Anum_pg_class_oid,
        PointerGetDatum(relname as *const c_void),
        ObjectIdGetDatum(relnamespace),
    )
}

/* NOT_USED in C source: get_relnatts */
#[cfg(any())] /* NOT_USED in C */
pub unsafe fn get_relnatts(relid: Oid) -> c_int {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: c_int;

        result = (*reltup).relnatts as c_int;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidAttrNumber as c_int;
    }
}

/*
 * get_rel_name
 *      Returns the name of a given relation.
 *
 * Returns a palloc'd copy of the string, or NULL if no such relation.
 *
 * NOTE: since relation name is not unique, be wary of code that uses this
 * for anything except preparing error messages.
 */
pub unsafe fn get_rel_name(relid: Oid) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*reltup).relname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return core::ptr::null_mut();
    }
}

/*
 * get_rel_namespace
 *
 *      Returns the pg_namespace OID associated with a given relation.
 */
pub unsafe fn get_rel_namespace(relid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: Oid;

        result = (*reltup).relnamespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_rel_type_id
 *
 *      Returns the pg_type OID associated with a given relation.
 *
 * Note: not all pg_class entries have associated pg_type OIDs; so be
 * careful to check for InvalidOid result.
 */
pub unsafe fn get_rel_type_id(relid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: Oid;

        result = (*reltup).reltype;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_rel_relkind
 *
 *      Returns the relkind associated with a given relation.
 */
pub unsafe fn get_rel_relkind(relid: Oid) -> c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: c_char;

        result = (*reltup).relkind;
        ReleaseSysCache(tp);
        return result;
    } else {
        return b'\0' as c_char;
    }
}

/*
 * get_rel_relispartition
 *
 *      Returns the relispartition flag associated with a given relation.
 */
pub unsafe fn get_rel_relispartition(relid: Oid) -> bool {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: bool;

        result = (*reltup).relispartition;
        ReleaseSysCache(tp);
        return result;
    } else {
        return false;
    }
}

/*
 * get_rel_tablespace
 *
 *      Returns the pg_tablespace OID associated with a given relation.
 *
 * Note: InvalidOid might mean either that we couldn't find the relation,
 * or that it is in the database's default tablespace.
 */
pub unsafe fn get_rel_tablespace(relid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if HeapTupleIsValid(tp) {
        let reltup: Form_pg_class = GETSTRUCT(tp) as Form_pg_class;
        let result: Oid;

        result = (*reltup).reltablespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_rel_persistence
 *
 *      Returns the relpersistence associated with a given relation.
 */
pub unsafe fn get_rel_persistence(relid: Oid) -> c_char {
    let tp: HeapTuple;
    let reltup: Form_pg_class;
    let result: c_char;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    reltup = GETSTRUCT(tp) as Form_pg_class;
    result = (*reltup).relpersistence;
    ReleaseSysCache(tp);

    result
}

/*
 * get_rel_relam
 *
 *      Returns the relam associated with a given relation.
 */
pub unsafe fn get_rel_relam(relid: Oid) -> Oid {
    let tp: HeapTuple;
    let reltup: Form_pg_class;
    let result: Oid;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    reltup = GETSTRUCT(tp) as Form_pg_class;
    result = (*reltup).relam;
    ReleaseSysCache(tp);

    result
}


/*              ---------- TRANSFORM CACHE ----------                          */

pub unsafe fn get_transform_fromsql(typid: Oid, langid: Oid, trftypes: *const List) -> Oid {
    let tup: HeapTuple;

    if !list_member_oid(trftypes, typid) {
        return InvalidOid;
    }

    tup = SearchSysCache2(
        TRFTYPELANG,
        ObjectIdGetDatum(typid),
        ObjectIdGetDatum(langid),
    );
    if HeapTupleIsValid(tup) {
        let funcid: Oid;

        funcid = (*(GETSTRUCT(tup) as Form_pg_transform)).trffromsql;
        ReleaseSysCache(tup);
        return funcid;
    } else {
        return InvalidOid;
    }
}

pub unsafe fn get_transform_tosql(typid: Oid, langid: Oid, trftypes: *const List) -> Oid {
    let tup: HeapTuple;

    if !list_member_oid(trftypes, typid) {
        return InvalidOid;
    }

    tup = SearchSysCache2(
        TRFTYPELANG,
        ObjectIdGetDatum(typid),
        ObjectIdGetDatum(langid),
    );
    if HeapTupleIsValid(tup) {
        let funcid: Oid;

        funcid = (*(GETSTRUCT(tup) as Form_pg_transform)).trftosql;
        ReleaseSysCache(tup);
        return funcid;
    } else {
        return InvalidOid;
    }
}


/*              ---------- TYPE CACHE ----------                               */

/*
 * get_typisdefined
 *
 *      Given the type OID, determine whether the type is defined
 *      (if not, it's only a shell).
 */
pub unsafe fn get_typisdefined(typid: Oid) -> bool {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: bool;

        result = (*typtup).typisdefined;
        ReleaseSysCache(tp);
        return result;
    } else {
        return false;
    }
}

/*
 * get_typlen
 *
 *      Given the type OID, return the length of the type.
 */
pub unsafe fn get_typlen(typid: Oid) -> i16 {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: i16;

        result = (*typtup).typlen;
        ReleaseSysCache(tp);
        return result;
    } else {
        return 0;
    }
}

/*
 * get_typbyval
 *
 *      Given the type OID, determine whether the type is returned by value or
 *      not.  Returns true if by value, false if by reference.
 */
pub unsafe fn get_typbyval(typid: Oid) -> bool {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: bool;

        result = (*typtup).typbyval;
        ReleaseSysCache(tp);
        return result;
    } else {
        return false;
    }
}

/*
 * get_typlenbyval
 *
 *      A two-fer: given the type OID, return both typlen and typbyval.
 *
 *      Since both pieces of info are needed to know how to copy a Datum,
 *      many places need both.  Might as well get them with one cache lookup
 *      instead of two.  Also, this routine raises an error instead of
 *      returning a bogus value when given a bad type OID.
 */
pub unsafe fn get_typlenbyval(typid: Oid, typlen: *mut i16, typbyval: *mut bool) {
    let tp: HeapTuple;
    let typtup: Form_pg_type;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }
    typtup = GETSTRUCT(tp) as Form_pg_type;
    *typlen = (*typtup).typlen;
    *typbyval = (*typtup).typbyval;
    ReleaseSysCache(tp);
}

/*
 * get_typlenbyvalalign
 *
 *      A three-fer: given the type OID, return typlen, typbyval, typalign.
 */
pub unsafe fn get_typlenbyvalalign(
    typid: Oid,
    typlen: *mut i16,
    typbyval: *mut bool,
    typalign: *mut c_char,
) {
    let tp: HeapTuple;
    let typtup: Form_pg_type;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }
    typtup = GETSTRUCT(tp) as Form_pg_type;
    *typlen = (*typtup).typlen;
    *typbyval = (*typtup).typbyval;
    *typalign = (*typtup).typalign;
    ReleaseSysCache(tp);
}

/*
 * getTypeIOParam
 *      Given a pg_type row, select the type OID to pass to I/O functions
 *
 * Formerly, all I/O functions were passed pg_type.typelem as their second
 * parameter, but we now have a more complex rule about what to pass.
 * This knowledge is intended to be centralized here --- direct references
 * to typelem elsewhere in the code are wrong, if they are associated with
 * I/O calls and not with actual subscripting operations!
 *
 * As of PostgreSQL 8.1, output functions receive only the value itself
 * and not any auxiliary parameters, so the name of this routine is now
 * a bit of a misnomer ... it should be getTypeInputParam.
 */
pub unsafe fn getTypeIOParam(type_tuple: HeapTuple) -> Oid {
    let type_struct: Form_pg_type = GETSTRUCT(type_tuple) as Form_pg_type;

    /*
     * Array types get their typelem as parameter; everybody else gets their
     * own type OID as parameter.
     */
    if OidIsValid((*type_struct).typelem) {
        return (*type_struct).typelem;
    } else {
        return (*type_struct).oid;
    }
}

/*
 * get_type_io_data
 *
 *      A six-fer:  given the type OID, return typlen, typbyval, typalign,
 *                  typdelim, typioparam, and IO function OID. The IO function
 *                  returned is controlled by IOFuncSelector
 */
pub unsafe fn get_type_io_data(
    typid: Oid,
    which_func: IOFuncSelector,
    typlen: *mut i16,
    typbyval: *mut bool,
    typalign: *mut c_char,
    typdelim: *mut c_char,
    typioparam: *mut Oid,
    func: *mut Oid,
) {
    let type_tuple: HeapTuple;
    let type_struct: Form_pg_type;

    /*
     * In bootstrap mode, pass it off to bootstrap.c.  This hack allows us to
     * use array_in and array_out during bootstrap.
     */
    if IsBootstrapProcessingMode() {
        let mut typinput: Oid = 0;
        let mut typoutput: Oid = 0;

        boot_get_type_io_data(
            typid,
            typlen,
            typbyval,
            typalign,
            typdelim,
            typioparam,
            &mut typinput,
            &mut typoutput,
        );
        if which_func == IOFunc_input {
            *func = typinput;
        } else if which_func == IOFunc_output {
            *func = typoutput;
        } else {
            elog!(ERROR, "binary I/O not supported during bootstrap");
        }
        return;
    }

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(type_tuple) {
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }
    type_struct = GETSTRUCT(type_tuple) as Form_pg_type;

    *typlen = (*type_struct).typlen;
    *typbyval = (*type_struct).typbyval;
    *typalign = (*type_struct).typalign;
    *typdelim = (*type_struct).typdelim;
    *typioparam = getTypeIOParam(type_tuple);
    if which_func == IOFunc_input {
        *func = (*type_struct).typinput;
    } else if which_func == IOFunc_output {
        *func = (*type_struct).typoutput;
    } else if which_func == IOFunc_receive {
        *func = (*type_struct).typreceive;
    } else {
        /* IOFunc_send */
        *func = (*type_struct).typsend;
    }
    ReleaseSysCache(type_tuple);
}

/* NOT_USED in C source: get_typalign */
#[cfg(any())] /* NOT_USED in C */
pub unsafe fn get_typalign(typid: Oid) -> c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: c_char;

        result = (*typtup).typalign;
        ReleaseSysCache(tp);
        return result;
    } else {
        return TYPALIGN_INT;
    }
}

pub unsafe fn get_typstorage(typid: Oid) -> c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: c_char;

        result = (*typtup).typstorage;
        ReleaseSysCache(tp);
        return result;
    } else {
        return TYPSTORAGE_PLAIN;
    }
}

/*
 * get_typdefault
 *    Given a type OID, return the type's default value, if any.
 *
 *    The result is a palloc'd expression node tree, or NULL if there
 *    is no defined default for the datatype.
 *
 * NB: caller should be prepared to coerce result to correct datatype;
 * the returned expression tree might produce something of the wrong type.
 */
pub unsafe fn get_typdefault(typid: Oid) -> *mut Node {
    let type_tuple: HeapTuple;
    let type_: Form_pg_type;
    let mut datum: Datum;
    let mut is_null: bool = false;
    let expr: *mut Node;

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(type_tuple) {
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }
    type_ = GETSTRUCT(type_tuple) as Form_pg_type;

    /*
     * typdefault and typdefaultbin are potentially null, so don't try to
     * access 'em as struct fields. Must do it the hard way with
     * SysCacheGetAttr.
     */
    datum = SysCacheGetAttr(
        TYPEOID,
        type_tuple,
        Anum_pg_type_typdefaultbin,
        &mut is_null,
    );

    if !is_null {
        /* We have an expression default */
        expr = stringToNode(TextDatumGetCString(datum));
    } else {
        /* Perhaps we have a plain literal default */
        datum = SysCacheGetAttr(TYPEOID, type_tuple, Anum_pg_type_typdefault, &mut is_null);

        if !is_null {
            let str_default_val: *mut c_char;

            /* Convert text datum to C string */
            str_default_val = TextDatumGetCString(datum);
            /* Convert C string to a value of the given type */
            datum = OidInputFunctionCall(
                (*type_).typinput,
                str_default_val,
                getTypeIOParam(type_tuple),
                -1,
            );
            /* Build a Const node containing the value */
            expr = makeConst(
                typid,
                -1,
                (*type_).typcollation,
                (*type_).typlen,
                datum,
                false,
                (*type_).typbyval,
            );
            pfree(str_default_val as *mut c_void);
        } else {
            /* No default */
            expr = core::ptr::null_mut();
        }
    }

    ReleaseSysCache(type_tuple);

    expr
}

/*
 * getBaseType
 *      If the given type is a domain, return its base type;
 *      otherwise return the type's own OID.
 */
pub unsafe fn getBaseType(typid: Oid) -> Oid {
    let mut typmod: i32 = -1;

    getBaseTypeAndTypmod(typid, &mut typmod)
}

/*
 * getBaseTypeAndTypmod
 *      If the given type is a domain, return its base type and typmod;
 *      otherwise return the type's own OID, and leave *typmod unchanged.
 *
 * Note that the "applied typmod" should be -1 for every domain level
 * above the bottommost; therefore, if the passed-in typid is indeed
 * a domain, *typmod should be -1.
 */
pub unsafe fn getBaseTypeAndTypmod(mut typid: Oid, typmod: *mut i32) -> Oid {
    /*
     * We loop to find the bottom base type in a stack of domains.
     */
    loop {
        let tup: HeapTuple;
        let typ_tup: Form_pg_type;

        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid(tup) {
            elog!(ERROR, "cache lookup failed for type {}", typid);
        }
        typ_tup = GETSTRUCT(tup) as Form_pg_type;
        if (*typ_tup).typtype != TYPTYPE_DOMAIN {
            /* Not a domain, so done */
            ReleaseSysCache(tup);
            break;
        }

        /* Assert(*typmod == -1); */
        typid = (*typ_tup).typbasetype;
        *typmod = (*typ_tup).typtypmod;

        ReleaseSysCache(tup);
    }

    typid
}

/*
 * get_typavgwidth
 *
 *    Given a type OID and a typmod value (pass -1 if typmod is unknown),
 *    estimate the average width of values of the type.  This is used by
 *    the planner, which doesn't require absolutely correct results;
 *    it's OK (and expected) to guess if we don't know for sure.
 */
pub unsafe fn get_typavgwidth(typid: Oid, typmod: i32) -> i32 {
    let typlen: i32 = get_typlen(typid) as i32;
    let maxwidth: i32;

    /*
     * Easy if it's a fixed-width type
     */
    if typlen > 0 {
        return typlen;
    }

    /*
     * type_maximum_size knows the encoding of typmod for some datatypes;
     * don't duplicate that knowledge here.
     */
    maxwidth = type_maximum_size(typid, typmod);
    if maxwidth > 0 {
        /*
         * For BPCHAR, the max width is also the only width.  Otherwise we
         * need to guess about the typical data width given the max. A sliding
         * scale for percentage of max width seems reasonable.
         */
        if typid == BPCHAROID {
            return maxwidth;
        }
        if maxwidth <= 32 {
            return maxwidth; /* assume full width */
        }
        if maxwidth < 1000 {
            return 32 + (maxwidth - 32) / 2; /* assume 50% */
        }

        /*
         * Beyond 1000, assume we're looking at something like
         * "varchar(10000)" where the limit isn't actually reached often, and
         * use a fixed estimate.
         */
        return 32 + (1000 - 32) / 2;
    }

    /*
     * Oops, we have no idea ... wild guess time.
     */
    return 32;
}

/*
 * get_typtype
 *
 *      Given the type OID, find if it is a basic type, a complex type, etc.
 *      It returns the null char if the cache lookup fails...
 */
pub unsafe fn get_typtype(typid: Oid) -> c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: c_char;

        result = (*typtup).typtype;
        ReleaseSysCache(tp);
        return result;
    } else {
        return b'\0' as c_char;
    }
}

/*
 * type_is_rowtype
 *
 *      Convenience function to determine whether a type OID represents
 *      a "rowtype" type --- either RECORD or a named composite type
 *      (including a domain over a named composite type).
 */
pub unsafe fn type_is_rowtype(typid: Oid) -> bool {
    if typid == RECORDOID {
        return true; /* easy case */
    }
    let tt = get_typtype(typid);
    if tt == TYPTYPE_COMPOSITE {
        return true;
    } else if tt == TYPTYPE_DOMAIN {
        if get_typtype(getBaseType(typid)) == TYPTYPE_COMPOSITE {
            return true;
        }
    }
    false
}

/*
 * type_is_enum
 *    Returns true if the given type is an enum type.
 */
pub unsafe fn type_is_enum(typid: Oid) -> bool {
    get_typtype(typid) == TYPTYPE_ENUM
}

/*
 * type_is_range
 *    Returns true if the given type is a range type.
 */
pub unsafe fn type_is_range(typid: Oid) -> bool {
    get_typtype(typid) == TYPTYPE_RANGE
}

/*
 * type_is_multirange
 *    Returns true if the given type is a multirange type.
 */
pub unsafe fn type_is_multirange(typid: Oid) -> bool {
    get_typtype(typid) == TYPTYPE_MULTIRANGE
}

/*
 * get_type_category_preferred
 *
 *      Given the type OID, fetch its category and preferred-type status.
 *      Throws error on failure.
 */
pub unsafe fn get_type_category_preferred(
    typid: Oid,
    typcategory: *mut c_char,
    typispreferred: *mut bool,
) {
    let tp: HeapTuple;
    let typtup: Form_pg_type;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }
    typtup = GETSTRUCT(tp) as Form_pg_type;
    *typcategory = (*typtup).typcategory;
    *typispreferred = (*typtup).typispreferred;
    ReleaseSysCache(tp);
}

/*
 * get_typ_typrelid
 *
 *      Given the type OID, get the typrelid (InvalidOid if not a complex
 *      type).
 */
pub unsafe fn get_typ_typrelid(typid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: Oid;

        result = (*typtup).typrelid;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_element_type
 *
 *      Given the type OID, get the typelem (InvalidOid if not an array type).
 *
 * NB: this only succeeds for "true" arrays having array_subscript_handler
 * as typsubscript.
 */
pub unsafe fn get_element_type(typid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: Oid;

        if IsTrueArrayType(typtup) {
            result = (*typtup).typelem;
        } else {
            result = InvalidOid;
        }
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_array_type
 *
 *      Given the type OID, get the corresponding "true" array type.
 *      Returns InvalidOid if no array type can be found.
 */
pub unsafe fn get_array_type(typid: Oid) -> Oid {
    let tp: HeapTuple;
    let mut result: Oid = InvalidOid;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        result = (*(GETSTRUCT(tp) as Form_pg_type)).typarray;
        ReleaseSysCache(tp);
    }
    result
}

/*
 * get_promoted_array_type
 *
 *      The "promoted" type is what you'd get from an ARRAY(SELECT ...)
 *      construct, that is, either the corresponding "true" array type
 *      if the input is a scalar type that has such an array type,
 *      or the same type if the input is already a "true" array type.
 *      Returns InvalidOid if neither rule is satisfied.
 */
pub unsafe fn get_promoted_array_type(typid: Oid) -> Oid {
    let array_type: Oid = get_array_type(typid);

    if OidIsValid(array_type) {
        return array_type;
    }
    if OidIsValid(get_element_type(typid)) {
        return typid;
    }
    InvalidOid
}

/*
 * get_base_element_type
 *      Given the type OID, get the typelem, looking "through" any domain
 *      to its underlying array type.
 *
 * This is equivalent to get_element_type(getBaseType(typid)), but avoids
 * an extra cache lookup.
 */
pub unsafe fn get_base_element_type(mut typid: Oid) -> Oid {
    /*
     * We loop to find the bottom base type in a stack of domains.
     */
    loop {
        let tup: HeapTuple;
        let typ_tup: Form_pg_type;

        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid(tup) {
            break;
        }
        typ_tup = GETSTRUCT(tup) as Form_pg_type;
        if (*typ_tup).typtype != TYPTYPE_DOMAIN {
            /* Not a domain, so stop descending */
            let result: Oid;

            /* This test must match get_element_type */
            if IsTrueArrayType(typ_tup) {
                result = (*typ_tup).typelem;
            } else {
                result = InvalidOid;
            }
            ReleaseSysCache(tup);
            return result;
        }

        typid = (*typ_tup).typbasetype;
        ReleaseSysCache(tup);
    }

    /* Like get_element_type, silently return InvalidOid for bogus input */
    InvalidOid
}

/*
 * getTypeInputInfo
 *
 *      Get info needed for converting values of a type to internal form
 */
pub unsafe fn getTypeInputInfo(type_: Oid, typ_input: *mut Oid, typ_io_param: *mut Oid) {
    let type_tuple: HeapTuple;
    let pt: Form_pg_type;

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_));
    if !HeapTupleIsValid(type_tuple) {
        elog!(ERROR, "cache lookup failed for type {}", type_);
    }
    pt = GETSTRUCT(type_tuple) as Form_pg_type;

    if !(*pt).typisdefined {
        ereport!(ERROR, errmsg!("type {} is only a shell", std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy())) /* C also: errcode */;
    }
    if !OidIsValid((*pt).typinput) {
        ereport!(ERROR, errmsg!(
                "no input function available for type {}",
                std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy()
            )) /* C also: errcode */;
    }

    *typ_input = (*pt).typinput;
    *typ_io_param = getTypeIOParam(type_tuple);

    ReleaseSysCache(type_tuple);
}

/*
 * getTypeOutputInfo
 *
 *      Get info needed for printing values of a type
 */
pub unsafe fn getTypeOutputInfo(type_: Oid, typ_output: *mut Oid, typ_is_varlena: *mut bool) {
    let type_tuple: HeapTuple;
    let pt: Form_pg_type;

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_));
    if !HeapTupleIsValid(type_tuple) {
        elog!(ERROR, "cache lookup failed for type {}", type_);
    }
    pt = GETSTRUCT(type_tuple) as Form_pg_type;

    if !(*pt).typisdefined {
        ereport!(ERROR, errmsg!("type {} is only a shell", std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy())) /* C also: errcode */;
    }
    if !OidIsValid((*pt).typoutput) {
        ereport!(ERROR, errmsg!(
                "no output function available for type {}",
                std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy()
            )) /* C also: errcode */;
    }

    *typ_output = (*pt).typoutput;
    *typ_is_varlena = (!(*pt).typbyval) && ((*pt).typlen == -1);

    ReleaseSysCache(type_tuple);
}

/*
 * getTypeBinaryInputInfo
 *
 *      Get info needed for binary input of values of a type
 */
pub unsafe fn getTypeBinaryInputInfo(
    type_: Oid,
    typ_receive: *mut Oid,
    typ_io_param: *mut Oid,
) {
    let type_tuple: HeapTuple;
    let pt: Form_pg_type;

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_));
    if !HeapTupleIsValid(type_tuple) {
        elog!(ERROR, "cache lookup failed for type {}", type_);
    }
    pt = GETSTRUCT(type_tuple) as Form_pg_type;

    if !(*pt).typisdefined {
        ereport!(ERROR, errmsg!("type {} is only a shell", std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy())) /* C also: errcode */;
    }
    if !OidIsValid((*pt).typreceive) {
        ereport!(ERROR, errmsg!(
                "no binary input function available for type {}",
                std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy()
            )) /* C also: errcode */;
    }

    *typ_receive = (*pt).typreceive;
    *typ_io_param = getTypeIOParam(type_tuple);

    ReleaseSysCache(type_tuple);
}

/*
 * getTypeBinaryOutputInfo
 *
 *      Get info needed for binary output of values of a type
 */
pub unsafe fn getTypeBinaryOutputInfo(
    type_: Oid,
    typ_send: *mut Oid,
    typ_is_varlena: *mut bool,
) {
    let type_tuple: HeapTuple;
    let pt: Form_pg_type;

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_));
    if !HeapTupleIsValid(type_tuple) {
        elog!(ERROR, "cache lookup failed for type {}", type_);
    }
    pt = GETSTRUCT(type_tuple) as Form_pg_type;

    if !(*pt).typisdefined {
        ereport!(ERROR, errmsg!("type {} is only a shell", std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy())) /* C also: errcode */;
    }
    if !OidIsValid((*pt).typsend) {
        ereport!(ERROR, errmsg!(
                "no binary output function available for type {}",
                std::ffi::CStr::from_ptr(format_type_be(type_)).to_string_lossy()
            )) /* C also: errcode */;
    }

    *typ_send = (*pt).typsend;
    *typ_is_varlena = (!(*pt).typbyval) && ((*pt).typlen == -1);

    ReleaseSysCache(type_tuple);
}

/*
 * get_typmodin
 *
 *      Given the type OID, return the type's typmodin procedure, if any.
 */
pub unsafe fn get_typmodin(typid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: Oid;

        result = (*typtup).typmodin;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/* NOT_USED in C source: get_typmodout */
#[cfg(any())] /* NOT_USED in C */
pub unsafe fn get_typmodout(typid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: Oid;

        result = (*typtup).typmodout;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_typcollation
 *
 *      Given the type OID, return the type's typcollation attribute.
 */
pub unsafe fn get_typcollation(typid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typtup: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let result: Oid;

        result = (*typtup).typcollation;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}


/*
 * type_is_collatable
 *
 *      Return whether the type cares about collations
 */
pub unsafe fn type_is_collatable(typid: Oid) -> bool {
    OidIsValid(get_typcollation(typid))
}


/*
 * get_typsubscript
 *
 *      Given the type OID, return the type's subscripting handler's OID,
 *      if it has one.
 *
 * If typelemp isn't NULL, we also store the type's typelem value there.
 */
pub unsafe fn get_typsubscript(typid: Oid, typelemp: *mut Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if HeapTupleIsValid(tp) {
        let typform: Form_pg_type = GETSTRUCT(tp) as Form_pg_type;
        let handler: Oid = (*typform).typsubscript;

        if !typelemp.is_null() {
            *typelemp = (*typform).typelem;
        }
        ReleaseSysCache(tp);
        return handler;
    } else {
        if !typelemp.is_null() {
            *typelemp = InvalidOid;
        }
        return InvalidOid;
    }
}

/*
 * getSubscriptingRoutines
 *
 *      Given the type OID, fetch the type's subscripting methods struct.
 *      Return NULL if type is not subscriptable.
 *
 * If typelemp isn't NULL, we also store the type's typelem value there.
 */
pub unsafe fn getSubscriptingRoutines(
    typid: Oid,
    typelemp: *mut Oid,
) -> *const SubscriptRoutines {
    let typsubscript: Oid = get_typsubscript(typid, typelemp);

    if !OidIsValid(typsubscript) {
        return core::ptr::null();
    }

    DatumGetPointer(OidFunctionCall0(typsubscript)) as *const SubscriptRoutines
}


/*              ---------- STATISTICS CACHE ----------                         */

/*
 * get_attavgwidth
 *
 *    Given the table and attribute number of a column, get the average
 *    width of entries in the column.  Return zero if no data available.
 *
 * Currently this is only consulted for individual tables, not for inheritance
 * trees, so we don't need an "inh" parameter.
 *
 * Calling a hook at this point looks somewhat strange, but is required
 * because the optimizer calls this function without any other way for
 * plug-ins to control the result.
 */
pub unsafe fn get_attavgwidth(relid: Oid, attnum: AttrNumber) -> i32 {
    let tp: HeapTuple;
    let stawidth: i32;

    if let Some(hook) = get_attavgwidth_hook {
        let sw = hook(relid, attnum);
        if sw > 0 {
            return sw;
        }
    }
    tp = SearchSysCache3(
        STATRELATTINH,
        ObjectIdGetDatum(relid),
        Int16GetDatum(attnum),
        BoolGetDatum(false),
    );
    if HeapTupleIsValid(tp) {
        stawidth = (*(GETSTRUCT(tp) as Form_pg_statistic)).stawidth;
        ReleaseSysCache(tp);
        if stawidth > 0 {
            return stawidth;
        }
    }
    0
}

/*
 * get_attstatsslot
 *
 *      Extract the contents of a "slot" of a pg_statistic tuple.
 *      Returns true if requested slot type was found, else false.
 *
 * Unlike other routines in this file, this takes a pointer to an
 * already-looked-up tuple in the pg_statistic cache.
 *
 * sslot: pointer to output area (typically, a local variable in the caller).
 * statstuple: pg_statistic tuple to be examined.
 * reqkind: STAKIND code for desired statistics slot kind.
 * reqop: STAOP value wanted, or InvalidOid if don't care.
 * flags: bitmask of ATTSTATSSLOT_VALUES and/or ATTSTATSSLOT_NUMBERS.
 */
pub unsafe fn get_attstatsslot(
    sslot: *mut AttStatsSlot,
    statstuple: HeapTuple,
    reqkind: c_int,
    reqop: Oid,
    flags: c_int,
) -> bool {
    let stats: Form_pg_statistic = GETSTRUCT(statstuple) as Form_pg_statistic;
    let mut i: c_int;
    let mut val: Datum;
    let statarray: *mut ArrayType;
    let arrayelemtype: Oid;
    let narrayelem: c_int;
    let type_tuple: HeapTuple;
    let type_form: Form_pg_type;

    /* initialize *sslot properly */
    core::ptr::write_bytes(sslot, 0, 1);

    i = 0;
    while i < STATISTIC_NUM_SLOTS {
        /* (&stats->stakind1)[i] and (&stats->staop1)[i] */
        let stakind_i = stakind_slot(stats, i);
        let staop_i = staop_slot(stats, i);
        if stakind_i == reqkind as i16
            && (reqop == InvalidOid || staop_i == reqop)
        {
            break;
        }
        i += 1;
    }
    if i >= STATISTIC_NUM_SLOTS {
        return false; /* not there */
    }

    (*sslot).staop = staop_slot(stats, i);
    (*sslot).stacoll = stacoll_slot(stats, i);

    if flags & ATTSTATSSLOT_VALUES != 0 {
        val = SysCacheGetAttrNotNull(
            STATRELATTINH,
            statstuple,
            Anum_pg_statistic_stavalues1 + i as AttrNumber,
        );

        /*
         * Detoast the array if needed, and in any case make a copy that's
         * under control of this AttStatsSlot.
         */
        let stat_arr: *mut ArrayType = DatumGetArrayTypePCopy(val);

        /*
         * Extract the actual array element type, and pass it back in case the
         * caller needs it.
         */
        (*sslot).valuetype = ARR_ELEMTYPE(stat_arr);
        let ae = (*sslot).valuetype;

        /* Need info about element type */
        type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(ae));
        if !HeapTupleIsValid(type_tuple) {
            elog!(ERROR, "cache lookup failed for type {}", ae);
        }
        type_form = GETSTRUCT(type_tuple) as Form_pg_type;

        /* Deconstruct array into Datum elements; NULLs not expected */
        deconstruct_array(
            stat_arr,
            ae,
            (*type_form).typlen,
            (*type_form).typbyval,
            (*type_form).typalign,
            &mut (*sslot).values,
            core::ptr::null_mut(),
            &mut (*sslot).nvalues,
        );

        /*
         * If the element type is pass-by-reference, we now have a bunch of
         * Datums that are pointers into the statarray, so we need to keep
         * that until free_attstatsslot.  Otherwise, all the useful info is in
         * sslot->values[], so we can free the array object immediately.
         */
        if !(*type_form).typbyval {
            (*sslot).values_arr = stat_arr;
        } else {
            pfree(stat_arr as *mut c_void);
        }

        ReleaseSysCache(type_tuple);
    }

    if flags & ATTSTATSSLOT_NUMBERS != 0 {
        val = SysCacheGetAttrNotNull(
            STATRELATTINH,
            statstuple,
            Anum_pg_statistic_stanumbers1 + i as AttrNumber,
        );

        /*
         * Detoast the array if needed, and in any case make a copy that's
         * under control of this AttStatsSlot.
         */
        let stat_arr: *mut ArrayType = DatumGetArrayTypePCopy(val);

        /*
         * We expect the array to be a 1-D float4 array; verify that.
         */
        narrayelem = if ARR_NDIM(stat_arr) >= 1 { *ARR_DIMS(stat_arr) } else { 0 };
        if ARR_NDIM(stat_arr) != 1
            || narrayelem <= 0
            || ARR_HASNULL(stat_arr)
            || ARR_ELEMTYPE(stat_arr) != FLOAT4OID
        {
            elog!(ERROR, "stanumbers is not a 1-D float4 array");
        }

        /* Give caller a pointer directly into the statarray */
        (*sslot).numbers = ARR_DATA_PTR(stat_arr) as *mut f32;
        (*sslot).nnumbers = narrayelem;

        /* We'll free the statarray in free_attstatsslot */
        (*sslot).numbers_arr = stat_arr;
    }

    true
}

/* Helper accessors for FormData_pg_statistic parallel arrays. */
/* TODO(pg-port): replace with real field access once FormData_pg_statistic has proper slot arrays */
unsafe fn stakind_slot(_stats: Form_pg_statistic, _i: c_int) -> i16 {
    0
}
unsafe fn staop_slot(_stats: Form_pg_statistic, _i: c_int) -> Oid {
    0
}
unsafe fn stacoll_slot(_stats: Form_pg_statistic, _i: c_int) -> Oid {
    0
}

/*
 * free_attstatsslot
 *      Free data allocated by get_attstatsslot
 */
pub unsafe fn free_attstatsslot(sslot: *mut AttStatsSlot) {
    /* The values[] array was separately palloc'd by deconstruct_array */
    if !(*sslot).values.is_null() {
        pfree((*sslot).values as *mut c_void);
    }
    /* The numbers[] array points into numbers_arr, do not pfree it */
    /* Free the detoasted array objects, if any */
    if !(*sslot).values_arr.is_null() {
        pfree((*sslot).values_arr as *mut c_void);
    }
    if !(*sslot).numbers_arr.is_null() {
        pfree((*sslot).numbers_arr as *mut c_void);
    }
}

/*              ---------- PG_NAMESPACE CACHE ----------                      */

/*
 * get_namespace_name
 *      Returns the name of a given namespace
 *
 * Returns a palloc'd copy of the string, or NULL if no such namespace.
 */
pub unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char {
    let tp: HeapTuple;

    tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));
    if HeapTupleIsValid(tp) {
        let nsptup: Form_pg_namespace = GETSTRUCT(tp) as Form_pg_namespace;
        let result: *mut c_char;

        result = pstrdup(NameStr(&(*nsptup).nspname));
        ReleaseSysCache(tp);
        return result;
    } else {
        return core::ptr::null_mut();
    }
}

/*
 * get_namespace_name_or_temp
 *      As above, but if it is this backend's temporary namespace, return
 *      "pg_temp" instead.
 */
pub unsafe fn get_namespace_name_or_temp(nspid: Oid) -> *mut c_char {
    if isTempNamespace(nspid) {
        return pstrdup(b"pg_temp\0".as_ptr() as *const c_char);
    } else {
        return get_namespace_name(nspid);
    }
}

/*              ---------- PG_RANGE CACHES ----------                         */

/*
 * get_range_subtype
 *      Returns the subtype of a given range type
 *
 * Returns InvalidOid if the type is not a range type.
 */
pub unsafe fn get_range_subtype(range_oid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(range_oid));
    if HeapTupleIsValid(tp) {
        let rngtup: Form_pg_range = GETSTRUCT(tp) as Form_pg_range;
        let result: Oid;

        result = (*rngtup).rngsubtype;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_range_collation
 *      Returns the collation of a given range type
 *
 * Returns InvalidOid if the type is not a range type,
 * or if its subtype is not collatable.
 */
pub unsafe fn get_range_collation(range_oid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(range_oid));
    if HeapTupleIsValid(tp) {
        let rngtup: Form_pg_range = GETSTRUCT(tp) as Form_pg_range;
        let result: Oid;

        result = (*rngtup).rngcollation;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_range_multirange
 *      Returns the multirange type of a given range type
 *
 * Returns InvalidOid if the type is not a range type.
 */
pub unsafe fn get_range_multirange(range_oid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(range_oid));
    if HeapTupleIsValid(tp) {
        let rngtup: Form_pg_range = GETSTRUCT(tp) as Form_pg_range;
        let result: Oid;

        result = (*rngtup).rngmultitypid;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*
 * get_multirange_range
 *      Returns the range type of a given multirange
 *
 * Returns InvalidOid if the type is not a multirange.
 */
pub unsafe fn get_multirange_range(multirange_oid: Oid) -> Oid {
    let tp: HeapTuple;

    tp = SearchSysCache1(RANGEMULTIRANGE, ObjectIdGetDatum(multirange_oid));
    if HeapTupleIsValid(tp) {
        let rngtup: Form_pg_range = GETSTRUCT(tp) as Form_pg_range;
        let result: Oid;

        result = (*rngtup).rngtypid;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid;
    }
}

/*              ---------- PG_INDEX CACHE ----------                          */

/*
 * get_index_column_opclass
 *
 *      Given the index OID and column number,
 *      return opclass of the index column
 *          or InvalidOid if the index was not found
 *              or column is non-key one.
 */
pub unsafe fn get_index_column_opclass(index_oid: Oid, attno: c_int) -> Oid {
    let tuple: HeapTuple;
    let rd_index: Form_pg_index;
    let datum: Datum;
    let indclass: *const oidvector;
    let opclass: Oid;

    /* First we need to know the column's opclass. */

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if !HeapTupleIsValid(tuple) {
        return InvalidOid;
    }

    rd_index = GETSTRUCT(tuple) as Form_pg_index;

    /* caller is supposed to guarantee this */
    /* Assert(attno > 0 && attno <= rd_index->indnatts); */

    /* Non-key attributes don't have an opclass */
    if attno > (*rd_index).indnkeyatts as c_int {
        ReleaseSysCache(tuple);
        return InvalidOid;
    }

    datum = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indclass);
    indclass = DatumGetPointer(datum) as *const oidvector;

    /* Assert(attno <= indclass->dim1); */
    opclass = (*indclass).values[(attno - 1) as usize];

    ReleaseSysCache(tuple);

    opclass
}

/*
 * get_index_isreplident
 *
 *      Given the index OID, return pg_index.indisreplident.
 */
pub unsafe fn get_index_isreplident(index_oid: Oid) -> bool {
    let tuple: HeapTuple;
    let rd_index: Form_pg_index;
    let result: bool;

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if !HeapTupleIsValid(tuple) {
        return false;
    }

    rd_index = GETSTRUCT(tuple) as Form_pg_index;
    result = (*rd_index).indisreplident;
    ReleaseSysCache(tuple);

    result
}

/*
 * get_index_isvalid
 *
 *      Given the index OID, return pg_index.indisvalid.
 */
pub unsafe fn get_index_isvalid(index_oid: Oid) -> bool {
    let isvalid: bool;
    let tuple: HeapTuple;
    let rd_index: Form_pg_index;

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for index {}", index_oid);
    }

    rd_index = GETSTRUCT(tuple) as Form_pg_index;
    isvalid = (*rd_index).indisvalid;
    ReleaseSysCache(tuple);

    isvalid
}

/*
 * get_index_isclustered
 *
 *      Given the index OID, return pg_index.indisclustered.
 */
pub unsafe fn get_index_isclustered(index_oid: Oid) -> bool {
    let isclustered: bool;
    let tuple: HeapTuple;
    let rd_index: Form_pg_index;

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for index {}", index_oid);
    }

    rd_index = GETSTRUCT(tuple) as Form_pg_index;
    isclustered = (*rd_index).indisclustered;
    ReleaseSysCache(tuple);

    isclustered
}

/*
 * get_publication_oid - given a publication name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
pub unsafe fn get_publication_oid(pubname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid1(
        PUBLICATIONNAME,
        Anum_pg_publication_oid,
        CStringGetDatum(pubname),
    );
    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, errmsg!("publication {:?} does not exist", pubname)) /* C also: errcode */;
    }
    oid
}

/*
 * get_publication_name - given a publication Oid, look up the name
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return NULL.
 */
pub unsafe fn get_publication_name(pubid: Oid, missing_ok: bool) -> *mut c_char {
    let tup: HeapTuple;
    let pubname: *mut c_char;
    let pubform: Form_pg_publication;

    tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

    if !HeapTupleIsValid(tup) {
        if !missing_ok {
            elog!(ERROR, "cache lookup failed for publication {}", pubid);
        }
        return core::ptr::null_mut();
    }

    pubform = GETSTRUCT(tup) as Form_pg_publication;
    pubname = pstrdup(NameStr(&(*pubform).pubname));

    ReleaseSysCache(tup);

    pubname
}

/*
 * get_subscription_oid - given a subscription name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
pub unsafe fn get_subscription_oid(subname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid2(
        SUBSCRIPTIONNAME,
        Anum_pg_subscription_oid,
        MyDatabaseId_datum(),
        CStringGetDatum(subname),
    );
    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, errmsg!("subscription {:?} does not exist", subname)) /* C also: errcode */;
    }
    oid
}

/*
 * get_subscription_name - given a subscription OID, look up the name
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return NULL.
 */
pub unsafe fn get_subscription_name(subid: Oid, missing_ok: bool) -> *mut c_char {
    let tup: HeapTuple;
    let subname: *mut c_char;
    let subform: Form_pg_subscription;

    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if !HeapTupleIsValid(tup) {
        if !missing_ok {
            elog!(ERROR, "cache lookup failed for subscription {}", subid);
        }
        return core::ptr::null_mut();
    }

    subform = GETSTRUCT(tup) as Form_pg_subscription;
    subname = pstrdup(NameStr(&(*subform).subname));

    ReleaseSysCache(tup);

    subname
}

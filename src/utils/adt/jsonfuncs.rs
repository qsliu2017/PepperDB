//! jsonfuncs.rs
//!   Functions to process JSON data types.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/jsonfuncs.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonfuncs.c

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::{
    PG_RETURN_NULL, PG_RETURN_TEXT_P, PG_RETURN_POINTER, PG_RETURN_INT32, PG_RETURN_DATUM,
};
// PG_RETURN_JSONB_P: not a crate macro; Jsonb is a pointer -> return as pointer.
macro_rules! PG_RETURN_JSONB_P {
    ($x:expr) => { return crate::postgres::PointerGetDatum($x as *const core::ffi::c_void) };
}
// extra local shims for jsonfuncs
use crate::IsA;
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{ let _ = &$escontext; $crate::ereport!(ERROR, $($arg)*); return $dummy; }};
}
macro_rules! errcontext {
    ($($arg:tt)*) => {{ let _ = format!($($arg)*); }};
}
// RECORDARRAYOID (pg_type.dat) - not yet in pg_type_d.
const RECORDARRAYOID: Oid = 2287;
// errsave(escontext, ...): soft-error shim -> ereport!(ERROR, ...) (the elog shim ignores escontext).
macro_rules! errsave {
    ($escontext:expr, $($arg:tt)*) => {{ let _ = &$escontext; $crate::ereport!(ERROR, $($arg)*); }};
}

use std::ffi::{c_char, c_int, c_void};

use crate::postgres_ext::Oid;
use crate::c::{int32, uint32, Size};

use crate::utils::adt::jsonb_util::{
    Jsonb, JsonbValue, JsonbContainer, JsonbIterator, JsonbParseState, jbvType,
    JsonbIteratorToken, JsonbPair,
    JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb, pushJsonbValue,
    getKeyJsonValueFromContainer, getIthJsonbValueFromContainer, IsAJsonbScalar,
    JsonbToJsonbValue, JsonContainerSize, JsonContainerIsArray, JsonContainerIsObject,
    JsonContainerIsScalar,
    JB_FSCALAR, JB_FOBJECT, JB_FARRAY, JB_CMASK,
};
pub use crate::utils::adt::jsonb_util::jbvType::*;
pub use crate::utils::adt::jsonb_util::JsonbIteratorToken::*;

use crate::common::jsonapi::{
    JsonLexContext, JsonSemAction, JsonParseErrorType, JsonTokenType,
    pg_parse_json, json_count_array_elements, makeJsonLexContextCstringLen,
    freeJsonLexContext, json_errdetail,
    JSON_SUCCESS, JSON_SEM_ACTION_FAILED,
    JSON_UNICODE_HIGH_ESCAPE, JSON_UNICODE_UNTRANSLATABLE, JSON_UNICODE_CODE_POINT_ZERO,
    JSON_TOKEN_INVALID, JSON_TOKEN_STRING, JSON_TOKEN_NUMBER, JSON_TOKEN_NULL,
    JSON_TOKEN_TRUE, JSON_TOKEN_FALSE, JSON_TOKEN_END,
    JSON_TOKEN_ARRAY_START, JSON_TOKEN_OBJECT_START,
};

use crate::utils::adt::varlena::{cstring_to_text, cstring_to_text_with_len, text_to_cstring};
use crate::utils::adt::json::{
    escape_json, escape_json_text, JsonTypeCategory,
};
pub use crate::utils::adt::json::JsonTypeCategory::*;

use crate::utils::fmgr::{FunctionCallInfo, FmgrInfo};
use crate::lib::stringinfo::StringInfo;
use crate::access::common::tupdesc::TupleDesc;
use crate::utils::hash::dynahash::HTAB;
use crate::catalog::pg_type_d::{
    JSONOID, JSONBOID, TEXTOID, RECORDOID, BOOLOID, INT2OID, INT4OID, INT8OID,
    FLOAT4OID, FLOAT8OID, NUMERICOID, DATEOID, TIMESTAMPOID, TIMESTAMPTZOID,
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID,
};

/* ------------------------------------------------------------------------
 * TODO(pg-port): the following symbols have no home yet in the port.  They
 * are stubbed minimally so this 1:1 translation type-checks; replace with the
 * real definitions as those modules land.
 * ------------------------------------------------------------------------ */

pub const NAMEDATALEN: usize = 64;
pub const VARHDRSZ: c_int = 4;
pub const INT_MIN: c_int = i32::MIN;
pub const PG_INT32_MIN: c_int = i32::MIN;
pub const InvalidOid: Oid = 0;
pub const FirstNormalObjectId: Oid = 16384;

/* TODO(pg-port): real text/ArrayType/HeapTuple types live in utils/adt/varlena.rs,
 * utils/array.rs, access/htup.rs. */
pub type text = c_void;
pub type ArrayType = c_void;
pub type HeapTuple = *mut c_void;
pub type HeapTupleHeader = *mut c_void;
pub type HeapTupleData = c_void;
pub type Tuplestorestate = c_void;
pub type MemoryContext = *mut c_void;
pub type Node = *mut c_void;
pub type ArrayBuildState = *mut c_void;
pub type ReturnSetInfo = c_void;
pub type Numeric = *mut c_void;
pub type Datum = crate::postgres::Datum;
pub type Form_pg_type = *mut c_void;
pub type Form_pg_attribute = *mut c_void;
pub type CoercionPathType = c_int;
pub type TypeFuncClass = c_int;

/* HASHCTL / hash flags - TODO(pg-port): real ones in utils/hash/hsearch.rs */
#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
    pub hcxt: MemoryContext,
}
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_STRINGS: c_int = 0x0040;
pub const HASH_CONTEXT: c_int = 0x0080;
pub const HASH_FIND: c_int = 0;
pub const HASH_ENTER: c_int = 1;

/* type categories - TODO(pg-port): catalog/pg_type.h */
pub const TYPTYPE_DOMAIN: c_char = b'd' as c_char;
pub const TYPTYPE_COMPOSITE: c_char = b'c' as c_char;

/* coercion - TODO(pg-port): parser/parse_coerce.h */
pub const COERCION_EXPLICIT: c_int = 2;
pub const COERCION_PATH_FUNC: c_int = 1;

/* funcapi TypeFuncClass - TODO(pg-port): funcapi.h */
pub const TYPEFUNC_COMPOSITE: c_int = 0;

/* SFRM flags - TODO(pg-port): nodes/execnodes.h ReturnSetInfo */
pub const SFRM_Materialize: c_int = 0x08;
pub const SFRM_Materialize_Random: c_int = 0x10;

/* MAT_SRF flags - TODO(pg-port): funcapi.h InitMaterializedSRF */
pub const MAT_SRF_USE_EXPECTED_DESC: c_int = 0x01;
pub const MAT_SRF_BLESS: c_int = 0x02;

/* json type index flags (jtiXXX) - TODO(pg-port): utils/jsonfuncs.h */
pub const jtiKey: uint32 = 0x01;
pub const jtiString: uint32 = 0x02;
pub const jtiNumeric: uint32 = 0x04;
pub const jtiBool: uint32 = 0x08;
pub const jtiAll: uint32 = 0x0F;

/* Operations available for setPath */
pub const JB_PATH_CREATE: c_int = 0x0001;
pub const JB_PATH_DELETE: c_int = 0x0002;
pub const JB_PATH_REPLACE: c_int = 0x0004;
pub const JB_PATH_INSERT_BEFORE: c_int = 0x0008;
pub const JB_PATH_INSERT_AFTER: c_int = 0x0010;
pub const JB_PATH_CREATE_OR_INSERT: c_int =
    JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE;
pub const JB_PATH_FILL_GAPS: c_int = 0x0020;
pub const JB_PATH_CONSISTENT_POSITION: c_int = 0x0040;

/* callback action types - TODO(pg-port): utils/jsonfuncs.h */
pub type JsonIterateStringValuesAction =
    unsafe fn(state: *mut c_void, token: *mut c_char, token_len: c_int);
pub type JsonTransformStringValuesAction =
    unsafe fn(state: *mut c_void, token: *mut c_char, token_len: c_int) -> *mut text;

/* ErrorSaveContext - TODO(pg-port): nodes/miscnodes.h */
#[repr(C)]
pub struct ErrorSaveContext {
    pub type_: c_int,
    pub error_occurred: bool,
}
pub const T_ErrorSaveContext: c_int = 0;

/* SRF / FuncCallContext - TODO(pg-port): real machinery lives in funcapi.rs
 * (mirrors the local stub in utils/adt/acl.rs). */
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub user_fctx: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
}
unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!("SRF_IS_FIRSTCALL (funcapi.h) not yet ported")
}
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!("SRF_FIRSTCALL_INIT (funcapi.h) not yet ported")
}
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!("SRF_PERCALL_SETUP (funcapi.h) not yet ported")
}
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!("SRF_RETURN_NEXT (funcapi.h) not yet ported")
}
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!("SRF_RETURN_DONE (funcapi.h) not yet ported")
}
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!("InitMaterializedSRF (funcapi.h) not yet ported")
}

/* ---- generic helpers used throughout (TODO(pg-port): real homes vary) ---- */

unsafe fn palloc(size: usize) -> *mut c_void {
    unimplemented!("palloc (utils/mmgr/mcxt.c) not yet ported")
}
unsafe fn palloc0(size: usize) -> *mut c_void {
    unimplemented!("palloc0 (utils/mmgr/mcxt.c) not yet ported")
}
unsafe fn pfree(_p: *mut c_void) {
    unimplemented!("pfree (utils/mmgr/mcxt.c) not yet ported")
}
unsafe fn repalloc(_p: *mut c_void, _size: usize) -> *mut c_void {
    unimplemented!("repalloc (utils/mmgr/mcxt.c) not yet ported")
}
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    unimplemented!("pstrdup (utils/mmgr/mcxt.c) not yet ported")
}
unsafe fn pnstrdup(_s: *const c_char, _len: c_int) -> *mut c_char {
    unimplemented!("pnstrdup (utils/mmgr/mcxt.c) not yet ported")
}
unsafe fn MemoryContextAlloc(_cxt: MemoryContext, _size: usize) -> *mut c_void {
    unimplemented!("MemoryContextAlloc not yet ported")
}
unsafe fn MemoryContextAllocZero(_cxt: MemoryContext, _size: usize) -> *mut c_void {
    unimplemented!("MemoryContextAllocZero not yet ported")
}
unsafe fn MemoryContextSwitchTo(_cxt: MemoryContext) -> MemoryContext {
    unimplemented!("MemoryContextSwitchTo not yet ported")
}
unsafe fn MemoryContextReset(_cxt: MemoryContext) {
    unimplemented!("MemoryContextReset not yet ported")
}
unsafe fn MemoryContextDelete(_cxt: MemoryContext) {
    unimplemented!("MemoryContextDelete not yet ported")
}
unsafe fn AllocSetContextCreate(_parent: MemoryContext, _name: *const c_char, _flags: c_int) -> MemoryContext {
    unimplemented!("AllocSetContextCreate not yet ported")
}
unsafe fn check_stack_depth() {}
fn CurrentMemoryContext() -> MemoryContext {
    unimplemented!("CurrentMemoryContext not yet ported")
}
const ALLOCSET_DEFAULT_SIZES: c_int = 0;
fn work_mem() -> c_int {
    unimplemented!("work_mem (GUC) not yet ported")
}

/* StringInfo helpers - TODO(pg-port): lib/stringinfo.rs */
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!("initStringInfo not yet ported")
}
unsafe fn makeStringInfo() -> *mut StringInfoData {
    unimplemented!("makeStringInfo not yet ported")
}
unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!("appendStringInfoString not yet ported")
}
unsafe fn appendStringInfoChar(_str: *mut StringInfoData, _ch: c_char) {
    unimplemented!("appendStringInfoChar not yet ported")
}

/* hash helpers - TODO(pg-port): utils/hash/dynahash.rs */
unsafe fn hash_create(_name: *const c_char, _nelem: c_long, _ctl: *mut HASHCTL, _flags: c_int) -> *mut HTAB {
    unimplemented!("hash_create not yet ported")
}
unsafe fn hash_destroy(_tab: *mut HTAB) {
    unimplemented!("hash_destroy not yet ported")
}
unsafe fn hash_search(_tab: *mut HTAB, _key: *const c_void, _action: c_int, _found: *mut bool) -> *mut c_void {
    unimplemented!("hash_search not yet ported")
}
unsafe fn hash_get_num_entries(_tab: *mut HTAB) -> c_long {
    unimplemented!("hash_get_num_entries not yet ported")
}
type c_long = i64;

/* tuple / array / type helpers - TODO(pg-port): assorted homes */
unsafe fn heap_form_tuple(_tupdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple {
    unimplemented!("heap_form_tuple not yet ported")
}
unsafe fn heap_deform_tuple(_tuple: *mut HeapTupleData, _tupdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) {
    unimplemented!("heap_deform_tuple not yet ported")
}
unsafe fn tuplestore_putvalues(_state: *mut Tuplestorestate, _tdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) {
    unimplemented!("tuplestore_putvalues not yet ported")
}
unsafe fn tuplestore_puttuple(_state: *mut Tuplestorestate, _tuple: *mut HeapTupleData) {
    unimplemented!("tuplestore_puttuple not yet ported")
}
unsafe fn tuplestore_begin_heap(_randomAccess: bool, _interXact: bool, _maxKBytes: c_int) -> *mut Tuplestorestate {
    unimplemented!("tuplestore_begin_heap not yet ported")
}
unsafe fn array_contains_nulls(_array: *mut ArrayType) -> bool {
    unimplemented!("array_contains_nulls not yet ported")
}
unsafe fn deconstruct_array_builtin(_array: *mut ArrayType, _elmtype: Oid, _elemsp: *mut *mut Datum, _nullsp: *mut *mut bool, _nelemsp: *mut c_int) {
    unimplemented!("deconstruct_array_builtin not yet ported")
}
unsafe fn initArrayResult(_element_type: Oid, _rcontext: MemoryContext, _subcontext: bool) -> ArrayBuildState {
    unimplemented!("initArrayResult not yet ported")
}
unsafe fn accumArrayResult(_astate: ArrayBuildState, _dvalue: Datum, _disnull: bool, _element_type: Oid, _rcontext: MemoryContext) -> ArrayBuildState {
    unimplemented!("accumArrayResult not yet ported")
}
unsafe fn makeMdArrayResult(_astate: ArrayBuildState, _ndims: c_int, _dims: *mut c_int, _lbs: *mut c_int, _rcontext: MemoryContext, _release: bool) -> Datum {
    unimplemented!("makeMdArrayResult not yet ported")
}

/* type cache / syscache - TODO(pg-port): utils/cache homes */
unsafe fn lookup_rowtype_tupdesc(_type_id: Oid, _typmod: int32) -> TupleDesc {
    unimplemented!("lookup_rowtype_tupdesc not yet ported")
}
unsafe fn CreateTupleDescCopy(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!("CreateTupleDescCopy not yet ported")
}
unsafe fn FreeTupleDesc(_tupdesc: TupleDesc) {
    unimplemented!("FreeTupleDesc not yet ported")
}
unsafe fn ReleaseTupleDesc(_tupdesc: TupleDesc) {
    unimplemented!("ReleaseTupleDesc not yet ported")
}
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!("SearchSysCache1 not yet ported")
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!("ReleaseSysCache not yet ported")
}
const TYPEOID: c_int = 0;
unsafe fn getBaseType(_typid: Oid) -> Oid {
    unimplemented!("getBaseType not yet ported")
}
unsafe fn getBaseTypeAndTypmod(_typid: Oid, _typmod: *mut int32) -> Oid {
    unimplemented!("getBaseTypeAndTypmod not yet ported")
}
unsafe fn get_typtype(_typid: Oid) -> c_char {
    unimplemented!("get_typtype not yet ported")
}
unsafe fn get_element_type(_typid: Oid) -> Oid {
    unimplemented!("get_element_type not yet ported")
}
unsafe fn type_is_rowtype(_typid: Oid) -> bool {
    unimplemented!("type_is_rowtype not yet ported")
}
unsafe fn getTypeInputInfo(_type: Oid, _typInput: *mut Oid, _typIOParam: *mut Oid) {
    unimplemented!("getTypeInputInfo not yet ported")
}
unsafe fn getTypeOutputInfo(_type: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!("getTypeOutputInfo not yet ported")
}
unsafe fn find_coercion_pathway(_targetTypeId: Oid, _sourceTypeId: Oid, _ccontext: c_int, _funcid: *mut Oid) -> CoercionPathType {
    unimplemented!("find_coercion_pathway not yet ported")
}
unsafe fn fmgr_info_cxt(_functionId: Oid, _finfo: *mut FmgrInfo, _mcxt: MemoryContext) {
    unimplemented!("fmgr_info_cxt not yet ported")
}
unsafe fn InputFunctionCallSafe(_flinfo: *mut FmgrInfo, _str: *mut c_char, _typioparam: Oid, _typmod: int32, _escontext: Node, _result: *mut Datum) -> bool {
    unimplemented!("InputFunctionCallSafe not yet ported")
}
unsafe fn domain_check_safe(_value: Datum, _isnull: bool, _domainType: Oid, _extra: *mut *mut c_void, _mcxt: MemoryContext, _escontext: Node) -> bool {
    unimplemented!("domain_check_safe not yet ported")
}
unsafe fn DirectFunctionCall1(_func: c_int, _arg1: Datum) -> Datum {
    unimplemented!("DirectFunctionCall1 not yet ported")
}

/* jsonb_util C-string helpers that are not yet exported - TODO(pg-port):
 * real ones live in utils/adt/jsonb_util.rs (JsonbToCString etc). */
unsafe fn JsonbToCString(_out: *mut StringInfoData, _in: *mut JsonbContainer, _estimated_len: c_int) -> *mut c_char {
    unimplemented!("JsonbToCString not yet ported")
}
unsafe fn JsonbToCStringIndent(_out: *mut StringInfoData, _in: *mut JsonbContainer, _estimated_len: c_int) -> *mut c_char {
    unimplemented!("JsonbToCStringIndent not yet ported")
}
unsafe fn JsonbUnquote(_jb: *mut Jsonb) -> *mut c_char {
    unimplemented!("JsonbUnquote not yet ported")
}

/* misc */
unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!("GetDatabaseEncoding not yet ported")
}
unsafe fn pg_mblen_range(_start: *const c_char, _end: *const c_char) -> c_int {
    unimplemented!("pg_mblen_range not yet ported")
}
unsafe fn pg_detoast_datum_packed(_datum: *mut c_void) -> *mut c_void {
    unimplemented!("pg_detoast_datum_packed not yet ported")
}
unsafe fn strtoint(_str: *const c_char, _endptr: *mut *mut c_char, _base: c_int) -> c_int {
    unimplemented!("strtoint not yet ported")
}
unsafe fn pg_strncasecmp(_s1: *const c_char, _s2: *const c_char, _n: usize) -> c_int {
    unimplemented!("pg_strncasecmp not yet ported")
}
unsafe fn pg_abs_s32(_a: int32) -> uint32 {
    unimplemented!("pg_abs_s32 not yet ported")
}
unsafe fn get_fn_expr_argtype(_flinfo: *mut FmgrInfo, _argnum: c_int) -> Oid {
    unimplemented!("get_fn_expr_argtype not yet ported")
}
unsafe fn get_call_result_type(_fcinfo: FunctionCallInfo, _resultTypeId: *mut Oid, _resultTupleDesc: *mut TupleDesc) -> TypeFuncClass {
    unimplemented!("get_call_result_type not yet ported")
}
unsafe fn json_lex(_lex: *mut JsonLexContext) -> JsonParseErrorType {
    unimplemented!("json_lex not yet ported")
}
fn OidIsValid(oid: Oid) -> bool { oid != InvalidOid }

/* PG argument/return macros - TODO(pg-port): fmgr.h family */
unsafe fn PG_GETARG_JSONB_P(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut Jsonb {
    unimplemented!("PG_GETARG_JSONB_P not yet ported")
}
unsafe fn PG_GETARG_TEXT_PP(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut text {
    unimplemented!("PG_GETARG_TEXT_PP not yet ported")
}
unsafe fn PG_GETARG_TEXT_P(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut text {
    unimplemented!("PG_GETARG_TEXT_P not yet ported")
}
unsafe fn PG_GETARG_ARRAYTYPE_P(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut ArrayType {
    unimplemented!("PG_GETARG_ARRAYTYPE_P not yet ported")
}
unsafe fn PG_GETARG_INT32(_fcinfo: FunctionCallInfo, _n: c_int) -> int32 {
    unimplemented!("PG_GETARG_INT32 not yet ported")
}
unsafe fn PG_GETARG_BOOL(_fcinfo: FunctionCallInfo, _n: c_int) -> bool {
    unimplemented!("PG_GETARG_BOOL not yet ported")
}
unsafe fn PG_GETARG_HEAPTUPLEHEADER(_fcinfo: FunctionCallInfo, _n: c_int) -> HeapTupleHeader {
    unimplemented!("PG_GETARG_HEAPTUPLEHEADER not yet ported")
}
unsafe fn PG_ARGISNULL(_fcinfo: FunctionCallInfo, _n: c_int) -> bool {
    unimplemented!("PG_ARGISNULL not yet ported")
}
unsafe fn PG_NARGS(_fcinfo: FunctionCallInfo) -> c_int {
    unimplemented!("PG_NARGS not yet ported")
}

/* VARDATA_ANY / VARSIZE_ANY_EXHDR / VARSIZE - TODO(pg-port): postgres.h varlena */
unsafe fn VARDATA_ANY(_ptr: *mut c_void) -> *mut c_char {
    unimplemented!("VARDATA_ANY not yet ported")
}
unsafe fn VARSIZE_ANY_EXHDR(_ptr: *mut c_void) -> c_int {
    unimplemented!("VARSIZE_ANY_EXHDR not yet ported")
}
unsafe fn VARSIZE(_ptr: *mut c_void) -> c_int {
    unimplemented!("VARSIZE not yet ported")
}

/* Datum conversion macros - TODO(pg-port): postgres.h */
unsafe fn PointerGetDatum(_p: *const c_void) -> Datum {
    unimplemented!("PointerGetDatum not yet ported")
}
unsafe fn DatumGetPointer(_d: Datum) -> *mut c_void {
    unimplemented!("DatumGetPointer not yet ported")
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!("CStringGetTextDatum not yet ported")
}
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!("TextDatumGetCString not yet ported")
}
unsafe fn DatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!("DatumGetCString not yet ported")
}
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum {
    unimplemented!("CStringGetDatum not yet ported")
}
unsafe fn DatumGetTextPP(_d: Datum) -> *mut text {
    unimplemented!("DatumGetTextPP not yet ported")
}
unsafe fn DatumGetJsonbP(_d: Datum) -> *mut Jsonb {
    unimplemented!("DatumGetJsonbP not yet ported")
}
unsafe fn JsonbPGetDatum(_jb: *mut Jsonb) -> Datum {
    unimplemented!("JsonbPGetDatum not yet ported")
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!("ObjectIdGetDatum not yet ported")
}
unsafe fn BoolGetDatum(_b: bool) -> Datum {
    unimplemented!("BoolGetDatum not yet ported")
}
unsafe fn HeapTupleHeaderGetDatum(_tuple: HeapTupleHeader) -> Datum {
    unimplemented!("HeapTupleHeaderGetDatum not yet ported")
}
unsafe fn DatumGetHeapTupleHeader(_d: Datum) -> HeapTupleHeader {
    unimplemented!("DatumGetHeapTupleHeader not yet ported")
}
unsafe fn NumericGetDatum(_n: Numeric) -> Datum {
    unimplemented!("NumericGetDatum not yet ported")
}

/* fmgroids referenced by json_categorize_type - TODO(pg-port): utils/fmgroids.h */
pub const F_BOOLOUT: c_int = 0;
pub const F_DATE_OUT: c_int = 0;
pub const F_TIMESTAMP_OUT: c_int = 0;
pub const F_TIMESTAMPTZ_OUT: c_int = 0;
pub const F_ARRAY_OUT: c_int = 0;
pub const F_RECORD_OUT: c_int = 0;
/* numeric_in/out, jsonb_in referenced via DirectFunctionCall1 */
pub const numeric_out: c_int = 0;
pub const jsonb_in: c_int = 0;

/* ------------------------------------------------------------------------
 * State structs (1:1 with the C typedefs)
 * ------------------------------------------------------------------------ */

/* state for json_object_keys */
#[repr(C)]
pub struct OkeysState {
    pub lex: *mut JsonLexContext,
    pub result: *mut *mut c_char,
    pub result_size: c_int,
    pub result_count: c_int,
    pub sent_count: c_int,
}

/* state for iterate_json_values function */
#[repr(C)]
pub struct IterateJsonStringValuesState {
    pub lex: *mut JsonLexContext,
    pub action: JsonIterateStringValuesAction,
    pub action_state: *mut c_void,
    pub flags: uint32,
}

/* state for transform_json_string_values function */
#[repr(C)]
pub struct TransformJsonStringValuesState {
    pub lex: *mut JsonLexContext,
    pub strval: StringInfo,
    pub action: JsonTransformStringValuesAction,
    pub action_state: *mut c_void,
}

/* state for json_get* functions */
#[repr(C)]
pub struct GetState {
    pub lex: *mut JsonLexContext,
    pub tresult: *mut text,
    pub result_start: *const c_char,
    pub normalize_results: bool,
    pub next_scalar: bool,
    pub npath: c_int,
    pub path_names: *mut *mut c_char,
    pub path_indexes: *mut c_int,
    pub pathok: *mut bool,
    pub array_cur_index: *mut c_int,
}

/* state for json_array_length */
#[repr(C)]
pub struct AlenState {
    pub lex: *mut JsonLexContext,
    pub count: c_int,
}

/* state for json_each */
#[repr(C)]
pub struct EachState {
    pub lex: *mut JsonLexContext,
    pub tuple_store: *mut Tuplestorestate,
    pub ret_tdesc: TupleDesc,
    pub tmp_cxt: MemoryContext,
    pub result_start: *const c_char,
    pub normalize_results: bool,
    pub next_scalar: bool,
    pub normalized_scalar: *mut c_char,
}

/* state for json_array_elements */
#[repr(C)]
pub struct ElementsState {
    pub lex: *mut JsonLexContext,
    pub function_name: *const c_char,
    pub tuple_store: *mut Tuplestorestate,
    pub ret_tdesc: TupleDesc,
    pub tmp_cxt: MemoryContext,
    pub result_start: *const c_char,
    pub normalize_results: bool,
    pub next_scalar: bool,
    pub normalized_scalar: *mut c_char,
}

/* state for get_json_object_as_hash */
#[repr(C)]
pub struct JHashState {
    pub lex: *mut JsonLexContext,
    pub function_name: *const c_char,
    pub hash: *mut HTAB,
    pub saved_scalar: *mut c_char,
    pub save_json_start: *const c_char,
    pub saved_token_type: JsonTokenType,
}

/* hashtable element */
#[repr(C)]
pub struct JsonHashEntry {
    pub fname: [c_char; NAMEDATALEN], /* hash key (MUST BE FIRST) */
    pub val: *mut c_char,
    pub type_: JsonTokenType,
}

/* structure to cache type I/O metadata needed for populate_scalar() */
#[repr(C)]
pub struct ScalarIOData {
    pub typioparam: Oid,
    pub typiofunc: FmgrInfo,
}

/* structure to cache metadata needed for populate_array() */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ArrayIOData {
    pub element_info: *mut ColumnIOData,
    pub element_type: Oid,
    pub element_typmod: int32,
}

/* structure to cache metadata needed for populate_composite() */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CompositeIOData {
    pub record_io: *mut RecordIOData,
    pub tupdesc: TupleDesc,
    pub base_typid: Oid,
    pub base_typmod: int32,
    pub domain_info: *mut c_void,
}

/* structure to cache metadata needed for populate_domain() */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DomainIOData {
    pub base_io: *mut ColumnIOData,
    pub base_typid: Oid,
    pub base_typmod: int32,
    pub domain_info: *mut c_void,
}

/* enumeration type categories */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum TypeCat {
    TYPECAT_SCALAR = b's' as i8,
    TYPECAT_ARRAY = b'a' as i8,
    TYPECAT_COMPOSITE = b'c' as i8,
    TYPECAT_COMPOSITE_DOMAIN = b'C' as i8,
    TYPECAT_DOMAIN = b'd' as i8,
}
pub use TypeCat::*;

#[repr(C)]
pub union ColumnIODataIO {
    pub array: ArrayIOData,
    pub composite: CompositeIOData,
    pub domain: DomainIOData,
}

/* structure to cache record metadata needed for populate_record_field() */
#[repr(C)]
pub struct ColumnIOData {
    pub typid: Oid,
    pub typmod: int32,
    pub typcat: TypeCat,
    pub scalar_io: ScalarIOData,
    pub io: ColumnIODataIO,
}

/* structure to cache record metadata needed for populate_record() */
#[repr(C)]
pub struct RecordIOData {
    pub record_type: Oid,
    pub record_typmod: int32,
    pub ncolumns: c_int,
    pub columns: [ColumnIOData; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* per-query cache for populate_record_worker and populate_recordset_worker */
#[repr(C)]
pub struct PopulateRecordCache {
    pub argtype: Oid,
    pub c: ColumnIOData,
    pub fn_mcxt: MemoryContext,
}

/* per-call state for populate_recordset */
#[repr(C)]
pub struct PopulateRecordsetState {
    pub lex: *mut JsonLexContext,
    pub function_name: *const c_char,
    pub json_hash: *mut HTAB,
    pub saved_scalar: *mut c_char,
    pub save_json_start: *const c_char,
    pub saved_token_type: JsonTokenType,
    pub tuple_store: *mut Tuplestorestate,
    pub rec: HeapTupleHeader,
    pub cache: *mut PopulateRecordCache,
}

/* common data for populate_array_json() and populate_array_dim_jsonb() */
#[repr(C)]
pub struct PopulateArrayContext {
    pub astate: ArrayBuildState,
    pub aio: *mut ArrayIOData,
    pub acxt: MemoryContext,
    pub mcxt: MemoryContext,
    pub colname: *const c_char,
    pub dims: *mut c_int,
    pub sizes: *mut c_int,
    pub ndims: c_int,
    pub escontext: Node,
}

/* state for populate_array_json() */
#[repr(C)]
pub struct PopulateArrayState {
    pub lex: *mut JsonLexContext,
    pub ctx: *mut PopulateArrayContext,
    pub element_start: *const c_char,
    pub element_scalar: *mut c_char,
    pub element_type: JsonTokenType,
}

/* state for json_strip_nulls */
#[repr(C)]
pub struct StripnullState {
    pub lex: *mut JsonLexContext,
    pub strval: StringInfo,
    pub skip_next_null: bool,
    pub strip_in_arrays: bool,
}

/* structure for generalized json/jsonb value passing */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsValueJson {
    pub str: *const c_char,
    pub len: c_int,
    pub type_: JsonTokenType,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union JsValueVal {
    pub json: JsValueJson,
    pub jsonb: *mut JsonbValue,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsValue {
    pub is_json: bool,
    pub val: JsValueVal,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union JsObjectVal {
    pub json_hash: *mut HTAB,
    pub jsonb_cont: *mut JsonbContainer,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsObject {
    pub is_json: bool,
    pub val: JsObjectVal,
}

/* useful inline helpers for testing JsValue properties (were macros) */
#[inline]
unsafe fn JsValueIsNull(jsv: *const JsValue) -> bool {
    if (*jsv).is_json {
        (*jsv).val.json.str.is_null() || (*jsv).val.json.type_ == JSON_TOKEN_NULL
    } else {
        (*jsv).val.jsonb.is_null() || (*(*jsv).val.jsonb).type_ == jbvNull
    }
}

#[inline]
unsafe fn JsValueIsString(jsv: *const JsValue) -> bool {
    if (*jsv).is_json {
        (*jsv).val.json.type_ == JSON_TOKEN_STRING
    } else {
        !(*jsv).val.jsonb.is_null() && (*(*jsv).val.jsonb).type_ == jbvString
    }
}

#[inline]
unsafe fn JsObjectIsEmpty(jso: *const JsObject) -> bool {
    if (*jso).is_json {
        hash_get_num_entries((*jso).val.json_hash) == 0
    } else {
        (*jso).val.jsonb_cont.is_null() || JsonContainerSize((*jso).val.jsonb_cont) == 0
    }
}

#[inline]
unsafe fn JsObjectFree(jso: *mut JsObject) {
    if (*jso).is_json {
        hash_destroy((*jso).val.json_hash);
    }
}

/* JB_ROOT_* helpers - operate on Jsonb root container */
#[inline]
unsafe fn JB_ROOT_COUNT(jbp: *mut Jsonb) -> uint32 {
    (*jbp).root.header & JB_CMASK
}
#[inline]
unsafe fn JB_ROOT_IS_SCALAR(jbp: *mut Jsonb) -> bool {
    ((*jbp).root.header & JB_FSCALAR) != 0
}
#[inline]
unsafe fn JB_ROOT_IS_OBJECT(jbp: *mut Jsonb) -> bool {
    ((*jbp).root.header & JB_FOBJECT) != 0
}
#[inline]
unsafe fn JB_ROOT_IS_ARRAY(jbp: *mut Jsonb) -> bool {
    ((*jbp).root.header & JB_FARRAY) != 0
}

/* pg_parse_json_or_ereport / errsave helpers - TODO(pg-port): json.rs has a
 * private pg_parse_json_or_ereport; mirror it locally until exported. */
unsafe fn pg_parse_json_or_ereport(_lex: *mut JsonLexContext, _sem: *mut JsonSemAction) {
    unimplemented!("pg_parse_json_or_ereport (common/jsonapi.c) not yet ported")
}
unsafe fn SOFT_ERROR_OCCURRED(_escontext: Node) -> bool {
    unimplemented!("SOFT_ERROR_OCCURRED (nodes/miscnodes.h) not yet ported")
}

/*
 * pg_parse_json_or_errsave
 *
 * This function is like pg_parse_json, except that it does not return a
 * JsonParseErrorType. Instead, in case of any failure, this function will
 * save error data into *escontext if that's an ErrorSaveContext, otherwise
 * ereport(ERROR).
 */
pub unsafe fn pg_parse_json_or_errsave(
    lex: *mut JsonLexContext,
    sem: *const JsonSemAction,
    escontext: Node,
) -> bool {
    let result: JsonParseErrorType;

    result = pg_parse_json(lex, sem);
    if result != JSON_SUCCESS {
        json_errsave_error(result, lex, escontext);
        return false;
    }
    true
}

/*
 * makeJsonLexContext
 *
 * This is like makeJsonLexContextCstringLen, but it accepts a text value
 * directly.
 */
pub unsafe fn makeJsonLexContext(
    lex: *mut JsonLexContext,
    mut json: *mut text,
    need_escapes: bool,
) -> *mut JsonLexContext {
    /*
     * Most callers pass a detoasted datum, but it's not clear that they all
     * do.  pg_detoast_datum_packed() is cheap insurance.
     */
    json = pg_detoast_datum_packed(json);

    makeJsonLexContextCstringLen(
        lex,
        VARDATA_ANY(json as *mut c_void),
        VARSIZE_ANY_EXHDR(json as *mut c_void) as usize,
        GetDatabaseEncoding(),
        need_escapes,
    )
}

/*
 * SQL function json_object_keys
 *
 * Returns the set of keys for the object argument.
 */
pub unsafe fn jsonb_object_keys(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let mut state: *mut OkeysState;

    if SRF_IS_FIRSTCALL() {
        let oldcontext: MemoryContext;
        let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
        let mut skipNested: bool = false;
        let mut it: *mut JsonbIterator;
        let mut v: JsonbValue = core::mem::zeroed();
        let mut r: JsonbIteratorToken;

        if JB_ROOT_IS_SCALAR(jb) {
            ereport!(ERROR, errmsg!("cannot call {} on a scalar", "jsonb_object_keys"));
        } else if JB_ROOT_IS_ARRAY(jb) {
            ereport!(ERROR, errmsg!("cannot call {} on an array", "jsonb_object_keys"));
        }

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        state = palloc(core::mem::size_of::<OkeysState>()) as *mut OkeysState;

        (*state).result_size = JB_ROOT_COUNT(jb) as c_int;
        (*state).result_count = 0;
        (*state).sent_count = 0;
        (*state).result =
            palloc((*state).result_size as usize * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

        it = JsonbIteratorInit(&mut (*jb).root);

        loop {
            r = JsonbIteratorNext(&mut it, &mut v, skipNested);
            if r == WJB_DONE {
                break;
            }
            skipNested = true;

            if r == WJB_KEY {
                let cstr: *mut c_char;

                cstr = palloc(v.val.string.len as usize + 1 * core::mem::size_of::<c_char>()) as *mut c_char;
                std::ptr::copy_nonoverlapping(v.val.string.val, cstr, v.val.string.len as usize);
                *cstr.offset(v.val.string.len as isize) = b'\0' as c_char;
                *(*state).result.offset((*state).result_count as isize) = cstr;
                (*state).result_count += 1;
            }
        }

        MemoryContextSwitchTo(oldcontext);
        (*funcctx).user_fctx = state as *mut c_void;
    }

    funcctx = SRF_PERCALL_SETUP();
    state = (*funcctx).user_fctx as *mut OkeysState;

    if (*state).sent_count < (*state).result_count {
        let nxt: *mut c_char = *(*state).result.offset((*state).sent_count as isize);
        (*state).sent_count += 1;

        return SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt));
    }

    SRF_RETURN_DONE(funcctx)
}

/*
 * Report a JSON error.
 */
pub unsafe fn json_errsave_error(
    error: JsonParseErrorType,
    lex: *mut JsonLexContext,
    escontext: Node,
) {
    if error == JSON_UNICODE_HIGH_ESCAPE
        || error == JSON_UNICODE_UNTRANSLATABLE
        || error == JSON_UNICODE_CODE_POINT_ZERO
    {
        errsave!(escontext, errmsg!("unsupported Unicode escape sequence"));
    } else if error == JSON_SEM_ACTION_FAILED {
        /* semantic action function had better have reported something */
        if !SOFT_ERROR_OCCURRED(escontext) {
            elog!(ERROR, "JSON semantic action function did not provide error information");
        }
    } else {
        let detail = std::ffi::CStr::from_ptr(json_errdetail(error, lex)).to_string_lossy();
        errsave!(
            escontext,
            errmsg!("invalid input syntax for type {}: {}", "json", detail)
        );
    }
}

/*
 * Report a CONTEXT line for bogus JSON input.
 */
unsafe fn report_json_context(lex: *mut JsonLexContext) -> c_int {
    let mut context_start: *const c_char;
    let context_end: *const c_char;
    let line_start: *const c_char;
    let ctxt: *mut c_char;
    let ctxtlen: c_int;
    let prefix: *const c_char;
    let suffix: *const c_char;

    /* Choose boundaries for the part of the input we will display */
    line_start = (*lex).line_start;
    context_start = line_start;
    context_end = (*lex).token_terminator;
    Assert!(context_end >= context_start);

    /* Advance until we are close enough to context_end */
    while (context_end as isize - context_start as isize) >= 50 {
        /* Advance to next multibyte character */
        if IS_HIGHBIT_SET(*context_start) {
            context_start = context_start.offset(pg_mblen_range(context_start, context_end) as isize);
        } else {
            context_start = context_start.offset(1);
        }
    }

    /*
     * We add "..." to indicate that the excerpt doesn't start at the
     * beginning of the line ... but if we're within 3 characters of the
     * beginning of the line, we might as well just show the whole line.
     */
    if (context_start as isize - line_start as isize) <= 3 {
        context_start = line_start;
    }

    /* Get a null-terminated copy of the data to present */
    ctxtlen = (context_end as isize - context_start as isize) as c_int;
    ctxt = palloc(ctxtlen as usize + 1) as *mut c_char;
    std::ptr::copy_nonoverlapping(context_start, ctxt, ctxtlen as usize);
    *ctxt.offset(ctxtlen as isize) = b'\0' as c_char;

    /*
     * Show the context, prefixing "..." if not starting at start of line, and
     * suffixing "..." if not ending at end of line.
     */
    prefix = if context_start > line_start { c"...".as_ptr() } else { c"".as_ptr() };
    suffix = if (*lex).token_type != JSON_TOKEN_END
        && ((context_end as isize - (*lex).input as isize) as usize) < (*lex).input_length
        && *context_end != b'\n' as c_char
        && *context_end != b'\r' as c_char
    {
        c"...".as_ptr()
    } else {
        c"".as_ptr()
    };

    let _ = (ctxt, prefix, suffix);
    errcontext!(
        "JSON data, line {}: {}{}{}",
        (*lex).line_number,
        std::ffi::CStr::from_ptr(prefix).to_string_lossy(),
        std::ffi::CStr::from_ptr(ctxt).to_string_lossy(),
        std::ffi::CStr::from_ptr(suffix).to_string_lossy()
    );
    0
}

unsafe fn IS_HIGHBIT_SET(ch: c_char) -> bool {
    (ch as u8 & 0x80) != 0
}

pub unsafe extern "C" fn okeys_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut OkeysState;

    /* only collecting keys for the top level object */
    if (*(*_state).lex).lex_level != 1 {
        return JSON_SUCCESS;
    }

    /* enlarge result array if necessary */
    if (*_state).result_count >= (*_state).result_size {
        (*_state).result_size *= 2;
        (*_state).result = repalloc(
            (*_state).result as *mut c_void,
            core::mem::size_of::<*mut c_char>() * (*_state).result_size as usize,
        ) as *mut *mut c_char;
    }

    /* save a copy of the field name */
    *(*_state).result.offset((*_state).result_count as isize) = pstrdup(fname);
    (*_state).result_count += 1;

    JSON_SUCCESS
}

pub unsafe extern "C" fn okeys_array_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut OkeysState;

    /* top level must be a json object */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(ERROR, errmsg!("cannot call {} on an array", "json_object_keys"));
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn okeys_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut OkeysState;

    /* top level must be a json object */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(ERROR, errmsg!("cannot call {} on a scalar", "json_object_keys"));
    }

    JSON_SUCCESS
}

pub unsafe fn json_object_keys(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let mut state: *mut OkeysState;

    if SRF_IS_FIRSTCALL() {
        let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
        let mut lex: JsonLexContext = core::mem::zeroed();
        let sem: *mut JsonSemAction;
        let oldcontext: MemoryContext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        state = palloc(core::mem::size_of::<OkeysState>()) as *mut OkeysState;
        sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;

        (*state).lex = makeJsonLexContext(&mut lex, json, true);
        (*state).result_size = 256;
        (*state).result_count = 0;
        (*state).sent_count = 0;
        (*state).result = palloc(256 * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

        (*sem).semstate = state as *mut c_void;
        (*sem).array_start = Some(okeys_array_start);
        (*sem).scalar = Some(okeys_scalar);
        (*sem).object_field_start = Some(okeys_object_field_start);
        /* remainder are all NULL, courtesy of palloc0 above */

        pg_parse_json_or_ereport(&mut lex, sem);
        /* keys are now in state->result */

        freeJsonLexContext(&mut lex);
        pfree(sem as *mut c_void);

        MemoryContextSwitchTo(oldcontext);
        (*funcctx).user_fctx = state as *mut c_void;
    }

    funcctx = SRF_PERCALL_SETUP();
    state = (*funcctx).user_fctx as *mut OkeysState;

    if (*state).sent_count < (*state).result_count {
        let nxt: *mut c_char = *(*state).result.offset((*state).sent_count as isize);
        (*state).sent_count += 1;

        return SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt));
    }

    SRF_RETURN_DONE(funcctx)
}

/*
 * json and jsonb getter functions
 * these implement the -> ->> #> and #>> operators
 * and the json{b?}_extract_path*(json, text, ...) functions
 */

pub unsafe fn json_object_field(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let fname: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let mut fnamestr: *mut c_char = text_to_cstring(fname as *const crate::c::text);
    let result: *mut text;

    result = get_worker(json, &mut fnamestr, std::ptr::null_mut(), 1, false);

    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

pub unsafe fn jsonb_object_field(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let key: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let v: *mut JsonbValue;
    let mut vbuf: JsonbValue = core::mem::zeroed();

    if !JB_ROOT_IS_OBJECT(jb) {
        PG_RETURN_NULL!(fcinfo);
    }

    v = getKeyJsonValueFromContainer(
        &mut (*jb).root,
        VARDATA_ANY(key),
        VARSIZE_ANY_EXHDR(key),
        &mut vbuf,
    );

    if !v.is_null() {
        PG_RETURN_JSONB_P!(JsonbValueToJsonb(v));
    }

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe fn json_object_field_text(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let fname: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let mut fnamestr: *mut c_char = text_to_cstring(fname as *const crate::c::text);
    let result: *mut text;

    result = get_worker(json, &mut fnamestr, std::ptr::null_mut(), 1, true);

    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

pub unsafe fn jsonb_object_field_text(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let key: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let v: *mut JsonbValue;
    let mut vbuf: JsonbValue = core::mem::zeroed();

    if !JB_ROOT_IS_OBJECT(jb) {
        PG_RETURN_NULL!(fcinfo);
    }

    v = getKeyJsonValueFromContainer(
        &mut (*jb).root,
        VARDATA_ANY(key),
        VARSIZE_ANY_EXHDR(key),
        &mut vbuf,
    );

    if !v.is_null() && (*v).type_ != jbvNull {
        PG_RETURN_TEXT_P!(JsonbValueAsText(v));
    }

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe fn json_array_element(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut element: c_int = PG_GETARG_INT32(fcinfo, 1);
    let result: *mut text;

    result = get_worker(json, std::ptr::null_mut(), &mut element, 1, false);

    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

pub unsafe fn jsonb_array_element(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let mut element: c_int = PG_GETARG_INT32(fcinfo, 1);
    let v: *mut JsonbValue;

    if !JB_ROOT_IS_ARRAY(jb) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Handle negative subscript */
    if element < 0 {
        let nelements: uint32 = JB_ROOT_COUNT(jb);

        if pg_abs_s32(element) > nelements {
            PG_RETURN_NULL!(fcinfo);
        } else {
            element += nelements as c_int;
        }
    }

    v = getIthJsonbValueFromContainer(&mut (*jb).root, element as uint32);
    if !v.is_null() {
        PG_RETURN_JSONB_P!(JsonbValueToJsonb(v));
    }

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe fn json_array_element_text(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut element: c_int = PG_GETARG_INT32(fcinfo, 1);
    let result: *mut text;

    result = get_worker(json, std::ptr::null_mut(), &mut element, 1, true);

    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

pub unsafe fn jsonb_array_element_text(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let mut element: c_int = PG_GETARG_INT32(fcinfo, 1);
    let v: *mut JsonbValue;

    if !JB_ROOT_IS_ARRAY(jb) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Handle negative subscript */
    if element < 0 {
        let nelements: uint32 = JB_ROOT_COUNT(jb);

        if pg_abs_s32(element) > nelements {
            PG_RETURN_NULL!(fcinfo);
        } else {
            element += nelements as c_int;
        }
    }

    v = getIthJsonbValueFromContainer(&mut (*jb).root, element as uint32);

    if !v.is_null() && (*v).type_ != jbvNull {
        PG_RETURN_TEXT_P!(JsonbValueAsText(v));
    }

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe fn json_extract_path(fcinfo: FunctionCallInfo) -> Datum {
    get_path_all(fcinfo, false)
}

pub unsafe fn json_extract_path_text(fcinfo: FunctionCallInfo) -> Datum {
    get_path_all(fcinfo, true)
}

/*
 * common routine for extract_path functions
 */
unsafe fn get_path_all(fcinfo: FunctionCallInfo, as_text: bool) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let path: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let result: *mut text;
    let mut pathtext: *mut Datum = std::ptr::null_mut();
    let mut pathnulls: *mut bool = std::ptr::null_mut();
    let mut npath: c_int = 0;
    let tpath: *mut *mut c_char;
    let ipath: *mut c_int;
    let mut i: c_int;

    /*
     * If the array contains any null elements, return NULL, on the grounds
     * that you'd have gotten NULL if any RHS value were NULL in a nested
     * series of applications of the -> operator.
     */
    if array_contains_nulls(path) {
        PG_RETURN_NULL!(fcinfo);
    }

    deconstruct_array_builtin(path, TEXTOID, &mut pathtext, &mut pathnulls, &mut npath);

    tpath = palloc(npath as usize * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    ipath = palloc(npath as usize * core::mem::size_of::<c_int>()) as *mut c_int;

    i = 0;
    while i < npath {
        Assert!(!*pathnulls.offset(i as isize));
        *tpath.offset(i as isize) = TextDatumGetCString(*pathtext.offset(i as isize));

        /*
         * we have no idea at this stage what structure the document is so
         * just convert anything in the path that we can to an integer and set
         * all the other integers to INT_MIN which will never match.
         */
        if *(*tpath.offset(i as isize)) != b'\0' as c_char {
            let ind: c_int;
            let mut endptr: *mut c_char = std::ptr::null_mut();

            set_errno(0);
            ind = strtoint(*tpath.offset(i as isize), &mut endptr, 10);
            if endptr == *tpath.offset(i as isize) || *endptr != b'\0' as c_char || get_errno() != 0 {
                *ipath.offset(i as isize) = INT_MIN;
            } else {
                *ipath.offset(i as isize) = ind;
            }
        } else {
            *ipath.offset(i as isize) = INT_MIN;
        }
        i += 1;
    }

    result = get_worker(json, tpath, ipath, npath, as_text);

    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

/* errno helpers - TODO(pg-port): port-level errno access */
unsafe fn set_errno(_v: c_int) {}
unsafe fn get_errno() -> c_int { 0 }

/*
 * get_worker
 *
 * common worker for all the json getter functions
 */
unsafe fn get_worker(
    json: *mut text,
    tpath: *mut *mut c_char,
    ipath: *mut c_int,
    npath: c_int,
    normalize_results: bool,
) -> *mut text {
    let sem: *mut JsonSemAction = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;
    let state: *mut GetState = palloc0(core::mem::size_of::<GetState>()) as *mut GetState;

    Assert!(npath >= 0);

    (*state).lex = makeJsonLexContext(std::ptr::null_mut(), json, true);

    /* is it "_as_text" variant? */
    (*state).normalize_results = normalize_results;
    (*state).npath = npath;
    (*state).path_names = tpath;
    (*state).path_indexes = ipath;
    (*state).pathok = palloc0(core::mem::size_of::<bool>() * npath as usize) as *mut bool;
    (*state).array_cur_index = palloc(core::mem::size_of::<c_int>() * npath as usize) as *mut c_int;

    if npath > 0 {
        *(*state).pathok = true;
    }

    (*sem).semstate = state as *mut c_void;

    /*
     * Not all variants need all the semantic routines. Only set the ones that
     * are actually needed for maximum efficiency.
     */
    (*sem).scalar = Some(get_scalar);
    if npath == 0 {
        (*sem).object_start = Some(get_object_start);
        (*sem).object_end = Some(get_object_end);
        (*sem).array_start = Some(get_array_start);
        (*sem).array_end = Some(get_array_end);
    }
    if !tpath.is_null() {
        (*sem).object_field_start = Some(get_object_field_start);
        (*sem).object_field_end = Some(get_object_field_end);
    }
    if !ipath.is_null() {
        (*sem).array_start = Some(get_array_start);
        (*sem).array_element_start = Some(get_array_element_start);
        (*sem).array_element_end = Some(get_array_element_end);
    }

    pg_parse_json_or_ereport((*state).lex, sem);
    freeJsonLexContext((*state).lex);

    (*state).tresult
}

pub unsafe extern "C" fn get_object_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    if lex_level == 0 && (*_state).npath == 0 {
        /*
         * Special case: we should match the entire object.  We only need this
         * at outermost level because at nested levels the match will have
         * been started by the outer field or array element callback.
         */
        (*_state).result_start = (*(*_state).lex).token_start;
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_object_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    if lex_level == 0 && (*_state).npath == 0 {
        /* Special case: return the entire object */
        let start: *const c_char = (*_state).result_start;
        let len: c_int = ((*(*_state).lex).prev_token_terminator as isize - start as isize) as c_int;

        (*_state).tresult = cstring_to_text_with_len(start, len) as *mut c_void;
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let mut get_next: bool = false;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    if lex_level <= (*_state).npath
        && *(*_state).pathok.offset((lex_level - 1) as isize)
        && !(*_state).path_names.is_null()
        && !(*(*_state).path_names.offset((lex_level - 1) as isize)).is_null()
        && libc_strcmp(fname, *(*_state).path_names.offset((lex_level - 1) as isize)) == 0
    {
        if lex_level < (*_state).npath {
            /* if not at end of path just mark path ok */
            *(*_state).pathok.offset(lex_level as isize) = true;
        } else {
            /* end of path, so we want this value */
            get_next = true;
        }
    }

    if get_next {
        /* this object overrides any previous matching object */
        (*_state).tresult = std::ptr::null_mut();
        (*_state).result_start = std::ptr::null();

        if (*_state).normalize_results && (*(*_state).lex).token_type == JSON_TOKEN_STRING {
            /* for as_text variants, tell get_scalar to set it for us */
            (*_state).next_scalar = true;
        } else {
            /* for non-as_text variants, just note the json starting point */
            (*_state).result_start = (*(*_state).lex).token_start;
        }
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_object_field_end(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let mut get_last: bool = false;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    /* same tests as in get_object_field_start */
    if lex_level <= (*_state).npath
        && *(*_state).pathok.offset((lex_level - 1) as isize)
        && !(*_state).path_names.is_null()
        && !(*(*_state).path_names.offset((lex_level - 1) as isize)).is_null()
        && libc_strcmp(fname, *(*_state).path_names.offset((lex_level - 1) as isize)) == 0
    {
        if lex_level < (*_state).npath {
            /* done with this field so reset pathok */
            *(*_state).pathok.offset(lex_level as isize) = false;
        } else {
            /* end of path, so we want this value */
            get_last = true;
        }
    }

    /* for as_text scalar case, our work is already done */
    if get_last && !(*_state).result_start.is_null() {
        /*
         * make a text object from the string from the previously noted json
         * start up to the end of the previous token (the lexer is by now
         * ahead of us on whatever came after what we're interested in).
         */
        if isnull && (*_state).normalize_results {
            (*_state).tresult = std::ptr::null_mut();
        } else {
            let start: *const c_char = (*_state).result_start;
            let len: c_int = ((*(*_state).lex).prev_token_terminator as isize - start as isize) as c_int;

            (*_state).tresult = cstring_to_text_with_len(start, len) as *mut c_void;
        }

        /* this should be unnecessary but let's do it for cleanliness: */
        (*_state).result_start = std::ptr::null();
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_array_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    if lex_level < (*_state).npath {
        /* Initialize counting of elements in this array */
        *(*_state).array_cur_index.offset(lex_level as isize) = -1;

        /* INT_MIN value is reserved to represent invalid subscript */
        if *(*_state).path_indexes.offset(lex_level as isize) < 0
            && *(*_state).path_indexes.offset(lex_level as isize) != INT_MIN
        {
            /* Negative subscript -- convert to positive-wise subscript */
            let error: JsonParseErrorType;
            let mut nelements: c_int = 0;

            error = json_count_array_elements((*_state).lex, &mut nelements);
            if error != JSON_SUCCESS {
                json_errsave_error(error, (*_state).lex, std::ptr::null_mut());
            }

            if -*(*_state).path_indexes.offset(lex_level as isize) <= nelements {
                *(*_state).path_indexes.offset(lex_level as isize) += nelements;
            }
        }
    } else if lex_level == 0 && (*_state).npath == 0 {
        /*
         * Special case: we should match the entire array.
         */
        (*_state).result_start = (*(*_state).lex).token_start;
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_array_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    if lex_level == 0 && (*_state).npath == 0 {
        /* Special case: return the entire array */
        let start: *const c_char = (*_state).result_start;
        let len: c_int = ((*(*_state).lex).prev_token_terminator as isize - start as isize) as c_int;

        (*_state).tresult = cstring_to_text_with_len(start, len) as *mut c_void;
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_array_element_start(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let mut get_next: bool = false;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    /* Update array element counter */
    if lex_level <= (*_state).npath {
        *(*_state).array_cur_index.offset((lex_level - 1) as isize) += 1;
    }

    if lex_level <= (*_state).npath
        && *(*_state).pathok.offset((lex_level - 1) as isize)
        && !(*_state).path_indexes.is_null()
        && *(*_state).array_cur_index.offset((lex_level - 1) as isize)
            == *(*_state).path_indexes.offset((lex_level - 1) as isize)
    {
        if lex_level < (*_state).npath {
            /* if not at end of path just mark path ok */
            *(*_state).pathok.offset(lex_level as isize) = true;
        } else {
            /* end of path, so we want this value */
            get_next = true;
        }
    }

    /* same logic as for objects */
    if get_next {
        (*_state).tresult = std::ptr::null_mut();
        (*_state).result_start = std::ptr::null();

        if (*_state).normalize_results && (*(*_state).lex).token_type == JSON_TOKEN_STRING {
            (*_state).next_scalar = true;
        } else {
            (*_state).result_start = (*(*_state).lex).token_start;
        }
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_array_element_end(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let mut get_last: bool = false;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    /* same tests as in get_array_element_start */
    if lex_level <= (*_state).npath
        && *(*_state).pathok.offset((lex_level - 1) as isize)
        && !(*_state).path_indexes.is_null()
        && *(*_state).array_cur_index.offset((lex_level - 1) as isize)
            == *(*_state).path_indexes.offset((lex_level - 1) as isize)
    {
        if lex_level < (*_state).npath {
            /* done with this element so reset pathok */
            *(*_state).pathok.offset(lex_level as isize) = false;
        } else {
            /* end of path, so we want this value */
            get_last = true;
        }
    }

    /* same logic as for objects */
    if get_last && !(*_state).result_start.is_null() {
        if isnull && (*_state).normalize_results {
            (*_state).tresult = std::ptr::null_mut();
        } else {
            let start: *const c_char = (*_state).result_start;
            let len: c_int = ((*(*_state).lex).prev_token_terminator as isize - start as isize) as c_int;

            (*_state).tresult = cstring_to_text_with_len(start, len) as *mut c_void;
        }

        (*_state).result_start = std::ptr::null();
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn get_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut GetState;
    let lex_level: c_int = (*(*_state).lex).lex_level;

    /* Check for whole-object match */
    if lex_level == 0 && (*_state).npath == 0 {
        if (*_state).normalize_results && tokentype == JSON_TOKEN_STRING {
            /* we want the de-escaped string */
            (*_state).next_scalar = true;
        } else if (*_state).normalize_results && tokentype == JSON_TOKEN_NULL {
            (*_state).tresult = std::ptr::null_mut();
        } else {
            /*
             * This is a bit hokey: we will suppress whitespace after the
             * scalar token, but not whitespace before it.
             */
            let start: *const c_char = (*(*_state).lex).input;
            let len: c_int = ((*(*_state).lex).prev_token_terminator as isize - start as isize) as c_int;

            (*_state).tresult = cstring_to_text_with_len(start, len) as *mut c_void;
        }
    }

    if (*_state).next_scalar {
        /* a de-escaped text value is wanted, so supply it */
        (*_state).tresult = cstring_to_text(token) as *mut c_void;
        /* make sure the next call to get_scalar doesn't overwrite it */
        (*_state).next_scalar = false;
    }

    JSON_SUCCESS
}

unsafe fn libc_strcmp(_a: *const c_char, _b: *const c_char) -> c_int {
    unimplemented!("strcmp (libc) not yet wired")
}

pub unsafe fn jsonb_extract_path(fcinfo: FunctionCallInfo) -> Datum {
    get_jsonb_path_all(fcinfo, false)
}

pub unsafe fn jsonb_extract_path_text(fcinfo: FunctionCallInfo) -> Datum {
    get_jsonb_path_all(fcinfo, true)
}

unsafe fn get_jsonb_path_all(fcinfo: FunctionCallInfo, as_text: bool) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let path: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let mut pathtext: *mut Datum = std::ptr::null_mut();
    let mut pathnulls: *mut bool = std::ptr::null_mut();
    let mut isnull: bool = false;
    let mut npath: c_int = 0;
    let res: Datum;

    /*
     * If the array contains any null elements, return NULL.
     */
    if array_contains_nulls(path) {
        PG_RETURN_NULL!(fcinfo);
    }

    deconstruct_array_builtin(path, TEXTOID, &mut pathtext, &mut pathnulls, &mut npath);

    res = jsonb_get_element(jb, pathtext, npath, &mut isnull, as_text);

    if isnull {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_DATUM!(res)
    }
}

pub unsafe fn jsonb_get_element(
    jb: *mut Jsonb,
    path: *mut Datum,
    npath: c_int,
    isnull: *mut bool,
    as_text: bool,
) -> Datum {
    let mut container: *mut JsonbContainer = &mut (*jb).root;
    let mut jbvp: *mut JsonbValue = std::ptr::null_mut();
    let mut i: c_int;
    let mut have_object: bool = false;
    let mut have_array: bool = false;

    *isnull = false;

    /* Identify whether we have object, array, or scalar at top-level */
    if JB_ROOT_IS_OBJECT(jb) {
        have_object = true;
    } else if JB_ROOT_IS_ARRAY(jb) && !JB_ROOT_IS_SCALAR(jb) {
        have_array = true;
    } else {
        Assert!(JB_ROOT_IS_ARRAY(jb) && JB_ROOT_IS_SCALAR(jb));
        /* Extract the scalar value, if it is what we'll return */
        if npath <= 0 {
            jbvp = getIthJsonbValueFromContainer(container, 0);
        }
    }

    /*
     * If the array is empty, return the entire LHS object.
     */
    if npath <= 0 && jbvp.is_null() {
        if as_text {
            return PointerGetDatum(cstring_to_text(JsonbToCString(
                std::ptr::null_mut(),
                container,
                VARSIZE(jb as *mut c_void),
            )) as *const c_void);
        } else {
            /* not text mode - just hand back the jsonb */
            PG_RETURN_JSONB_P!(jb);
        }
    }

    i = 0;
    while i < npath {
        if have_object {
            let subscr: *mut text = DatumGetTextPP(*path.offset(i as isize));

            jbvp = getKeyJsonValueFromContainer(
                container,
                VARDATA_ANY(subscr),
                VARSIZE_ANY_EXHDR(subscr),
                std::ptr::null_mut(),
            );
        } else if have_array {
            let lindex: c_int;
            let index: uint32;
            let indextext: *mut c_char = TextDatumGetCString(*path.offset(i as isize));
            let mut endptr: *mut c_char = std::ptr::null_mut();

            set_errno(0);
            lindex = strtoint(indextext, &mut endptr, 10);
            if endptr == indextext || *endptr != b'\0' as c_char || get_errno() != 0 {
                *isnull = true;
                return PointerGetDatum(std::ptr::null());
            }

            if lindex >= 0 {
                index = lindex as uint32;
            } else {
                /* Handle negative subscript */
                let nelements: uint32;

                /* Container must be array, but make sure */
                if !JsonContainerIsArray(container) {
                    elog!(ERROR, "not a jsonb array");
                }

                nelements = JsonContainerSize(container);

                if lindex == INT_MIN || (-lindex) as uint32 > nelements {
                    *isnull = true;
                    return PointerGetDatum(std::ptr::null());
                } else {
                    index = (nelements as c_int + lindex) as uint32;
                }
            }

            jbvp = getIthJsonbValueFromContainer(container, index);
        } else {
            /* scalar, extraction yields a null */
            *isnull = true;
            return PointerGetDatum(std::ptr::null());
        }

        if jbvp.is_null() {
            *isnull = true;
            return PointerGetDatum(std::ptr::null());
        } else if i == npath - 1 {
            break;
        }

        if (*jbvp).type_ == jbvBinary {
            container = (*jbvp).val.binary.data;
            have_object = JsonContainerIsObject(container);
            have_array = JsonContainerIsArray(container);
            Assert!(!JsonContainerIsScalar(container));
        } else {
            Assert!(IsAJsonbScalar(jbvp));
            have_object = false;
            have_array = false;
        }
        i += 1;
    }

    if as_text {
        if (*jbvp).type_ == jbvNull {
            *isnull = true;
            return PointerGetDatum(std::ptr::null());
        }

        return PointerGetDatum(JsonbValueAsText(jbvp) as *const c_void);
    } else {
        let res: *mut Jsonb = JsonbValueToJsonb(jbvp);

        /* not text mode - just hand back the jsonb */
        PG_RETURN_JSONB_P!(res);
    }
}

pub unsafe fn jsonb_set_element(
    jb: *mut Jsonb,
    path: *mut Datum,
    path_len: c_int,
    mut newval: *mut JsonbValue,
) -> Datum {
    let res: *mut JsonbValue;
    let mut state: *mut JsonbParseState = std::ptr::null_mut();
    let mut it: *mut JsonbIterator;
    let path_nulls: *mut bool = palloc0(path_len as usize * core::mem::size_of::<bool>()) as *mut bool;

    if (*newval).type_ == jbvArray && (*newval).val.array.rawScalar {
        *newval = *(*newval).val.array.elems.offset(0);
    }

    it = JsonbIteratorInit(&mut (*jb).root);

    res = setPath(
        &mut it,
        path,
        path_nulls,
        path_len,
        &mut state,
        0,
        newval,
        JB_PATH_CREATE | JB_PATH_FILL_GAPS | JB_PATH_CONSISTENT_POSITION,
    );

    pfree(path_nulls as *mut c_void);

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

unsafe fn push_null_elements(ps: *mut *mut JsonbParseState, mut num: c_int) {
    let mut null: JsonbValue = core::mem::zeroed();

    null.type_ = jbvNull;

    while num > 0 {
        num -= 1;
        pushJsonbValue(ps, WJB_ELEM, &mut null);
    }
}

/*
 * Prepare a new structure containing nested empty objects and arrays
 * corresponding to the specified path, and assign a new value at the end of
 * this path.
 */
unsafe fn push_path(
    st: *mut *mut JsonbParseState,
    level: c_int,
    path_elems: *mut Datum,
    path_nulls: *mut bool,
    path_len: c_int,
    newval: *mut JsonbValue,
) {
    /*
     * tpath contains expected type of an empty jsonb created at each level
     * higher or equal to the current one, either jbvObject or jbvArray.
     */
    let tpath: *mut jbvType =
        palloc0((path_len - level) as usize * core::mem::size_of::<jbvType>()) as *mut jbvType;
    let mut newkey: JsonbValue = core::mem::zeroed();

    /*
     * Create first part of the chain with beginning tokens.
     */
    let mut i: c_int = level + 1;
    while i < path_len {
        let c: *mut c_char;
        let mut badp: *mut c_char = std::ptr::null_mut();
        let lindex: c_int;

        if *path_nulls.offset(i as isize) {
            break;
        }

        /*
         * Try to convert to an integer to find out the expected type, object
         * or array.
         */
        c = TextDatumGetCString(*path_elems.offset(i as isize));
        set_errno(0);
        lindex = strtoint(c, &mut badp, 10);
        if badp == c || *badp != b'\0' as c_char || get_errno() != 0 {
            /* text, an object is expected */
            newkey.type_ = jbvString;
            newkey.val.string.val = c;
            newkey.val.string.len = libc_strlen(c) as c_int;

            pushJsonbValue(st, WJB_BEGIN_OBJECT, std::ptr::null_mut());
            pushJsonbValue(st, WJB_KEY, &mut newkey);

            *tpath.offset((i - level) as isize) = jbvObject;
        } else {
            /* integer, an array is expected */
            pushJsonbValue(st, WJB_BEGIN_ARRAY, std::ptr::null_mut());

            push_null_elements(st, lindex);

            *tpath.offset((i - level) as isize) = jbvArray;
        }
        i += 1;
    }

    /* Insert an actual value for either an object or array */
    if *tpath.offset(((path_len - level) - 1) as isize) == jbvArray {
        pushJsonbValue(st, WJB_ELEM, newval);
    } else {
        pushJsonbValue(st, WJB_VALUE, newval);
    }

    /*
     * Close everything up to the last but one level.
     */
    let mut i: c_int = path_len - 1;
    while i > level {
        if *path_nulls.offset(i as isize) {
            break;
        }

        if *tpath.offset((i - level) as isize) == jbvObject {
            pushJsonbValue(st, WJB_END_OBJECT, std::ptr::null_mut());
        } else {
            pushJsonbValue(st, WJB_END_ARRAY, std::ptr::null_mut());
        }
        i -= 1;
    }
}

unsafe fn libc_strlen(_s: *const c_char) -> usize {
    unimplemented!("strlen (libc) not yet wired")
}

/*
 * Return the text representation of the given JsonbValue.
 */
unsafe fn JsonbValueAsText(v: *mut JsonbValue) -> *mut text {
    match (*v).type_ {
        jbvNull => std::ptr::null_mut(),

        jbvBool => {
            if (*v).val.boolean {
                cstring_to_text_with_len(c"true".as_ptr(), 4) as *mut c_void
            } else {
                cstring_to_text_with_len(c"false".as_ptr(), 5) as *mut c_void
            }
        }

        jbvString => cstring_to_text_with_len((*v).val.string.val, (*v).val.string.len) as *mut c_void,

        jbvNumeric => {
            let cstr: Datum;

            cstr = DirectFunctionCall1(numeric_out, PointerGetDatum((*v).val.numeric as *const c_void));

            cstring_to_text(DatumGetCString(cstr)) as *mut c_void
        }

        jbvBinary => {
            let mut jtext: StringInfoData = core::mem::zeroed();

            initStringInfo(&mut jtext);
            JsonbToCString(&mut jtext, (*v).val.binary.data, (*v).val.binary.len);

            cstring_to_text_with_len(jtext.data, jtext.len) as *mut c_void
        }

        _ => {
            elog!(ERROR, "unrecognized jsonb type: {}", (*v).type_ as c_int);
            std::ptr::null_mut()
        }
    }
}

/*
 * SQL function json_array_length(json) -> int
 */
pub unsafe fn json_array_length(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let state: *mut AlenState;
    let mut lex: JsonLexContext = core::mem::zeroed();
    let sem: *mut JsonSemAction;

    state = palloc0(core::mem::size_of::<AlenState>()) as *mut AlenState;
    (*state).lex = makeJsonLexContext(&mut lex, json, false);
    /* palloc0 does this for us */

    sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;
    (*sem).semstate = state as *mut c_void;
    (*sem).object_start = Some(alen_object_start);
    (*sem).scalar = Some(alen_scalar);
    (*sem).array_element_start = Some(alen_array_element_start);

    pg_parse_json_or_ereport((*state).lex, sem);

    PG_RETURN_INT32!((*state).count)
}

pub unsafe fn jsonb_array_length(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);

    if JB_ROOT_IS_SCALAR(jb) {
        ereport!(ERROR, errmsg!("cannot get array length of a scalar"));
    } else if !JB_ROOT_IS_ARRAY(jb) {
        ereport!(ERROR, errmsg!("cannot get array length of a non-array"));
    }

    PG_RETURN_INT32!(JB_ROOT_COUNT(jb) as int32)
}

/*
 * These next two checks ensure that the json is an array.
 */
pub unsafe extern "C" fn alen_object_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut AlenState;

    /* json structure check */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(ERROR, errmsg!("cannot get array length of a non-array"));
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn alen_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut AlenState;

    /* json structure check */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(ERROR, errmsg!("cannot get array length of a scalar"));
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn alen_array_element_start(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut AlenState;

    /* just count up all the level 1 elements */
    if (*(*_state).lex).lex_level == 1 {
        (*_state).count += 1;
    }

    JSON_SUCCESS
}

/*
 * SQL function json_each and json_each_text
 */
pub unsafe fn json_each(fcinfo: FunctionCallInfo) -> Datum {
    each_worker(fcinfo, false)
}

pub unsafe fn jsonb_each(fcinfo: FunctionCallInfo) -> Datum {
    each_worker_jsonb(fcinfo, c"jsonb_each".as_ptr(), false)
}

pub unsafe fn json_each_text(fcinfo: FunctionCallInfo) -> Datum {
    each_worker(fcinfo, true)
}

pub unsafe fn jsonb_each_text(fcinfo: FunctionCallInfo) -> Datum {
    each_worker_jsonb(fcinfo, c"jsonb_each_text".as_ptr(), true)
}

unsafe fn each_worker_jsonb(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    as_text: bool,
) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let rsi: *mut ReturnSetInfo;
    let mut old_cxt: MemoryContext;
    let tmp_cxt: MemoryContext;
    let mut skipNested: bool = false;
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;

    if !JB_ROOT_IS_OBJECT(jb) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on a non-object",
                std::ffi::CStr::from_ptr(funcname).to_string_lossy()
            )
        );
    }

    rsi = fcinfo_resultinfo(fcinfo);
    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);

    tmp_cxt = AllocSetContextCreate(
        CurrentMemoryContext(),
        c"jsonb_each temporary cxt".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, skipNested);
        if r == WJB_DONE {
            break;
        }
        skipNested = true;

        if r == WJB_KEY {
            let key: *mut text;
            let mut values: [Datum; 2] = [0 as Datum; 2];
            let mut nulls: [bool; 2] = [false, false];

            /* Use the tmp context so we can clean up after each tuple is done */
            old_cxt = MemoryContextSwitchTo(tmp_cxt);

            key = cstring_to_text_with_len(v.val.string.val, v.val.string.len) as *mut c_void;

            /*
             * The next thing the iterator fetches should be the value.
             */
            r = JsonbIteratorNext(&mut it, &mut v, skipNested);
            Assert!(r != WJB_DONE);

            values[0] = PointerGetDatum(key as *const c_void);

            if as_text {
                if v.type_ == jbvNull {
                    /* a json null is an sql null in text mode */
                    nulls[1] = true;
                    values[1] = 0 as Datum;
                } else {
                    values[1] = PointerGetDatum(JsonbValueAsText(&mut v) as *const c_void);
                }
            } else {
                /* Not in text mode, just return the Jsonb */
                let val: *mut Jsonb = JsonbValueToJsonb(&mut v);

                values[1] = PointerGetDatum(val as *const c_void);
            }

            tuplestore_putvalues(
                rsi_setResult(rsi),
                rsi_setDesc(rsi),
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            /* clean up and switch back */
            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }

    MemoryContextDelete(tmp_cxt);

    PG_RETURN_NULL!(fcinfo)
}

unsafe fn each_worker(fcinfo: FunctionCallInfo, as_text: bool) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut lex: JsonLexContext = core::mem::zeroed();
    let sem: *mut JsonSemAction;
    let rsi: *mut ReturnSetInfo;
    let state: *mut EachState;

    state = palloc0(core::mem::size_of::<EachState>()) as *mut EachState;
    sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;

    rsi = fcinfo_resultinfo(fcinfo);

    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    (*state).tuple_store = rsi_setResult(rsi);
    (*state).ret_tdesc = rsi_setDesc(rsi);

    (*sem).semstate = state as *mut c_void;
    (*sem).array_start = Some(each_array_start);
    (*sem).scalar = Some(each_scalar);
    (*sem).object_field_start = Some(each_object_field_start);
    (*sem).object_field_end = Some(each_object_field_end);

    (*state).normalize_results = as_text;
    (*state).next_scalar = false;
    (*state).lex = makeJsonLexContext(&mut lex, json, true);
    (*state).tmp_cxt = AllocSetContextCreate(
        CurrentMemoryContext(),
        c"json_each temporary cxt".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    pg_parse_json_or_ereport(&mut lex, sem);

    MemoryContextDelete((*state).tmp_cxt);
    freeJsonLexContext(&mut lex);

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe extern "C" fn each_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut EachState;

    /* save a pointer to where the value starts */
    if (*(*_state).lex).lex_level == 1 {
        /*
         * next_scalar will be reset in the object_field_end handler.
         */
        if (*_state).normalize_results && (*(*_state).lex).token_type == JSON_TOKEN_STRING {
            (*_state).next_scalar = true;
        } else {
            (*_state).result_start = (*(*_state).lex).token_start;
        }
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn each_object_field_end(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut EachState;
    let old_cxt: MemoryContext;
    let len: c_int;
    let val: *mut text;
    let tuple: HeapTuple;
    let mut values: [Datum; 2] = [0 as Datum; 2];
    let mut nulls: [bool; 2] = [false, false];

    /* skip over nested objects */
    if (*(*_state).lex).lex_level != 1 {
        return JSON_SUCCESS;
    }

    /* use the tmp context so we can clean up after each tuple is done */
    old_cxt = MemoryContextSwitchTo((*_state).tmp_cxt);

    values[0] = CStringGetTextDatum(fname);

    if isnull && (*_state).normalize_results {
        nulls[1] = true;
        values[1] = 0 as Datum;
    } else if (*_state).next_scalar {
        values[1] = CStringGetTextDatum((*_state).normalized_scalar);
        (*_state).next_scalar = false;
    } else {
        len = ((*(*_state).lex).prev_token_terminator as isize - (*_state).result_start as isize) as c_int;
        val = cstring_to_text_with_len((*_state).result_start, len) as *mut c_void;
        values[1] = PointerGetDatum(val as *const c_void);
    }

    tuple = heap_form_tuple((*_state).ret_tdesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    tuplestore_puttuple((*_state).tuple_store, tuple as *mut HeapTupleData);

    /* clean up and switch back */
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset((*_state).tmp_cxt);

    JSON_SUCCESS
}

pub unsafe extern "C" fn each_array_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut EachState;

    /* json structure check */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(ERROR, errmsg!("cannot deconstruct an array as an object"));
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn each_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut EachState;

    /* json structure check */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(ERROR, errmsg!("cannot deconstruct a scalar"));
    }

    /* supply de-escaped value if required */
    if (*_state).next_scalar {
        (*_state).normalized_scalar = token;
    }

    JSON_SUCCESS
}

/* ReturnSetInfo accessors - TODO(pg-port): nodes/execnodes.h ReturnSetInfo */
unsafe fn fcinfo_resultinfo(_fcinfo: FunctionCallInfo) -> *mut ReturnSetInfo {
    unimplemented!("fcinfo->resultinfo (nodes/execnodes.h) not yet ported")
}
unsafe fn rsi_setResult(_rsi: *mut ReturnSetInfo) -> *mut Tuplestorestate {
    unimplemented!("ReturnSetInfo.setResult not yet ported")
}
unsafe fn rsi_setDesc(_rsi: *mut ReturnSetInfo) -> TupleDesc {
    unimplemented!("ReturnSetInfo.setDesc not yet ported")
}

/*
 * SQL functions json_array_elements and json_array_elements_text
 */
pub unsafe fn jsonb_array_elements(fcinfo: FunctionCallInfo) -> Datum {
    elements_worker_jsonb(fcinfo, c"jsonb_array_elements".as_ptr(), false)
}

pub unsafe fn jsonb_array_elements_text(fcinfo: FunctionCallInfo) -> Datum {
    elements_worker_jsonb(fcinfo, c"jsonb_array_elements_text".as_ptr(), true)
}

unsafe fn elements_worker_jsonb(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    as_text: bool,
) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let rsi: *mut ReturnSetInfo;
    let mut old_cxt: MemoryContext;
    let tmp_cxt: MemoryContext;
    let mut skipNested: bool = false;
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;

    if JB_ROOT_IS_SCALAR(jb) {
        ereport!(ERROR, errmsg!("cannot extract elements from a scalar"));
    } else if !JB_ROOT_IS_ARRAY(jb) {
        ereport!(ERROR, errmsg!("cannot extract elements from an object"));
    }

    rsi = fcinfo_resultinfo(fcinfo);

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);

    tmp_cxt = AllocSetContextCreate(
        CurrentMemoryContext(),
        c"jsonb_array_elements temporary cxt".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, skipNested);
        if r == WJB_DONE {
            break;
        }
        skipNested = true;

        if r == WJB_ELEM {
            let mut values: [Datum; 1] = [0 as Datum; 1];
            let mut nulls: [bool; 1] = [false];

            /* use the tmp context so we can clean up after each tuple is done */
            old_cxt = MemoryContextSwitchTo(tmp_cxt);

            if as_text {
                if v.type_ == jbvNull {
                    /* a json null is an sql null in text mode */
                    nulls[0] = true;
                    values[0] = 0 as Datum;
                } else {
                    values[0] = PointerGetDatum(JsonbValueAsText(&mut v) as *const c_void);
                }
            } else {
                /* Not in text mode, just return the Jsonb */
                let val: *mut Jsonb = JsonbValueToJsonb(&mut v);

                values[0] = PointerGetDatum(val as *const c_void);
            }

            tuplestore_putvalues(
                rsi_setResult(rsi),
                rsi_setDesc(rsi),
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );

            /* clean up and switch back */
            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }

    MemoryContextDelete(tmp_cxt);

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe fn json_array_elements(fcinfo: FunctionCallInfo) -> Datum {
    elements_worker(fcinfo, c"json_array_elements".as_ptr(), false)
}

pub unsafe fn json_array_elements_text(fcinfo: FunctionCallInfo) -> Datum {
    elements_worker(fcinfo, c"json_array_elements_text".as_ptr(), true)
}

unsafe fn elements_worker(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    as_text: bool,
) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let mut lex: JsonLexContext = core::mem::zeroed();
    let sem: *mut JsonSemAction;
    let rsi: *mut ReturnSetInfo;
    let state: *mut ElementsState;

    /* elements only needs escaped strings when as_text */
    makeJsonLexContext(&mut lex, json, as_text);

    state = palloc0(core::mem::size_of::<ElementsState>()) as *mut ElementsState;
    sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
    rsi = fcinfo_resultinfo(fcinfo);
    (*state).tuple_store = rsi_setResult(rsi);
    (*state).ret_tdesc = rsi_setDesc(rsi);

    (*sem).semstate = state as *mut c_void;
    (*sem).object_start = Some(elements_object_start);
    (*sem).scalar = Some(elements_scalar);
    (*sem).array_element_start = Some(elements_array_element_start);
    (*sem).array_element_end = Some(elements_array_element_end);

    (*state).function_name = funcname;
    (*state).normalize_results = as_text;
    (*state).next_scalar = false;
    (*state).lex = &mut lex;
    (*state).tmp_cxt = AllocSetContextCreate(
        CurrentMemoryContext(),
        c"json_array_elements temporary cxt".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    pg_parse_json_or_ereport(&mut lex, sem);

    MemoryContextDelete((*state).tmp_cxt);
    freeJsonLexContext(&mut lex);

    PG_RETURN_NULL!(fcinfo)
}

pub unsafe extern "C" fn elements_array_element_start(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut ElementsState;

    /* save a pointer to where the value starts */
    if (*(*_state).lex).lex_level == 1 {
        /*
         * next_scalar will be reset in the array_element_end handler.
         */
        if (*_state).normalize_results && (*(*_state).lex).token_type == JSON_TOKEN_STRING {
            (*_state).next_scalar = true;
        } else {
            (*_state).result_start = (*(*_state).lex).token_start;
        }
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn elements_array_element_end(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut ElementsState;
    let old_cxt: MemoryContext;
    let len: c_int;
    let val: *mut text;
    let tuple: HeapTuple;
    let mut values: [Datum; 1] = [0 as Datum; 1];
    let mut nulls: [bool; 1] = [false];

    /* skip over nested objects */
    if (*(*_state).lex).lex_level != 1 {
        return JSON_SUCCESS;
    }

    /* use the tmp context so we can clean up after each tuple is done */
    old_cxt = MemoryContextSwitchTo((*_state).tmp_cxt);

    if isnull && (*_state).normalize_results {
        nulls[0] = true;
        values[0] = 0 as Datum;
    } else if (*_state).next_scalar {
        values[0] = CStringGetTextDatum((*_state).normalized_scalar);
        (*_state).next_scalar = false;
    } else {
        len = ((*(*_state).lex).prev_token_terminator as isize - (*_state).result_start as isize) as c_int;
        val = cstring_to_text_with_len((*_state).result_start, len) as *mut c_void;
        values[0] = PointerGetDatum(val as *const c_void);
    }

    tuple = heap_form_tuple((*_state).ret_tdesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    tuplestore_puttuple((*_state).tuple_store, tuple as *mut HeapTupleData);

    /* clean up and switch back */
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset((*_state).tmp_cxt);

    JSON_SUCCESS
}

pub unsafe extern "C" fn elements_object_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut ElementsState;

    /* json structure check */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on a non-array",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn elements_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut ElementsState;

    /* json structure check */
    if (*(*_state).lex).lex_level == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on a scalar",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    /* supply de-escaped value if required */
    if (*_state).next_scalar {
        (*_state).normalized_scalar = token;
    }

    JSON_SUCCESS
}

/*
 * SQL function json_populate_record
 */
pub unsafe fn jsonb_populate_record(fcinfo: FunctionCallInfo) -> Datum {
    populate_record_worker(fcinfo, c"jsonb_populate_record".as_ptr(), false, true, std::ptr::null_mut())
}

/*
 * SQL function that can be used for testing json_populate_record().
 */
pub unsafe fn jsonb_populate_record_valid(fcinfo: FunctionCallInfo) -> Datum {
    let mut escontext: ErrorSaveContext = ErrorSaveContext { type_: T_ErrorSaveContext, error_occurred: false };

    populate_record_worker(
        fcinfo,
        c"jsonb_populate_record".as_ptr(),
        false,
        true,
        &mut escontext as *mut ErrorSaveContext as Node,
    );

    BoolGetDatum(!escontext.error_occurred)
}

pub unsafe fn jsonb_to_record(fcinfo: FunctionCallInfo) -> Datum {
    populate_record_worker(fcinfo, c"jsonb_to_record".as_ptr(), false, false, std::ptr::null_mut())
}

pub unsafe fn json_populate_record(fcinfo: FunctionCallInfo) -> Datum {
    populate_record_worker(fcinfo, c"json_populate_record".as_ptr(), true, true, std::ptr::null_mut())
}

pub unsafe fn json_to_record(fcinfo: FunctionCallInfo) -> Datum {
    populate_record_worker(fcinfo, c"json_to_record".as_ptr(), true, false, std::ptr::null_mut())
}

/* helper function for diagnostics */
unsafe fn populate_array_report_expected_array(ctx: *mut PopulateArrayContext, ndim: c_int) {
    if ndim <= 0 {
        if !(*ctx).colname.is_null() {
            errsave!(
                (*ctx).escontext,
                errmsg!("expected JSON array")
            );
        } else {
            errsave!((*ctx).escontext, errmsg!("expected JSON array"));
        }
        return;
    } else {
        let mut indices: StringInfoData = core::mem::zeroed();
        let mut i: c_int;

        initStringInfo(&mut indices);

        Assert!((*ctx).ndims > 0 && ndim < (*ctx).ndims);

        i = 0;
        while i < ndim {
            appendStringInfo_indices(&mut indices, *(*ctx).sizes.offset(i as isize));
            i += 1;
        }

        if !(*ctx).colname.is_null() {
            errsave!((*ctx).escontext, errmsg!("expected JSON array"));
        } else {
            errsave!((*ctx).escontext, errmsg!("expected JSON array"));
        }
        return;
    }
}

unsafe fn appendStringInfo_indices(_str: *mut StringInfoData, _v: c_int) {
    /* appendStringInfo(&indices, "[%d]", ...) - TODO(pg-port): real vararg StringInfo */
    unimplemented!("appendStringInfo not yet ported")
}

/*
 * Validate and set ndims for populating an array.
 */
unsafe fn populate_array_assign_ndims(ctx: *mut PopulateArrayContext, ndims: c_int) -> bool {
    let mut i: c_int;

    Assert!((*ctx).ndims <= 0);

    if ndims <= 0 {
        populate_array_report_expected_array(ctx, ndims);
        /* Getting here means the error was reported softly. */
        Assert!(SOFT_ERROR_OCCURRED((*ctx).escontext));
        return false;
    }

    (*ctx).ndims = ndims;
    (*ctx).dims = palloc(core::mem::size_of::<c_int>() * ndims as usize) as *mut c_int;
    (*ctx).sizes = palloc0(core::mem::size_of::<c_int>() * ndims as usize) as *mut c_int;

    i = 0;
    while i < ndims {
        *(*ctx).dims.offset(i as isize) = -1; /* dimensions are unknown yet */
        i += 1;
    }

    true
}

/*
 * Check the populated subarray dimension
 */
unsafe fn populate_array_check_dimension(ctx: *mut PopulateArrayContext, ndim: c_int) -> bool {
    let dim: c_int = *(*ctx).sizes.offset(ndim as isize); /* current dimension counter */

    if *(*ctx).dims.offset(ndim as isize) == -1 {
        *(*ctx).dims.offset(ndim as isize) = dim; /* assign dimension if not yet known */
    } else if *(*ctx).dims.offset(ndim as isize) != dim {
        ereturn!(
            (*ctx).escontext,
            false,
            errmsg!("malformed JSON array")
        );
    }

    /* reset the current array dimension size counter */
    *(*ctx).sizes.offset(ndim as isize) = 0;

    /* increment the parent dimension counter if it is a nested sub-array */
    if ndim > 0 {
        *(*ctx).sizes.offset((ndim - 1) as isize) += 1;
    }

    true
}

/*
 * Returns true if the array element value was successfully extracted from jsv.
 */
unsafe fn populate_array_element(
    ctx: *mut PopulateArrayContext,
    ndim: c_int,
    jsv: *mut JsValue,
) -> bool {
    let element: Datum;
    let mut element_isnull: bool = false;

    /* populate the array element */
    element = populate_record_field(
        (*(*ctx).aio).element_info,
        (*(*ctx).aio).element_type,
        (*(*ctx).aio).element_typmod,
        std::ptr::null(),
        (*ctx).mcxt,
        PointerGetDatum(std::ptr::null()),
        jsv,
        &mut element_isnull,
        (*ctx).escontext,
        false,
    );
    /* Nothing to do on an error. */
    if SOFT_ERROR_OCCURRED((*ctx).escontext) {
        return false;
    }

    (*ctx).astate = accumArrayResult(
        (*ctx).astate,
        element,
        element_isnull,
        (*(*ctx).aio).element_type,
        (*ctx).acxt,
    );

    Assert!(ndim > 0);
    *(*ctx).sizes.offset((ndim - 1) as isize) += 1; /* increment current dimension counter */

    true
}

/* json object start handler for populate_array_json() */
pub unsafe extern "C" fn populate_array_object_start(_state: *mut c_void) -> JsonParseErrorType {
    let state = _state as *mut PopulateArrayState;
    let ndim: c_int = (*(*state).lex).lex_level;

    if (*(*state).ctx).ndims <= 0 {
        if !populate_array_assign_ndims((*state).ctx, ndim) {
            return JSON_SEM_ACTION_FAILED;
        }
    } else if ndim < (*(*state).ctx).ndims {
        populate_array_report_expected_array((*state).ctx, ndim);
        /* Getting here means the error was reported softly. */
        Assert!(SOFT_ERROR_OCCURRED((*(*state).ctx).escontext));
        return JSON_SEM_ACTION_FAILED;
    }

    JSON_SUCCESS
}

/* json array end handler for populate_array_json() */
pub unsafe extern "C" fn populate_array_array_end(_state: *mut c_void) -> JsonParseErrorType {
    let state = _state as *mut PopulateArrayState;
    let ctx: *mut PopulateArrayContext = (*state).ctx;
    let ndim: c_int = (*(*state).lex).lex_level;

    if (*ctx).ndims <= 0 {
        if !populate_array_assign_ndims(ctx, ndim + 1) {
            return JSON_SEM_ACTION_FAILED;
        }
    }

    if ndim < (*ctx).ndims {
        /* Report if an error occurred. */
        if !populate_array_check_dimension(ctx, ndim) {
            return JSON_SEM_ACTION_FAILED;
        }
    }

    JSON_SUCCESS
}

/* json array element start handler for populate_array_json() */
pub unsafe extern "C" fn populate_array_element_start(
    _state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let state = _state as *mut PopulateArrayState;
    let ndim: c_int = (*(*state).lex).lex_level;

    if (*(*state).ctx).ndims <= 0 || ndim == (*(*state).ctx).ndims {
        /* remember current array element start */
        (*state).element_start = (*(*state).lex).token_start;
        (*state).element_type = (*(*state).lex).token_type;
        (*state).element_scalar = std::ptr::null_mut();
    }

    JSON_SUCCESS
}

/* json array element end handler for populate_array_json() */
pub unsafe extern "C" fn populate_array_element_end(
    _state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let state = _state as *mut PopulateArrayState;
    let ctx: *mut PopulateArrayContext = (*state).ctx;
    let ndim: c_int = (*(*state).lex).lex_level;

    Assert!((*ctx).ndims > 0);

    if ndim == (*ctx).ndims {
        let mut jsv: JsValue = core::mem::zeroed();

        jsv.is_json = true;
        jsv.val.json.type_ = (*state).element_type;

        if isnull {
            Assert!(jsv.val.json.type_ == JSON_TOKEN_NULL);
            jsv.val.json.str = std::ptr::null();
            jsv.val.json.len = 0;
        } else if !(*state).element_scalar.is_null() {
            jsv.val.json.str = (*state).element_scalar;
            jsv.val.json.len = -1; /* null-terminated */
        } else {
            jsv.val.json.str = (*state).element_start;
            jsv.val.json.len = (((*(*state).lex).prev_token_terminator as isize
                - (*state).element_start as isize) as c_int)
                * core::mem::size_of::<c_char>() as c_int;
        }

        /* Report if an error occurred. */
        if !populate_array_element(ctx, ndim, &mut jsv) {
            return JSON_SEM_ACTION_FAILED;
        }
    }

    JSON_SUCCESS
}

/* json scalar handler for populate_array_json() */
pub unsafe extern "C" fn populate_array_scalar(
    _state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let state = _state as *mut PopulateArrayState;
    let ctx: *mut PopulateArrayContext = (*state).ctx;
    let ndim: c_int = (*(*state).lex).lex_level;

    if (*ctx).ndims <= 0 {
        if !populate_array_assign_ndims(ctx, ndim) {
            return JSON_SEM_ACTION_FAILED;
        }
    } else if ndim < (*ctx).ndims {
        populate_array_report_expected_array(ctx, ndim);
        /* Getting here means the error was reported softly. */
        Assert!(SOFT_ERROR_OCCURRED((*ctx).escontext));
        return JSON_SEM_ACTION_FAILED;
    }

    if ndim == (*ctx).ndims {
        /* remember the scalar element token */
        (*state).element_scalar = token;
        /* element_type must already be set in populate_array_element_start() */
        Assert!((*state).element_type == tokentype);
    }

    JSON_SUCCESS
}

/*
 * Parse a json array and populate array
 */
unsafe fn populate_array_json(ctx: *mut PopulateArrayContext, json: *const c_char, len: c_int) -> bool {
    let mut state: PopulateArrayState = core::mem::zeroed();
    let mut sem: JsonSemAction = core::mem::zeroed();

    state.lex = makeJsonLexContextCstringLen(std::ptr::null_mut(), json as *const c_char, len as usize, GetDatabaseEncoding(), true);
    state.ctx = ctx;

    sem.semstate = &mut state as *mut PopulateArrayState as *mut c_void;
    sem.object_start = Some(populate_array_object_start);
    sem.array_end = Some(populate_array_array_end);
    sem.array_element_start = Some(populate_array_element_start);
    sem.array_element_end = Some(populate_array_element_end);
    sem.scalar = Some(populate_array_scalar);

    if pg_parse_json_or_errsave(state.lex, &sem, (*ctx).escontext) {
        /* number of dimensions should be already known */
        Assert!((*ctx).ndims > 0 && !(*ctx).dims.is_null());
    }

    freeJsonLexContext(state.lex);

    !SOFT_ERROR_OCCURRED((*ctx).escontext)
}

/*
 * populate_array_dim_jsonb() -- Iterate recursively through jsonb sub-array.
 */
unsafe fn populate_array_dim_jsonb(
    ctx: *mut PopulateArrayContext,
    jbv: *mut JsonbValue,
    ndim: c_int,
) -> bool {
    let jbc: *mut JsonbContainer = (*jbv).val.binary.data;
    let mut it: *mut JsonbIterator;
    let mut tok: JsonbIteratorToken;
    let mut val: JsonbValue = core::mem::zeroed();
    let mut jsv: JsValue = core::mem::zeroed();

    check_stack_depth();

    /* Even scalars can end up here thanks to ExecEvalJsonCoercion(). */
    if (*jbv).type_ != jbvBinary || !JsonContainerIsArray(jbc) || JsonContainerIsScalar(jbc) {
        populate_array_report_expected_array(ctx, ndim - 1);
        /* Getting here means the error was reported softly. */
        Assert!(SOFT_ERROR_OCCURRED((*ctx).escontext));
        return false;
    }

    it = JsonbIteratorInit(jbc);

    tok = JsonbIteratorNext(&mut it, &mut val, true);
    Assert!(tok == WJB_BEGIN_ARRAY);

    tok = JsonbIteratorNext(&mut it, &mut val, true);

    /*
     * If the number of dimensions is not yet known and we have found end of
     * the array, or the first child element is not an array, then assign the
     * number of dimensions now.
     */
    if (*ctx).ndims <= 0
        && (tok == WJB_END_ARRAY
            || (tok == WJB_ELEM
                && (val.type_ != jbvBinary || !JsonContainerIsArray(val.val.binary.data))))
    {
        if !populate_array_assign_ndims(ctx, ndim) {
            return false;
        }
    }

    jsv.is_json = false;
    jsv.val.jsonb = &mut val;

    /* process all the array elements */
    while tok == WJB_ELEM {
        /*
         * Recurse only if the dimensions of dimensions is still unknown or if
         * it is not the innermost dimension.
         */
        if (*ctx).ndims > 0 && ndim >= (*ctx).ndims {
            if !populate_array_element(ctx, ndim, &mut jsv) {
                return false;
            }
        } else {
            /* populate child sub-array */
            if !populate_array_dim_jsonb(ctx, &mut val, ndim + 1) {
                return false;
            }

            /* number of dimensions should be already known */
            Assert!((*ctx).ndims > 0 && !(*ctx).dims.is_null());

            if !populate_array_check_dimension(ctx, ndim) {
                return false;
            }
        }

        tok = JsonbIteratorNext(&mut it, &mut val, true);
    }

    Assert!(tok == WJB_END_ARRAY);

    /* free iterator, iterating until WJB_DONE */
    tok = JsonbIteratorNext(&mut it, &mut val, true);
    Assert!(tok == WJB_DONE && it.is_null());

    true
}

/*
 * Recursively populate an array from json/jsonb
 */
unsafe fn populate_array(
    aio: *mut ArrayIOData,
    colname: *const c_char,
    mcxt: MemoryContext,
    jsv: *mut JsValue,
    isnull: *mut bool,
    escontext: Node,
) -> Datum {
    let mut ctx: PopulateArrayContext = core::mem::zeroed();
    let result: Datum;
    let lbs: *mut c_int;
    let mut i: c_int;

    ctx.aio = aio;
    ctx.mcxt = mcxt;
    ctx.acxt = CurrentMemoryContext();
    ctx.astate = initArrayResult((*aio).element_type, ctx.acxt, true);
    ctx.colname = colname;
    ctx.ndims = 0; /* unknown yet */
    ctx.dims = std::ptr::null_mut();
    ctx.sizes = std::ptr::null_mut();
    ctx.escontext = escontext;

    if (*jsv).is_json {
        /* Return null if an error was found. */
        if !populate_array_json(
            &mut ctx,
            (*jsv).val.json.str,
            if (*jsv).val.json.len >= 0 {
                (*jsv).val.json.len
            } else {
                libc_strlen((*jsv).val.json.str) as c_int
            },
        ) {
            *isnull = true;
            return 0 as Datum;
        }
    } else {
        /* Return null if an error was found. */
        if !populate_array_dim_jsonb(&mut ctx, (*jsv).val.jsonb, 1) {
            *isnull = true;
            return 0 as Datum;
        }
        *ctx.dims.offset(0) = *ctx.sizes.offset(0);
    }

    Assert!(ctx.ndims > 0);

    lbs = palloc(core::mem::size_of::<c_int>() * ctx.ndims as usize) as *mut c_int;

    i = 0;
    while i < ctx.ndims {
        *lbs.offset(i as isize) = 1;
        i += 1;
    }

    result = makeMdArrayResult(ctx.astate, ctx.ndims, ctx.dims, lbs, ctx.acxt, true);

    pfree(ctx.dims as *mut c_void);
    pfree(ctx.sizes as *mut c_void);
    pfree(lbs as *mut c_void);

    *isnull = false;
    result
}

/*
 * Returns false if an error occurs.
 */
unsafe fn JsValueToJsObject(jsv: *mut JsValue, jso: *mut JsObject, escontext: Node) -> bool {
    (*jso).is_json = (*jsv).is_json;

    if (*jsv).is_json {
        /* convert plain-text json into a hash table */
        (*jso).val.json_hash = get_json_object_as_hash(
            (*jsv).val.json.str,
            if (*jsv).val.json.len >= 0 {
                (*jsv).val.json.len
            } else {
                libc_strlen((*jsv).val.json.str) as c_int
            },
            c"populate_composite".as_ptr(),
            escontext,
        );
        Assert!(!(*jso).val.json_hash.is_null() || SOFT_ERROR_OCCURRED(escontext));
    } else {
        let jbv: *mut JsonbValue = (*jsv).val.jsonb;

        if (*jbv).type_ == jbvBinary && JsonContainerIsObject((*jbv).val.binary.data) {
            (*jso).val.jsonb_cont = (*jbv).val.binary.data;
        } else {
            let is_scalar: bool;

            is_scalar = IsAJsonbScalar(jbv)
                || ((*jbv).type_ == jbvBinary && JsonContainerIsScalar((*jbv).val.binary.data));
            if is_scalar {
                errsave!(escontext, errmsg!("cannot call {} on a scalar", "populate_composite"));
            } else {
                errsave!(escontext, errmsg!("cannot call {} on an array", "populate_composite"));
            }
        }
    }

    !SOFT_ERROR_OCCURRED(escontext)
}

/* acquire or update cached tuple descriptor for a composite type */
unsafe fn update_cached_tupdesc(io: *mut CompositeIOData, mcxt: MemoryContext) {
    if (*io).tupdesc.is_null()
        || tupdesc_tdtypeid((*io).tupdesc) != (*io).base_typid
        || tupdesc_tdtypmod((*io).tupdesc) != (*io).base_typmod
    {
        let tupdesc: TupleDesc = lookup_rowtype_tupdesc((*io).base_typid, (*io).base_typmod);
        let oldcxt: MemoryContext;

        if !(*io).tupdesc.is_null() {
            FreeTupleDesc((*io).tupdesc);
        }

        /* copy tuple desc without constraints into cache memory context */
        oldcxt = MemoryContextSwitchTo(mcxt);
        (*io).tupdesc = CreateTupleDescCopy(tupdesc);
        MemoryContextSwitchTo(oldcxt);

        ReleaseTupleDesc(tupdesc);
    }
}

unsafe fn tupdesc_tdtypeid(_td: TupleDesc) -> Oid {
    unimplemented!("TupleDesc.tdtypeid not yet ported")
}
unsafe fn tupdesc_tdtypmod(_td: TupleDesc) -> int32 {
    unimplemented!("TupleDesc.tdtypmod not yet ported")
}
unsafe fn tupdesc_natts(_td: TupleDesc) -> c_int {
    unimplemented!("TupleDesc.natts not yet ported")
}
unsafe fn TupleDescAttr(_td: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!("TupleDescAttr not yet ported")
}

/*
 * Recursively populate a composite (row type) value from json/jsonb
 */
unsafe fn populate_composite(
    io: *mut CompositeIOData,
    typid: Oid,
    colname: *const c_char,
    mcxt: MemoryContext,
    defaultval: HeapTupleHeader,
    jsv: *mut JsValue,
    isnull: *mut bool,
    escontext: Node,
) -> Datum {
    let mut result: Datum;

    /* acquire/update cached tuple descriptor */
    update_cached_tupdesc(io, mcxt);

    if *isnull {
        result = 0 as Datum;
    } else {
        let tuple: HeapTupleHeader;
        let mut jso: JsObject = core::mem::zeroed();

        /* prepare input value */
        if !JsValueToJsObject(jsv, &mut jso, escontext) {
            *isnull = true;
            return 0 as Datum;
        }

        /* populate resulting record tuple */
        tuple = populate_record((*io).tupdesc, &mut (*io).record_io, defaultval, mcxt, &mut jso, escontext);

        if SOFT_ERROR_OCCURRED(escontext) {
            *isnull = true;
            return 0 as Datum;
        }
        result = HeapTupleHeaderGetDatum(tuple);

        JsObjectFree(&mut jso);
    }

    /*
     * If it's domain over composite, check domain constraints.
     */
    if typid != (*io).base_typid && typid != RECORDOID {
        if !domain_check_safe(result, *isnull, typid, &mut (*io).domain_info, mcxt, escontext) {
            *isnull = true;
            return 0 as Datum;
        }
    }

    result
}

/*
 * Populate non-null scalar value from json/jsonb value.
 */
unsafe fn populate_scalar(
    io: *mut ScalarIOData,
    typid: Oid,
    typmod: int32,
    jsv: *mut JsValue,
    isnull: *mut bool,
    escontext: Node,
    omit_quotes: bool,
) -> Datum {
    let mut res: Datum = 0;
    let mut str: *mut c_char = std::ptr::null_mut();
    let mut json: *const c_char = std::ptr::null();

    if (*jsv).is_json {
        let len: c_int = (*jsv).val.json.len;

        json = (*jsv).val.json.str;
        Assert!(!json.is_null());

        /* If converting to json/jsonb, make string into valid JSON literal */
        if (typid == JSONOID || typid == JSONBOID) && (*jsv).val.json.type_ == JSON_TOKEN_STRING {
            let mut buf: StringInfoData = core::mem::zeroed();

            initStringInfo(&mut buf);
            if len >= 0 {
                escape_json_with_len(&mut buf, json, len);
            } else {
                escape_json(&mut buf as *mut StringInfoData as StringInfo, json);
            }
            str = buf.data;
        } else if len >= 0 {
            /* create a NUL-terminated version */
            str = palloc(len as usize + 1) as *mut c_char;
            std::ptr::copy_nonoverlapping(json, str, len as usize);
            *str.offset(len as isize) = b'\0' as c_char;
        } else {
            /* string is already NUL-terminated */
            str = json as *mut c_char;
        }
    } else {
        let jbv: *mut JsonbValue = (*jsv).val.jsonb;

        if (*jbv).type_ == jbvString && omit_quotes {
            str = pnstrdup((*jbv).val.string.val, (*jbv).val.string.len);
        } else if typid == JSONBOID {
            let jsonb: *mut Jsonb = JsonbValueToJsonb(jbv); /* directly use jsonb */

            return JsonbPGetDatum(jsonb);
        }
        /* convert jsonb to string for typio call */
        else if typid == JSONOID && (*jbv).type_ != jbvBinary {
            /*
             * Convert scalar jsonb (non-scalars are passed here as jbvBinary)
             * to json string, preserving quotes around top-level strings.
             */
            let jsonb: *mut Jsonb = JsonbValueToJsonb(jbv);

            str = JsonbToCString(std::ptr::null_mut(), &mut (*jsonb).root, VARSIZE(jsonb as *mut c_void));
        } else if (*jbv).type_ == jbvString {
            /* quotes are stripped */
            str = pnstrdup((*jbv).val.string.val, (*jbv).val.string.len);
        } else if (*jbv).type_ == jbvBool {
            str = pstrdup(if (*jbv).val.boolean { c"true".as_ptr() } else { c"false".as_ptr() });
        } else if (*jbv).type_ == jbvNumeric {
            str = DatumGetCString(DirectFunctionCall1(
                numeric_out,
                PointerGetDatum((*jbv).val.numeric as *const c_void),
            ));
        } else if (*jbv).type_ == jbvBinary {
            str = JsonbToCString(std::ptr::null_mut(), (*jbv).val.binary.data, (*jbv).val.binary.len);
        } else {
            elog!(ERROR, "unrecognized jsonb type: {}", (*jbv).type_ as c_int);
        }
    }

    if !InputFunctionCallSafe(io_typiofunc(io), str, (*io).typioparam, typmod, escontext, &mut res) {
        res = 0 as Datum;
        *isnull = true;
    }

    /* free temporary buffer */
    if str != json as *mut c_char {
        pfree(str as *mut c_void);
    }

    res
}

unsafe fn io_typiofunc(io: *mut ScalarIOData) -> *mut FmgrInfo {
    &mut (*io).typiofunc
}
unsafe fn escape_json_with_len(_buf: *mut StringInfoData, _json: *const c_char, _len: c_int) {
    /* escape_json_with_len exists in json.rs - TODO(pg-port): wire StringInfo type */
    unimplemented!("escape_json_with_len not yet wired")
}

unsafe fn populate_domain(
    io: *mut DomainIOData,
    typid: Oid,
    colname: *const c_char,
    mcxt: MemoryContext,
    jsv: *mut JsValue,
    isnull: *mut bool,
    escontext: Node,
    omit_quotes: bool,
) -> Datum {
    let mut res: Datum;

    if *isnull {
        res = 0 as Datum;
    } else {
        res = populate_record_field(
            (*io).base_io,
            (*io).base_typid,
            (*io).base_typmod,
            colname,
            mcxt,
            PointerGetDatum(std::ptr::null()),
            jsv,
            isnull,
            escontext,
            omit_quotes,
        );
        Assert!(!*isnull || SOFT_ERROR_OCCURRED(escontext));
    }

    if !domain_check_safe(res, *isnull, typid, &mut (*io).domain_info, mcxt, escontext) {
        *isnull = true;
        return 0 as Datum;
    }

    res
}

/* prepare column metadata cache for the given type */
unsafe fn prepare_column_cache(
    column: *mut ColumnIOData,
    typid: Oid,
    typmod: int32,
    mcxt: MemoryContext,
    mut need_scalar: bool,
) {
    let tup: HeapTuple;
    let type_: Form_pg_type;

    (*column).typid = typid;
    (*column).typmod = typmod;

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }

    type_ = GETSTRUCT(tup) as Form_pg_type;

    if pgtype_typtype(type_) == TYPTYPE_DOMAIN {
        /*
         * We can move directly to the bottom base type.
         */
        let base_typid: Oid;
        let mut base_typmod: int32 = typmod;

        base_typid = getBaseTypeAndTypmod(typid, &mut base_typmod);
        if get_typtype(base_typid) == TYPTYPE_COMPOSITE {
            /* domain over composite has its own code path */
            (*column).typcat = TYPECAT_COMPOSITE_DOMAIN;
            (*column).io.composite.record_io = std::ptr::null_mut();
            (*column).io.composite.tupdesc = std::ptr::null_mut();
            (*column).io.composite.base_typid = base_typid;
            (*column).io.composite.base_typmod = base_typmod;
            (*column).io.composite.domain_info = std::ptr::null_mut();
        } else {
            /* domain over anything else */
            (*column).typcat = TYPECAT_DOMAIN;
            (*column).io.domain.base_typid = base_typid;
            (*column).io.domain.base_typmod = base_typmod;
            (*column).io.domain.base_io =
                MemoryContextAllocZero(mcxt, core::mem::size_of::<ColumnIOData>()) as *mut ColumnIOData;
            (*column).io.domain.domain_info = std::ptr::null_mut();
        }
    } else if pgtype_typtype(type_) == TYPTYPE_COMPOSITE || typid == RECORDOID {
        (*column).typcat = TYPECAT_COMPOSITE;
        (*column).io.composite.record_io = std::ptr::null_mut();
        (*column).io.composite.tupdesc = std::ptr::null_mut();
        (*column).io.composite.base_typid = typid;
        (*column).io.composite.base_typmod = typmod;
        (*column).io.composite.domain_info = std::ptr::null_mut();
    } else if IsTrueArrayType(type_) {
        (*column).typcat = TYPECAT_ARRAY;
        (*column).io.array.element_info =
            MemoryContextAllocZero(mcxt, core::mem::size_of::<ColumnIOData>()) as *mut ColumnIOData;
        (*column).io.array.element_type = pgtype_typelem(type_);
        /* array element typemod stored in attribute's typmod */
        (*column).io.array.element_typmod = typmod;
    } else {
        (*column).typcat = TYPECAT_SCALAR;
        need_scalar = true;
    }

    /* caller can force us to look up scalar_io info even for non-scalars */
    if need_scalar {
        let mut typioproc: Oid = InvalidOid;

        getTypeInputInfo(typid, &mut typioproc, &mut (*column).scalar_io.typioparam);
        fmgr_info_cxt(typioproc, &mut (*column).scalar_io.typiofunc, mcxt);
    }

    ReleaseSysCache(tup);
}

unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}
unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut c_void {
    unimplemented!("GETSTRUCT not yet ported")
}
unsafe fn pgtype_typtype(_type: Form_pg_type) -> c_char {
    unimplemented!("Form_pg_type.typtype not yet ported")
}
unsafe fn pgtype_typelem(_type: Form_pg_type) -> Oid {
    unimplemented!("Form_pg_type.typelem not yet ported")
}
unsafe fn IsTrueArrayType(_type: Form_pg_type) -> bool {
    unimplemented!("IsTrueArrayType not yet ported")
}

/*
 * Populate and return the value of specified type from a given json/jsonb value.
 */
pub unsafe fn json_populate_type(
    json_val: Datum,
    json_type: Oid,
    typid: Oid,
    typmod: int32,
    cache: *mut *mut c_void,
    mcxt: MemoryContext,
    isnull: *mut bool,
    omit_quotes: bool,
    escontext: Node,
) -> Datum {
    let mut jsv: JsValue = core::mem::zeroed();
    let mut jbv: JsonbValue = core::mem::zeroed();

    jsv.is_json = json_type == JSONOID;

    if *isnull {
        if jsv.is_json {
            jsv.val.json.str = std::ptr::null();
        } else {
            jsv.val.jsonb = std::ptr::null_mut();
        }
    } else if jsv.is_json {
        let json: *mut text = DatumGetTextPP(json_val);

        jsv.val.json.str = VARDATA_ANY(json);
        jsv.val.json.len = VARSIZE_ANY_EXHDR(json);
        jsv.val.json.type_ = JSON_TOKEN_INVALID; /* not used in populate_composite() */
    } else {
        let jsonb: *mut Jsonb = DatumGetJsonbP(json_val);

        jsv.val.jsonb = &mut jbv;

        if omit_quotes {
            let str: *mut c_char = JsonbUnquote(DatumGetJsonbP(json_val));

            /* fill the quote-stripped string */
            jbv.type_ = jbvString;
            jbv.val.string.len = libc_strlen(str) as c_int;
            jbv.val.string.val = str;
        } else {
            /* fill binary jsonb value pointing to jb */
            jbv.type_ = jbvBinary;
            jbv.val.binary.data = &mut (*jsonb).root;
            jbv.val.binary.len = VARSIZE(jsonb as *mut c_void) - VARHDRSZ;
        }
    }

    if (*cache).is_null() {
        *cache = MemoryContextAllocZero(mcxt, core::mem::size_of::<ColumnIOData>());
    }

    populate_record_field(
        *cache as *mut ColumnIOData,
        typid,
        typmod,
        std::ptr::null(),
        mcxt,
        PointerGetDatum(std::ptr::null()),
        &mut jsv,
        isnull,
        escontext,
        omit_quotes,
    )
}

/* recursively populate a record field or an array element from a json/jsonb value */
unsafe fn populate_record_field(
    col: *mut ColumnIOData,
    typid: Oid,
    typmod: int32,
    colname: *const c_char,
    mcxt: MemoryContext,
    defaultval: Datum,
    jsv: *mut JsValue,
    isnull: *mut bool,
    escontext: Node,
    omit_scalar_quotes: bool,
) -> Datum {
    let mut typcat: TypeCat;

    check_stack_depth();

    /*
     * Prepare column metadata cache for the given type.  Force lookup of the
     * scalar_io data so that the json string hack below will work.
     */
    if (*col).typid != typid || (*col).typmod != typmod {
        prepare_column_cache(col, typid, typmod, mcxt, true);
    }

    *isnull = JsValueIsNull(jsv);

    typcat = (*col).typcat;

    /* try to convert json string to a non-scalar type through input function */
    if JsValueIsString(jsv)
        && (typcat == TYPECAT_ARRAY
            || typcat == TYPECAT_COMPOSITE
            || typcat == TYPECAT_COMPOSITE_DOMAIN)
    {
        typcat = TYPECAT_SCALAR;
    }

    /* we must perform domain checks for NULLs, otherwise exit immediately */
    if *isnull && typcat != TYPECAT_DOMAIN && typcat != TYPECAT_COMPOSITE_DOMAIN {
        return 0 as Datum;
    }

    match typcat {
        TYPECAT_SCALAR => populate_scalar(
            &mut (*col).scalar_io,
            typid,
            typmod,
            jsv,
            isnull,
            escontext,
            omit_scalar_quotes,
        ),

        TYPECAT_ARRAY => populate_array(&mut (*col).io.array, colname, mcxt, jsv, isnull, escontext),

        TYPECAT_COMPOSITE | TYPECAT_COMPOSITE_DOMAIN => populate_composite(
            &mut (*col).io.composite,
            typid,
            colname,
            mcxt,
            if !DatumGetPointer(defaultval).is_null() {
                DatumGetHeapTupleHeader(defaultval)
            } else {
                std::ptr::null_mut()
            },
            jsv,
            isnull,
            escontext,
        ),

        TYPECAT_DOMAIN => populate_domain(
            &mut (*col).io.domain,
            typid,
            colname,
            mcxt,
            jsv,
            isnull,
            escontext,
            omit_scalar_quotes,
        ),
    }
}

unsafe fn allocate_record_info(mcxt: MemoryContext, ncolumns: c_int) -> *mut RecordIOData {
    let data: *mut RecordIOData = MemoryContextAlloc(
        mcxt,
        core::mem::offset_of!(RecordIOData, columns) + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
    ) as *mut RecordIOData;

    (*data).record_type = InvalidOid;
    (*data).record_typmod = 0;
    (*data).ncolumns = ncolumns;
    std::ptr::write_bytes(
        (*data).columns.as_mut_ptr(),
        0,
        ncolumns as usize,
    );

    data
}

unsafe fn JsObjectGetField(obj: *mut JsObject, field: *mut c_char, jsv: *mut JsValue) -> bool {
    (*jsv).is_json = (*obj).is_json;

    if (*jsv).is_json {
        let hashentry: *mut JsonHashEntry =
            hash_search((*obj).val.json_hash, field as *const c_void, HASH_FIND, std::ptr::null_mut())
                as *mut JsonHashEntry;

        (*jsv).val.json.type_ = if !hashentry.is_null() {
            (*hashentry).type_
        } else {
            JSON_TOKEN_NULL
        };
        (*jsv).val.json.str = if (*jsv).val.json.type_ == JSON_TOKEN_NULL {
            std::ptr::null()
        } else {
            (*hashentry).val
        };
        (*jsv).val.json.len = if !(*jsv).val.json.str.is_null() { -1 } else { 0 }; /* null-terminated */

        !hashentry.is_null()
    } else {
        (*jsv).val.jsonb = if (*obj).val.jsonb_cont.is_null() {
            std::ptr::null_mut()
        } else {
            getKeyJsonValueFromContainer(
                (*obj).val.jsonb_cont,
                field,
                libc_strlen(field) as c_int,
                std::ptr::null_mut(),
            )
        };

        !(*jsv).val.jsonb.is_null()
    }
}

/* populate a record tuple from json/jsonb value */
unsafe fn populate_record(
    tupdesc: TupleDesc,
    record_p: *mut *mut RecordIOData,
    defaultval: HeapTupleHeader,
    mcxt: MemoryContext,
    obj: *mut JsObject,
    escontext: Node,
) -> HeapTupleHeader {
    let mut record: *mut RecordIOData = *record_p;
    let values: *mut Datum;
    let nulls: *mut bool;
    let res: HeapTuple;
    let ncolumns: c_int = tupdesc_natts(tupdesc);
    let mut i: c_int;

    /*
     * if the input json is empty, we can only skip the rest if we were passed
     * in a non-null record.
     */
    if !defaultval.is_null() && JsObjectIsEmpty(obj) {
        return defaultval;
    }

    /* (re)allocate metadata cache */
    if record.is_null() || (*record).ncolumns != ncolumns {
        record = allocate_record_info(mcxt, ncolumns);
        *record_p = record;
    }

    /* invalidate metadata cache if the record type has changed */
    if (*record).record_type != tupdesc_tdtypeid(tupdesc)
        || (*record).record_typmod != tupdesc_tdtypmod(tupdesc)
    {
        std::ptr::write_bytes(
            record as *mut u8,
            0,
            core::mem::offset_of!(RecordIOData, columns) + ncolumns as usize * core::mem::size_of::<ColumnIOData>(),
        );
        (*record).record_type = tupdesc_tdtypeid(tupdesc);
        (*record).record_typmod = tupdesc_tdtypmod(tupdesc);
        (*record).ncolumns = ncolumns;
    }

    values = palloc(ncolumns as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(ncolumns as usize * core::mem::size_of::<bool>()) as *mut bool;

    if !defaultval.is_null() {
        let mut tuple: HeapTupleData_local = core::mem::zeroed();

        /* Build a temporary HeapTuple control structure */
        tuple.t_len = HeapTupleHeaderGetDatumLength(defaultval);
        ItemPointerSetInvalid(&mut tuple.t_self);
        tuple.t_tableOid = InvalidOid;
        tuple.t_data = defaultval;

        /* Break down the tuple into fields */
        heap_deform_tuple(&mut tuple as *mut HeapTupleData_local as *mut HeapTupleData, tupdesc, values, nulls);
    } else {
        i = 0;
        while i < ncolumns {
            *values.offset(i as isize) = 0 as Datum;
            *nulls.offset(i as isize) = true;
            i += 1;
        }
    }

    i = 0;
    while i < ncolumns {
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);
        let colname: *mut c_char = NameStr(pgattr_attname(att));
        let mut field: JsValue = core::mem::zeroed();
        let found: bool;

        /* Ignore dropped columns in datatype */
        if pgattr_attisdropped(att) {
            *nulls.offset(i as isize) = true;
            i += 1;
            continue;
        }

        found = JsObjectGetField(obj, colname, &mut field);

        /*
         * we can't just skip here if the key wasn't found since we might have
         * a domain to deal with.
         */
        if !defaultval.is_null() && !found {
            i += 1;
            continue;
        }

        *values.offset(i as isize) = populate_record_field(
            (*record).columns.as_mut_ptr().offset(i as isize),
            pgattr_atttypid(att),
            pgattr_atttypmod(att),
            colname,
            mcxt,
            if *nulls.offset(i as isize) { 0 as Datum } else { *values.offset(i as isize) },
            &mut field,
            nulls.offset(i as isize),
            escontext,
            false,
        );
        i += 1;
    }

    res = heap_form_tuple(tupdesc, values, nulls);

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);

    htup_t_data(res)
}

/* HeapTupleData mirror - TODO(pg-port): access/htup.h HeapTupleData */
#[repr(C)]
pub struct HeapTupleData_local {
    pub t_len: uint32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: HeapTupleHeader,
}
#[repr(C)]
pub struct ItemPointerData {
    pub _opaque: [u8; 6],
}
unsafe fn HeapTupleHeaderGetDatumLength(_tuple: HeapTupleHeader) -> uint32 {
    unimplemented!("HeapTupleHeaderGetDatumLength not yet ported")
}
unsafe fn ItemPointerSetInvalid(_pointer: *mut ItemPointerData) {
    unimplemented!("ItemPointerSetInvalid not yet ported")
}
unsafe fn htup_t_data(_tuple: HeapTuple) -> HeapTupleHeader {
    unimplemented!("HeapTuple.t_data not yet ported")
}
unsafe fn NameStr(_name: *mut c_void) -> *mut c_char {
    unimplemented!("NameStr not yet ported")
}
unsafe fn pgattr_attname(_att: Form_pg_attribute) -> *mut c_void {
    unimplemented!("Form_pg_attribute.attname not yet ported")
}
unsafe fn pgattr_attisdropped(_att: Form_pg_attribute) -> bool {
    unimplemented!("Form_pg_attribute.attisdropped not yet ported")
}
unsafe fn pgattr_atttypid(_att: Form_pg_attribute) -> Oid {
    unimplemented!("Form_pg_attribute.atttypid not yet ported")
}
unsafe fn pgattr_atttypmod(_att: Form_pg_attribute) -> int32 {
    unimplemented!("Form_pg_attribute.atttypmod not yet ported")
}

/*
 * Setup for json{b}_populate_record{set}: result type will be same as first
 * argument's type.
 */
unsafe fn get_record_type_from_argument(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    cache: *mut PopulateRecordCache,
) {
    (*cache).argtype = get_fn_expr_argtype(fcinfo_flinfo(fcinfo), 0);
    prepare_column_cache(&mut (*cache).c, (*cache).argtype, -1, (*cache).fn_mcxt, false);
    if (*cache).c.typcat != TYPECAT_COMPOSITE && (*cache).c.typcat != TYPECAT_COMPOSITE_DOMAIN {
        ereport!(
            ERROR,
            errmsg!(
                "first argument of {} must be a row type",
                std::ffi::CStr::from_ptr(funcname).to_string_lossy()
            )
        );
    }
}

/*
 * Setup for json{b}_to_record{set}: result type is specified by calling query.
 */
unsafe fn get_record_type_from_query(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    cache: *mut PopulateRecordCache,
) {
    let mut tupdesc: TupleDesc = std::ptr::null_mut();
    let old_cxt: MemoryContext;

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        ereport!(
            ERROR,
            errmsg!(
                "could not determine row type for result of {}",
                std::ffi::CStr::from_ptr(funcname).to_string_lossy()
            )
        );
    }

    Assert!(!tupdesc.is_null());
    (*cache).argtype = tupdesc_tdtypeid(tupdesc);

    /* If we go through this more than once, avoid memory leak */
    if !(*cache).c.io.composite.tupdesc.is_null() {
        FreeTupleDesc((*cache).c.io.composite.tupdesc);
    }

    /* Save identified tupdesc */
    old_cxt = MemoryContextSwitchTo((*cache).fn_mcxt);
    (*cache).c.io.composite.tupdesc = CreateTupleDescCopy(tupdesc);
    (*cache).c.io.composite.base_typid = tupdesc_tdtypeid(tupdesc);
    (*cache).c.io.composite.base_typmod = tupdesc_tdtypmod(tupdesc);
    MemoryContextSwitchTo(old_cxt);
}

/*
 * common worker for json{b}_populate_record() and json{b}_to_record()
 */
unsafe fn populate_record_worker(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    is_json: bool,
    have_record_arg: bool,
    escontext: Node,
) -> Datum {
    let json_arg_num: c_int = if have_record_arg { 1 } else { 0 };
    let mut jsv: JsValue = core::mem::zeroed();
    let mut rec: HeapTupleHeader;
    let rettuple: Datum;
    let mut isnull: bool;
    let mut jbv: JsonbValue = core::mem::zeroed();
    let fnmcxt: MemoryContext = flinfo_fn_mcxt(fcinfo_flinfo(fcinfo));
    let mut cache: *mut PopulateRecordCache = flinfo_fn_extra(fcinfo_flinfo(fcinfo)) as *mut PopulateRecordCache;

    /*
     * If first time through, identify input/result record type.
     */
    if cache.is_null() {
        cache = MemoryContextAllocZero(fnmcxt, core::mem::size_of::<PopulateRecordCache>())
            as *mut PopulateRecordCache;
        set_flinfo_fn_extra(fcinfo_flinfo(fcinfo), cache as *mut c_void);
        (*cache).fn_mcxt = fnmcxt;

        if have_record_arg {
            get_record_type_from_argument(fcinfo, funcname, cache);
        } else {
            get_record_type_from_query(fcinfo, funcname, cache);
        }
    }

    /* Collect record arg if we have one */
    if !have_record_arg {
        rec = std::ptr::null_mut(); /* it's json{b}_to_record() */
    } else if !PG_ARGISNULL(fcinfo, 0) {
        rec = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);

        /*
         * When declared arg type is RECORD, identify actual record type from
         * the tuple itself.
         */
        if (*cache).argtype == RECORDOID {
            (*cache).c.io.composite.base_typid = HeapTupleHeaderGetTypeId(rec);
            (*cache).c.io.composite.base_typmod = HeapTupleHeaderGetTypMod(rec);
        }
    } else {
        rec = std::ptr::null_mut();

        /*
         * When declared arg type is RECORD, identify actual record type from
         * calling query, or fail if we can't.
         */
        if (*cache).argtype == RECORDOID {
            get_record_type_from_query(fcinfo, funcname, cache);
            /* This can't change argtype */
            Assert!((*cache).argtype == RECORDOID);
        }
    }

    /* If no JSON argument, just return the record (if any) unchanged */
    if PG_ARGISNULL(fcinfo, json_arg_num) {
        if !rec.is_null() {
            PG_RETURN_POINTER!(rec);
        } else {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    jsv.is_json = is_json;

    if is_json {
        let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, json_arg_num);

        jsv.val.json.str = VARDATA_ANY(json);
        jsv.val.json.len = VARSIZE_ANY_EXHDR(json);
        jsv.val.json.type_ = JSON_TOKEN_INVALID; /* not used in populate_composite() */
    } else {
        let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, json_arg_num);

        jsv.val.jsonb = &mut jbv;

        /* fill binary jsonb value pointing to jb */
        jbv.type_ = jbvBinary;
        jbv.val.binary.data = &mut (*jb).root;
        jbv.val.binary.len = VARSIZE(jb as *mut c_void) - VARHDRSZ;
    }

    isnull = false;
    rettuple = populate_composite(
        &mut (*cache).c.io.composite,
        (*cache).argtype,
        std::ptr::null(),
        fnmcxt,
        rec,
        &mut jsv,
        &mut isnull,
        escontext,
    );
    Assert!(!isnull || SOFT_ERROR_OCCURRED(escontext));

    PG_RETURN_DATUM!(rettuple)
}

/* FmgrInfo / fcinfo field accessors - TODO(pg-port): fmgr.h family */
unsafe fn fcinfo_flinfo(_fcinfo: FunctionCallInfo) -> *mut FmgrInfo {
    unimplemented!("fcinfo->flinfo not yet ported")
}
unsafe fn flinfo_fn_mcxt(_flinfo: *mut FmgrInfo) -> MemoryContext {
    unimplemented!("FmgrInfo.fn_mcxt not yet ported")
}
unsafe fn flinfo_fn_extra(_flinfo: *mut FmgrInfo) -> *mut c_void {
    unimplemented!("FmgrInfo.fn_extra not yet ported")
}
unsafe fn set_flinfo_fn_extra(_flinfo: *mut FmgrInfo, _v: *mut c_void) {
    unimplemented!("FmgrInfo.fn_extra (set) not yet ported")
}
unsafe fn HeapTupleHeaderGetTypeId(_tup: HeapTupleHeader) -> Oid {
    unimplemented!("HeapTupleHeaderGetTypeId not yet ported")
}
unsafe fn HeapTupleHeaderGetTypMod(_tup: HeapTupleHeader) -> int32 {
    unimplemented!("HeapTupleHeaderGetTypMod not yet ported")
}

/*
 * get_json_object_as_hash
 */
unsafe fn get_json_object_as_hash(
    json: *const c_char,
    len: c_int,
    funcname: *const c_char,
    escontext: Node,
) -> *mut HTAB {
    let mut ctl: HASHCTL = core::mem::zeroed();
    let mut tab: *mut HTAB;
    let state: *mut JHashState;
    let sem: *mut JsonSemAction;

    ctl.keysize = NAMEDATALEN as Size;
    ctl.entrysize = core::mem::size_of::<JsonHashEntry>() as Size;
    ctl.hcxt = CurrentMemoryContext();
    tab = hash_create(
        c"json object hashtable".as_ptr(),
        100,
        &mut ctl,
        HASH_ELEM | HASH_STRINGS | HASH_CONTEXT,
    );

    state = palloc0(core::mem::size_of::<JHashState>()) as *mut JHashState;
    sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;

    (*state).function_name = funcname;
    (*state).hash = tab;
    (*state).lex = makeJsonLexContextCstringLen(std::ptr::null_mut(), json as *const c_char, len as usize, GetDatabaseEncoding(), true);

    (*sem).semstate = state as *mut c_void;
    (*sem).array_start = Some(hash_array_start);
    (*sem).scalar = Some(hash_scalar);
    (*sem).object_field_start = Some(hash_object_field_start);
    (*sem).object_field_end = Some(hash_object_field_end);

    if !pg_parse_json_or_errsave((*state).lex, sem, escontext) {
        hash_destroy((*state).hash);
        tab = std::ptr::null_mut();
    }

    freeJsonLexContext((*state).lex);

    tab
}

pub unsafe extern "C" fn hash_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut JHashState;

    if (*(*_state).lex).lex_level > 1 {
        return JSON_SUCCESS;
    }

    /* remember token type */
    (*_state).saved_token_type = (*(*_state).lex).token_type;

    if (*(*_state).lex).token_type == JSON_TOKEN_ARRAY_START
        || (*(*_state).lex).token_type == JSON_TOKEN_OBJECT_START
    {
        /* remember start position of the whole text of the subobject */
        (*_state).save_json_start = (*(*_state).lex).token_start;
    } else {
        /* must be a scalar */
        (*_state).save_json_start = std::ptr::null();
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn hash_object_field_end(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut JHashState;
    let hashentry: *mut JsonHashEntry;
    let mut found: bool = false;

    /* Ignore nested fields. */
    if (*(*_state).lex).lex_level > 1 {
        return JSON_SUCCESS;
    }

    /*
     * Ignore field names >= NAMEDATALEN - they can't match a record field.
     */
    if libc_strlen(fname) >= NAMEDATALEN {
        return JSON_SUCCESS;
    }

    hashentry =
        hash_search((*_state).hash, fname as *const c_void, HASH_ENTER, &mut found) as *mut JsonHashEntry;

    /*
     * found being true indicates a duplicate. A later field with the same
     * name overrides the earlier field.
     */

    (*hashentry).type_ = (*_state).saved_token_type;
    Assert!(isnull == ((*hashentry).type_ == JSON_TOKEN_NULL));

    if !(*_state).save_json_start.is_null() {
        let len: c_int =
            ((*(*_state).lex).prev_token_terminator as isize - (*_state).save_json_start as isize) as c_int;
        let val: *mut c_char = palloc((len + 1) as usize * core::mem::size_of::<c_char>()) as *mut c_char;

        std::ptr::copy_nonoverlapping((*_state).save_json_start, val, len as usize);
        *val.offset(len as isize) = b'\0' as c_char;
        (*hashentry).val = val;
    } else {
        /* must have had a scalar instead */
        (*hashentry).val = (*_state).saved_scalar;
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn hash_array_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut JHashState;

    if (*(*_state).lex).lex_level == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on an array",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn hash_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut JHashState;

    if (*(*_state).lex).lex_level == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on a scalar",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    if (*(*_state).lex).lex_level == 1 {
        (*_state).saved_scalar = token;
        /* saved_token_type must already be set in hash_object_field_start() */
        Assert!((*_state).saved_token_type == tokentype);
    }

    JSON_SUCCESS
}

/*
 * SQL function json_populate_recordset
 */
pub unsafe fn jsonb_populate_recordset(fcinfo: FunctionCallInfo) -> Datum {
    populate_recordset_worker(fcinfo, c"jsonb_populate_recordset".as_ptr(), false, true)
}

pub unsafe fn jsonb_to_recordset(fcinfo: FunctionCallInfo) -> Datum {
    populate_recordset_worker(fcinfo, c"jsonb_to_recordset".as_ptr(), false, false)
}

pub unsafe fn json_populate_recordset(fcinfo: FunctionCallInfo) -> Datum {
    populate_recordset_worker(fcinfo, c"json_populate_recordset".as_ptr(), true, true)
}

pub unsafe fn json_to_recordset(fcinfo: FunctionCallInfo) -> Datum {
    populate_recordset_worker(fcinfo, c"json_to_recordset".as_ptr(), true, false)
}

unsafe fn populate_recordset_record(state: *mut PopulateRecordsetState, obj: *mut JsObject) {
    let cache: *mut PopulateRecordCache = (*state).cache;
    let tuphead: HeapTupleHeader;
    let mut tuple: HeapTupleData_local = core::mem::zeroed();

    /* acquire/update cached tuple descriptor */
    update_cached_tupdesc(&mut (*cache).c.io.composite, (*cache).fn_mcxt);

    /* replace record fields from json */
    tuphead = populate_record(
        (*cache).c.io.composite.tupdesc,
        &mut (*cache).c.io.composite.record_io,
        (*state).rec,
        (*cache).fn_mcxt,
        obj,
        std::ptr::null_mut(),
    );

    /* if it's domain over composite, check domain constraints */
    if (*cache).c.typcat == TYPECAT_COMPOSITE_DOMAIN {
        domain_check_safe(
            HeapTupleHeaderGetDatum(tuphead),
            false,
            (*cache).argtype,
            &mut (*cache).c.io.composite.domain_info,
            (*cache).fn_mcxt,
            std::ptr::null_mut(),
        );
    }

    /* ok, save into tuplestore */
    tuple.t_len = HeapTupleHeaderGetDatumLength(tuphead);
    ItemPointerSetInvalid(&mut tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = tuphead;

    tuplestore_puttuple((*state).tuple_store, &mut tuple as *mut HeapTupleData_local as *mut HeapTupleData);
}

/*
 * common worker for json{b}_populate_recordset() and json{b}_to_recordset()
 */
unsafe fn populate_recordset_worker(
    fcinfo: FunctionCallInfo,
    funcname: *const c_char,
    is_json: bool,
    have_record_arg: bool,
) -> Datum {
    let json_arg_num: c_int = if have_record_arg { 1 } else { 0 };
    let rsi: *mut ReturnSetInfo;
    let old_cxt: MemoryContext;
    let mut rec: HeapTupleHeader;
    let mut cache: *mut PopulateRecordCache = flinfo_fn_extra(fcinfo_flinfo(fcinfo)) as *mut PopulateRecordCache;
    let state: *mut PopulateRecordsetState;

    rsi = fcinfo_resultinfo(fcinfo);

    if rsi.is_null() || !IsA!(rsi, T_ReturnSetInfo) {
        ereport!(ERROR, errmsg!("set-valued function called in context that cannot accept a set"));
    }

    if (rsi_allowedModes(rsi) & SFRM_Materialize) == 0 {
        ereport!(ERROR, errmsg!("materialize mode required, but it is not allowed in this context"));
    }

    set_rsi_returnMode(rsi, SFRM_Materialize);

    /*
     * If first time through, identify input/result record type.
     */
    if cache.is_null() {
        cache = MemoryContextAllocZero(
            flinfo_fn_mcxt(fcinfo_flinfo(fcinfo)),
            core::mem::size_of::<PopulateRecordCache>(),
        ) as *mut PopulateRecordCache;
        set_flinfo_fn_extra(fcinfo_flinfo(fcinfo), cache as *mut c_void);
        (*cache).fn_mcxt = flinfo_fn_mcxt(fcinfo_flinfo(fcinfo));

        if have_record_arg {
            get_record_type_from_argument(fcinfo, funcname, cache);
        } else {
            get_record_type_from_query(fcinfo, funcname, cache);
        }
    }

    /* Collect record arg if we have one */
    if !have_record_arg {
        rec = std::ptr::null_mut(); /* it's json{b}_to_recordset() */
    } else if !PG_ARGISNULL(fcinfo, 0) {
        rec = PG_GETARG_HEAPTUPLEHEADER(fcinfo, 0);

        /*
         * When declared arg type is RECORD, identify actual record type from
         * the tuple itself.
         */
        if (*cache).argtype == RECORDOID {
            (*cache).c.io.composite.base_typid = HeapTupleHeaderGetTypeId(rec);
            (*cache).c.io.composite.base_typmod = HeapTupleHeaderGetTypMod(rec);
        }
    } else {
        rec = std::ptr::null_mut();

        /*
         * When declared arg type is RECORD, identify actual record type from
         * calling query, or fail if we can't.
         */
        if (*cache).argtype == RECORDOID {
            get_record_type_from_query(fcinfo, funcname, cache);
            /* This can't change argtype */
            Assert!((*cache).argtype == RECORDOID);
        }
    }

    /* if the json is null send back an empty set */
    if PG_ARGISNULL(fcinfo, json_arg_num) {
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * Forcibly update the cached tupdesc.
     */
    update_cached_tupdesc(&mut (*cache).c.io.composite, (*cache).fn_mcxt);

    state = palloc0(core::mem::size_of::<PopulateRecordsetState>()) as *mut PopulateRecordsetState;

    /* make tuplestore in a sufficiently long-lived memory context */
    old_cxt = MemoryContextSwitchTo(rsi_econtext_per_query(rsi));
    (*state).tuple_store =
        tuplestore_begin_heap((rsi_allowedModes(rsi) & SFRM_Materialize_Random) != 0, false, work_mem());
    MemoryContextSwitchTo(old_cxt);

    (*state).function_name = funcname;
    (*state).cache = cache;
    (*state).rec = rec;

    if is_json {
        let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, json_arg_num);
        let mut lex: JsonLexContext = core::mem::zeroed();
        let sem: *mut JsonSemAction;

        sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;

        makeJsonLexContext(&mut lex, json, true);

        (*sem).semstate = state as *mut c_void;
        (*sem).array_start = Some(populate_recordset_array_start);
        (*sem).array_element_start = Some(populate_recordset_array_element_start);
        (*sem).scalar = Some(populate_recordset_scalar);
        (*sem).object_field_start = Some(populate_recordset_object_field_start);
        (*sem).object_field_end = Some(populate_recordset_object_field_end);
        (*sem).object_start = Some(populate_recordset_object_start);
        (*sem).object_end = Some(populate_recordset_object_end);

        (*state).lex = &mut lex;

        pg_parse_json_or_ereport(&mut lex, sem);

        freeJsonLexContext(&mut lex);
        (*state).lex = std::ptr::null_mut();
    } else {
        let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, json_arg_num);
        let mut it: *mut JsonbIterator;
        let mut v: JsonbValue = core::mem::zeroed();
        let mut skipNested: bool = false;
        let mut r: JsonbIteratorToken;

        if JB_ROOT_IS_SCALAR(jb) || !JB_ROOT_IS_ARRAY(jb) {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot call {} on a non-array",
                    std::ffi::CStr::from_ptr(funcname).to_string_lossy()
                )
            );
        }

        it = JsonbIteratorInit(&mut (*jb).root);

        loop {
            r = JsonbIteratorNext(&mut it, &mut v, skipNested);
            if r == WJB_DONE {
                break;
            }
            skipNested = true;

            if r == WJB_ELEM {
                let mut obj: JsObject = core::mem::zeroed();

                if v.type_ != jbvBinary || !JsonContainerIsObject(v.val.binary.data) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "argument of {} must be an array of objects",
                            std::ffi::CStr::from_ptr(funcname).to_string_lossy()
                        )
                    );
                }

                obj.is_json = false;
                obj.val.jsonb_cont = v.val.binary.data;

                populate_recordset_record(state, &mut obj);
            }
        }
    }

    /*
     * Note: we must copy the cached tupdesc because the executor will free
     * the passed-back setDesc.
     */
    set_rsi_setResult(rsi, (*state).tuple_store);
    set_rsi_setDesc(rsi, CreateTupleDescCopy((*cache).c.io.composite.tupdesc));

    PG_RETURN_NULL!(fcinfo)
}

/* ReturnSetInfo accessors - TODO(pg-port): nodes/execnodes.h */
unsafe fn rsi_allowedModes(_rsi: *mut ReturnSetInfo) -> c_int {
    unimplemented!("ReturnSetInfo.allowedModes not yet ported")
}
unsafe fn set_rsi_returnMode(_rsi: *mut ReturnSetInfo, _mode: c_int) {
    unimplemented!("ReturnSetInfo.returnMode (set) not yet ported")
}
unsafe fn rsi_econtext_per_query(_rsi: *mut ReturnSetInfo) -> MemoryContext {
    unimplemented!("ReturnSetInfo.econtext->ecxt_per_query_memory not yet ported")
}
unsafe fn set_rsi_setResult(_rsi: *mut ReturnSetInfo, _ts: *mut Tuplestorestate) {
    unimplemented!("ReturnSetInfo.setResult (set) not yet ported")
}
unsafe fn set_rsi_setDesc(_rsi: *mut ReturnSetInfo, _td: TupleDesc) {
    unimplemented!("ReturnSetInfo.setDesc (set) not yet ported")
}

pub unsafe extern "C" fn populate_recordset_object_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut PopulateRecordsetState;
    let lex_level: c_int = (*(*_state).lex).lex_level;
    let mut ctl: HASHCTL = core::mem::zeroed();

    /* Reject object at top level: we must have an array at level 0 */
    if lex_level == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on an object",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    /* Nested objects require no special processing */
    if lex_level > 1 {
        return JSON_SUCCESS;
    }

    /* Object at level 1: set up a new hash table for this object */
    ctl.keysize = NAMEDATALEN as Size;
    ctl.entrysize = core::mem::size_of::<JsonHashEntry>() as Size;
    ctl.hcxt = CurrentMemoryContext();
    (*_state).json_hash = hash_create(
        c"json object hashtable".as_ptr(),
        100,
        &mut ctl,
        HASH_ELEM | HASH_STRINGS | HASH_CONTEXT,
    );

    JSON_SUCCESS
}

pub unsafe extern "C" fn populate_recordset_object_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut PopulateRecordsetState;
    let mut obj: JsObject = core::mem::zeroed();

    /* Nested objects require no special processing */
    if (*(*_state).lex).lex_level > 1 {
        return JSON_SUCCESS;
    }

    obj.is_json = true;
    obj.val.json_hash = (*_state).json_hash;

    /* Otherwise, construct and return a tuple based on this level-1 object */
    populate_recordset_record(_state, &mut obj);

    /* Done with hash for this object */
    hash_destroy((*_state).json_hash);
    (*_state).json_hash = std::ptr::null_mut();

    JSON_SUCCESS
}

pub unsafe extern "C" fn populate_recordset_array_element_start(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut PopulateRecordsetState;

    if (*(*_state).lex).lex_level == 1 && (*(*_state).lex).token_type != JSON_TOKEN_OBJECT_START {
        ereport!(
            ERROR,
            errmsg!(
                "argument of {} must be an array of objects",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn populate_recordset_array_start(state: *mut c_void) -> JsonParseErrorType {
    /* nothing to do */
    JSON_SUCCESS
}

pub unsafe extern "C" fn populate_recordset_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut PopulateRecordsetState;

    if (*(*_state).lex).lex_level == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot call {} on a scalar",
                std::ffi::CStr::from_ptr((*_state).function_name).to_string_lossy()
            )
        );
    }

    if (*(*_state).lex).lex_level == 2 {
        (*_state).saved_scalar = token;
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn populate_recordset_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut PopulateRecordsetState;

    if (*(*_state).lex).lex_level > 2 {
        return JSON_SUCCESS;
    }

    (*_state).saved_token_type = (*(*_state).lex).token_type;

    if (*(*_state).lex).token_type == JSON_TOKEN_ARRAY_START
        || (*(*_state).lex).token_type == JSON_TOKEN_OBJECT_START
    {
        (*_state).save_json_start = (*(*_state).lex).token_start;
    } else {
        (*_state).save_json_start = std::ptr::null();
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn populate_recordset_object_field_end(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut PopulateRecordsetState;
    let hashentry: *mut JsonHashEntry;
    let mut found: bool = false;

    /* Ignore nested fields. */
    if (*(*_state).lex).lex_level > 2 {
        return JSON_SUCCESS;
    }

    /*
     * Ignore field names >= NAMEDATALEN - they can't match a record field.
     */
    if libc_strlen(fname) >= NAMEDATALEN {
        return JSON_SUCCESS;
    }

    hashentry =
        hash_search((*_state).json_hash, fname as *const c_void, HASH_ENTER, &mut found) as *mut JsonHashEntry;

    /*
     * found being true indicates a duplicate. A later field with the same
     * name overrides the earlier field.
     */

    (*hashentry).type_ = (*_state).saved_token_type;
    Assert!(isnull == ((*hashentry).type_ == JSON_TOKEN_NULL));

    if !(*_state).save_json_start.is_null() {
        let len: c_int =
            ((*(*_state).lex).prev_token_terminator as isize - (*_state).save_json_start as isize) as c_int;
        let val: *mut c_char = palloc((len + 1) as usize * core::mem::size_of::<c_char>()) as *mut c_char;

        std::ptr::copy_nonoverlapping((*_state).save_json_start, val, len as usize);
        *val.offset(len as isize) = b'\0' as c_char;
        (*hashentry).val = val;
    } else {
        /* must have had a scalar instead */
        (*hashentry).val = (*_state).saved_scalar;
    }

    JSON_SUCCESS
}

/*
 * Semantic actions for json_strip_nulls.
 */
pub unsafe extern "C" fn sn_object_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    appendStringInfoChar_si((*_state).strval, b'{' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn sn_object_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    appendStringInfoChar_si((*_state).strval, b'}' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn sn_array_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    appendStringInfoChar_si((*_state).strval, b'[' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn sn_array_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    appendStringInfoChar_si((*_state).strval, b']' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn sn_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    if isnull {
        /*
         * The next thing must be a scalar or isnull couldn't be true.
         */
        (*_state).skip_next_null = true;
        return JSON_SUCCESS;
    }

    if *si_data((*_state).strval).offset((si_len((*_state).strval) - 1) as isize) != b'{' as c_char {
        appendStringInfoChar_si((*_state).strval, b',' as c_char);
    }

    /*
     * Unfortunately we don't have the quoted and escaped string any more, so
     * we have to re-escape it.
     */
    escape_json((*_state).strval, fname);

    appendStringInfoChar_si((*_state).strval, b':' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn sn_array_element_start(state: *mut c_void, isnull: bool) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    /* If strip_in_arrays is enabled and this is a null, mark it for skipping */
    if isnull && (*_state).strip_in_arrays {
        (*_state).skip_next_null = true;
        return JSON_SUCCESS;
    }

    /* Only add a comma if this is not the first valid element */
    if si_len((*_state).strval) > 0
        && *si_data((*_state).strval).offset((si_len((*_state).strval) - 1) as isize) != b'[' as c_char
    {
        appendStringInfoChar_si((*_state).strval, b',' as c_char);
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn sn_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut StripnullState;

    if (*_state).skip_next_null {
        Assert!(tokentype == JSON_TOKEN_NULL);
        (*_state).skip_next_null = false;
        return JSON_SUCCESS;
    }

    if tokentype == JSON_TOKEN_STRING {
        escape_json((*_state).strval, token);
    } else {
        appendStringInfoString_si((*_state).strval, token);
    }

    JSON_SUCCESS
}

/* StringInfo helpers operating on the exported StringInfo type from lib::stringinfo */
unsafe fn appendStringInfoChar_si(_str: StringInfo, _ch: c_char) {
    unimplemented!("appendStringInfoCharMacro not yet wired")
}
unsafe fn appendStringInfoString_si(_str: StringInfo, _s: *const c_char) {
    unimplemented!("appendStringInfoString not yet wired")
}
unsafe fn si_data(_str: StringInfo) -> *mut c_char {
    unimplemented!("StringInfo.data not yet wired")
}
unsafe fn si_len(_str: StringInfo) -> c_int {
    unimplemented!("StringInfo.len not yet wired")
}

/*
 * SQL function json_strip_nulls(json) -> json
 */
pub unsafe fn json_strip_nulls(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let strip_in_arrays: bool = if PG_NARGS(fcinfo) == 2 { PG_GETARG_BOOL(fcinfo, 1) } else { false };
    let state: *mut StripnullState;
    let mut lex: JsonLexContext = core::mem::zeroed();
    let sem: *mut JsonSemAction;

    state = palloc0(core::mem::size_of::<StripnullState>()) as *mut StripnullState;
    sem = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;

    (*state).lex = makeJsonLexContext(&mut lex, json, true);
    (*state).strval = makeStringInfo() as StringInfo;
    (*state).skip_next_null = false;
    (*state).strip_in_arrays = strip_in_arrays;

    (*sem).semstate = state as *mut c_void;
    (*sem).object_start = Some(sn_object_start);
    (*sem).object_end = Some(sn_object_end);
    (*sem).array_start = Some(sn_array_start);
    (*sem).array_end = Some(sn_array_end);
    (*sem).scalar = Some(sn_scalar);
    (*sem).array_element_start = Some(sn_array_element_start);
    (*sem).object_field_start = Some(sn_object_field_start);

    pg_parse_json_or_ereport(&mut lex, sem);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len(si_data((*state).strval), si_len((*state).strval)) as *mut c_void)
}

/*
 * SQL function jsonb_strip_nulls(jsonb, bool) -> jsonb
 */
pub unsafe fn jsonb_strip_nulls(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let mut strip_in_arrays: bool = false;
    let mut it: *mut JsonbIterator;
    let mut parseState: *mut JsonbParseState = std::ptr::null_mut();
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut v: JsonbValue = core::mem::zeroed();
    let mut k: JsonbValue = core::mem::zeroed();
    let mut type_: JsonbIteratorToken;
    let mut last_was_key: bool = false;

    if PG_NARGS(fcinfo) == 2 {
        strip_in_arrays = PG_GETARG_BOOL(fcinfo, 1);
    }

    if JB_ROOT_IS_SCALAR(jb) {
        PG_RETURN_POINTER!(jb);
    }

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        type_ = JsonbIteratorNext(&mut it, &mut v, false);
        if type_ == WJB_DONE {
            break;
        }
        Assert!(!(type_ == WJB_KEY && last_was_key));

        if type_ == WJB_KEY {
            /* stash the key until we know if it has a null value */
            k = v;
            last_was_key = true;
            continue;
        }

        if last_was_key {
            /* if the last element was a key this one can't be */
            last_was_key = false;

            /* skip this field if value is null */
            if type_ == WJB_VALUE && v.type_ == jbvNull {
                continue;
            }

            /* otherwise, do a delayed push of the key */
            pushJsonbValue(&mut parseState, WJB_KEY, &mut k);
        }

        /* if strip_in_arrays is set, also skip null array elements */
        if strip_in_arrays {
            if type_ == WJB_ELEM && v.type_ == jbvNull {
                continue;
            }
        }

        if type_ == WJB_VALUE || type_ == WJB_ELEM {
            res = pushJsonbValue(&mut parseState, type_, &mut v);
        } else {
            res = pushJsonbValue(&mut parseState, type_, std::ptr::null_mut());
        }
    }

    Assert!(!res.is_null());

    PG_RETURN_POINTER!(JsonbValueToJsonb(res))
}

/*
 * SQL function jsonb_pretty (jsonb)
 */
pub unsafe fn jsonb_pretty(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let str: *mut StringInfoData = makeStringInfo();

    JsonbToCStringIndent(str, &mut (*jb).root, VARSIZE(jb as *mut c_void));

    PG_RETURN_TEXT_P!(cstring_to_text_with_len((*str).data, (*str).len))
}

/*
 * SQL function jsonb_concat (jsonb, jsonb)
 */
pub unsafe fn jsonb_concat(fcinfo: FunctionCallInfo) -> Datum {
    let jb1: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jb2: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let mut state: *mut JsonbParseState = std::ptr::null_mut();
    let res: *mut JsonbValue;
    let mut it1: *mut JsonbIterator;
    let mut it2: *mut JsonbIterator;

    /*
     * If one of the jsonb is empty, just return the other if it's not scalar
     * and both are of the same kind.
     */
    if JB_ROOT_IS_OBJECT(jb1) == JB_ROOT_IS_OBJECT(jb2) {
        if JB_ROOT_COUNT(jb1) == 0 && !JB_ROOT_IS_SCALAR(jb2) {
            PG_RETURN_JSONB_P!(jb2);
        } else if JB_ROOT_COUNT(jb2) == 0 && !JB_ROOT_IS_SCALAR(jb1) {
            PG_RETURN_JSONB_P!(jb1);
        }
    }

    it1 = JsonbIteratorInit(&mut (*jb1).root);
    it2 = JsonbIteratorInit(&mut (*jb2).root);

    res = IteratorConcat(&mut it1, &mut it2, &mut state);

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

/*
 * SQL function jsonb_delete (jsonb, text)
 */
pub unsafe fn jsonb_delete(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let key: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let keyptr: *mut c_char = VARDATA_ANY(key);
    let keylen: c_int = VARSIZE_ANY_EXHDR(key);
    let mut state: *mut JsonbParseState = std::ptr::null_mut();
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut skipNested: bool = false;
    let mut r: JsonbIteratorToken;

    if JB_ROOT_IS_SCALAR(in_) {
        ereport!(ERROR, errmsg!("cannot delete from scalar"));
    }

    if JB_ROOT_COUNT(in_) == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    it = JsonbIteratorInit(&mut (*in_).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, skipNested);
        if r == WJB_DONE {
            break;
        }
        skipNested = true;

        if (r == WJB_ELEM || r == WJB_KEY)
            && (v.type_ == jbvString
                && keylen == v.val.string.len
                && libc_memcmp(keyptr, v.val.string.val, keylen as usize) == 0)
        {
            /* skip corresponding value as well */
            if r == WJB_KEY {
                JsonbIteratorNext(&mut it, &mut v, true);
            }

            continue;
        }

        res = pushJsonbValue(&mut state, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
    }

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

unsafe fn libc_memcmp(_a: *const c_char, _b: *const c_char, _n: usize) -> c_int {
    unimplemented!("memcmp (libc) not yet wired")
}

/*
 * SQL function jsonb_delete (jsonb, variadic text[])
 */
pub unsafe fn jsonb_delete_array(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let keys: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let mut keys_elems: *mut Datum = std::ptr::null_mut();
    let mut keys_nulls: *mut bool = std::ptr::null_mut();
    let mut keys_len: c_int = 0;
    let mut state: *mut JsonbParseState = std::ptr::null_mut();
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut skipNested: bool = false;
    let mut r: JsonbIteratorToken;

    if ARR_NDIM(keys) > 1 {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    if JB_ROOT_IS_SCALAR(in_) {
        ereport!(ERROR, errmsg!("cannot delete from scalar"));
    }

    if JB_ROOT_COUNT(in_) == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    deconstruct_array_builtin(keys, TEXTOID, &mut keys_elems, &mut keys_nulls, &mut keys_len);

    if keys_len == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    it = JsonbIteratorInit(&mut (*in_).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, skipNested);
        if r == WJB_DONE {
            break;
        }
        skipNested = true;

        if (r == WJB_ELEM || r == WJB_KEY) && v.type_ == jbvString {
            let mut i: c_int;
            let mut found: bool = false;

            i = 0;
            while i < keys_len {
                let keyptr: *mut c_char;
                let keylen: c_int;

                if *keys_nulls.offset(i as isize) {
                    i += 1;
                    continue;
                }

                /* We rely on the array elements not being toasted */
                keyptr = VARDATA_ANY(*keys_elems.offset(i as isize) as *mut c_void);
                keylen = VARSIZE_ANY_EXHDR(*keys_elems.offset(i as isize) as *mut c_void);
                if keylen == v.val.string.len
                    && libc_memcmp(keyptr, v.val.string.val, keylen as usize) == 0
                {
                    found = true;
                    break;
                }
                i += 1;
            }
            if found {
                /* skip corresponding value as well */
                if r == WJB_KEY {
                    JsonbIteratorNext(&mut it, &mut v, true);
                }

                continue;
            }
        }

        res = pushJsonbValue(&mut state, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
    }

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

unsafe fn ARR_NDIM(_a: *mut ArrayType) -> c_int {
    unimplemented!("ARR_NDIM not yet ported")
}

/*
 * SQL function jsonb_delete (jsonb, int)
 */
pub unsafe fn jsonb_delete_idx(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let mut idx: c_int = PG_GETARG_INT32(fcinfo, 1);
    let mut state: *mut JsonbParseState = std::ptr::null_mut();
    let mut it: *mut JsonbIterator;
    let mut i: uint32 = 0;
    let n: uint32;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut r: JsonbIteratorToken;

    if JB_ROOT_IS_SCALAR(in_) {
        ereport!(ERROR, errmsg!("cannot delete from scalar"));
    }

    if JB_ROOT_IS_OBJECT(in_) {
        ereport!(ERROR, errmsg!("cannot delete from object using integer index"));
    }

    if JB_ROOT_COUNT(in_) == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    it = JsonbIteratorInit(&mut (*in_).root);

    r = JsonbIteratorNext(&mut it, &mut v, false);
    Assert!(r == WJB_BEGIN_ARRAY);
    n = v.val.array.nElems as uint32;

    if idx < 0 {
        if pg_abs_s32(idx) > n {
            idx = n as c_int;
        } else {
            idx = n as c_int + idx;
        }
    }

    if idx as uint32 >= n {
        PG_RETURN_JSONB_P!(in_);
    }

    pushJsonbValue(&mut state, r, std::ptr::null_mut());

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, true);
        if r == WJB_DONE {
            break;
        }
        if r == WJB_ELEM {
            let cur = i;
            i += 1;
            if cur == idx as uint32 {
                continue;
            }
        }

        res = pushJsonbValue(&mut state, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
    }

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

/*
 * SQL function jsonb_set(jsonb, text[], jsonb, boolean)
 */
pub unsafe fn jsonb_set(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let path: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let newjsonb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 2);
    let mut newval: JsonbValue = core::mem::zeroed();
    let create: bool = PG_GETARG_BOOL(fcinfo, 3);
    let res: *mut JsonbValue;
    let mut path_elems: *mut Datum = std::ptr::null_mut();
    let mut path_nulls: *mut bool = std::ptr::null_mut();
    let mut path_len: c_int = 0;
    let mut it: *mut JsonbIterator;
    let mut st: *mut JsonbParseState = std::ptr::null_mut();

    JsonbToJsonbValue(newjsonb, &mut newval);

    if ARR_NDIM(path) > 1 {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    if JB_ROOT_IS_SCALAR(in_) {
        ereport!(ERROR, errmsg!("cannot set path in scalar"));
    }

    if JB_ROOT_COUNT(in_) == 0 && !create {
        PG_RETURN_JSONB_P!(in_);
    }

    deconstruct_array_builtin(path, TEXTOID, &mut path_elems, &mut path_nulls, &mut path_len);

    if path_len == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    it = JsonbIteratorInit(&mut (*in_).root);

    res = setPath(
        &mut it,
        path_elems,
        path_nulls,
        path_len,
        &mut st,
        0,
        &mut newval,
        if create { JB_PATH_CREATE } else { JB_PATH_REPLACE },
    );

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

/*
 * SQL function jsonb_set_lax(jsonb, text[], jsonb, boolean, text)
 */
pub unsafe fn jsonb_set_lax(fcinfo: FunctionCallInfo) -> Datum {
    /* Jsonb        *in = PG_GETARG_JSONB_P(0); */
    /* ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1); */
    /* Jsonb      *newval = PG_GETARG_JSONB_P(2); */
    /* bool     create = PG_GETARG_BOOL(3); */
    let handle_null: *mut text;
    let handle_val: *mut c_char;

    if PG_ARGISNULL(fcinfo, 0) || PG_ARGISNULL(fcinfo, 1) || PG_ARGISNULL(fcinfo, 3) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* could happen if they pass in an explicit NULL */
    if PG_ARGISNULL(fcinfo, 4) {
        ereport!(
            ERROR,
            errmsg!("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")
        );
    }

    /* if the new value isn't an SQL NULL just call jsonb_set */
    if !PG_ARGISNULL(fcinfo, 2) {
        return jsonb_set(fcinfo);
    }

    handle_null = PG_GETARG_TEXT_P(fcinfo, 4);
    handle_val = text_to_cstring(handle_null as *const crate::c::text);

    if libc_strcmp(handle_val, c"raise_exception".as_ptr()) == 0 {
        ereport!(ERROR, errmsg!("JSON value must not be null"));
        0 as Datum /* silence stupider compilers */
    } else if libc_strcmp(handle_val, c"use_json_null".as_ptr()) == 0 {
        let newval: Datum;

        newval = DirectFunctionCall1(jsonb_in, CStringGetDatum(c"null".as_ptr()));

        set_fcinfo_arg(fcinfo, 2, newval, false);
        jsonb_set(fcinfo)
    } else if libc_strcmp(handle_val, c"delete_key".as_ptr()) == 0 {
        jsonb_delete_path(fcinfo)
    } else if libc_strcmp(handle_val, c"return_target".as_ptr()) == 0 {
        let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);

        PG_RETURN_JSONB_P!(in_)
    } else {
        ereport!(
            ERROR,
            errmsg!("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")
        );
        0 as Datum /* silence stupider compilers */
    }
}

unsafe fn set_fcinfo_arg(_fcinfo: FunctionCallInfo, _n: c_int, _value: Datum, _isnull: bool) {
    unimplemented!("fcinfo->args[n] (set) not yet ported")
}

/*
 * SQL function jsonb_delete_path(jsonb, text[])
 */
pub unsafe fn jsonb_delete_path(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let path: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let res: *mut JsonbValue;
    let mut path_elems: *mut Datum = std::ptr::null_mut();
    let mut path_nulls: *mut bool = std::ptr::null_mut();
    let mut path_len: c_int = 0;
    let mut it: *mut JsonbIterator;
    let mut st: *mut JsonbParseState = std::ptr::null_mut();

    if ARR_NDIM(path) > 1 {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    if JB_ROOT_IS_SCALAR(in_) {
        ereport!(ERROR, errmsg!("cannot delete path in scalar"));
    }

    if JB_ROOT_COUNT(in_) == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    deconstruct_array_builtin(path, TEXTOID, &mut path_elems, &mut path_nulls, &mut path_len);

    if path_len == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    it = JsonbIteratorInit(&mut (*in_).root);

    res = setPath(&mut it, path_elems, path_nulls, path_len, &mut st, 0, std::ptr::null_mut(), JB_PATH_DELETE);

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

/*
 * SQL function jsonb_insert(jsonb, text[], jsonb, boolean)
 */
pub unsafe fn jsonb_insert(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let path: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let newjsonb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 2);
    let mut newval: JsonbValue = core::mem::zeroed();
    let after: bool = PG_GETARG_BOOL(fcinfo, 3);
    let res: *mut JsonbValue;
    let mut path_elems: *mut Datum = std::ptr::null_mut();
    let mut path_nulls: *mut bool = std::ptr::null_mut();
    let mut path_len: c_int = 0;
    let mut it: *mut JsonbIterator;
    let mut st: *mut JsonbParseState = std::ptr::null_mut();

    JsonbToJsonbValue(newjsonb, &mut newval);

    if ARR_NDIM(path) > 1 {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    if JB_ROOT_IS_SCALAR(in_) {
        ereport!(ERROR, errmsg!("cannot set path in scalar"));
    }

    deconstruct_array_builtin(path, TEXTOID, &mut path_elems, &mut path_nulls, &mut path_len);

    if path_len == 0 {
        PG_RETURN_JSONB_P!(in_);
    }

    it = JsonbIteratorInit(&mut (*in_).root);

    res = setPath(
        &mut it,
        path_elems,
        path_nulls,
        path_len,
        &mut st,
        0,
        &mut newval,
        if after { JB_PATH_INSERT_AFTER } else { JB_PATH_INSERT_BEFORE },
    );

    Assert!(!res.is_null());

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(res))
}

/*
 * Iterate over all jsonb objects and merge them into one.
 */
unsafe fn IteratorConcat(
    it1: *mut *mut JsonbIterator,
    it2: *mut *mut JsonbIterator,
    state: *mut *mut JsonbParseState,
) -> *mut JsonbValue {
    let mut v1: JsonbValue = core::mem::zeroed();
    let mut v2: JsonbValue = core::mem::zeroed();
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut r1: JsonbIteratorToken;
    let mut r2: JsonbIteratorToken;
    let rk1: JsonbIteratorToken;
    let rk2: JsonbIteratorToken;

    rk1 = JsonbIteratorNext(it1, &mut v1, false);
    rk2 = JsonbIteratorNext(it2, &mut v2, false);

    /*
     * JsonbIteratorNext reports raw scalars as if they were single-element
     * arrays; hence we only need consider "object" and "array" cases here.
     */
    if rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT {
        /*
         * Both inputs are objects.
         */
        pushJsonbValue(state, rk1, std::ptr::null_mut());
        loop {
            r1 = JsonbIteratorNext(it1, &mut v1, true);
            if r1 == WJB_END_OBJECT {
                break;
            }
            pushJsonbValue(state, r1, &mut v1);
        }

        loop {
            r2 = JsonbIteratorNext(it2, &mut v2, true);
            if r2 == WJB_DONE {
                break;
            }
            res = pushJsonbValue(state, r2, if r2 != WJB_END_OBJECT { &mut v2 } else { std::ptr::null_mut() });
        }
    } else if rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY {
        /*
         * Both inputs are arrays.
         */
        pushJsonbValue(state, rk1, std::ptr::null_mut());

        loop {
            r1 = JsonbIteratorNext(it1, &mut v1, true);
            if r1 == WJB_END_ARRAY {
                break;
            }
            Assert!(r1 == WJB_ELEM);
            pushJsonbValue(state, r1, &mut v1);
        }

        loop {
            r2 = JsonbIteratorNext(it2, &mut v2, true);
            if r2 == WJB_END_ARRAY {
                break;
            }
            Assert!(r2 == WJB_ELEM);
            pushJsonbValue(state, WJB_ELEM, &mut v2);
        }

        res = pushJsonbValue(state, WJB_END_ARRAY, std::ptr::null_mut() /* signal to sort */);
    } else if rk1 == WJB_BEGIN_OBJECT {
        /*
         * We have object || array.
         */
        Assert!(rk2 == WJB_BEGIN_ARRAY);

        pushJsonbValue(state, WJB_BEGIN_ARRAY, std::ptr::null_mut());

        pushJsonbValue(state, WJB_BEGIN_OBJECT, std::ptr::null_mut());
        loop {
            r1 = JsonbIteratorNext(it1, &mut v1, true);
            if r1 == WJB_DONE {
                break;
            }
            pushJsonbValue(state, r1, if r1 != WJB_END_OBJECT { &mut v1 } else { std::ptr::null_mut() });
        }

        loop {
            r2 = JsonbIteratorNext(it2, &mut v2, true);
            if r2 == WJB_DONE {
                break;
            }
            res = pushJsonbValue(state, r2, if r2 != WJB_END_ARRAY { &mut v2 } else { std::ptr::null_mut() });
        }
    } else {
        /*
         * We have array || object.
         */
        Assert!(rk1 == WJB_BEGIN_ARRAY);
        Assert!(rk2 == WJB_BEGIN_OBJECT);

        pushJsonbValue(state, WJB_BEGIN_ARRAY, std::ptr::null_mut());

        loop {
            r1 = JsonbIteratorNext(it1, &mut v1, true);
            if r1 == WJB_END_ARRAY {
                break;
            }
            pushJsonbValue(state, r1, &mut v1);
        }

        pushJsonbValue(state, WJB_BEGIN_OBJECT, std::ptr::null_mut());
        loop {
            r2 = JsonbIteratorNext(it2, &mut v2, true);
            if r2 == WJB_DONE {
                break;
            }
            pushJsonbValue(state, r2, if r2 != WJB_END_OBJECT { &mut v2 } else { std::ptr::null_mut() });
        }

        res = pushJsonbValue(state, WJB_END_ARRAY, std::ptr::null_mut());
    }

    res
}

/*
 * Do most of the heavy work for jsonb_set/jsonb_insert
 */
unsafe fn setPath(
    it: *mut *mut JsonbIterator,
    path_elems: *mut Datum,
    path_nulls: *mut bool,
    path_len: c_int,
    st: *mut *mut JsonbParseState,
    level: c_int,
    newval: *mut JsonbValue,
    op_type: c_int,
) -> *mut JsonbValue {
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;
    let res: *mut JsonbValue;

    check_stack_depth();

    if *path_nulls.offset(level as isize) {
        ereport!(ERROR, errmsg!("path element at position {} is null", level + 1));
    }

    r = JsonbIteratorNext(it, &mut v, false);

    match r {
        WJB_BEGIN_ARRAY => {
            /*
             * If instructed complain about attempts to replace within a raw
             * scalar value.
             */
            if (op_type & JB_PATH_FILL_GAPS) != 0
                && (level <= path_len - 1)
                && v.val.array.rawScalar
            {
                ereport!(ERROR, errmsg!("cannot replace existing key"));
            }

            pushJsonbValue(st, r, std::ptr::null_mut());
            setPathArray(it, path_elems, path_nulls, path_len, st, level, newval, v.val.array.nElems as uint32, op_type);
            r = JsonbIteratorNext(it, &mut v, false);
            Assert!(r == WJB_END_ARRAY);
            res = pushJsonbValue(st, r, std::ptr::null_mut());
        }
        WJB_BEGIN_OBJECT => {
            pushJsonbValue(st, r, std::ptr::null_mut());
            setPathObject(it, path_elems, path_nulls, path_len, st, level, newval, v.val.object.nPairs as uint32, op_type);
            r = JsonbIteratorNext(it, &mut v, true);
            Assert!(r == WJB_END_OBJECT);
            res = pushJsonbValue(st, r, std::ptr::null_mut());
        }
        WJB_ELEM | WJB_VALUE => {
            /*
             * If instructed complain about attempts to replace within a
             * scalar value.
             */
            if (op_type & JB_PATH_FILL_GAPS) != 0 && (level <= path_len - 1) {
                ereport!(ERROR, errmsg!("cannot replace existing key"));
            }

            res = pushJsonbValue(st, r, &mut v);
        }
        _ => {
            elog!(ERROR, "unrecognized iterator result: {}", r as c_int);
            res = std::ptr::null_mut(); /* keep compiler quiet */
        }
    }

    res
}

/*
 * Object walker for setPath
 */
unsafe fn setPathObject(
    it: *mut *mut JsonbIterator,
    path_elems: *mut Datum,
    path_nulls: *mut bool,
    path_len: c_int,
    st: *mut *mut JsonbParseState,
    level: c_int,
    newval: *mut JsonbValue,
    npairs: uint32,
    op_type: c_int,
) {
    let mut pathelem: *mut text = std::ptr::null_mut();
    let mut i: uint32;
    let mut k: JsonbValue = core::mem::zeroed();
    let mut v: JsonbValue = core::mem::zeroed();
    let mut done: bool = false;

    if level >= path_len || *path_nulls.offset(level as isize) {
        done = true;
    } else {
        /* The path Datum could be toasted, in which case we must detoast it */
        pathelem = DatumGetTextPP(*path_elems.offset(level as isize));
    }

    /* empty object is a special case for create */
    if npairs == 0 && (op_type & JB_PATH_CREATE_OR_INSERT) != 0 && level == path_len - 1 {
        let mut newkey: JsonbValue = core::mem::zeroed();

        newkey.type_ = jbvString;
        newkey.val.string.val = VARDATA_ANY(pathelem);
        newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

        pushJsonbValue(st, WJB_KEY, &mut newkey);
        pushJsonbValue(st, WJB_VALUE, newval);
    }

    i = 0;
    while i < npairs {
        let mut r: JsonbIteratorToken = JsonbIteratorNext(it, &mut k, true);

        Assert!(r == WJB_KEY);

        if !done
            && k.val.string.len == VARSIZE_ANY_EXHDR(pathelem)
            && libc_memcmp(k.val.string.val, VARDATA_ANY(pathelem), k.val.string.len as usize) == 0
        {
            done = true;

            if level == path_len - 1 {
                /*
                 * called from jsonb_insert(), it forbids redefining an
                 * existing value
                 */
                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER)) != 0 {
                    ereport!(ERROR, errmsg!("cannot replace existing key"));
                }

                r = JsonbIteratorNext(it, &mut v, true); /* skip value */
                if (op_type & JB_PATH_DELETE) == 0 {
                    pushJsonbValue(st, WJB_KEY, &mut k);
                    pushJsonbValue(st, WJB_VALUE, newval);
                }
            } else {
                pushJsonbValue(st, r, &mut k);
                setPath(it, path_elems, path_nulls, path_len, st, level + 1, newval, op_type);
            }
        } else {
            if (op_type & JB_PATH_CREATE_OR_INSERT) != 0
                && !done
                && level == path_len - 1
                && i == npairs - 1
            {
                let mut newkey: JsonbValue = core::mem::zeroed();

                newkey.type_ = jbvString;
                newkey.val.string.val = VARDATA_ANY(pathelem);
                newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

                pushJsonbValue(st, WJB_KEY, &mut newkey);
                pushJsonbValue(st, WJB_VALUE, newval);
            }

            pushJsonbValue(st, r, &mut k);
            r = JsonbIteratorNext(it, &mut v, false);
            pushJsonbValue(st, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
            if r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT {
                let mut walking_level: c_int = 1;

                while walking_level != 0 {
                    r = JsonbIteratorNext(it, &mut v, false);

                    if r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT {
                        walking_level += 1;
                    }
                    if r == WJB_END_ARRAY || r == WJB_END_OBJECT {
                        walking_level -= 1;
                    }

                    pushJsonbValue(st, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
                }
            }
        }
        i += 1;
    }

    /*--
     * If we got here there are only few possibilities (see C comment).
     */
    if !done && (op_type & JB_PATH_FILL_GAPS) != 0 && (level < path_len - 1) {
        let mut newkey: JsonbValue = core::mem::zeroed();

        newkey.type_ = jbvString;
        newkey.val.string.val = VARDATA_ANY(pathelem);
        newkey.val.string.len = VARSIZE_ANY_EXHDR(pathelem);

        pushJsonbValue(st, WJB_KEY, &mut newkey);
        push_path(st, level, path_elems, path_nulls, path_len, newval);

        /* Result is closed with WJB_END_OBJECT outside of this function */
    }
}

/*
 * Array walker for setPath
 */
unsafe fn setPathArray(
    it: *mut *mut JsonbIterator,
    path_elems: *mut Datum,
    path_nulls: *mut bool,
    path_len: c_int,
    st: *mut *mut JsonbParseState,
    level: c_int,
    newval: *mut JsonbValue,
    nelems: uint32,
    op_type: c_int,
) {
    let mut v: JsonbValue = core::mem::zeroed();
    let mut idx: c_int;
    let mut i: c_int;
    let mut done: bool = false;

    /* pick correct index */
    if level < path_len && !*path_nulls.offset(level as isize) {
        let c: *mut c_char = TextDatumGetCString(*path_elems.offset(level as isize));
        let mut badp: *mut c_char = std::ptr::null_mut();

        set_errno(0);
        idx = strtoint(c, &mut badp, 10);
        if badp == c || *badp != b'\0' as c_char || get_errno() != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "path element at position {} is not an integer: \"{}\"",
                    level + 1,
                    std::ffi::CStr::from_ptr(c).to_string_lossy()
                )
            );
        }
    } else {
        idx = nelems as c_int;
    }

    if idx < 0 {
        if pg_abs_s32(idx) > nelems {
            /*
             * If asked to keep elements position consistent, it's not allowed
             * to prepend the array.
             */
            if (op_type & JB_PATH_CONSISTENT_POSITION) != 0 {
                ereport!(
                    ERROR,
                    errmsg!("path element at position {} is out of range: {}", level + 1, idx)
                );
            } else {
                idx = PG_INT32_MIN;
            }
        } else {
            idx = nelems as c_int + idx;
        }
    }

    /*
     * Filling the gaps means there are no limits on the positive index.
     */
    if (op_type & JB_PATH_FILL_GAPS) == 0 {
        if idx > 0 && idx > nelems as c_int {
            idx = nelems as c_int;
        }
    }

    /*
     * if we're creating, and idx == INT_MIN, we prepend the new value to the
     * array also if the array is empty.
     */
    if (idx == INT_MIN || nelems == 0)
        && (level == path_len - 1)
        && (op_type & JB_PATH_CREATE_OR_INSERT) != 0
    {
        Assert!(!newval.is_null());

        if (op_type & JB_PATH_FILL_GAPS) != 0 && nelems == 0 && idx > 0 {
            push_null_elements(st, idx);
        }

        pushJsonbValue(st, WJB_ELEM, newval);

        done = true;
    }

    /* iterate over the array elements */
    i = 0;
    while i < nelems as c_int {
        let mut r: JsonbIteratorToken;

        if i == idx && level < path_len {
            done = true;

            if level == path_len - 1 {
                r = JsonbIteratorNext(it, &mut v, true); /* skip */

                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE)) != 0 {
                    pushJsonbValue(st, WJB_ELEM, newval);
                }

                /*
                 * We should keep current value only in case of
                 * JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER.
                 */
                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE)) != 0 {
                    pushJsonbValue(st, r, &mut v);
                }

                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE)) != 0 {
                    pushJsonbValue(st, WJB_ELEM, newval);
                }
            } else {
                setPath(it, path_elems, path_nulls, path_len, st, level + 1, newval, op_type);
            }
        } else {
            r = JsonbIteratorNext(it, &mut v, false);

            pushJsonbValue(st, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });

            if r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT {
                let mut walking_level: c_int = 1;

                while walking_level != 0 {
                    r = JsonbIteratorNext(it, &mut v, false);

                    if r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT {
                        walking_level += 1;
                    }
                    if r == WJB_END_ARRAY || r == WJB_END_OBJECT {
                        walking_level -= 1;
                    }

                    pushJsonbValue(st, r, if r < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
                }
            }
        }
        i += 1;
    }

    if (op_type & JB_PATH_CREATE_OR_INSERT) != 0 && !done && level == path_len - 1 {
        /*
         * If asked to fill the gaps, idx could be bigger than nelems.
         */
        if (op_type & JB_PATH_FILL_GAPS) != 0 && idx > nelems as c_int {
            push_null_elements(st, idx - nelems as c_int);
        }

        pushJsonbValue(st, WJB_ELEM, newval);
        done = true;
    }

    /*--
     * If we got here there are only few possibilities (see C comment).
     */
    if !done && (op_type & JB_PATH_FILL_GAPS) != 0 && (level < path_len - 1) {
        if idx > 0 {
            push_null_elements(st, idx - nelems as c_int);
        }

        push_path(st, level, path_elems, path_nulls, path_len, newval);

        /* Result is closed with WJB_END_OBJECT outside of this function */
    }
}

/*
 * Parse information about what elements of a jsonb document we want to iterate.
 */
pub unsafe fn parse_jsonb_index_flags(jb: *mut Jsonb) -> uint32 {
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut type_: JsonbIteratorToken;
    let mut flags: uint32 = 0;

    it = JsonbIteratorInit(&mut (*jb).root);

    type_ = JsonbIteratorNext(&mut it, &mut v, false);

    /*
     * We iterate over array (scalar internally is represented as array, so,
     * we will accept it too) to check all its elements.
     */
    if type_ != WJB_BEGIN_ARRAY {
        ereport!(ERROR, errmsg!("wrong flag type, only arrays and scalars are allowed"));
    }

    loop {
        type_ = JsonbIteratorNext(&mut it, &mut v, false);
        if type_ != WJB_ELEM {
            break;
        }
        if v.type_ != jbvString {
            ereport!(ERROR, errmsg!("flag array element is not a string"));
        }

        if v.val.string.len == 3 && pg_strncasecmp(v.val.string.val, c"all".as_ptr(), 3) == 0 {
            flags |= jtiAll;
        } else if v.val.string.len == 3 && pg_strncasecmp(v.val.string.val, c"key".as_ptr(), 3) == 0 {
            flags |= jtiKey;
        } else if v.val.string.len == 6 && pg_strncasecmp(v.val.string.val, c"string".as_ptr(), 6) == 0 {
            flags |= jtiString;
        } else if v.val.string.len == 7 && pg_strncasecmp(v.val.string.val, c"numeric".as_ptr(), 7) == 0 {
            flags |= jtiNumeric;
        } else if v.val.string.len == 7 && pg_strncasecmp(v.val.string.val, c"boolean".as_ptr(), 7) == 0 {
            flags |= jtiBool;
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "wrong flag in flag array: \"{}\"",
                    std::ffi::CStr::from_ptr(pnstrdup(v.val.string.val, v.val.string.len)).to_string_lossy()
                )
            );
        }
    }

    /* expect end of array now */
    if type_ != WJB_END_ARRAY {
        elog!(ERROR, "unexpected end of flag array");
    }

    /* get final WJB_DONE and free iterator */
    type_ = JsonbIteratorNext(&mut it, &mut v, false);
    if type_ != WJB_DONE {
        elog!(ERROR, "unexpected end of flag array");
    }

    flags
}

/*
 * Iterate over jsonb values or elements, specified by flags.
 */
pub unsafe fn iterate_jsonb_values(
    jb: *mut Jsonb,
    flags: uint32,
    state: *mut c_void,
    action: JsonIterateStringValuesAction,
) {
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut type_: JsonbIteratorToken;

    it = JsonbIteratorInit(&mut (*jb).root);

    /*
     * Just recursively iterating over jsonb and call callback on all
     * corresponding elements
     */
    loop {
        type_ = JsonbIteratorNext(&mut it, &mut v, false);
        if type_ == WJB_DONE {
            break;
        }
        if type_ == WJB_KEY {
            if (flags & jtiKey) != 0 {
                action(state, v.val.string.val, v.val.string.len);
            }

            continue;
        } else if !(type_ == WJB_VALUE || type_ == WJB_ELEM) {
            /* do not call callback for composite JsonbValue */
            continue;
        }

        /* JsonbValue is a value of object or element of array */
        match v.type_ {
            jbvString => {
                if (flags & jtiString) != 0 {
                    action(state, v.val.string.val, v.val.string.len);
                }
            }
            jbvNumeric => {
                if (flags & jtiNumeric) != 0 {
                    let val: *mut c_char;

                    val = DatumGetCString(DirectFunctionCall1(
                        numeric_out,
                        NumericGetDatum(v.val.numeric as Numeric),
                    ));

                    action(state, val, libc_strlen(val) as c_int);
                    pfree(val as *mut c_void);
                }
            }
            jbvBool => {
                if (flags & jtiBool) != 0 {
                    if v.val.boolean {
                        action(state, c"true".as_ptr() as *mut c_char, 4);
                    } else {
                        action(state, c"false".as_ptr() as *mut c_char, 5);
                    }
                }
            }
            _ => {
                /* do not call callback for composite JsonbValue */
            }
        }
    }
}

/*
 * Iterate over json values and elements, specified by flags.
 */
pub unsafe fn iterate_json_values(
    json: *mut text,
    flags: uint32,
    action_state: *mut c_void,
    action: JsonIterateStringValuesAction,
) {
    let mut lex: JsonLexContext = core::mem::zeroed();
    let sem: *mut JsonSemAction = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;
    let state: *mut IterateJsonStringValuesState =
        palloc0(core::mem::size_of::<IterateJsonStringValuesState>()) as *mut IterateJsonStringValuesState;

    (*state).lex = makeJsonLexContext(&mut lex, json, true);
    (*state).action = action;
    (*state).action_state = action_state;
    (*state).flags = flags;

    (*sem).semstate = state as *mut c_void;
    (*sem).scalar = Some(iterate_values_scalar);
    (*sem).object_field_start = Some(iterate_values_object_field_start);

    pg_parse_json_or_ereport(&mut lex, sem);
    freeJsonLexContext(&mut lex);
}

pub unsafe extern "C" fn iterate_values_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut IterateJsonStringValuesState;

    if tokentype == JSON_TOKEN_STRING {
        if ((*_state).flags & jtiString) != 0 {
            ((*_state).action)((*_state).action_state, token, libc_strlen(token) as c_int);
        }
    } else if tokentype == JSON_TOKEN_NUMBER {
        if ((*_state).flags & jtiNumeric) != 0 {
            ((*_state).action)((*_state).action_state, token, libc_strlen(token) as c_int);
        }
    } else if tokentype == JSON_TOKEN_TRUE || tokentype == JSON_TOKEN_FALSE {
        if ((*_state).flags & jtiBool) != 0 {
            ((*_state).action)((*_state).action_state, token, libc_strlen(token) as c_int);
        }
    } else {
        /* do not call callback for any other token */
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn iterate_values_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut IterateJsonStringValuesState;

    if ((*_state).flags & jtiKey) != 0 {
        let val: *mut c_char = pstrdup(fname);

        ((*_state).action)((*_state).action_state, val, libc_strlen(val) as c_int);
    }

    JSON_SUCCESS
}

/*
 * Iterate over a jsonb, and apply a specified JsonTransformStringValuesAction.
 */
pub unsafe fn transform_jsonb_string_values(
    jsonb: *mut Jsonb,
    action_state: *mut c_void,
    transform_action: JsonTransformStringValuesAction,
) -> *mut Jsonb {
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut type_: JsonbIteratorToken;
    let mut st: *mut JsonbParseState = std::ptr::null_mut();
    let mut out: *mut text;
    let mut is_scalar: bool = false;

    it = JsonbIteratorInit(&mut (*jsonb).root);
    is_scalar = (*it).isScalar;

    loop {
        type_ = JsonbIteratorNext(&mut it, &mut v, false);
        if type_ == WJB_DONE {
            break;
        }
        if (type_ == WJB_VALUE || type_ == WJB_ELEM) && v.type_ == jbvString {
            out = transform_action(action_state, v.val.string.val, v.val.string.len);
            /* out is probably not toasted, but let's be sure */
            out = pg_detoast_datum_packed(out);
            v.val.string.val = VARDATA_ANY(out);
            v.val.string.len = VARSIZE_ANY_EXHDR(out);
            res = pushJsonbValue(&mut st, type_, if type_ < WJB_BEGIN_ARRAY { &mut v } else { std::ptr::null_mut() });
        } else {
            res = pushJsonbValue(
                &mut st,
                type_,
                if type_ == WJB_KEY || type_ == WJB_VALUE || type_ == WJB_ELEM {
                    &mut v
                } else {
                    std::ptr::null_mut()
                },
            );
        }
    }

    if (*res).type_ == jbvArray {
        (*res).val.array.rawScalar = is_scalar;
    }

    JsonbValueToJsonb(res)
}

/*
 * Iterate over a json, and apply a specified JsonTransformStringValuesAction.
 */
pub unsafe fn transform_json_string_values(
    json: *mut text,
    action_state: *mut c_void,
    transform_action: JsonTransformStringValuesAction,
) -> *mut text {
    let mut lex: JsonLexContext = core::mem::zeroed();
    let sem: *mut JsonSemAction = palloc0(core::mem::size_of::<JsonSemAction>()) as *mut JsonSemAction;
    let state: *mut TransformJsonStringValuesState =
        palloc0(core::mem::size_of::<TransformJsonStringValuesState>()) as *mut TransformJsonStringValuesState;

    (*state).lex = makeJsonLexContext(&mut lex, json, true);
    (*state).strval = makeStringInfo() as StringInfo;
    (*state).action = transform_action;
    (*state).action_state = action_state;

    (*sem).semstate = state as *mut c_void;
    (*sem).object_start = Some(transform_string_values_object_start);
    (*sem).object_end = Some(transform_string_values_object_end);
    (*sem).array_start = Some(transform_string_values_array_start);
    (*sem).array_end = Some(transform_string_values_array_end);
    (*sem).scalar = Some(transform_string_values_scalar);
    (*sem).array_element_start = Some(transform_string_values_array_element_start);
    (*sem).object_field_start = Some(transform_string_values_object_field_start);

    pg_parse_json_or_ereport(&mut lex, sem);
    freeJsonLexContext(&mut lex);

    cstring_to_text_with_len(si_data((*state).strval), si_len((*state).strval)) as *mut c_void
}

pub unsafe extern "C" fn transform_string_values_object_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    appendStringInfoChar_si((*_state).strval, b'{' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn transform_string_values_object_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    appendStringInfoChar_si((*_state).strval, b'}' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn transform_string_values_array_start(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    appendStringInfoChar_si((*_state).strval, b'[' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn transform_string_values_array_end(state: *mut c_void) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    appendStringInfoChar_si((*_state).strval, b']' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn transform_string_values_object_field_start(
    state: *mut c_void,
    fname: *mut c_char,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    if *si_data((*_state).strval).offset((si_len((*_state).strval) - 1) as isize) != b'{' as c_char {
        appendStringInfoChar_si((*_state).strval, b',' as c_char);
    }

    /*
     * Unfortunately we don't have the quoted and escaped string any more, so
     * we have to re-escape it.
     */
    escape_json((*_state).strval, fname);
    appendStringInfoChar_si((*_state).strval, b':' as c_char);

    JSON_SUCCESS
}

pub unsafe extern "C" fn transform_string_values_array_element_start(
    state: *mut c_void,
    isnull: bool,
) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    if *si_data((*_state).strval).offset((si_len((*_state).strval) - 1) as isize) != b'[' as c_char {
        appendStringInfoChar_si((*_state).strval, b',' as c_char);
    }

    JSON_SUCCESS
}

pub unsafe extern "C" fn transform_string_values_scalar(
    state: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state = state as *mut TransformJsonStringValuesState;

    if tokentype == JSON_TOKEN_STRING {
        let out: *mut text = ((*_state).action)((*_state).action_state, token, libc_strlen(token) as c_int);

        escape_json_text((*_state).strval, out as *const crate::c::text);
    } else {
        appendStringInfoString_si((*_state).strval, token);
    }

    JSON_SUCCESS
}

pub unsafe fn json_get_first_token(json: *mut text, throw_error: bool) -> JsonTokenType {
    let mut lex: JsonLexContext = core::mem::zeroed();
    let result: JsonParseErrorType;

    makeJsonLexContext(&mut lex, json, false);

    /* Lex exactly one token from the input and check its type. */
    result = json_lex(&mut lex);

    if result == JSON_SUCCESS {
        return lex.token_type;
    }

    if throw_error {
        json_errsave_error(result, &mut lex, std::ptr::null_mut());
    }

    JSON_TOKEN_INVALID /* invalid json */
}

/*
 * Determine how we want to print values of a given type in datum_to_json(b).
 */
pub unsafe fn json_categorize_type(
    mut typoid: Oid,
    is_jsonb: bool,
    tcategory: *mut JsonTypeCategory,
    outfuncoid: *mut Oid,
) {
    let mut typisvarlena: bool = false;

    /* Look through any domain */
    typoid = getBaseType(typoid);

    *outfuncoid = InvalidOid;

    match typoid {
        BOOLOID => {
            *outfuncoid = F_BOOLOUT as Oid;
            *tcategory = JSONTYPE_BOOL;
        }

        INT2OID | INT4OID | INT8OID | FLOAT4OID | FLOAT8OID | NUMERICOID => {
            getTypeOutputInfo(typoid, outfuncoid, &mut typisvarlena);
            *tcategory = JSONTYPE_NUMERIC;
        }

        DATEOID => {
            *outfuncoid = F_DATE_OUT as Oid;
            *tcategory = JSONTYPE_DATE;
        }

        TIMESTAMPOID => {
            *outfuncoid = F_TIMESTAMP_OUT as Oid;
            *tcategory = JSONTYPE_TIMESTAMP;
        }

        TIMESTAMPTZOID => {
            *outfuncoid = F_TIMESTAMPTZ_OUT as Oid;
            *tcategory = JSONTYPE_TIMESTAMPTZ;
        }

        JSONOID => {
            getTypeOutputInfo(typoid, outfuncoid, &mut typisvarlena);
            *tcategory = JSONTYPE_JSON;
        }

        JSONBOID => {
            getTypeOutputInfo(typoid, outfuncoid, &mut typisvarlena);
            *tcategory = if is_jsonb { JSONTYPE_JSONB } else { JSONTYPE_JSON };
        }

        _ => {
            /* Check for arrays and composites */
            if OidIsValid(get_element_type(typoid))
                || typoid == ANYARRAYOID
                || typoid == ANYCOMPATIBLEARRAYOID
                || typoid == RECORDARRAYOID
            {
                *outfuncoid = F_ARRAY_OUT as Oid;
                *tcategory = JSONTYPE_ARRAY;
            } else if type_is_rowtype(typoid) {
                /* includes RECORDOID */
                *outfuncoid = F_RECORD_OUT as Oid;
                *tcategory = JSONTYPE_COMPOSITE;
            } else {
                /*
                 * It's probably the general case.  But let's look for a cast
                 * to json (note: not to jsonb even if is_jsonb is true), if
                 * it's not built-in.
                 */
                *tcategory = JSONTYPE_OTHER;
                if typoid >= FirstNormalObjectId {
                    let mut castfunc: Oid = InvalidOid;
                    let ctype: CoercionPathType;

                    ctype = find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT, &mut castfunc);
                    if ctype == COERCION_PATH_FUNC && OidIsValid(castfunc) {
                        *outfuncoid = castfunc;
                        *tcategory = JSONTYPE_CAST;
                    } else {
                        /* non builtin type with no cast */
                        getTypeOutputInfo(typoid, outfuncoid, &mut typisvarlena);
                    }
                } else {
                    /* any other builtin type */
                    getTypeOutputInfo(typoid, outfuncoid, &mut typisvarlena);
                }
            }
        }
    }
}

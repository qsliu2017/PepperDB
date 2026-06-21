/*-------------------------------------------------------------------------
 *
 * execExprInterp.rs
 *   Interpreted evaluation of an expression step list.
 *
 * This file provides a "switch threaded" implementation of expression
 * evaluation (the C file supports both computed-goto and switch-thread
 * dispatch; we translate only the switch-threaded path since Rust has no
 * computed-goto).
 *
 * In the switch-threaded implementation we use a plain loop { match opcode }
 * to dispatch, advancing `op` after each step (EEO_NEXT) or jumping to an
 * absolute step index (EEO_JUMP).
 *
 * Complex or uncommon instructions are implemented as out-of-line helper
 * functions at the bottom of this file, mirroring the C layout.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/backend/executor/execExprInterp.c -> src/executor/execExprInterp.rs
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case, non_upper_case_globals, unused_variables, dead_code)]
use crate::prelude::*;

use crate::executor::execExpr::{
    ExprEvalOp, ExprEvalOp::*, ExprEvalRowtypeCache, ExprEvalStep,
    ScalarArrayOpExprHashTable, SubscriptingRefState,
};
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{
    AggState, AggStatePerGroup, AggStatePerGroupData, AggStatePerTrans, AggStatePerTransData,
    ExprContext, ExprState, JsonExprState, WindowFuncExprState,
};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::primnodes::{
    JsonIsPredicate, MinMaxOp, SQLValueFunction, VarReturningType,
};
use crate::postgres::NullableDatum;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};

/* Flag bits imported from execExpr.rs */
use crate::executor::execExpr::{EEO_FLAG_INTERPRETER_INITIALIZED, EEO_FLAG_DIRECT_THREADED};
use crate::nodes::execnodes::{
    EEO_FLAG_HAS_OLD, EEO_FLAG_HAS_NEW, EEO_FLAG_IS_QUAL,
};
/* castNode macro is #[macro_export] at crate root - import before first use */
use crate::castNode;
/* IsA macro */
use crate::IsA;
/* HeapTuple type */
use crate::access::htup_details::HeapTuple;
/* SelfItemPointerAttributeNumber */
use crate::access::sysattr::SelfItemPointerAttributeNumber;
/* VARATT_IS_EXTERNAL_EXPANDED */
use crate::varatt::VARATT_IS_EXTERNAL_EXPANDED;

/* TODO(pg-port): pgstat types -- use opaque stubs until pgstat is ported */
#[repr(C)]
pub struct PgStat_FunctionCallUsage {
    _opaque: [u8; 0],
}

/* TODO(pg-port): commands/sequence.h */
unsafe fn nextval_internal(_seqid: Oid, _check: bool) -> i64 {
    unimplemented!("TODO(pg-port): commands::sequence::nextval_internal")
}

/* TODO(pg-port): pgstat.h */
unsafe fn pgstat_init_function_usage(
    _fcinfo: FunctionCallInfo,
    _fusage: *mut PgStat_FunctionCallUsage,
) {
    crate::utils::activity::pgstat_function::pgstat_init_function_usage(_fcinfo as _, _fusage as _)
}
unsafe fn pgstat_end_function_usage(_fusage: *mut PgStat_FunctionCallUsage, _finalize: bool) {
    crate::utils::activity::pgstat_function::pgstat_end_function_usage(_fusage as _, _finalize as _)
}

/* TODO(pg-port): utils/expandedrecord.h */
#[repr(C)]
pub struct ExpandedRecordHeader {
    pub er_magic: c_int,
    _opaque: [u8; 0],
}
const ER_MAGIC: c_int = 0x2B1EF6B1u32 as c_int;
unsafe fn expanded_record_get_tupdesc(_erh: *mut ExpandedRecordHeader) -> crate::access::common::tupdesc::TupleDesc {
    unimplemented!("TODO(pg-port): expandedrecord::expanded_record_get_tupdesc")
}
unsafe fn expanded_record_get_field(
    _erh: *mut ExpandedRecordHeader,
    _fieldnum: crate::access::attnum::AttrNumber,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!("TODO(pg-port): expandedrecord::expanded_record_get_field")
}

/* TODO(pg-port): utils/expandeddatum.h */
unsafe fn MakeExpandedObjectReadOnlyInternal(_d: Datum) -> Datum {
    _d /* stub: return as-is */
}
unsafe fn DatumIsReadWriteExpandedObject(_d: Datum, _isnull: bool, _typlen: i16) -> bool {
    false
}
unsafe fn DatumGetEOHP(_d: Datum) -> *mut ExpandedRecordHeader {
    crate::utils::adt::expandeddatum::DatumGetEOHP(_d as _) as _
}
unsafe fn DeleteExpandedObject(_d: Datum) {
    crate::utils::adt::expandeddatum::DeleteExpandedObject(_d as _)
}

/* TODO(pg-port): utils/datum.h */
unsafe fn datumCopy(value: Datum, typbyval: bool, typlen: i16) -> Datum {
    crate::utils::adt::datum::datumCopy(value, typbyval, typlen as c_int)
}

/* TODO(pg-port): access/tuptoast.h */
unsafe fn toast_build_flattened_tuple(
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> *mut crate::access::htup_details::HeapTupleData {
    crate::access::heap::heaptoast::toast_build_flattened_tuple(_tupdesc as _, _values as _, _isnull as _) as _
}

/* TODO(pg-port): access/heapam.h heap_* functions */
unsafe fn heap_attisnull(
    _tup: *mut crate::access::htup_details::HeapTupleData,
    _attnum: c_int,
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
) -> bool {
    crate::access::common::heaptuple::heap_attisnull(_tup as _, _attnum as _, _tupdesc as _) as _
}
unsafe fn heap_getattr(
    _tup: *mut crate::access::htup_details::HeapTupleData,
    _attnum: crate::access::attnum::AttrNumber,
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!("TODO(pg-port): access::heapam::heap_getattr")
}
unsafe fn heap_deform_tuple(
    _tup: *mut crate::access::htup_details::HeapTupleData,
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    crate::access::common::heaptuple::heap_deform_tuple(_tup as _, _tupdesc as _, _values as _, _isnull as _)
}
unsafe fn heap_form_tuple(
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> *mut crate::access::htup_details::HeapTupleData {
    unimplemented!("TODO(pg-port): access::heapam::heap_form_tuple")
}

/* TODO(pg-port): access/heapam.h HeapTupleGetDatum etc */
unsafe fn HeapTupleGetDatum(
    _tup: *mut crate::access::htup_details::HeapTupleData,
) -> Datum {
    unimplemented!("TODO(pg-port): HeapTupleGetDatum")
}
unsafe fn heap_copy_tuple_as_datum(
    _tup: *mut crate::access::htup_details::HeapTupleData,
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
) -> Datum {
    crate::access::common::heaptuple::heap_copy_tuple_as_datum(_tup as _, _tupdesc as _) as _
}

/* TODO(pg-port): access/tupconvert.h */
unsafe fn execute_attr_map_tuple(
    _tup: *mut crate::access::htup_details::HeapTupleData,
    _map: *mut crate::access::common::tupconvert::TupleConversionMap,
) -> *mut crate::access::htup_details::HeapTupleData {
    crate::access::common::tupconvert::execute_attr_map_tuple(_tup as _, _map as _) as _
}
unsafe fn convert_tuples_by_name(
    _indesc: crate::access::common::tupdesc::TupleDesc,
    _outdesc: crate::access::common::tupdesc::TupleDesc,
) -> *mut crate::access::common::tupconvert::TupleConversionMap {
    crate::access::common::tupconvert::convert_tuples_by_name(_indesc as _, _outdesc as _) as _
}

/* TODO(pg-port): access/tupdesc.h ref-count */
unsafe fn IncrTupleDescRefCount(_tupdesc: crate::access::common::tupdesc::TupleDesc) {
    crate::access::common::tupdesc::IncrTupleDescRefCount(_tupdesc as _)
}
unsafe fn DecrTupleDescRefCount(_tupdesc: crate::access::common::tupdesc::TupleDesc) {
    crate::access::common::tupdesc::DecrTupleDescRefCount(_tupdesc as _)
}
unsafe fn CreateTupleDescCopy(
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
) -> crate::access::common::tupdesc::TupleDesc {
    crate::access::common::tupdesc::CreateTupleDescCopy(_tupdesc as _) as _
}
unsafe fn ReleaseTupleDesc(_tupdesc: crate::access::common::tupdesc::TupleDesc) {
    crate::access::common::tupdesc::ReleaseTupleDesc(_tupdesc as _)
}
unsafe fn BlessTupleDesc(
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
) -> crate::access::common::tupdesc::TupleDesc {
    crate::executor::execTuples::BlessTupleDesc(_tupdesc as _) as _
}

/* TODO(pg-port): utils/typcache.h */
use crate::utils::cache::typcache::{DomainConstraintRef, TypeCacheEntry};
const TYPECACHE_TUPDESC: c_int = 0x200;
unsafe fn lookup_type_cache(
    _typid: Oid,
    _flags: c_int,
) -> *mut TypeCacheEntry {
    unimplemented!("TODO(pg-port): utils::typcache::lookup_type_cache")
}
unsafe fn lookup_rowtype_tupdesc(
    _typid: Oid,
    _typmod: i32,
) -> crate::access::common::tupdesc::TupleDesc {
    crate::utils::cache::typcache::lookup_rowtype_tupdesc(_typid as _, _typmod as _) as _
}
unsafe fn lookup_rowtype_tupdesc_domain(
    _typid: Oid,
    _typmod: i32,
    _noerror: bool,
) -> crate::access::common::tupdesc::TupleDesc {
    crate::utils::cache::typcache::lookup_rowtype_tupdesc_domain(_typid as _, _typmod as _, _noerror as _) as _
}
unsafe fn domain_check_safe(
    _value: Datum,
    _isnull: bool,
    _domaintype: Oid,
    _typcache: *mut *mut c_void,
    _mcxt: MemoryContext,
    _escontext: *mut crate::nodes::nodes::Node,
) -> bool {
    unimplemented!("TODO(pg-port): utils::typcache::domain_check_safe")
}

/* TODO(pg-port): utils/lsyscache.h */
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut i16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) {
    crate::utils::cache::lsyscache::get_typlenbyvalalign(_typid as _, _typlen as _, _typbyval as _, _typalign as _)
}
unsafe fn format_type_be(_typid: Oid) -> *mut c_char {
    unimplemented!("TODO(pg-port): utils::lsyscache::format_type_be")
}

/* TODO(pg-port): utils/array.h / arrayfuncs.c */
unsafe fn construct_md_array(
    _dvalues: *mut Datum,
    _dnulls: *mut bool,
    _ndims: c_int,
    _dims: *mut c_int,
    _lbs: *mut c_int,
    _element_type: Oid,
    _elemlength: i16,
    _elembyval: bool,
    _elemalign: c_char,
) -> *mut ArrayType {
    unimplemented!("TODO(pg-port): utils::array::construct_md_array")
}
unsafe fn construct_empty_array(_element_type: Oid) -> *mut ArrayType {
    unimplemented!("TODO(pg-port): utils::array::construct_empty_array")
}
unsafe fn ArrayGetNItems(_ndim: c_int, _dims: *const c_int) -> c_int {
    crate::utils::adt::arrayutils::ArrayGetNItems(_ndim as _, _dims as _) as _
}
unsafe fn ArrayCheckBounds(_ndim: c_int, _dims: *const c_int, _lbs: *const c_int) {
    crate::utils::adt::arrayutils::ArrayCheckBounds(_ndim as _, _dims as _, _lbs as _)
}
unsafe fn array_bitmap_copy(
    _destbitmap: *mut u8,
    _destoffset: c_int,
    _srcbitmap: *const u8,
    _srcoffset: c_int,
    _nitems: c_int,
) {
    crate::utils::adt::arrayfuncs::array_bitmap_copy(_destbitmap as _, _destoffset as _, _srcbitmap as _, _srcoffset as _, _nitems as _)
}
unsafe fn array_map(
    _arraydatum: Datum,
    _elemexprstate: *mut ExprState,
    _econtext: *mut ExprContext,
    _resultelemtype: Oid,
    _amstate: *mut crate::utils::adt::arrayfuncs::ArrayMapState,
) -> Datum {
    crate::utils::adt::arrayfuncs::array_map(_arraydatum as _, _elemexprstate as _, _econtext as _, _resultelemtype as _, _amstate as _) as _
}
unsafe fn DatumGetArrayTypePCopy(_d: Datum) -> *mut ArrayType {
    unimplemented!("TODO(pg-port): DatumGetArrayTypePCopy")
}

/* TODO(pg-port): array macros / type */
#[repr(C)]
pub struct ArrayType {
    pub vl_len_: i32,
    pub ndim: c_int,
    pub dataoffset: i32,
    pub elemtype: Oid,
}
unsafe fn ARR_NDIM(a: *mut ArrayType) -> c_int { (*a).ndim }
unsafe fn ARR_ELEMTYPE(a: *mut ArrayType) -> Oid { (*a).elemtype }
unsafe fn ARR_DIMS(a: *mut ArrayType) -> *mut c_int {
    (a as *mut u8).add(core::mem::size_of::<ArrayType>()) as *mut c_int
}
unsafe fn ARR_LBOUND(a: *mut ArrayType) -> *mut c_int {
    ARR_DIMS(a).add(ARR_NDIM(a) as usize)
}
unsafe fn ARR_OVERHEAD_WITHNULLS(ndims: c_int, nitems: c_int) -> i32 {
    crate::utils::array::ARR_OVERHEAD_WITHNULLS(ndims as _, nitems as _) as _
}
unsafe fn ARR_OVERHEAD_NONULLS(ndims: c_int) -> i32 {
    crate::utils::array::ARR_OVERHEAD_NONULLS(ndims as _) as _
}
unsafe fn ARR_DATA_PTR(a: *mut ArrayType) -> *mut c_char {
    crate::utils::array::ARR_DATA_PTR(a as _) as _
}
unsafe fn ARR_NULLBITMAP(a: *mut ArrayType) -> *mut u8 {
    crate::utils::array::ARR_NULLBITMAP(a as _) as _
}
unsafe fn ARR_HASNULL(a: *mut ArrayType) -> bool {
    crate::utils::array::ARR_HASNULL(a as _) as _
}
unsafe fn ARR_SIZE(a: *mut ArrayType) -> usize {
    crate::utils::array::ARR_SIZE(a as _) as _
}
unsafe fn ARR_DATA_OFFSET(a: *mut ArrayType) -> usize {
    crate::utils::array::ARR_DATA_OFFSET(a as _) as _
}
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!("TODO(pg-port): DatumGetArrayTypeP")
}
unsafe fn SET_VARSIZE(a: *mut ArrayType, _sz: usize) {
    crate::varatt::SET_VARSIZE(a as _, _sz as _)
}
const MAXDIM: usize = 6;
type bits8 = u8;
unsafe fn MaxAllocSize() -> usize { 0x3fffffff }
unsafe fn AllocSizeIsValid(sz: usize) -> bool { sz <= MaxAllocSize() }

/* TODO(pg-port): utils/memutils.h */
unsafe fn palloc(size: usize) -> *mut c_void {
    unimplemented!("TODO(pg-port): palloc")
}
unsafe fn palloc0(size: usize) -> *mut c_void {
    unimplemented!("TODO(pg-port): palloc0")
}
unsafe fn pfree(_ptr: *mut c_void) {}
unsafe fn pstrdup(s: *const c_char) -> *mut c_char {
    unimplemented!("TODO(pg-port): pstrdup")
}

/* TODO(pg-port): MemoryContext switch / current */
unsafe fn MemoryContextSwitchTo(cxt: MemoryContext) -> MemoryContext {
    crate::utils::mmgr::mcxt::MemoryContextSwitchTo(cxt as _) as _
}
unsafe fn CurrentMemoryContext() -> MemoryContext {
    crate::utils::palloc::CurrentMemoryContext as _
}
unsafe fn MemoryContextGetParent(_cxt: MemoryContext) -> MemoryContext {
    crate::utils::mmgr::mcxt::MemoryContextGetParent(_cxt as _) as _
}

/* TODO(pg-port): utils/fmgr.h FunctionCallInvoke / InitFunctionCallInfoData */
unsafe fn FunctionCallInvoke(fcinfo: FunctionCallInfo) -> Datum {
    crate::FunctionCallInvoke!(fcinfo)
}
unsafe fn InitFunctionCallInfoData(
    fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
    finfo: *mut FmgrInfo,
    nargs: c_int,
    collation: Oid,
    context: *mut crate::nodes::nodes::Node,
    resultinfo: *mut crate::nodes::nodes::Node,
) {
    crate::InitFunctionCallInfoData!(fcinfo, finfo, nargs as i16, collation, context, resultinfo);
}
unsafe fn fmgr_info(_funcid: Oid, _finfo: *mut FmgrInfo) {
    crate::utils::fmgr::fmgr_info(_funcid as _, _finfo as _)
}
unsafe fn fmgr_info_set_expr(_expr: *mut crate::nodes::nodes::Node, _finfo: *mut FmgrInfo) {
    unimplemented!("TODO(pg-port): fmgr_info_set_expr")
}
unsafe fn SizeForFunctionCallInfo(_nargs: c_int) -> usize {
    unimplemented!("TODO(pg-port): SizeForFunctionCallInfo")
}
unsafe fn FunctionCall2Coll(
    _finfo: *mut FmgrInfo,
    _collation: Oid,
    _arg1: Datum,
    _arg2: Datum,
) -> Datum {
    crate::utils::fmgr::FunctionCall2Coll(_finfo as _, _collation as _, _arg1 as _, _arg2 as _) as _
}

/* TODO(pg-port): utils/builtins.h / utils/date.h / utils/timestamp.h */
type DateADT = i32;
type TimeTzADT = [u8; 0]; /* opaque */
type TimeADT = i64;
type Timestamp = i64;
type TimestampTz = i64;
unsafe fn GetSQLCurrentDate() -> DateADT {
    crate::utils::adt::date::GetSQLCurrentDate() as _
}
unsafe fn GetSQLCurrentTime(_typmod: i32) -> *mut TimeTzADT {
    crate::utils::adt::date::GetSQLCurrentTime(_typmod as _) as _
}
unsafe fn GetSQLCurrentTimestamp(_typmod: i32) -> TimestampTz {
    crate::utils::adt::timestamp::GetSQLCurrentTimestamp(_typmod as _) as _
}
unsafe fn GetSQLLocalTime(_typmod: i32) -> TimeADT {
    crate::utils::adt::date::GetSQLLocalTime(_typmod as _) as _
}
unsafe fn GetSQLLocalTimestamp(_typmod: i32) -> Timestamp {
    crate::utils::adt::timestamp::GetSQLLocalTimestamp(_typmod as _) as _
}
unsafe fn DateADTGetDatum(_d: DateADT) -> Datum {
    crate::utils::adt::date::DateADTGetDatum(_d as _) as _
}
unsafe fn TimeTzADTPGetDatum(_d: *mut TimeTzADT) -> Datum {
    crate::utils::adt::date::TimeTzADTPGetDatum(_d as _) as _
}
unsafe fn TimestampTzGetDatum(_d: TimestampTz) -> Datum { unimplemented!("TODO(pg-port)") }
unsafe fn TimeADTGetDatum(_d: TimeADT) -> Datum {
    crate::utils::adt::date::TimeADTGetDatum(_d as _) as _
}
unsafe fn TimestampGetDatum(_d: Timestamp) -> Datum { unimplemented!("TODO(pg-port)") }
unsafe fn current_user(
    _fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
) -> Datum {
    crate::utils::adt::name::current_user(_fcinfo as _) as _
}
unsafe fn session_user(
    _fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
) -> Datum {
    crate::utils::adt::name::session_user(_fcinfo as _) as _
}
unsafe fn current_database(
    _fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
) -> Datum {
    crate::utils::adt::misc::current_database(_fcinfo as _) as _
}
unsafe fn current_schema(
    _fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
) -> Datum {
    crate::utils::adt::name::current_schema(_fcinfo as _) as _
}

/* TODO(pg-port): executor/nodeSubplan.h */
use crate::nodes::execnodes::SubPlanState;
unsafe fn ExecSubPlan(
    _sstate: *mut SubPlanState,
    _econtext: *mut ExprContext,
    _isnull: *mut bool,
) -> Datum {
    crate::executor::nodeSubplan::ExecSubPlan(_sstate as _, _econtext as _, _isnull as _) as _
}
unsafe fn ExecSetParamPlan(
    _execplan: *mut c_void,
    _econtext: *mut ExprContext,
) {
    crate::executor::nodeSubplan::ExecSetParamPlan(_execplan as _, _econtext as _)
}

/* TODO(pg-port): executor/executor.h */
unsafe fn ExecFilterJunk(
    _jf: *mut crate::nodes::execnodes::JunkFilter,
    _slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    crate::executor::execJunk::ExecFilterJunk(_jf as _, _slot as _) as _
}

/* TODO(pg-port): executor/tuptable.h slot_* */
unsafe fn slot_getsomeattrs(_slot: *mut TupleTableSlot, _attnum: c_int) {
    crate::executor::tuptable::slot_getsomeattrs(_slot as _, _attnum as _)
}
unsafe fn slot_getattr(
    _slot: *mut TupleTableSlot,
    _attnum: crate::access::attnum::AttrNumber,
    _isnull: *mut bool,
) -> Datum {
    crate::executor::tuptable::slot_getattr(_slot as _, _attnum as _, _isnull as _) as _
}
unsafe fn slot_getsysattr(
    _slot: *mut TupleTableSlot,
    _attnum: c_int,
    _isnull: *mut bool,
) -> Datum {
    crate::executor::tuptable::slot_getsysattr(_slot as _, _attnum as _, _isnull as _) as _
}
unsafe fn slot_getallattrs(_slot: *mut TupleTableSlot) {
    crate::executor::tuptable::slot_getallattrs(_slot as _)
}
unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) {
    unimplemented!()
}
unsafe fn ExecStoreVirtualTuple(_slot: *mut TupleTableSlot) {
    unimplemented!()
}
/* TODO(pg-port): executor/execTuples.h */
unsafe fn ExecMaterializeSlot(_slot: *mut TupleTableSlot) {
    crate::executor::tuptable::ExecMaterializeSlot(_slot as _)
}
unsafe fn ExecCopySlotMinimalTuple(_slot: *mut TupleTableSlot) -> *mut crate::access::htup_details::MinimalTupleData {
    crate::executor::tuptable::ExecCopySlotMinimalTuple(_slot as _) as _
}
/* TODO(pg-port): utils/expandedrecord.h */
unsafe fn make_expanded_record_from_tuple(
    _tup: *mut HeapTuple,
    _typid: Oid,
    _typmod: i32,
    _mcxt: MemoryContext,
) -> Datum {
    unimplemented!("TODO(pg-port): make_expanded_record_from_tuple")
}
/* TODO(pg-port): executor/execUtils.h build_virtual_tuple */
unsafe fn build_virtual_tuple(
    _slot: *mut TupleTableSlot,
    _tupdesc: crate::access::common::tupdesc::TupleDesc,
) -> *mut crate::access::htup_details::HeapTupleData {
    unimplemented!("TODO(pg-port): build_virtual_tuple")
}
/* TODO(pg-port): utils/array.h CStringGetTextDatum */
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    crate::utils::builtins::CStringGetTextDatum(_s as _) as _
}
/* TODO(pg-port): none_fn placeholder for null function pointer */
unsafe fn none_fn(_fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData) -> Datum {
    unimplemented!("TODO(pg-port): none_fn should never be called")
}
unsafe fn ExecCopySlot(
    _dstslot: *mut TupleTableSlot,
    _srcslot: *mut TupleTableSlot,
) {
    unimplemented!()
}
unsafe fn ExecQual(
    _state: *mut ExprState,
    _econtext: *mut ExprContext,
) -> bool {
    crate::executor::executor::ExecQual(_state as _, _econtext as _) as _
}

/* TODO(pg-port): utils/xml.h */
type xmltype = [u8; 0]; /* opaque */
unsafe fn xmlconcat(_vals: *mut crate::nodes::pg_list::List) -> *mut xmltype {
    crate::utils::adt::xml::xmlconcat(_vals as _) as _
}
unsafe fn xmlelement(
    _xexpr: *mut crate::nodes::primnodes::XmlExpr,
    _named_argvalue: *mut Datum,
    _named_argnull: *mut bool,
    _argvalue: *mut Datum,
    _argnull: *mut bool,
) -> *mut xmltype {
    crate::utils::adt::xml::xmlelement(_xexpr as _, _named_argvalue as _, _named_argnull as _, _argvalue as _, _argnull as _) as _
}
unsafe fn xmlparse(
    _data: *mut text,
    _xmloption: c_int,
    _preserve_whitespace: bool,
) -> *mut xmltype {
    unimplemented!()
}
unsafe fn xmlpi(
    _name: *const c_char,
    _arg: *mut text,
    _argisnull: bool,
    _resnull: *mut bool,
) -> *mut xmltype {
    crate::utils::adt::xml::xmlpi(_name as _, _arg as _, _argisnull as _, _resnull as _) as _
}
unsafe fn xmlroot(
    _data: *mut xmltype,
    _version: *mut text,
    _standalone: c_int,
) -> *mut xmltype {
    crate::utils::adt::xml::xmlroot(_data as _, _version as _, _standalone as _) as _
}
unsafe fn xmltotext_with_options(
    _data: *mut xmltype,
    _xmloption: c_int,
    _indent: bool,
) -> *mut text {
    unimplemented!()
}
unsafe fn xml_is_document(_arg: *mut xmltype) -> bool {
    crate::utils::adt::xml::xml_is_document(_arg as _) as _
}
unsafe fn map_sql_value_to_xml_value(
    _value: Datum,
    _typid: Oid,
    _xml_escape_strings: bool,
) -> *const c_char {
    crate::utils::adt::xml::map_sql_value_to_xml_value(_value as _, _typid as _, _xml_escape_strings as _) as _
}
unsafe fn DatumGetXmlP(_d: Datum) -> *mut xmltype {
    unimplemented!("TODO(pg-port)")
}
type text = [u8; 0]; /* opaque */
unsafe fn cstring_to_text_with_len(_s: *const c_char, _len: usize) -> *mut text {
    unimplemented!("TODO(pg-port)")
}
unsafe fn DatumGetTextP(_d: Datum) -> *mut text {
    unimplemented!("TODO(pg-port)")
}
unsafe fn DatumGetTextPP(_d: Datum) -> *mut text {
    unimplemented!("TODO(pg-port)")
}

/* TODO(pg-port): StringInfo */
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: usize,
    pub maxlen: usize,
    pub cursor: usize,
}
unsafe fn initStringInfo(_buf: *mut StringInfoData) {
    crate::lib::stringinfo::initStringInfo(_buf as _)
}
unsafe fn appendStringInfo(_buf: *mut StringInfoData, _fmt: *const c_char) {
    unimplemented!("TODO(pg-port)")
}

/* TODO(pg-port): utils/json.h / jsonfuncs.h */
#[repr(C)]
pub struct JsonbValue {
    pub r#type: c_int, /* jbvNull=0, jbvString=1, jbvNumeric=2, jbvBool=3, jbvArray=4, jbvObject=5, jbvBinary=6, jbvDatetime=7 */
    pub val: JsonbValue_val,
}
#[repr(C)]
pub union JsonbValue_val {
    pub string: JsonbValue_string,
    pub numeric: *mut c_void,
    pub boolean: bool,
    pub datetime: JsonbValue_datetime,
    /* array/object/binary skipped */
    _pad: [u8; 32],
}
#[repr(C)]
#[derive(Copy, Clone)]
pub struct JsonbValue_string {
    pub val: *const c_char,
    pub len: c_int,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub struct JsonbValue_datetime {
    pub value: Datum,
    pub typid: Oid,
    pub typmod: i32,
    pub tz: i32,
}
/* jbvXxx constants */
const jbvNull: c_int = 0;
const jbvString: c_int = 1;
const jbvNumeric: c_int = 2;
const jbvBool: c_int = 3;
const jbvArray: c_int = 4;
const jbvObject: c_int = 5;
const jbvBinary: c_int = 6;
const jbvDatetime: c_int = 7;

#[repr(C)]
pub struct Jsonb {
    _opaque: [u8; 0],
}
unsafe fn JB_ROOT_IS_OBJECT(_jb: *mut Jsonb) -> bool { false }
unsafe fn JB_ROOT_IS_ARRAY(_jb: *mut Jsonb) -> bool { false }
unsafe fn JB_ROOT_IS_SCALAR(_jb: *mut Jsonb) -> bool { false }
unsafe fn DatumGetJsonbP(_d: Datum) -> *mut Jsonb {
    unimplemented!("TODO(pg-port)")
}
unsafe fn JsonbPGetDatum(_jb: *mut Jsonb) -> Datum {
    unimplemented!("TODO(pg-port)")
}
unsafe fn JsonbValueToJsonb(_val: *mut JsonbValue) -> *mut Jsonb {
    crate::utils::adt::jsonb_util::JsonbValueToJsonb(_val as _) as _
}
unsafe fn jsonb_out(_arg: Datum) -> Datum {
    unimplemented!("TODO(pg-port)")
}
unsafe fn jsonb_in(_arg: Datum) -> Datum {
    unimplemented!("TODO(pg-port)")
}
unsafe fn jsonb_from_text(_js: *mut text, _unique: bool) -> Datum {
    unimplemented!("TODO(pg-port)")
}
unsafe fn json_validate(_js: *mut text, _unique: bool, _throw: bool) -> bool {
    crate::utils::adt::json::json_validate(_js as _, _unique as _, _throw as _) as _
}
unsafe fn datum_to_jsonb(_value: Datum, _category: c_int, _outfuncid: Oid) -> Datum {
    unimplemented!("TODO(pg-port)")
}
unsafe fn datum_to_json(_value: Datum, _category: c_int, _outfuncid: Oid) -> Datum {
    unimplemented!()
}
unsafe fn json_build_array_worker(
    _nargs: c_int, _arg_values: *mut Datum, _arg_nulls: *mut bool,
    _arg_types: *mut Oid, _absent_on_null: bool,
) -> Datum {
    crate::utils::adt::json::json_build_array_worker(_nargs as _, _arg_values as _, _arg_nulls as _, _arg_types as _, _absent_on_null as _) as _
}
unsafe fn jsonb_build_array_worker(
    _nargs: c_int, _arg_values: *mut Datum, _arg_nulls: *mut bool,
    _arg_types: *mut Oid, _absent_on_null: bool,
) -> Datum { unimplemented!("TODO(pg-port)") }
unsafe fn json_build_object_worker(
    _nargs: c_int, _arg_values: *mut Datum, _arg_nulls: *mut bool,
    _arg_types: *mut Oid, _absent_on_null: bool, _unique: bool,
) -> Datum {
    crate::utils::adt::json::json_build_object_worker(_nargs as _, _arg_values as _, _arg_nulls as _, _arg_types as _, _absent_on_null as _, _unique as _) as _
}
unsafe fn jsonb_build_object_worker(
    _nargs: c_int, _arg_values: *mut Datum, _arg_nulls: *mut bool,
    _arg_types: *mut Oid, _absent_on_null: bool, _unique: bool,
) -> Datum { unimplemented!("TODO(pg-port)") }
/* json_get_first_token tokens */
const JSON_TOKEN_OBJECT_START: c_int = 1;
const JSON_TOKEN_ARRAY_START: c_int = 2;
const JSON_TOKEN_STRING: c_int = 3;
const JSON_TOKEN_NUMBER: c_int = 4;
const JSON_TOKEN_TRUE: c_int = 5;
const JSON_TOKEN_FALSE: c_int = 6;
const JSON_TOKEN_NULL: c_int = 7;
unsafe fn json_get_first_token(_js: *mut text, _throw: bool) -> c_int {
    crate::utils::adt::jsonfuncs::json_get_first_token(_js as _, _throw as _) as _
}

/* TODO(pg-port): utils/jsonpath.h */
#[repr(C)]
pub struct JsonPath {
    _opaque: [u8; 0],
}
unsafe fn DatumGetJsonPathP(_d: Datum) -> *mut JsonPath {
    unimplemented!("TODO(pg-port)")
}
unsafe fn JsonPathExists(
    _item: Datum, _path: *mut JsonPath, _error: *mut bool, _args: *mut c_void,
) -> bool {
    crate::utils::adt::jsonpath_exec::JsonPathExists(_item as _, _path as _, _error as _, _args as _) as _
}
unsafe fn JsonPathQuery(
    _item: Datum, _path: *mut JsonPath, _wrapper: c_int,
    _empty: *mut bool, _error: *mut bool, _args: *mut c_void,
    _column_name: *const c_char,
) -> Datum {
    unimplemented!()
}
unsafe fn JsonPathValue(
    _item: Datum, _path: *mut JsonPath, _empty: *mut bool,
    _error: *mut bool, _args: *mut c_void, _column_name: *const c_char,
) -> *mut JsonbValue {
    crate::utils::adt::jsonpath_exec::JsonPathValue(_item as _, _path as _, _empty as _, _error as _, _args as _, _column_name as _) as _
}
unsafe fn json_populate_type(
    _jb: Datum, _jb_typid: Oid, _typid: Oid, _typmod: i32,
    _cache: *mut *mut c_void, _mcxt: MemoryContext,
    _isnull: *mut bool, _omit_quotes: bool,
    _escontext: *mut crate::nodes::nodes::Node,
) -> Datum {
    crate::utils::adt::jsonfuncs::json_populate_type(_jb as _, _jb_typid as _, _typid as _, _typmod as _, _cache as _, _mcxt as _, _isnull as _, _omit_quotes as _, _escontext as _) as _
}

/* TODO(pg-port): DirectFunctionCall1 etc */
unsafe fn DirectFunctionCall1(
    _func: unsafe fn(Datum) -> Datum,
    _arg1: Datum,
) -> Datum {
    unimplemented!("TODO(pg-port): DirectFunctionCall1")
}
unsafe fn numeric_out(_d: Datum) -> Datum {
    crate::utils::adt::numeric::numeric_out(_d as _) as _
}
unsafe fn boolout(_d: Datum) -> Datum {
    crate::utils::adt::bool::boolout(_d as _) as _
}
unsafe fn date_out(_d: Datum) -> Datum {
    crate::utils::adt::date::date_out(_d as _) as _
}
unsafe fn time_out(_d: Datum) -> Datum {
    crate::utils::adt::date::time_out(_d as _) as _
}
unsafe fn timetz_out(_d: Datum) -> Datum {
    crate::utils::adt::date::timetz_out(_d as _) as _
}
unsafe fn timestamp_out(_d: Datum) -> Datum {
    crate::utils::adt::timestamp::timestamp_out(_d as _) as _
}
unsafe fn timestamptz_out(_d: Datum) -> Datum {
    crate::utils::adt::timestamp::timestamptz_out(_d as _) as _
}
unsafe fn bool_int4(_d: Datum) -> Datum {
    crate::utils::adt::int::bool_int4(_d as _) as _
}
unsafe fn textin(_d: Datum) -> Datum {
    crate::utils::adt::varlena::textin(_d as _) as _
}

/* TODO(pg-port): nodes/execnodes.h ModifyTableState / MergeActionState */
#[repr(C)]
pub struct ModifyTableState {
    pub ps: crate::nodes::execnodes::PlanState,
    pub mt_merge_action: *mut MergeActionState,
}
#[repr(C)]
pub struct MergeActionState {
    pub mas_action: *mut MergeAction,
}
#[repr(C)]
pub struct MergeAction {
    pub commandType: c_int,
}
const CMD_INSERT: c_int = 1;
const CMD_UPDATE: c_int = 2;
const CMD_DELETE: c_int = 3;
const CMD_NOTHING: c_int = 6;

/* TODO(pg-port): nodes/parsenodes.h RangeTblEntry */
#[repr(C)]
pub struct RangeTblEntry {
    pub eref: *mut Alias,
}
#[repr(C)]
pub struct Alias {
    pub colnames: *mut crate::nodes::pg_list::List,
}

/* TODO(pg-port): executor/execUtils.h exec_rt_fetch */
unsafe fn exec_rt_fetch(
    _rti: Index,
    _estate: *mut crate::nodes::execnodes::EState,
) -> *mut RangeTblEntry {
    crate::executor::executor::exec_rt_fetch(_rti as _, _estate as _) as _
}

/* TODO(pg-port): nodes/pg_list.h helpers used in xmlexpr */
use crate::nodes::pg_list::{List, NIL};
unsafe fn list_length(_list: *mut List) -> c_int {
    crate::nodes::pg_list::list_length(_list as _) as _
}
unsafe fn list_nth(_list: *mut List, _n: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_nth(_list as _, _n as _) as _
}
unsafe fn list_nth_int(_list: *mut List, _n: c_int) -> c_int {
    crate::nodes::pg_list::list_nth_int(_list as _, _n as _) as _
}
unsafe fn list_member_int(_list: *mut List, _datum: c_int) -> bool { false }
macro_rules! forboth {
    ($lc:ident, $list1:expr, $lc2:ident, $list2:expr, $body:block) => {
        /* TODO(pg-port): forboth stub */ {}
    };
}
unsafe fn lfirst(_lc: *mut c_void) -> *mut c_void {
    crate::nodes::pg_list::lfirst(_lc as _) as _
}
unsafe fn lfirst_int(_lc: *mut c_void) -> c_int {
    crate::nodes::pg_list::lfirst_int(_lc as _) as _
}
unsafe fn strVal(_lc: *mut c_void) -> *mut c_char {
    crate::catalog::objectaddress_impl::strVal(_lc as _) as _
}

/* TODO(pg-port): nodes/primnodes.h SVFOp constants */
use crate::nodes::primnodes::SQLValueFunctionOp;

/* TODO(pg-port): utils/typcache.h TYPECACHE constants */
const RECORDOID: Oid = 2249;
const TEXTOID: Oid = 25;
const JSONOID: Oid = 114;
const JSONBOID: Oid = 3802;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const DATEOID: Oid = 1082;
const TIMEOID: Oid = 1083;
const TIMETZOID: Oid = 1266;
const TIMESTAMPOID: Oid = 1114;
const TIMESTAMPTZOID: Oid = 1184;
const InvalidOid: Oid = 0;

/* TODO(pg-port): miscadmin.h / utils/acl.h */
unsafe fn check_stack_depth() {}

/* JsonConstructorType variants imported via prelude/primnodes pub use */
use crate::nodes::primnodes::JsonConstructorType;

/* TODO(pg-port): utils/jsonfuncs.h JS_TYPE_xxx */
const JS_TYPE_ANY: c_int = 0;
const JS_TYPE_OBJECT: c_int = 1;
const JS_TYPE_ARRAY: c_int = 2;
const JS_TYPE_SCALAR: c_int = 3;

/* TODO(pg-port): utils/jsonpath.h JS_FORMAT_JSONB */
const JS_FORMAT_JSONB: c_int = 2;

/* TODO(pg-port): nodes/primnodes.h JSON_QUERY_OP / JSON_VALUE_OP */
use crate::nodes::primnodes::JsonExprOp::{JSON_EXISTS_OP, JSON_QUERY_OP, JSON_VALUE_OP};

/* TODO(pg-port): nodes/primnodes.h INNER_VAR / OUTER_VAR */
const INNER_VAR: c_int = 65000;
const OUTER_VAR: c_int = 65001;

/* TODO(pg-port): nodes/primnodes.h VarReturningType */
use crate::nodes::primnodes::VarReturningType::{
    VAR_RETURNING_DEFAULT, VAR_RETURNING_OLD, VAR_RETURNING_NEW,
};

/* TODO(pg-port): nodes/primnodes.h MinMaxOp */
use crate::nodes::primnodes::MinMaxOp::{IS_GREATEST, IS_LEAST};

/* TODO(pg-port): nodes/primnodes.h SQLValueFunctionOp */
use crate::nodes::primnodes::SQLValueFunctionOp::*;

/* TODO(pg-port): access/cmptype.h CompareType */
use crate::access::cmptype::{COMPARE_GE, COMPARE_GT, COMPARE_LE, COMPARE_LT};

/* TODO(pg-port): nodes/pg_list.h bms_is_member */
unsafe fn bms_is_member(_x: c_int, _a: *const c_void) -> bool {
    crate::nodes::bitmapset::bms_is_member(_x as _, _a as _) as _
}

/* TODO(pg-port): utils/tuplesort.h */
unsafe fn tuplesort_putdatum(
    _state: *mut c_void,
    _val: Datum,
    _isnull: bool,
) {
    crate::utils::sort::tuplesortvariants::tuplesort_putdatum(_state as _, _val as _, _isnull as _)
}
unsafe fn tuplesort_puttupleslot(
    _state: *mut c_void,
    _slot: *mut TupleTableSlot,
) {
    crate::utils::sort::tuplesortvariants::tuplesort_puttupleslot(_state as _, _slot as _)
}

/* TODO(pg-port): HeapTupleHeader accessors */
#[repr(C)]
pub struct HeapTupleHeader {
    _opaque: [u8; 0],
}
unsafe fn DatumGetHeapTupleHeader(_d: Datum) -> *mut HeapTupleHeader {
    unimplemented!("TODO(pg-port)")
}
unsafe fn HeapTupleHeaderGetTypeId(_h: *mut HeapTupleHeader) -> Oid {
    crate::access::htup_details::HeapTupleHeaderGetTypeId(_h as _) as _
}
unsafe fn HeapTupleHeaderGetTypMod(_h: *mut HeapTupleHeader) -> i32 {
    crate::access::htup_details::HeapTupleHeaderGetTypMod(_h as _) as _
}
unsafe fn HeapTupleHeaderGetDatumLength(_h: *mut HeapTupleHeader) -> u32 {
    crate::access::htup_details::HeapTupleHeaderGetDatumLength(_h as _) as _
}
unsafe fn HeapTupleHeaderSetTypeId(_h: *mut HeapTupleHeader, _typid: Oid) {
    crate::access::htup_details::HeapTupleHeaderSetTypeId(_h as _, _typid as _)
}
unsafe fn HeapTupleHeaderSetTypMod(_h: *mut HeapTupleHeader, _typmod: i32) {
    crate::access::htup_details::HeapTupleHeaderSetTypMod(_h as _, _typmod as _)
}
unsafe fn ItemPointerSetInvalid(_ip: *mut crate::storage::itemptr::ItemPointerData) {
    crate::storage::itemptr::ItemPointerSetInvalid(_ip as _)
}

/* TODO(pg-port): pg_bitutils.h / utils/hsearch.h */
unsafe fn pg_rotate_left32(v: u32, n: u32) -> u32 {
    v.rotate_left(n)
}
/* TODO(pg-port): lib/simplehash.h SH_TYPE (saophash_hash) header; only
 * private_data is accessed by the saop callbacks below, but the leading fields
 * are laid out to match SH_TYPE so the field offset is correct. */
#[repr(C)]
pub struct saophash_hash {
    pub size: u64,
    pub members: u32,
    pub sizemask: u32,
    pub grow_threshold: u32,
    pub data: *mut c_void,
    pub ctx: MemoryContext,
    pub private_data: *mut c_void,
}
unsafe fn saophash_create(
    _mcxt: MemoryContext,
    _nelements: c_int,
    _private_data: *mut ScalarArrayOpExprHashTable,
) -> *mut c_void {
    unimplemented!("TODO(pg-port): saophash_create")
}
unsafe fn saophash_insert(
    _hashtab: *mut c_void,
    _element: Datum,
    _found: *mut bool,
) {
    unimplemented!("TODO(pg-port): saophash_insert")
}
unsafe fn saophash_lookup(
    _hashtab: *mut c_void,
    _element: Datum,
) -> *mut c_void {
    unimplemented!("TODO(pg-port): saophash_lookup")
}

/* TODO(pg-port): array att_* macros */
unsafe fn fetch_att(_s: *const c_char, _typbyval: bool, _typlen: i16) -> Datum {
    crate::access::tupmacs::fetch_att(_s as _, _typbyval as _, _typlen as _) as _
}
unsafe fn att_addlength_pointer(
    _s: *const c_char,
    _typlen: i16,
    _ptr: *const c_char,
) -> *const c_char {
    crate::access::tupmacs::att_addlength_pointer(_s as _, _typlen as _, _ptr as _) as _
}
unsafe fn att_align_nominal(_s: *const c_char, _typalign: c_char) -> *const c_char {
    crate::access::tupmacs::att_align_nominal(_s as _, _typalign as _) as _
}

/* errsave(context, ...): record a soft error into a real ErrorSaveContext and
 * return; otherwise raise a hard ERROR. */
unsafe fn errsave(
    context: *mut crate::nodes::nodes::Node,
    _code: c_int,
    msg: *const c_char,
) {
    const T_ErrorSaveContext: c_int = 447;
    if !context.is_null() && *(context as *const c_int) == T_ErrorSaveContext {
        (*(context as *mut ErrorSaveContext)).error_occurred = true;
        return;
    }
    crate::utils::elog::emit_log(
        ERROR,
        &std::ffi::CStr::from_ptr(msg).to_string_lossy(),
        file!(),
        line!(),
    );
}
unsafe fn SOFT_ERROR_OCCURRED(escontext: *const ErrorSaveContext) -> bool {
    const T_ErrorSaveContext: c_int = 447;
    !escontext.is_null()
        && *(escontext as *const c_int) == T_ErrorSaveContext
        && (*escontext).error_occurred
}

/* TODO(pg-port): nodes/primnodes.h JSON_VALUE_OP enum re-export */
use crate::nodes::primnodes::JsonExprOp;

/* TODO(pg-port): nodes/execnodes.h EEO_FLAG_OLD_IS_NULL / EEO_FLAG_NEW_IS_NULL */
const EEO_FLAG_OLD_IS_NULL: u8 = EEO_FLAG_HAS_OLD;  /* flag reuse from EEO_FLAG_HAS_OLD */
const EEO_FLAG_NEW_IS_NULL: u8 = EEO_FLAG_HAS_NEW;  /* flag reuse from EEO_FLAG_HAS_NEW */

/* offsetof stub */
macro_rules! offsetof_stub {
    ($ty:ty, $field:ident) => {
        0usize /* TODO(pg-port): offsetof */
    };
}

/* ===================================================================
 * ExecReadyInterpretedExpr
 *
 * Prepare ExprState for interpreted execution.
 * ===================================================================
 */
pub unsafe fn ExecReadyInterpretedExpr(state: *mut ExprState) {
    /* Ensure one-time interpreter setup has been done */
    ExecInitInterpreter();

    /* Simple validity checks on expression */
    debug_assert!((*state).steps_len >= 1);
    debug_assert!(
        (*(*state).steps.add((*state).steps_len as usize - 1)).opcode
            == EEOP_DONE_RETURN as isize
            || (*(*state).steps.add((*state).steps_len as usize - 1)).opcode
                == EEOP_DONE_NO_RETURN as isize
    );

    /*
     * Don't perform redundant initialization.
     */
    if (*state).flags & EEO_FLAG_INTERPRETER_INITIALIZED != 0 {
        return;
    }

    /*
     * First time through, check whether attribute matches Var.  Might not be
     * ok anymore, due to schema changes.  We set up a callback that does
     * checking on the first call, which then sets evalfunc to the real method.
     */
    (*state).evalfunc = Some(ExecInterpExprStillValid);

    /* DIRECT_THREADED should not already be set */
    debug_assert!(((*state).flags & EEO_FLAG_DIRECT_THREADED) == 0);

    /*
     * There shouldn't be any errors before the expression is fully
     * initialized.  So we can set the flag now and save some code.
     */
    (*state).flags |= EEO_FLAG_INTERPRETER_INITIALIZED;

    /*
     * Select fast-path evalfuncs for very simple expressions.  "Starting up"
     * the full interpreter is a measurable overhead for these, and these
     * patterns occur often enough to be worth optimizing.
     */
    if (*state).steps_len == 5 {
        let step0 = ExprEvalOp::from_isize((*(*state).steps.add(0)).opcode);
        let step1 = ExprEvalOp::from_isize((*(*state).steps.add(1)).opcode);
        let step2 = ExprEvalOp::from_isize((*(*state).steps.add(2)).opcode);
        let step3 = ExprEvalOp::from_isize((*(*state).steps.add(3)).opcode);

        if step0 == EEOP_INNER_FETCHSOME
            && step1 == EEOP_HASHDATUM_SET_INITVAL
            && step2 == EEOP_INNER_VAR
            && step3 == EEOP_HASHDATUM_NEXT32
        {
            (*state).evalfunc_private = ExecJustHashInnerVarWithIV as *mut c_void;
            return;
        }
    } else if (*state).steps_len == 4 {
        let step0 = ExprEvalOp::from_isize((*(*state).steps.add(0)).opcode);
        let step1 = ExprEvalOp::from_isize((*(*state).steps.add(1)).opcode);
        let step2 = ExprEvalOp::from_isize((*(*state).steps.add(2)).opcode);

        if step0 == EEOP_OUTER_FETCHSOME
            && step1 == EEOP_OUTER_VAR
            && step2 == EEOP_HASHDATUM_FIRST
        {
            (*state).evalfunc_private = ExecJustHashOuterVar as *mut c_void;
            return;
        } else if step0 == EEOP_INNER_FETCHSOME
            && step1 == EEOP_INNER_VAR
            && step2 == EEOP_HASHDATUM_FIRST
        {
            (*state).evalfunc_private = ExecJustHashInnerVar as *mut c_void;
            return;
        } else if step0 == EEOP_OUTER_FETCHSOME
            && step1 == EEOP_OUTER_VAR
            && step2 == EEOP_HASHDATUM_FIRST_STRICT
        {
            (*state).evalfunc_private = ExecJustHashOuterVarStrict as *mut c_void;
            return;
        }
    } else if (*state).steps_len == 3 {
        let step0 = ExprEvalOp::from_isize((*(*state).steps.add(0)).opcode);
        let step1 = ExprEvalOp::from_isize((*(*state).steps.add(1)).opcode);

        if step0 == EEOP_INNER_FETCHSOME && step1 == EEOP_INNER_VAR {
            (*state).evalfunc_private = ExecJustInnerVar as *mut c_void;
            return;
        } else if step0 == EEOP_OUTER_FETCHSOME && step1 == EEOP_OUTER_VAR {
            (*state).evalfunc_private = ExecJustOuterVar as *mut c_void;
            return;
        } else if step0 == EEOP_SCAN_FETCHSOME && step1 == EEOP_SCAN_VAR {
            (*state).evalfunc_private = ExecJustScanVar as *mut c_void;
            return;
        } else if step0 == EEOP_INNER_FETCHSOME && step1 == EEOP_ASSIGN_INNER_VAR {
            (*state).evalfunc_private = ExecJustAssignInnerVar as *mut c_void;
            return;
        } else if step0 == EEOP_OUTER_FETCHSOME && step1 == EEOP_ASSIGN_OUTER_VAR {
            (*state).evalfunc_private = ExecJustAssignOuterVar as *mut c_void;
            return;
        } else if step0 == EEOP_SCAN_FETCHSOME && step1 == EEOP_ASSIGN_SCAN_VAR {
            (*state).evalfunc_private = ExecJustAssignScanVar as *mut c_void;
            return;
        } else if step0 == EEOP_CASE_TESTVAL
            && (step1 == EEOP_FUNCEXPR_STRICT
                || step1 == EEOP_FUNCEXPR_STRICT_1
                || step1 == EEOP_FUNCEXPR_STRICT_2)
        {
            (*state).evalfunc_private = ExecJustApplyFuncToCase as *mut c_void;
            return;
        } else if step0 == EEOP_INNER_VAR && step1 == EEOP_HASHDATUM_FIRST {
            (*state).evalfunc_private = ExecJustHashInnerVarVirt as *mut c_void;
            return;
        } else if step0 == EEOP_OUTER_VAR && step1 == EEOP_HASHDATUM_FIRST {
            (*state).evalfunc_private = ExecJustHashOuterVarVirt as *mut c_void;
            return;
        }
    } else if (*state).steps_len == 2 {
        let step0 = ExprEvalOp::from_isize((*(*state).steps.add(0)).opcode);

        if step0 == EEOP_CONST {
            (*state).evalfunc_private = ExecJustConst as *mut c_void;
            return;
        } else if step0 == EEOP_INNER_VAR {
            (*state).evalfunc_private = ExecJustInnerVarVirt as *mut c_void;
            return;
        } else if step0 == EEOP_OUTER_VAR {
            (*state).evalfunc_private = ExecJustOuterVarVirt as *mut c_void;
            return;
        } else if step0 == EEOP_SCAN_VAR {
            (*state).evalfunc_private = ExecJustScanVarVirt as *mut c_void;
            return;
        } else if step0 == EEOP_ASSIGN_INNER_VAR {
            (*state).evalfunc_private = ExecJustAssignInnerVarVirt as *mut c_void;
            return;
        } else if step0 == EEOP_ASSIGN_OUTER_VAR {
            (*state).evalfunc_private = ExecJustAssignOuterVarVirt as *mut c_void;
            return;
        } else if step0 == EEOP_ASSIGN_SCAN_VAR {
            (*state).evalfunc_private = ExecJustAssignScanVarVirt as *mut c_void;
            return;
        }
    }

    (*state).evalfunc_private = ExecInterpExpr as *mut c_void;
}

/* ===================================================================
 * Helper: convert raw isize opcode to ExprEvalOp enum.
 * We use switch-threading so opcode is always the plain enum value.
 * ===================================================================
 */
impl ExprEvalOp {
    #[inline]
    pub fn from_isize(v: isize) -> ExprEvalOp {
        /* Safety: C and Rust enums share the same repr; all values are valid. */
        unsafe { core::mem::transmute(v as u32) }
    }
}

/* ===================================================================
 * ExecInterpExpr
 *
 * Evaluate expression identified by "state" in the execution context
 * given by "econtext".  *isnull is set to the is-null flag for the result,
 * and the Datum value is the function result.
 * ===================================================================
 */
unsafe fn ExecInterpExpr(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    /* index of current step within state->steps[] */
    let mut op_idx: usize = 0;

    /* frequently used slots */
    let resultslot: *mut TupleTableSlot = (*state).resultslot;
    let innerslot: *mut TupleTableSlot = (*econtext).ecxt_innertuple;
    let outerslot: *mut TupleTableSlot = (*econtext).ecxt_outertuple;
    let scanslot: *mut TupleTableSlot = (*econtext).ecxt_scantuple;
    let oldslot: *mut TupleTableSlot = (*econtext).ecxt_oldtuple;
    let newslot: *mut TupleTableSlot = (*econtext).ecxt_newtuple;

    'interp: loop {
        let op: *mut ExprEvalStep = (*state).steps.add(op_idx);
        let opcode = ExprEvalOp::from_isize((*op).opcode);

        /* Macro equivalents:
         *   EEO_NEXT()  -> op_idx += 1; continue 'interp
         *   EEO_JUMP(n) -> op_idx = n; continue 'interp
         */

        match opcode {
            EEOP_DONE_RETURN => {
                *isnull = (*state).resnull;
                return (*state).resvalue;
            }

            EEOP_DONE_NO_RETURN => {
                debug_assert!(isnull.is_null());
                return 0 as Datum;
            }

            EEOP_INNER_FETCHSOME => {
                CheckOpSlotCompatibility(op, innerslot);
                slot_getsomeattrs(innerslot, (*op).d.fetch.last_var);
                op_idx += 1; continue 'interp;
            }

            EEOP_OUTER_FETCHSOME => {
                CheckOpSlotCompatibility(op, outerslot);
                slot_getsomeattrs(outerslot, (*op).d.fetch.last_var);
                op_idx += 1; continue 'interp;
            }

            EEOP_SCAN_FETCHSOME => {
                CheckOpSlotCompatibility(op, scanslot);
                slot_getsomeattrs(scanslot, (*op).d.fetch.last_var);
                op_idx += 1; continue 'interp;
            }

            EEOP_OLD_FETCHSOME => {
                CheckOpSlotCompatibility(op, oldslot);
                slot_getsomeattrs(oldslot, (*op).d.fetch.last_var);
                op_idx += 1; continue 'interp;
            }

            EEOP_NEW_FETCHSOME => {
                CheckOpSlotCompatibility(op, newslot);
                slot_getsomeattrs(newslot, (*op).d.fetch.last_var);
                op_idx += 1; continue 'interp;
            }

            EEOP_INNER_VAR => {
                let attnum = (*op).d.var.attnum as usize;
                /*
                 * Since we already extracted all referenced columns from the
                 * tuple with a FETCHSOME step, we can just grab the value
                 * directly out of the slot's decomposed-data arrays.
                 */
                debug_assert!(attnum < (*innerslot).tts_nvalid as usize);
                *(*op).resvalue = *(*innerslot).tts_values.add(attnum);
                *(*op).resnull = *(*innerslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_OUTER_VAR => {
                let attnum = (*op).d.var.attnum as usize;
                debug_assert!(attnum < (*outerslot).tts_nvalid as usize);
                *(*op).resvalue = *(*outerslot).tts_values.add(attnum);
                *(*op).resnull = *(*outerslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_SCAN_VAR => {
                let attnum = (*op).d.var.attnum as usize;
                debug_assert!(attnum < (*scanslot).tts_nvalid as usize);
                *(*op).resvalue = *(*scanslot).tts_values.add(attnum);
                *(*op).resnull = *(*scanslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_OLD_VAR => {
                let attnum = (*op).d.var.attnum as usize;
                debug_assert!(attnum < (*oldslot).tts_nvalid as usize);
                *(*op).resvalue = *(*oldslot).tts_values.add(attnum);
                *(*op).resnull = *(*oldslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_NEW_VAR => {
                let attnum = (*op).d.var.attnum as usize;
                debug_assert!(attnum < (*newslot).tts_nvalid as usize);
                *(*op).resvalue = *(*newslot).tts_values.add(attnum);
                *(*op).resnull = *(*newslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_INNER_SYSVAR => {
                ExecEvalSysVar(state, op, econtext, innerslot);
                op_idx += 1; continue 'interp;
            }

            EEOP_OUTER_SYSVAR => {
                ExecEvalSysVar(state, op, econtext, outerslot);
                op_idx += 1; continue 'interp;
            }

            EEOP_SCAN_SYSVAR => {
                ExecEvalSysVar(state, op, econtext, scanslot);
                op_idx += 1; continue 'interp;
            }

            EEOP_OLD_SYSVAR => {
                ExecEvalSysVar(state, op, econtext, oldslot);
                op_idx += 1; continue 'interp;
            }

            EEOP_NEW_SYSVAR => {
                ExecEvalSysVar(state, op, econtext, newslot);
                op_idx += 1; continue 'interp;
            }

            EEOP_WHOLEROW => {
                /* too complex for an inline implementation */
                ExecEvalWholeRowVar(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_INNER_VAR => {
                let resultnum = (*op).d.assign_var.resultnum as usize;
                let attnum = (*op).d.assign_var.attnum as usize;
                /*
                 * We do not need CheckVarSlotCompatibility here; that was taken
                 * care of at compilation time.  But see EEOP_INNER_VAR comments.
                 */
                debug_assert!(attnum < (*innerslot).tts_nvalid as usize);
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_values.add(resultnum) = *(*innerslot).tts_values.add(attnum);
                *(*resultslot).tts_isnull.add(resultnum) = *(*innerslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_OUTER_VAR => {
                let resultnum = (*op).d.assign_var.resultnum as usize;
                let attnum = (*op).d.assign_var.attnum as usize;
                debug_assert!(attnum < (*outerslot).tts_nvalid as usize);
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_values.add(resultnum) = *(*outerslot).tts_values.add(attnum);
                *(*resultslot).tts_isnull.add(resultnum) = *(*outerslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_SCAN_VAR => {
                let resultnum = (*op).d.assign_var.resultnum as usize;
                let attnum = (*op).d.assign_var.attnum as usize;
                debug_assert!(attnum < (*scanslot).tts_nvalid as usize);
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_values.add(resultnum) = *(*scanslot).tts_values.add(attnum);
                *(*resultslot).tts_isnull.add(resultnum) = *(*scanslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_OLD_VAR => {
                let resultnum = (*op).d.assign_var.resultnum as usize;
                let attnum = (*op).d.assign_var.attnum as usize;
                debug_assert!(attnum < (*oldslot).tts_nvalid as usize);
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_values.add(resultnum) = *(*oldslot).tts_values.add(attnum);
                *(*resultslot).tts_isnull.add(resultnum) = *(*oldslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_NEW_VAR => {
                let resultnum = (*op).d.assign_var.resultnum as usize;
                let attnum = (*op).d.assign_var.attnum as usize;
                debug_assert!(attnum < (*newslot).tts_nvalid as usize);
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_values.add(resultnum) = *(*newslot).tts_values.add(attnum);
                *(*resultslot).tts_isnull.add(resultnum) = *(*newslot).tts_isnull.add(attnum);
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_TMP => {
                let resultnum = (*op).d.assign_tmp.resultnum as usize;
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_values.add(resultnum) = (*state).resvalue;
                *(*resultslot).tts_isnull.add(resultnum) = (*state).resnull;
                op_idx += 1; continue 'interp;
            }

            EEOP_ASSIGN_TMP_MAKE_RO => {
                let resultnum = (*op).d.assign_tmp.resultnum as usize;
                debug_assert!(resultnum < (*(*resultslot).tts_tupleDescriptor).natts as usize);
                *(*resultslot).tts_isnull.add(resultnum) = (*state).resnull;
                if !*(*resultslot).tts_isnull.add(resultnum) {
                    *(*resultslot).tts_values.add(resultnum) =
                        MakeExpandedObjectReadOnlyInternal((*state).resvalue);
                } else {
                    *(*resultslot).tts_values.add(resultnum) = (*state).resvalue;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_CONST => {
                *(*op).resnull = (*op).d.constval.isnull;
                *(*op).resvalue = (*op).d.constval.value;
                op_idx += 1; continue 'interp;
            }

            /*
             * Function-call implementations. Arguments have previously been
             * evaluated directly into fcinfo->args.
             *
             * Note: the reason for using a temporary variable "d", here and in
             * other places, is that some compilers think "*op->resvalue = f();"
             * requires them to evaluate op->resvalue into a register before
             * calling f(), just in case f() is able to modify op->resvalue
             * somehow.  The extra line of code can save a useless register spill
             * and reload across the function call.
             */
            EEOP_FUNCEXPR => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                (*fcinfo).isnull = false;
                let d = ((*op).d.func.fn_addr)(fcinfo);
                *(*op).resvalue = d;
                *(*op).resnull = (*fcinfo).isnull;
                op_idx += 1; continue 'interp;
            }

            /* strict function call with more than two arguments */
            EEOP_FUNCEXPR_STRICT => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                let nargs = (*op).d.func.nargs as usize;
                debug_assert!(nargs > 2);
                /* strict function, so check for NULL args */
                let mut strict_null = false;
                for argno in 0..nargs {
                    if (*(*fcinfo).args.as_ptr().add(argno)).isnull {
                        *(*op).resnull = true;
                        strict_null = true;
                        break;
                    }
                }
                if !strict_null {
                    (*fcinfo).isnull = false;
                    let d = ((*op).d.func.fn_addr)(fcinfo);
                    *(*op).resvalue = d;
                    *(*op).resnull = (*fcinfo).isnull;
                }
                op_idx += 1; continue 'interp;
            }

            /* strict function call with one argument */
            EEOP_FUNCEXPR_STRICT_1 => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                debug_assert!((*op).d.func.nargs == 1);
                if (*(*fcinfo).args.as_ptr()).isnull {
                    *(*op).resnull = true;
                } else {
                    (*fcinfo).isnull = false;
                    let d = ((*op).d.func.fn_addr)(fcinfo);
                    *(*op).resvalue = d;
                    *(*op).resnull = (*fcinfo).isnull;
                }
                op_idx += 1; continue 'interp;
            }

            /* strict function call with two arguments */
            EEOP_FUNCEXPR_STRICT_2 => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                debug_assert!((*op).d.func.nargs == 2);
                let arg0_null = (*(*fcinfo).args.as_ptr()).isnull;
                let arg1_null = (*(*fcinfo).args.as_ptr().add(1)).isnull;
                if arg0_null || arg1_null {
                    *(*op).resnull = true;
                } else {
                    (*fcinfo).isnull = false;
                    let d = ((*op).d.func.fn_addr)(fcinfo);
                    *(*op).resvalue = d;
                    *(*op).resnull = (*fcinfo).isnull;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_FUNCEXPR_FUSAGE => {
                /* not common enough to inline */
                ExecEvalFuncExprFusage(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_FUNCEXPR_STRICT_FUSAGE => {
                /* not common enough to inline */
                ExecEvalFuncExprStrictFusage(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            /*
             * If any of its clauses is FALSE, an AND's result is FALSE regardless
             * of the states of the rest of the clauses, so we can stop evaluating
             * and return FALSE immediately.
             */
            EEOP_BOOL_AND_STEP_FIRST => {
                *(*op).d.boolexpr.anynull = false;
                /* FALL THROUGH to EEOP_BOOL_AND_STEP */
                if *(*op).resnull {
                    *(*op).d.boolexpr.anynull = true;
                } else if !DatumGetBool(*(*op).resvalue) {
                    /* result is already set to FALSE, need not change it */
                    op_idx = (*op).d.boolexpr.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOL_AND_STEP => {
                if *(*op).resnull {
                    *(*op).d.boolexpr.anynull = true;
                } else if !DatumGetBool(*(*op).resvalue) {
                    /* result is already set to FALSE, need not change it */
                    /* bail out early */
                    op_idx = (*op).d.boolexpr.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOL_AND_STEP_LAST => {
                if *(*op).resnull {
                    /* result is already set to NULL, need not change it */
                } else if !DatumGetBool(*(*op).resvalue) {
                    /* result is already set to FALSE, need not change it */
                    /*
                     * No point jumping early to jumpdone - would be same target
                     * (as this is the last argument to the AND expression),
                     * except more expensive.
                     */
                } else if *(*op).d.boolexpr.anynull {
                    *(*op).resvalue = 0 as Datum;
                    *(*op).resnull = true;
                } else {
                    /* result is already set to TRUE, need not change it */
                }
                op_idx += 1; continue 'interp;
            }

            /*
             * If any of its clauses is TRUE, an OR's result is TRUE regardless of
             * the states of the rest of the clauses.
             */
            EEOP_BOOL_OR_STEP_FIRST => {
                *(*op).d.boolexpr.anynull = false;
                /* FALL THROUGH to EEOP_BOOL_OR_STEP */
                if *(*op).resnull {
                    *(*op).d.boolexpr.anynull = true;
                } else if DatumGetBool(*(*op).resvalue) {
                    /* result is already set to TRUE, need not change it */
                    /* bail out early */
                    op_idx = (*op).d.boolexpr.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOL_OR_STEP => {
                if *(*op).resnull {
                    *(*op).d.boolexpr.anynull = true;
                } else if DatumGetBool(*(*op).resvalue) {
                    /* result is already set to TRUE, need not change it */
                    /* bail out early */
                    op_idx = (*op).d.boolexpr.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOL_OR_STEP_LAST => {
                if *(*op).resnull {
                    /* result is already set to NULL, need not change it */
                } else if DatumGetBool(*(*op).resvalue) {
                    /* result is already set to TRUE, need not change it */
                } else if *(*op).d.boolexpr.anynull {
                    *(*op).resvalue = 0 as Datum;
                    *(*op).resnull = true;
                } else {
                    /* result is already set to FALSE, need not change it */
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOL_NOT_STEP => {
                /*
                 * Evaluation of 'not' is simple... if expr is false, then return
                 * 'true' and vice versa.  It's safe to do this even on a
                 * nominally null value, so we ignore resnull; that means that
                 * NULL in produces NULL out, which is what we want.
                 */
                *(*op).resvalue = BoolGetDatum(!DatumGetBool(*(*op).resvalue));
                op_idx += 1; continue 'interp;
            }

            EEOP_QUAL => {
                /* simplified version of BOOL_AND_STEP for use by ExecQual() */
                /* If argument (also result) is false or null ... */
                if *(*op).resnull || !DatumGetBool(*(*op).resvalue) {
                    /* ... bail out early, returning FALSE */
                    *(*op).resnull = false;
                    *(*op).resvalue = BoolGetDatum(false);
                    op_idx = (*op).d.qualexpr.jumpdone as usize;
                    continue 'interp;
                }
                /*
                 * Otherwise, leave the TRUE value in place, in case this is the
                 * last qual.  Then, TRUE is the correct answer.
                 */
                op_idx += 1; continue 'interp;
            }

            EEOP_JUMP => {
                /* Unconditionally jump to target step */
                op_idx = (*op).d.jump.jumpdone as usize;
                continue 'interp;
            }

            EEOP_JUMP_IF_NULL => {
                /* Transfer control if current result is null */
                if *(*op).resnull {
                    op_idx = (*op).d.jump.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_JUMP_IF_NOT_NULL => {
                /* Transfer control if current result is non-null */
                if !*(*op).resnull {
                    op_idx = (*op).d.jump.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_JUMP_IF_NOT_TRUE => {
                /* Transfer control if current result is null or false */
                if *(*op).resnull || !DatumGetBool(*(*op).resvalue) {
                    op_idx = (*op).d.jump.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_NULLTEST_ISNULL => {
                *(*op).resvalue = BoolGetDatum(*(*op).resnull);
                *(*op).resnull = false;
                op_idx += 1; continue 'interp;
            }

            EEOP_NULLTEST_ISNOTNULL => {
                *(*op).resvalue = BoolGetDatum(!*(*op).resnull);
                *(*op).resnull = false;
                op_idx += 1; continue 'interp;
            }

            EEOP_NULLTEST_ROWISNULL => {
                /* out of line implementation: too large */
                ExecEvalRowNull(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_NULLTEST_ROWISNOTNULL => {
                /* out of line implementation: too large */
                ExecEvalRowNotNull(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            /* BooleanTest implementations for all booltesttypes */

            EEOP_BOOLTEST_IS_TRUE => {
                if *(*op).resnull {
                    *(*op).resvalue = BoolGetDatum(false);
                    *(*op).resnull = false;
                }
                /* else, input value is the correct output as well */
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOLTEST_IS_NOT_TRUE => {
                if *(*op).resnull {
                    *(*op).resvalue = BoolGetDatum(true);
                    *(*op).resnull = false;
                } else {
                    *(*op).resvalue = BoolGetDatum(!DatumGetBool(*(*op).resvalue));
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOLTEST_IS_FALSE => {
                if *(*op).resnull {
                    *(*op).resvalue = BoolGetDatum(false);
                    *(*op).resnull = false;
                } else {
                    *(*op).resvalue = BoolGetDatum(!DatumGetBool(*(*op).resvalue));
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_BOOLTEST_IS_NOT_FALSE => {
                if *(*op).resnull {
                    *(*op).resvalue = BoolGetDatum(true);
                    *(*op).resnull = false;
                }
                /* else, input value is the correct output as well */
                op_idx += 1; continue 'interp;
            }

            EEOP_PARAM_EXEC => {
                /* out of line implementation: too large */
                ExecEvalParamExec(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_PARAM_EXTERN => {
                /* out of line implementation: too large */
                ExecEvalParamExtern(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_PARAM_CALLBACK => {
                /* allow an extension module to supply a PARAM_EXTERN value */
                (*op).d.cparam.paramfunc.unwrap()(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_PARAM_SET => {
                /* out of line, unlikely to matter performance-wise */
                ExecEvalParamSet(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_CASE_TESTVAL => {
                *(*op).resvalue = *(*op).d.casetest.value;
                *(*op).resnull = *(*op).d.casetest.isnull;
                op_idx += 1; continue 'interp;
            }

            EEOP_CASE_TESTVAL_EXT => {
                *(*op).resvalue = (*econtext).caseValue_datum;
                *(*op).resnull = (*econtext).caseValue_isNull;
                op_idx += 1; continue 'interp;
            }

            EEOP_MAKE_READONLY => {
                /*
                 * Force a varlena value that might be read multiple times to R/O
                 */
                if !*(*op).d.make_readonly.isnull {
                    *(*op).resvalue =
                        MakeExpandedObjectReadOnlyInternal(*(*op).d.make_readonly.value);
                }
                *(*op).resnull = *(*op).d.make_readonly.isnull;
                op_idx += 1; continue 'interp;
            }

            EEOP_IOCOERCE => {
                /*
                 * Evaluate a CoerceViaIO node.  This can be quite a hot path, so
                 * inline as much work as possible.  The source value is in our
                 * result variable.
                 *
                 * Also look at ExecEvalCoerceViaIOSafe() if you change anything
                 * here.
                 */
                let str_ptr: *mut c_char;

                /* call output function (similar to OutputFunctionCall) */
                if *(*op).resnull {
                    /* output functions are not called on nulls */
                    str_ptr = core::ptr::null_mut();
                } else {
                    let fcinfo_out: FunctionCallInfo = (*op).d.iocoerce.fcinfo_data_out;
                    (*(*fcinfo_out).args.as_mut_ptr()).value = *(*op).resvalue;
                    (*(*fcinfo_out).args.as_mut_ptr()).isnull = false;
                    (*fcinfo_out).isnull = false;
                    str_ptr = DatumGetCString(FunctionCallInvoke(fcinfo_out));
                    /* OutputFunctionCall assumes result isn't null */
                    debug_assert!(!(*fcinfo_out).isnull);
                }

                /* call input function (similar to InputFunctionCall) */
                if !(*(*op).d.iocoerce.finfo_in).fn_strict || !str_ptr.is_null() {
                    let fcinfo_in: FunctionCallInfo = (*op).d.iocoerce.fcinfo_data_in;
                    (*(*fcinfo_in).args.as_mut_ptr()).value = PointerGetDatum(str_ptr as *mut c_void);
                    (*(*fcinfo_in).args.as_mut_ptr()).isnull = *(*op).resnull;
                    /* second and third arguments are already set up */
                    (*fcinfo_in).isnull = false;
                    let d = FunctionCallInvoke(fcinfo_in);
                    *(*op).resvalue = d;

                    /* Should get null result if and only if str is NULL */
                    if str_ptr.is_null() {
                        debug_assert!(*(*op).resnull);
                        debug_assert!((*fcinfo_in).isnull);
                    } else {
                        debug_assert!(!*(*op).resnull);
                        debug_assert!(!(*fcinfo_in).isnull);
                    }
                }

                op_idx += 1; continue 'interp;
            }

            EEOP_IOCOERCE_SAFE => {
                ExecEvalCoerceViaIOSafe(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_DISTINCT => {
                /*
                 * IS DISTINCT FROM must evaluate arguments (already done into
                 * fcinfo->args) to determine whether they are NULL; if either is
                 * NULL then the result is determined.
                 */
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                let arg0_null = (*(*fcinfo).args.as_ptr()).isnull;
                let arg1_null = (*(*fcinfo).args.as_ptr().add(1)).isnull;

                if arg0_null && arg1_null {
                    /* Both NULL? Then is not distinct... */
                    *(*op).resvalue = BoolGetDatum(false);
                    *(*op).resnull = false;
                } else if arg0_null || arg1_null {
                    /* Only one is NULL? Then is distinct... */
                    *(*op).resvalue = BoolGetDatum(true);
                    *(*op).resnull = false;
                } else {
                    /* Neither null, so apply the equality function */
                    (*fcinfo).isnull = false;
                    let eqresult = ((*op).d.func.fn_addr)(fcinfo);
                    /* Must invert result of "="; safe to do even if null */
                    *(*op).resvalue = BoolGetDatum(!DatumGetBool(eqresult));
                    *(*op).resnull = (*fcinfo).isnull;
                }
                op_idx += 1; continue 'interp;
            }

            /* see EEOP_DISTINCT for comments, this is just inverted */
            EEOP_NOT_DISTINCT => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                let arg0_null = (*(*fcinfo).args.as_ptr()).isnull;
                let arg1_null = (*(*fcinfo).args.as_ptr().add(1)).isnull;

                if arg0_null && arg1_null {
                    *(*op).resvalue = BoolGetDatum(true);
                    *(*op).resnull = false;
                } else if arg0_null || arg1_null {
                    *(*op).resvalue = BoolGetDatum(false);
                    *(*op).resnull = false;
                } else {
                    (*fcinfo).isnull = false;
                    let eqresult = ((*op).d.func.fn_addr)(fcinfo);
                    *(*op).resvalue = eqresult;
                    *(*op).resnull = (*fcinfo).isnull;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_NULLIF => {
                /*
                 * The arguments are already evaluated into fcinfo->args.
                 */
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                let save_arg0 = (*(*fcinfo).args.as_ptr()).value;

                /* if either argument is NULL they can't be equal */
                let arg0_null = (*(*fcinfo).args.as_ptr()).isnull;
                let arg1_null = (*(*fcinfo).args.as_ptr().add(1)).isnull;
                if !arg0_null && !arg1_null {
                    /*
                     * If first argument is of varlena type, it might be an
                     * expanded datum.  We need to ensure that the value passed to
                     * the comparison function is a read-only pointer.
                     */
                    if (*op).d.func.make_ro {
                        (*(*fcinfo).args.as_mut_ptr()).value =
                            MakeExpandedObjectReadOnlyInternal(save_arg0);
                    }
                    (*fcinfo).isnull = false;
                    let result = ((*op).d.func.fn_addr)(fcinfo);

                    /* if the arguments are equal return null */
                    if !(*fcinfo).isnull && DatumGetBool(result) {
                        *(*op).resvalue = 0 as Datum;
                        *(*op).resnull = true;
                        op_idx += 1; continue 'interp;
                    }
                }

                /* Arguments aren't equal, so return the first one */
                *(*op).resvalue = save_arg0;
                *(*op).resnull = (*(*fcinfo).args.as_ptr()).isnull;
                op_idx += 1; continue 'interp;
            }

            EEOP_SQLVALUEFUNCTION => {
                /*
                 * Doesn't seem worthwhile to have an inline implementation
                 * efficiency-wise.
                 */
                ExecEvalSQLValueFunction(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_CURRENTOFEXPR => {
                /* error invocation uses space, and shouldn't ever occur */
                ExecEvalCurrentOfExpr(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_NEXTVALUEEXPR => {
                /*
                 * Doesn't seem worthwhile to have an inline implementation
                 * efficiency-wise.
                 */
                ExecEvalNextValueExpr(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_RETURNINGEXPR => {
                /*
                 * The next op actually evaluates the expression.  If the OLD/NEW
                 * row doesn't exist, skip that and return NULL.
                 */
                if (*state).flags & (*op).d.returningexpr.nullflag != 0 {
                    *(*op).resvalue = 0 as Datum;
                    *(*op).resnull = true;
                    op_idx = (*op).d.returningexpr.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_ARRAYEXPR => {
                /* too complex for an inline implementation */
                ExecEvalArrayExpr(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_ARRAYCOERCE => {
                /* too complex for an inline implementation */
                ExecEvalArrayCoerce(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_ROW => {
                /* too complex for an inline implementation */
                ExecEvalRow(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_ROWCOMPARE_STEP => {
                let fcinfo: FunctionCallInfo = (*op).d.rowcompare_step.fcinfo_data;
                let arg0_null = (*(*fcinfo).args.as_ptr()).isnull;
                let arg1_null = (*(*fcinfo).args.as_ptr().add(1)).isnull;

                /* force NULL result if strict fn and NULL input */
                if (*(*op).d.rowcompare_step.finfo).fn_strict && (arg0_null || arg1_null) {
                    *(*op).resnull = true;
                    op_idx = (*op).d.rowcompare_step.jumpnull as usize;
                    continue 'interp;
                }

                /* Apply comparison function */
                (*fcinfo).isnull = false;
                let d = ((*op).d.rowcompare_step.fn_addr)(fcinfo);
                *(*op).resvalue = d;

                /* force NULL result if NULL function result */
                if (*fcinfo).isnull {
                    *(*op).resnull = true;
                    op_idx = (*op).d.rowcompare_step.jumpnull as usize;
                    continue 'interp;
                }
                *(*op).resnull = false;

                /* If unequal, no need to compare remaining columns */
                if DatumGetInt32(*(*op).resvalue) != 0 {
                    op_idx = (*op).d.rowcompare_step.jumpdone as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_ROWCOMPARE_FINAL => {
                let cmpresult = DatumGetInt32(*(*op).resvalue);
                let cmptype = (*op).d.rowcompare_final.cmptype;

                *(*op).resnull = false;
                match cmptype {
                    /* EQ and NE cases aren't allowed here */
                    COMPARE_LT => { *(*op).resvalue = BoolGetDatum(cmpresult < 0); }
                    COMPARE_LE => { *(*op).resvalue = BoolGetDatum(cmpresult <= 0); }
                    COMPARE_GE => { *(*op).resvalue = BoolGetDatum(cmpresult >= 0); }
                    COMPARE_GT => { *(*op).resvalue = BoolGetDatum(cmpresult > 0); }
                    _ => { debug_assert!(false); }
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_MINMAX => {
                /* too complex for an inline implementation */
                ExecEvalMinMax(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_FIELDSELECT => {
                /* too complex for an inline implementation */
                ExecEvalFieldSelect(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_FIELDSTORE_DEFORM => {
                /* too complex for an inline implementation */
                ExecEvalFieldStoreDeForm(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_FIELDSTORE_FORM => {
                /* too complex for an inline implementation */
                ExecEvalFieldStoreForm(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_SBSREF_SUBSCRIPTS => {
                /* Precheck SubscriptingRef subscript(s) */
                if (*op).d.sbsref_subscript.subscriptfunc.unwrap()(state, op, econtext) {
                    op_idx += 1; continue 'interp;
                } else {
                    /* Subscript is null, short-circuit SubscriptingRef to NULL */
                    op_idx = (*op).d.sbsref_subscript.jumpdone as usize;
                    continue 'interp;
                }
            }

            EEOP_SBSREF_OLD | EEOP_SBSREF_ASSIGN | EEOP_SBSREF_FETCH => {
                /* Perform a SubscriptingRef fetch or assignment */
                (*op).d.sbsref.subscriptfunc.unwrap()(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_CONVERT_ROWTYPE => {
                /* too complex for an inline implementation */
                ExecEvalConvertRowtype(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_SCALARARRAYOP => {
                /* too complex for an inline implementation */
                ExecEvalScalarArrayOp(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_HASHED_SCALARARRAYOP => {
                /* too complex for an inline implementation */
                ExecEvalHashedScalarArrayOp(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_DOMAIN_TESTVAL => {
                *(*op).resvalue = *(*op).d.casetest.value;
                *(*op).resnull = *(*op).d.casetest.isnull;
                op_idx += 1; continue 'interp;
            }

            EEOP_DOMAIN_TESTVAL_EXT => {
                *(*op).resvalue = (*econtext).domainValue_datum;
                *(*op).resnull = (*econtext).domainValue_isNull;
                op_idx += 1; continue 'interp;
            }

            EEOP_DOMAIN_NOTNULL => {
                /* too complex for an inline implementation */
                ExecEvalConstraintNotNull(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_DOMAIN_CHECK => {
                /* too complex for an inline implementation */
                ExecEvalConstraintCheck(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_HASHDATUM_SET_INITVAL => {
                *(*op).resvalue = (*op).d.hashdatum_initvalue.init_value;
                *(*op).resnull = false;
                op_idx += 1; continue 'interp;
            }

            EEOP_HASHDATUM_FIRST => {
                let fcinfo: FunctionCallInfo = (*op).d.hashdatum.fcinfo_data;
                /*
                 * Save the Datum on non-null inputs, otherwise store 0 so that
                 * subsequent NEXT32 operations combine with an initialized value.
                 */
                if !(*(*fcinfo).args.as_ptr()).isnull {
                    *(*op).resvalue = ((*op).d.hashdatum.fn_addr)(fcinfo);
                } else {
                    *(*op).resvalue = 0 as Datum;
                }
                *(*op).resnull = false;
                op_idx += 1; continue 'interp;
            }

            EEOP_HASHDATUM_FIRST_STRICT => {
                let fcinfo: FunctionCallInfo = (*op).d.hashdatum.fcinfo_data;

                if (*(*fcinfo).args.as_ptr()).isnull {
                    /*
                     * With strict we have the expression return NULL instead of
                     * ignoring NULL input values.  We've nothing more to do after
                     * finding a NULL.
                     */
                    *(*op).resnull = true;
                    *(*op).resvalue = 0 as Datum;
                    op_idx = (*op).d.hashdatum.jumpdone as usize;
                    continue 'interp;
                }

                /* execute the hash function and save the resulting value */
                *(*op).resvalue = ((*op).d.hashdatum.fn_addr)(fcinfo);
                *(*op).resnull = false;
                op_idx += 1; continue 'interp;
            }

            EEOP_HASHDATUM_NEXT32 => {
                let fcinfo: FunctionCallInfo = (*op).d.hashdatum.fcinfo_data;
                let mut existinghash = DatumGetUInt32((*(*op).d.hashdatum.iresult).value);
                /* combine successive hash values by rotating */
                existinghash = pg_rotate_left32(existinghash, 1);

                /* leave the hash value alone on NULL inputs */
                if !(*(*fcinfo).args.as_ptr()).isnull {
                    let hashvalue = DatumGetUInt32(((*op).d.hashdatum.fn_addr)(fcinfo));
                    existinghash ^= hashvalue;
                }

                *(*op).resvalue = UInt32GetDatum(existinghash);
                *(*op).resnull = false;
                op_idx += 1; continue 'interp;
            }

            EEOP_HASHDATUM_NEXT32_STRICT => {
                let fcinfo: FunctionCallInfo = (*op).d.hashdatum.fcinfo_data;

                if (*(*fcinfo).args.as_ptr()).isnull {
                    /*
                     * With strict we have the expression return NULL instead of
                     * ignoring NULL input values.  We've nothing more to do after
                     * finding a NULL.
                     */
                    *(*op).resnull = true;
                    *(*op).resvalue = 0 as Datum;
                    op_idx = (*op).d.hashdatum.jumpdone as usize;
                    continue 'interp;
                } else {
                    let mut existinghash = DatumGetUInt32((*(*op).d.hashdatum.iresult).value);
                    /* combine successive hash values by rotating */
                    existinghash = pg_rotate_left32(existinghash, 1);

                    /* execute hash func and combine with previous hash value */
                    let hashvalue = DatumGetUInt32(((*op).d.hashdatum.fn_addr)(fcinfo));
                    *(*op).resvalue = UInt32GetDatum(existinghash ^ hashvalue);
                    *(*op).resnull = false;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_XMLEXPR => {
                /* too complex for an inline implementation */
                ExecEvalXmlExpr(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_JSON_CONSTRUCTOR => {
                /* too complex for an inline implementation */
                ExecEvalJsonConstructor(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_IS_JSON => {
                /* too complex for an inline implementation */
                ExecEvalJsonIsPredicate(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_JSONEXPR_PATH => {
                /* too complex for an inline implementation */
                let jump = ExecEvalJsonExprPath(state, op, econtext);
                op_idx = jump as usize;
                continue 'interp;
            }

            EEOP_JSONEXPR_COERCION => {
                /* too complex for an inline implementation */
                ExecEvalJsonCoercion(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_JSONEXPR_COERCION_FINISH => {
                /* too complex for an inline implementation */
                ExecEvalJsonCoercionFinish(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_AGGREF => {
                /*
                 * Returns a Datum whose value is the precomputed aggregate value
                 * found in the given expression context.
                 */
                let aggno = (*op).d.aggref.aggno as usize;
                debug_assert!(!(*econtext).ecxt_aggvalues.is_null());
                *(*op).resvalue = *(*econtext).ecxt_aggvalues.add(aggno);
                *(*op).resnull = *(*econtext).ecxt_aggnulls.add(aggno);
                op_idx += 1; continue 'interp;
            }

            EEOP_GROUPING_FUNC => {
                /* too complex/uncommon for an inline implementation */
                ExecEvalGroupingFunc(state, op);
                op_idx += 1; continue 'interp;
            }

            EEOP_WINDOW_FUNC => {
                /*
                 * Like Aggref, just return a precomputed value from the econtext.
                 */
                let wfunc: *mut WindowFuncExprState = (*op).d.window_func.wfstate;
                debug_assert!(!(*econtext).ecxt_aggvalues.is_null());
                let wfuncno = (*wfunc).wfuncno as usize;
                *(*op).resvalue = *(*econtext).ecxt_aggvalues.add(wfuncno);
                *(*op).resnull = *(*econtext).ecxt_aggnulls.add(wfuncno);
                op_idx += 1; continue 'interp;
            }

            EEOP_MERGE_SUPPORT_FUNC => {
                /* too complex/uncommon for an inline implementation */
                ExecEvalMergeSupportFunc(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_SUBPLAN => {
                /* too complex for an inline implementation */
                ExecEvalSubPlan(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            /* evaluate a strict aggregate deserialization function */
            EEOP_AGG_STRICT_DESERIALIZE => {
                /* Don't call a strict deserialization function with NULL input */
                if (*(*op).d.agg_deserialize.fcinfo_data).args.as_ptr().read().isnull {
                    op_idx = (*op).d.agg_deserialize.jumpnull as usize;
                    continue 'interp;
                }
                /* fallthrough to EEOP_AGG_DESERIALIZE */
                let fcinfo: FunctionCallInfo = (*op).d.agg_deserialize.fcinfo_data;
                let aggstate = (*state).parent as *mut AggState;
                let old_context = MemoryContextSwitchTo((*(*aggstate).tmpcontext).ecxt_per_tuple_memory);
                (*fcinfo).isnull = false;
                *(*op).resvalue = FunctionCallInvoke(fcinfo);
                *(*op).resnull = (*fcinfo).isnull;
                MemoryContextSwitchTo(old_context);
                op_idx += 1; continue 'interp;
            }

            /* evaluate aggregate deserialization function (non-strict portion) */
            EEOP_AGG_DESERIALIZE => {
                let fcinfo: FunctionCallInfo = (*op).d.agg_deserialize.fcinfo_data;
                let aggstate = (*state).parent as *mut AggState;
                /*
                 * We run the deserialization functions in per-input-tuple memory
                 * context.
                 */
                let old_context = MemoryContextSwitchTo((*(*aggstate).tmpcontext).ecxt_per_tuple_memory);
                (*fcinfo).isnull = false;
                *(*op).resvalue = FunctionCallInvoke(fcinfo);
                *(*op).resnull = (*fcinfo).isnull;
                MemoryContextSwitchTo(old_context);
                op_idx += 1; continue 'interp;
            }

            /*
             * Check that a strict aggregate transition / combination function's
             * input is not NULL.
             */

            /* when checking more than one argument */
            EEOP_AGG_STRICT_INPUT_CHECK_ARGS => {
                let args: *mut NullableDatum = (*op).d.agg_strict_input_check.args;
                let nargs = (*op).d.agg_strict_input_check.nargs as usize;
                debug_assert!(nargs > 1);
                let mut jumped = false;
                for argno in 0..nargs {
                    if (*args.add(argno)).isnull {
                        op_idx = (*op).d.agg_strict_input_check.jumpnull as usize;
                        jumped = true;
                        break;
                    }
                }
                if jumped { continue 'interp; }
                op_idx += 1; continue 'interp;
            }

            /* special case for just one argument */
            EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1 => {
                let args: *mut NullableDatum = (*op).d.agg_strict_input_check.args;
                debug_assert!((*op).d.agg_strict_input_check.nargs == 1);
                if (*args).isnull {
                    op_idx = (*op).d.agg_strict_input_check.jumpnull as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            EEOP_AGG_STRICT_INPUT_CHECK_NULLS => {
                let nulls: *mut bool = (*op).d.agg_strict_input_check.nulls;
                let nargs = (*op).d.agg_strict_input_check.nargs as usize;
                let mut jumped = false;
                for argno in 0..nargs {
                    if *nulls.add(argno) {
                        op_idx = (*op).d.agg_strict_input_check.jumpnull as usize;
                        jumped = true;
                        break;
                    }
                }
                if jumped { continue 'interp; }
                op_idx += 1; continue 'interp;
            }

            /*
             * Check for a NULL pointer to the per-group states.
             */
            EEOP_AGG_PLAIN_PERGROUP_NULLCHECK => {
                let aggstate = (*state).parent as *mut AggState;
                let setoff = (*op).d.agg_plain_pergroup_nullcheck.setoff as usize;
                let pergroup_allaggs = *(*aggstate).all_pergroups.add(setoff);
                if pergroup_allaggs.is_null() {
                    op_idx = (*op).d.agg_plain_pergroup_nullcheck.jumpnull as usize;
                    continue 'interp;
                }
                op_idx += 1; continue 'interp;
            }

            /*
             * Different types of aggregate transition functions are implemented
             * as different types of steps.
             */

            EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_trans.pertrans;
                let setoff = (*op).d.agg_trans.setoff as usize;
                let transno = (*op).d.agg_trans.transno as usize;
                let pergroup: *mut crate::nodes::execnodes::AggStatePerGroupData =
                    (*(*aggstate).all_pergroups.add(setoff)).add(transno);
                debug_assert!((*pertrans).transtypeByVal);

                if (*pergroup).noTransValue {
                    /* If transValue has not yet been initialized, do so now. */
                    ExecAggInitGroup(aggstate, pertrans, pergroup, (*op).d.agg_trans.aggcontext);
                    /* copied trans value from input, done this round */
                } else if /* likely */ !(*pergroup).transValueIsNull {
                    /* invoke transition function, unless prevented by strictness */
                    ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                           (*op).d.agg_trans.aggcontext,
                                           (*op).d.agg_trans.setno);
                }
                op_idx += 1; continue 'interp;
            }

            /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
            EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_trans.pertrans;
                let setoff = (*op).d.agg_trans.setoff as usize;
                let transno = (*op).d.agg_trans.transno as usize;
                let pergroup: *mut crate::nodes::execnodes::AggStatePerGroupData =
                    (*(*aggstate).all_pergroups.add(setoff)).add(transno);
                debug_assert!((*pertrans).transtypeByVal);

                if /* likely */ !(*pergroup).transValueIsNull {
                    ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                           (*op).d.agg_trans.aggcontext,
                                           (*op).d.agg_trans.setno);
                }
                op_idx += 1; continue 'interp;
            }

            /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
            EEOP_AGG_PLAIN_TRANS_BYVAL => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_trans.pertrans;
                let setoff = (*op).d.agg_trans.setoff as usize;
                let transno = (*op).d.agg_trans.transno as usize;
                let pergroup: *mut crate::nodes::execnodes::AggStatePerGroupData =
                    (*(*aggstate).all_pergroups.add(setoff)).add(transno);
                debug_assert!((*pertrans).transtypeByVal);

                ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                       (*op).d.agg_trans.aggcontext,
                                       (*op).d.agg_trans.setno);
                op_idx += 1; continue 'interp;
            }

            /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
            EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_trans.pertrans;
                let setoff = (*op).d.agg_trans.setoff as usize;
                let transno = (*op).d.agg_trans.transno as usize;
                let pergroup: *mut crate::nodes::execnodes::AggStatePerGroupData =
                    (*(*aggstate).all_pergroups.add(setoff)).add(transno);
                debug_assert!(!(*pertrans).transtypeByVal);

                if (*pergroup).noTransValue {
                    ExecAggInitGroup(aggstate, pertrans, pergroup, (*op).d.agg_trans.aggcontext);
                } else if /* likely */ !(*pergroup).transValueIsNull {
                    ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                           (*op).d.agg_trans.aggcontext,
                                           (*op).d.agg_trans.setno);
                }
                op_idx += 1; continue 'interp;
            }

            /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
            EEOP_AGG_PLAIN_TRANS_STRICT_BYREF => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_trans.pertrans;
                let setoff = (*op).d.agg_trans.setoff as usize;
                let transno = (*op).d.agg_trans.transno as usize;
                let pergroup: *mut crate::nodes::execnodes::AggStatePerGroupData =
                    (*(*aggstate).all_pergroups.add(setoff)).add(transno);
                debug_assert!(!(*pertrans).transtypeByVal);

                if /* likely */ !(*pergroup).transValueIsNull {
                    ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                           (*op).d.agg_trans.aggcontext,
                                           (*op).d.agg_trans.setno);
                }
                op_idx += 1; continue 'interp;
            }

            /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
            EEOP_AGG_PLAIN_TRANS_BYREF => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_trans.pertrans;
                let setoff = (*op).d.agg_trans.setoff as usize;
                let transno = (*op).d.agg_trans.transno as usize;
                let pergroup: *mut crate::nodes::execnodes::AggStatePerGroupData =
                    (*(*aggstate).all_pergroups.add(setoff)).add(transno);
                debug_assert!(!(*pertrans).transtypeByVal);

                ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                       (*op).d.agg_trans.aggcontext,
                                       (*op).d.agg_trans.setno);
                op_idx += 1; continue 'interp;
            }

            EEOP_AGG_PRESORTED_DISTINCT_SINGLE => {
                let pertrans: AggStatePerTrans = (*op).d.agg_presorted_distinctcheck.pertrans;
                let aggstate = (*state).parent as *mut AggState;

                if ExecEvalPreOrderedDistinctSingle(aggstate, pertrans) {
                    op_idx += 1; continue 'interp;
                } else {
                    op_idx = (*op).d.agg_presorted_distinctcheck.jumpdistinct as usize;
                    continue 'interp;
                }
            }

            EEOP_AGG_PRESORTED_DISTINCT_MULTI => {
                let aggstate = (*state).parent as *mut AggState;
                let pertrans: AggStatePerTrans = (*op).d.agg_presorted_distinctcheck.pertrans;

                if ExecEvalPreOrderedDistinctMulti(aggstate, pertrans) {
                    op_idx += 1; continue 'interp;
                } else {
                    op_idx = (*op).d.agg_presorted_distinctcheck.jumpdistinct as usize;
                    continue 'interp;
                }
            }

            /* process single-column ordered aggregate datum */
            EEOP_AGG_ORDERED_TRANS_DATUM => {
                /* too complex for an inline implementation */
                ExecEvalAggOrderedTransDatum(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            /* process multi-column ordered aggregate tuple */
            EEOP_AGG_ORDERED_TRANS_TUPLE => {
                /* too complex for an inline implementation */
                ExecEvalAggOrderedTransTuple(state, op, econtext);
                op_idx += 1; continue 'interp;
            }

            EEOP_LAST => {
                /* unreachable */
                debug_assert!(false);
                break 'interp;
            }

            #[allow(unreachable_patterns)]
            _ => {
                /* unreachable in correct usage */
                debug_assert!(false, "unknown ExprEvalOp");
                break 'interp;
            }
        }
    }

    /* pg_unreachable() equivalent */
    0 as Datum
}

/* ===================================================================
 * castNode! macro stub (mirrors C castNode macro)
 * ===================================================================
 */
macro_rules! castNode {
    ($ty:ty, $ptr:expr) => {
        ($ptr as *mut $ty)
    };
}

/* ===================================================================
 * ExecInterpExprStillValid
 *
 * Expression evaluation callback that performs extra checks before executing
 * the expression.
 * ===================================================================
 */
pub unsafe fn ExecInterpExprStillValid(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    is_null: *mut bool,
) -> Datum {
    /*
     * First time through, check whether attribute matches Var.  Might not be
     * ok anymore, due to schema changes.
     */
    CheckExprStillValid(state, econtext);

    /* skip the check during further executions */
    (*state).evalfunc = core::mem::transmute::<*mut c_void, _>((*state).evalfunc_private);

    /* and actually execute */
    (*state).evalfunc.unwrap()(state, econtext, is_null)
}

/*
 * Check that an expression is still valid in the face of potential schema
 * changes since the plan has been created.
 */
pub unsafe fn CheckExprStillValid(state: *mut ExprState, econtext: *mut ExprContext) {
    let innerslot = (*econtext).ecxt_innertuple;
    let outerslot = (*econtext).ecxt_outertuple;
    let scanslot = (*econtext).ecxt_scantuple;
    let oldslot = (*econtext).ecxt_oldtuple;
    let newslot = (*econtext).ecxt_newtuple;

    for i in 0..(*state).steps_len as usize {
        let op: *mut ExprEvalStep = (*state).steps.add(i);

        match ExecEvalStepOp(state, op) {
            EEOP_INNER_VAR => {
                let attnum = (*op).d.var.attnum;
                CheckVarSlotCompatibility(innerslot, attnum + 1, (*op).d.var.vartype);
            }

            EEOP_OUTER_VAR => {
                let attnum = (*op).d.var.attnum;
                CheckVarSlotCompatibility(outerslot, attnum + 1, (*op).d.var.vartype);
            }

            EEOP_SCAN_VAR => {
                let attnum = (*op).d.var.attnum;
                CheckVarSlotCompatibility(scanslot, attnum + 1, (*op).d.var.vartype);
            }

            EEOP_OLD_VAR => {
                let attnum = (*op).d.var.attnum;
                CheckVarSlotCompatibility(oldslot, attnum + 1, (*op).d.var.vartype);
            }

            EEOP_NEW_VAR => {
                let attnum = (*op).d.var.attnum;
                CheckVarSlotCompatibility(newslot, attnum + 1, (*op).d.var.vartype);
            }

            _ => {}
        }
    }
}

/*
 * Check whether a user attribute in a slot can be referenced by a Var
 * expression.
 */
unsafe fn CheckVarSlotCompatibility(slot: *mut TupleTableSlot, attnum: c_int, vartype: Oid) {
    /*
     * What we have to check for here is the possibility of an attribute
     * having been dropped or changed in type since the plan tree was created.
     * System attributes don't require checking since their types never change.
     */
    if attnum > 0 {
        let slot_tupdesc = (*slot).tts_tupleDescriptor;
        if attnum > (*slot_tupdesc).natts {
            /* should never happen */
            elog!(ERROR, "attribute number {} exceeds number of columns {}", attnum, (*slot_tupdesc).natts);
        }

        let attr = crate::access::common::tupdesc::TupleDescAttr(slot_tupdesc, (attnum - 1) as c_int);

        /* Internal error: somebody forgot to expand it. */
        /* TODO(pg-port): ATTRIBUTE_GENERATED_VIRTUAL not yet defined */

        if (*attr).attisdropped {
            ereport!(ERROR, errmsg!("attribute {} of type {} has been dropped",
                                    attnum,
                                    /* format_type_be((*slot_tupdesc).tdtypeid) */ "?"));
        }

        if vartype != (*attr).atttypid {
            ereport!(ERROR, errmsg!("attribute {} of type {} has wrong type",
                                    attnum,
                                    /* format_type_be((*slot_tupdesc).tdtypeid) */ "?"));
        }
    }
}

/*
 * Verify that the slot is compatible with a EEOP_*_FETCHSOME operation.
 */
unsafe fn CheckOpSlotCompatibility(op: *mut ExprEvalStep, slot: *mut TupleTableSlot) {
    #[cfg(debug_assertions)]
    {
        use crate::executor::execTuples::TTSOpsVirtual;
        /* there's nothing to check */
        if !(*op).d.fetch.fixed {
            return;
        }
        /* At the moment we consider it OK if a virtual slot is used instead of a
         * specific type of slot, as a virtual slot never needs to be deformed. */
        if (*slot).tts_ops as *const _ == &TTSOpsVirtual as *const _ {
            return;
        }
        debug_assert!((*op).d.fetch.kind == (*slot).tts_ops);
    }
}

/*
 * get_cached_rowtype: utility function to lookup a rowtype tupdesc
 *
 * type_id, typmod: identity of the rowtype
 * rowcache: space for caching identity info
 *   (rowcache->cacheptr must be initialized to NULL)
 * changed: if not NULL, *changed is set to true on any update
 */
unsafe fn get_cached_rowtype(
    type_id: Oid,
    typmod: i32,
    rowcache: *mut ExprEvalRowtypeCache,
    changed: *mut bool,
) -> crate::access::common::tupdesc::TupleDesc {
    if type_id != RECORDOID {
        /*
         * It's a named composite type, so use the regular typcache.
         */
        let mut typentry = (*rowcache).cacheptr as *mut TypeCacheEntry;

        if typentry.is_null()
            || (*rowcache).tupdesc_id == 0
            || (*typentry).tupDesc_identifier != (*rowcache).tupdesc_id
        {
            typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);
            if (*typentry).tupDesc.is_null() {
                ereport!(ERROR, errmsg!("type {} is not composite", /* format_type_be(type_id) */ type_id));
            }
            (*rowcache).cacheptr = typentry as *mut c_void;
            (*rowcache).tupdesc_id = (*typentry).tupDesc_identifier;
            if !changed.is_null() {
                *changed = true;
            }
        }
        (*typentry).tupDesc
    } else {
        /*
         * A RECORD type, once registered, doesn't change for the life of the
         * backend.
         */
        let mut tupdesc = (*rowcache).cacheptr as crate::access::common::tupdesc::TupleDesc;

        if tupdesc.is_null()
            || (*rowcache).tupdesc_id != 0
            || type_id != (*tupdesc).tdtypeid
            || typmod != (*tupdesc).tdtypmod
        {
            tupdesc = lookup_rowtype_tupdesc(type_id, typmod);
            /* Drop pin acquired by lookup_rowtype_tupdesc */
            ReleaseTupleDesc(tupdesc);
            (*rowcache).cacheptr = tupdesc as *mut c_void;
            (*rowcache).tupdesc_id = 0; /* not a valid value for non-RECORD */
            if !changed.is_null() {
                *changed = true;
            }
        }
        tupdesc
    }
}

/*
 * Do one-time initialization of interpretation machinery.
 * (In the switch-threaded case there's nothing to do.)
 */
/* TODO(pg-port): EEO_USE_COMPUTED_GOTO ExprEvalOpLookup (only used by the
 * computed-goto dispatch path, which this switch-threaded port does not use). */
#[repr(C)]
struct ExprEvalOpLookup {
    opcode: *const c_void,
    op: ExprEvalOp,
}

/*
 * Comparator used when building address->opcode lookup table for
 * ExecEvalStepOp() in the threaded dispatch case.
 *
 * C: gated by #if defined(EEO_USE_COMPUTED_GOTO); this port uses switch
 * threading, so this is retained for fidelity but not referenced.
 */
#[allow(dead_code)]
unsafe extern "C" fn dispatch_compare_ptr(a: *const c_void, b: *const c_void) -> c_int {
    let la = a as *const ExprEvalOpLookup;
    let lb = b as *const ExprEvalOpLookup;

    if (*la).opcode < (*lb).opcode {
        -1
    } else if (*la).opcode > (*lb).opcode {
        1
    } else {
        0
    }
}

unsafe fn ExecInitInterpreter() {
    /* nothing needed for switch-threaded dispatch */
}

/*
 * Function to return the opcode of an expression step.
 * In switch-threaded mode, ExprState->opcode is always the plain enum value.
 */
pub unsafe fn ExecEvalStepOp(state: *mut ExprState, op: *mut ExprEvalStep) -> ExprEvalOp {
    ExprEvalOp::from_isize((*op).opcode)
}

/* ===================================================================
 * Fast-path evaluation functions
 * ===================================================================
 */

/* implementation of ExecJust(Inner|Outer|Scan)Var */
#[inline(always)]
unsafe fn ExecJustVarImpl(
    state: *mut ExprState,
    slot: *mut TupleTableSlot,
    isnull: *mut bool,
) -> Datum {
    let op: *mut ExprEvalStep = (*state).steps.add(1);
    let attnum = ((*op).d.var.attnum + 1) as crate::access::attnum::AttrNumber;

    CheckOpSlotCompatibility((*state).steps.add(0), slot);

    /*
     * Since we use slot_getattr(), we don't need to implement the FETCHSOME
     * step explicitly.
     */
    slot_getattr(slot, attnum, isnull)
}

/* Simple reference to inner Var */
unsafe fn ExecJustInnerVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustVarImpl(state, (*econtext).ecxt_innertuple, isnull)
}

/* Simple reference to outer Var */
unsafe fn ExecJustOuterVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustVarImpl(state, (*econtext).ecxt_outertuple, isnull)
}

/* Simple reference to scan Var */
unsafe fn ExecJustScanVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustVarImpl(state, (*econtext).ecxt_scantuple, isnull)
}

/* implementation of ExecJustAssign(Inner|Outer|Scan)Var */
#[inline(always)]
unsafe fn ExecJustAssignVarImpl(
    state: *mut ExprState,
    inslot: *mut TupleTableSlot,
    isnull: *mut bool,
) -> Datum {
    let op: *mut ExprEvalStep = (*state).steps.add(1);
    let attnum = ((*op).d.assign_var.attnum + 1) as crate::access::attnum::AttrNumber;
    let resultnum = (*op).d.assign_var.resultnum as usize;
    let outslot: *mut TupleTableSlot = (*state).resultslot;

    CheckOpSlotCompatibility((*state).steps.add(0), inslot);

    /*
     * We do not need CheckVarSlotCompatibility here.
     */
    debug_assert!(resultnum < (*(*outslot).tts_tupleDescriptor).natts as usize);
    *(*outslot).tts_values.add(resultnum) =
        slot_getattr(inslot, attnum, &mut *(*outslot).tts_isnull.add(resultnum));
    0 as Datum
}

/* Evaluate inner Var and assign to appropriate column of result tuple */
unsafe fn ExecJustAssignInnerVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustAssignVarImpl(state, (*econtext).ecxt_innertuple, isnull)
}

/* Evaluate outer Var and assign to appropriate column of result tuple */
unsafe fn ExecJustAssignOuterVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustAssignVarImpl(state, (*econtext).ecxt_outertuple, isnull)
}

/* Evaluate scan Var and assign to appropriate column of result tuple */
unsafe fn ExecJustAssignScanVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustAssignVarImpl(state, (*econtext).ecxt_scantuple, isnull)
}

/* Evaluate CASE_TESTVAL and apply a strict function to it */
unsafe fn ExecJustApplyFuncToCase(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    let mut op: *mut ExprEvalStep = (*state).steps.add(0);

    /*
     * XXX with some redesign of the CaseTestExpr mechanism, maybe we could
     * get rid of this data shuffling?
     */
    *(*op).resvalue = *(*op).d.casetest.value;
    *(*op).resnull = *(*op).d.casetest.isnull;

    op = op.add(1);

    let nargs = (*op).d.func.nargs as usize;
    let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;

    /* strict function, so check for NULL args */
    for argno in 0..nargs {
        if (*(*fcinfo).args.as_ptr().add(argno)).isnull {
            *isnull = true;
            return 0 as Datum;
        }
    }
    (*fcinfo).isnull = false;
    let d = ((*op).d.func.fn_addr)(fcinfo);
    *isnull = (*fcinfo).isnull;
    d
}

/* Simple Const expression */
unsafe fn ExecJustConst(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    let op: *mut ExprEvalStep = (*state).steps.add(0);
    *isnull = (*op).d.constval.isnull;
    (*op).d.constval.value
}

/* implementation of ExecJust(Inner|Outer|Scan)VarVirt */
#[inline(always)]
unsafe fn ExecJustVarVirtImpl(
    state: *mut ExprState,
    slot: *mut TupleTableSlot,
    isnull: *mut bool,
) -> Datum {
    let op: *mut ExprEvalStep = (*state).steps.add(0);
    let attnum = (*op).d.var.attnum as usize;

    /*
     * As it is guaranteed that a virtual slot is used, there never is a need
     * to perform tuple deforming.
     */
    debug_assert!(attnum < (*slot).tts_nvalid as usize);
    *isnull = *(*slot).tts_isnull.add(attnum);
    *(*slot).tts_values.add(attnum)
}

/* Like ExecJustInnerVar, optimized for virtual slots */
unsafe fn ExecJustInnerVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustVarVirtImpl(state, (*econtext).ecxt_innertuple, isnull)
}

/* Like ExecJustOuterVar, optimized for virtual slots */
unsafe fn ExecJustOuterVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustVarVirtImpl(state, (*econtext).ecxt_outertuple, isnull)
}

/* Like ExecJustScanVar, optimized for virtual slots */
unsafe fn ExecJustScanVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustVarVirtImpl(state, (*econtext).ecxt_scantuple, isnull)
}

/* implementation of ExecJustAssign(Inner|Outer|Scan)VarVirt */
#[inline(always)]
unsafe fn ExecJustAssignVarVirtImpl(
    state: *mut ExprState,
    inslot: *mut TupleTableSlot,
    isnull: *mut bool,
) -> Datum {
    let op: *mut ExprEvalStep = (*state).steps.add(0);
    let attnum = (*op).d.assign_var.attnum as usize;
    let resultnum = (*op).d.assign_var.resultnum as usize;
    let outslot: *mut TupleTableSlot = (*state).resultslot;

    /* see ExecJustVarVirtImpl for comments */
    debug_assert!(attnum < (*inslot).tts_nvalid as usize);
    debug_assert!(resultnum < (*(*outslot).tts_tupleDescriptor).natts as usize);

    *(*outslot).tts_values.add(resultnum) = *(*inslot).tts_values.add(attnum);
    *(*outslot).tts_isnull.add(resultnum) = *(*inslot).tts_isnull.add(attnum);
    0 as Datum
}

/* Like ExecJustAssignInnerVar, optimized for virtual slots */
unsafe fn ExecJustAssignInnerVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustAssignVarVirtImpl(state, (*econtext).ecxt_innertuple, isnull)
}

/* Like ExecJustAssignOuterVar, optimized for virtual slots */
unsafe fn ExecJustAssignOuterVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustAssignVarVirtImpl(state, (*econtext).ecxt_outertuple, isnull)
}

/* Like ExecJustAssignScanVar, optimized for virtual slots */
unsafe fn ExecJustAssignScanVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustAssignVarVirtImpl(state, (*econtext).ecxt_scantuple, isnull)
}

/*
 * implementation for hashing an inner Var, seeding with an initial value.
 */
unsafe fn ExecJustHashInnerVarWithIV(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    let fetchop: *mut ExprEvalStep = (*state).steps.add(0);
    let setivop: *mut ExprEvalStep = (*state).steps.add(1);
    let innervar: *mut ExprEvalStep = (*state).steps.add(2);
    let hashop: *mut ExprEvalStep = (*state).steps.add(3);
    let fcinfo: FunctionCallInfo = (*hashop).d.hashdatum.fcinfo_data;
    let attnum = (*innervar).d.var.attnum as usize;

    CheckOpSlotCompatibility(fetchop, (*econtext).ecxt_innertuple);
    slot_getsomeattrs((*econtext).ecxt_innertuple, (*fetchop).d.fetch.last_var);

    (*(*fcinfo).args.as_mut_ptr()).value = *(*(*econtext).ecxt_innertuple).tts_values.add(attnum);
    (*(*fcinfo).args.as_mut_ptr()).isnull = *(*(*econtext).ecxt_innertuple).tts_isnull.add(attnum);

    let mut hashkey = DatumGetUInt32((*setivop).d.hashdatum_initvalue.init_value);
    hashkey = pg_rotate_left32(hashkey, 1);

    if !(*(*fcinfo).args.as_ptr()).isnull {
        let hashvalue = DatumGetUInt32(((*hashop).d.hashdatum.fn_addr)(fcinfo));
        hashkey ^= hashvalue;
    }

    *isnull = false;
    UInt32GetDatum(hashkey)
}

/* implementation of ExecJustHash(Inner|Outer)Var */
#[inline(always)]
unsafe fn ExecJustHashVarImpl(
    state: *mut ExprState,
    slot: *mut TupleTableSlot,
    isnull: *mut bool,
) -> Datum {
    let fetchop: *mut ExprEvalStep = (*state).steps.add(0);
    let var: *mut ExprEvalStep = (*state).steps.add(1);
    let hashop: *mut ExprEvalStep = (*state).steps.add(2);
    let fcinfo: FunctionCallInfo = (*hashop).d.hashdatum.fcinfo_data;
    let attnum = (*var).d.var.attnum as usize;

    CheckOpSlotCompatibility(fetchop, slot);
    slot_getsomeattrs(slot, (*fetchop).d.fetch.last_var);

    (*(*fcinfo).args.as_mut_ptr()).value = *(*slot).tts_values.add(attnum);
    (*(*fcinfo).args.as_mut_ptr()).isnull = *(*slot).tts_isnull.add(attnum);

    *isnull = false;

    if !(*(*fcinfo).args.as_ptr()).isnull {
        DatumGetUInt32(((*hashop).d.hashdatum.fn_addr)(fcinfo)) as Datum
    } else {
        0 as Datum
    }
}

/* implementation for hashing an outer Var */
unsafe fn ExecJustHashOuterVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustHashVarImpl(state, (*econtext).ecxt_outertuple, isnull)
}

/* implementation for hashing an inner Var */
unsafe fn ExecJustHashInnerVar(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustHashVarImpl(state, (*econtext).ecxt_innertuple, isnull)
}

/* implementation of ExecJustHash(Inner|Outer)VarVirt */
#[inline(always)]
unsafe fn ExecJustHashVarVirtImpl(
    state: *mut ExprState,
    slot: *mut TupleTableSlot,
    isnull: *mut bool,
) -> Datum {
    let var: *mut ExprEvalStep = (*state).steps.add(0);
    let hashop: *mut ExprEvalStep = (*state).steps.add(1);
    let fcinfo: FunctionCallInfo = (*hashop).d.hashdatum.fcinfo_data;
    let attnum = (*var).d.var.attnum as usize;

    (*(*fcinfo).args.as_mut_ptr()).value = *(*slot).tts_values.add(attnum);
    (*(*fcinfo).args.as_mut_ptr()).isnull = *(*slot).tts_isnull.add(attnum);

    *isnull = false;

    if !(*(*fcinfo).args.as_ptr()).isnull {
        DatumGetUInt32(((*hashop).d.hashdatum.fn_addr)(fcinfo)) as Datum
    } else {
        0 as Datum
    }
}

/* Like ExecJustHashInnerVar, optimized for virtual slots */
unsafe fn ExecJustHashInnerVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustHashVarVirtImpl(state, (*econtext).ecxt_innertuple, isnull)
}

/* Like ExecJustHashOuterVar, optimized for virtual slots */
unsafe fn ExecJustHashOuterVarVirt(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    ExecJustHashVarVirtImpl(state, (*econtext).ecxt_outertuple, isnull)
}

/*
 * implementation for hashing an outer Var.  Returns NULL on NULL input.
 */
unsafe fn ExecJustHashOuterVarStrict(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    let fetchop: *mut ExprEvalStep = (*state).steps.add(0);
    let var: *mut ExprEvalStep = (*state).steps.add(1);
    let hashop: *mut ExprEvalStep = (*state).steps.add(2);
    let fcinfo: FunctionCallInfo = (*hashop).d.hashdatum.fcinfo_data;
    let attnum = (*var).d.var.attnum as usize;

    CheckOpSlotCompatibility(fetchop, (*econtext).ecxt_outertuple);
    slot_getsomeattrs((*econtext).ecxt_outertuple, (*fetchop).d.fetch.last_var);

    (*(*fcinfo).args.as_mut_ptr()).value = *(*(*econtext).ecxt_outertuple).tts_values.add(attnum);
    (*(*fcinfo).args.as_mut_ptr()).isnull = *(*(*econtext).ecxt_outertuple).tts_isnull.add(attnum);

    if !(*(*fcinfo).args.as_ptr()).isnull {
        *isnull = false;
        DatumGetUInt32(((*hashop).d.hashdatum.fn_addr)(fcinfo)) as Datum
    } else {
        /* return NULL on NULL input */
        *isnull = true;
        0 as Datum
    }
}

/* ===================================================================
 * Out-of-line helper functions for complex instructions.
 * ===================================================================
 */

/*
 * Evaluate EEOP_FUNCEXPR_FUSAGE
 */
pub unsafe fn ExecEvalFuncExprFusage(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
    let mut fcusage = core::mem::MaybeUninit::<PgStat_FunctionCallUsage>::uninit();

    pgstat_init_function_usage(fcinfo, fcusage.as_mut_ptr());

    (*fcinfo).isnull = false;
    let d = ((*op).d.func.fn_addr)(fcinfo);
    *(*op).resvalue = d;
    *(*op).resnull = (*fcinfo).isnull;

    pgstat_end_function_usage(fcusage.as_mut_ptr(), true);
}

/*
 * Evaluate EEOP_FUNCEXPR_STRICT_FUSAGE
 */
pub unsafe fn ExecEvalFuncExprStrictFusage(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
    let mut fcusage = core::mem::MaybeUninit::<PgStat_FunctionCallUsage>::uninit();
    let nargs = (*op).d.func.nargs as usize;

    /* strict function, so check for NULL args */
    for argno in 0..nargs {
        if (*(*fcinfo).args.as_ptr().add(argno)).isnull {
            *(*op).resnull = true;
            return;
        }
    }

    pgstat_init_function_usage(fcinfo, fcusage.as_mut_ptr());

    (*fcinfo).isnull = false;
    let d = ((*op).d.func.fn_addr)(fcinfo);
    *(*op).resvalue = d;
    *(*op).resnull = (*fcinfo).isnull;

    pgstat_end_function_usage(fcusage.as_mut_ptr(), true);
}

/*
 * Evaluate a PARAM_EXEC parameter.
 *
 * PARAM_EXEC params (internal executor parameters) are stored in the
 * ecxt_param_exec_vals array, and can be accessed by array index.
 */
pub unsafe fn ExecEvalParamExec(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let prm = &mut *(*econtext)
        .ecxt_param_exec_vals
        .add((*op).d.param.paramid as usize);

    if !prm.execPlan.is_null() {
        /* Parameter not evaluated yet, so go do it */
        ExecSetParamPlan(prm.execPlan, econtext);
        /* ExecSetParamPlan should have processed this param... */
        debug_assert!(prm.execPlan.is_null());
    }
    *(*op).resvalue = prm.value;
    *(*op).resnull = prm.isnull;
}

/*
 * Evaluate a PARAM_EXTERN parameter.
 *
 * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
 */
pub unsafe fn ExecEvalParamExtern(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let param_info = (*econtext).ecxt_param_list_info;
    let param_id = (*op).d.param.paramid;

    if !param_info.is_null()
        && param_id > 0
        && param_id <= (*param_info).numParams
    {
        let prm: *mut crate::nodes::params::ParamExternData;
        let mut prmdata = core::mem::MaybeUninit::<crate::nodes::params::ParamExternData>::uninit();

        /* give hook a chance in case parameter is dynamic */
        if let Some(fetch) = (*param_info).paramFetch {
            prm = fetch(param_info, param_id, false, prmdata.as_mut_ptr());
        } else {
            prm = (*param_info).params.as_ptr().add((param_id - 1) as usize) as *mut crate::nodes::params::ParamExternData;
        }

        if OidIsValid((*prm).ptype) {
            /* safety check in case hook did something unexpected */
            if (*prm).ptype != (*op).d.param.paramtype {
                ereport!(ERROR, errmsg!(
                    "type of parameter {} does not match that when preparing the plan",
                    param_id
                ));
            }
            *(*op).resvalue = (*prm).value;
            *(*op).resnull = (*prm).isnull;
            return;
        }
    }

    ereport!(ERROR, errmsg!("no value found for parameter {}", param_id));
}

/*
 * Set value of a param (currently always PARAM_EXEC) from op->res{value,null}.
 */
pub unsafe fn ExecEvalParamSet(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let prm = &mut *(*econtext)
        .ecxt_param_exec_vals
        .add((*op).d.param.paramid as usize);

    /* Shouldn't have a pending evaluation anymore */
    debug_assert!(prm.execPlan.is_null());

    prm.value = *(*op).resvalue;
    prm.isnull = *(*op).resnull;
}

/*
 * Evaluate a CoerceViaIO node in soft-error mode.
 *
 * Note: This implements EEOP_IOCOERCE_SAFE. If you change anything here,
 * also look at the inline code for EEOP_IOCOERCE.
 */
pub unsafe fn ExecEvalCoerceViaIOSafe(state: *mut ExprState, op: *mut ExprEvalStep) {
    let str_ptr: *mut c_char;

    /* call output function (similar to OutputFunctionCall) */
    if *(*op).resnull {
        /* output functions are not called on nulls */
        str_ptr = core::ptr::null_mut();
    } else {
        let fcinfo_out: FunctionCallInfo = (*op).d.iocoerce.fcinfo_data_out;
        (*(*fcinfo_out).args.as_mut_ptr()).value = *(*op).resvalue;
        (*(*fcinfo_out).args.as_mut_ptr()).isnull = false;
        (*fcinfo_out).isnull = false;
        str_ptr = DatumGetCString(FunctionCallInvoke(fcinfo_out));
        /* OutputFunctionCall assumes result isn't null */
        debug_assert!(!(*fcinfo_out).isnull);
    }

    /* call input function (similar to InputFunctionCallSafe) */
    if !(*(*op).d.iocoerce.finfo_in).fn_strict || !str_ptr.is_null() {
        let fcinfo_in: FunctionCallInfo = (*op).d.iocoerce.fcinfo_data_in;
        (*(*fcinfo_in).args.as_mut_ptr()).value = PointerGetDatum(str_ptr as *mut c_void);
        (*(*fcinfo_in).args.as_mut_ptr()).isnull = *(*op).resnull;
        /* second and third arguments are already set up */

        /* ErrorSaveContext must be present */
        /* debug_assert!(IsA((*fcinfo_in).context, ErrorSaveContext)); */

        (*fcinfo_in).isnull = false;
        *(*op).resvalue = FunctionCallInvoke(fcinfo_in);

        if SOFT_ERROR_OCCURRED((*fcinfo_in).context as *const ErrorSaveContext) {
            *(*op).resnull = true;
            *(*op).resvalue = 0 as Datum;
            return;
        }

        /* Should get null result if and only if str is NULL */
        if str_ptr.is_null() {
            debug_assert!(*(*op).resnull);
        } else {
            debug_assert!(!*(*op).resnull);
        }
    }
}

/*
 * Evaluate a SQLValueFunction expression.
 */
pub unsafe fn ExecEvalSQLValueFunction(state: *mut ExprState, op: *mut ExprEvalStep) {
    use crate::utils::fmgr::FunctionCallInfoBaseData;
    /* LOCAL_FCINFO(fcinfo, 0) -- allocate inline on stack; simplified here */
    let mut fcinfo_storage = core::mem::MaybeUninit::<FunctionCallInfoBaseData>::zeroed();
    let fcinfo = fcinfo_storage.as_mut_ptr();
    let svf: *mut SQLValueFunction = (*op).d.sqlvaluefunction.svf;

    *(*op).resnull = false;

    /*
     * Note: current_schema() can return NULL.  current_user() etc currently
     * cannot, but might as well code those cases the same way for safety.
     */
    use crate::nodes::primnodes::SQLValueFunctionOp::*;
    match (*svf).op {
        SVFOP_CURRENT_DATE => {
            *(*op).resvalue = DateADTGetDatum(GetSQLCurrentDate());
        }
        SVFOP_CURRENT_TIME | SVFOP_CURRENT_TIME_N => {
            *(*op).resvalue = TimeTzADTPGetDatum(GetSQLCurrentTime((*svf).typmod));
        }
        SVFOP_CURRENT_TIMESTAMP | SVFOP_CURRENT_TIMESTAMP_N => {
            *(*op).resvalue = TimestampTzGetDatum(GetSQLCurrentTimestamp((*svf).typmod));
        }
        SVFOP_LOCALTIME | SVFOP_LOCALTIME_N => {
            *(*op).resvalue = TimeADTGetDatum(GetSQLLocalTime((*svf).typmod));
        }
        SVFOP_LOCALTIMESTAMP | SVFOP_LOCALTIMESTAMP_N => {
            *(*op).resvalue = TimestampGetDatum(GetSQLLocalTimestamp((*svf).typmod));
        }
        SVFOP_CURRENT_ROLE | SVFOP_CURRENT_USER | SVFOP_USER => {
            InitFunctionCallInfoData(fcinfo, core::ptr::null_mut(), 0, InvalidOid,
                                     core::ptr::null_mut(), core::ptr::null_mut());
            *(*op).resvalue = current_user(fcinfo);
            *(*op).resnull = (*fcinfo).isnull;
        }
        SVFOP_SESSION_USER => {
            InitFunctionCallInfoData(fcinfo, core::ptr::null_mut(), 0, InvalidOid,
                                     core::ptr::null_mut(), core::ptr::null_mut());
            *(*op).resvalue = session_user(fcinfo);
            *(*op).resnull = (*fcinfo).isnull;
        }
        SVFOP_CURRENT_CATALOG => {
            InitFunctionCallInfoData(fcinfo, core::ptr::null_mut(), 0, InvalidOid,
                                     core::ptr::null_mut(), core::ptr::null_mut());
            *(*op).resvalue = current_database(fcinfo);
            *(*op).resnull = (*fcinfo).isnull;
        }
        SVFOP_CURRENT_SCHEMA => {
            InitFunctionCallInfoData(fcinfo, core::ptr::null_mut(), 0, InvalidOid,
                                     core::ptr::null_mut(), core::ptr::null_mut());
            *(*op).resvalue = current_schema(fcinfo);
            *(*op).resnull = (*fcinfo).isnull;
        }
    }
}

/*
 * Raise error if a CURRENT OF expression is evaluated.
 */
pub unsafe fn ExecEvalCurrentOfExpr(state: *mut ExprState, op: *mut ExprEvalStep) {
    ereport!(ERROR, errmsg!("WHERE CURRENT OF is not supported for this table type"));
}

/*
 * Evaluate NextValueExpr.
 */
pub unsafe fn ExecEvalNextValueExpr(state: *mut ExprState, op: *mut ExprEvalStep) {
    let newval: i64 = nextval_internal((*op).d.nextvalueexpr.seqid, false);

    match (*op).d.nextvalueexpr.seqtypid {
        INT2OID => {
            *(*op).resvalue = Int16GetDatum(newval as i16);
        }
        INT4OID => {
            *(*op).resvalue = Int32GetDatum(newval as i32);
        }
        INT8OID => {
            *(*op).resvalue = Int64GetDatum(newval);
        }
        _ => {
            elog!(ERROR, "unsupported sequence type {}", (*op).d.nextvalueexpr.seqtypid);
        }
    }
    *(*op).resnull = false;
}

/*
 * Evaluate NullTest / IS NULL for rows.
 */
pub unsafe fn ExecEvalRowNull(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    ExecEvalRowNullInt(state, op, econtext, true);
}

/*
 * Evaluate NullTest / IS NOT NULL for rows.
 */
pub unsafe fn ExecEvalRowNotNull(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    ExecEvalRowNullInt(state, op, econtext, false);
}

/* Common code for IS [NOT] NULL on a row value */
unsafe fn ExecEvalRowNullInt(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
    checkisnull: bool,
) {
    let value = *(*op).resvalue;
    let isnull = *(*op).resnull;
    let tuple: *mut HeapTupleHeader;
    let tup_type: Oid;
    let tup_typmod: i32;
    let tup_desc: crate::access::common::tupdesc::TupleDesc;
    let mut tmptup = core::mem::MaybeUninit::<crate::access::htup_details::HeapTupleData>::uninit();

    *(*op).resnull = false;

    /* NULL row variables are treated just as NULL scalar columns */
    if isnull {
        *(*op).resvalue = BoolGetDatum(checkisnull);
        return;
    }

    /*
     * The SQL standard defines IS [NOT] NULL for a non-null rowtype argument:
     * "R IS NULL" is true if every field is the null value.
     * "R IS NOT NULL" is true if no field is the null value.
     */
    tuple = DatumGetHeapTupleHeader(value);
    tup_type = HeapTupleHeaderGetTypeId(tuple);
    tup_typmod = HeapTupleHeaderGetTypMod(tuple);

    /* Lookup tupdesc if first time through or if type changes */
    tup_desc = get_cached_rowtype(tup_type, tup_typmod,
                                   &mut (*op).d.nulltest_row.rowcache, core::ptr::null_mut());

    /*
     * heap_attisnull needs a HeapTuple not a bare HeapTupleHeader.
     */
    let tp = tmptup.as_mut_ptr();
    (*tp).t_len = HeapTupleHeaderGetDatumLength(tuple);
    (*tp).t_data = tuple as *mut _ as *mut _;

    for att in 1..=(*tup_desc).natts {
        /* ignore dropped columns */
        let cattr = crate::access::common::tupdesc::TupleDescCompactAttr(tup_desc, (att - 1) as c_int);
        if (*cattr).attisdropped {
            continue;
        }
        if heap_attisnull(tp, att, tup_desc) {
            /* null field disproves IS NOT NULL */
            if !checkisnull {
                *(*op).resvalue = BoolGetDatum(false);
                return;
            }
        } else {
            /* non-null field disproves IS NULL */
            if checkisnull {
                *(*op).resvalue = BoolGetDatum(false);
                return;
            }
        }
    }

    *(*op).resvalue = BoolGetDatum(true);
}

/*
 * Evaluate an ARRAY[] expression.
 *
 * The individual array elements (or subarrays) have already been evaluated
 * into op->d.arrayexpr.elemvalues[]/elemnulls[].
 */
pub unsafe fn ExecEvalArrayExpr(state: *mut ExprState, op: *mut ExprEvalStep) {
    let element_type = (*op).d.arrayexpr.elemtype;
    let nelems = (*op).d.arrayexpr.nelems as usize;
    let mut ndims: c_int = 0;
    let mut dims = [0i32; MAXDIM];
    let mut lbs = [0i32; MAXDIM];

    /* Set non-null as default */
    *(*op).resnull = false;

    let result: *mut ArrayType;

    if !(*op).d.arrayexpr.multidims {
        /* Elements are presumably of scalar type */
        let dvalues = (*op).d.arrayexpr.elemvalues;
        let dnulls = (*op).d.arrayexpr.elemnulls;

        /* setup for 1-D array of the given length */
        ndims = 1;
        dims[0] = nelems as i32;
        lbs[0] = 1;

        result = construct_md_array(
            dvalues, dnulls, ndims, dims.as_mut_ptr(), lbs.as_mut_ptr(),
            element_type,
            (*op).d.arrayexpr.elemlength,
            (*op).d.arrayexpr.elembyval,
            (*op).d.arrayexpr.elemalign,
        );
    } else {
        /* Must be nested array expressions */
        let mut nbytes: usize = 0;
        let mut outer_nelems: usize = 0;
        let mut elem_ndims: c_int = 0;
        let mut elem_dims: *mut c_int = core::ptr::null_mut();
        let mut elem_lbs: *mut c_int = core::ptr::null_mut();
        let mut firstone = true;
        let mut havenulls = false;
        let mut haveempty = false;

        let subdata: *mut *mut c_char = palloc(nelems * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        let subbitmaps: *mut *mut u8 = palloc(nelems * core::mem::size_of::<*mut u8>()) as *mut *mut u8;
        let subbytes: *mut usize = palloc(nelems * core::mem::size_of::<usize>()) as *mut usize;
        let subnitems: *mut c_int = palloc(nelems * core::mem::size_of::<c_int>()) as *mut c_int;

        /* loop through and get data area from each element */
        for elemoff in 0..nelems {
            let arraydatum = *(*op).d.arrayexpr.elemvalues.add(elemoff);
            let eisnull = *(*op).d.arrayexpr.elemnulls.add(elemoff);

            /* temporarily ignore null subarrays */
            if eisnull {
                haveempty = true;
                continue;
            }

            let array = DatumGetArrayTypeP(arraydatum);

            /* run-time double-check on element type */
            if element_type != ARR_ELEMTYPE(array) {
                ereport!(ERROR, errmsg!(
                    "cannot merge incompatible arrays"
                ));
            }

            let this_ndims = ARR_NDIM(array);
            /* temporarily ignore zero-dimensional subarrays */
            if this_ndims <= 0 {
                haveempty = true;
                continue;
            }

            if firstone {
                /* Get sub-array details from first member */
                elem_ndims = this_ndims;
                ndims = elem_ndims + 1;
                if ndims <= 0 || ndims > MAXDIM as c_int {
                    ereport!(ERROR, errmsg!(
                        "number of array dimensions ({}) exceeds the maximum allowed ({})",
                        ndims, MAXDIM
                    ));
                }

                elem_dims = palloc(elem_ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
                core::ptr::copy_nonoverlapping(ARR_DIMS(array), elem_dims, elem_ndims as usize);
                elem_lbs = palloc(elem_ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
                core::ptr::copy_nonoverlapping(ARR_LBOUND(array), elem_lbs, elem_ndims as usize);

                firstone = false;
            } else {
                /* Check other sub-arrays are compatible */
                if elem_ndims != this_ndims
                    || core::slice::from_raw_parts(elem_dims, elem_ndims as usize)
                        != core::slice::from_raw_parts(ARR_DIMS(array), elem_ndims as usize)
                    || core::slice::from_raw_parts(elem_lbs, elem_ndims as usize)
                        != core::slice::from_raw_parts(ARR_LBOUND(array), elem_ndims as usize)
                {
                    ereport!(ERROR, errmsg!(
                        "multidimensional arrays must have array expressions with matching dimensions"
                    ));
                }
            }

            *subdata.add(outer_nelems) = ARR_DATA_PTR(array);
            *subbitmaps.add(outer_nelems) = ARR_NULLBITMAP(array);
            let this_subbytes = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
            *subbytes.add(outer_nelems) = this_subbytes;
            nbytes += this_subbytes;
            /* check for overflow of total request */
            if !AllocSizeIsValid(nbytes) {
                ereport!(ERROR, errmsg!(
                    "array size exceeds the maximum allowed ({})", MaxAllocSize()
                ));
            }
            *subnitems.add(outer_nelems) = ArrayGetNItems(this_ndims, ARR_DIMS(array));
            havenulls |= ARR_HASNULL(array);
            outer_nelems += 1;
        }

        /*
         * If all items were null or empty arrays, return an empty array;
         * otherwise, if some were and some weren't, raise error.
         */
        if haveempty {
            if ndims == 0 {
                /* didn't find any nonempty array */
                *(*op).resvalue = PointerGetDatum(construct_empty_array(element_type) as *mut c_void);
                return;
            }
            ereport!(ERROR, errmsg!(
                "multidimensional arrays must have array expressions with matching dimensions"
            ));
        }

        /* setup for multi-D array */
        dims[0] = outer_nelems as i32;
        lbs[0] = 1;
        for i in 1..ndims as usize {
            dims[i] = *elem_dims.add(i - 1);
            lbs[i] = *elem_lbs.add(i - 1);
        }

        /* check for subscript overflow */
        let nitems = ArrayGetNItems(ndims, dims.as_ptr());
        ArrayCheckBounds(ndims, dims.as_ptr(), lbs.as_ptr());

        let dataoffset: i32;
        if havenulls {
            dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
            nbytes += dataoffset as usize;
        } else {
            dataoffset = 0; /* marker for no null bitmap */
            nbytes += ARR_OVERHEAD_NONULLS(ndims) as usize;
        }

        result = palloc0(nbytes) as *mut ArrayType;
        SET_VARSIZE(result, nbytes);
        (*result).ndim = ndims;
        (*result).dataoffset = dataoffset;
        (*result).elemtype = element_type;
        core::ptr::copy_nonoverlapping(dims.as_ptr(), ARR_DIMS(result), ndims as usize);
        core::ptr::copy_nonoverlapping(lbs.as_ptr(), ARR_LBOUND(result), ndims as usize);

        let mut dat = ARR_DATA_PTR(result);
        let mut iitem: c_int = 0;
        for i in 0..outer_nelems {
            core::ptr::copy_nonoverlapping(*subdata.add(i), dat, *subbytes.add(i));
            dat = dat.add(*subbytes.add(i));
            if havenulls {
                array_bitmap_copy(ARR_NULLBITMAP(result), iitem,
                                  *subbitmaps.add(i), 0, *subnitems.add(i));
            }
            iitem += *subnitems.add(i);
        }
    }

    *(*op).resvalue = PointerGetDatum(result as *mut c_void);
}

/*
 * Evaluate an ArrayCoerceExpr expression.
 *
 * Source array is in step's result variable.
 */
pub unsafe fn ExecEvalArrayCoerce(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    /* NULL array -> NULL result */
    if *(*op).resnull {
        return;
    }

    let arraydatum = *(*op).resvalue;

    /*
     * If it's binary-compatible, modify the element type in the array header,
     * but otherwise leave the array as we received it.
     */
    if (*op).d.arraycoerce.elemexprstate.is_null() {
        /* Detoast input array if necessary, and copy in any case */
        let array = DatumGetArrayTypePCopy(arraydatum);
        (*array).elemtype = (*op).d.arraycoerce.resultelemtype;
        *(*op).resvalue = PointerGetDatum(array as *mut c_void);
        return;
    }

    /*
     * Use array_map to apply the sub-expression to each array element.
     */
    *(*op).resvalue = array_map(
        arraydatum,
        (*op).d.arraycoerce.elemexprstate,
        econtext,
        (*op).d.arraycoerce.resultelemtype,
        (*op).d.arraycoerce.amstate,
    );
}

/*
 * Evaluate a ROW() expression.
 *
 * The individual columns have already been evaluated into
 * op->d.row.elemvalues[]/elemnulls[].
 */
pub unsafe fn ExecEvalRow(state: *mut ExprState, op: *mut ExprEvalStep) {
    /* build tuple from evaluated field values */
    let tuple = heap_form_tuple(
        (*op).d.row.tupdesc,
        (*op).d.row.elemvalues,
        (*op).d.row.elemnulls,
    );

    *(*op).resvalue = HeapTupleGetDatum(tuple);
    *(*op).resnull = false;
}

/*
 * Evaluate GREATEST() or LEAST() expression.
 *
 * All of the to-be-compared expressions have already been evaluated into
 * op->d.minmax.values[]/nulls[].
 */
pub unsafe fn ExecEvalMinMax(state: *mut ExprState, op: *mut ExprEvalStep) {
    let values = (*op).d.minmax.values;
    let nulls = (*op).d.minmax.nulls;
    let fcinfo: FunctionCallInfo = (*op).d.minmax.fcinfo_data;
    let operator = (*op).d.minmax.op;

    /* set at initialization */
    debug_assert!(!(*(*fcinfo).args.as_ptr()).isnull);
    debug_assert!(!(*(*fcinfo).args.as_ptr().add(1)).isnull);

    /* default to null result */
    *(*op).resnull = true;

    for off in 0..(*op).d.minmax.nelems as usize {
        /* ignore NULL inputs */
        if *nulls.add(off) {
            continue;
        }

        if *(*op).resnull {
            /* first nonnull input, adopt value */
            *(*op).resvalue = *values.add(off);
            *(*op).resnull = false;
        } else {
            /* apply comparison function */
            (*(*fcinfo).args.as_mut_ptr()).value = *(*op).resvalue;
            (*(*fcinfo).args.as_mut_ptr().add(1)).value = *values.add(off);

            (*fcinfo).isnull = false;
            let cmpresult = DatumGetInt32(FunctionCallInvoke(fcinfo));
            if (*fcinfo).isnull {
                /* probably should not happen */
                continue;
            }

            if cmpresult > 0 && operator == IS_LEAST {
                *(*op).resvalue = *values.add(off);
            } else if cmpresult < 0 && operator == IS_GREATEST {
                *(*op).resvalue = *values.add(off);
            }
        }
    }
}

/*
 * Evaluate a FieldSelect node.
 *
 * Source record is in step's result variable.
 */
pub unsafe fn ExecEvalFieldSelect(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let fieldnum = (*op).d.fieldselect.fieldnum;
    let tup_desc: crate::access::common::tupdesc::TupleDesc;
    let attr: *const crate::catalog::pg_attribute::FormData_pg_attribute;

    /* NULL record -> NULL result */
    if *(*op).resnull {
        return;
    }

    let tup_datum = *(*op).resvalue;

    /* We can special-case expanded records for speed */
    if VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(tup_datum) as *const c_char) {
        let erh = DatumGetEOHP(tup_datum);

        debug_assert!((*erh).er_magic == ER_MAGIC);

        /* Extract record's TupleDesc */
        tup_desc = expanded_record_get_tupdesc(erh);

        /*
         * Find field's attr record.  Note we don't support system columns here.
         */
        if (fieldnum as i32) <= 0 {
            /* should never happen */
            elog!(ERROR, "unsupported reference to system column {} in FieldSelect", fieldnum);
        }
        if (fieldnum as i32) > (*tup_desc).natts {
            /* should never happen */
            elog!(ERROR, "attribute number {} exceeds number of columns {}", fieldnum, (*tup_desc).natts);
        }
        attr = crate::access::common::tupdesc::TupleDescAttr(tup_desc, (fieldnum - 1) as c_int);

        /* Check for dropped column, and force a NULL result if so */
        if (*attr).attisdropped {
            *(*op).resnull = true;
            return;
        }

        /* Check for type mismatch --- possible after ALTER COLUMN TYPE? */
        if (*op).d.fieldselect.resulttype != (*attr).atttypid {
            ereport!(ERROR, errmsg!("attribute {} has wrong type", fieldnum));
        }

        /* extract the field */
        *(*op).resvalue = expanded_record_get_field(erh, fieldnum, (*op).resnull);
    } else {
        /* Get the composite datum and extract its type fields */
        let tuple = DatumGetHeapTupleHeader(tup_datum);
        let tup_type = HeapTupleHeaderGetTypeId(tuple);
        let tup_typmod = HeapTupleHeaderGetTypMod(tuple);

        /* Lookup tupdesc if first time through or if type changes */
        tup_desc = get_cached_rowtype(tup_type, tup_typmod,
                                       &mut (*op).d.fieldselect.rowcache, core::ptr::null_mut());

        if (fieldnum as i32) <= 0 {
            /* should never happen */
            elog!(ERROR, "unsupported reference to system column {} in FieldSelect", fieldnum);
        }
        if (fieldnum as i32) > (*tup_desc).natts {
            /* should never happen */
            elog!(ERROR, "attribute number {} exceeds number of columns {}", fieldnum, (*tup_desc).natts);
        }
        attr = crate::access::common::tupdesc::TupleDescAttr(tup_desc, (fieldnum - 1) as c_int);

        /* Check for dropped column, and force a NULL result if so */
        if (*attr).attisdropped {
            *(*op).resnull = true;
            return;
        }

        /* Check for type mismatch */
        if (*op).d.fieldselect.resulttype != (*attr).atttypid {
            ereport!(ERROR, errmsg!("attribute {} has wrong type", fieldnum));
        }

        /* heap_getattr needs a HeapTuple not a bare HeapTupleHeader */
        let mut tmptup = core::mem::MaybeUninit::<crate::access::htup_details::HeapTupleData>::uninit();
        let tp = tmptup.as_mut_ptr();
        (*tp).t_len = HeapTupleHeaderGetDatumLength(tuple);
        (*tp).t_data = tuple as *mut _ as *mut _;

        /* extract the field */
        *(*op).resvalue = heap_getattr(tp, fieldnum, tup_desc, (*op).resnull);
    }
}

/*
 * Deform source tuple, filling in the step's values/nulls arrays, before
 * evaluating individual new values as part of a FieldStore expression.
 */
pub unsafe fn ExecEvalFieldStoreDeForm(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    if *(*op).resnull {
        /* Convert null input tuple into an all-nulls row */
        core::ptr::write_bytes(
            (*op).d.fieldstore.nulls as *mut u8,
            1u8, /* true */
            (*op).d.fieldstore.ncolumns as usize * core::mem::size_of::<bool>(),
        );
    } else {
        /*
         * heap_deform_tuple needs a HeapTuple not a bare HeapTupleHeader.
         */
        let tup_datum = *(*op).resvalue;
        let tuphdr = DatumGetHeapTupleHeader(tup_datum);
        let mut tmptup = core::mem::MaybeUninit::<crate::access::htup_details::HeapTupleData>::uninit();
        let tp = tmptup.as_mut_ptr();
        (*tp).t_len = HeapTupleHeaderGetDatumLength(tuphdr);
        ItemPointerSetInvalid(&mut (*tp).t_self);
        (*tp).t_tableOid = InvalidOid;
        (*tp).t_data = tuphdr as *mut _ as *mut _;

        /*
         * Lookup tupdesc if first time through or if type changes.
         */
        let tup_desc = get_cached_rowtype(
            (*(*op).d.fieldstore.fstore).resulttype,
            -1,
            (*op).d.fieldstore.rowcache,
            core::ptr::null_mut(),
        );

        /* Check that current tupdesc doesn't have more fields than allocated */
        if (*tup_desc).natts > (*op).d.fieldstore.ncolumns {
            elog!(ERROR, "too many columns in composite type {}",
                  (*(*op).d.fieldstore.fstore).resulttype);
        }

        heap_deform_tuple(tp, tup_desc,
                          (*op).d.fieldstore.values,
                          (*op).d.fieldstore.nulls);
    }
}

/*
 * Compute the new composite datum after each individual field value of a
 * FieldStore expression has been evaluated.
 */
pub unsafe fn ExecEvalFieldStoreForm(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    /* Lookup tupdesc (should be valid already) */
    let tup_desc = get_cached_rowtype(
        (*(*op).d.fieldstore.fstore).resulttype,
        -1,
        (*op).d.fieldstore.rowcache,
        core::ptr::null_mut(),
    );

    let tuple = heap_form_tuple(
        tup_desc,
        (*op).d.fieldstore.values,
        (*op).d.fieldstore.nulls,
    );

    *(*op).resvalue = HeapTupleGetDatum(tuple);
    *(*op).resnull = false;
}

/*
 * Evaluate a rowtype coercion operation.
 * This may require rearranging field positions.
 *
 * Source record is in step's result variable.
 */
pub unsafe fn ExecEvalConvertRowtype(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    /* NULL in -> NULL out */
    if *(*op).resnull {
        return;
    }

    let tup_datum = *(*op).resvalue;
    let tuple = DatumGetHeapTupleHeader(tup_datum);

    /*
     * Lookup tupdescs if first time through or if type changes.
     */
    let mut changed = false;
    let indesc = get_cached_rowtype(
        (*op).d.convert_rowtype.inputtype,
        -1,
        (*op).d.convert_rowtype.incache,
        &mut changed,
    );
    IncrTupleDescRefCount(indesc);
    let outdesc = get_cached_rowtype(
        (*op).d.convert_rowtype.outputtype,
        -1,
        (*op).d.convert_rowtype.outcache,
        &mut changed,
    );
    IncrTupleDescRefCount(outdesc);

    /* if first time through, or after change, initialize conversion map */
    if changed {
        let old_cxt = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);
        /* prepare map from old to new attribute numbers */
        (*op).d.convert_rowtype.map = convert_tuples_by_name(indesc, outdesc);
        MemoryContextSwitchTo(old_cxt);
    }

    /* Following steps need a HeapTuple not a bare HeapTupleHeader */
    let mut tmptup = core::mem::MaybeUninit::<crate::access::htup_details::HeapTupleData>::uninit();
    let tp = tmptup.as_mut_ptr();
    (*tp).t_len = HeapTupleHeaderGetDatumLength(tuple);
    (*tp).t_data = tuple as *mut _ as *mut _;

    if !(*op).d.convert_rowtype.map.is_null() {
        /* Full conversion with attribute rearrangement needed */
        let result = execute_attr_map_tuple(tp, (*op).d.convert_rowtype.map);
        /* Result already has appropriate composite-datum header fields */
        *(*op).resvalue = HeapTupleGetDatum(result);
    } else {
        /*
         * The tuple is physically compatible as-is, but we need to insert the
         * destination rowtype OID in its composite-datum header field.
         */
        *(*op).resvalue = heap_copy_tuple_as_datum(tp, outdesc);
    }

    DecrTupleDescRefCount(indesc);
    DecrTupleDescRefCount(outdesc);
}

/*
 * Evaluate "scalar op ANY/ALL (array)".
 *
 * Source array is in our result area, scalar arg is already evaluated into
 * fcinfo->args[0].
 */
pub unsafe fn ExecEvalScalarArrayOp(state: *mut ExprState, op: *mut ExprEvalStep) {
    let fcinfo: FunctionCallInfo = (*op).d.scalararrayop.fcinfo_data;
    let use_or = (*op).d.scalararrayop.useOr;
    let strictfunc = (*(*op).d.scalararrayop.finfo).fn_strict;

    /*
     * If the array is NULL then we return NULL.
     */
    if *(*op).resnull {
        return;
    }

    /* Else okay to fetch and detoast the array */
    let arr = DatumGetArrayTypeP(*(*op).resvalue);

    /*
     * If the array is empty, we return either FALSE or TRUE per the useOr flag.
     */
    let nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
    if nitems <= 0 {
        *(*op).resvalue = BoolGetDatum(!use_or);
        *(*op).resnull = false;
        return;
    }

    /*
     * If the scalar is NULL, and the function is strict, return NULL.
     */
    if (*(*fcinfo).args.as_ptr()).isnull && strictfunc {
        *(*op).resnull = true;
        return;
    }

    /*
     * We arrange to look up info about the element type only once per series
     * of calls, assuming the element type doesn't change underneath us.
     */
    if (*op).d.scalararrayop.element_type != ARR_ELEMTYPE(arr) {
        get_typlenbyvalalign(
            ARR_ELEMTYPE(arr),
            &mut (*op).d.scalararrayop.typlen,
            &mut (*op).d.scalararrayop.typbyval,
            &mut (*op).d.scalararrayop.typalign,
        );
        (*op).d.scalararrayop.element_type = ARR_ELEMTYPE(arr);
    }

    let typlen = (*op).d.scalararrayop.typlen;
    let typbyval = (*op).d.scalararrayop.typbyval;
    let typalign = (*op).d.scalararrayop.typalign;

    /* Initialize result appropriately depending on useOr */
    let mut result = BoolGetDatum(!use_or);
    let mut resultnull = false;

    /* Loop over the array elements */
    let mut s = ARR_DATA_PTR(arr) as *const c_char;
    let mut bitmap = ARR_NULLBITMAP(arr);
    let mut bitmask: c_int = 1;

    for _ in 0..nitems as usize {
        /* Get array element, checking for NULL */
        if !bitmap.is_null() && (*bitmap & bitmask as u8) == 0 {
            (*(*fcinfo).args.as_mut_ptr().add(1)).value = 0 as Datum;
            (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = true;
        } else {
            let elt = fetch_att(s, typbyval, typlen);
            s = att_addlength_pointer(s, typlen, s);
            s = att_align_nominal(s, typalign);
            (*(*fcinfo).args.as_mut_ptr().add(1)).value = elt;
            (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
        }

        /* Call comparison function */
        let thisresult: Datum;
        if (*(*fcinfo).args.as_ptr().add(1)).isnull && strictfunc {
            (*fcinfo).isnull = true;
            thisresult = 0 as Datum;
        } else {
            (*fcinfo).isnull = false;
            thisresult = ((*op).d.scalararrayop.fn_addr)(fcinfo);
        }

        /* Combine results per OR or AND semantics */
        if (*fcinfo).isnull {
            resultnull = true;
        } else if use_or {
            if DatumGetBool(thisresult) {
                result = BoolGetDatum(true);
                resultnull = false;
                break; /* needn't look at any more elements */
            }
        } else {
            if !DatumGetBool(thisresult) {
                result = BoolGetDatum(false);
                resultnull = false;
                break; /* needn't look at any more elements */
            }
        }

        /* advance bitmap pointer if any */
        if !bitmap.is_null() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmap = bitmap.add(1);
                bitmask = 1;
            }
        }
    }

    *(*op).resvalue = result;
    *(*op).resnull = resultnull;
}

/*
 * Hash function for scalar array hash op elements.
 *
 * We use the element type's default hash opclass, and the column collation
 * if the type is collation-sensitive.
 */
unsafe fn saop_element_hash(tb: *mut saophash_hash, key: Datum) -> u32 {
    let elements_tab = (*tb).private_data as *mut ScalarArrayOpExprHashTable;
    let fcinfo: FunctionCallInfo = &mut (*elements_tab).hash_fcinfo_data;
    let hash: Datum;

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = key;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;

    hash = ((*elements_tab).hash_finfo.fn_addr.unwrap())(fcinfo);

    DatumGetUInt32(hash)
}

/*
 * Matching function for scalar array hash op elements, to be used in hashtable
 * lookups.
 */
unsafe fn saop_hash_element_match(tb: *mut saophash_hash, key1: Datum, key2: Datum) -> bool {
    let result: Datum;

    let elements_tab = (*tb).private_data as *mut ScalarArrayOpExprHashTable;
    let fcinfo: FunctionCallInfo = (*(*elements_tab).op).d.hashedscalararrayop.fcinfo_data;

    (*(*fcinfo).args.as_mut_ptr().add(0)).value = key1;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
    (*(*fcinfo).args.as_mut_ptr().add(1)).value = key2;
    (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;

    result = ((*(*(*elements_tab).op).d.hashedscalararrayop.finfo).fn_addr.unwrap())(fcinfo);

    DatumGetBool(result)
}

/*
 * Evaluate "scalar op ANY (const array)" using a hashtable.
 *
 * Similar to ExecEvalScalarArrayOp, but optimized for faster repeat lookups
 * by building a hashtable on the first lookup.
 */
pub unsafe fn ExecEvalHashedScalarArrayOp(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let mut elements_tab = (*op).d.hashedscalararrayop.elements_tab;
    let fcinfo: FunctionCallInfo = (*op).d.hashedscalararrayop.fcinfo_data;
    let inclause = (*op).d.hashedscalararrayop.inclause;
    let strictfunc = (*(*op).d.hashedscalararrayop.finfo).fn_strict;
    let scalar = (*(*fcinfo).args.as_ptr()).value;
    let scalar_isnull = (*(*fcinfo).args.as_ptr()).isnull;

    /* We don't setup a hashed scalar array op if the array const is null. */
    debug_assert!(!*(*op).resnull);

    /*
     * If the scalar is NULL, and the function is strict, return NULL.
     */
    if scalar_isnull && strictfunc {
        *(*op).resnull = true;
        return;
    }

    /* Build the hash table on first evaluation */
    if elements_tab.is_null() {
        let saop = (*op).d.hashedscalararrayop.saop;
        let mut typlen: i16 = 0;
        let mut typbyval = false;
        let mut typalign: c_char = 0;
        let mut has_nulls = false;
        let mut hashfound = false;

        let arr = DatumGetArrayTypeP(*(*op).resvalue);
        let nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

        get_typlenbyvalalign(ARR_ELEMTYPE(arr), &mut typlen, &mut typbyval, &mut typalign);

        let oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

        elements_tab = palloc0(
            /* offsetof(ScalarArrayOpExprHashTable, hash_fcinfo_data) */
            core::mem::size_of::<ScalarArrayOpExprHashTable>()
                + SizeForFunctionCallInfo(1),
        ) as *mut ScalarArrayOpExprHashTable;
        (*op).d.hashedscalararrayop.elements_tab = elements_tab;
        (*elements_tab).op = op;

        fmgr_info((*saop).hashfuncid, &mut (*elements_tab).hash_finfo);
        fmgr_info_set_expr(saop as *mut crate::nodes::nodes::Node, &mut (*elements_tab).hash_finfo);

        InitFunctionCallInfoData(
            &mut (*elements_tab).hash_fcinfo_data,
            &mut (*elements_tab).hash_finfo,
            1,
            (*saop).inputcollid,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );

        /*
         * Create the hash table sizing it according to the number of elements
         * in the array.
         */
        (*elements_tab).hashtab = saophash_create(CurrentMemoryContext(), nitems, elements_tab);

        MemoryContextSwitchTo(oldcontext);

        let mut s = ARR_DATA_PTR(arr) as *const c_char;
        let mut bitmap = ARR_NULLBITMAP(arr);
        let mut bitmask: c_int = 1;

        for _ in 0..nitems as usize {
            /* Get array element, checking for NULL. */
            if !bitmap.is_null() && (*bitmap & bitmask as u8) == 0 {
                has_nulls = true;
            } else {
                let element = fetch_att(s, typbyval, typlen);
                s = att_addlength_pointer(s, typlen, s);
                s = att_align_nominal(s, typalign);
                saophash_insert((*elements_tab).hashtab, element, &mut hashfound);
            }

            /* Advance bitmap pointer if any. */
            if !bitmap.is_null() {
                bitmask <<= 1;
                if bitmask == 0x100 {
                    bitmap = bitmap.add(1);
                    bitmask = 1;
                }
            }
        }

        /*
         * Remember if we had any nulls.
         */
        (*op).d.hashedscalararrayop.has_nulls = has_nulls;
    }

    /* Check the hash to see if we have a match. */
    let hashfound = !saophash_lookup((*elements_tab).hashtab, scalar).is_null();

    /* the result depends on if the clause is an IN or NOT IN clause */
    let mut result = if inclause {
        BoolGetDatum(hashfound) /* IN */
    } else {
        BoolGetDatum(!hashfound) /* NOT IN */
    };

    let mut resultnull = false;

    /*
     * If we didn't find a match in the array, we still might need to handle
     * the possibility of null values.
     */
    if !hashfound && (*op).d.hashedscalararrayop.has_nulls {
        if strictfunc {
            /*
             * We have nulls in the array so a non-null lhs and no match must
             * yield NULL.
             */
            result = 0 as Datum;
            resultnull = true;
        } else {
            /*
             * Execute function with null rhs just once.
             */
            (*(*fcinfo).args.as_mut_ptr()).value = scalar;
            (*(*fcinfo).args.as_mut_ptr()).isnull = scalar_isnull;
            (*(*fcinfo).args.as_mut_ptr().add(1)).value = 0 as Datum;
            (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = true;

            result = ((*(*op).d.hashedscalararrayop.finfo).fn_addr.unwrap())(fcinfo);
            resultnull = (*fcinfo).isnull;

            /*
             * Reverse the result for NOT IN clauses since the above function
             * is the equality function and we need not-equals.
             */
            if !inclause {
                result = (!DatumGetBool(result)) as Datum;
            }
        }
    }

    *(*op).resvalue = result;
    *(*op).resnull = resultnull;
}

/*
 * Evaluate a NOT NULL domain constraint.
 */
pub unsafe fn ExecEvalConstraintNotNull(state: *mut ExprState, op: *mut ExprEvalStep) {
    if *(*op).resnull {
        errsave(
            (*op).d.domaincheck.escontext as *mut crate::nodes::nodes::Node,
            /* ERRCODE_NOT_NULL_VIOLATION */ 0,
            c"domain does not allow null values".as_ptr(),
        );
    }
}

/*
 * Evaluate a CHECK domain constraint.
 */
pub unsafe fn ExecEvalConstraintCheck(state: *mut ExprState, op: *mut ExprEvalStep) {
    if !*(*op).d.domaincheck.checknull && !DatumGetBool(*(*op).d.domaincheck.checkvalue) {
        errsave(
            (*op).d.domaincheck.escontext as *mut crate::nodes::nodes::Node,
            /* ERRCODE_CHECK_VIOLATION */ 0,
            (*op).d.domaincheck.constraintname,
        );
    }
}

/*
 * Evaluate the various forms of XmlExpr.
 *
 * Arguments have been evaluated into named_argvalue/named_argnull
 * and/or argvalue/argnull arrays.
 */
pub unsafe fn ExecEvalXmlExpr(state: *mut ExprState, op: *mut ExprEvalStep) {
    use crate::nodes::primnodes::XmlExprOp::*;
    let xexpr: *mut crate::nodes::primnodes::XmlExpr = (*op).d.xmlexpr.xexpr;

    *(*op).resnull = true; /* until we get a result */
    *(*op).resvalue = 0 as Datum;

    match (*xexpr).op {
        IS_XMLCONCAT => {
            let argvalue = (*op).d.xmlexpr.argvalue;
            let argnull = (*op).d.xmlexpr.argnull;
            let mut values: *mut crate::nodes::pg_list::List = core::ptr::null_mut();

            let nargs = list_length((*xexpr).args);
            for i in 0..nargs as usize {
                if !*argnull.add(i) {
                    values = crate::nodes::pg_list::lappend(
                        values,
                        DatumGetPointer(*argvalue.add(i)) as *mut c_void,
                    );
                }
            }

            if !values.is_null() {
                *(*op).resvalue = PointerGetDatum(xmlconcat(values) as *mut c_void);
                *(*op).resnull = false;
            }
        }

        IS_XMLFOREST => {
            /* TODO(pg-port): IS_XMLFOREST requires forboth/ListCell iteration */
            unimplemented!("TODO(pg-port): ExecEvalXmlExpr IS_XMLFOREST");
        }

        IS_XMLELEMENT => {
            *(*op).resvalue = PointerGetDatum(xmlelement(
                xexpr,
                (*op).d.xmlexpr.named_argvalue,
                (*op).d.xmlexpr.named_argnull,
                (*op).d.xmlexpr.argvalue,
                (*op).d.xmlexpr.argnull,
            ) as *mut c_void);
            *(*op).resnull = false;
        }

        IS_XMLPARSE => {
            let argvalue = (*op).d.xmlexpr.argvalue;
            let argnull = (*op).d.xmlexpr.argnull;

            /* arguments are known to be text, bool */
            debug_assert!(list_length((*xexpr).args) == 2);

            if *argnull.add(0) {
                return;
            }
            let value = *argvalue.add(0);
            let data = DatumGetTextPP(value);

            if *argnull.add(1) {
                /* probably can't happen */
                return;
            }
            let preserve_whitespace = DatumGetBool(*argvalue.add(1));

            *(*op).resvalue = PointerGetDatum(xmlparse(data, (*xexpr).xmloption as c_int, preserve_whitespace) as *mut c_void);
            *(*op).resnull = false;
        }

        IS_XMLPI => {
            let arg: *mut text;
            let isnull: bool;

            /* optional argument is known to be text */
            debug_assert!(list_length((*xexpr).args) <= 1);

            if !(*xexpr).args.is_null() {
                isnull = *(*op).d.xmlexpr.argnull.add(0);
                if isnull {
                    arg = core::ptr::null_mut();
                } else {
                    arg = DatumGetTextPP(*(*op).d.xmlexpr.argvalue.add(0));
                }
            } else {
                arg = core::ptr::null_mut();
                isnull = false;
            }

            *(*op).resvalue = PointerGetDatum(xmlpi((*xexpr).name, arg, isnull, (*op).resnull) as *mut c_void);
        }

        IS_XMLROOT => {
            let argvalue = (*op).d.xmlexpr.argvalue;
            let argnull = (*op).d.xmlexpr.argnull;

            /* arguments are known to be xml, text, int */
            debug_assert!(list_length((*xexpr).args) == 3);

            if *argnull.add(0) {
                return;
            }
            let data = DatumGetXmlP(*argvalue.add(0));

            let version = if *argnull.add(1) {
                core::ptr::null_mut()
            } else {
                DatumGetTextPP(*argvalue.add(1))
            };

            debug_assert!(!*argnull.add(2)); /* always present */
            let standalone = DatumGetInt32(*argvalue.add(2));

            *(*op).resvalue = PointerGetDatum(xmlroot(data, version, standalone) as *mut c_void);
            *(*op).resnull = false;
        }

        IS_XMLSERIALIZE => {
            let argvalue = (*op).d.xmlexpr.argvalue;
            let argnull = (*op).d.xmlexpr.argnull;

            /* argument type is known to be xml */
            debug_assert!(list_length((*xexpr).args) == 1);

            if *argnull.add(0) {
                return;
            }
            let value = *argvalue.add(0);

            *(*op).resvalue = PointerGetDatum(xmltotext_with_options(
                DatumGetXmlP(value),
                (*xexpr).xmloption as c_int,
                (*xexpr).indent,
            ) as *mut c_void);
            *(*op).resnull = false;
        }

        IS_DOCUMENT => {
            let argvalue = (*op).d.xmlexpr.argvalue;
            let argnull = (*op).d.xmlexpr.argnull;

            /* optional argument is known to be xml */
            debug_assert!(list_length((*xexpr).args) == 1);

            if *argnull.add(0) {
                return;
            }
            let value = *argvalue.add(0);

            *(*op).resvalue = BoolGetDatum(xml_is_document(DatumGetXmlP(value)));
            *(*op).resnull = false;
        }

        // All XmlExprOp variants covered above (IS_XMLCONCAT .. IS_DOCUMENT).
    }
}

/*
 * Evaluate a JSON constructor expression.
 */
pub unsafe fn ExecEvalJsonConstructor(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    use crate::executor::execExpr::JsonConstructorExprState;
    let jcstate: *mut JsonConstructorExprState = (*op).d.json_constructor.jcstate;
    let ctor = (*jcstate).constructor;
    let is_jsonb = (*(*(*ctor).returning).format).format_type == crate::nodes::primnodes::JsonFormatType::JS_FORMAT_JSONB;
    let mut isnull = false;
    let res: Datum;

    if (*ctor).r#type == JsonConstructorType::JSCTOR_JSON_ARRAY {
        res = (if is_jsonb { jsonb_build_array_worker } else { json_build_array_worker })(
            (*jcstate).nargs,
            (*jcstate).arg_values,
            (*jcstate).arg_nulls,
            (*jcstate).arg_types,
            (*ctor).absent_on_null,
        );
    } else if (*ctor).r#type == JsonConstructorType::JSCTOR_JSON_OBJECT {
        res = (if is_jsonb { jsonb_build_object_worker } else { json_build_object_worker })(
            (*jcstate).nargs,
            (*jcstate).arg_values,
            (*jcstate).arg_nulls,
            (*jcstate).arg_types,
            (*ctor).absent_on_null,
            (*ctor).unique,
        );
    } else if (*ctor).r#type == JsonConstructorType::JSCTOR_JSON_SCALAR {
        if *(*jcstate).arg_nulls.add(0) {
            res = 0 as Datum;
            isnull = true;
        } else {
            let value = *(*jcstate).arg_values.add(0);
            let outfuncid = (*(*jcstate).arg_type_cache.add(0)).outfuncid;
            let category = (*(*jcstate).arg_type_cache.add(0)).category;

            res = if is_jsonb {
                datum_to_jsonb(value, category, outfuncid)
            } else {
                datum_to_json(value, category, outfuncid)
            };
        }
    } else if (*ctor).r#type == JsonConstructorType::JSCTOR_JSON_PARSE {
        if *(*jcstate).arg_nulls.add(0) {
            res = 0 as Datum;
            isnull = true;
        } else {
            let value = *(*jcstate).arg_values.add(0);
            let js = DatumGetTextP(value);

            res = if is_jsonb {
                jsonb_from_text(js, true)
            } else {
                json_validate(js, true, true);
                value
            };
        }
    } else {
        elog!(ERROR, "invalid JsonConstructorExpr type {}", (*ctor).r#type as i32);
        res = 0 as Datum; /* unreachable */
    }

    *(*op).resvalue = res;
    *(*op).resnull = isnull;
}

/*
 * Evaluate a IS JSON predicate.
 */
pub unsafe fn ExecEvalJsonIsPredicate(state: *mut ExprState, op: *mut ExprEvalStep) {
    let pred: *mut JsonIsPredicate = (*op).d.is_json.pred;
    let _js = *(*op).resvalue;

    if *(*op).resnull {
        *(*op).resvalue = BoolGetDatum(false);
        return;
    }

    // TODO(pg-port): exprType(pred->expr) not yet ported; return false as stub.
    *(*op).resvalue = BoolGetDatum(false);
}

/*
 * Evaluate a jsonpath against a document.
 *
 * Return value is the step address to be performed next.
 */
pub unsafe fn ExecEvalJsonExprPath(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) -> c_int {
    let jsestate: *mut JsonExprState = (*op).d.jsonexpr.jsestate;
    let jsexpr = (*jsestate).jsexpr;
    let item = (*jsestate).formatted_expr.value;
    let path = DatumGetJsonPathP((*jsestate).pathspec.value);
    let throw_error = (*(*jsexpr).on_error).btype == crate::nodes::primnodes::JsonBehaviorType::JSON_BEHAVIOR_ERROR;
    let mut error = false;
    let mut empty = false;
    let jump_eval_coercion = (*jsestate).jump_eval_coercion;
    let mut val_string: *mut c_char = core::ptr::null_mut();

    /* Set error/empty to false. */
    core::ptr::write_bytes(&mut (*jsestate).error as *mut _ as *mut u8,
                            0, core::mem::size_of::<NullableDatum>());
    core::ptr::write_bytes(&mut (*jsestate).empty as *mut _ as *mut u8,
                            0, core::mem::size_of::<NullableDatum>());

    /* Also reset ErrorSaveContext contents for the next row. */
    if (*jsestate).escontext.details_wanted {
        (*jsestate).escontext.error_data = core::ptr::null_mut();
        (*jsestate).escontext.details_wanted = false;
    }
    (*jsestate).escontext.error_occurred = false;

    match (*jsexpr).op {
        JSON_EXISTS_OP => {
            let exists = JsonPathExists(
                item, path,
                if !throw_error { &mut error } else { core::ptr::null_mut() },
                (*jsestate).args as *mut c_void,
            );
            if !error {
                *(*op).resnull = false;
                *(*op).resvalue = BoolGetDatum(exists);
            }
        }

        JSON_QUERY_OP => {
            *(*op).resvalue = JsonPathQuery(
                item, path, (*jsexpr).wrapper as c_int,
                &mut empty,
                if !throw_error { &mut error } else { core::ptr::null_mut() },
                (*jsestate).args as *mut c_void,
                (*jsexpr).column_name,
            );
            *(*op).resnull = DatumGetPointer(*(*op).resvalue).is_null();
        }

        JSON_VALUE_OP => {
            let jbv = JsonPathValue(
                item, path, &mut empty,
                if !throw_error { &mut error } else { core::ptr::null_mut() },
                (*jsestate).args as *mut c_void,
                (*jsexpr).column_name,
            );

            if jbv.is_null() {
                /* Will be coerced with json_populate_type(), if needed. */
                *(*op).resvalue = 0 as Datum;
                *(*op).resnull = true;
            } else if !error && !empty {
                if (*(*jsexpr).returning).typid == JSONOID
                    || (*(*jsexpr).returning).typid == JSONBOID
                {
                    val_string = DatumGetCString(DirectFunctionCall1(
                        jsonb_out,
                        JsonbPGetDatum(JsonbValueToJsonb(jbv)),
                    ));
                } else if (*jsexpr).use_json_coercion {
                    *(*op).resvalue = JsonbPGetDatum(JsonbValueToJsonb(jbv));
                    *(*op).resnull = false;
                } else {
                    val_string = ExecGetJsonValueItemString(jbv, (*op).resnull);
                    /*
                     * Simply convert to the default RETURNING type (text)
                     * if no coercion needed.
                     */
                    if !(*jsexpr).use_io_coercion {
                        *(*op).resvalue = DirectFunctionCall1(textin, CStringGetDatum(val_string));
                    }
                }
            }
        }

        /* JSON_TABLE_OP can't happen here */
        _ => {
            elog!(ERROR, "unrecognized SQL/JSON expression op {}", (*jsexpr).op as c_int);
            return 0;
        }
    }

    /*
     * Coerce the result value to the RETURNING type by calling its input function.
     */
    if !*(*op).resnull && (*jsexpr).use_io_coercion {
        debug_assert!(jump_eval_coercion == -1);
        let fcinfo = (*jsestate).input_fcinfo;
        debug_assert!(!fcinfo.is_null());
        debug_assert!(!val_string.is_null());
        (*(*fcinfo).args.as_mut_ptr()).value = PointerGetDatum(val_string as *mut c_void);
        (*(*fcinfo).args.as_mut_ptr()).isnull = *(*op).resnull;

        (*fcinfo).isnull = false;
        *(*op).resvalue = FunctionCallInvoke(fcinfo);
        if SOFT_ERROR_OCCURRED(&(*jsestate).escontext) {
            error = true;
        }
    }

    /* Handle ON EMPTY. */
    if empty {
        *(*op).resvalue = 0 as Datum;
        *(*op).resnull = true;
        if !(*jsexpr).on_empty.is_null() {
            if (*(*jsexpr).on_empty).btype != crate::nodes::primnodes::JsonBehaviorType::JSON_BEHAVIOR_ERROR {
                (*jsestate).empty.value = BoolGetDatum(true);
                (*jsestate).escontext.error_occurred = false;
                (*jsestate).escontext.details_wanted = true;
                return if (*jsestate).jump_empty >= 0 {
                    (*jsestate).jump_empty
                } else {
                    (*jsestate).jump_end
                };
            }
        } else if (*(*jsexpr).on_error).btype != crate::nodes::primnodes::JsonBehaviorType::JSON_BEHAVIOR_ERROR {
            (*jsestate).error.value = BoolGetDatum(true);
            (*jsestate).escontext.error_occurred = false;
            (*jsestate).escontext.details_wanted = true;
            debug_assert!(!throw_error);
            return if (*jsestate).jump_error >= 0 {
                (*jsestate).jump_error
            } else {
                (*jsestate).jump_end
            };
        }

        ereport!(ERROR, errmsg!("no SQL/JSON item found for specified path"));
    }

    /*
     * ON ERROR.  Wouldn't get here if the behavior is ERROR.
     */
    if error {
        debug_assert!(!throw_error);
        *(*op).resvalue = 0 as Datum;
        *(*op).resnull = true;
        (*jsestate).error.value = BoolGetDatum(true);
        (*jsestate).escontext.error_occurred = false;
        (*jsestate).escontext.details_wanted = true;
        return if (*jsestate).jump_error >= 0 {
            (*jsestate).jump_error
        } else {
            (*jsestate).jump_end
        };
    }

    if jump_eval_coercion >= 0 { jump_eval_coercion } else { (*jsestate).jump_end }
}

/*
 * Convert the given JsonbValue to its C string representation.
 * *resnull is set if the JsonbValue is a jbvNull.
 */
unsafe fn ExecGetJsonValueItemString(item: *mut JsonbValue, resnull: *mut bool) -> *mut c_char {
    *resnull = false;

    match (*item).r#type {
        jbvNull => {
            *resnull = true;
            return core::ptr::null_mut();
        }
        jbvString => {
            let str_val = (*item).val.string.val;
            let str_len = (*item).val.string.len as usize;
            let s = palloc(str_len + 1) as *mut c_char;
            core::ptr::copy_nonoverlapping(str_val as *const u8, s as *mut u8, str_len);
            *s.add(str_len) = 0;
            return s;
        }
        jbvNumeric => {
            return DatumGetCString(DirectFunctionCall1(numeric_out,
                                    (*item).val.numeric as Datum));
        }
        jbvBool => {
            return DatumGetCString(DirectFunctionCall1(boolout,
                                    BoolGetDatum((*item).val.boolean)));
        }
        jbvDatetime => {
            match (*item).val.datetime.typid {
                DATEOID => return DatumGetCString(DirectFunctionCall1(date_out, (*item).val.datetime.value)),
                TIMEOID => return DatumGetCString(DirectFunctionCall1(time_out, (*item).val.datetime.value)),
                TIMETZOID => return DatumGetCString(DirectFunctionCall1(timetz_out, (*item).val.datetime.value)),
                TIMESTAMPOID => return DatumGetCString(DirectFunctionCall1(timestamp_out, (*item).val.datetime.value)),
                TIMESTAMPTZOID => return DatumGetCString(DirectFunctionCall1(timestamptz_out, (*item).val.datetime.value)),
                _ => {
                    elog!(ERROR, "unexpected jsonb datetime type oid {}", (*item).val.datetime.typid);
                }
            }
        }
        jbvArray | jbvObject | jbvBinary => {
            return DatumGetCString(DirectFunctionCall1(jsonb_out,
                                    JsonbPGetDatum(JsonbValueToJsonb(item))));
        }
        _ => {
            elog!(ERROR, "unexpected jsonb value type {}", (*item).r#type);
        }
    }

    debug_assert!(false);
    *resnull = true;
    core::ptr::null_mut()
}

/*
 * Coerce a jsonb value to the target type.
 */
pub unsafe fn ExecEvalJsonCoercion(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let escontext = (*op).d.jsonexpr_coercion.escontext;

    if (*op).d.jsonexpr_coercion.exists_coerce {
        if (*op).d.jsonexpr_coercion.exists_cast_to_int {
            /* Check domain constraints if any. */
            if (*op).d.jsonexpr_coercion.exists_check_domain
                && !domain_check_safe(
                    *(*op).resvalue, *(*op).resnull,
                    (*op).d.jsonexpr_coercion.targettype,
                    &mut (*op).d.jsonexpr_coercion.json_coercion_cache,
                    (*econtext).ecxt_per_query_memory,
                    escontext as *mut crate::nodes::nodes::Node,
                )
            {
                *(*op).resnull = true;
                *(*op).resvalue = 0 as Datum;
            } else {
                *(*op).resvalue = DirectFunctionCall1(bool_int4, *(*op).resvalue);
            }
            return;
        }

        *(*op).resvalue = DirectFunctionCall1(
            jsonb_in,
            if DatumGetBool(*(*op).resvalue) {
                CStringGetDatum(b"true\0".as_ptr() as *const c_char)
            } else {
                CStringGetDatum(b"false\0".as_ptr() as *const c_char)
            },
        );
    }

    *(*op).resvalue = json_populate_type(
        *(*op).resvalue, JSONBOID,
        (*op).d.jsonexpr_coercion.targettype,
        (*op).d.jsonexpr_coercion.targettypmod,
        &mut (*op).d.jsonexpr_coercion.json_coercion_cache,
        (*econtext).ecxt_per_query_memory,
        (*op).resnull,
        (*op).d.jsonexpr_coercion.omit_quotes,
        escontext as *mut crate::nodes::nodes::Node,
    );
}

unsafe fn GetJsonBehaviorValueString(
    behavior: *mut crate::nodes::primnodes::JsonBehavior,
) -> *mut c_char {
    use crate::nodes::primnodes::JsonBehaviorType::*;
    let s: &'static str = match (*behavior).btype as c_int {
        x if x == JSON_BEHAVIOR_NULL as c_int => "NULL",
        x if x == JSON_BEHAVIOR_ERROR as c_int => "ERROR",
        x if x == JSON_BEHAVIOR_EMPTY as c_int => "EMPTY",
        x if x == JSON_BEHAVIOR_TRUE as c_int => "TRUE",
        x if x == JSON_BEHAVIOR_FALSE as c_int => "FALSE",
        x if x == JSON_BEHAVIOR_UNKNOWN as c_int => "UNKNOWN",
        x if x == JSON_BEHAVIOR_EMPTY_ARRAY as c_int => "EMPTY ARRAY",
        x if x == JSON_BEHAVIOR_EMPTY_OBJECT as c_int => "EMPTY OBJECT",
        x if x == JSON_BEHAVIOR_DEFAULT as c_int => "DEFAULT",
        _ => "UNKNOWN",
    };
    pstrdup(s.as_ptr() as *const c_char)
}

/*
 * Checks if an error occurred in ExecEvalJsonCoercion().
 */
pub unsafe fn ExecEvalJsonCoercionFinish(state: *mut ExprState, op: *mut ExprEvalStep) {
    let jsestate: *mut JsonExprState = (*op).d.jsonexpr.jsestate;

    if SOFT_ERROR_OCCURRED(&(*jsestate).escontext) {
        if DatumGetBool((*jsestate).error.value) {
            ereport!(ERROR, errmsg!(
                "could not coerce ON ERROR expression to the RETURNING type"
            ));
        } else if DatumGetBool((*jsestate).empty.value) {
            ereport!(ERROR, errmsg!(
                "could not coerce ON EMPTY expression to the RETURNING type"
            ));
        }

        *(*op).resvalue = 0 as Datum;
        *(*op).resnull = true;

        (*jsestate).error.value = BoolGetDatum(true);

        (*jsestate).escontext.error_occurred = false;
        (*jsestate).escontext.details_wanted = true;
    }
}

/*
 * Evaluate GROUPING() expression.
 *
 * Returns a bitmask with the corresponding bit set for each provided argument
 * expression that is NOT part of the current grouping set.
 */
pub unsafe fn ExecEvalGroupingFunc(state: *mut ExprState, op: *mut ExprEvalStep) {
    let aggstate: *mut AggState = (*state).parent as *mut AggState;
    let mut result: i64 = 0;
    let args: *mut crate::nodes::pg_list::List = (*op).d.grouping_func.args;
    let argno = list_length(args);
    let current_grouping_set = (*aggstate).current_set;

    for i in 0..argno as usize {
        let grplist: *mut crate::nodes::pg_list::List =
            (*op).d.grouping_func.grouped_cols;
        /* Get the grplist for the current set */
        let gc = list_nth(grplist, current_grouping_set as c_int);
        let gc_list = gc as *mut crate::nodes::pg_list::List;
        let arg_sortcol = list_nth_int(args, i as c_int);

        /* find arg_sortcol in gc_list; if not found, it's not grouped */
        if !list_member_int(gc_list, arg_sortcol) {
            result |= 1 << (argno as usize - i - 1);
        }
    }

    *(*op).resvalue = Int64GetDatum(result);
    *(*op).resnull = false;
}

/*
 * ExecEvalMergeSupportFunc
 *
 * Returns information about the current MERGE action when called in the
 * RETURNING list of a MERGE command.
 *
 * TODO(pg-port): Full implementation requires MergeActionState::mas_action::commandType.
 *   C: mtstate->mt_merge_action->mas_action->commandType
 *   MergeActionState is not yet ported; stub unimplemented for now.
 */
pub unsafe fn ExecEvalMergeSupportFunc(
    _state: *mut ExprState,
    _op: *mut ExprEvalStep,
    _econtext: *mut ExprContext,
) {
    unimplemented!("TODO(pg-port): ExecEvalMergeSupportFunc requires MergeActionState port")
}

/*
 * ExecEvalSubPlan
 *
 * Evaluate a subselect; this is a quick entry point for the main plan.
 */
pub unsafe fn ExecEvalSubPlan(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sstate: *mut SubPlanState = (*op).d.subplan.sstate;

    /* Could be NULL if subplan is degenerate */
    *(*op).resvalue = ExecSubPlan(sstate, econtext, (*op).resnull);
}

/*
 * ExecEvalWholeRowVar
 *
 * Returns a Datum for a whole-row variable.
 *
 * This is called from the main interpreter loop to handle EEOP_WHOLEROW.
 */
pub unsafe fn ExecEvalWholeRowVar(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let needslow = (*op).d.wholerow.slow;

    if needslow {
        ExecEvalWholeRowSlow(state, op, econtext);
    } else {
        /* Fast path: just copy the slot's tuple */
        let slot = (*op).d.wholerow.slot;
        let junkFilter = (*op).d.wholerow.junkFilter;
        let want_expanded = (*op).d.wholerow.give_expanded;

        /* Make sure the slot is materialized. */
        ExecMaterializeSlot(slot);

        let tuple: *mut crate::access::htup_details::MinimalTupleData = if !junkFilter.is_null() {
            /* Apply junk filter to the tuple. */
            ExecFilterJunk(junkFilter, slot) as *mut crate::access::htup_details::MinimalTupleData
        } else {
            ExecCopySlotMinimalTuple(slot)
        };

        if want_expanded {
            /* Return an expanded record. */
            let tupType: Oid;
            let tupTypmod: i32;

            if !junkFilter.is_null() {
                tupType = (*(*junkFilter).jf_cleanTupType).tdtypeid;
                tupTypmod = (*(*junkFilter).jf_cleanTupType).tdtypmod;
            } else {
                let tupdesc = (*slot).tts_tupleDescriptor;
                tupType = (*tupdesc).tdtypeid;
                tupTypmod = (*tupdesc).tdtypmod;
            }

            *(*op).resvalue = make_expanded_record_from_tuple(
                tuple as *mut HeapTuple,
                tupType, tupTypmod,
                (*econtext).ecxt_per_tuple_memory,
            );
            *(*op).resnull = false;
            return;
        }

        *(*op).resvalue = PointerGetDatum(tuple as *mut c_void);
        *(*op).resnull = false;
    }
}

/*
 * ExecEvalWholeRowSlow
 *
 * Checks that the attribute count of the tuple matches the expected tuple
 * descriptor and filters out any junk attributes.
 */
unsafe fn ExecEvalWholeRowSlow(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let slot = (*op).d.wholerow.slot;
    let output_tupdesc = (*op).d.wholerow.tupdesc;
    let oldcontext: MemoryContext;

    /* Evaluate the tuple. */
    ExecMaterializeSlot(slot);

    let input_tupdesc = (*slot).tts_tupleDescriptor;
    let resultslot = (*state).resultslot;

    /*
     * Need to project the tuple to the expected format.
     * Obtain a "clean" version via project.
     */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    let result = build_virtual_tuple(slot, output_tupdesc);

    MemoryContextSwitchTo(oldcontext);

    *(*op).resvalue = PointerGetDatum(result as *mut c_void);
    *(*op).resnull = false;
}

/*
 * ExecEvalSysVar
 *
 * Return a system attribute column of a tuple.  This is called for Var
 * nodes with varattno < 0.
 */
pub unsafe fn ExecEvalSysVar(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
    slot: *mut TupleTableSlot,
) {
    /* Collect the needed information. */
    let attnum = (*op).d.var.attnum;

    let value: Datum;
    let isnull: bool;

    /* system columns are never NULL */
    isnull = false;

    // All system column attrnums are fetched uniformly via slot_getsysattr.
    value = slot_getsysattr(slot, attnum as c_int, &mut (false));

    *(*op).resvalue = value;
    *(*op).resnull = isnull;
}

/*
 * ExecAggInitGroup
 *
 * Initialize a new aggregate group by copying the first input value as the
 * transition value.
 */
pub unsafe fn ExecAggInitGroup(
    aggstate: *mut AggState,
    pertrans: *mut AggStatePerTransData,
    pergroup: *mut AggStatePerGroupData,
    aggcontext: *mut ExprContext,
) {
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;
    let newValue = (*(*fcinfo).args.as_ptr().add(1)).value;
    let newValueIsNull = (*(*fcinfo).args.as_ptr().add(1)).isnull;
    let oldContext = MemoryContextSwitchTo((*aggcontext).ecxt_per_tuple_memory);

    /*
     * We must copy the datum into aggcontext if it is pass-by-ref.  We do
     * not need to pfree the prior transValue, since it's NULL.
     */
    (*pergroup).transValue = if !newValueIsNull {
        datumCopy(newValue, (*pertrans).transtypeByVal, (*pertrans).transtypeLen)
    } else {
        0 as Datum
    };
    (*pergroup).transValueIsNull = newValueIsNull;
    (*pergroup).noTransValue = false;
    MemoryContextSwitchTo(oldContext);
}

/*
 * ExecAggCopyTransValue
 *
 * Copy a just-computed transition value into the right memory context,
 * and update *pergroup accordingly.
 */
pub unsafe fn ExecAggCopyTransValue(
    aggstate: *mut AggState,
    pertrans: *mut AggStatePerTransData,
    pergroup: *mut AggStatePerGroupData,
    aggcontext: *mut ExprContext,
) {
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;
    // TODO(pg-port): C signature takes (newValue, newValueIsNull) as params;
    // Rust adaptation reads them from the last FunctionCallInvoke result stored
    // in args[1] (the new input). Use isnull from fcinfo; newValue from first arg.
    let newValue = (*(*fcinfo).args.as_ptr()).value;
    let newValueIsNull = (*fcinfo).isnull;

    /*
     * If pass-by-ref datatype, copy the new value into the aggcontext.
     */
    let oldContext = MemoryContextSwitchTo((*aggcontext).ecxt_per_tuple_memory);
    if !newValueIsNull {
        if !(*pertrans).transtypeByVal {
            let oldValue = (*pergroup).transValue;
            let oldIsNull = (*pergroup).transValueIsNull;

            (*pergroup).transValue = datumCopy(newValue, false, (*pertrans).transtypeLen);

            /* Free the old value if it was a pointer. */
            if !oldIsNull && !oldValue == 0 as Datum {
                pfree(DatumGetPointer(oldValue) as *mut c_void);
            }
        } else {
            (*pergroup).transValue = newValue;
        }
    } else {
        /* New value is null. Free the old value if applicable. */
        if !(*pertrans).transtypeByVal && !(*pergroup).transValueIsNull {
            pfree(DatumGetPointer((*pergroup).transValue) as *mut c_void);
        }
        (*pergroup).transValue = 0 as Datum;
    }
    (*pergroup).transValueIsNull = newValueIsNull;
    MemoryContextSwitchTo(oldContext);
}

/*
 * ExecEvalPreOrderedDistinctSingle
 *
 * Check whether the current input value for an ordered-set or hypothetical-
 * set aggregate is distinct from the previous one.  Used to implement the
 * DISTINCT option for ordered-set aggregates.
 */
pub unsafe fn ExecEvalPreOrderedDistinctSingle(
    aggstate: *mut AggState,
    pertrans: *mut AggStatePerTransData,
) -> bool {
    let fcinfo: FunctionCallInfo = (*pertrans).equalfnOne;
    let value = (*(*fcinfo).args.as_ptr()).value;
    let isnull = (*(*fcinfo).args.as_ptr()).isnull;

    let prior_value = (*(*fcinfo).args.as_ptr().add(1)).value;
    let prior_isnull = (*(*fcinfo).args.as_ptr().add(1)).isnull;

    /* Nulls are not distinct from each other */
    if isnull && prior_isnull {
        return false;
    }
    /* A null is distinct from any non-null */
    if isnull != prior_isnull {
        return true;
    }

    /* Both non-null: apply the equality function */
    (*fcinfo).isnull = false;
    let equal = (*pertrans).equalfnOneAddr.unwrap()(fcinfo);
    if !DatumGetBool(equal) {
        return true;
    }

    false
}

/*
 * ExecEvalPreOrderedDistinctMulti
 *
 * Like ExecEvalPreOrderedDistinctSingle, but for multi-column orderings.
 */
pub unsafe fn ExecEvalPreOrderedDistinctMulti(
    aggstate: *mut AggState,
    pertrans: *mut AggStatePerTransData,
) -> bool {
    let numDistinctCols = (*pertrans).numDistinctCols;

    for i in 0..numDistinctCols as usize {
        let isnull1 = (*(*pertrans).sortslot).tts_isnull.add(i);
        let isnull2 = (*(*pertrans).uniqslot).tts_isnull.add(i);
        let val1 = (*(*pertrans).sortslot).tts_values.add(i);
        let val2 = (*(*pertrans).uniqslot).tts_values.add(i);
        let fcinfo = (*pertrans).equalfnMulti.add(i);

        /* Nulls are not distinct from each other */
        if *isnull1 && *isnull2 {
            continue;
        }
        /* A null is distinct from any non-null */
        if *isnull1 != *isnull2 {
            return true;
        }

        (*(*fcinfo).args.as_mut_ptr()).value = *val1;
        (*(*fcinfo).args.as_mut_ptr()).isnull = *isnull1;
        (*(*fcinfo).args.as_mut_ptr().add(1)).value = *val2;
        (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = *isnull2;
        (*fcinfo).isnull = false;

        let fn_ptr = *(*pertrans).equalfnMultiAddr.add(i);
        let equal = fn_ptr.unwrap_or(none_fn)(fcinfo);
        if !DatumGetBool(equal) {
            return true;
        }
    }

    false
}

/*
 * ExecEvalAggOrderedTransDatum
 *
 * Invoke the transition function on a single datum value. Used for
 * ordered-set aggregates.
 */
pub unsafe fn ExecEvalAggOrderedTransDatum(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let pertrans: *mut AggStatePerTransData = (*op).d.agg_trans.pertrans;
    let setno = (*op).d.agg_trans.setno;

    /* store the input value in the sort object */
    tuplesort_putdatum(
        *(*pertrans).sortstates.add(setno as usize) as *mut c_void,
        *(*op).resvalue,
        *(*op).resnull,
    );
}

/*
 * ExecEvalAggOrderedTransTuple
 *
 * Invoke the transition function on a whole tuple value.
 */
pub unsafe fn ExecEvalAggOrderedTransTuple(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let pertrans: *mut AggStatePerTransData = (*op).d.agg_trans.pertrans;
    let setno = (*op).d.agg_trans.setno;

    ExecClearTuple((*pertrans).sortslot);
    (*pertrans).sortslot = (*econtext).ecxt_outertuple;

    tuplesort_puttupleslot(
        *(*pertrans).sortstates.add(setno as usize) as *mut c_void,
        (*pertrans).sortslot,
    );
}

/*
 * Transition value advance for a plain (non-ordered-set) aggregate.
 * The transition value is pass-by-value.
 */
pub unsafe fn ExecAggPlainTransByVal(
    aggstate: *mut AggState,
    pertrans: *mut AggStatePerTransData,
    pergroup: *mut AggStatePerGroupData,
    aggcontext: *mut ExprContext,
    setno: c_int,
) {
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;

    /* use the appropriate current input slot */
    debug_assert_eq!(setno, (*pertrans).aggref_set);

    /* set up current transition value in fcinfo */
    (*(*fcinfo).args.as_mut_ptr()).value = (*pergroup).transValue;
    (*(*fcinfo).args.as_mut_ptr()).isnull = (*pergroup).transValueIsNull;

    let newVal = FunctionCallInvoke(fcinfo);

    /* for pass-by-val types, just update the transValue */
    (*pergroup).transValue = newVal;
    (*pergroup).transValueIsNull = (*fcinfo).isnull;
}

/*
 * Transition value advance for a plain (non-ordered-set) aggregate.
 * The transition value is pass-by-reference.
 */
pub unsafe fn ExecAggPlainTransByRef(
    aggstate: *mut AggState,
    pertrans: *mut AggStatePerTransData,
    pergroup: *mut AggStatePerGroupData,
    aggcontext: *mut ExprContext,
    setno: c_int,
) {
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;
    let oldContext: MemoryContext;

    /* use the appropriate current input slot */
    debug_assert_eq!(setno, (*pertrans).aggref_set);

    /* set up current transition value in fcinfo */
    (*(*fcinfo).args.as_mut_ptr()).value = (*pergroup).transValue;
    (*(*fcinfo).args.as_mut_ptr()).isnull = (*pergroup).transValueIsNull;

    /* call the transition function */
    oldContext = MemoryContextSwitchTo((*aggcontext).ecxt_per_tuple_memory);
    let newVal = FunctionCallInvoke(fcinfo);
    MemoryContextSwitchTo(oldContext);

    /*
     * If the function returned a pointer to its first input, we don't need
     * to do anything; the agg state has been updated in-place.
     */
    if DatumGetPointer(newVal) != DatumGetPointer((*pergroup).transValue) {
        /*
         * New value is different.  If the new value is a pass-by-ref type,
         * copy it into the right context and free the old value (if it was
         * pass-by-ref and not null).
         */
        let oldIsNull = (*pergroup).transValueIsNull;
        let oldValue = (*pergroup).transValue;

        if !(*fcinfo).isnull {
            let saveContext = MemoryContextSwitchTo((*aggcontext).ecxt_per_tuple_memory);
            (*pergroup).transValue = datumCopy(newVal, false, (*pertrans).transtypeLen);
            MemoryContextSwitchTo(saveContext);
        } else {
            (*pergroup).transValue = 0 as Datum;
        }
        (*pergroup).transValueIsNull = (*fcinfo).isnull;

        /* Free old pass-by-ref value if any. */
        if !oldIsNull {
            pfree(DatumGetPointer(oldValue) as *mut c_void);
        }
    }
}

//! misc.rs
//! Translated 1:1 from postgres/src/backend/utils/adt/misc.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/misc.c

use crate::prelude::*;

// PG_RETURN_* / PG_GETARG_* / FunctionCall macros are #[macro_export]ed at the
// crate root (utils/fmgr.rs); import the ones used below.
use crate::{
    PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_FLOAT8, PG_GETARG_INT16,
    PG_GETARG_OID, PG_GETARG_TEXT_PP, PG_GET_COLLATION, PG_NARGS, PG_ARGISNULL, PG_RETURN_BOOL,
    PG_RETURN_DATUM, PG_RETURN_INT32, PG_RETURN_NAME, PG_RETURN_NULL, PG_RETURN_OID,
    PG_RETURN_TEXT_P, PG_RETURN_VOID, Assert, elog, ereport, errmsg,
};

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, int64, float8, text, Name};
use crate::postgres_ext::Oid;
use crate::utils::fmgr::{FunctionCallInfo, FmgrInfo};
use crate::utils::adt::varlena::{cstring_to_text, cstring_to_text_with_len, text_to_cstring};

// ---------------------------------------------------------------------------
// Type aliases for not-yet-ported (or opaque) C types.
// ---------------------------------------------------------------------------

use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::utils::rel::Relation;
use crate::utils::array::ArrayType;
use crate::utils::adt::arrayfuncs::ArrayBuildState;
use crate::utils::fmgr::funcapi::{FuncCallContext, AttInMetadata};
use crate::nodes::execnodes::ReturnSetInfo;
type Node = c_void;
type AttrNumber = int16;
type bits8 = u8;

// FILE / DIR / dirent: DIR/dirent come from storage/file/fd.rs (canonical).
type FILE = c_void;
use crate::storage::file::fd::{DIR, dirent};
#[repr(C)]
struct stat_t {
    st_mode: u32,
}

// ErrorData / ErrorSaveContext (nodes/miscnodes.h, utils/elog.h).  We only touch
// a handful of fields, accessed through stub accessors below.
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::utils::error::elog_impl::ErrorData;

/*
 * structure to cache metadata needed in pg_input_is_valid_common
 */
#[repr(C)]
struct ValidIOData {
    typoid: Oid,
    typmod: int32,
    typname_constant: bool,
    typiofunc: Oid,
    typioparam: Oid,
    inputproc: FmgrInfo,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const InvalidOid: Oid = 0;
const GLOBALTABLESPACE_OID: Oid = 1664;
const DEFAULTTABLESPACE_OID: Oid = 1663;
const TEXTOID: Oid = 25;
const UNKNOWNOID: Oid = 705;
const TYPTYPE_DOMAIN: c_char = b'd' as c_char;

const MAXPGPATH: usize = crate::pg_config_manual::MAXPGPATH;
const NAMEDATALEN: usize = crate::pg_config_manual::NAMEDATALEN;

const ENOENT: c_int = 2;

const FirstLowInvalidHeapAttributeNumber: AttrNumber =
    crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;

// CmdType (nodes/nodes.h)
const CMD_UPDATE: c_int = 2;
const CMD_DELETE: c_int = 4;
// #define REQ_EVENTS ((1 << CMD_UPDATE) | (1 << CMD_DELETE))
const REQ_EVENTS: c_int = (1 << CMD_UPDATE) | (1 << CMD_DELETE);

// SearchSysCache cache ids (utils/syscache.h)
const TYPEOID: c_int = 82;

// TYPEFUNC_COMPOSITE (funcapi.h)
const TYPEFUNC_COMPOSITE: c_int = 1;

// MAT_SRF flags (funcapi.h)
const MAT_SRF_USE_EXPECTED_DESC: c_int = 0x01;

// keyword categories (common/kwlookup.h)
const UNRESERVED_KEYWORD: c_int = 0;
const COL_NAME_KEYWORD: c_int = 1;
const TYPE_FUNC_NAME_KEYWORD: c_int = 2;
const RESERVED_KEYWORD: c_int = 3;

// lock modes (storage/lockdefs.h)
const AccessShareLock: c_int = 1;

// T_ErrorSaveContext node tag
const T_ErrorSaveContext: c_int = 447;

// NIL (pg_list.h)
const NIL: *mut c_void = std::ptr::null_mut();

// WaitLatch event bits (storage/latch.h)
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 2;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;
const WAIT_EVENT_PG_SLEEP: u32 = 0;

// path strings (catalog/pg_tablespace.h / common/relpath.h)
const PG_TBLSPC_DIR: *const c_char = c"pg_tblspc".as_ptr();
const TABLESPACE_VERSION_DIRECTORY: *const c_char = c"PG_VERSION".as_ptr();
const LOG_METAINFO_DATAFILE: *const c_char = c"current_logfiles".as_ptr();

// F_ARRAY_IN (utils/fmgroids.h)
const F_ARRAY_IN: Oid = 750;

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn ceil(x: f64) -> f64;
}

// ---------------------------------------------------------------------------
// Local stubs / accessors for dependencies not yet ported.
// ---------------------------------------------------------------------------

// errno access (errno.h)
unsafe fn get_errno() -> c_int {
    *libc::__error()
}

// miscadmin.h globals
unsafe fn MyDatabaseId() -> Oid {
    crate::utils::init::globals::MyDatabaseId
}
unsafe fn MyDatabaseTableSpace() -> Oid {
    crate::utils::init::globals::MyDatabaseTableSpace
}
unsafe fn MyLatch() -> *mut c_void {
    crate::miscadmin::MyLatch as *mut c_void
}
unsafe fn debug_query_string() -> *const c_char {
    crate::tcop::postgres::debug_query_string
}

// commands/dbcommands.c
unsafe fn get_database_name(dbid: Oid) -> *mut c_char {
    crate::commands::dbcommands::get_database_name(dbid)
}

// utils/adt/name.c - namestrcpy
unsafe fn namestrcpy(name: Name, str: *const c_char) -> c_int {
    crate::utils::adt::name::namestrcpy(name as _, str);
    0
}

// utils/builtins.h - psprintf (variadic in C; we expose the fixed-arg forms used)
unsafe fn psprintf_loc(location: *const c_char, oid: Oid, version: *const c_char) -> *mut c_char {
    let buf = palloc(MAXPGPATH as Size) as *mut c_char;
    snprintf(buf, MAXPGPATH, c"%s/%u/%s".as_ptr(), location, oid, version);
    buf
}
unsafe fn psprintf_subdir(location: *const c_char, d_name: *const c_char) -> *mut c_char {
    let buf = palloc(MAXPGPATH as Size) as *mut c_char;
    snprintf(buf, MAXPGPATH, c"%s/%s".as_ptr(), location, d_name);
    buf
}

// storage/file/fd.c
unsafe fn AllocateDir(dirname: *const c_char) -> *mut DIR {
    crate::storage::file::fd::AllocateDir(dirname)
}
unsafe fn ReadDir(dir: *mut DIR, dirname: *const c_char) -> *mut dirent {
    crate::storage::file::fd::ReadDir(dir, dirname)
}
unsafe fn FreeDir(dir: *mut DIR) -> c_int {
    crate::storage::file::fd::FreeDir(dir)
}
unsafe fn dirent_d_name(de: *mut dirent) -> *mut c_char {
    (*de).d_name.as_mut_ptr()
}
unsafe fn directory_is_empty(_path: *const c_char) -> bool { crate::commands::tablespace::directory_is_empty(_path as _) }
unsafe fn AllocateFile(name: *const c_char, mode: *const c_char) -> *mut FILE {
    crate::storage::file::fd::AllocateFile(name, mode)
}
unsafe fn FreeFile(file: *mut FILE) -> c_int {
    crate::storage::file::fd::FreeFile(file)
}
unsafe fn fgets(buf: *mut c_char, n: c_int, file: *mut FILE) -> *mut c_char {
    libc::fgets(buf, n, file as *mut libc::FILE)
}

// utils/adt/oid.c - atooid
unsafe fn atooid(s: *const c_char) -> Oid {
    let cs = std::ffi::CStr::from_ptr(s);
    crate::postgres_ext::atooid(cs.to_str().unwrap_or(""))
}

// catalog/objectaddress.c style helpers not used directly here.

// filesystem syscalls (sys/stat.h, unistd.h)
unsafe fn lstat(path: *const c_char, st: *mut stat_t) -> c_int {
    let mut s: libc::stat = core::mem::zeroed();
    let rc = libc::lstat(path, &mut s);
    (*st).st_mode = s.st_mode as u32;
    rc
}
unsafe fn readlink(path: *const c_char, buf: *mut c_char, bufsiz: usize) -> isize {
    libc::readlink(path, buf, bufsiz)
}
unsafe fn S_ISLNK(mode: u32) -> bool {
    (mode & libc::S_IFMT as u32) == libc::S_IFLNK as u32
}

// utils/adt/timestamp.c
unsafe fn GetCurrentTimestamp() -> int64 {
    crate::utils::adt::timestamp::GetCurrentTimestamp() as int64
}

// storage/ipc/latch.c
unsafe fn WaitLatch(
    latch: *mut c_void,
    wakeEvents: c_int,
    timeout: i64,
    wait_event_info: u32,
) -> c_int {
    crate::storage::ipc::latch::WaitLatch(latch as _, wakeEvents, timeout as _, wait_event_info)
}
unsafe fn ResetLatch(latch: *mut c_void) {
    crate::storage::ipc::latch::ResetLatch(latch as _)
}

// CHECK_FOR_INTERRUPTS (miscadmin.h)
unsafe fn CHECK_FOR_INTERRUPTS() {
    crate::miscadmin::CHECK_FOR_INTERRUPTS()
}

// funcapi.h SRF machinery
unsafe fn SRF_IS_FIRSTCALL(fcinfo: FunctionCallInfo) -> bool {
    (*(*fcinfo).flinfo).fn_extra.is_null()
}
unsafe fn SRF_FIRSTCALL_INIT(fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    crate::utils::fmgr::funcapi::init_MultiFuncCall(fcinfo)
}
unsafe fn SRF_PERCALL_SETUP(fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    crate::utils::fmgr::funcapi::per_MultiFuncCall(fcinfo)
}
unsafe fn SRF_RETURN_NEXT(fcinfo: FunctionCallInfo, funcctx: *mut FuncCallContext, result: Datum) -> Datum {
    let rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    (*funcctx).call_cntr += 1;
    (*rsi).isDone = crate::nodes::execnodes::ExprMultipleResult;
    result
}
unsafe fn SRF_RETURN_DONE(fcinfo: FunctionCallInfo, funcctx: *mut FuncCallContext) -> Datum {
    let rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    crate::utils::fmgr::funcapi::end_MultiFuncCall(fcinfo, funcctx);
    (*rsi).isDone = crate::nodes::execnodes::ExprEndResult;
    PointerGetDatum(std::ptr::null())
}

// FuncCallContext field accessors (funcapi.h)
unsafe fn funcctx_multi_call_memory_ctx(funcctx: *mut FuncCallContext) -> MemoryContext {
    (*funcctx).multi_call_memory_ctx
}
unsafe fn set_funcctx_tuple_desc(funcctx: *mut FuncCallContext, tupdesc: TupleDesc) {
    (*funcctx).tuple_desc = tupdesc;
}
unsafe fn funcctx_tuple_desc(funcctx: *mut FuncCallContext) -> TupleDesc {
    (*funcctx).tuple_desc
}
unsafe fn set_funcctx_attinmeta(funcctx: *mut FuncCallContext, attinmeta: *mut AttInMetadata) {
    (*funcctx).attinmeta = attinmeta;
}
unsafe fn funcctx_attinmeta(funcctx: *mut FuncCallContext) -> *mut AttInMetadata {
    (*funcctx).attinmeta
}
unsafe fn set_funcctx_user_fctx(funcctx: *mut FuncCallContext, user_fctx: *mut c_void) {
    (*funcctx).user_fctx = user_fctx;
}
unsafe fn funcctx_user_fctx(funcctx: *mut FuncCallContext) -> *mut c_void {
    (*funcctx).user_fctx
}
unsafe fn funcctx_call_cntr(funcctx: *mut FuncCallContext) -> u64 {
    (*funcctx).call_cntr
}

unsafe fn get_call_result_type(
    fcinfo: FunctionCallInfo,
    resultTypeId: *mut Oid,
    resultTupleDesc: *mut TupleDesc,
) -> c_int {
    core::mem::transmute(crate::utils::fmgr::funcapi::get_call_result_type(
        fcinfo, resultTypeId, resultTupleDesc,
    ))
}
unsafe fn TupleDescGetAttInMetadata(tupdesc: TupleDesc) -> *mut AttInMetadata {
    crate::executor::execTuples::TupleDescGetAttInMetadata(tupdesc) as _
}
unsafe fn BlessTupleDesc(tupdesc: TupleDesc) -> TupleDesc {
    crate::executor::execTuples::BlessTupleDesc(tupdesc)
}
unsafe fn BuildTupleFromCStrings(attinmeta: *mut AttInMetadata, values: *mut *mut c_char) -> HeapTuple {
    crate::executor::execTuples::BuildTupleFromCStrings(attinmeta as _, values)
}
unsafe fn HeapTupleGetDatum(tuple: HeapTuple) -> Datum {
    crate::executor::execTuples::HeapTupleHeaderGetDatum((*tuple).t_data as _)
}
unsafe fn heap_form_tuple(desc: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple {
    crate::access::common::heaptuple::heap_form_tuple(desc, values, isnull)
}
unsafe fn InitMaterializedSRF(fcinfo: FunctionCallInfo, flags: c_int) {
    crate::utils::fmgr::funcapi::InitMaterializedSRF(fcinfo, flags as u32)
}
unsafe fn tuplestore_putvalues(
    state: *mut c_void,
    tdesc: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) {
    crate::utils::sort::tuplestore::tuplestore_putvalues(state as _, tdesc, values, isnull)
}
unsafe fn fcinfo_resultinfo(fcinfo: FunctionCallInfo) -> *mut ReturnSetInfo {
    (*fcinfo).resultinfo as *mut ReturnSetInfo
}
unsafe fn rsinfo_setResult(rsinfo: *mut ReturnSetInfo) -> *mut c_void {
    (*rsinfo).setResult as *mut c_void
}
unsafe fn rsinfo_setDesc(rsinfo: *mut ReturnSetInfo) -> TupleDesc {
    (*rsinfo).setDesc
}

// fmgr.h argument-expression introspection (used outside of macros here)
unsafe fn get_fn_expr_argtype(flinfo: *mut FmgrInfo, argnum: c_int) -> Oid {
    crate::utils::fmgr::get_fn_expr_argtype(flinfo, argnum)
}
unsafe fn get_fn_expr_variadic(flinfo: *mut FmgrInfo) -> bool {
    crate::utils::fmgr::get_fn_expr_variadic(flinfo)
}
unsafe fn get_fn_expr_arg_stable(flinfo: *mut FmgrInfo, argnum: c_int) -> bool {
    crate::utils::fmgr::get_fn_expr_arg_stable(flinfo, argnum)
}
unsafe fn get_base_element_type(typid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_base_element_type(typid)
}
unsafe fn flinfo_of(fcinfo: FunctionCallInfo) -> *mut FmgrInfo {
    (*fcinfo).flinfo
}
unsafe fn flinfo_fn_extra(flinfo: *mut FmgrInfo) -> *mut c_void {
    (*flinfo).fn_extra
}
unsafe fn set_flinfo_fn_extra(flinfo: *mut FmgrInfo, extra: *mut c_void) {
    (*flinfo).fn_extra = extra;
}
unsafe fn flinfo_fn_mcxt(flinfo: *mut FmgrInfo) -> MemoryContext {
    (*flinfo).fn_mcxt
}

// utils/array.h
unsafe fn ARR_NDIM(arr: *mut ArrayType) -> c_int {
    crate::utils::array::ARR_NDIM(arr)
}
unsafe fn ARR_DIMS(arr: *mut ArrayType) -> *mut c_int {
    crate::utils::array::ARR_DIMS(arr)
}
unsafe fn ARR_NULLBITMAP(arr: *mut ArrayType) -> *mut bits8 {
    crate::utils::array::ARR_NULLBITMAP(arr)
}
unsafe fn ArrayGetNItems(ndim: c_int, dims: *mut c_int) -> c_int {
    crate::utils::adt::arrayutils::ArrayGetNItems(ndim, dims)
}

// utils/array.h ArrayBuildState helpers
unsafe fn accumArrayResult(
    astate: *mut ArrayBuildState,
    dvalue: Datum,
    disnull: bool,
    element_type: Oid,
    rcontext: MemoryContext,
) -> *mut ArrayBuildState {
    crate::utils::adt::arrayfuncs::accumArrayResult(astate, dvalue, disnull, element_type, rcontext)
}
unsafe fn makeArrayResult(astate: *mut ArrayBuildState, rcontext: MemoryContext) -> Datum {
    crate::utils::adt::arrayfuncs::makeArrayResult(astate, rcontext)
}

// utils/builtins.h CStringGetTextDatum / CStringGetDatum-related
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    PointerGetDatum(cstring_to_text(s) as *const c_void)
}

// utils/cache/syscache.c
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(cacheId, key1) as _
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(tuple as _)
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
// GETSTRUCT(tup) for pg_type: we read typtype and typbasetype through accessors.
unsafe fn pg_type_typtype(tup: HeapTuple) -> c_char {
    let form = crate::access::htup_details::GETSTRUCT(tup as _)
        as *mut crate::catalog::pg_type::FormData_pg_type;
    (*form).typtype
}
unsafe fn pg_type_typbasetype(tup: HeapTuple) -> Oid {
    let form = crate::access::htup_details::GETSTRUCT(tup as _)
        as *mut crate::catalog::pg_type::FormData_pg_type;
    (*form).typbasetype
}

// utils/adt/ruleutils.c / lsyscache.c
unsafe fn type_is_collatable(typid: Oid) -> bool {
    crate::utils::cache::lsyscache::type_is_collatable(typid)
}
unsafe fn generate_collation_name(_collid: Oid) -> *mut c_char { unimplemented!() }
unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(type_oid)
}

// rewrite/rewriteHandler.c
unsafe fn relation_is_updatable(
    reloid: Oid,
    outer_reloids: *mut c_void,
    include_triggers: bool,
    include_cols: *mut c_void,
) -> c_int {
    crate::rewrite::rewriteHandler::relation_is_updatable(
        reloid,
        outer_reloids as _,
        include_triggers,
        include_cols as _,
    )
}

// nodes/bitmapset.c
unsafe fn bms_make_singleton(x: c_int) -> *mut c_void {
    crate::nodes::bitmapset::bms_make_singleton(x) as *mut c_void
}

// parser/parse_type.c
unsafe fn parseTypeString(
    str: *const c_char,
    typeid_p: *mut Oid,
    typmod_p: *mut int32,
    escontext: *mut Node,
) {
    crate::parser::parse_type::parseTypeString(str, typeid_p, typmod_p, escontext as _);
}
// utils/cache/lsyscache.c
unsafe fn getTypeInputInfo(type_: Oid, typInput: *mut Oid, typIOParam: *mut Oid) {
    crate::utils::cache::lsyscache::getTypeInputInfo(type_, typInput, typIOParam)
}
// fmgr.c
unsafe fn fmgr_info(functionId: Oid, finfo: *mut FmgrInfo) {
    crate::utils::fmgr::fmgr_info(functionId, finfo)
}
unsafe fn fmgr_info_cxt(functionId: Oid, finfo: *mut FmgrInfo, mcxt: MemoryContext) {
    crate::utils::fmgr::fmgr_info_cxt(functionId, finfo, mcxt)
}
unsafe fn InputFunctionCallSafe(
    flinfo: *mut FmgrInfo,
    str: *mut c_char,
    typioparam: Oid,
    typmod: int32,
    escontext: *mut Node,
    result: *mut Datum,
) -> bool {
    crate::utils::fmgr::InputFunctionCallSafe(flinfo, str, typioparam, typmod, escontext as _, result)
}

// FunctionCall3 (fmgr.h) - call with default collation.
unsafe fn FunctionCall3(flinfo: *mut FmgrInfo, arg1: Datum, arg2: Datum, arg3: Datum) -> Datum {
    crate::utils::fmgr::FunctionCall3Coll(flinfo, InvalidOid, arg1, arg2, arg3)
}

// parser/scansup.c
unsafe fn scanner_isspace(ch: c_char) -> bool {
    crate::parser::scansup::scanner_isspace(ch)
}
unsafe fn downcase_identifier(
    ident: *const c_char,
    len: c_int,
    warn: bool,
    truncate: bool,
) -> *mut c_char {
    crate::parser::scansup::downcase_identifier(ident, len, warn, truncate)
}

// access/table/table.c
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::table::table::table_open(relationId, lockmode as _) as _
}
unsafe fn table_close(relation: Relation, lockmode: c_int) {
    crate::access::table::table::table_close(relation as _, lockmode as _)
}
unsafe fn RelationGetReplicaIndex(relation: Relation) -> Oid {
    crate::utils::cache::relcache::RelationGetReplicaIndex(relation as _)
}

// utils/adt/misc.c keyword tables (common/keywords.c, common/kwlookup.c)
use crate::common::kwlookup::ScanKeywordList;
unsafe fn ScanKeywords_num_keywords() -> c_int {
    crate::common::keywords::ScanKeywords.num_keywords
}
unsafe fn GetScanKeyword(n: c_int, keywords: *const ScanKeywordList) -> *const c_char {
    crate::common::kwlookup::GetScanKeyword(n, keywords)
}
unsafe fn ScanKeywords() -> *const ScanKeywordList {
    &raw const crate::common::keywords::ScanKeywords
}
unsafe fn ScanKeywordCategories(n: c_int) -> c_int {
    crate::common::keywords::ScanKeywordCategories[n as usize] as c_int
}
unsafe fn ScanKeywordBareLabel(n: c_int) -> bool {
    crate::common::keywords::ScanKeywordBareLabel[n as usize]
}

// catalog/system_fk_info.h
#[repr(C)]
struct SysFKRelationship {
    fk_table: Oid,
    fk_columns: *const c_char,
    pk_table: Oid,
    pk_columns: *const c_char,
    is_array: bool,
    is_opt: bool,
}
unsafe fn sys_fk_relationships_len() -> c_int {
    unimplemented!() // TODO(pg-port): catalog/system_fk_info.h lengthof(sys_fk_relationships)
}
unsafe fn sys_fk_relationships(_idx: c_int) -> *const SysFKRelationship {
    unimplemented!() // TODO(pg-port): catalog/system_fk_info.h sys_fk_relationships[]
}

// utils/error/elog.c - unpack_sql_state
unsafe fn unpack_sql_state(sql_state: c_int) -> *mut c_char {
    crate::utils::error::elog_impl::unpack_sql_state(sql_state) as *mut c_char
}

// ErrorSaveContext / ErrorData field accessors (nodes/miscnodes.h, utils/elog.h)
unsafe fn escontext_set_details_wanted(_escontext: *mut ErrorSaveContext, _val: bool) {
    (*_escontext).details_wanted = _val;
}
unsafe fn escontext_error_occurred(_escontext: *mut ErrorSaveContext) -> bool {
    (*_escontext).error_occurred
}
unsafe fn escontext_error_data(_escontext: *mut ErrorSaveContext) -> *mut ErrorData {
    (*_escontext).error_data as *mut ErrorData
}
unsafe fn errordata_message(_ed: *mut ErrorData) -> *mut c_char {
    (*_ed).message
}
unsafe fn errordata_detail(_ed: *mut ErrorData) -> *mut c_char {
    (*_ed).detail
}
unsafe fn errordata_hint(_ed: *mut ErrorData) -> *mut c_char {
    (*_ed).hint
}
unsafe fn errordata_sqlerrcode(_ed: *mut ErrorData) -> c_int {
    (*_ed).sqlerrcode
}

// _() gettext no-op passthrough
#[inline]
unsafe fn gettext_(s: *const c_char) -> *const c_char {
    s
}

// PG_GETARG_ARRAYTYPE_P(n) == DatumGetArrayTypeP(PG_GETARG_DATUM(n)); provide the detoaster.
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    crate::utils::fmgr::pg_detoast_datum(d as *mut _) as *mut ArrayType
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/*
 * Common subroutine for num_nulls() and num_nonnulls().
 * Returns true if successful, false if function should return NULL.
 * If successful, total argument count and number of nulls are
 * returned into *nargs and *nulls.
 */
unsafe fn count_nulls(fcinfo: FunctionCallInfo, nargs: *mut int32, nulls: *mut int32) -> bool {
    let mut count: int32 = 0;
    let mut i: c_int;

    /* Did we get a VARIADIC array argument, or separate arguments? */
    if get_fn_expr_variadic(flinfo_of(fcinfo)) {
        let arr: *mut ArrayType;
        let ndims: c_int;
        let nitems: c_int;
        let dims: *mut c_int;

        Assert!((PG_NARGS!(fcinfo) as c_int) == 1);

        /*
         * If we get a null as VARIADIC array argument, we can't say anything
         * useful about the number of elements, so return NULL.  This behavior
         * is consistent with other variadic functions - see concat_internal.
         */
        if PG_ARGISNULL!(fcinfo, 0) {
            return false;
        }

        /*
         * Non-null argument had better be an array.  We assume that any call
         * context that could let get_fn_expr_variadic return true will have
         * checked that a VARIADIC-labeled parameter actually is an array.  So
         * it should be okay to just Assert that it's an array rather than
         * doing a full-fledged error check.
         */
        Assert!(OidIsValid(get_base_element_type(get_fn_expr_argtype(flinfo_of(fcinfo), 0))));

        /* OK, safe to fetch the array value */
        // PG_GETARG_ARRAYTYPE_P(0) == DatumGetArrayTypeP(PG_GETARG_DATUM(0))
        arr = DatumGetArrayTypeP(PG_GETARG_DATUM!(fcinfo, 0));

        /* Count the array elements */
        ndims = ARR_NDIM(arr);
        dims = ARR_DIMS(arr);
        nitems = ArrayGetNItems(ndims, dims);

        /* Count those that are NULL */
        let mut bitmap_p: *mut bits8 = ARR_NULLBITMAP(arr);
        if !bitmap_p.is_null() {
            let mut bitmask: c_int = 1;

            i = 0;
            while i < nitems {
                if (*bitmap_p as c_int & bitmask) == 0 {
                    count += 1;
                }

                bitmask <<= 1;
                if bitmask == 0x100 {
                    bitmap_p = bitmap_p.offset(1);
                    bitmask = 1;
                }
                i += 1;
            }
        }

        *nargs = nitems;
        *nulls = count;
    } else {
        /* Separate arguments, so just count 'em */
        i = 0;
        while i < (PG_NARGS!(fcinfo) as c_int) {
            if PG_ARGISNULL!(fcinfo, i) {
                count += 1;
            }
            i += 1;
        }

        *nargs = (PG_NARGS!(fcinfo) as c_int);
        *nulls = count;
    }

    true
}

/*
 * num_nulls()
 *	Count the number of NULL arguments
 */
// PG_FUNCTION_INFO_V1(pg_num_nulls)
pub unsafe fn pg_num_nulls(fcinfo: FunctionCallInfo) -> Datum {
    let mut nargs: int32 = 0;
    let mut nulls: int32 = 0;

    if !count_nulls(fcinfo, &mut nargs, &mut nulls) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32!(nulls)
}

/*
 * num_nonnulls()
 *	Count the number of non-NULL arguments
 */
// PG_FUNCTION_INFO_V1(pg_num_nonnulls)
pub unsafe fn pg_num_nonnulls(fcinfo: FunctionCallInfo) -> Datum {
    let mut nargs: int32 = 0;
    let mut nulls: int32 = 0;

    if !count_nulls(fcinfo, &mut nargs, &mut nulls) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32!(nargs - nulls)
}

/*
 * current_database()
 *	Expose the current database to the user
 */
// PG_FUNCTION_INFO_V1(current_database)
pub unsafe fn current_database(fcinfo: FunctionCallInfo) -> Datum {
    let db: Name;

    db = palloc(NAMEDATALEN as Size) as Name;

    namestrcpy(db, get_database_name(MyDatabaseId()));
    PG_RETURN_NAME!(db)
}

/*
 * current_query()
 *	Expose the current query to the user (useful in stored procedures)
 *	We might want to use ActivePortal->sourceText someday.
 */
// PG_FUNCTION_INFO_V1(current_query)
pub unsafe fn current_query(fcinfo: FunctionCallInfo) -> Datum {
    /* there is no easy way to access the more concise 'query_string' */
    if !debug_query_string().is_null() {
        PG_RETURN_TEXT_P!(cstring_to_text(debug_query_string()))
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

/* Function to find out which databases make use of a tablespace */
// PG_FUNCTION_INFO_V1(pg_tablespace_databases)
pub unsafe fn pg_tablespace_databases(fcinfo: FunctionCallInfo) -> Datum {
    let tablespaceOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let rsinfo: *mut ReturnSetInfo = fcinfo_resultinfo(fcinfo);
    let location: *mut c_char;
    let dirdesc: *mut DIR;
    let mut de: *mut dirent;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    if tablespaceOid == GLOBALTABLESPACE_OID {
        ereport!(WARNING, errmsg!("global tablespace never has databases"));
        /* return empty tuplestore */
        return 0 as Datum;
    }

    if tablespaceOid == DEFAULTTABLESPACE_OID {
        location = c"base".as_ptr() as *mut c_char;
    } else {
        location = psprintf_loc(PG_TBLSPC_DIR, tablespaceOid, TABLESPACE_VERSION_DIRECTORY);
    }

    dirdesc = AllocateDir(location);

    if dirdesc.is_null() {
        /* the only expected error is ENOENT */
        if get_errno() != ENOENT {
            ereport!(
                ERROR,
                errmsg!(
                    "could not open directory \"{}\": %m",
                    std::ffi::CStr::from_ptr(location).to_string_lossy()
                )
            );
        }
        ereport!(
            WARNING,
            errmsg!("{} is not a tablespace OID", tablespaceOid)
        );
        /* return empty tuplestore */
        return 0 as Datum;
    }

    loop {
        de = ReadDir(dirdesc, location);
        if de.is_null() {
            break;
        }

        let datOid: Oid = atooid(dirent_d_name(de));
        let subdir: *mut c_char;
        let isempty: bool;
        let mut values: [Datum; 1] = [0; 1];
        let mut nulls: [bool; 1] = [false; 1];

        /* this test skips . and .., but is awfully weak */
        if datOid == 0 {
            continue;
        }

        /* if database subdir is empty, don't report tablespace as used */

        subdir = psprintf_subdir(location, dirent_d_name(de));
        isempty = directory_is_empty(subdir);
        pfree(subdir as *mut c_void);

        if isempty {
            continue; /* indeed, nothing in it */
        }

        values[0] = ObjectIdGetDatum(datOid);
        nulls[0] = false;

        tuplestore_putvalues(
            rsinfo_setResult(rsinfo),
            rsinfo_setDesc(rsinfo),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    FreeDir(dirdesc);
    0 as Datum
}

/*
 * pg_tablespace_location - get location for a tablespace
 */
// PG_FUNCTION_INFO_V1(pg_tablespace_location)
pub unsafe fn pg_tablespace_location(fcinfo: FunctionCallInfo) -> Datum {
    let mut tablespaceOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut sourcepath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut targetpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let rllen: c_int;
    let mut st: stat_t = stat_t { st_mode: 0 };

    /*
     * It's useful to apply this function to pg_class.reltablespace, wherein
     * zero means "the database's default tablespace".  So, rather than
     * throwing an error for zero, we choose to assume that's what is meant.
     */
    if tablespaceOid == InvalidOid {
        tablespaceOid = MyDatabaseTableSpace();
    }

    /*
     * Return empty string for the cluster's default tablespaces
     */
    if tablespaceOid == DEFAULTTABLESPACE_OID || tablespaceOid == GLOBALTABLESPACE_OID {
        PG_RETURN_TEXT_P!(cstring_to_text(c"".as_ptr()));
    }

    /*
     * Find the location of the tablespace by reading the symbolic link that
     * is in pg_tblspc/<oid>.
     */
    snprintf(
        sourcepath.as_mut_ptr(),
        std::mem::size_of_val(&sourcepath),
        c"%s/%u".as_ptr(),
        PG_TBLSPC_DIR,
        tablespaceOid,
    );

    /*
     * Before reading the link, check if the source path is a link or a
     * junction point.  Note that a directory is possible for a tablespace
     * created with allow_in_place_tablespaces enabled.  If a directory is
     * found, a relative path to the data directory is returned.
     */
    if lstat(sourcepath.as_ptr(), &mut st) < 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not stat file \"{}\": %m",
                std::ffi::CStr::from_ptr(sourcepath.as_ptr()).to_string_lossy()
            )
        );
    }

    if !S_ISLNK(st.st_mode) {
        PG_RETURN_TEXT_P!(cstring_to_text(sourcepath.as_ptr()));
    }

    /*
     * In presence of a link or a junction point, return the path pointing to.
     */
    rllen = readlink(
        sourcepath.as_ptr(),
        targetpath.as_mut_ptr(),
        std::mem::size_of_val(&targetpath),
    ) as c_int;
    if rllen < 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not read symbolic link \"{}\": %m",
                std::ffi::CStr::from_ptr(sourcepath.as_ptr()).to_string_lossy()
            )
        );
    }
    if rllen as usize >= std::mem::size_of_val(&targetpath) {
        ereport!(
            ERROR,
            errmsg!(
                "symbolic link \"{}\" target is too long",
                std::ffi::CStr::from_ptr(sourcepath.as_ptr()).to_string_lossy()
            )
        );
    }
    targetpath[rllen as usize] = b'\0' as c_char;

    PG_RETURN_TEXT_P!(cstring_to_text(targetpath.as_ptr()))
}

/*
 * pg_sleep - delay for N seconds
 */
// PG_FUNCTION_INFO_V1(pg_sleep)
pub unsafe fn pg_sleep(fcinfo: FunctionCallInfo) -> Datum {
    let secs: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let endtime: float8;

    /*
     * We sleep using WaitLatch, to ensure that we'll wake up promptly if an
     * important signal (such as SIGALRM or SIGINT) arrives.  Because
     * WaitLatch's upper limit of delay is INT_MAX milliseconds, and the user
     * might ask for more than that, we sleep for at most 10 minutes and then
     * loop.
     *
     * By computing the intended stop time initially, we avoid accumulation of
     * extra delay across multiple sleeps.  This also ensures we won't delay
     * less than the specified time when WaitLatch is terminated early by a
     * non-query-canceling signal such as SIGHUP.
     */
    // #define GetNowFloat()	((float8) GetCurrentTimestamp() / 1000000.0)

    endtime = GetNowFloat() + secs;

    loop {
        let delay: float8;
        let delay_ms: i64;

        CHECK_FOR_INTERRUPTS();

        delay = endtime - GetNowFloat();
        if delay >= 600.0 {
            delay_ms = 600000;
        } else if delay > 0.0 {
            delay_ms = ceil(delay * 1000.0) as i64;
        } else {
            break;
        }

        let _ = WaitLatch(
            MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            delay_ms,
            WAIT_EVENT_PG_SLEEP,
        );
        ResetLatch(MyLatch());
    }

    PG_RETURN_VOID!()
}

// GetNowFloat() == ((float8) GetCurrentTimestamp() / 1000000.0)
#[inline]
unsafe fn GetNowFloat() -> float8 {
    GetCurrentTimestamp() as float8 / 1000000.0
}

/* Function to return the list of grammar keywords */
// PG_FUNCTION_INFO_V1(pg_get_keywords)
pub unsafe fn pg_get_keywords(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;

    if SRF_IS_FIRSTCALL(fcinfo) {
        let oldcontext: MemoryContext;
        let mut tupdesc: TupleDesc = std::ptr::null_mut();

        let funcctx = SRF_FIRSTCALL_INIT(fcinfo);
        oldcontext = MemoryContextSwitchTo(funcctx_multi_call_memory_ctx(funcctx));

        if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
            elog!(ERROR, "return type must be a row type");
        }
        set_funcctx_tuple_desc(funcctx, tupdesc);
        set_funcctx_attinmeta(funcctx, TupleDescGetAttInMetadata(tupdesc));

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP(fcinfo);

    if (funcctx_call_cntr(funcctx) as c_int) < ScanKeywords_num_keywords() {
        let mut values: [*mut c_char; 5] = [std::ptr::null_mut(); 5];
        let tuple: HeapTuple;
        let cntr = funcctx_call_cntr(funcctx) as c_int;

        /* cast-away-const is ugly but alternatives aren't much better */
        values[0] = GetScanKeyword(cntr, ScanKeywords()) as *mut c_char;

        match ScanKeywordCategories(cntr) {
            UNRESERVED_KEYWORD => {
                values[1] = c"U".as_ptr() as *mut c_char;
                values[3] = gettext_(c"unreserved".as_ptr()) as *mut c_char;
            }
            COL_NAME_KEYWORD => {
                values[1] = c"C".as_ptr() as *mut c_char;
                values[3] = gettext_(c"unreserved (cannot be function or type name)".as_ptr()) as *mut c_char;
            }
            TYPE_FUNC_NAME_KEYWORD => {
                values[1] = c"T".as_ptr() as *mut c_char;
                values[3] = gettext_(c"reserved (can be function or type name)".as_ptr()) as *mut c_char;
            }
            RESERVED_KEYWORD => {
                values[1] = c"R".as_ptr() as *mut c_char;
                values[3] = gettext_(c"reserved".as_ptr()) as *mut c_char;
            }
            _ => {
                /* shouldn't be possible */
                values[1] = std::ptr::null_mut();
                values[3] = std::ptr::null_mut();
            }
        }

        if ScanKeywordBareLabel(cntr) {
            values[2] = c"true".as_ptr() as *mut c_char;
            values[4] = gettext_(c"can be bare label".as_ptr()) as *mut c_char;
        } else {
            values[2] = c"false".as_ptr() as *mut c_char;
            values[4] = gettext_(c"requires AS".as_ptr()) as *mut c_char;
        }

        tuple = BuildTupleFromCStrings(funcctx_attinmeta(funcctx), values.as_mut_ptr());

        return SRF_RETURN_NEXT(fcinfo, funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(fcinfo, funcctx)
}

/* Function to return the list of catalog foreign key relationships */
// PG_FUNCTION_INFO_V1(pg_get_catalog_foreign_keys)
pub unsafe fn pg_get_catalog_foreign_keys(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;
    let arrayinp: *mut FmgrInfo;

    if SRF_IS_FIRSTCALL(fcinfo) {
        let oldcontext: MemoryContext;
        let mut tupdesc: TupleDesc = std::ptr::null_mut();

        let funcctx = SRF_FIRSTCALL_INIT(fcinfo);
        oldcontext = MemoryContextSwitchTo(funcctx_multi_call_memory_ctx(funcctx));

        if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
            elog!(ERROR, "return type must be a row type");
        }
        set_funcctx_tuple_desc(funcctx, BlessTupleDesc(tupdesc));

        /*
         * We use array_in to convert the C strings in sys_fk_relationships[]
         * to text arrays.  But we cannot use DirectFunctionCallN to call
         * array_in, and it wouldn't be very efficient if we could.  Fill an
         * FmgrInfo to use for the call.
         */
        let arrayinp = palloc(std::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        fmgr_info(F_ARRAY_IN, arrayinp);
        set_funcctx_user_fctx(funcctx, arrayinp as *mut c_void);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP(fcinfo);
    arrayinp = funcctx_user_fctx(funcctx) as *mut FmgrInfo;

    if (funcctx_call_cntr(funcctx) as c_int) < sys_fk_relationships_len() {
        let fkrel: *const SysFKRelationship =
            sys_fk_relationships(funcctx_call_cntr(funcctx) as c_int);
        let mut values: [Datum; 6] = [0; 6];
        let mut nulls: [bool; 6] = [false; 6];
        let tuple: HeapTuple;

        memset(nulls.as_mut_ptr() as *mut c_void, 0, std::mem::size_of_val(&nulls));

        values[0] = ObjectIdGetDatum((*fkrel).fk_table);
        values[1] = FunctionCall3(
            arrayinp,
            CStringGetDatum((*fkrel).fk_columns),
            ObjectIdGetDatum(TEXTOID),
            Int32GetDatum(-1),
        );
        values[2] = ObjectIdGetDatum((*fkrel).pk_table);
        values[3] = FunctionCall3(
            arrayinp,
            CStringGetDatum((*fkrel).pk_columns),
            ObjectIdGetDatum(TEXTOID),
            Int32GetDatum(-1),
        );
        values[4] = BoolGetDatum((*fkrel).is_array);
        values[5] = BoolGetDatum((*fkrel).is_opt);

        tuple = heap_form_tuple(funcctx_tuple_desc(funcctx), values.as_mut_ptr(), nulls.as_mut_ptr());

        return SRF_RETURN_NEXT(fcinfo, funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(fcinfo, funcctx)
}

/*
 * Return the type of the argument.
 */
// PG_FUNCTION_INFO_V1(pg_typeof)
pub unsafe fn pg_typeof(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_OID!(get_fn_expr_argtype(flinfo_of(fcinfo), 0))
}

/*
 * Return the base type of the argument.
 *		If the given type is a domain, return its base type;
 *		otherwise return the type's own OID.
 *		Return NULL if the type OID doesn't exist or points to a
 *		non-existent base type.
 *
 * This is a SQL-callable version of getBaseType().  Unlike that function,
 * we don't want to fail for a bogus type OID; this is helpful to keep race
 * conditions from turning into query failures when scanning the catalogs.
 * Hence we need our own implementation.
 */
// PG_FUNCTION_INFO_V1(pg_basetype)
pub unsafe fn pg_basetype(fcinfo: FunctionCallInfo) -> Datum {
    let mut typid: Oid = PG_GETARG_OID!(fcinfo, 0);

    /*
     * We loop to find the bottom base type in a stack of domains.
     */
    loop {
        let tup: HeapTuple;

        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid(tup) {
            PG_RETURN_NULL!(fcinfo); /* return NULL for bogus OID */
        }
        if pg_type_typtype(tup) != TYPTYPE_DOMAIN {
            /* Not a domain, so done */
            ReleaseSysCache(tup);
            break;
        }

        typid = pg_type_typbasetype(tup);
        ReleaseSysCache(tup);
    }

    PG_RETURN_OID!(typid)
}

/*
 * Implementation of the COLLATE FOR expression; returns the collation
 * of the argument.
 */
// PG_FUNCTION_INFO_V1(pg_collation_for)
pub unsafe fn pg_collation_for(fcinfo: FunctionCallInfo) -> Datum {
    let typeid: Oid;
    let collid: Oid;

    typeid = get_fn_expr_argtype(flinfo_of(fcinfo), 0);
    if typeid == 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    if !type_is_collatable(typeid) && typeid != UNKNOWNOID {
        ereport!(
            ERROR,
            errmsg!(
                "collations are not supported by type {}",
                std::ffi::CStr::from_ptr(format_type_be(typeid)).to_string_lossy()
            )
        );
    }

    collid = PG_GET_COLLATION!(fcinfo);
    if collid == 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_TEXT_P!(cstring_to_text(generate_collation_name(collid)))
}

/*
 * pg_relation_is_updatable - determine which update events the specified
 * relation supports.
 *
 * This relies on relation_is_updatable() in rewriteHandler.c, which see
 * for additional information.
 */
// PG_FUNCTION_INFO_V1(pg_relation_is_updatable)
pub unsafe fn pg_relation_is_updatable(fcinfo: FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let include_triggers: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_INT32!(relation_is_updatable(reloid, NIL, include_triggers, std::ptr::null_mut()))
}

/*
 * pg_column_is_updatable - determine whether a column is updatable
 *
 * This function encapsulates the decision about just what
 * information_schema.columns.is_updatable actually means.  It's not clear
 * whether deletability of the column's relation should be required, so
 * we want that decision in C code where we could change it without initdb.
 */
// PG_FUNCTION_INFO_V1(pg_column_is_updatable)
pub unsafe fn pg_column_is_updatable(fcinfo: FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let attnum: AttrNumber = PG_GETARG_INT16!(fcinfo, 1);
    let col: AttrNumber = attnum - FirstLowInvalidHeapAttributeNumber;
    let include_triggers: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let events: c_int;

    /* System columns are never updatable */
    if attnum <= 0 {
        PG_RETURN_BOOL!(false);
    }

    events = relation_is_updatable(
        reloid,
        NIL,
        include_triggers,
        bms_make_singleton(col as c_int),
    );

    /* We require both updatability and deletability of the relation */
    // #define REQ_EVENTS ((1 << CMD_UPDATE) | (1 << CMD_DELETE))

    PG_RETURN_BOOL!((events & REQ_EVENTS) == REQ_EVENTS)
}

/*
 * pg_input_is_valid - test whether string is valid input for datatype.
 *
 * Returns true if OK, false if not.
 *
 * This will only work usefully if the datatype's input function has been
 * updated to return "soft" errors via errsave/ereturn.
 */
// PG_FUNCTION_INFO_V1(pg_input_is_valid)
pub unsafe fn pg_input_is_valid(fcinfo: FunctionCallInfo) -> Datum {
    let txt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let typname: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut escontext: ErrorSaveContext = make_error_save_context();

    PG_RETURN_BOOL!(pg_input_is_valid_common(fcinfo, txt, typname, &mut escontext))
}

// ErrorSaveContext escontext = {T_ErrorSaveContext};
#[inline]
unsafe fn make_error_save_context() -> ErrorSaveContext {
    let mut ctx: ErrorSaveContext = core::mem::zeroed();
    ctx.r#type = core::mem::transmute(T_ErrorSaveContext);
    ctx
}

/*
 * pg_input_error_info - test whether string is valid input for datatype.
 *
 * Returns NULL if OK, else the primary message, detail message, hint message
 * and sql error code from the error.
 *
 * This will only work usefully if the datatype's input function has been
 * updated to return "soft" errors via errsave/ereturn.
 */
// PG_FUNCTION_INFO_V1(pg_input_error_info)
pub unsafe fn pg_input_error_info(fcinfo: FunctionCallInfo) -> Datum {
    let txt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let typname: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut escontext: ErrorSaveContext = make_error_save_context();
    let mut tupdesc: TupleDesc = std::ptr::null_mut();
    let mut values: [Datum; 4] = [0; 4];
    let mut isnull: [bool; 4] = [false; 4];

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }
    tupdesc = BlessTupleDesc(tupdesc);

    /* Enable details_wanted */
    escontext_set_details_wanted(&mut escontext, true);

    if pg_input_is_valid_common(fcinfo, txt, typname, &mut escontext) {
        memset(isnull.as_mut_ptr() as *mut c_void, 1, std::mem::size_of_val(&isnull));
    } else {
        let sqlstate: *mut c_char;
        let ed: *mut ErrorData = escontext_error_data(&mut escontext);

        Assert!(escontext_error_occurred(&mut escontext));
        Assert!(!escontext_error_data(&mut escontext).is_null());
        Assert!(!errordata_message(ed).is_null());

        memset(isnull.as_mut_ptr() as *mut c_void, 0, std::mem::size_of_val(&isnull));

        values[0] = CStringGetTextDatum(errordata_message(ed));

        if !errordata_detail(ed).is_null() {
            values[1] = CStringGetTextDatum(errordata_detail(ed));
        } else {
            isnull[1] = true;
        }

        if !errordata_hint(ed).is_null() {
            values[2] = CStringGetTextDatum(errordata_hint(ed));
        } else {
            isnull[2] = true;
        }

        sqlstate = unpack_sql_state(errordata_sqlerrcode(ed));
        values[3] = CStringGetTextDatum(sqlstate);
    }

    HeapTupleGetDatum(heap_form_tuple(tupdesc, values.as_mut_ptr(), isnull.as_mut_ptr()))
}

/* Common subroutine for the above */
unsafe fn pg_input_is_valid_common(
    fcinfo: FunctionCallInfo,
    txt: *mut text,
    typname: *mut text,
    escontext: *mut ErrorSaveContext,
) -> bool {
    let str: *mut c_char = text_to_cstring(txt);
    let mut my_extra: *mut ValidIOData;
    let mut converted: Datum = 0;

    /*
     * We arrange to look up the needed I/O info just once per series of
     * calls, assuming the data type doesn't change underneath us.
     */
    my_extra = flinfo_fn_extra(flinfo_of(fcinfo)) as *mut ValidIOData;
    if my_extra.is_null() {
        set_flinfo_fn_extra(
            flinfo_of(fcinfo),
            MemoryContextAlloc(flinfo_fn_mcxt(flinfo_of(fcinfo)), std::mem::size_of::<ValidIOData>()),
        );
        my_extra = flinfo_fn_extra(flinfo_of(fcinfo)) as *mut ValidIOData;
        (*my_extra).typoid = InvalidOid;
        /* Detect whether typname argument is constant. */
        (*my_extra).typname_constant = get_fn_expr_arg_stable(flinfo_of(fcinfo), 1);
    }

    /*
     * If the typname argument is constant, we only need to parse it the first
     * time through.
     */
    if (*my_extra).typoid == InvalidOid || !(*my_extra).typname_constant {
        let typnamestr: *mut c_char = text_to_cstring(typname);
        let mut typoid: Oid = 0;

        /* Parse type-name argument to obtain type OID and encoded typmod. */
        parseTypeString(typnamestr, &mut typoid, &mut (*my_extra).typmod, std::ptr::null_mut());

        /* Update type-specific info if typoid changed. */
        if (*my_extra).typoid != typoid {
            getTypeInputInfo(typoid, &mut (*my_extra).typiofunc, &mut (*my_extra).typioparam);
            fmgr_info_cxt(
                (*my_extra).typiofunc,
                &mut (*my_extra).inputproc,
                flinfo_fn_mcxt(flinfo_of(fcinfo)),
            );
            (*my_extra).typoid = typoid;
        }
    }

    /* Now we can try to perform the conversion. */
    InputFunctionCallSafe(
        &mut (*my_extra).inputproc,
        str,
        (*my_extra).typioparam,
        (*my_extra).typmod,
        escontext as *mut Node,
        &mut converted,
    )
}

/*
 * Is character a valid identifier start?
 * Must match scan.l's {ident_start} character class.
 */
unsafe fn is_ident_start(c: u8) -> bool {
    /* Underscores and ASCII letters are OK */
    if c == b'_' {
        return true;
    }
    if (c >= b'a' && c <= b'z') || (c >= b'A' && c <= b'Z') {
        return true;
    }
    /* Any high-bit-set character is OK (might be part of a multibyte char) */
    if IS_HIGHBIT_SET(c) {
        return true;
    }
    false
}

// IS_HIGHBIT_SET(ch) == ((unsigned char)(ch) & HIGHBIT) (c.h)
#[inline]
fn IS_HIGHBIT_SET(c: u8) -> bool {
    (c & 0x80) != 0
}

/*
 * Is character a valid identifier continuation?
 * Must match scan.l's {ident_cont} character class.
 */
unsafe fn is_ident_cont(c: u8) -> bool {
    /* Can be digit or dollar sign ... */
    if (c >= b'0' && c <= b'9') || c == b'$' {
        return true;
    }
    /* ... or an identifier start character */
    is_ident_start(c)
}

/*
 * parse_ident - parse a SQL qualified identifier into separate identifiers.
 * When strict mode is active (second parameter), then any chars after
 * the last identifier are disallowed.
 */
// PG_FUNCTION_INFO_V1(parse_ident)
pub unsafe fn parse_ident(fcinfo: FunctionCallInfo) -> Datum {
    let qualname: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let strict: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let qualname_str: *mut c_char = text_to_cstring(qualname);
    let mut astate: *mut ArrayBuildState = std::ptr::null_mut();
    let mut nextp: *mut c_char;
    let mut after_dot: bool = false;

    /*
     * The code below scribbles on qualname_str in some cases, so we should
     * reconvert qualname if we need to show the original string in error
     * messages.
     */
    nextp = qualname_str;

    /* skip leading whitespace */
    while scanner_isspace(*nextp) {
        nextp = nextp.offset(1);
    }

    loop {
        let curname: *mut c_char;
        let mut missing_ident: bool = true;

        if *nextp == b'"' as c_char {
            let mut endp: *mut c_char;

            curname = nextp.offset(1);
            loop {
                endp = strchr(nextp.offset(1), b'"' as c_int);
                if endp.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "string is not a valid identifier: \"{}\"",
                            std::ffi::CStr::from_ptr(text_to_cstring(qualname)).to_string_lossy()
                        )
                    );
                }
                if *endp.offset(1) != b'"' as c_char {
                    break;
                }
                memmove(endp as *mut c_void, endp.offset(1) as *const c_void, strlen(endp));
                nextp = endp;
            }
            nextp = endp.offset(1);
            *endp = b'\0' as c_char;

            if endp.offset_from(curname) == 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "string is not a valid identifier: \"{}\"",
                        std::ffi::CStr::from_ptr(text_to_cstring(qualname)).to_string_lossy()
                    )
                );
            }

            astate = accumArrayResult(
                astate,
                CStringGetTextDatum(curname),
                false,
                TEXTOID,
                CurrentMemoryContext,
            );
            missing_ident = false;
        } else if is_ident_start(*nextp as u8) {
            let downname: *mut c_char;
            let len: c_int;
            let part: *mut text;

            curname = nextp;
            nextp = nextp.offset(1);
            while is_ident_cont(*nextp as u8) {
                nextp = nextp.offset(1);
            }

            len = nextp.offset_from(curname) as c_int;

            /*
             * We don't implicitly truncate identifiers. This is useful for
             * allowing the user to check for specific parts of the identifier
             * being too long. It's easy enough for the user to get the
             * truncated names by casting our output to name[].
             */
            downname = downcase_identifier(curname, len, false, false);
            part = cstring_to_text_with_len(downname, len);
            astate = accumArrayResult(
                astate,
                PointerGetDatum(part as *const c_void),
                false,
                TEXTOID,
                CurrentMemoryContext,
            );
            missing_ident = false;
        }

        if missing_ident {
            /* Different error messages based on where we failed. */
            if *nextp == b'.' as c_char {
                ereport!(
                    ERROR,
                    errmsg!(
                        "string is not a valid identifier: \"{}\"",
                        std::ffi::CStr::from_ptr(text_to_cstring(qualname)).to_string_lossy()
                    )
                );
            } else if after_dot {
                ereport!(
                    ERROR,
                    errmsg!(
                        "string is not a valid identifier: \"{}\"",
                        std::ffi::CStr::from_ptr(text_to_cstring(qualname)).to_string_lossy()
                    )
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "string is not a valid identifier: \"{}\"",
                        std::ffi::CStr::from_ptr(text_to_cstring(qualname)).to_string_lossy()
                    )
                );
            }
        }

        while scanner_isspace(*nextp) {
            nextp = nextp.offset(1);
        }

        if *nextp == b'.' as c_char {
            after_dot = true;
            nextp = nextp.offset(1);
            while scanner_isspace(*nextp) {
                nextp = nextp.offset(1);
            }
        } else if *nextp == b'\0' as c_char {
            break;
        } else {
            if strict {
                ereport!(
                    ERROR,
                    errmsg!(
                        "string is not a valid identifier: \"{}\"",
                        std::ffi::CStr::from_ptr(text_to_cstring(qualname)).to_string_lossy()
                    )
                );
            }
            break;
        }
    }

    PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext))
}

/*
 * pg_current_logfile
 *
 * Report current log file used by log collector by scanning current_logfiles.
 */
// PG_FUNCTION_INFO_V1(pg_current_logfile)
pub unsafe fn pg_current_logfile(fcinfo: FunctionCallInfo) -> Datum {
    let fd: *mut FILE;
    let mut lbuffer: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let logfmt: *mut c_char;

    /* The log format parameter is optional */
    if (PG_NARGS!(fcinfo) as c_int) == 0 || PG_ARGISNULL!(fcinfo, 0) {
        logfmt = std::ptr::null_mut();
    } else {
        logfmt = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0));

        if strcmp(logfmt, c"stderr".as_ptr()) != 0
            && strcmp(logfmt, c"csvlog".as_ptr()) != 0
            && strcmp(logfmt, c"jsonlog".as_ptr()) != 0
        {
            ereport!(
                ERROR,
                errmsg!(
                    "log format \"{}\" is not supported",
                    std::ffi::CStr::from_ptr(logfmt).to_string_lossy()
                )
            );
        }
    }

    fd = AllocateFile(LOG_METAINFO_DATAFILE, c"r".as_ptr());
    if fd.is_null() {
        if get_errno() != ENOENT {
            ereport!(
                ERROR,
                errmsg!(
                    "could not read file \"{}\": %m",
                    std::ffi::CStr::from_ptr(LOG_METAINFO_DATAFILE).to_string_lossy()
                )
            );
        }
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * Read the file to gather current log filename(s) registered by the
     * syslogger.
     */
    while !fgets(lbuffer.as_mut_ptr(), std::mem::size_of_val(&lbuffer) as c_int, fd).is_null() {
        let log_format: *mut c_char;
        let mut log_filepath: *mut c_char;
        let nlpos: *mut c_char;

        /* Extract log format and log file path from the line. */
        log_format = lbuffer.as_mut_ptr();
        log_filepath = strchr(lbuffer.as_ptr(), b' ' as c_int);
        if log_filepath.is_null() {
            /* Uh oh.  No space found, so file content is corrupted. */
            elog!(
                ERROR,
                "missing space character in \"{}\"",
                std::ffi::CStr::from_ptr(LOG_METAINFO_DATAFILE).to_string_lossy()
            );
            break;
        }

        *log_filepath = b'\0' as c_char;
        log_filepath = log_filepath.offset(1);
        nlpos = strchr(log_filepath, b'\n' as c_int);
        if nlpos.is_null() {
            /* Uh oh.  No newline found, so file content is corrupted. */
            elog!(
                ERROR,
                "missing newline character in \"{}\"",
                std::ffi::CStr::from_ptr(LOG_METAINFO_DATAFILE).to_string_lossy()
            );
            break;
        }
        *nlpos = b'\0' as c_char;

        if logfmt.is_null() || strcmp(logfmt, log_format) == 0 {
            FreeFile(fd);
            PG_RETURN_TEXT_P!(cstring_to_text(log_filepath));
        }
    }

    /* Close the current log filename file. */
    FreeFile(fd);

    PG_RETURN_NULL!(fcinfo)
}

/*
 * Report current log file used by log collector (1 argument version)
 *
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments
 */
// PG_FUNCTION_INFO_V1(pg_current_logfile_1arg)
pub unsafe fn pg_current_logfile_1arg(fcinfo: FunctionCallInfo) -> Datum {
    pg_current_logfile(fcinfo)
}

/*
 * SQL wrapper around RelationGetReplicaIndex().
 */
// PG_FUNCTION_INFO_V1(pg_get_replica_identity_index)
pub unsafe fn pg_get_replica_identity_index(fcinfo: FunctionCallInfo) -> Datum {
    let reloid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let idxoid: Oid;
    let rel: Relation;

    rel = table_open(reloid, AccessShareLock);
    idxoid = RelationGetReplicaIndex(rel);
    table_close(rel, AccessShareLock);

    if OidIsValid(idxoid) {
        PG_RETURN_OID!(idxoid)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

/*
 * Transition function for the ANY_VALUE aggregate
 */
// PG_FUNCTION_INFO_V1(any_value_transfn)
pub unsafe fn any_value_transfn(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 0))
}

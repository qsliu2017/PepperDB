//! Translation of postgres/src/include/catalog/pg_proc.h
//!
//! The `FormData_pg_proc` struct: the fixed-layout part of a pg_proc catalog
//! row, i.e. the fields from the opening brace up to the start of the
//! variable-length columns.
//!
//! NOTE on the CATALOG_VARLEN cutoff: in the C header, `prorettype` is the last
//! fixed-width field.  The comment "variable-length fields start here, but we
//! allow direct access to proargtypes" precedes `proargtypes` (an oidvector),
//! which is the FIRST variable-length field - it lives outside the #ifdef
//! CATALOG_VARLEN block only so C code can reference it directly, but it is NOT
//! a fixed-width member and is OMITTED here.  All remaining fields
//! (proallargtypes, proargmodes, proargnames, proargdefaults, protrftypes,
//! prosrc, probin, prosqlbody, proconfig, proacl) are guarded by
//! CATALOG_VARLEN and are likewise not part of this struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{float4, int16, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

// ===========================================================================
// Translation of postgres/src/backend/catalog/pg_proc.c
//   routines to support manipulation of the pg_proc relation
// ===========================================================================

use crate::prelude::*;
use crate::{foreach, foreach_oid, current_cell, castNode, IsA, lfirst_node};
use crate::{PG_GETARG_OID, PG_RETURN_VOID, list_make1};

use crate::nodes::pg_list::{lfirst, linitial, list_length, list_nth_cell, lnext, lappend, lappend_oid, NIL, List, ListCell};
use crate::nodes::nodes::Node;

use crate::parser::parse_coerce::{check_valid_polymorphic_signature, check_valid_internal_signature};

/*
 * Symbols pulled in from other backend units.  Where a faithful home exists in
 * src/ we import it; the remainder are genuinely-unported deps and get a local
 * TODO(pg-port) stub so this file stays self-consistent (the crate build is
 * expected to be RED during the port).
 */

/* fixed catalog cardinality (gencat output); pg_proc has this many columns. */
const Natts_pg_proc: usize = 33;

/* From catalog/objectaddress.h: ObjectAddressSet(addr, classId, objectId). */
macro_rules! ObjectAddressSet {
    ($addr:expr, $classId:expr, $objectId:expr) => {{
        $addr.classId = $classId;
        $addr.objectId = $objectId;
        $addr.objectSubId = 0;
    }};
}

/* From access/htup_details.h */
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool { !tuple.is_null() }

/* Attribute numbers for pg_proc (1-based), from the generated header. */
const Anum_pg_proc_oid: usize = 1;
const Anum_pg_proc_proname: usize = 2;
const Anum_pg_proc_pronamespace: usize = 3;
const Anum_pg_proc_proowner: usize = 4;
const Anum_pg_proc_prolang: usize = 5;
const Anum_pg_proc_procost: usize = 6;
const Anum_pg_proc_prorows: usize = 7;
const Anum_pg_proc_provariadic: usize = 8;
const Anum_pg_proc_prosupport: usize = 9;
const Anum_pg_proc_prokind: usize = 10;
const Anum_pg_proc_prosecdef: usize = 11;
const Anum_pg_proc_proleakproof: usize = 12;
const Anum_pg_proc_proisstrict: usize = 13;
const Anum_pg_proc_proretset: usize = 14;
const Anum_pg_proc_provolatile: usize = 15;
const Anum_pg_proc_proparallel: usize = 16;
const Anum_pg_proc_pronargs: usize = 17;
const Anum_pg_proc_pronargdefaults: usize = 18;
const Anum_pg_proc_prorettype: usize = 19;
const Anum_pg_proc_proargtypes: usize = 20;
const Anum_pg_proc_proallargtypes: usize = 21;
const Anum_pg_proc_proargmodes: usize = 22;
const Anum_pg_proc_proargnames: usize = 23;
const Anum_pg_proc_proargdefaults: usize = 24;
const Anum_pg_proc_protrftypes: usize = 25;
const Anum_pg_proc_prosrc: usize = 26;
const Anum_pg_proc_probin: usize = 27;
const Anum_pg_proc_prosqlbody: usize = 28;
const Anum_pg_proc_proconfig: usize = 29;
const Anum_pg_proc_proacl: usize = 30;

/* Type/relation/catalog OIDs and limits (genbki / pg_type / lockdefs). */
const FUNC_MAX_ARGS: c_int = 100;
const OIDOID: Oid = 26;
const CHAROID: Oid = 18;
const RECORDOID: Oid = 2249;
const VOIDOID: Oid = 2278;
const ANYOID: Oid = 2276;
const ANYARRAYOID: Oid = 2277;
const ANYELEMENTOID: Oid = 2283;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;
const ANYCOMPATIBLEOID: Oid = 5077;
const TYPTYPE_PSEUDO: c_char = b'p' as c_char;
const SQLlanguageId: Oid = 14;
const ProcedureRelationId: Oid = 1255;
const NamespaceRelationId: Oid = 2615;
const LanguageRelationId: Oid = 2612;
const TypeRelationId: Oid = 1247;
const TransformRelationId: Oid = 3576;
const ProcedureOidIndexId: Oid = 2690;
const RowExclusiveLock: c_int = 3;
const PROCNAMEARGSNSP: c_int = 46;
const PROCOID: c_int = 47;

/* errcodes (utils/errcodes.h) used by ereport calls below. */
const ERRCODE_TOO_MANY_ARGUMENTS: c_int = 0;
const ERRCODE_INVALID_FUNCTION_DEFINITION: c_int = 0;
const ERRCODE_DUPLICATE_FUNCTION: c_int = 0;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;
const ERRCODE_UNDEFINED_FUNCTION: c_int = 0;

/* GUC and object-type tags. */
const PGC_SUSET: c_int = 0;
const PGC_USERSET: c_int = 0;
const PGC_S_SESSION: c_int = 0;
const GUC_ACTION_SAVE: c_int = 0;
const ACLCHECK_NOT_OWNER: c_int = 0;
const OBJECT_FUNCTION: c_int = 0;
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;

#[allow(non_upper_case_globals)]
static mut check_function_bodies: bool = true;

/*
 * Active-portal status enum (utils/portal.h); only PORTAL_ACTIVE is referenced.
 */
const PORTAL_ACTIVE: c_int = 2;

/* ErrorContextCallback fields are exercised below; provide a faithful shape. */
extern "C" {
    static mut error_context_stack: *mut ErrorContextCallback;
    static mut ActivePortal: *mut PortalData;
}

#[repr(C)]
struct PortalData {
    status: c_int,
    sourceText: *const c_char,
}

/* ---- genuinely-unported helper deps: local TODO(pg-port) stubs ---- */

unsafe fn get_element_type(_typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_element_type(_typid as _) as _ }
unsafe fn namestrcpy(_name: *mut NameData, _s: *const c_char) -> c_int { crate::utils::adt::name::namestrcpy(_name as _, _s as _); 0 }
unsafe fn NameGetDatum(_n: *const NameData) -> Datum { crate::postgres::NameGetDatum(_n as _) as _ }
unsafe fn nodeToString(_obj: *const c_void) -> *mut c_char { crate::nodes::outfuncs::nodeToString(_obj as _) as _ }
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum { crate::utils::builtins::CStringGetTextDatum(_s as _) as _ }
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char { crate::utils::builtins::TextDatumGetCString(_d as _) as _ }
unsafe fn table_open(_relid: Oid, _lockmode: c_int) -> Relation { crate::access::table::table::table_open(_relid as _, _lockmode as _) as _ }
unsafe fn table_close(_rel: Relation, _lockmode: c_int) { crate::access::table::table::table_close(_rel as _, _lockmode as _) }
unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc { crate::utils::rel::RelationGetDescr(_rel as _) as _ }
unsafe fn SearchSysCache1(_id: c_int, _k1: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache1(_id as _, _k1 as _) as _ }
unsafe fn SearchSysCache3(_id: c_int, _k1: Datum, _k2: Datum, _k3: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache3(_id as _, _k1 as _, _k2 as _, _k3 as _) as _ }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) { crate::utils::cache::syscache::ReleaseSysCache(_tuple as _) }
unsafe fn SysCacheGetAttr(_id: c_int, _tup: HeapTuple, _attno: usize, _isnull: *mut bool) -> Datum { crate::utils::cache::syscache::SysCacheGetAttr(_id as _, _tup as _, _attno as _, _isnull as _) as _ }
unsafe fn SysCacheGetAttrNotNull(_id: c_int, _tup: HeapTuple, _attno: usize) -> Datum { crate::utils::cache::syscache::SysCacheGetAttrNotNull(_id as _, _tup as _, _attno as _) as _ }
unsafe fn GETSTRUCT(_tup: HeapTuple) -> Form_pg_proc { crate::access::htup_details::GETSTRUCT(_tup as _) as _ }
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool { crate::catalog::aclchk::object_ownercheck(_classid as _, _objectid as _, _roleid as _) as _ }
unsafe fn aclcheck_error(_aclerr: c_int, _objtype: c_int, _objname: *const c_char) { crate::catalog::aclchk::aclcheck_error(core::mem::transmute(_aclerr), core::mem::transmute(_objtype), _objname as _) }
unsafe fn format_procedure(_procedure_oid: Oid) -> *mut c_char { crate::utils::adt::regproc::format_procedure(_procedure_oid as _) as _ }
unsafe fn build_function_result_tupdesc_t(_proctup: HeapTuple) -> TupleDesc { crate::utils::fmgr::funcapi::build_function_result_tupdesc_t(_proctup as _) as _ }
unsafe fn build_function_result_tupdesc_d(_prokind: c_char, _allargtypes: Datum, _argmodes: Datum, _argnames: Datum) -> TupleDesc { crate::utils::fmgr::funcapi::build_function_result_tupdesc_d(_prokind as _, _allargtypes as _, _argmodes as _, _argnames as _) as _ }
unsafe fn equalRowTypes(_a: TupleDesc, _b: TupleDesc) -> bool { crate::access::common::tupdesc::equalRowTypes(_a as _, _b as _) as _ }
unsafe fn get_func_input_arg_names(_argnames: Datum, _argmodes: Datum, _arg_names: *mut *mut *mut c_char) -> c_int { crate::utils::fmgr::funcapi::get_func_input_arg_names(_argnames as _, _argmodes as _, _arg_names as _) as _ }
unsafe fn stringToNode(_s: *const c_char) -> *mut Node { crate::nodes::read::stringToNode(_s as _) as _ }
unsafe fn exprType(_expr: *const Node) -> Oid { crate::nodes::nodeFuncs::exprType(_expr as _) as _ }
unsafe fn heap_modify_tuple(_tuple: HeapTuple, _td: TupleDesc, _values: *mut Datum, _nulls: *mut bool, _replaces: *mut bool) -> HeapTuple { crate::access::common::heaptuple::heap_modify_tuple(_tuple as _, _td as _, _values as _, _nulls as _, _replaces as _) as _ }
unsafe fn heap_form_tuple(_td: TupleDesc, _values: *mut Datum, _nulls: *mut bool) -> HeapTuple { crate::access::common::heaptuple::heap_form_tuple(_td as _, _values as _, _nulls as _) as _ }
unsafe fn heap_freetuple(_tuple: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(_tuple as _) }
unsafe fn CatalogTupleUpdate(_rel: Relation, _otid: *mut c_void, _tup: HeapTuple) { crate::catalog::indexing::CatalogTupleUpdate(_rel as _, _otid as _, _tup as _) }
unsafe fn CatalogTupleInsert(_rel: Relation, _tup: HeapTuple) { crate::catalog::indexing::CatalogTupleInsert(_rel as _, _tup as _) }
unsafe fn get_user_default_acl(_objtype: c_int, _owner_id: Oid, _nsp_oid: Oid) -> *mut Acl { crate::catalog::aclchk::get_user_default_acl(core::mem::transmute(_objtype), _owner_id as _, _nsp_oid as _) as _ }
unsafe fn GetNewOidWithIndex(_rel: Relation, _index_id: Oid, _oidcolumn: usize) -> Oid { crate::catalog::catalog::GetNewOidWithIndex(_rel as _, _index_id as _, _oidcolumn as _) as _ }
unsafe fn deleteDependencyRecordsFor(_class_id: Oid, _object_id: Oid, _skip_extension_deps: bool) -> c_long { crate::catalog::pg_depend::deleteDependencyRecordsFor(_class_id as _, _object_id as _, _skip_extension_deps as _) as _ }
unsafe fn new_object_addresses() -> *mut ObjectAddresses { crate::catalog::dependency::new_object_addresses() as _ }
unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) { crate::catalog::dependency::add_exact_object_address(_object as _, _addrs as _) }
unsafe fn record_object_address_dependencies(_depender: *const ObjectAddress, _referenced: *mut ObjectAddresses, _behavior: c_char) { crate::catalog::dependency::record_object_address_dependencies(_depender as _, _referenced as _, _behavior as _) }
unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) { crate::catalog::dependency::free_object_addresses(_addrs as _) }
unsafe fn recordDependencyOnExpr(_depender: *const ObjectAddress, _expr: *const Node, _rtable: *mut List, _behavior: c_char) { crate::catalog::dependency::recordDependencyOnExpr(_depender as _, _expr as _, _rtable as _, _behavior as _) }
unsafe fn recordDependencyOnOwner(_class_id: Oid, _object_id: Oid, _owner: Oid) { crate::catalog::pg_shdepend::recordDependencyOnOwner(_class_id as _, _object_id as _, _owner as _) }
unsafe fn recordDependencyOnNewAcl(_class_id: Oid, _object_id: Oid, _objsub_id: c_int, _owner: Oid, _acl: *mut Acl) { crate::catalog::aclchk::recordDependencyOnNewAcl(_class_id as _, _object_id as _, _objsub_id as _, _owner as _, _acl as _) }
unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _is_update: bool) { crate::catalog::pg_depend::recordDependencyOnCurrentExtension(_object as _, _is_update as _) }
unsafe fn InvokeObjectPostCreateHook(_class_id: Oid, _object_id: Oid, _subid: c_int) { crate::parser_link_shims::InvokeObjectPostCreateHook(_class_id as _, _object_id as _, _subid as _) }
unsafe fn CommandCounterIncrement() { crate::access::transam::xact::CommandCounterIncrement() }
unsafe fn NewGUCNestLevel() -> c_int { crate::utils::misc::guc::NewGUCNestLevel() as _ }
unsafe fn ProcessGUCArray(_array: *mut ArrayType, _context: c_int, _source: c_int, _action: c_int) { crate::utils::misc::guc::ProcessGUCArray(_array as _, core::mem::transmute(_context), core::mem::transmute(_source), core::mem::transmute(_action)) }
unsafe fn superuser() -> bool { crate::utils::misc::superuser::superuser() as _ }
unsafe fn OidFunctionCall1(_function_id: Oid, _arg1: Datum) -> Datum { crate::utils::fmgr::OidFunctionCall1Coll(_function_id as _, crate::postgres_ext::InvalidOid, _arg1 as _) as _ }
unsafe fn AtEOXact_GUC(_is_commit: bool, _nestlevel: c_int) { crate::utils::misc::guc::AtEOXact_GUC(_is_commit as _, _nestlevel as _) }
unsafe fn pgstat_create_function(_proid: Oid) { crate::utils::activity::pgstat_function::pgstat_create_function(_proid as _) }
unsafe fn CheckFunctionValidatorAccess(_validator_oid: Oid, _function_oid: Oid) -> bool { crate::utils::fmgr::CheckFunctionValidatorAccess(_validator_oid as _, _function_oid as _) as _ }
unsafe fn fmgr_internal_function(_proname: *const c_char) -> Oid { crate::utils::fmgr::fmgr_internal_function(_proname as _) as _ }
unsafe fn load_external_function(_filename: *const c_char, _funcname: *const c_char, _signal_not_found: bool, _filehandle: *mut *mut c_void) -> *mut c_void { crate::utils::fmgr::dfmgr::load_external_function(_filename as _, _funcname as _, _signal_not_found as _, _filehandle as _) as _ }
unsafe fn fetch_finfo_record(_filehandle: *mut c_void, _funcname: *const c_char) -> *mut c_void { crate::utils::fmgr::fetch_finfo_record(_filehandle as _, _funcname as _) as _ }
unsafe fn get_typtype(_typid: Oid) -> c_char { crate::utils::cache::lsyscache::get_typtype(_typid as _) as _ }
unsafe fn IsPolymorphicType(_typid: Oid) -> bool { todo!("TODO(pg-port): IsPolymorphicType") }
unsafe fn format_type_be(_typid: Oid) -> *mut c_char { crate::utils::adt::format_type::format_type_be(_typid as _) as _ }
unsafe fn pg_parse_query(_query_string: *const c_char) -> *mut List { crate::tcop::postgres::pg_parse_query(_query_string as _) as _ }
unsafe fn pg_rewrite_query(_query: *mut Query) -> *mut List { crate::tcop::postgres::pg_rewrite_query(_query as _) as _ }
unsafe fn AcquireRewriteLocks(_parsetree: *mut Query, _forexecute: bool, _forupdatepusheddown: bool) { crate::rewrite::rewriteHandler::AcquireRewriteLocks(_parsetree as _, _forexecute as _, _forupdatepusheddown as _) }
unsafe fn prepare_sql_fn_parse_info(_proceduretuple: HeapTuple, _call_expr: *mut Node, _input_collation: Oid) -> SQLFunctionParseInfoPtr { crate::executor::functions::prepare_sql_fn_parse_info(_proceduretuple as _, _call_expr as _, _input_collation as _) as _ }
unsafe fn sql_fn_parser_setup(_pstate: *mut c_void, _arg: *mut c_void) { crate::executor::functions::sql_fn_parser_setup(_pstate as _, _arg as _) }
unsafe fn pg_analyze_and_rewrite_withcb(_parsetree: *mut RawStmt, _query_string: *const c_char, _parser_setup: ParserSetupHook, _parser_setup_arg: *mut c_void, _query_env: *mut c_void) -> *mut List { crate::tcop::postgres::pg_analyze_and_rewrite_withcb(_parsetree as _, _query_string as _, _parser_setup as _, _parser_setup_arg as _, _query_env as _) as _ }
unsafe fn check_sql_fn_statements(_queryTreeLists: *mut List) { crate::executor::functions::check_sql_fn_statements(_queryTreeLists as _) }
unsafe fn get_func_result_type(_functionId: Oid, _resultTypeId: *mut Oid, _resultTupleDesc: *mut TupleDesc) -> c_int { crate::utils::fmgr::funcapi::get_func_result_type(_functionId as _, _resultTypeId as _, _resultTupleDesc as _) as _ }
unsafe fn check_sql_fn_retval(_queryTreeLists: *mut List, _rettype: Oid, _rettupdesc: TupleDesc, _prokind: c_char, _insertDroppedCols: bool) -> bool { crate::executor::functions::check_sql_fn_retval(_queryTreeLists as _, _rettype as _, _rettupdesc as _, _prokind as _, _insertDroppedCols as _) as _ }
unsafe fn geterrposition() -> c_int { crate::utils::error::elog_impl::geterrposition() as _ }
unsafe fn getinternalerrposition() -> c_int { crate::utils::error::elog_impl::getinternalerrposition() as _ }
unsafe fn errposition(_cursorpos: c_int) -> c_int { crate::utils::error::elog_impl::errposition(_cursorpos as _) as _ }
unsafe fn internalerrposition(_cursorpos: c_int) -> c_int { crate::utils::error::elog_impl::internalerrposition(_cursorpos as _) as _ }
unsafe fn internalerrquery(_query: *const c_char) -> c_int { crate::utils::error::elog_impl::internalerrquery(_query as _) as _ }
unsafe fn pg_mbstrlen_with_len(_mbstr: *const c_char, _limit: c_int) -> c_int { crate::mb::mbutils::pg_mbstrlen_with_len(_mbstr as _, _limit as _) as _ }
unsafe fn pg_mblen_cstr(_s: *const c_char) -> c_int { crate::mb::mbutils::pg_mblen_cstr(_s as _) as _ }
unsafe fn deconstruct_array_builtin(_array: *mut ArrayType, _elmtype: Oid, _elemsp: *mut *mut Datum, _nullsp: *mut *mut bool, _nelemsp: *mut c_int) { crate::utils::adt::arrayfuncs::deconstruct_array_builtin(_array as _, _elmtype as _, _elemsp as _, _nullsp as _, _nelemsp as _) }
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType { crate::access::nbtree::nbtpreprocesskeys::DatumGetArrayTypeP(_d as _) as _ }

/* errcontext! has no crate-root macro_export; provide a local no-op shim that
 * still formats its argument, matching the sibling-file stub convention. */
macro_rules! errcontext { ($($arg:tt)*) => {{ let _ = format!($($arg)*); }}; }

/* ArrayType field accessors (utils/array.h macros), as local fns. */
unsafe fn ARR_NDIM(_a: *mut ArrayType) -> c_int { crate::utils::array::ARR_NDIM(_a as _) as _ }
unsafe fn ARR_DIMS(_a: *mut ArrayType) -> *mut c_int { crate::utils::array::ARR_DIMS(_a as _) as _ }
unsafe fn ARR_HASNULL(_a: *mut ArrayType) -> bool { crate::utils::array::ARR_HASNULL(_a as _) as _ }
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid { crate::utils::array::ARR_ELEMTYPE(_a as _) as _ }
unsafe fn ARR_DATA_PTR(_a: *mut ArrayType) -> *mut c_char { crate::utils::array::ARR_DATA_PTR(_a as _) as _ }

type Relation = *mut crate::utils::rel::RelationData;
type HeapTuple = *mut crate::access::htup_details::HeapTupleData;
type TupleDesc = *mut crate::access::common::tupdesc::TupleDescData;
type Query = crate::nodes::parsenodes::Query;
type RawStmt = crate::nodes::parsenodes::RawStmt;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type oidvector = crate::c::oidvector;
type ErrorContextCallback = crate::access::transam::xlogrecovery::ErrorContextCallback;

// Faithful pointer/opaque aliases for types whose concrete defs live elsewhere.
type ArrayType = c_void;
type Acl = c_void;
type ObjectAddresses = c_void;
type SQLFunctionParseInfoPtr = *mut c_void;
type ParserSetupHook = unsafe fn(*mut c_void, *mut c_void);

#[repr(C)]
struct parse_error_callback_arg {
    proname: *mut c_char,
    prosrc: *mut c_char,
}

/* regproc is a C typedef for Oid (see postgres_ext.h / c.h usage). */
pub type regproc = Oid;

/*
 * FormData_pg_proc - the fixed part of a pg_proc row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly.
 */
#[repr(C)]
pub struct FormData_pg_proc {
    /* oid */
    pub oid: Oid,
    /* procedure name */
    pub proname: NameData,
    /* OID of namespace containing this proc */
    pub pronamespace: Oid,
    /* procedure owner */
    pub proowner: Oid,
    /* OID of pg_language entry */
    pub prolang: Oid,
    /* estimated execution cost */
    pub procost: float4,
    /* estimated # of rows out (if proretset) */
    pub prorows: float4,
    /* element type of variadic array, or 0 if not variadic */
    pub provariadic: Oid,
    /* planner support function for this function, or 0 if none */
    pub prosupport: regproc,
    /* see PROKIND_ categories below */
    pub prokind: c_char,
    /* security definer */
    pub prosecdef: bool,
    /* is it a leakproof function? */
    pub proleakproof: bool,
    /* strict with respect to NULLs? */
    pub proisstrict: bool,
    /* returns a set? */
    pub proretset: bool,
    /* see PROVOLATILE_ categories below */
    pub provolatile: c_char,
    /* see PROPARALLEL_ categories below */
    pub proparallel: c_char,
    /* number of arguments */
    pub pronargs: int16,
    /* number of arguments with defaults */
    pub pronargdefaults: int16,
    /* OID of result type */
    pub prorettype: Oid,
    /*
     * variable-length fields start here, but we allow direct access to
     * proargtypes
     */
    pub proargtypes: crate::c::oidvector,
}

/*
 * Form_pg_proc corresponds to a pointer to a tuple with the format of the
 * pg_proc relation.
 */
pub type Form_pg_proc = *mut FormData_pg_proc;

/* Symbolic values for prokind column (EXPOSE_TO_CLIENT_CODE) */
pub const PROKIND_FUNCTION: c_char = b'f' as c_char;
pub const PROKIND_AGGREGATE: c_char = b'a' as c_char;
pub const PROKIND_WINDOW: c_char = b'w' as c_char;
pub const PROKIND_PROCEDURE: c_char = b'p' as c_char;

/* Symbolic values for provolatile column (EXPOSE_TO_CLIENT_CODE) */
pub const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char; /* never changes for given input */
pub const PROVOLATILE_STABLE: c_char = b's' as c_char; /* does not change within a scan */
pub const PROVOLATILE_VOLATILE: c_char = b'v' as c_char; /* can change even within a scan */

/* Symbolic values for proparallel column (EXPOSE_TO_CLIENT_CODE) */
pub const PROPARALLEL_SAFE: c_char = b's' as c_char; /* can run in worker or leader */
pub const PROPARALLEL_RESTRICTED: c_char = b'r' as c_char; /* can run in parallel leader only */
pub const PROPARALLEL_UNSAFE: c_char = b'u' as c_char; /* banned while in parallel mode */

/* Symbolic values for proargmodes column (EXPOSE_TO_CLIENT_CODE) */
pub const PROARGMODE_IN: c_char = b'i' as c_char;
pub const PROARGMODE_OUT: c_char = b'o' as c_char;
pub const PROARGMODE_INOUT: c_char = b'b' as c_char;
pub const PROARGMODE_VARIADIC: c_char = b'v' as c_char;
pub const PROARGMODE_TABLE: c_char = b't' as c_char;

/* ----------------------------------------------------------------
 *		ProcedureCreate
 *
 *	procedureName: string name of routine (proname)
 *	procNamespace: OID of namespace (pronamespace)
 *	replace: true to allow replacement of an existing pg_proc entry
 *	returnsSet: returns set? (proretset)
 *	returnType: OID of result type (prorettype)
 *	proowner: OID of owner role (proowner)
 *	languageObjectId: OID of function language (prolang)
 *	languageValidator: OID of validator function to apply, if any
 *	prosrc: string form of function definition (prosrc)
 *	probin: string form of binary reference, or NULL (probin)
 *	prosqlbody: Node tree of pre-parsed SQL body, or NULL (prosqlbody)
 *	prokind: function/aggregate/procedure/etc code (prokind)
 *	security_definer: security definer? (prosecdef)
 *	isLeakProof: leak proof? (proleakproof)
 *	isStrict: strict? (proisstrict)
 *	volatility: volatility code (provolatile)
 *	parallel: parallel safety code (proparallel)
 *	parameterTypes: input parameter types, as an oidvector (proargtypes)
 *	allParameterTypes: all parameter types, as an OID array (proallargtypes)
 *	parameterModes: parameter modes, as a "char" array (proargmodes)
 *	parameterNames: parameter names, as a text array (proargnames)
 *	parameterDefaults: defaults, as a List of Node trees (proargdefaults)
 *	trftypes: transformable type OIDs, as an OID array (protrftypes)
 *	trfoids: List of transform OIDs that routine should depend on
 *	proconfig: GUC set clauses, as a text array (proconfig)
 *	prosupport: OID of support function, if any (prosupport)
 *	procost: cost factor (procost)
 *	prorows: estimated output rows for a SRF (prorows)
 *
 * Note: allParameterTypes, parameterModes, parameterNames, trftypes, and proconfig
 * are either arrays of the proper types or NULL.  We declare them Datum,
 * not "ArrayType *", to avoid importing array.h into pg_proc.h.
 * ----------------------------------------------------------------
 */
pub unsafe fn ProcedureCreate(
    procedureName: *const c_char,
    procNamespace: Oid,
    replace: bool,
    returnsSet: bool,
    returnType: Oid,
    proowner: Oid,
    languageObjectId: Oid,
    languageValidator: Oid,
    prosrc: *const c_char,
    probin: *const c_char,
    prosqlbody: *mut Node,
    prokind: c_char,
    security_definer: bool,
    isLeakProof: bool,
    isStrict: bool,
    volatility: c_char,
    parallel: c_char,
    parameterTypes: *mut oidvector,
    allParameterTypes: Datum,
    parameterModes: Datum,
    parameterNames: Datum,
    parameterDefaults: *mut List,
    trftypes: Datum,
    trfoids: *mut List,
    proconfig: Datum,
    prosupport: Oid,
    procost: float4,
    prorows: float4,
) -> ObjectAddress {
    let retval: Oid;
    let parameterCount: c_int;
    let allParamCount: c_int;
    let allParams: *mut Oid;
    let mut paramModes: *mut c_char = null_mut();
    let mut variadicType: Oid = InvalidOid;
    let mut proacl: *mut Acl = null_mut();
    let rel: Relation;
    let tup: HeapTuple;
    let oldtup: HeapTuple;
    let mut nulls: [bool; Natts_pg_proc] = [false; Natts_pg_proc];
    let mut values: [Datum; Natts_pg_proc] = [0 as Datum; Natts_pg_proc];
    let mut replaces: [bool; Natts_pg_proc] = [false; Natts_pg_proc];
    let mut procname: NameData = core::mem::zeroed();
    let tupDesc: TupleDesc;
    let is_update: bool;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let mut detailmsg: *mut c_char;
    let mut i: c_int;
    let addrs: *mut ObjectAddresses;

    /*
     * sanity checks
     */
    Assert!(PointerIsValid(prosrc));

    parameterCount = (*parameterTypes).dim1;
    if parameterCount < 0 || parameterCount > FUNC_MAX_ARGS {
        ereport!(ERROR, errmsg!("functions cannot have more than {} arguments", FUNC_MAX_ARGS));
        /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS); errmsg_plural singular/plural */
        let _ = ERRCODE_TOO_MANY_ARGUMENTS;
    }
    /* note: the above is correct, we do NOT count output arguments */

    /* Deconstruct array inputs */
    if allParameterTypes != PointerGetDatum(null::<c_void>()) {
        /*
         * We expect the array to be a 1-D OID array; verify that. We don't
         * need to use deconstruct_array() since the array data is just going
         * to look like a C array of OID values.
         */
        let allParamArray: *mut ArrayType = DatumGetPointer(allParameterTypes) as *mut ArrayType;

        allParamCount = *ARR_DIMS(allParamArray).add(0);
        if ARR_NDIM(allParamArray) != 1
            || allParamCount <= 0
            || ARR_HASNULL(allParamArray)
            || ARR_ELEMTYPE(allParamArray) != OIDOID
        {
            elog!(ERROR, "allParameterTypes is not a 1-D Oid array");
        }
        allParams = ARR_DATA_PTR(allParamArray) as *mut Oid;
        Assert!(allParamCount >= parameterCount);
        /* we assume caller got the contents right */
    } else {
        allParamCount = parameterCount;
        allParams = (*parameterTypes).values.as_mut_ptr();
    }

    if parameterModes != PointerGetDatum(null::<c_void>()) {
        /*
         * We expect the array to be a 1-D CHAR array; verify that. We don't
         * need to use deconstruct_array() since the array data is just going
         * to look like a C array of char values.
         */
        let modesArray: *mut ArrayType = DatumGetPointer(parameterModes) as *mut ArrayType;

        if ARR_NDIM(modesArray) != 1
            || *ARR_DIMS(modesArray).add(0) != allParamCount
            || ARR_HASNULL(modesArray)
            || ARR_ELEMTYPE(modesArray) != CHAROID
        {
            elog!(ERROR, "parameterModes is not a 1-D char array");
        }
        paramModes = ARR_DATA_PTR(modesArray);
    }

    /*
     * Do not allow polymorphic return type unless there is a polymorphic
     * input argument that we can use to deduce the actual return type.
     */
    detailmsg = check_valid_polymorphic_signature(
        returnType,
        (*parameterTypes).values.as_ptr(),
        parameterCount,
    );
    if !detailmsg.is_null() {
        ereport!(ERROR, errmsg!("cannot determine result data type"));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION); errdetail_internal("%s", detailmsg) */
        let _ = ERRCODE_INVALID_FUNCTION_DEFINITION;
    }

    /*
     * Also, do not allow return type INTERNAL unless at least one input
     * argument is INTERNAL.
     */
    detailmsg = check_valid_internal_signature(
        returnType,
        (*parameterTypes).values.as_ptr(),
        parameterCount,
    );
    if !detailmsg.is_null() {
        ereport!(ERROR, errmsg!("unsafe use of pseudo-type \"internal\""));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION); errdetail_internal("%s", detailmsg) */
    }

    /*
     * Apply the same tests to any OUT arguments.
     */
    if allParameterTypes != PointerGetDatum(null::<c_void>()) {
        i = 0;
        while i < allParamCount {
            if paramModes.is_null()
                || *paramModes.add(i as usize) == PROARGMODE_IN
                || *paramModes.add(i as usize) == PROARGMODE_VARIADIC
            {
                i += 1;
                continue; /* ignore input-only params */
            }

            detailmsg = check_valid_polymorphic_signature(
                *allParams.add(i as usize),
                (*parameterTypes).values.as_ptr(),
                parameterCount,
            );
            if !detailmsg.is_null() {
                ereport!(ERROR, errmsg!("cannot determine result data type"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION); errdetail_internal("%s", detailmsg) */
            }
            detailmsg = check_valid_internal_signature(
                *allParams.add(i as usize),
                (*parameterTypes).values.as_ptr(),
                parameterCount,
            );
            if !detailmsg.is_null() {
                ereport!(ERROR, errmsg!("unsafe use of pseudo-type \"internal\""));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION); errdetail_internal("%s", detailmsg) */
            }
            i += 1;
        }
    }

    /* Identify variadic argument type, if any */
    if !paramModes.is_null() {
        /*
         * Only the last input parameter can be variadic; if it is, save its
         * element type.  Errors here are just elog since caller should have
         * checked this already.
         */
        i = 0;
        while i < allParamCount {
            match *paramModes.add(i as usize) {
                m if m == PROARGMODE_IN || m == PROARGMODE_INOUT => {
                    if OidIsValid(variadicType) {
                        elog!(ERROR, "variadic parameter must be last");
                    }
                }
                m if m == PROARGMODE_OUT => {
                    if OidIsValid(variadicType) && prokind == PROKIND_PROCEDURE {
                        elog!(ERROR, "variadic parameter must be last");
                    }
                }
                m if m == PROARGMODE_TABLE => {
                    /* okay */
                }
                m if m == PROARGMODE_VARIADIC => {
                    if OidIsValid(variadicType) {
                        elog!(ERROR, "variadic parameter must be last");
                    }
                    match *allParams.add(i as usize) {
                        ANYOID => {
                            variadicType = ANYOID;
                        }
                        ANYARRAYOID => {
                            variadicType = ANYELEMENTOID;
                        }
                        ANYCOMPATIBLEARRAYOID => {
                            variadicType = ANYCOMPATIBLEOID;
                        }
                        _ => {
                            variadicType = get_element_type(*allParams.add(i as usize));
                            if !OidIsValid(variadicType) {
                                elog!(ERROR, "variadic parameter is not an array");
                            }
                        }
                    }
                }
                _ => {
                    elog!(ERROR, "invalid parameter mode '{}'", *paramModes.add(i as usize) as u8 as char);
                }
            }
            i += 1;
        }
    }

    /*
     * All seems OK; prepare the data to be inserted into pg_proc.
     */

    i = 0;
    while (i as usize) < Natts_pg_proc {
        nulls[i as usize] = false;
        values[i as usize] = 0 as Datum;
        replaces[i as usize] = true;
        i += 1;
    }

    namestrcpy(&raw mut procname, procedureName);
    values[Anum_pg_proc_proname - 1] = NameGetDatum(&raw const procname);
    values[Anum_pg_proc_pronamespace - 1] = ObjectIdGetDatum(procNamespace);
    values[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(proowner);
    values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(languageObjectId);
    values[Anum_pg_proc_procost - 1] = Float4GetDatum(procost);
    values[Anum_pg_proc_prorows - 1] = Float4GetDatum(prorows);
    values[Anum_pg_proc_provariadic - 1] = ObjectIdGetDatum(variadicType);
    values[Anum_pg_proc_prosupport - 1] = ObjectIdGetDatum(prosupport);
    values[Anum_pg_proc_prokind - 1] = CharGetDatum(prokind);
    values[Anum_pg_proc_prosecdef - 1] = BoolGetDatum(security_definer);
    values[Anum_pg_proc_proleakproof - 1] = BoolGetDatum(isLeakProof);
    values[Anum_pg_proc_proisstrict - 1] = BoolGetDatum(isStrict);
    values[Anum_pg_proc_proretset - 1] = BoolGetDatum(returnsSet);
    values[Anum_pg_proc_provolatile - 1] = CharGetDatum(volatility);
    values[Anum_pg_proc_proparallel - 1] = CharGetDatum(parallel);
    values[Anum_pg_proc_pronargs - 1] = UInt16GetDatum(parameterCount as u16);
    values[Anum_pg_proc_pronargdefaults - 1] = UInt16GetDatum(list_length(parameterDefaults) as u16);
    values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(returnType);
    values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameterTypes as *const c_void);
    if allParameterTypes != PointerGetDatum(null::<c_void>()) {
        values[Anum_pg_proc_proallargtypes - 1] = allParameterTypes;
    } else {
        nulls[Anum_pg_proc_proallargtypes - 1] = true;
    }
    if parameterModes != PointerGetDatum(null::<c_void>()) {
        values[Anum_pg_proc_proargmodes - 1] = parameterModes;
    } else {
        nulls[Anum_pg_proc_proargmodes - 1] = true;
    }
    if parameterNames != PointerGetDatum(null::<c_void>()) {
        values[Anum_pg_proc_proargnames - 1] = parameterNames;
    } else {
        nulls[Anum_pg_proc_proargnames - 1] = true;
    }
    if parameterDefaults != NIL {
        values[Anum_pg_proc_proargdefaults - 1] =
            CStringGetTextDatum(nodeToString(parameterDefaults as *const c_void));
    } else {
        nulls[Anum_pg_proc_proargdefaults - 1] = true;
    }
    if trftypes != PointerGetDatum(null::<c_void>()) {
        values[Anum_pg_proc_protrftypes - 1] = trftypes;
    } else {
        nulls[Anum_pg_proc_protrftypes - 1] = true;
    }
    values[Anum_pg_proc_prosrc - 1] = CStringGetTextDatum(prosrc);
    if !probin.is_null() {
        values[Anum_pg_proc_probin - 1] = CStringGetTextDatum(probin);
    } else {
        nulls[Anum_pg_proc_probin - 1] = true;
    }
    if !prosqlbody.is_null() {
        values[Anum_pg_proc_prosqlbody - 1] =
            CStringGetTextDatum(nodeToString(prosqlbody as *const c_void));
    } else {
        nulls[Anum_pg_proc_prosqlbody - 1] = true;
    }
    if proconfig != PointerGetDatum(null::<c_void>()) {
        values[Anum_pg_proc_proconfig - 1] = proconfig;
    } else {
        nulls[Anum_pg_proc_proconfig - 1] = true;
    }
    /* proacl will be determined later */

    rel = table_open(ProcedureRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* Check for pre-existing definition */
    oldtup = SearchSysCache3(
        PROCNAMEARGSNSP,
        PointerGetDatum(procedureName as *const c_void),
        PointerGetDatum(parameterTypes as *const c_void),
        ObjectIdGetDatum(procNamespace),
    );

    if HeapTupleIsValid(oldtup) {
        /* There is one; okay to replace it? */
        let oldproc: Form_pg_proc = GETSTRUCT(oldtup);
        let proargnames: Datum;
        let mut isnull: bool = false;
        let dropcmd: *const c_char;

        if !replace {
            ereport!(ERROR, errmsg!("function \"{}\" already exists with same argument types",
                std::ffi::CStr::from_ptr(procedureName).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_FUNCTION) */
            let _ = ERRCODE_DUPLICATE_FUNCTION;
        }
        if !object_ownercheck(ProcedureRelationId, (*oldproc).oid, proowner) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION, procedureName);
        }

        /* Not okay to change routine kind */
        if (*oldproc).prokind != prokind {
            ereport!(ERROR, errmsg!("cannot change routine kind"));
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE); errdetail naming the existing kind */
            let _ = ERRCODE_WRONG_OBJECT_TYPE;
        }

        dropcmd = if prokind == PROKIND_PROCEDURE {
            b"DROP PROCEDURE\0".as_ptr() as *const c_char
        } else if prokind == PROKIND_AGGREGATE {
            b"DROP AGGREGATE\0".as_ptr() as *const c_char
        } else {
            b"DROP FUNCTION\0".as_ptr() as *const c_char
        };

        /*
         * Not okay to change the return type of the existing proc, since
         * existing rules, views, etc may depend on the return type.
         *
         * In case of a procedure, a changing return type means that whether
         * the procedure has output parameters was changed.  Since there is no
         * user visible return type, we produce a more specific error message.
         */
        if returnType != (*oldproc).prorettype || returnsSet != (*oldproc).proretset {
            if prokind == PROKIND_PROCEDURE {
                ereport!(ERROR, errmsg!("cannot change whether a procedure has output parameters"));
            } else {
                ereport!(ERROR, errmsg!("cannot change return type of existing function"));
            }
            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION);
             * errhint("Use %s %s first.", dropcmd, format_procedure(oldproc->oid)) */
        }

        /*
         * If it returns RECORD, check for possible change of record type
         * implied by OUT parameters
         */
        if returnType == RECORDOID {
            let olddesc: TupleDesc;
            let newdesc: TupleDesc;

            olddesc = build_function_result_tupdesc_t(oldtup);
            newdesc = build_function_result_tupdesc_d(
                prokind,
                allParameterTypes,
                parameterModes,
                parameterNames,
            );
            if olddesc.is_null() && newdesc.is_null() {
                /* ok, both are runtime-defined RECORDs */
            } else if olddesc.is_null() || newdesc.is_null() || !equalRowTypes(olddesc, newdesc) {
                ereport!(ERROR, errmsg!("cannot change return type of existing function"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION);
                 * errdetail("Row type defined by OUT parameters is different.");
                 * errhint("Use %s %s first.", dropcmd, format_procedure(oldproc->oid)) */
                let _ = format_procedure((*oldproc).oid);
            }
        }

        /*
         * If there were any named input parameters, check to make sure the
         * names have not been changed, as this could break existing calls. We
         * allow adding names to formerly unnamed parameters, though.
         */
        proargnames = SysCacheGetAttr(
            PROCNAMEARGSNSP,
            oldtup,
            Anum_pg_proc_proargnames,
            &raw mut isnull,
        );
        if !isnull {
            let mut proargmodes: Datum;
            let mut old_arg_names: *mut *mut c_char = null_mut();
            let mut new_arg_names: *mut *mut c_char = null_mut();
            let n_old_arg_names: c_int;
            let n_new_arg_names: c_int;
            let mut j: c_int;

            proargmodes = SysCacheGetAttr(
                PROCNAMEARGSNSP,
                oldtup,
                Anum_pg_proc_proargmodes,
                &raw mut isnull,
            );
            if isnull {
                proargmodes = PointerGetDatum(null::<c_void>()); /* just to be sure */
            }

            n_old_arg_names = get_func_input_arg_names(proargnames, proargmodes, &raw mut old_arg_names);
            n_new_arg_names = get_func_input_arg_names(parameterNames, parameterModes, &raw mut new_arg_names);
            j = 0;
            while j < n_old_arg_names {
                if (*old_arg_names.add(j as usize)).is_null() {
                    j += 1;
                    continue;
                }
                if j >= n_new_arg_names
                    || (*new_arg_names.add(j as usize)).is_null()
                    || libc_strcmp(*old_arg_names.add(j as usize), *new_arg_names.add(j as usize)) != 0
                {
                    ereport!(ERROR, errmsg!("cannot change name of input parameter \"{}\"",
                        std::ffi::CStr::from_ptr(*old_arg_names.add(j as usize)).to_string_lossy()));
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION);
                     * errhint("Use %s %s first.", dropcmd, format_procedure(oldproc->oid)) */
                }
                j += 1;
            }
        }

        /*
         * If there are existing defaults, check compatibility: redefinition
         * must not remove any defaults nor change their types.  (Removing a
         * default might cause a function to fail to satisfy an existing call.
         * Changing type would only be possible if the associated parameter is
         * polymorphic, and in such cases a change of default type might alter
         * the resolved output type of existing calls.)
         */
        if (*oldproc).pronargdefaults != 0 {
            let proargdefaults: Datum;
            let oldDefaults: *mut List;
            let oldlc: *mut ListCell;
            let mut newlc: *mut ListCell;

            if list_length(parameterDefaults) < (*oldproc).pronargdefaults as c_int {
                ereport!(ERROR, errmsg!("cannot remove parameter defaults from existing function"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION);
                 * errhint("Use %s %s first.", dropcmd, format_procedure(oldproc->oid)) */
            }

            proargdefaults = SysCacheGetAttrNotNull(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargdefaults);
            oldDefaults = castNode!(List, T_List, stringToNode(TextDatumGetCString(proargdefaults)));
            Assert!(list_length(oldDefaults) == (*oldproc).pronargdefaults as c_int);

            /* new list can have more defaults than old, advance over 'em */
            newlc = list_nth_cell(
                parameterDefaults,
                list_length(parameterDefaults) - (*oldproc).pronargdefaults as c_int,
            );

            foreach!(oldlc_cell, oldDefaults, {
                let oldlc = current_cell!(oldlc_cell);
                let _ = oldlc;
                let oldDef: *mut Node = lfirst(current_cell!(oldlc_cell)) as *mut Node;
                let newDef: *mut Node = lfirst(newlc) as *mut Node;

                if exprType(oldDef) != exprType(newDef) {
                    ereport!(ERROR, errmsg!("cannot change data type of existing parameter default value"));
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION);
                     * errhint("Use %s %s first.", dropcmd, format_procedure(oldproc->oid)) */
                }
                newlc = lnext(parameterDefaults, newlc);
            });
            let _ = oldlc;
        }

        /*
         * Do not change existing oid, ownership or permissions, either.  Note
         * dependency-update code below has to agree with this decision.
         */
        replaces[Anum_pg_proc_oid - 1] = false;
        replaces[Anum_pg_proc_proowner - 1] = false;
        replaces[Anum_pg_proc_proacl - 1] = false;

        /* Okay, do it... */
        tup = heap_modify_tuple(
            oldtup,
            tupDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
        );
        CatalogTupleUpdate(rel, &raw mut (*tup).t_self as *mut c_void, tup);

        ReleaseSysCache(oldtup);
        is_update = true;
        let _ = dropcmd;
    } else {
        /* Creating a new procedure */
        let newOid: Oid;

        /* First, get default permissions and set up proacl */
        proacl = get_user_default_acl(OBJECT_FUNCTION, proowner, procNamespace);
        if !proacl.is_null() {
            values[Anum_pg_proc_proacl - 1] = PointerGetDatum(proacl as *const c_void);
        } else {
            nulls[Anum_pg_proc_proacl - 1] = true;
        }

        newOid = GetNewOidWithIndex(rel, ProcedureOidIndexId, Anum_pg_proc_oid);
        values[Anum_pg_proc_oid - 1] = ObjectIdGetDatum(newOid);
        tup = heap_form_tuple(tupDesc, values.as_mut_ptr(), nulls.as_mut_ptr());
        CatalogTupleInsert(rel, tup);
        is_update = false;
    }

    retval = (*GETSTRUCT(tup)).oid;

    /*
     * Create dependencies for the new function.  If we are updating an
     * existing function, first delete any existing pg_depend entries.
     * (However, since we are not changing ownership or permissions, the
     * shared dependencies do *not* need to change, and we leave them alone.)
     */
    if is_update {
        deleteDependencyRecordsFor(ProcedureRelationId, retval, true);
    }

    addrs = new_object_addresses();

    ObjectAddressSet!(myself, ProcedureRelationId, retval);

    /* dependency on namespace */
    ObjectAddressSet!(referenced, NamespaceRelationId, procNamespace);
    add_exact_object_address(&raw const referenced, addrs);

    /* dependency on implementation language */
    ObjectAddressSet!(referenced, LanguageRelationId, languageObjectId);
    add_exact_object_address(&raw const referenced, addrs);

    /* dependency on return type */
    ObjectAddressSet!(referenced, TypeRelationId, returnType);
    add_exact_object_address(&raw const referenced, addrs);

    /* dependency on parameter types */
    i = 0;
    while i < allParamCount {
        ObjectAddressSet!(referenced, TypeRelationId, *allParams.add(i as usize));
        add_exact_object_address(&raw const referenced, addrs);
        i += 1;
    }

    /* dependency on transforms, if any */
    foreach_oid!(transformid, trfoids, {
        ObjectAddressSet!(referenced, TransformRelationId, transformid);
        add_exact_object_address(&raw const referenced, addrs);
    });

    /* dependency on support function, if any */
    if OidIsValid(prosupport) {
        ObjectAddressSet!(referenced, ProcedureRelationId, prosupport);
        add_exact_object_address(&raw const referenced, addrs);
    }

    record_object_address_dependencies(&raw const myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    /* dependency on SQL routine body */
    if languageObjectId == SQLlanguageId && !prosqlbody.is_null() {
        recordDependencyOnExpr(&raw const myself, prosqlbody, NIL, DEPENDENCY_NORMAL);
    }

    /* dependency on parameter default expressions */
    if !parameterDefaults.is_null() {
        recordDependencyOnExpr(&raw const myself, parameterDefaults as *const Node, NIL, DEPENDENCY_NORMAL);
    }

    /* dependency on owner */
    if !is_update {
        recordDependencyOnOwner(ProcedureRelationId, retval, proowner);
    }

    /* dependency on any roles mentioned in ACL */
    if !is_update {
        recordDependencyOnNewAcl(ProcedureRelationId, retval, 0, proowner, proacl);
    }

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&raw const myself, is_update);

    heap_freetuple(tup);

    /* Post creation hook for new function */
    InvokeObjectPostCreateHook(ProcedureRelationId, retval, 0);

    table_close(rel, RowExclusiveLock);

    /* Verify function body */
    if OidIsValid(languageValidator) {
        let mut set_items: *mut ArrayType = null_mut();
        let mut save_nestlevel: c_int = 0;

        /* Advance command counter so new tuple can be seen by validator */
        CommandCounterIncrement();

        /*
         * Set per-function configuration parameters so that the validation is
         * done with the environment the function expects.  However, if
         * check_function_bodies is off, we don't do this, because that would
         * create dump ordering hazards that pg_dump doesn't know how to deal
         * with.  (For example, a SET clause might refer to a not-yet-created
         * text search configuration.)	This means that the validator
         * shouldn't complain about anything that might depend on a GUC
         * parameter when check_function_bodies is off.
         */
        if check_function_bodies {
            set_items = DatumGetPointer(proconfig) as *mut ArrayType;
            if !set_items.is_null() {
                /* Need a new GUC nesting level */
                save_nestlevel = NewGUCNestLevel();
                ProcessGUCArray(
                    set_items,
                    if superuser() { PGC_SUSET } else { PGC_USERSET },
                    PGC_S_SESSION,
                    GUC_ACTION_SAVE,
                );
            }
        }

        OidFunctionCall1(languageValidator, ObjectIdGetDatum(retval));

        if !set_items.is_null() {
            AtEOXact_GUC(true, save_nestlevel);
        }
    }

    /* ensure that stats are dropped if transaction aborts */
    if !is_update {
        pgstat_create_function(retval);
    }

    myself
}

/*
 * Validator for internal functions
 *
 * Check that the given internal function name (the "prosrc" value) is
 * a known builtin function.
 */
pub unsafe fn fmgr_internal_validator(fcinfo: FunctionCallInfo) -> Datum {
    let funcoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let tuple: HeapTuple;
    let tmp: Datum;
    let prosrc: *mut c_char;

    if !CheckFunctionValidatorAccess((*(*fcinfo).flinfo).fn_oid, funcoid) {
        PG_RETURN_VOID!();
    }

    /*
     * We do not honor check_function_bodies since it's unlikely the function
     * name will be found later if it isn't there now.
     */

    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for function {}", funcoid);
    }

    tmp = SysCacheGetAttrNotNull(PROCOID, tuple, Anum_pg_proc_prosrc);
    prosrc = TextDatumGetCString(tmp);

    if fmgr_internal_function(prosrc) == InvalidOid {
        ereport!(ERROR, errmsg!("there is no built-in function named \"{}\"",
            std::ffi::CStr::from_ptr(prosrc).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
        let _ = ERRCODE_UNDEFINED_FUNCTION;
    }

    ReleaseSysCache(tuple);

    PG_RETURN_VOID!()
}

/*
 * Validator for C language functions
 *
 * Make sure that the library file exists, is loadable, and contains
 * the specified link symbol. Also check for a valid function
 * information record.
 */
pub unsafe fn fmgr_c_validator(fcinfo: FunctionCallInfo) -> Datum {
    let funcoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut libraryhandle: *mut c_void = null_mut();
    let tuple: HeapTuple;
    let mut tmp: Datum;
    let prosrc: *mut c_char;
    let probin: *mut c_char;

    if !CheckFunctionValidatorAccess((*(*fcinfo).flinfo).fn_oid, funcoid) {
        PG_RETURN_VOID!();
    }

    /*
     * It'd be most consistent to skip the check if !check_function_bodies,
     * but the purpose of that switch is to be helpful for pg_dump loading,
     * and for pg_dump loading it's much better if we *do* check.
     */

    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for function {}", funcoid);
    }

    tmp = SysCacheGetAttrNotNull(PROCOID, tuple, Anum_pg_proc_prosrc);
    prosrc = TextDatumGetCString(tmp);

    tmp = SysCacheGetAttrNotNull(PROCOID, tuple, Anum_pg_proc_probin);
    probin = TextDatumGetCString(tmp);

    let _ = load_external_function(probin, prosrc, true, &raw mut libraryhandle);
    let _ = fetch_finfo_record(libraryhandle, prosrc);

    ReleaseSysCache(tuple);

    PG_RETURN_VOID!()
}

/*
 * Validator for SQL language functions
 *
 * Parse it here in order to be sure that it contains no syntax errors.
 */
pub unsafe fn fmgr_sql_validator(fcinfo: FunctionCallInfo) -> Datum {
    let funcoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let tuple: HeapTuple;
    let proc: Form_pg_proc;
    let raw_parsetree_list: *mut List;
    let mut querytree_list: *mut List;
    let lc: *mut ListCell;
    let mut isnull: bool = false;
    let mut tmp: Datum;
    let prosrc: *mut c_char;
    let mut callback_arg: parse_error_callback_arg = core::mem::zeroed();
    let mut sqlerrcontext: ErrorContextCallback = ErrorContextCallback {
        callback: sql_function_parse_error_callback,
        arg: core::ptr::null_mut(),
        previous: core::ptr::null_mut(),
    };
    let mut haspolyarg: bool;
    let mut i: c_int;

    if !CheckFunctionValidatorAccess((*(*fcinfo).flinfo).fn_oid, funcoid) {
        PG_RETURN_VOID!();
    }

    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for function {}", funcoid);
    }
    proc = GETSTRUCT(tuple);

    /* Disallow pseudotype result */
    /* except for RECORD, VOID, or polymorphic */
    if get_typtype((*proc).prorettype) == TYPTYPE_PSEUDO
        && (*proc).prorettype != RECORDOID
        && (*proc).prorettype != VOIDOID
        && !IsPolymorphicType((*proc).prorettype)
    {
        ereport!(ERROR, errmsg!("SQL functions cannot return type {}",
            std::ffi::CStr::from_ptr(format_type_be((*proc).prorettype)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
    }

    /* Disallow pseudotypes in arguments */
    /* except for polymorphic */
    haspolyarg = false;
    i = 0;
    while i < (*proc).pronargs as c_int {
        if get_typtype(*(*proc).proargtypes.values.as_ptr().add(i as usize)) == TYPTYPE_PSEUDO {
            if IsPolymorphicType(*(*proc).proargtypes.values.as_ptr().add(i as usize)) {
                haspolyarg = true;
            } else {
                ereport!(ERROR, errmsg!("SQL functions cannot have arguments of type {}",
                    std::ffi::CStr::from_ptr(format_type_be(*(*proc).proargtypes.values.as_ptr().add(i as usize))).to_string_lossy()));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
            }
        }
        i += 1;
    }

    /* Postpone body checks if !check_function_bodies */
    if check_function_bodies {
        tmp = SysCacheGetAttrNotNull(PROCOID, tuple, Anum_pg_proc_prosrc);
        prosrc = TextDatumGetCString(tmp);

        /*
         * Setup error traceback support for ereport().
         */
        callback_arg.proname = NameStr(&(*proc).proname) as *mut c_char;
        callback_arg.prosrc = prosrc;

        sqlerrcontext.callback = sql_function_parse_error_callback;
        sqlerrcontext.arg = &raw mut callback_arg as *mut c_void;
        sqlerrcontext.previous = error_context_stack;
        error_context_stack = &raw mut sqlerrcontext;

        /* If we have prosqlbody, pay attention to that not prosrc */
        tmp = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosqlbody, &raw mut isnull);
        if !isnull {
            let n: *mut Node;
            let stored_query_list: *mut List;

            n = stringToNode(TextDatumGetCString(tmp));
            if IsA!(n, T_List) {
                stored_query_list = linitial(castNode!(List, T_List, n)) as *mut List;
            } else {
                stored_query_list = list_make1!(n as *mut c_void);
            }

            querytree_list = NIL;
            foreach!(lc_cell, stored_query_list, {
                let parsetree: *mut Query = lfirst_node!(Query, T_Query, current_cell!(lc_cell));
                let querytree_sublist: *mut List;

                /*
                 * Typically, we'd have acquired locks already while parsing
                 * the body of the CREATE FUNCTION command.  However, a
                 * validator function cannot assume that it's only called in
                 * that context.
                 */
                AcquireRewriteLocks(parsetree, true, false);
                querytree_sublist = pg_rewrite_query(parsetree);
                querytree_list = lappend(querytree_list, querytree_sublist as *mut c_void);
            });
        } else {
            /*
             * We can't do full prechecking of the function definition if
             * there are any polymorphic input types, because actual datatypes
             * of expression results will be unresolvable.  The check will be
             * done at runtime instead.
             *
             * We can run the text through the raw parser though; this will at
             * least catch silly syntactic errors.
             */
            raw_parsetree_list = pg_parse_query(prosrc);
            querytree_list = NIL;

            if !haspolyarg {
                /*
                 * OK to do full precheck: analyze and rewrite the queries,
                 * then verify the result type.
                 */
                let pinfo: SQLFunctionParseInfoPtr;

                /* But first, set up parameter information */
                pinfo = prepare_sql_fn_parse_info(tuple, null_mut(), InvalidOid);

                foreach!(lc_cell, raw_parsetree_list, {
                    let parsetree: *mut RawStmt = lfirst_node!(RawStmt, T_RawStmt, current_cell!(lc_cell));
                    let querytree_sublist: *mut List;

                    querytree_sublist = pg_analyze_and_rewrite_withcb(
                        parsetree,
                        prosrc,
                        sql_fn_parser_setup as ParserSetupHook,
                        pinfo,
                        null_mut(),
                    );
                    querytree_list = lappend(querytree_list, querytree_sublist as *mut c_void);
                });
            }
        }
        let _ = lc;

        if !haspolyarg {
            let mut rettype: Oid = InvalidOid;
            let mut rettupdesc: TupleDesc = null_mut();

            check_sql_fn_statements(querytree_list);
            let _ = get_func_result_type(funcoid, &raw mut rettype, &raw mut rettupdesc);
            let _ = check_sql_fn_retval(querytree_list, rettype, rettupdesc, (*proc).prokind, false);
        }

        error_context_stack = sqlerrcontext.previous;
    }

    ReleaseSysCache(tuple);

    PG_RETURN_VOID!()
}

/*
 * Error context callback for handling errors in SQL function definitions
 */
unsafe fn sql_function_parse_error_callback(arg: *mut c_void) {
    let callback_arg: *mut parse_error_callback_arg = arg as *mut parse_error_callback_arg;

    /* See if it's a syntax error; if so, transpose to CREATE FUNCTION */
    if !function_parse_error_transpose((*callback_arg).prosrc) {
        /* If it's not a syntax error, push info onto context stack */
        errcontext!("SQL function \"{}\"",
            std::ffi::CStr::from_ptr((*callback_arg).proname).to_string_lossy());
    }
}

/*
 * Adjust a syntax error occurring inside the function body of a CREATE
 * FUNCTION or DO command.  This can be used by any function validator or
 * anonymous-block handler, not only for SQL-language functions.
 * It is assumed that the syntax error position is initially relative to the
 * function body string (as passed in).  If possible, we adjust the position
 * to reference the original command text; if we can't manage that, we set
 * up an "internal query" syntax error instead.
 *
 * Returns true if a syntax error was processed, false if not.
 */
#[no_mangle]
pub unsafe fn function_parse_error_transpose(prosrc: *const c_char) -> bool {
    let mut origerrposition: c_int;
    let newerrposition: c_int;

    /*
     * Nothing to do unless we are dealing with a syntax error that has a
     * cursor position.
     *
     * Some PLs may prefer to report the error position as an internal error
     * to begin with, so check that too.
     */
    origerrposition = geterrposition();
    if origerrposition <= 0 {
        origerrposition = getinternalerrposition();
        if origerrposition <= 0 {
            return false;
        }
    }

    /* We can get the original query text from the active portal (hack...) */
    if !ActivePortal.is_null() && (*ActivePortal).status == PORTAL_ACTIVE {
        let queryText: *const c_char = (*ActivePortal).sourceText;

        /* Try to locate the prosrc in the original text */
        newerrposition = match_prosrc_to_query(prosrc, queryText, origerrposition);
    } else {
        /*
         * Quietly give up if no ActivePortal.  This is an unusual situation
         * but it can happen in, e.g., logical replication workers.
         */
        newerrposition = -1;
    }

    if newerrposition > 0 {
        /* Successful, so fix error position to reference original query */
        errposition(newerrposition);
        /* Get rid of any report of the error as an "internal query" */
        internalerrposition(0);
        internalerrquery(null());
    } else {
        /*
         * If unsuccessful, convert the position to an internal position
         * marker and give the function text as the internal query.
         */
        errposition(0);
        internalerrposition(origerrposition);
        internalerrquery(prosrc);
    }

    true
}

/*
 * Try to locate the string literal containing the function body in the
 * given text of the CREATE FUNCTION or DO command.  If successful, return
 * the character (not byte) index within the command corresponding to the
 * given character index within the literal.  If not successful, return 0.
 */
unsafe fn match_prosrc_to_query(prosrc: *const c_char, queryText: *const c_char, cursorpos: c_int) -> c_int {
    /*
     * Rather than fully parsing the original command, we just scan the
     * command looking for $prosrc$ or 'prosrc'.  This could be fooled (though
     * not in any very probable scenarios), so fail if we find more than one
     * match.
     */
    let prosrclen: c_int = strlen(prosrc) as c_int;
    let querylen: c_int = strlen(queryText) as c_int;
    let mut matchpos: c_int = 0;
    let mut curpos: c_int;
    let mut newcursorpos: c_int = 0;

    curpos = 0;
    while curpos < querylen - prosrclen {
        if *queryText.add(curpos as usize) == b'$' as c_char
            && strncmp(prosrc, queryText.add((curpos + 1) as usize), prosrclen as usize) == 0
            && *queryText.add((curpos + 1 + prosrclen) as usize) == b'$' as c_char
        {
            /*
             * Found a $foo$ match.  Since there are no embedded quoting
             * characters in a dollar-quoted literal, we don't have to do any
             * fancy arithmetic; just offset by the starting position.
             */
            if matchpos != 0 {
                return 0; /* multiple matches, fail */
            }
            matchpos = pg_mbstrlen_with_len(queryText, curpos + 1) + cursorpos;
        } else if *queryText.add(curpos as usize) == b'\'' as c_char
            && match_prosrc_to_literal(prosrc, queryText.add((curpos + 1) as usize), cursorpos, &raw mut newcursorpos)
        {
            /*
             * Found a 'foo' match.  match_prosrc_to_literal() has adjusted
             * for any quotes or backslashes embedded in the literal.
             */
            if matchpos != 0 {
                return 0; /* multiple matches, fail */
            }
            matchpos = pg_mbstrlen_with_len(queryText, curpos + 1) + newcursorpos;
        }
        curpos += 1;
    }

    matchpos
}

/*
 * Try to match the given source text to a single-quoted literal.
 * If successful, adjust newcursorpos to correspond to the character
 * (not byte) index corresponding to cursorpos in the source text.
 *
 * At entry, literal points just past a ' character.  We must check for the
 * trailing quote.
 */
unsafe fn match_prosrc_to_literal(
    mut prosrc: *const c_char,
    mut literal: *const c_char,
    mut cursorpos: c_int,
    newcursorpos: *mut c_int,
) -> bool {
    let mut newcp: c_int = cursorpos;
    let chlen: c_int;

    /*
     * This implementation handles backslashes and doubled quotes in the
     * string literal.  It does not handle the SQL syntax for literals
     * continued across line boundaries.
     *
     * We do the comparison a character at a time, not a byte at a time, so
     * that we can do the correct cursorpos math.
     */
    'outer: loop {
        if *prosrc == 0 {
            break;
        }
        cursorpos -= 1; /* characters left before cursor */

        /*
         * Check for backslashes and doubled quotes in the literal; adjust
         * newcp when one is found before the cursor.
         */
        if *literal == b'\\' as c_char {
            literal = literal.add(1);
            if cursorpos > 0 {
                newcp += 1;
            }
        } else if *literal == b'\'' as c_char {
            if *literal.add(1) != b'\'' as c_char {
                break 'outer; /* goto fail */
            }
            literal = literal.add(1);
            if cursorpos > 0 {
                newcp += 1;
            }
        }
        let chlen_inner: c_int = pg_mblen_cstr(prosrc);
        if strncmp(prosrc, literal, chlen_inner as usize) != 0 {
            break 'outer; /* goto fail */
        }
        prosrc = prosrc.add(chlen_inner as usize);
        literal = literal.add(chlen_inner as usize);
    }

    if *prosrc == 0 && *literal == b'\'' as c_char && *literal.add(1) != b'\'' as c_char {
        /* success */
        *newcursorpos = newcp;
        return true;
    }

    /* fail: */
    /* Must set *newcursorpos to suppress compiler warning */
    *newcursorpos = newcp;
    let _ = chlen;
    false
}

pub unsafe fn oid_array_to_list(datum: Datum) -> *mut List {
    let array: *mut ArrayType = DatumGetArrayTypeP(datum);
    let mut values: *mut Datum = null_mut();
    let mut nelems: c_int = 0;
    let mut i: c_int;
    let mut result: *mut List = NIL;

    deconstruct_array_builtin(array, OIDOID, &raw mut values, null_mut(), &raw mut nelems);
    i = 0;
    while i < nelems {
        result = lappend_oid(result, DatumGetObjectId(*values.add(i as usize)));
        i += 1;
    }
    result
}

/* libc/str helpers used above (string.h). */
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}
unsafe fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    let mut i: usize = 0;
    while i < n {
        let ca = *a.add(i);
        let cb = *b.add(i);
        if ca != cb {
            return (ca as u8 as c_int) - (cb as u8 as c_int);
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
    0
}
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i: usize = 0;
    loop {
        let ca = *a.add(i);
        let cb = *b.add(i);
        if ca != cb {
            return (ca as u8 as c_int) - (cb as u8 as c_int);
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/* DatumGetObjectId (postgres.h); local shim if not imported via prelude. */
unsafe fn DatumGetObjectId(d: Datum) -> Oid { d as u32 as Oid }

/* FunctionCallInfo (fmgr.h): use the real fmgr types (they carry flinfo + args). */
use crate::utils::fmgr::{FunctionCallInfo, FunctionCallInfoBaseData, FmgrInfo};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // proname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_proc, proname), 4);
        // prorettype is the last fixed field; the struct must be at least large
        // enough to hold it (alignment may add trailing padding, as in C).
        assert!(
            core::mem::size_of::<FormData_pg_proc>()
                >= core::mem::offset_of!(FormData_pg_proc, prorettype) + core::mem::size_of::<Oid>()
        );
        // regproc is faithfully an Oid (4 bytes).
        assert_eq!(core::mem::size_of::<regproc>(), core::mem::size_of::<Oid>());
    }
}

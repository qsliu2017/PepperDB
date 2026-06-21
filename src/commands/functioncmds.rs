/*-------------------------------------------------------------------------
 *
 * functioncmds.rs
 *
 *	  Routines for CREATE and DROP FUNCTION commands and CREATE and DROP
 *	  CAST commands.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/functioncmds.c
 *
 * DESCRIPTION
 *	  These routines take the parse tree and pick out the
 *	  appropriate arguments/flags, and pass the results to the
 *	  corresponding "FooCreate" routines (in src/backend/catalog) that do
 *	  the actual catalog-munging.  These routines also verify permission
 *	  of the user to execute the command.
 *
 * NOTES
 *	  These things must be defined and committed in the following order:
 *		"create function":
 *				input/output, recv/send procedures
 *		"create type":
 *				type
 *		"create operator":
 *				operators
 *
 *-------------------------------------------------------------------------
 */
#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]
#![allow(unused_variables, unused_mut, unused_assignments, dead_code)]

use crate::prelude::*;
use crate::{foreach, current_cell, lfirst_node, castNode, makeNode, IsA};

use std::ffi::{c_char, c_int, c_void};

use crate::nodes::pg_list::{List, ListCell, NIL};
use crate::nodes::nodes::Node;
use crate::parser::parse_node::ParseState;

/* errmsg_plural stub  TODO(pg-port) */
macro_rules! errmsg_plural {
    ($singular:expr, $plural:expr, $n:expr, $($arg:tt)*) => {
        if $n == 1 {
            format!($singular, $($arg)*)
        } else {
            format!($plural, $($arg)*)
        }
    };
}

/* --------------------------------------------------------------------------
 * Local type stubs for unported dependencies  TODO(pg-port)
 * -------------------------------------------------------------------------- */

// HeapTuple is a single pointer (HeapTupleData*).
use crate::access::htup_details::HeapTupleData;
type HeapTuple = *mut HeapTupleData;

// Relation pointer
type RelationData = crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// TupleDesc
use crate::access::common::tupdesc::{TupleDescData, TupleDesc};

// oidvector / ArrayType
use crate::c::oidvector;
use crate::utils::array::ArrayType;

// ObjectAddress / ObjectAddresses  TODO(pg-port)
use crate::catalog::objectaccess::ObjectAddress;
#[repr(C)] pub struct ObjectAddresses { _opaque: [u8; 0] }

// Parse-tree node stubs  TODO(pg-port)
#[repr(C)] pub struct TypeName {
    pub r#type: NodeTag,
    pub names: *mut List,
    pub typeOid: Oid,
    pub setof: bool,
    pub pct_type: bool,
    pub typmods: *mut List,
    pub typemod: int32,
    pub arrayBounds: *mut List,
    pub location: c_int,
}
#[repr(C)] pub struct DefElem {
    pub r#type: NodeTag,
    pub defnamespace: *mut c_char,
    pub defname: *mut c_char,
    pub arg: *mut Node,
    pub defaction: c_int,
    pub location: c_int,
}
#[repr(C)] pub struct FunctionParameter {
    pub r#type: NodeTag,
    pub name: *mut c_char,
    pub argType: *mut TypeName,
    pub mode: FunctionParameterMode,
    pub defexpr: *mut Node,
}
#[repr(C)] pub struct VariableSetStmt {
    pub r#type: NodeTag,
    pub kind: VariableSetKind,
    pub name: *mut c_char,
    pub args: *mut List,
    pub jumble_args: bool,
    pub is_local: bool,
    pub location: c_int,
}
#[repr(C)] pub struct CreateFunctionStmt {
    pub r#type: NodeTag,
    pub is_procedure: bool,
    pub replace: bool,
    pub funcname: *mut List,
    pub parameters: *mut List,
    pub returnType: *mut TypeName,
    pub options: *mut List,
    pub sql_body: *mut Node,
}
#[repr(C)] pub struct AlterFunctionStmt {
    pub r#type: NodeTag,
    pub objtype: ObjectType,
    pub func: *mut ObjectWithArgs,
    pub actions: *mut List,
}
#[repr(C)] pub struct CreateCastStmt {
    pub r#type: NodeTag,
    pub sourcetype: *mut TypeName,
    pub targettype: *mut TypeName,
    pub func: *mut ObjectWithArgs,
    pub context: CoercionContext,
    pub inout: bool,
}
#[repr(C)] pub struct CreateTransformStmt {
    pub r#type: NodeTag,
    pub replace: bool,
    pub type_name: *mut TypeName,
    pub lang: *mut c_char,
    pub fromsql: *mut ObjectWithArgs,
    pub tosql: *mut ObjectWithArgs,
}
#[repr(C)] pub struct DoStmt {
    pub r#type: NodeTag,
    pub args: *mut List,
}
#[repr(C)] pub struct CallStmt {
    pub r#type: NodeTag,
    pub funcexpr: *mut FuncExpr,
    pub outargs: *mut List,
}
#[repr(C)] pub struct ObjectWithArgs {
    pub r#type: NodeTag,
    pub objname: *mut List,
    pub objargs: *mut List,
    pub objfuncargs: *mut List,
    pub args_unspecified: bool,
}
#[repr(C)] pub struct InlineCodeBlock {
    pub r#type: NodeTag,
    pub source_text: *mut c_char,
    pub langOid: Oid,
    pub langIsTrusted: bool,
    pub atomic: bool,
}
#[repr(C)] pub struct CallContext {
    pub r#type: NodeTag,
    pub atomic: bool,
}
#[repr(C)] pub struct FuncExpr {
    pub r#type: NodeTag,
    pub funcid: Oid,
    pub funcresulttype: Oid,
    pub funcretset: bool,
    pub funcvariadic: bool,
    pub funcformat: c_int,
    pub funccollid: Oid,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub location: c_int,
}
#[repr(C)] pub struct Query {
    pub r#type: NodeTag,
    pub commandType: CmdType,
    pub utilityStmt: *mut Node,
}

// FunctionParameterMode  TODO(pg-port)
type FunctionParameterMode = c_char;
const FUNC_PARAM_IN: FunctionParameterMode = b'i' as c_char;
const FUNC_PARAM_OUT: FunctionParameterMode = b'o' as c_char;
const FUNC_PARAM_INOUT: FunctionParameterMode = b'b' as c_char;
const FUNC_PARAM_VARIADIC: FunctionParameterMode = b'v' as c_char;
const FUNC_PARAM_TABLE: FunctionParameterMode = b't' as c_char;
const FUNC_PARAM_DEFAULT: FunctionParameterMode = b'd' as c_char;

// VariableSetKind  TODO(pg-port)
type VariableSetKind = c_int;
const VAR_SET_VALUE: VariableSetKind = 0;
const VAR_SET_DEFAULT: VariableSetKind = 1;
const VAR_SET_CURRENT: VariableSetKind = 2;
const VAR_SET_MULTI: VariableSetKind = 3;
const VAR_RESET: VariableSetKind = 4;
const VAR_RESET_ALL: VariableSetKind = 5;

// ObjectType  TODO(pg-port)
type ObjectType = c_int;
const OBJECT_AGGREGATE: ObjectType = 1;
const OBJECT_FUNCTION: ObjectType = 18;
const OBJECT_PROCEDURE: ObjectType = 30;
const OBJECT_SCHEMA: ObjectType = 37;
const OBJECT_LANGUAGE: ObjectType = 23;

// CmdType  TODO(pg-port)
type CmdType = c_int;
const CMD_UTILITY: CmdType = 6;

// CoercionContext  TODO(pg-port)
type CoercionContext = c_int;
const COERCION_IMPLICIT: CoercionContext = 0;
const COERCION_ASSIGNMENT: CoercionContext = 1;
const COERCION_PLPGSQL: CoercionContext = 2;
const COERCION_EXPLICIT: CoercionContext = 3;

// NodeTag - use the real enum so makeNode!/IsA!/castNode! tag writes typecheck.
use crate::nodes::nodes::NodeTag;

// AclResult  TODO(pg-port)
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NO_PRIV: AclResult = 1;
const ACLCHECK_NOT_OWNER: AclResult = 2;

// Type (a HeapTuple from the type syscache)  TODO(pg-port)
type Type = HeapTuple;

// Form structs  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_type { _opaque: [u8; 0] }
type Form_pg_type = *mut FormData_pg_type;
#[repr(C)] pub struct FormData_pg_proc { _opaque: [u8; 0] }
type Form_pg_proc = *mut FormData_pg_proc;
#[repr(C)] pub struct FormData_pg_language { _opaque: [u8; 0] }
type Form_pg_language = *mut FormData_pg_language;
#[repr(C)] pub struct FormData_pg_transform { _opaque: [u8; 0] }
type Form_pg_transform = *mut FormData_pg_transform;
#[repr(C)] pub struct FormData_pg_attribute { _opaque: [u8; 0] }
type Form_pg_attribute = *mut FormData_pg_attribute;

// oidvector backing accessor: an oidvector has a `values` flexible array.

// SQL function parse info  TODO(pg-port)
#[repr(C)] pub struct SQLFunctionParseInfo {
    pub fname: *mut c_char,
    pub nargs: c_int,
    pub argtypes: *mut Oid,
    pub argnames: *mut *mut c_char,
    pub collation: Oid,
}
type SQLFunctionParseInfoPtr = *mut SQLFunctionParseInfo;

// ItemPointerData / HeapTupleHeader and CALL types  TODO(pg-port)
use crate::storage::itemptr::ItemPointerData;
use crate::access::htup_details::HeapTupleHeaderData;
type HeapTupleHeader = *mut HeapTupleHeaderData;
#[repr(C)] pub struct EState { _opaque: [u8; 0] }
use crate::nodes::execnodes::ExprContext;
#[repr(C)] pub struct ExprState { _opaque: [u8; 0] }
#[repr(C)] pub struct Expr { _opaque: [u8; 0] }
#[repr(C)] pub struct TupleTableSlotData { _opaque: [u8; 0] }
type TupleTableSlot = *mut TupleTableSlotData;
#[repr(C)] pub struct TupOutputState { _opaque: [u8; 0] }
#[repr(C)] pub struct DestReceiver { _opaque: [u8; 0] }
#[repr(C)] pub struct ParamListInfoData { _opaque: [u8; 0] }
type ParamListInfo = *mut ParamListInfoData;
#[repr(C)] pub struct FmgrInfo { _opaque: [u8; 0] }
#[repr(C)] pub struct PgStat_FunctionCallUsage { _opaque: [u8; 0] }
#[repr(C)] pub struct SnapshotData { _opaque: [u8; 0] }
type Snapshot = *mut SnapshotData;

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const NoLock: LOCKMODE = 0;
const RowExclusiveLock: LOCKMODE = 3;

// catalog OIDs  TODO(pg-port)
const TypeRelationId: Oid = 1247;
const ProcedureRelationId: Oid = 1255;
const NamespaceRelationId: Oid = 2615;
const LanguageRelationId: Oid = 2612;
const TransformRelationId: Oid = 3576;
const AggregateRelationId: Oid = 2600;
const CastRelationId: Oid = 2605;

// index OIDs  TODO(pg-port)
const TransformOidIndexId: Oid = 3574;

// syscache ids  TODO(pg-port)
const TYPEOID: c_int = 82;
const PROCOID: c_int = 47;
const PROCNAMEARGSNSP: c_int = 46;
const LANGNAME: c_int = 35;
const AGGFNOID: c_int = 0;
const TRFTYPELANG: c_int = 71;

// type OIDs  TODO(pg-port)
const OIDOID: Oid = 26;
const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const TEXTOID: Oid = 25;
const CHAROID: Oid = 18;
const VOIDOID: Oid = 2278;
const RECORDOID: Oid = 2249;
const INTERNALOID: Oid = 2281;
const ANYOID: Oid = 2276;
const ANYARRAYOID: Oid = 2277;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;

// language OIDs  TODO(pg-port)
const INTERNALlanguageId: Oid = 12;
const ClanguageId: Oid = 13;
const SQLlanguageId: Oid = 14;

// prokind  TODO(pg-port)
const PROKIND_FUNCTION: c_char = b'f' as c_char;
const PROKIND_PROCEDURE: c_char = b'p' as c_char;
const PROKIND_AGGREGATE: c_char = b'a' as c_char;
const PROKIND_WINDOW: c_char = b'w' as c_char;

// provolatile  TODO(pg-port)
const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;
const PROVOLATILE_STABLE: c_char = b's' as c_char;
const PROVOLATILE_VOLATILE: c_char = b'v' as c_char;

// proparallel  TODO(pg-port)
const PROPARALLEL_SAFE: c_char = b's' as c_char;
const PROPARALLEL_RESTRICTED: c_char = b'r' as c_char;
const PROPARALLEL_UNSAFE: c_char = b'u' as c_char;

// typtype  TODO(pg-port)
const TYPTYPE_PSEUDO: c_char = b'p' as c_char;
const TYPTYPE_DOMAIN: c_char = b'd' as c_char;
const TYPTYPE_COMPOSITE: c_char = b'c' as c_char;
const TYPTYPE_RANGE: c_char = b'r' as c_char;
const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char;
const TYPTYPE_ENUM: c_char = b'e' as c_char;

// coercion  TODO(pg-port)
const COERCION_METHOD_FUNCTION: c_char = b'f' as c_char;
const COERCION_METHOD_BINARY: c_char = b'b' as c_char;
const COERCION_METHOD_INOUT: c_char = b'i' as c_char;
const COERCION_CODE_IMPLICIT: c_char = b'i' as c_char;
const COERCION_CODE_ASSIGNMENT: c_char = b'a' as c_char;
const COERCION_CODE_EXPLICIT: c_char = b'e' as c_char;

// dependency types  TODO(pg-port)
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;

// ACL permissions  TODO(pg-port)
const ACL_CREATE: c_uint = 1 << 3;
const ACL_USAGE: c_uint = 1 << 10;
const ACL_EXECUTE: c_uint = 1 << 5;

// Anum_pg_proc_*  TODO(pg-port)
const Anum_pg_proc_proconfig: c_int = 29;
const Natts_pg_proc: usize = 30;

// Anum_pg_transform_*  TODO(pg-port)
const Anum_pg_transform_oid: c_int = 1;
const Anum_pg_transform_trftype: c_int = 2;
const Anum_pg_transform_trflang: c_int = 3;
const Anum_pg_transform_trffromsql: c_int = 4;
const Anum_pg_transform_trftosql: c_int = 5;
const Natts_pg_transform: usize = 5;

// EXPR_KIND_*  TODO(pg-port)
const EXPR_KIND_FUNCTION_DEFAULT: c_int = 0;

// FUNC_MAX_ARGS  TODO(pg-port)
const FUNC_MAX_ARGS: c_int = 100;

/* TTSOpsHeapTuple  TODO(pg-port) */
#[repr(C)] pub struct TupleTableSlotOps { _opaque: [u8; 0] }
extern "C" {
    static TTSOpsHeapTuple: TupleTableSlotOps;
}

/* --------------------------------------------------------------------------
 * Stub implementations for unported dependencies  TODO(pg-port)
 * -------------------------------------------------------------------------- */

unsafe fn LookupTypeName(pstate: *mut ParseState, typeName: *const TypeName,
                         typmod_p: *mut int32, missing_ok: bool) -> Type { crate::parser::parse_type::LookupTypeName(pstate as _, typeName as _, typmod_p as _, missing_ok as _) as _ }
unsafe fn TypeNameToString(typeName: *const TypeName) -> *mut c_char { crate::parser::parse_type::TypeNameToString(typeName as _) as _ }
unsafe fn typeTypeId(tup: Type) -> Oid { crate::parser::parse_type::typeTypeId(tup as _) as _ }
unsafe fn typenameTypeId(pstate: *mut ParseState, typeName: *const TypeName) -> Oid { crate::parser::parse_type::typenameTypeId(pstate as _, typeName as _) as _ }
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void { crate::access::htup_details::GETSTRUCT(tup as _) as _ }
unsafe fn ReleaseSysCache(tup: HeapTuple) { crate::utils::cache::syscache::ReleaseSysCache(tup as _) }
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache1(cacheId as _, key1 as _) as _ }
unsafe fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple { crate::utils::cache::syscache::SearchSysCache2(cacheId as _, key1 as _, key2 as _) as _ }
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple { crate::catalog::objectaddress_impl::SearchSysCacheCopy1(cacheId as _, key1 as _) as _ }
unsafe fn SearchSysCacheExists3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> bool { crate::utils::cache::lsyscache::SearchSysCacheExists3(cacheId as _, key1 as _, key2 as _, key3 as _) as _ }
unsafe fn SysCacheGetAttr(cacheId: c_int, tup: HeapTuple, attNum: c_int, isnull: *mut bool) -> Datum { crate::utils::cache::syscache::SysCacheGetAttr(cacheId as _, tup as _, attNum as _, isnull as _) as _ }
unsafe fn GetSysCacheOid2(cacheId: c_int, oidAttNum: c_int, key1: Datum, key2: Datum) -> Oid { crate::utils::cache::lsyscache::GetSysCacheOid2(cacheId as _, oidAttNum as _, key1 as _, key2 as _) as _ }
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool { !tup.is_null() }

unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn superuser() -> bool { crate::utils::misc::superuser::superuser() as _ }
unsafe fn object_aclcheck(classId: Oid, objectId: Oid, userId: Oid, mode: c_uint) -> AclResult { crate::catalog::aclchk::object_aclcheck(classId as _, objectId as _, userId as _, mode as _) as _ }
unsafe fn object_ownercheck(classId: Oid, objectId: Oid, userId: Oid) -> bool { crate::catalog::aclchk::object_ownercheck(classId as _, objectId as _, userId as _) as _ }
unsafe fn aclcheck_error(res: AclResult, objtype: ObjectType, name: *const c_char) { crate::catalog::aclchk::aclcheck_error(core::mem::transmute(res), core::mem::transmute(objtype), name as _) }
unsafe fn aclcheck_error_type(res: AclResult, typeOid: Oid) { crate::catalog::aclchk::aclcheck_error_type(core::mem::transmute(res), typeOid as _) }

unsafe fn QualifiedNameGetCreationNamespace(names: *mut List, objname_p: *mut *mut c_char) -> Oid { crate::catalog::namespace::QualifiedNameGetCreationNamespace(names as _, objname_p as _) as _ }
unsafe fn TypeShellMake(typeName: *const c_char, typeNamespace: Oid, ownerId: Oid) -> ObjectAddress { crate::catalog::pg_type::TypeShellMake(typeName as _, typeNamespace as _, ownerId as _) as _ }
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_namespace_name(nspid as _) as _ }
unsafe fn get_element_type(typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_element_type(typid as _) as _ }
unsafe fn get_base_element_type(typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_base_element_type(typid as _) as _ }
unsafe fn get_typtype(typid: Oid) -> c_char { crate::utils::cache::lsyscache::get_typtype(typid as _) as _ }
unsafe fn get_typlenbyvalalign(typid: Oid, typlen: *mut int16, typbyval: *mut bool, typalign: *mut c_char) { crate::utils::cache::lsyscache::get_typlenbyvalalign(typid as _, typlen as _, typbyval as _, typalign as _) }
unsafe fn get_func_rettype(funcid: Oid) -> Oid { crate::utils::cache::lsyscache::get_func_rettype(funcid as _) as _ }
unsafe fn get_func_name(funcid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_func_name(funcid as _) as _ }
unsafe fn get_language_oid(langname: *const c_char, missing_ok: bool) -> Oid { crate::commands::proclang::get_language_oid(langname as _, missing_ok as _) as _ }
unsafe fn get_language_name(langid: Oid, missing_ok: bool) -> *mut c_char { crate::utils::cache::lsyscache::get_language_name(langid as _, missing_ok as _) as _ }
unsafe fn format_type_be(typid: Oid) -> *mut c_char { crate::utils::adt::format_type::format_type_be(typid as _) as _ }
unsafe fn OidIsValid(oid: Oid) -> bool { oid != InvalidOid }
unsafe fn IsPolymorphicType(typid: Oid) -> bool {
    matches!(typid, 2283 | 2277 | 2776 | 3500 | 3831 | 4537 | 5077 | 5078 | 5079 | 5080 | 4538)
}

unsafe fn buildoidvector(oids: *const Oid, n: c_int) -> *mut oidvector { crate::utils::adt::oid::buildoidvector(oids as _, n as _) as _ }
unsafe fn construct_array_builtin(elems: *const Datum, nelems: c_int, elmtype: Oid) -> *mut ArrayType { crate::utils::adt::arrayfuncs::construct_array_builtin(elems as _, nelems as _, elmtype as _) as _ }
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType { crate::access::nbtree::nbtpreprocesskeys::DatumGetArrayTypeP(d as _) as _ }
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum { crate::utils::builtins::CStringGetTextDatum(s as _) as _ }

unsafe fn palloc(size: usize) -> *mut c_void { crate::utils::mmgr::mcxt::palloc(size as _) as _ }
unsafe fn palloc0(size: usize) -> *mut c_void { crate::utils::mmgr::mcxt::palloc0(size as _) as _ }
unsafe fn pstrdup(s: *const c_char) -> *mut c_char { crate::utils::mmgr::mcxt::pstrdup(s as _) as _ }
unsafe fn pfree(p: *mut c_void) { crate::utils::mmgr::mcxt::pfree(p as _) }

unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List { crate::nodes::pg_list::lappend(list as _, datum as _) as _ }
unsafe fn lappend_oid(list: *mut List, datum: Oid) -> *mut List { crate::nodes::pg_list::lappend_oid(list as _, datum as _) as _ }
unsafe fn list_length(list: *const List) -> c_int { crate::nodes::pg_list::list_length(list as _) as _ }
unsafe fn list_make1(d: *mut c_void) -> *mut List { crate::list_make1!(d) as _ }
unsafe fn lfirst(lc: *const ListCell) -> *mut c_void { crate::nodes::pg_list::lfirst(lc as _) as _ }
unsafe fn lfirst_oid(lc: *const ListCell) -> Oid { crate::nodes::pg_list::lfirst_oid(lc as _) as _ }
unsafe fn linitial(list: *const List) -> *mut c_void { crate::nodes::pg_list::linitial(list as _) as _ }
unsafe fn lsecond(list: *const List) -> *mut c_void { crate::nodes::pg_list::lsecond(list as _) as _ }
unsafe fn list_nth(list: *const List, n: c_int) -> *mut c_void { crate::nodes::pg_list::list_nth(list as _, n as _) as _ }
unsafe fn list_nth_oid(list: *const List, n: c_int) -> Oid { crate::nodes::pg_list::list_nth_oid(list as _, n as _) as _ }
unsafe fn linitial_node_List(list: *const List) -> *mut List { crate::nodes::pg_list::linitial(list as _) as _ }

unsafe fn strVal(node: *const Node) -> *mut c_char { crate::strVal!(node) as _ }
unsafe fn boolVal(node: *const Node) -> bool { crate::boolVal!(node) as _ }
unsafe fn makeString(s: *mut c_char) -> *mut Node { crate::nodes::value::makeString(s as _) as _ }
unsafe fn defGetNumeric(def: *mut DefElem) -> float4 { crate::commands::define::defGetNumeric(def as _) as _ }
unsafe fn defGetQualifiedName(def: *mut DefElem) -> *mut List { crate::commands::define::defGetQualifiedName(def as _) as _ }
unsafe fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState) -> ! { crate::commands::define::errorConflictingDefElem(defel as _, pstate as _) }

unsafe fn NameStr(name: *const c_void) -> *const c_char { crate::parser_link_shims::NameStr(name as _) as _ }
unsafe fn NameListToString(names: *const List) -> *mut c_char { crate::catalog::namespace::NameListToString(names as _) as _ }
unsafe fn func_signature_string(funcname: *mut List, nargs: c_int, argnames: *mut List, argtypes: *const Oid) -> *mut c_char { crate::parser::parse_func::func_signature_string(funcname as _, nargs as _, argnames as _, argtypes as _) as _ }
unsafe fn funcname_signature_string(funcname: *const c_char, nargs: c_int, argnames: *mut List, argtypes: *const Oid) -> *mut c_char { crate::parser::parse_func::funcname_signature_string(funcname as _, nargs as _, argnames as _, argtypes as _) as _ }

unsafe fn LookupFuncName(funcname: *mut List, nargs: c_int, argtypes: *const Oid, missing_ok: bool) -> Oid { crate::parser::parse_func::LookupFuncName(funcname as _, nargs as _, argtypes as _, missing_ok as _) as _ }
unsafe fn LookupFuncWithArgs(objtype: ObjectType, func: *mut ObjectWithArgs, missing_ok: bool) -> Oid { crate::parser::parse_func::LookupFuncWithArgs(core::mem::transmute(objtype), func as _, missing_ok as _) as _ }

unsafe fn ExtractSetVariableArgs(stmt: *mut VariableSetStmt) -> *mut c_char { crate::utils::misc::guc_funcs::ExtractSetVariableArgs(stmt as _) as _ }
unsafe fn GUCArrayAdd(array: *mut ArrayType, name: *const c_char, value: *const c_char) -> *mut ArrayType { crate::utils::misc::guc::GUCArrayAdd(array as _, name as _, value as _) as _ }
unsafe fn GUCArrayDelete(array: *mut ArrayType, name: *const c_char) -> *mut ArrayType { crate::utils::misc::guc::GUCArrayDelete(array as _, name as _) as _ }

unsafe fn extension_file_exists(extensionName: *const c_char) -> bool { crate::commands::extension::extension_file_exists(extensionName as _) as _ }
unsafe fn IsBinaryCoercibleWithCast(srctype: Oid, targettype: Oid, castoid: *mut Oid) -> bool { crate::parser::parse_coerce::IsBinaryCoercibleWithCast(srctype as _, targettype as _, castoid as _) as _ }
unsafe fn get_transform_oid_impl() { }

unsafe fn make_parsestate(parentParseState: *mut ParseState) -> *mut ParseState { crate::parser::parse_node::make_parsestate(parentParseState as _) as _ }
unsafe fn free_parsestate(pstate: *mut ParseState) { crate::parser::parse_node::free_parsestate(pstate as _) }
unsafe fn sql_fn_parser_setup(pstate: *mut ParseState, pinfo: SQLFunctionParseInfoPtr) { crate::executor::functions::sql_fn_parser_setup(pstate as _, pinfo as _) }
unsafe fn transformStmt(pstate: *mut ParseState, parseTree: *mut Node) -> *mut Query { crate::parser::analyze::transformStmt(pstate as _, parseTree as _) as _ }
unsafe fn transformExpr(pstate: *mut ParseState, expr: *mut Node, exprKind: c_int) -> *mut Node { crate::parser::parse_expr::transformExpr(pstate as _, expr as _, core::mem::transmute(exprKind)) as _ }
unsafe fn coerce_to_specific_type(pstate: *mut ParseState, node: *mut Node, targetTypeId: Oid, constructName: *const c_char) -> *mut Node { crate::parser::parse_coerce::coerce_to_specific_type(pstate as _, node as _, targetTypeId as _, constructName as _) as _ }
unsafe fn assign_expr_collations(pstate: *mut ParseState, expr: *mut Node) { crate::parser::parse_collate::assign_expr_collations(pstate as _, expr as _) }
unsafe fn contain_var_clause(node: *mut Node) -> bool { crate::optimizer::util::var::contain_var_clause(node as _) as _ }
unsafe fn parser_errposition(pstate: *mut ParseState, location: c_int) -> c_int { crate::parser::parse_node::parser_errposition(pstate as _, location as _) as _ }

unsafe fn GetCommandTagName(commandTag: c_int) -> *const c_char { crate::tcop::cmdtag::GetCommandTagName(core::mem::transmute(commandTag)) as _ }
unsafe fn CreateCommandTag(parsetree: *mut Node) -> c_int { core::mem::transmute(crate::tcop::utility::CreateCommandTag(parsetree as _)) }

unsafe fn ProcedureCreate(
    procedureName: *const c_char, procNamespace: Oid, replace: bool, returnsSet: bool,
    returnType: Oid, proowner: Oid, languageObjectId: Oid, languageValidator: Oid,
    prosrc: *const c_char, probin: *const c_char, prosqlbody: *mut Node, prokind: c_char,
    security_definer: bool, isLeakProof: bool, isStrict: bool, volatility: c_char,
    parallel: c_char, parameterTypes: *mut oidvector, allParameterTypes: Datum,
    parameterModes: Datum, parameterNames: Datum, parameterDefaults: *mut List,
    trftypes: Datum, trftypes_list: *mut List, proconfig: Datum, prosupport: Oid,
    procost: float4, prorows: float4) -> ObjectAddress { crate::catalog::pg_proc::ProcedureCreate(procedureName as _, procNamespace as _, replace as _, returnsSet as _, returnType as _, proowner as _, languageObjectId as _, languageValidator as _, prosrc as _, probin as _, prosqlbody as _, prokind as _, security_definer as _, isLeakProof as _, isStrict as _, volatility as _, parallel as _, parameterTypes as _, allParameterTypes as _, parameterModes as _, parameterNames as _, parameterDefaults as _, trftypes as _, trftypes_list as _, proconfig as _, prosupport as _, procost as _, prorows as _) as _ }

unsafe fn CatalogTupleDelete(heapRel: Relation, tid: *mut ItemPointerData) { crate::catalog::indexing::CatalogTupleDelete(heapRel as _, tid as _) }
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *mut ItemPointerData, tup: HeapTuple) { crate::catalog::indexing::CatalogTupleUpdate(heapRel as _, otid as _, tup as _) }
unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) { crate::catalog::indexing::CatalogTupleInsert(heapRel as _, tup as _) }
unsafe fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation { crate::access::table::table::table_open(relationId as _, lockmode as _) as _ }
unsafe fn table_close(relation: Relation, lockmode: LOCKMODE) { crate::access::table::table::table_close(relation as _, lockmode as _) }
unsafe fn heap_modify_tuple(tuple: HeapTuple, tupleDesc: TupleDesc, replValues: *mut Datum,
                            replIsnull: *mut bool, doReplace: *mut bool) -> HeapTuple { crate::access::common::heaptuple::heap_modify_tuple(tuple as _, tupleDesc as _, replValues as _, replIsnull as _, doReplace as _) as _ }
unsafe fn heap_form_tuple(tupleDescriptor: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple { crate::access::common::heaptuple::heap_form_tuple(tupleDescriptor as _, values as _, isnull as _) as _ }
unsafe fn heap_freetuple(htup: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(htup as _) }
unsafe fn heap_attisnull(tup: HeapTuple, attnum: c_int, tupleDesc: TupleDesc) -> bool { crate::access::common::heaptuple::heap_attisnull(tup as _, attnum as _, tupleDesc as _) as _ }
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc { crate::utils::rel::RelationGetDescr(relation as _) as _ }
unsafe fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: c_int) -> Oid { crate::catalog::catalog::GetNewOidWithIndex(relation as _, indexId as _, oidcolumn as _) as _ }

unsafe fn pgstat_drop_function(proid: Oid) { crate::utils::activity::pgstat_function::pgstat_drop_function(proid as _) }
unsafe fn changeDependencyFor(classId: Oid, objectId: Oid, refClassId: Oid, oldRefObjectId: Oid, newRefObjectId: Oid) -> c_long { crate::catalog::pg_depend::changeDependencyFor(classId as _, objectId as _, refClassId as _, oldRefObjectId as _, newRefObjectId as _) as _ }
unsafe fn recordDependencyOn(depender: *const ObjectAddress, referenced: *const ObjectAddress, behavior: c_char) { crate::catalog::pg_depend::recordDependencyOn(depender as _, referenced as _, behavior as _) }
unsafe fn recordDependencyOnCurrentExtension(myself: *const ObjectAddress, isReplace: bool) { crate::catalog::pg_depend::recordDependencyOnCurrentExtension(myself as _, isReplace as _) }
unsafe fn deleteDependencyRecordsFor(classId: Oid, objectId: Oid, skipExtensionDeps: bool) -> c_long { crate::catalog::pg_depend::deleteDependencyRecordsFor(classId as _, objectId as _, skipExtensionDeps as _) as _ }
unsafe fn new_object_addresses() -> *mut ObjectAddresses { crate::catalog::dependency::new_object_addresses() as _ }
unsafe fn add_exact_object_address(object: *const ObjectAddress, addrs: *mut ObjectAddresses) { crate::catalog::dependency::add_exact_object_address(object as _, addrs as _) }
unsafe fn record_object_address_dependencies(depender: *const ObjectAddress, referenced: *mut ObjectAddresses, behavior: c_char) { crate::catalog::dependency::record_object_address_dependencies(depender as _, referenced as _, behavior as _) }
unsafe fn free_object_addresses(addrs: *mut ObjectAddresses) { crate::catalog::dependency::free_object_addresses(addrs as _) }
unsafe fn ObjectAddressSet(obj: *mut ObjectAddress, classId: Oid, objectId: Oid) { crate::catalog::objectaddress_impl::ObjectAddressSet(&mut *(obj as *mut _), classId as _, objectId as _) }
unsafe fn CastCreate(sourcetypeid: Oid, targettypeid: Oid, funcid: Oid, incastid: Oid, outcastid: Oid,
                     castcontext: c_char, castmethod: c_char, behavior: c_char) -> ObjectAddress { crate::catalog::pg_cast::CastCreate(sourcetypeid as _, targettypeid as _, funcid as _, incastid as _, outcastid as _, castcontext as _, castmethod as _, behavior as _) as _ }

unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) { }
unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) { }
unsafe fn InvokeFunctionExecuteHook(objectId: Oid) { }

unsafe fn OidFunctionCall1(functionId: Oid, arg1: Datum) -> Datum { crate::utils::fmgr::OidFunctionCall1Coll(functionId as _, InvalidOid, arg1 as _) as _ }
unsafe fn build_function_result_tupdesc_t(procTuple: HeapTuple) -> TupleDesc { crate::utils::fmgr::funcapi::build_function_result_tupdesc_t(procTuple as _) as _ }
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute { crate::access::common::tupdesc::TupleDescAttr(tupdesc as _, i as _) as _ }
unsafe fn TupleDescInitEntry(desc: TupleDesc, attributeNumber: c_int, attributeName: *const c_char,
                             oidtypeid: Oid, typmod: int32, attdim: c_int) { crate::access::common::tupdesc::TupleDescInitEntry(desc as _, attributeNumber as _, attributeName as _, oidtypeid as _, typmod as _, attdim as _) }
unsafe fn exprType(expr: *mut Node) -> Oid { crate::nodes::nodeFuncs::exprType(expr as _) as _ }

unsafe fn fmgr_info(functionId: Oid, finfo: *mut FmgrInfo) { crate::utils::fmgr::fmgr_info(functionId as _, finfo as _) }
unsafe fn CreateExecutorState() -> *mut EState { crate::executor::execUtils::CreateExecutorState() as _ }
unsafe fn FreeExecutorState(estate: *mut EState) { crate::executor::execUtils::FreeExecutorState(estate as _) }
unsafe fn CreateExprContext(estate: *mut EState) -> *mut ExprContext { crate::executor::execUtils::CreateExprContext(estate as _) as _ }
unsafe fn ExecPrepareExpr(node: *mut Expr, estate: *mut EState) -> *mut ExprState { crate::executor::execExpr::ExecPrepareExpr(node as _, estate as _) as _ }
unsafe fn ExecEvalExprSwitchContext(state: *mut ExprState, econtext: *mut ExprContext, isNull: *mut bool) -> Datum { crate::executor::executor::ExecEvalExprSwitchContext(state as _, econtext as _, isNull as _) as _ }
unsafe fn pgstat_init_function_usage(fcinfo: *mut c_void, fcu: *mut PgStat_FunctionCallUsage) { crate::utils::activity::pgstat_function::pgstat_init_function_usage(fcinfo as _, fcu as _) }
unsafe fn pgstat_end_function_usage(fcu: *mut PgStat_FunctionCallUsage, finalize: bool) { crate::utils::activity::pgstat_function::pgstat_end_function_usage(fcu as _, finalize as _) }
unsafe fn PushActiveSnapshot(snap: Snapshot) { crate::utils::time::snapmgr::PushActiveSnapshot(snap as _) }
unsafe fn PopActiveSnapshot() { crate::utils::time::snapmgr::PopActiveSnapshot() }
unsafe fn GetTransactionSnapshot() -> Snapshot { crate::utils::time::snapmgr::GetTransactionSnapshot() as _ }
unsafe fn EnsurePortalSnapshotExists() { crate::tcop::pquery::EnsurePortalSnapshotExists() }
unsafe fn lookup_rowtype_tupdesc(type_id: Oid, typmod: int32) -> TupleDesc { crate::utils::cache::typcache::lookup_rowtype_tupdesc(type_id as _, typmod as _) as _ }
unsafe fn ReleaseTupleDesc(tupdesc: TupleDesc) { crate::access::common::tupdesc::ReleaseTupleDesc(tupdesc as _) }
unsafe fn begin_tup_output_tupdesc(dest: *mut DestReceiver, tupdesc: TupleDesc, tts_ops: *const TupleTableSlotOps) -> *mut TupOutputState { crate::executor::execTuples::begin_tup_output_tupdesc(dest as _, tupdesc as _, tts_ops as _) as _ }
unsafe fn end_tup_output(tstate: *mut TupOutputState) { crate::executor::execTuples::end_tup_output(tstate as _) }
unsafe fn ExecStoreHeapTuple(tuple: HeapTuple, slot: TupleTableSlot, shouldFree: bool) -> TupleTableSlot { crate::executor::execTuples::ExecStoreHeapTuple(tuple as _, slot as _, shouldFree as _) as _ }

/*
 *	 Examine the RETURNS clause of the CREATE FUNCTION statement
 *	 and return information about it as *prorettype_p and *returnsSet_p.
 *
 * This is more complex than the average typename lookup because we want to
 * allow a shell type to be used, or even created if the specified return type
 * doesn't exist yet.  (Without this, there's no way to define the I/O procs
 * for a new type.)  But SQL function creation won't cope, so error out if
 * the target language is SQL.  (We do this here, not in the SQL-function
 * validator, so as not to produce a NOTICE and then an ERROR for the same
 * condition.)
 */
unsafe fn compute_return_type(returnType: *mut TypeName, languageOid: Oid,
                              prorettype_p: *mut Oid, returnsSet_p: *mut bool) {
    let rettype: Oid;
    let typtup: Type;
    let mut aclresult: AclResult;

    typtup = LookupTypeName(null_mut(), returnType, null_mut(), false);

    if !typtup.is_null() {
        if !(*((GETSTRUCT(typtup) as Form_pg_type) as *mut FormData_pg_type_typisdefined)).typisdefined {
            if languageOid == SQLlanguageId {
                ereport!(ERROR, errmsg!("SQL function cannot return shell type {}",
                    cstr_display(TypeNameToString(returnType))));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
            } else {
                ereport!(NOTICE, errmsg!("return type {} is only a shell",
                    cstr_display(TypeNameToString(returnType))));
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            }
        }
        rettype = typeTypeId(typtup);
        ReleaseSysCache(typtup);
    } else {
        let typnam: *mut c_char = TypeNameToString(returnType);
        let namespaceId: Oid;
        let typname: *mut c_char = null_mut();
        let address: ObjectAddress;
        let mut typname_out: *mut c_char = typname;

        /*
         * Only C-coded functions can be I/O functions.  We enforce this
         * restriction here mainly to prevent littering the catalogs with
         * shell types due to simple typos in user-defined function
         * definitions.
         */
        if languageOid != INTERNALlanguageId &&
            languageOid != ClanguageId {
            ereport!(ERROR, errmsg!("type \"{}\" does not exist",
                cstr_display(typnam)));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }

        /* Reject if there's typmod decoration, too */
        if (*returnType).typmods != NIL {
            ereport!(ERROR, errmsg!("type modifier cannot be specified for shell type \"{}\"",
                cstr_display(typnam)));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }

        /* Otherwise, go ahead and make a shell type */
        ereport!(NOTICE, errmsg!("type \"{}\" is not yet defined",
            cstr_display(typnam)));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT), errdetail("Creating a shell type definition.") */
        namespaceId = QualifiedNameGetCreationNamespace((*returnType).names,
                                                        &mut typname_out);
        aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(),
                                    ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_SCHEMA,
                           get_namespace_name(namespaceId));
        }
        address = TypeShellMake(typname_out, namespaceId, GetUserId());
        rettype = address.objectId;
        Assert!(OidIsValid(rettype));
    }

    aclresult = object_aclcheck(TypeRelationId, rettype, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, rettype);
    }

    *prorettype_p = rettype;
    *returnsSet_p = (*returnType).setof;
}

/* Helper opaque struct giving access to typisdefined from a pg_type GETSTRUCT  TODO(pg-port) */
#[repr(C)] pub struct FormData_pg_type_typisdefined { pub _pad: [u8; 82], pub typisdefined: bool }

unsafe fn cstr_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() { return std::borrow::Cow::Borrowed("(null)"); }
    std::ffi::CStr::from_ptr(s).to_string_lossy()
}

/*
 * Interpret the function parameter list of a CREATE FUNCTION,
 * CREATE PROCEDURE, or CREATE AGGREGATE statement.
 *
 * Input parameters:
 * parameters: list of FunctionParameter structs
 * languageOid: OID of function language (InvalidOid if it's CREATE AGGREGATE)
 * objtype: identifies type of object being created
 *
 * Results are stored into output parameters.  parameterTypes must always
 * be created, but the other arrays/lists can be NULL pointers if not needed.
 * variadicArgType is set to the variadic array type if there's a VARIADIC
 * parameter (there can be only one); or to InvalidOid if not.
 * requiredResultType is set to InvalidOid if there are no OUT parameters,
 * else it is set to the OID of the implied result type.
 */
pub unsafe fn interpret_function_parameter_list(
    pstate: *mut ParseState,
    parameters: *mut List,
    languageOid: Oid,
    objtype: ObjectType,
    parameterTypes: *mut *mut oidvector,
    parameterTypes_list: *mut *mut List,
    allParameterTypes: *mut *mut ArrayType,
    parameterModes: *mut *mut ArrayType,
    parameterNames: *mut *mut ArrayType,
    inParameterNames_list: *mut *mut List,
    parameterDefaults: *mut *mut List,
    variadicArgType: *mut Oid,
    requiredResultType: *mut Oid,
) {
    let parameterCount: c_int = list_length(parameters);
    let inTypes: *mut Oid;
    let mut inCount: c_int = 0;
    let allTypes: *mut Datum;
    let paramModes: *mut Datum;
    let paramNames: *mut Datum;
    let mut outCount: c_int = 0;
    let mut varCount: c_int = 0;
    let mut have_names: bool = false;
    let mut have_defaults: bool = false;
    let mut i: c_int;

    *variadicArgType = InvalidOid;  /* default result */
    *requiredResultType = InvalidOid;   /* default result */

    inTypes = palloc(parameterCount as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    allTypes = palloc(parameterCount as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    paramModes = palloc(parameterCount as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    paramNames = palloc0(parameterCount as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    *parameterDefaults = NIL;

    /* Scan the list and extract data into work arrays */
    i = 0;
    foreach!(x, parameters, {
        let fp: *mut FunctionParameter = lfirst(current_cell!(x)) as *mut FunctionParameter;
        let t: *mut TypeName = (*fp).argType;
        let mut fpmode: FunctionParameterMode = (*fp).mode;
        let mut isinput: bool = false;
        let toid: Oid;
        let typtup: Type;
        let mut aclresult: AclResult;

        /* For our purposes here, a defaulted mode spec is identical to IN */
        if fpmode == FUNC_PARAM_DEFAULT {
            fpmode = FUNC_PARAM_IN;
        }

        typtup = LookupTypeName(pstate, t, null_mut(), false);
        if !typtup.is_null() {
            if !(*((GETSTRUCT(typtup) as Form_pg_type) as *mut FormData_pg_type_typisdefined)).typisdefined {
                /* As above, hard error if language is SQL */
                if languageOid == SQLlanguageId {
                    ereport!(ERROR, errmsg!("SQL function cannot accept shell type {}",
                        cstr_display(TypeNameToString(t))));
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*t).location) */
                }
                /* We don't allow creating aggregates on shell types either */
                else if objtype == OBJECT_AGGREGATE {
                    ereport!(ERROR, errmsg!("aggregate cannot accept shell type {}",
                        cstr_display(TypeNameToString(t))));
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*t).location) */
                } else {
                    ereport!(NOTICE, errmsg!("argument type {} is only a shell",
                        cstr_display(TypeNameToString(t))));
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition(pstate, (*t).location) */
                }
            }
            toid = typeTypeId(typtup);
            ReleaseSysCache(typtup);
        } else {
            ereport!(ERROR, errmsg!("type {} does not exist",
                cstr_display(TypeNameToString(t))));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT), parser_errposition(pstate, (*t).location) */
            #[allow(unreachable_code)]
            { toid = InvalidOid; }  /* keep compiler quiet */
        }

        aclresult = object_aclcheck(TypeRelationId, toid, GetUserId(), ACL_USAGE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type(aclresult, toid);
        }

        if (*t).setof {
            if objtype == OBJECT_AGGREGATE {
                ereport!(ERROR, errmsg!("aggregates cannot accept set arguments"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            } else if objtype == OBJECT_PROCEDURE {
                ereport!(ERROR, errmsg!("procedures cannot accept set arguments"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            } else {
                ereport!(ERROR, errmsg!("functions cannot accept set arguments"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            }
        }

        /* handle input parameters */
        if fpmode != FUNC_PARAM_OUT && fpmode != FUNC_PARAM_TABLE {
            /* other input parameters can't follow a VARIADIC parameter */
            if varCount > 0 {
                ereport!(ERROR, errmsg!("VARIADIC parameter must be the last input parameter"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            }
            *inTypes.add(inCount as usize) = toid;
            inCount += 1;
            isinput = true;
            if !parameterTypes_list.is_null() {
                *parameterTypes_list = lappend_oid(*parameterTypes_list, toid);
            }
        }

        /* handle output parameters */
        if fpmode != FUNC_PARAM_IN && fpmode != FUNC_PARAM_VARIADIC {
            if objtype == OBJECT_PROCEDURE {
                /*
                 * We disallow OUT-after-VARIADIC only for procedures.  While
                 * such a case causes no confusion in ordinary function calls,
                 * it would cause confusion in a CALL statement.
                 */
                if varCount > 0 {
                    ereport!(ERROR, errmsg!("VARIADIC parameter must be the last parameter"));
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
                }
                /* Procedures with output parameters always return RECORD */
                *requiredResultType = RECORDOID;
            } else if outCount == 0 {    /* save first output param's type */
                *requiredResultType = toid;
            }
            outCount += 1;
        }

        if fpmode == FUNC_PARAM_VARIADIC {
            *variadicArgType = toid;
            varCount += 1;
            /* validate variadic parameter type */
            match toid {
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID | ANYOID => {
                    /* okay */
                }
                _ => {
                    if !OidIsValid(get_element_type(toid)) {
                        ereport!(ERROR, errmsg!("VARIADIC parameter must be an array"));
                        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
                    }
                }
            }
        }

        *allTypes.add(i as usize) = ObjectIdGetDatum(toid);

        *paramModes.add(i as usize) = CharGetDatum(fpmode);

        if !(*fp).name.is_null() && *(*fp).name != 0 {
            /*
             * As of Postgres 9.0 we disallow using the same name for two
             * input or two output function parameters.  Depending on the
             * function's language, conflicting input and output names might
             * be bad too, but we leave it to the PL to complain if so.
             */
            foreach!(px, parameters, {
                let prevfp: *mut FunctionParameter = lfirst(current_cell!(px)) as *mut FunctionParameter;
                let mut prevfpmode: FunctionParameterMode;

                if prevfp == fp {
                    break;
                }
                /* as above, default mode is IN */
                prevfpmode = (*prevfp).mode;
                if prevfpmode == FUNC_PARAM_DEFAULT {
                    prevfpmode = FUNC_PARAM_IN;
                }
                /* pure in doesn't conflict with pure out */
                if (fpmode == FUNC_PARAM_IN ||
                     fpmode == FUNC_PARAM_VARIADIC) &&
                    (prevfpmode == FUNC_PARAM_OUT ||
                     prevfpmode == FUNC_PARAM_TABLE) {
                    /* C: continue; */
                } else if (prevfpmode == FUNC_PARAM_IN ||
                     prevfpmode == FUNC_PARAM_VARIADIC) &&
                    (fpmode == FUNC_PARAM_OUT ||
                     fpmode == FUNC_PARAM_TABLE) {
                    /* C: continue; */
                } else if !(*prevfp).name.is_null() && *(*prevfp).name != 0 &&
                    libc_strcmp((*prevfp).name, (*fp).name) == 0 {
                    ereport!(ERROR, errmsg!("parameter name \"{}\" used more than once",
                        cstr_display((*fp).name)));
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
                }
            });

            *paramNames.add(i as usize) = CStringGetTextDatum((*fp).name);
            have_names = true;
        }

        if !inParameterNames_list.is_null() {
            *inParameterNames_list = lappend(*inParameterNames_list,
                makeString(if !(*fp).name.is_null() { (*fp).name } else { pstrdup(EMPTY_CSTR.as_ptr() as *const c_char) }) as *mut c_void);
        }

        if !(*fp).defexpr.is_null() {
            let mut def: *mut Node;

            if !isinput {
                ereport!(ERROR, errmsg!("only input parameters can have default values"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            }

            def = transformExpr(pstate, (*fp).defexpr,
                                EXPR_KIND_FUNCTION_DEFAULT);
            def = coerce_to_specific_type(pstate, def, toid, DEFAULT_CSTR.as_ptr() as *const c_char);
            assign_expr_collations(pstate, def);

            /*
             * Make sure no variables are referred to (this is probably dead
             * code now that add_missing_from is history).
             */
            if (*pstate).p_rtable != NIL ||
                contain_var_clause(def) {
                ereport!(ERROR, errmsg!("cannot use table references in parameter default value"));
                /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE), parser_errposition(pstate, (*fp).location) */
            }

            /*
             * transformExpr() should have already rejected subqueries,
             * aggregates, and window functions, based on the EXPR_KIND_ for a
             * default expression.
             *
             * It can't return a set either --- but coerce_to_specific_type
             * already checked that for us.
             *
             * Note: the point of these restrictions is to ensure that an
             * expression that, on its face, hasn't got subplans, aggregates,
             * etc cannot suddenly have them after function default arguments
             * are inserted.
             */

            *parameterDefaults = lappend(*parameterDefaults, def as *mut c_void);
            have_defaults = true;
        } else {
            if isinput && have_defaults {
                ereport!(ERROR, errmsg!("input parameters after one with a default value must also have defaults"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            }

            /*
             * For procedures, we also can't allow OUT parameters after one
             * with a default, because the same sort of confusion arises in a
             * CALL statement.
             */
            if objtype == OBJECT_PROCEDURE && have_defaults {
                ereport!(ERROR, errmsg!("procedure OUT parameters cannot appear after one with a default value"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*fp).location) */
            }
        }

        i += 1;
    });

    /* Now construct the proper outputs as needed */
    *parameterTypes = buildoidvector(inTypes, inCount);

    if outCount > 0 || varCount > 0 {
        *allParameterTypes = construct_array_builtin(allTypes, parameterCount, OIDOID);
        *parameterModes = construct_array_builtin(paramModes, parameterCount, CHAROID);
        if outCount > 1 {
            *requiredResultType = RECORDOID;
        }
        /* otherwise we set requiredResultType correctly above */
    } else {
        *allParameterTypes = null_mut();
        *parameterModes = null_mut();
    }

    if have_names {
        i = 0;
        while i < parameterCount {
            if *paramNames.add(i as usize) == PointerGetDatum(null()) {
                *paramNames.add(i as usize) = CStringGetTextDatum(EMPTY_CSTR.as_ptr() as *const c_char);
            }
            i += 1;
        }
        *parameterNames = construct_array_builtin(paramNames, parameterCount, TEXTOID);
    } else {
        *parameterNames = null_mut();
    }
}

const EMPTY_CSTR: &[u8] = b"\0";
const DEFAULT_CSTR: &[u8] = b"DEFAULT\0";

unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let ca = std::ffi::CStr::from_ptr(a);
    let cb = std::ffi::CStr::from_ptr(b);
    match ca.to_bytes().cmp(cb.to_bytes()) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/*
 * Recognize one of the options that can be passed to both CREATE
 * FUNCTION and ALTER FUNCTION and return it via one of the out
 * parameters. Returns true if the passed option was recognized. If
 * the out parameter we were going to assign to points to non-NULL,
 * raise a duplicate-clause error.  (We don't try to detect duplicate
 * SET parameters though --- if you're redundant, the last one wins.)
 */
unsafe fn compute_common_attribute(
    pstate: *mut ParseState,
    is_procedure: bool,
    defel: *mut DefElem,
    volatility_item: *mut *mut DefElem,
    strict_item: *mut *mut DefElem,
    security_item: *mut *mut DefElem,
    leakproof_item: *mut *mut DefElem,
    set_items: *mut *mut List,
    cost_item: *mut *mut DefElem,
    rows_item: *mut *mut DefElem,
    support_item: *mut *mut DefElem,
    parallel_item: *mut *mut DefElem,
) -> bool {
    if cstr_eq((*defel).defname, "volatility") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*volatility_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *volatility_item = defel;
    } else if cstr_eq((*defel).defname, "strict") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*strict_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *strict_item = defel;
    } else if cstr_eq((*defel).defname, "security") {
        if !(*security_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *security_item = defel;
    } else if cstr_eq((*defel).defname, "leakproof") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*leakproof_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *leakproof_item = defel;
    } else if cstr_eq((*defel).defname, "set") {
        *set_items = lappend(*set_items, (*defel).arg as *mut c_void);
    } else if cstr_eq((*defel).defname, "cost") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*cost_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *cost_item = defel;
    } else if cstr_eq((*defel).defname, "rows") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*rows_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *rows_item = defel;
    } else if cstr_eq((*defel).defname, "support") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*support_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *support_item = defel;
    } else if cstr_eq((*defel).defname, "parallel") {
        if is_procedure {
            return procedure_error(pstate, defel);
        }
        if !(*parallel_item).is_null() {
            errorConflictingDefElem(defel, pstate);
        }

        *parallel_item = defel;
    } else {
        return false;
    }

    /* Recognized an option */
    return true;
}

/* C: procedure_error: label */
unsafe fn procedure_error(pstate: *mut ParseState, defel: *mut DefElem) -> bool {
    ereport!(ERROR, errmsg!("invalid attribute in procedure definition"));
    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*defel).location) */
    #[allow(unreachable_code)]
    false
}

unsafe fn cstr_eq(s: *const c_char, lit: &str) -> bool {
    if s.is_null() { return false; }
    std::ffi::CStr::from_ptr(s).to_bytes() == lit.as_bytes()
}

unsafe fn interpret_func_volatility(defel: *mut DefElem) -> c_char {
    let str_: *mut c_char = strVal((*defel).arg);

    if cstr_eq(str_, "immutable") {
        return PROVOLATILE_IMMUTABLE;
    } else if cstr_eq(str_, "stable") {
        return PROVOLATILE_STABLE;
    } else if cstr_eq(str_, "volatile") {
        return PROVOLATILE_VOLATILE;
    } else {
        elog!(ERROR, "invalid volatility \"{}\"", cstr_display(str_));
        #[allow(unreachable_code)]
        { return 0; }   /* keep compiler quiet */
    }
}

unsafe fn interpret_func_parallel(defel: *mut DefElem) -> c_char {
    let str_: *mut c_char = strVal((*defel).arg);

    if cstr_eq(str_, "safe") {
        return PROPARALLEL_SAFE;
    } else if cstr_eq(str_, "unsafe") {
        return PROPARALLEL_UNSAFE;
    } else if cstr_eq(str_, "restricted") {
        return PROPARALLEL_RESTRICTED;
    } else {
        ereport!(ERROR, errmsg!("parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE"));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        #[allow(unreachable_code)]
        { return PROPARALLEL_UNSAFE; }  /* keep compiler quiet */
    }
}

/*
 * Update a proconfig value according to a list of VariableSetStmt items.
 *
 * The input and result may be NULL to signify a null entry.
 */
unsafe fn update_proconfig_value(mut a: *mut ArrayType, set_items: *mut List) -> *mut ArrayType {
    foreach!(l, set_items, {
        let sstmt: *mut VariableSetStmt = lfirst_node!(VariableSetStmt, T_VariableSetStmt, current_cell!(l));

        if (*sstmt).kind == VAR_RESET_ALL {
            a = null_mut();
        } else {
            let valuestr: *mut c_char = ExtractSetVariableArgs(sstmt);

            if !valuestr.is_null() {
                a = GUCArrayAdd(a, (*sstmt).name, valuestr);
            } else {            /* RESET */
                a = GUCArrayDelete(a, (*sstmt).name);
            }
        }
    });

    return a;
}

unsafe fn interpret_func_support(defel: *mut DefElem) -> Oid {
    let procName: *mut List = defGetQualifiedName(defel);
    let procOid: Oid;
    let mut argList: [Oid; 1] = [0; 1];

    /*
     * Support functions always take one INTERNAL argument and return
     * INTERNAL.
     */
    argList[0] = INTERNALOID;

    procOid = LookupFuncName(procName, 1, argList.as_ptr(), true);
    if !OidIsValid(procOid) {
        ereport!(ERROR, errmsg!("function {} does not exist",
            cstr_display(func_signature_string(procName, 1, NIL, argList.as_ptr()))));
        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
    }

    if get_func_rettype(procOid) != INTERNALOID {
        ereport!(ERROR, errmsg!("support function {} must return type {}",
            cstr_display(NameListToString(procName)), "internal"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * Someday we might want an ACL check here; but for now, we insist that
     * you be superuser to specify a support function, so privilege on the
     * support function is moot.
     */
    if !superuser() {
        ereport!(ERROR, errmsg!("must be superuser to specify a support function"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    return procOid;
}

/*
 * Dissect the list of options assembled in gram.y into function
 * attributes.
 */
unsafe fn compute_function_attributes(
    pstate: *mut ParseState,
    is_procedure: bool,
    options: *mut List,
    as_: *mut *mut List,
    language: *mut *mut c_char,
    transform: *mut *mut Node,
    windowfunc_p: *mut bool,
    volatility_p: *mut c_char,
    strict_p: *mut bool,
    security_definer: *mut bool,
    leakproof_p: *mut bool,
    proconfig: *mut *mut ArrayType,
    procost: *mut float4,
    prorows: *mut float4,
    prosupport: *mut Oid,
    parallel_p: *mut c_char,
) {
    let mut as_item: *mut DefElem = null_mut();
    let mut language_item: *mut DefElem = null_mut();
    let mut transform_item: *mut DefElem = null_mut();
    let mut windowfunc_item: *mut DefElem = null_mut();
    let mut volatility_item: *mut DefElem = null_mut();
    let mut strict_item: *mut DefElem = null_mut();
    let mut security_item: *mut DefElem = null_mut();
    let mut leakproof_item: *mut DefElem = null_mut();
    let mut set_items: *mut List = NIL;
    let mut cost_item: *mut DefElem = null_mut();
    let mut rows_item: *mut DefElem = null_mut();
    let mut support_item: *mut DefElem = null_mut();
    let mut parallel_item: *mut DefElem = null_mut();

    foreach!(option, options, {
        let defel: *mut DefElem = lfirst(current_cell!(option)) as *mut DefElem;

        if cstr_eq((*defel).defname, "as") {
            if !as_item.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            as_item = defel;
        } else if cstr_eq((*defel).defname, "language") {
            if !language_item.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            language_item = defel;
        } else if cstr_eq((*defel).defname, "transform") {
            if !transform_item.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            transform_item = defel;
        } else if cstr_eq((*defel).defname, "window") {
            if !windowfunc_item.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            if is_procedure {
                ereport!(ERROR, errmsg!("invalid attribute in procedure definition"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition(pstate, (*defel).location) */
            }
            windowfunc_item = defel;
        } else if compute_common_attribute(pstate,
                                          is_procedure,
                                          defel,
                                          &mut volatility_item,
                                          &mut strict_item,
                                          &mut security_item,
                                          &mut leakproof_item,
                                          &mut set_items,
                                          &mut cost_item,
                                          &mut rows_item,
                                          &mut support_item,
                                          &mut parallel_item) {
            /* recognized common option */
            /* C: continue; */
        } else {
            elog!(ERROR, "option \"{}\" not recognized",
                 cstr_display((*defel).defname));
        }
    });

    if !as_item.is_null() {
        *as_ = (*as_item).arg as *mut List;
    }
    if !language_item.is_null() {
        *language = strVal((*language_item).arg);
    }
    if !transform_item.is_null() {
        *transform = (*transform_item).arg;
    }
    if !windowfunc_item.is_null() {
        *windowfunc_p = boolVal((*windowfunc_item).arg);
    }
    if !volatility_item.is_null() {
        *volatility_p = interpret_func_volatility(volatility_item);
    }
    if !strict_item.is_null() {
        *strict_p = boolVal((*strict_item).arg);
    }
    if !security_item.is_null() {
        *security_definer = boolVal((*security_item).arg);
    }
    if !leakproof_item.is_null() {
        *leakproof_p = boolVal((*leakproof_item).arg);
    }
    if !set_items.is_null() {
        *proconfig = update_proconfig_value(null_mut(), set_items);
    }
    if !cost_item.is_null() {
        *procost = defGetNumeric(cost_item);
        if *procost <= 0.0 {
            ereport!(ERROR, errmsg!("COST must be positive"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !rows_item.is_null() {
        *prorows = defGetNumeric(rows_item);
        if *prorows <= 0.0 {
            ereport!(ERROR, errmsg!("ROWS must be positive"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !support_item.is_null() {
        *prosupport = interpret_func_support(support_item);
    }
    if !parallel_item.is_null() {
        *parallel_p = interpret_func_parallel(parallel_item);
    }
}

/*
 * For a dynamically linked C language object, the form of the clause is
 *
 *	   AS <object file name> [, <link symbol name> ]
 *
 * In all other cases
 *
 *	   AS <object reference, or sql code>
 */
unsafe fn interpret_AS_clause(
    languageOid: Oid, languageName: *const c_char,
    funcname: *mut c_char, as_: *mut List, sql_body_in: *mut Node,
    parameterTypes: *mut List, inParameterNames: *mut List,
    prosrc_str_p: *mut *mut c_char, probin_str_p: *mut *mut c_char,
    sql_body_out: *mut *mut Node,
    queryString: *const c_char,
) {
    if sql_body_in.is_null() && as_.is_null() {
        ereport!(ERROR, errmsg!("no function body specified"));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
    }

    if !sql_body_in.is_null() && !as_.is_null() {
        ereport!(ERROR, errmsg!("duplicate function body specified"));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
    }

    if !sql_body_in.is_null() && languageOid != SQLlanguageId {
        ereport!(ERROR, errmsg!("inline SQL function body only valid for language SQL"));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
    }

    *sql_body_out = null_mut();

    if languageOid == ClanguageId {
        /*
         * For "C" language, store the file name in probin and, when given,
         * the link symbol name in prosrc.  If link symbol is omitted,
         * substitute procedure name.  We also allow link symbol to be
         * specified as "-", since that was the habit in PG versions before
         * 8.4, and there might be dump files out there that don't translate
         * that back to "omitted".
         */
        *probin_str_p = strVal(linitial(as_) as *const Node);
        if list_length(as_) == 1 {
            *prosrc_str_p = funcname;
        } else {
            *prosrc_str_p = strVal(lsecond(as_) as *const Node);
            if cstr_eq(*prosrc_str_p, "-") {
                *prosrc_str_p = funcname;
            }
        }
    } else if !sql_body_in.is_null() {
        let pinfo: SQLFunctionParseInfoPtr;

        pinfo = palloc0(core::mem::size_of::<SQLFunctionParseInfo>()) as SQLFunctionParseInfoPtr;

        (*pinfo).fname = funcname;
        (*pinfo).nargs = list_length(parameterTypes);
        (*pinfo).argtypes = palloc((*pinfo).nargs as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        (*pinfo).argnames = palloc((*pinfo).nargs as usize * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        let mut i: c_int = 0;
        while i < list_length(parameterTypes) {
            let s: *mut c_char = strVal(list_nth(inParameterNames, i) as *const Node);

            *(*pinfo).argtypes.add(i as usize) = list_nth_oid(parameterTypes, i);
            if IsPolymorphicType(*(*pinfo).argtypes.add(i as usize)) {
                ereport!(ERROR, errmsg!("SQL function with unquoted function body cannot have polymorphic arguments"));
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
            }

            if *s != 0 {
                *(*pinfo).argnames.add(i as usize) = s;
            } else {
                *(*pinfo).argnames.add(i as usize) = null_mut();
            }
            i += 1;
        }

        if IsA!(sql_body_in, T_List) {
            let stmts: *mut List = linitial_node_List(castNode!(List, T_List, sql_body_in) as *const List);
            let mut transformed_stmts: *mut List = NIL;

            foreach!(lc, stmts, {
                let stmt: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
                let q: *mut Query;
                let pstate: *mut ParseState = make_parsestate(null_mut());

                (*pstate).p_sourcetext = queryString;
                sql_fn_parser_setup(pstate, pinfo);
                q = transformStmt(pstate, stmt);
                if (*q).commandType == CMD_UTILITY {
                    ereport!(ERROR, errmsg!("{} is not yet supported in unquoted SQL function body",
                        cstr_display(GetCommandTagName(CreateCommandTag((*q).utilityStmt)))));
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                }
                transformed_stmts = lappend(transformed_stmts, q as *mut c_void);
                free_parsestate(pstate);
            });

            *sql_body_out = list_make1(transformed_stmts as *mut c_void) as *mut Node;
        } else {
            let q: *mut Query;
            let pstate: *mut ParseState = make_parsestate(null_mut());

            (*pstate).p_sourcetext = queryString;
            sql_fn_parser_setup(pstate, pinfo);
            q = transformStmt(pstate, sql_body_in);
            if (*q).commandType == CMD_UTILITY {
                ereport!(ERROR, errmsg!("{} is not yet supported in unquoted SQL function body",
                    cstr_display(GetCommandTagName(CreateCommandTag((*q).utilityStmt)))));
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            }
            free_parsestate(pstate);

            *sql_body_out = q as *mut Node;
        }

        /*
         * We must put something in prosrc.  For the moment, just record an
         * empty string.  It might be useful to store the original text of the
         * CREATE FUNCTION statement --- but to make actual use of that in
         * error reports, we'd also have to adjust readfuncs.c to not throw
         * away node location fields when reading prosqlbody.
         */
        *prosrc_str_p = pstrdup(EMPTY_CSTR.as_ptr() as *const c_char);

        /* But we definitely don't need probin. */
        *probin_str_p = null_mut();
    } else {
        /* Everything else wants the given string in prosrc. */
        *prosrc_str_p = strVal(linitial(as_) as *const Node);
        *probin_str_p = null_mut();

        if list_length(as_) != 1 {
            ereport!(ERROR, errmsg!("only one AS item needed for language \"{}\"",
                cstr_display(languageName)));
            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
        }

        if languageOid == INTERNALlanguageId {
            /*
             * In PostgreSQL versions before 6.5, the SQL name of the created
             * function could not be different from the internal name, and
             * "prosrc" wasn't used.  So there is code out there that does
             * CREATE FUNCTION xyz AS '' LANGUAGE internal. To preserve some
             * modicum of backwards compatibility, accept an empty "prosrc"
             * value as meaning the supplied SQL function name.
             */
            if c_strlen(*prosrc_str_p) == 0 {
                *prosrc_str_p = funcname;
            }
        }
    }
}

unsafe fn c_strlen(s: *const c_char) -> usize {
    if s.is_null() { return 0; }
    std::ffi::CStr::from_ptr(s).to_bytes().len()
}

/*
 * CreateFunction
 *	 Execute a CREATE FUNCTION (or CREATE PROCEDURE) utility statement.
 */
pub unsafe fn CreateFunction(pstate: *mut ParseState, stmt: *mut CreateFunctionStmt) -> ObjectAddress {
    let mut probin_str: *mut c_char = null_mut();
    let mut prosrc_str: *mut c_char = null_mut();
    let mut prosqlbody: *mut Node = null_mut();
    let mut prorettype: Oid = InvalidOid;
    let mut returnsSet: bool = false;
    let mut language: *mut c_char;
    let languageOid: Oid;
    let languageValidator: Oid;
    let mut transformDefElem: *mut Node = null_mut();
    let funcname: *mut c_char;
    let mut funcname_out: *mut c_char = null_mut();
    let namespaceId: Oid;
    let mut aclresult: AclResult;
    let mut parameterTypes: *mut oidvector = null_mut();
    let mut parameterTypes_list: *mut List = NIL;
    let mut allParameterTypes: *mut ArrayType = null_mut();
    let mut parameterModes: *mut ArrayType = null_mut();
    let mut parameterNames: *mut ArrayType = null_mut();
    let mut inParameterNames_list: *mut List = NIL;
    let mut parameterDefaults: *mut List = null_mut();
    let mut variadicArgType: Oid = InvalidOid;
    let mut trftypes_list: *mut List = NIL;
    let mut trfoids_list: *mut List = NIL;
    let trftypes: *mut ArrayType;
    let mut requiredResultType: Oid = InvalidOid;
    let mut isWindowFunc: bool;
    let mut isStrict: bool;
    let mut security: bool;
    let mut isLeakProof: bool;
    let mut volatility: c_char;
    let mut proconfig: *mut ArrayType;
    let mut procost: float4;
    let mut prorows: float4;
    let mut prosupport: Oid;
    let languageTuple: HeapTuple;
    let languageStruct: Form_pg_language;
    let mut as_clause: *mut List;
    let mut parallel: c_char;

    /* Convert list of names to a name and namespace */
    namespaceId = QualifiedNameGetCreationNamespace((*stmt).funcname,
                                                    &mut funcname_out);
    funcname = funcname_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA,
                       get_namespace_name(namespaceId));
    }

    /* Set default attributes */
    as_clause = NIL;
    language = null_mut();
    isWindowFunc = false;
    isStrict = false;
    security = false;
    isLeakProof = false;
    volatility = PROVOLATILE_VOLATILE;
    proconfig = null_mut();
    procost = -1.0;             /* indicates not set */
    prorows = -1.0;             /* indicates not set */
    prosupport = InvalidOid;
    parallel = PROPARALLEL_UNSAFE;

    /* Extract non-default attributes from stmt->options list */
    compute_function_attributes(pstate,
                                (*stmt).is_procedure,
                                (*stmt).options,
                                &mut as_clause, &mut language, &mut transformDefElem,
                                &mut isWindowFunc, &mut volatility,
                                &mut isStrict, &mut security, &mut isLeakProof,
                                &mut proconfig, &mut procost, &mut prorows,
                                &mut prosupport, &mut parallel);

    if language.is_null() {
        if !(*stmt).sql_body.is_null() {
            language = SQL_CSTR.as_ptr() as *mut c_char;
        } else {
            ereport!(ERROR, errmsg!("no language specified"));
            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
        }
    }

    /* Look up the language and validate permissions */
    languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language as *const c_void));
    if !HeapTupleIsValid(languageTuple) {
        ereport!(ERROR, errmsg!("language \"{}\" does not exist", cstr_display(language)));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT),
         * (extension_file_exists(language) ?
         *  errhint("Use CREATE EXTENSION to load the language into the database.") : 0) */
    }

    languageStruct = GETSTRUCT(languageTuple) as Form_pg_language;
    languageOid = (*(languageStruct as *mut FormData_pg_language_fields)).oid;

    if (*(languageStruct as *mut FormData_pg_language_fields)).lanpltrusted {
        /* if trusted language, need USAGE privilege */
        aclresult = object_aclcheck(LanguageRelationId, languageOid, GetUserId(), ACL_USAGE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_LANGUAGE,
                           NameStr(&(*(languageStruct as *mut FormData_pg_language_fields)).lanname as *const _ as *const c_void));
        }
    } else {
        /* if untrusted language, must be superuser */
        if !superuser() {
            aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_LANGUAGE,
                           NameStr(&(*(languageStruct as *mut FormData_pg_language_fields)).lanname as *const _ as *const c_void));
        }
    }

    languageValidator = (*(languageStruct as *mut FormData_pg_language_fields)).lanvalidator;

    ReleaseSysCache(languageTuple);

    /*
     * Only superuser is allowed to create leakproof functions because
     * leakproof functions can see tuples which have not yet been filtered out
     * by security barrier views or row-level security policies.
     */
    if isLeakProof && !superuser() {
        ereport!(ERROR, errmsg!("only superuser can define a leakproof function"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    if !transformDefElem.is_null() {
        foreach!(lc, castNode!(List, T_List, transformDefElem) as *mut List, {
            let mut typeid: Oid = typenameTypeId(null_mut(),
                                                lfirst_node!(TypeName, T_TypeName, current_cell!(lc)));
            let elt: Oid = get_base_element_type(typeid);
            let transformid: Oid;

            typeid = if elt != InvalidOid { elt } else { typeid };
            transformid = get_transform_oid(typeid, languageOid, false);
            trftypes_list = lappend_oid(trftypes_list, typeid);
            trfoids_list = lappend_oid(trfoids_list, transformid);
        });
    }

    /*
     * Convert remaining parameters of CREATE to form wanted by
     * ProcedureCreate.
     */
    interpret_function_parameter_list(pstate,
                                      (*stmt).parameters,
                                      languageOid,
                                      if (*stmt).is_procedure { OBJECT_PROCEDURE } else { OBJECT_FUNCTION },
                                      &mut parameterTypes,
                                      &mut parameterTypes_list,
                                      &mut allParameterTypes,
                                      &mut parameterModes,
                                      &mut parameterNames,
                                      &mut inParameterNames_list,
                                      &mut parameterDefaults,
                                      &mut variadicArgType,
                                      &mut requiredResultType);

    if (*stmt).is_procedure {
        Assert!((*stmt).returnType.is_null());
        prorettype = if requiredResultType != InvalidOid { requiredResultType } else { VOIDOID };
        returnsSet = false;
    } else if !(*stmt).returnType.is_null() {
        /* explicit RETURNS clause */
        compute_return_type((*stmt).returnType, languageOid,
                            &mut prorettype, &mut returnsSet);
        if OidIsValid(requiredResultType) && prorettype != requiredResultType {
            ereport!(ERROR, errmsg!("function result type must be {} because of OUT parameters",
                cstr_display(format_type_be(requiredResultType))));
            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
        }
    } else if OidIsValid(requiredResultType) {
        /* default RETURNS clause from OUT parameters */
        prorettype = requiredResultType;
        returnsSet = false;
    } else {
        ereport!(ERROR, errmsg!("function result type must be specified"));
        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
        /* Alternative possibility: default to RETURNS VOID */
        #[allow(unreachable_code)]
        { prorettype = VOIDOID; }
        #[allow(unreachable_code)]
        { returnsSet = false; }
    }

    if trftypes_list != NIL {
        let arr: *mut Datum;
        let mut i: c_int;

        arr = palloc(list_length(trftypes_list) as usize * core::mem::size_of::<Datum>()) as *mut Datum;
        i = 0;
        foreach!(lc, trftypes_list, {
            *arr.add(i as usize) = ObjectIdGetDatum(lfirst_oid(current_cell!(lc)));
            i += 1;
        });
        trftypes = construct_array_builtin(arr, list_length(trftypes_list), OIDOID);
    } else {
        /* store SQL NULL instead of empty array */
        trftypes = null_mut();
    }

    interpret_AS_clause(languageOid, language, funcname, as_clause, (*stmt).sql_body,
                        parameterTypes_list, inParameterNames_list,
                        &mut prosrc_str, &mut probin_str, &mut prosqlbody,
                        (*pstate).p_sourcetext);

    /*
     * Set default values for COST and ROWS depending on other parameters;
     * reject ROWS if it's not returnsSet.  NB: pg_dump knows these default
     * values, keep it in sync if you change them.
     */
    if procost < 0.0 {
        /* SQL and PL-language functions are assumed more expensive */
        if languageOid == INTERNALlanguageId ||
            languageOid == ClanguageId {
            procost = 1.0;
        } else {
            procost = 100.0;
        }
    }
    if prorows < 0.0 {
        if returnsSet {
            prorows = 1000.0;
        } else {
            prorows = 0.0;      /* dummy value if not returnsSet */
        }
    } else if !returnsSet {
        ereport!(ERROR, errmsg!("ROWS is not applicable when function does not return a set"));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /*
     * And now that we have all the parameters, and know we're permitted to do
     * so, go ahead and create the function.
     */
    return ProcedureCreate(funcname,
                           namespaceId,
                           (*stmt).replace,
                           returnsSet,
                           prorettype,
                           GetUserId(),
                           languageOid,
                           languageValidator,
                           prosrc_str,  /* converted to text later */
                           probin_str,  /* converted to text later */
                           prosqlbody,
                           if (*stmt).is_procedure { PROKIND_PROCEDURE } else if isWindowFunc { PROKIND_WINDOW } else { PROKIND_FUNCTION },
                           security,
                           isLeakProof,
                           isStrict,
                           volatility,
                           parallel,
                           parameterTypes,
                           PointerGetDatum(allParameterTypes as *const c_void),
                           PointerGetDatum(parameterModes as *const c_void),
                           PointerGetDatum(parameterNames as *const c_void),
                           parameterDefaults,
                           PointerGetDatum(trftypes as *const c_void),
                           trfoids_list,
                           PointerGetDatum(proconfig as *const c_void),
                           prosupport,
                           procost,
                           prorows);
}

const SQL_CSTR: &[u8] = b"sql\0";

/* Helper opaque accessor for pg_language form fields from GETSTRUCT  TODO(pg-port) */
#[repr(C)] pub struct FormData_pg_language_fields {
    pub oid: Oid,
    pub lanname: NameData,
    pub lanowner: Oid,
    pub lanispl: bool,
    pub lanpltrusted: bool,
    pub lanplcallfoid: Oid,
    pub laninline: Oid,
    pub lanvalidator: Oid,
}
#[repr(C)] pub struct NameData { pub data: [c_char; 64] }

/*
 * Guts of function deletion.
 *
 * Note: this is also used for aggregate deletion, since the OIDs of
 * both functions and aggregates point to pg_proc.
 */
pub unsafe fn RemoveFunctionById(funcOid: Oid) {
    let mut relation: Relation;
    let mut tup: HeapTuple;
    let prokind: c_char;

    /*
     * Delete the pg_proc tuple.
     */
    relation = table_open(ProcedureRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
    if !HeapTupleIsValid(tup) {  /* should not happen */
        elog!(ERROR, "cache lookup failed for function {}", funcOid);
    }

    prokind = (*(GETSTRUCT(tup) as Form_pg_proc as *mut FormData_pg_proc_fields)).prokind;

    CatalogTupleDelete(relation, &mut (*tup).t_self);

    ReleaseSysCache(tup);

    table_close(relation, RowExclusiveLock);

    pgstat_drop_function(funcOid);

    /*
     * If there's a pg_aggregate tuple, delete that too.
     */
    if prokind == PROKIND_AGGREGATE {
        relation = table_open(AggregateRelationId, RowExclusiveLock);

        tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcOid));
        if !HeapTupleIsValid(tup) {  /* should not happen */
            elog!(ERROR, "cache lookup failed for pg_aggregate tuple for function {}", funcOid);
        }

        CatalogTupleDelete(relation, &mut (*tup).t_self);

        ReleaseSysCache(tup);

        table_close(relation, RowExclusiveLock);
    }
}

/*
 * Implements the ALTER FUNCTION utility command (except for the
 * RENAME and OWNER clauses, which are handled as part of the generic
 * ALTER framework).
 */
pub unsafe fn AlterFunction(pstate: *mut ParseState, stmt: *mut AlterFunctionStmt) -> ObjectAddress {
    let mut tup: HeapTuple;
    let funcOid: Oid;
    let procForm: Form_pg_proc;
    let is_procedure: bool;
    let rel: Relation;
    let mut volatility_item: *mut DefElem = null_mut();
    let mut strict_item: *mut DefElem = null_mut();
    let mut security_def_item: *mut DefElem = null_mut();
    let mut leakproof_item: *mut DefElem = null_mut();
    let mut set_items: *mut List = NIL;
    let mut cost_item: *mut DefElem = null_mut();
    let mut rows_item: *mut DefElem = null_mut();
    let mut support_item: *mut DefElem = null_mut();
    let mut parallel_item: *mut DefElem = null_mut();
    let mut address: ObjectAddress = core::mem::zeroed();

    rel = table_open(ProcedureRelationId, RowExclusiveLock);

    funcOid = LookupFuncWithArgs((*stmt).objtype, (*stmt).func, false);

    ObjectAddressSet(&mut address, ProcedureRelationId, funcOid);

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
    if !HeapTupleIsValid(tup) {  /* should not happen */
        elog!(ERROR, "cache lookup failed for function {}", funcOid);
    }

    procForm = GETSTRUCT(tup) as Form_pg_proc;
    let pf: *mut FormData_pg_proc_fields = procForm as *mut FormData_pg_proc_fields;

    /* Permission check: must own function */
    if !object_ownercheck(ProcedureRelationId, funcOid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, (*stmt).objtype,
                       NameListToString((*(*stmt).func).objname));
    }

    if (*pf).prokind == PROKIND_AGGREGATE {
        ereport!(ERROR, errmsg!("\"{}\" is an aggregate function",
            cstr_display(NameListToString((*(*stmt).func).objname))));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    is_procedure = (*pf).prokind == PROKIND_PROCEDURE;

    /* Examine requested actions. */
    foreach!(l, (*stmt).actions, {
        let defel: *mut DefElem = lfirst(current_cell!(l)) as *mut DefElem;

        if compute_common_attribute(pstate,
                                     is_procedure,
                                     defel,
                                     &mut volatility_item,
                                     &mut strict_item,
                                     &mut security_def_item,
                                     &mut leakproof_item,
                                     &mut set_items,
                                     &mut cost_item,
                                     &mut rows_item,
                                     &mut support_item,
                                     &mut parallel_item) == false {
            elog!(ERROR, "option \"{}\" not recognized", cstr_display((*defel).defname));
        }
    });

    if !volatility_item.is_null() {
        (*pf).provolatile = interpret_func_volatility(volatility_item);
    }
    if !strict_item.is_null() {
        (*pf).proisstrict = boolVal((*strict_item).arg);
    }
    if !security_def_item.is_null() {
        (*pf).prosecdef = boolVal((*security_def_item).arg);
    }
    if !leakproof_item.is_null() {
        (*pf).proleakproof = boolVal((*leakproof_item).arg);
        if (*pf).proleakproof && !superuser() {
            ereport!(ERROR, errmsg!("only superuser can define a leakproof function"));
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        }
    }
    if !cost_item.is_null() {
        (*pf).procost = defGetNumeric(cost_item);
        if (*pf).procost <= 0.0 {
            ereport!(ERROR, errmsg!("COST must be positive"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !rows_item.is_null() {
        (*pf).prorows = defGetNumeric(rows_item);
        if (*pf).prorows <= 0.0 {
            ereport!(ERROR, errmsg!("ROWS must be positive"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
        if !(*pf).proretset {
            ereport!(ERROR, errmsg!("ROWS is not applicable when function does not return a set"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }
    if !support_item.is_null() {
        /* interpret_func_support handles the privilege check */
        let newsupport: Oid = interpret_func_support(support_item);

        /* Add or replace dependency on support function */
        if OidIsValid((*pf).prosupport) {
            if changeDependencyFor(ProcedureRelationId, funcOid,
                                    ProcedureRelationId, (*pf).prosupport,
                                    newsupport) != 1 {
                elog!(ERROR, "could not change support dependency for function {}",
                     cstr_display(get_func_name(funcOid)));
            }
        } else {
            let mut referenced: ObjectAddress = core::mem::zeroed();

            referenced.classId = ProcedureRelationId;
            referenced.objectId = newsupport;
            referenced.objectSubId = 0;
            recordDependencyOn(&address, &referenced, DEPENDENCY_NORMAL);
        }

        (*pf).prosupport = newsupport;
    }
    if !parallel_item.is_null() {
        (*pf).proparallel = interpret_func_parallel(parallel_item);
    }
    if !set_items.is_null() {
        let datum: Datum;
        let mut isnull: bool = false;
        let mut a: *mut ArrayType;
        let mut repl_val: [Datum; Natts_pg_proc] = [0; Natts_pg_proc];
        let mut repl_null: [bool; Natts_pg_proc] = [false; Natts_pg_proc];
        let mut repl_repl: [bool; Natts_pg_proc] = [false; Natts_pg_proc];

        /* extract existing proconfig setting */
        datum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proconfig, &mut isnull);
        a = if isnull { null_mut() } else { DatumGetArrayTypeP(datum) };

        /* update according to each SET or RESET item, left to right */
        a = update_proconfig_value(a, set_items);

        /* update the tuple */
        for v in repl_repl.iter_mut() { *v = false; }
        repl_repl[(Anum_pg_proc_proconfig - 1) as usize] = true;

        if a.is_null() {
            repl_val[(Anum_pg_proc_proconfig - 1) as usize] = 0;
            repl_null[(Anum_pg_proc_proconfig - 1) as usize] = true;
        } else {
            repl_val[(Anum_pg_proc_proconfig - 1) as usize] = PointerGetDatum(a as *const c_void);
            repl_null[(Anum_pg_proc_proconfig - 1) as usize] = false;
        }

        tup = heap_modify_tuple(tup, RelationGetDescr(rel),
                                repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());
    }
    /* DO NOT put more touches of procForm below here; it's now dangling. */

    /* Do the update */
    CatalogTupleUpdate(rel, &mut (*tup).t_self, tup);

    InvokeObjectPostAlterHook(ProcedureRelationId, funcOid, 0);

    table_close(rel, NoLock);
    heap_freetuple(tup);

    return address;
}

/* Helper opaque accessor for pg_proc form fields from GETSTRUCT  TODO(pg-port) */
#[repr(C)] pub struct FormData_pg_proc_fields {
    pub oid: Oid,
    pub proname: NameData,
    pub pronamespace: Oid,
    pub proowner: Oid,
    pub prolang: Oid,
    pub procost: float4,
    pub prorows: float4,
    pub provariadic: Oid,
    pub prosupport: Oid,
    pub prokind: c_char,
    pub prosecdef: bool,
    pub proleakproof: bool,
    pub proisstrict: bool,
    pub proretset: bool,
    pub provolatile: c_char,
    pub proparallel: c_char,
    pub pronargs: int16,
    pub pronargdefaults: int16,
    pub prorettype: Oid,
    pub proargtypes: oidvector,
}

/*
 * CREATE CAST
 */
pub unsafe fn CreateCast(stmt: *mut CreateCastStmt) -> ObjectAddress {
    let sourcetypeid: Oid;
    let targettypeid: Oid;
    let sourcetyptype: c_char;
    let targettyptype: c_char;
    let funcid: Oid;
    let mut incastid: Oid = InvalidOid;
    let mut outcastid: Oid = InvalidOid;
    let nargs: c_int;
    let castcontext: c_char;
    let castmethod: c_char;
    let mut tuple: HeapTuple;
    let mut aclresult: AclResult;
    let myself: ObjectAddress;

    sourcetypeid = typenameTypeId(null_mut(), (*stmt).sourcetype);
    targettypeid = typenameTypeId(null_mut(), (*stmt).targettype);
    sourcetyptype = get_typtype(sourcetypeid);
    targettyptype = get_typtype(targettypeid);

    /* No pseudo-types allowed */
    if sourcetyptype == TYPTYPE_PSEUDO {
        ereport!(ERROR, errmsg!("source data type {} is a pseudo-type",
            cstr_display(TypeNameToString((*stmt).sourcetype))));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    if targettyptype == TYPTYPE_PSEUDO {
        ereport!(ERROR, errmsg!("target data type {} is a pseudo-type",
            cstr_display(TypeNameToString((*stmt).targettype))));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* Permission check */
    if !object_ownercheck(TypeRelationId, sourcetypeid, GetUserId())
        && !object_ownercheck(TypeRelationId, targettypeid, GetUserId()) {
        ereport!(ERROR, errmsg!("must be owner of type {} or type {}",
            cstr_display(format_type_be(sourcetypeid)),
            cstr_display(format_type_be(targettypeid))));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    aclresult = object_aclcheck(TypeRelationId, sourcetypeid, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, sourcetypeid);
    }

    aclresult = object_aclcheck(TypeRelationId, targettypeid, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, targettypeid);
    }

    /* Domains are allowed for historical reasons, but we warn */
    if sourcetyptype == TYPTYPE_DOMAIN {
        ereport!(WARNING, errmsg!("cast will be ignored because the source data type is a domain"));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    } else if targettyptype == TYPTYPE_DOMAIN {
        ereport!(WARNING, errmsg!("cast will be ignored because the target data type is a domain"));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    /* Determine the cast method */
    if !(*stmt).func.is_null() {
        castmethod = COERCION_METHOD_FUNCTION;
    } else if (*stmt).inout {
        castmethod = COERCION_METHOD_INOUT;
    } else {
        castmethod = COERCION_METHOD_BINARY;
    }

    if castmethod == COERCION_METHOD_FUNCTION {
        let procstruct: Form_pg_proc;

        funcid = LookupFuncWithArgs(OBJECT_FUNCTION, (*stmt).func, false);

        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for function {}", funcid);
        }

        procstruct = GETSTRUCT(tuple) as Form_pg_proc;
        let ps: *mut FormData_pg_proc_fields = procstruct as *mut FormData_pg_proc_fields;
        nargs = (*ps).pronargs as c_int;
        if nargs < 1 || nargs > 3 {
            ereport!(ERROR, errmsg!("cast function must take one to three arguments"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        if !IsBinaryCoercibleWithCast(sourcetypeid,
                                       *(*ps).proargtypes.values.as_ptr().add(0),
                                       &mut incastid) {
            ereport!(ERROR, errmsg!("argument of cast function must match or be binary-coercible from source data type"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        if nargs > 1 && *(*ps).proargtypes.values.as_ptr().add(1) != INT4OID {
            ereport!(ERROR, errmsg!("second argument of cast function must be type {}",
                "integer"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        if nargs > 2 && *(*ps).proargtypes.values.as_ptr().add(2) != BOOLOID {
            ereport!(ERROR, errmsg!("third argument of cast function must be type {}",
                "boolean"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        if !IsBinaryCoercibleWithCast((*ps).prorettype,
                                       targettypeid,
                                       &mut outcastid) {
            ereport!(ERROR, errmsg!("return data type of cast function must match or be binary-coercible to target data type"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        /*
         * Restricting the volatility of a cast function may or may not be a
         * good idea in the abstract, but it definitely breaks many old
         * user-defined types.  Disable this check --- tgl 2/1/03
         */
        /* #ifdef NOT_USED
        if (*ps).provolatile == PROVOLATILE_VOLATILE {
            ereport!(ERROR, errmsg!("cast function must not be volatile"));
        }
        #endif */
        if (*ps).prokind != PROKIND_FUNCTION {
            ereport!(ERROR, errmsg!("cast function must be a normal function"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        if (*ps).proretset {
            ereport!(ERROR, errmsg!("cast function must not return a set"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        ReleaseSysCache(tuple);
    } else {
        funcid = InvalidOid;
        nargs = 0;
    }

    if castmethod == COERCION_METHOD_BINARY {
        let mut typ1len: int16 = 0;
        let mut typ2len: int16 = 0;
        let mut typ1byval: bool = false;
        let mut typ2byval: bool = false;
        let mut typ1align: c_char = 0;
        let mut typ2align: c_char = 0;

        /*
         * Must be superuser to create binary-compatible casts, since
         * erroneous casts can easily crash the backend.
         */
        if !superuser() {
            ereport!(ERROR, errmsg!("must be superuser to create a cast WITHOUT FUNCTION"));
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        }

        /*
         * Also, insist that the types match as to size, alignment, and
         * pass-by-value attributes; this provides at least a crude check that
         * they have similar representations.  A pair of types that fail this
         * test should certainly not be equated.
         */
        get_typlenbyvalalign(sourcetypeid, &mut typ1len, &mut typ1byval, &mut typ1align);
        get_typlenbyvalalign(targettypeid, &mut typ2len, &mut typ2byval, &mut typ2align);
        if typ1len != typ2len ||
            typ1byval != typ2byval ||
            typ1align != typ2align {
            ereport!(ERROR, errmsg!("source and target data types are not physically compatible"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        /*
         * We know that composite, array, range and enum types are never
         * binary-compatible with each other.  They all have OIDs embedded in
         * them.
         *
         * Theoretically you could build a user-defined base type that is
         * binary-compatible with such a type.  But we disallow it anyway, as
         * in practice such a cast is surely a mistake.  You can always work
         * around that by writing a cast function.
         *
         * NOTE: if we ever have a kind of container type that doesn't need to
         * be rejected for this reason, we'd likely need to recursively apply
         * all of these same checks to the contained type(s).
         */
        if sourcetyptype == TYPTYPE_COMPOSITE ||
            targettyptype == TYPTYPE_COMPOSITE {
            ereport!(ERROR, errmsg!("composite data types are not binary-compatible"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        if OidIsValid(get_element_type(sourcetypeid)) ||
            OidIsValid(get_element_type(targettypeid)) {
            ereport!(ERROR, errmsg!("array data types are not binary-compatible"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        if sourcetyptype == TYPTYPE_RANGE ||
            targettyptype == TYPTYPE_RANGE ||
            sourcetyptype == TYPTYPE_MULTIRANGE ||
            targettyptype == TYPTYPE_MULTIRANGE {
            ereport!(ERROR, errmsg!("range data types are not binary-compatible"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        if sourcetyptype == TYPTYPE_ENUM ||
            targettyptype == TYPTYPE_ENUM {
            ereport!(ERROR, errmsg!("enum data types are not binary-compatible"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }

        /*
         * We also disallow creating binary-compatibility casts involving
         * domains.  Casting from a domain to its base type is already
         * allowed, and casting the other way ought to go through domain
         * coercion to permit constraint checking.  Again, if you're intent on
         * having your own semantics for that, create a no-op cast function.
         *
         * NOTE: if we were to relax this, the above checks for composites
         * etc. would have to be modified to look through domains to their
         * base types.
         */
        if sourcetyptype == TYPTYPE_DOMAIN ||
            targettyptype == TYPTYPE_DOMAIN {
            ereport!(ERROR, errmsg!("domain data types must not be marked binary-compatible"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
    }

    /*
     * Allow source and target types to be same only for length coercion
     * functions.  We assume a multi-arg function does length coercion.
     */
    if sourcetypeid == targettypeid && nargs < 2 {
        ereport!(ERROR, errmsg!("source data type and target data type are the same"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /* convert CoercionContext enum to char value for castcontext */
    /* C: switch (stmt->context) -- CoercionContext is a c_int alias here, so
     * use if/else to avoid const-pattern binding in a Rust match. */
    if (*stmt).context == COERCION_IMPLICIT {
        castcontext = COERCION_CODE_IMPLICIT;
    } else if (*stmt).context == COERCION_ASSIGNMENT {
        castcontext = COERCION_CODE_ASSIGNMENT;
    }
    /* COERCION_PLPGSQL is intentionally not covered here */
    else if (*stmt).context == COERCION_EXPLICIT {
        castcontext = COERCION_CODE_EXPLICIT;
    } else {
        elog!(ERROR, "unrecognized CoercionContext: {}", (*stmt).context);
        #[allow(unreachable_code)]
        { castcontext = 0; }    /* keep compiler quiet */
    }

    myself = CastCreate(sourcetypeid, targettypeid, funcid, incastid, outcastid,
                        castcontext, castmethod, DEPENDENCY_NORMAL);
    return myself;
}

unsafe fn check_transform_function(procstruct: Form_pg_proc) {
    let ps: *mut FormData_pg_proc_fields = procstruct as *mut FormData_pg_proc_fields;
    if (*ps).provolatile == PROVOLATILE_VOLATILE {
        ereport!(ERROR, errmsg!("transform function must not be volatile"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }
    if (*ps).prokind != PROKIND_FUNCTION {
        ereport!(ERROR, errmsg!("transform function must be a normal function"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }
    if (*ps).proretset {
        ereport!(ERROR, errmsg!("transform function must not return a set"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }
    if (*ps).pronargs != 1 {
        ereport!(ERROR, errmsg!("transform function must take one argument"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }
    if *(*ps).proargtypes.values.as_ptr().add(0) != INTERNALOID {
        ereport!(ERROR, errmsg!("first argument of transform function must be type {}",
            "internal"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }
}

/*
 * CREATE TRANSFORM
 */
pub unsafe fn CreateTransform(stmt: *mut CreateTransformStmt) -> ObjectAddress {
    let typeid: Oid;
    let typtype: c_char;
    let langid: Oid;
    let fromsqlfuncid: Oid;
    let tosqlfuncid: Oid;
    let mut aclresult: AclResult;
    let mut procstruct: Form_pg_proc;
    let mut values: [Datum; Natts_pg_transform] = [0; Natts_pg_transform];
    let mut nulls: [bool; Natts_pg_transform] = [false; Natts_pg_transform];
    let mut replaces: [bool; Natts_pg_transform] = [false; Natts_pg_transform];
    let transformid: Oid;
    let mut tuple: HeapTuple;
    let newtuple: HeapTuple;
    let relation: Relation;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let addrs: *mut ObjectAddresses;
    let is_replace: bool;

    /*
     * Get the type
     */
    typeid = typenameTypeId(null_mut(), (*stmt).type_name);
    typtype = get_typtype(typeid);

    if typtype == TYPTYPE_PSEUDO {
        ereport!(ERROR, errmsg!("data type {} is a pseudo-type",
            cstr_display(TypeNameToString((*stmt).type_name))));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    if typtype == TYPTYPE_DOMAIN {
        ereport!(ERROR, errmsg!("data type {} is a domain",
            cstr_display(TypeNameToString((*stmt).type_name))));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    if !object_ownercheck(TypeRelationId, typeid, GetUserId()) {
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typeid);
    }

    aclresult = object_aclcheck(TypeRelationId, typeid, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, typeid);
    }

    /*
     * Get the language
     */
    langid = get_language_oid((*stmt).lang, false);

    aclresult = object_aclcheck(LanguageRelationId, langid, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_LANGUAGE, (*stmt).lang);
    }

    /*
     * Get the functions
     */
    if !(*stmt).fromsql.is_null() {
        fromsqlfuncid = LookupFuncWithArgs(OBJECT_FUNCTION, (*stmt).fromsql, false);

        if !object_ownercheck(ProcedureRelationId, fromsqlfuncid, GetUserId()) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION, NameListToString((*(*stmt).fromsql).objname));
        }

        aclresult = object_aclcheck(ProcedureRelationId, fromsqlfuncid, GetUserId(), ACL_EXECUTE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString((*(*stmt).fromsql).objname));
        }

        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fromsqlfuncid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for function {}", fromsqlfuncid);
        }
        procstruct = GETSTRUCT(tuple) as Form_pg_proc;
        if (*(procstruct as *mut FormData_pg_proc_fields)).prorettype != INTERNALOID {
            ereport!(ERROR, errmsg!("return data type of FROM SQL function must be {}",
                "internal"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        check_transform_function(procstruct);
        ReleaseSysCache(tuple);
    } else {
        fromsqlfuncid = InvalidOid;
    }

    if !(*stmt).tosql.is_null() {
        tosqlfuncid = LookupFuncWithArgs(OBJECT_FUNCTION, (*stmt).tosql, false);

        if !object_ownercheck(ProcedureRelationId, tosqlfuncid, GetUserId()) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION, NameListToString((*(*stmt).tosql).objname));
        }

        aclresult = object_aclcheck(ProcedureRelationId, tosqlfuncid, GetUserId(), ACL_EXECUTE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString((*(*stmt).tosql).objname));
        }

        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(tosqlfuncid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for function {}", tosqlfuncid);
        }
        procstruct = GETSTRUCT(tuple) as Form_pg_proc;
        if (*(procstruct as *mut FormData_pg_proc_fields)).prorettype != typeid {
            ereport!(ERROR, errmsg!("return data type of TO SQL function must be the transform data type"));
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        }
        check_transform_function(procstruct);
        ReleaseSysCache(tuple);
    } else {
        tosqlfuncid = InvalidOid;
    }

    /*
     * Ready to go
     */
    values[(Anum_pg_transform_trftype - 1) as usize] = ObjectIdGetDatum(typeid);
    values[(Anum_pg_transform_trflang - 1) as usize] = ObjectIdGetDatum(langid);
    values[(Anum_pg_transform_trffromsql - 1) as usize] = ObjectIdGetDatum(fromsqlfuncid);
    values[(Anum_pg_transform_trftosql - 1) as usize] = ObjectIdGetDatum(tosqlfuncid);

    relation = table_open(TransformRelationId, RowExclusiveLock);

    tuple = SearchSysCache2(TRFTYPELANG,
                            ObjectIdGetDatum(typeid),
                            ObjectIdGetDatum(langid));
    if HeapTupleIsValid(tuple) {
        let form: Form_pg_transform = GETSTRUCT(tuple) as Form_pg_transform;

        if !(*stmt).replace {
            ereport!(ERROR, errmsg!("transform for type {} language \"{}\" already exists",
                cstr_display(format_type_be(typeid)),
                cstr_display((*stmt).lang)));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }

        replaces[(Anum_pg_transform_trffromsql - 1) as usize] = true;
        replaces[(Anum_pg_transform_trftosql - 1) as usize] = true;

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values.as_mut_ptr(), nulls.as_mut_ptr(), replaces.as_mut_ptr());
        CatalogTupleUpdate(relation, &mut (*newtuple).t_self, newtuple);

        transformid = (*(form as *mut FormData_pg_transform_fields)).oid;
        ReleaseSysCache(tuple);
        is_replace = true;
    } else {
        transformid = GetNewOidWithIndex(relation, TransformOidIndexId,
                                         Anum_pg_transform_oid);
        values[(Anum_pg_transform_oid - 1) as usize] = ObjectIdGetDatum(transformid);
        newtuple = heap_form_tuple(RelationGetDescr(relation), values.as_mut_ptr(), nulls.as_mut_ptr());
        CatalogTupleInsert(relation, newtuple);
        is_replace = false;
    }

    if is_replace {
        deleteDependencyRecordsFor(TransformRelationId, transformid, true);
    }

    addrs = new_object_addresses();

    /* make dependency entries */
    ObjectAddressSet(&mut myself, TransformRelationId, transformid);

    /* dependency on language */
    ObjectAddressSet(&mut referenced, LanguageRelationId, langid);
    add_exact_object_address(&referenced, addrs);

    /* dependency on type */
    ObjectAddressSet(&mut referenced, TypeRelationId, typeid);
    add_exact_object_address(&referenced, addrs);

    /* dependencies on functions */
    if OidIsValid(fromsqlfuncid) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, fromsqlfuncid);
        add_exact_object_address(&referenced, addrs);
    }
    if OidIsValid(tosqlfuncid) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, tosqlfuncid);
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, is_replace);

    /* Post creation hook for new transform */
    InvokeObjectPostCreateHook(TransformRelationId, transformid, 0);

    heap_freetuple(newtuple);

    table_close(relation, RowExclusiveLock);

    return myself;
}

/* Helper opaque accessor for pg_transform form fields from GETSTRUCT  TODO(pg-port) */
#[repr(C)] pub struct FormData_pg_transform_fields {
    pub oid: Oid,
    pub trftype: Oid,
    pub trflang: Oid,
    pub trffromsql: Oid,
    pub trftosql: Oid,
}

/*
 * get_transform_oid - given type OID and language OID, look up a transform OID
 *
 * If missing_ok is false, throw an error if the transform is not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_transform_oid(type_id: Oid, lang_id: Oid, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid2(TRFTYPELANG, Anum_pg_transform_oid,
                          ObjectIdGetDatum(type_id),
                          ObjectIdGetDatum(lang_id));
    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, errmsg!("transform for type {} language \"{}\" does not exist",
            cstr_display(format_type_be(type_id)),
            cstr_display(get_language_name(lang_id, false))));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }
    return oid;
}

/*
 * Subroutine for ALTER FUNCTION/AGGREGATE SET SCHEMA/RENAME
 *
 * Is there a function with the given name and signature already in the given
 * namespace?  If so, raise an appropriate error message.
 */
pub unsafe fn IsThereFunctionInNamespace(proname: *const c_char, pronargs: c_int,
                                         proargtypes: *mut oidvector, nspOid: Oid) {
    /* check for duplicate name (more friendly than unique-index failure) */
    if SearchSysCacheExists3(PROCNAMEARGSNSP,
                              CStringGetDatum(proname),
                              PointerGetDatum(proargtypes as *const c_void),
                              ObjectIdGetDatum(nspOid)) {
        ereport!(ERROR, errmsg!("function {} already exists in schema \"{}\"",
            cstr_display(funcname_signature_string(proname, pronargs,
                                      NIL, (*proargtypes).values.as_ptr())),
            cstr_display(get_namespace_name(nspOid))));
        /* C also: errcode(ERRCODE_DUPLICATE_FUNCTION) */
    }
}

/* --------------------------------------------------------------------------
 * Additional stubs for ExecuteDoStmt / ExecuteCallStmt / CallStmtResultDesc
 * -------------------------------------------------------------------------- */

// NullableDatum / FunctionCallInfo  TODO(pg-port)
#[repr(C)] pub struct NullableDatum { pub value: Datum, pub isnull: bool }
#[repr(C)] pub struct FunctionCallInfoBaseData {
    pub flinfo: *mut FmgrInfo,
    pub context: *mut Node,
    pub resultinfo: *mut Node,
    pub fncollation: Oid,
    pub isnull: bool,
    pub nargs: i16,
    pub args: [NullableDatum; 0],
}
type FunctionCallInfo = *mut FunctionCallInfoBaseData;

// HeapTupleData fields we touch for the RECORD return  TODO(pg-port)
// (uses the real HeapTupleData from htup_details for t_len/t_self/t_tableOid/t_data)

unsafe fn fcinfo_args(fcinfo: FunctionCallInfo) -> *mut NullableDatum {
    (*fcinfo).args.as_ptr() as *mut NullableDatum
}

/* LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS): stack fcinfo with room for nargs  TODO(pg-port) */
unsafe fn alloc_local_fcinfo(nargs: c_int) -> FunctionCallInfo {
    let sz = core::mem::size_of::<FunctionCallInfoBaseData>()
        + core::mem::size_of::<NullableDatum>() * nargs as usize;
    palloc0(sz) as FunctionCallInfo
}

unsafe fn InitFunctionCallInfoData(fcinfo: FunctionCallInfo, flinfo: *mut FmgrInfo,
                                   nargs: i16, collation: Oid, context: *mut Node,
                                   resultinfo: *mut Node) {
    (*fcinfo).flinfo = flinfo;
    (*fcinfo).context = context;
    (*fcinfo).resultinfo = resultinfo;
    (*fcinfo).fncollation = collation;
    (*fcinfo).isnull = false;
    (*fcinfo).nargs = nargs;
}
unsafe fn FunctionCallInvoke(fcinfo: FunctionCallInfo) -> Datum { unimplemented!() }
unsafe fn fmgr_info_set_expr(node: *mut Node, finfo: *mut FmgrInfo) { unimplemented!() }

unsafe fn DatumGetHeapTupleHeader(d: Datum) -> HeapTupleHeader { unimplemented!() }
unsafe fn HeapTupleHeaderGetTypeId(td: HeapTupleHeader) -> Oid { crate::access::htup_details::HeapTupleHeaderGetTypeId(td as _) as _ }
unsafe fn HeapTupleHeaderGetTypMod(td: HeapTupleHeader) -> int32 { crate::access::htup_details::HeapTupleHeaderGetTypMod(td as _) as _ }
unsafe fn HeapTupleHeaderGetDatumLength(td: HeapTupleHeader) -> u32 { crate::access::htup_details::HeapTupleHeaderGetDatumLength(td as _) as _ }
unsafe fn ItemPointerSetInvalid(pointer: *mut ItemPointerData) { crate::storage::itemptr::ItemPointerSetInvalid(pointer as _) }
unsafe fn NameStr_attname(att: Form_pg_attribute) -> *const c_char { unimplemented!() }

/* opaque-field accessor stubs for EState / TupOutputState  TODO(pg-port) */
unsafe fn estate_set_param_list_info(estate: *mut EState, params: ParamListInfo) { unimplemented!() }
unsafe fn tupoutput_slot(tstate: *mut TupOutputState) -> TupleTableSlot { unimplemented!() }
unsafe fn tupoutput_receive(tstate: *mut TupOutputState, slot: TupleTableSlot) { unimplemented!() }

/*
 * ExecuteDoStmt
 *		Execute inline procedural-language code
 *
 * See at ExecuteCallStmt() about the atomic argument.
 */
pub unsafe fn ExecuteDoStmt(pstate: *mut ParseState, stmt: *mut DoStmt, atomic: bool) {
    let codeblock: *mut InlineCodeBlock = makeNode!(InlineCodeBlock, T_InlineCodeBlock);
    let mut as_item: *mut DefElem = null_mut();
    let mut language_item: *mut DefElem = null_mut();
    let language: *mut c_char;
    let laninline: Oid;
    let languageTuple: HeapTuple;
    let languageStruct: Form_pg_language;

    /* Process options we got from gram.y */
    foreach!(arg, (*stmt).args, {
        let defel: *mut DefElem = lfirst(current_cell!(arg)) as *mut DefElem;

        if cstr_eq((*defel).defname, "as") {
            if !as_item.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            as_item = defel;
        } else if cstr_eq((*defel).defname, "language") {
            if !language_item.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            language_item = defel;
        } else {
            elog!(ERROR, "option \"{}\" not recognized",
                 cstr_display((*defel).defname));
        }
    });

    if !as_item.is_null() {
        (*codeblock).source_text = strVal((*as_item).arg);
    } else {
        ereport!(ERROR, errmsg!("no inline code specified"));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
    }

    /* if LANGUAGE option wasn't specified, use the default */
    if !language_item.is_null() {
        language = strVal((*language_item).arg);
    } else {
        language = PLPGSQL_CSTR.as_ptr() as *mut c_char;
    }

    /* Look up the language and validate permissions */
    languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language as *const c_void));
    if !HeapTupleIsValid(languageTuple) {
        ereport!(ERROR, errmsg!("language \"{}\" does not exist", cstr_display(language)));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT),
         * (extension_file_exists(language) ?
         *  errhint("Use CREATE EXTENSION to load the language into the database.") : 0) */
    }

    languageStruct = GETSTRUCT(languageTuple) as Form_pg_language;
    let ls: *mut FormData_pg_language_fields = languageStruct as *mut FormData_pg_language_fields;
    (*codeblock).langOid = (*ls).oid;
    (*codeblock).langIsTrusted = (*ls).lanpltrusted;
    (*codeblock).atomic = atomic;

    if (*ls).lanpltrusted {
        /* if trusted language, need USAGE privilege */
        let aclresult: AclResult;

        aclresult = object_aclcheck(LanguageRelationId, (*codeblock).langOid, GetUserId(),
                                    ACL_USAGE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_LANGUAGE,
                           NameStr(&(*ls).lanname as *const _ as *const c_void));
        }
    } else {
        /* if untrusted language, must be superuser */
        if !superuser() {
            aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_LANGUAGE,
                           NameStr(&(*ls).lanname as *const _ as *const c_void));
        }
    }

    /* get the handler function's OID */
    laninline = (*ls).laninline;
    if !OidIsValid(laninline) {
        ereport!(ERROR, errmsg!("language \"{}\" does not support inline code execution",
            cstr_display(NameStr(&(*ls).lanname as *const _ as *const c_void))));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    ReleaseSysCache(languageTuple);

    /* execute the inline handler */
    OidFunctionCall1(laninline, PointerGetDatum(codeblock as *const c_void));
}

const PLPGSQL_CSTR: &[u8] = b"plpgsql\0";

/*
 * Execute CALL statement
 *
 * Inside a top-level CALL statement, transaction-terminating commands such as
 * COMMIT or a PL-specific equivalent are allowed.  The terminology in the SQL
 * standard is that CALL establishes a non-atomic execution context.  Most
 * other commands establish an atomic execution context, in which transaction
 * control actions are not allowed.  If there are nested executions of CALL,
 * we want to track the execution context recursively, so that the nested
 * CALLs can also do transaction control.  Note, however, that for example in
 * CALL -> SELECT -> CALL, the second call cannot do transaction control,
 * because the SELECT in between establishes an atomic execution context.
 *
 * So when ExecuteCallStmt() is called from the top level, we pass in atomic =
 * false (recall that that means transactions = yes).  We then create a
 * CallContext node with content atomic = false, which is passed in the
 * fcinfo->context field to the procedure invocation.  The language
 * implementation should then take appropriate measures to allow or prevent
 * transaction commands based on that information, e.g., call
 * SPI_connect_ext(SPI_OPT_NONATOMIC).  The language should also pass on the
 * atomic flag to any nested invocations to CALL.
 *
 * The expression data structures and execution context that we create
 * within this function are children of the portalContext of the Portal
 * that the CALL utility statement runs in.  Therefore, any pass-by-ref
 * values that we're passing to the procedure will survive transaction
 * commits that might occur inside the procedure.
 */
pub unsafe fn ExecuteCallStmt(stmt: *mut CallStmt, params: ParamListInfo, atomic: bool, dest: *mut DestReceiver) {
    let fcinfo: FunctionCallInfo = alloc_local_fcinfo(FUNC_MAX_ARGS);
    let fexpr: *mut FuncExpr;
    let nargs: c_int;
    let mut i: c_int;
    let aclresult: AclResult;
    let mut flinfo: FmgrInfo = core::mem::zeroed();
    let callcontext: *mut CallContext;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let tp: HeapTuple;
    let mut fcusage: PgStat_FunctionCallUsage = core::mem::zeroed();
    let retval: Datum;

    fexpr = (*stmt).funcexpr;
    Assert!(!fexpr.is_null());
    Assert!(IsA!(fexpr, T_FuncExpr));

    aclresult = object_aclcheck(ProcedureRelationId, (*fexpr).funcid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_PROCEDURE, get_func_name((*fexpr).funcid));
    }

    /* Prep the context object we'll pass to the procedure */
    callcontext = makeNode!(CallContext, T_CallContext);
    (*callcontext).atomic = atomic;

    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum((*fexpr).funcid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", (*fexpr).funcid);
    }

    /*
     * If proconfig is set we can't allow transaction commands because of the
     * way the GUC stacking works: The transaction boundary would have to pop
     * the proconfig setting off the stack.  That restriction could be lifted
     * by redesigning the GUC nesting mechanism a bit.
     */
    if !heap_attisnull(tp, Anum_pg_proc_proconfig, null_mut()) {
        (*callcontext).atomic = true;
    }

    /*
     * In security definer procedures, we can't allow transaction commands.
     * StartTransaction() insists that the security context stack is empty,
     * and AbortTransaction() resets the security context.  This could be
     * reorganized, but right now it doesn't work.
     */
    if (*(GETSTRUCT(tp) as Form_pg_proc as *mut FormData_pg_proc_fields)).prosecdef {
        (*callcontext).atomic = true;
    }

    ReleaseSysCache(tp);

    /* safety check; see ExecInitFunc() */
    nargs = list_length((*fexpr).args);
    if nargs > FUNC_MAX_ARGS {
        ereport!(ERROR, errmsg_plural!(
            "cannot pass more than {} argument to a procedure",
            "cannot pass more than {} arguments to a procedure",
            FUNC_MAX_ARGS as i64,
            FUNC_MAX_ARGS));
        /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS) */
    }

    /* Initialize function call structure */
    InvokeFunctionExecuteHook((*fexpr).funcid);
    fmgr_info((*fexpr).funcid, &mut flinfo);
    fmgr_info_set_expr(fexpr as *mut Node, &mut flinfo);
    InitFunctionCallInfoData(fcinfo, &mut flinfo, nargs as i16, (*fexpr).inputcollid,
                             callcontext as *mut Node, null_mut());

    /*
     * Evaluate procedure arguments inside a suitable execution context.  Note
     * we can't free this context till the procedure returns.
     */
    estate = CreateExecutorState();
    estate_set_param_list_info(estate, params);
    econtext = CreateExprContext(estate);

    /*
     * If we're called in non-atomic context, we also have to ensure that the
     * argument expressions run with an up-to-date snapshot.  Our caller will
     * have provided a current snapshot in atomic contexts, but not in
     * non-atomic contexts, because the possibility of a COMMIT/ROLLBACK
     * destroying the snapshot makes higher-level management too complicated.
     */
    if !atomic {
        PushActiveSnapshot(GetTransactionSnapshot());
    }

    i = 0;
    foreach!(lc, (*fexpr).args, {
        let exprstate: *mut ExprState;
        let val: Datum;
        let mut isnull: bool = false;

        exprstate = ExecPrepareExpr(lfirst(current_cell!(lc)) as *mut Expr, estate);

        val = ExecEvalExprSwitchContext(exprstate, econtext, &mut isnull);

        (*fcinfo_args(fcinfo).add(i as usize)).value = val;
        (*fcinfo_args(fcinfo).add(i as usize)).isnull = isnull;

        i += 1;
    });

    /* Get rid of temporary snapshot for arguments, if we made one */
    if !atomic {
        PopActiveSnapshot();
    }

    /* Here we actually call the procedure */
    pgstat_init_function_usage(fcinfo as *mut c_void, &mut fcusage);
    retval = FunctionCallInvoke(fcinfo);
    pgstat_end_function_usage(&mut fcusage, true);

    /* Handle the procedure's outputs */
    if (*fexpr).funcresulttype == VOIDOID {
        /* do nothing */
    } else if (*fexpr).funcresulttype == RECORDOID {
        /* send tuple to client */
        let td: HeapTupleHeader;
        let tupType: Oid;
        let tupTypmod: int32;
        let retdesc: TupleDesc;
        let mut rettupdata: HeapTupleData = core::mem::zeroed();
        let tstate: *mut TupOutputState;
        let slot: TupleTableSlot;

        if (*fcinfo).isnull {
            elog!(ERROR, "procedure returned null record");
        }

        /*
         * Ensure there's an active snapshot whilst we execute whatever's
         * involved here.  Note that this is *not* sufficient to make the
         * world safe for TOAST pointers to be included in the returned data:
         * the referenced data could have gone away while we didn't hold a
         * snapshot.  Hence, it's incumbent on PLs that can do COMMIT/ROLLBACK
         * to not return TOAST pointers, unless those pointers were fetched
         * after the last COMMIT/ROLLBACK in the procedure.
         *
         * XXX that is a really nasty, hard-to-test requirement.  Is there a
         * way to remove it?
         */
        EnsurePortalSnapshotExists();

        td = DatumGetHeapTupleHeader(retval);
        tupType = HeapTupleHeaderGetTypeId(td);
        tupTypmod = HeapTupleHeaderGetTypMod(td);
        retdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

        tstate = begin_tup_output_tupdesc(dest, retdesc,
                                          &TTSOpsHeapTuple);

        rettupdata.t_len = HeapTupleHeaderGetDatumLength(td);
        ItemPointerSetInvalid(&mut rettupdata.t_self);
        rettupdata.t_tableOid = InvalidOid;
        rettupdata.t_data = td;

        slot = ExecStoreHeapTuple(&mut rettupdata, tupoutput_slot(tstate), false);
        tupoutput_receive(tstate, slot);

        end_tup_output(tstate);

        ReleaseTupleDesc(retdesc);
    } else {
        elog!(ERROR, "unexpected result type for procedure: {}",
             (*fexpr).funcresulttype);
    }

    FreeExecutorState(estate);
}

/*
 * Construct the tuple descriptor for a CALL statement return
 */
pub unsafe fn CallStmtResultDesc(stmt: *mut CallStmt) -> TupleDesc {
    let fexpr: *mut FuncExpr;
    let tuple: HeapTuple;
    let tupdesc: TupleDesc;

    fexpr = (*stmt).funcexpr;

    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum((*fexpr).funcid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for procedure {}", (*fexpr).funcid);
    }

    tupdesc = build_function_result_tupdesc_t(tuple);

    ReleaseSysCache(tuple);

    /*
     * The result of build_function_result_tupdesc_t has the right column
     * names, but it just has the declared output argument types, which is the
     * wrong thing in polymorphic cases.  Get the correct types by examining
     * stmt->outargs.  We intentionally keep the atttypmod as -1 and the
     * attcollation as the type's default, since that's always the appropriate
     * thing for function outputs; there's no point in considering any
     * additional info available from outargs.  Note that tupdesc is null if
     * there are no outargs.
     */
    if !tupdesc.is_null() {
        Assert!((*tupdesc).natts == list_length((*stmt).outargs));
        let mut i: c_int = 0;
        while i < (*tupdesc).natts {
            let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);
            let outarg: *mut Node = list_nth((*stmt).outargs, i) as *mut Node;

            TupleDescInitEntry(tupdesc,
                               i + 1,
                               NameStr_attname(att),
                               exprType(outarg),
                               -1,
                               0);
            i += 1;
        }
    }

    return tupdesc;
}

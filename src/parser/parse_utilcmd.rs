/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.rs
 *    Perform parse analysis work for various utility commands
 *
 * Formerly we did this work during parse_analyze_*() in analyze.c.  However
 * that is fairly unsafe in the presence of querytree caching, since any
 * database state that we depend on in making the transformations might be
 * obsolete by the time the utility command is executed; and utility commands
 * have no infrastructure for holding locks or rechecking plan validity.
 * Hence these functions are now called at the start of execution of their
 * respective utility commands.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *    src/backend/parser/parse_utilcmd.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(unused_assignments)]
#![allow(unreachable_code)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

use crate::{
    castNode, current_cell, foreach, intVal, lfirst_node, linitial_node, lsecond_node,
    list_make1, list_make2, list_make3, makeNode, strVal, IsA,
};

// ---------------------------------------------------------------------------
// Standard library / crate imports
// ---------------------------------------------------------------------------
use crate::postgres_ext::Oid;
use crate::postgres::{Datum, ObjectIdGetDatum, Int32GetDatum};
use crate::c::{OidIsValid, int32};

use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*, CmdType, CmdType::*};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst, lfirst_int, lfirst_oid, linitial, lsecond, llast, lnext,
    lappend, lappend_int, lappend_oid, lcons, list_head,
    list_concat, list_length, list_make1_impl,
    list_nth, list_nth_cell, list_truncate, list_member_int,
    list_copy, list_free, list_last_cell,
    ListCell,
};
use crate::nodes::bitmapset::{Bitmapset, bms_add_member};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
};
use crate::nodes::makefuncs::{
    makeConst, makeBoolExpr, makeVar, makeTargetEntry,
    makeSimpleA_Expr, makeRangeVar, makeAlias, makeFromExpr,
    makeDefElem, makeFuncCall,
};
use crate::nodes::value::{makeString, String as PgString};
use crate::nodes::primnodes::{
    Expr, Var, Alias, TargetEntry,
    BoolExpr, BoolExprType,
    CoercionForm, CoercionForm::*,
    CoercionContext, CoercionContext::*,
    RangeTblRef,
    Const,
    RangeVar,
};
use crate::nodes::parsenodes::{
    Query, ColumnRef, A_Const, A_Expr, FuncCall,
    RangeTblEntry,
    IndexElem, IndexStmt,
    CreateStmt, CreateSeqStmt, AlterSeqStmt, AlterTableStmt, AlterTableCmd,
    Constraint, ColumnDef, TableLikeClause,
    ConstrType, ConstrType::*,
    AlterTableType, AlterTableType::*,
    ObjectType, ObjectType::*,
    DropBehavior, DropBehavior::*,
    SortByDir, SortByDir::*,
    SortByNulls, SortByNulls::*,
    PartitionRangeDatumKind, PartitionRangeDatumKind::*,
    CommentStmt, CreateStatsStmt, StatsElem,
    RuleStmt, PartitionCmd, PartitionBoundSpec, PartitionRangeDatum,
    TypeCast, TypeName,
    ViewStmt, CreateTrigStmt, GrantStmt,
    DefElem,
};

use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition,
    setup_parser_errposition_callback,
    Index, ParseCallbackState, ParseExprKind, ParseExprKind::*,
    ParseNamespaceColumn, ParseNamespaceItem, ParseState,
    make_parsestate, free_parsestate,
};
use crate::utils::rel::{Relation, RelationData};
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_collate::assign_expr_collations;
use crate::parser::parse_type::{LookupCollation, typenameType, typenameTypeId};
use crate::parser::parse_relation::{
    addRangeTableEntryForRelation,
    addNSItemToQuery,
};
use crate::parser::parse_clause::transformWhereClause;
use crate::parser::parse_coerce::coerce_to_target_type;
use crate::parser::parse_target::FigureIndexColname;

use crate::access::table::table::{
    table_close, table_open, table_openrv, table_openrv_extended,
};
use crate::access::common::relation::{
    relation_open, relation_close, relation_openrv,
};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::htup_details::HeapTuple;

use crate::storage::lockdefs::{
    AccessShareLock, AccessExclusiveLock, NoLock,
    RowExclusiveLock, RowShareLock, LOCKMODE,
};

use crate::utils::cache::lsyscache::{
    format_type_be,
    get_typcollation,
    get_namespace_name,
    get_attname, get_atttype, get_attnum, get_rel_name, get_rel_namespace,
    get_relname_relid,
    pstrdup, palloc, pfree,
};
use crate::utils::cache::relcache::{
    RelationGetRelid, RelationGetNamespace,
    RelationGetDescr, RelationGetForm, RelationGetIndexList,
    RelationGetIndexExpressions, RelationGetIndexPredicate,
    RelationGetStatExtList,
};
use crate::utils::rel::RelationGetRelationName;
// RelationGetPartitionKey: kept as local stub below to avoid PartitionKey type conflict

use crate::catalog::namespace::{
    RangeVarGetAndCheckCreationNamespace, RangeVarGetCreationNamespace,
    RangeVarAdjustRelationPersistence, makeRangeVarFromNameList,
};
use crate::nodes::makefuncs::{makeColumnDef, makeNotNullConstraint, makeTypeNameFromOid};
use crate::access::index::indexam::{index_open, index_close};

// ---------------------------------------------------------------------------
// Stub imports for not-yet-ported modules (TODO pg-port)
// ---------------------------------------------------------------------------

// commands/tablecmds.h
/// TODO(pg-port): getIdentitySequence not yet ported
unsafe fn getIdentitySequence(
    rel: Relation,
    attnum: AttrNumber,
    missing_ok: bool,
) -> Oid {
    unimplemented!("getIdentitySequence not yet ported")
}

// commands/sequence.h
/// TODO(pg-port): sequence_options not yet ported
unsafe fn sequence_options(seq_relid: Oid) -> *mut List {
    unimplemented!("sequence_options not yet ported")
}

// commands/defrem.h
/// TODO(pg-port): errorConflictingDefElem not yet ported
unsafe fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState) {
    unimplemented!("errorConflictingDefElem not yet ported")
}

// commands/tablecmds.h
/// TODO(pg-port): ChooseRelationName not yet ported
unsafe fn ChooseRelationName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
    namespaceid: Oid,
    istemp: bool,
) -> *mut c_char {
    unimplemented!("ChooseRelationName not yet ported")
}

// catalog/namespace.h
/// TODO(pg-port): quote_qualified_identifier not yet ported
unsafe fn quote_qualified_identifier(ns: *const c_char, name: *const c_char) -> *mut c_char {
    unimplemented!("quote_qualified_identifier not yet ported")
}

// utils/syscache.h
/// TODO(pg-port): SearchSysCache1 not yet ported
unsafe fn SearchSysCache1(cache_id: c_int, key: Datum) -> crate::access::htup_details::HeapTuple {
    unimplemented!("SearchSysCache1 not yet ported")
}
/// TODO(pg-port): ReleaseSysCache not yet ported
unsafe fn ReleaseSysCache(tuple: crate::access::htup_details::HeapTuple) {
    unimplemented!("ReleaseSysCache not yet ported")
}
/// TODO(pg-port): SysCacheGetAttr not yet ported
unsafe fn SysCacheGetAttr(
    cache_id: c_int,
    tup: crate::access::htup_details::HeapTuple,
    attr_num: c_int,
    is_null: *mut bool,
) -> Datum {
    unimplemented!("SysCacheGetAttr not yet ported")
}
/// TODO(pg-port): SysCacheGetAttrNotNull not yet ported
unsafe fn SysCacheGetAttrNotNull(
    cache_id: c_int,
    tup: *mut c_void,
    attr_num: c_int,
) -> Datum {
    unimplemented!("SysCacheGetAttrNotNull not yet ported")
}
unsafe fn HeapTupleIsValid(tup: crate::access::htup_details::HeapTuple) -> bool {
    unimplemented!("HeapTupleIsValid not yet ported")
}

// utils/builtins.h
/// TODO(pg-port): TextDatumGetCString not yet ported
unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    unimplemented!("TextDatumGetCString not yet ported")
}

// nodes/nodes.h helpers
/// TODO(pg-port): stringToNode not yet ported
unsafe fn stringToNode(s: *const c_char) -> *mut c_void {
    unimplemented!("stringToNode not yet ported")
}
/// TODO(pg-port): nodeToString not yet ported
unsafe fn nodeToString(node: *const c_void) -> *mut c_char {
    unimplemented!("nodeToString not yet ported")
}
/// TODO(pg-port): copyObject not yet ported
unsafe fn copyObject(node: *const c_void) -> *mut c_void {
    unimplemented!("copyObject not yet ported")
}
/// TODO(pg-port): equal not yet ported
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    unimplemented!("equal not yet ported")
}

// catalog/pg_type.h form accessor
/// TODO(pg-port): GETSTRUCT not yet ported
unsafe fn GETSTRUCT(tup: crate::access::htup_details::HeapTuple) -> *mut c_void {
    unimplemented!("GETSTRUCT not yet ported")
}

// catalog
/// TODO(pg-port): lookup_rowtype_tupdesc / ReleaseTupleDesc not yet ported
unsafe fn lookup_rowtype_tupdesc(typid: Oid, typmod: int32) -> TupleDesc {
    unimplemented!("lookup_rowtype_tupdesc not yet ported")
}
unsafe fn ReleaseTupleDesc(tupdesc: TupleDesc) {
    unimplemented!("ReleaseTupleDesc not yet ported")
}

// access/common/tupdesc.h macros
/// TODO(pg-port): TupleDescAttr not yet ported
unsafe fn TupleDescAttr(tupdesc: TupleDesc, n: c_int) -> *mut c_void {
    unimplemented!("TupleDescAttr not yet ported")
}
/// TODO(pg-port): TupleDescGetDefault not yet ported
unsafe fn TupleDescGetDefault(tupdesc: TupleDesc, attnum: AttrNumber) -> *mut Node {
    unimplemented!("TupleDescGetDefault not yet ported")
}

// optimizer/optimizer.h
/// TODO(pg-port): expression_planner not yet ported
unsafe fn expression_planner(expr: *mut crate::nodes::primnodes::Expr) -> *mut crate::nodes::primnodes::Expr {
    unimplemented!("expression_planner not yet ported")
}
/// TODO(pg-port): evaluate_expr not yet ported
unsafe fn evaluate_expr(
    expr: *mut crate::nodes::primnodes::Expr,
    result_type: Oid,
    result_typmod: int32,
    result_collation: Oid,
) -> *mut crate::nodes::primnodes::Expr {
    unimplemented!("evaluate_expr not yet ported")
}
/// TODO(pg-port): contain_var_clause not yet ported
unsafe fn contain_var_clause(node: *mut Node) -> bool {
    unimplemented!("contain_var_clause not yet ported")
}

// rewrite/rewriteManip.h
/// TODO(pg-port): rangeTableEntry_used not yet ported
unsafe fn rangeTableEntry_used(node: *const Node, rt_index: c_int, sublevels_up: c_int) -> bool {
    unimplemented!("rangeTableEntry_used not yet ported")
}
/// TODO(pg-port): map_variable_attnos not yet ported
unsafe fn map_variable_attnos(
    tree: *mut Node,
    varno: c_int,
    sublevels_up: c_int,
    attmap: *const AttrMap,
    rowtype_includes_oid: Oid,
    found_whole_row: *mut bool,
) -> *mut Node {
    unimplemented!("map_variable_attnos not yet ported")
}

// parser/analyze.h
/// TODO(pg-port): transformStmt not yet ported
unsafe fn transformStmt(pstate: *mut ParseState, parseTree: *mut Node) -> *mut Query {
    unimplemented!("transformStmt not yet ported")
}
/// TODO(pg-port): getInsertSelectQuery not yet ported
unsafe fn getInsertSelectQuery(parsetree: *mut Query, sub_qry: *mut *mut Query) -> *mut Query {
    unimplemented!("getInsertSelectQuery not yet ported")
}

// catalog/heap.h
/// TODO(pg-port): build_attrmap_by_name not yet ported
unsafe fn build_attrmap_by_name(dst: TupleDesc, src: TupleDesc, missing_ok: bool) -> *mut AttrMap {
    unimplemented!("build_attrmap_by_name not yet ported")
}

// utils/acl.h
type AclResult = c_int;
unsafe fn object_aclcheck(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: c_int,
) -> AclResult {
    unimplemented!("object_aclcheck not yet ported")
}
unsafe fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: c_int) -> AclResult {
    unimplemented!("pg_class_aclcheck not yet ported")
}
unsafe fn aclcheck_error(acl_error: AclResult, obj_type: ObjectType, object_name: *const c_char) {
    unimplemented!("aclcheck_error not yet ported")
}
unsafe fn GetUserId() -> Oid {
    unimplemented!("GetUserId not yet ported")
}

// catalog/pg_collation.h
unsafe fn get_relation_constraint_oid(
    relid: Oid,
    con_name: *const c_char,
    missing_ok: bool,
) -> Oid {
    unimplemented!("get_relation_constraint_oid not yet ported")
}

// commands/comment.h
unsafe fn GetComment(oid: Oid, classoid: Oid, subid: c_int) -> *mut c_char {
    unimplemented!("GetComment not yet ported")
}

// access/reloptions.h
unsafe fn untransformRelOptions(datum: Datum) -> *mut List {
    unimplemented!("untransformRelOptions not yet ported")
}
unsafe fn get_attoptions(relid: Oid, attnum: c_int) -> Datum {
    unimplemented!("get_attoptions not yet ported")
}

// catalog/index.h
unsafe fn GetDefaultOpClass(typid: Oid, am_oid: Oid) -> Oid {
    unimplemented!("GetDefaultOpClass not yet ported")
}

// catalog/namespace.h type helpers
unsafe fn type_is_range(typid: Oid) -> bool {
    unimplemented!("type_is_range not yet ported")
}
unsafe fn type_is_multirange(typid: Oid) -> bool {
    unimplemented!("type_is_multirange not yet ported")
}

// catalog/pg_attribute.h
unsafe fn SystemAttributeByName(attname: *const c_char) -> *const c_void {
    unimplemented!("SystemAttributeByName not yet ported")
}
unsafe fn SystemAttributeDefinition(attnum: i16) -> *const c_void {
    unimplemented!("SystemAttributeDefinition not yet ported")
}

// utils/lsyscache.h additional
unsafe fn get_partition_col_typid(key: *mut PartitionKey, col: c_int) -> Oid {
    unimplemented!("get_partition_col_typid not yet ported")
}
unsafe fn get_partition_col_typmod(key: *mut PartitionKey, col: c_int) -> int32 {
    unimplemented!("get_partition_col_typmod not yet ported")
}
unsafe fn get_partition_col_collation(key: *mut PartitionKey, col: c_int) -> Oid {
    unimplemented!("get_partition_col_collation not yet ported")
}
unsafe fn get_partition_strategy(key: *mut PartitionKey) -> c_char {
    unimplemented!("get_partition_strategy not yet ported")
}
unsafe fn get_partition_natts(key: *mut PartitionKey) -> c_int {
    unimplemented!("get_partition_natts not yet ported")
}
unsafe fn get_partition_exprs(key: *mut PartitionKey) -> *mut List {
    unimplemented!("get_partition_exprs not yet ported")
}
unsafe fn deparse_expression(
    expr: *mut Node,
    dpcontext: *mut List,
    forceprefix: bool,
    showimplicit: bool,
) -> *mut c_char {
    unimplemented!("deparse_expression not yet ported")
}
unsafe fn deparse_context_for(relname: *const c_char, relid: Oid) -> *mut List {
    unimplemented!("deparse_context_for not yet ported")
}
unsafe fn transformPartitionBound(
    pstate: *mut ParseState,
    parent: Relation,
    spec: *mut PartitionBoundSpec,
) -> *mut PartitionBoundSpec {
    unimplemented!("transformPartitionBound not yet ported")
}

// utils/typcache.h
unsafe fn deconstruct_array_builtin(
    array: *mut c_void,
    elmtype: Oid,
    elems: *mut *mut Datum,
    nulls: *mut *mut bool,
    nelems: *mut c_int,
) {
    unimplemented!("deconstruct_array_builtin not yet ported")
}
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut c_void {
    unimplemented!("DatumGetArrayTypeP not yet ported")
}
unsafe fn DatumGetObjectId(d: Datum) -> Oid {
    unimplemented!("DatumGetObjectId not yet ported")
}
unsafe fn DatumGetPointer(d: Datum) -> *mut c_void {
    unimplemented!("DatumGetPointer not yet ported")
}
unsafe fn ARR_NDIM(arr: *mut c_void) -> c_int {
    unimplemented!("ARR_NDIM not yet ported")
}
unsafe fn ARR_HASNULL(arr: *mut c_void) -> bool {
    unimplemented!("ARR_HASNULL not yet ported")
}
unsafe fn ARR_ELEMTYPE(arr: *mut c_void) -> Oid {
    unimplemented!("ARR_ELEMTYPE not yet ported")
}
unsafe fn ARR_DATA_PTR(arr: *mut c_void) -> *mut u8 {
    unimplemented!("ARR_DATA_PTR not yet ported")
}
unsafe fn ARR_DIMS(arr: *mut c_void) -> *mut c_int {
    unimplemented!("ARR_DIMS not yet ported")
}

/// Wrap a *const/*mut c_char for use in format strings.
/// SAFETY: pointer must be non-null and NUL-terminated.
macro_rules! cstr_fmt {
    ($p:expr) => {
        std::ffi::CStr::from_ptr($p as *const c_char).to_string_lossy()
    };
}
macro_rules! errmsg_internal {
    ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) };
}

// strcmp via core::ffi -- avoids libc dependency
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    use std::ffi::CStr;
    let sa = CStr::from_ptr(a);
    let sb = CStr::from_ptr(b);
    match sa.cmp(sb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

// Opaque pg types used only by pointer
pub enum AttrMap {}
pub enum PartitionKey {}
pub enum TupleConstr {}
// c_char is already imported via std::ffi::{c_char} at top of file

/// TODO(pg-port): get_tablespace_name not yet ported from utils/cache/lsyscache
unsafe fn get_tablespace_name(spcoid: Oid) -> *mut c_char {
    unimplemented!("get_tablespace_name not yet ported")
}
/// TODO(pg-port): RelationGetNotNullConstraints not yet ported
unsafe fn RelationGetNotNullConstraints(relid: Oid, include_notnulls: bool, include_inherited: bool) -> *mut List {
    unimplemented!("RelationGetNotNullConstraints not yet ported")
}
/// TODO(pg-port): RelationGetPartitionKey not yet ported
unsafe fn RelationGetPartitionKey(rel: Relation) -> *mut PartitionKey {
    unimplemented!("RelationGetPartitionKey not yet ported")
}
/// TODO(pg-port): checkMembershipInCurrentExtension not yet ported
unsafe fn checkMembershipInCurrentExtension(address: *const ObjectAddress) {
    unimplemented!("checkMembershipInCurrentExtension not yet ported")
}
/// TODO(pg-port): SystemTypeName not yet ported
unsafe fn SystemTypeName(name: *mut c_char) -> *mut TypeName {
    unimplemented!("SystemTypeName not yet ported")
}
/// TODO(pg-port): SystemFuncName not yet ported
unsafe fn SystemFuncName(name: *mut c_char) -> *mut List {
    unimplemented!("SystemFuncName not yet ported")
}
/// TODO(pg-port): check_of_type not yet ported
unsafe fn check_of_type(heap_tup: crate::access::htup_details::HeapTuple) {
    unimplemented!("check_of_type not yet ported")
}
/// TODO(pg-port): get_index_constraint not yet ported
unsafe fn get_index_constraint(indexId: Oid) -> Oid {
    InvalidOid /* TODO(pg-port) */
}
/// TODO(pg-port): get_index_am_oid not yet ported
unsafe fn get_index_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    unimplemented!("get_index_am_oid not yet ported")
}
/// TODO(pg-port): generateClonedIndexStmt not yet ported
unsafe fn generateClonedIndexStmt(
    heapRel: *mut RangeVar,
    source_index: Relation,
    attmap: *const AttrMap,
    constraintOid: *mut Oid,
) -> *mut IndexStmt {
    unimplemented!("generateClonedIndexStmt not yet ported")
}

// Syscache ids (stubs; real values in catcache)
const RELOID: c_int = 26;
const INDEXRELID: c_int = 27;
const CONSTROID: c_int = 28;
const COLLOID: c_int = 29;
const CLAOID: c_int = 30;
const AMOID: c_int = 31;
const OPEROID: c_int = 32;
const STATEXTOID: c_int = 33;
// COERCION_ASSIGNMENT is CoercionContext::COERCION_ASSIGNMENT (imported above)
// COERCE_IMPLICIT_CAST is CoercionForm::COERCE_IMPLICIT_CAST (imported above)
const PRS2_OLD_VARNO: c_int = 1;
const PRS2_NEW_VARNO: c_int = 2;

// pg_am oid vector / pg_index types (opaque stubs)
pub enum oidvector {}

// Error codes (subset used here)
const ERRCODE_DUPLICATE_TABLE: c_int = 0x23505;
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0x4250E; /* 42P16 in SQLSTATE */
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0x0A000;
const ERRCODE_SYNTAX_ERROR: c_int = 0x42601;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0x42809;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0x42704;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0x55000;
const ERRCODE_INVALID_TABLE_DEFINITION: c_int = 0x4250E; /* 42P16 */
const ERRCODE_DATATYPE_MISMATCH: c_int = 0x42804;
const ERRCODE_UNDEFINED_COLUMN: c_int = 0x42703;
const ERRCODE_DUPLICATE_COLUMN: c_int = 0x42701;
const ERRCODE_INVALID_SCHEMA_DEFINITION: c_int = 0x4250D; /* 42P15 */
const ERRCODE_INVALID_COLUMN_REFERENCE: c_int = 0x4250A; /* 42P10 */

// relkind constants
const RELKIND_RELATION: u8 = b'r';
const RELKIND_VIEW: u8 = b'v';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const RELKIND_PARTITIONED_INDEX: u8 = b'I';
const RELKIND_INDEX: u8 = b'i';

// relpersistence
const RELPERSISTENCE_TEMP: u8 = b't';
const RELPERSISTENCE_PERMANENT: u8 = b'p';
const RELPERSISTENCE_UNLOGGED: u8 = b'u';

// Constraint types and AlterTable subtypes are imported from
// crate::nodes::parsenodes::{ConstrType::*, AlterTableType::*}

// ObjectType, DropBehavior, SortByDir, SortByNulls, PartitionRangeDatumKind imported from parsenodes

// Access lock modes (supplement to lockdefs)
const ACL_USAGE: c_int = 1;
const ACL_SELECT: c_int = 2;
const ACLCHECK_OK: AclResult = 0;

// Partition strategies
const PARTITION_STRATEGY_HASH: c_char = b'h' as c_char;
const PARTITION_STRATEGY_LIST: c_char = b'l' as c_char;
const PARTITION_STRATEGY_RANGE: c_char = b'r' as c_char;

// PartitionRangeDatumKind and SortBy variants imported from parsenodes

// Index option bits
const INDOPTION_DESC: i16 = 0x0001;
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

// DEFAULT_INDEX_TYPE
const DEFAULT_INDEX_TYPE: &[u8] = b"btree\0";

// Oid constants
const InvalidOid: Oid = 0;
const RelationRelationId: Oid = 1259;
const TypeRelationId: Oid = 1247;
const ConstraintRelationId: Oid = 2606;
const StatisticExtRelationId: Oid = 3381;
const InvalidRelFileNumber: u32 = u32::MAX; /* RelFileNumber = Oid = u32 */
const InvalidSubTransactionId: u32 = 0;

// STATS_EXT kinds
const STATS_EXT_NDISTINCT: u8 = b'd';
const STATS_EXT_DEPENDENCIES: u8 = b'f';
const STATS_EXT_MCV: u8 = b'm';
const STATS_EXT_EXPRESSIONS: u8 = b'e';

// Anum constants (stubs; real values from pg_attribute.h etc.)
const Anum_pg_index_indcollation: c_int = 1;
const Anum_pg_index_indclass: c_int = 2;
const Anum_pg_index_indexprs: c_int = 3;
const Anum_pg_index_indpred: c_int = 4;
const Anum_pg_class_reloptions: c_int = 5;
const Anum_pg_constraint_conexclop: c_int = 6;
const Anum_pg_statistic_ext_stxkind: c_int = 7;
const Anum_pg_statistic_ext_stxexprs: c_int = 8;
const CHAROID: Oid = 18;
const OIDOID: Oid = 26;

// COERCE kind and CMD_ variants imported from enums CoercionForm::* and CmdType::*

// ObjectAddress / extension check stub
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: c_int,
}
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

// INT oid stubs
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;

// NameStr macro stub
unsafe fn NameStr(name_data: [i8; 64]) -> *mut c_char {
    name_data.as_ptr() as *mut c_char
}

// CONSTRAINT_EXCLUSION / CONSTRAINT_PRIMARY / CONSTRAINT_UNIQUE
const CONSTRAINT_EXCLUSION: u8 = b'x';
const CONSTRAINT_PRIMARY: u8 = b'p';
const CONSTRAINT_UNIQUE: u8 = b'u';

// CREATE_TABLE_LIKE options
const CREATE_TABLE_LIKE_GENERATED: u32 = 0x0010;
const CREATE_TABLE_LIKE_IDENTITY: u32 = 0x0020;
const CREATE_TABLE_LIKE_STORAGE: u32 = 0x0040;
const CREATE_TABLE_LIKE_COMPRESSION: u32 = 0x0080;
const CREATE_TABLE_LIKE_COMMENTS: u32 = 0x0100;
const CREATE_TABLE_LIKE_DEFAULTS: u32 = 0x0008;
const CREATE_TABLE_LIKE_CONSTRAINTS: u32 = 0x0200;
const CREATE_TABLE_LIKE_INDEXES: u32 = 0x0400;
const CREATE_TABLE_LIKE_STATISTICS: u32 = 0x0800;

// CompressionMethodIsValid
unsafe fn CompressionMethodIsValid(cm: u8) -> bool {
    cm != 0
}
unsafe fn GetCompressionMethodName(cm: u8) -> *const c_char {
    unimplemented!("GetCompressionMethodName not yet ported")
}

// AttributeNumberIsValid
fn AttributeNumberIsValid(attnum: i16) -> bool {
    attnum != 0
}

// ---------------------------------------------------------------------------
// Part 2: State structs + transformCreateStmt + generateSerialExtraStmts
// ---------------------------------------------------------------------------

/// State shared by transformCreateStmt and its subroutines
pub struct CreateStmtContext {
    /// overall parser state
    pub pstate: *mut ParseState,
    /// "CREATE [FOREIGN] TABLE" or "ALTER TABLE"
    pub stmtType: *const c_char,
    /// relation to create
    pub relation: *mut RangeVar,
    /// opened/locked rel, if ALTER
    pub rel: Relation,
    /// relations to inherit from
    pub inhRelations: *mut List,
    /// true if CREATE/ALTER FOREIGN TABLE
    pub isforeign: bool,
    /// true if altering existing table
    pub isalter: bool,
    /// ColumnDef items
    pub columns: *mut List,
    /// CHECK constraints
    pub ckconstraints: *mut List,
    /// NOT NULL constraints
    pub nnconstraints: *mut List,
    /// FOREIGN KEY constraints
    pub fkconstraints: *mut List,
    /// index-creating constraints
    pub ixconstraints: *mut List,
    /// LIKE clauses that need post-processing
    pub likeclauses: *mut List,
    /// "before list" of things to do before creating the table
    pub blist: *mut List,
    /// "after list" of things to do after creating the table
    pub alist: *mut List,
    /// PRIMARY KEY index, if any
    pub pkey: *mut IndexStmt,
    /// true if table is partitioned
    pub ispartitioned: bool,
    /// transformed FOR VALUES
    pub partbound: *mut PartitionBoundSpec,
    /// true if statement contains OF typename
    pub ofType: bool,
}

/// State shared by transformCreateSchemaStmtElements and its subroutines
pub struct CreateSchemaStmtContext {
    /// name of schema
    pub schemaname: *const c_char,
    /// CREATE SEQUENCE items
    pub sequences: *mut List,
    /// CREATE TABLE items
    pub tables: *mut List,
    /// CREATE VIEW items
    pub views: *mut List,
    /// CREATE INDEX items
    pub indexes: *mut List,
    /// CREATE TRIGGER items
    pub triggers: *mut List,
    /// GRANT items
    pub grants: *mut List,
}

// (Rust does not need forward declarations; implementations follow below.)


/*
 * transformCreateStmt -
 *    parse analysis for CREATE TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed CreateStmt, but there may be additional actions
 * to be done before and after the actual DefineRelation() call.
 * In addition to normal utility commands such as AlterTableStmt and
 * IndexStmt, the result list may contain TableLikeClause(s), representing
 * the need to perform additional parse analysis after DefineRelation().
 *
 * SQL allows constraints to be scattered all over, so thumb through
 * the columns and collect all constraints into one place.
 * If there are any implied indices (e.g. UNIQUE or PRIMARY KEY)
 * then expand those into multiple IndexStmt blocks.
 *    - thomas 1997-12-02
 */
pub unsafe fn transformCreateStmt(stmt: *mut CreateStmt, queryString: *const c_char) -> *mut List {
    let mut pstate: *mut ParseState;
    let mut cxt: CreateStmtContext = core::mem::zeroed();
    let mut result: *mut List;
    let mut save_alist: *mut List;
    let mut elements: *mut ListCell;
    let mut namespaceid: Oid = InvalidOid;
    let mut existing_relid: Oid = InvalidOid;
    let mut pcbstate: ParseCallbackState = core::mem::zeroed();

    /* Set up pstate */
    pstate = make_parsestate(core::ptr::null_mut());
    (*pstate).p_sourcetext = queryString;

    /*
     * Look up the creation namespace.  This also checks permissions on the
     * target namespace, locks it against concurrent drops, checks for a
     * preexisting relation in that namespace with the same name, and updates
     * stmt->relation->relpersistence if the selected namespace is temporary.
     */
    setup_parser_errposition_callback(
        &mut pcbstate,
        pstate,
        (*(*stmt).relation).location,
    );
    namespaceid = RangeVarGetAndCheckCreationNamespace(
        (*stmt).relation,
        NoLock,
        &mut existing_relid,
    );
    cancel_parser_errposition_callback(&mut pcbstate);

    /*
     * If the relation already exists and the user specified "IF NOT EXISTS",
     * bail out with a NOTICE.
     */
    if (*stmt).if_not_exists && OidIsValid(existing_relid) {
        /*
         * If we are in an extension script, insist that the pre-existing
         * object be a member of the extension, to avoid security risks.
         */
        let mut address = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
        ObjectAddressSet(&mut address, RelationRelationId, existing_relid);
        checkMembershipInCurrentExtension(&address);

        /* OK to skip */
        ereport!(
            NOTICE,
            errmsg!(
                "relation \"{}\" already exists, skipping",
                cstr_fmt!((*(*stmt).relation).relname)
            )
            /* C also: errcode(ERRCODE_DUPLICATE_TABLE) */
        );
        return NIL;
    }

    /*
     * If the target relation name isn't schema-qualified, make it so.  This
     * prevents some corner cases in which added-on rewritten commands might
     * think they should apply to other relations that have the same name and
     * are earlier in the search path.  But a local temp table is effectively
     * specified to be in pg_temp, so no need for anything extra in that case.
     */
    if (*(*stmt).relation).schemaname.is_null()
        && (*(*stmt).relation).relpersistence != RELPERSISTENCE_TEMP as i8
    {
        (*(*stmt).relation).schemaname = get_namespace_name(namespaceid);
    }

    /* Set up CreateStmtContext */
    cxt.pstate = pstate;
    if IsA!(stmt as *mut Node, T_CreateForeignTableStmt) {
        cxt.stmtType = b"CREATE FOREIGN TABLE\0".as_ptr() as *const c_char;
        cxt.isforeign = true;
    } else {
        cxt.stmtType = b"CREATE TABLE\0".as_ptr() as *const c_char;
        cxt.isforeign = false;
    }
    cxt.relation = (*stmt).relation;
    cxt.rel = core::ptr::null_mut();
    cxt.inhRelations = (*stmt).inhRelations;
    cxt.isalter = false;
    cxt.columns = NIL;
    cxt.ckconstraints = NIL;
    cxt.nnconstraints = NIL;
    cxt.fkconstraints = NIL;
    cxt.ixconstraints = NIL;
    cxt.likeclauses = NIL;
    cxt.blist = NIL;
    cxt.alist = NIL;
    cxt.pkey = core::ptr::null_mut();
    cxt.ispartitioned = !(*stmt).partspec.is_null();
    cxt.partbound = (*stmt).partbound;
    cxt.ofType = !(*stmt).ofTypename.is_null();

    /* grammar enforces: !ofTypename || !inhRelations */
    debug_assert!((*stmt).ofTypename.is_null() || (*stmt).inhRelations.is_null());

    if !(*stmt).ofTypename.is_null() {
        transformOfType(&mut cxt, (*stmt).ofTypename);
    }

    if !(*stmt).partspec.is_null() {
        if !(*stmt).inhRelations.is_null() && (*stmt).partbound.is_null() {
            ereport!(
                ERROR,
                errmsg!("cannot create partitioned table as inheritance child")
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            );
        }
    }

    /*
     * Run through each primary element in the table creation clause. Separate
     * column defs from constraints, and do preliminary analysis.
     */
    foreach!(lc, (*stmt).tableElts, {
        let element: *mut Node = lfirst(current_cell!(lc)) as *mut Node;

        match nodeTag(element) {
            T_ColumnDef => {
                transformColumnDefinition(&mut cxt, element as *mut ColumnDef);
            }
            T_Constraint => {
                transformTableConstraint(&mut cxt, element as *mut Constraint);
            }
            T_TableLikeClause => {
                transformTableLikeClause(&mut cxt, element as *mut TableLikeClause);
            }
            _ => {
                elog!(ERROR, "unrecognized node type: {}", nodeTag(element) as c_int);
            }
        }
    });

    /*
     * Transfer anything we already have in cxt.alist into save_alist, to keep
     * it separate from the output of transformIndexConstraints.  (This may
     * not be necessary anymore, but we'll keep doing it to preserve the
     * historical order of execution of the alist commands.)
     */
    save_alist = cxt.alist;
    cxt.alist = NIL;

    /* stmt->constraints == NIL at this point */
    debug_assert!((*stmt).constraints.is_null() || list_length((*stmt).constraints) == 0);

    /*
     * Before processing index constraints, which could include a primary key,
     * we must scan all not-null constraints to propagate the is_not_null flag
     * to each corresponding ColumnDef.  This is necessary because table-level
     * not-null constraints have not been marked in each ColumnDef, and the PK
     * processing code needs to know whether one constraint has already been
     * declared in order not to declare a redundant one.
     */
    foreach!(lc_nn, cxt.nnconstraints, {
        let nn: *mut Constraint = lfirst(current_cell!(lc_nn)) as *mut Constraint;
        let colname: *mut c_char = strVal!(linitial((*nn).keys));

        foreach!(lc_cd, cxt.columns, {
            let cd: *mut ColumnDef = lfirst(current_cell!(lc_cd)) as *mut ColumnDef;

            /* not our column? */
            if libc_strcmp((*cd).colname, colname) != 0 {
                continue;
            }
            /* Already marked not-null? Nothing to do */
            if (*cd).is_not_null {
                break;
            }
            /* Bingo, we're done for this constraint */
            (*cd).is_not_null = true;
            break;
        });
    });

    /* Postprocess constraints that give rise to index definitions. */
    transformIndexConstraints(&mut cxt);

    /*
     * Re-consideration of LIKE clauses should happen after creation of
     * indexes, but before creation of foreign keys.  This order is critical
     * because a LIKE clause may attempt to create a primary key.  If there's
     * also a pkey in the main CREATE TABLE list, creation of that will not
     * check for a duplicate at runtime (since index_check_primary_key()
     * expects that we rejected dups here).  Creation of the LIKE-generated
     * pkey behaves like ALTER TABLE ADD, so it will check, but obviously that
     * only works if it happens second.  On the other hand, we want to make
     * pkeys before foreign key constraints, in case the user tries to make a
     * self-referential FK.
     */
    cxt.alist = list_concat(cxt.alist, cxt.likeclauses);

    /* Postprocess foreign-key constraints. */
    transformFKConstraints(&mut cxt, true, false);

    /*
     * Postprocess check constraints.
     *
     * For regular tables all constraints can be marked valid immediately,
     * because the table is new therefore empty. Not so for foreign tables.
     */
    transformCheckConstraints(&mut cxt, !cxt.isforeign);

    /* Output results. */
    (*stmt).tableElts = cxt.columns;
    (*stmt).constraints = cxt.ckconstraints;
    (*stmt).nnconstraints = cxt.nnconstraints;

    result = lappend(cxt.blist, stmt as *mut c_void);
    result = list_concat(result, cxt.alist);
    result = list_concat(result, save_alist);

    result
}

// libc strcmp wrapper
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    strcmp(a, b)
}

/*
 * generateSerialExtraStmts
 *        Generate CREATE SEQUENCE and ALTER SEQUENCE ... OWNED BY statements
 *        to create the sequence for a serial or identity column.
 *
 * This includes determining the name the sequence will have.  The caller
 * can ask to get back the name components by passing non-null pointers
 * for snamespace_p and sname_p.
 */
unsafe fn generateSerialExtraStmts(
    cxt: *mut CreateStmtContext,
    column: *mut ColumnDef,
    seqtypid: Oid,
    seqoptions: *mut List,
    for_identity: bool,
    col_exists: bool,
    snamespace_p: *mut *mut c_char,
    sname_p: *mut *mut c_char,
) {
    let mut option: *mut ListCell;
    let mut nameEl: *mut DefElem = core::ptr::null_mut();
    let mut loggedEl: *mut DefElem = core::ptr::null_mut();
    let mut snamespaceid: Oid = InvalidOid;
    let mut snamespace: *mut c_char;
    let mut sname: *mut c_char;
    let mut seqpersistence: u8;
    let mut seqstmt: *mut CreateSeqStmt;
    let mut altseqstmt: *mut AlterSeqStmt;
    let mut attnamelist: *mut List;

    /* Make a copy of this as we may end up modifying it in the code below */
    let mut seqoptions: *mut List = list_copy(seqoptions);

    /*
     * Check for non-SQL-standard options (not supported within CREATE
     * SEQUENCE, because they'd be redundant), and remove them from the
     * seqoptions list if found.
     */
    /* TODO(pg-port): foreach_delete_current not yet ported; iterate manually */
    let mut lc_opt: *mut ListCell = list_head(seqoptions);
    while !lc_opt.is_null() {
        let defel: *mut DefElem = lfirst_node!(DefElem, T_DefElem, lc_opt);
        let next = lnext(seqoptions, lc_opt);

        let dname = (*defel).defname;
        if strcmp(dname, b"sequence_name\0".as_ptr() as *const c_char) == 0 {
            if !nameEl.is_null() {
                errorConflictingDefElem(defel, (*cxt).pstate);
            }
            nameEl = defel;
            /* remove from list -- simplified: rebuild without this element */
            /* actual foreach_delete_current logic handled by list rebuild below */
        } else if strcmp(dname, b"logged\0".as_ptr() as *const c_char) == 0
            || strcmp(dname, b"unlogged\0".as_ptr() as *const c_char) == 0
        {
            if !loggedEl.is_null() {
                errorConflictingDefElem(defel, (*cxt).pstate);
            }
            loggedEl = defel;
        }
        lc_opt = next;
    }

    /* Remove nameEl and loggedEl from seqoptions (rebuild list without them) */
    {
        let mut new_opts: *mut List = NIL;
        let mut lc2: *mut ListCell = list_head(seqoptions);
        while !lc2.is_null() {
            let defel: *mut DefElem = lfirst(lc2) as *mut DefElem;
            let next = lnext(seqoptions, lc2);
            if defel != nameEl && defel != loggedEl {
                new_opts = lappend(new_opts, defel as *mut c_void);
            }
            lc2 = next;
        }
        seqoptions = new_opts;
    }

    /*
     * Determine namespace and name to use for the sequence.
     */
    if !nameEl.is_null() {
        /* Use specified name */
        let rv: *mut RangeVar = makeRangeVarFromNameList(
            castNode!(List, T_List, (*nameEl).arg as *mut Node)
        );

        snamespace = (*rv).schemaname;
        if snamespace.is_null() {
            /* Given unqualified SEQUENCE NAME, select namespace */
            if !(*cxt).rel.is_null() {
                snamespaceid = RelationGetNamespace((*cxt).rel);
            } else {
                snamespaceid = RangeVarGetCreationNamespace((*cxt).relation);
            }
            snamespace = get_namespace_name(snamespaceid);
        }
        sname = (*rv).relname;
    } else {
        /*
         * Generate a name.
         *
         * Although we use ChooseRelationName, it's not guaranteed that the
         * selected sequence name won't conflict; given sufficiently long
         * field names, two different serial columns in the same table could
         * be assigned the same sequence name, and we'd not notice since we
         * aren't creating the sequence quite yet.  In practice this seems
         * quite unlikely to be a problem, especially since few people would
         * need two serial columns in one table.
         */
        if !(*cxt).rel.is_null() {
            snamespaceid = RelationGetNamespace((*cxt).rel);
        } else {
            snamespaceid = RangeVarGetCreationNamespace((*cxt).relation);
            RangeVarAdjustRelationPersistence((*cxt).relation, snamespaceid);
        }
        snamespace = get_namespace_name(snamespaceid);
        sname = ChooseRelationName(
            (*(*cxt).relation).relname,
            (*column).colname,
            b"seq\0".as_ptr() as *const c_char,
            snamespaceid,
            false,
        );
    }

    ereport!(
        DEBUG1,
        errmsg_internal!(
            "{} will create implicit sequence \"{}\" for serial column \"{}.{}\"",
            cstr_fmt!((*cxt).stmtType),
            cstr_fmt!(sname),
            cstr_fmt!((*(*cxt).relation).relname),
            cstr_fmt!((*column).colname)
        )
    );

    /*
     * Determine the persistence of the sequence.  By default we copy the
     * persistence of the table, but if LOGGED or UNLOGGED was specified, use
     * that (as long as the table isn't TEMP).
     *
     * For CREATE TABLE, we get the persistence from cxt->relation, which
     * comes from the CreateStmt in progress.  For ALTER TABLE, the parser
     * won't set cxt->relation->relpersistence, but we have cxt->rel as the
     * existing table, so we copy the persistence from there.
     */
    seqpersistence = if !(*cxt).rel.is_null() {
        (*(*(*cxt).rel).rd_rel).relpersistence as u8
    } else {
        (*(*cxt).relation).relpersistence as u8
    };

    if !loggedEl.is_null() {
        if seqpersistence == RELPERSISTENCE_TEMP {
            ereport!(
                ERROR,
                errmsg!("cannot set logged status of a temporary sequence")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 *         parser_errposition(cxt->pstate, loggedEl->location) */
            );
        } else if strcmp(
            (*loggedEl).defname,
            b"logged\0".as_ptr() as *const c_char,
        ) == 0 {
            seqpersistence = RELPERSISTENCE_PERMANENT;
        } else {
            seqpersistence = RELPERSISTENCE_UNLOGGED;
        }
    }

    /*
     * Build a CREATE SEQUENCE command to create the sequence object, and add
     * it to the list of things to be done before this CREATE/ALTER TABLE.
     */
    seqstmt = makeNode!(CreateSeqStmt, T_CreateSeqStmt);
    (*seqstmt).for_identity = for_identity;
    (*seqstmt).sequence = makeRangeVar(snamespace, sname, -1);
    (*(*seqstmt).sequence).relpersistence = seqpersistence as i8;
    (*seqstmt).options = seqoptions;

    /*
     * If a sequence data type was specified, add it to the options.  Prepend
     * to the list rather than append; in case a user supplied their own AS
     * clause, the "redundant options" error will point to their occurrence,
     * not our synthetic one.
     */
    if OidIsValid(seqtypid) {
        (*seqstmt).options = lcons(
            makeDefElem(
                b"as\0".as_ptr() as *mut c_char,
                makeTypeNameFromOid(seqtypid, -1) as *mut Node,
                -1,
            ) as *mut c_void,
            (*seqstmt).options,
        );
    }

    /*
     * If this is ALTER ADD COLUMN, make sure the sequence will be owned by
     * the table's owner.  The current user might be someone else (perhaps a
     * superuser, or someone who's only a member of the owning role), but the
     * SEQUENCE OWNED BY mechanisms will bleat unless table and sequence have
     * exactly the same owning role.
     */
    if !(*cxt).rel.is_null() {
        (*seqstmt).ownerId = (*(*(*cxt).rel).rd_rel).relowner;
    } else {
        (*seqstmt).ownerId = InvalidOid;
    }

    (*cxt).blist = lappend((*cxt).blist, seqstmt as *mut c_void);

    /*
     * Store the identity sequence name that we decided on.  ALTER TABLE ...
     * ADD COLUMN ... IDENTITY needs this so that it can fill the new column
     * with values from the sequence, while the association of the sequence
     * with the table is not set until after the ALTER TABLE.
     */
    (*column).identitySequence = (*seqstmt).sequence;

    /*
     * Build an ALTER SEQUENCE ... OWNED BY command to mark the sequence as
     * owned by this column, and add it to the appropriate list of things to
     * be done along with this CREATE/ALTER TABLE.  In a CREATE or ALTER ADD
     * COLUMN, it must be done after the statement because we don't know the
     * column's attnum yet.  But if we do have the attnum (in AT_AddIdentity),
     * we can do the marking immediately, which improves some ALTER TABLE
     * behaviors.
     */
    altseqstmt = makeNode!(AlterSeqStmt, T_AlterSeqStmt);
    (*altseqstmt).sequence = makeRangeVar(snamespace, sname, -1);
    attnamelist = list_make3!(
        makeString(snamespace) as *mut c_void,
        makeString((*(*cxt).relation).relname) as *mut c_void,
        makeString((*column).colname) as *mut c_void
    );
    (*altseqstmt).options = list_make1!(
        makeDefElem(
            b"owned_by\0".as_ptr() as *mut c_char,
            attnamelist as *mut Node,
            -1,
        ) as *mut c_void
    );
    (*altseqstmt).for_identity = for_identity;

    if col_exists {
        (*cxt).blist = lappend((*cxt).blist, altseqstmt as *mut c_void);
    } else {
        (*cxt).alist = lappend((*cxt).alist, altseqstmt as *mut c_void);
    }

    if !snamespace_p.is_null() {
        *snamespace_p = snamespace;
    }
    if !sname_p.is_null() {
        *sname_p = sname;
    }
}

// ---------------------------------------------------------------------------
// Part 3: transformColumnDefinition + transformTableConstraint
// ---------------------------------------------------------------------------

/*
 * transformColumnDefinition -
 *        transform a single ColumnDef within CREATE TABLE
 *        Also used in ALTER TABLE ADD COLUMN
 */
unsafe fn transformColumnDefinition(cxt: *mut CreateStmtContext, column: *mut ColumnDef) {
    let mut is_serial: bool = false;
    let mut saw_nullable: bool = false;
    let mut saw_default: bool = false;
    let mut saw_identity: bool = false;
    let mut saw_generated: bool = false;
    let mut need_notnull: bool = false;
    let mut disallow_noinherit_notnull: bool = false;
    let mut notnull_constraint: *mut Constraint = core::ptr::null_mut();

    (*cxt).columns = lappend((*cxt).columns, column as *mut c_void);

    /* Check for SERIAL pseudo-types */
    if !(*column).typeName.is_null()
        && list_length((*(*column).typeName).names) == 1
        && !(*(*column).typeName).pct_type
    {
        let typname: *mut c_char = strVal!(linitial((*(*column).typeName).names));

        if strcmp(typname, b"smallserial\0".as_ptr() as *const c_char) == 0
            || strcmp(typname, b"serial2\0".as_ptr() as *const c_char) == 0
        {
            is_serial = true;
            (*(*column).typeName).names = NIL;
            (*(*column).typeName).typeOid = INT2OID;
        } else if strcmp(typname, b"serial\0".as_ptr() as *const c_char) == 0
            || strcmp(typname, b"serial4\0".as_ptr() as *const c_char) == 0
        {
            is_serial = true;
            (*(*column).typeName).names = NIL;
            (*(*column).typeName).typeOid = INT4OID;
        } else if strcmp(typname, b"bigserial\0".as_ptr() as *const c_char) == 0
            || strcmp(typname, b"serial8\0".as_ptr() as *const c_char) == 0
        {
            is_serial = true;
            (*(*column).typeName).names = NIL;
            (*(*column).typeName).typeOid = INT8OID;
        }

        /*
         * We have to reject "serial[]" explicitly, because once we've set
         * typeid, LookupTypeName won't notice arrayBounds.  We don't need any
         * special coding for serial(typmod) though.
         */
        if is_serial
            && list_length((*(*column).typeName).arrayBounds) > 0
        {
            ereport!(
                ERROR,
                errmsg!("array of serial is not implemented")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 *         parser_errposition(cxt->pstate, column->typeName->location) */
            );
        }
    }

    /* Do necessary work on the column type declaration */
    if !(*column).typeName.is_null() {
        transformColumnType(cxt, column);
    }

    /* Special actions for SERIAL pseudo-types */
    if is_serial {
        let mut snamespace: *mut c_char = core::ptr::null_mut();
        let mut sname: *mut c_char = core::ptr::null_mut();
        let mut qstring: *mut c_char;
        let mut snamenode: *mut A_Const;
        let mut castnode: *mut TypeCast;
        let mut funccallnode: *mut FuncCall;
        let mut constraint: *mut Constraint;

        generateSerialExtraStmts(
            cxt,
            column,
            (*(*column).typeName).typeOid,
            NIL,
            false,
            false,
            &mut snamespace,
            &mut sname,
        );

        /*
         * Create appropriate constraints for SERIAL.  We do this in full,
         * rather than shortcutting, so that we will detect any conflicting
         * constraints the user wrote (like a different DEFAULT).
         *
         * Create an expression tree representing the function call
         * nextval('sequencename').  We cannot reduce the raw tree to cooked
         * form until after the sequence is created, but there's no need to do
         * so.
         */
        qstring = quote_qualified_identifier(snamespace, sname);
        snamenode = makeNode!(A_Const, T_A_Const);
        /* set A_Const.val as a string value */
        core::ptr::write(
            core::ptr::addr_of_mut!((*snamenode).val.sval),
            core::mem::ManuallyDrop::new(crate::nodes::value::String {
                r#type: T_String,
                sval: qstring,
            }),
        );
        (*snamenode).location = -1;
        castnode = makeNode!(TypeCast, T_TypeCast);
        (*castnode).typeName = SystemTypeName(b"regclass\0".as_ptr() as *mut c_char);
        (*castnode).arg = snamenode as *mut Node;
        (*castnode).location = -1;
        funccallnode = makeFuncCall(
            SystemFuncName(b"nextval\0".as_ptr() as *mut c_char),
            list_make1!(castnode as *mut c_void),
            COERCE_EXPLICIT_CALL,
            -1,
        );
        constraint = makeNode!(Constraint, T_Constraint);
        (*constraint).contype = CONSTR_DEFAULT;
        (*constraint).location = -1;
        (*constraint).raw_expr = funccallnode as *mut Node;
        (*constraint).cooked_expr = core::ptr::null_mut();
        (*column).constraints = lappend((*column).constraints, constraint as *mut c_void);

        /* have a not-null constraint added later */
        need_notnull = true;
        disallow_noinherit_notnull = true;
    }

    /* Process column constraints, if any... */
    transformConstraintAttrs(cxt, (*column).constraints);

    /*
     * First, scan the column's constraints to see if a not-null constraint
     * that we add must be prevented from being NO INHERIT.  This should be
     * enforced only for PRIMARY KEY, not IDENTITY or SERIAL.  However, if the
     * not-null constraint is specified as a table constraint rather than as a
     * column constraint, AddRelationNotNullConstraints would raise an error
     * if a NO INHERIT mismatch is found.  To avoid inconsistently disallowing
     * it in the table constraint case but not the column constraint case, we
     * disallow it here as well.  Maybe AddRelationNotNullConstraints can be
     * improved someday, so that it doesn't complain, and then we can remove
     * the restriction for SERIAL and IDENTITY here as well.
     */
    if !disallow_noinherit_notnull {
        foreach!(lc, (*column).constraints, {
            let constraint: *mut Constraint = lfirst(current_cell!(lc)) as *mut Constraint;
            match (*constraint).contype {
                CONSTR_IDENTITY | CONSTR_PRIMARY => {
                    disallow_noinherit_notnull = true;
                }
                _ => {}
            }
        });
    }

    /* Now scan them again to do full processing */
    saw_nullable = false;
    saw_default = false;
    saw_identity = false;
    saw_generated = false;

    foreach!(lc, (*column).constraints, {
        let constraint: *mut Constraint = lfirst(current_cell!(lc)) as *mut Constraint;

        match (*constraint).contype {
            CONSTR_NULL => {
                if (saw_nullable && (*column).is_not_null) || need_notnull {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                         *         parser_errposition(cxt->pstate, constraint->location) */
                    );
                }
                (*column).is_not_null = false;
                saw_nullable = true;
            }
            CONSTR_NOTNULL => {
                if (*cxt).ispartitioned && (*constraint).is_no_inherit {
                    ereport!(
                        ERROR,
                        errmsg!("not-null constraints on partitioned tables cannot be NO INHERIT")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }

                /* Disallow conflicting [NOT] NULL markings */
                if saw_nullable && !(*column).is_not_null {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                         *         parser_errposition */
                    );
                }

                if disallow_noinherit_notnull && (*constraint).is_no_inherit {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "conflicting NO INHERIT declarations for not-null constraints on column \"{}\"",
                            cstr_fmt!((*column).colname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                    );
                }

                /*
                 * If this is the first time we see this column being marked
                 * not-null, add the constraint entry and keep track of it.
                 * Also, remove previous markings that we need one.
                 *
                 * If this is a redundant not-null specification, just check
                 * that it doesn't conflict with what was specified earlier.
                 *
                 * Any conflicts with table constraints will be further
                 * checked in AddRelationNotNullConstraints().
                 */
                if !(*column).is_not_null {
                    (*column).is_not_null = true;
                    saw_nullable = true;
                    need_notnull = false;

                    (*constraint).keys =
                        list_make1!(makeString((*column).colname) as *mut c_void);
                    notnull_constraint = constraint;
                    (*cxt).nnconstraints =
                        lappend((*cxt).nnconstraints, constraint as *mut c_void);
                } else if !notnull_constraint.is_null() {
                    if !(*constraint).conname.is_null()
                        && !(*notnull_constraint).conname.is_null()
                        && strcmp(
                            (*notnull_constraint).conname,
                            (*constraint).conname,
                        ) != 0
                    {
                        elog!(
                            ERROR,
                            "conflicting not-null constraint names \"{}\" and \"{}\"",
                            cstr_fmt!((*notnull_constraint).conname),
                            cstr_fmt!((*constraint).conname)
                        );
                    }

                    if (*notnull_constraint).is_no_inherit != (*constraint).is_no_inherit {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "conflicting NO INHERIT declarations for not-null constraints on column \"{}\"",
                                cstr_fmt!((*column).colname)
                            )
                            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                        );
                    }

                    if (*notnull_constraint).conname.is_null()
                        && !(*constraint).conname.is_null()
                    {
                        (*notnull_constraint).conname = (*constraint).conname;
                    }
                }
            }
            CONSTR_DEFAULT => {
                if saw_default {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "multiple default values specified for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                         *         parser_errposition */
                    );
                }
                (*column).raw_default = (*constraint).raw_expr;
                /* constraint->cooked_expr == NULL */
                saw_default = true;
            }
            CONSTR_IDENTITY => {
                let ctype_tup: crate::access::htup_details::HeapTuple;
                let typeOid: Oid;

                if (*cxt).ofType {
                    ereport!(
                        ERROR,
                        errmsg!("identity columns are not supported on typed tables")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
                if !(*cxt).partbound.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("identity columns are not supported on partitions")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }

                ctype_tup = typenameType((*cxt).pstate, (*column).typeName, core::ptr::null_mut());
                typeOid = (*(GETSTRUCT(ctype_tup) as *mut FormData_pg_type)).oid;
                ReleaseSysCache(ctype_tup);

                if saw_identity {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "multiple identity specifications for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }

                generateSerialExtraStmts(
                    cxt,
                    column,
                    typeOid,
                    (*constraint).options,
                    true,
                    false,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );

                (*column).identity = (*constraint).generated_when;
                saw_identity = true;

                /*
                 * Identity columns are always NOT NULL, but we may have a
                 * constraint already.
                 */
                if !saw_nullable {
                    need_notnull = true;
                } else if !(*column).is_not_null {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
            }
            CONSTR_GENERATED => {
                if (*cxt).ofType {
                    ereport!(
                        ERROR,
                        errmsg!("generated columns are not supported on typed tables")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
                if saw_generated {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "multiple generation clauses specified for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                (*column).generated = (*constraint).generated_kind;
                (*column).raw_default = (*constraint).raw_expr;
                /* constraint->cooked_expr == NULL */
                saw_generated = true;
            }
            CONSTR_CHECK => {
                (*cxt).ckconstraints = lappend((*cxt).ckconstraints, constraint as *mut c_void);
            }
            CONSTR_PRIMARY => {
                if saw_nullable && !(*column).is_not_null {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            cstr_fmt!((*column).colname),
                            cstr_fmt!((*(*cxt).relation).relname)
                        )
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                need_notnull = true;

                if (*cxt).isforeign {
                    ereport!(
                        ERROR,
                        errmsg!("primary key constraints are not supported on foreign tables")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                    );
                }
                /* FALL THRU */
                if (*constraint).keys.is_null() || list_length((*constraint).keys) == 0 {
                    (*constraint).keys =
                        list_make1!(makeString((*column).colname) as *mut c_void);
                }
                (*cxt).ixconstraints = lappend((*cxt).ixconstraints, constraint as *mut c_void);
            }
            CONSTR_UNIQUE => {
                if (*cxt).isforeign {
                    ereport!(
                        ERROR,
                        errmsg!("unique constraints are not supported on foreign tables")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                    );
                }
                if (*constraint).keys.is_null() || list_length((*constraint).keys) == 0 {
                    (*constraint).keys =
                        list_make1!(makeString((*column).colname) as *mut c_void);
                }
                (*cxt).ixconstraints = lappend((*cxt).ixconstraints, constraint as *mut c_void);
            }
            CONSTR_EXCLUSION => {
                /* grammar does not allow EXCLUDE as a column constraint */
                elog!(ERROR, "column exclusion constraints are not supported");
            }
            CONSTR_FOREIGN => {
                if (*cxt).isforeign {
                    ereport!(
                        ERROR,
                        errmsg!("foreign key constraints are not supported on foreign tables")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                    );
                }

                /*
                 * Fill in the current attribute's name and throw it into the
                 * list of FK constraints to be processed later.
                 */
                (*constraint).fk_attrs =
                    list_make1!(makeString((*column).colname) as *mut c_void);
                (*cxt).fkconstraints = lappend((*cxt).fkconstraints, constraint as *mut c_void);
            }
            CONSTR_ATTR_DEFERRABLE
            | CONSTR_ATTR_NOT_DEFERRABLE
            | CONSTR_ATTR_DEFERRED
            | CONSTR_ATTR_IMMEDIATE
            | CONSTR_ATTR_ENFORCED
            | CONSTR_ATTR_NOT_ENFORCED => {
                /* transformConstraintAttrs took care of these */
            }
            #[allow(unreachable_patterns)]
            _ => {
                elog!(
                    ERROR,
                    "unrecognized constraint type: {}",
                    (*constraint).contype as i32
                );
            }
        }

        if saw_default && saw_identity {
            ereport!(
                ERROR,
                errmsg!(
                    "both default and identity specified for column \"{}\" of table \"{}\"",
                    cstr_fmt!((*column).colname),
                    cstr_fmt!((*(*cxt).relation).relname)
                )
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            );
        }

        if saw_default && saw_generated {
            ereport!(
                ERROR,
                errmsg!(
                    "both default and generation expression specified for column \"{}\" of table \"{}\"",
                    cstr_fmt!((*column).colname),
                    cstr_fmt!((*(*cxt).relation).relname)
                )
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            );
        }

        if saw_identity && saw_generated {
            ereport!(
                ERROR,
                errmsg!(
                    "both identity and generation expression specified for column \"{}\" of table \"{}\"",
                    cstr_fmt!((*column).colname),
                    cstr_fmt!((*(*cxt).relation).relname)
                )
                /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
            );
        }
    }); /* end foreach constraints */

    /*
     * If we need a not-null constraint for PRIMARY KEY, SERIAL or IDENTITY,
     * and one was not explicitly specified, add one now.
     */
    if need_notnull && !(saw_nullable && (*column).is_not_null) {
        (*column).is_not_null = true;
        notnull_constraint =
            makeNotNullConstraint(makeString((*column).colname));
        (*cxt).nnconstraints =
            lappend((*cxt).nnconstraints, notnull_constraint as *mut c_void);
    }

    /*
     * If needed, generate ALTER FOREIGN TABLE ALTER COLUMN statement to add
     * per-column foreign data wrapper options to this column after creation.
     */
    if !(*column).fdwoptions.is_null() && list_length((*column).fdwoptions) > 0 {
        let stmt2: *mut AlterTableStmt;
        let cmd: *mut AlterTableCmd;

        cmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        (*cmd).subtype = AT_AlterColumnGenericOptions;
        (*cmd).name = (*column).colname;
        (*cmd).def = (*column).fdwoptions as *mut Node;
        (*cmd).behavior = DROP_RESTRICT;
        (*cmd).missing_ok = false;

        stmt2 = makeNode!(AlterTableStmt, T_AlterTableStmt);
        (*stmt2).relation = (*cxt).relation;
        (*stmt2).cmds = NIL;
        (*stmt2).objtype = OBJECT_FOREIGN_TABLE;
        (*stmt2).cmds = lappend((*stmt2).cmds, cmd as *mut c_void);

        (*cxt).alist = lappend((*cxt).alist, stmt2 as *mut c_void);
    }
}

// FormData_pg_type stub
#[repr(C)]
struct FormData_pg_type {
    pub oid: Oid,
    pub typname: [i8; 64],
    pub typnamespace: Oid,
    pub typowner: Oid,
    pub typlen: i16,
    pub typcollation: Oid,
    /* ... remaining fields omitted */
}

// DROP_RESTRICT
// DROP_RESTRICT imported from DropBehavior::*

/*
 * transformTableConstraint
 *        transform a Constraint node within CREATE TABLE or ALTER TABLE
 */
unsafe fn transformTableConstraint(cxt: *mut CreateStmtContext, constraint: *mut Constraint) {
    match (*constraint).contype {
        CONSTR_PRIMARY => {
            if (*cxt).isforeign {
                ereport!(
                    ERROR,
                    errmsg!("primary key constraints are not supported on foreign tables")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                );
            }
            (*cxt).ixconstraints = lappend((*cxt).ixconstraints, constraint as *mut c_void);
        }
        CONSTR_UNIQUE => {
            if (*cxt).isforeign {
                ereport!(
                    ERROR,
                    errmsg!("unique constraints are not supported on foreign tables")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                );
            }
            (*cxt).ixconstraints = lappend((*cxt).ixconstraints, constraint as *mut c_void);
        }
        CONSTR_EXCLUSION => {
            if (*cxt).isforeign {
                ereport!(
                    ERROR,
                    errmsg!("exclusion constraints are not supported on foreign tables")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                );
            }
            (*cxt).ixconstraints = lappend((*cxt).ixconstraints, constraint as *mut c_void);
        }
        CONSTR_CHECK => {
            (*cxt).ckconstraints = lappend((*cxt).ckconstraints, constraint as *mut c_void);
        }
        CONSTR_NOTNULL => {
            if (*cxt).ispartitioned && (*constraint).is_no_inherit {
                ereport!(
                    ERROR,
                    errmsg!("not-null constraints on partitioned tables cannot be NO INHERIT")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }
            (*cxt).nnconstraints = lappend((*cxt).nnconstraints, constraint as *mut c_void);
        }
        CONSTR_FOREIGN => {
            if (*cxt).isforeign {
                ereport!(
                    ERROR,
                    errmsg!("foreign key constraints are not supported on foreign tables")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                );
            }
            (*cxt).fkconstraints = lappend((*cxt).fkconstraints, constraint as *mut c_void);
        }
        CONSTR_NULL
        | CONSTR_DEFAULT
        | CONSTR_ATTR_DEFERRABLE
        | CONSTR_ATTR_NOT_DEFERRABLE
        | CONSTR_ATTR_DEFERRED
        | CONSTR_ATTR_IMMEDIATE
        | CONSTR_ATTR_ENFORCED
        | CONSTR_ATTR_NOT_ENFORCED => {
            elog!(
                ERROR,
                "invalid context for constraint type {}",
                (*constraint).contype as i32
            );
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized constraint type: {}",
                (*constraint).contype as i32
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Part 4: transformTableLikeClause + expandTableLikeClause + transformOfType
//         + generateClonedIndexStmt (thin wrapper) + generateClonedExtStatsStmt
// ---------------------------------------------------------------------------

/*
 * transformTableLikeClause
 *
 * Change the LIKE <srctable> portion of a CREATE TABLE statement into
 * column definitions that recreate the user defined column portions of
 * <srctable>.  Also, if there are any LIKE options that we can't fully
 * process at this point, add the TableLikeClause to cxt->likeclauses, which
 * will cause utility.c to call expandTableLikeClause() after the new
 * table has been created.
 *
 * Some options are ignored.  For example, as foreign tables have no storage,
 * these INCLUDING options have no effect: STORAGE, COMPRESSION, IDENTITY
 * and INDEXES.  Similarly, INCLUDING INDEXES is ignored from a view.
 */
unsafe fn transformTableLikeClause(
    cxt: *mut CreateStmtContext,
    table_like_clause: *mut TableLikeClause,
) {
    let mut parent_attno: AttrNumber;
    let mut relation: Relation;
    let mut tupleDesc: TupleDesc;
    let mut aclresult: AclResult;
    let mut comment: *mut c_char;
    let mut pcbstate: ParseCallbackState = core::mem::zeroed();

    setup_parser_errposition_callback(
        &mut pcbstate,
        (*cxt).pstate,
        (*(*table_like_clause).relation).location,
    );

    /* Open the relation referenced by the LIKE clause */
    relation = relation_openrv((*table_like_clause).relation, AccessShareLock);

    if (*(*relation).rd_rel).relkind != RELKIND_RELATION as i8
        && (*(*relation).rd_rel).relkind != RELKIND_VIEW as i8
        && (*(*relation).rd_rel).relkind != RELKIND_MATVIEW as i8
        && (*(*relation).rd_rel).relkind != RELKIND_COMPOSITE_TYPE as i8
        && (*(*relation).rd_rel).relkind != RELKIND_FOREIGN_TABLE as i8
        && (*(*relation).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
    {
        ereport!(
            ERROR,
            errmsg!(
                "relation \"{}\" is invalid in LIKE clause",
                cstr_fmt!(RelationGetRelationName(relation))
            )
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
             *         errdetail_relkind_not_supported(relation->rd_rel->relkind) */
        );
    }

    cancel_parser_errposition_callback(&mut pcbstate);

    /* Check for privileges */
    if (*(*relation).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as i8 {
        aclresult = object_aclcheck(
            TypeRelationId,
            (*(*relation).rd_rel).reltype,
            GetUserId(),
            ACL_USAGE,
        );
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TYPE, RelationGetRelationName(relation));
        }
    } else {
        aclresult = pg_class_aclcheck(RelationGetRelid(relation), GetUserId(), ACL_SELECT);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                get_relkind_objtype((*(*relation).rd_rel).relkind as u8),
                RelationGetRelationName(relation),
            );
        }
    }

    tupleDesc = RelationGetDescr(relation);

    /*
     * Insert the copied attributes into the cxt for the new table definition.
     * We must do this now so that they appear in the table in the relative
     * position where the LIKE clause is, as required by SQL99.
     */
    parent_attno = 1;
    while (parent_attno as c_int) <= (*tupleDesc).natts {
        let attribute = TupleDescAttr(tupleDesc, (parent_attno - 1) as c_int)
            as *mut FormData_pg_attribute;
        let def: *mut ColumnDef;

        /* Ignore dropped columns in the parent. */
        if (*attribute).attisdropped {
            parent_attno += 1;
            continue;
        }

        /* Create a new column definition */
        def = makeColumnDef(
            NameStr((*attribute).attname),
            (*attribute).atttypid,
            (*attribute).atttypmod,
            (*attribute).attcollation,
        );

        /* Add to column list */
        (*cxt).columns = lappend((*cxt).columns, def as *mut c_void);

        /*
         * Although we don't transfer the column's default/generation
         * expression now, we need to mark it GENERATED if appropriate.
         */
        if (*attribute).atthasdef
            && (*attribute).attgenerated != 0
            && ((*table_like_clause).options & CREATE_TABLE_LIKE_GENERATED) != 0
        {
            (*def).generated = (*attribute).attgenerated;
        }

        /* Copy identity if requested */
        if (*attribute).attidentity != 0
            && ((*table_like_clause).options & CREATE_TABLE_LIKE_IDENTITY) != 0
            && !(*cxt).isforeign
        {
            let seq_relid: Oid;
            let seq_options: *mut List;

            /*
             * find sequence owned by old column; extract sequence parameters;
             * build new create sequence command
             */
            seq_relid = getIdentitySequence(relation, (*attribute).attnum, false);
            seq_options = sequence_options(seq_relid);
            generateSerialExtraStmts(
                cxt,
                def,
                InvalidOid,
                seq_options,
                true,
                false,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );
            (*def).identity = (*attribute).attidentity;
        }

        /* Likewise, copy storage if requested */
        if ((*table_like_clause).options & CREATE_TABLE_LIKE_STORAGE) != 0
            && !(*cxt).isforeign
        {
            (*def).storage = (*attribute).attstorage;
        } else {
            (*def).storage = 0;
        }

        /* Likewise, copy compression if requested */
        if ((*table_like_clause).options & CREATE_TABLE_LIKE_COMPRESSION) != 0
            && CompressionMethodIsValid((*attribute).attcompression)
            && !(*cxt).isforeign
        {
            (*def).compression = pstrdup(GetCompressionMethodName((*attribute).attcompression));
        } else {
            (*def).compression = core::ptr::null_mut();
        }

        /* Likewise, copy comment if requested */
        if ((*table_like_clause).options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
            comment = GetComment(
                (*attribute).attrelid,
                RelationRelationId,
                (*attribute).attnum as c_int,
            );
            if !comment.is_null() {
                let stmt: *mut CommentStmt = makeNode!(CommentStmt, T_CommentStmt);
                (*stmt).objtype = OBJECT_COLUMN;
                (*stmt).object = list_make3!(
                    makeString((*(*cxt).relation).schemaname) as *mut c_void,
                    makeString((*(*cxt).relation).relname) as *mut c_void,
                    makeString((*def).colname) as *mut c_void
                ) as *mut Node;
                (*stmt).comment = comment;
                (*cxt).alist = lappend((*cxt).alist, stmt as *mut c_void);
            }
        }

        parent_attno += 1;
    }

    /*
     * Reproduce not-null constraints, if any, by copying them.  We do this
     * regardless of options given.
     */
    if !(*tupleDesc).constr.is_null()
        && (*((*tupleDesc).constr as *mut TupleConstrStub)).has_not_null
    {
        let lst: *mut List;

        lst = RelationGetNotNullConstraints(RelationGetRelid(relation), false, true);
        (*cxt).nnconstraints = list_concat((*cxt).nnconstraints, lst);

        /* Copy comments on not-null constraints */
        if ((*table_like_clause).options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
            foreach!(lc, lst, {
                let nnconstr: *mut Constraint = lfirst(current_cell!(lc)) as *mut Constraint;

                comment = GetComment(
                    get_relation_constraint_oid(
                        RelationGetRelid(relation),
                        (*nnconstr).conname,
                        false,
                    ),
                    ConstraintRelationId,
                    0,
                );
                if !comment.is_null() {
                    let stmt: *mut CommentStmt = makeNode!(CommentStmt, T_CommentStmt);
                    (*stmt).objtype = OBJECT_TABCONSTRAINT;
                    (*stmt).object = list_make3!(
                        makeString((*(*cxt).relation).schemaname) as *mut c_void,
                        makeString((*(*cxt).relation).relname) as *mut c_void,
                        makeString((*nnconstr).conname) as *mut c_void
                    ) as *mut Node;
                    (*stmt).comment = comment;
                    (*cxt).alist = lappend((*cxt).alist, stmt as *mut c_void);
                }
            });
        }
    }

    /*
     * We cannot yet deal with defaults, CHECK constraints, indexes, or
     * statistics, since we don't yet know what column numbers the copied
     * columns will have in the finished table.  If any of those options are
     * specified, add the LIKE clause to cxt->likeclauses so that
     * expandTableLikeClause will be called after we do know that.
     *
     * In order for this to work, we remember the relation OID so that
     * expandTableLikeClause is certain to open the same table.
     */
    if ((*table_like_clause).options
        & (CREATE_TABLE_LIKE_DEFAULTS
            | CREATE_TABLE_LIKE_GENERATED
            | CREATE_TABLE_LIKE_CONSTRAINTS
            | CREATE_TABLE_LIKE_INDEXES
            | CREATE_TABLE_LIKE_STATISTICS))
        != 0
    {
        (*table_like_clause).relationOid = RelationGetRelid(relation);
        (*cxt).likeclauses = lappend((*cxt).likeclauses, table_like_clause as *mut c_void);
    }

    /*
     * Close the parent rel, but keep our AccessShareLock on it until xact
     * commit.  That will prevent someone else from deleting or ALTERing the
     * parent before we can run expandTableLikeClause.
     */
    table_close(relation, NoLock);
}

// Opaque TupleConstr stub
#[repr(C)]
struct TupleConstrStub {
    pub has_not_null: bool,
    pub num_check: c_int,
    pub check: *mut ConstrCheck,
}

#[repr(C)]
struct ConstrCheck {
    pub ccname: *mut c_char,
    pub ccbin: *mut c_char,
    pub ccenforced: bool,
    pub ccnoinherit: bool,
}

// FormData_pg_attribute stub
#[repr(C)]
struct FormData_pg_attribute {
    pub attrelid: Oid,
    pub attname: [i8; 64],
    pub atttypid: Oid,
    pub attstattarget: i32,
    pub attlen: i16,
    pub attnum: AttrNumber,
    pub attndims: i32,
    pub attcacheoff: i32,
    pub atttypmod: int32,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcompression: u8,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: i8,
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i32,
    pub attcollation: Oid,
}

unsafe fn get_relkind_objtype(relkind: u8) -> ObjectType {
    /* stub: returns generic object type */
    OBJECT_TABLE
}

/*
 * expandTableLikeClause
 *
 * Process LIKE options that require knowing the final column numbers
 * assigned to the new table's columns.  This executes after we have
 * run DefineRelation for the new table.  It returns a list of utility
 * commands that should be run to generate indexes etc.
 */
pub unsafe fn expandTableLikeClause(
    heapRel: *mut RangeVar,
    table_like_clause: *mut TableLikeClause,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut atsubcmds: *mut List = NIL;
    let mut parent_attno: AttrNumber;
    let mut relation: Relation;
    let mut childrel: Relation;
    let mut tupleDesc: TupleDesc;
    let mut constr: *mut TupleConstrStub;
    let mut attmap: *mut AttrMap;
    let mut comment: *mut c_char;

    /*
     * Open the relation referenced by the LIKE clause.  We should still have
     * the table lock obtained by transformTableLikeClause (and this'll throw
     * an assertion failure if not).  Hence, no need to recheck privileges
     * etc.  We must open the rel by OID not name, to be sure we get the same
     * table.
     */
    if !OidIsValid((*table_like_clause).relationOid) {
        elog!(ERROR, "expandTableLikeClause called on untransformed LIKE clause");
    }

    relation = relation_open((*table_like_clause).relationOid, NoLock);

    tupleDesc = RelationGetDescr(relation);
    constr = (*tupleDesc).constr as *mut TupleConstrStub;

    /* Open the newly-created child relation; we have lock on that too. */
    childrel = relation_openrv(heapRel, NoLock);

    /*
     * Construct a map from the LIKE relation's attnos to the child rel's.
     * This re-checks type match etc, although it shouldn't be possible to
     * have a failure since both tables are locked.
     */
    attmap = build_attrmap_by_name(RelationGetDescr(childrel), tupleDesc, false);

    /* Process defaults, if required. */
    if ((*table_like_clause).options
        & (CREATE_TABLE_LIKE_DEFAULTS | CREATE_TABLE_LIKE_GENERATED))
        != 0
        && !constr.is_null()
    {
        parent_attno = 1;
        while (parent_attno as c_int) <= (*tupleDesc).natts {
            let attribute = TupleDescAttr(tupleDesc, (parent_attno - 1) as c_int)
                as *mut FormData_pg_attribute;

            /* Ignore dropped columns in the parent. */
            if (*attribute).attisdropped {
                parent_attno += 1;
                continue;
            }

            /*
             * Copy default, if present and it should be copied.  We have
             * separate options for plain default expressions and GENERATED
             * defaults.
             */
            if (*attribute).atthasdef
                && (if (*attribute).attgenerated != 0 {
                    ((*table_like_clause).options & CREATE_TABLE_LIKE_GENERATED) != 0
                } else {
                    ((*table_like_clause).options & CREATE_TABLE_LIKE_DEFAULTS) != 0
                })
            {
                let this_default: *mut Node;
                let atsubcmd: *mut AlterTableCmd;
                let mut found_whole_row: bool = false;

                this_default = TupleDescGetDefault(tupleDesc, parent_attno);
                if this_default.is_null() {
                    elog!(
                        ERROR,
                        "default expression not found for attribute {} of relation \"{}\"",
                        parent_attno as c_int,
                        cstr_fmt!(RelationGetRelationName(relation))
                    );
                }

                atsubcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
                (*atsubcmd).subtype = AT_CookedColumnDefault;
                (*atsubcmd).num = get_attmap_num(attmap, (parent_attno - 1) as usize);
                (*atsubcmd).def = map_variable_attnos(
                    this_default,
                    1,
                    0,
                    attmap,
                    InvalidOid,
                    &mut found_whole_row,
                );

                /*
                 * Prevent this for the same reason as for constraints below.
                 * Note that defaults cannot contain any vars, so it's OK that
                 * the error message refers to generated columns.
                 */
                if found_whole_row {
                    ereport!(
                        ERROR,
                        errmsg!("cannot convert whole-row table reference")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         *         errdetail("Generation expression for column...") */
                    );
                }

                atsubcmds = lappend(atsubcmds, atsubcmd as *mut c_void);
            }

            parent_attno += 1;
        }
    }

    /*
     * Copy CHECK constraints if requested, being careful to adjust attribute
     * numbers so they match the child.
     */
    if ((*table_like_clause).options & CREATE_TABLE_LIKE_CONSTRAINTS) != 0
        && !constr.is_null()
    {
        let mut ccnum: c_int = 0;
        while ccnum < (*constr).num_check {
            let ccname: *mut c_char = (*(*constr).check.offset(ccnum as isize)).ccname;
            let ccbin: *mut c_char = (*(*constr).check.offset(ccnum as isize)).ccbin;
            let ccenforced: bool = (*(*constr).check.offset(ccnum as isize)).ccenforced;
            let ccnoinherit: bool = (*(*constr).check.offset(ccnum as isize)).ccnoinherit;
            let mut found_whole_row: bool = false;

            let ccbin_node: *mut Node = map_variable_attnos(
                stringToNode(ccbin) as *mut Node,
                1,
                0,
                attmap,
                InvalidOid,
                &mut found_whole_row,
            );

            /*
             * We reject whole-row variables because the whole point of LIKE
             * is that the new table's rowtype might later diverge from the
             * parent's.  So, while translation might be possible right now,
             * it wouldn't be possible to guarantee it would work in future.
             */
            if found_whole_row {
                ereport!(
                    ERROR,
                    errmsg!("cannot convert whole-row table reference")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     *         errdetail("Constraint \"{}\" contains a whole-row reference...") */
                );
            }

            let n: *mut Constraint = makeNode!(Constraint, T_Constraint);
            (*n).contype = CONSTR_CHECK;
            (*n).conname = pstrdup(ccname);
            (*n).location = -1;
            (*n).is_enforced = ccenforced;
            (*n).initially_valid = ccenforced; /* sic */
            (*n).is_no_inherit = ccnoinherit;
            (*n).raw_expr = core::ptr::null_mut();
            (*n).cooked_expr = nodeToString(ccbin_node as *const c_void);

            /* We can skip validation, since the new table should be empty. */
            (*n).skip_validation = true;

            let atsubcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
            (*atsubcmd).subtype = AT_AddConstraint;
            (*atsubcmd).def = n as *mut Node;
            atsubcmds = lappend(atsubcmds, atsubcmd as *mut c_void);

            /* Copy comment on constraint */
            if ((*table_like_clause).options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
                comment = GetComment(
                    get_relation_constraint_oid(
                        RelationGetRelid(relation),
                        (*n).conname,
                        false,
                    ),
                    ConstraintRelationId,
                    0,
                );
                if !comment.is_null() {
                    let stmt: *mut CommentStmt = makeNode!(CommentStmt, T_CommentStmt);
                    (*stmt).objtype = OBJECT_TABCONSTRAINT;
                    (*stmt).object = list_make3!(
                        makeString((*heapRel).schemaname) as *mut c_void,
                        makeString((*heapRel).relname) as *mut c_void,
                        makeString((*n).conname) as *mut c_void
                    ) as *mut Node;
                    (*stmt).comment = comment;
                    result = lappend(result, stmt as *mut c_void);
                }
            }

            ccnum += 1;
        }
    }

    /*
     * If we generated any ALTER TABLE actions above, wrap them into a single
     * ALTER TABLE command.  Stick it at the front of the result, so it runs
     * before any CommentStmts we made above.
     */
    if !atsubcmds.is_null() && list_length(atsubcmds) > 0 {
        let atcmd: *mut AlterTableStmt = makeNode!(AlterTableStmt, T_AlterTableStmt);
        (*atcmd).relation = copyObject(heapRel as *const c_void) as *mut RangeVar;
        (*atcmd).cmds = atsubcmds;
        (*atcmd).objtype = OBJECT_TABLE;
        (*atcmd).missing_ok = false;
        result = lcons(atcmd as *mut c_void, result);
    }

    /* Process indexes if required. */
    if ((*table_like_clause).options & CREATE_TABLE_LIKE_INDEXES) != 0
        && (*(*relation).rd_rel).relhasindex
        && (*(*childrel).rd_rel).relkind != RELKIND_FOREIGN_TABLE as i8
    {
        let parent_indexes: *mut List = RelationGetIndexList(relation);

        foreach!(l, parent_indexes, {
            let parent_index_oid: Oid = lfirst_oid(current_cell!(l));
            let parent_index: Relation;
            let index_stmt: *mut IndexStmt;

            parent_index = index_open(parent_index_oid, AccessShareLock);

            /* Build CREATE INDEX statement to recreate the parent_index */
            index_stmt = generateClonedIndexStmt(heapRel, parent_index, attmap, core::ptr::null_mut());

            /* Copy comment on index, if requested */
            if ((*table_like_clause).options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
                comment = GetComment(parent_index_oid, RelationRelationId, 0);
                /*
                 * We make use of IndexStmt's idxcomment option, so as not to
                 * need to know now what name the index will have.
                 */
                (*index_stmt).idxcomment = comment;
            }

            result = lappend(result, index_stmt as *mut c_void);
            index_close(parent_index, AccessShareLock);
        });
    }

    /* Process extended statistics if required. */
    if ((*table_like_clause).options & CREATE_TABLE_LIKE_STATISTICS) != 0 {
        let parent_extstats: *mut List = RelationGetStatExtList(relation);

        foreach!(l, parent_extstats, {
            let parent_stat_oid: Oid = lfirst_oid(current_cell!(l));
            let stats_stmt: *mut CreateStatsStmt;

            stats_stmt = generateClonedExtStatsStmt(
                heapRel,
                RelationGetRelid(childrel),
                parent_stat_oid,
                attmap,
            );

            /* Copy comment on statistics object, if requested */
            if ((*table_like_clause).options & CREATE_TABLE_LIKE_COMMENTS) != 0 {
                comment = GetComment(parent_stat_oid, StatisticExtRelationId, 0);
                /*
                 * We make use of CreateStatsStmt's stxcomment option, so as
                 * not to need to know now what name the statistics will have.
                 */
                (*stats_stmt).stxcomment = comment;
            }

            result = lappend(result, stats_stmt as *mut c_void);
        });

        list_free(parent_extstats);
    }

    /* Done with child rel */
    table_close(childrel, NoLock);

    /*
     * Close the parent rel, but keep our AccessShareLock on it until xact
     * commit.  That will prevent someone else from deleting or ALTERing the
     * parent before the child is committed.
     */
    table_close(relation, NoLock);

    result
}

/// Helper to read attmap->attnums[idx]  -- TODO(pg-port): AttrMap not yet ported
unsafe fn get_attmap_num(attmap: *mut AttrMap, idx: usize) -> AttrNumber {
    /* stub; actual AttrMap layout TBD */
    0
}

unsafe fn transformOfType(cxt: *mut CreateStmtContext, ofTypename: *mut TypeName) {
    let tuple: crate::access::htup_details::HeapTuple;
    let tupdesc: TupleDesc;
    let ofTypeId: Oid;

    debug_assert!(!ofTypename.is_null());

    tuple = typenameType((*cxt).pstate, ofTypename, core::ptr::null_mut());
    check_of_type(tuple);
    ofTypeId = (*(GETSTRUCT(tuple) as *mut FormData_pg_type)).oid;
    (*ofTypename).typeOid = ofTypeId; /* cached for later */

    tupdesc = lookup_rowtype_tupdesc(ofTypeId, -1);
    let mut i: c_int = 0;
    while i < (*tupdesc).natts {
        let attr = TupleDescAttr(tupdesc, i) as *mut FormData_pg_attribute;
        let n: *mut ColumnDef;

        if (*attr).attisdropped {
            i += 1;
            continue;
        }

        n = makeColumnDef(
            NameStr((*attr).attname),
            (*attr).atttypid,
            (*attr).atttypmod,
            (*attr).attcollation,
        );
        (*n).is_from_type = true;

        (*cxt).columns = lappend((*cxt).columns, n as *mut c_void);
        i += 1;
    }
    ReleaseTupleDesc(tupdesc);
    ReleaseSysCache(tuple);
}

/*
 * Generate a CreateStatsStmt node using information from an already existing
 * extended statistic "source_statsid", for the rel identified by heapRel and
 * heapRelid.
 *
 * Attribute numbers in expression Vars are adjusted according to attmap.
 */
unsafe fn generateClonedExtStatsStmt(
    heapRel: *mut RangeVar,
    heapRelid: Oid,
    source_statsid: Oid,
    attmap: *const AttrMap,
) -> *mut CreateStatsStmt {
    let ht_stats: crate::access::htup_details::HeapTuple;
    let statsrec: *mut FormData_pg_statistic_ext;
    let stats: *mut CreateStatsStmt;
    let mut stat_types: *mut List = NIL;
    let mut def_names: *mut List = NIL;
    let mut isnull: bool = false;
    let datum: Datum;
    let arr: *mut c_void;
    let enabled: *mut u8;

    debug_assert!(OidIsValid(heapRelid));
    debug_assert!(!heapRel.is_null());

    /* Fetch pg_statistic_ext tuple of source statistics object. */
    ht_stats = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(heapRelid));
    if !HeapTupleIsValid(ht_stats) {
        elog!(ERROR, "cache lookup failed for statistics object {}", source_statsid);
    }
    statsrec = GETSTRUCT(ht_stats) as *mut FormData_pg_statistic_ext;

    /* Determine which statistics types exist */
    let datum2 = SysCacheGetAttrNotNull(
        STATEXTOID,
        ht_stats as *mut c_void,
        Anum_pg_statistic_ext_stxkind,
    );
    arr = DatumGetArrayTypeP(datum2);
    if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID {
        elog!(ERROR, "stxkind is not a 1-D char array");
    }
    enabled = ARR_DATA_PTR(arr);
    let ndims = *ARR_DIMS(arr);
    let mut i: c_int = 0;
    while i < ndims {
        let kind: u8 = *enabled.offset(i as isize);
        if kind == STATS_EXT_NDISTINCT {
            stat_types = lappend(stat_types, makeString(b"ndistinct\0".as_ptr() as *mut c_char) as *mut c_void);
        } else if kind == STATS_EXT_DEPENDENCIES {
            stat_types = lappend(stat_types, makeString(b"dependencies\0".as_ptr() as *mut c_char) as *mut c_void);
        } else if kind == STATS_EXT_MCV {
            stat_types = lappend(stat_types, makeString(b"mcv\0".as_ptr() as *mut c_char) as *mut c_void);
        } else if kind == STATS_EXT_EXPRESSIONS {
            /* expression stats are not exposed to users */
        } else {
            elog!(ERROR, "unrecognized statistics kind {}", kind as c_int);
        }
        i += 1;
    }

    /* Determine which columns the statistics are on */
    let nkeys = (*statsrec).stxkeys_dim1;
    let mut i: c_int = 0;
    while i < nkeys {
        let selem: *mut StatsElem = makeNode!(StatsElem, T_StatsElem);
        let attnum: AttrNumber = *(*statsrec).stxkeys_values.offset(i as isize);
        (*selem).name = get_attname(heapRelid, attnum, false);
        (*selem).expr = core::ptr::null_mut();
        def_names = lappend(def_names, selem as *mut c_void);
        i += 1;
    }

    /*
     * Now handle expressions, if there are any. The order (with respect to
     * regular attributes) does not really matter for extended stats, so we
     * simply append them after simple column references.
     *
     * XXX Some places during build/estimation treat expressions as if they
     * are before attributes, but for the CREATE command that's entirely
     * irrelevant.
     */
    let datum3 = SysCacheGetAttr(
        STATEXTOID,
        ht_stats,
        Anum_pg_statistic_ext_stxexprs,
        &mut isnull,
    );

    if !isnull {
        let mut exprs: *mut List = NIL;
        let exprsString: *mut c_char = TextDatumGetCString(datum3);
        exprs = stringToNode(exprsString) as *mut List;

        foreach!(lc, exprs, {
            let expr: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
            let selem: *mut StatsElem = makeNode!(StatsElem, T_StatsElem);
            let mut found_whole_row: bool = false;

            /* Adjust Vars to match new table's column numbering */
            let expr = map_variable_attnos(
                expr,
                1,
                0,
                attmap,
                InvalidOid,
                &mut found_whole_row,
            );

            (*selem).name = core::ptr::null_mut();
            (*selem).expr = expr;
            def_names = lappend(def_names, selem as *mut c_void);
        });

        pfree(exprsString as *mut c_void);
    }

    /* finally, build the output node */
    stats = makeNode!(CreateStatsStmt, T_CreateStatsStmt);
    (*stats).defnames = core::ptr::null_mut();
    (*stats).stat_types = stat_types;
    (*stats).exprs = def_names;
    (*stats).relations = list_make1!(heapRel as *mut c_void);
    (*stats).stxcomment = core::ptr::null_mut();
    (*stats).transformed = true; /* don't need transformStatsStmt again */
    (*stats).if_not_exists = false;

    /* Clean up */
    ReleaseSysCache(ht_stats);

    stats
}

// Stub for pg_statistic_ext form
#[repr(C)]
struct FormData_pg_statistic_ext {
    pub stxrelid: Oid,
    pub stxname: [i8; 64],
    pub stxnamespace: Oid,
    pub stxowner: Oid,
    pub stxstattarget: i32,
    pub stxkeys_dim1: c_int,
    pub stxkeys_values: *mut AttrNumber,
}

// ---------------------------------------------------------------------------
// Part 5: get_collation + get_opclass + transformIndexConstraints
//         + transformIndexConstraint + transformCheckConstraints
//         + transformFKConstraints + transformIndexStmt + transformStatsStmt
//         + transformRuleStmt
// ---------------------------------------------------------------------------

/*
 * get_collation        - fetch qualified name of a collation
 *
 * If collation is InvalidOid or is the default for the given actual_datatype,
 * then the return value is NIL.
 */
unsafe fn get_collation(collation: Oid, actual_datatype: Oid) -> *mut List {
    let result: *mut List;
    let ht_coll: crate::access::htup_details::HeapTuple;
    let coll_rec: *mut FormData_pg_collation;
    let nsp_name: *mut c_char;
    let coll_name: *mut c_char;

    if !OidIsValid(collation) {
        return NIL; /* easy case */
    }
    if collation == get_typcollation(actual_datatype) {
        return NIL; /* just let it default */
    }

    ht_coll = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
    if !HeapTupleIsValid(ht_coll) {
        elog!(ERROR, "cache lookup failed for collation {}", collation);
    }
    coll_rec = GETSTRUCT(ht_coll) as *mut FormData_pg_collation;

    /* For simplicity, we always schema-qualify the name */
    nsp_name = get_namespace_name((*coll_rec).collnamespace);
    coll_name = pstrdup(NameStr((*coll_rec).collname));
    let result: *mut List = list_make2!(
        makeString(nsp_name) as *mut c_void,
        makeString(coll_name) as *mut c_void
    );

    ReleaseSysCache(ht_coll);
    result
}

// pg_collation form stub
#[repr(C)]
struct FormData_pg_collation {
    pub oid: Oid,
    pub collname: [i8; 64],
    pub collnamespace: Oid,
    pub collowner: Oid,
}

/*
 * get_opclass            - fetch qualified name of an index operator class
 *
 * If the opclass is the default for the given actual_datatype, then
 * the return value is NIL.
 */
unsafe fn get_opclass(opclass: Oid, actual_datatype: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let ht_opc: crate::access::htup_details::HeapTuple;
    let opc_rec: *mut FormData_pg_opclass;

    ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if !HeapTupleIsValid(ht_opc) {
        elog!(ERROR, "cache lookup failed for opclass {}", opclass);
    }
    opc_rec = GETSTRUCT(ht_opc) as *mut FormData_pg_opclass;

    if GetDefaultOpClass(actual_datatype, (*opc_rec).opcmethod) != opclass {
        /* For simplicity, we always schema-qualify the name */
        let nsp_name: *mut c_char = get_namespace_name((*opc_rec).opcnamespace);
        let opc_name: *mut c_char = pstrdup(NameStr((*opc_rec).opcname));
        result = list_make2!(
            makeString(nsp_name) as *mut c_void,
            makeString(opc_name) as *mut c_void
        );
    }

    ReleaseSysCache(ht_opc);
    result
}

// pg_opclass form stub
#[repr(C)]
struct FormData_pg_opclass {
    pub oid: Oid,
    pub opcmethod: Oid,
    pub opcname: [i8; 64],
    pub opcnamespace: Oid,
    pub opcowner: Oid,
}

/*
 * transformIndexConstraints
 *        Handle UNIQUE, PRIMARY KEY, EXCLUDE constraints, which create indexes.
 *        We also merge in any index definitions arising from
 *        LIKE ... INCLUDING INDEXES.
 */
unsafe fn transformIndexConstraints(cxt: *mut CreateStmtContext) {
    let mut index: *mut IndexStmt;
    let mut indexlist: *mut List = NIL;
    let mut finalindexlist: *mut List = NIL;

    /*
     * Run through the constraints that need to generate an index, and do so.
     *
     * For PRIMARY KEY, this queues not-null constraints for each column, if
     * needed.
     */
    foreach!(lc, (*cxt).ixconstraints, {
        let constraint: *mut Constraint = lfirst_node!(Constraint, T_Constraint, current_cell!(lc));

        debug_assert!(
            (*constraint).contype == CONSTR_PRIMARY
                || (*constraint).contype == CONSTR_UNIQUE
                || (*constraint).contype == CONSTR_EXCLUSION
        );

        index = transformIndexConstraint(constraint, cxt);
        indexlist = lappend(indexlist, index as *mut c_void);
    });

    /*
     * Scan the index list and remove any redundant index specifications. This
     * can happen if, for instance, the user writes UNIQUE PRIMARY KEY. A
     * strict reading of SQL would suggest raising an error instead, but that
     * strikes me as too anal-retentive. - tgl 2001-02-14
     *
     * XXX in ALTER TABLE case, it'd be nice to look for duplicate
     * pre-existing indexes, too.
     */
    if !(*cxt).pkey.is_null() {
        /* Make sure we keep the PKEY index in preference to others... */
        finalindexlist = list_make1!((*cxt).pkey as *mut c_void);
    }

    foreach!(lc, indexlist, {
        let mut keep: bool = true;

        index = lfirst(current_cell!(lc)) as *mut IndexStmt;

        /* if it's pkey, it's already in finalindexlist */
        if index == (*cxt).pkey {
            continue;
        }

        foreach!(k, finalindexlist, {
            let priorindex: *mut IndexStmt = lfirst(current_cell!(k)) as *mut IndexStmt;

            if equal((*index).indexParams as *const c_void, (*priorindex).indexParams as *const c_void)
                && equal((*index).indexIncludingParams as *const c_void, (*priorindex).indexIncludingParams as *const c_void)
                && equal((*index).whereClause as *const c_void, (*priorindex).whereClause as *const c_void)
                && equal((*index).excludeOpNames as *const c_void, (*priorindex).excludeOpNames as *const c_void)
                && strcmp((*index).accessMethod, (*priorindex).accessMethod) == 0
                && (*index).nulls_not_distinct == (*priorindex).nulls_not_distinct
                && (*index).deferrable == (*priorindex).deferrable
                && (*index).initdeferred == (*priorindex).initdeferred
            {
                (*priorindex).unique |= (*index).unique;

                /*
                 * If the prior index is as yet unnamed, and this one is
                 * named, then transfer the name to the prior index. This
                 * ensures that if we have named and unnamed constraints,
                 * we'll use (at least one of) the names for the index.
                 */
                if (*priorindex).idxname.is_null() {
                    (*priorindex).idxname = (*index).idxname;
                }
                keep = false;
                break;
            }
        });

        if keep {
            finalindexlist = lappend(finalindexlist, index as *mut c_void);
        }
    });

    /* Now append all the IndexStmts to cxt->alist. */
    (*cxt).alist = list_concat((*cxt).alist, finalindexlist);
}

/*
 * transformIndexConstraint
 *        Transform one UNIQUE, PRIMARY KEY, or EXCLUDE constraint for
 *        transformIndexConstraints. An IndexStmt is returned.
 *
 * For a PRIMARY KEY constraint, we additionally create not-null constraints
 * for columns that don't already have them.
 */
unsafe fn transformIndexConstraint(
    constraint: *mut Constraint,
    cxt: *mut CreateStmtContext,
) -> *mut IndexStmt {
    let mut index: *mut IndexStmt;

    index = makeNode!(IndexStmt, T_IndexStmt);

    (*index).unique = ((*constraint).contype != CONSTR_EXCLUSION);
    (*index).primary = ((*constraint).contype == CONSTR_PRIMARY);
    if (*index).primary {
        if !(*cxt).pkey.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "multiple primary keys for table \"{}\" are not allowed",
                    cstr_fmt!((*(*cxt).relation).relname)
                )
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 *         parser_errposition */
            );
        }
        (*cxt).pkey = index;
        /*
         * In ALTER TABLE case, a primary index might already exist, but
         * DefineIndex will check for it.
         */
    }
    (*index).nulls_not_distinct = (*constraint).nulls_not_distinct;
    (*index).isconstraint = true;
    (*index).iswithoutoverlaps = (*constraint).without_overlaps;
    (*index).deferrable = (*constraint).deferrable;
    (*index).initdeferred = (*constraint).initdeferred;

    if !(*constraint).conname.is_null() {
        (*index).idxname = pstrdup((*constraint).conname);
    } else {
        (*index).idxname = core::ptr::null_mut(); /* DefineIndex will choose name */
    }

    (*index).relation = (*cxt).relation;
    (*index).accessMethod = if !(*constraint).access_method.is_null() {
        (*constraint).access_method
    } else {
        DEFAULT_INDEX_TYPE.as_ptr() as *mut c_char
    };
    (*index).options = (*constraint).options;
    (*index).tableSpace = (*constraint).indexspace;
    (*index).whereClause = (*constraint).where_clause;
    (*index).indexParams = NIL;
    (*index).indexIncludingParams = NIL;
    (*index).excludeOpNames = NIL;
    (*index).idxcomment = core::ptr::null_mut();
    (*index).indexOid = InvalidOid;
    (*index).oldNumber = InvalidRelFileNumber;
    (*index).oldCreateSubid = InvalidSubTransactionId;
    (*index).oldFirstRelfilelocatorSubid = InvalidSubTransactionId;
    (*index).transformed = false;
    (*index).concurrent = false;
    (*index).if_not_exists = false;
    (*index).reset_default_tblspc = (*constraint).reset_default_tblspc;

    /*
     * If it's ALTER TABLE ADD CONSTRAINT USING INDEX, look up the index and
     * verify it's usable, then extract the implied column name list.  (We
     * will not actually need the column name list at runtime, but we need it
     * now to check for duplicate column entries below.)
     */
    if !(*constraint).indexname.is_null() {
        let index_name: *mut c_char = (*constraint).indexname;
        let heap_rel: Relation = (*cxt).rel;
        let mut index_oid: Oid;
        let index_rel: Relation;
        let index_form: *mut FormData_pg_index;
        let indclass: *mut oidvector;
        let indclassDatum: Datum;

        /* Grammar should not allow this with explicit column list */
        debug_assert!((*constraint).keys.is_null());

        /* Grammar should only allow PRIMARY and UNIQUE constraints */
        debug_assert!(
            (*constraint).contype == CONSTR_PRIMARY
                || (*constraint).contype == CONSTR_UNIQUE
        );

        /* Must be ALTER, not CREATE, but grammar doesn't enforce that */
        if !(*cxt).isalter {
            ereport!(
                ERROR,
                errmsg!("cannot use an existing index in CREATE TABLE")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            );
        }

        /* Look for the index in the same schema as the table */
        index_oid = get_relname_relid(index_name, RelationGetNamespace(heap_rel));

        if !OidIsValid(index_oid) {
            ereport!(
                ERROR,
                errmsg!("index \"{}\" does not exist", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT), parser_errposition */
            );
        }

        /* Open the index (this will throw an error if it is not an index) */
        index_rel = index_open(index_oid, AccessShareLock);
        index_form = (*index_rel).rd_index as *mut FormData_pg_index;

        /* Check that it does not have an associated constraint already */
        if OidIsValid(get_index_constraint(index_oid)) {
            ereport!(
                ERROR,
                errmsg!("index \"{}\" is already associated with a constraint", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), parser_errposition */
            );
        }

        /* Perform validity checks on the index */
        if (*index_form).indrelid != RelationGetRelid(heap_rel) {
            ereport!(
                ERROR,
                errmsg!(
                    "index \"{}\" does not belong to table \"{}\"",
                    cstr_fmt!(index_name),
                    cstr_fmt!(RelationGetRelationName(heap_rel))
                )
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), parser_errposition */
            );
        }

        if !(*index_form).indisvalid {
            ereport!(
                ERROR,
                errmsg!("index \"{}\" is not valid", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), parser_errposition */
            );
        }

        /*
         * Today we forbid non-unique indexes, but we could permit GiST
         * indexes whose last entry is a range type and use that to create a
         * WITHOUT OVERLAPS constraint (i.e. a temporal constraint).
         */
        if !(*index_form).indisunique {
            ereport!(
                ERROR,
                errmsg!("\"{}\" is not a unique index", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 *         errdetail("Cannot create a primary key or unique constraint using such an index."),
                 *         parser_errposition */
            );
        }

        if !RelationGetIndexExpressions(index_rel).is_null()
            && list_length(RelationGetIndexExpressions(index_rel)) > 0
        {
            ereport!(
                ERROR,
                errmsg!("index \"{}\" contains expressions", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 *         errdetail("Cannot create a primary key or unique constraint using such an index."),
                 *         parser_errposition */
            );
        }

        if !RelationGetIndexPredicate(index_rel).is_null()
            && list_length(RelationGetIndexPredicate(index_rel)) > 0
        {
            ereport!(
                ERROR,
                errmsg!("\"{}\" is a partial index", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 *         errdetail("Cannot create a primary key or unique constraint using such an index."),
                 *         parser_errposition */
            );
        }

        /*
         * It's probably unsafe to change a deferred index to non-deferred. (A
         * non-constraint index couldn't be deferred anyway, so this case
         * should never occur; no need to sweat, but let's check it.)
         */
        if !(*index_form).indimmediate && !(*constraint).deferrable {
            ereport!(
                ERROR,
                errmsg!("\"{}\" is a deferrable index", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 *         errdetail("Cannot create a non-deferrable constraint using a deferrable index."),
                 *         parser_errposition */
            );
        }

        /*
         * Insist on it being a btree.  We must have an index that exactly
         * matches what you'd get from plain ADD CONSTRAINT syntax, else dump
         * and reload will produce a different index (breaking pg_upgrade in
         * particular).
         */
        if (*(*index_rel).rd_rel).relam != get_index_am_oid(DEFAULT_INDEX_TYPE.as_ptr() as *const c_char, false) {
            ereport!(
                ERROR,
                errmsg!("index \"{}\" is not a btree", cstr_fmt!(index_name))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
            );
        }

        /* Must get indclass the hard way */
        indclassDatum = SysCacheGetAttrNotNull(
            INDEXRELID,
            (*index_rel).rd_indextuple,
            Anum_pg_index_indclass,
        );
        indclass = DatumGetPointer(indclassDatum) as *mut oidvector;

        let mut i: c_int = 0;
        while i < (*index_form).indnatts {
            let attnum: i16 = (*index_form).indkey_values[i as usize];
            let attform: *const FormData_pg_attribute;
            let attname: *mut c_char;
            let defopclass: Oid;

            /*
             * We shouldn't see attnum == 0 here, since we already rejected
             * expression indexes.  If we do, SystemAttributeDefinition will
             * throw an error.
             */
            if attnum > 0 {
                debug_assert!((attnum as c_int) <= (*(*heap_rel).rd_att).natts);
                attform = TupleDescAttr((*heap_rel).rd_att, (attnum - 1) as c_int)
                    as *const FormData_pg_attribute;
            } else {
                attform = SystemAttributeDefinition(attnum) as *const FormData_pg_attribute;
            }
            attname = pstrdup(NameStr((*attform).attname));

            if i < (*index_form).indnkeyatts {
                /*
                 * Insist on default opclass, collation, and sort options.
                 * While the index would still work as a constraint with
                 * non-default settings, it might not provide exactly the same
                 * uniqueness semantics as you'd get from a normally-created
                 * constraint; and there's also the dump/reload problem
                 * mentioned above.
                 */
                let attoptions: Datum =
                    get_attoptions(RelationGetRelid(index_rel), i + 1);

                defopclass = GetDefaultOpClass(
                    (*attform).atttypid,
                    (*(*index_rel).rd_rel).relam,
                );
                if get_oidvector_val(indclass, i as usize) != defopclass
                    || (*attform).attcollation != get_rd_indcollation(index_rel, i as usize)
                    || attoptions != 0
                    || get_rd_indoption(index_rel, i as usize) != 0
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "index \"{}\" column number {} does not have default sorting behavior",
                            cstr_fmt!(index_name),
                            i + 1
                        )
                        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         *         errdetail("Cannot create a primary key or unique constraint using such an index."),
                         *         parser_errposition */
                    );
                }

                /* If a PK, ensure the columns get not null constraints */
                if (*constraint).contype == CONSTR_PRIMARY {
                    (*cxt).nnconstraints = lappend(
                        (*cxt).nnconstraints,
                        makeNotNullConstraint(makeString(attname)) as *mut c_void,
                    );
                }

                (*constraint).keys = lappend((*constraint).keys, makeString(attname) as *mut c_void);
            } else {
                (*constraint).including =
                    lappend((*constraint).including, makeString(attname) as *mut c_void);
            }

            i += 1;
        }

        /* Close the index relation but keep the lock */
        relation_close(index_rel, NoLock);

        (*index).indexOid = index_oid;
    }

    /*
     * If it's an EXCLUDE constraint, the grammar returns a list of pairs of
     * IndexElems and operator names.  We have to break that apart into
     * separate lists.
     */
    if (*constraint).contype == CONSTR_EXCLUSION {
        foreach!(lc, (*constraint).exclusions, {
            let pair: *mut List = lfirst(current_cell!(lc)) as *mut List;
            let elem: *mut IndexElem;
            let opname: *mut List;

            debug_assert!(list_length(pair) == 2);
            elem = linitial_node!(IndexElem, T_IndexElem, pair);
            opname = lsecond_node!(List, T_List, pair);

            (*index).indexParams = lappend((*index).indexParams, elem as *mut c_void);
            (*index).excludeOpNames =
                lappend((*index).excludeOpNames, opname as *mut c_void);
        });
    }
    /*
     * For UNIQUE and PRIMARY KEY, we just have a list of column names.
     *
     * Make sure referenced keys exist.  If we are making a PRIMARY KEY index,
     * also make sure they are not-null.  For WITHOUT OVERLAPS constraints, we
     * make sure the last part is a range or multirange.
     */
    else {
        foreach!(lc, (*constraint).keys, {
            let key: *mut c_char = strVal!(lfirst(current_cell!(lc)));
            let mut found: bool = false;
            let mut column: *mut ColumnDef = core::ptr::null_mut();
            let mut typid: Oid = InvalidOid;

            /* Make sure referenced column exists. */
            foreach!(columns, (*cxt).columns, {
                column = lfirst_node!(ColumnDef, T_ColumnDef, current_cell!(columns));
                if strcmp((*column).colname, key) == 0 {
                    found = true;
                    break;
                }
            });
            if !found {
                column = core::ptr::null_mut();
            }

            if found {
                /*
                 * column is defined in the new table.  For CREATE TABLE with
                 * a PRIMARY KEY, we can apply the not-null constraint cheaply
                 * here.  If the not-null constraint already exists, we can
                 * (albeit not so cheaply) verify that it's not a NO INHERIT
                 * constraint.
                 *
                 * Note that ALTER TABLE never needs either check, because
                 * those constraints have already been added by
                 * ATPrepAddPrimaryKey.
                 */
                if (*constraint).contype == CONSTR_PRIMARY && !(*cxt).isalter {
                    if (*column).is_not_null {
                        foreach!(lc_nn, (*cxt).nnconstraints, {
                            let nn: *mut Constraint = lfirst(current_cell!(lc_nn)) as *mut Constraint;
                            if strcmp(strVal!(linitial((*nn).keys)), key) == 0 {
                                if (*nn).is_no_inherit {
                                    ereport!(
                                        ERROR,
                                        errmsg!(
                                            "conflicting NO INHERIT declaration for not-null constraint on column \"{}\"",
                                            cstr_fmt!(key)
                                        )
                                        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                                    );
                                }
                                break;
                            }
                        });
                    } else {
                        (*column).is_not_null = true;
                        (*cxt).nnconstraints = lappend(
                            (*cxt).nnconstraints,
                            makeNotNullConstraint(makeString(key)) as *mut c_void,
                        );
                    }
                } else if (*constraint).contype == CONSTR_PRIMARY {
                    debug_assert!((*column).is_not_null);
                }
            } else if !SystemAttributeByName(key).is_null() {
                /*
                 * column will be a system column in the new table, so accept
                 * it. System columns can't ever be null, so no need to worry
                 * about PRIMARY/NOT NULL constraint.
                 */
                found = true;
            } else if !(*cxt).inhRelations.is_null() {
                /* try inherited tables */
                foreach!(inher, (*cxt).inhRelations, {
                    let inh: *mut RangeVar = lfirst_node!(RangeVar, T_RangeVar, current_cell!(inher));
                    let rel: Relation;
                    let mut count: c_int;

                    rel = table_openrv(inh, AccessShareLock);
                    /* check user requested inheritance from valid relkind */
                    if (*(*rel).rd_rel).relkind != RELKIND_RELATION as i8
                        && (*(*rel).rd_rel).relkind != RELKIND_FOREIGN_TABLE as i8
                        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
                    {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "inherited relation \"{}\" is not a table or foreign table",
                                cstr_fmt!((*inh).relname)
                            )
                            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                        );
                    }
                    count = 0;
                    while count < (*(*rel).rd_att).natts {
                        let inhattr = TupleDescAttr((*rel).rd_att, count) as *mut FormData_pg_attribute;
                        let inhname: *mut c_char = NameStr((*inhattr).attname);

                        if (*inhattr).attisdropped {
                            count += 1;
                            continue;
                        }
                        if strcmp(key, inhname) == 0 {
                            found = true;
                            typid = (*inhattr).atttypid;

                            if (*constraint).contype == CONSTR_PRIMARY {
                                (*cxt).nnconstraints = lappend(
                                    (*cxt).nnconstraints,
                                    makeNotNullConstraint(makeString(pstrdup(inhname))) as *mut c_void,
                                );
                            }
                            break;
                        }
                        count += 1;
                    }
                    table_close(rel, NoLock);
                    if found {
                        break;
                    }
                });
            }

            /*
             * In the ALTER TABLE case, don't complain about index keys not
             * created in the command; they may well exist already.
             * DefineIndex will complain about them if not.
             */
            if !found && !(*cxt).isalter {
                ereport!(
                    ERROR,
                    errmsg!("column \"{}\" named in key does not exist", cstr_fmt!(key))
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN), parser_errposition */
                );
            }

            /* Check for PRIMARY KEY(foo, foo) */
            foreach!(columns, (*index).indexParams, {
                let iparam: *mut IndexElem = lfirst(current_cell!(columns)) as *mut IndexElem;
                if !(*iparam).name.is_null() && strcmp(key, (*iparam).name) == 0 {
                    if (*index).primary {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "column \"{}\" appears twice in primary key constraint",
                                cstr_fmt!(key)
                            )
                            /* C also: errcode(ERRCODE_DUPLICATE_COLUMN), parser_errposition */
                        );
                    } else {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "column \"{}\" appears twice in unique constraint",
                                cstr_fmt!(key)
                            )
                            /* C also: errcode(ERRCODE_DUPLICATE_COLUMN), parser_errposition */
                        );
                    }
                }
            });

            /*
             * The WITHOUT OVERLAPS part (if any) must be a range or
             * multirange type.
             */
            if (*constraint).without_overlaps
                && current_cell!(lc) == list_last_cell((*constraint).keys)
            {
                if !found && (*cxt).isalter {
                    /*
                     * Look up the column type on existing table. If we can't
                     * find it, let things fail in DefineIndex.
                     */
                    let rel = (*cxt).rel;

                    let mut j: c_int = 0;
                    while j < (*(*rel).rd_att).natts {
                        let attr = TupleDescAttr((*rel).rd_att, j) as *mut FormData_pg_attribute;
                        let attname_ptr: *const c_char = NameStr((*attr).attname) as *const c_char;

                        if (*attr).attisdropped {
                            break;
                        }

                        if strcmp(attname_ptr, key) == 0 {
                            found = true;
                            typid = (*attr).atttypid;
                            break;
                        }
                        j += 1;
                    }
                }
                if found {
                    if !OidIsValid(typid) && !column.is_null() {
                        typid = typenameTypeId(core::ptr::null_mut(), (*column).typeName);
                    }

                    if !OidIsValid(typid) || !(type_is_range(typid) || type_is_multirange(typid)) {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                                cstr_fmt!(key)
                            )
                            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition */
                        );
                    }
                }
            }

            /* OK, add it to the index definition */
            let iparam: *mut IndexElem = makeNode!(IndexElem, T_IndexElem);
            (*iparam).name = pstrdup(key);
            (*iparam).expr = core::ptr::null_mut();
            (*iparam).indexcolname = core::ptr::null_mut();
            (*iparam).collation = NIL;
            (*iparam).opclass = NIL;
            (*iparam).opclassopts = NIL;
            (*iparam).ordering = SORTBY_DEFAULT;
            (*iparam).nulls_ordering = SORTBY_NULLS_DEFAULT;
            (*index).indexParams = lappend((*index).indexParams, iparam as *mut c_void);
        });

        if (*constraint).without_overlaps {
            /*
             * This enforces that there is at least one equality column
             * besides the WITHOUT OVERLAPS columns.  This is per SQL
             * standard.  XXX Do we need this?
             */
            if list_length((*constraint).keys) < 2 {
                ereport!(
                    ERROR,
                    errmsg!("constraint using WITHOUT OVERLAPS needs at least two columns")
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                );
            }

            /* WITHOUT OVERLAPS requires a GiST index */
            (*index).accessMethod = b"gist\0".as_ptr() as *mut c_char;
        }
    }

    /*
     * Add included columns to index definition.  This is much like the
     * simple-column-name-list code above, except that we don't worry about
     * NOT NULL marking; included columns in a primary key should not be
     * forced NOT NULL.  We don't complain about duplicate columns, either,
     * though maybe we should?
     */
    foreach!(lc, (*constraint).including, {
        let key: *mut c_char = strVal!(lfirst(current_cell!(lc)));
        let mut found: bool = false;
        let mut column: *mut ColumnDef = core::ptr::null_mut();

        foreach!(columns, (*cxt).columns, {
            column = lfirst_node!(ColumnDef, T_ColumnDef, current_cell!(columns));
            if strcmp((*column).colname, key) == 0 {
                found = true;
                break;
            }
        });

        if !found {
            if !SystemAttributeByName(key).is_null() {
                /*
                 * column will be a system column in the new table, so accept it.
                 */
                found = true;
            } else if !(*cxt).inhRelations.is_null() {
                /* try inherited tables */
                foreach!(inher, (*cxt).inhRelations, {
                    let inh: *mut RangeVar = lfirst_node!(RangeVar, T_RangeVar, current_cell!(inher));
                    let rel: Relation;
                    let mut count: c_int;

                    rel = table_openrv(inh, AccessShareLock);
                    if (*(*rel).rd_rel).relkind != RELKIND_RELATION as i8
                        && (*(*rel).rd_rel).relkind != RELKIND_FOREIGN_TABLE as i8
                        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
                    {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "inherited relation \"{}\" is not a table or foreign table",
                                cstr_fmt!((*inh).relname)
                            )
                            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                        );
                    }
                    count = 0;
                    while count < (*(*rel).rd_att).natts {
                        let inhattr = TupleDescAttr((*rel).rd_att, count) as *mut FormData_pg_attribute;
                        let inhname: *mut c_char = NameStr((*inhattr).attname);

                        if (*inhattr).attisdropped {
                            count += 1;
                            continue;
                        }
                        if strcmp(key, inhname) == 0 {
                            found = true;
                            break;
                        }
                        count += 1;
                    }
                    table_close(rel, NoLock);
                    if found {
                        break;
                    }
                });
            }
        }

        /*
         * In the ALTER TABLE case, don't complain about index keys not
         * created in the command; they may well exist already. DefineIndex
         * will complain about them if not.
         */
        if !found && !(*cxt).isalter {
            ereport!(
                ERROR,
                errmsg!("column \"{}\" named in key does not exist", cstr_fmt!(key))
                /* C also: errcode(ERRCODE_UNDEFINED_COLUMN), parser_errposition */
            );
        }

        /* OK, add it to the index definition */
        let iparam: *mut IndexElem = makeNode!(IndexElem, T_IndexElem);
        (*iparam).name = pstrdup(key);
        (*iparam).expr = core::ptr::null_mut();
        (*iparam).indexcolname = core::ptr::null_mut();
        (*iparam).collation = NIL;
        (*iparam).opclass = NIL;
        (*iparam).opclassopts = NIL;
        (*index).indexIncludingParams =
            lappend((*index).indexIncludingParams, iparam as *mut c_void);
    });

    index
}

// Opaque pg_index form and rd_ field stubs
#[repr(C)]
struct FormData_pg_index {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: c_int,
    pub indnkeyatts: c_int,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisvalid: bool,
    pub indkey_values: [i16; 32],
}

unsafe fn get_oidvector_val(v: *mut oidvector, idx: usize) -> Oid {
    unimplemented!("get_oidvector_val stub")
}
unsafe fn get_rd_indcollation(rel: Relation, idx: usize) -> Oid {
    unimplemented!("get_rd_indcollation stub")
}
unsafe fn get_rd_indoption(rel: Relation, idx: usize) -> i16 {
    unimplemented!("get_rd_indoption stub")
}

/*
 * transformCheckConstraints
 *        handle CHECK constraints
 *
 * Right now, there's nothing to do here when called from ALTER TABLE,
 * but the other constraint-transformation functions are called in both
 * the CREATE TABLE and ALTER TABLE paths, so do the same here, and just
 * don't do anything if we're not authorized to skip validation.
 */
unsafe fn transformCheckConstraints(cxt: *mut CreateStmtContext, skipValidation: bool) {
    if (*cxt).ckconstraints.is_null() {
        return;
    }

    /*
     * When creating a new table (but not a foreign table), we can safely skip
     * the validation of check constraints and mark them as valid based on the
     * constraint enforcement flag, since NOT ENFORCED constraints must always
     * be marked as NOT VALID. (This will override any user-supplied NOT VALID
     * flag.)
     */
    if skipValidation {
        foreach!(ckclist, (*cxt).ckconstraints, {
            let constraint: *mut Constraint = lfirst(current_cell!(ckclist)) as *mut Constraint;
            (*constraint).skip_validation = true;
            (*constraint).initially_valid = (*constraint).is_enforced;
        });
    }
}

/*
 * transformFKConstraints
 *        handle FOREIGN KEY constraints
 */
unsafe fn transformFKConstraints(
    cxt: *mut CreateStmtContext,
    skipValidation: bool,
    isAddConstraint: bool,
) {
    if (*cxt).fkconstraints.is_null() {
        return;
    }

    /*
     * If CREATE TABLE or adding a column with NULL default, we can safely
     * skip validation of FK constraints, and mark them as valid based on the
     * constraint enforcement flag, since NOT ENFORCED constraints must always
     * be marked as NOT VALID. (This will override any user-supplied NOT VALID
     * flag.)
     */
    if skipValidation {
        foreach!(fkclist, (*cxt).fkconstraints, {
            let constraint: *mut Constraint = lfirst(current_cell!(fkclist)) as *mut Constraint;
            (*constraint).skip_validation = true;
            (*constraint).initially_valid = (*constraint).is_enforced;
        });
    }

    /*
     * For CREATE TABLE or ALTER TABLE ADD COLUMN, gin up an ALTER TABLE ADD
     * CONSTRAINT command to execute after the basic command is complete. (If
     * called from ADD CONSTRAINT, that routine will add the FK constraints to
     * its own subcommand list.)
     *
     * Note: the ADD CONSTRAINT command must also execute after any index
     * creation commands.  Thus, this should run after
     * transformIndexConstraints, so that the CREATE INDEX commands are
     * already in cxt->alist.  See also the handling of cxt->likeclauses.
     */
    if !isAddConstraint {
        let alterstmt: *mut AlterTableStmt = makeNode!(AlterTableStmt, T_AlterTableStmt);
        (*alterstmt).relation = (*cxt).relation;
        (*alterstmt).cmds = NIL;
        (*alterstmt).objtype = OBJECT_TABLE;

        foreach!(fkclist, (*cxt).fkconstraints, {
            let constraint: *mut Constraint = lfirst(current_cell!(fkclist)) as *mut Constraint;
            let altercmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
            (*altercmd).subtype = AT_AddConstraint;
            (*altercmd).name = core::ptr::null_mut();
            (*altercmd).def = constraint as *mut Node;
            (*alterstmt).cmds = lappend((*alterstmt).cmds, altercmd as *mut c_void);
        });

        (*cxt).alist = lappend((*cxt).alist, alterstmt as *mut c_void);
    }
}

/*
 * transformIndexStmt - parse analysis for CREATE INDEX and ALTER TABLE
 *
 * Note: this is a no-op for an index not using either index expressions or
 * a predicate expression.  There are several code paths that create indexes
 * without bothering to call this, because they know they don't have any
 * such expressions to deal with.
 *
 * To avoid race conditions, it's important that this function rely only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
pub unsafe fn transformIndexStmt(
    relid: Oid,
    stmt: *mut IndexStmt,
    queryString: *const c_char,
) -> *mut IndexStmt {
    let mut pstate: *mut ParseState;
    let mut nsitem: *mut ParseNamespaceItem;
    let mut rel: Relation;

    /* Nothing to do if statement already transformed. */
    if (*stmt).transformed {
        return stmt;
    }

    /* Set up pstate */
    pstate = make_parsestate(core::ptr::null_mut());
    (*pstate).p_sourcetext = queryString;

    /*
     * Put the parent table into the rtable so that the expressions can refer
     * to its fields without qualification.  Caller is responsible for locking
     * relation, but we still need to open it.
     */
    rel = relation_open(relid, NoLock);
    nsitem = addRangeTableEntryForRelation(pstate, rel as *mut c_void, AccessShareLock, core::ptr::null_mut(), false, true);

    /* no to join list, yes to namespaces */
    addNSItemToQuery(pstate, nsitem, false, true, true);

    /* take care of the where clause */
    if !(*stmt).whereClause.is_null() {
        (*stmt).whereClause = transformWhereClause(
            pstate,
            (*stmt).whereClause,
            EXPR_KIND_INDEX_PREDICATE,
            b"WHERE\0".as_ptr() as *const c_char,
        );
        /* we have to fix its collations too */
        assign_expr_collations(pstate, (*stmt).whereClause);
    }

    /* take care of any index expressions */
    foreach!(l, (*stmt).indexParams, {
        let ielem: *mut IndexElem = lfirst(current_cell!(l)) as *mut IndexElem;

        if !(*ielem).expr.is_null() {
            /* Extract preliminary index col name before transforming expr */
            if (*ielem).indexcolname.is_null() {
                (*ielem).indexcolname = FigureIndexColname((*ielem).expr);
            }

            /* Now do parse transformation of the expression */
            (*ielem).expr = transformExpr(
                pstate,
                (*ielem).expr,
                EXPR_KIND_INDEX_EXPRESSION,
            );

            /* We have to fix its collations too */
            assign_expr_collations(pstate, (*ielem).expr);

            /*
             * transformExpr() should have already rejected subqueries,
             * aggregates, window functions, and SRFs, based on the EXPR_KIND_
             * for an index expression.
             *
             * DefineIndex() will make more checks.
             */
        }
    });

    /*
     * Check that only the base rel is mentioned.  (This should be dead code
     * now that add_missing_from is history.)
     */
    if list_length((*pstate).p_rtable) != 1 {
        ereport!(
            ERROR,
            errmsg!("index expressions and predicates can refer only to the table being indexed")
            /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
        );
    }

    free_parsestate(pstate);

    /* Close relation */
    table_close(rel, NoLock);

    /* Mark statement as successfully transformed */
    (*stmt).transformed = true;

    stmt
}

/*
 * transformStatsStmt - parse analysis for CREATE STATISTICS
 *
 * To avoid race conditions, it's important that this function relies only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
pub unsafe fn transformStatsStmt(
    relid: Oid,
    stmt: *mut CreateStatsStmt,
    queryString: *const c_char,
) -> *mut CreateStatsStmt {
    let mut pstate: *mut ParseState;
    let mut nsitem: *mut ParseNamespaceItem;
    let mut rel: Relation;

    /* Nothing to do if statement already transformed. */
    if (*stmt).transformed {
        return stmt;
    }

    /* Set up pstate */
    pstate = make_parsestate(core::ptr::null_mut());
    (*pstate).p_sourcetext = queryString;

    /*
     * Put the parent table into the rtable so that the expressions can refer
     * to its fields without qualification.  Caller is responsible for locking
     * relation, but we still need to open it.
     */
    rel = relation_open(relid, NoLock);
    nsitem = addRangeTableEntryForRelation(pstate, rel as *mut c_void, AccessShareLock, core::ptr::null_mut(), false, true);

    /* no to join list, yes to namespaces */
    addNSItemToQuery(pstate, nsitem, false, true, true);

    /* take care of any expressions */
    foreach!(l, (*stmt).exprs, {
        let selem: *mut StatsElem = lfirst(current_cell!(l)) as *mut StatsElem;

        if !(*selem).expr.is_null() {
            /* Now do parse transformation of the expression */
            (*selem).expr = transformExpr(pstate, (*selem).expr, EXPR_KIND_STATS_EXPRESSION);

            /* We have to fix its collations too */
            assign_expr_collations(pstate, (*selem).expr);
        }
    });

    /*
     * Check that only the base rel is mentioned.  (This should be dead code
     * now that add_missing_from is history.)
     */
    if list_length((*pstate).p_rtable) != 1 {
        ereport!(
            ERROR,
            errmsg!("statistics expressions can refer only to the table being referenced")
            /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
        );
    }

    free_parsestate(pstate);

    /* Close relation */
    table_close(rel, NoLock);

    /* Mark statement as successfully transformed */
    (*stmt).transformed = true;

    stmt
}

// ---------------------------------------------------------------------------
// Part 6: transformRuleStmt + transformAlterTableStmt + transformConstraintAttrs
//         + transformColumnType + transformCreateSchemaStmtElements
//         + setSchemaName + transformPartitionCmd + transformPartitionBound
//         + transformPartitionRangeBounds + validateInfiniteBounds
//         + transformPartitionBoundValue
// ---------------------------------------------------------------------------

/*
 * transformRuleStmt -
 *    transform a CREATE RULE Statement. The action is a list of parse
 *    trees which is transformed into a list of query trees, and we also
 *    transform the WHERE clause if any.
 *
 * actions and whereClause are output parameters that receive the
 * transformed results.
 */
pub unsafe fn transformRuleStmt(
    stmt: *mut RuleStmt,
    queryString: *const c_char,
    actions: *mut *mut List,
    whereClause: *mut *mut Node,
) {
    let mut rel: Relation;
    let mut pstate: *mut ParseState;
    let mut oldnsitem: *mut ParseNamespaceItem;
    let mut newnsitem: *mut ParseNamespaceItem;

    /*
     * To avoid deadlock, make sure the first thing we do is grab
     * AccessExclusiveLock on the target relation.  This will be needed by
     * DefineQueryRewrite(), and we don't want to grab a lesser lock
     * beforehand.
     */
    rel = table_openrv((*stmt).relation, AccessExclusiveLock);

    if (*(*rel).rd_rel).relkind == RELKIND_MATVIEW as i8 {
        ereport!(
            ERROR,
            errmsg!("rules on materialized views are not supported")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Set up pstate */
    pstate = make_parsestate(core::ptr::null_mut());
    (*pstate).p_sourcetext = queryString;

    /*
     * NOTE: 'OLD' must always have a varno equal to 1 and 'NEW' equal to 2.
     * Set up their ParseNamespaceItems in the main pstate for use in parsing
     * the rule qualification.
     */
    oldnsitem = addRangeTableEntryForRelation(
        pstate,
        rel as *mut c_void,
        AccessShareLock,
        makeAlias(b"old\0".as_ptr() as *mut c_char, NIL),
        false,
        false,
    );
    newnsitem = addRangeTableEntryForRelation(
        pstate,
        rel as *mut c_void,
        AccessShareLock,
        makeAlias(b"new\0".as_ptr() as *mut c_char, NIL),
        false,
        false,
    );

    /*
     * They must be in the namespace too for lookup purposes, but only add the
     * one(s) that are relevant for the current kind of rule.  In an UPDATE
     * rule, quals must refer to OLD.field or NEW.field to be unambiguous, but
     * there's no need to be so picky for INSERT & DELETE.  We do not add them
     * to the joinlist.
     */
    match (*stmt).event {
        CMD_SELECT => {
            addNSItemToQuery(pstate, oldnsitem, false, true, true);
        }
        CMD_UPDATE => {
            addNSItemToQuery(pstate, oldnsitem, false, true, true);
            addNSItemToQuery(pstate, newnsitem, false, true, true);
        }
        CMD_INSERT => {
            addNSItemToQuery(pstate, newnsitem, false, true, true);
        }
        CMD_DELETE => {
            addNSItemToQuery(pstate, oldnsitem, false, true, true);
        }
        _ => {
            elog!(ERROR, "unrecognized event type: {}", (*stmt).event as c_int);
        }
    }

    /* take care of the where clause */
    *whereClause = transformWhereClause(
        pstate,
        (*stmt).whereClause,
        EXPR_KIND_WHERE,
        b"WHERE\0".as_ptr() as *const c_char,
    );
    /* we have to fix its collations too */
    assign_expr_collations(pstate, *whereClause);

    /* this is probably dead code without add_missing_from: */
    if list_length((*pstate).p_rtable) != 2 {
        /* naughty, naughty... */
        ereport!(
            ERROR,
            errmsg!("rule WHERE condition cannot contain references to other relations")
            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        );
    }

    /*
     * 'instead nothing' rules with a qualification need a query rangetable so
     * the rewrite handler can add the negated rule qualification to the
     * original query. We create a query with the new command type CMD_NOTHING
     * here that is treated specially by the rewrite system.
     */
    if (*stmt).actions.is_null() {
        let nothing_qry: *mut Query = makeNode!(Query, T_Query);
        (*nothing_qry).commandType = CMD_NOTHING;
        (*nothing_qry).rtable = (*pstate).p_rtable;
        (*nothing_qry).rteperminfos = (*pstate).p_rteperminfos;
        (*nothing_qry).jointree = makeFromExpr(NIL, core::ptr::null_mut());

        *actions = list_make1!(nothing_qry as *mut c_void);
    } else {
        let mut newactions: *mut List = NIL;

        /*
         * transform each statement, like parse_sub_analyze()
         */
        foreach!(l, (*stmt).actions, {
            let action: *mut Node = lfirst(current_cell!(l)) as *mut Node;
            let sub_pstate: *mut ParseState = make_parsestate(core::ptr::null_mut());
            let mut sub_qry: *mut Query;
            let mut top_subqry: *mut Query;
            let has_old: bool;
            let has_new: bool;

            /*
             * Since outer ParseState isn't parent of inner, have to pass down
             * the query text by hand.
             */
            (*sub_pstate).p_sourcetext = queryString;

            /*
             * Set up OLD/NEW in the rtable for this statement.  The entries
             * are added only to relnamespace, not varnamespace, because we
             * don't want them to be referred to by unqualified field names
             * nor "*" in the rule actions.  We decide later whether to put
             * them in the joinlist.
             */
            oldnsitem = addRangeTableEntryForRelation(
                sub_pstate,
                rel as *mut c_void,
                AccessShareLock,
                makeAlias(b"old\0".as_ptr() as *mut c_char, NIL),
                false,
                false,
            );
            newnsitem = addRangeTableEntryForRelation(
                sub_pstate,
                rel as *mut c_void,
                AccessShareLock,
                makeAlias(b"new\0".as_ptr() as *mut c_char, NIL),
                false,
                false,
            );
            addNSItemToQuery(sub_pstate, oldnsitem, false, true, false);
            addNSItemToQuery(sub_pstate, newnsitem, false, true, false);

            /* Transform the rule action statement */
            top_subqry = transformStmt(sub_pstate, action);

            /*
             * We cannot support utility-statement actions (eg NOTIFY) with
             * nonempty rule WHERE conditions, because there's no way to make
             * the utility action execute conditionally.
             */
            if (*top_subqry).commandType == CMD_UTILITY && !(*whereClause).is_null() {
                ereport!(
                    ERROR,
                    errmsg!("rules with WHERE conditions can only have SELECT, INSERT, UPDATE, or DELETE actions")
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * If the action is INSERT...SELECT, OLD/NEW have been pushed down
             * into the SELECT, and that's what we need to look at. (Ugly
             * kluge ... try to fix this when we redesign querytrees.)
             */
            sub_qry = getInsertSelectQuery(top_subqry, core::ptr::null_mut());

            /*
             * If the sub_qry is a setop, we cannot attach any qualifications
             * to it, because the planner won't notice them.  This could
             * perhaps be relaxed someday, but for now, we may as well reject
             * such a rule immediately.
             */
            if !(*sub_qry).setOperations.is_null() && !(*whereClause).is_null() {
                ereport!(
                    ERROR,
                    errmsg!("conditional UNION/INTERSECT/EXCEPT statements are not implemented")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }

            /* Validate action's use of OLD/NEW, qual too */
            has_old = rangeTableEntry_used(sub_qry as *const Node, PRS2_OLD_VARNO, 0)
                || rangeTableEntry_used(*whereClause, PRS2_OLD_VARNO, 0);
            has_new = rangeTableEntry_used(sub_qry as *const Node, PRS2_NEW_VARNO, 0)
                || rangeTableEntry_used(*whereClause, PRS2_NEW_VARNO, 0);

            match (*stmt).event {
                CMD_SELECT => {
                    if has_old {
                        ereport!(ERROR, errmsg!("ON SELECT rule cannot use OLD")
                            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                        );
                    }
                    if has_new {
                        ereport!(ERROR, errmsg!("ON SELECT rule cannot use NEW")
                            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                        );
                    }
                }
                CMD_UPDATE => { /* both are OK */ }
                CMD_INSERT => {
                    if has_old {
                        ereport!(ERROR, errmsg!("ON INSERT rule cannot use OLD")
                            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                        );
                    }
                }
                CMD_DELETE => {
                    if has_new {
                        ereport!(ERROR, errmsg!("ON DELETE rule cannot use NEW")
                            /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                        );
                    }
                }
                _ => {
                    elog!(ERROR, "unrecognized event type: {}", (*stmt).event as c_int);
                }
            }

            /*
             * OLD/NEW are not allowed in WITH queries, because they would
             * amount to outer references for the WITH, which we disallow.
             * However, they were already in the outer rangetable when we
             * analyzed the query, so we have to check.
             *
             * Note that in the INSERT...SELECT case, we need to examine the
             * CTE lists of both top_subqry and sub_qry.
             *
             * Note that we aren't digging into the body of the query looking
             * for WITHs in nested sub-SELECTs.  A WITH down there can
             * legitimately refer to OLD/NEW, because it'd be an
             * indirect-correlated outer reference.
             */
            if rangeTableEntry_used((*top_subqry).cteList as *const Node, PRS2_OLD_VARNO, 0)
                || rangeTableEntry_used((*sub_qry).cteList as *const Node, PRS2_OLD_VARNO, 0)
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot refer to OLD within WITH query")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }
            if rangeTableEntry_used((*top_subqry).cteList as *const Node, PRS2_NEW_VARNO, 0)
                || rangeTableEntry_used((*sub_qry).cteList as *const Node, PRS2_NEW_VARNO, 0)
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot refer to NEW within WITH query")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                );
            }

            /*
             * For efficiency's sake, add OLD to the rule action's jointree
             * only if it was actually referenced in the statement or qual.
             *
             * For INSERT, NEW is not really a relation (only a reference to
             * the to-be-inserted tuple) and should never be added to the
             * jointree.
             *
             * For UPDATE, we treat NEW as being another kind of reference to
             * OLD, because it represents references to *transformed* tuples
             * of the existing relation.  It would be wrong to enter NEW
             * separately in the jointree, since that would cause a double
             * join of the updated relation.  It's also wrong to fail to make
             * a jointree entry if only NEW and not OLD is mentioned.
             */
            if has_old || (has_new && (*stmt).event == CMD_UPDATE) {
                let rtr: *mut RangeTblRef;

                /*
                 * If sub_qry is a setop, manipulating its jointree will do no
                 * good at all, because the jointree is dummy. (This should be
                 * a can't-happen case because of prior tests.)
                 */
                if !(*sub_qry).setOperations.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("conditional UNION/INTERSECT/EXCEPT statements are not implemented")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
                /* hackishly add OLD to the already-built FROM clause */
                rtr = makeNode!(RangeTblRef, T_RangeTblRef);
                (*rtr).rtindex = (*oldnsitem).p_rtindex;
                (*(*sub_qry).jointree).fromlist =
                    lappend((*(*sub_qry).jointree).fromlist, rtr as *mut c_void);
            }

            newactions = lappend(newactions, top_subqry as *mut c_void);

            free_parsestate(sub_pstate);
        });

        *actions = newactions;
    }

    free_parsestate(pstate);

    /* Close relation, but keep the exclusive lock */
    table_close(rel, NoLock);
}

// CMD_SELECT/UPDATE/INSERT/DELETE/UTILITY are CmdType enum variants (imported from nodes::nodes)

/*
 * transformAlterTableStmt -
 *        parse analysis for ALTER TABLE
 *
 * Returns the transformed AlterTableStmt.  There may be additional actions
 * to be done before and after the transformed statement, which are returned
 * in *beforeStmts and *afterStmts as lists of utility command parsetrees.
 *
 * To avoid race conditions, it's important that this function rely only on
 * the passed-in relid (and not on stmt->relation) to determine the target
 * relation.
 */
pub unsafe fn transformAlterTableStmt(
    relid: Oid,
    stmt: *mut AlterTableStmt,
    queryString: *const c_char,
    beforeStmts: *mut *mut List,
    afterStmts: *mut *mut List,
) -> *mut AlterTableStmt {
    let mut rel: Relation;
    let mut tupdesc: TupleDesc;
    let mut pstate: *mut ParseState;
    let mut cxt: CreateStmtContext = core::mem::zeroed();
    let mut save_alist: *mut List;
    let mut newcmds: *mut List = NIL;
    let mut skipValidation: bool = true;
    let mut newcmd: *mut AlterTableCmd;
    let mut nsitem: *mut ParseNamespaceItem;

    /* Caller is responsible for locking the relation */
    rel = relation_open(relid, NoLock);
    tupdesc = RelationGetDescr(rel);

    /* Set up pstate */
    pstate = make_parsestate(core::ptr::null_mut());
    (*pstate).p_sourcetext = queryString;
    nsitem = addRangeTableEntryForRelation(pstate, rel as *mut c_void, AccessShareLock, core::ptr::null_mut(), false, true);
    addNSItemToQuery(pstate, nsitem, false, true, true);

    /* Set up CreateStmtContext */
    cxt.pstate = pstate;
    if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as i8 {
        cxt.stmtType = b"ALTER FOREIGN TABLE\0".as_ptr() as *const c_char;
        cxt.isforeign = true;
    } else {
        cxt.stmtType = b"ALTER TABLE\0".as_ptr() as *const c_char;
        cxt.isforeign = false;
    }
    cxt.relation = (*stmt).relation;
    cxt.rel = rel;
    cxt.inhRelations = NIL;
    cxt.isalter = true;
    cxt.columns = NIL;
    cxt.ckconstraints = NIL;
    cxt.nnconstraints = NIL;
    cxt.fkconstraints = NIL;
    cxt.ixconstraints = NIL;
    cxt.likeclauses = NIL;
    cxt.blist = NIL;
    cxt.alist = NIL;
    cxt.pkey = core::ptr::null_mut();
    cxt.ispartitioned =
        ((*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8);
    cxt.partbound = core::ptr::null_mut();
    cxt.ofType = false;

    /*
     * Transform ALTER subcommands that need it (most don't).  These largely
     * re-use code from CREATE TABLE.
     */
    foreach!(lcmd, (*stmt).cmds, {
        let cmd: *mut AlterTableCmd = lfirst(current_cell!(lcmd)) as *mut AlterTableCmd;

        match (*cmd).subtype {
            AT_AddColumn => {
                let def: *mut ColumnDef = castNode!(ColumnDef, T_ColumnDef, (*cmd).def as *mut Node);

                transformColumnDefinition(&mut cxt, def);

                /*
                 * If the column has a non-null default, we can't skip
                 * validation of foreign keys.
                 */
                if !(*def).raw_default.is_null() {
                    skipValidation = false;
                }

                /*
                 * All constraints are processed in other ways. Remove the
                 * original list
                 */
                (*def).constraints = NIL;

                newcmds = lappend(newcmds, cmd as *mut c_void);
            }
            AT_AddConstraint => {
                /*
                 * The original AddConstraint cmd node doesn't go to newcmds
                 */
                if IsA!((*cmd).def as *mut Node, T_Constraint) {
                    transformTableConstraint(&mut cxt, (*cmd).def as *mut Constraint);
                    if (*((*cmd).def as *mut Constraint)).contype == CONSTR_FOREIGN {
                        skipValidation = false;
                    }
                } else {
                    elog!(ERROR, "unrecognized node type: {}", nodeTag((*cmd).def as *mut Node) as c_int);
                }
            }
            AT_AlterColumnType => {
                let def: *mut ColumnDef = castNode!(ColumnDef, T_ColumnDef, (*cmd).def as *mut Node);
                let attnum: AttrNumber;

                /*
                 * For ALTER COLUMN TYPE, transform the USING clause if
                 * one was specified.
                 */
                if !(*def).raw_default.is_null() {
                    (*def).cooked_default = transformExpr(
                        pstate,
                        (*def).raw_default,
                        EXPR_KIND_ALTER_COL_TRANSFORM,
                    );
                }

                /*
                 * For identity column, create ALTER SEQUENCE command to
                 * change the data type of the sequence. Identity sequence
                 * is associated with the top level partitioned table.
                 * Hence ignore partitions.
                 */
                if !(*RelationGetForm(rel)).relispartition {
                    attnum = get_attnum(relid, (*cmd).name);
                    if attnum == InvalidAttrNumber {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "column \"{}\" of relation \"{}\" does not exist",
                                cstr_fmt!((*cmd).name),
                                cstr_fmt!(RelationGetRelationName(rel))
                            )
                            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
                        );
                    }

                    if attnum > 0
                        && (*(TupleDescAttr(tupdesc, (attnum - 1) as c_int) as *mut FormData_pg_attribute)).attidentity != 0
                    {
                        let seq_relid: Oid = getIdentitySequence(rel, attnum, false);
                        let typeOid: Oid = typenameTypeId(pstate, (*def).typeName);
                        let altseqstmt: *mut AlterSeqStmt = makeNode!(AlterSeqStmt, T_AlterSeqStmt);

                        (*altseqstmt).sequence = makeRangeVar(
                            get_namespace_name(get_rel_namespace(seq_relid)),
                            get_rel_name(seq_relid),
                            -1,
                        );
                        (*altseqstmt).options = list_make1!(
                            makeDefElem(
                                b"as\0".as_ptr() as *mut c_char,
                                makeTypeNameFromOid(typeOid, -1) as *mut Node,
                                -1,
                            ) as *mut c_void
                        );
                        (*altseqstmt).for_identity = true;
                        cxt.blist = lappend(cxt.blist, altseqstmt as *mut c_void);
                    }
                }

                newcmds = lappend(newcmds, cmd as *mut c_void);
            }
            AT_AddIdentity => {
                let def: *mut Constraint = castNode!(Constraint, T_Constraint, (*cmd).def as *mut Node);
                let newdef: *mut ColumnDef = makeNode!(ColumnDef, T_ColumnDef);
                let attnum: AttrNumber;

                (*newdef).colname = (*cmd).name;
                (*newdef).identity = (*def).generated_when;
                (*cmd).def = newdef as *mut Node;

                attnum = get_attnum(relid, (*cmd).name);
                if attnum == InvalidAttrNumber {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" of relation \"{}\" does not exist",
                            cstr_fmt!((*cmd).name),
                            cstr_fmt!(RelationGetRelationName(rel))
                        )
                        /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
                    );
                }

                generateSerialExtraStmts(
                    &mut cxt,
                    newdef,
                    get_atttype(relid, attnum),
                    (*def).options,
                    true,
                    true,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );

                newcmds = lappend(newcmds, cmd as *mut c_void);
            }
            AT_SetIdentity => {
                /*
                 * Create an ALTER SEQUENCE statement for the internal
                 * sequence of the identity column.
                 */
                let mut newseqopts: *mut List = NIL;
                let mut newdef2: *mut List = NIL;
                let attnum: AttrNumber;
                let seq_relid: Oid;

                /*
                 * Split options into those handled by ALTER SEQUENCE and
                 * those for ALTER TABLE proper.
                 */
                foreach!(lc, castNode!(List, T_List, (*cmd).def as *mut Node), {
                    let def2: *mut DefElem = lfirst_node!(DefElem, T_DefElem, current_cell!(lc));

                    if strcmp(
                        (*def2).defname,
                        b"generated\0".as_ptr() as *const c_char,
                    ) == 0 {
                        newdef2 = lappend(newdef2, def2 as *mut c_void);
                    } else {
                        newseqopts = lappend(newseqopts, def2 as *mut c_void);
                    }
                });

                attnum = get_attnum(relid, (*cmd).name);
                if attnum == InvalidAttrNumber {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" of relation \"{}\" does not exist",
                            cstr_fmt!((*cmd).name),
                            cstr_fmt!(RelationGetRelationName(rel))
                        )
                        /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
                    );
                }

                seq_relid = getIdentitySequence(rel, attnum, true);

                if OidIsValid(seq_relid) {
                    let seqstmt: *mut AlterSeqStmt = makeNode!(AlterSeqStmt, T_AlterSeqStmt);
                    (*seqstmt).sequence = makeRangeVar(
                        get_namespace_name(get_rel_namespace(seq_relid)),
                        get_rel_name(seq_relid),
                        -1,
                    );
                    (*seqstmt).options = newseqopts;
                    (*seqstmt).for_identity = true;
                    (*seqstmt).missing_ok = false;

                    cxt.blist = lappend(cxt.blist, seqstmt as *mut c_void);
                }

                /*
                 * If column was not an identity column, we just let the
                 * ALTER TABLE command error out later.  (There are cases
                 * this fails to cover, but we'll need to restructure
                 * where creation of the sequence dependency linkage
                 * happens before we can fix it.)
                 */

                (*cmd).def = newdef2 as *mut Node;
                newcmds = lappend(newcmds, cmd as *mut c_void);
            }
            AT_AttachPartition | AT_DetachPartition => {
                let partcmd: *mut PartitionCmd = (*cmd).def as *mut PartitionCmd;
                transformPartitionCmd(&mut cxt, partcmd);
                /* assign transformed value of the partition bound */
                (*partcmd).bound = cxt.partbound;

                newcmds = lappend(newcmds, cmd as *mut c_void);
            }
            _ => {
                /*
                 * Currently, we shouldn't actually get here for subcommand
                 * types that don't require transformation; but if we do, just
                 * emit them unchanged.
                 */
                newcmds = lappend(newcmds, cmd as *mut c_void);
            }
        }
    });

    /*
     * Transfer anything we already have in cxt.alist into save_alist, to keep
     * it separate from the output of transformIndexConstraints.
     */
    save_alist = cxt.alist;
    cxt.alist = NIL;

    /* Postprocess constraints */
    transformIndexConstraints(&mut cxt);
    transformFKConstraints(&mut cxt, skipValidation, true);
    transformCheckConstraints(&mut cxt, false);

    /*
     * Push any index-creation commands into the ALTER, so that they can be
     * scheduled nicely by tablecmds.c.  Note that tablecmds.c assumes that
     * the IndexStmt attached to an AT_AddIndex or AT_AddIndexConstraint
     * subcommand has already been through transformIndexStmt.
     */
    foreach!(l, cxt.alist, {
        let istmt: *mut Node = lfirst(current_cell!(l)) as *mut Node;

        /*
         * We assume here that cxt.alist contains only IndexStmts generated
         * from primary key constraints.
         */
        if IsA!(istmt, T_IndexStmt) {
            let mut idxstmt: *mut IndexStmt = istmt as *mut IndexStmt;

            idxstmt = transformIndexStmt(relid, idxstmt, queryString);
            newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
            (*newcmd).subtype = if OidIsValid((*idxstmt).indexOid) {
                AT_AddIndexConstraint
            } else {
                AT_AddIndex
            };
            (*newcmd).def = idxstmt as *mut Node;
            newcmds = lappend(newcmds, newcmd as *mut c_void);
        } else {
            elog!(ERROR, "unexpected stmt type {}", nodeTag(istmt) as c_int);
        }
    });
    cxt.alist = NIL;

    /* Append any CHECK, NOT NULL or FK constraints to the commands list */
    foreach!(lc, cxt.ckconstraints, {
        let def3: *mut Constraint = lfirst(current_cell!(lc)) as *mut Constraint;
        newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        (*newcmd).subtype = AT_AddConstraint;
        (*newcmd).def = def3 as *mut Node;
        newcmds = lappend(newcmds, newcmd as *mut c_void);
    });
    foreach!(lc, cxt.nnconstraints, {
        let def3: *mut Constraint = lfirst(current_cell!(lc)) as *mut Constraint;
        newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        (*newcmd).subtype = AT_AddConstraint;
        (*newcmd).def = def3 as *mut Node;
        newcmds = lappend(newcmds, newcmd as *mut c_void);
    });
    foreach!(lc, cxt.fkconstraints, {
        let def3: *mut Constraint = lfirst(current_cell!(lc)) as *mut Constraint;
        newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        (*newcmd).subtype = AT_AddConstraint;
        (*newcmd).def = def3 as *mut Node;
        newcmds = lappend(newcmds, newcmd as *mut c_void);
    });

    /* Close rel */
    relation_close(rel, NoLock);

    /* Output results. */
    (*stmt).cmds = newcmds;

    *beforeStmts = cxt.blist;
    *afterStmts = list_concat(cxt.alist, save_alist);

    stmt
}

/*
 * Preprocess a list of column constraint clauses
 * to attach constraint attributes to their primary constraint nodes
 * and detect inconsistent/misplaced constraint attributes.
 *
 * NOTE: currently, attributes are only supported for FOREIGN KEY, UNIQUE,
 * EXCLUSION, and PRIMARY KEY constraints, but someday they ought to be
 * supported for other constraint types.
 */
unsafe fn transformConstraintAttrs(cxt: *mut CreateStmtContext, constraintList: *mut List) {
    let mut lastprimarycon: *mut Constraint = core::ptr::null_mut();
    let mut saw_deferrability: bool = false;
    let mut saw_initially: bool = false;
    let mut saw_enforced: bool = false;

    /* SUPPORTS_ATTRS macro: node != NULL && contype in {PRIMARY,UNIQUE,EXCLUSION,FOREIGN} */
    macro_rules! SUPPORTS_ATTRS {
        ($node:expr) => {
            !$node.is_null()
                && ((*$node).contype == CONSTR_PRIMARY
                    || (*$node).contype == CONSTR_UNIQUE
                    || (*$node).contype == CONSTR_EXCLUSION
                    || (*$node).contype == CONSTR_FOREIGN)
        };
    }

    foreach!(clist, constraintList, {
        let con: *mut Constraint = lfirst(current_cell!(clist)) as *mut Constraint;

        if !IsA!(con as *mut Node, T_Constraint) {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(con as *mut Node) as c_int);
        }
        match (*con).contype {
            CONSTR_ATTR_DEFERRABLE => {
                if !SUPPORTS_ATTRS!(lastprimarycon) {
                    ereport!(
                        ERROR,
                        errmsg!("misplaced DEFERRABLE clause")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                if saw_deferrability {
                    ereport!(
                        ERROR,
                        errmsg!("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                saw_deferrability = true;
                (*lastprimarycon).deferrable = true;
            }
            CONSTR_ATTR_NOT_DEFERRABLE => {
                if !SUPPORTS_ATTRS!(lastprimarycon) {
                    ereport!(
                        ERROR,
                        errmsg!("misplaced NOT DEFERRABLE clause")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                if saw_deferrability {
                    ereport!(
                        ERROR,
                        errmsg!("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                saw_deferrability = true;
                (*lastprimarycon).deferrable = false;
                if saw_initially && (*lastprimarycon).initdeferred {
                    ereport!(
                        ERROR,
                        errmsg!("constraint declared INITIALLY DEFERRED must be DEFERRABLE")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
            }
            CONSTR_ATTR_DEFERRED => {
                if !SUPPORTS_ATTRS!(lastprimarycon) {
                    ereport!(
                        ERROR,
                        errmsg!("misplaced INITIALLY DEFERRED clause")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                if saw_initially {
                    ereport!(
                        ERROR,
                        errmsg!("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                saw_initially = true;
                (*lastprimarycon).initdeferred = true;

                /*
                 * If only INITIALLY DEFERRED appears, assume DEFERRABLE
                 */
                if !saw_deferrability {
                    (*lastprimarycon).deferrable = true;
                } else if !(*lastprimarycon).deferrable {
                    ereport!(
                        ERROR,
                        errmsg!("constraint declared INITIALLY DEFERRED must be DEFERRABLE")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
            }
            CONSTR_ATTR_IMMEDIATE => {
                if !SUPPORTS_ATTRS!(lastprimarycon) {
                    ereport!(
                        ERROR,
                        errmsg!("misplaced INITIALLY IMMEDIATE clause")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                if saw_initially {
                    ereport!(
                        ERROR,
                        errmsg!("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                saw_initially = true;
                (*lastprimarycon).initdeferred = false;
            }
            CONSTR_ATTR_ENFORCED => {
                if lastprimarycon.is_null()
                    || ((*lastprimarycon).contype != CONSTR_CHECK
                        && (*lastprimarycon).contype != CONSTR_FOREIGN)
                {
                    ereport!(
                        ERROR,
                        errmsg!("misplaced ENFORCED clause")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                if saw_enforced {
                    ereport!(
                        ERROR,
                        errmsg!("multiple ENFORCED/NOT ENFORCED clauses not allowed")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                saw_enforced = true;
                (*lastprimarycon).is_enforced = true;
            }
            CONSTR_ATTR_NOT_ENFORCED => {
                if lastprimarycon.is_null()
                    || ((*lastprimarycon).contype != CONSTR_CHECK
                        && (*lastprimarycon).contype != CONSTR_FOREIGN)
                {
                    ereport!(
                        ERROR,
                        errmsg!("misplaced NOT ENFORCED clause")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                if saw_enforced {
                    ereport!(
                        ERROR,
                        errmsg!("multiple ENFORCED/NOT ENFORCED clauses not allowed")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
                saw_enforced = true;
                (*lastprimarycon).is_enforced = false;

                /* A NOT ENFORCED constraint must be marked as invalid. */
                (*lastprimarycon).skip_validation = true;
                (*lastprimarycon).initially_valid = false;
            }
            _ => {
                /* Otherwise it's not an attribute */
                lastprimarycon = con;
                /* reset flags for new primary node */
                saw_deferrability = false;
                saw_initially = false;
                saw_enforced = false;
            }
        }
    });
}

/*
 * Special handling of type definition for a column
 */
unsafe fn transformColumnType(cxt: *mut CreateStmtContext, column: *mut ColumnDef) {
    /*
     * All we really need to do here is verify that the type is valid,
     * including any collation spec that might be present.
     */
    let ctype: crate::access::htup_details::HeapTuple = typenameType((*cxt).pstate, (*column).typeName, core::ptr::null_mut());

    if !(*column).collClause.is_null() {
        let typtup: *mut FormData_pg_type = GETSTRUCT(ctype) as *mut FormData_pg_type;

        LookupCollation(
            (*cxt).pstate,
            (*(*column).collClause).collname,
            (*(*column).collClause).location,
        );
        /* Complain if COLLATE is applied to an uncollatable type */
        if !OidIsValid((*typtup).typcollation) {
            ereport!(
                ERROR,
                errmsg!(
                    "collations are not supported by type {}",
                    cstr_fmt!(format_type_be((*typtup).oid))
                )
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition */
            );
        }
    }

    ReleaseSysCache(ctype);
}

/*
 * transformCreateSchemaStmtElements -
 *    analyzes the elements of a CREATE SCHEMA statement
 *
 * Split the schema element list from a CREATE SCHEMA statement into
 * individual commands and place them in the result list in an order
 * such that there are no forward references (e.g. GRANT to a table
 * created later in the list). Note that the logic we use for determining
 * forward references is presently quite incomplete.
 *
 * "schemaName" is the name of the schema that will be used for the creation
 * of the objects listed, that may be compiled from the schema name defined
 * in the statement or a role specification.
 *
 * SQL also allows constraints to make forward references, so thumb through
 * the table columns and move forward references to a posterior alter-table
 * command.
 *
 * The result is a list of parse nodes that still need to be analyzed ---
 * but we can't analyze the later commands until we've executed the earlier
 * ones, because of possible inter-object references.
 *
 * Note: this breaks the rules a little bit by modifying schema-name fields
 * within passed-in structs.  However, the transformation would be the same
 * if done over, so it should be all right to scribble on the input to this
 * extent.
 */
pub unsafe fn transformCreateSchemaStmtElements(
    schemaElts: *mut List,
    schemaName: *const c_char,
) -> *mut List {
    let mut cxt: CreateSchemaStmtContext = core::mem::zeroed();
    let mut result: *mut List;

    cxt.schemaname = schemaName;
    cxt.sequences = NIL;
    cxt.tables = NIL;
    cxt.views = NIL;
    cxt.indexes = NIL;
    cxt.triggers = NIL;
    cxt.grants = NIL;

    /*
     * Run through each schema element in the schema element list. Separate
     * statements by type, and do preliminary analysis.
     */
    foreach!(elements, schemaElts, {
        let element: *mut Node = lfirst(current_cell!(elements)) as *mut Node;

        match nodeTag(element) {
            T_CreateSeqStmt => {
                let elp: *mut CreateSeqStmt = element as *mut CreateSeqStmt;
                setSchemaName(cxt.schemaname, &mut (*(*elp).sequence).schemaname);
                cxt.sequences = lappend(cxt.sequences, element as *mut c_void);
            }
            T_CreateStmt => {
                let elp: *mut CreateStmt = element as *mut CreateStmt;
                setSchemaName(cxt.schemaname, &mut (*(*elp).relation).schemaname);
                /*
                 * XXX todo: deal with constraints
                 */
                cxt.tables = lappend(cxt.tables, element as *mut c_void);
            }
            T_ViewStmt => {
                let elp: *mut ViewStmt = element as *mut ViewStmt;
                setSchemaName(cxt.schemaname, &mut (*(*elp).view).schemaname);
                /*
                 * XXX todo: deal with references between views
                 */
                cxt.views = lappend(cxt.views, element as *mut c_void);
            }
            T_IndexStmt => {
                let elp: *mut IndexStmt = element as *mut IndexStmt;
                setSchemaName(cxt.schemaname, &mut (*(*elp).relation).schemaname);
                cxt.indexes = lappend(cxt.indexes, element as *mut c_void);
            }
            T_CreateTrigStmt => {
                let elp: *mut CreateTrigStmt = element as *mut CreateTrigStmt;
                setSchemaName(cxt.schemaname, &mut (*(*elp).relation).schemaname);
                cxt.triggers = lappend(cxt.triggers, element as *mut c_void);
            }
            T_GrantStmt => {
                cxt.grants = lappend(cxt.grants, element as *mut c_void);
            }
            _ => {
                elog!(ERROR, "unrecognized node type: {}", nodeTag(element) as c_int);
            }
        }
    });

    result = NIL;
    result = list_concat(result, cxt.sequences);
    result = list_concat(result, cxt.tables);
    result = list_concat(result, cxt.views);
    result = list_concat(result, cxt.indexes);
    result = list_concat(result, cxt.triggers);
    result = list_concat(result, cxt.grants);

    result
}

/*
 * setSchemaName
 *        Set or check schema name in an element of a CREATE SCHEMA command
 */
unsafe fn setSchemaName(context_schema: *const c_char, stmt_schema_name: *mut *mut c_char) {
    if (*stmt_schema_name).is_null() {
        *stmt_schema_name = context_schema as *mut c_char; /* unconstify */
    } else if strcmp(context_schema, *stmt_schema_name) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "CREATE specifies a schema ({}) different from the one being created ({})",
                cstr_fmt!(*stmt_schema_name),
                cstr_fmt!(context_schema)
            )
            /* C also: errcode(ERRCODE_INVALID_SCHEMA_DEFINITION) */
        );
    }
}

/*
 * transformPartitionCmd
 *        Analyze the ATTACH/DETACH PARTITION command
 *
 * In case of the ATTACH PARTITION command, cxt->partbound is set to the
 * transformed value of cmd->bound.
 */
unsafe fn transformPartitionCmd(cxt: *mut CreateStmtContext, cmd: *mut PartitionCmd) {
    let parentRel: Relation = (*cxt).rel;

    match (*(*parentRel).rd_rel).relkind as u8 {
        RELKIND_PARTITIONED_TABLE => {
            /* transform the partition bound, if any */
            debug_assert!(!RelationGetPartitionKey(parentRel).is_null());
            if !(*cmd).bound.is_null() {
                (*cxt).partbound =
                    transformPartitionBound((*cxt).pstate, parentRel, (*cmd).bound);
            }
        }
        RELKIND_PARTITIONED_INDEX => {
            /*
             * A partitioned index cannot have a partition bound set.  ALTER
             * INDEX prevents that with its grammar, but not ALTER TABLE.
             */
            if !(*cmd).bound.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "\"{}\" is not a partitioned table",
                        cstr_fmt!(RelationGetRelationName(parentRel))
                    )
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }
        }
        RELKIND_RELATION => {
            /* the table must be partitioned */
            ereport!(
                ERROR,
                errmsg!(
                    "table \"{}\" is not partitioned",
                    cstr_fmt!(RelationGetRelationName(parentRel))
                )
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            );
        }
        RELKIND_INDEX => {
            /* the index must be partitioned */
            ereport!(
                ERROR,
                errmsg!(
                    "index \"{}\" is not partitioned",
                    cstr_fmt!(RelationGetRelationName(parentRel))
                )
                /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
            );
        }
        _ => {
            /* parser shouldn't let this case through */
            elog!(
                ERROR,
                "\"{}\" is not a partitioned table or index",
                cstr_fmt!(RelationGetRelationName(parentRel))
            );
        }
    }
}

/*
 * transformPartitionBound
 *
 * Transform a partition bound specification
 */
pub unsafe fn transformPartitionBound_pub(
    pstate: *mut ParseState,
    parent: Relation,
    spec: *mut PartitionBoundSpec,
) -> *mut PartitionBoundSpec {
    let mut result_spec: *mut PartitionBoundSpec;
    let key: *mut PartitionKey = RelationGetPartitionKey(parent);
    let strategy: c_char = get_partition_strategy(key);
    let partnatts: c_int = get_partition_natts(key);
    let partexprs: *mut List = get_partition_exprs(key);

    /* Avoid scribbling on input */
    result_spec = copyObject(spec as *const c_void) as *mut PartitionBoundSpec;

    if (*spec).is_default {
        /*
         * Hash partitioning does not support a default partition; there's no
         * use case for it (since the set of partitions to create is perfectly
         * defined), and if users do get into it accidentally, it's hard to
         * back out from it afterwards.
         */
        if strategy == PARTITION_STRATEGY_HASH {
            ereport!(
                ERROR,
                errmsg!("a hash-partitioned table may not have a default partition")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }

        /*
         * In case of the default partition, parser had no way to identify the
         * partition strategy. Assign the parent's strategy to the default
         * partition bound spec.
         */
        (*result_spec).strategy = strategy;

        return result_spec;
    }

    if strategy == PARTITION_STRATEGY_HASH {
        if (*spec).strategy != PARTITION_STRATEGY_HASH {
            ereport!(
                ERROR,
                errmsg!("invalid bound specification for a hash partition")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 *         parser_errposition(pstate, exprLocation((Node *) spec)) */
            );
        }

        if (*spec).modulus <= 0 {
            ereport!(
                ERROR,
                errmsg!("modulus for hash partition must be an integer value greater than zero")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }

        debug_assert!((*spec).remainder >= 0);

        if (*spec).remainder >= (*spec).modulus {
            ereport!(
                ERROR,
                errmsg!("remainder for hash partition must be less than modulus")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }
    } else if strategy == PARTITION_STRATEGY_LIST {
        let colname: *mut c_char;
        let coltype: Oid;
        let coltypmod: int32;
        let partcollation: Oid;

        if (*spec).strategy != PARTITION_STRATEGY_LIST {
            ereport!(
                ERROR,
                errmsg!("invalid bound specification for a list partition")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 *         parser_errposition(pstate, exprLocation((Node *) spec)) */
            );
        }

        /* Get the only column's name in case we need to output an error */
        if *partkey_partattrs_ptr(key).offset(0) != 0 {
            colname = get_attname(
                RelationGetRelid(parent),
                *partkey_partattrs_ptr(key).offset(0),
                false,
            );
        } else {
            colname = deparse_expression(
                linitial(partexprs) as *mut Node,
                deparse_context_for(RelationGetRelationName(parent), RelationGetRelid(parent)),
                false,
                false,
            );
        }
        /* Need its type data too */
        coltype = get_partition_col_typid(key, 0);
        coltypmod = get_partition_col_typmod(key, 0);
        partcollation = get_partition_col_collation(key, 0);

        (*result_spec).listdatums = NIL;
        foreach!(cell, (*spec).listdatums, {
            let expr: *mut Node = lfirst(current_cell!(cell)) as *mut Node;
            let value: *mut Const;
            let mut duplicate: bool = false;

            value = transformPartitionBoundValue(
                pstate,
                expr,
                colname,
                coltype,
                coltypmod,
                partcollation,
            );

            /* Don't add to the result if the value is a duplicate */
            foreach!(cell2, (*result_spec).listdatums, {
                let value2: *mut Const =
                    lfirst_node!(Const, T_Const, current_cell!(cell2));

                if equal(value as *const c_void, value2 as *const c_void) {
                    duplicate = true;
                    break;
                }
            });
            if duplicate {
                continue;
            }

            (*result_spec).listdatums =
                lappend((*result_spec).listdatums, value as *mut c_void);
        });
    } else if strategy == PARTITION_STRATEGY_RANGE {
        if (*spec).strategy != PARTITION_STRATEGY_RANGE {
            ereport!(
                ERROR,
                errmsg!("invalid bound specification for a range partition")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 *         parser_errposition(pstate, exprLocation((Node *) spec)) */
            );
        }

        if list_length((*spec).lowerdatums) != partnatts {
            ereport!(
                ERROR,
                errmsg!("FROM must specify exactly one value per partitioning column")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }
        if list_length((*spec).upperdatums) != partnatts {
            ereport!(
                ERROR,
                errmsg!("TO must specify exactly one value per partitioning column")
                /* C also: errcode(ERRCODE_INVALID_TABLE_DEFINITION) */
            );
        }

        /*
         * Convert raw parse nodes into PartitionRangeDatum nodes and perform
         * any necessary validation.
         */
        (*result_spec).lowerdatums =
            transformPartitionRangeBounds(pstate, (*spec).lowerdatums, parent);
        (*result_spec).upperdatums =
            transformPartitionRangeBounds(pstate, (*spec).upperdatums, parent);
    } else {
        elog!(ERROR, "unexpected partition strategy: {}", strategy as c_int);
    }

    result_spec
}

// PartitionKey partattrs field accessor stub
impl PartitionKey {
    /// TODO(pg-port): not yet real
    unsafe fn partattrs_field(&self) -> *const i16 {
        core::ptr::null()
    }
}
// Access partattrs as pointer -- stub
unsafe fn partkey_partattrs_ptr(key: *mut PartitionKey) -> *mut i16 {
    unimplemented!("partkey_partattrs_ptr not yet ported")
}

/*
 * transformPartitionRangeBounds
 *        This converts the expressions for range partition bounds from the raw
 *        grammar representation to PartitionRangeDatum structs
 */
unsafe fn transformPartitionRangeBounds(
    pstate: *mut ParseState,
    blist: *mut List,
    parent: Relation,
) -> *mut List {
    let mut result: *mut List = NIL;
    let key: *mut PartitionKey = RelationGetPartitionKey(parent);
    let partexprs: *mut List = get_partition_exprs(key);
    let mut j: c_int = 0;

    foreach!(lc, blist, {
        let expr: *mut Node = lfirst(current_cell!(lc)) as *mut Node;
        let mut prd: *mut PartitionRangeDatum = core::ptr::null_mut();

        let i: c_int = foreach_current_index(current_cell!(lc));

        /*
         * Infinite range bounds -- "minvalue" and "maxvalue" -- get passed in
         * as ColumnRefs.
         */
        if IsA!(expr, T_ColumnRef) {
            let cref: *mut ColumnRef = expr as *mut ColumnRef;
            let mut cname: *mut c_char = core::ptr::null_mut();

            /*
             * There should be a single field named either "minvalue" or
             * "maxvalue".
             */
            if list_length((*cref).fields) == 1
                && IsA!(linitial((*cref).fields) as *mut Node, T_String)
            {
                cname = strVal!(linitial((*cref).fields));
            }

            if cname.is_null() {
                /*
                 * ColumnRef is not in the desired single-field-name form. For
                 * consistency between all partition strategies, let the
                 * expression transformation report any errors rather than
                 * doing it ourselves.
                 */
            } else if strcmp(cname, b"minvalue\0".as_ptr() as *const c_char) == 0 {
                prd = makeNode!(PartitionRangeDatum, T_PartitionRangeDatum);
                (*prd).kind = PARTITION_RANGE_DATUM_MINVALUE;
                (*prd).value = core::ptr::null_mut();
            } else if strcmp(cname, b"maxvalue\0".as_ptr() as *const c_char) == 0 {
                prd = makeNode!(PartitionRangeDatum, T_PartitionRangeDatum);
                (*prd).kind = PARTITION_RANGE_DATUM_MAXVALUE;
                (*prd).value = core::ptr::null_mut();
            }
        }

        if prd.is_null() {
            let colname: *mut c_char;
            let coltype: Oid;
            let coltypmod: int32;
            let partcollation: Oid;
            let value: *mut Const;

            /* Get the column's name in case we need to output an error */
            if get_partkey_partattrs_at(key, i as usize) != 0 {
                colname = get_attname(
                    RelationGetRelid(parent),
                    get_partkey_partattrs_at(key, i as usize),
                    false,
                );
            } else {
                colname = deparse_expression(
                    list_nth(partexprs, j as c_int) as *mut Node,
                    deparse_context_for(
                        RelationGetRelationName(parent),
                        RelationGetRelid(parent),
                    ),
                    false,
                    false,
                );
                j += 1;
            }

            /* Need its type data too */
            coltype = get_partition_col_typid(key, i);
            coltypmod = get_partition_col_typmod(key, i);
            partcollation = get_partition_col_collation(key, i);

            value = transformPartitionBoundValue(
                pstate,
                expr,
                colname,
                coltype,
                coltypmod,
                partcollation,
            );
            if (*value).constisnull {
                ereport!(
                    ERROR,
                    errmsg!("cannot specify NULL in range bound")
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }
            prd = makeNode!(PartitionRangeDatum, T_PartitionRangeDatum);
            (*prd).kind = PARTITION_RANGE_DATUM_VALUE;
            (*prd).value = value as *mut Node;
        }

        (*prd).location = exprLocation(expr);

        result = lappend(result, prd as *mut c_void);
    });

    /*
     * Once we see MINVALUE or MAXVALUE for one column, the remaining columns
     * must be the same.
     */
    validateInfiniteBounds(pstate, result);

    result
}

unsafe fn get_partkey_partattrs_at(key: *mut PartitionKey, idx: usize) -> AttrNumber {
    unimplemented!("get_partkey_partattrs_at not yet ported")
}

unsafe fn foreach_current_index(lc: *mut ListCell) -> c_int {
    unimplemented!("foreach_current_index not yet ported")
}

/*
 * validateInfiniteBounds
 *
 * Check that a MAXVALUE or MINVALUE specification in a partition bound is
 * followed only by more of the same.
 */
unsafe fn validateInfiniteBounds(pstate: *mut ParseState, blist: *mut List) {
    let mut kind: PartitionRangeDatumKind = PARTITION_RANGE_DATUM_VALUE;

    foreach!(lc, blist, {
        let prd: *mut PartitionRangeDatum =
            lfirst_node!(PartitionRangeDatum, T_PartitionRangeDatum, current_cell!(lc));

        if kind == (*prd).kind {
            continue;
        }

        match kind {
            PARTITION_RANGE_DATUM_VALUE => {
                kind = (*prd).kind;
            }
            PARTITION_RANGE_DATUM_MAXVALUE => {
                ereport!(
                    ERROR,
                    errmsg!("every bound following MAXVALUE must also be MAXVALUE")
                    /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
                     *         parser_errposition(pstate, exprLocation((Node *) prd)) */
                );
            }
            PARTITION_RANGE_DATUM_MINVALUE => {
                ereport!(
                    ERROR,
                    errmsg!("every bound following MINVALUE must also be MINVALUE")
                    /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
                     *         parser_errposition(pstate, exprLocation((Node *) prd)) */
                );
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
    });
}

/*
 * Transform one entry in a partition bound spec, producing a constant.
 */
unsafe fn transformPartitionBoundValue(
    pstate: *mut ParseState,
    val: *mut Node,
    colName: *const c_char,
    colType: Oid,
    colTypmod: int32,
    partCollation: Oid,
) -> *mut Const {
    let mut value: *mut Node;

    /* Transform raw parsetree */
    value = transformExpr(pstate, val, EXPR_KIND_PARTITION_BOUND);

    /*
     * transformExpr() should have already rejected column references,
     * subqueries, aggregates, window functions, and SRFs, based on the
     * EXPR_KIND_ of a partition bound expression.
     */
    debug_assert!(!contain_var_clause(value));

    /*
     * Coerce to the correct type.  This might cause an explicit coercion step
     * to be added on top of the expression, which must be evaluated before
     * returning the result to the caller.
     */
    value = coerce_to_target_type(
        pstate,
        value,
        exprType(value),
        colType,
        colTypmod,
        COERCION_ASSIGNMENT,
        COERCE_IMPLICIT_CAST,
        -1,
    );

    if value.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "specified value cannot be cast to type {} for column \"{}\"",
                cstr_fmt!(format_type_be(colType)),
                cstr_fmt!(colName)
            )
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
             *         parser_errposition(pstate, exprLocation(val)) */
        );
    }

    /*
     * Evaluate the expression, if needed, assigning the partition key's data
     * type and collation to the resulting Const node.
     */
    if !IsA!(value, T_Const) {
        assign_expr_collations(pstate, value);
        value = expression_planner(value as *mut crate::nodes::primnodes::Expr) as *mut Node;
        value = evaluate_expr(
            value as *mut crate::nodes::primnodes::Expr,
            colType,
            colTypmod,
            partCollation,
        ) as *mut Node;
        if !IsA!(value, T_Const) {
            elog!(ERROR, "could not evaluate partition bound expression");
        }
    } else {
        /*
         * If the expression is already a Const, as is often the case, we can
         * skip the rather expensive steps above.  But we still have to insert
         * the right collation, since coerce_to_target_type doesn't handle
         * that.
         */
        (*(value as *mut Const)).constcollid = partCollation;
    }

    /*
     * Attach original expression's parse location to the Const, so that
     * that's what will be reported for any later errors related to this
     * partition bound.
     */
    (*(value as *mut Const)).location = exprLocation(val);

    value as *mut Const
}

// Const_ stub alias -- actual type lives in primnodes
// used above as *mut Const
// partattrs access stubs for PartitionKey  (fields are opaque here)
// These are all TODO(pg-port) stubs since PartitionKey is not yet translated.

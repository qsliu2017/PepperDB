/*-------------------------------------------------------------------------
 *
 * ruleutils.c
 *    Functions to convert stored expressions/querytrees back to
 *    source text
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/ruleutils.c
 *
 *-------------------------------------------------------------------------
 */
//! ruleutils.rs
//!   Functions to convert stored expressions/querytrees back to source text.
//! Translated 1:1 from postgres/src/backend/utils/adt/ruleutils.c
//!
//! #include mapping (selected):
//!   access/htup_details.h    -> crate::access::htup_details::*
//!   catalog/pg_*             -> crate::catalog::*
//!   nodes/pathnodes.h        -> crate::nodes::pathnodes::*
//!   utils/lsyscache.h        -> STUB (lsyscache not yet ported)
//!   utils/syscache.h         -> STUB
//!   executor/spi.h           -> STUB
//!   rewrite/rewriteHandler.h -> STUB

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(clippy::all)]

use crate::prelude::*;
use std::ffi::{CStr, CString};
use std::ptr;

use crate::{
    PG_GETARG_OID, PG_GETARG_BOOL, PG_GETARG_INT32, PG_GETARG_TEXT_PP,
    PG_RETURN_NULL, PG_RETURN_TEXT_P, PG_RETURN_NAME, PG_RETURN_DATUM,
};
use crate::lib::stringinfo::{
    StringInfoData, StringInfo, initStringInfo, appendStringInfo,
    appendStringInfoString, appendStringInfoChar, appendBinaryStringInfo,
    resetStringInfo,
};
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::*;
use crate::utils::fmgr::{FunctionCallInfo, Datum};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTupleData;
use crate::miscadmin::check_stack_depth;

// ----------
// Pretty formatting constants
// ----------

// Indent counts
const PRETTYINDENT_STD: i32 = 8;
const PRETTYINDENT_JOIN: i32 = 4;
const PRETTYINDENT_VAR: i32 = 4;

const PRETTYINDENT_LIMIT: i32 = 40; /* wrap limit */

// Pretty flags
const PRETTYFLAG_PAREN: i32 = 0x0001;
const PRETTYFLAG_INDENT: i32 = 0x0002;
const PRETTYFLAG_SCHEMA: i32 = 0x0004;

// Standard conversion of a "bool pretty" option to detailed flags
#[inline]
fn GET_PRETTY_FLAGS(pretty: bool) -> i32 {
    if pretty {
        PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA
    } else {
        PRETTYFLAG_INDENT
    }
}

// Default line length for pretty-print wrapping: 0 means wrap always
const WRAP_COLUMN_DEFAULT: i32 = 0;

// macros to test if pretty action needed
#[inline]
fn PRETTY_PAREN(context: &deparse_context) -> bool {
    (context.prettyFlags & PRETTYFLAG_PAREN) != 0
}
#[inline]
fn PRETTY_INDENT(context: &deparse_context) -> bool {
    (context.prettyFlags & PRETTYFLAG_INDENT) != 0
}
#[inline]
fn PRETTY_SCHEMA(context: &deparse_context) -> bool {
    (context.prettyFlags & PRETTYFLAG_SCHEMA) != 0
}

// ----------
// Local data types
// ----------

// Context info needed for invoking a recursive querytree display routine
#[derive(Clone)]
pub struct deparse_context {
    pub buf: *mut StringInfoData,     /* output buffer to append to */
    pub namespaces: *mut List,         /* List of deparse_namespace nodes */
    pub resultDesc: TupleDesc,         /* if top level of a view, the view's tupdesc */
    pub targetList: *mut List,         /* Current query level's SELECT targetlist */
    pub windowClause: *mut List,       /* Current query level's WINDOW clause */
    pub prettyFlags: i32,              /* enabling of pretty-print functions */
    pub wrapColumn: i32,               /* max line length, or -1 for no limit */
    pub indentLevel: i32,              /* current indent level for pretty-print */
    pub varprefix: bool,               /* true to print prefixes on Vars */
    pub colNamesVisible: bool,         /* do we care about output column names? */
    pub inGroupBy: bool,               /* deparsing GROUP BY clause? */
    pub varInOrderBy: bool,            /* deparsing simple Var in ORDER BY? */
    pub appendparents: *mut Bitmapset, /* if not null, map child Vars of these relids back to the parent rel */
}

impl Default for deparse_context {
    fn default() -> Self {
        deparse_context {
            buf: ptr::null_mut(),
            namespaces: ptr::null_mut(),
            resultDesc: ptr::null_mut(),
            targetList: ptr::null_mut(),
            windowClause: ptr::null_mut(),
            prettyFlags: 0,
            wrapColumn: 0,
            indentLevel: 0,
            varprefix: false,
            colNamesVisible: false,
            inGroupBy: false,
            varInOrderBy: false,
            appendparents: ptr::null_mut(),
        }
    }
}

/*
 * Each level of query context around a subtree needs a level of Var namespace.
 * A Var having varlevelsup=N refers to the N'th item (counting from 0) in
 * the current context's namespaces list.
 *
 * rtable is the list of actual RTEs from the Query or PlannedStmt.
 * rtable_names holds the alias name to be used for each RTE (either a C
 * string, or NULL for nameless RTEs such as unnamed joins).
 * rtable_columns holds the column alias names to be used for each RTE.
 *
 * subplans is a list of Plan trees for SubPlans and CTEs (it's only used
 * in the PlannedStmt case).
 * ctes is a list of CommonTableExpr nodes (only used in the Query case).
 * appendrels, if not null (it's only used in the PlannedStmt case), is an
 * array of AppendRelInfo nodes, indexed by child relid.  We use that to map
 * child-table Vars to their inheritance parents.
 *
 * In some cases we need to make names of merged JOIN USING columns unique
 * across the whole query, not only per-RTE.  If so, unique_using is true
 * and using_names is a list of C strings representing names already assigned
 * to USING columns.
 *
 * When deparsing plan trees, there is always just a single item in the
 * deparse_namespace list (since a plan tree never contains Vars with
 * varlevelsup > 0).  We store the Plan node that is the immediate
 * parent of the expression to be deparsed, as well as a list of that
 * Plan's ancestors.  In addition, we store its outer and inner subplan nodes,
 * as well as their targetlists, and the index tlist if the current plan node
 * might contain INDEX_VAR Vars.  (These fields could be derived on-the-fly
 * from the current Plan node, but it seems notationally clearer to set them
 * up as separate fields.)
 */
#[repr(C)]
pub struct deparse_namespace {
    pub rtable: *mut List,           /* List of RangeTblEntry nodes */
    pub rtable_names: *mut List,     /* Parallel list of names for RTEs */
    pub rtable_columns: *mut List,   /* Parallel list of deparse_columns structs */
    pub subplans: *mut List,         /* List of Plan trees for SubPlans */
    pub ctes: *mut List,             /* List of CommonTableExpr nodes */
    pub appendrels: *mut *mut AppendRelInfo, /* Array of AppendRelInfo nodes, or NULL */
    pub ret_old_alias: *mut c_char,  /* alias for OLD in RETURNING list */
    pub ret_new_alias: *mut c_char,  /* alias for NEW in RETURNING list */
    /* Workspace for column alias assignment: */
    pub unique_using: bool,          /* Are we making USING names globally unique */
    pub using_names: *mut List,      /* List of assigned names for USING columns */
    /* Remaining fields are used only when deparsing a Plan tree: */
    pub plan: *mut Plan,             /* immediate parent of current expression */
    pub ancestors: *mut List,        /* ancestors of plan */
    pub outer_plan: *mut Plan,       /* outer subnode, or NULL if none */
    pub inner_plan: *mut Plan,       /* inner subnode, or NULL if none */
    pub outer_tlist: *mut List,      /* referent for OUTER_VAR Vars */
    pub inner_tlist: *mut List,      /* referent for INNER_VAR Vars */
    pub index_tlist: *mut List,      /* referent for INDEX_VAR Vars */
    /* Special namespace representing a function signature: */
    pub funcname: *mut c_char,
    pub numargs: i32,
    pub argnames: *mut *mut c_char,
}

impl Default for deparse_namespace {
    fn default() -> Self {
        // SAFETY: all-zeros is valid for this struct (all pointers null, bools false, ints 0)
        unsafe { core::mem::zeroed() }
    }
}

/*
 * Per-relation data about column alias names.
 *
 * Selecting aliases is unreasonably complicated because of the need to dump
 * rules/views whose underlying tables may have had columns added, deleted, or
 * renamed since the query was parsed.
 */
#[repr(C)]
pub struct deparse_columns {
    /*
     * colnames is an array containing column aliases to use for columns that
     * existed when the query was parsed.  Dropped columns have NULL entries.
     * This array can be directly indexed by varattno to get a Var's name.
     */
    pub num_cols: i32,           /* length of colnames[] array */
    pub colnames: *mut *mut c_char, /* array of C strings and NULLs */

    /*
     * new_colnames is an array containing column aliases to use for columns
     * that would exist if the query was re-parsed against the current
     * definitions of its base tables.
     */
    pub num_new_cols: i32,          /* length of new_colnames[] array */
    pub new_colnames: *mut *mut c_char, /* array of C strings */
    pub is_new_col: *mut bool,      /* array of bool flags */

    /* This flag tells whether we should actually print a column alias list */
    pub printaliases: bool,

    /* This list has all names used as USING names in joins above this RTE */
    pub parentUsing: *mut List,     /* names assigned to parent merged columns */

    /*
     * If this struct is for a JOIN RTE, we fill these fields during the
     * set_using_names() pass to describe its relationship to its child RTEs.
     */
    pub leftrti: i32,               /* rangetable index of left child */
    pub rightrti: i32,              /* rangetable index of right child */
    pub leftattnos: *mut i32,       /* left-child varattnos of join cols, or 0 */
    pub rightattnos: *mut i32,      /* right-child varattnos of join cols, or 0 */
    pub usingNames: *mut List,      /* names assigned to merged columns */

    /*
     * Hash table holding copies of all the strings appearing in this struct's
     * colnames, new_colnames, and parentUsing.  We use a hash table only for
     * sufficiently wide relations, and only during the colname-assignment
     * functions set_relation_column_names and set_join_column_names;
     * otherwise, names_hash is NULL.
     */
    pub names_hash: *mut HTAB,      /* entries are just strings */
}

impl Default for deparse_columns {
    fn default() -> Self {
        unsafe { core::mem::zeroed() }
    }
}

/* This macro is analogous to rt_fetch(), but for deparse_columns structs */
#[inline]
unsafe fn deparse_columns_fetch(rangetable_index: i32, dpns: *mut deparse_namespace) -> *mut deparse_columns {
    list_nth((*dpns).rtable_columns, rangetable_index - 1) as *mut deparse_columns
}

/*
 * Entry in set_rtable_names' hash table
 */
#[repr(C)]
pub struct NameHashEntry {
    pub name: [c_char; NAMEDATALEN as usize], /* Hash key --- must be first */
    pub counter: i32,                          /* Largest addition used so far for name */
}

/* Callback signature for resolve_special_varno() */
pub type rsv_callback = unsafe fn(node: *mut Node, context: *mut deparse_context, callback_arg: *mut c_void);

// ----------
// Global data
// ----------
static mut plan_getrulebyoid: SPIPlanPtr = ptr::null_mut();
static query_getrulebyoid: &str = "SELECT * FROM pg_catalog.pg_rewrite WHERE oid = $1";
static mut plan_getviewrule: SPIPlanPtr = ptr::null_mut();
static query_getviewrule: &str = "SELECT * FROM pg_catalog.pg_rewrite WHERE ev_class = $1 AND rulename = $2";

/* GUC parameters */
pub static mut quote_all_identifiers: bool = false;

#[inline]
fn only_marker(rte: *const RangeTblEntry) -> &'static str {
    unsafe {
        if (*rte).inh { "" } else { "ONLY " }
    }
}

// ----------
// Forward declarations (stubs for unported dependencies)
// ----------

// TODO(pg-port): SPI stubs
extern "C" {
    fn SPI_connect() -> i32;
    fn SPI_finish() -> i32;
    fn SPI_prepare(query: *const c_char, nargs: i32, argtypes: *mut Oid) -> SPIPlanPtr;
    fn SPI_keepplan(plan: SPIPlanPtr);
    fn SPI_execute_plan(plan: SPIPlanPtr, values: *mut Datum, nulls: *const c_char, readonly: bool, count: i64) -> i32;
    fn SPI_fnumber(tupdesc: TupleDesc, fname: *const c_char) -> i32;
    fn SPI_getbinval(tuple: HeapTuple, tupdesc: TupleDesc, fnumber: i32, isnull: *mut bool) -> Datum;
    fn SPI_getvalue(tuple: HeapTuple, tupdesc: TupleDesc, fnumber: i32) -> *mut c_char;
    static SPI_processed: u64;
    static SPI_tuptable: *mut SPITupleTable;
}
// TODO(pg-port): catalog/syscache stubs
extern "C" {
    fn SearchSysCache1(cacheId: i32, key1: Datum) -> HeapTuple;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn SysCacheGetAttr(cacheId: i32, tuple: HeapTuple, attnum: AttrNumber, isnull: *mut bool) -> Datum;
    fn SysCacheGetAttrNotNull(cacheId: i32, tuple: HeapTuple, attnum: AttrNumber) -> Datum;
    fn heap_attisnull(tup: HeapTuple, attnum: i32, tupdesc: TupleDesc) -> bool;
}
// TODO(pg-port): access/table stubs
extern "C" {
    fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation;
    fn table_close(relation: Relation, lockmode: LOCKMODE);
    fn relation_open(relationId: Oid, lockmode: LOCKMODE) -> Relation;
    fn relation_close(relation: Relation, lockmode: LOCKMODE);
    fn try_relation_open(relationId: Oid, lockmode: LOCKMODE) -> Relation;
}
// TODO(pg-port): access/heapam/sysan stubs
extern "C" {
    fn systable_beginscan(relation: Relation, indexId: Oid, indexOK: bool, snapshot: Snapshot, nkeys: i32, key: *mut ScanKeyData) -> SysScanDesc;
    fn systable_getnext(scandesc: SysScanDesc) -> HeapTuple;
    fn systable_endscan(scandesc: SysScanDesc);
}
// TODO(pg-port): utils/lsyscache stubs
extern "C" {
    fn get_attname(relid: Oid, attnum: AttrNumber, missing_ok: bool) -> *mut c_char;
    fn get_atttypetypmodcoll(relid: Oid, attnum: AttrNumber, typid: *mut Oid, typmod: *mut i32, collid: *mut Oid);
    fn get_attoptions(relid: Oid, attnum: AttrNumber) -> Datum;
    fn get_rel_name(relid: Oid) -> *mut c_char;
    fn get_rel_relkind(relid: Oid) -> c_char;
    fn get_rel_tablespace(relid: Oid) -> Oid;
    fn get_attnum(relid: Oid, attname: *const c_char) -> AttrNumber;
    fn get_language_name(langoid: Oid, missing_ok: bool) -> *mut c_char;
    fn get_namespace_name_or_temp(nspid: Oid) -> *mut c_char;
    fn get_tablespace_name(spcid: Oid) -> *mut c_char;
}
// TODO(pg-port): utils/builtins stubs
extern "C" {
    fn format_type_be(type_oid: Oid) -> *mut c_char;
    fn text_to_cstring(t: *const text) -> *mut c_char;
    fn cstring_to_text(s: *const c_char) -> *mut text;
    fn cstring_to_text_with_len(s: *const c_char, len: i32) -> *mut text;
    fn makeRangeVarFromNameList(names: *mut List) -> *mut RangeVar;
    fn textToQualifiedNameList(textval: *mut text) -> *mut List;
    fn RangeVarGetRelid(relation: *mut RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> Oid;
}
// TODO(pg-port): nodes/makefuncs stubs
extern "C" {
    fn makeAlias(aliasname: *const c_char, colnames: *mut List) -> *mut Alias;
    fn makeNode_RangeTblEntry() -> *mut RangeTblEntry;
}
// TODO(pg-port): nodes/nodeFuncs stubs
extern "C" {
    fn exprType(expr: *const Node) -> Oid;
    fn exprCollation(expr: *const Node) -> Oid;
    fn nodeTag(node: *const Node) -> NodeTag;
    fn stringToNode(str_: *const c_char) -> *mut c_void;
    fn nodeToString(obj: *const c_void) -> *mut c_char;
}
// TODO(pg-port): parser/parsetree stubs
extern "C" {
    fn rt_fetch(rtindex: i32, rtable: *mut List) -> *mut RangeTblEntry;
    fn get_sortgroupref_tle(tleSortGroupRef: Index, targetList: *mut List) -> *mut TargetEntry;
}
// TODO(pg-port): rewrite stubs
extern "C" {
    fn AcquireRewriteLocks(parsetree: *mut Query, forExecute: bool, forUpdatePushedDown: bool);
    fn getInsertSelectQuery(parsetree: *mut Query, subquery_ptr: *mut *mut Query) -> *mut Query;
}
// TODO(pg-port): optimizer stubs
extern "C" {
    fn pull_varnos(root: *mut PlannerInfo, node: *mut Node) -> *mut Bitmapset;
    fn get_partition_qual_relid(relid: Oid) -> *mut Expr;
    fn expandRTE(rte: *mut RangeTblEntry, rtindex: i32, sublevels_up: i32, returning_whichvar: i32, location: i32, include_dropped: bool, colnames: *mut *mut List, colvars: *mut *mut List);
    fn flatten_group_exprs(root: *mut PlannerInfo, query: *mut Query, node: *mut Node) -> *mut Node;
}
// TODO(pg-port): miscadmin/catalog stubs
extern "C" {
    fn ObjectIdGetDatum(oid: Oid) -> Datum;
    fn DatumGetObjectId(d: Datum) -> Oid;
    fn DatumGetPointer(d: Datum) -> *mut c_void;
    fn DatumGetChar(d: Datum) -> c_char;
    fn DatumGetBool(d: Datum) -> bool;
    fn DatumGetName(d: Datum) -> *mut NameData;
    fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType;
    fn DatumGetInt16(d: Datum) -> i16;
    fn DatumGetInt32(d: Datum) -> i32;
    fn DatumGetByteaPP(d: Datum) -> *mut bytea;
    fn Int32GetDatum(i: i32) -> Datum;
    fn PointerGetDatum(p: *const c_void) -> Datum;
    fn CStringGetDatum(p: *const c_char) -> Datum;
    fn DirectFunctionCall1(func: unsafe extern "C" fn(FunctionCallInfo) -> Datum, arg1: Datum) -> Datum;
    fn namein(fcinfo: FunctionCallInfo) -> Datum;
    fn OidIsValid(oid: Oid) -> bool;
    fn HeapTupleIsValid(tuple: HeapTuple) -> bool;
    fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void;
    fn NameStr_fn(n: *mut NameData) -> *mut c_char;
    fn fastgetattr(tup: HeapTuple, attnum: i32, tupleDesc: TupleDesc, isnull: *mut bool) -> Datum;
    fn palloc(size: usize) -> *mut c_void;
    fn palloc0(size: usize) -> *mut c_void;
    fn palloc0_array_char_p(n: usize) -> *mut *mut c_char;
    fn repalloc0_array_char_p(ptr: *mut *mut c_char, old_n: usize, new_n: usize) -> *mut *mut c_char;
    fn pfree(ptr: *mut c_void);
    fn pstrdup(s: *const c_char) -> *mut c_char;
    fn memset(s: *mut c_void, c: i32, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn sprintf(s: *mut c_char, format: *const c_char, ...) -> i32;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> i32;
    fn strchr(s: *const c_char, c: i32) -> *mut c_char;
    fn strstr(haystack: *const c_char, needle: *const c_char) -> *mut c_char;
    fn strrchr(s: *const c_char, c: i32) -> *mut c_char;
    fn strVal(v: *const c_void) -> *mut c_char;
    fn intVal(v: *const c_void) -> i32;
    fn lfirst_int_fn(lc: *mut ListCell) -> i32;
    fn lnext(l: *mut List, lc: *mut ListCell) -> *mut ListCell;
    fn list_head(l: *mut List) -> *mut ListCell;
    fn list_tail(l: *mut List) -> *mut ListCell;
    fn list_nth(l: *mut List, n: i32) -> *mut c_void;
    fn list_length(l: *const List) -> i32;
    fn list_make1(x: *mut c_void) -> *mut List;
    fn list_make2(x1: *mut c_void, x2: *mut c_void) -> *mut List;
    fn lappend(list: *mut List, datum: *mut c_void) -> *mut List;
    fn lcons(datum: *mut c_void, list: *mut List) -> *mut List;
    fn list_copy(list: *const List) -> *mut List;
    fn list_copy_tail(list: *const List, nskip: i32) -> *mut List;
    fn list_free(list: *mut List);
    fn list_delete_first(list: *mut List) -> *mut List;
    fn linitial(list: *const List) -> *mut c_void;
    fn llast(list: *const List) -> *mut c_void;
    fn forboth_begin(lc1: *mut *mut ListCell, lc2: *mut *mut ListCell, list1: *mut List, list2: *mut List);
    fn forboth_check(lc1: *mut ListCell, lc2: *mut ListCell) -> bool;
    fn forboth_next(lc1: *mut *mut ListCell, lc2: *mut *mut ListCell);
    fn lfirst(lc: *mut ListCell) -> *mut c_void;
    fn list_cell_number(l: *const List, lc: *mut ListCell) -> i32;
    fn bms_is_subset(a: *const Bitmapset, b: *const Bitmapset) -> bool;
    fn bms_make_singleton(x: i32) -> *mut Bitmapset;
    fn bms_is_empty(a: *const Bitmapset) -> bool;
    fn bms_is_member(x: i32, a: *const Bitmapset) -> bool;
    fn bms_add_member(a: *mut Bitmapset, x: i32) -> *mut Bitmapset;
    fn ScanKeyInit(entry: *mut ScanKeyData, attnum: AttrNumber, strategy: StrategyNumber, procedure: RegProcedure, argument: Datum);
    fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot;
    fn UnregisterSnapshot(snapshot: Snapshot);
    fn GetTransactionSnapshot() -> Snapshot;
    fn hash_create(tabname: *const c_char, nelem: i64, info: *const HASHCTL, flags: i32) -> *mut HTAB;
    fn hash_destroy(hashp: *mut HTAB);
    fn hash_search(hashp: *mut HTAB, keyPtr: *const c_void, action: HASHACTION, foundPtr: *mut bool) -> *mut c_void;
    fn pg_mbcliplen(mbstr: *const c_char, len: usize, limit: usize) -> usize;
    fn GetIndexAmRoutine(handler: RegProcedure) -> *mut IndexAmRoutine;
    fn deconstruct_array_builtin(array: *mut ArrayType, elmtype: Oid, elemsp: *mut *mut Datum, nullsp: *mut *mut bool, nelemsp: *mut i32);
    fn array_ref(array: *mut ArrayType, nSubscripts: i32, indx: *mut i32, arraytyplen: i32, elmlen: i32, elmbyval: bool, elmalign: c_char, isNull: *mut bool) -> Datum;
    fn accumArrayResult(astate: *mut ArrayBuildState, dvalue: Datum, disnull: bool, element_type: Oid, rcontext: MemoryContext) -> *mut ArrayBuildState;
    fn makeArrayResult(astate: *mut ArrayBuildState, rcontext: MemoryContext) -> Datum;
    fn ARR_NDIM(a: *const ArrayType) -> i32;
    fn ARR_HASNULL(a: *const ArrayType) -> bool;
    fn ARR_ELEMTYPE(a: *const ArrayType) -> Oid;
    fn ARR_DATA_PTR(a: *mut ArrayType) -> *mut c_char;
    fn ARR_DIMS(a: *const ArrayType) -> *const i32;
    fn ARR_LBOUND(a: *const ArrayType) -> *const i32;
    fn VARDATA_ANY(p: *const c_void) -> *mut c_char;
    fn CHECK_FOR_INTERRUPTS();
    fn extractNotNullColumn(constrTup: HeapTuple) -> AttrNumber;
    fn get_func_arg_info(proctup: HeapTuple, argtypes: *mut *mut Oid, argnames: *mut *mut *mut c_char, argmodes: *mut *mut c_char) -> i32;
    fn get_func_trftypes(proctup: HeapTuple, trftypes: *mut *mut Oid) -> i32;
    fn AGGKIND_IS_ORDERED_SET(kind: c_char) -> bool;
    fn GetConfigOptionFlags(name: *const c_char, missing_ok: bool) -> i32;
    fn SplitGUCList(rawstring: *mut c_char, separator: c_char, namelist: *mut *mut List) -> bool;
    fn lookup_type_cache(type_id: Oid, flags: i32) -> *mut TypeCacheEntry;
    fn RelationGetDescr(rel: Relation) -> TupleDesc;
    fn RelationGetRelationName(rel: Relation) -> *const c_char;
    fn outerPlan(plan: *mut Plan) -> *mut Plan;
    fn innerPlan(plan: *mut Plan) -> *mut Plan;
    fn IsA_fn(node: *const Node, tag: NodeTag) -> bool;
    fn castNode_fn(tag: NodeTag, node: *const Node) -> *mut c_void;
    fn IsA_List(node: *const Node) -> bool;
    fn IsA_Query(node: *const Node) -> bool;
    fn IsA_Var(node: *const Node) -> bool;
    fn IsA_Const(node: *const Node) -> bool;
    fn IsA_FuncExpr(node: *const Node) -> bool;
    fn IsA_Aggref(node: *const Node) -> bool;
    fn IsA_WindowFunc(node: *const Node) -> bool;
    fn IsA_JsonConstructorExpr(node: *const Node) -> bool;
    fn IsA_RangeTblRef(node: *const Node) -> bool;
    fn IsA_FromExpr(node: *const Node) -> bool;
    fn IsA_JoinExpr(node: *const Node) -> bool;
    fn IsA_SetOperationStmt(node: *const Node) -> bool;
    fn IsA_Append(node: *const Node) -> bool;
    fn IsA_MergeAppend(node: *const Node) -> bool;
    fn IsA_SubqueryScan(node: *const Node) -> bool;
    fn IsA_CteScan(node: *const Node) -> bool;
    fn IsA_WorkTableScan(node: *const Node) -> bool;
    fn IsA_ModifyTable(node: *const Node) -> bool;
    fn IsA_RecursiveUnion(node: *const Node) -> bool;
    fn IsA_IndexOnlyScan(node: *const Node) -> bool;
    fn IsA_ForeignScan(node: *const Node) -> bool;
    fn IsA_CustomScan(node: *const Node) -> bool;
}

// Type aliases for stubs
type SPIPlanPtr = *mut c_void;
type SPITupleTable = c_void; // stub
type HeapTuple = *mut HeapTupleData;
type Relation = *mut RelationData;
type RelationData = c_void; // stub
type Snapshot = *mut c_void;
type SysScanDesc = *mut c_void;
type ScanKeyData = c_void; // stub
type StrategyNumber = u16;
type LOCKMODE = i32;
type Plan = c_void; // stub
type PlannerInfo = c_void; // stub
type Expr = Node;
type ArrayType = c_void; // stub
type bytea = c_void; // stub
type text = c_void; // stub
type MemoryContext = *mut c_void;
type HTAB = c_void; // stub
type HASHCTL = c_void; // stub
type HASHACTION = i32; // HASH_ENTER=1, HASH_FIND=0, HASH_REMOVE=2
type ArrayBuildState = c_void; // stub
type TypeCacheEntry = c_void; // stub
type IndexAmRoutine = c_void; // stub
type Bitmapset = c_void; // stub
type AppendRelInfo = c_void; // stub
type oidvector = c_void; // stub
type int2vector = c_void; // stub
type NameData = [c_char; NAMEDATALEN as usize];

const HASH_FIND: HASHACTION = 0;
const HASH_ENTER: HASHACTION = 1;
const HASH_REMOVE: HASHACTION = 2;
const HASH_ELEM: i32 = 0x0040;
const HASH_STRINGS: i32 = 0x0400;
const HASH_CONTEXT: i32 = 0x2000;

const NAMEDATALEN: i32 = 64;
const SPI_OK_SELECT: i32 = 5;
const SPI_OK_FINISH: i32 = 1;
const GUC_LIST_QUOTE: i32 = 0x0008;

// ----------
// pg_get_ruledef         - Do it all and return a text
//                   that could be used as a statement
//                   to recreate the rule
// ----------
pub unsafe extern "C" fn pg_get_ruledef(fcinfo: FunctionCallInfo) -> Datum {
    let ruleoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let prettyFlags: i32;
    let res: *mut c_char;

    prettyFlags = PRETTYFLAG_INDENT;

    res = pg_get_ruledef_worker(ruleoid, prettyFlags);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_ruledef_ext(fcinfo: FunctionCallInfo) -> Datum {
    let ruleoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let prettyFlags: i32;
    let res: *mut c_char;

    prettyFlags = GET_PRETTY_FLAGS(pretty);

    res = pg_get_ruledef_worker(ruleoid, prettyFlags);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

unsafe fn pg_get_ruledef_worker(ruleoid: Oid, prettyFlags: i32) -> *mut c_char {
    let mut args: [Datum; 1] = [0];
    let mut nulls: [c_char; 1] = [b' ' as c_char];
    let spirc: i32;
    let ruletup: HeapTuple;
    let rulettc: TupleDesc;
    let mut buf: StringInfoData = core::mem::zeroed();

    /*
     * Do this first so that string is alloc'd in outer context not SPI's.
     */
    initStringInfo(&mut buf);

    /*
     * Connect to SPI manager
     */
    SPI_connect();

    /*
     * On the first call prepare the plan to lookup pg_rewrite. We read
     * pg_rewrite over the SPI manager instead of using the syscache to be
     * checked for read access on pg_rewrite.
     */
    if plan_getrulebyoid.is_null() {
        let mut argtypes: [Oid; 1] = [OIDOID];
        let plan: SPIPlanPtr;

        let qstr = CString::new(query_getrulebyoid).unwrap();
        plan = SPI_prepare(qstr.as_ptr(), 1, argtypes.as_mut_ptr());
        if plan.is_null() {
            let qstr2 = CString::new(query_getrulebyoid).unwrap();
            elog!(ERROR, "SPI_prepare failed for \"{}\"", query_getrulebyoid);
        }
        SPI_keepplan(plan);
        plan_getrulebyoid = plan;
    }

    /*
     * Get the pg_rewrite tuple for this rule
     */
    args[0] = ObjectIdGetDatum(ruleoid);
    nulls[0] = b' ' as c_char;
    spirc = SPI_execute_plan(plan_getrulebyoid, args.as_mut_ptr(), nulls.as_ptr(), true, 0);
    if spirc != SPI_OK_SELECT {
        elog!(ERROR, "failed to get pg_rewrite tuple for rule {}", ruleoid);
    }
    if SPI_processed != 1 {
        /*
         * There is no tuple data available here, just keep the output buffer
         * empty.
         */
    } else {
        /*
         * Get the rule's definition and put it into executor's memory
         */
        ruletup = (*(SPI_tuptable as *mut SPITupleTableReal)).vals[0] as HeapTuple;
        rulettc = (*(SPI_tuptable as *mut SPITupleTableReal)).tupdesc;
        make_ruledef(&mut buf, ruletup, rulettc, prettyFlags);
    }

    /*
     * Disconnect from SPI manager
     */
    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    if buf.len == 0 {
        return ptr::null_mut();
    }

    buf.data
}

// Stub for SPI tuple table structure
#[repr(C)]
struct SPITupleTableReal {
    tupdesc: TupleDesc,
    vals: *mut *mut HeapTupleData,
    // ... other fields
}

// ----------
// pg_get_viewdef         - Mainly the same thing, but we
//                   only return the SELECT part of a view
// ----------
pub unsafe extern "C" fn pg_get_viewdef(fcinfo: FunctionCallInfo) -> Datum {
    /* By OID */
    let viewoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let prettyFlags: i32;
    let res: *mut c_char;

    prettyFlags = PRETTYFLAG_INDENT;

    res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_viewdef_ext(fcinfo: FunctionCallInfo) -> Datum {
    /* By OID */
    let viewoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let prettyFlags: i32;
    let res: *mut c_char;

    prettyFlags = GET_PRETTY_FLAGS(pretty);

    res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_viewdef_wrap(fcinfo: FunctionCallInfo) -> Datum {
    /* By OID */
    let viewoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let wrap: i32 = PG_GETARG_INT32!(fcinfo, 1);
    let prettyFlags: i32;
    let res: *mut c_char;

    /* calling this implies we want pretty printing */
    prettyFlags = GET_PRETTY_FLAGS(true);

    res = pg_get_viewdef_worker(viewoid, prettyFlags, wrap);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_viewdef_name(fcinfo: FunctionCallInfo) -> Datum {
    /* By qualified name */
    let viewname: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let prettyFlags: i32;
    let viewrel: *mut RangeVar;
    let viewoid: Oid;
    let res: *mut c_char;

    prettyFlags = PRETTYFLAG_INDENT;

    /* Look up view name.  Can't lock it - we might not have privileges. */
    viewrel = makeRangeVarFromNameList(textToQualifiedNameList(viewname));
    viewoid = RangeVarGetRelid(viewrel, NoLock, false);

    res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_viewdef_name_ext(fcinfo: FunctionCallInfo) -> Datum {
    /* By qualified name */
    let viewname: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let prettyFlags: i32;
    let viewrel: *mut RangeVar;
    let viewoid: Oid;
    let res: *mut c_char;

    prettyFlags = GET_PRETTY_FLAGS(pretty);

    /* Look up view name.  Can't lock it - we might not have privileges. */
    viewrel = makeRangeVarFromNameList(textToQualifiedNameList(viewname));
    viewoid = RangeVarGetRelid(viewrel, NoLock, false);

    res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

/*
 * Common code for by-OID and by-name variants of pg_get_viewdef
 */
unsafe fn pg_get_viewdef_worker(viewoid: Oid, prettyFlags: i32, wrapColumn: i32) -> *mut c_char {
    let mut args: [Datum; 2] = [0; 2];
    let mut nulls: [c_char; 2] = [b' ' as c_char; 2];
    let spirc: i32;
    let ruletup: HeapTuple;
    let rulettc: TupleDesc;
    let mut buf: StringInfoData = core::mem::zeroed();

    /*
     * Do this first so that string is alloc'd in outer context not SPI's.
     */
    initStringInfo(&mut buf);

    /*
     * Connect to SPI manager
     */
    SPI_connect();

    /*
     * On the first call prepare the plan to lookup pg_rewrite. We read
     * pg_rewrite over the SPI manager instead of using the syscache to be
     * checked for read access on pg_rewrite.
     */
    if plan_getviewrule.is_null() {
        let mut argtypes: [Oid; 2] = [OIDOID, NAMEOID];
        let plan: SPIPlanPtr;

        let qstr = CString::new(query_getviewrule).unwrap();
        plan = SPI_prepare(qstr.as_ptr(), 2, argtypes.as_mut_ptr());
        if plan.is_null() {
            elog!(ERROR, "SPI_prepare failed for \"{}\"", query_getviewrule);
        }
        SPI_keepplan(plan);
        plan_getviewrule = plan;
    }

    /*
     * Get the pg_rewrite tuple for the view's SELECT rule
     */
    args[0] = ObjectIdGetDatum(viewoid);
    let vrname = CString::new(ViewSelectRuleName).unwrap();
    args[1] = DirectFunctionCall1(namein, CStringGetDatum(vrname.as_ptr()));
    nulls[0] = b' ' as c_char;
    nulls[1] = b' ' as c_char;
    spirc = SPI_execute_plan(plan_getviewrule, args.as_mut_ptr(), nulls.as_ptr(), true, 0);
    if spirc != SPI_OK_SELECT {
        elog!(ERROR, "failed to get pg_rewrite tuple for view {}", viewoid);
    }
    if SPI_processed != 1 {
        /*
         * There is no tuple data available here, just keep the output buffer
         * empty.
         */
    } else {
        /*
         * Get the rule's definition and put it into executor's memory
         */
        ruletup = (*(SPI_tuptable as *mut SPITupleTableReal)).vals[0] as HeapTuple;
        rulettc = (*(SPI_tuptable as *mut SPITupleTableReal)).tupdesc;
        make_viewdef(&mut buf, ruletup, rulettc, prettyFlags, wrapColumn);
    }

    /*
     * Disconnect from SPI manager
     */
    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    if buf.len == 0 {
        return ptr::null_mut();
    }

    buf.data
}

// ----------
// pg_get_triggerdef - Get the definition of a trigger
// ----------
pub unsafe extern "C" fn pg_get_triggerdef(fcinfo: FunctionCallInfo) -> Datum {
    let trigid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let res: *mut c_char;

    res = pg_get_triggerdef_worker(trigid, false);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_triggerdef_ext(fcinfo: FunctionCallInfo) -> Datum {
    let trigid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let res: *mut c_char;

    res = pg_get_triggerdef_worker(trigid, pretty);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

unsafe fn pg_get_triggerdef_worker(trigid: Oid, pretty: bool) -> *mut c_char {
    let mut ht_trig: HeapTuple;
    let trigrec: Form_pg_trigger;
    let mut buf: StringInfoData = std::mem::zeroed();
    let tgrel: Relation;
    let mut skey: [ScanKeyData; 1] = std::mem::zeroed();
    let tgscan: SysScanDesc;
    let mut findx: i32 = 0;
    let tgname: *mut c_char;
    let tgoldtable: *mut c_char;
    let tgnewtable: *mut c_char;
    let mut value: Datum;
    let mut isnull: bool;

    // Fetch the pg_trigger tuple by the Oid of the trigger
    tgrel = table_open(TriggerRelationId, AccessShareLock as i32);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_trigger_oid as i16,
        BTEqualStrategyNumber as i32,
        F_OIDEQ,
        ObjectIdGetDatum(trigid),
    );

    tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, std::ptr::null_mut(), 1, skey.as_mut_ptr());

    ht_trig = systable_getnext(tgscan);

    if !HeapTupleIsValid(ht_trig) {
        systable_endscan(tgscan);
        table_close(tgrel, AccessShareLock as i32);
        return std::ptr::null_mut();
    }

    trigrec = GETSTRUCT(ht_trig) as Form_pg_trigger;

    // Start the trigger definition. Note that the trigger's name should never
    // be schema-qualified, but the trigger rel's name may be.
    initStringInfo(&mut buf);

    tgname = NameStr((*trigrec).tgname).as_mut_ptr();
    appendStringInfo!(
        &mut buf,
        "CREATE {}TRIGGER {} ",
        if OidIsValid((*trigrec).tgconstraint) { "CONSTRAINT " } else { "" },
        CStr::from_ptr(quote_identifier(tgname)).to_str().unwrap_or("")
    );

    if TRIGGER_FOR_BEFORE((*trigrec).tgtype) {
        appendStringInfoString(&mut buf, cstr!("BEFORE"));
    } else if TRIGGER_FOR_AFTER((*trigrec).tgtype) {
        appendStringInfoString(&mut buf, cstr!("AFTER"));
    } else if TRIGGER_FOR_INSTEAD((*trigrec).tgtype) {
        appendStringInfoString(&mut buf, cstr!("INSTEAD OF"));
    } else {
        elog!(ERROR, "unexpected tgtype value: {}", (*trigrec).tgtype);
    }

    if TRIGGER_FOR_INSERT((*trigrec).tgtype) {
        appendStringInfoString(&mut buf, cstr!(" INSERT"));
        findx += 1;
    }
    if TRIGGER_FOR_DELETE((*trigrec).tgtype) {
        if findx > 0 {
            appendStringInfoString(&mut buf, cstr!(" OR DELETE"));
        } else {
            appendStringInfoString(&mut buf, cstr!(" DELETE"));
        }
        findx += 1;
    }
    if TRIGGER_FOR_UPDATE((*trigrec).tgtype) {
        if findx > 0 {
            appendStringInfoString(&mut buf, cstr!(" OR UPDATE"));
        } else {
            appendStringInfoString(&mut buf, cstr!(" UPDATE"));
        }
        findx += 1;
        // tgattr is first var-width field, so OK to access directly
        if (*trigrec).tgattr.dim1 > 0 {
            let mut i: i32 = 0;
            appendStringInfoString(&mut buf, cstr!(" OF "));
            while i < (*trigrec).tgattr.dim1 {
                let attname: *mut c_char;
                if i > 0 {
                    appendStringInfoString(&mut buf, cstr!(", "));
                }
                attname = get_attname((*trigrec).tgrelid, (*trigrec).tgattr.values[i as usize], false);
                appendStringInfoString(&mut buf, quote_identifier(attname));
                i += 1;
            }
        }
    }
    if TRIGGER_FOR_TRUNCATE((*trigrec).tgtype) {
        if findx > 0 {
            appendStringInfoString(&mut buf, cstr!(" OR TRUNCATE"));
        } else {
            appendStringInfoString(&mut buf, cstr!(" TRUNCATE"));
        }
        findx += 1;
    }

    // In non-pretty mode, always schema-qualify the target table name for
    // safety.  In pretty mode, schema-qualify only if not visible.
    {
        let relname = if pretty {
            generate_relation_name((*trigrec).tgrelid, NIL)
        } else {
            generate_qualified_relation_name((*trigrec).tgrelid)
        };
        appendStringInfo!(&mut buf, " ON {} ", CStr::from_ptr(relname).to_str().unwrap_or(""));
    }

    if OidIsValid((*trigrec).tgconstraint) {
        if OidIsValid((*trigrec).tgconstrrelid) {
            appendStringInfo!(
                &mut buf,
                "FROM {} ",
                CStr::from_ptr(generate_relation_name((*trigrec).tgconstrrelid, NIL)).to_str().unwrap_or("")
            );
        }
        if !(*trigrec).tgdeferrable {
            appendStringInfoString(&mut buf, cstr!("NOT "));
        }
        appendStringInfoString(&mut buf, cstr!("DEFERRABLE INITIALLY "));
        if (*trigrec).tginitdeferred {
            appendStringInfoString(&mut buf, cstr!("DEFERRED "));
        } else {
            appendStringInfoString(&mut buf, cstr!("IMMEDIATE "));
        }
    }

    isnull = false;
    value = fastgetattr(ht_trig, Anum_pg_trigger_tgoldtable as i32, (*tgrel).rd_att, &mut isnull);
    if !isnull {
        tgoldtable = NameStr(*DatumGetName(value)).as_mut_ptr();
    } else {
        tgoldtable = std::ptr::null_mut();
    }
    value = fastgetattr(ht_trig, Anum_pg_trigger_tgnewtable as i32, (*tgrel).rd_att, &mut isnull);
    if !isnull {
        tgnewtable = NameStr(*DatumGetName(value)).as_mut_ptr();
    } else {
        tgnewtable = std::ptr::null_mut();
    }
    if !tgoldtable.is_null() || !tgnewtable.is_null() {
        appendStringInfoString(&mut buf, cstr!("REFERENCING "));
        if !tgoldtable.is_null() {
            appendStringInfo!(
                &mut buf,
                "OLD TABLE AS {} ",
                CStr::from_ptr(quote_identifier(tgoldtable)).to_str().unwrap_or("")
            );
        }
        if !tgnewtable.is_null() {
            appendStringInfo!(
                &mut buf,
                "NEW TABLE AS {} ",
                CStr::from_ptr(quote_identifier(tgnewtable)).to_str().unwrap_or("")
            );
        }
    }

    if TRIGGER_FOR_ROW((*trigrec).tgtype) {
        appendStringInfoString(&mut buf, cstr!("FOR EACH ROW "));
    } else {
        appendStringInfoString(&mut buf, cstr!("FOR EACH STATEMENT "));
    }

    // If the trigger has a WHEN qualification, add that
    value = fastgetattr(ht_trig, Anum_pg_trigger_tgqual as i32, (*tgrel).rd_att, &mut isnull);
    if !isnull {
        let qual: *mut Node;
        let relkind: c_char;
        let mut context: deparse_context = std::mem::zeroed();
        let mut dpns: deparse_namespace = std::mem::zeroed();
        let oldrte: *mut RangeTblEntry;
        let newrte: *mut RangeTblEntry;

        appendStringInfoString(&mut buf, cstr!("WHEN ("));

        qual = stringToNode(TextDatumGetCString(value)) as *mut Node;

        relkind = get_rel_relkind((*trigrec).tgrelid);

        // Build minimal OLD and NEW RTEs for the rel
        oldrte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
        (*oldrte).rtekind = RTE_RELATION;
        (*oldrte).relid = (*trigrec).tgrelid;
        (*oldrte).relkind = relkind;
        (*oldrte).rellockmode = AccessShareLock as i32;
        (*oldrte).alias = makeAlias(cstr!("old"), NIL);
        (*oldrte).eref = (*oldrte).alias;
        (*oldrte).lateral = false;
        (*oldrte).inh = false;
        (*oldrte).inFromCl = true;

        newrte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
        (*newrte).rtekind = RTE_RELATION;
        (*newrte).relid = (*trigrec).tgrelid;
        (*newrte).relkind = relkind;
        (*newrte).rellockmode = AccessShareLock as i32;
        (*newrte).alias = makeAlias(cstr!("new"), NIL);
        (*newrte).eref = (*newrte).alias;
        (*newrte).lateral = false;
        (*newrte).inh = false;
        (*newrte).inFromCl = true;

        // Build two-element rtable
        std::ptr::write_bytes(&mut dpns as *mut deparse_namespace, 0, 1);
        dpns.rtable = list_make2(oldrte as *mut c_void, newrte as *mut c_void);
        dpns.subplans = NIL;
        dpns.ctes = NIL;
        dpns.appendrels = std::ptr::null_mut();
        set_rtable_names(&mut dpns, NIL, std::ptr::null_mut());
        set_simple_column_names(&mut dpns);

        // Set up context with one-deep namespace stack
        context.buf = &mut buf;
        context.namespaces = list_make1(&mut dpns as *mut deparse_namespace as *mut c_void);
        context.resultDesc = std::ptr::null_mut();
        context.targetList = NIL;
        context.windowClause = NIL;
        context.varprefix = true;
        context.prettyFlags = GET_PRETTY_FLAGS!(pretty);
        context.wrapColumn = WRAP_COLUMN_DEFAULT;
        context.indentLevel = PRETTYINDENT_STD;
        context.colNamesVisible = true;
        context.inGroupBy = false;
        context.varInOrderBy = false;
        context.appendparents = std::ptr::null_mut();

        get_rule_expr(qual, &mut context, false);

        appendStringInfoString(&mut buf, cstr!(") "));
    }

    {
        let fname = generate_function_name((*trigrec).tgfoid, 0, NIL, std::ptr::null_mut(), false, std::ptr::null_mut(), false);
        appendStringInfo!(&mut buf, "EXECUTE FUNCTION {}(", CStr::from_ptr(fname).to_str().unwrap_or(""));
    }

    if (*trigrec).tgnargs > 0 {
        let mut p: *mut c_char;
        let mut i: i32 = 0;

        value = fastgetattr(ht_trig, Anum_pg_trigger_tgargs as i32, (*tgrel).rd_att, &mut isnull);
        if isnull {
            elog!(ERROR, "tgargs is null for trigger {}", trigid);
        }
        p = VARDATA_ANY(DatumGetByteaPP(value)) as *mut c_char;
        while i < (*trigrec).tgnargs as i32 {
            if i > 0 {
                appendStringInfoString(&mut buf, cstr!(", "));
            }
            simple_quote_literal(&mut buf, p);
            // advance p to next string embedded in tgargs
            while *p != 0 {
                p = p.add(1);
            }
            p = p.add(1);
            i += 1;
        }
    }

    // We deliberately do not put semi-colon at end
    appendStringInfoChar(&mut buf, b')' as c_char);

    // Clean up
    systable_endscan(tgscan);
    table_close(tgrel, AccessShareLock as i32);

    buf.data
}

// ----------
// pg_get_indexdef - Get the definition of an index
//
// In the extended version, there is a colno argument as well as pretty bool.
//   if colno == 0, we want a complete index definition.
//   if colno > 0, we only want the Nth index key's variable or expression.
//
// Note that the SQL-function versions of this omit any info about the
// index tablespace; this is intentional because pg_dump wants it that way.
// However pg_get_indexdef_string() includes the index tablespace.
// ----------
pub unsafe extern "C" fn pg_get_indexdef(fcinfo: FunctionCallInfo) -> Datum {
    let indexrelid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let pretty_flags: i32;
    let res: *mut c_char;

    pretty_flags = PRETTYFLAG_INDENT;

    res = pg_get_indexdef_worker(
        indexrelid, 0, std::ptr::null(),
        false, false,
        false, false,
        pretty_flags, true,
    );

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_indexdef_ext(fcinfo: FunctionCallInfo) -> Datum {
    let indexrelid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let colno: i32 = PG_GETARG_INT32!(fcinfo, 1);
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let pretty_flags: i32;
    let res: *mut c_char;

    pretty_flags = GET_PRETTY_FLAGS!(pretty);

    res = pg_get_indexdef_worker(
        indexrelid, colno, std::ptr::null(),
        colno != 0, false,
        false, false,
        pretty_flags, true,
    );

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

// Internal version for use by ALTER TABLE.
// Includes a tablespace clause in the result.
// Returns a palloc'd C string; no pretty-printing.
pub unsafe fn pg_get_indexdef_string(indexrelid: Oid) -> *mut c_char {
    pg_get_indexdef_worker(
        indexrelid, 0, std::ptr::null(),
        false, false,
        true, true,
        0, false,
    )
}

// Internal version that just reports the key-column definitions
pub unsafe fn pg_get_indexdef_columns(indexrelid: Oid, pretty: bool) -> *mut c_char {
    let pretty_flags = GET_PRETTY_FLAGS!(pretty);

    pg_get_indexdef_worker(
        indexrelid, 0, std::ptr::null(),
        true, true,
        false, false,
        pretty_flags, false,
    )
}

// Internal version, extensible with flags to control its behavior
pub unsafe fn pg_get_indexdef_columns_extended(indexrelid: Oid, flags: u16) -> *mut c_char {
    let pretty: bool = (flags & RULE_INDEXDEF_PRETTY) != 0;
    let keys_only: bool = (flags & RULE_INDEXDEF_KEYS_ONLY) != 0;
    let pretty_flags = GET_PRETTY_FLAGS!(pretty);

    pg_get_indexdef_worker(
        indexrelid, 0, std::ptr::null(),
        true, keys_only,
        false, false,
        pretty_flags, false,
    )
}

// Internal workhorse to decompile an index definition.
//
// This is now used for exclusion constraints as well: if excludeOps is not
// NULL then it points to an array of exclusion operator OIDs.
unsafe fn pg_get_indexdef_worker(
    indexrelid: Oid,
    colno: i32,
    exclude_ops: *const Oid,
    attrs_only: bool,
    keys_only: bool,
    show_tbl_spc: bool,
    inherits: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> *mut c_char {
    // might want a separate isConstraint parameter later
    let is_constraint: bool = !exclude_ops.is_null();
    let ht_idx: HeapTuple;
    let ht_idxrel: HeapTuple;
    let ht_am: HeapTuple;
    let idxrec: Form_pg_index;
    let idxrelrec: Form_pg_class;
    let amrec: Form_pg_am;
    let amroutine: *mut IndexAmRoutine;
    let mut indexprs: *mut List;
    let mut indexpr_item: *mut ListCell;
    let context: *mut List;
    let indrelid: Oid;
    let mut keyno: i32;
    let indcoll_datum: Datum;
    let indclass_datum: Datum;
    let indoption_datum: Datum;
    let indcollation: *mut oidvector;
    let indclass: *mut oidvector;
    let indoption: *mut int2vector;
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut str_: *mut c_char;
    let mut sep: *const c_char;

    // Fetch the pg_index tuple by the Oid of the index
    ht_idx = SearchSysCache1(INDEXRELID as i32, ObjectIdGetDatum(indexrelid));
    if !HeapTupleIsValid(ht_idx) {
        if missing_ok {
            return std::ptr::null_mut();
        }
        elog!(ERROR, "cache lookup failed for index {}", indexrelid);
    }
    idxrec = GETSTRUCT(ht_idx) as Form_pg_index;

    let indrelid_ = (*idxrec).indrelid;
    indrelid = indrelid_;
    // Assert(indexrelid == idxrec->indexrelid);

    // Must get indcollation, indclass, and indoption the hard way
    indcoll_datum = SysCacheGetAttrNotNull(INDEXRELID as i32, ht_idx, Anum_pg_index_indcollation as i32);
    indcollation = DatumGetPointer(indcoll_datum) as *mut oidvector;

    indclass_datum = SysCacheGetAttrNotNull(INDEXRELID as i32, ht_idx, Anum_pg_index_indclass as i32);
    indclass = DatumGetPointer(indclass_datum) as *mut oidvector;

    indoption_datum = SysCacheGetAttrNotNull(INDEXRELID as i32, ht_idx, Anum_pg_index_indoption as i32);
    indoption = DatumGetPointer(indoption_datum) as *mut int2vector;

    // Fetch the pg_class tuple of the index relation
    ht_idxrel = SearchSysCache1(RELOID as i32, ObjectIdGetDatum(indexrelid));
    if !HeapTupleIsValid(ht_idxrel) {
        elog!(ERROR, "cache lookup failed for relation {}", indexrelid);
    }
    idxrelrec = GETSTRUCT(ht_idxrel) as Form_pg_class;

    // Fetch the pg_am tuple of the index' access method
    ht_am = SearchSysCache1(AMOID as i32, ObjectIdGetDatum((*idxrelrec).relam));
    if !HeapTupleIsValid(ht_am) {
        elog!(ERROR, "cache lookup failed for access method {}", (*idxrelrec).relam);
    }
    amrec = GETSTRUCT(ht_am) as Form_pg_am;

    // Fetch the index AM's API struct
    amroutine = GetIndexAmRoutine((*amrec).amhandler);

    // Get the index expressions, if any.  (NOTE: we do not use the relcache
    // versions of the expressions and predicate, because we want to display
    // non-const-folded expressions.)
    if !heap_attisnull(ht_idx, Anum_pg_index_indexprs as i32, std::ptr::null()) {
        let exprs_datum: Datum;
        let exprs_string: *mut c_char;

        exprs_datum = SysCacheGetAttrNotNull(INDEXRELID as i32, ht_idx, Anum_pg_index_indexprs as i32);
        exprs_string = TextDatumGetCString!(exprs_datum);
        indexprs = stringToNode(exprs_string) as *mut List;
        pfree(exprs_string as *mut c_void);
    } else {
        indexprs = NIL;
    }

    indexpr_item = list_head(indexprs);

    context = deparse_context_for(get_relation_name(indrelid), indrelid);

    // Start the index definition.  Note that the index's name should never be
    // schema-qualified, but the indexed rel's name may be.
    initStringInfo(&mut buf);

    if !attrs_only {
        if !is_constraint {
            appendStringInfo!(
                &mut buf,
                "CREATE {}INDEX {} ON {}{} USING {} (",
                if (*idxrec).indisunique { "UNIQUE " } else { "" },
                CStr::from_ptr(quote_identifier(NameStr((*idxrelrec).relname).as_mut_ptr())).to_str().unwrap_or(""),
                if (*idxrelrec).relkind == RELKIND_PARTITIONED_INDEX as c_char && !inherits { "ONLY " } else { "" },
                CStr::from_ptr(
                    if (pretty_flags & PRETTYFLAG_SCHEMA) != 0 {
                        generate_relation_name(indrelid, NIL)
                    } else {
                        generate_qualified_relation_name(indrelid)
                    }
                ).to_str().unwrap_or(""),
                CStr::from_ptr(quote_identifier(NameStr((*amrec).amname).as_mut_ptr())).to_str().unwrap_or("")
            );
        } else {
            // currently, must be EXCLUDE constraint
            appendStringInfo!(
                &mut buf,
                "EXCLUDE USING {} (",
                CStr::from_ptr(quote_identifier(NameStr((*amrec).amname).as_mut_ptr())).to_str().unwrap_or("")
            );
        }
    }

    // Report the indexed attributes
    sep = cstr!("");
    keyno = 0;
    while keyno < (*idxrec).indnatts as i32 {
        let attnum: AttrNumber = (*idxrec).indkey.values[keyno as usize];
        let keycoltype: Oid;
        let keycolcollation: Oid;

        // Ignore non-key attributes if told to.
        if keys_only && keyno >= (*idxrec).indnkeyatts as i32 {
            break;
        }

        // Otherwise, print INCLUDE to divide key and non-key attrs.
        if colno == 0 && keyno == (*idxrec).indnkeyatts as i32 {
            appendStringInfoString(&mut buf, cstr!(") INCLUDE ("));
            sep = cstr!("");
        }

        if colno == 0 {
            appendStringInfoString(&mut buf, sep);
        }
        sep = cstr!(", ");

        if attnum != 0 {
            // Simple index column
            let attname: *mut c_char;
            let mut keycoltypmod: i32 = 0;
            let mut kct: Oid = 0;
            let mut kcc: Oid = 0;

            attname = get_attname(indrelid, attnum, false);
            if colno == 0 || colno == keyno + 1 {
                appendStringInfoString(&mut buf, quote_identifier(attname));
            }
            get_atttypetypmodcoll(indrelid, attnum, &mut kct, &mut keycoltypmod, &mut kcc);
            keycoltype = kct;
            keycolcollation = kcc;
        } else {
            // expressional index
            let indexkey: *mut Node;

            if indexpr_item.is_null() {
                elog!(ERROR, "too few entries in indexprs list");
            }
            indexkey = lfirst(indexpr_item) as *mut Node;
            indexpr_item = lnext(indexprs, indexpr_item);
            // Deparse
            str_ = deparse_expression_pretty(indexkey, context, false, false, pretty_flags, 0);
            if colno == 0 || colno == keyno + 1 {
                // Need parens if it's not a bare function call
                if looks_like_function(indexkey) {
                    appendStringInfoString(&mut buf, str_);
                } else {
                    appendStringInfo!(&mut buf, "({})", CStr::from_ptr(str_).to_str().unwrap_or(""));
                }
            }
            keycoltype = exprType(indexkey);
            keycolcollation = exprCollation(indexkey);
        }

        // Print additional decoration for (selected) key columns
        if !attrs_only && keyno < (*idxrec).indnkeyatts as i32 && (colno == 0 || colno == keyno + 1) {
            let opt: i16 = (*indoption).values[keyno as usize];
            let indcoll: Oid = (*indcollation).values[keyno as usize];
            let attoptions: Datum = get_attoptions(indexrelid, keyno + 1);
            let has_options: bool = attoptions != 0;

            // Add collation, if not default for column
            if OidIsValid(indcoll) && indcoll != keycolcollation {
                appendStringInfo!(
                    &mut buf,
                    " COLLATE {}",
                    CStr::from_ptr(generate_collation_name(indcoll)).to_str().unwrap_or("")
                );
            }

            // Add the operator class name, if not default
            get_opclass_name(
                (*indclass).values[keyno as usize],
                if has_options { InvalidOid } else { keycoltype },
                &mut buf,
            );

            if has_options {
                appendStringInfoString(&mut buf, cstr!(" ("));
                get_reloptions(&mut buf, attoptions);
                appendStringInfoChar(&mut buf, b')' as c_char);
            }

            // Add options if relevant
            if (*amroutine).amcanorder {
                // if it supports sort ordering, report DESC and NULLS opts
                if (opt & INDOPTION_DESC as i16) != 0 {
                    appendStringInfoString(&mut buf, cstr!(" DESC"));
                    // NULLS FIRST is the default in this case
                    if (opt & INDOPTION_NULLS_FIRST as i16) == 0 {
                        appendStringInfoString(&mut buf, cstr!(" NULLS LAST"));
                    }
                } else {
                    if (opt & INDOPTION_NULLS_FIRST as i16) != 0 {
                        appendStringInfoString(&mut buf, cstr!(" NULLS FIRST"));
                    }
                }
            }

            // Add the exclusion operator if relevant
            if !exclude_ops.is_null() {
                appendStringInfo!(
                    &mut buf,
                    " WITH {}",
                    CStr::from_ptr(generate_operator_name(*exclude_ops.add(keyno as usize), keycoltype, keycoltype)).to_str().unwrap_or("")
                );
            }
        }

        keyno += 1;
    }

    if !attrs_only {
        appendStringInfoChar(&mut buf, b')' as c_char);

        if (*idxrec).indnullsnotdistinct {
            appendStringInfoString(&mut buf, cstr!(" NULLS NOT DISTINCT"));
        }

        // If it has options, append "WITH (options)"
        str_ = flatten_reloptions(indexrelid);
        if !str_.is_null() {
            appendStringInfo!(&mut buf, " WITH ({})", CStr::from_ptr(str_).to_str().unwrap_or(""));
            pfree(str_ as *mut c_void);
        }

        // Print tablespace, but only if requested
        if show_tbl_spc {
            let tblspc: Oid;

            tblspc = get_rel_tablespace(indexrelid);
            if OidIsValid(tblspc) {
                if is_constraint {
                    appendStringInfoString(&mut buf, cstr!(" USING INDEX"));
                }
                appendStringInfo!(
                    &mut buf,
                    " TABLESPACE {}",
                    CStr::from_ptr(quote_identifier(get_tablespace_name(tblspc))).to_str().unwrap_or("")
                );
            }
        }

        // If it's a partial index, decompile and append the predicate
        if !heap_attisnull(ht_idx, Anum_pg_index_indpred as i32, std::ptr::null()) {
            let node: *mut Node;
            let pred_datum: Datum;
            let pred_string: *mut c_char;

            // Convert text string to node tree
            pred_datum = SysCacheGetAttrNotNull(INDEXRELID as i32, ht_idx, Anum_pg_index_indpred as i32);
            pred_string = TextDatumGetCString!(pred_datum);
            node = stringToNode(pred_string) as *mut Node;
            pfree(pred_string as *mut c_void);

            // Deparse
            str_ = deparse_expression_pretty(node, context, false, false, pretty_flags, 0);
            if is_constraint {
                appendStringInfo!(&mut buf, " WHERE ({})", CStr::from_ptr(str_).to_str().unwrap_or(""));
            } else {
                appendStringInfo!(&mut buf, " WHERE {}", CStr::from_ptr(str_).to_str().unwrap_or(""));
            }
        }
    }

    // Clean up
    ReleaseSysCache(ht_idx);
    ReleaseSysCache(ht_idxrel);
    ReleaseSysCache(ht_am);

    buf.data
}

// ----------
// pg_get_querydef
//
// Public entry point to deparse one query parsetree.
// The pretty flags are determined by GET_PRETTY_FLAGS(pretty).
//
// The result is a palloc'd C string.
// ----------
pub unsafe fn pg_get_querydef(query: *mut Query, pretty: bool) -> *mut c_char {
    let mut buf: StringInfoData = std::mem::zeroed();
    let pretty_flags: i32;

    pretty_flags = GET_PRETTY_FLAGS!(pretty);

    initStringInfo(&mut buf);

    get_query_def(query, &mut buf, NIL, std::ptr::null_mut(), true, pretty_flags, WRAP_COLUMN_DEFAULT, 0);

    buf.data
}

// pg_get_statisticsobjdef
//    Get the definition of an extended statistics object
pub unsafe extern "C" fn pg_get_statisticsobjdef(fcinfo: FunctionCallInfo) -> Datum {
    let statextid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let res: *mut c_char;

    res = pg_get_statisticsobj_worker(statextid, false, true);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

// Internal version for use by ALTER TABLE.
// Includes a tablespace clause in the result.
// Returns a palloc'd C string; no pretty-printing.
pub unsafe fn pg_get_statisticsobjdef_string(statextid: Oid) -> *mut c_char {
    pg_get_statisticsobj_worker(statextid, false, false)
}

// pg_get_statisticsobjdef_columns
//    Get columns and expressions for an extended statistics object
pub unsafe extern "C" fn pg_get_statisticsobjdef_columns(fcinfo: FunctionCallInfo) -> Datum {
    let statextid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let res: *mut c_char;

    res = pg_get_statisticsobj_worker(statextid, true, true);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

// Internal workhorse to decompile an extended statistics object.
unsafe fn pg_get_statisticsobj_worker(statextid: Oid, columns_only: bool, missing_ok: bool) -> *mut c_char {
    let statextrec: Form_pg_statistic_ext;
    let statexttup: HeapTuple;
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut colno: i32;
    let nsp: *mut c_char;
    let arr: *mut ArrayType;
    let enabled: *mut c_char;
    let mut datum: Datum;
    let mut ndistinct_enabled: bool;
    let mut dependencies_enabled: bool;
    let mut mcv_enabled: bool;
    let mut i: i32;
    let context: *mut List;
    let mut lc: *mut ListCell;
    let mut exprs: *mut List = NIL;
    let has_exprs: bool;
    let ncolumns: i32;

    statexttup = SearchSysCache1(STATEXTOID as i32, ObjectIdGetDatum(statextid));

    if !HeapTupleIsValid(statexttup) {
        if missing_ok {
            return std::ptr::null_mut();
        }
        elog!(ERROR, "cache lookup failed for statistics object {}", statextid);
    }

    // has the statistics expressions?
    has_exprs = !heap_attisnull(statexttup, Anum_pg_statistic_ext_stxexprs as i32, std::ptr::null());

    statextrec = GETSTRUCT(statexttup) as Form_pg_statistic_ext;

    // Get the statistics expressions, if any.  (NOTE: we do not use the
    // relcache versions of the expressions, because we want to display
    // non-const-folded expressions.)
    if has_exprs {
        let exprs_datum: Datum;
        let exprs_string: *mut c_char;

        exprs_datum = SysCacheGetAttrNotNull(STATEXTOID as i32, statexttup, Anum_pg_statistic_ext_stxexprs as i32);
        exprs_string = TextDatumGetCString!(exprs_datum);
        exprs = stringToNode(exprs_string) as *mut List;
        pfree(exprs_string as *mut c_void);
    } else {
        exprs = NIL;
    }

    // count the number of columns (attributes and expressions)
    ncolumns = (*statextrec).stxkeys.dim1 + list_length(exprs);

    initStringInfo(&mut buf);

    if !columns_only {
        nsp = get_namespace_name_or_temp((*statextrec).stxnamespace);
        appendStringInfo!(
            &mut buf,
            "CREATE STATISTICS {}",
            CStr::from_ptr(
                quote_qualified_identifier(nsp, NameStr((*statextrec).stxname).as_mut_ptr())
            ).to_str().unwrap_or("")
        );

        // Decode the stxkind column so that we know which stats types to print.
        datum = SysCacheGetAttrNotNull(STATEXTOID as i32, statexttup, Anum_pg_statistic_ext_stxkind as i32);
        arr = DatumGetArrayTypeP!(datum) as *mut ArrayType;
        if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID {
            elog!(ERROR, "stxkind is not a 1-D char array");
        }
        enabled = ARR_DATA_PTR(arr) as *mut c_char;

        ndistinct_enabled = false;
        dependencies_enabled = false;
        mcv_enabled = false;

        i = 0;
        while i < ARR_DIMS(arr)[0] {
            if *enabled.add(i as usize) == STATS_EXT_NDISTINCT as c_char {
                ndistinct_enabled = true;
            } else if *enabled.add(i as usize) == STATS_EXT_DEPENDENCIES as c_char {
                dependencies_enabled = true;
            } else if *enabled.add(i as usize) == STATS_EXT_MCV as c_char {
                mcv_enabled = true;
            }
            // ignore STATS_EXT_EXPRESSIONS (it's built automatically)
            i += 1;
        }

        // If any option is disabled, then we'll need to append the types
        // clause to show which options are enabled.  We omit the types clause
        // on purpose when all options are enabled, so a pg_dump/pg_restore
        // will create all statistics types on a newer postgres version, if
        // the statistics had all options enabled on the original version.
        //
        // But if the statistics is defined on just a single column, it has to
        // be an expression statistics. In that case we don't need to specify kinds.
        if (!ndistinct_enabled || !dependencies_enabled || !mcv_enabled) && (ncolumns > 1) {
            let mut gotone: bool = false;

            appendStringInfoString(&mut buf, cstr!(" ("));

            if ndistinct_enabled {
                appendStringInfoString(&mut buf, cstr!("ndistinct"));
                gotone = true;
            }

            if dependencies_enabled {
                appendStringInfo!(
                    &mut buf, "{}dependencies",
                    if gotone { ", " } else { "" }
                );
                gotone = true;
            }

            if mcv_enabled {
                appendStringInfo!(
                    &mut buf, "{}mcv",
                    if gotone { ", " } else { "" }
                );
            }

            appendStringInfoChar(&mut buf, b')' as c_char);
        }

        appendStringInfoString(&mut buf, cstr!(" ON "));
    }

    // decode simple column references
    colno = 0;
    while colno < (*statextrec).stxkeys.dim1 {
        let attnum: AttrNumber = (*statextrec).stxkeys.values[colno as usize];
        let attname: *mut c_char;

        if colno > 0 {
            appendStringInfoString(&mut buf, cstr!(", "));
        }

        attname = get_attname((*statextrec).stxrelid, attnum, false);
        appendStringInfoString(&mut buf, quote_identifier(attname));
        colno += 1;
    }

    context = deparse_context_for(get_relation_name((*statextrec).stxrelid), (*statextrec).stxrelid);

    lc = list_head(exprs);
    while !lc.is_null() {
        let expr: *mut Node = lfirst(lc) as *mut Node;
        let str_: *mut c_char;
        let pretty_flags: i32 = PRETTYFLAG_PAREN;

        str_ = deparse_expression_pretty(expr, context, false, false, pretty_flags, 0);

        if colno > 0 {
            appendStringInfoString(&mut buf, cstr!(", "));
        }

        // Need parens if it's not a bare function call
        if looks_like_function(expr) {
            appendStringInfoString(&mut buf, str_);
        } else {
            appendStringInfo!(&mut buf, "({})", CStr::from_ptr(str_).to_str().unwrap_or(""));
        }

        colno += 1;
        lc = lnext(exprs, lc);
    }

    if !columns_only {
        appendStringInfo!(
            &mut buf,
            " FROM {}",
            CStr::from_ptr(generate_relation_name((*statextrec).stxrelid, NIL)).to_str().unwrap_or("")
        );
    }

    ReleaseSysCache(statexttup);

    buf.data
}

// Generate text array of expressions for statistics object.
pub unsafe extern "C" fn pg_get_statisticsobjdef_expressions(fcinfo: FunctionCallInfo) -> Datum {
    let statextid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let statextrec: Form_pg_statistic_ext;
    let statexttup: HeapTuple;
    let mut datum: Datum;
    let context: *mut List;
    let mut lc: *mut ListCell;
    let mut exprs: *mut List = NIL;
    let has_exprs: bool;
    let mut tmp: *mut c_char;
    let mut astate: *mut ArrayBuildState = std::ptr::null_mut();

    statexttup = SearchSysCache1(STATEXTOID as i32, ObjectIdGetDatum(statextid));

    if !HeapTupleIsValid(statexttup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    // Does the stats object have expressions?
    has_exprs = !heap_attisnull(statexttup, Anum_pg_statistic_ext_stxexprs as i32, std::ptr::null());

    // no expressions? we're done
    if !has_exprs {
        ReleaseSysCache(statexttup);
        return PG_RETURN_NULL!(fcinfo);
    }

    statextrec = GETSTRUCT(statexttup) as Form_pg_statistic_ext;

    // Get the statistics expressions, and deparse them into text values.
    datum = SysCacheGetAttrNotNull(STATEXTOID as i32, statexttup, Anum_pg_statistic_ext_stxexprs as i32);
    tmp = TextDatumGetCString!(datum);
    exprs = stringToNode(tmp) as *mut List;
    pfree(tmp as *mut c_void);

    context = deparse_context_for(get_relation_name((*statextrec).stxrelid), (*statextrec).stxrelid);

    lc = list_head(exprs);
    while !lc.is_null() {
        let expr: *mut Node = lfirst(lc) as *mut Node;
        let str_: *mut c_char;
        let pretty_flags: i32 = PRETTYFLAG_INDENT;

        str_ = deparse_expression_pretty(expr, context, false, false, pretty_flags, 0);

        astate = accumArrayResult(
            astate,
            PointerGetDatum(cstring_to_text(str_)),
            false,
            TEXTOID,
            CurrentMemoryContext,
        );

        lc = lnext(exprs, lc);
    }

    ReleaseSysCache(statexttup);

    PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext))
}

// pg_get_partkeydef
//
// Returns the partition key specification, ie, the following:
//
// { RANGE | LIST | HASH } (column opt_collation opt_opclass [, ...])
pub unsafe extern "C" fn pg_get_partkeydef(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let res: *mut c_char;

    res = pg_get_partkeydef_worker(relid, PRETTYFLAG_INDENT, false, true);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

// Internal version that just reports the column definitions
pub unsafe fn pg_get_partkeydef_columns(relid: Oid, pretty: bool) -> *mut c_char {
    let pretty_flags = GET_PRETTY_FLAGS!(pretty);

    pg_get_partkeydef_worker(relid, pretty_flags, true, false)
}

// Internal workhorse to decompile a partition key definition.
unsafe fn pg_get_partkeydef_worker(relid: Oid, pretty_flags: i32, attrs_only: bool, missing_ok: bool) -> *mut c_char {
    let form: Form_pg_partitioned_table;
    let tuple: HeapTuple;
    let partclass: *mut oidvector;
    let partcollation: *mut oidvector;
    let mut partexprs: *mut List;
    let mut partexpr_item: *mut ListCell;
    let context: *mut List;
    let mut datum: Datum;
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut keyno: i32;
    let mut str_: *mut c_char;
    let mut sep: *const c_char;

    tuple = SearchSysCache1(PARTRELID as i32, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        if missing_ok {
            return std::ptr::null_mut();
        }
        elog!(ERROR, "cache lookup failed for partition key of {}", relid);
    }

    form = GETSTRUCT(tuple) as Form_pg_partitioned_table;

    // Assert(form->partrelid == relid);

    // Must get partclass and partcollation the hard way
    datum = SysCacheGetAttrNotNull(PARTRELID as i32, tuple, Anum_pg_partitioned_table_partclass as i32);
    partclass = DatumGetPointer(datum) as *mut oidvector;

    datum = SysCacheGetAttrNotNull(PARTRELID as i32, tuple, Anum_pg_partitioned_table_partcollation as i32);
    partcollation = DatumGetPointer(datum) as *mut oidvector;

    // Get the expressions, if any.  (NOTE: we do not use the relcache
    // versions of the expressions, because we want to display
    // non-const-folded expressions.)
    if !heap_attisnull(tuple, Anum_pg_partitioned_table_partexprs as i32, std::ptr::null()) {
        let exprs_datum: Datum;
        let exprs_string: *mut c_char;

        exprs_datum = SysCacheGetAttrNotNull(PARTRELID as i32, tuple, Anum_pg_partitioned_table_partexprs as i32);
        exprs_string = TextDatumGetCString!(exprs_datum);
        partexprs = stringToNode(exprs_string) as *mut List;

        if !IsA(partexprs as *mut c_void, List) {
            elog!(ERROR, "unexpected node type found in partexprs: {}", nodeTag(partexprs as *const c_void) as i32);
        }

        pfree(exprs_string as *mut c_void);
    } else {
        partexprs = NIL;
    }

    partexpr_item = list_head(partexprs);
    context = deparse_context_for(get_relation_name(relid), relid);

    initStringInfo(&mut buf);

    match (*form).partstrat as u8 {
        PARTITION_STRATEGY_HASH => {
            if !attrs_only {
                appendStringInfoString(&mut buf, cstr!("HASH"));
            }
        }
        PARTITION_STRATEGY_LIST => {
            if !attrs_only {
                appendStringInfoString(&mut buf, cstr!("LIST"));
            }
        }
        PARTITION_STRATEGY_RANGE => {
            if !attrs_only {
                appendStringInfoString(&mut buf, cstr!("RANGE"));
            }
        }
        _ => {
            elog!(ERROR, "unexpected partition strategy: {}", (*form).partstrat as i32);
        }
    }

    if !attrs_only {
        appendStringInfoString(&mut buf, cstr!(" ("));
    }
    sep = cstr!("");
    keyno = 0;
    while keyno < (*form).partnatts as i32 {
        let attnum: AttrNumber = (*form).partattrs.values[keyno as usize];
        let keycoltype: Oid;
        let keycolcollation: Oid;
        let partcoll: Oid;

        appendStringInfoString(&mut buf, sep);
        sep = cstr!(", ");
        if attnum != 0 {
            // Simple attribute reference
            let attname: *mut c_char;
            let mut keycoltypmod: i32 = 0;
            let mut kct: Oid = 0;
            let mut kcc: Oid = 0;

            attname = get_attname(relid, attnum, false);
            appendStringInfoString(&mut buf, quote_identifier(attname));
            get_atttypetypmodcoll(relid, attnum, &mut kct, &mut keycoltypmod, &mut kcc);
            keycoltype = kct;
            keycolcollation = kcc;
        } else {
            // Expression
            let partkey: *mut Node;

            if partexpr_item.is_null() {
                elog!(ERROR, "too few entries in partexprs list");
            }
            partkey = lfirst(partexpr_item) as *mut Node;
            partexpr_item = lnext(partexprs, partexpr_item);

            // Deparse
            str_ = deparse_expression_pretty(partkey, context, false, false, pretty_flags, 0);
            // Need parens if it's not a bare function call
            if looks_like_function(partkey) {
                appendStringInfoString(&mut buf, str_);
            } else {
                appendStringInfo!(&mut buf, "({})", CStr::from_ptr(str_).to_str().unwrap_or(""));
            }

            keycoltype = exprType(partkey);
            keycolcollation = exprCollation(partkey);
        }

        // Add collation, if not default for column
        partcoll = (*partcollation).values[keyno as usize];
        if !attrs_only && OidIsValid(partcoll) && partcoll != keycolcollation {
            appendStringInfo!(
                &mut buf,
                " COLLATE {}",
                CStr::from_ptr(generate_collation_name(partcoll)).to_str().unwrap_or("")
            );
        }

        // Add the operator class name, if not default
        if !attrs_only {
            get_opclass_name((*partclass).values[keyno as usize], keycoltype, &mut buf);
        }

        keyno += 1;
    }

    if !attrs_only {
        appendStringInfoChar(&mut buf, b')' as c_char);
    }

    // Clean up
    ReleaseSysCache(tuple);

    buf.data
}

// pg_get_partition_constraintdef
//
// Returns partition constraint expression as a string for the input relation
pub unsafe extern "C" fn pg_get_partition_constraintdef(fcinfo: FunctionCallInfo) -> Datum {
    let relation_id: Oid = PG_GETARG_OID!(fcinfo, 0);
    let constr_expr: *mut Expr;
    let pretty_flags: i32;
    let context: *mut List;
    let consrc: *mut c_char;

    constr_expr = get_partition_qual_relid(relation_id);

    // Quick exit if no partition constraint
    if constr_expr.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    // Deparse and return the constraint expression.
    pretty_flags = PRETTYFLAG_INDENT;
    context = deparse_context_for(get_relation_name(relation_id), relation_id);
    consrc = deparse_expression_pretty(
        constr_expr as *mut Node,
        context,
        false,
        false,
        pretty_flags,
        0,
    );

    PG_RETURN_TEXT_P!(string_to_text(consrc))
}

// pg_get_partconstrdef_string
//
// Returns the partition constraint as a C-string for the input relation, with
// the given alias.  No pretty-printing.
pub unsafe fn pg_get_partconstrdef_string(partition_id: Oid, aliasname: *mut c_char) -> *mut c_char {
    let constr_expr: *mut Expr;
    let context: *mut List;

    constr_expr = get_partition_qual_relid(partition_id);
    context = deparse_context_for(aliasname, partition_id);

    deparse_expression(constr_expr as *mut Node, context, true, false)
}

// pg_get_constraintdef
//
// Returns the definition for the constraint, ie, everything that needs to
// appear after "ALTER TABLE ... ADD CONSTRAINT <constraintname>".
pub unsafe extern "C" fn pg_get_constraintdef(fcinfo: FunctionCallInfo) -> Datum {
    let constraint_id: Oid = PG_GETARG_OID!(fcinfo, 0);
    let pretty_flags: i32;
    let res: *mut c_char;

    pretty_flags = PRETTYFLAG_INDENT;

    res = pg_get_constraintdef_worker(constraint_id, false, pretty_flags, true);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

pub unsafe extern "C" fn pg_get_constraintdef_ext(fcinfo: FunctionCallInfo) -> Datum {
    let constraint_id: Oid = PG_GETARG_OID!(fcinfo, 0);
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let pretty_flags: i32;
    let res: *mut c_char;

    pretty_flags = GET_PRETTY_FLAGS!(pretty);

    res = pg_get_constraintdef_worker(constraint_id, false, pretty_flags, true);

    if res.is_null() {
        return PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(string_to_text(res))
}

// Internal version that returns a full ALTER TABLE ... ADD CONSTRAINT command
pub unsafe fn pg_get_constraintdef_command(constraint_id: Oid) -> *mut c_char {
    pg_get_constraintdef_worker(constraint_id, true, 0, false)
}

// As of 9.4, we now use an MVCC snapshot for this.
unsafe fn pg_get_constraintdef_worker(
    constraint_id: Oid,
    full_command: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> *mut c_char {
    let tup: HeapTuple;
    let con_form: Form_pg_constraint;
    let mut buf: StringInfoData = std::mem::zeroed();
    let scandesc: SysScanDesc;
    let mut scankey: [ScanKeyData; 1] = std::mem::zeroed();
    let snapshot: Snapshot = RegisterSnapshot(GetTransactionSnapshot());
    let relation: Relation = table_open(ConstraintRelationId, AccessShareLock as i32);

    ScanKeyInit(
        &mut scankey[0],
        Anum_pg_constraint_oid as i16,
        BTEqualStrategyNumber as i32,
        F_OIDEQ,
        ObjectIdGetDatum(constraint_id),
    );

    scandesc = systable_beginscan(
        relation,
        ConstraintOidIndexId,
        true,
        snapshot,
        1,
        scankey.as_mut_ptr(),
    );

    // We later use the tuple with SysCacheGetAttr() as if we had obtained it
    // via SearchSysCache, which works fine.
    tup = systable_getnext(scandesc);

    UnregisterSnapshot(snapshot);

    if !HeapTupleIsValid(tup) {
        if missing_ok {
            systable_endscan(scandesc);
            table_close(relation, AccessShareLock as i32);
            return std::ptr::null_mut();
        }
        elog!(ERROR, "could not find tuple for constraint {}", constraint_id);
    }

    con_form = GETSTRUCT(tup) as Form_pg_constraint;

    initStringInfo(&mut buf);

    if full_command {
        if OidIsValid((*con_form).conrelid) {
            // Currently, callers want ALTER TABLE (without ONLY) for CHECK
            // constraints, and other types of constraints don't inherit
            // anyway so it doesn't matter whether we say ONLY or not. Someday
            // we might need to let callers specify whether to put ONLY in the
            // command.
            appendStringInfo!(
                &mut buf,
                "ALTER TABLE {} ADD CONSTRAINT {} ",
                CStr::from_ptr(generate_qualified_relation_name((*con_form).conrelid)).to_str().unwrap_or(""),
                CStr::from_ptr(quote_identifier(NameStr((*con_form).conname).as_mut_ptr())).to_str().unwrap_or("")
            );
        } else {
            // Must be a domain constraint
            // Assert(OidIsValid(con_form->contypid));
            appendStringInfo!(
                &mut buf,
                "ALTER DOMAIN {} ADD CONSTRAINT {} ",
                CStr::from_ptr(generate_qualified_type_name((*con_form).contypid)).to_str().unwrap_or(""),
                CStr::from_ptr(quote_identifier(NameStr((*con_form).conname).as_mut_ptr())).to_str().unwrap_or("")
            );
        }
    }

    match (*con_form).contype as u8 {
        CONSTRAINT_FOREIGN => {
            let mut val: Datum;
            let mut isnull: bool = false;
            let string: &str;

            // Start off the constraint definition
            appendStringInfoString(&mut buf, cstr!("FOREIGN KEY ("));

            // Fetch and build referencing-column list
            val = SysCacheGetAttrNotNull(CONSTROID as i32, tup, Anum_pg_constraint_conkey as i32);

            // If it is a temporal foreign key then it uses PERIOD.
            decompile_column_index_array(val, (*con_form).conrelid, (*con_form).conperiod, &mut buf);

            // add foreign relation name
            appendStringInfo!(
                &mut buf,
                ") REFERENCES {}(",
                CStr::from_ptr(generate_relation_name((*con_form).confrelid, NIL)).to_str().unwrap_or("")
            );

            // Fetch and build referenced-column list
            val = SysCacheGetAttrNotNull(CONSTROID as i32, tup, Anum_pg_constraint_confkey as i32);

            decompile_column_index_array(val, (*con_form).confrelid, (*con_form).conperiod, &mut buf);

            appendStringInfoChar(&mut buf, b')' as c_char);

            // Add match type
            string = match (*con_form).confmatchtype as u8 {
                FKCONSTR_MATCH_FULL => " MATCH FULL",
                FKCONSTR_MATCH_PARTIAL => " MATCH PARTIAL",
                FKCONSTR_MATCH_SIMPLE => "",
                _ => {
                    elog!(ERROR, "unrecognized confmatchtype: {}", (*con_form).confmatchtype as i32);
                    ""
                }
            };
            appendStringInfoString(&mut buf, string.as_ptr() as *const c_char);

            // Add ON UPDATE and ON DELETE clauses, if needed
            let upd_string: Option<&str> = match (*con_form).confupdtype as u8 {
                FKCONSTR_ACTION_NOACTION => None,
                FKCONSTR_ACTION_RESTRICT => Some("RESTRICT"),
                FKCONSTR_ACTION_CASCADE => Some("CASCADE"),
                FKCONSTR_ACTION_SETNULL => Some("SET NULL"),
                FKCONSTR_ACTION_SETDEFAULT => Some("SET DEFAULT"),
                _ => {
                    elog!(ERROR, "unrecognized confupdtype: {}", (*con_form).confupdtype as i32);
                    None
                }
            };
            if let Some(s) = upd_string {
                appendStringInfo!(&mut buf, " ON UPDATE {}", s);
            }

            let del_string: Option<&str> = match (*con_form).confdeltype as u8 {
                FKCONSTR_ACTION_NOACTION => None,
                FKCONSTR_ACTION_RESTRICT => Some("RESTRICT"),
                FKCONSTR_ACTION_CASCADE => Some("CASCADE"),
                FKCONSTR_ACTION_SETNULL => Some("SET NULL"),
                FKCONSTR_ACTION_SETDEFAULT => Some("SET DEFAULT"),
                _ => {
                    elog!(ERROR, "unrecognized confdeltype: {}", (*con_form).confdeltype as i32);
                    None
                }
            };
            if let Some(s) = del_string {
                appendStringInfo!(&mut buf, " ON DELETE {}", s);
            }

            // Add columns specified to SET NULL or SET DEFAULT if provided.
            val = SysCacheGetAttr(CONSTROID as i32, tup, Anum_pg_constraint_confdelsetcols as i32, &mut isnull);
            if !isnull {
                appendStringInfoString(&mut buf, cstr!(" ("));
                decompile_column_index_array(val, (*con_form).conrelid, false, &mut buf);
                appendStringInfoChar(&mut buf, b')' as c_char);
            }
        }
        CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE => {
            let val: Datum;
            let index_id: Oid;
            let keyatts: i32;
            let indtup: HeapTuple;

            // Start off the constraint definition
            if (*con_form).contype as u8 == CONSTRAINT_PRIMARY {
                appendStringInfoString(&mut buf, cstr!("PRIMARY KEY "));
            } else {
                appendStringInfoString(&mut buf, cstr!("UNIQUE "));
            }

            index_id = (*con_form).conindid;

            indtup = SearchSysCache1(INDEXRELID as i32, ObjectIdGetDatum(index_id));
            if !HeapTupleIsValid(indtup) {
                elog!(ERROR, "cache lookup failed for index {}", index_id);
            }
            if (*con_form).contype as u8 == CONSTRAINT_UNIQUE
                && (GETSTRUCT(indtup) as Form_pg_index != std::ptr::null_mut())
                && (*( GETSTRUCT(indtup) as Form_pg_index)).indnullsnotdistinct
            {
                appendStringInfoString(&mut buf, cstr!("NULLS NOT DISTINCT "));
            }

            appendStringInfoChar(&mut buf, b'(' as c_char);

            // Fetch and build target column list
            val = SysCacheGetAttrNotNull(CONSTROID as i32, tup, Anum_pg_constraint_conkey as i32);

            keyatts = decompile_column_index_array(val, (*con_form).conrelid, false, &mut buf);
            if (*con_form).conperiod {
                appendStringInfoString(&mut buf, cstr!(" WITHOUT OVERLAPS"));
            }

            appendStringInfoChar(&mut buf, b')' as c_char);

            // Build including column list (from pg_index.indkeys)
            val = SysCacheGetAttrNotNull(INDEXRELID as i32, indtup, Anum_pg_index_indnatts as i32);
            if DatumGetInt32(val) > keyatts {
                let cols: Datum;
                let mut keys: *mut Datum = std::ptr::null_mut();
                let mut n_keys: i32 = 0;
                let mut j: i32;

                appendStringInfoString(&mut buf, cstr!(" INCLUDE ("));

                cols = SysCacheGetAttrNotNull(INDEXRELID as i32, indtup, Anum_pg_index_indkey as i32);

                deconstruct_array_builtin(DatumGetArrayTypeP!(cols) as *mut ArrayType, INT2OID, &mut keys, std::ptr::null_mut(), &mut n_keys);

                j = keyatts;
                while j < n_keys {
                    let col_name: *mut c_char;

                    col_name = get_attname((*con_form).conrelid, DatumGetInt16(*keys.add(j as usize)), false);
                    if j > keyatts {
                        appendStringInfoString(&mut buf, cstr!(", "));
                    }
                    appendStringInfoString(&mut buf, quote_identifier(col_name));
                    j += 1;
                }

                appendStringInfoChar(&mut buf, b')' as c_char);
            }
            ReleaseSysCache(indtup);

            // XXX why do we only print these bits if fullCommand?
            if full_command && OidIsValid(index_id) {
                let options: *mut c_char = flatten_reloptions(index_id);
                let tblspc: Oid;

                if !options.is_null() {
                    appendStringInfo!(&mut buf, " WITH ({})", CStr::from_ptr(options).to_str().unwrap_or(""));
                    pfree(options as *mut c_void);
                }

                // Print the tablespace, unless it's the database default.
                // This is to help ALTER TABLE usage of this facility,
                // which needs this behavior to recreate exact catalog state.
                tblspc = get_rel_tablespace(index_id);
                if OidIsValid(tblspc) {
                    appendStringInfo!(
                        &mut buf,
                        " USING INDEX TABLESPACE {}",
                        CStr::from_ptr(quote_identifier(get_tablespace_name(tblspc))).to_str().unwrap_or("")
                    );
                }
            }
        }
        CONSTRAINT_CHECK => {
            let val: Datum;
            let conbin: *mut c_char;
            let consrc: *mut c_char;
            let expr: *mut Node;
            let context: *mut List;

            // Fetch constraint expression in parsetree form
            val = SysCacheGetAttrNotNull(CONSTROID as i32, tup, Anum_pg_constraint_conbin as i32);

            conbin = TextDatumGetCString!(val);
            expr = stringToNode(conbin) as *mut Node;

            // Set up deparsing context for Var nodes in constraint
            if (*con_form).conrelid != InvalidOid {
                // relation constraint
                context = deparse_context_for(get_relation_name((*con_form).conrelid), (*con_form).conrelid);
            } else {
                // domain constraint --- can't have Vars
                context = NIL;
            }

            consrc = deparse_expression_pretty(expr, context, false, false, pretty_flags, 0);

            // Now emit the constraint definition, adding NO INHERIT if necessary.
            //
            // There are cases where the constraint expression will be
            // fully parenthesized and we don't need the outer parens ...
            // but there are other cases where we do need 'em.  Be
            // conservative for now.
            //
            // Note that simply checking for leading '(' and trailing ')'
            // would NOT be good enough, consider "(x > 0) AND (y > 0)".
            appendStringInfo!(
                &mut buf,
                "CHECK ({}){}",
                CStr::from_ptr(consrc).to_str().unwrap_or(""),
                if (*con_form).connoinherit { " NO INHERIT" } else { "" }
            );
        }
        CONSTRAINT_NOTNULL => {
            if (*con_form).conrelid != InvalidOid {
                let attnum: AttrNumber;

                attnum = extractNotNullColumn(tup);

                appendStringInfo!(
                    &mut buf,
                    "NOT NULL {}",
                    CStr::from_ptr(quote_identifier(get_attname((*con_form).conrelid, attnum, false))).to_str().unwrap_or("")
                );
                if (GETSTRUCT(tup) as Form_pg_constraint != std::ptr::null_mut())
                    && (*(GETSTRUCT(tup) as Form_pg_constraint)).connoinherit
                {
                    appendStringInfoString(&mut buf, cstr!(" NO INHERIT"));
                }
            } else if (*con_form).contypid != InvalidOid {
                // conkey is null for domain not-null constraints
                appendStringInfoString(&mut buf, cstr!("NOT NULL"));
            }
        }
        CONSTRAINT_TRIGGER => {
            // There isn't an ALTER TABLE syntax for creating a user-defined
            // constraint trigger, but it seems better to print something than
            // throw an error; if we throw error then this function couldn't
            // safely be applied to all rows of pg_constraint.
            appendStringInfoString(&mut buf, cstr!("TRIGGER"));
        }
        CONSTRAINT_EXCLUSION => {
            let index_oid: Oid = (*con_form).conindid;
            let val: Datum;
            let mut elems: *mut Datum = std::ptr::null_mut();
            let mut n_elems: i32 = 0;
            let mut i: i32;
            let operators: *mut Oid;

            // Extract operator OIDs from the pg_constraint tuple
            val = SysCacheGetAttrNotNull(CONSTROID as i32, tup, Anum_pg_constraint_conexclop as i32);

            deconstruct_array_builtin(DatumGetArrayTypeP!(val) as *mut ArrayType, OIDOID, &mut elems, std::ptr::null_mut(), &mut n_elems);

            operators = palloc(n_elems as usize * std::mem::size_of::<Oid>()) as *mut Oid;
            i = 0;
            while i < n_elems {
                *operators.add(i as usize) = DatumGetObjectId(*elems.add(i as usize));
                i += 1;
            }

            // pg_get_indexdef_worker does the rest
            // suppress tablespace because pg_dump wants it that way
            let worker_result = pg_get_indexdef_worker(
                index_oid,
                0,
                operators,
                false,
                false,
                false,
                false,
                pretty_flags,
                false,
            );
            appendStringInfoString(&mut buf, worker_result);
        }
        _ => {
            elog!(ERROR, "invalid constraint type \"{}\"", (*con_form).contype as u8 as char);
        }
    }

    if (*con_form).condeferrable {
        appendStringInfoString(&mut buf, cstr!(" DEFERRABLE"));
    }
    if (*con_form).condeferred {
        appendStringInfoString(&mut buf, cstr!(" INITIALLY DEFERRED"));
    }

    // Validated status is irrelevant when the constraint is NOT ENFORCED.
    if !(*con_form).conenforced {
        appendStringInfoString(&mut buf, cstr!(" NOT ENFORCED"));
    } else if !(*con_form).convalidated {
        appendStringInfoString(&mut buf, cstr!(" NOT VALID"));
    }

    // Cleanup
    systable_endscan(scandesc);
    table_close(relation, AccessShareLock as i32);

    buf.data
}

// Convert an int16[] Datum into a comma-separated list of column names
// for the indicated relation; append the list to buf.  Returns the number
// of keys.
unsafe fn decompile_column_index_array(
    column_index_array: Datum,
    rel_id: Oid,
    with_period: bool,
    buf: *mut StringInfoData,
) -> i32 {
    let mut keys: *mut Datum = std::ptr::null_mut();
    let mut n_keys: i32 = 0;
    let mut j: i32;

    // Extract data from array of int16
    deconstruct_array_builtin(DatumGetArrayTypeP!(column_index_array) as *mut ArrayType, INT2OID, &mut keys, std::ptr::null_mut(), &mut n_keys);

    j = 0;
    while j < n_keys {
        let col_name: *mut c_char;

        col_name = get_attname(rel_id, DatumGetInt16(*keys.add(j as usize)), false);

        if j == 0 {
            appendStringInfoString(buf, quote_identifier(col_name));
        } else {
            appendStringInfo!(
                buf,
                ", {}{}",
                if with_period && j == n_keys - 1 { "PERIOD " } else { "" },
                CStr::from_ptr(quote_identifier(col_name)).to_str().unwrap_or("")
            );
        }
        j += 1;
    }

    n_keys
}

// ----------
// pg_get_expr - Decompile an expression tree
//
// Input: an expression tree in nodeToString form, and a relation OID
//
// Output: reverse-listed expression
//
// Currently, the expression can only refer to a single relation, namely
// the one specified by the second parameter.  This is sufficient for
// partial indexes, column default expressions, etc.  We also support
// Var-free expressions, for which the OID can be InvalidOid.
//
// If the OID is nonzero but not actually valid, don't throw an error,
// just return NULL.  This is a bit questionable, but it's what we've
// done historically, and it can help avoid unwanted failures when
// examining catalog entries for just-deleted relations.
//
// We expect this function to work, or throw a reasonably clean error,
// for any node tree that can appear in a catalog pg_node_tree column.
// Query trees, such as those appearing in pg_rewrite.ev_action, are
// not supported.  Nor are expressions in more than one relation, which
// can appear in places like pg_rewrite.ev_qual.
// ----------
pub unsafe extern "C" fn pg_get_expr(fcinfo: FunctionCallInfo) -> Datum {
    let expr: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let relid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let result: *mut text;
    let pretty_flags: i32;

    pretty_flags = PRETTYFLAG_INDENT;

    result = pg_get_expr_worker(expr, relid, pretty_flags);
    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

pub unsafe extern "C" fn pg_get_expr_ext(fcinfo: FunctionCallInfo) -> Datum {
    let expr: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let relid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let pretty: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let result: *mut text;
    let pretty_flags: i32;

    pretty_flags = GET_PRETTY_FLAGS!(pretty);

    result = pg_get_expr_worker(expr, relid, pretty_flags);
    if !result.is_null() {
        PG_RETURN_TEXT_P!(result)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

unsafe fn pg_get_expr_worker(expr: *mut text, relid: Oid, pretty_flags: i32) -> *mut text {
    let node: *mut Node;
    let mut tst: *mut Node;
    let relids: *mut Relids;
    let context: *mut List;
    let exprstr: *mut c_char;
    let mut rel: Relation = std::ptr::null_mut();
    let str_: *mut c_char;

    // Convert input pg_node_tree (really TEXT) object to C string
    exprstr = text_to_cstring(expr);

    // Convert expression to node tree
    node = stringToNode(exprstr) as *mut Node;

    pfree(exprstr as *mut c_void);

    // Throw error if the input is a querytree rather than an expression tree.
    // While we could support queries here, there seems no very good reason
    // to.  In most such catalog columns, we'll see a List of Query nodes, or
    // even nested Lists, so drill down to a non-List node before checking.
    tst = node;
    while !tst.is_null() && IsA(tst as *mut c_void, List) {
        tst = linitial(tst as *mut List) as *mut Node;
    }
    if !tst.is_null() && IsA(tst as *mut c_void, Query) {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            errmsg!("input is a query, not an expression")
        );
    }

    // Throw error if the expression contains Vars we won't be able to deparse.
    relids = pull_varnos(std::ptr::null_mut(), node) as *mut Relids;
    if OidIsValid(relid) {
        if !bms_is_subset(relids as Relids, bms_make_singleton(1)) {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                errmsg!("expression contains variables of more than one relation")
            );
        }
    } else {
        if !bms_is_empty(relids as Relids) {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                errmsg!("expression contains variables")
            );
        }
    }

    // Prepare deparse context if needed.  If we are deparsing with a relid,
    // we need to transiently open and lock the rel, to make sure it won't go
    // away underneath us.  (set_relation_column_names would lock it anyway,
    // so this isn't really introducing any new behavior.)
    if OidIsValid(relid) {
        rel = try_relation_open(relid, AccessShareLock as i32);
        if rel.is_null() {
            return std::ptr::null_mut();
        }
        context = deparse_context_for(RelationGetRelationName(rel), relid);
    } else {
        context = NIL;
    }

    // Deparse
    str_ = deparse_expression_pretty(node, context, false, false, pretty_flags, 0);

    if !rel.is_null() {
        relation_close(rel, AccessShareLock as i32);
    }

    string_to_text(str_)
}


// ----------
// pg_get_userbyid - Get a user name by roleid and
//           fallback to 'unknown (OID=n)'
// ----------
pub unsafe extern "C" fn pg_get_userbyid(fcinfo: FunctionCallInfo) -> Datum {
    let roleid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut Name;
    let roletup: HeapTuple;
    let role_rec: Form_pg_authid;

    // Allocate space for the result
    result = palloc(NAMEDATALEN) as *mut Name;
    std::ptr::write_bytes(NameStr(**result).as_mut_ptr(), 0, NAMEDATALEN);

    // Get the pg_authid entry and print the result
    roletup = SearchSysCache1(AUTHOID as i32, ObjectIdGetDatum(roleid));
    if HeapTupleIsValid(roletup) {
        role_rec = GETSTRUCT(roletup) as Form_pg_authid;
        **result = (*role_rec).rolname;
        ReleaseSysCache(roletup);
    } else {
        // TODO(pg-port): sprintf
        let s = format!("unknown (OID={})\0", roleid);
        let dst = NameStr(**result).as_mut_ptr();
        std::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, dst, s.len().min(NAMEDATALEN));
    }

    PG_RETURN_NAME!(result)
}


// pg_get_serial_sequence
//    Get the name of the sequence used by an identity or serial column,
//    formatted suitably for passing to setval, nextval or currval.
//    First parameter is not treated as double-quoted, second parameter
//    is --- see documentation for reason.
pub unsafe extern "C" fn pg_get_serial_sequence(fcinfo: FunctionCallInfo) -> Datum {
    let tablename: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let columnname: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let tablerv: *mut RangeVar;
    let table_oid: Oid;
    let column: *mut c_char;
    let attnum: AttrNumber;
    let mut sequence_id: Oid = InvalidOid;
    let dep_rel: Relation;
    let mut key: [ScanKeyData; 3] = std::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    // Look up table name.  Can't lock it - we might not have privileges.
    tablerv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
    table_oid = RangeVarGetRelid(tablerv, NoLock as i32, false);

    // Get the number of the column
    column = text_to_cstring(columnname);

    attnum = get_attnum(table_oid, column);
    if attnum == InvalidAttrNumber {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(column).to_str().unwrap_or(""),
                CStr::from_ptr((*tablerv).relname).to_str().unwrap_or("")
            )
        );
    }

    // Search the dependency table for the dependent sequence
    dep_rel = table_open(DependRelationId, AccessShareLock as i32);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid as i16,
        BTEqualStrategyNumber as i32,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid as i16,
        BTEqualStrategyNumber as i32,
        F_OIDEQ,
        ObjectIdGetDatum(table_oid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_refobjsubid as i16,
        BTEqualStrategyNumber as i32,
        F_INT4EQ,
        Int32GetDatum(attnum as i32),
    );

    scan = systable_beginscan(dep_rel, DependReferenceIndexId, true, std::ptr::null_mut(), 3, key.as_mut_ptr());

    tup = systable_getnext(scan);
    while HeapTupleIsValid(tup) {
        let deprec: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        // Look for an auto dependency (serial column) or internal dependency
        // (identity column) of a sequence on a column.  (We need the relkind
        // test because indexes can also have auto dependencies on columns.)
        if (*deprec).classid == RelationRelationId
            && (*deprec).objsubid == 0
            && ((*deprec).deptype == DEPENDENCY_AUTO as c_char
                || (*deprec).deptype == DEPENDENCY_INTERNAL as c_char)
            && get_rel_relkind((*deprec).objid) == RELKIND_SEQUENCE as c_char
        {
            sequence_id = (*deprec).objid;
            break;
        }
        tup = systable_getnext(scan);
    }

    systable_endscan(scan);
    table_close(dep_rel, AccessShareLock as i32);

    if OidIsValid(sequence_id) {
        let result: *mut c_char;

        result = generate_qualified_relation_name(sequence_id);

        PG_RETURN_TEXT_P!(string_to_text(result));
    }

    PG_RETURN_NULL!(fcinfo)
}

// pg_get_functiondef
//    Returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
//    the specified function.
//
// Note: if you change the output format of this function, be careful not
// to break psql's rules (in \ef and \sf) for identifying the start of the
// function body.  To wit: the function body starts on a line that begins with
// "AS ", "BEGIN ", or "RETURN ", and no preceding line will look like that.
pub unsafe extern "C" fn pg_get_functiondef(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut dq: StringInfoData = std::mem::zeroed();
    let proctup: HeapTuple;
    let proc_: Form_pg_proc;
    let isfunction: bool;
    let mut tmp: Datum;
    let mut isnull: bool;
    let prosrc: *const c_char;
    let name: *const c_char;
    let nsp: *mut c_char;
    let procost: f32;
    let oldlen: i32;

    initStringInfo(&mut buf);

    // Look up the function
    proctup = SearchSysCache1(PROCOID as i32, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    proc_ = GETSTRUCT(proctup) as Form_pg_proc;
    name = NameStr((*proc_).proname).as_ptr();

    if (*proc_).prokind == PROKIND_AGGREGATE as c_char {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            errmsg!("\"{}\" is an aggregate function", CStr::from_ptr(name).to_str().unwrap_or(""))
        );
    }

    isfunction = (*proc_).prokind != PROKIND_PROCEDURE as c_char;

    // We always qualify the function name, to ensure the right function gets replaced.
    nsp = get_namespace_name_or_temp((*proc_).pronamespace);
    appendStringInfo!(
        &mut buf,
        "CREATE OR REPLACE {} {}(",
        if isfunction { "FUNCTION" } else { "PROCEDURE" },
        CStr::from_ptr(quote_qualified_identifier(nsp, name as *mut c_char)).to_str().unwrap_or("")
    );
    let _ = print_function_arguments(&mut buf, proctup, false, true);
    appendStringInfoString(&mut buf, cstr!(")\n"));
    if isfunction {
        appendStringInfoString(&mut buf, cstr!(" RETURNS "));
        print_function_rettype(&mut buf, proctup);
        appendStringInfoChar(&mut buf, b'\n' as c_char);
    }

    print_function_trftypes(&mut buf, proctup);

    appendStringInfo!(
        &mut buf,
        " LANGUAGE {}\n",
        CStr::from_ptr(quote_identifier(get_language_name((*proc_).prolang, false))).to_str().unwrap_or("")
    );

    // Emit some miscellaneous options on one line
    oldlen = buf.len;

    if (*proc_).prokind == PROKIND_WINDOW as c_char {
        appendStringInfoString(&mut buf, cstr!(" WINDOW"));
    }
    match (*proc_).provolatile as u8 {
        PROVOLATILE_IMMUTABLE => {
            appendStringInfoString(&mut buf, cstr!(" IMMUTABLE"));
        }
        PROVOLATILE_STABLE => {
            appendStringInfoString(&mut buf, cstr!(" STABLE"));
        }
        PROVOLATILE_VOLATILE => {}
        _ => {}
    }

    match (*proc_).proparallel as u8 {
        PROPARALLEL_SAFE => {
            appendStringInfoString(&mut buf, cstr!(" PARALLEL SAFE"));
        }
        PROPARALLEL_RESTRICTED => {
            appendStringInfoString(&mut buf, cstr!(" PARALLEL RESTRICTED"));
        }
        PROPARALLEL_UNSAFE => {}
        _ => {}
    }

    if (*proc_).proisstrict {
        appendStringInfoString(&mut buf, cstr!(" STRICT"));
    }
    if (*proc_).prosecdef {
        appendStringInfoString(&mut buf, cstr!(" SECURITY DEFINER"));
    }
    if (*proc_).proleakproof {
        appendStringInfoString(&mut buf, cstr!(" LEAKPROOF"));
    }

    // This code for the default cost and rows should match functioncmds.c
    if (*proc_).prolang == INTERNALlanguageId || (*proc_).prolang == ClanguageId {
        procost = 1.0f32;
    } else {
        procost = 100.0f32;
    }
    if (*proc_).procost != procost {
        appendStringInfo!(&mut buf, " COST {}", (*proc_).procost);
    }

    if (*proc_).prorows > 0.0f32 && (*proc_).prorows != 1000.0f32 {
        appendStringInfo!(&mut buf, " ROWS {}", (*proc_).prorows);
    }

    if (*proc_).prosupport != InvalidOid {
        let mut argtypes: [Oid; 1] = [INTERNALOID];

        // We should qualify the support function's name if it wouldn't be
        // resolved by lookup in the current search path.
        appendStringInfo!(
            &mut buf,
            " SUPPORT {}",
            CStr::from_ptr(
                generate_function_name((*proc_).prosupport, 1, NIL, argtypes.as_mut_ptr(), false, std::ptr::null_mut(), false)
            ).to_str().unwrap_or("")
        );
    }

    if oldlen != buf.len {
        appendStringInfoChar(&mut buf, b'\n' as c_char);
    }

    // Emit any proconfig options, one per line
    isnull = false;
    tmp = SysCacheGetAttr(PROCOID as i32, proctup, Anum_pg_proc_proconfig as i32, &mut isnull);
    if !isnull {
        let a: *mut ArrayType = DatumGetArrayTypeP!(tmp) as *mut ArrayType;
        let mut i: i32;

        // Assert(ARR_ELEMTYPE(a) == TEXTOID);
        // Assert(ARR_NDIM(a) == 1);
        // Assert(ARR_LBOUND(a)[0] == 1);

        i = 1;
        while i <= ARR_DIMS(a)[0] {
            let mut d: Datum;
            let mut item_null: bool = false;

            d = array_ref(
                a,
                1,
                &mut i,
                -1, // varlenarray
                -1, // TEXT's typlen
                false, // TEXT's typbyval
                TYPALIGN_INT as c_char, // TEXT's typalign
                &mut item_null,
            );
            if !item_null {
                let configitem: *mut c_char = TextDatumGetCString!(d);
                let pos: *mut c_char;

                pos = strchr(configitem, b'=' as i32);
                if pos.is_null() {
                    i += 1;
                    continue;
                }
                *pos = 0;
                let pos = pos.add(1);

                appendStringInfo!(
                    &mut buf,
                    " SET {} TO ",
                    CStr::from_ptr(quote_identifier(configitem)).to_str().unwrap_or("")
                );

                // Variables that are marked GUC_LIST_QUOTE were already fully
                // quoted by flatten_set_variable_args() before they were put
                // into the proconfig array.  However, because the quoting
                // rules used there aren't exactly like SQL's, we have to
                // break the list value apart and then quote the elements as
                // string literals.  (The elements may be double-quoted as-is,
                // but we can't just feed them to the SQL parser; it would do
                // the wrong thing with elements that are zero-length or
                // longer than NAMEDATALEN.)
                //
                // Variables that are not so marked should just be emitted as
                // simple string literals.  If the variable is not known to
                // guc.c, we'll do that; this makes it unsafe to use
                // GUC_LIST_QUOTE for extension variables.
                if (GetConfigOptionFlags(configitem, true) & GUC_LIST_QUOTE as i32) != 0 {
                    let mut namelist: *mut List = std::ptr::null_mut();
                    let mut lc: *mut ListCell;

                    // Parse string into list of identifiers
                    if !SplitGUCList(pos, b',' as c_char, &mut namelist) {
                        // this shouldn't fail really
                        elog!(ERROR, "invalid list syntax in proconfig item");
                    }
                    lc = list_head(namelist);
                    while !lc.is_null() {
                        let curname: *mut c_char = lfirst(lc) as *mut c_char;

                        simple_quote_literal(&mut buf, curname);
                        if !lnext(namelist, lc).is_null() {
                            appendStringInfoString(&mut buf, cstr!(", "));
                        }
                        lc = lnext(namelist, lc);
                    }
                } else {
                    simple_quote_literal(&mut buf, pos);
                }
                appendStringInfoChar(&mut buf, b'\n' as c_char);
            }
            i += 1;
        }
    }

    // And finally the function definition ...
    isnull = false;
    let _ = SysCacheGetAttr(PROCOID as i32, proctup, Anum_pg_proc_prosqlbody as i32, &mut isnull);
    if (*proc_).prolang == SQLlanguageId && !isnull {
        print_function_sqlbody(&mut buf, proctup);
    } else {
        appendStringInfoString(&mut buf, cstr!("AS "));

        isnull = false;
        tmp = SysCacheGetAttr(PROCOID as i32, proctup, Anum_pg_proc_probin as i32, &mut isnull);
        if !isnull {
            simple_quote_literal(&mut buf, TextDatumGetCString!(tmp));
            appendStringInfoString(&mut buf, cstr!(", ")); // assume prosrc isn't null
        }

        tmp = SysCacheGetAttrNotNull(PROCOID as i32, proctup, Anum_pg_proc_prosrc as i32);
        prosrc = TextDatumGetCString!(tmp);

        // We always use dollar quoting.  Figure out a suitable delimiter.
        //
        // Since the user is likely to be editing the function body string, we
        // shouldn't use a short delimiter that he might easily create a
        // conflict with.  Hence prefer "$function$"/"$procedure$", but extend
        // if needed.
        initStringInfo(&mut dq);
        appendStringInfoChar(&mut dq, b'$' as c_char);
        appendStringInfoString(&mut dq, if isfunction { cstr!("function") } else { cstr!("procedure") });
        while !strstr(prosrc, dq.data).is_null() {
            appendStringInfoChar(&mut dq, b'x' as c_char);
        }
        appendStringInfoChar(&mut dq, b'$' as c_char);

        appendBinaryStringInfo(&mut buf, dq.data, dq.len);
        appendStringInfoString(&mut buf, prosrc);
        appendBinaryStringInfo(&mut buf, dq.data, dq.len);
    }

    appendStringInfoChar(&mut buf, b'\n' as c_char);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P!(string_to_text(buf.data))
}

// pg_get_function_arguments
//    Get a nicely-formatted list of arguments for a function.
//    This is everything that would go between the parentheses in
//    CREATE FUNCTION.
pub unsafe extern "C" fn pg_get_function_arguments(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let proctup: HeapTuple;

    proctup = SearchSysCache1(PROCOID as i32, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    initStringInfo(&mut buf);

    let _ = print_function_arguments(&mut buf, proctup, false, true);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P!(string_to_text(buf.data))
}

// pg_get_function_identity_arguments
//    Get a formatted list of arguments for a function.
//    This is everything that would go between the parentheses in
//    ALTER FUNCTION, etc.  In particular, don't print defaults.
pub unsafe extern "C" fn pg_get_function_identity_arguments(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let proctup: HeapTuple;

    proctup = SearchSysCache1(PROCOID as i32, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    initStringInfo(&mut buf);

    let _ = print_function_arguments(&mut buf, proctup, false, false);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P!(string_to_text(buf.data))
}

// pg_get_function_result
//    Get a nicely-formatted version of the result type of a function.
//    This is what would appear after RETURNS in CREATE FUNCTION.
pub unsafe extern "C" fn pg_get_function_result(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let proctup: HeapTuple;

    proctup = SearchSysCache1(PROCOID as i32, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    if (GETSTRUCT(proctup) as Form_pg_proc != std::ptr::null_mut())
        && (*(GETSTRUCT(proctup) as Form_pg_proc)).prokind == PROKIND_PROCEDURE as c_char
    {
        ReleaseSysCache(proctup);
        return PG_RETURN_NULL!(fcinfo);
    }

    initStringInfo(&mut buf);

    print_function_rettype(&mut buf, proctup);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P!(string_to_text(buf.data))
}

// Guts of pg_get_function_result: append the function's return type
// to the specified buffer.
unsafe fn print_function_rettype(buf: *mut StringInfoData, proctup: HeapTuple) {
    let proc_: Form_pg_proc = GETSTRUCT(proctup) as Form_pg_proc;
    let mut ntabargs: i32 = 0;
    let mut rbuf: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut rbuf);

    if (*proc_).proretset {
        // It might be a table function; try to print the arguments
        appendStringInfoString(&mut rbuf, cstr!("TABLE("));
        ntabargs = print_function_arguments(&mut rbuf, proctup, true, false);
        if ntabargs > 0 {
            appendStringInfoChar(&mut rbuf, b')' as c_char);
        } else {
            resetStringInfo(&mut rbuf);
        }
    }

    if ntabargs == 0 {
        // Not a table function, so do the normal thing
        if (*proc_).proretset {
            appendStringInfoString(&mut rbuf, cstr!("SETOF "));
        }
        appendStringInfoString(&mut rbuf, format_type_be((*proc_).prorettype));
    }

    appendBinaryStringInfo(buf, rbuf.data, rbuf.len);
}

// Common code for pg_get_function_arguments and pg_get_function_result:
// append the desired subset of arguments to buf.  We print only TABLE
// arguments when print_table_args is true, and all the others when it's false.
// We print argument defaults only if print_defaults is true.
// Function return value is the number of arguments printed.
unsafe fn print_function_arguments(
    buf: *mut StringInfoData,
    proctup: HeapTuple,
    print_table_args: bool,
    print_defaults: bool,
) -> i32 {
    let proc_: Form_pg_proc = GETSTRUCT(proctup) as Form_pg_proc;
    let mut numargs: i32;
    let mut argtypes: *mut Oid = std::ptr::null_mut();
    let mut argnames: *mut *mut c_char = std::ptr::null_mut();
    let mut argmodes: *mut c_char = std::ptr::null_mut();
    let mut insertorderbyat: i32 = -1;
    let mut argsprinted: i32;
    let mut inputargno: i32;
    let mut nlackdefaults: i32;
    let mut argdefaults: *mut List = NIL;
    let mut nextargdefault: *mut ListCell = std::ptr::null_mut();
    let mut i: i32;
    let mut print_defaults = print_defaults;

    numargs = get_func_arg_info(proctup, &mut argtypes, &mut argnames, &mut argmodes);

    nlackdefaults = numargs;
    if print_defaults && (*proc_).pronargdefaults > 0 {
        let mut proargdefaults: Datum;
        let mut isnull: bool = false;

        proargdefaults = SysCacheGetAttr(PROCOID as i32, proctup, Anum_pg_proc_proargdefaults as i32, &mut isnull);
        if !isnull {
            let mut str_: *mut c_char;

            str_ = TextDatumGetCString!(proargdefaults);
            argdefaults = castNode!(List, T_List, stringToNode(str_)) as *mut List;
            pfree(str_ as *mut c_void);
            nextargdefault = list_head(argdefaults);
            // nlackdefaults counts only *input* arguments lacking defaults
            nlackdefaults = (*proc_).pronargs as i32 - list_length(argdefaults);
        }
    }

    // Check for special treatment of ordered-set aggregates
    if (*proc_).prokind == PROKIND_AGGREGATE as c_char {
        let aggtup: HeapTuple;
        let agg: Form_pg_aggregate;

        aggtup = SearchSysCache1(AGGFNOID as i32, ObjectIdGetDatum((*proc_).oid));
        if !HeapTupleIsValid(aggtup) {
            elog!(ERROR, "cache lookup failed for aggregate {}", (*proc_).oid);
        }
        agg = GETSTRUCT(aggtup) as Form_pg_aggregate;
        if AGGKIND_IS_ORDERED_SET!((*agg).aggkind) {
            insertorderbyat = (*agg).aggnumdirectargs as i32;
        }
        ReleaseSysCache(aggtup);
    }

    argsprinted = 0;
    inputargno = 0;
    i = 0;
    while i < numargs {
        let argtype: Oid = *argtypes.add(i as usize);
        let argname: *mut c_char = if !argnames.is_null() { *argnames.add(i as usize) } else { std::ptr::null_mut() };
        let argmode: c_char = if !argmodes.is_null() { *argmodes.add(i as usize) } else { PROARGMODE_IN as c_char };
        let modename: &str;
        let isinput: bool;

        match argmode as u8 {
            PROARGMODE_IN => {
                // For procedures, explicitly mark all argument modes, so as
                // to avoid ambiguity with the SQL syntax for DROP PROCEDURE.
                if (*proc_).prokind == PROKIND_PROCEDURE as c_char {
                    modename = "IN ";
                } else {
                    modename = "";
                }
                isinput = true;
            }
            PROARGMODE_INOUT => {
                modename = "INOUT ";
                isinput = true;
            }
            PROARGMODE_OUT => {
                modename = "OUT ";
                isinput = false;
            }
            PROARGMODE_VARIADIC => {
                modename = "VARIADIC ";
                isinput = true;
            }
            PROARGMODE_TABLE => {
                modename = "";
                isinput = false;
            }
            _ => {
                elog!(ERROR, "invalid parameter mode '{}'", argmode as u8 as char);
                modename = "";
                isinput = false;
            }
        }
        if isinput {
            inputargno += 1; // this is a 1-based counter
        }

        if print_table_args != (argmode as u8 == PROARGMODE_TABLE) {
            i += 1;
            continue;
        }

        if argsprinted == insertorderbyat {
            if argsprinted != 0 {
                appendStringInfoChar(buf, b' ' as c_char);
            }
            appendStringInfoString(buf, cstr!("ORDER BY "));
        } else if argsprinted != 0 {
            appendStringInfoString(buf, cstr!(", "));
        }

        appendStringInfoString(buf, modename.as_ptr() as *const c_char);
        if !argname.is_null() && *argname != 0 {
            appendStringInfo!(
                buf,
                "{} ",
                CStr::from_ptr(quote_identifier(argname)).to_str().unwrap_or("")
            );
        }
        appendStringInfoString(buf, format_type_be(argtype));
        if print_defaults && isinput && inputargno > nlackdefaults {
            let expr: *mut Node;

            // Assert(nextargdefault != NULL);
            expr = lfirst(nextargdefault) as *mut Node;
            nextargdefault = lnext(argdefaults, nextargdefault);

            appendStringInfo!(
                buf,
                " DEFAULT {}",
                CStr::from_ptr(deparse_expression(expr, NIL, false, false)).to_str().unwrap_or("")
            );
        }
        argsprinted += 1;

        // nasty hack: print the last arg twice for variadic ordered-set agg
        if argsprinted == insertorderbyat && i == numargs - 1 {
            i -= 1;
            // aggs shouldn't have defaults anyway, but just to be sure ...
            print_defaults = false;
        }

        i += 1;
    }

    argsprinted
}

unsafe fn is_input_argument(nth: i32, argmodes: *const c_char) -> bool {
    argmodes.is_null()
        || *argmodes.add(nth as usize) == PROARGMODE_IN as c_char
        || *argmodes.add(nth as usize) == PROARGMODE_INOUT as c_char
        || *argmodes.add(nth as usize) == PROARGMODE_VARIADIC as c_char
}

// Append used transformed types to specified buffer
unsafe fn print_function_trftypes(buf: *mut StringInfoData, proctup: HeapTuple) {
    let mut trftypes: *mut Oid = std::ptr::null_mut();
    let ntypes: i32;

    ntypes = get_func_trftypes(proctup, &mut trftypes);
    if ntypes > 0 {
        let mut i: i32;

        appendStringInfoString(buf, cstr!(" TRANSFORM "));
        i = 0;
        while i < ntypes {
            if i != 0 {
                appendStringInfoString(buf, cstr!(", "));
            }
            appendStringInfo!(
                buf,
                "FOR TYPE {}",
                CStr::from_ptr(format_type_be(*trftypes.add(i as usize))).to_str().unwrap_or("")
            );
            i += 1;
        }
        appendStringInfoChar(buf, b'\n' as c_char);
    }
}

// Get textual representation of a function argument's default value.  The
// second argument of this function is the argument number among all arguments
// (i.e. proallargtypes, *not* proargtypes), starting with 1, because that's
// how information_schema.sql uses it.
pub unsafe extern "C" fn pg_get_function_arg_default(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let nth_arg: i32 = PG_GETARG_INT32!(fcinfo, 1);
    let proctup: HeapTuple;
    let proc_: Form_pg_proc;
    let mut numargs: i32;
    let mut argtypes: *mut Oid = std::ptr::null_mut();
    let mut argnames: *mut *mut c_char = std::ptr::null_mut();
    let mut argmodes: *mut c_char = std::ptr::null_mut();
    let mut i: i32;
    let argdefaults: *mut List;
    let node: *mut Node;
    let str_: *mut c_char;
    let mut nth_inputarg: i32;
    let proargdefaults: Datum;
    let mut isnull: bool = false;
    let nth_default: i32;

    proctup = SearchSysCache1(PROCOID as i32, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    numargs = get_func_arg_info(proctup, &mut argtypes, &mut argnames, &mut argmodes);
    if nth_arg < 1 || nth_arg > numargs || !is_input_argument(nth_arg - 1, argmodes) {
        ReleaseSysCache(proctup);
        return PG_RETURN_NULL!(fcinfo);
    }

    nth_inputarg = 0;
    i = 0;
    while i < nth_arg {
        if is_input_argument(i, argmodes) {
            nth_inputarg += 1;
        }
        i += 1;
    }

    proargdefaults = SysCacheGetAttr(PROCOID as i32, proctup, Anum_pg_proc_proargdefaults as i32, &mut isnull);
    if isnull {
        ReleaseSysCache(proctup);
        return PG_RETURN_NULL!(fcinfo);
    }

    let tmp_str = TextDatumGetCString!(proargdefaults);
    argdefaults = castNode!(List, T_List, stringToNode(tmp_str)) as *mut List;
    pfree(tmp_str as *mut c_void);

    proc_ = GETSTRUCT(proctup) as Form_pg_proc;

    // Calculate index into proargdefaults: proargdefaults corresponds to the
    // last N input arguments, where N = pronargdefaults.
    nth_default = nth_inputarg - 1 - ((*proc_).pronargs as i32 - (*proc_).pronargdefaults as i32);

    if nth_default < 0 || nth_default >= list_length(argdefaults) {
        ReleaseSysCache(proctup);
        return PG_RETURN_NULL!(fcinfo);
    }
    node = list_nth(argdefaults, nth_default) as *mut Node;
    str_ = deparse_expression(node, NIL, false, false);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P!(string_to_text(str_))
}

unsafe fn print_function_sqlbody(buf: *mut StringInfoData, proctup: HeapTuple) {
    let mut numargs: i32;
    let mut argtypes: *mut Oid = std::ptr::null_mut();
    let mut argnames: *mut *mut c_char = std::ptr::null_mut();
    let mut argmodes: *mut c_char = std::ptr::null_mut();
    let mut dpns: deparse_namespace = std::mem::zeroed();
    let tmp: Datum;
    let n: *mut Node;

    dpns.funcname = pstrdup(NameStr((*( GETSTRUCT(proctup) as Form_pg_proc)).proname).as_ptr());
    numargs = get_func_arg_info(proctup, &mut argtypes, &mut argnames, &mut argmodes);
    dpns.numargs = numargs;
    dpns.argnames = argnames;

    tmp = SysCacheGetAttrNotNull(PROCOID as i32, proctup, Anum_pg_proc_prosqlbody as i32);
    n = stringToNode(TextDatumGetCString!(tmp)) as *mut Node;

    if IsA(n as *mut c_void, List) {
        let stmts: *mut List;
        let mut lc: *mut ListCell;

        stmts = linitial(n as *mut List) as *mut List;
        // TODO(pg-port): castNode for List

        appendStringInfoString(buf, cstr!("BEGIN ATOMIC\n"));

        lc = list_head(stmts);
        while !lc.is_null() {
            let query: *mut Query = lfirst_node!(Query, T_Query, lc) as *mut Query;

            // It seems advisable to get at least AccessShareLock on rels
            AcquireRewriteLocks(query, false, false);
            get_query_def(
                query,
                buf,
                list_make1(&mut dpns as *mut deparse_namespace as *mut c_void),
                std::ptr::null_mut(),
                false,
                PRETTYFLAG_INDENT,
                WRAP_COLUMN_DEFAULT,
                1,
            );
            appendStringInfoChar(buf, b';' as c_char);
            appendStringInfoChar(buf, b'\n' as c_char);

            lc = lnext(stmts, lc);
        }

        appendStringInfoString(buf, cstr!("END"));
    } else {
        let query: *mut Query = n as *mut Query;
        // TODO(pg-port): castNode for Query

        // It seems advisable to get at least AccessShareLock on rels
        AcquireRewriteLocks(query, false, false);
        get_query_def(
            query,
            buf,
            list_make1(&mut dpns as *mut deparse_namespace as *mut c_void),
            std::ptr::null_mut(),
            false,
            0,
            WRAP_COLUMN_DEFAULT,
            0,
        );
    }
}

pub unsafe extern "C" fn pg_get_function_sqlbody(fcinfo: FunctionCallInfo) -> Datum {
    let funcid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let proctup: HeapTuple;
    let mut isnull: bool = false;

    initStringInfo(&mut buf);

    // Look up the function
    proctup = SearchSysCache1(PROCOID as i32, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        return PG_RETURN_NULL!(fcinfo);
    }

    let _ = SysCacheGetAttr(PROCOID as i32, proctup, Anum_pg_proc_prosqlbody as i32, &mut isnull);
    if isnull {
        ReleaseSysCache(proctup);
        return PG_RETURN_NULL!(fcinfo);
    }

    print_function_sqlbody(&mut buf, proctup);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len(buf.data, buf.len))
}

// ----------
// deparse_expression - General utility for deparsing expressions
//
// calls deparse_expression_pretty with all prettyPrinting disabled
// ----------
pub unsafe fn deparse_expression(
    expr: *mut Node,
    dpcontext: *mut List,
    forceprefix: bool,
    showimplicit: bool,
) -> *mut c_char {
    deparse_expression_pretty(expr, dpcontext, forceprefix, showimplicit, 0, 0)
}

// ----------
// deparse_expression_pretty - General utility for deparsing expressions
//
// expr is the node tree to be deparsed.  It must be a transformed expression
// tree (ie, not the raw output of gram.y).
//
// dpcontext is a list of deparse_namespace nodes representing the context
// for interpreting Vars in the node tree.  It can be NIL if no Vars are
// expected.
//
// forceprefix is true to force all Vars to be prefixed with their table names.
//
// showimplicit is true to force all implicit casts to be shown explicitly.
//
// Tries to pretty up the output according to prettyFlags and startIndent.
//
// The result is a palloc'd string.
// ----------
pub(crate) unsafe fn deparse_expression_pretty(
    expr: *mut Node,
    dpcontext: *mut List,
    forceprefix: bool,
    showimplicit: bool,
    pretty_flags: i32,
    start_indent: i32,
) -> *mut c_char {
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut context: deparse_context = std::mem::zeroed();

    initStringInfo(&mut buf);
    context.buf = &mut buf;
    context.namespaces = dpcontext;
    context.resultDesc = std::ptr::null_mut();
    context.targetList = NIL;
    context.windowClause = NIL;
    context.varprefix = forceprefix;
    context.prettyFlags = pretty_flags;
    context.wrapColumn = WRAP_COLUMN_DEFAULT;
    context.indentLevel = start_indent;
    context.colNamesVisible = true;
    context.inGroupBy = false;
    context.varInOrderBy = false;
    context.appendparents = std::ptr::null_mut();

    get_rule_expr(expr, &mut context, showimplicit);

    buf.data
}

// ----------
// deparse_context_for - Build deparse context for a single relation
//
// Given the reference name (alias) and OID of a relation, build deparsing
// context for an expression referencing only that relation (as varno 1,
// varlevelsup 0).  This is sufficient for many uses of deparse_expression.
// ----------
pub unsafe fn deparse_context_for(aliasname: *const c_char, relid: Oid) -> *mut List {
    let dpns: *mut deparse_namespace;
    let rte: *mut RangeTblEntry;

    dpns = palloc0(std::mem::size_of::<deparse_namespace>()) as *mut deparse_namespace;

    // Build a minimal RTE for the rel
    rte = makeNode!(RangeTblEntry, T_RangeTblEntry) as *mut RangeTblEntry;
    (*rte).rtekind = RTE_RELATION;
    (*rte).relid = relid;
    (*rte).relkind = RELKIND_RELATION as c_char; // no need for exactness here
    (*rte).rellockmode = AccessShareLock as i32;
    (*rte).alias = makeAlias(aliasname, NIL);
    (*rte).eref = (*rte).alias;
    (*rte).lateral = false;
    (*rte).inh = false;
    (*rte).inFromCl = true;

    // Build one-element rtable
    (*dpns).rtable = list_make1(rte as *mut c_void);
    (*dpns).subplans = NIL;
    (*dpns).ctes = NIL;
    (*dpns).appendrels = std::ptr::null_mut();
    set_rtable_names(dpns, NIL, std::ptr::null_mut());
    set_simple_column_names(dpns);

    // Return a one-deep namespace stack
    list_make1(dpns as *mut c_void)
}

// deparse_context_for_plan_tree - Build deparse context for a Plan tree
//
// When deparsing an expression in a Plan tree, we use the plan's rangetable
// to resolve names of simple Vars.  The initialization of column names for
// this is rather expensive if the rangetable is large, and it'll be the same
// for every expression in the Plan tree; so we do it just once and re-use
// the result of this function for each expression.  (Note that the result
// is not usable until set_deparse_context_plan() is applied to it.)
//
// In addition to the PlannedStmt, pass the per-RTE alias names
// assigned by a previous call to select_rtable_names_for_explain.
pub unsafe fn deparse_context_for_plan_tree(pstmt: *mut PlannedStmt, rtable_names: *mut List) -> *mut List {
    let dpns: *mut deparse_namespace;

    dpns = palloc0(std::mem::size_of::<deparse_namespace>()) as *mut deparse_namespace;

    // Initialize fields that stay the same across the whole plan tree
    (*dpns).rtable = (*pstmt).rtable;
    (*dpns).rtable_names = rtable_names;
    (*dpns).subplans = (*pstmt).subplans;
    (*dpns).ctes = NIL;
    if !(*pstmt).appendRelations.is_null() {
        // Set up the array, indexed by child relid
        let ntables: i32 = list_length((*dpns).rtable);
        let mut lc: *mut ListCell;

        (*dpns).appendrels = palloc0((ntables as usize + 1) * std::mem::size_of::<*mut AppendRelInfo>()) as *mut *mut AppendRelInfo;
        lc = list_head((*pstmt).appendRelations);
        while !lc.is_null() {
            let appinfo: *mut AppendRelInfo = lfirst_node!(AppendRelInfo, T_AppendRelInfo, lc) as *mut AppendRelInfo;
            let crelid: Index = (*appinfo).child_relid;

            // Assert(crelid > 0 && crelid <= ntables);
            // Assert((*dpns).appendrels[crelid] == NULL);
            *(*dpns).appendrels.add(crelid as usize) = appinfo;

            lc = lnext((*pstmt).appendRelations, lc);
        }
    } else {
        (*dpns).appendrels = std::ptr::null_mut(); // don't need it
    }

    // Set up column name aliases, ignoring any join RTEs; they don't matter
    // because plan trees don't contain any join alias Vars.
    set_simple_column_names(dpns);

    // Return a one-deep namespace stack
    list_make1(dpns as *mut c_void)
}

// set_deparse_context_plan - Specify Plan node containing expression
//
// When deparsing an expression in a Plan tree, we might have to resolve
// OUTER_VAR, INNER_VAR, or INDEX_VAR references.  To do this, the caller must
// provide the parent Plan node.  Then OUTER_VAR and INNER_VAR references
// can be resolved by drilling down into the left and right child plans.
// Similarly, INDEX_VAR references can be resolved by reference to the
// indextlist given in a parent IndexOnlyScan node, or to the scan tlist in
// ForeignScan and CustomScan nodes.  (Note that we don't currently support
// deparsing of indexquals in regular IndexScan or BitmapIndexScan nodes;
// for those, we can only deparse the indexqualorig fields, which won't
// contain INDEX_VAR Vars.)
//
// The ancestors list is a list of the Plan's parent Plan and SubPlan nodes,
// the most-closely-nested first.  This is needed to resolve PARAM_EXEC
// Params.  Note we assume that all the Plan nodes share the same rtable.
//
// For a ModifyTable plan, we might also need to resolve references to OLD/NEW
// variables in the RETURNING list, so we copy the alias names of the OLD and
// NEW rows from the ModifyTable plan node.
//
// Once this function has been called, deparse_expression() can be called on
// subsidiary expression(s) of the specified Plan node.  To deparse
// expressions of a different Plan node in the same Plan tree, re-call this
// function to identify the new parent Plan node.
//
// The result is the same List passed in; this is a notational convenience.
pub unsafe fn set_deparse_context_plan(dpcontext: *mut List, plan: *mut Plan, ancestors: *mut List) -> *mut List {
    let dpns: *mut deparse_namespace;

    // Should always have one-entry namespace list for Plan deparsing
    // Assert(list_length(dpcontext) == 1);
    dpns = linitial(dpcontext) as *mut deparse_namespace;

    // Set our attention on the specific plan node passed in
    (*dpns).ancestors = ancestors;
    set_deparse_plan(dpns, plan);

    // For ModifyTable, set aliases for OLD and NEW in RETURNING
    if IsA(plan as *mut c_void, ModifyTable) {
        (*dpns).ret_old_alias = (*(plan as *mut ModifyTable)).returningOldAlias;
        (*dpns).ret_new_alias = (*(plan as *mut ModifyTable)).returningNewAlias;
    }

    dpcontext
}

// select_rtable_names_for_explain - Select RTE aliases for EXPLAIN
//
// Determine the relation aliases we'll use during an EXPLAIN operation.
// This is just a frontend to set_rtable_names.  We have to expose the aliases
// to EXPLAIN because EXPLAIN needs to know the right alias names to print.
pub unsafe fn select_rtable_names_for_explain(rtable: *mut List, rels_used: Bitmapset) -> *mut List {
    let mut dpns: deparse_namespace = std::mem::zeroed();

    std::ptr::write_bytes(&mut dpns as *mut deparse_namespace, 0, 1);
    dpns.rtable = rtable;
    dpns.subplans = NIL;
    dpns.ctes = NIL;
    dpns.appendrels = std::ptr::null_mut();
    set_rtable_names(&mut dpns, NIL, rels_used as *mut Bitmapset);
    // We needn't bother computing column aliases yet

    dpns.rtable_names
}

// set_rtable_names: select RTE aliases to be used in printing a query
//
// We fill in dpns->rtable_names with a list of names that is one-for-one with
// the already-filled dpns->rtable list.  Each RTE name is unique among those
// in the new namespace plus any ancestor namespaces listed in
// parent_namespaces.
//
// If rels_used isn't NULL, only RTE indexes listed in it are given aliases.
//
// Note that this function is only concerned with relation names, not column names.
pub(crate) unsafe fn set_rtable_names(
    dpns: *mut deparse_namespace,
    parent_namespaces: *mut List,
    rels_used: *mut Bitmapset,
) {
    let mut hash_ctl: HASHCTL = std::mem::zeroed();
    let names_hash: *mut HTAB;
    let mut hentry: *mut NameHashEntry;
    let mut found: bool = false;
    let mut rtindex: i32;
    let mut lc: *mut ListCell;

    (*dpns).rtable_names = NIL;
    // nothing more to do if empty rtable
    if (*dpns).rtable == NIL {
        return;
    }

    // We use a hash table to hold known names, so that this process is O(N)
    // not O(N^2) for N names.
    hash_ctl.keysize = NAMEDATALEN as u64;
    hash_ctl.entrysize = std::mem::size_of::<NameHashEntry>() as u64;
    hash_ctl.hcxt = CurrentMemoryContext;
    names_hash = hash_create(
        cstr!("set_rtable_names names"),
        list_length((*dpns).rtable),
        &mut hash_ctl,
        (HASH_ELEM | HASH_STRINGS | HASH_CONTEXT) as i32,
    );

    // Preload the hash table with names appearing in parent_namespaces
    lc = list_head(parent_namespaces);
    while !lc.is_null() {
        let olddpns: *mut deparse_namespace = lfirst(lc) as *mut deparse_namespace;
        let mut lc2: *mut ListCell;

        lc2 = list_head((*olddpns).rtable_names);
        while !lc2.is_null() {
            let oldname: *mut c_char = lfirst(lc2) as *mut c_char;

            if oldname.is_null() {
                lc2 = lnext((*olddpns).rtable_names, lc2);
                continue;
            }
            hentry = hash_search(names_hash, oldname as *const c_void, HASH_ENTER as i32, &mut found) as *mut NameHashEntry;
            // we do not complain about duplicate names in parent namespaces
            (*hentry).counter = 0;

            lc2 = lnext((*olddpns).rtable_names, lc2);
        }

        lc = lnext(parent_namespaces, lc);
    }

    // Now we can scan the rtable
    rtindex = 1;
    lc = list_head((*dpns).rtable);
    while !lc.is_null() {
        let rte: *mut RangeTblEntry = lfirst(lc) as *mut RangeTblEntry;
        let mut refname: *mut c_char;

        // Just in case this takes an unreasonable amount of time ...
        CHECK_FOR_INTERRUPTS!();

        if !rels_used.is_null() && !bms_is_member(rtindex, rels_used as Bitmapset) {
            // Ignore unreferenced RTE
            refname = std::ptr::null_mut();
        } else if !(*rte).alias.is_null() {
            // If RTE has a user-defined alias, prefer that
            refname = (*(*rte).alias).aliasname;
        } else if (*rte).rtekind == RTE_RELATION as i32 {
            // Use the current actual name of the relation
            refname = get_rel_name((*rte).relid);
        } else if (*rte).rtekind == RTE_JOIN as i32 {
            // Unnamed join has no refname
            refname = std::ptr::null_mut();
        } else {
            // Otherwise use whatever the parser assigned
            refname = (*(*rte).eref).aliasname;
        }

        // If the selected name isn't unique, append digits to make it so, and
        // make a new hash entry for it once we've got a unique name.  For a
        // very long input name, we might have to truncate to stay within
        // NAMEDATALEN.
        if !refname.is_null() {
            hentry = hash_search(names_hash, refname as *const c_void, HASH_ENTER as i32, &mut found) as *mut NameHashEntry;
            if found {
                // Name already in use, must choose a new one
                let refnamelen: usize = strlen(refname);
                let mut refnamelen = refnamelen;
                let modname: *mut c_char = palloc(refnamelen + 16) as *mut c_char;
                let mut hentry2: *mut NameHashEntry;

                loop {
                    (*hentry).counter += 1;
                    loop {
                        std::ptr::copy_nonoverlapping(refname, modname, refnamelen);
                        let suffix = format!("_{}\0", (*hentry).counter);
                        std::ptr::copy_nonoverlapping(
                            suffix.as_ptr() as *const c_char,
                            modname.add(refnamelen),
                            suffix.len(),
                        );
                        if strlen(modname) < NAMEDATALEN {
                            break;
                        }
                        // drop chars from refname to keep all the digits
                        refnamelen = pg_mbcliplen(refname, refnamelen as i32, refnamelen as i32 - 1) as usize;
                    }
                    hentry2 = hash_search(names_hash, modname as *const c_void, HASH_ENTER as i32, &mut found) as *mut NameHashEntry;
                    if !found {
                        break;
                    }
                }
                (*hentry2).counter = 0; // init new hash entry
                refname = modname;
            } else {
                // Name not previously used, need only initialize hentry
                (*hentry).counter = 0;
            }
        }

        (*dpns).rtable_names = lappend((*dpns).rtable_names, refname as *mut c_void);
        rtindex += 1;

        lc = lnext((*dpns).rtable, lc);
    }

    hash_destroy(names_hash);
}

// ---------------------------------------------------------------------------
// set_deparse_for_query, set_simple_column_names and helpers
// ---------------------------------------------------------------------------

// set_deparse_for_query: fill in column alias info for a Query
unsafe fn set_deparse_for_query(
    dpns: *mut deparse_namespace,
    query: *mut Query,
    parent_namespaces: *mut List,
) {
    // Initialize *dpns and fill rtable/ctes links
    libc::memset(dpns as *mut c_void, 0, std::mem::size_of::<deparse_namespace>());
    (*dpns).rtable = (*query).rtable;
    (*dpns).subplans = std::ptr::null_mut();
    (*dpns).ctes = (*query).cteList;
    (*dpns).appendrels = std::ptr::null_mut();
    (*dpns).ret_old_alias = (*query).returningOldAlias;
    (*dpns).ret_new_alias = (*query).returningNewAlias;

    // Assign a unique relation alias to each RTE
    set_rtable_names(dpns, parent_namespaces, std::ptr::null_mut());

    // Initialize dpns->rtable_columns to contain zeroed structs
    (*dpns).rtable_columns = std::ptr::null_mut();
    while list_length((*dpns).rtable_columns) < list_length((*dpns).rtable) {
        (*dpns).rtable_columns = lappend(
            (*dpns).rtable_columns,
            palloc0(std::mem::size_of::<deparse_columns>()) as *mut c_void,
        );
    }

    // If it's a utility query, it won't have a jointree
    if !(*query).jointree.is_null() {
        // Detect whether global uniqueness of USING names is needed
        (*dpns).unique_using =
            has_dangerous_join_using(dpns, (*query).jointree as *mut Node);

        // Select names for columns merged by USING, via a recursive pass over
        // the query jointree.
        set_using_names(dpns, (*query).jointree as *mut Node, std::ptr::null_mut());
    }

    // Now assign remaining column aliases for each RTE.
    // JOIN RTEs must be processed after their children, but they appear later
    // in the rtable list than their children.
    let mut lc = list_head((*dpns).rtable);
    let mut lc2 = list_head((*dpns).rtable_columns);
    while !lc.is_null() {
        let rte = lfirst(lc) as *mut RangeTblEntry;
        let colinfo = lfirst(lc2) as *mut deparse_columns;
        if (*rte).rtekind == RTE_JOIN {
            set_join_column_names(dpns, rte, colinfo);
        } else {
            set_relation_column_names(dpns, rte, colinfo);
        }
        lc = lnext((*dpns).rtable, lc);
        lc2 = lnext((*dpns).rtable_columns, lc2);
    }
}

// set_simple_column_names: fill in column aliases for non-query situations
// (EXPLAIN and cases where we only have relation RTEs)
unsafe fn set_simple_column_names(dpns: *mut deparse_namespace) {
    // Initialize dpns->rtable_columns to contain zeroed structs
    (*dpns).rtable_columns = std::ptr::null_mut();
    while list_length((*dpns).rtable_columns) < list_length((*dpns).rtable) {
        (*dpns).rtable_columns = lappend(
            (*dpns).rtable_columns,
            palloc0(std::mem::size_of::<deparse_columns>()) as *mut c_void,
        );
    }

    // Assign unique column aliases within each non-join RTE
    let mut lc = list_head((*dpns).rtable);
    let mut lc2 = list_head((*dpns).rtable_columns);
    while !lc.is_null() {
        let rte = lfirst(lc) as *mut RangeTblEntry;
        let colinfo = lfirst(lc2) as *mut deparse_columns;
        if (*rte).rtekind != RTE_JOIN {
            set_relation_column_names(dpns, rte, colinfo);
        }
        lc = lnext((*dpns).rtable, lc);
        lc2 = lnext((*dpns).rtable_columns, lc2);
    }
}

// has_dangerous_join_using: search jointree for unnamed JOIN USING that
// requires globally-unique column aliases.
unsafe fn has_dangerous_join_using(dpns: *mut deparse_namespace, jtnode: *mut Node) -> bool {
    if IsA(jtnode, T_RangeTblRef) {
        // nothing to do here
    } else if IsA(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let mut lc = list_head((*f).fromlist);
        while !lc.is_null() {
            if has_dangerous_join_using(dpns, lfirst(lc) as *mut Node) {
                return true;
            }
            lc = lnext((*f).fromlist, lc);
        }
    } else if IsA(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;

        // Is it an unnamed JOIN with USING?
        if (*j).alias.is_null() && !(*j).usingClause.is_null() {
            // Yes, so check each join alias var to see if any of them are
            // not simple references to underlying columns.
            let jrte = rt_fetch((*j).rtindex, (*dpns).rtable);
            // We need only examine the merged columns
            for i in 0..(*jrte).joinmergedcols {
                let aliasvar = list_nth((*jrte).joinaliasvars, i) as *mut Node;
                if !IsA(aliasvar, T_Var) {
                    return true;
                }
            }
        }

        // Nope, but inspect children
        if has_dangerous_join_using(dpns, (*j).larg as *mut Node) {
            return true;
        }
        if has_dangerous_join_using(dpns, (*j).rarg as *mut Node) {
            return true;
        }
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    false
}

// set_using_names: select column aliases to be used for merged USING columns.
// dpns->unique_using must already be set.
// parentUsing is a list of all USING aliases assigned in parent joins.
unsafe fn set_using_names(
    dpns: *mut deparse_namespace,
    jtnode: *mut Node,
    parent_using: *mut List,
) {
    if IsA(jtnode, T_RangeTblRef) {
        // nothing to do now
    } else if IsA(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let mut lc = list_head((*f).fromlist);
        while !lc.is_null() {
            set_using_names(dpns, lfirst(lc) as *mut Node, parent_using);
            lc = lnext((*f).fromlist, lc);
        }
    } else if IsA(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;
        let rte = rt_fetch((*j).rtindex, (*dpns).rtable);
        let colinfo = deparse_columns_fetch((*j).rtindex, dpns);
        let leftattnos: *mut i32;
        let rightattnos: *mut i32;
        let leftcolinfo: *mut deparse_columns;
        let rightcolinfo: *mut deparse_columns;

        // Get info about the shape of the join
        identify_join_columns(j, rte, colinfo);
        leftattnos = (*colinfo).leftattnos;
        rightattnos = (*colinfo).rightattnos;

        // Look up the not-yet-filled-in child deparse_columns structs
        leftcolinfo = deparse_columns_fetch((*colinfo).leftrti, dpns);
        rightcolinfo = deparse_columns_fetch((*colinfo).rightrti, dpns);

        // If this join is unnamed, push down any required names to children.
        if (*rte).alias.is_null() {
            for i in 0..(*colinfo).num_cols {
                let colname = *(*colinfo).colnames.add(i as usize);
                if colname.is_null() {
                    continue;
                }
                // Push down to left column, unless it's a system column
                let la = *leftattnos.add(i as usize);
                if la > 0 {
                    expand_colnames_array_to(leftcolinfo, la);
                    *(*leftcolinfo).colnames.add((la - 1) as usize) = colname;
                }
                // Same on the righthand side
                let ra = *rightattnos.add(i as usize);
                if ra > 0 {
                    expand_colnames_array_to(rightcolinfo, ra);
                    *(*rightcolinfo).colnames.add((ra - 1) as usize) = colname;
                }
            }
        }

        // If there's a USING clause, select the USING column names and push
        // those names down to the children.
        let mut cur_parent_using = parent_using;
        if !(*j).usingClause.is_null() {
            // Copy the input parentUsing list so we don't modify it
            cur_parent_using = list_copy(cur_parent_using);

            // USING names must correspond to the first join output columns
            expand_colnames_array_to(colinfo, list_length((*j).usingClause));
            let mut i = 0i32;
            let mut lc = list_head((*j).usingClause);
            while !lc.is_null() {
                let mut colname = strVal(lfirst(lc) as *mut Node) as *mut c_char;

                // Adopt passed-down name if any, else select unique name
                let existing = *(*colinfo).colnames.add(i as usize);
                if !existing.is_null() {
                    colname = existing;
                } else {
                    // Prefer user-written output alias if any
                    if !(*rte).alias.is_null()
                        && i < list_length((*(*rte).alias).colnames)
                    {
                        colname = strVal(
                            list_nth((*(*rte).alias).colnames, i) as *mut Node,
                        ) as *mut c_char;
                    }
                    // Make it appropriately unique
                    colname = make_colname_unique(colname, dpns, colinfo);
                    if (*dpns).unique_using {
                        (*dpns).using_names =
                            lappend((*dpns).using_names, colname as *mut c_void);
                    }
                    // Save it as output column name, too
                    *(*colinfo).colnames.add(i as usize) = colname;
                }

                // Remember selected names for use later
                (*colinfo).usingNames =
                    lappend((*colinfo).usingNames, colname as *mut c_void);
                cur_parent_using =
                    lappend(cur_parent_using, colname as *mut c_void);

                // Push down to left column, unless it's a system column
                let la = *leftattnos.add(i as usize);
                if la > 0 {
                    expand_colnames_array_to(leftcolinfo, la);
                    *(*leftcolinfo).colnames.add((la - 1) as usize) = colname;
                }
                // Same on the righthand side
                let ra = *rightattnos.add(i as usize);
                if ra > 0 {
                    expand_colnames_array_to(rightcolinfo, ra);
                    *(*rightcolinfo).colnames.add((ra - 1) as usize) = colname;
                }

                i += 1;
                lc = lnext((*j).usingClause, lc);
            }
        }

        // Mark child deparse_columns structs with correct parentUsing info
        (*leftcolinfo).parentUsing = cur_parent_using;
        (*rightcolinfo).parentUsing = cur_parent_using;

        // Now recursively assign USING column names in children
        set_using_names(dpns, (*j).larg as *mut Node, cur_parent_using);
        set_using_names(dpns, (*j).rarg as *mut Node, cur_parent_using);
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
}

// set_relation_column_names: select column aliases for a non-join RTE
unsafe fn set_relation_column_names(
    dpns: *mut deparse_namespace,
    rte: *mut RangeTblEntry,
    colinfo: *mut deparse_columns,
) {
    let ncolumns: i32;
    let real_colnames: *mut *mut c_char;
    let mut changed_any: bool;
    let noldcolumns: i32;

    // Construct an array of the current "real" column names of the RTE.
    // real_colnames[] is indexed by physical column number, NULL for dropped cols.
    if (*rte).rtekind == RTE_RELATION {
        // Relation --- look to the system catalogs for up-to-date info
        let rel = relation_open((*rte).relid, AccessShareLock as i32);
        let tupdesc = RelationGetDescr(rel);

        let nc = (*tupdesc).natts;
        ncolumns = nc;
        real_colnames =
            palloc(nc as usize * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

        for i in 0..nc {
            let attr = TupleDescAttr(tupdesc, i);
            if (*attr).attisdropped {
                *real_colnames.add(i as usize) = std::ptr::null_mut();
            } else {
                *real_colnames.add(i as usize) =
                    pstrdup(NameStr((*attr).attname) as *const c_char);
            }
        }
        relation_close(rel, AccessShareLock as i32);
    } else {
        // Otherwise get the column names from eref or expandRTE()
        let colnames: *mut List;

        // Functions returning composites may have dropped columns; use
        // expandRTE() when available to handle that, else fall back to eref.
        if (*rte).rtekind == RTE_FUNCTION && !(*rte).functions.is_null() {
            // Since we're not creating Vars, rtindex etc. don't matter
            let mut cn: *mut List = std::ptr::null_mut();
            expandRTE(
                rte,
                1,
                0,
                VAR_RETURNING_DEFAULT as i32,
                -1,
                true, // include dropped
                &mut cn,
                std::ptr::null_mut(),
            );
            colnames = cn;
        } else {
            colnames = (*(*rte).eref).colnames;
        }

        let nc = list_length(colnames);
        ncolumns = nc;
        real_colnames =
            palloc(nc as usize * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

        let mut i = 0i32;
        let mut lc = list_head(colnames);
        while !lc.is_null() {
            // If the column name is an empty string, it's a dropped column -> NULL
            let mut cname = strVal(lfirst(lc) as *mut Node) as *mut c_char;
            if *cname == 0 {
                cname = std::ptr::null_mut();
            }
            *real_colnames.add(i as usize) = cname;
            i += 1;
            lc = lnext(colnames, lc);
        }
    }

    // Ensure colinfo->colnames has a slot for each column.
    expand_colnames_array_to(colinfo, ncolumns);

    // Make sufficiently large new_colnames and is_new_col arrays.
    // Note: colinfo->num_new_cols is left zero until after the loop so that
    // colname_is_unique will not consult that array.
    (*colinfo).new_colnames =
        palloc(ncolumns as usize * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    (*colinfo).is_new_col =
        palloc(ncolumns as usize * std::mem::size_of::<bool>()) as *mut bool;

    // If the RTE is wide enough, use a hash table to avoid O(N^2) costs
    build_colinfo_names_hash(colinfo);

    // Scan the columns, select a unique alias for each one.
    noldcolumns = list_length((*(*rte).eref).colnames);
    changed_any = false;
    let mut j = 0i32;
    for i in 0..ncolumns {
        let real_colname = *real_colnames.add(i as usize);
        let mut colname = *(*colinfo).colnames.add(i as usize);

        // Skip dropped columns
        if real_colname.is_null() {
            // colnames[i] is already NULL
            continue;
        }

        // If alias already assigned, that's what to use
        if colname.is_null() {
            // If user wrote an alias, prefer that over real column name
            if !(*rte).alias.is_null()
                && i < list_length((*(*rte).alias).colnames)
            {
                colname = strVal(
                    list_nth((*(*rte).alias).colnames, i) as *mut Node,
                ) as *mut c_char;
            } else {
                colname = real_colname;
            }

            // Unique-ify and insert into colinfo
            colname = make_colname_unique(colname, dpns, colinfo);
            *(*colinfo).colnames.add(i as usize) = colname;
            add_to_names_hash(colinfo, colname);
        }

        // Put names of non-dropped columns in new_colnames[] too
        *(*colinfo).new_colnames.add(j as usize) = colname;
        // And mark them as new or not
        *(*colinfo).is_new_col.add(j as usize) = i >= noldcolumns;
        j += 1;

        // Remember if any assigned aliases differ from "real" name
        if !changed_any && libc::strcmp(colname, real_colname) != 0 {
            changed_any = true;
        }
    }

    // We're now done needing the colinfo's names_hash
    destroy_colinfo_names_hash(colinfo);

    // Set correct length for new_colnames[] array.
    (*colinfo).num_new_cols = j;

    // For a relation RTE, print aliases only if any differ from "real" names.
    // For a function RTE, always emit a complete column alias list.
    // For tablefunc RTEs, never print aliases (column names are part of the clause).
    // For other RTE types, print if we changed anything OR if there were
    // user-written column aliases.
    if (*rte).rtekind == RTE_RELATION {
        (*colinfo).printaliases = changed_any;
    } else if (*rte).rtekind == RTE_FUNCTION {
        (*colinfo).printaliases = true;
    } else if (*rte).rtekind == RTE_TABLEFUNC {
        (*colinfo).printaliases = false;
    } else if !(*rte).alias.is_null() && !(*(*rte).alias).colnames.is_null() {
        (*colinfo).printaliases = true;
    } else {
        (*colinfo).printaliases = changed_any;
    }
}

// set_join_column_names: select column aliases for a join RTE
unsafe fn set_join_column_names(
    dpns: *mut deparse_namespace,
    rte: *mut RangeTblEntry,
    colinfo: *mut deparse_columns,
) {
    let leftcolinfo: *mut deparse_columns;
    let rightcolinfo: *mut deparse_columns;
    let mut changed_any: bool;
    let noldcolumns: i32;
    let nnewcolumns: i32;
    let mut leftmerged: *mut Bitmapset = std::ptr::null_mut();
    let mut rightmerged: *mut Bitmapset = std::ptr::null_mut();

    // Look up the previously-filled-in child deparse_columns structs
    leftcolinfo = deparse_columns_fetch((*colinfo).leftrti, dpns);
    rightcolinfo = deparse_columns_fetch((*colinfo).rightrti, dpns);

    // Ensure colinfo->colnames has a slot for each column.
    noldcolumns = list_length((*(*rte).eref).colnames);
    expand_colnames_array_to(colinfo, noldcolumns);

    // If the RTE is wide enough, use a hash table to avoid O(N^2) costs
    build_colinfo_names_hash(colinfo);

    // Scan the join output columns, select an alias for each one.
    // USING columns were already named by set_using_names(), so start after them.
    changed_any = false;
    for i in list_length((*colinfo).usingNames)..noldcolumns {
        let mut colname = *(*colinfo).colnames.add(i as usize);
        let real_colname: *mut c_char;

        // Join column must refer to at least one input column
        let la = *(*colinfo).leftattnos.add(i as usize);
        let ra = *(*colinfo).rightattnos.add(i as usize);

        // Get the child column name
        if la > 0 {
            real_colname = *(*leftcolinfo).colnames.add((la - 1) as usize);
        } else if ra > 0 {
            real_colname = *(*rightcolinfo).colnames.add((ra - 1) as usize);
        } else {
            // We're joining system columns --- use eref name
            real_colname =
                strVal(list_nth((*(*rte).eref).colnames, i) as *mut Node) as *mut c_char;
        }

        // If child col has been dropped, no need to assign a join colname
        if real_colname.is_null() {
            *(*colinfo).colnames.add(i as usize) = std::ptr::null_mut();
            continue;
        }

        // In an unnamed join, just report child column names as-is
        if (*rte).alias.is_null() {
            *(*colinfo).colnames.add(i as usize) = real_colname;
            add_to_names_hash(colinfo, real_colname);
            continue;
        }

        // If alias already assigned, that's what to use
        if colname.is_null() {
            // If user wrote an alias, prefer that over real column name
            if !(*rte).alias.is_null()
                && i < list_length((*(*rte).alias).colnames)
            {
                colname = strVal(
                    list_nth((*(*rte).alias).colnames, i) as *mut Node,
                ) as *mut c_char;
            } else {
                colname = real_colname;
            }

            // Unique-ify and insert into colinfo
            colname = make_colname_unique(colname, dpns, colinfo);
            *(*colinfo).colnames.add(i as usize) = colname;
            add_to_names_hash(colinfo, colname);
        }

        // Remember if any assigned aliases differ from "real" name
        if !changed_any && libc::strcmp(colname, real_colname) != 0 {
            changed_any = true;
        }
    }

    // Calculate number of columns the join would have if re-parsed now,
    // and create storage for the new_colnames and is_new_col arrays.
    // Entries must be zeroed since colname_is_unique consults them during loops.
    nnewcolumns = (*leftcolinfo).num_new_cols + (*rightcolinfo).num_new_cols
        - list_length((*colinfo).usingNames);
    (*colinfo).num_new_cols = nnewcolumns;
    (*colinfo).new_colnames =
        palloc0(nnewcolumns as usize * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    (*colinfo).is_new_col =
        palloc0(nnewcolumns as usize * std::mem::size_of::<bool>()) as *mut bool;

    // Handle merged columns; they are first and can't be new.
    // i indexes leftattnos/rightattnos, j indexes new_colnames/is_new_col.
    let mut i = 0i32;
    let mut j = 0i32;
    while i < noldcolumns
        && *(*colinfo).leftattnos.add(i as usize) != 0
        && *(*colinfo).rightattnos.add(i as usize) != 0
    {
        // column name is already determined and known unique
        *(*colinfo).new_colnames.add(j as usize) = *(*colinfo).colnames.add(i as usize);
        *(*colinfo).is_new_col.add(j as usize) = false;

        // build bitmapsets of child attnums of merged columns
        let la = *(*colinfo).leftattnos.add(i as usize);
        if la > 0 {
            leftmerged = bms_add_member(leftmerged, la);
        }
        let ra = *(*colinfo).rightattnos.add(i as usize);
        if ra > 0 {
            rightmerged = bms_add_member(rightmerged, ra);
        }

        i += 1;
        j += 1;
    }

    // Handle non-merged left-child columns
    let mut ic = 0i32;
    for jc in 0..(*leftcolinfo).num_new_cols {
        let child_colname = *(*leftcolinfo).new_colnames.add(jc as usize);

        if !(*leftcolinfo).is_new_col[jc as usize] {
            // Advance ic to next non-dropped old column of left child
            while ic < (*leftcolinfo).num_cols
                && (*(*leftcolinfo).colnames.add(ic as usize)).is_null()
            {
                ic += 1;
            }
            ic += 1;
            // If it is a merged column, we already processed it
            if bms_is_member(ic, leftmerged) {
                continue;
            }
            // Advance i to the corresponding existing join column
            while i < (*colinfo).num_cols
                && (*(*colinfo).colnames.add(i as usize)).is_null()
            {
                i += 1;
            }
            // Use the already-assigned name of this column
            *(*colinfo).new_colnames.add(j as usize) = *(*colinfo).colnames.add(i as usize);
            i += 1;
        } else {
            // Unique-ify the new child column name and assign, unless we're
            // in an unnamed join, in which case just copy
            if !(*rte).alias.is_null() {
                let newname = make_colname_unique(child_colname, dpns, colinfo);
                *(*colinfo).new_colnames.add(j as usize) = newname;
                if !changed_any && libc::strcmp(newname, child_colname) != 0 {
                    changed_any = true;
                }
            } else {
                *(*colinfo).new_colnames.add(j as usize) = child_colname;
            }
            add_to_names_hash(colinfo, *(*colinfo).new_colnames.add(j as usize));
        }

        *(*colinfo).is_new_col.add(j as usize) = (*leftcolinfo).is_new_col[jc as usize];
        j += 1;
    }

    // Handle non-merged right-child columns in exactly the same way
    ic = 0;
    for jc in 0..(*rightcolinfo).num_new_cols {
        let child_colname = *(*rightcolinfo).new_colnames.add(jc as usize);

        if !(*rightcolinfo).is_new_col[jc as usize] {
            // Advance ic to next non-dropped old column of right child
            while ic < (*rightcolinfo).num_cols
                && (*(*rightcolinfo).colnames.add(ic as usize)).is_null()
            {
                ic += 1;
            }
            ic += 1;
            // If it is a merged column, we already processed it
            if bms_is_member(ic, rightmerged) {
                continue;
            }
            // Advance i to the corresponding existing join column
            while i < (*colinfo).num_cols
                && (*(*colinfo).colnames.add(i as usize)).is_null()
            {
                i += 1;
            }
            // Use the already-assigned name of this column
            *(*colinfo).new_colnames.add(j as usize) = *(*colinfo).colnames.add(i as usize);
            i += 1;
        } else {
            // Unique-ify the new child column name and assign, unless we're
            // in an unnamed join, in which case just copy
            if !(*rte).alias.is_null() {
                let newname = make_colname_unique(child_colname, dpns, colinfo);
                *(*colinfo).new_colnames.add(j as usize) = newname;
                if !changed_any && libc::strcmp(newname, child_colname) != 0 {
                    changed_any = true;
                }
            } else {
                *(*colinfo).new_colnames.add(j as usize) = child_colname;
            }
            add_to_names_hash(colinfo, *(*colinfo).new_colnames.add(j as usize));
        }

        *(*colinfo).is_new_col.add(j as usize) = (*rightcolinfo).is_new_col[jc as usize];
        j += 1;
    }

    // We're now done needing the colinfo's names_hash
    destroy_colinfo_names_hash(colinfo);

    // For a named join, print column aliases if we changed any from the child names.
    // Unnamed joins cannot print aliases.
    if !(*rte).alias.is_null() {
        (*colinfo).printaliases = changed_any;
    } else {
        (*colinfo).printaliases = false;
    }
}

// colname_is_unique: is colname distinct from already-chosen column names?
unsafe fn colname_is_unique(
    colname: *const c_char,
    dpns: *mut deparse_namespace,
    colinfo: *mut deparse_columns,
) -> bool {
    // If we have a hash table, consult that instead of linearly scanning
    if !(*colinfo).names_hash.is_null() {
        if !hash_search(
            (*colinfo).names_hash,
            colname as *const c_void,
            HASH_FIND as i32,
            std::ptr::null_mut(),
        )
        .is_null()
        {
            return false;
        }
    } else {
        // Check against already-assigned column aliases within RTE
        for i in 0..(*colinfo).num_cols {
            let oldname = *(*colinfo).colnames.add(i as usize);
            if !oldname.is_null() && libc::strcmp(oldname, colname) == 0 {
                return false;
            }
        }

        // If we're building a new_colnames array, check that too
        for i in 0..(*colinfo).num_new_cols {
            let oldname = *(*colinfo).new_colnames.add(i as usize);
            if !oldname.is_null() && libc::strcmp(oldname, colname) == 0 {
                return false;
            }
        }

        // Also check against names already assigned for parent-join USING cols
        let mut lc = list_head((*colinfo).parentUsing);
        while !lc.is_null() {
            let oldname = lfirst(lc) as *const c_char;
            if libc::strcmp(oldname, colname) == 0 {
                return false;
            }
            lc = lnext((*colinfo).parentUsing, lc);
        }
    }

    // Also check against USING-column names that must be globally unique.
    let mut lc = list_head((*dpns).using_names);
    while !lc.is_null() {
        let oldname = lfirst(lc) as *const c_char;
        if libc::strcmp(oldname, colname) == 0 {
            return false;
        }
        lc = lnext((*dpns).using_names, lc);
    }

    true
}

// make_colname_unique: modify colname if necessary to make it unique
unsafe fn make_colname_unique(
    colname: *mut c_char,
    dpns: *mut deparse_namespace,
    colinfo: *mut deparse_columns,
) -> *mut c_char {
    // If the selected name isn't unique, append digits to make it so.
    if !colname_is_unique(colname, dpns, colinfo) {
        let mut colnamelen = libc::strlen(colname) as i32;
        let modname = palloc(colnamelen as usize + 16) as *mut c_char;
        let mut i = 0i32;
        loop {
            i += 1;
            loop {
                libc::memcpy(
                    modname as *mut c_void,
                    colname as *const c_void,
                    colnamelen as usize,
                );
                libc::sprintf(modname.add(colnamelen as usize), c"_%d".as_ptr(), i);
                if libc::strlen(modname) < NAMEDATALEN as usize {
                    break;
                }
                // drop chars from colname to keep all the digits
                colnamelen =
                    pg_mbcliplen(colname, colnamelen, colnamelen - 1);
            }
            if colname_is_unique(modname, dpns, colinfo) {
                break;
            }
        }
        return modname;
    }
    colname
}

// expand_colnames_array_to: make colinfo->colnames at least n items long.
// Any added array entries are initialized to zero.
unsafe fn expand_colnames_array_to(colinfo: *mut deparse_columns, n: i32) {
    if n > (*colinfo).num_cols {
        if (*colinfo).colnames.is_null() {
            (*colinfo).colnames =
                palloc0_array!(c_char_ptr, n as usize) as *mut *mut c_char;
        } else {
            (*colinfo).colnames = repalloc0_array!(
                (*colinfo).colnames,
                c_char_ptr,
                (*colinfo).num_cols as usize,
                n as usize
            ) as *mut *mut c_char;
        }
        (*colinfo).num_cols = n;
    }
}

// build_colinfo_names_hash: optionally construct a hash table for colinfo.
// Only used for RTEs with at least 32 columns.
unsafe fn build_colinfo_names_hash(colinfo: *mut deparse_columns) {
    if (*colinfo).num_cols < 32 {
        return;
    }

    let mut hash_ctl: HASHCTL = std::mem::zeroed();
    hash_ctl.keysize = NAMEDATALEN as usize;
    hash_ctl.entrysize = NAMEDATALEN as usize;
    hash_ctl.hcxt = CurrentMemoryContext;
    (*colinfo).names_hash = hash_create(
        c"deparse_columns names".as_ptr(),
        ((*colinfo).num_cols + (*colinfo).num_new_cols) as i64,
        &mut hash_ctl,
        (HASH_ELEM | HASH_STRINGS | HASH_CONTEXT) as i32,
    );

    // Preload the hash table with any names already present
    for i in 0..(*colinfo).num_cols {
        let oldname = *(*colinfo).colnames.add(i as usize);
        if !oldname.is_null() {
            add_to_names_hash(colinfo, oldname);
        }
    }

    for i in 0..(*colinfo).num_new_cols {
        let oldname = *(*colinfo).new_colnames.add(i as usize);
        if !oldname.is_null() {
            add_to_names_hash(colinfo, oldname);
        }
    }

    let mut lc = list_head((*colinfo).parentUsing);
    while !lc.is_null() {
        let oldname = lfirst(lc) as *mut c_char;
        add_to_names_hash(colinfo, oldname);
        lc = lnext((*colinfo).parentUsing, lc);
    }
}

// add_to_names_hash: add a string to the names_hash, if we're using one
unsafe fn add_to_names_hash(colinfo: *mut deparse_columns, name: *const c_char) {
    if !(*colinfo).names_hash.is_null() {
        let _ = hash_search(
            (*colinfo).names_hash,
            name as *const c_void,
            HASH_ENTER as i32,
            std::ptr::null_mut(),
        );
    }
}

// destroy_colinfo_names_hash: destroy hash table when done with it
unsafe fn destroy_colinfo_names_hash(colinfo: *mut deparse_columns) {
    if !(*colinfo).names_hash.is_null() {
        hash_destroy((*colinfo).names_hash);
        (*colinfo).names_hash = std::ptr::null_mut();
    }
}

// identify_join_columns: figure out where columns of a join come from.
// Fills the join-specific fields of the colinfo struct (except usingNames).
unsafe fn identify_join_columns(
    j: *mut JoinExpr,
    jrte: *mut RangeTblEntry,
    colinfo: *mut deparse_columns,
) {
    // Extract left/right child RT indexes
    if IsA((*j).larg as *mut Node, T_RangeTblRef) {
        (*colinfo).leftrti = (*((*j).larg as *mut RangeTblRef)).rtindex;
    } else if IsA((*j).larg as *mut Node, T_JoinExpr) {
        (*colinfo).leftrti = (*((*j).larg as *mut JoinExpr)).rtindex;
    } else {
        elog!(
            ERROR,
            "unrecognized node type in jointree: {}",
            nodeTag((*j).larg as *mut Node) as i32
        );
    }
    if IsA((*j).rarg as *mut Node, T_RangeTblRef) {
        (*colinfo).rightrti = (*((*j).rarg as *mut RangeTblRef)).rtindex;
    } else if IsA((*j).rarg as *mut Node, T_JoinExpr) {
        (*colinfo).rightrti = (*((*j).rarg as *mut JoinExpr)).rtindex;
    } else {
        elog!(
            ERROR,
            "unrecognized node type in jointree: {}",
            nodeTag((*j).rarg as *mut Node) as i32
        );
    }

    // Initialize result arrays with zeroes
    let numjoincols = list_length((*jrte).joinaliasvars);
    (*colinfo).leftattnos =
        palloc0(numjoincols as usize * std::mem::size_of::<i32>()) as *mut i32;
    (*colinfo).rightattnos =
        palloc0(numjoincols as usize * std::mem::size_of::<i32>()) as *mut i32;

    // Deconstruct RTE's joinleftcols/joinrightcols into desired format.
    // Merged columns (due to USING) are the first columns of the join output.
    let mut jcolno = 0i32;
    let mut lc = list_head((*jrte).joinleftcols);
    while !lc.is_null() {
        let leftattno = lfirst_int(lc);
        *(*colinfo).leftattnos.add(jcolno as usize) = leftattno;
        jcolno += 1;
        lc = lnext((*jrte).joinleftcols, lc);
    }
    let mut rcolno = 0i32;
    let mut lc = list_head((*jrte).joinrightcols);
    while !lc.is_null() {
        let rightattno = lfirst_int(lc);
        if rcolno < (*jrte).joinmergedcols {
            // merged column
            *(*colinfo).rightattnos.add(rcolno as usize) = rightattno;
        } else {
            *(*colinfo).rightattnos.add(jcolno as usize) = rightattno;
            jcolno += 1;
        }
        rcolno += 1;
        lc = lnext((*jrte).joinrightcols, lc);
    }
}

// get_rtable_name: convenience function to get a previously assigned RTE alias.
// The RTE must belong to the topmost namespace level in "context".
unsafe fn get_rtable_name(rtindex: i32, context: *mut deparse_context) -> *mut c_char {
    let dpns = linitial((*context).namespaces) as *mut deparse_namespace;
    list_nth((*dpns).rtable_names, rtindex - 1) as *mut c_char
}

// set_deparse_plan: set up deparse_namespace to parse subexpressions
// of a given Plan node.
// Sets plan, outer_plan, inner_plan, outer_tlist, inner_tlist, index_tlist.
unsafe fn set_deparse_plan(dpns: *mut deparse_namespace, plan: *mut Plan) {
    (*dpns).plan = plan;

    // We special-case Append and MergeAppend to pretend that the first child
    // plan is the OUTER referent.
    if IsA(plan as *mut Node, T_Append) {
        (*dpns).outer_plan =
            linitial((*(plan as *mut Append)).appendplans) as *mut Plan;
    } else if IsA(plan as *mut Node, T_MergeAppend) {
        (*dpns).outer_plan =
            linitial((*(plan as *mut MergeAppend)).mergeplans) as *mut Plan;
    } else {
        (*dpns).outer_plan = outerPlan(plan);
    }

    if !(*dpns).outer_plan.is_null() {
        (*dpns).outer_tlist = (*(*dpns).outer_plan).targetlist;
    } else {
        (*dpns).outer_tlist = std::ptr::null_mut();
    }

    // For a SubqueryScan, pretend the subplan is INNER referent.
    // For a CteScan, pretend the subquery's plan is INNER referent.
    // For a WorkTableScan, locate the parent RecursiveUnion plan node.
    // For MERGE, pretend the ModifyTable's source plan (outer plan) is INNER.
    // For ON CONFLICT .. UPDATE, point inner tlist to excluded expression's tlist.
    if IsA(plan as *mut Node, T_SubqueryScan) {
        (*dpns).inner_plan = (*(plan as *mut SubqueryScan)).subplan;
    } else if IsA(plan as *mut Node, T_CteScan) {
        (*dpns).inner_plan = list_nth(
            (*dpns).subplans,
            (*(plan as *mut CteScan)).ctePlanId - 1,
        ) as *mut Plan;
    } else if IsA(plan as *mut Node, T_WorkTableScan) {
        (*dpns).inner_plan =
            find_recursive_union(dpns, plan as *mut WorkTableScan);
    } else if IsA(plan as *mut Node, T_ModifyTable) {
        let mt = plan as *mut ModifyTable;
        if (*mt).operation == CMD_MERGE as i32 {
            (*dpns).inner_plan = outerPlan(plan);
        } else {
            (*dpns).inner_plan = plan;
        }
    } else {
        (*dpns).inner_plan = innerPlan(plan);
    }

    if IsA(plan as *mut Node, T_ModifyTable)
        && (*(plan as *mut ModifyTable)).operation == CMD_INSERT as i32
    {
        (*dpns).inner_tlist = (*(plan as *mut ModifyTable)).exclRelTlist;
    } else if !(*dpns).inner_plan.is_null() {
        (*dpns).inner_tlist = (*(*dpns).inner_plan).targetlist;
    } else {
        (*dpns).inner_tlist = std::ptr::null_mut();
    }

    // Set up referent for INDEX_VAR Vars, if needed
    if IsA(plan as *mut Node, T_IndexOnlyScan) {
        (*dpns).index_tlist = (*(plan as *mut IndexOnlyScan)).indextlist;
    } else if IsA(plan as *mut Node, T_ForeignScan) {
        (*dpns).index_tlist = (*(plan as *mut ForeignScan)).fdw_scan_tlist;
    } else if IsA(plan as *mut Node, T_CustomScan) {
        (*dpns).index_tlist = (*(plan as *mut CustomScan)).custom_scan_tlist;
    } else {
        (*dpns).index_tlist = std::ptr::null_mut();
    }
}

// find_recursive_union: locate the ancestor plan node that is the
// RecursiveUnion generating the WorkTableScan's work table.
unsafe fn find_recursive_union(
    dpns: *mut deparse_namespace,
    wtscan: *mut WorkTableScan,
) -> *mut Plan {
    let mut lc = list_head((*dpns).ancestors);
    while !lc.is_null() {
        let ancestor = lfirst(lc) as *mut Plan;
        if IsA(ancestor as *mut Node, T_RecursiveUnion)
            && (*(ancestor as *mut RecursiveUnion)).wtParam == (*wtscan).wtParam
        {
            return ancestor;
        }
        lc = lnext((*dpns).ancestors, lc);
    }
    elog!(
        ERROR,
        "could not find RecursiveUnion for WorkTableScan with wtParam {}",
        (*wtscan).wtParam
    );
    std::ptr::null_mut()
}

// push_child_plan: temporarily transfer deparsing attention to a child plan.
// Modifies the top stack entry in-place. Caller must provide a local
// deparse_namespace to save the previous state for pop_child_plan.
unsafe fn push_child_plan(
    dpns: *mut deparse_namespace,
    plan: *mut Plan,
    save_dpns: *mut deparse_namespace,
) {
    // Save state for restoration later
    *save_dpns = *dpns;

    // Link current plan node into ancestors list
    (*dpns).ancestors = lcons((*dpns).plan as *mut c_void, (*dpns).ancestors);

    // Set attention on selected child
    set_deparse_plan(dpns, plan);
}

// pop_child_plan: undo the effects of push_child_plan
unsafe fn pop_child_plan(
    dpns: *mut deparse_namespace,
    save_dpns: *mut deparse_namespace,
) {
    // Get rid of ancestors list cell added by push_child_plan
    let ancestors = list_delete_first((*dpns).ancestors);

    // Restore fields changed by push_child_plan
    *dpns = *save_dpns;

    // Make sure dpns->ancestors is right
    (*dpns).ancestors = ancestors;
}

// push_ancestor_plan: temporarily transfer deparsing attention to an
// ancestor plan. The target ancestor is identified by its ListCell.
unsafe fn push_ancestor_plan(
    dpns: *mut deparse_namespace,
    ancestor_cell: *mut ListCell,
    save_dpns: *mut deparse_namespace,
) {
    let plan = lfirst(ancestor_cell) as *mut Plan;

    // Save state for restoration later
    *save_dpns = *dpns;

    // Build a new ancestor list with just this node's ancestors
    (*dpns).ancestors = list_copy_tail(
        (*dpns).ancestors,
        list_cell_number((*dpns).ancestors, ancestor_cell) + 1,
    );

    // Set attention on selected ancestor
    set_deparse_plan(dpns, plan);
}

// pop_ancestor_plan: undo the effects of push_ancestor_plan
unsafe fn pop_ancestor_plan(
    dpns: *mut deparse_namespace,
    save_dpns: *mut deparse_namespace,
) {
    // Free the ancestor list made in push_ancestor_plan
    list_free((*dpns).ancestors);

    // Restore fields changed by push_ancestor_plan
    *dpns = *save_dpns;
}

// ----------
// make_ruledef -- reconstruct the CREATE RULE command
//                 for a given pg_rewrite tuple
// ----------
unsafe fn make_ruledef(
    buf: *mut StringInfo,
    ruletup: *mut HeapTupleData,
    rulettc: TupleDesc,
    pretty_flags: i32,
) {
    let rulename: *mut c_char;
    let ev_type: c_char;
    let ev_class: Oid;
    let is_instead: bool;
    let ev_qual: *mut c_char;
    let ev_action: *mut c_char;
    let actions: *mut List;
    let ev_relation: *mut Relation;
    let mut view_result_desc: TupleDesc = std::ptr::null_mut();
    let fno: i32;
    let dat: Datum;
    let mut isnull: bool = false;

    // Get the attribute values from the rules tuple
    fno = SPI_fnumber(rulettc, c"rulename".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    rulename = NameStr(*(DatumGetName(dat)));

    fno = SPI_fnumber(rulettc, c"ev_type".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    ev_type = DatumGetChar(dat);

    fno = SPI_fnumber(rulettc, c"ev_class".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    ev_class = DatumGetObjectId(dat);

    fno = SPI_fnumber(rulettc, c"is_instead".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    is_instead = DatumGetBool(dat);

    fno = SPI_fnumber(rulettc, c"ev_qual".as_ptr());
    ev_qual = SPI_getvalue(ruletup, rulettc, fno);

    fno = SPI_fnumber(rulettc, c"ev_action".as_ptr());
    ev_action = SPI_getvalue(ruletup, rulettc, fno);
    actions = stringToNode(ev_action) as *mut List;
    if actions.is_null() || list_length(actions) == 0 {
        elog!(ERROR, "invalid empty ev_action list");
    }

    ev_relation = table_open(ev_class, AccessShareLock as i32) as *mut Relation;

    // Build the rules definition text
    appendStringInfo!(buf, "CREATE RULE {} AS", { /* TODO(pg-port): quote_identifier */ std::ffi::CStr::from_ptr(rulename).to_str().unwrap_or("") });

    if (pretty_flags & PRETTYFLAG_INDENT) != 0 {
        appendStringInfoString(buf, c"\n    ON ".as_ptr());
    } else {
        appendStringInfoString(buf, c" ON ".as_ptr());
    }

    // The event the rule is fired for
    match ev_type as u8 {
        b'1' => {
            appendStringInfoString(buf, c"SELECT".as_ptr());
            view_result_desc = RelationGetDescr(*(ev_relation as *mut *mut RelationData));
        }
        b'2' => {
            appendStringInfoString(buf, c"UPDATE".as_ptr());
        }
        b'3' => {
            appendStringInfoString(buf, c"INSERT".as_ptr());
        }
        b'4' => {
            appendStringInfoString(buf, c"DELETE".as_ptr());
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!(
                    "rule \"{}\" has unsupported event type {}",
                    std::ffi::CStr::from_ptr(rulename).to_str().unwrap_or(""),
                    ev_type as i32
                )
            );
        }
    }

    // The relation the rule is fired on
    if (pretty_flags & PRETTYFLAG_SCHEMA) != 0 {
        appendStringInfo!(buf, " TO {}", { /* TODO(pg-port): generate_relation_name */ "" });
    } else {
        appendStringInfo!(buf, " TO {}", { /* TODO(pg-port): generate_qualified_relation_name */ "" });
    }

    // If the rule has an event qualification, add it
    if libc::strcmp(ev_qual, c"<>".as_ptr()) != 0 {
        let qual: *mut Node;
        let query: *mut Query;
        let mut context: deparse_context = std::mem::zeroed();
        let mut dpns: deparse_namespace = std::mem::zeroed();

        if (pretty_flags & PRETTYFLAG_INDENT) != 0 {
            appendStringInfoString(buf, c"\n  ".as_ptr());
        }
        appendStringInfoString(buf, c" WHERE ".as_ptr());

        qual = stringToNode(ev_qual) as *mut Node;

        // We need to make a context for recognizing any Vars in the qual
        // (which can only be references to OLD and NEW). Use the rtable of
        // the first query in the action list for this purpose.
        query = linitial(actions) as *mut Query;

        // If the action is INSERT...SELECT, OLD/NEW have been pushed down
        // into the SELECT, and that's what we need to look at.
        let query = getInsertSelectQuery(query, std::ptr::null_mut());

        // Must acquire locks right away; see notes in get_query_def()
        AcquireRewriteLocks(query, false, false);

        context.buf = buf;
        context.namespaces = list_make1(&mut dpns as *mut _ as *mut c_void);
        context.resultDesc = std::ptr::null_mut();
        context.targetList = std::ptr::null_mut();
        context.windowClause = std::ptr::null_mut();
        context.varprefix = list_length((*query).rtable) != 1;
        context.prettyFlags = pretty_flags;
        context.wrapColumn = WRAP_COLUMN_DEFAULT;
        context.indentLevel = PRETTYINDENT_STD;
        context.colNamesVisible = true;
        context.inGroupBy = false;
        context.varInOrderBy = false;
        context.appendparents = std::ptr::null_mut();

        set_deparse_for_query(&mut dpns, query, std::ptr::null_mut());

        get_rule_expr(qual, &mut context, false);
    }

    appendStringInfoString(buf, c" DO ".as_ptr());

    // The INSTEAD keyword (if so)
    if is_instead {
        appendStringInfoString(buf, c"INSTEAD ".as_ptr());
    }

    // Finally the rules actions
    if list_length(actions) > 1 {
        appendStringInfoChar(buf, b'(' as c_char);
        let mut action_lc = list_head(actions);
        while !action_lc.is_null() {
            let query = lfirst(action_lc) as *mut Query;
            get_query_def(
                query,
                buf,
                std::ptr::null_mut(),
                view_result_desc,
                true,
                pretty_flags,
                WRAP_COLUMN_DEFAULT,
                0,
            );
            if pretty_flags != 0 {
                appendStringInfoString(buf, c";\n".as_ptr());
            } else {
                appendStringInfoString(buf, c"; ".as_ptr());
            }
            action_lc = lnext(actions, action_lc);
        }
        appendStringInfoString(buf, c");".as_ptr());
    } else {
        let query = linitial(actions) as *mut Query;
        get_query_def(
            query,
            buf,
            std::ptr::null_mut(),
            view_result_desc,
            true,
            pretty_flags,
            WRAP_COLUMN_DEFAULT,
            0,
        );
        appendStringInfoChar(buf, b';' as c_char);
    }

    table_close(*(ev_relation as *mut *mut RelationData), AccessShareLock as i32);
}

// ----------
// make_viewdef -- reconstruct the SELECT part of a view rewrite rule
// ----------
unsafe fn make_viewdef(
    buf: *mut StringInfo,
    ruletup: *mut HeapTupleData,
    rulettc: TupleDesc,
    pretty_flags: i32,
    wrap_column: i32,
) {
    let query: *mut Query;
    let ev_type: c_char;
    let ev_class: Oid;
    let is_instead: bool;
    let ev_qual: *mut c_char;
    let ev_action: *mut c_char;
    let actions: *mut List;
    let ev_relation: *mut Relation;
    let fno: i32;
    let dat: Datum;
    let mut isnull: bool = false;

    // Get the attribute values from the rules tuple
    fno = SPI_fnumber(rulettc, c"ev_type".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    ev_type = DatumGetChar(dat);

    fno = SPI_fnumber(rulettc, c"ev_class".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    ev_class = DatumGetObjectId(dat);

    fno = SPI_fnumber(rulettc, c"is_instead".as_ptr());
    dat = SPI_getbinval(ruletup, rulettc, fno, &mut isnull);
    is_instead = DatumGetBool(dat);

    fno = SPI_fnumber(rulettc, c"ev_qual".as_ptr());
    ev_qual = SPI_getvalue(ruletup, rulettc, fno);

    fno = SPI_fnumber(rulettc, c"ev_action".as_ptr());
    ev_action = SPI_getvalue(ruletup, rulettc, fno);
    actions = stringToNode(ev_action) as *mut List;

    if list_length(actions) != 1 {
        // keep output buffer empty and leave
        return;
    }

    query = linitial(actions) as *mut Query;

    if ev_type as u8 != b'1'
        || !is_instead
        || libc::strcmp(ev_qual, c"<>".as_ptr()) != 0
        || (*query).commandType != CMD_SELECT as i32
    {
        // keep output buffer empty and leave
        return;
    }

    ev_relation = table_open(ev_class, AccessShareLock as i32) as *mut Relation;

    get_query_def(
        query,
        buf,
        std::ptr::null_mut(),
        RelationGetDescr(*(ev_relation as *mut *mut RelationData)),
        true,
        pretty_flags,
        wrap_column,
        0,
    );
    appendStringInfoChar(buf, b';' as c_char);

    table_close(*(ev_relation as *mut *mut RelationData), AccessShareLock as i32);
}

// ----------
// get_query_def -- Parse back one query parsetree
//
// query: parsetree to be displayed
// buf: output text is appended to buf
// parentnamespace: list (initially empty) of outer-level deparse_namespace's
// resultDesc: if not NULL, the output tuple descriptor for the view
// colNamesVisible: true if surrounding context cares about output column names
// prettyFlags: bitmask of PRETTYFLAG_XXX options
// wrapColumn: maximum line length, or -1 to disable wrapping
// startIndent: initial indentation amount
// ----------
unsafe fn get_query_def(
    query: *mut Query,
    buf: *mut StringInfo,
    parentnamespace: *mut List,
    result_desc: TupleDesc,
    col_names_visible: bool,
    pretty_flags: i32,
    wrap_column: i32,
    start_indent: i32,
) {
    let mut context: deparse_context = std::mem::zeroed();
    let mut dpns: deparse_namespace = std::mem::zeroed();
    let rtable_size: i32;

    // Guard against excessively long or deeply-nested queries
    CHECK_FOR_INTERRUPTS!();
    check_stack_depth();

    rtable_size = if (*query).hasGroupRTE {
        list_length((*query).rtable) - 1
    } else {
        list_length((*query).rtable)
    };

    // Replace any Vars in the query's targetlist and havingQual that
    // reference GROUP outputs with the underlying grouping expressions.
    if (*query).hasGroupRTE {
        (*query).targetList = flatten_group_exprs(
            std::ptr::null_mut(),
            query,
            (*query).targetList as *mut Node,
        ) as *mut List;
        (*query).havingQual =
            flatten_group_exprs(std::ptr::null_mut(), query, (*query).havingQual);
    }

    // Before we begin to examine the query, acquire locks on referenced
    // relations, and fix up deleted columns in JOIN RTEs. This ensures
    // consistent results.
    AcquireRewriteLocks(query, false, false);

    context.buf = buf;
    context.namespaces = lcons(&mut dpns as *mut _ as *mut c_void, list_copy(parentnamespace));
    context.resultDesc = std::ptr::null_mut();
    context.targetList = std::ptr::null_mut();
    context.windowClause = std::ptr::null_mut();
    context.varprefix = !parentnamespace.is_null() || rtable_size != 1;
    context.prettyFlags = pretty_flags;
    context.wrapColumn = wrap_column;
    context.indentLevel = start_indent;
    context.colNamesVisible = col_names_visible;
    context.inGroupBy = false;
    context.varInOrderBy = false;
    context.appendparents = std::ptr::null_mut();

    set_deparse_for_query(&mut dpns, query, parentnamespace);

    match (*query).commandType as u32 {
        CMD_SELECT => {
            // We set context.resultDesc only if it's a SELECT
            context.resultDesc = result_desc;
            get_select_query_def(query, &mut context);
        }
        CMD_UPDATE => {
            get_update_query_def(query, &mut context);
        }
        CMD_INSERT => {
            get_insert_query_def(query, &mut context);
        }
        CMD_DELETE => {
            get_delete_query_def(query, &mut context);
        }
        CMD_MERGE => {
            get_merge_query_def(query, &mut context);
        }
        CMD_NOTHING => {
            appendStringInfoString(buf, c"NOTHING".as_ptr());
        }
        CMD_UTILITY => {
            get_utility_query_def(query, &mut context);
        }
        _ => {
            elog!(ERROR, "unrecognized query command type: {}", (*query).commandType);
        }
    }
}

// ----------
// get_values_def -- Parse back a VALUES list
// ----------
unsafe fn get_values_def(values_lists: *mut List, context: *mut deparse_context) {
    let buf = (*context).buf;
    let mut first_list = true;

    appendStringInfoString(buf, c"VALUES ".as_ptr());

    let mut vtl = list_head(values_lists);
    while !vtl.is_null() {
        let sublist = lfirst(vtl) as *mut List;
        let mut first_col = true;

        if first_list {
            first_list = false;
        } else {
            appendStringInfoString(buf, c", ".as_ptr());
        }

        appendStringInfoChar(buf, b'(' as c_char);
        let mut lc = list_head(sublist);
        while !lc.is_null() {
            let col = lfirst(lc) as *mut Node;

            if first_col {
                first_col = false;
            } else {
                appendStringInfoChar(buf, b',' as c_char);
            }

            // Print the value. Whole-row Vars need special treatment.
            get_rule_expr_toplevel(col, context, false);

            lc = lnext(sublist, lc);
        }
        appendStringInfoChar(buf, b')' as c_char);

        vtl = lnext(values_lists, vtl);
    }
}

// ----------
// get_with_clause -- Parse back a WITH clause
// ----------
unsafe fn get_with_clause(query: *mut Query, context: *mut deparse_context) {
    let buf = (*context).buf;
    let sep: *const c_char;

    if (*query).cteList.is_null() {
        return;
    }

    if PRETTY_INDENT(context) {
        (*context).indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, b' ' as c_char);
    }

    if (*query).hasRecursive {
        sep = c"WITH RECURSIVE ".as_ptr();
    } else {
        sep = c"WITH ".as_ptr();
    }

    let mut l = list_head((*query).cteList);
    while !l.is_null() {
        let cte = lfirst(l) as *mut CommonTableExpr;

        appendStringInfoString(buf, sep);
        appendStringInfoString(buf, quote_identifier((*cte).ctename));
        if !(*cte).aliascolnames.is_null() {
            let mut first = true;
            let mut col = list_head((*cte).aliascolnames);
            appendStringInfoChar(buf, b'(' as c_char);
            while !col.is_null() {
                if first {
                    first = false;
                } else {
                    appendStringInfoString(buf, c", ".as_ptr());
                }
                appendStringInfoString(
                    buf,
                    quote_identifier(strVal(lfirst(col) as *mut Node) as *mut c_char),
                );
                col = lnext((*cte).aliascolnames, col);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        appendStringInfoString(buf, c" AS ".as_ptr());
        match (*cte).ctematerialized as u32 {
            CTEMaterializeDefault => {}
            CTEMaterializeAlways => {
                appendStringInfoString(buf, c"MATERIALIZED ".as_ptr());
            }
            CTEMaterializeNever => {
                appendStringInfoString(buf, c"NOT MATERIALIZED ".as_ptr());
            }
            _ => {}
        }
        appendStringInfoChar(buf, b'(' as c_char);
        if PRETTY_INDENT(context) {
            appendContextKeyword(context, c"".as_ptr(), 0, 0, 0);
        }
        get_query_def(
            (*cte).ctequery as *mut Query,
            buf,
            (*context).namespaces,
            std::ptr::null_mut(),
            true,
            (*context).prettyFlags,
            (*context).wrapColumn,
            (*context).indentLevel,
        );
        if PRETTY_INDENT(context) {
            appendContextKeyword(context, c"".as_ptr(), 0, 0, 0);
        }
        appendStringInfoChar(buf, b')' as c_char);

        if !(*cte).search_clause.is_null() {
            let sc = (*cte).search_clause;
            let mut first = true;

            if (*sc).search_breadth_first {
                appendStringInfoString(buf, c" SEARCH BREADTH FIRST BY ".as_ptr());
            } else {
                appendStringInfoString(buf, c" SEARCH DEPTH FIRST BY ".as_ptr());
            }

            let mut lc = list_head((*sc).search_col_list);
            while !lc.is_null() {
                if first {
                    first = false;
                } else {
                    appendStringInfoString(buf, c", ".as_ptr());
                }
                appendStringInfoString(
                    buf,
                    quote_identifier(strVal(lfirst(lc) as *mut Node) as *mut c_char),
                );
                lc = lnext((*sc).search_col_list, lc);
            }

            appendStringInfo!(buf, " SET {}", { /* TODO(pg-port): quote_identifier */ std::ffi::CStr::from_ptr((*sc).search_seq_column).to_str().unwrap_or("") });
        }

        if !(*cte).cycle_clause.is_null() {
            let cc = (*cte).cycle_clause;
            let mut first = true;

            appendStringInfoString(buf, c" CYCLE ".as_ptr());

            let mut lc = list_head((*cc).cycle_col_list);
            while !lc.is_null() {
                if first {
                    first = false;
                } else {
                    appendStringInfoString(buf, c", ".as_ptr());
                }
                appendStringInfoString(
                    buf,
                    quote_identifier(strVal(lfirst(lc) as *mut Node) as *mut c_char),
                );
                lc = lnext((*cc).cycle_col_list, lc);
            }

            appendStringInfo!(buf, " SET {}", { /* TODO(pg-port): quote_identifier */ std::ffi::CStr::from_ptr((*cc).cycle_mark_column).to_str().unwrap_or("") });

            {
                let cmv = (*cc).cycle_mark_value as *mut Const;
                let cmd = (*cc).cycle_mark_default as *mut Const;

                // Only omit TO..DEFAULT if both are the default BOOL true/false
                let is_default_bool = (*cmv).consttype == BOOLOID
                    && !(*cmv).constisnull
                    && DatumGetBool((*cmv).constvalue)
                    && (*cmd).consttype == BOOLOID
                    && !(*cmd).constisnull
                    && !DatumGetBool((*cmd).constvalue);

                if !is_default_bool {
                    appendStringInfoString(buf, c" TO ".as_ptr());
                    get_rule_expr((*cc).cycle_mark_value, context, false);
                    appendStringInfoString(buf, c" DEFAULT ".as_ptr());
                    get_rule_expr((*cc).cycle_mark_default, context, false);
                }
            }

            appendStringInfo!(buf, " USING {}", { /* TODO(pg-port): quote_identifier */ std::ffi::CStr::from_ptr((*cc).cycle_path_column).to_str().unwrap_or("") });
        }

        sep = c", ".as_ptr();
        l = lnext((*query).cteList, l);
    }

    if PRETTY_INDENT(context) {
        (*context).indentLevel -= PRETTYINDENT_STD;
        appendContextKeyword(context, c"".as_ptr(), 0, 0, 0);
    } else {
        appendStringInfoChar(buf, b' ' as c_char);
    }
}

// ----------
// get_select_query_def -- Parse back a SELECT parsetree
// ----------
unsafe fn get_select_query_def(query: *mut Query, context: *mut deparse_context) {
    let buf = (*context).buf;
    let force_colno: bool;

    // Insert the WITH clause if given
    get_with_clause(query, context);

    // Subroutines may need to consult the SELECT targetlist and windowClause
    (*context).targetList = (*query).targetList;
    (*context).windowClause = (*query).windowClause;

    // If the Query node has a setOperations tree, then it's the top level of
    // a UNION/INTERSECT/EXCEPT query; only the WITH, ORDER BY and LIMIT
    // fields are interesting in the top query itself.
    if !(*query).setOperations.is_null() {
        get_setop_query((*query).setOperations, query, context);
        // ORDER BY clauses must be simple in this case
        force_colno = true;
    } else {
        get_basic_select_query(query, context);
        force_colno = false;
    }

    // Add the ORDER BY clause if given
    if !(*query).sortClause.is_null() {
        appendContextKeyword(
            context,
            c" ORDER BY ".as_ptr(),
            -(PRETTYINDENT_STD),
            PRETTYINDENT_STD,
            1,
        );
        get_rule_orderby((*query).sortClause, (*query).targetList, force_colno, context);
    }

    // Add the LIMIT/OFFSET clauses if given.
    if !(*query).limitOffset.is_null() {
        appendContextKeyword(
            context,
            c" OFFSET ".as_ptr(),
            -(PRETTYINDENT_STD),
            PRETTYINDENT_STD,
            0,
        );
        get_rule_expr((*query).limitOffset, context, false);
    }
    if !(*query).limitCount.is_null() {
        if (*query).limitOption == LIMIT_OPTION_WITH_TIES as i32 {
            // The limitCount arg is a c_expr, so it needs parens.
            appendContextKeyword(
                context,
                c" FETCH FIRST ".as_ptr(),
                -(PRETTYINDENT_STD),
                PRETTYINDENT_STD,
                0,
            );
            appendStringInfoChar(buf, b'(' as c_char);
            get_rule_expr((*query).limitCount, context, false);
            appendStringInfoChar(buf, b')' as c_char);
            appendStringInfoString(buf, c" ROWS WITH TIES".as_ptr());
        } else {
            appendContextKeyword(
                context,
                c" LIMIT ".as_ptr(),
                -(PRETTYINDENT_STD),
                PRETTYINDENT_STD,
                0,
            );
            if IsA((*query).limitCount, T_Const)
                && (*((*query).limitCount as *mut Const)).constisnull
            {
                appendStringInfoString(buf, c"ALL".as_ptr());
            } else {
                get_rule_expr((*query).limitCount, context, false);
            }
        }
    }

    // Add FOR [KEY] UPDATE/SHARE clauses if present
    if (*query).hasForUpdate {
        let mut l = list_head((*query).rowMarks);
        while !l.is_null() {
            let rc = lfirst(l) as *mut RowMarkClause;

            // don't print implicit clauses
            if (*rc).pushedDown {
                l = lnext((*query).rowMarks, l);
                continue;
            }

            match (*rc).strength as u32 {
                LCS_NONE => {
                    elog!(ERROR, "unrecognized LockClauseStrength {}", (*rc).strength as i32);
                }
                LCS_FORKEYSHARE => {
                    appendContextKeyword(
                        context,
                        c" FOR KEY SHARE".as_ptr(),
                        -(PRETTYINDENT_STD),
                        PRETTYINDENT_STD,
                        0,
                    );
                }
                LCS_FORSHARE => {
                    appendContextKeyword(
                        context,
                        c" FOR SHARE".as_ptr(),
                        -(PRETTYINDENT_STD),
                        PRETTYINDENT_STD,
                        0,
                    );
                }
                LCS_FORNOKEYUPDATE => {
                    appendContextKeyword(
                        context,
                        c" FOR NO KEY UPDATE".as_ptr(),
                        -(PRETTYINDENT_STD),
                        PRETTYINDENT_STD,
                        0,
                    );
                }
                LCS_FORUPDATE => {
                    appendContextKeyword(
                        context,
                        c" FOR UPDATE".as_ptr(),
                        -(PRETTYINDENT_STD),
                        PRETTYINDENT_STD,
                        0,
                    );
                }
                _ => {}
            }

            appendStringInfo!(buf, " OF {}", { /* TODO(pg-port): quote_identifier(get_rtable_name) */ "" });
            if (*rc).waitPolicy == LockWaitError as i32 {
                appendStringInfoString(buf, c" NOWAIT".as_ptr());
            } else if (*rc).waitPolicy == LockWaitSkip as i32 {
                appendStringInfoString(buf, c" SKIP LOCKED".as_ptr());
            }

            l = lnext((*query).rowMarks, l);
        }
    }
}

// get_simple_values_rte: detect whether query looks like SELECT ... FROM VALUES(),
// with no need to rename the output columns of the VALUES RTE.
// If so, return the VALUES RTE. Otherwise return NULL.
unsafe fn get_simple_values_rte(
    query: *mut Query,
    result_desc: TupleDesc,
) -> *mut RangeTblEntry {
    let mut result: *mut RangeTblEntry = std::ptr::null_mut();

    // Scan the rtable and see if there is only one inFromCl RTE that is a VALUES RTE.
    let mut lc = list_head((*query).rtable);
    while !lc.is_null() {
        let rte = lfirst(lc) as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_VALUES && (*rte).inFromCl {
            if !result.is_null() {
                return std::ptr::null_mut(); // multiple VALUES (probably not possible)
            }
            result = rte;
        } else if (*rte).rtekind == RTE_RELATION && !(*rte).inFromCl {
            // ignore rule entries
        } else {
            return std::ptr::null_mut(); // something else -> not simple VALUES
        }

        lc = lnext((*query).rtable, lc);
    }

    // We don't need to check the targetlist in great detail. But we can only
    // simplify if the RTE's column names match what get_target_list() would select.
    if !result.is_null() {
        if list_length((*query).targetList) != list_length((*(*result).eref).colnames) {
            return std::ptr::null_mut();
        }
        let mut colno = 0i32;
        let mut lc_tl = list_head((*query).targetList);
        let mut lc_cn = list_head((*(*result).eref).colnames);
        while !lc_tl.is_null() {
            let tle = lfirst(lc_tl) as *mut TargetEntry;
            let cname = strVal(lfirst(lc_cn) as *mut Node) as *const c_char;
            let colname: *const c_char;

            if (*tle).resjunk {
                return std::ptr::null_mut();
            }

            // compute name that get_target_list would use for column
            colno += 1;
            if !result_desc.is_null() && colno <= (*result_desc).natts {
                colname =
                    NameStr((*TupleDescAttr(result_desc, colno - 1)).attname) as *const c_char;
            } else {
                colname = (*tle).resname;
            }

            // does it match the VALUES RTE?
            if colname.is_null() || libc::strcmp(colname, cname) != 0 {
                return std::ptr::null_mut(); // column name has been changed
            }

            lc_tl = lnext((*query).targetList, lc_tl);
            lc_cn = lnext((*(*result).eref).colnames, lc_cn);
        }
    }

    result
}

unsafe fn get_basic_select_query(query: *mut Query, context: *mut deparse_context) {
    let buf = (*context).buf;
    let values_rte: *mut RangeTblEntry;
    let mut sep: *const c_char;

    if PRETTY_INDENT(context) {
        (*context).indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, b' ' as c_char);
    }

    // If the query looks like SELECT * FROM (VALUES ...), print just the VALUES part.
    values_rte = get_simple_values_rte(query, (*context).resultDesc);
    if !values_rte.is_null() {
        get_values_def((*values_rte).values_lists, context);
        return;
    }

    // Build up the query string - first we say SELECT
    if (*query).isReturn {
        appendStringInfoString(buf, c"RETURN".as_ptr());
    } else {
        appendStringInfoString(buf, c"SELECT".as_ptr());
    }

    // Add the DISTINCT clause if given
    if !(*query).distinctClause.is_null() {
        if (*query).hasDistinctOn {
            appendStringInfoString(buf, c" DISTINCT ON (".as_ptr());
            sep = c"".as_ptr();
            let mut l = list_head((*query).distinctClause);
            while !l.is_null() {
                let srt = lfirst(l) as *mut SortGroupClause;
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(
                    (*srt).tleSortGroupRef,
                    (*query).targetList,
                    false,
                    context,
                );
                sep = c", ".as_ptr();
                l = lnext((*query).distinctClause, l);
            }
            appendStringInfoChar(buf, b')' as c_char);
        } else {
            appendStringInfoString(buf, c" DISTINCT".as_ptr());
        }
    }

    // Then we tell what to select (the targetlist)
    get_target_list((*query).targetList, context);

    // Add the FROM clause if needed
    get_from_clause(query, c" FROM ".as_ptr(), context);

    // Add the WHERE clause if given
    if !(*(*query).jointree).quals.is_null() {
        appendContextKeyword(
            context,
            c" WHERE ".as_ptr(),
            -(PRETTYINDENT_STD),
            PRETTYINDENT_STD,
            1,
        );
        get_rule_expr((*(*query).jointree).quals, context, false);
    }

    // Add the GROUP BY clause if given
    if !(*query).groupClause.is_null() || !(*query).groupingSets.is_null() {
        let save_ingroupby: bool;

        appendContextKeyword(
            context,
            c" GROUP BY ".as_ptr(),
            -(PRETTYINDENT_STD),
            PRETTYINDENT_STD,
            1,
        );
        if (*query).groupDistinct {
            appendStringInfoString(buf, c"DISTINCT ".as_ptr());
        }

        save_ingroupby = (*context).inGroupBy;
        (*context).inGroupBy = true;

        if (*query).groupingSets.is_null() {
            sep = c"".as_ptr();
            let mut l = list_head((*query).groupClause);
            while !l.is_null() {
                let grp = lfirst(l) as *mut SortGroupClause;
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(
                    (*grp).tleSortGroupRef,
                    (*query).targetList,
                    false,
                    context,
                );
                sep = c", ".as_ptr();
                l = lnext((*query).groupClause, l);
            }
        } else {
            sep = c"".as_ptr();
            let mut l = list_head((*query).groupingSets);
            while !l.is_null() {
                let grp = lfirst(l) as *mut GroupingSet;
                appendStringInfoString(buf, sep);
                get_rule_groupingset(grp, (*query).targetList, true, context);
                sep = c", ".as_ptr();
                l = lnext((*query).groupingSets, l);
            }
        }

        (*context).inGroupBy = save_ingroupby;
    }

    // Add the HAVING clause if given
    if !(*query).havingQual.is_null() {
        appendContextKeyword(
            context,
            c" HAVING ".as_ptr(),
            -(PRETTYINDENT_STD),
            PRETTYINDENT_STD,
            0,
        );
        get_rule_expr((*query).havingQual, context, false);
    }

    // Add the WINDOW clause if needed
    if !(*query).windowClause.is_null() {
        get_rule_windowclause(query, context);
    }
}

// ----------
// get_target_list -- Parse back a SELECT target list
// Also used for RETURNING lists in INSERT/UPDATE/DELETE/MERGE.
// ----------
unsafe fn get_target_list(target_list: *mut List, context: *mut deparse_context) {
    let buf = (*context).buf;
    let mut targetbuf: StringInfoData = std::mem::zeroed();
    let mut last_was_multiline = false;
    let mut sep: *const c_char;
    let mut colno: i32;

    // we use targetbuf to hold each TLE's text temporarily
    initStringInfo(&mut targetbuf);

    sep = c" ".as_ptr();
    colno = 0;

    let mut l = list_head(target_list);
    while !l.is_null() {
        let tle = lfirst(l) as *mut TargetEntry;
        let colname: *const c_char;
        let attname: *mut c_char;

        if (*tle).resjunk {
            l = lnext(target_list, l);
            continue; // ignore junk entries
        }

        appendStringInfoString(buf, sep);
        sep = c", ".as_ptr();
        colno += 1;

        // Put the new field text into targetbuf so we can decide whether
        // or not it needs to go on a new line.
        resetStringInfo(&mut targetbuf);
        (*context).buf = &mut targetbuf;

        // We special-case Var nodes rather than using get_rule_expr.
        // This is needed because get_rule_expr displays whole-row Vars as "foo.*",
        // which is not right at the top level of a SELECT list.
        if !(*tle).expr.is_null() && IsA((*tle).expr as *mut Node, T_Var) {
            attname = get_variable((*tle).expr as *mut Var, 0, true, context);
        } else {
            get_rule_expr((*tle).expr as *mut Node, context, true);

            // When colNamesVisible is true, always show the assigned column name
            // explicitly. Otherwise, show it only if it's not FigureColname's fallback.
            attname = if (*context).colNamesVisible {
                std::ptr::null_mut()
            } else {
                c"?column?".as_ptr() as *mut c_char
            };
        }

        // Figure out what the result column should be called.
        // In the context of a view, use the view's tuple descriptor.
        if !(*context).resultDesc.is_null() && colno <= (*(*context).resultDesc).natts {
            colname = NameStr(
                (*TupleDescAttr((*context).resultDesc, colno - 1)).attname,
            ) as *const c_char;
        } else {
            colname = (*tle).resname;
        }

        // Show AS unless the column's name is correct as-is
        if !colname.is_null() {
            // resname could be NULL
            if attname.is_null() || libc::strcmp(attname, colname) != 0 {
                appendStringInfo!(
                    &mut targetbuf,
                    " AS {}",
                    std::ffi::CStr::from_ptr(quote_identifier(colname as *mut c_char))
                        .to_str()
                        .unwrap_or("")
                );
            }
        }

        // Restore context's output buffer
        (*context).buf = buf;

        // Consider line-wrapping if enabled
        if PRETTY_INDENT(context) && (*context).wrapColumn >= 0 {
            let leading_nl_pos: i32;

            // Does the new field start with a new line?
            if targetbuf.len > 0 && *targetbuf.data == b'\n' as c_char {
                leading_nl_pos = 0;
            } else {
                leading_nl_pos = -1;
            }

            if leading_nl_pos >= 0 {
                // remove any trailing spaces currently in buf
                removeStringInfoSpaces(buf);
            } else {
                // Locate the start of the current line in the output buffer
                let trailing_nl = libc::strrchr((*buf).data, b'\n' as c_int);
                let trailing_nl_ptr: *const c_char = if trailing_nl.is_null() {
                    (*buf).data
                } else {
                    trailing_nl.add(1)
                };

                // Add a newline, plus some indentation, if the new field is
                // not the first and either the new field would cause an overflow
                // or the last field used more than one line.
                if colno > 1
                    && (libc::strlen(trailing_nl_ptr) + targetbuf.len as usize
                        > (*context).wrapColumn as usize
                        || last_was_multiline)
                {
                    appendContextKeyword(
                        context,
                        c"".as_ptr(),
                        -(PRETTYINDENT_STD),
                        PRETTYINDENT_STD,
                        PRETTYINDENT_VAR,
                    );
                }
            }

            // Remember this field's multiline status for next iteration
            last_was_multiline = !libc::strchr(
                targetbuf.data.add((leading_nl_pos + 1) as usize),
                b'\n' as c_int,
            )
            .is_null();
        }

        // Add the new field
        appendBinaryStringInfo(buf, targetbuf.data, targetbuf.len);

        l = lnext(target_list, l);
    }

    // clean up
    pfree(targetbuf.data as *mut c_void);
}

unsafe fn get_returning_clause(query: *mut Query, context: *mut deparse_context) {
    let buf = (*context).buf;

    if !(*query).returningList.is_null() {
        let mut have_with = false;

        appendContextKeyword(
            context,
            c" RETURNING".as_ptr(),
            -(PRETTYINDENT_STD),
            PRETTYINDENT_STD,
            1,
        );

        // Add WITH (OLD/NEW) options, if they're not the defaults
        if !(*query).returningOldAlias.is_null()
            && libc::strcmp((*query).returningOldAlias, c"old".as_ptr()) != 0
        {
            appendStringInfo!(
                buf,
                " WITH (OLD AS {}",
                std::ffi::CStr::from_ptr(quote_identifier((*query).returningOldAlias))
                    .to_str()
                    .unwrap_or("")
            );
            have_with = true;
        }
        if !(*query).returningNewAlias.is_null()
            && libc::strcmp((*query).returningNewAlias, c"new".as_ptr()) != 0
        {
            if have_with {
                appendStringInfo!(
                    buf,
                    ", NEW AS {}",
                    std::ffi::CStr::from_ptr(quote_identifier((*query).returningNewAlias))
                        .to_str()
                        .unwrap_or("")
                );
            } else {
                appendStringInfo!(
                    buf,
                    " WITH (NEW AS {}",
                    std::ffi::CStr::from_ptr(quote_identifier((*query).returningNewAlias))
                        .to_str()
                        .unwrap_or("")
                );
                have_with = true;
            }
        }
        if have_with {
            appendStringInfoChar(buf, b')' as c_char);
        }

        // Add the returning expressions themselves
        get_target_list((*query).returningList, context);
    }
}

unsafe fn get_setop_query(set_op: *mut Node, query: *mut Query, context: *mut deparse_context) {
    let buf = (*context).buf;
    let need_paren: bool;

    // Guard against excessively long or deeply-nested queries
    CHECK_FOR_INTERRUPTS!();
    check_stack_depth();

    if IsA(set_op, T_RangeTblRef) {
        let rtr = set_op as *mut RangeTblRef;
        let rte = rt_fetch((*rtr).rtindex, (*query).rtable);
        let subquery = (*rte).subquery;

        // We need parens if WITH, ORDER BY, FOR UPDATE, or LIMIT; see gram.y.
        // Also add parens if the leaf query contains its own set operations.
        let need_paren = !(*subquery).cteList.is_null()
            || !(*subquery).sortClause.is_null()
            || !(*subquery).rowMarks.is_null()
            || !(*subquery).limitOffset.is_null()
            || !(*subquery).limitCount.is_null()
            || !(*subquery).setOperations.is_null();

        if need_paren {
            appendStringInfoChar(buf, b'(' as c_char);
        }
        get_query_def(
            subquery,
            buf,
            (*context).namespaces,
            (*context).resultDesc,
            (*context).colNamesVisible,
            (*context).prettyFlags,
            (*context).wrapColumn,
            (*context).indentLevel,
        );
        if need_paren {
            appendStringInfoChar(buf, b')' as c_char);
        }
    } else if IsA(set_op, T_SetOperationStmt) {
        let op = set_op as *mut SetOperationStmt;
        let subindent: i32;
        let save_colnamesvisible: bool;

        // We force parens when nesting two SetOperationStmts, except when the
        // lefthand input is another setop of the same kind.
        let need_paren = if IsA((*op).larg, T_SetOperationStmt) {
            let lop = (*op).larg as *mut SetOperationStmt;
            !((*op).op == (*lop).op && (*op).all == (*lop).all)
        } else {
            false
        };

        if need_paren {
            appendStringInfoChar(buf, b'(' as c_char);
            subindent = PRETTYINDENT_STD;
            appendContextKeyword(context, c"".as_ptr(), subindent, 0, 0);
        } else {
            subindent = 0;
        }

        get_setop_query((*op).larg, query, context);

        if need_paren {
            appendContextKeyword(context, c") ".as_ptr(), -(subindent), 0, 0);
        } else if PRETTY_INDENT(context) {
            appendContextKeyword(context, c"".as_ptr(), -(subindent), 0, 0);
        } else {
            appendStringInfoChar(buf, b' ' as c_char);
        }

        match (*op).op as u32 {
            SETOP_UNION => {
                appendStringInfoString(buf, c"UNION ".as_ptr());
            }
            SETOP_INTERSECT => {
                appendStringInfoString(buf, c"INTERSECT ".as_ptr());
            }
            SETOP_EXCEPT => {
                appendStringInfoString(buf, c"EXCEPT ".as_ptr());
            }
            _ => {
                elog!(ERROR, "unrecognized set op: {}", (*op).op as i32);
            }
        }
        if (*op).all {
            appendStringInfoString(buf, c"ALL ".as_ptr());
        }

        // Always parenthesize if RHS is another setop
        let need_paren = IsA((*op).rarg, T_SetOperationStmt);

        if need_paren {
            appendStringInfoChar(buf, b'(' as c_char);
            subindent = PRETTYINDENT_STD;
        } else {
            subindent = 0;
        }
        appendContextKeyword(context, c"".as_ptr(), subindent, 0, 0);

        // The output column names of the RHS sub-select don't matter.
        save_colnamesvisible = (*context).colNamesVisible;
        (*context).colNamesVisible = false;

        get_setop_query((*op).rarg, query, context);

        (*context).colNamesVisible = save_colnamesvisible;

        if PRETTY_INDENT(context) {
            (*context).indentLevel -= subindent;
        }
        if need_paren {
            appendContextKeyword(context, c")".as_ptr(), 0, 0, 0);
        }
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(set_op) as i32);
    }
}

// Display a sort/group clause.
// Also returns the expression tree, so caller need not find it again.
unsafe fn get_rule_sortgroupclause(
    r#ref: Index,
    tlist: *mut List,
    force_colno: bool,
    context: *mut deparse_context,
) -> *mut Node {
    let buf = (*context).buf;
    let tle: *mut TargetEntry;
    let expr: *mut Node;

    tle = get_sortgroupref_tle(r#ref, tlist);
    expr = (*tle).expr as *mut Node;

    // Use column-number form if requested by caller.
    // Otherwise, if expression is a constant, force it to be dumped with an
    // explicit cast as decoration --- a simple integer constant is ambiguous
    // (and will be misinterpreted by findTargetlistEntrySQL92()) if dumped
    // without any decoration.
    // Similarly, if it's just a Var, there is risk of misinterpretation if
    // the column name is reassigned in the SELECT list, so we may need to
    // force table qualification.
    // And, if it's anything more complex than a simple Var, force extra parens.
    if force_colno {
        appendStringInfo!(buf, "{}", (*tle).resno as i32);
    } else if expr.is_null() {
        // do nothing, probably can't happen
    } else if IsA(expr, T_Const) {
        get_const_expr(expr as *mut Const, context, 1);
    } else if IsA(expr, T_Var) {
        // Tell get_variable to check for name conflict
        let save_varinorderby = (*context).varInOrderBy;
        (*context).varInOrderBy = true;
        let _ = get_variable(expr as *mut Var, 0, false, context);
        (*context).varInOrderBy = save_varinorderby;
    } else {
        // We must force parens for function-like expressions even if
        // PRETTY_PAREN is off, since those are the ones in danger of
        // misparsing.
        let need_paren = PRETTY_PAREN(context)
            || IsA(expr, T_FuncExpr)
            || IsA(expr, T_Aggref)
            || IsA(expr, T_WindowFunc)
            || IsA(expr, T_JsonConstructorExpr);

        if need_paren {
            appendStringInfoChar((*context).buf, b'(' as c_char);
        }
        get_rule_expr(expr, context, true);
        if need_paren {
            appendStringInfoChar((*context).buf, b')' as c_char);
        }
    }

    expr
}

// Display a GroupingSet
unsafe fn get_rule_groupingset(
    gset: *mut GroupingSet,
    targetlist: *mut List,
    omit_parens: bool,
    context: *mut deparse_context,
) {
    let buf = (*context).buf;
    let omit_child_parens: bool;
    let mut sep: *const c_char = c"".as_ptr();

    match (*gset).kind as u32 {
        GROUPING_SET_EMPTY => {
            appendStringInfoString(buf, c"()".as_ptr());
            return;
        }
        GROUPING_SET_SIMPLE => {
            if !omit_parens || list_length((*gset).content) != 1 {
                appendStringInfoChar(buf, b'(' as c_char);
            }
            let mut l = list_head((*gset).content);
            while !l.is_null() {
                let r#ref = lfirst_int(l) as Index;
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(r#ref, targetlist, false, context);
                sep = c", ".as_ptr();
                l = lnext((*gset).content, l);
            }
            if !omit_parens || list_length((*gset).content) != 1 {
                appendStringInfoChar(buf, b')' as c_char);
            }
            return;
        }
        GROUPING_SET_ROLLUP => {
            appendStringInfoString(buf, c"ROLLUP(".as_ptr());
            omit_child_parens = true;
        }
        GROUPING_SET_CUBE => {
            appendStringInfoString(buf, c"CUBE(".as_ptr());
            omit_child_parens = true;
        }
        GROUPING_SET_SETS => {
            appendStringInfoString(buf, c"GROUPING SETS (".as_ptr());
            omit_child_parens = false;
        }
        _ => {
            omit_child_parens = true;
        }
    }

    let mut l = list_head((*gset).content);
    while !l.is_null() {
        appendStringInfoString(buf, sep);
        get_rule_groupingset(lfirst(l) as *mut GroupingSet, targetlist, omit_child_parens, context);
        sep = c", ".as_ptr();
        l = lnext((*gset).content, l);
    }

    appendStringInfoChar(buf, b')' as c_char);
}

// Display an ORDER BY list.
unsafe fn get_rule_orderby(
    order_list: *mut List,
    target_list: *mut List,
    force_colno: bool,
    context: *mut deparse_context,
) {
    let buf = (*context).buf;
    let mut sep: *const c_char = c"".as_ptr();

    let mut l = list_head(order_list);
    while !l.is_null() {
        let srt = lfirst(l) as *mut SortGroupClause;
        let sortexpr: *mut Node;
        let sortcoltype: Oid;
        let typentry: *mut TypeCacheEntry;

        appendStringInfoString(buf, sep);
        sortexpr = get_rule_sortgroupclause((*srt).tleSortGroupRef, target_list, force_colno, context);
        sortcoltype = exprType(sortexpr);
        // See whether operator is default < or > for datatype
        typentry = lookup_type_cache(
            sortcoltype,
            (TYPECACHE_LT_OPR | TYPECACHE_GT_OPR) as i32,
        );
        if (*srt).sortop == (*typentry).lt_opr {
            // ASC is default, so emit nothing for it
            if (*srt).nulls_first {
                appendStringInfoString(buf, c" NULLS FIRST".as_ptr());
            }
        } else if (*srt).sortop == (*typentry).gt_opr {
            appendStringInfoString(buf, c" DESC".as_ptr());
            // DESC defaults to NULLS FIRST
            if !(*srt).nulls_first {
                appendStringInfoString(buf, c" NULLS LAST".as_ptr());
            }
        } else {
            appendStringInfo!(
                buf,
                " USING {}",
                { /* TODO(pg-port): generate_operator_name */ "" }
            );
            // be specific to eliminate ambiguity
            if (*srt).nulls_first {
                appendStringInfoString(buf, c" NULLS FIRST".as_ptr());
            } else {
                appendStringInfoString(buf, c" NULLS LAST".as_ptr());
            }
        }
        sep = c", ".as_ptr();

        l = lnext(order_list, l);
    }
}
// section: ruleutils C lines 6749-13709

fn get_rule_windowclause(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut sep: *const ::std::os::raw::c_char = ::std::ptr::null();
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        foreach!(lc, (*query).windowClause, {
            let wc = lfirst(crate::current_cell!(lc)) as *mut WindowClause;
            if (*wc).name.is_null() {
                continue; // ignore anonymous windows
            }
            if sep.is_null() {
                appendContextKeyword(context,
                    b" WINDOW \0".as_ptr() as _, -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
            } else {
                appendStringInfoString(buf, sep);
            }
            appendStringInfo!(buf, "{} AS ",
                ::std::ffi::CStr::from_ptr(quote_identifier((*wc).name)).to_string_lossy());
            get_rule_windowspec(wc, (*query).targetList, context);
            sep = b", \0".as_ptr() as _;
        });
    }
}

// Display a window definition
fn get_rule_windowspec(wc: *mut WindowClause, target_list: *mut List, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut needspace = false;
        let mut sep: *const ::std::os::raw::c_char;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();

        appendStringInfoChar(buf, b'(' as _);
        if !(*wc).refname.is_null() {
            appendStringInfoString(buf, quote_identifier((*wc).refname));
            needspace = true;
        }
        /* partition clauses are always inherited, so only print if no refname */
        if !(*wc).partitionClause.is_null() && (*wc).refname.is_null() {
            if needspace { appendStringInfoChar(buf, b' ' as _); }
            appendStringInfoString(buf, b"PARTITION BY \0".as_ptr() as _);
            sep = b"\0".as_ptr() as _;
            foreach!(lc, (*wc).partitionClause, {
                let grp = lfirst(crate::current_cell!(lc)) as *mut SortGroupClause;
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause((*grp).tleSortGroupRef, target_list, false, context);
                sep = b", \0".as_ptr() as _;
            });
            needspace = true;
        }
        /* print ordering clause only if not inherited */
        if !(*wc).orderClause.is_null() && !(*wc).copiedOrder {
            if needspace { appendStringInfoChar(buf, b' ' as _); }
            appendStringInfoString(buf, b"ORDER BY \0".as_ptr() as _);
            get_rule_orderby((*wc).orderClause, target_list, false, context);
            needspace = true;
        }
        /* framing clause is never inherited, so print unless it's default */
        if ((*wc).frameOptions & FRAMEOPTION_NONDEFAULT) != 0 {
            if needspace { appendStringInfoChar(buf, b' ' as _); }
            get_window_frame_options((*wc).frameOptions, (*wc).startOffset, (*wc).endOffset, context);
        }
        appendStringInfoChar(buf, b')' as _);
    }
}

// Append the description of a window's framing options to context->buf
fn get_window_frame_options(frame_options: i32, start_offset: *mut Node, end_offset: *mut Node, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        if (frame_options & FRAMEOPTION_NONDEFAULT) != 0 {
            if (frame_options & FRAMEOPTION_RANGE) != 0 {
                appendStringInfoString(buf, b"RANGE \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_ROWS) != 0 {
                appendStringInfoString(buf, b"ROWS \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_GROUPS) != 0 {
                appendStringInfoString(buf, b"GROUPS \0".as_ptr() as _);
            } else {
                debug_assert!(false);
            }
            if (frame_options & FRAMEOPTION_BETWEEN) != 0 {
                appendStringInfoString(buf, b"BETWEEN \0".as_ptr() as _);
            }
            if (frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING) != 0 {
                appendStringInfoString(buf, b"UNBOUNDED PRECEDING \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_START_CURRENT_ROW) != 0 {
                appendStringInfoString(buf, b"CURRENT ROW \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_START_OFFSET) != 0 {
                get_rule_expr(start_offset, context, false);
                if (frame_options & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                    appendStringInfoString(buf, b" PRECEDING \0".as_ptr() as _);
                } else if (frame_options & FRAMEOPTION_START_OFFSET_FOLLOWING) != 0 {
                    appendStringInfoString(buf, b" FOLLOWING \0".as_ptr() as _);
                } else { debug_assert!(false); }
            } else { debug_assert!(false); }
            if (frame_options & FRAMEOPTION_BETWEEN) != 0 {
                appendStringInfoString(buf, b"AND \0".as_ptr() as _);
                if (frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) != 0 {
                    appendStringInfoString(buf, b"UNBOUNDED FOLLOWING \0".as_ptr() as _);
                } else if (frame_options & FRAMEOPTION_END_CURRENT_ROW) != 0 {
                    appendStringInfoString(buf, b"CURRENT ROW \0".as_ptr() as _);
                } else if (frame_options & FRAMEOPTION_END_OFFSET) != 0 {
                    get_rule_expr(end_offset, context, false);
                    if (frame_options & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                        appendStringInfoString(buf, b" PRECEDING \0".as_ptr() as _);
                    } else if (frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING) != 0 {
                        appendStringInfoString(buf, b" FOLLOWING \0".as_ptr() as _);
                    } else { debug_assert!(false); }
                } else { debug_assert!(false); }
            }
            if (frame_options & FRAMEOPTION_EXCLUDE_CURRENT_ROW) != 0 {
                appendStringInfoString(buf, b"EXCLUDE CURRENT ROW \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_EXCLUDE_GROUP) != 0 {
                appendStringInfoString(buf, b"EXCLUDE GROUP \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_EXCLUDE_TIES) != 0 {
                appendStringInfoString(buf, b"EXCLUDE TIES \0".as_ptr() as _);
            }
            /* we will now have a trailing space; remove it */
            (*buf).len -= 1;
            *(*buf).data.add((*buf).len as usize) = b'\0' as _;
        }
    }
}

// Return the description of a window's framing options as a palloc'd string
pub unsafe fn get_window_frame_options_for_explain(
    frame_options: i32,
    start_offset: *mut Node,
    end_offset: *mut Node,
    dpcontext: *mut List,
    forceprefix: bool,
) -> *mut ::std::os::raw::c_char {
    let mut buf: StringInfoData = ::std::mem::zeroed();
    let mut context: deparse_context = ::std::mem::zeroed();
    initStringInfo(&mut buf);
    context.buf = &mut buf;
    context.namespaces = dpcontext;
    context.resultDesc = ::std::ptr::null_mut();
    context.targetList = ::std::ptr::null_mut();
    context.windowClause = ::std::ptr::null_mut();
    context.varprefix = forceprefix;
    context.prettyFlags = 0;
    context.wrapColumn = WRAP_COLUMN_DEFAULT;
    context.indentLevel = 0;
    context.colNamesVisible = true;
    context.inGroupBy = false;
    context.varInOrderBy = false;
    context.appendparents = ::std::ptr::null_mut();
    get_window_frame_options(frame_options, start_offset, end_offset, &mut context);
    buf.data
}

// get_insert_query_def - Parse back an INSERT parsetree
fn get_insert_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut select_rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut values_rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut sep: *const ::std::os::raw::c_char;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        let mut strippedexprs: *mut List = ::std::ptr::null_mut();

        /* Insert the WITH clause if given */
        get_with_clause(query, context);

        /*
         * If it's an INSERT ... SELECT or multi-row VALUES, there will be a
         * single RTE for the SELECT or VALUES.  Plain VALUES has neither.
         */
        foreach!(lc, (*query).rtable, {
            rte = lfirst(crate::current_cell!(lc)) as *mut RangeTblEntry;
            if (*rte).rtekind == RTE_SUBQUERY {
                if !select_rte.is_null() { elog!(ERROR, "too many subquery RTEs in INSERT"); }
                select_rte = rte;
            }
            if (*rte).rtekind == RTE_VALUES {
                if !values_rte.is_null() { elog!(ERROR, "too many values RTEs in INSERT"); }
                values_rte = rte;
            }
        });
        if !select_rte.is_null() && !values_rte.is_null() {
            elog!(ERROR, "both subquery and values RTEs in INSERT");
        }

        /* Start the query with INSERT INTO relname */
        rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            (*context).indentLevel += PRETTYINDENT_STD as i32;
            appendStringInfoChar(buf, b' ' as _);
        }
        appendStringInfo!(buf, "INSERT INTO {}",
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());

        /* Print the relation alias, if needed; INSERT requires explicit AS */
        get_rte_alias(rte, (*query).resultRelation, true, context);
        /* always want a space here */
        appendStringInfoChar(buf, b' ' as _);

        /*
         * Add the insert-column-names list.
         */
        strippedexprs = ::std::ptr::null_mut();
        sep = b"\0".as_ptr() as _;
        if !(*query).targetList.is_null() {
            appendStringInfoChar(buf, b'(' as _);
        }
        foreach!(lc, (*query).targetList, {
            let tle = lfirst(crate::current_cell!(lc)) as *mut TargetEntry;
            if (*tle).resjunk { continue; } // ignore junk entries
            appendStringInfoString(buf, sep);
            sep = b", \0".as_ptr() as _;
            appendStringInfoString(buf, quote_identifier(get_attname((*rte).relid, (*tle).resno, false)));
            strippedexprs = lappend(strippedexprs,
                processIndirection((*tle).expr as *mut Node, context));
        });
        if !(*query).targetList.is_null() {
            appendStringInfoString(buf, b") \0".as_ptr() as _);
        }

        if (*query).r#override == OVERRIDING_SYSTEM_VALUE {
            appendStringInfoString(buf, b"OVERRIDING SYSTEM VALUE \0".as_ptr() as _);
        } else if (*query).r#override == OVERRIDING_USER_VALUE {
            appendStringInfoString(buf, b"OVERRIDING USER VALUE \0".as_ptr() as _);
        }

        if !select_rte.is_null() {
            /* Add the SELECT */
            get_query_def((*select_rte).subquery, buf, (*context).namespaces,
                ::std::ptr::null_mut(), false,
                (*context).prettyFlags, (*context).wrapColumn, (*context).indentLevel);
        } else if !values_rte.is_null() {
            /* Add the multi-VALUES expression lists */
            get_values_def((*values_rte).values_lists, context);
        } else if !strippedexprs.is_null() {
            /* Add the single-VALUES expression list */
            appendContextKeyword(context, b"VALUES (\0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 2);
            get_rule_list_toplevel(strippedexprs, context, false);
            appendStringInfoChar(buf, b')' as _);
        } else {
            /* No expressions, so it must be DEFAULT VALUES */
            appendStringInfoString(buf, b"DEFAULT VALUES\0".as_ptr() as _);
        }

        /* Add ON CONFLICT if present */
        if !(*query).onConflict.is_null() {
            let confl = (*query).onConflict;
            appendStringInfoString(buf, b" ON CONFLICT\0".as_ptr() as _);
            if !(*confl).arbiterElems.is_null() {
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr((*confl).arbiterElems as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                if !(*confl).arbiterWhere.is_null() {
                    let save_varprefix = (*context).varprefix;
                    /*
                     * Force non-prefixing of Vars, since parser assumes that they
                     * belong to target relation.  WHERE clause does not use
                     * InferenceElem, so this is separately required.
                     */
                    (*context).varprefix = false;
                    appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
                    get_rule_expr((*confl).arbiterWhere, context, false);
                    (*context).varprefix = save_varprefix;
                }
            } else if OidIsValid((*confl).constraint) {
                let constraint = get_constraint_name((*confl).constraint);
                if constraint.is_null() {
                    elog!(ERROR, "cache lookup failed for constraint {}", (*confl).constraint);
                }
                appendStringInfo!(buf, " ON CONSTRAINT {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(constraint)).to_string_lossy());
            }
            if (*confl).action == ONCONFLICT_NOTHING {
                appendStringInfoString(buf, b" DO NOTHING\0".as_ptr() as _);
            } else {
                appendStringInfoString(buf, b" DO UPDATE SET \0".as_ptr() as _);
                /* Deparse targetlist */
                get_update_query_targetlist_def(query, (*confl).onConflictSet, context, rte);
                /* Add a WHERE clause if given */
                if !(*confl).onConflictWhere.is_null() {
                    appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
                    get_rule_expr((*confl).onConflictWhere, context, false);
                }
            }
        }

        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_update_query_def - Parse back an UPDATE parsetree
fn get_update_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        /* Insert the WITH clause if given */
        get_with_clause(query, context);
        /* Start the query with UPDATE relname SET */
        let rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            appendStringInfoChar(buf, b' ' as _);
            (*context).indentLevel += PRETTYINDENT_STD as i32;
        }
        appendStringInfo!(buf, "UPDATE {}{}",
            ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());
        /* Print the relation alias, if needed */
        get_rte_alias(rte, (*query).resultRelation, false, context);
        appendStringInfoString(buf, b" SET \0".as_ptr() as _);
        /* Deparse targetlist */
        get_update_query_targetlist_def(query, (*query).targetList, context, rte);
        /* Add the FROM clause if needed */
        get_from_clause(query, b" FROM \0".as_ptr() as _, context);
        /* Add a WHERE clause if given */
        if !(*(*query).jointree).quals.is_null() {
            appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
            get_rule_expr((*(*query).jointree).quals, context, false);
        }
        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_update_query_targetlist_def - Parse back an UPDATE targetlist
fn get_update_query_targetlist_def(
    query: *mut Query,
    target_list: *mut List,
    context: *mut deparse_context,
    rte: *mut RangeTblEntry,
) {
    unsafe {
        let buf = (*context).buf;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        let mut next_ma_cell: *mut ListCell;
        let mut remaining_ma_columns: i32;
        let mut sep: *const ::std::os::raw::c_char;
        let mut cur_ma_sublink: *mut SubLink;
        let mut ma_sublinks: *mut List = ::std::ptr::null_mut();

        /*
         * Prepare to deal with MULTIEXPR assignments: collect the source SubLinks
         * into a list.  We expect them to appear, in ID order, in resjunk tlist
         * entries.
         */
        if (*query).hasSubLinks {
            foreach!(lc, target_list, {
                let tle = lfirst(crate::current_cell!(lc)) as *mut TargetEntry;
                if (*tle).resjunk && IsA!((*tle).expr, T_SubLink) {
                    let sl = (*tle).expr as *mut SubLink;
                    if (*sl).subLinkType == MULTIEXPR_SUBLINK {
                        ma_sublinks = lappend(ma_sublinks, sl as *mut _);
                        debug_assert!((*sl).subLinkId == list_length(ma_sublinks));
                    }
                }
            });
        }
        next_ma_cell = list_head(ma_sublinks);
        cur_ma_sublink = ::std::ptr::null_mut();
        remaining_ma_columns = 0;

        /* Add the comma separated list of 'attname = value' */
        sep = b"\0".as_ptr() as _;
        foreach!(lc, target_list, {
            let tle = lfirst(crate::current_cell!(lc)) as *mut TargetEntry;
            let mut expr: *mut Node;

            if (*tle).resjunk { continue; } // ignore junk entries

            /* Emit separator (OK whether we're in multiassignment or not) */
            appendStringInfoString(buf, sep);
            sep = b", \0".as_ptr() as _;

            /*
             * Check to see if we're starting a multiassignment group: if so,
             * output a left paren.
             */
            if !next_ma_cell.is_null() && cur_ma_sublink.is_null() {
                /*
                 * We must dig down into the expr to see if it's a PARAM_MULTIEXPR
                 * Param.  That could be buried under FieldStores and
                 * SubscriptingRefs and CoerceToDomains (cf processIndirection()),
                 * and underneath those there could be an implicit type coercion.
                 */
                expr = (*tle).expr as *mut Node;
                loop {
                    if expr.is_null() { break; }
                    if IsA!(expr, T_FieldStore) {
                        let fstore = expr as *mut FieldStore;
                        expr = linitial!((*fstore).newvals) as *mut Node;
                    } else if IsA!(expr, T_SubscriptingRef) {
                        let sbsref = expr as *mut SubscriptingRef;
                        if (*sbsref).refassgnexpr.is_null() { break; }
                        expr = (*sbsref).refassgnexpr as *mut Node;
                    } else if IsA!(expr, T_CoerceToDomain) {
                        let cdomain = expr as *mut CoerceToDomain;
                        if (*cdomain).coercionformat != COERCE_IMPLICIT_CAST { break; }
                        expr = (*cdomain).arg as *mut Node;
                    } else { break; }
                }
                expr = strip_implicit_coercions(expr);

                if !expr.is_null()
                    && IsA!(expr, T_Param)
                    && (*(expr as *mut Param)).paramkind == PARAM_MULTIEXPR
                {
                    cur_ma_sublink = lfirst(crate::current_cell!(next_ma_cell)) as *mut SubLink;
                    next_ma_cell = lnext(ma_sublinks, next_ma_cell);
                    remaining_ma_columns = count_nonjunk_tlist_entries(
                        (*((*cur_ma_sublink).subselect as *mut Query)).targetList);
                    debug_assert!(
                        (*(expr as *mut Param)).paramid == (((*cur_ma_sublink).subLinkId << 16) | 1));
                    appendStringInfoChar(buf, b'(' as _);
                }
            }

            /*
             * Put out name of target column; look in the catalogs, not at
             * tle->resname, since resname will fail to track RENAME.
             */
            appendStringInfoString(buf,
                quote_identifier(get_attname((*rte).relid, (*tle).resno, false)));

            /*
             * Print any indirection needed (subfields or subscripts), and strip
             * off the top-level nodes representing the indirection assignments.
             */
            expr = processIndirection((*tle).expr as *mut Node, context);

            /*
             * If we're in a multiassignment, skip printing anything more, unless
             * this is the last column; in which case, what we print should be the
             * sublink, not the Param.
             */
            if !cur_ma_sublink.is_null() {
                remaining_ma_columns -= 1;
                if remaining_ma_columns > 0 { continue; } // not the last column of multiassignment
                appendStringInfoChar(buf, b')' as _);
                expr = cur_ma_sublink as *mut Node;
                cur_ma_sublink = ::std::ptr::null_mut();
            }

            appendStringInfoString(buf, b" = \0".as_ptr() as _);
            get_rule_expr(expr, context, false);
        });
    }
}

// get_delete_query_def - Parse back a DELETE parsetree
fn get_delete_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        /* Insert the WITH clause if given */
        get_with_clause(query, context);
        /* Start the query with DELETE FROM relname */
        let rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            appendStringInfoChar(buf, b' ' as _);
            (*context).indentLevel += PRETTYINDENT_STD as i32;
        }
        appendStringInfo!(buf, "DELETE FROM {}{}",
            ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());
        /* Print the relation alias, if needed */
        get_rte_alias(rte, (*query).resultRelation, false, context);
        /* Add the USING clause if given */
        get_from_clause(query, b" USING \0".as_ptr() as _, context);
        /* Add a WHERE clause if given */
        if !(*(*query).jointree).quals.is_null() {
            appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
            get_rule_expr((*(*query).jointree).quals, context, false);
        }
        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_merge_query_def - Parse back a MERGE parsetree
fn get_merge_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        let mut have_not_matched_by_source = false;

        /* Insert the WITH clause if given */
        get_with_clause(query, context);
        /* Start the query with MERGE INTO relname */
        let rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            appendStringInfoChar(buf, b' ' as _);
            (*context).indentLevel += PRETTYINDENT_STD as i32;
        }
        appendStringInfo!(buf, "MERGE INTO {}{}",
            ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());
        /* Print the relation alias, if needed */
        get_rte_alias(rte, (*query).resultRelation, false, context);
        /* Print the source relation and join clause */
        get_from_clause(query, b" USING \0".as_ptr() as _, context);
        appendContextKeyword(context, b" ON \0".as_ptr() as _,
            -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 2);
        get_rule_expr((*query).mergeJoinCondition, context, false);

        /*
         * Test for any NOT MATCHED BY SOURCE actions.  If there are none, then
         * any NOT MATCHED BY TARGET actions are output as "WHEN NOT MATCHED", per
         * SQL standard.  Otherwise, we have a non-SQL-standard query, so output
         * "BY SOURCE" / "BY TARGET" qualifiers for all NOT MATCHED actions, to be
         * more explicit.
         */
        foreach!(lc, (*query).mergeActionList, {
            let action = lfirst_node!(MergeAction, T_MergeAction, lc);
            if (*action).matchKind == MERGE_WHEN_NOT_MATCHED_BY_SOURCE {
                have_not_matched_by_source = true;
                break;
            }
        });

        /* Print each merge action */
        foreach!(lc, (*query).mergeActionList, {
            let action = lfirst_node!(MergeAction, T_MergeAction, lc);

            appendContextKeyword(context, b" WHEN \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 2);
            match (*action).matchKind {
                MERGE_WHEN_MATCHED => {
                    appendStringInfoString(buf, b"MATCHED\0".as_ptr() as _);
                }
                MERGE_WHEN_NOT_MATCHED_BY_SOURCE => {
                    appendStringInfoString(buf, b"NOT MATCHED BY SOURCE\0".as_ptr() as _);
                }
                MERGE_WHEN_NOT_MATCHED_BY_TARGET => {
                    if have_not_matched_by_source {
                        appendStringInfoString(buf, b"NOT MATCHED BY TARGET\0".as_ptr() as _);
                    } else {
                        appendStringInfoString(buf, b"NOT MATCHED\0".as_ptr() as _);
                    }
                }
                _ => {
                    elog!(ERROR, "unrecognized matchKind: {}", (*action).matchKind as i32);
                }
            }
            if !(*action).qual.is_null() {
                appendContextKeyword(context, b" AND \0".as_ptr() as _,
                    -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 3);
                get_rule_expr((*action).qual, context, false);
            }
            appendContextKeyword(context, b" THEN \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 3);

            if (*action).commandType == CMD_INSERT {
                /* This generally matches get_insert_query_def() */
                let mut strippedexprs: *mut List = ::std::ptr::null_mut();
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                let mut lc2: *mut ListCell = ::std::ptr::null_mut();

                appendStringInfoString(buf, b"INSERT\0".as_ptr() as _);
                if !(*action).targetList.is_null() {
                    appendStringInfoString(buf, b" (\0".as_ptr() as _);
                }
                foreach!(lc2, (*action).targetList, {
                    let tle = lfirst(crate::current_cell!(lc2)) as *mut TargetEntry;
                    debug_assert!(!(*tle).resjunk);
                    appendStringInfoString(buf, sep);
                    sep = b", \0".as_ptr() as _;
                    appendStringInfoString(buf, quote_identifier(get_attname((*rte).relid, (*tle).resno, false)));
                    strippedexprs = lappend(strippedexprs,
                        processIndirection((*tle).expr as *mut Node, context));
                });
                if !(*action).targetList.is_null() {
                    appendStringInfoChar(buf, b')' as _);
                }
                if (*action).r#override == OVERRIDING_SYSTEM_VALUE {
                    appendStringInfoString(buf, b" OVERRIDING SYSTEM VALUE\0".as_ptr() as _);
                } else if (*action).r#override == OVERRIDING_USER_VALUE {
                    appendStringInfoString(buf, b" OVERRIDING USER VALUE\0".as_ptr() as _);
                }
                if !strippedexprs.is_null() {
                    appendContextKeyword(context, b" VALUES (\0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 4);
                    get_rule_list_toplevel(strippedexprs, context, false);
                    appendStringInfoChar(buf, b')' as _);
                } else {
                    appendStringInfoString(buf, b" DEFAULT VALUES\0".as_ptr() as _);
                }
            } else if (*action).commandType == CMD_UPDATE {
                appendStringInfoString(buf, b"UPDATE SET \0".as_ptr() as _);
                get_update_query_targetlist_def(query, (*action).targetList, context, rte);
            } else if (*action).commandType == CMD_DELETE {
                appendStringInfoString(buf, b"DELETE\0".as_ptr() as _);
            } else if (*action).commandType == CMD_NOTHING {
                appendStringInfoString(buf, b"DO NOTHING\0".as_ptr() as _);
            }
        });

        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_utility_query_def - Parse back a UTILITY parsetree
fn get_utility_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        if !(*query).utilityStmt.is_null() && IsA!((*query).utilityStmt, T_NotifyStmt) {
            let stmt = (*query).utilityStmt as *mut NotifyStmt;
            appendContextKeyword(context, b"\0".as_ptr() as _, 0, PRETTYINDENT_STD as i32, 1);
            appendStringInfo!(buf, "NOTIFY {}",
                ::std::ffi::CStr::from_ptr(quote_identifier((*stmt).conditionname)).to_string_lossy());
            if !(*stmt).payload.is_null() {
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                simple_quote_literal(buf, (*stmt).payload);
            }
        } else {
            /* Currently only NOTIFY utility commands can appear in rules */
            elog!(ERROR, "unexpected utility statement type");
        }
    }
}

/*
 * Display a Var appropriately.
 *
 * In some cases (currently only when recursing into an unnamed join)
 * the Var's varlevelsup has to be interpreted with respect to a context
 * above the current one; levelsup indicates the offset.
 *
 * If istoplevel is true, the Var is at the top level of a SELECT's
 * targetlist, which means we need special treatment of whole-row Vars.
 * Instead of the normal "tab.*", we'll print "tab.*::typename".
 *
 * Returns the attname of the Var, or NULL if the Var has no attname.
 */
fn get_variable(
    var: *mut Var,
    levelsup: i32,
    istoplevel: bool,
    context: *mut deparse_context,
) -> *mut ::std::os::raw::c_char {
    unsafe {
        let buf = (*context).buf;
        let rte: *mut RangeTblEntry;
        let mut attnum: AttrNumber;
        let netlevelsup: i32;
        let dpns: *mut deparse_namespace;
        let mut varno: i32;
        let mut varattno: AttrNumber;
        let colinfo: *mut deparse_columns;
        let refname: *mut ::std::os::raw::c_char;
        let attname: *mut ::std::os::raw::c_char;
        let mut need_prefix: bool;

        /* Find appropriate nesting depth */
        netlevelsup = (*var).varlevelsup as i32 + levelsup;
        if netlevelsup >= list_length((*context).namespaces) {
            elog!(ERROR, "bogus varlevelsup: {} offset {}",
                (*var).varlevelsup, levelsup);
        }
        dpns = list_nth((*context).namespaces, netlevelsup) as *mut deparse_namespace;

        /*
         * If we have a syntactic referent for the Var, and we're working from a
         * parse tree, prefer to use the syntactic referent.  Otherwise, fall back
         * on the semantic referent.
         */
        if (*var).varnosyn > 0 && (*dpns).plan.is_null() {
            varno = (*var).varnosyn as i32;
            varattno = (*var).varattnosyn;
        } else {
            varno = (*var).varno as i32;
            varattno = (*var).varattno;
        }

        /*
         * Try to find the relevant RTE in this rtable.  In a plan tree, it's
         * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
         * down into the subplans, or INDEX_VAR, which is resolved similarly. Also
         * find the aliases previously assigned for this RTE.
         */
        if varno >= 1 && varno <= list_length((*dpns).rtable) {
            /*
             * We might have been asked to map child Vars to some parent relation.
             */
            if !(*context).appendparents.is_null() && !(*dpns).appendrels.is_null() {
                let mut pvarno = varno;
                let mut pvarattno = varattno;
                let mut appinfo = *(*dpns).appendrels.add(pvarno as usize);
                let mut found = false;

                /* Only map up to inheritance parents, not UNION ALL appendrels */
                while !appinfo.is_null()
                    && (*rt_fetch((*appinfo).parent_relid as i32, (*dpns).rtable)).rtekind == RTE_RELATION
                {
                    found = false;
                    if pvarattno > 0 {
                        // system columns stay as-is
                        if pvarattno > (*appinfo).num_child_cols {
                            break; // safety check
                        }
                        pvarattno = *(*appinfo).parent_colnos.add(pvarattno as usize - 1);
                        if pvarattno == 0 {
                            break; // Var is local to child
                        }
                    }
                    pvarno = (*appinfo).parent_relid as i32;
                    found = true;
                    /* If the parent is itself a child, continue up. */
                    debug_assert!(pvarno > 0 && pvarno <= list_length((*dpns).rtable));
                    appinfo = *(*dpns).appendrels.add(pvarno as usize);
                }
                /*
                 * If we found an ancestral rel, and that rel is included in
                 * appendparents, print that column not the original one.
                 */
                if found && bms_is_member(pvarno, (*context).appendparents) {
                    varno = pvarno;
                    varattno = pvarattno;
                }
            }

            rte = rt_fetch(varno, (*dpns).rtable);

            /* might be returning old/new column value */
            if (*var).varreturningtype == VAR_RETURNING_OLD {
                refname = (*dpns).ret_old_alias;
            } else if (*var).varreturningtype == VAR_RETURNING_NEW {
                refname = (*dpns).ret_new_alias;
            } else {
                refname = list_nth((*dpns).rtable_names, varno - 1) as *mut ::std::os::raw::c_char;
            }

            colinfo = deparse_columns_fetch(varno, dpns);
            attnum = varattno;
        } else {
            resolve_special_varno(var as *mut Node, context, get_special_variable, ::std::ptr::null_mut());
            return ::std::ptr::null_mut();
        }

        /*
         * The planner will sometimes emit Vars referencing resjunk elements of a
         * subquery's target list.  If that is the case, drill down to the subplan
         * and print the contents of the referenced tlist item.
         */
        if ((*rte).rtekind == RTE_SUBQUERY || (*rte).rtekind == RTE_CTE)
            && attnum > list_length((*rte).eref.colnames)
            && !(*dpns).inner_plan.is_null()
        {
            let tle: *mut TargetEntry;
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();

            tle = get_tle_by_resno((*dpns).inner_tlist, attnum);
            if tle.is_null() {
                elog!(ERROR, "invalid attnum {} for relation \"{}\"",
                    attnum,
                    ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy());
            }
            debug_assert!(netlevelsup == 0);
            push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns);

            /*
             * Force parentheses because our caller probably assumed a Var is a
             * simple expression.
             */
            if !IsA!((*tle).expr, T_Var) { appendStringInfoChar(buf, b'(' as _); }
            get_rule_expr((*tle).expr as *mut Node, context, true);
            if !IsA!((*tle).expr, T_Var) { appendStringInfoChar(buf, b')' as _); }

            pop_child_plan(dpns, &mut save_dpns);
            return ::std::ptr::null_mut();
        }

        /*
         * If it's an unnamed join, look at the expansion of the alias variable.
         * If it's a simple reference to one of the input vars, then recursively
         * print the name of that var instead.
         */
        if (*rte).rtekind == RTE_JOIN && (*rte).alias.is_null() {
            if (*rte).joinaliasvars.is_null() {
                elog!(ERROR, "cannot decompile join alias var in plan tree");
            }
            if attnum > 0 {
                let aliasvar = list_nth((*rte).joinaliasvars, attnum as i32 - 1) as *mut Var;
                /* we intentionally don't strip implicit coercions here */
                if !aliasvar.is_null() && IsA!(aliasvar, T_Var) {
                    return get_variable(aliasvar, (*var).varlevelsup as i32 + levelsup, istoplevel, context);
                }
            }
            /*
             * Unnamed join has no refname.
             */
            // refname is already set, but for unnamed join it should be NULL
            // (asserted by the C code via Assert(refname == NULL))
            debug_assert!(refname.is_null());
        }

        if attnum == InvalidAttrNumber {
            attname = ::std::ptr::null_mut();
        } else if attnum > 0 {
            /* Get column name to use from the colinfo struct */
            if attnum > (*colinfo).num_cols {
                elog!(ERROR, "invalid attnum {} for relation \"{}\"",
                    attnum,
                    ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy());
            }
            attname = *(*colinfo).colnames.add(attnum as usize - 1);
            /*
             * If we find a Var referencing a dropped column, print something
             * rather than fail.
             */
            if attname.is_null() {
                attname = b"?dropped?column?\0".as_ptr() as *mut _;
            }
        } else {
            /* System column - name is fixed, get it from the catalog */
            attname = get_rte_attribute_name(rte, attnum);
        }

        need_prefix = (*context).varprefix
            || attname.is_null()
            || (*var).varreturningtype != VAR_RETURNING_DEFAULT;

        /*
         * If we're considering a plain Var in an ORDER BY (but not GROUP BY)
         * clause, we may need to add a table-name prefix.
         */
        if (*context).varInOrderBy && !(*context).inGroupBy && !need_prefix {
            let mut colno = 0i32;
            let mut lc_tl: *mut ListCell = ::std::ptr::null_mut();
            foreach!(lc_tl, (*context).targetList, {
                let tle = lfirst(crate::current_cell!(lc_tl)) as *mut TargetEntry;
                let colname: *mut ::std::os::raw::c_char;
                if (*tle).resjunk { continue; } // ignore junk entries
                colno += 1;
                /* This must match colname-choosing logic in get_target_list() */
                if !(*context).resultDesc.is_null() && colno <= (*(*context).resultDesc).natts {
                    colname = NameStr!((*TupleDescAttr((*context).resultDesc, colno - 1)).attname);
                } else {
                    colname = (*tle).resname;
                }
                if !colname.is_null()
                    && !attname.is_null()
                    && libc::strcmp(colname, attname) == 0
                    && !equal(var as *mut _, (*tle).expr as *mut _)
                {
                    need_prefix = true;
                    break;
                }
            });
        }

        if !refname.is_null() && need_prefix {
            appendStringInfoString(buf, quote_identifier(refname));
            appendStringInfoChar(buf, b'.' as _);
        }
        if !attname.is_null() {
            appendStringInfoString(buf, quote_identifier(attname));
        } else {
            appendStringInfoChar(buf, b'*' as _);
            if istoplevel {
                appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                    format_type_with_typemod((*var).vartype, (*var).vartypmod)).to_string_lossy());
            }
        }

        attname
    }
}

/*
 * Deparse a Var which references OUTER_VAR, INNER_VAR, or INDEX_VAR.
 * This routine is actually a callback for resolve_special_varno.
 */
fn get_special_variable(node: *mut Node, context: *mut deparse_context, _callback_arg: *mut ::std::os::raw::c_void) {
    unsafe {
        let buf = (*context).buf;
        /*
         * For a non-Var referent, force parentheses because our caller probably
         * assumed a Var is a simple expression.
         */
        if !IsA!(node, T_Var) { appendStringInfoChar(buf, b'(' as _); }
        get_rule_expr(node, context, true);
        if !IsA!(node, T_Var) { appendStringInfoChar(buf, b')' as _); }
    }
}

/*
 * Chase through plan references to special varnos (OUTER_VAR, INNER_VAR,
 * INDEX_VAR) until we find a real Var or some kind of non-Var node; then,
 * invoke the callback provided.
 */
fn resolve_special_varno(
    node: *mut Node,
    context: *mut deparse_context,
    callback: rsv_callback,
    callback_arg: *mut ::std::os::raw::c_void,
) {
    unsafe {
        /* This function is recursive, so let's be paranoid. */
        check_stack_depth();

        /* If it's not a Var, invoke the callback. */
        if !IsA!(node, T_Var) {
            callback(node, context, callback_arg);
            return;
        }

        /* Find appropriate nesting depth */
        let var = node as *mut Var;
        let dpns = list_nth((*context).namespaces, (*var).varlevelsup as i32) as *mut deparse_namespace;

        /*
         * If varno is special, recurse.  (Don't worry about varnosyn; if we're
         * here, we already decided not to use that.)
         */
        if (*var).varno == OUTER_VAR && !(*dpns).outer_tlist.is_null() {
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let save_appendparents = (*context).appendparents;

            let tle = get_tle_by_resno((*dpns).outer_tlist, (*var).varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for OUTER_VAR var: {}", (*var).varattno);
            }

            /*
             * If we're descending to the first child of an Append or MergeAppend,
             * update appendparents.
             */
            if IsA!((*dpns).plan, T_Append) {
                (*context).appendparents = bms_union((*context).appendparents,
                    (*((*dpns).plan as *mut Append)).apprelids);
            } else if IsA!((*dpns).plan, T_MergeAppend) {
                (*context).appendparents = bms_union((*context).appendparents,
                    (*((*dpns).plan as *mut MergeAppend)).apprelids);
            }

            push_child_plan(dpns, (*dpns).outer_plan, &mut save_dpns);
            resolve_special_varno((*tle).expr as *mut Node, context, callback, callback_arg);
            pop_child_plan(dpns, &mut save_dpns);
            (*context).appendparents = save_appendparents;
            return;
        } else if (*var).varno == INNER_VAR && !(*dpns).inner_tlist.is_null() {
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();

            let tle = get_tle_by_resno((*dpns).inner_tlist, (*var).varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INNER_VAR var: {}", (*var).varattno);
            }
            push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns);
            resolve_special_varno((*tle).expr as *mut Node, context, callback, callback_arg);
            pop_child_plan(dpns, &mut save_dpns);
            return;
        } else if (*var).varno == INDEX_VAR && !(*dpns).index_tlist.is_null() {
            let tle = get_tle_by_resno((*dpns).index_tlist, (*var).varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INDEX_VAR var: {}", (*var).varattno);
            }
            resolve_special_varno((*tle).expr as *mut Node, context, callback, callback_arg);
            return;
        } else if (*var).varno < 1 || (*var).varno > list_length((*dpns).rtable) as u32 {
            elog!(ERROR, "bogus varno: {}", (*var).varno);
        }

        /* Not special.  Just invoke the callback. */
        callback(node, context, callback_arg);
    }
}

/*
 * Get the name of a field of an expression of composite type.
 * The expression is usually a Var, but we handle other cases too.
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 */
fn get_name_for_var_field(
    var: *mut Var,
    fieldno: i32,
    levelsup: i32,
    context: *mut deparse_context,
) -> *const ::std::os::raw::c_char {
    unsafe {
        let mut rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut attnum: AttrNumber = 0;
        let netlevelsup: i32;
        let mut dpns: *mut deparse_namespace = ::std::ptr::null_mut();
        let mut varno: i32;
        let mut varattno: AttrNumber;
        let mut tupdesc: TupleDesc = ::std::ptr::null_mut();
        let mut expr: *mut Node;

        /*
         * If it's a RowExpr that was expanded from a whole-row Var, use the
         * column names attached to it.
         */
        if IsA!(var, T_RowExpr) {
            let r = var as *mut RowExpr;
            if fieldno > 0 && fieldno <= list_length((*r).colnames) {
                return strVal!(list_nth((*r).colnames, fieldno - 1));
            }
        }

        /*
         * If it's a Param of type RECORD, try to find what the Param refers to.
         */
        if IsA!(var, T_Param) {
            let param = var as *mut Param;
            let mut ancestor_cell: *mut ListCell = ::std::ptr::null_mut();
            let mut local_dpns: *mut deparse_namespace = ::std::ptr::null_mut();

            let expr_r = find_param_referent(param, context, &mut local_dpns, &mut ancestor_cell);
            if !expr_r.is_null() {
                /* Found a match, so recurse to decipher the field name */
                let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
                let result: *const ::std::os::raw::c_char;

                push_ancestor_plan(local_dpns, ancestor_cell, &mut save_dpns);
                result = get_name_for_var_field(expr_r as *mut Var, fieldno, 0, context);
                pop_ancestor_plan(local_dpns, &mut save_dpns);
                return result;
            }
        }

        /*
         * If it's a Var of type RECORD, we have to find what the Var refers to;
         * if not, we can use get_expr_result_tupdesc().
         */
        if !IsA!(var, T_Var) || (*var).vartype != RECORDOID {
            tupdesc = get_expr_result_tupdesc(var as *mut Node, false);
            /* Got the tupdesc, so we can extract the field name */
            debug_assert!(fieldno >= 1 && fieldno <= (*tupdesc).natts);
            return NameStr!((*TupleDescAttr(tupdesc, fieldno - 1)).attname);
        }

        /* Find appropriate nesting depth */
        netlevelsup = (*var).varlevelsup as i32 + levelsup;
        if netlevelsup >= list_length((*context).namespaces) {
            elog!(ERROR, "bogus varlevelsup: {} offset {}", (*var).varlevelsup, levelsup);
        }
        dpns = list_nth((*context).namespaces, netlevelsup) as *mut deparse_namespace;

        /*
         * If we have a syntactic referent for the Var, and we're working from a
         * parse tree, prefer to use the syntactic referent.
         */
        if (*var).varnosyn > 0 && (*dpns).plan.is_null() {
            varno = (*var).varnosyn as i32;
            varattno = (*var).varattnosyn;
        } else {
            varno = (*var).varno as i32;
            varattno = (*var).varattno;
        }

        /*
         * Try to find the relevant RTE in this rtable.
         */
        if varno >= 1 && varno <= list_length((*dpns).rtable) {
            rte = rt_fetch(varno, (*dpns).rtable);
            attnum = varattno;
        } else if varno == OUTER_VAR as i32 && !(*dpns).outer_tlist.is_null() {
            let tle: *mut TargetEntry;
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let result: *const ::std::os::raw::c_char;

            tle = get_tle_by_resno((*dpns).outer_tlist, varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for OUTER_VAR var: {}", varattno);
            }
            debug_assert!(netlevelsup == 0);
            push_child_plan(dpns, (*dpns).outer_plan, &mut save_dpns);
            result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
            pop_child_plan(dpns, &mut save_dpns);
            return result;
        } else if varno == INNER_VAR as i32 && !(*dpns).inner_tlist.is_null() {
            let tle: *mut TargetEntry;
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let result: *const ::std::os::raw::c_char;

            tle = get_tle_by_resno((*dpns).inner_tlist, varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INNER_VAR var: {}", varattno);
            }
            debug_assert!(netlevelsup == 0);
            push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns);
            result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
            pop_child_plan(dpns, &mut save_dpns);
            return result;
        } else if varno == INDEX_VAR as i32 && !(*dpns).index_tlist.is_null() {
            let tle: *mut TargetEntry;
            let result: *const ::std::os::raw::c_char;

            tle = get_tle_by_resno((*dpns).index_tlist, varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INDEX_VAR var: {}", varattno);
            }
            debug_assert!(netlevelsup == 0);
            result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
            return result;
        } else {
            elog!(ERROR, "bogus varno: {}", varno);
            return ::std::ptr::null(); // keep compiler quiet
        }

        if attnum == InvalidAttrNumber {
            /* Var is whole-row reference to RTE, so select the right field */
            return get_rte_attribute_name(rte, fieldno as AttrNumber);
        }

        /*
         * This part has essentially the same logic as the parser's
         * expandRecordVariable() function.
         */
        expr = var as *mut Node; // default if we can't drill down

        match (*rte).rtekind {
            RTE_RELATION | RTE_VALUES | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
                /*
                 * This case should not occur: a column of a table, values list,
                 * or ENR shouldn't have type RECORD.  Fall through and fail (most
                 * likely) at the bottom.
                 */
            }
            RTE_SUBQUERY => {
                /* Subselect-in-FROM: examine sub-select's output expr */
                if !(*rte).subquery.is_null() {
                    let ste = get_tle_by_resno((*(*rte).subquery).targetList, attnum);
                    if ste.is_null() || (*ste).resjunk {
                        elog!(ERROR, "subquery {} does not have attribute {}",
                            ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(), attnum);
                    }
                    expr = (*ste).expr as *mut Node;
                    if IsA!(expr, T_Var) {
                        /*
                         * Recurse into the sub-select to see what its Var refers to.
                         */
                        let save_nslist = (*context).namespaces;
                        let parent_namespaces = list_copy_tail((*context).namespaces, netlevelsup);
                        let mut mydpns: deparse_namespace = ::std::mem::zeroed();
                        let result: *const ::std::os::raw::c_char;

                        set_deparse_for_query(&mut mydpns, (*rte).subquery, parent_namespaces);
                        (*context).namespaces = lcons(&mut mydpns as *mut _ as *mut _, parent_namespaces);
                        result = get_name_for_var_field(expr as *mut Var, fieldno, 0, context);
                        (*context).namespaces = save_nslist;
                        return result;
                    }
                    /* else fall through to inspect the expression */
                } else {
                    /*
                     * We're deparsing a Plan tree so we don't have complete
                     * RTE entries (in particular, rte->subquery is NULL).
                     */
                    if (*dpns).inner_plan.is_null() {
                        let dummy_name = palloc(32) as *mut ::std::os::raw::c_char;
                        debug_assert!(!(*dpns).plan.is_null() && IsA!((*dpns).plan, T_Result));
                        libc::snprintf(dummy_name, 32, b"f%d\0".as_ptr() as _, fieldno);
                        return dummy_name;
                    }
                    debug_assert!(!(*dpns).plan.is_null() && IsA!((*dpns).plan, T_SubqueryScan));

                    let tle = get_tle_by_resno((*dpns).inner_tlist, attnum);
                    if tle.is_null() {
                        elog!(ERROR, "bogus varattno for subquery var: {}", attnum);
                    }
                    debug_assert!(netlevelsup == 0);
                    let mut save_dpns2: deparse_namespace = ::std::mem::zeroed();
                    push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns2);
                    let result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
                    pop_child_plan(dpns, &mut save_dpns2);
                    return result;
                }
            }
            RTE_JOIN => {
                /* Join RTE --- recursively inspect the alias variable */
                if (*rte).joinaliasvars.is_null() {
                    elog!(ERROR, "cannot decompile join alias var in plan tree");
                }
                debug_assert!(attnum > 0 && attnum <= list_length((*rte).joinaliasvars) as AttrNumber);
                expr = list_nth((*rte).joinaliasvars, attnum as i32 - 1) as *mut Node;
                debug_assert!(!expr.is_null());
                /* we intentionally don't strip implicit coercions here */
                if IsA!(expr, T_Var) {
                    return get_name_for_var_field(expr as *mut Var, fieldno,
                        (*var).varlevelsup as i32 + levelsup, context);
                }
                /* else fall through to inspect the expression */
            }
            RTE_FUNCTION | RTE_TABLEFUNC => {
                /*
                 * We couldn't get here unless a function is declared with one of
                 * its result columns as RECORD, which is not allowed.
                 */
            }
            RTE_CTE => {
                /* CTE reference: examine subquery's output expr */
                let mut cte: *mut CommonTableExpr = ::std::ptr::null_mut();
                let ctelevelsup: u32 = (*rte).ctelevelsup + netlevelsup as u32;
                let mut lc_cte: *mut ListCell = ::std::ptr::null_mut();

                if ctelevelsup >= list_length((*context).namespaces) as u32 {
                    lc_cte = ::std::ptr::null_mut();
                } else {
                    let ctedpns = list_nth((*context).namespaces, ctelevelsup as i32) as *mut deparse_namespace;
                    foreach!(lc_cte, (*ctedpns).ctes, {
                        cte = lfirst(crate::current_cell!(lc_cte)) as *mut CommonTableExpr;
                        if libc::strcmp((*cte).ctename, (*rte).ctename) == 0 { break; }
                    });
                }

                if !lc_cte.is_null() {
                    let ctequery = (*cte).ctequery as *mut Query;
                    let ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
                    if ste.is_null() || (*ste).resjunk {
                        elog!(ERROR, "CTE {} does not have attribute {}",
                            ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(), attnum);
                    }
                    expr = (*ste).expr as *mut Node;
                    if IsA!(expr, T_Var) {
                        let save_nslist = (*context).namespaces;
                        let parent_namespaces = list_copy_tail((*context).namespaces, ctelevelsup as i32);
                        let mut mydpns: deparse_namespace = ::std::mem::zeroed();
                        let result: *const ::std::os::raw::c_char;

                        set_deparse_for_query(&mut mydpns, ctequery, parent_namespaces);
                        (*context).namespaces = lcons(&mut mydpns as *mut _ as *mut _, parent_namespaces);
                        result = get_name_for_var_field(expr as *mut Var, fieldno, 0, context);
                        (*context).namespaces = save_nslist;
                        return result;
                    }
                    /* else fall through to inspect the expression */
                } else {
                    /*
                     * We're deparsing a Plan tree so we don't have a CTE list.
                     */
                    if (*dpns).inner_plan.is_null() {
                        let dummy_name = palloc(32) as *mut ::std::os::raw::c_char;
                        debug_assert!(!(*dpns).plan.is_null() && IsA!((*dpns).plan, T_Result));
                        libc::snprintf(dummy_name, 32, b"f%d\0".as_ptr() as _, fieldno);
                        return dummy_name;
                    }
                    debug_assert!(!(*dpns).plan.is_null()
                        && (IsA!((*dpns).plan, T_CteScan) || IsA!((*dpns).plan, T_WorkTableScan)));

                    let tle = get_tle_by_resno((*dpns).inner_tlist, attnum);
                    if tle.is_null() {
                        elog!(ERROR, "bogus varattno for subquery var: {}", attnum);
                    }
                    debug_assert!(netlevelsup == 0);
                    let mut save_dpns2: deparse_namespace = ::std::mem::zeroed();
                    push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns2);
                    let result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
                    pop_child_plan(dpns, &mut save_dpns2);
                    return result;
                }
            }
            RTE_GROUP => {
                /*
                 * We couldn't get here: any Vars that reference the RTE_GROUP RTE
                 * should have been replaced with the underlying grouping
                 * expressions.
                 */
            }
            _ => {}
        }

        /*
         * We now have an expression we can't expand any more, so see if
         * get_expr_result_tupdesc() can do anything with it.
         */
        tupdesc = get_expr_result_tupdesc(expr, false);
        /* Got the tupdesc, so we can extract the field name */
        debug_assert!(fieldno >= 1 && fieldno <= (*tupdesc).natts);
        NameStr!((*TupleDescAttr(tupdesc, fieldno - 1)).attname)
    }
}

/*
 * Try to find the referenced expression for a PARAM_EXEC Param that might
 * reference a parameter supplied by an upper NestLoop or SubPlan plan node.
 *
 * If successful, return the expression and set *dpns_p and *ancestor_cell_p
 * appropriately for calling push_ancestor_plan().  If no referent can be
 * found, return NULL.
 */
fn find_param_referent(
    param: *mut Param,
    context: *mut deparse_context,
    dpns_p: *mut *mut deparse_namespace,
    ancestor_cell_p: *mut *mut ListCell,
) -> *mut Node {
    unsafe {
        /* Initialize output parameters to prevent compiler warnings */
        *dpns_p = ::std::ptr::null_mut();
        *ancestor_cell_p = ::std::ptr::null_mut();

        /*
         * If it's a PARAM_EXEC parameter, look for a matching NestLoopParam or
         * SubPlan argument.
         */
        if (*param).paramkind == PARAM_EXEC {
            let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
            let mut child_plan = (*dpns).plan;
            let mut lc: *mut ListCell = ::std::ptr::null_mut();

            foreach!(lc, (*dpns).ancestors, {
                let ancestor = lfirst(crate::current_cell!(lc)) as *mut Node;
                let mut lc2: *mut ListCell = ::std::ptr::null_mut();

                /*
                 * NestLoops transmit params to their inner child only.
                 */
                if IsA!(ancestor, T_NestLoop)
                    && child_plan == innerPlan!(ancestor)
                {
                    let nl = ancestor as *mut NestLoop;
                    foreach!(lc2, (*nl).nestParams, {
                        let nlp = lfirst(crate::current_cell!(lc2)) as *mut NestLoopParam;
                        if (*nlp).paramno == (*param).paramid {
                            /* Found a match, so return it */
                            *dpns_p = dpns;
                            *ancestor_cell_p = lc;
                            return (*nlp).paramval as *mut Node;
                        }
                    });
                }

                /*
                 * If ancestor is a SubPlan, check the arguments it provides.
                 */
                if IsA!(ancestor, T_SubPlan) {
                    let subplan = ancestor as *mut SubPlan;
                    let mut lc3: *mut ListCell = ::std::ptr::null_mut();
                    let mut lc4: *mut ListCell = ::std::ptr::null_mut();

                    forboth!(lc3, (*subplan).parParam, lc4, (*subplan).args, {
                        let paramid = lfirst_int!(lc3);
                        let arg = lfirst(crate::current_cell!(lc4)) as *mut Node;

                        if paramid == (*param).paramid {
                            /*
                             * Found a match, so return it.  But, since Vars in
                             * the arg are to be evaluated in the surrounding
                             * context, we have to point to the next ancestor item
                             * that is *not* a SubPlan.
                             */
                            let mut rest: *mut ListCell = ::std::ptr::null_mut();
                            for_each_cell!(rest, (*dpns).ancestors,
                                lnext((*dpns).ancestors, lc), {
                                let ancestor2 = lfirst(crate::current_cell!(rest)) as *mut Node;
                                if !IsA!(ancestor2, T_SubPlan) {
                                    *dpns_p = dpns;
                                    *ancestor_cell_p = rest;
                                    return arg;
                                }
                            });
                            elog!(ERROR, "SubPlan cannot be outermost ancestor");
                        }
                    });

                    /* SubPlan isn't a kind of Plan, so skip the rest */
                    continue;
                }

                /*
                 * We need not consider the ancestor's initPlan list, since
                 * initplans never have any parParams.
                 */

                /* No luck, crawl up to next ancestor */
                child_plan = ancestor as *mut Plan;
            });
        }

        /* No referent found */
        ::std::ptr::null_mut()
    }
}

/*
 * Try to find a subplan/initplan that emits the value for a PARAM_EXEC Param.
 *
 * If successful, return the generating subplan/initplan and set *column_p
 * to the subplan's 0-based output column number.
 * Otherwise, return NULL.
 */
fn find_param_generator(
    param: *mut Param,
    context: *mut deparse_context,
    column_p: *mut i32,
) -> *mut SubPlan {
    unsafe {
        /* Initialize output parameter to prevent compiler warnings */
        *column_p = 0;

        /*
         * If it's a PARAM_EXEC parameter, search the current plan node as well as
         * ancestor nodes looking for a subplan or initplan that emits the value.
         */
        if (*param).paramkind == PARAM_EXEC {
            let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
            let mut lc: *mut ListCell = ::std::ptr::null_mut();

            /* First check the innermost plan node's initplans */
            let result = find_param_generator_initplan(param, (*dpns).plan, column_p);
            if !result.is_null() { return result; }

            /*
             * The plan's targetlist might contain MULTIEXPR_SUBLINK SubPlans.
             */
            let mut lc_tle: *mut ListCell = ::std::ptr::null_mut();
            foreach!(lc_tle, (*(*dpns).plan).targetlist, {
                let tle = lfirst(crate::current_cell!(lc_tle)) as *mut TargetEntry;
                if !(*tle).expr.is_null() && IsA!((*tle).expr, T_SubPlan) {
                    let subplan = (*tle).expr as *mut SubPlan;
                    if (*subplan).subLinkType == MULTIEXPR_SUBLINK {
                        let mut idx = 0i32;
                        let mut lc_p: *mut ListCell = ::std::ptr::null_mut();
                        foreach!(lc_p, (*subplan).setParam, {
                            let paramid = lfirst_int!(lc_p);
                            if paramid == (*param).paramid {
                                /* Found a match, so return it. */
                                *column_p = idx;
                                return subplan;
                            }
                            idx += 1;
                        });
                    }
                }
            });

            /* No luck, so check the ancestor nodes */
            foreach!(lc, (*dpns).ancestors, {
                let ancestor = lfirst(crate::current_cell!(lc)) as *mut Node;

                /*
                 * If ancestor is a SubPlan, check the paramIds it provides.
                 */
                if IsA!(ancestor, T_SubPlan) {
                    let subplan = ancestor as *mut SubPlan;
                    let mut idx = 0i32;
                    let mut lc_p: *mut ListCell = ::std::ptr::null_mut();
                    foreach!(lc_p, (*subplan).paramIds, {
                        let paramid = lfirst_int!(lc_p);
                        if paramid == (*param).paramid {
                            /* Found a match, so return it. */
                            *column_p = idx;
                            return subplan;
                        }
                        idx += 1;
                    });

                    /* SubPlan isn't a kind of Plan, so skip the rest */
                    continue;
                }

                /*
                 * Otherwise, it's some kind of Plan node, so check its initplans.
                 */
                let result2 = find_param_generator_initplan(param, ancestor as *mut Plan, column_p);
                if !result2.is_null() { return result2; }

                /* No luck, crawl up to next ancestor */
            });
        }

        /* No generator found */
        ::std::ptr::null_mut()
    }
}

// Subroutine for find_param_generator: search one Plan node's initplans
fn find_param_generator_initplan(param: *mut Param, plan: *mut Plan, column_p: *mut i32) -> *mut SubPlan {
    unsafe {
        let mut lc_sp: *mut ListCell = ::std::ptr::null_mut();
        foreach!(lc_sp, (*plan).initPlan, {
            let subplan = lfirst(crate::current_cell!(lc_sp)) as *mut SubPlan;
            let mut idx = 0i32;
            let mut lc_p: *mut ListCell = ::std::ptr::null_mut();
            foreach!(lc_p, (*subplan).setParam, {
                let paramid = lfirst_int!(lc_p);
                if paramid == (*param).paramid {
                    /* Found a match, so return it. */
                    *column_p = idx;
                    return subplan;
                }
                idx += 1;
            });
        });
        ::std::ptr::null_mut()
    }
}

// Display a Param appropriately.
fn get_parameter(param: *mut Param, context: *mut deparse_context) {
    unsafe {
        let mut dpns: *mut deparse_namespace = ::std::ptr::null_mut();
        let mut ancestor_cell: *mut ListCell = ::std::ptr::null_mut();
        let mut column: i32 = 0;

        /*
         * If it's a PARAM_EXEC parameter, try to locate the expression from which
         * the parameter was computed.
         */
        let expr = find_param_referent(param, context, &mut dpns, &mut ancestor_cell);
        if !expr.is_null() {
            /* Found a match, so print it */
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let save_varprefix: bool;
            let need_paren: bool;

            /* Switch attention to the ancestor plan node */
            push_ancestor_plan(dpns, ancestor_cell, &mut save_dpns);

            /*
             * Force prefixing of Vars, since they won't belong to the relation
             * being scanned in the original plan node.
             */
            save_varprefix = (*context).varprefix;
            (*context).varprefix = true;

            /*
             * A Param's expansion is typically a Var, Aggref, GroupingFunc, or
             * upper-level Param, which wouldn't need extra parentheses.
             */
            need_paren = !(IsA!(expr, T_Var)
                || IsA!(expr, T_Aggref)
                || IsA!(expr, T_GroupingFunc)
                || IsA!(expr, T_Param));
            if need_paren { appendStringInfoChar((*context).buf, b'(' as _); }

            get_rule_expr(expr, context, false);

            if need_paren { appendStringInfoChar((*context).buf, b')' as _); }

            (*context).varprefix = save_varprefix;

            pop_ancestor_plan(dpns, &mut save_dpns);

            return;
        }

        /*
         * Alternatively, maybe it's a subplan output.
         */
        let subplan = find_param_generator(param, context, &mut column);
        if !subplan.is_null() {
            let hashstr = if (*subplan).useHashTable { "hashed " } else { "" };
            appendStringInfo!((*context).buf, "({}{}).col{}",
                hashstr,
                ::std::ffi::CStr::from_ptr((*subplan).plan_name).to_string_lossy(),
                column + 1);
            return;
        }

        /*
         * If it's an external parameter, see if the outermost namespace provides
         * function argument names.
         */
        if (*param).paramkind == PARAM_EXTERN && !(*context).namespaces.is_null() {
            dpns = llast!((*context).namespaces) as *mut deparse_namespace;
            if !(*dpns).argnames.is_null()
                && (*param).paramid > 0
                && (*param).paramid <= (*dpns).numargs
            {
                let argname = *(*dpns).argnames.add((*param).paramid as usize - 1);
                if !argname.is_null() {
                    let mut should_qualify = false;
                    let mut lc: *mut ListCell = ::std::ptr::null_mut();

                    /*
                     * Qualify the parameter name if there are any other deparse
                     * namespaces with range tables.
                     */
                    foreach!(lc, (*context).namespaces, {
                        let depns = lfirst(crate::current_cell!(lc)) as *mut deparse_namespace;
                        if !(*depns).rtable_names.is_null() {
                            should_qualify = true;
                            break;
                        }
                    });
                    if should_qualify {
                        appendStringInfoString((*context).buf, quote_identifier((*dpns).funcname));
                        appendStringInfoChar((*context).buf, b'.' as _);
                    }

                    appendStringInfoString((*context).buf, quote_identifier(argname));
                    return;
                }
            }
        }

        /*
         * Not PARAM_EXEC, or couldn't find referent: just print $N.
         *
         * It's a bug if we get here for anything except PARAM_EXTERN Params, but
         * in production builds printing $N seems more useful than failing.
         */
        debug_assert!((*param).paramkind == PARAM_EXTERN);
        appendStringInfo!((*context).buf, "${}", (*param).paramid);
    }
}

/*
 * get_simple_binary_op_name
 *
 * helper function for isSimpleNode
 * will return single char binary operator name, or NULL if it's not
 */
fn get_simple_binary_op_name(expr: *mut OpExpr) -> *const ::std::os::raw::c_char {
    unsafe {
        let args = (*expr).args;
        if list_length(args) == 2 {
            /* binary operator */
            let arg1 = linitial!(args) as *mut Node;
            let arg2 = lsecond!(args) as *mut Node;
            let op = generate_operator_name((*expr).opno, exprType(arg1), exprType(arg2));
            if !op.is_null() && libc::strlen(op) == 1 {
                return op;
            }
        }
        ::std::ptr::null()
    }
}

/*
 * isSimpleNode - check if given node is simple (doesn't need parenthesizing)
 *
 *  true   : simple in the context of parent node's type
 *  false  : not simple
 */
fn isSimpleNode(node: *mut Node, parent_node: *mut Node, pretty_flags: i32) -> bool {
    unsafe {
        if node.is_null() { return false; }

        match nodeTag!(node) {
            T_Var | T_Const | T_Param | T_CoerceToDomainValue | T_SetToDefault | T_CurrentOfExpr => {
                /* single words: always simple */
                true
            }
            T_SubscriptingRef
            | T_ArrayExpr
            | T_RowExpr
            | T_CoalesceExpr
            | T_MinMaxExpr
            | T_SQLValueFunction
            | T_XmlExpr
            | T_NextValueExpr
            | T_NullIfExpr
            | T_Aggref
            | T_GroupingFunc
            | T_WindowFunc
            | T_MergeSupportFunc
            | T_FuncExpr
            | T_JsonConstructorExpr
            | T_JsonExpr => {
                /* function-like: name(..) or name[..] */
                true
            }
            /* CASE keywords act as parentheses */
            T_CaseExpr => true,

            T_FieldSelect => {
                /*
                 * appears simple since . has top precedence, unless parent is
                 * T_FieldSelect itself!
                 */
                !IsA!(parent_node, T_FieldSelect)
            }
            T_FieldStore => {
                /* treat like FieldSelect (probably doesn't matter) */
                !IsA!(parent_node, T_FieldStore)
            }
            T_CoerceToDomain => {
                /* maybe simple, check args */
                isSimpleNode((*(node as *mut CoerceToDomain)).arg as *mut Node, node, pretty_flags)
            }
            T_RelabelType => {
                isSimpleNode((*(node as *mut RelabelType)).arg as *mut Node, node, pretty_flags)
            }
            T_CoerceViaIO => {
                isSimpleNode((*(node as *mut CoerceViaIO)).arg as *mut Node, node, pretty_flags)
            }
            T_ArrayCoerceExpr => {
                isSimpleNode((*(node as *mut ArrayCoerceExpr)).arg as *mut Node, node, pretty_flags)
            }
            T_ConvertRowtypeExpr => {
                isSimpleNode((*(node as *mut ConvertRowtypeExpr)).arg as *mut Node, node, pretty_flags)
            }
            T_ReturningExpr => {
                isSimpleNode((*(node as *mut ReturningExpr)).retexpr as *mut Node, node, pretty_flags)
            }
            T_OpExpr => {
                /* depends on parent node type; needs further checking */
                if (pretty_flags & PRETTYFLAG_PAREN) != 0 && IsA!(parent_node, T_OpExpr) {
                    let op = get_simple_binary_op_name(node as *mut OpExpr);
                    if op.is_null() { return false; }

                    /* We know only the basic operators + - and * / % */
                    let oc = *op as u8;
                    let is_lopriop = oc == b'+' || oc == b'-';
                    let is_hipriop = oc == b'*' || oc == b'/' || oc == b'%';
                    if !(is_lopriop || is_hipriop) { return false; }

                    let parent_op = get_simple_binary_op_name(parent_node as *mut OpExpr);
                    if parent_op.is_null() { return false; }

                    let poc = *parent_op as u8;
                    let is_lopriparent = poc == b'+' || poc == b'-';
                    let is_hipriparent = poc == b'*' || poc == b'/' || poc == b'%';
                    if !(is_lopriparent || is_hipriparent) { return false; }

                    if is_hipriop && is_lopriparent { return true; } // op binds tighter than parent
                    if is_lopriop && is_hipriparent { return false; }

                    /*
                     * Operators are same priority --- can skip parens only if
                     * we have (a - b) - c, not a - (b - c).
                     */
                    if node == linitial!((*(parent_node as *mut OpExpr)).args) as *mut Node {
                        return true;
                    }
                    return false;
                }
                /* else do the same stuff as for T_SubLink et al. */
                // fall through
                match nodeTag!(parent_node) {
                    T_FuncExpr => {
                        let r#type = (*(parent_node as *mut FuncExpr)).funcformat;
                        if r#type == COERCE_EXPLICIT_CAST || r#type == COERCE_IMPLICIT_CAST || r#type == COERCE_SQL_SYNTAX {
                            false
                        } else {
                            true // own parentheses
                        }
                    }
                    T_BoolExpr      // lower precedence
                    | T_SubscriptingRef // other separators
                    | T_ArrayExpr   // other separators
                    | T_RowExpr     // other separators
                    | T_CoalesceExpr  // own parentheses
                    | T_MinMaxExpr  // own parentheses
                    | T_XmlExpr     // own parentheses
                    | T_NullIfExpr  // other separators
                    | T_Aggref      // own parentheses
                    | T_GroupingFunc // own parentheses
                    | T_WindowFunc  // own parentheses
                    | T_CaseExpr => true, // other separators
                    _ => false,
                }
            }
            T_SubLink | T_NullTest | T_BooleanTest | T_DistinctExpr | T_JsonIsPredicate => {
                match nodeTag!(parent_node) {
                    T_FuncExpr => {
                        let r#type = (*(parent_node as *mut FuncExpr)).funcformat;
                        if r#type == COERCE_EXPLICIT_CAST || r#type == COERCE_IMPLICIT_CAST || r#type == COERCE_SQL_SYNTAX {
                            false
                        } else {
                            true // own parentheses
                        }
                    }
                    T_BoolExpr      // lower precedence
                    | T_SubscriptingRef // other separators
                    | T_ArrayExpr   // other separators
                    | T_RowExpr     // other separators
                    | T_CoalesceExpr  // own parentheses
                    | T_MinMaxExpr  // own parentheses
                    | T_XmlExpr     // own parentheses
                    | T_NullIfExpr  // other separators
                    | T_Aggref      // own parentheses
                    | T_GroupingFunc // own parentheses
                    | T_WindowFunc  // own parentheses
                    | T_CaseExpr => true, // other separators
                    _ => false,
                }
            }
            T_BoolExpr => {
                match nodeTag!(parent_node) {
                    T_BoolExpr => {
                        if (pretty_flags & PRETTYFLAG_PAREN) != 0 {
                            let r#type = (*(node as *mut BoolExpr)).boolop;
                            let parent_type = (*(parent_node as *mut BoolExpr)).boolop;
                            match r#type {
                                NOT_EXPR | AND_EXPR => {
                                    if parent_type == AND_EXPR || parent_type == OR_EXPR { return true; }
                                }
                                OR_EXPR => {
                                    if parent_type == OR_EXPR { return true; }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    T_FuncExpr => {
                        let r#type = (*(parent_node as *mut FuncExpr)).funcformat;
                        if r#type == COERCE_EXPLICIT_CAST || r#type == COERCE_IMPLICIT_CAST || r#type == COERCE_SQL_SYNTAX {
                            false
                        } else {
                            true // own parentheses
                        }
                    }
                    T_SubscriptingRef // other separators
                    | T_ArrayExpr   // other separators
                    | T_RowExpr     // other separators
                    | T_CoalesceExpr  // own parentheses
                    | T_MinMaxExpr  // own parentheses
                    | T_XmlExpr     // own parentheses
                    | T_NullIfExpr  // other separators
                    | T_Aggref      // own parentheses
                    | T_GroupingFunc // own parentheses
                    | T_WindowFunc  // own parentheses
                    | T_CaseExpr    // other separators
                    | T_JsonExpr => true, // own parentheses
                    _ => false,
                }
            }
            T_JsonValueExpr => {
                /* maybe simple, check args */
                isSimpleNode((*(node as *mut JsonValueExpr)).raw_expr as *mut Node, node, pretty_flags)
            }
            _ => {
                false // those we don't know: in dubio complexo
            }
        }
    }
}

/*
 * appendContextKeyword - append a keyword to buffer
 *
 * If prettyPrint is enabled, perform a line break, and adjust indentation.
 * Otherwise, just append the keyword.
 */
fn appendContextKeyword(
    context: *mut deparse_context,
    str: *const ::std::os::raw::c_char,
    indent_before: i32,
    indent_after: i32,
    indent_plus: i32,
) {
    unsafe {
        let buf = (*context).buf;

        if PRETTY_INDENT(context) {
            let indent_amount: i32;

            (*context).indentLevel += indent_before;

            /* remove any trailing spaces currently in the buffer ... */
            removeStringInfoSpaces(buf);
            /* ... then add a newline and some spaces */
            appendStringInfoChar(buf, b'\n' as _);

            if (*context).indentLevel < PRETTYINDENT_LIMIT as i32 {
                indent_amount = ::std::cmp::max((*context).indentLevel, 0) + indent_plus;
            } else {
                /*
                 * If we're indented more than PRETTYINDENT_LIMIT characters, try
                 * to conserve horizontal space by reducing the per-level
                 * indentation.
                 */
                let mut ia = PRETTYINDENT_LIMIT as i32
                    + ((*context).indentLevel - PRETTYINDENT_LIMIT as i32)
                        / (PRETTYINDENT_STD as i32 / 2);
                ia %= PRETTYINDENT_LIMIT as i32;
                /* scale/wrap logic affects indentLevel, but not indentPlus */
                ia += indent_plus;
                indent_amount = ia;
            }
            appendStringInfoSpaces(buf, indent_amount as u32);

            appendStringInfoString(buf, str);

            (*context).indentLevel += indent_after;
            if (*context).indentLevel < 0 {
                (*context).indentLevel = 0;
            }
        } else {
            appendStringInfoString(buf, str);
        }
    }
}

/*
 * removeStringInfoSpaces - delete trailing spaces from a buffer.
 *
 * Possibly this should move to stringinfo.c at some point.
 */
fn removeStringInfoSpaces(str: *mut StringInfo) {
    unsafe {
        while (*str).len > 0 && *(*str).data.add((*str).len as usize - 1) == b' ' as _ {
            (*str).len -= 1;
            *(*str).data.add((*str).len as usize) = b'\0' as _;
        }
    }
}

/*
 * get_rule_expr_paren - deparse expr using get_rule_expr,
 * embracing the string with parentheses if necessary for prettyPrint.
 *
 * Never embrace if prettyFlags=0, because it's done in the calling node.
 *
 * Any node that does *not* embrace its argument node by sql syntax should
 * use get_rule_expr_paren instead of get_rule_expr so parentheses can be
 * added.
 */
fn get_rule_expr_paren(
    node: *mut Node,
    context: *mut deparse_context,
    showimplicit: bool,
    parent_node: *mut Node,
) {
    unsafe {
        let need_paren = PRETTY_PAREN(context)
            && !isSimpleNode(node, parent_node, (*context).prettyFlags);

        if need_paren { appendStringInfoChar((*context).buf, b'(' as _); }
        get_rule_expr(node, context, showimplicit);
        if need_paren { appendStringInfoChar((*context).buf, b')' as _); }
    }
}

fn get_json_behavior(behavior: *mut JsonBehavior, context: *mut deparse_context, on: *const ::std::os::raw::c_char) {
    unsafe {
        /*
         * The order of array elements must correspond to the order of
         * JsonBehaviorType members.
         */
        let behavior_names: [&[u8]; 9] = [
            b" NULL\0",
            b" ERROR\0",
            b" EMPTY\0",
            b" TRUE\0",
            b" FALSE\0",
            b" UNKNOWN\0",
            b" EMPTY ARRAY\0",
            b" EMPTY OBJECT\0",
            b" DEFAULT \0",
        ];

        if ((*behavior).btype as usize) >= behavior_names.len() {
            elog!(ERROR, "invalid json behavior type: {}", (*behavior).btype as i32);
        }

        appendStringInfoString((*context).buf, behavior_names[(*behavior).btype as usize].as_ptr() as _);

        if (*behavior).btype == JSON_BEHAVIOR_DEFAULT {
            get_rule_expr((*behavior).expr, context, false);
        }

        appendStringInfo!((*context).buf, " ON {}",
            ::std::ffi::CStr::from_ptr(on).to_string_lossy());
    }
}

/*
 * get_json_expr_options
 *
 * Parse back common options for JSON_QUERY, JSON_VALUE, JSON_EXISTS and
 * JSON_TABLE columns.
 */
fn get_json_expr_options(
    jsexpr: *mut JsonExpr,
    context: *mut deparse_context,
    default_behavior: JsonBehaviorType,
) {
    unsafe {
        if (*jsexpr).op == JSON_QUERY_OP {
            if (*jsexpr).wrapper == JSW_CONDITIONAL {
                appendStringInfoString((*context).buf, b" WITH CONDITIONAL WRAPPER\0".as_ptr() as _);
            } else if (*jsexpr).wrapper == JSW_UNCONDITIONAL {
                appendStringInfoString((*context).buf, b" WITH UNCONDITIONAL WRAPPER\0".as_ptr() as _);
            } else if (*jsexpr).wrapper == JSW_NONE || (*jsexpr).wrapper == JSW_UNSPEC {
                /* The default */
                appendStringInfoString((*context).buf, b" WITHOUT WRAPPER\0".as_ptr() as _);
            }

            if (*jsexpr).omit_quotes {
                appendStringInfoString((*context).buf, b" OMIT QUOTES\0".as_ptr() as _);
            } else {
                /* The default */
                appendStringInfoString((*context).buf, b" KEEP QUOTES\0".as_ptr() as _);
            }
        }

        if !(*jsexpr).on_empty.is_null() && (*(*jsexpr).on_empty).btype != default_behavior {
            get_json_behavior((*jsexpr).on_empty, context, b"EMPTY\0".as_ptr() as _);
        }

        if !(*jsexpr).on_error.is_null() && (*(*jsexpr).on_error).btype != default_behavior {
            get_json_behavior((*jsexpr).on_error, context, b"ERROR\0".as_ptr() as _);
        }
    }
}

/*
 * get_rule_expr           - Parse back an expression
 *
 * Note: showimplicit determines whether we display any implicit cast that
 * is present at the top of the expression tree.  It is a passed argument,
 * not a field of the context struct, because we change the value as we
 * recurse down into the expression.  In general we suppress implicit casts
 * when the result type is known with certainty (eg, the arguments of an
 * OR must be boolean).  We display implicit casts for arguments of functions
 * and operators, since this is needed to be certain that the same function
 * or operator will be chosen when the expression is re-parsed.
 */
fn get_rule_expr(node: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;

        if node.is_null() {
            return;
        }

        /* Guard against excessively long or deeply-nested queries */
        check_stack_depth();

        /*
         * Each level of get_rule_expr must emit an indivisible term
         * (parenthesized if necessary) to ensure result is reparsed into the same
         * expression tree.  The only exception is that when the input is a List,
         * we emit the component items comma-separated with no surrounding
         * decoration; this is convenient for most callers.
         */
        match nodeTag(node) {
            T_Var => {
                get_variable(node as *mut Var, 0, false, context);
            }

            T_Const => {
                get_const_expr(node as *mut Const, context, 0);
            }

            T_Param => {
                get_parameter(node as *mut Param, context);
            }

            T_Aggref => {
                get_agg_expr(node as *mut Aggref, context, node as *mut Aggref);
            }

            T_GroupingFunc => {
                let gexpr = node as *mut GroupingFunc;
                appendStringInfoString(buf, b"GROUPING(\0".as_ptr() as _);
                get_rule_expr((*gexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_WindowFunc => {
                get_windowfunc_expr(node as *mut WindowFunc, context);
            }

            T_MergeSupportFunc => {
                appendStringInfoString(buf, b"MERGE_ACTION()\0".as_ptr() as _);
            }

            T_SubscriptingRef => {
                let sbsref = node as *mut SubscriptingRef;
                /*
                 * If the argument is a CaseTestExpr, we must be inside a
                 * FieldStore, ie, we are assigning to an element of an array
                 * within a composite column.  Since we already punted on
                 * displaying the FieldStore's target information, just punt
                 * here too, and display only the assignment source expression.
                 */
                if IsA!((*sbsref).refexpr, CaseTestExpr) {
                    assert!(!(*sbsref).refassgnexpr.is_null());
                    get_rule_expr((*sbsref).refassgnexpr as *mut Node, context, showimplicit);
                } else {
                    /*
                     * Parenthesize the argument unless it's a simple Var or a
                     * FieldSelect.  (In particular, if it's another
                     * SubscriptingRef, we *must* parenthesize to avoid confusion.)
                     */
                    let need_parens = !IsA!((*sbsref).refexpr, Var) &&
                        !IsA!((*sbsref).refexpr, FieldSelect);
                    if need_parens { appendStringInfoChar(buf, b'(' as _); }
                    get_rule_expr((*sbsref).refexpr as *mut Node, context, showimplicit);
                    if need_parens { appendStringInfoChar(buf, b')' as _); }

                    /*
                     * If there's a refassgnexpr, we want to print the node in the
                     * format "container[subscripts] := refassgnexpr".  This is
                     * not legal SQL, so decompilation of INSERT or UPDATE
                     * statements should always use processIndirection as part of
                     * the statement-level syntax.  We should only see this when
                     * EXPLAIN tries to print the targetlist of a plan resulting
                     * from such a statement.
                     */
                    if !(*sbsref).refassgnexpr.is_null() {
                        /*
                         * Use processIndirection to print this node's subscripts
                         * as well as any additional field selections or
                         * subscripting in immediate descendants.  It returns the
                         * RHS expr that is actually being "assigned".
                         */
                        let refassgnexpr = processIndirection(node, context);
                        appendStringInfoString(buf, b" := \0".as_ptr() as _);
                        get_rule_expr(refassgnexpr, context, showimplicit);
                    } else {
                        /* Just an ordinary container fetch, so print subscripts */
                        printSubscripts(sbsref, context);
                    }
                }
            }

            T_FuncExpr => {
                get_func_expr(node as *mut FuncExpr, context, showimplicit);
            }

            T_NamedArgExpr => {
                let na = node as *mut NamedArgExpr;
                appendStringInfo!(buf, "{} => ",
                    ::std::ffi::CStr::from_ptr(quote_identifier((*na).name)).to_string_lossy());
                get_rule_expr((*na).arg as *mut Node, context, showimplicit);
            }

            T_OpExpr => {
                get_oper_expr(node as *mut OpExpr, context);
            }

            T_DistinctExpr => {
                let expr = node as *mut DistinctExpr;
                let args = (*expr).args;
                let arg1 = linitial!(args) as *mut Node;
                let arg2 = lsecond!(args) as *mut Node;

                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren(arg1, context, true, node);
                appendStringInfoString(buf, b" IS DISTINCT FROM \0".as_ptr() as _);
                get_rule_expr_paren(arg2, context, true, node);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_NullIfExpr => {
                let nullifexpr = node as *mut NullIfExpr;
                appendStringInfoString(buf, b"NULLIF(\0".as_ptr() as _);
                get_rule_expr((*nullifexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_ScalarArrayOpExpr => {
                let expr = node as *mut ScalarArrayOpExpr;
                let args = (*expr).args;
                let arg1 = linitial!(args) as *mut Node;
                let arg2 = lsecond!(args) as *mut Node;

                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren(arg1, context, true, node);
                appendStringInfo!(buf, " {} {} (",
                    ::std::ffi::CStr::from_ptr(generate_operator_name((*expr).opno,
                        exprType(arg1),
                        get_base_element_type(exprType(arg2)))).to_string_lossy(),
                    if (*expr).useOr { "ANY" } else { "ALL" });
                get_rule_expr_paren(arg2, context, true, node);

                /*
                 * There's inherent ambiguity in "x op ANY/ALL (y)" when y is
                 * a bare sub-SELECT.  Since we're here, the sub-SELECT must
                 * be meant as a scalar sub-SELECT yielding an array value to
                 * be used in ScalarArrayOpExpr; but the grammar will
                 * preferentially interpret such a construct as an ANY/ALL
                 * SubLink.  To prevent misparsing the output that way, insert
                 * a dummy coercion (which will be stripped by parse analysis,
                 * so no inefficiency is added in dump and reload).  This is
                 * indeed most likely what the user wrote to get the construct
                 * accepted in the first place.
                 */
                if IsA!(arg2, SubLink) &&
                    (*(arg2 as *mut SubLink)).subLinkType == EXPR_SUBLINK
                {
                    appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                        format_type_with_typemod(exprType(arg2), exprTypmod(arg2))).to_string_lossy());
                }
                appendStringInfoChar(buf, b')' as _);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_BoolExpr => {
                let expr = node as *mut BoolExpr;
                let first_arg = linitial!((*expr).args) as *mut Node;

                match (*expr).boolop {
                    AND_EXPR => {
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                        get_rule_expr_paren(first_arg, context, false, node);
                        let mut lc = lnext!((*expr).args, list_head((*expr).args));
                        while !lc.is_null() {
                            appendStringInfoString(buf, b" AND \0".as_ptr() as _);
                            get_rule_expr_paren(crate::current_cell!(lc) as *mut Node, context, false, node);
                            lc = lnext!((*expr).args, lc);
                        }
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
                    }
                    OR_EXPR => {
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                        get_rule_expr_paren(first_arg, context, false, node);
                        let mut lc = lnext!((*expr).args, list_head((*expr).args));
                        while !lc.is_null() {
                            appendStringInfoString(buf, b" OR \0".as_ptr() as _);
                            get_rule_expr_paren(crate::current_cell!(lc) as *mut Node, context, false, node);
                            lc = lnext!((*expr).args, lc);
                        }
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
                    }
                    NOT_EXPR => {
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                        appendStringInfoString(buf, b"NOT \0".as_ptr() as _);
                        get_rule_expr_paren(first_arg, context, false, node);
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
                    }
                    _ => {
                        elog!(ERROR, "unrecognized boolop: {}", (*expr).boolop as i32);
                    }
                }
            }

            T_SubLink => {
                get_sublink_expr(node as *mut SubLink, context);
            }

            T_SubPlan => {
                let subplan = node as *mut SubPlan;
                /*
                 * We cannot see an already-planned subplan in rule deparsing,
                 * only while EXPLAINing a query plan.  We don't try to
                 * reconstruct the original SQL, just reference the subplan
                 * that appears elsewhere in EXPLAIN's result.  It does seem
                 * useful to show the subLinkType and testexpr (if any), and
                 * we also note whether the subplan will be hashed.
                 */
                match (*subplan).subLinkType {
                    EXISTS_SUBLINK => {
                        appendStringInfoString(buf, b"EXISTS(\0".as_ptr() as _);
                    }
                    ALL_SUBLINK => {
                        appendStringInfoString(buf, b"(ALL \0".as_ptr() as _);
                    }
                    ANY_SUBLINK => {
                        appendStringInfoString(buf, b"(ANY \0".as_ptr() as _);
                    }
                    ROWCOMPARE_SUBLINK => {
                        /* Parenthesizing the testexpr seems sufficient */
                        appendStringInfoChar(buf, b'(' as _);
                    }
                    EXPR_SUBLINK => {
                        /* No need to decorate these subplan references */
                        appendStringInfoChar(buf, b'(' as _);
                    }
                    MULTIEXPR_SUBLINK => {
                        /* MULTIEXPR isn't executed in the normal way */
                        appendStringInfoString(buf, b"(rescan \0".as_ptr() as _);
                    }
                    ARRAY_SUBLINK => {
                        appendStringInfoString(buf, b"ARRAY(\0".as_ptr() as _);
                    }
                    CTE_SUBLINK => {
                        /* This case is unreachable within expressions */
                        appendStringInfoString(buf, b"CTE(\0".as_ptr() as _);
                    }
                    _ => {}
                }

                if !(*subplan).testexpr.is_null() {
                    let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
                    /*
                     * Push SubPlan into ancestors list while deparsing
                     * testexpr, so that we can handle PARAM_EXEC references
                     * to the SubPlan's paramIds.  (This makes it look like
                     * the SubPlan is an "ancestor" of the current plan node,
                     * which is a little weird, but it does no harm.)  In this
                     * path, we don't need to mention the SubPlan explicitly,
                     * because the referencing Params will show its existence.
                     */
                    (*dpns).ancestors = lcons(subplan as *mut _, (*dpns).ancestors);

                    get_rule_expr((*subplan).testexpr, context, showimplicit);
                    appendStringInfoChar(buf, b')' as _);

                    (*dpns).ancestors = list_delete_first((*dpns).ancestors);
                } else {
                    /* No referencing Params, so show the SubPlan's name */
                    if (*subplan).useHashTable {
                        appendStringInfo!(buf, "hashed {})",
                            ::std::ffi::CStr::from_ptr((*subplan).plan_name).to_string_lossy());
                    } else {
                        appendStringInfo!(buf, "{})",
                            ::std::ffi::CStr::from_ptr((*subplan).plan_name).to_string_lossy());
                    }
                }
            }

            T_AlternativeSubPlan => {
                let asplan = node as *mut AlternativeSubPlan;
                /*
                 * This case cannot be reached in normal usage, since no
                 * AlternativeSubPlan can appear either in parsetrees or
                 * finished plan trees.  We keep it just in case somebody
                 * wants to use this code to print planner data structures.
                 */
                appendStringInfoString(buf, b"(alternatives: \0".as_ptr() as _);
                let mut lc = list_head((*asplan).subplans);
                while !lc.is_null() {
                    let splan = crate::current_cell!(lc) as *mut SubPlan;
                    if (*splan).useHashTable {
                        appendStringInfo!(buf, "hashed {}",
                            ::std::ffi::CStr::from_ptr((*splan).plan_name).to_string_lossy());
                    } else {
                        appendStringInfoString(buf, (*splan).plan_name);
                    }
                    if !lnext!((*asplan).subplans, lc).is_null() {
                        appendStringInfoString(buf, b" or \0".as_ptr() as _);
                    }
                    lc = lnext!((*asplan).subplans, lc);
                }
                appendStringInfoChar(buf, b')' as _);
            }

            T_FieldSelect => {
                let fselect = node as *mut FieldSelect;
                let arg = (*fselect).arg as *mut Node;
                let fno = (*fselect).fieldnum;

                /*
                 * Parenthesize the argument unless it's an SubscriptingRef or
                 * another FieldSelect.  Note in particular that it would be
                 * WRONG to not parenthesize a Var argument; simplicity is not
                 * the issue here, having the right number of names is.
                 */
                let need_parens = !IsA!(arg, SubscriptingRef) && !IsA!(arg, FieldSelect);
                if need_parens { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr(arg, context, true);
                if need_parens { appendStringInfoChar(buf, b')' as _); }

                /*
                 * Get and print the field name.
                 */
                let fieldname = get_name_for_var_field(arg as *mut Var, fno as _, 0, context);
                appendStringInfo!(buf, ".{}", ::std::ffi::CStr::from_ptr(
                    quote_identifier(fieldname)).to_string_lossy());
            }

            T_FieldStore => {
                let fstore = node as *mut FieldStore;
                /*
                 * There is no good way to represent a FieldStore as real SQL,
                 * so decompilation of INSERT or UPDATE statements should
                 * always use processIndirection as part of the
                 * statement-level syntax.  We should only get here when
                 * EXPLAIN tries to print the targetlist of a plan resulting
                 * from such a statement.  The plan case is even harder than
                 * ordinary rules would be, because the planner tries to
                 * collapse multiple assignments to the same field or subfield
                 * into one FieldStore; so we can see a list of target fields
                 * not just one, and the arguments could be FieldStores
                 * themselves.  We don't bother to try to print the target
                 * field names; we just print the source arguments, with a
                 * ROW() around them if there's more than one.  This isn't
                 * terribly complete, but it's probably good enough for
                 * EXPLAIN's purposes; especially since anything more would be
                 * either hopelessly confusing or an even poorer
                 * representation of what the plan is actually doing.
                 */
                let need_parens = list_length((*fstore).newvals) != 1;
                if need_parens { appendStringInfoString(buf, b"ROW(\0".as_ptr() as _); }
                get_rule_expr((*fstore).newvals as *mut Node, context, showimplicit);
                if need_parens { appendStringInfoChar(buf, b')' as _); }
            }

            T_RelabelType => {
                let relabel = node as *mut RelabelType;
                let arg = (*relabel).arg as *mut Node;
                if (*relabel).relabelformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context,
                        (*relabel).resulttype, (*relabel).resulttypmod, node);
                }
            }

            T_CoerceViaIO => {
                let iocoerce = node as *mut CoerceViaIO;
                let arg = (*iocoerce).arg as *mut Node;
                if (*iocoerce).coerceformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context, (*iocoerce).resulttype, -1, node);
                }
            }

            T_ArrayCoerceExpr => {
                let acoerce = node as *mut ArrayCoerceExpr;
                let arg = (*acoerce).arg as *mut Node;
                if (*acoerce).coerceformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context,
                        (*acoerce).resulttype, (*acoerce).resulttypmod, node);
                }
            }

            T_ConvertRowtypeExpr => {
                let convert = node as *mut ConvertRowtypeExpr;
                let arg = (*convert).arg as *mut Node;
                if (*convert).convertformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context, (*convert).resulttype, -1, node);
                }
            }

            T_CollateExpr => {
                let collate = node as *mut CollateExpr;
                let arg = (*collate).arg as *mut Node;
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren(arg, context, showimplicit, node);
                appendStringInfo!(buf, " COLLATE {}",
                    ::std::ffi::CStr::from_ptr(generate_collation_name((*collate).collOid)).to_string_lossy());
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_CaseExpr => {
                let caseexpr = node as *mut CaseExpr;
                appendContextKeyword(context, b"CASE\0".as_ptr() as _, 0, PRETTYINDENT_VAR, 0);
                if !(*caseexpr).arg.is_null() {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr((*caseexpr).arg as *mut Node, context, true);
                }
                let mut temp = list_head((*caseexpr).args);
                while !temp.is_null() {
                    let when = crate::current_cell!(temp) as *mut CaseWhen;
                    let mut w = (*when).expr as *mut Node;

                    if !(*caseexpr).arg.is_null() {
                        /*
                         * The parser should have produced WHEN clauses of the
                         * form "CaseTestExpr = RHS", possibly with an
                         * implicit coercion inserted above the CaseTestExpr.
                         * For accurate decompilation of rules it's essential
                         * that we show just the RHS.  However in an
                         * expression that's been through the optimizer, the
                         * WHEN clause could be almost anything (since the
                         * equality operator could have been expanded into an
                         * inline function).  If we don't recognize the form
                         * of the WHEN clause, just punt and display it as-is.
                         */
                        if IsA!(w, OpExpr) {
                            let args = (*(w as *mut OpExpr)).args;
                            if list_length(args) == 2 &&
                                IsA!(strip_implicit_coercions(linitial!(args) as *mut Node), CaseTestExpr)
                            {
                                w = lsecond!(args) as *mut Node;
                            }
                        }
                    }

                    if !PRETTY_INDENT!(context) { appendStringInfoChar(buf, b' ' as _); }
                    appendContextKeyword(context, b"WHEN \0".as_ptr() as _, 0, 0, 0);
                    get_rule_expr(w, context, false);
                    appendStringInfoString(buf, b" THEN \0".as_ptr() as _);
                    get_rule_expr((*when).result as *mut Node, context, true);
                    temp = lnext!((*caseexpr).args, temp);
                }
                if !PRETTY_INDENT!(context) { appendStringInfoChar(buf, b' ' as _); }
                appendContextKeyword(context, b"ELSE \0".as_ptr() as _, 0, 0, 0);
                get_rule_expr((*caseexpr).defresult as *mut Node, context, true);
                if !PRETTY_INDENT!(context) { appendStringInfoChar(buf, b' ' as _); }
                appendContextKeyword(context, b"END\0".as_ptr() as _, -(PRETTYINDENT_VAR as i32), 0, 0);
            }

            T_CaseTestExpr => {
                /*
                 * Normally we should never get here, since for expressions
                 * that can contain this node type we attempt to avoid
                 * recursing to it.  But in an optimized expression we might
                 * be unable to avoid that (see comments for CaseExpr).  If we
                 * do see one, print it as CASE_TEST_EXPR.
                 */
                appendStringInfoString(buf, b"CASE_TEST_EXPR\0".as_ptr() as _);
            }

            T_ArrayExpr => {
                let arrayexpr = node as *mut ArrayExpr;
                appendStringInfoString(buf, b"ARRAY[\0".as_ptr() as _);
                get_rule_expr((*arrayexpr).elements as *mut Node, context, true);
                appendStringInfoChar(buf, b']' as _);

                /*
                 * If the array isn't empty, we assume its elements are
                 * coerced to the desired type.  If it's empty, though, we
                 * need an explicit coercion to the array type.
                 */
                if (*arrayexpr).elements.is_null() {
                    appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                        format_type_with_typemod((*arrayexpr).array_typeid, -1)).to_string_lossy());
                }
            }

            T_RowExpr => {
                let rowexpr = node as *mut RowExpr;
                let mut tupdesc: TupleDesc = std::ptr::null_mut();

                /*
                 * If it's a named type and not RECORD, we may have to skip
                 * dropped columns and/or claim there are NULLs for added columns.
                 */
                if (*rowexpr).row_typeid != RECORDOID {
                    tupdesc = lookup_rowtype_tupdesc((*rowexpr).row_typeid, -1);
                    assert!(list_length((*rowexpr).args) <= (*tupdesc).natts);
                }

                /*
                 * SQL99 allows "ROW" to be omitted when there is more than
                 * one column, but for simplicity we always print it.
                 */
                appendStringInfoString(buf, b"ROW(\0".as_ptr() as _);
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                let mut i: i32 = 0;
                let mut arg = list_head((*rowexpr).args);
                while !arg.is_null() {
                    let e = crate::current_cell!(arg) as *mut Node;
                    if tupdesc.is_null() || !(*TupleDescAttr(tupdesc, i as _)).attisdropped {
                        appendStringInfoString(buf, sep);
                        /* Whole-row Vars need special treatment here */
                        get_rule_expr_toplevel(e, context, true);
                        sep = b", \0".as_ptr() as _;
                    }
                    i += 1;
                    arg = lnext!((*rowexpr).args, arg);
                }
                if !tupdesc.is_null() {
                    while i < (*tupdesc).natts {
                        if !(*TupleDescAttr(tupdesc, i as _)).attisdropped {
                            appendStringInfoString(buf, sep);
                            appendStringInfoString(buf, b"NULL\0".as_ptr() as _);
                            sep = b", \0".as_ptr() as _;
                        }
                        i += 1;
                    }
                    ReleaseTupleDesc(tupdesc);
                }
                appendStringInfoChar(buf, b')' as _);
                if (*rowexpr).row_format == COERCE_EXPLICIT_CAST {
                    appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                        format_type_with_typemod((*rowexpr).row_typeid, -1)).to_string_lossy());
                }
            }

            T_RowCompareExpr => {
                let rcexpr = node as *mut RowCompareExpr;
                /*
                 * SQL99 allows "ROW" to be omitted when there is more than
                 * one column, but for simplicity we always print it.  Within
                 * a ROW expression, whole-row Vars need special treatment, so
                 * use get_rule_list_toplevel.
                 */
                appendStringInfoString(buf, b"(ROW(\0".as_ptr() as _);
                get_rule_list_toplevel((*rcexpr).largs, context, true);

                /*
                 * We assume that the name of the first-column operator will
                 * do for all the rest too.  This is definitely open to
                 * failure, eg if some but not all operators were renamed
                 * since the construct was parsed, but there seems no way to
                 * be perfect.
                 */
                appendStringInfo!(buf, ") {} ROW(",
                    ::std::ffi::CStr::from_ptr(generate_operator_name(
                        linitial_oid!((*rcexpr).opnos),
                        exprType(linitial!((*rcexpr).largs) as *mut Node),
                        exprType(linitial!((*rcexpr).rargs) as *mut Node))).to_string_lossy());
                get_rule_list_toplevel((*rcexpr).rargs, context, true);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
            }

            T_CoalesceExpr => {
                let coalesceexpr = node as *mut CoalesceExpr;
                appendStringInfoString(buf, b"COALESCE(\0".as_ptr() as _);
                get_rule_expr((*coalesceexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_MinMaxExpr => {
                let minmaxexpr = node as *mut MinMaxExpr;
                match (*minmaxexpr).op {
                    IS_GREATEST => { appendStringInfoString(buf, b"GREATEST(\0".as_ptr() as _); }
                    IS_LEAST    => { appendStringInfoString(buf, b"LEAST(\0".as_ptr() as _); }
                    _ => {}
                }
                get_rule_expr((*minmaxexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_SQLValueFunction => {
                let svf = node as *mut SQLValueFunction;
                /*
                 * Note: this code knows that typmod for time, timestamp, and
                 * timestamptz just prints as integer.
                 */
                match (*svf).op {
                    SVFOP_CURRENT_DATE => {
                        appendStringInfoString(buf, b"CURRENT_DATE\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_TIME => {
                        appendStringInfoString(buf, b"CURRENT_TIME\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_TIME_N => {
                        appendStringInfo!(buf, "CURRENT_TIME({})", (*svf).typmod);
                    }
                    SVFOP_CURRENT_TIMESTAMP => {
                        appendStringInfoString(buf, b"CURRENT_TIMESTAMP\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_TIMESTAMP_N => {
                        appendStringInfo!(buf, "CURRENT_TIMESTAMP({})", (*svf).typmod);
                    }
                    SVFOP_LOCALTIME => {
                        appendStringInfoString(buf, b"LOCALTIME\0".as_ptr() as _);
                    }
                    SVFOP_LOCALTIME_N => {
                        appendStringInfo!(buf, "LOCALTIME({})", (*svf).typmod);
                    }
                    SVFOP_LOCALTIMESTAMP => {
                        appendStringInfoString(buf, b"LOCALTIMESTAMP\0".as_ptr() as _);
                    }
                    SVFOP_LOCALTIMESTAMP_N => {
                        appendStringInfo!(buf, "LOCALTIMESTAMP({})", (*svf).typmod);
                    }
                    SVFOP_CURRENT_ROLE => {
                        appendStringInfoString(buf, b"CURRENT_ROLE\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_USER => {
                        appendStringInfoString(buf, b"CURRENT_USER\0".as_ptr() as _);
                    }
                    SVFOP_USER => {
                        appendStringInfoString(buf, b"USER\0".as_ptr() as _);
                    }
                    SVFOP_SESSION_USER => {
                        appendStringInfoString(buf, b"SESSION_USER\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_CATALOG => {
                        appendStringInfoString(buf, b"CURRENT_CATALOG\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_SCHEMA => {
                        appendStringInfoString(buf, b"CURRENT_SCHEMA\0".as_ptr() as _);
                    }
                    _ => {}
                }
            }

            T_XmlExpr => {
                let xexpr = node as *mut XmlExpr;
                let mut needcomma = false;

                match (*xexpr).op {
                    IS_XMLCONCAT   => { appendStringInfoString(buf, b"XMLCONCAT(\0".as_ptr() as _); }
                    IS_XMLELEMENT  => { appendStringInfoString(buf, b"XMLELEMENT(\0".as_ptr() as _); }
                    IS_XMLFOREST   => { appendStringInfoString(buf, b"XMLFOREST(\0".as_ptr() as _); }
                    IS_XMLPARSE    => { appendStringInfoString(buf, b"XMLPARSE(\0".as_ptr() as _); }
                    IS_XMLPI       => { appendStringInfoString(buf, b"XMLPI(\0".as_ptr() as _); }
                    IS_XMLROOT     => { appendStringInfoString(buf, b"XMLROOT(\0".as_ptr() as _); }
                    IS_XMLSERIALIZE => { appendStringInfoString(buf, b"XMLSERIALIZE(\0".as_ptr() as _); }
                    IS_DOCUMENT    => { /* nothing */ }
                    _ => {}
                }
                if (*xexpr).op == IS_XMLPARSE || (*xexpr).op == IS_XMLSERIALIZE {
                    if (*xexpr).xmloption == XMLOPTION_DOCUMENT {
                        appendStringInfoString(buf, b"DOCUMENT \0".as_ptr() as _);
                    } else {
                        appendStringInfoString(buf, b"CONTENT \0".as_ptr() as _);
                    }
                }
                if !(*xexpr).name.is_null() {
                    appendStringInfo!(buf, "NAME {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier(
                            map_xml_name_to_sql_identifier((*xexpr).name))).to_string_lossy());
                    needcomma = true;
                }
                if !(*xexpr).named_args.is_null() {
                    if (*xexpr).op != IS_XMLFOREST {
                        if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                        appendStringInfoString(buf, b"XMLATTRIBUTES(\0".as_ptr() as _);
                        needcomma = false;
                    }
                    let mut arg = list_head((*xexpr).named_args);
                    let mut narg = list_head((*xexpr).arg_names);
                    while !arg.is_null() {
                        let e = crate::current_cell!(arg) as *mut Node;
                        let argname = strVal!(crate::current_cell!(narg) as *mut Node);
                        if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                        get_rule_expr(e, context, true);
                        appendStringInfo!(buf, " AS {}",
                            ::std::ffi::CStr::from_ptr(quote_identifier(
                                map_xml_name_to_sql_identifier(argname))).to_string_lossy());
                        needcomma = true;
                        arg = lnext!((*xexpr).named_args, arg);
                        narg = lnext!((*xexpr).arg_names, narg);
                    }
                    if (*xexpr).op != IS_XMLFOREST {
                        appendStringInfoChar(buf, b')' as _);
                    }
                }
                if !(*xexpr).args.is_null() {
                    if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                    match (*xexpr).op {
                        IS_XMLCONCAT | IS_XMLELEMENT | IS_XMLFOREST | IS_XMLPI | IS_XMLSERIALIZE => {
                            /* no extra decoration needed */
                            get_rule_expr((*xexpr).args as *mut Node, context, true);
                        }
                        IS_XMLPARSE => {
                            assert!(list_length((*xexpr).args) == 2);
                            get_rule_expr(linitial!((*xexpr).args) as *mut Node, context, true);
                            let con = lsecond!((*xexpr).args) as *mut Const;
                            assert!(!(*con).constisnull);
                            if DatumGetBool((*con).constvalue) {
                                appendStringInfoString(buf, b" PRESERVE WHITESPACE\0".as_ptr() as _);
                            } else {
                                appendStringInfoString(buf, b" STRIP WHITESPACE\0".as_ptr() as _);
                            }
                        }
                        IS_XMLROOT => {
                            assert!(list_length((*xexpr).args) == 3);
                            get_rule_expr(linitial!((*xexpr).args) as *mut Node, context, true);
                            appendStringInfoString(buf, b", VERSION \0".as_ptr() as _);
                            let con = lsecond!((*xexpr).args) as *mut Const;
                            if IsA!(con as *mut Node, Const) && (*con).constisnull {
                                appendStringInfoString(buf, b"NO VALUE\0".as_ptr() as _);
                            } else {
                                get_rule_expr(con as *mut Node, context, false);
                            }
                            let con3 = lthird!((*xexpr).args) as *mut Const;
                            if !(*con3).constisnull {
                                match DatumGetInt32((*con3).constvalue) {
                                    XML_STANDALONE_YES => {
                                        appendStringInfoString(buf, b", STANDALONE YES\0".as_ptr() as _);
                                    }
                                    XML_STANDALONE_NO => {
                                        appendStringInfoString(buf, b", STANDALONE NO\0".as_ptr() as _);
                                    }
                                    XML_STANDALONE_NO_VALUE => {
                                        appendStringInfoString(buf, b", STANDALONE NO VALUE\0".as_ptr() as _);
                                    }
                                    _ => {}
                                }
                            }
                            /* suppress STANDALONE NO VALUE */
                        }
                        IS_DOCUMENT => {
                            get_rule_expr_paren((*xexpr).args as *mut Node, context, false, node);
                        }
                        _ => {}
                    }
                }
                if (*xexpr).op == IS_XMLSERIALIZE {
                    appendStringInfo!(buf, " AS {}",
                        ::std::ffi::CStr::from_ptr(
                            format_type_with_typemod((*xexpr).r#type, (*xexpr).typmod)).to_string_lossy());
                    if (*xexpr).indent {
                        appendStringInfoString(buf, b" INDENT\0".as_ptr() as _);
                    } else {
                        appendStringInfoString(buf, b" NO INDENT\0".as_ptr() as _);
                    }
                }
                if (*xexpr).op == IS_DOCUMENT {
                    appendStringInfoString(buf, b" IS DOCUMENT\0".as_ptr() as _);
                } else {
                    appendStringInfoChar(buf, b')' as _);
                }
            }

            T_NullTest => {
                let ntest = node as *mut NullTest;
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren((*ntest).arg as *mut Node, context, true, node);

                /*
                 * For scalar inputs, we prefer to print as IS [NOT] NULL,
                 * which is shorter and traditional.  If it's a rowtype input
                 * but we're applying a scalar test, must print IS [NOT]
                 * DISTINCT FROM NULL to be semantically correct.
                 */
                if (*ntest).argisrow || !type_is_rowtype(exprType((*ntest).arg as *mut Node)) {
                    match (*ntest).nulltesttype {
                        IS_NULL     => { appendStringInfoString(buf, b" IS NULL\0".as_ptr() as _); }
                        IS_NOT_NULL => { appendStringInfoString(buf, b" IS NOT NULL\0".as_ptr() as _); }
                        _ => { elog!(ERROR, "unrecognized nulltesttype: {}", (*ntest).nulltesttype as i32); }
                    }
                } else {
                    match (*ntest).nulltesttype {
                        IS_NULL     => { appendStringInfoString(buf, b" IS NOT DISTINCT FROM NULL\0".as_ptr() as _); }
                        IS_NOT_NULL => { appendStringInfoString(buf, b" IS DISTINCT FROM NULL\0".as_ptr() as _); }
                        _ => { elog!(ERROR, "unrecognized nulltesttype: {}", (*ntest).nulltesttype as i32); }
                    }
                }
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_BooleanTest => {
                let btest = node as *mut BooleanTest;
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren((*btest).arg as *mut Node, context, false, node);
                match (*btest).booltesttype {
                    IS_TRUE        => { appendStringInfoString(buf, b" IS TRUE\0".as_ptr() as _); }
                    IS_NOT_TRUE    => { appendStringInfoString(buf, b" IS NOT TRUE\0".as_ptr() as _); }
                    IS_FALSE       => { appendStringInfoString(buf, b" IS FALSE\0".as_ptr() as _); }
                    IS_NOT_FALSE   => { appendStringInfoString(buf, b" IS NOT FALSE\0".as_ptr() as _); }
                    IS_UNKNOWN     => { appendStringInfoString(buf, b" IS UNKNOWN\0".as_ptr() as _); }
                    IS_NOT_UNKNOWN => { appendStringInfoString(buf, b" IS NOT UNKNOWN\0".as_ptr() as _); }
                    _ => { elog!(ERROR, "unrecognized booltesttype: {}", (*btest).booltesttype as i32); }
                }
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_CoerceToDomain => {
                let ctest = node as *mut CoerceToDomain;
                let arg = (*ctest).arg as *mut Node;
                if (*ctest).coercionformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr(arg, context, false);
                } else {
                    get_coercion_expr(arg, context,
                        (*ctest).resulttype, (*ctest).resulttypmod, node);
                }
            }

            T_CoerceToDomainValue => {
                appendStringInfoString(buf, b"VALUE\0".as_ptr() as _);
            }

            T_SetToDefault => {
                appendStringInfoString(buf, b"DEFAULT\0".as_ptr() as _);
            }

            T_CurrentOfExpr => {
                let cexpr = node as *mut CurrentOfExpr;
                if !(*cexpr).cursor_name.is_null() {
                    appendStringInfo!(buf, "CURRENT OF {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier((*cexpr).cursor_name)).to_string_lossy());
                } else {
                    appendStringInfo!(buf, "CURRENT OF ${}", (*cexpr).cursor_param);
                }
            }

            T_NextValueExpr => {
                let nvexpr = node as *mut NextValueExpr;
                /*
                 * This isn't exactly nextval(), but that seems close enough
                 * for EXPLAIN's purposes.
                 */
                appendStringInfoString(buf, b"nextval(\0".as_ptr() as _);
                simple_quote_literal(buf,
                    generate_relation_name((*nvexpr).seqid, std::ptr::null_mut()));
                appendStringInfoChar(buf, b')' as _);
            }

            T_InferenceElem => {
                let iexpr = node as *mut InferenceElem;
                /*
                 * InferenceElem can only refer to target relation, so a
                 * prefix is not useful, and indeed would cause parse errors.
                 */
                let save_varprefix = (*context).varprefix;
                (*context).varprefix = false;

                /*
                 * Parenthesize the element unless it's a simple Var or a bare
                 * function call.  Follows pg_get_indexdef_worker().
                 */
                let mut need_parens = !IsA!((*iexpr).expr, Var);
                if IsA!((*iexpr).expr, FuncExpr) &&
                    (*((*iexpr).expr as *mut FuncExpr)).funcformat == COERCE_EXPLICIT_CALL
                {
                    need_parens = false;
                }

                if need_parens { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr((*iexpr).expr as *mut Node, context, false);
                if need_parens { appendStringInfoChar(buf, b')' as _); }

                (*context).varprefix = save_varprefix;

                if (*iexpr).infercollid != 0 {
                    appendStringInfo!(buf, " COLLATE {}",
                        ::std::ffi::CStr::from_ptr(generate_collation_name((*iexpr).infercollid)).to_string_lossy());
                }

                /* Add the operator class name, if not default */
                if (*iexpr).inferopclass != 0 {
                    let inferopclass = (*iexpr).inferopclass;
                    let inferopcinputtype = get_opclass_input_type((*iexpr).inferopclass);
                    get_opclass_name(inferopclass, inferopcinputtype, buf);
                }
            }

            T_ReturningExpr => {
                let ret_expr = node as *mut ReturningExpr;
                /*
                 * We cannot see a ReturningExpr in rule deparsing, only while
                 * EXPLAINing a query plan (ReturningExpr nodes are only ever
                 * added during query rewriting). Just display the expression
                 * returned (an expanded view column).
                 */
                get_rule_expr((*ret_expr).retexpr as *mut Node, context, showimplicit);
            }

            T_PartitionBoundSpec => {
                let spec = node as *mut PartitionBoundSpec;

                if (*spec).is_default {
                    appendStringInfoString(buf, b"DEFAULT\0".as_ptr() as _);
                } else {
                    match (*spec).strategy as u8 {
                        PARTITION_STRATEGY_HASH => {
                            assert!((*spec).modulus > 0 && (*spec).remainder >= 0);
                            assert!((*spec).modulus > (*spec).remainder);
                            appendStringInfoString(buf, b"FOR VALUES\0".as_ptr() as _);
                            appendStringInfo!(buf, " WITH (modulus {}, remainder {})",
                                (*spec).modulus, (*spec).remainder);
                        }
                        PARTITION_STRATEGY_LIST => {
                            assert!(!(*spec).listdatums.is_null());
                            appendStringInfoString(buf, b"FOR VALUES IN (\0".as_ptr() as _);
                            let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                            let mut cell = list_head((*spec).listdatums);
                            while !cell.is_null() {
                                let val = crate::current_cell!(cell) as *mut Const;
                                appendStringInfoString(buf, sep);
                                get_const_expr(val, context, -1);
                                sep = b", \0".as_ptr() as _;
                                cell = lnext!((*spec).listdatums, cell);
                            }
                            appendStringInfoChar(buf, b')' as _);
                        }
                        PARTITION_STRATEGY_RANGE => {
                            assert!(!(*spec).lowerdatums.is_null() &&
                                !(*spec).upperdatums.is_null() &&
                                list_length((*spec).lowerdatums) == list_length((*spec).upperdatums));
                            appendStringInfo!(buf, "FOR VALUES FROM {} TO {}",
                                ::std::ffi::CStr::from_ptr(get_range_partbound_string((*spec).lowerdatums)).to_string_lossy(),
                                ::std::ffi::CStr::from_ptr(get_range_partbound_string((*spec).upperdatums)).to_string_lossy());
                        }
                        _ => {
                            elog!(ERROR, "unrecognized partition strategy: {}", (*spec).strategy as i32);
                        }
                    }
                }
            }

            T_JsonValueExpr => {
                let jve = node as *mut JsonValueExpr;
                get_rule_expr((*jve).raw_expr as *mut Node, context, false);
                get_json_format((*jve).format, (*context).buf);
            }

            T_JsonConstructorExpr => {
                get_json_constructor(node as *mut JsonConstructorExpr, context, false);
            }

            T_JsonIsPredicate => {
                let pred = node as *mut JsonIsPredicate;
                if !PRETTY_PAREN!(context) { appendStringInfoChar((*context).buf, b'(' as _); }
                get_rule_expr_paren((*pred).expr, context, true, node);
                appendStringInfoString((*context).buf, b" IS JSON\0".as_ptr() as _);
                /* TODO: handle FORMAT clause */
                match (*pred).item_type {
                    JS_TYPE_SCALAR => { appendStringInfoString((*context).buf, b" SCALAR\0".as_ptr() as _); }
                    JS_TYPE_ARRAY  => { appendStringInfoString((*context).buf, b" ARRAY\0".as_ptr() as _); }
                    JS_TYPE_OBJECT => { appendStringInfoString((*context).buf, b" OBJECT\0".as_ptr() as _); }
                    _ => {}
                }
                if (*pred).unique_keys {
                    appendStringInfoString((*context).buf, b" WITH UNIQUE KEYS\0".as_ptr() as _);
                }
                if !PRETTY_PAREN!(context) { appendStringInfoChar((*context).buf, b')' as _); }
            }

            T_JsonExpr => {
                let jexpr = node as *mut JsonExpr;
                match (*jexpr).op {
                    JSON_EXISTS_OP => { appendStringInfoString(buf, b"JSON_EXISTS(\0".as_ptr() as _); }
                    JSON_QUERY_OP  => { appendStringInfoString(buf, b"JSON_QUERY(\0".as_ptr() as _); }
                    JSON_VALUE_OP  => { appendStringInfoString(buf, b"JSON_VALUE(\0".as_ptr() as _); }
                    _ => { elog!(ERROR, "unrecognized JsonExpr op: {}", (*jexpr).op as i32); }
                }

                get_rule_expr((*jexpr).formatted_expr, context, showimplicit);
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                get_json_path_spec((*jexpr).path_spec, context, showimplicit);

                if !(*jexpr).passing_values.is_null() {
                    let mut needcomma = false;
                    appendStringInfoString(buf, b" PASSING \0".as_ptr() as _);
                    let mut lc1 = list_head((*jexpr).passing_names);
                    let mut lc2 = list_head((*jexpr).passing_values);
                    while !lc1.is_null() {
                        if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                        needcomma = true;
                        get_rule_expr(crate::current_cell!(lc2) as *mut Node, context, showimplicit);
                        appendStringInfo!(buf, " AS {}",
                            ::std::ffi::CStr::from_ptr(quote_identifier(
                                (*(crate::current_cell!(lc1) as *mut String)).sval)).to_string_lossy());
                        lc1 = lnext!((*jexpr).passing_names, lc1);
                        lc2 = lnext!((*jexpr).passing_values, lc2);
                    }
                }

                if (*jexpr).op != JSON_EXISTS_OP ||
                    (*(*jexpr).returning).typid != BOOLOID
                {
                    get_json_returning((*jexpr).returning, (*context).buf,
                        (*jexpr).op == JSON_QUERY_OP);
                }

                get_json_expr_options(jexpr, context,
                    if (*jexpr).op != JSON_EXISTS_OP { JSON_BEHAVIOR_NULL } else { JSON_BEHAVIOR_FALSE });

                appendStringInfoChar(buf, b')' as _);
            }

            T_List => {
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                let mut l = list_head(node as *mut List);
                while !l.is_null() {
                    appendStringInfoString(buf, sep);
                    get_rule_expr(crate::current_cell!(l) as *mut Node, context, showimplicit);
                    sep = b", \0".as_ptr() as _;
                    l = lnext!(node as *mut List, l);
                }
            }

            T_TableFunc => {
                get_tablefunc(node as *mut TableFunc, context, showimplicit);
            }

            _ => {
                elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as i32);
            }
        }
    }
}

/*
 * get_rule_expr_toplevel        - Parse back a toplevel expression
 *
 * Same as get_rule_expr(), except that if the expr is just a Var, we pass
 * istoplevel = true not false to get_variable().  This causes whole-row Vars
 * to get printed with decoration that will prevent expansion of "*".
 * We need to use this in contexts such as ROW() and VALUES(), where the
 * parser would expand "foo.*" appearing at top level.  (In principle we'd
 * use this in get_target_list() too, but that has additional worries about
 * whether to print AS, so it needs to invoke get_variable() directly anyway.)
 */
fn get_rule_expr_toplevel(node: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        if !node.is_null() && IsA!(node, Var) {
            get_variable(node as *mut Var, 0, true, context);
        } else {
            get_rule_expr(node, context, showimplicit);
        }
    }
}

/*
 * get_rule_list_toplevel        - Parse back a list of toplevel expressions
 *
 * Apply get_rule_expr_toplevel() to each element of a List.
 *
 * This adds commas between the expressions, but caller is responsible
 * for printing surrounding decoration.
 */
fn get_rule_list_toplevel(lst: *mut List, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
        let mut lc = list_head(lst);
        while !lc.is_null() {
            let e = crate::current_cell!(lc) as *mut Node;
            appendStringInfoString((*context).buf, sep);
            get_rule_expr_toplevel(e, context, showimplicit);
            sep = b", \0".as_ptr() as _;
            lc = lnext!(lst, lc);
        }
    }
}

/*
 * get_rule_expr_funccall        - Parse back a function-call expression
 *
 * Same as get_rule_expr(), except that we guarantee that the output will
 * look like a function call, or like one of the things the grammar treats as
 * equivalent to a function call (see the func_expr_windowless production).
 * This is needed in places where the grammar uses func_expr_windowless and
 * you can't substitute a parenthesized a_expr.  If what we have isn't going
 * to look like a function call, wrap it in a dummy CAST() expression, which
 * will satisfy the grammar --- and, indeed, is likely what the user wrote to
 * produce such a thing.
 */
fn get_rule_expr_funccall(node: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        if looks_like_function(node) {
            get_rule_expr(node, context, showimplicit);
        } else {
            let buf = (*context).buf;
            appendStringInfoString(buf, b"CAST(\0".as_ptr() as _);
            /* no point in showing any top-level implicit cast */
            get_rule_expr(node, context, false);
            appendStringInfo!(buf, " AS {})",
                ::std::ffi::CStr::from_ptr(
                    format_type_with_typemod(exprType(node), exprTypmod(node))).to_string_lossy());
        }
    }
}

/*
 * Helper function to identify node types that satisfy func_expr_windowless.
 * If in doubt, "false" is always a safe answer.
 */
fn looks_like_function(node: *mut Node) -> bool {
    unsafe {
        if node.is_null() {
            return false; /* probably shouldn't happen */
        }
        match nodeTag(node) {
            T_FuncExpr => {
                /* OK, unless it's going to deparse as a cast */
                (*(node as *mut FuncExpr)).funcformat == COERCE_EXPLICIT_CALL ||
                (*(node as *mut FuncExpr)).funcformat == COERCE_SQL_SYNTAX
            }
            T_NullIfExpr | T_CoalesceExpr | T_MinMaxExpr |
            T_SQLValueFunction | T_XmlExpr | T_JsonExpr => {
                /* these are all accepted by func_expr_common_subexpr */
                true
            }
            _ => false,
        }
    }
}

/*
 * get_oper_expr           - Parse back an OpExpr node
 */
fn get_oper_expr(expr: *mut OpExpr, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let opno = (*expr).opno;
        let args = (*expr).args;

        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
        if list_length(args) == 2 {
            /* binary operator */
            let arg1 = linitial!(args) as *mut Node;
            let arg2 = lsecond!(args) as *mut Node;
            get_rule_expr_paren(arg1, context, true, expr as *mut Node);
            appendStringInfo!(buf, " {} ",
                ::std::ffi::CStr::from_ptr(generate_operator_name(opno,
                    exprType(arg1), exprType(arg2))).to_string_lossy());
            get_rule_expr_paren(arg2, context, true, expr as *mut Node);
        } else {
            /* prefix operator */
            let arg = linitial!(args) as *mut Node;
            appendStringInfo!(buf, "{} ",
                ::std::ffi::CStr::from_ptr(generate_operator_name(opno,
                    InvalidOid, exprType(arg))).to_string_lossy());
            get_rule_expr_paren(arg, context, true, expr as *mut Node);
        }
        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
    }
}

/*
 * get_func_expr           - Parse back a FuncExpr node
 */
fn get_func_expr(expr: *mut FuncExpr, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;
        let funcoid = (*expr).funcid;
        let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
        let mut nargs: i32 = 0;
        let mut argnames: *mut List = std::ptr::null_mut();
        let mut use_variadic: bool = false;

        /*
         * If the function call came from an implicit coercion, then just show the
         * first argument --- unless caller wants to see implicit coercions.
         */
        if (*expr).funcformat == COERCE_IMPLICIT_CAST && !showimplicit {
            get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context,
                false, expr as *mut Node);
            return;
        }

        /*
         * If the function call came from a cast, then show the first argument
         * plus an explicit cast operation.
         */
        if (*expr).funcformat == COERCE_EXPLICIT_CAST ||
           (*expr).funcformat == COERCE_IMPLICIT_CAST
        {
            let arg = linitial!((*expr).args) as *mut Node;
            let rettype = (*expr).funcresulttype;
            let mut coerced_typmod: i32 = 0;

            /* Get the typmod if this is a length-coercion function */
            exprIsLengthCoercion(expr as *mut Node, &mut coerced_typmod);

            get_coercion_expr(arg, context, rettype, coerced_typmod, expr as *mut Node);
            return;
        }

        /*
         * If the function was called using one of the SQL spec's random special
         * syntaxes, try to reproduce that.  If we don't recognize the function,
         * fall through.
         */
        if (*expr).funcformat == COERCE_SQL_SYNTAX {
            if get_func_sql_syntax(expr, context) {
                return;
            }
        }

        /*
         * Normal function: display as proname(args).  First we need to extract
         * the argument datatypes.
         */
        if list_length((*expr).args) > FUNC_MAX_ARGS as i32 {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS) */
                errmsg!("too many arguments"));
        }
        nargs = 0;
        argnames = std::ptr::null_mut();
        let mut l = list_head((*expr).args);
        while !l.is_null() {
            let arg = crate::current_cell!(l) as *mut Node;
            if IsA!(arg, NamedArgExpr) {
                argnames = lappend(argnames, (*(arg as *mut NamedArgExpr)).name as *mut _);
            }
            argtypes[nargs as usize] = exprType(arg);
            nargs += 1;
            l = lnext!((*expr).args, l);
        }

        appendStringInfo!(buf, "{}(",
            ::std::ffi::CStr::from_ptr(generate_function_name(funcoid, nargs,
                argnames, argtypes.as_mut_ptr(),
                (*expr).funcvariadic,
                &mut use_variadic,
                (*context).inGroupBy)).to_string_lossy());
        nargs = 0;
        let mut l = list_head((*expr).args);
        while !l.is_null() {
            if nargs > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            if use_variadic && lnext!((*expr).args, l).is_null() {
                appendStringInfoString(buf, b"VARIADIC \0".as_ptr() as _);
            }
            get_rule_expr(crate::current_cell!(l) as *mut Node, context, true);
            nargs += 1;
            l = lnext!((*expr).args, l);
        }
        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * get_agg_expr            - Parse back an Aggref node
 */
fn get_agg_expr(aggref: *mut Aggref, context: *mut deparse_context, original_aggref: *mut Aggref) {
    get_agg_expr_helper(aggref, context, original_aggref,
        std::ptr::null(), std::ptr::null(), false);
}

/*
 * get_agg_expr_helper     - subroutine for get_agg_expr and
 *                          get_json_agg_constructor
 */
fn get_agg_expr_helper(
    aggref: *mut Aggref,
    context: *mut deparse_context,
    original_aggref: *mut Aggref,
    funcname: *const ::std::os::raw::c_char,
    options: *const ::std::os::raw::c_char,
    is_json_objectagg: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
        let mut nargs: i32;
        let mut use_variadic = false;

        /*
         * For a combining aggregate, we look up and deparse the corresponding
         * partial aggregate instead.  This is necessary because our input
         * argument list has been replaced; the new argument list always has just
         * one element, which will point to a partial Aggref that supplies us with
         * transition states to combine.
         */
        if DO_AGGSPLIT_COMBINE!((*aggref).aggsplit) {
            assert!(list_length((*aggref).args) == 1);
            let tle = linitial!((*aggref).args) as *mut TargetEntry;
            resolve_special_varno((*tle).expr as *mut Node, context,
                Some(get_agg_combine_expr), original_aggref as *mut _);
            return;
        }

        /*
         * Mark as PARTIAL, if appropriate.  We look to the original aggref so as
         * to avoid printing this when recursing from the code just above.
         */
        if DO_AGGSPLIT_SKIPFINAL!((*original_aggref).aggsplit) {
            appendStringInfoString(buf, b"PARTIAL \0".as_ptr() as _);
        }

        /* Extract the argument types as seen by the parser */
        nargs = get_aggregate_argtypes(aggref, argtypes.as_mut_ptr());

        let funcname_ptr = if !funcname.is_null() {
            funcname
        } else {
            generate_function_name((*aggref).aggfnoid, nargs, std::ptr::null_mut(),
                argtypes.as_mut_ptr(), (*aggref).aggvariadic,
                &mut use_variadic, (*context).inGroupBy)
        };

        /* Print the aggregate name, schema-qualified if needed */
        appendStringInfo!(buf, "{}({}",
            ::std::ffi::CStr::from_ptr(funcname_ptr).to_string_lossy(),
            if !(*aggref).aggdistinct.is_null() { "DISTINCT " } else { "" });

        if AGGKIND_IS_ORDERED_SET!((*aggref).aggkind) {
            /*
             * Ordered-set aggregates do not use "*" syntax.  Also, we needn't
             * worry about inserting VARIADIC.  So we can just dump the direct
             * args as-is.
             */
            assert!(!(*aggref).aggvariadic);
            get_rule_expr((*aggref).aggdirectargs as *mut Node, context, true);
            assert!(!(*aggref).aggorder.is_null());
            appendStringInfoString(buf, b") WITHIN GROUP (ORDER BY \0".as_ptr() as _);
            get_rule_orderby((*aggref).aggorder, (*aggref).args, false, context);
        } else {
            /* aggstar can be set only in zero-argument aggregates */
            if (*aggref).aggstar {
                appendStringInfoChar(buf, b'*' as _);
            } else {
                let mut i: i32 = 0;
                let mut l = list_head((*aggref).args);
                while !l.is_null() {
                    let tle = crate::current_cell!(l) as *mut TargetEntry;
                    let arg = (*tle).expr as *mut Node;
                    assert!(!IsA!(arg, NamedArgExpr));
                    if (*tle).resjunk {
                        l = lnext!((*aggref).args, l);
                        continue;
                    }
                    if i > 0 {
                        if is_json_objectagg {
                            /*
                             * the ABSENT ON NULL and WITH UNIQUE args are printed
                             * separately, so ignore them here
                             */
                            if i > 2 { break; }
                            appendStringInfoString(buf, b" : \0".as_ptr() as _);
                        } else {
                            appendStringInfoString(buf, b", \0".as_ptr() as _);
                        }
                    }
                    if use_variadic && i == nargs - 1 {
                        appendStringInfoString(buf, b"VARIADIC \0".as_ptr() as _);
                    }
                    get_rule_expr(arg, context, true);
                    i += 1;
                    l = lnext!((*aggref).args, l);
                }
            }

            if !(*aggref).aggorder.is_null() {
                appendStringInfoString(buf, b" ORDER BY \0".as_ptr() as _);
                get_rule_orderby((*aggref).aggorder, (*aggref).args, false, context);
            }
        }

        if !options.is_null() {
            appendStringInfoString(buf, options);
        }

        if !(*aggref).aggfilter.is_null() {
            appendStringInfoString(buf, b") FILTER (WHERE \0".as_ptr() as _);
            get_rule_expr((*aggref).aggfilter as *mut Node, context, false);
        }

        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * This is a helper function for get_agg_expr().  It's used when we deparse
 * a combining Aggref; resolve_special_varno locates the corresponding partial
 * Aggref and then calls this.
 */
unsafe extern "C" fn get_agg_combine_expr(
    node: *mut Node,
    context: *mut deparse_context,
    callback_arg: *mut ::std::os::raw::c_void,
) {
    let original_aggref = callback_arg as *mut Aggref;

    if !IsA!(node, Aggref) {
        elog!(ERROR, "combining Aggref does not point to an Aggref");
    }

    let aggref = node as *mut Aggref;
    get_agg_expr(aggref, context, original_aggref);
}

/*
 * get_windowfunc_expr - Parse back a WindowFunc node
 */
fn get_windowfunc_expr(wfunc: *mut WindowFunc, context: *mut deparse_context) {
    get_windowfunc_expr_helper(wfunc, context,
        std::ptr::null(), std::ptr::null(), false);
}

/*
 * get_windowfunc_expr_helper    - subroutine for get_windowfunc_expr and
 *                                get_json_agg_constructor
 */
fn get_windowfunc_expr_helper(
    wfunc: *mut WindowFunc,
    context: *mut deparse_context,
    funcname: *const ::std::os::raw::c_char,
    options: *const ::std::os::raw::c_char,
    is_json_objectagg: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
        let mut nargs: i32 = 0;
        let mut argnames: *mut List = std::ptr::null_mut();

        if list_length((*wfunc).args) > FUNC_MAX_ARGS as i32 {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS) */
                errmsg!("too many arguments"));
        }
        let mut l = list_head((*wfunc).args);
        while !l.is_null() {
            let arg = crate::current_cell!(l) as *mut Node;
            if IsA!(arg, NamedArgExpr) {
                argnames = lappend(argnames, (*(arg as *mut NamedArgExpr)).name as *mut _);
            }
            argtypes[nargs as usize] = exprType(arg);
            nargs += 1;
            l = lnext!((*wfunc).args, l);
        }

        let funcname_ptr = if !funcname.is_null() {
            funcname
        } else {
            generate_function_name((*wfunc).winfnoid, nargs, argnames,
                argtypes.as_mut_ptr(), false, std::ptr::null_mut(),
                (*context).inGroupBy)
        };

        appendStringInfo!(buf, "{}(", ::std::ffi::CStr::from_ptr(funcname_ptr).to_string_lossy());

        /* winstar can be set only in zero-argument aggregates */
        if (*wfunc).winstar {
            appendStringInfoChar(buf, b'*' as _);
        } else {
            if is_json_objectagg {
                get_rule_expr(linitial!((*wfunc).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" : \0".as_ptr() as _);
                get_rule_expr(lsecond!((*wfunc).args) as *mut Node, context, false);
            } else {
                get_rule_expr((*wfunc).args as *mut Node, context, true);
            }
        }

        if !options.is_null() {
            appendStringInfoString(buf, options);
        }

        if !(*wfunc).aggfilter.is_null() {
            appendStringInfoString(buf, b") FILTER (WHERE \0".as_ptr() as _);
            get_rule_expr((*wfunc).aggfilter as *mut Node, context, false);
        }

        appendStringInfoString(buf, b") OVER \0".as_ptr() as _);

        if !(*context).windowClause.is_null() {
            /* Query-decompilation case: search the windowClause list */
            let mut l = list_head((*context).windowClause);
            let mut found = false;
            while !l.is_null() {
                let wc = crate::current_cell!(l) as *mut WindowClause;
                if (*wc).winref == (*wfunc).winref {
                    if !(*wc).name.is_null() {
                        appendStringInfoString(buf, quote_identifier((*wc).name));
                    } else {
                        get_rule_windowspec(wc, (*context).targetList, context);
                    }
                    found = true;
                    break;
                }
                l = lnext!((*context).windowClause, l);
            }
            if !found {
                elog!(ERROR, "could not find window clause for winref {}", (*wfunc).winref);
            }
        } else {
            /*
             * In EXPLAIN, search the namespace stack for a matching WindowAgg
             * node (probably it's always the first entry), and print winname.
             */
            let mut l = list_head((*context).namespaces);
            let mut found = false;
            while !l.is_null() {
                let dpns = crate::current_cell!(l) as *mut deparse_namespace;
                if !(*dpns).plan.is_null() && IsA!((*dpns).plan as *mut Node, WindowAgg) {
                    let wagg = (*dpns).plan as *mut WindowAgg;
                    if (*wagg).winref == (*wfunc).winref {
                        appendStringInfoString(buf, quote_identifier((*wagg).winname));
                        found = true;
                        break;
                    }
                }
                l = lnext!((*context).namespaces, l);
            }
            if !found {
                elog!(ERROR, "could not find window clause for winref {}", (*wfunc).winref);
            }
        }
    }
}

/*
 * get_func_sql_syntax     - Parse back a SQL-syntax function call
 *
 * Returns true if we successfully deparsed, false if we did not
 * recognize the function.
 */
fn get_func_sql_syntax(expr: *mut FuncExpr, context: *mut deparse_context) -> bool {
    unsafe {
        let buf = (*context).buf;
        let funcoid = (*expr).funcid;

        match funcoid {
            F_TIMEZONE_INTERVAL_TIMESTAMP |
            F_TIMEZONE_INTERVAL_TIMESTAMPTZ |
            F_TIMEZONE_INTERVAL_TIMETZ |
            F_TIMEZONE_TEXT_TIMESTAMP |
            F_TIMEZONE_TEXT_TIMESTAMPTZ |
            F_TIMEZONE_TEXT_TIMETZ => {
                /* AT TIME ZONE ... note reversed argument order */
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr_paren(lsecond!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoString(buf, b" AT TIME ZONE \0".as_ptr() as _);
                get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_TIMEZONE_TIMESTAMP | F_TIMEZONE_TIMESTAMPTZ | F_TIMEZONE_TIMETZ => {
                /* AT LOCAL */
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoString(buf, b" AT LOCAL)\0".as_ptr() as _);
                return true;
            }
            F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_INTERVAL |
            F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_TIMESTAMPTZ |
            F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_INTERVAL |
            F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ |
            F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_INTERVAL |
            F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_TIMESTAMP |
            F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_INTERVAL |
            F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_TIMESTAMP |
            F_OVERLAPS_TIMETZ_TIMETZ_TIMETZ_TIMETZ |
            F_OVERLAPS_TIME_INTERVAL_TIME_INTERVAL |
            F_OVERLAPS_TIME_INTERVAL_TIME_TIME |
            F_OVERLAPS_TIME_TIME_TIME_INTERVAL |
            F_OVERLAPS_TIME_TIME_TIME_TIME => {
                /* (x1, x2) OVERLAPS (y1, y2) */
                appendStringInfoString(buf, b"((\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b") OVERLAPS (\0".as_ptr() as _);
                get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                get_rule_expr(lfourth!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
                return true;
            }
            F_EXTRACT_TEXT_DATE |
            F_EXTRACT_TEXT_TIME |
            F_EXTRACT_TEXT_TIMETZ |
            F_EXTRACT_TEXT_TIMESTAMP |
            F_EXTRACT_TEXT_TIMESTAMPTZ |
            F_EXTRACT_TEXT_INTERVAL => {
                /* EXTRACT (x FROM y) */
                appendStringInfoString(buf, b"EXTRACT(\0".as_ptr() as _);
                let con = linitial!((*expr).args) as *mut Const;
                assert!(IsA!(con as *mut Node, Const) &&
                    (*con).consttype == TEXTOID && !(*con).constisnull);
                appendStringInfoString(buf, TextDatumGetCString((*con).constvalue));
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_IS_NORMALIZED => {
                /* IS xxx NORMALIZED */
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoString(buf, b" IS\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    let con = lsecond!((*expr).args) as *mut Const;
                    assert!(IsA!(con as *mut Node, Const) &&
                        (*con).consttype == TEXTOID && !(*con).constisnull);
                    appendStringInfo!(buf, " {}",
                        ::std::ffi::CStr::from_ptr(TextDatumGetCString((*con).constvalue)).to_string_lossy());
                }
                appendStringInfoString(buf, b" NORMALIZED)\0".as_ptr() as _);
                return true;
            }
            F_PG_COLLATION_FOR => {
                /* COLLATION FOR */
                appendStringInfoString(buf, b"COLLATION FOR (\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_NORMALIZE => {
                /* NORMALIZE() */
                appendStringInfoString(buf, b"NORMALIZE(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                if list_length((*expr).args) == 2 {
                    let con = lsecond!((*expr).args) as *mut Const;
                    assert!(IsA!(con as *mut Node, Const) &&
                        (*con).consttype == TEXTOID && !(*con).constisnull);
                    appendStringInfo!(buf, ", {}",
                        ::std::ffi::CStr::from_ptr(TextDatumGetCString((*con).constvalue)).to_string_lossy());
                }
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_OVERLAY_BIT_BIT_INT4 | F_OVERLAY_BIT_BIT_INT4_INT4 |
            F_OVERLAY_BYTEA_BYTEA_INT4 | F_OVERLAY_BYTEA_BYTEA_INT4_INT4 |
            F_OVERLAY_TEXT_TEXT_INT4 | F_OVERLAY_TEXT_TEXT_INT4_INT4 => {
                /* OVERLAY() */
                appendStringInfoString(buf, b"OVERLAY(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" PLACING \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                if list_length((*expr).args) == 4 {
                    appendStringInfoString(buf, b" FOR \0".as_ptr() as _);
                    get_rule_expr(lfourth!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_POSITION_BIT_BIT | F_POSITION_BYTEA_BYTEA | F_POSITION_TEXT_TEXT => {
                /* POSITION() ... extra parens since args are b_expr not a_expr */
                appendStringInfoString(buf, b"POSITION((\0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b") IN (\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
                return true;
            }
            F_SUBSTRING_BIT_INT4 | F_SUBSTRING_BIT_INT4_INT4 |
            F_SUBSTRING_BYTEA_INT4 | F_SUBSTRING_BYTEA_INT4_INT4 |
            F_SUBSTRING_TEXT_INT4 | F_SUBSTRING_TEXT_INT4_INT4 => {
                /* SUBSTRING FROM/FOR (i.e., integer-position variants) */
                appendStringInfoString(buf, b"SUBSTRING(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                if list_length((*expr).args) == 3 {
                    appendStringInfoString(buf, b" FOR \0".as_ptr() as _);
                    get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_SUBSTRING_TEXT_TEXT_TEXT => {
                /* SUBSTRING SIMILAR/ESCAPE */
                appendStringInfoString(buf, b"SUBSTRING(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" SIMILAR \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" ESCAPE \0".as_ptr() as _);
                get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_BTRIM_BYTEA_BYTEA | F_BTRIM_TEXT | F_BTRIM_TEXT_TEXT => {
                /* TRIM() */
                appendStringInfoString(buf, b"TRIM(BOTH\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_LTRIM_BYTEA_BYTEA | F_LTRIM_TEXT | F_LTRIM_TEXT_TEXT => {
                /* TRIM() */
                appendStringInfoString(buf, b"TRIM(LEADING\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_RTRIM_BYTEA_BYTEA | F_RTRIM_TEXT | F_RTRIM_TEXT_TEXT => {
                /* TRIM() */
                appendStringInfoString(buf, b"TRIM(TRAILING\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_SYSTEM_USER => {
                appendStringInfoString(buf, b"SYSTEM_USER\0".as_ptr() as _);
                return true;
            }
            F_XMLEXISTS => {
                /* XMLEXISTS ... extra parens because args are c_expr */
                appendStringInfoString(buf, b"XMLEXISTS((\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b") PASSING (\0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
                return true;
            }
            _ => {}
        }
        false
    }
}

/* ----------
 * get_coercion_expr
 *
 *  Make a string representation of a value coerced to a specific type
 * ----------
 */
fn get_coercion_expr(
    arg: *mut Node,
    context: *mut deparse_context,
    resulttype: Oid,
    resulttypmod: i32,
    parent_node: *mut Node,
) {
    unsafe {
        let buf = (*context).buf;

        /*
         * Since parse_coerce.c doesn't immediately collapse application of
         * length-coercion functions to constants, what we'll typically see in
         * such cases is a Const with typmod -1 and a length-coercion function
         * right above it.  Avoid generating redundant output. However, beware of
         * suppressing casts when the user actually wrote something like
         * 'foo'::text::char(3).
         *
         * Note: it might seem that we are missing the possibility of needing to
         * print a COLLATE clause for such a Const.  However, a Const could only
         * have nondefault collation in a post-constant-folding tree, in which the
         * length coercion would have been folded too.  See also the special
         * handling of CollateExpr in coerce_to_target_type(): any collation
         * marking will be above the coercion node, not below it.
         */
        if !arg.is_null() && IsA!(arg, Const) &&
            (*(arg as *mut Const)).consttype == resulttype &&
            (*(arg as *mut Const)).consttypmod == -1
        {
            /* Show the constant without normal ::typename decoration */
            get_const_expr(arg as *mut Const, context, -1);
        } else {
            if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
            get_rule_expr_paren(arg, context, false, parent_node);
            if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
        }

        /*
         * Never emit resulttype(arg) functional notation. A pg_proc entry could
         * take precedence, and a resulttype in pg_temp would require schema
         * qualification that format_type_with_typemod() would usually omit. We've
         * standardized on arg::resulttype, but CAST(arg AS resulttype) notation
         * would work fine.
         */
        appendStringInfo!(buf, "::{}",
            ::std::ffi::CStr::from_ptr(format_type_with_typemod(resulttype, resulttypmod)).to_string_lossy());
    }
}

/* ----------
 * get_const_expr
 *
 *  Make a string representation of a Const
 *
 * showtype can be -1 to never show "::typename" decoration, or +1 to always
 * show it, or 0 to show it only if the constant wouldn't be assumed to be
 * the right type by default.
 *
 * If the Const's collation isn't default for its type, show that too.
 * We mustn't do this when showtype is -1 (since that means the caller will
 * print "::typename", and we can't put a COLLATE clause in between).  It's
 * caller's responsibility that collation isn't missed in such cases.
 * ----------
 */
fn get_const_expr(constval: *mut Const, context: *mut deparse_context, showtype: i32) {
    unsafe {
        let buf = (*context).buf;
        let mut typoutput: Oid = 0;
        let mut typ_is_varlena: bool = false;
        let mut needlabel = false;

        if (*constval).constisnull {
            /*
             * Always label the type of a NULL constant to prevent misdecisions
             * about type when reparsing.
             */
            appendStringInfoString(buf, b"NULL\0".as_ptr() as _);
            if showtype >= 0 {
                appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                    format_type_with_typemod((*constval).consttype, (*constval).consttypmod)).to_string_lossy());
                get_const_collation(constval, context);
            }
            return;
        }

        getTypeOutputInfo((*constval).consttype, &mut typoutput, &mut typ_is_varlena);

        let extval = OidOutputFunctionCall(typoutput, (*constval).constvalue);

        match (*constval).consttype {
            INT4OID => {
                /*
                 * INT4 can be printed without any decoration, unless it is
                 * negative; in that case print it as '-nnn'::integer to ensure
                 * that the output will re-parse as a constant, not as a constant
                 * plus operator.  In most cases we could get away with printing
                 * (-nnn) instead, because of the way that gram.y handles negative
                 * literals; but that doesn't work for INT_MIN, and it doesn't
                 * seem that much prettier anyway.
                 */
                if *extval != b'-' as i8 {
                    appendStringInfoString(buf, extval);
                } else {
                    appendStringInfo!(buf, "'{}'",
                        ::std::ffi::CStr::from_ptr(extval).to_string_lossy());
                    needlabel = true; /* we must attach a cast */
                }
            }
            NUMERICOID => {
                /*
                 * NUMERIC can be printed without quotes if it looks like a float
                 * constant (not an integer, and not Infinity or NaN) and doesn't
                 * have a leading sign (for the same reason as for INT4).
                 */
                let s = ::std::ffi::CStr::from_ptr(extval).to_bytes();
                if s.first().map_or(false, |c| c.is_ascii_digit()) &&
                    s.iter().any(|&c| c == b'e' || c == b'E' || c == b'.')
                {
                    appendStringInfoString(buf, extval);
                } else {
                    appendStringInfo!(buf, "'{}'",
                        ::std::ffi::CStr::from_ptr(extval).to_string_lossy());
                    needlabel = true; /* we must attach a cast */
                }
            }
            BOOLOID => {
                if ::std::ffi::CStr::from_ptr(extval).to_bytes() == b"t" {
                    appendStringInfoString(buf, b"true\0".as_ptr() as _);
                } else {
                    appendStringInfoString(buf, b"false\0".as_ptr() as _);
                }
            }
            _ => {
                simple_quote_literal(buf, extval);
            }
        }

        pfree(extval as *mut _);

        if showtype < 0 {
            return;
        }

        /*
         * For showtype == 0, append ::typename unless the constant will be
         * implicitly typed as the right type when it is read in.
         *
         * XXX this code has to be kept in sync with the behavior of the parser,
         * especially make_const.
         */
        match (*constval).consttype {
            BOOLOID | UNKNOWNOID => {
                /* These types can be left unlabeled */
                needlabel = false;
            }
            INT4OID => {
                /* We determined above whether a label is needed */
            }
            NUMERICOID => {
                /*
                 * Float-looking constants will be typed as numeric, which we
                 * checked above; but if there's a nondefault typmod we need to
                 * show it.
                 */
                needlabel |= (*constval).consttypmod >= 0;
            }
            _ => {
                needlabel = true;
            }
        }
        if needlabel || showtype > 0 {
            appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                format_type_with_typemod((*constval).consttype, (*constval).consttypmod)).to_string_lossy());
        }

        get_const_collation(constval, context);
    }
}

/*
 * helper for get_const_expr: append COLLATE if needed
 */
fn get_const_collation(constval: *mut Const, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        if OidIsValid!((*constval).constcollid) {
            let typcollation = get_typcollation((*constval).consttype);
            if (*constval).constcollid != typcollation {
                appendStringInfo!(buf, " COLLATE {}",
                    ::std::ffi::CStr::from_ptr(generate_collation_name((*constval).constcollid)).to_string_lossy());
            }
        }
    }
}

/*
 * get_json_path_spec      - Parse back a JSON path specification
 */
fn get_json_path_spec(path_spec: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        if IsA!(path_spec, Const) {
            get_const_expr(path_spec as *mut Const, context, -1);
        } else {
            get_rule_expr(path_spec, context, showimplicit);
        }
    }
}

/*
 * get_json_format          - Parse back a JsonFormat node
 */
fn get_json_format(format: *mut JsonFormat, buf: *mut StringInfoData) {
    unsafe {
        if (*format).format_type == JS_FORMAT_DEFAULT {
            return;
        }
        appendStringInfoString(buf,
            if (*format).format_type == JS_FORMAT_JSONB {
                b" FORMAT JSONB\0".as_ptr() as _
            } else {
                b" FORMAT JSON\0".as_ptr() as _
            });
        if (*format).encoding != JS_ENC_DEFAULT {
            let encoding = if (*format).encoding == JS_ENC_UTF16 {
                "UTF16"
            } else if (*format).encoding == JS_ENC_UTF32 {
                "UTF32"
            } else {
                "UTF8"
            };
            appendStringInfo!(buf, " ENCODING {}", encoding);
        }
    }
}

/*
 * get_json_returning       - Parse back a JsonReturning structure
 */
fn get_json_returning(
    returning: *mut JsonReturning,
    buf: *mut StringInfoData,
    json_format_by_default: bool,
) {
    unsafe {
        if !OidIsValid!((*returning).typid) {
            return;
        }
        appendStringInfo!(buf, " RETURNING {}",
            ::std::ffi::CStr::from_ptr(format_type_with_typemod(
                (*returning).typid, (*returning).typmod)).to_string_lossy());

        if !json_format_by_default ||
            (*(*returning).format).format_type !=
            (if (*returning).typid == JSONBOID { JS_FORMAT_JSONB } else { JS_FORMAT_JSON })
        {
            get_json_format((*returning).format, buf);
        }
    }
}

/*
 * get_json_constructor     - Parse back a JsonConstructorExpr node
 */
fn get_json_constructor(
    ctor: *mut JsonConstructorExpr,
    context: *mut deparse_context,
    showimplicit: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let is_json_object: bool;

        if (*ctor).r#type == JSCTOR_JSON_OBJECTAGG {
            get_json_agg_constructor(ctor, context, b"JSON_OBJECTAGG\0".as_ptr() as _, true);
            return;
        } else if (*ctor).r#type == JSCTOR_JSON_ARRAYAGG {
            get_json_agg_constructor(ctor, context, b"JSON_ARRAYAGG\0".as_ptr() as _, false);
            return;
        }

        let funcname: *const ::std::os::raw::c_char = match (*ctor).r#type {
            JSCTOR_JSON_OBJECT    => b"JSON_OBJECT\0".as_ptr() as _,
            JSCTOR_JSON_ARRAY     => b"JSON_ARRAY\0".as_ptr() as _,
            JSCTOR_JSON_PARSE     => b"JSON\0".as_ptr() as _,
            JSCTOR_JSON_SCALAR    => b"JSON_SCALAR\0".as_ptr() as _,
            JSCTOR_JSON_SERIALIZE => b"JSON_SERIALIZE\0".as_ptr() as _,
            _ => {
                elog!(ERROR, "invalid JsonConstructorType {}", (*ctor).r#type as i32);
                std::ptr::null()
            }
        };

        appendStringInfo!(buf, "{}(", ::std::ffi::CStr::from_ptr(funcname).to_string_lossy());

        is_json_object = (*ctor).r#type == JSCTOR_JSON_OBJECT;
        let mut curridx: i32 = 0;
        let mut lc = list_head((*ctor).args);
        while !lc.is_null() {
            if curridx > 0 {
                let sep = if is_json_object && (curridx % 2) != 0 { " : " } else { ", " };
                appendStringInfoString(buf, sep.as_ptr() as _);
            }
            get_rule_expr(crate::current_cell!(lc) as *mut Node, context, true);
            curridx += 1;
            lc = lnext!((*ctor).args, lc);
        }

        get_json_constructor_options(ctor, buf);
        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * Append options, if any, to the JSON constructor being deparsed
 */
fn get_json_constructor_options(ctor: *mut JsonConstructorExpr, buf: *mut StringInfoData) {
    unsafe {
        if (*ctor).absent_on_null {
            if (*ctor).r#type == JSCTOR_JSON_OBJECT || (*ctor).r#type == JSCTOR_JSON_OBJECTAGG {
                appendStringInfoString(buf, b" ABSENT ON NULL\0".as_ptr() as _);
            }
        } else {
            if (*ctor).r#type == JSCTOR_JSON_ARRAY || (*ctor).r#type == JSCTOR_JSON_ARRAYAGG {
                appendStringInfoString(buf, b" NULL ON NULL\0".as_ptr() as _);
            }
        }

        if (*ctor).unique {
            appendStringInfoString(buf, b" WITH UNIQUE KEYS\0".as_ptr() as _);
        }

        /*
         * Append RETURNING clause if needed; JSON() and JSON_SCALAR() don't
         * support one.
         */
        if (*ctor).r#type != JSCTOR_JSON_PARSE && (*ctor).r#type != JSCTOR_JSON_SCALAR {
            get_json_returning((*ctor).returning, buf, true);
        }
    }
}

/*
 * get_json_agg_constructor - Parse back an aggregate JsonConstructorExpr node
 */
fn get_json_agg_constructor(
    ctor: *mut JsonConstructorExpr,
    context: *mut deparse_context,
    funcname: *const ::std::os::raw::c_char,
    is_json_objectagg: bool,
) {
    unsafe {
        let mut options = StringInfoData {
            data: std::ptr::null_mut(),
            len: 0,
            maxlen: 0,
            cursor: 0,
        };
        initStringInfo(&mut options);
        get_json_constructor_options(ctor, &mut options);

        if IsA!((*ctor).func as *mut Node, Aggref) {
            get_agg_expr_helper((*ctor).func as *mut Aggref, context,
                (*ctor).func as *mut Aggref,
                funcname, options.data, is_json_objectagg);
        } else if IsA!((*ctor).func as *mut Node, WindowFunc) {
            get_windowfunc_expr_helper((*ctor).func as *mut WindowFunc, context,
                funcname, options.data, is_json_objectagg);
        } else {
            elog!(ERROR, "invalid JsonConstructorExpr underlying node type: {}",
                nodeTag((*ctor).func as *mut Node) as i32);
        }
    }
}

/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 */
fn simple_quote_literal(buf: *mut StringInfoData, val: *const ::std::os::raw::c_char) {
    unsafe {
        /*
         * We form the string literal according to the prevailing setting of
         * standard_conforming_strings; we never use E''. User is responsible for
         * making sure result is used correctly.
         */
        appendStringInfoChar(buf, b'\'' as _);
        let mut valptr = val;
        while *valptr != 0 {
            let ch = *valptr as u8;
            if SQL_STR_DOUBLE!(ch, !standard_conforming_strings) {
                appendStringInfoChar(buf, ch as _);
            }
            appendStringInfoChar(buf, ch as _);
            valptr = valptr.add(1);
        }
        appendStringInfoChar(buf, b'\'' as _);
    }
}

/* ----------
 * get_sublink_expr         - Parse back a sublink
 * ----------
 */
fn get_sublink_expr(sublink: *mut SubLink, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let query = (*sublink).subselect as *mut Query;
        let mut opname: *mut ::std::os::raw::c_char = std::ptr::null_mut();
        let mut need_paren: bool;

        if (*sublink).subLinkType == ARRAY_SUBLINK {
            appendStringInfoString(buf, b"ARRAY(\0".as_ptr() as _);
        } else {
            appendStringInfoChar(buf, b'(' as _);
        }

        /*
         * Note that we print the name of only the first operator, when there are
         * multiple combining operators.  This is an approximation that could go
         * wrong in various scenarios (operators in different schemas, renamed
         * operators, etc) but there is not a whole lot we can do about it, since
         * the syntax allows only one operator to be shown.
         */
        if !(*sublink).testexpr.is_null() {
            if IsA!((*sublink).testexpr, OpExpr) {
                /* single combining operator */
                let opexpr = (*sublink).testexpr as *mut OpExpr;
                get_rule_expr(linitial!((*opexpr).args) as *mut Node, context, true);
                opname = generate_operator_name((*opexpr).opno,
                    exprType(linitial!((*opexpr).args) as *mut Node),
                    exprType(lsecond!((*opexpr).args) as *mut Node));
            } else if IsA!((*sublink).testexpr, BoolExpr) {
                /* multiple combining operators, = or <> cases */
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                appendStringInfoChar(buf, b'(' as _);
                let mut l = list_head((*((*sublink).testexpr as *mut BoolExpr)).args);
                while !l.is_null() {
                    let opexpr = crate::current_cell!(l) as *mut OpExpr;
                    appendStringInfoString(buf, sep);
                    get_rule_expr(linitial!((*opexpr).args) as *mut Node, context, true);
                    if opname.is_null() {
                        opname = generate_operator_name((*opexpr).opno,
                            exprType(linitial!((*opexpr).args) as *mut Node),
                            exprType(lsecond!((*opexpr).args) as *mut Node));
                    }
                    sep = b", \0".as_ptr() as _;
                    l = lnext!((*((*sublink).testexpr as *mut BoolExpr)).args, l);
                }
                appendStringInfoChar(buf, b')' as _);
            } else if IsA!((*sublink).testexpr, RowCompareExpr) {
                /* multiple combining operators, < <= > >= cases */
                let rcexpr = (*sublink).testexpr as *mut RowCompareExpr;
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr((*rcexpr).largs as *mut Node, context, true);
                opname = generate_operator_name(linitial_oid!((*rcexpr).opnos),
                    exprType(linitial!((*rcexpr).largs) as *mut Node),
                    exprType(linitial!((*rcexpr).rargs) as *mut Node));
                appendStringInfoChar(buf, b')' as _);
            } else {
                elog!(ERROR, "unrecognized testexpr type: {}",
                    nodeTag((*sublink).testexpr) as i32);
            }
        }

        need_paren = true;

        match (*sublink).subLinkType {
            EXISTS_SUBLINK => {
                appendStringInfoString(buf, b"EXISTS \0".as_ptr() as _);
            }
            ANY_SUBLINK => {
                if ::std::ffi::CStr::from_ptr(opname).to_bytes() == b"=" {
                    /* Represent = ANY as IN */
                    appendStringInfoString(buf, b" IN \0".as_ptr() as _);
                } else {
                    appendStringInfo!(buf, " {} ANY ",
                        ::std::ffi::CStr::from_ptr(opname).to_string_lossy());
                }
            }
            ALL_SUBLINK => {
                appendStringInfo!(buf, " {} ALL ",
                    ::std::ffi::CStr::from_ptr(opname).to_string_lossy());
            }
            ROWCOMPARE_SUBLINK => {
                appendStringInfo!(buf, " {} ",
                    ::std::ffi::CStr::from_ptr(opname).to_string_lossy());
            }
            EXPR_SUBLINK | MULTIEXPR_SUBLINK | ARRAY_SUBLINK => {
                need_paren = false;
            }
            CTE_SUBLINK | _ => {
                /* shouldn't occur in a SubLink */
                elog!(ERROR, "unrecognized sublink type: {}", (*sublink).subLinkType as i32);
            }
        }

        if need_paren {
            appendStringInfoChar(buf, b'(' as _);
        }

        get_query_def(query, buf, (*context).namespaces, std::ptr::null_mut(), false,
            (*context).prettyFlags, (*context).wrapColumn, (*context).indentLevel);

        if need_paren {
            appendStringInfoString(buf, b"))\0".as_ptr() as _);
        } else {
            appendStringInfoChar(buf, b')' as _);
        }
    }
}

/* ----------
 * get_xmltable             - Parse back a XMLTABLE function
 * ----------
 */
fn get_xmltable(tf: *mut TableFunc, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;

        appendStringInfoString(buf, b"XMLTABLE(\0".as_ptr() as _);

        if !(*tf).ns_uris.is_null() {
            let mut first = true;
            appendStringInfoString(buf, b"XMLNAMESPACES (\0".as_ptr() as _);
            let mut lc1 = list_head((*tf).ns_uris);
            let mut lc2 = list_head((*tf).ns_names);
            while !lc1.is_null() {
                let expr = crate::current_cell!(lc1) as *mut Node;
                let ns_node = crate::current_cell!(lc2) as *mut String;

                if !first { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                else { first = false; }

                if !ns_node.is_null() {
                    get_rule_expr(expr, context, showimplicit);
                    appendStringInfo!(buf, " AS {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier((*ns_node).sval)).to_string_lossy());
                } else {
                    appendStringInfoString(buf, b"DEFAULT \0".as_ptr() as _);
                    get_rule_expr(expr, context, showimplicit);
                }
                lc1 = lnext!((*tf).ns_uris, lc1);
                lc2 = lnext!((*tf).ns_names, lc2);
            }
            appendStringInfoString(buf, b"), \0".as_ptr() as _);
        }

        appendStringInfoChar(buf, b'(' as _);
        get_rule_expr((*tf).rowexpr as *mut Node, context, showimplicit);
        appendStringInfoString(buf, b") PASSING (\0".as_ptr() as _);
        get_rule_expr((*tf).docexpr as *mut Node, context, showimplicit);
        appendStringInfoChar(buf, b')' as _);

        if !(*tf).colexprs.is_null() {
            let mut colnum: i32 = 0;
            appendStringInfoString(buf, b" COLUMNS \0".as_ptr() as _);
            let mut l1 = list_head((*tf).colnames);
            let mut l2 = list_head((*tf).coltypes);
            let mut l3 = list_head((*tf).coltypmods);
            let mut l4 = list_head((*tf).colexprs);
            let mut l5 = list_head((*tf).coldefexprs);
            while !l1.is_null() {
                let colname = strVal!(crate::current_cell!(l1) as *mut Node);
                let typid = lfirst_oid!(l2);
                let typmod = lfirst_int!(l3);
                let colexpr = crate::current_cell!(l4) as *mut Node;
                let coldefexpr = crate::current_cell!(l5) as *mut Node;
                let ordinality = (*tf).ordinalitycol == colnum;
                let notnull = bms_is_member(colnum, (*tf).notnulls);

                if colnum > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                colnum += 1;

                appendStringInfo!(buf, "{} {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(colname)).to_string_lossy(),
                    if ordinality { "FOR ORDINALITY".to_string() }
                    else { ::std::ffi::CStr::from_ptr(format_type_with_typemod(typid, typmod)).to_string_lossy().into_owned() });

                if ordinality {
                    l1 = lnext!((*tf).colnames, l1);
                    l2 = lnext!((*tf).coltypes, l2);
                    l3 = lnext!((*tf).coltypmods, l3);
                    l4 = lnext!((*tf).colexprs, l4);
                    l5 = lnext!((*tf).coldefexprs, l5);
                    continue;
                }

                if !coldefexpr.is_null() {
                    appendStringInfoString(buf, b" DEFAULT (\0".as_ptr() as _);
                    get_rule_expr(coldefexpr, context, showimplicit);
                    appendStringInfoChar(buf, b')' as _);
                }
                if !colexpr.is_null() {
                    appendStringInfoString(buf, b" PATH (\0".as_ptr() as _);
                    get_rule_expr(colexpr, context, showimplicit);
                    appendStringInfoChar(buf, b')' as _);
                }
                if notnull {
                    appendStringInfoString(buf, b" NOT NULL\0".as_ptr() as _);
                }

                l1 = lnext!((*tf).colnames, l1);
                l2 = lnext!((*tf).coltypes, l2);
                l3 = lnext!((*tf).coltypmods, l3);
                l4 = lnext!((*tf).colexprs, l4);
                l5 = lnext!((*tf).coldefexprs, l5);
            }
        }

        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * get_json_table_nested_columns - Parse back nested JSON_TABLE columns
 */
fn get_json_table_nested_columns(
    tf: *mut TableFunc,
    plan: *mut JsonTablePlan,
    context: *mut deparse_context,
    showimplicit: bool,
    needcomma: bool,
) {
    unsafe {
        if IsA!(plan as *mut Node, JsonTablePathScan) {
            let scan = plan as *mut JsonTablePathScan;
            if needcomma { appendStringInfoChar((*context).buf, b',' as _); }
            appendStringInfoChar((*context).buf, b' ' as _);
            appendContextKeyword(context, b"NESTED PATH \0".as_ptr() as _, 0, 0, 0);
            get_const_expr((*scan).path_value, context, -1);
            appendStringInfo!((*context).buf, " AS {}",
                ::std::ffi::CStr::from_ptr(quote_identifier((*scan).path_name)).to_string_lossy());
            get_json_table_columns(tf, scan, context, showimplicit);
        } else if IsA!(plan as *mut Node, JsonTableSiblingJoin) {
            let join = plan as *mut JsonTableSiblingJoin;
            get_json_table_nested_columns(tf, (*join).lplan, context, showimplicit, needcomma);
            get_json_table_nested_columns(tf, (*join).rplan, context, showimplicit, true);
        }
    }
}

/*
 * get_json_table_columns - Parse back JSON_TABLE columns
 */
fn get_json_table_columns(
    tf: *mut TableFunc,
    scan: *mut JsonTablePathScan,
    context: *mut deparse_context,
    showimplicit: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let mut colnum: i32 = 0;

        appendStringInfoChar(buf, b' ' as _);
        appendContextKeyword(context, b"COLUMNS (\0".as_ptr() as _, 0, 0, 0);

        if PRETTY_INDENT!(context) {
            (*context).indentLevel += PRETTYINDENT_VAR;
        }

        let mut lc_colname = list_head((*tf).colnames);
        let mut lc_coltype = list_head((*tf).coltypes);
        let mut lc_coltypmod = list_head((*tf).coltypmods);
        let mut lc_colvalexpr = list_head((*tf).colvalexprs);
        while !lc_colname.is_null() {
            let colname = strVal!(crate::current_cell!(lc_colname) as *mut Node);
            let typid = lfirst_oid!(lc_coltype);
            let typmod = lfirst_int!(lc_coltypmod);
            let colexpr_node = crate::current_cell!(lc_colvalexpr);
            let colexpr: *mut JsonExpr = if colexpr_node.is_null() {
                std::ptr::null_mut()
            } else {
                colexpr_node as *mut JsonExpr
            };
            let default_behavior: JsonBehaviorType;

            /* Skip columns that don't belong to this scan. */
            if (*scan).colMin < 0 || colnum < (*scan).colMin {
                colnum += 1;
                lc_colname = lnext!((*tf).colnames, lc_colname);
                lc_coltype = lnext!((*tf).coltypes, lc_coltype);
                lc_coltypmod = lnext!((*tf).coltypmods, lc_coltypmod);
                lc_colvalexpr = lnext!((*tf).colvalexprs, lc_colvalexpr);
                continue;
            }
            if colnum > (*scan).colMax { break; }

            if colnum > (*scan).colMin {
                appendStringInfoString(buf, b", \0".as_ptr() as _);
            }

            colnum += 1;

            let ordinality = colexpr.is_null();

            appendContextKeyword(context, b"\0".as_ptr() as _, 0, 0, 0);

            appendStringInfo!(buf, "{} {}",
                ::std::ffi::CStr::from_ptr(quote_identifier(colname)).to_string_lossy(),
                if ordinality { "FOR ORDINALITY".to_string() }
                else { ::std::ffi::CStr::from_ptr(format_type_with_typemod(typid, typmod)).to_string_lossy().into_owned() });

            if ordinality {
                lc_colname = lnext!((*tf).colnames, lc_colname);
                lc_coltype = lnext!((*tf).coltypes, lc_coltype);
                lc_coltypmod = lnext!((*tf).coltypmods, lc_coltypmod);
                lc_colvalexpr = lnext!((*tf).colvalexprs, lc_colvalexpr);
                continue;
            }

            /*
             * Set default_behavior to guide get_json_expr_options() on whether to
             * emit the ON ERROR / EMPTY clauses.
             */
            if (*colexpr).op == JSON_EXISTS_OP {
                appendStringInfoString(buf, b" EXISTS\0".as_ptr() as _);
                default_behavior = JSON_BEHAVIOR_FALSE;
            } else {
                if (*colexpr).op == JSON_QUERY_OP {
                    let mut typcategory: ::std::os::raw::c_char = 0;
                    let mut typispreferred = false;
                    get_type_category_preferred(typid, &mut typcategory, &mut typispreferred);
                    if typcategory == TYPCATEGORY_STRING as i8 {
                        appendStringInfoString(buf,
                            if (*(*colexpr).format).format_type == JS_FORMAT_JSONB {
                                b" FORMAT JSONB\0".as_ptr() as _
                            } else {
                                b" FORMAT JSON\0".as_ptr() as _
                            });
                    }
                }
                default_behavior = JSON_BEHAVIOR_NULL;
            }

            appendStringInfoString(buf, b" PATH \0".as_ptr() as _);
            get_json_path_spec((*colexpr).path_spec, context, showimplicit);
            get_json_expr_options(colexpr, context, default_behavior);

            lc_colname = lnext!((*tf).colnames, lc_colname);
            lc_coltype = lnext!((*tf).coltypes, lc_coltype);
            lc_coltypmod = lnext!((*tf).coltypmods, lc_coltypmod);
            lc_colvalexpr = lnext!((*tf).colvalexprs, lc_colvalexpr);
        }

        if !(*scan).child.is_null() {
            get_json_table_nested_columns(tf, (*scan).child, context, showimplicit,
                (*scan).colMin >= 0);
        }

        if PRETTY_INDENT!(context) {
            (*context).indentLevel -= PRETTYINDENT_VAR;
        }

        appendContextKeyword(context, b")\0".as_ptr() as _, 0, 0, 0);
    }
}

/* ----------
 * get_json_table           - Parse back a JSON_TABLE function
 * ----------
 */
fn get_json_table(tf: *mut TableFunc, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;
        let jexpr = (*tf).docexpr as *mut JsonExpr;
        let root = (*tf).plan as *mut JsonTablePathScan;

        appendStringInfoString(buf, b"JSON_TABLE(\0".as_ptr() as _);

        if PRETTY_INDENT!(context) {
            (*context).indentLevel += PRETTYINDENT_VAR;
        }

        appendContextKeyword(context, b"\0".as_ptr() as _, 0, 0, 0);

        get_rule_expr((*jexpr).formatted_expr, context, showimplicit);
        appendStringInfoString(buf, b", \0".as_ptr() as _);
        get_const_expr((*root).path_value, context, -1);
        appendStringInfo!(buf, " AS {}",
            ::std::ffi::CStr::from_ptr(quote_identifier((*root).path_name)).to_string_lossy());

        if !(*jexpr).passing_values.is_null() {
            let mut needcomma = false;
            appendStringInfoChar(buf, b' ' as _);
            appendContextKeyword(context, b"PASSING \0".as_ptr() as _, 0, 0, 0);

            if PRETTY_INDENT!(context) { (*context).indentLevel += PRETTYINDENT_VAR; }

            let mut lc1 = list_head((*jexpr).passing_names);
            let mut lc2 = list_head((*jexpr).passing_values);
            while !lc1.is_null() {
                if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                needcomma = true;
                appendContextKeyword(context, b"\0".as_ptr() as _, 0, 0, 0);
                get_rule_expr(crate::current_cell!(lc2) as *mut Node, context, false);
                appendStringInfo!(buf, " AS {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(
                        (*(crate::current_cell!(lc1) as *mut String)).sval)).to_string_lossy());
                lc1 = lnext!((*jexpr).passing_names, lc1);
                lc2 = lnext!((*jexpr).passing_values, lc2);
            }

            if PRETTY_INDENT!(context) { (*context).indentLevel -= PRETTYINDENT_VAR; }
        }

        get_json_table_columns(tf, root, context, showimplicit);

        if (*(*jexpr).on_error).btype != JSON_BEHAVIOR_EMPTY_ARRAY {
            get_json_behavior((*jexpr).on_error, context, b"ERROR\0".as_ptr() as _);
        }

        if PRETTY_INDENT!(context) { (*context).indentLevel -= PRETTYINDENT_VAR; }

        appendContextKeyword(context, b")\0".as_ptr() as _, 0, 0, 0);
    }
}

/* ----------
 * get_tablefunc             - Parse back a table function
 * ----------
 */
fn get_tablefunc(tf: *mut TableFunc, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        /* XMLTABLE and JSON_TABLE are the only existing implementations. */
        if (*tf).functype == TFT_XMLTABLE {
            get_xmltable(tf, context, showimplicit);
        } else if (*tf).functype == TFT_JSON_TABLE {
            get_json_table(tf, context, showimplicit);
        }
    }
}

/* ----------
 * get_from_clause           - Parse back a FROM clause
 *
 * "prefix" is the keyword that denotes the start of the list of FROM
 * elements. It is FROM when used to parse back SELECT and UPDATE, but
 * is USING when parsing back DELETE.
 * ----------
 */
fn get_from_clause(
    query: *mut Query,
    prefix: *const ::std::os::raw::c_char,
    context: *mut deparse_context,
) {
    unsafe {
        let buf = (*context).buf;
        let mut first = true;

        /*
         * We use the query's jointree as a guide to what to print.  However, we
         * must ignore auto-added RTEs that are marked not inFromCl. (These can
         * only appear at the top level of the jointree, so it's sufficient to
         * check here.)  This check also ensures we ignore the rule pseudo-RTEs
         * for NEW and OLD.
         */
        let mut l = list_head((*(*query).jointree).fromlist);
        while !l.is_null() {
            let jtnode = crate::current_cell!(l) as *mut Node;

            if IsA!(jtnode, RangeTblRef) {
                let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
                let rte = rt_fetch(varno, (*query).rtable);
                if !(*rte).inFromCl {
                    l = lnext!((*(*query).jointree).fromlist, l);
                    continue;
                }
            }

            if first {
                appendContextKeyword(context, prefix,
                    -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, 2);
                first = false;
                get_from_clause_item(jtnode, query, context);
            } else {
                let mut itembuf = StringInfoData {
                    data: std::ptr::null_mut(),
                    len: 0, maxlen: 0, cursor: 0,
                };
                appendStringInfoString(buf, b", \0".as_ptr() as _);

                /*
                 * Put the new FROM item's text into itembuf so we can decide
                 * after we've got it whether or not it needs to go on a new line.
                 */
                initStringInfo(&mut itembuf);
                (*context).buf = &mut itembuf;

                get_from_clause_item(jtnode, query, context);

                /* Restore context's output buffer */
                (*context).buf = buf;

                /* Consider line-wrapping if enabled */
                if PRETTY_INDENT!(context) && (*context).wrapColumn >= 0 {
                    /* Does the new item start with a new line? */
                    if itembuf.len > 0 && *itembuf.data == b'\n' as i8 {
                        /* If so, we shouldn't add anything */
                        /* instead, remove any trailing spaces currently in buf */
                        removeStringInfoSpaces(buf);
                    } else {
                        /* Locate the start of the current line in the buffer */
                        let trailing_nl = strrchr((*buf).data, b'\n' as i32);
                        let trailing_nl_ptr = if trailing_nl.is_null() {
                            (*buf).data
                        } else {
                            trailing_nl.add(1)
                        };

                        /*
                         * Add a newline, plus some indentation, if the new item
                         * would cause an overflow.
                         */
                        let trailing_len = libc::strlen(trailing_nl_ptr) as i32;
                        if trailing_len + itembuf.len > (*context).wrapColumn {
                            appendContextKeyword(context, b"\0".as_ptr() as _,
                                -(PRETTYINDENT_STD as i32),
                                PRETTYINDENT_STD,
                                PRETTYINDENT_VAR);
                        }
                    }
                }

                /* Add the new item */
                appendBinaryStringInfo(buf, itembuf.data, itembuf.len);

                /* clean up */
                pfree(itembuf.data as *mut _);
            }
            l = lnext!((*(*query).jointree).fromlist, l);
        }
    }
}

fn get_from_clause_item(jtnode: *mut Node, query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;

        if IsA!(jtnode, RangeTblRef) {
            let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
            let rte = rt_fetch(varno, (*query).rtable);
            let colinfo = deparse_columns_fetch(varno, dpns);
            let mut rtfunc1: *mut RangeTblFunction = std::ptr::null_mut();

            if (*rte).lateral {
                appendStringInfoString(buf, b"LATERAL \0".as_ptr() as _);
            }

            /* Print the FROM item proper */
            match (*rte).rtekind {
                RTE_RELATION => {
                    /* Normal relation RTE */
                    appendStringInfo!(buf, "{}{}",
                        ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
                        ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid,
                            (*context).namespaces)).to_string_lossy());
                }
                RTE_SUBQUERY => {
                    /* Subquery RTE */
                    appendStringInfoChar(buf, b'(' as _);
                    get_query_def((*rte).subquery, buf, (*context).namespaces,
                        std::ptr::null_mut(), true,
                        (*context).prettyFlags, (*context).wrapColumn,
                        (*context).indentLevel);
                    appendStringInfoChar(buf, b')' as _);
                }
                RTE_FUNCTION => {
                    /* Function RTE */
                    rtfunc1 = linitial!((*rte).functions) as *mut RangeTblFunction;

                    /*
                     * Omit ROWS FROM() syntax for just one function, unless it
                     * has both a coldeflist and WITH ORDINALITY. If it has both,
                     * we must use ROWS FROM() syntax to avoid ambiguity about
                     * whether the coldeflist includes the ordinality column.
                     */
                    if list_length((*rte).functions) == 1 &&
                        ((*rtfunc1).funccolnames.is_null() || !(*rte).funcordinality)
                    {
                        get_rule_expr_funccall((*rtfunc1).funcexpr, context, true);
                        /* we'll print the coldeflist below, if it has one */
                    } else {
                        /*
                         * If all the function calls in the list are to unnest,
                         * and none need a coldeflist, then collapse the list back
                         * down to UNNEST(args).
                         */
                        let mut all_unnest = true;
                        let mut lc = list_head((*rte).functions);
                        while !lc.is_null() {
                            let rtfunc = crate::current_cell!(lc) as *mut RangeTblFunction;
                            if !IsA!((*rtfunc).funcexpr, FuncExpr) ||
                                (*((*rtfunc).funcexpr as *mut FuncExpr)).funcid != F_UNNEST_ANYARRAY ||
                                !(*rtfunc).funccolnames.is_null()
                            {
                                all_unnest = false;
                                break;
                            }
                            lc = lnext!((*rte).functions, lc);
                        }

                        if all_unnest {
                            let mut allargs: *mut List = std::ptr::null_mut();
                            let mut lc = list_head((*rte).functions);
                            while !lc.is_null() {
                                let rtfunc = crate::current_cell!(lc) as *mut RangeTblFunction;
                                let args = (*((*rtfunc).funcexpr as *mut FuncExpr)).args;
                                allargs = list_concat(allargs, args);
                                lc = lnext!((*rte).functions, lc);
                            }
                            appendStringInfoString(buf, b"UNNEST(\0".as_ptr() as _);
                            get_rule_expr(allargs as *mut Node, context, true);
                            appendStringInfoChar(buf, b')' as _);
                        } else {
                            let mut funcno: i32 = 0;
                            appendStringInfoString(buf, b"ROWS FROM(\0".as_ptr() as _);
                            let mut lc = list_head((*rte).functions);
                            while !lc.is_null() {
                                let rtfunc = crate::current_cell!(lc) as *mut RangeTblFunction;
                                if funcno > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                                get_rule_expr_funccall((*rtfunc).funcexpr, context, true);
                                if !(*rtfunc).funccolnames.is_null() {
                                    /* Reconstruct the column definition list */
                                    appendStringInfoString(buf, b" AS \0".as_ptr() as _);
                                    get_from_clause_coldeflist(rtfunc, std::ptr::null_mut(), context);
                                }
                                funcno += 1;
                                lc = lnext!((*rte).functions, lc);
                            }
                            appendStringInfoChar(buf, b')' as _);
                        }
                        /* prevent printing duplicate coldeflist below */
                        rtfunc1 = std::ptr::null_mut();
                    }
                    if (*rte).funcordinality {
                        appendStringInfoString(buf, b" WITH ORDINALITY\0".as_ptr() as _);
                    }
                }
                RTE_TABLEFUNC => {
                    get_tablefunc((*rte).tablefunc, context, true);
                }
                RTE_VALUES => {
                    /* Values list RTE */
                    appendStringInfoChar(buf, b'(' as _);
                    get_values_def((*rte).values_lists, context);
                    appendStringInfoChar(buf, b')' as _);
                }
                RTE_CTE => {
                    appendStringInfoString(buf, quote_identifier((*rte).ctename));
                }
                _ => {
                    elog!(ERROR, "unrecognized RTE kind: {}", (*rte).rtekind as i32);
                }
            }

            /* Print the relation alias, if needed */
            get_rte_alias(rte, varno, false, context);

            /* Print the column definitions or aliases, if needed */
            if !rtfunc1.is_null() && !(*rtfunc1).funccolnames.is_null() {
                /* Reconstruct the columndef list, which is also the aliases */
                get_from_clause_coldeflist(rtfunc1, colinfo, context);
            } else {
                /* Else print column aliases as needed */
                get_column_alias_list(colinfo, context);
            }

            /* Tablesample clause must go after any alias */
            if (*rte).rtekind == RTE_RELATION && !(*rte).tablesample.is_null() {
                get_tablesample_def((*rte).tablesample, context);
            }
        } else if IsA!(jtnode, JoinExpr) {
            let j = jtnode as *mut JoinExpr;
            let colinfo = deparse_columns_fetch((*j).rtindex, dpns);
            let need_paren_on_right = PRETTY_PAREN!(context) &&
                !IsA!((*j).rarg, RangeTblRef) &&
                !(IsA!((*j).rarg, JoinExpr) && (*((*j).rarg as *mut JoinExpr)).alias != std::ptr::null_mut());

            if !PRETTY_PAREN!(context) || (*j).alias != std::ptr::null_mut() {
                appendStringInfoChar(buf, b'(' as _);
            }

            get_from_clause_item((*j).larg, query, context);

            match (*j).jointype {
                JOIN_INNER => {
                    if !(*j).quals.is_null() {
                        appendContextKeyword(context, b" JOIN \0".as_ptr() as _,
                            -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                    } else {
                        appendContextKeyword(context, b" CROSS JOIN \0".as_ptr() as _,
                            -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                    }
                }
                JOIN_LEFT => {
                    appendContextKeyword(context, b" LEFT JOIN \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                }
                JOIN_FULL => {
                    appendContextKeyword(context, b" FULL JOIN \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                }
                JOIN_RIGHT => {
                    appendContextKeyword(context, b" RIGHT JOIN \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                }
                _ => {
                    elog!(ERROR, "unrecognized join type: {}", (*j).jointype as i32);
                }
            }

            if need_paren_on_right { appendStringInfoChar(buf, b'(' as _); }
            get_from_clause_item((*j).rarg, query, context);
            if need_paren_on_right { appendStringInfoChar(buf, b')' as _); }

            if !(*j).usingClause.is_null() {
                let mut first = true;
                appendStringInfoString(buf, b" USING (\0".as_ptr() as _);
                /* Use the assigned names, not what's in usingClause */
                let mut lc = list_head((*colinfo).usingNames);
                while !lc.is_null() {
                    let colname = crate::current_cell!(lc) as *mut ::std::os::raw::c_char;
                    if first { first = false; }
                    else { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                    appendStringInfoString(buf, quote_identifier(colname));
                    lc = lnext!((*colinfo).usingNames, lc);
                }
                appendStringInfoChar(buf, b')' as _);

                if !(*j).join_using_alias.is_null() {
                    appendStringInfo!(buf, " AS {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier(
                            (*((*j).join_using_alias)).aliasname)).to_string_lossy());
                }
            } else if !(*j).quals.is_null() {
                appendStringInfoString(buf, b" ON \0".as_ptr() as _);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr((*j).quals, context, false);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            } else if (*j).jointype != JOIN_INNER {
                /* If we didn't say CROSS JOIN above, we must provide an ON */
                appendStringInfoString(buf, b" ON TRUE\0".as_ptr() as _);
            }

            if !PRETTY_PAREN!(context) || (*j).alias != std::ptr::null_mut() {
                appendStringInfoChar(buf, b')' as _);
            }

            /* Yes, it's correct to put alias after the right paren ... */
            if (*j).alias != std::ptr::null_mut() {
                /*
                 * Note that it's correct to emit an alias clause if and only if
                 * there was one originally.  Otherwise we'd be converting a named
                 * join to unnamed or vice versa, which creates semantic
                 * subtleties we don't want.  However, we might print a different
                 * alias name than was there originally.
                 */
                appendStringInfo!(buf, " {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(
                        get_rtable_name((*j).rtindex, context))).to_string_lossy());
                get_column_alias_list(colinfo, context);
            }
        } else {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as i32);
        }
    }
}

/*
 * get_rte_alias - print the relation's alias, if needed
 *
 * If printed, the alias is preceded by a space, or by " AS " if use_as is true.
 */
fn get_rte_alias(
    rte: *mut RangeTblEntry,
    varno: i32,
    use_as: bool,
    context: *mut deparse_context,
) {
    unsafe {
        let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
        let refname = get_rtable_name(varno, context);
        let colinfo = deparse_columns_fetch(varno, dpns);
        let mut printalias = false;

        if !(*rte).alias.is_null() {
            /* Always print alias if user provided one */
            printalias = true;
        } else if (*colinfo).printaliases {
            /* Always print alias if we need to print column aliases */
            printalias = true;
        } else if (*rte).rtekind == RTE_RELATION {
            /*
             * No need to print alias if it's same as relation name (this would
             * normally be the case, but not if set_rtable_names had to resolve a
             * conflict).
             */
            if libc::strcmp(refname, get_relation_name((*rte).relid)) != 0 {
                printalias = true;
            }
        } else if (*rte).rtekind == RTE_FUNCTION {
            /*
             * For a function RTE, always print alias.  This covers possible
             * renaming of the function and/or instability of the FigureColname
             * rules for things that aren't simple functions.  Note we'd need to
             * force it anyway for the columndef list case.
             */
            printalias = true;
        } else if (*rte).rtekind == RTE_SUBQUERY || (*rte).rtekind == RTE_VALUES {
            /*
             * For a subquery, always print alias.  This makes the output
             * SQL-spec-compliant, even though we allow such aliases to be omitted
             * on input.
             */
            printalias = true;
        } else if (*rte).rtekind == RTE_CTE {
            /*
             * No need to print alias if it's same as CTE name (this would
             * normally be the case, but not if set_rtable_names had to resolve a
             * conflict).
             */
            if libc::strcmp(refname, (*rte).ctename) != 0 {
                printalias = true;
            }
        }

        if printalias {
            appendStringInfo!((*context).buf, "{}{}",
                if use_as { " AS " } else { " " },
                ::std::ffi::CStr::from_ptr(quote_identifier(refname)).to_string_lossy());
        }
    }
}

/*
 * get_column_alias_list - print column alias list for an RTE
 *
 * Caller must already have printed the relation's alias name.
 */
fn get_column_alias_list(colinfo: *mut deparse_columns, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut first = true;

        /* Don't print aliases if not needed */
        if !(*colinfo).printaliases {
            return;
        }

        for i in 0..(*colinfo).num_new_cols as usize {
            let colname = (*colinfo).new_colnames[i];
            if first {
                appendStringInfoChar(buf, b'(' as _);
                first = false;
            } else {
                appendStringInfoString(buf, b", \0".as_ptr() as _);
            }
            appendStringInfoString(buf, quote_identifier(colname));
        }
        if !first {
            appendStringInfoChar(buf, b')' as _);
        }
    }
}

/*
 * get_from_clause_coldeflist - reproduce FROM clause coldeflist
 *
 * When printing a top-level coldeflist (which is syntactically also the
 * relation's column alias list), use column names from colinfo.  But when
 * printing a coldeflist embedded inside ROWS FROM(), we prefer to use the
 * original coldeflist's names, which are available in rtfunc->funccolnames.
 * Pass NULL for colinfo to select the latter behavior.
 *
 * The coldeflist is appended immediately (no space) to buf.  Caller is
 * responsible for ensuring that an alias or AS is present before it.
 */
fn get_from_clause_coldeflist(
    rtfunc: *mut RangeTblFunction,
    colinfo: *mut deparse_columns,
    context: *mut deparse_context,
) {
    unsafe {
        let buf = (*context).buf;
        let mut i: i32 = 0;

        appendStringInfoChar(buf, b'(' as _);

        let mut l1 = list_head((*rtfunc).funccoltypes);
        let mut l2 = list_head((*rtfunc).funccoltypmods);
        let mut l3 = list_head((*rtfunc).funccolcollations);
        let mut l4 = list_head((*rtfunc).funccolnames);
        while !l1.is_null() {
            let atttypid = lfirst_oid!(l1);
            let atttypmod = lfirst_int!(l2);
            let attcollation = lfirst_oid!(l3);
            let attname: *const ::std::os::raw::c_char = if !colinfo.is_null() {
                (*colinfo).colnames[i as usize]
            } else {
                strVal!(crate::current_cell!(l4) as *mut Node)
            };

            assert!(!attname.is_null()); /* shouldn't be any dropped columns here */

            if i > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            appendStringInfo!(buf, "{} {}",
                ::std::ffi::CStr::from_ptr(quote_identifier(attname)).to_string_lossy(),
                ::std::ffi::CStr::from_ptr(format_type_with_typemod(atttypid, atttypmod)).to_string_lossy());
            if OidIsValid!(attcollation) && attcollation != get_typcollation(atttypid) {
                appendStringInfo!(buf, " COLLATE {}",
                    ::std::ffi::CStr::from_ptr(generate_collation_name(attcollation)).to_string_lossy());
            }

            i += 1;
            l1 = lnext!((*rtfunc).funccoltypes, l1);
            l2 = lnext!((*rtfunc).funccoltypmods, l2);
            l3 = lnext!((*rtfunc).funccolcollations, l3);
            l4 = lnext!((*rtfunc).funccolnames, l4);
        }

        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * get_tablesample_def          - print a TableSampleClause
 */
fn get_tablesample_def(tablesample: *mut TableSampleClause, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let argtypes: [Oid; 1] = [INTERNALOID];
        let mut nargs: i32 = 0;

        /*
         * We should qualify the handler's function name if it wouldn't be
         * resolved by lookup in the current search path.
         */
        appendStringInfo!(buf, " TABLESAMPLE {} (",
            ::std::ffi::CStr::from_ptr(generate_function_name(
                (*tablesample).tsmhandler, 1,
                std::ptr::null_mut(), argtypes.as_ptr() as *mut _,
                false, std::ptr::null_mut(), false)).to_string_lossy());

        let mut l = list_head((*tablesample).args);
        while !l.is_null() {
            if nargs > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            get_rule_expr(crate::current_cell!(l) as *mut Node, context, false);
            nargs += 1;
            l = lnext!((*tablesample).args, l);
        }
        appendStringInfoChar(buf, b')' as _);

        if !(*tablesample).repeatable.is_null() {
            appendStringInfoString(buf, b" REPEATABLE (\0".as_ptr() as _);
            get_rule_expr((*tablesample).repeatable as *mut Node, context, false);
            appendStringInfoChar(buf, b')' as _);
        }
    }
}

/*
 * get_opclass_name           - fetch name of an index operator class
 *
 * The opclass name is appended (after a space) to buf.
 *
 * Output is suppressed if the opclass is the default for the given
 * actual_datatype.  (If you don't want this behavior, just pass
 * InvalidOid for actual_datatype.)
 */
fn get_opclass_name(opclass: Oid, actual_datatype: Oid, buf: *mut StringInfoData) {
    unsafe {
        let ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
        if !HeapTupleIsValid!(ht_opc) {
            elog!(ERROR, "cache lookup failed for opclass {}", opclass);
        }
        let opcrec = GETSTRUCT!(ht_opc) as Form_pg_opclass;

        if !OidIsValid!(actual_datatype) ||
            GetDefaultOpClass(actual_datatype, (*opcrec).opcmethod) != opclass
        {
            /* Okay, we need the opclass name.  Do we need to qualify it? */
            let opcname = NameStr!((*opcrec).opcname);
            if OpclassIsVisible(opclass) {
                appendStringInfo!(buf, " {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(opcname)).to_string_lossy());
            } else {
                let nspname = get_namespace_name_or_temp((*opcrec).opcnamespace);
                appendStringInfo!(buf, " {}.{}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy(),
                    ::std::ffi::CStr::from_ptr(quote_identifier(opcname)).to_string_lossy());
            }
        }
        ReleaseSysCache(ht_opc);
    }
}

/*
 * generate_opclass_name
 *      Compute the name to display for an opclass specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
pub fn generate_opclass_name(opclass: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut buf = StringInfoData {
            data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0,
        };
        initStringInfo(&mut buf);
        get_opclass_name(opclass, InvalidOid, &mut buf);
        buf.data.add(1) /* get_opclass_name() prepends space */
    }
}

/*
 * processIndirection - take care of array and subfield assignment
 *
 * We strip any top-level FieldStore or assignment SubscriptingRef nodes that
 * appear in the input, printing them as decoration for the base column
 * name (which we assume the caller just printed).  We might also need to
 * strip CoerceToDomain nodes, but only ones that appear above assignment
 * nodes.
 *
 * Returns the subexpression that's to be assigned.
 */
fn processIndirection(node: *mut Node, context: *mut deparse_context) -> *mut Node {
    unsafe {
        let buf = (*context).buf;
        let mut cdomain: *mut CoerceToDomain = std::ptr::null_mut();
        let mut node = node;

        loop {
            if node.is_null() { break; }
            if IsA!(node, FieldStore) {
                let fstore = node as *mut FieldStore;
                let mut typrelid: Oid = 0;

                /* lookup tuple type */
                typrelid = get_typ_typrelid((*fstore).resulttype);
                if !OidIsValid!(typrelid) {
                    elog!(ERROR,
                        "argument type {} of FieldStore is not a tuple type",
                        ::std::ffi::CStr::from_ptr(format_type_be((*fstore).resulttype)).to_string_lossy());
                }

                /*
                 * Print the field name.  There should only be one target field in
                 * stored rules.  There could be more than that in executable
                 * target lists, but this function cannot be used for that case.
                 */
                assert!(list_length((*fstore).fieldnums) == 1);
                let fieldname = get_attname(typrelid,
                    linitial_int!((*fstore).fieldnums) as _, false);
                appendStringInfo!(buf, ".{}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(fieldname)).to_string_lossy());

                /*
                 * We ignore arg since it should be an uninteresting reference to
                 * the target column or subcolumn.
                 */
                node = linitial!((*fstore).newvals) as *mut Node;
            } else if IsA!(node, SubscriptingRef) {
                let sbsref = node as *mut SubscriptingRef;
                if (*sbsref).refassgnexpr.is_null() { break; }
                printSubscripts(sbsref, context);
                /*
                 * We ignore refexpr since it should be an uninteresting reference
                 * to the target column or subcolumn.
                 */
                node = (*sbsref).refassgnexpr as *mut Node;
            } else if IsA!(node, CoerceToDomain) {
                cdomain = node as *mut CoerceToDomain;
                /* If it's an explicit domain coercion, we're done */
                if (*cdomain).coercionformat != COERCE_IMPLICIT_CAST { break; }
                /* Tentatively descend past the CoerceToDomain */
                node = (*cdomain).arg as *mut Node;
            } else {
                break;
            }
        }

        /*
         * If we descended past a CoerceToDomain whose argument turned out not to
         * be a FieldStore or array assignment, back up to the CoerceToDomain.
         * (This is not enough to be fully correct if there are nested implicit
         * CoerceToDomains, but such cases shouldn't ever occur.)
         */
        if !cdomain.is_null() && node == (*cdomain).arg as *mut Node {
            node = cdomain as *mut Node;
        }

        node
    }
}

fn printSubscripts(sbsref: *mut SubscriptingRef, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut lowlist_item = list_head((*sbsref).reflowerindexpr); /* could be NULL */
        let mut uplist_item = list_head((*sbsref).refupperindexpr);
        while !uplist_item.is_null() {
            appendStringInfoChar(buf, b'[' as _);
            if !lowlist_item.is_null() {
                /* If subexpression is NULL, get_rule_expr prints nothing */
                get_rule_expr(crate::current_cell!(lowlist_item) as *mut Node, context, false);
                appendStringInfoChar(buf, b':' as _);
                lowlist_item = lnext!((*sbsref).reflowerindexpr, lowlist_item);
            }
            /* If subexpression is NULL, get_rule_expr prints nothing */
            get_rule_expr(crate::current_cell!(uplist_item) as *mut Node, context, false);
            appendStringInfoChar(buf, b']' as _);
            uplist_item = lnext!((*sbsref).refupperindexpr, uplist_item);
        }
    }
}

/*
 * quote_identifier           - Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
pub fn quote_identifier(ident: *const ::std::os::raw::c_char) -> *const ::std::os::raw::c_char {
    unsafe {
        /*
         * Can avoid quoting if ident starts with a lowercase letter or underscore
         * and contains only lowercase letters, digits, and underscores, *and* is
         * not any SQL keyword.  Otherwise, supply quotes.
         *
         * would like to use <ctype.h> macros here, but they might yield unwanted
         * locale-specific results...
         */
        let mut nquotes: i32 = 0;
        let mut safe: bool;
        let mut ptr: *const u8 = ident as _;
        let c0 = *ptr;
        safe = (c0 >= b'a' && c0 <= b'z') || c0 == b'_';

        while *ptr != 0 {
            let ch = *ptr;
            if !((ch >= b'a' && ch <= b'z') || (ch >= b'0' && ch <= b'9') || ch == b'_') {
                safe = false;
                if ch == b'"' { nquotes += 1; }
            }
            ptr = ptr.add(1);
        }

        if quote_all_identifiers { safe = false; }

        if safe {
            /*
             * Check for keyword.  We quote keywords except for unreserved ones.
             * (In some cases we could avoid quoting a col_name or type_func_name
             * keyword, but it seems much harder than it's worth to tell that.)
             *
             * Note: ScanKeywordLookup() does case-insensitive comparison, but
             * that's fine, since we already know we have all-lower-case.
             */
            let kwnum = ScanKeywordLookup(ident, &ScanKeywords);
            if kwnum >= 0 && ScanKeywordCategories[kwnum as usize] != UNRESERVED_KEYWORD as u8 {
                safe = false;
            }
        }

        if safe {
            return ident; /* no change needed */
        }

        let identlen = libc::strlen(ident);
        let result = palloc(identlen + nquotes as usize + 2 + 1) as *mut u8;

        let mut optr = result;
        *optr = b'"'; optr = optr.add(1);
        let mut ptr: *const u8 = ident as _;
        while *ptr != 0 {
            let ch = *ptr;
            if ch == b'"' { *optr = b'"'; optr = optr.add(1); }
            *optr = ch; optr = optr.add(1);
            ptr = ptr.add(1);
        }
        *optr = b'"'; optr = optr.add(1);
        *optr = 0;

        result as *const ::std::os::raw::c_char
    }
}

/*
 * quote_qualified_identifier  - Quote a possibly-qualified identifier
 *
 * Return a name of the form qualifier.ident, or just ident if qualifier
 * is NULL, quoting each component if necessary.  The result is palloc'd.
 */
pub fn quote_qualified_identifier(
    qualifier: *const ::std::os::raw::c_char,
    ident: *const ::std::os::raw::c_char,
) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut buf = StringInfoData {
            data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0,
        };
        initStringInfo(&mut buf);
        if !qualifier.is_null() {
            appendStringInfo!((&mut buf), "{}.",
                ::std::ffi::CStr::from_ptr(quote_identifier(qualifier)).to_string_lossy());
        }
        appendStringInfoString(&mut buf, quote_identifier(ident));
        buf.data
    }
}

/*
 * get_relation_name
 *      Get the unqualified name of a relation specified by OID
 *
 * This differs from the underlying get_rel_name() function in that it will
 * throw error instead of silently returning NULL if the OID is bad.
 */
fn get_relation_name(relid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let relname = get_rel_name(relid);
        if relname.is_null() {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        relname
    }
}

/*
 * generate_relation_name
 *      Compute the name to display for a relation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 *
 * If namespaces isn't NIL, it must be a list of deparse_namespace nodes.
 * We will forcibly qualify the relation name if it equals any CTE name
 * visible in the namespace list.
 */
fn generate_relation_name(relid: Oid, namespaces: *mut List) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let reltup = GETSTRUCT!(tp) as Form_pg_class;
        let relname = NameStr!((*reltup).relname);

        /* Check for conflicting CTE name */
        let mut need_qual = false;
        let mut nslist = list_head(namespaces);
        'outer: while !nslist.is_null() {
            let dpns = crate::current_cell!(nslist) as *mut deparse_namespace;
            let mut ctlist = list_head((*dpns).ctes);
            while !ctlist.is_null() {
                let cte = crate::current_cell!(ctlist) as *mut CommonTableExpr;
                if libc::strcmp((*cte).ctename, relname) == 0 {
                    need_qual = true;
                    break 'outer;
                }
                ctlist = lnext!((*dpns).ctes, ctlist);
            }
            nslist = lnext!(namespaces, nslist);
        }

        /* Otherwise, qualify the name if not visible in search path */
        if !need_qual {
            need_qual = !RelationIsVisible(relid);
        }

        let nspname = if need_qual {
            get_namespace_name_or_temp((*reltup).relnamespace)
        } else {
            std::ptr::null_mut()
        };

        let result = quote_qualified_identifier(nspname, relname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * generate_qualified_relation_name
 *      Compute the name to display for a relation specified by OID
 *
 * As above, but unconditionally schema-qualify the name.
 */
fn generate_qualified_relation_name(relid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let reltup = GETSTRUCT!(tp) as Form_pg_class;
        let relname = NameStr!((*reltup).relname);
        let nspname = get_namespace_name_or_temp((*reltup).relnamespace);
        if nspname.is_null() {
            elog!(ERROR, "cache lookup failed for namespace {}", (*reltup).relnamespace);
        }
        let result = quote_qualified_identifier(nspname, relname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * generate_function_name
 *      Compute the name to display for a function specified by OID,
 *      given that it is being called with the specified actual arg names and
 *      types.  (Those matter because of ambiguous-function resolution rules.)
 *
 * If we're dealing with a potentially variadic function (in practice, this
 * means a FuncExpr or Aggref, not some other way of calling a function), then
 * has_variadic must specify whether variadic arguments have been merged,
 * and *use_variadic_p will be set to indicate whether to print VARIADIC in
 * the output.  For non-FuncExpr cases, has_variadic should be false and
 * use_variadic_p can be NULL.
 *
 * inGroupBy must be true if we're deparsing a GROUP BY clause.
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
fn generate_function_name(
    funcid: Oid,
    nargs: i32,
    argnames: *mut List,
    argtypes: *mut Oid,
    has_variadic: bool,
    use_variadic_p: *mut bool,
    in_group_by: bool,
) -> *mut ::std::os::raw::c_char {
    unsafe {
        let proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
        if !HeapTupleIsValid!(proctup) {
            elog!(ERROR, "cache lookup failed for function {}", funcid);
        }
        let procform = GETSTRUCT!(proctup) as Form_pg_proc;
        let proname = NameStr!((*procform).proname);
        let mut force_qualify = false;

        /*
         * Due to parser hacks to avoid needing to reserve CUBE, we need to force
         * qualification of some function names within GROUP BY.
         */
        if in_group_by {
            let s = ::std::ffi::CStr::from_ptr(proname).to_bytes();
            if s == b"cube" || s == b"rollup" {
                force_qualify = true;
            }
        }

        /*
         * Determine whether VARIADIC should be printed.  We must do this first
         * since it affects the lookup rules in func_get_detail().
         *
         * We always print VARIADIC if the function has a merged variadic-array
         * argument.  Note that this is always the case for functions taking a
         * VARIADIC argument type other than VARIADIC ANY.  If we omitted VARIADIC
         * and printed the array elements as separate arguments, the call could
         * match a newer non-VARIADIC function.
         */
        let use_variadic: bool;
        if !use_variadic_p.is_null() {
            /* Parser should not have set funcvariadic unless fn is variadic */
            assert!(!has_variadic || OidIsValid!((*procform).provariadic));
            use_variadic = has_variadic;
            *use_variadic_p = use_variadic;
        } else {
            assert!(!has_variadic);
            use_variadic = false;
        }

        /*
         * The idea here is to schema-qualify only if the parser would fail to
         * resolve the correct function given the unqualified func name with the
         * specified argtypes and VARIADIC flag.  But if we already decided to
         * force qualification, then we can skip the lookup and pretend we didn't
         * find it.
         */
        let mut p_funcid: Oid = 0;
        let mut p_rettype: Oid = 0;
        let mut p_retset = false;
        let mut p_nvargs: i32 = 0;
        let mut p_vatype: Oid = 0;
        let mut p_true_typeids: *mut Oid = std::ptr::null_mut();
        let p_result: FuncDetailCode;

        if !force_qualify {
            p_result = func_get_detail(
                list_make1(makeString(proname)),
                std::ptr::null_mut(), argnames, nargs, argtypes,
                !use_variadic, true, false,
                &mut p_funcid, &mut p_rettype,
                &mut p_retset, &mut p_nvargs, &mut p_vatype,
                &mut p_true_typeids, std::ptr::null_mut());
        } else {
            p_result = FUNCDETAIL_NOTFOUND;
            p_funcid = InvalidOid;
        }

        let nspname = if (p_result == FUNCDETAIL_NORMAL ||
             p_result == FUNCDETAIL_AGGREGATE ||
             p_result == FUNCDETAIL_WINDOWFUNC) &&
            p_funcid == funcid
        {
            std::ptr::null_mut()
        } else {
            get_namespace_name_or_temp((*procform).pronamespace)
        };

        let result = quote_qualified_identifier(nspname, proname);
        ReleaseSysCache(proctup);
        result
    }
}

/*
 * generate_operator_name
 *      Compute the name to display for an operator specified by OID,
 *      given that it is being called with the specified actual arg types.
 *      (Arg types matter because of ambiguous-operator resolution rules.
 *      Pass InvalidOid for unused arg of a unary operator.)
 *
 * The result includes all necessary quoting and schema-prefixing,
 * plus the OPERATOR() decoration needed to use a qualified operator name
 * in an expression.
 */
fn generate_operator_name(operid: Oid, arg1: Oid, arg2: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut buf = StringInfoData { data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0 };
        initStringInfo(&mut buf);

        let opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
        if !HeapTupleIsValid!(opertup) {
            elog!(ERROR, "cache lookup failed for operator {}", operid);
        }
        let operform = GETSTRUCT!(opertup) as Form_pg_operator;
        let oprname = NameStr!((*operform).oprname);

        /*
         * The idea here is to schema-qualify only if the parser would fail to
         * resolve the correct operator given the unqualified op name with the
         * specified argtypes.
         */
        let p_result: Operator = match (*operform).oprkind as u8 {
            b'b' => oper(std::ptr::null_mut(),
                list_make1(makeString(oprname)), arg1, arg2, true, -1),
            b'l' => left_oper(std::ptr::null_mut(),
                list_make1(makeString(oprname)), arg2, true, -1),
            _ => {
                elog!(ERROR, "unrecognized oprkind: {}", (*operform).oprkind as i32);
                std::ptr::null_mut()
            }
        };

        let nspname: *mut ::std::os::raw::c_char = if !p_result.is_null() && oprid(p_result) == operid {
            std::ptr::null_mut()
        } else {
            let ns = get_namespace_name_or_temp((*operform).oprnamespace);
            appendStringInfo!((&mut buf), "OPERATOR({}.{}",
                ::std::ffi::CStr::from_ptr(quote_identifier(ns)).to_string_lossy(),
                ::std::ffi::CStr::from_ptr(oprname).to_string_lossy());
            ns /* not actually used below since we already appended */
        };

        if nspname.is_null() {
            appendStringInfoString(&mut buf, oprname);
        }

        if !nspname.is_null() {
            appendStringInfoChar(&mut buf, b')' as _);
        }

        if !p_result.is_null() { ReleaseSysCache(p_result); }
        ReleaseSysCache(opertup);

        buf.data
    }
}

/*
 * generate_operator_clause --- generate a binary-operator WHERE clause
 *
 * This is used for internally-generated-and-executed SQL queries, where
 * precision is essential and readability is secondary.  The basic
 * requirement is to append "leftop op rightop" to buf, where leftop and
 * rightop are given as strings and are assumed to yield types leftoptype
 * and rightoptype; the operator is identified by OID.  The complexity
 * comes from needing to be sure that the parser will select the desired
 * operator when the query is parsed.  We always name the operator using
 * OPERATOR(schema.op) syntax, so as to avoid search-path uncertainties.
 * We have to emit casts too, if either input isn't already the input type
 * of the operator; else we are at the mercy of the parser's heuristics for
 * ambiguous-operator resolution.  The caller must ensure that leftop and
 * rightop are suitable arguments for a cast operation; it's best to insert
 * parentheses if they aren't just variables or parameters.
 */
pub fn generate_operator_clause(
    buf: *mut StringInfoData,
    leftop: *const ::std::os::raw::c_char,
    leftoptype: Oid,
    opoid: Oid,
    rightop: *const ::std::os::raw::c_char,
    rightoptype: Oid,
) {
    unsafe {
        let opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
        if !HeapTupleIsValid!(opertup) {
            elog!(ERROR, "cache lookup failed for operator {}", opoid);
        }
        let operform = GETSTRUCT!(opertup) as Form_pg_operator;
        assert!((*operform).oprkind == b'b' as i8);
        let oprname = NameStr!((*operform).oprname);
        let nspname = get_namespace_name((*operform).oprnamespace);

        appendStringInfoString(buf, leftop);
        if leftoptype != (*operform).oprleft { add_cast_to(buf, (*operform).oprleft); }
        appendStringInfo!(buf, " OPERATOR({}.{}",
            ::std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(oprname).to_string_lossy());
        appendStringInfo!(buf, ") {}", ::std::ffi::CStr::from_ptr(rightop).to_string_lossy());
        if rightoptype != (*operform).oprright { add_cast_to(buf, (*operform).oprright); }

        ReleaseSysCache(opertup);
    }
}

/*
 * Add a cast specification to buf.  We spell out the type name the hard way,
 * intentionally not using format_type_be().  This is to avoid corner cases
 * for CHARACTER, BIT, and perhaps other types, where specifying the type
 * using SQL-standard syntax results in undesirable data truncation.  By
 * doing it this way we can be certain that the cast will have default (-1)
 * target typmod.
 */
fn add_cast_to(buf: *mut StringInfoData, typid: Oid) {
    unsafe {
        let typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid!(typetup) {
            elog!(ERROR, "cache lookup failed for type {}", typid);
        }
        let typform = GETSTRUCT!(typetup) as Form_pg_type;
        let typname = NameStr!((*typform).typname);
        let nspname = get_namespace_name_or_temp((*typform).typnamespace);
        appendStringInfo!(buf, "::{}.{}",
            ::std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(quote_identifier(typname)).to_string_lossy());
        ReleaseSysCache(typetup);
    }
}

/*
 * generate_qualified_type_name
 *      Compute the name to display for a type specified by OID
 *
 * This is different from format_type_be() in that we unconditionally
 * schema-qualify the name.  That also means no special syntax for
 * SQL-standard type names ... although in current usage, this should
 * only get used for domains, so such cases wouldn't occur anyway.
 */
fn generate_qualified_type_name(typid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for type {}", typid);
        }
        let typtup = GETSTRUCT!(tp) as Form_pg_type;
        let typname = NameStr!((*typtup).typname);
        let nspname = get_namespace_name_or_temp((*typtup).typnamespace);
        if nspname.is_null() {
            elog!(ERROR, "cache lookup failed for namespace {}", (*typtup).typnamespace);
        }
        let result = quote_qualified_identifier(nspname, typname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * generate_collation_name
 *      Compute the name to display for a collation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
pub fn generate_collation_name(collid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for collation {}", collid);
        }
        let colltup = GETSTRUCT!(tp) as Form_pg_collation;
        let collname = NameStr!((*colltup).collname);
        let nspname = if !CollationIsVisible(collid) {
            get_namespace_name_or_temp((*colltup).collnamespace)
        } else {
            std::ptr::null_mut()
        };
        let result = quote_qualified_identifier(nspname, collname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * Given a C string, produce a TEXT datum.
 *
 * We assume that the input was palloc'd and may be freed.
 */
fn string_to_text(str_: *mut ::std::os::raw::c_char) -> *mut text {
    unsafe {
        let result = cstring_to_text(str_);
        pfree(str_ as *mut _);
        result
    }
}

/*
 * Generate a C string representing a relation options from text[] datum.
 */
fn get_reloptions(buf: *mut StringInfoData, reloptions: Datum) {
    unsafe {
        let mut options: *mut Datum = std::ptr::null_mut();
        let mut noptions: i32 = 0;

        deconstruct_array_builtin(DatumGetArrayTypeP!(reloptions), TEXTOID,
            &mut options, std::ptr::null_mut(), &mut noptions);

        for i in 0..noptions as usize {
            let option = TextDatumGetCString(*options.add(i));
            let name = option;
            let separator = libc::strchr(option, b'=' as i32);
            let value: *const ::std::os::raw::c_char;
            if !separator.is_null() {
                *separator = 0;
                value = separator.add(1);
            } else {
                value = b"\0".as_ptr() as _;
            }

            if i > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            appendStringInfo!(buf, "{}=",
                ::std::ffi::CStr::from_ptr(quote_identifier(name)).to_string_lossy());

            /*
             * In general we need to quote the value; but to avoid unnecessary
             * clutter, do not quote if it is an identifier that would not need
             * quoting.
             */
            if quote_identifier(value) == value {
                appendStringInfoString(buf, value);
            } else {
                simple_quote_literal(buf, value);
            }

            pfree(option as *mut _);
        }
    }
}

/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 */
fn flatten_reloptions(relid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut result: *mut ::std::os::raw::c_char = std::ptr::null_mut();
        let tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid!(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let mut isnull = false;
        let reloptions = SysCacheGetAttr(RELOID, tuple,
            Anum_pg_class_reloptions as _, &mut isnull);
        if !isnull {
            let mut buf = StringInfoData { data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0 };
            initStringInfo(&mut buf);
            get_reloptions(&mut buf, reloptions);
            result = buf.data;
        }
        ReleaseSysCache(tuple);
        result
    }
}

/*
 * get_range_partbound_string
 *      A C string representation of one range partition bound
 */
pub fn get_range_partbound_string(bound_datums: *mut List) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut context: deparse_context = std::mem::zeroed();
        let buf = makeStringInfo();
        context.buf = buf;

        appendStringInfoChar(buf, b'(' as _);
        let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
        let mut cell = list_head(bound_datums);
        while !cell.is_null() {
            let datum = crate::current_cell!(cell) as *mut PartitionRangeDatum;
            appendStringInfoString(buf, sep);
            if (*datum).kind == PARTITION_RANGE_DATUM_MINVALUE {
                appendStringInfoString(buf, b"MINVALUE\0".as_ptr() as _);
            } else if (*datum).kind == PARTITION_RANGE_DATUM_MAXVALUE {
                appendStringInfoString(buf, b"MAXVALUE\0".as_ptr() as _);
            } else {
                let val = (*datum).value as *mut Const;
                get_const_expr(val, &mut context, -1);
            }
            sep = b", \0".as_ptr() as _;
            cell = lnext!(bound_datums, cell);
        }
        appendStringInfoChar(buf, b')' as _);

        (*buf).data
    }
}

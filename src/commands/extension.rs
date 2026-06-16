/*-------------------------------------------------------------------------
 *
 * extension.c
 *    Commands to manipulate extensions
 *
 * Extensions in PostgreSQL allow management of collections of SQL objects.
 *
 * All we need internally to manage an extension is an OID so that the
 * dependent objects can be associated with it.  An extension is created by
 * populating the pg_extension catalog from a "control" file.
 * The extension control file is parsed with the same parser we use for
 * postgresql.conf.  An extension also has an installation script file,
 * containing SQL commands to create the extension's objects.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/commands/extension.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{foreach, current_cell, makeNode};

use std::ffi::{c_char, c_int, c_uint, c_void};
use core::ptr::{null, null_mut};

// ---------------------------------------------------------------------------
// Type aliases and stub types for unported dependencies.
// ---------------------------------------------------------------------------

/// TODO(pg-port): postgres_ext.h
pub use crate::postgres_ext::Oid;
pub use crate::postgres::Datum;

// nodes
use crate::nodes::nodes::{Node, NodeTag, ParseLoc};
use crate::nodes::pg_list::List;
use crate::nodes::pg_list::lfirst;
use crate::catalog::objectaccess::ObjectAddress;

// Heap tuple
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};

/// TODO(pg-port): access/relation.h
type Relation = *mut c_void;
/// TODO(pg-port): access/genam.h
type SysScanDesc = *mut c_void;
use crate::access::common::scankey::ScanKeyData;
/// TODO(pg-port): catalog/pg_extension.h
type Form_pg_extension = *mut c_void;
/// TODO(pg-port): catalog/pg_depend.h
type Form_pg_depend = *mut c_void;
/// TODO(pg-port): utils/tqual.h / utils/snapmgr.h
type Snapshot = *mut c_void;
/// TODO(pg-port): tcop/dest.h
type DestReceiver = *mut c_void;
/// TODO(pg-port): tcop/pquery.h
type QueryDesc = *mut c_void;
/// TODO(pg-port): tcop/utility.h
type ProcessUtilityContext = c_int;
const PROCESS_UTILITY_QUERY: ProcessUtilityContext = 0;
/// TODO(pg-port): executor/executor.h
type ForwardScanDirection_t = c_int;
const ForwardScanDirection: ForwardScanDirection_t = 1;
/// TODO(pg-port): nodes/plannodes.h (PlannedStmt/RawStmt stubs)
type PlannedStmt = c_void;
type RawStmt = c_void;
/// TODO(pg-port): nodes/parsenodes.h
type ParseState = *mut c_void;
type DefElem = c_void;
type CreateExtensionStmt = c_void;
type AlterExtensionStmt = c_void;
type AlterExtensionContentsStmt = c_void;
type CreateSchemaStmt = c_void;
type TransactionStmt = c_void;
/// TODO(pg-port): nodes/value.h
type String_t = c_void;
/// TODO(pg-port): catalog stubs
type Tuplestorestate = *mut c_void;
type TupleDesc = *mut c_void;
type ArrayType = c_void;
type ObjectAddresses = *mut c_void;
/// TODO(pg-port): access/htup_details.h form_pg_*
/// TODO(pg-port): utils/array.h
/// TODO(pg-port): funcapi.h
type ReturnSetInfo = c_void;

// AclResult
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;
// AclMode flags
type AclMode = u64;
const ACL_CREATE: AclMode = 1 << 11;
// ObjectType enum (subset of parsenodes.h)
type ObjectType = c_int;
const OBJECT_DATABASE: ObjectType   = 0;
const OBJECT_EXTENSION: ObjectType  = 1;
const OBJECT_INDEX: ObjectType      = 2;
const OBJECT_PUBLICATION: ObjectType = 3;
const OBJECT_ROLE: ObjectType       = 4;
const OBJECT_STATISTIC_EXT: ObjectType = 5;
const OBJECT_SUBSCRIPTION: ObjectType = 6;
const OBJECT_TABLESPACE: ObjectType  = 7;
const OBJECT_SCHEMA: ObjectType     = 8;

// DependencyType
type DependencyType = c_char;
const DEPENDENCY_NORMAL: DependencyType = b'n' as c_char;
const DEPENDENCY_EXTENSION: DependencyType = b'e' as c_char;

// LockMode constants
type LOCKMODE = c_int;
const AccessShareLock: LOCKMODE     = 1;
const RowExclusiveLock: LOCKMODE    = 3;
const ShareUpdateExclusiveLock: LOCKMODE = 4;
const NoLock: LOCKMODE              = 0;

// GUC enums
type GucContext = c_int;
const PGC_USERSET: GucContext = 0;
const PGC_SUSET: GucContext   = 1;
type GucSource = c_int;
const PGC_S_SESSION: GucSource = 0;
type GucAction = c_int;
const GUC_ACTION_SAVE: GucAction = 0;

// SysCacheIdentifier
type SysCacheIdentifier = c_int;
const EXTENSIONNAME: SysCacheIdentifier = 55;
const EXTENSIONOID: SysCacheIdentifier  = 56;

// CURSOR_OPT flags
const CURSOR_OPT_PARALLEL_OK: c_int = 0x0400;

// Oid constants
const InvalidOid: Oid             = 0;
const DatabaseRelationId: Oid     = 1262;
const ExtensionRelationId: Oid    = 3079;
const NamespaceRelationId: Oid    = 2615;
const ProcedureRelationId: Oid    = 1255;
const RelationRelationId: Oid     = 1259;
const TypeRelationId: Oid         = 1247;
const DependRelationId: Oid       = 2608;
const NAMEOID: Oid                = 19;
const OIDOID: Oid                 = 26;
const TEXTOID: Oid                = 25;
const BOOTSTRAP_SUPERUSERID: Oid  = 10;
const C_COLLATION_OID: Oid        = 950;

// Index OIDs
const ExtensionOidIndexId: Oid     = 3080;
const ExtensionNameIndexId: Oid    = 3081;
const DependReferenceIndexId: Oid  = 2678;

// scan strategy
const BTEqualStrategyNumber: c_int = 3;

// Attribute numbers for pg_extension
const Anum_pg_extension_oid: c_int           = 1;
const Anum_pg_extension_extname: c_int       = 2;
const Anum_pg_extension_extowner: c_int      = 3;
const Anum_pg_extension_extnamespace: c_int  = 4;
const Anum_pg_extension_extrelocatable: c_int = 5;
const Anum_pg_extension_extversion: c_int    = 6;
const Anum_pg_extension_extconfig: c_int     = 7;
const Anum_pg_extension_extcondition: c_int  = 8;
const Natts_pg_extension: usize              = 8;

// Attribute numbers for pg_depend
const Anum_pg_depend_refclassid: c_int  = 5;
const Anum_pg_depend_refobjid: c_int    = 6;
const Anum_pg_depend_classid: c_int     = 1;
const Anum_pg_depend_objid: c_int       = 2;
const Anum_pg_depend_objsubid: c_int    = 3;
const Anum_pg_depend_deptype: c_int     = 8;

// Attribute numbers for pg_namespace
const Anum_pg_namespace_oid: c_int = 1;

// OidIsValid macro
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

// InvalidObjectAddress
static InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: 0,
    objectId: 0,
    objectSubId: 0,
};

// CONF_FILE_START_DEPTH
const CONF_FILE_START_DEPTH: c_int = 0;

// XactFlags
const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: u32 = 0x0001;

// SECURITY_LOCAL_USERID_CHANGE
const SECURITY_LOCAL_USERID_CHANGE: c_int = 0x0001;

// PG_BINARY_R
const PG_BINARY_R: &[u8] = b"rb\0";

// TYPALIGN
const TYPALIGN_INT: c_char = b'i' as c_char;

// DestNone
const DestNone: c_int = 0;

// ---------------------------------------------------------------------------
// ConfigVariable (parse_config stub)
// ---------------------------------------------------------------------------
/// TODO(pg-port): guc_tables.h ConfigVariable
#[repr(C)]
pub struct ConfigVariable {
    pub name: *mut c_char,
    pub value: *mut c_char,
    pub errmsg: *mut c_char,
    pub filename: *mut c_char,
    pub sourceline: c_int,
    pub ignore: bool,
    pub applied: bool,
    pub next: *mut ConfigVariable,
}

// ---------------------------------------------------------------------------
// DynamicFileList (dfmgr.h stub)
// ---------------------------------------------------------------------------
/// TODO(pg-port): utils/dynamic_loader.h DynamicFileList
#[repr(C)]
pub struct DynamicFileList {
    _opaque: c_void,
}

// ---------------------------------------------------------------------------
// Globally visible state variables (GUC + extension tracking)
// ---------------------------------------------------------------------------

/// GUC: extension_control_path
pub static mut Extension_control_path: *mut c_char = null_mut();

/// True while executing CREATE/ALTER EXTENSION scripts
pub static mut creating_extension: bool = false;

/// OID of the extension currently being created (InvalidOid otherwise)
pub static mut CurrentExtensionObject: Oid = InvalidOid;

// ---------------------------------------------------------------------------
// Internal structs
// ---------------------------------------------------------------------------

/// Internal data structure to hold the results of parsing a control file
#[repr(C)]
pub struct ExtensionControlFile {
    pub name: *mut c_char,           /* name of the extension */
    pub basedir: *mut c_char,        /* base directory where control and script
                                      * files are located */
    pub control_dir: *mut c_char,    /* directory where control file was found */
    pub directory: *mut c_char,      /* directory for script files */
    pub default_version: *mut c_char, /* default install target version, if any */
    pub module_pathname: *mut c_char, /* string to substitute for MODULE_PATHNAME */
    pub comment: *mut c_char,        /* comment, if any */
    pub schema: *mut c_char,         /* target schema (allowed if !relocatable) */
    pub relocatable: bool,           /* is ALTER EXTENSION SET SCHEMA supported? */
    pub superuser: bool,             /* must be superuser to install? */
    pub trusted: bool,               /* allow becoming superuser on the fly? */
    pub encoding: c_int,             /* encoding of the script file, or -1 */
    pub requires: *mut List,         /* names of prerequisite extensions */
    pub no_relocate: *mut List,      /* names of prerequisite extensions that
                                      * should not be relocated */
}

/// Internal data structure for update path information
#[repr(C)]
pub struct ExtensionVersionInfo {
    pub name: *mut c_char,           /* name of the starting version */
    pub reachable: *mut List,        /* List of ExtensionVersionInfo's */
    pub installable: bool,           /* does this version have an install script? */
    /* working state for Dijkstra's algorithm: */
    pub distance_known: bool,        /* is distance from start known yet? */
    pub distance: c_int,             /* current worst-case distance estimate */
    pub previous: *mut ExtensionVersionInfo, /* current best predecessor */
}

/// Information for script_error_callback()
#[repr(C)]
pub struct script_error_callback_arg {
    pub sql: *const c_char,           /* entire script file contents */
    pub filename: *const c_char,      /* script file pathname */
    pub stmt_location: ParseLoc,      /* current stmt start loc, or -1 if unknown */
    pub stmt_len: ParseLoc,           /* length in bytes; 0 means "rest of string" */
}

/// Cache structure for get_function_sibling_type (and maybe later, allied lookup functions).
#[repr(C)]
pub struct ExtensionSiblingCache {
    pub next: *mut ExtensionSiblingCache, /* list link */
    /* lookup key: requesting function's OID and type name */
    pub reqfuncoid: Oid,
    pub typname: *const c_char,
    pub valid: bool,                  /* is entry currently valid? */
    pub exthash: u32,                 /* cache hash of owning extension's OID */
    pub typeoid: Oid,                 /* OID associated with typname */
}

/* Head of linked list of ExtensionSiblingCache structs */
static mut ext_sibling_list: *mut ExtensionSiblingCache = null_mut();

// ---------------------------------------------------------------------------
// ErrorContextCallback stub
// ---------------------------------------------------------------------------
/// TODO(pg-port): utils/error/elog.h ErrorContextCallback
#[repr(C)]
pub struct ErrorContextCallback {
    pub callback: unsafe fn(*mut c_void),
    pub arg: *mut c_void,
    pub previous: *mut ErrorContextCallback,
}

// ---------------------------------------------------------------------------
// Unported function stubs (TODO(pg-port))
// ---------------------------------------------------------------------------

unsafe fn GetSysCacheOid1(cacheId: SysCacheIdentifier, oidcolumn: c_int, key1: Datum) -> Oid {
    let _ = (cacheId, oidcolumn, key1);
    unimplemented!("TODO(pg-port): GetSysCacheOid1")
}
unsafe fn SearchSysCache1(cacheId: SysCacheIdentifier, key1: Datum) -> HeapTuple {
    let _ = (cacheId, key1);
    unimplemented!("TODO(pg-port): SearchSysCache1")
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    let _ = tuple;
    unimplemented!("TODO(pg-port): ReleaseSysCache")
}
unsafe fn getExtensionOfObject(classId: Oid, objectId: Oid) -> Oid {
    let _ = (classId, objectId);
    unimplemented!("TODO(pg-port): getExtensionOfObject")
}
unsafe fn getExtensionType(extoid: Oid, typname: *const c_char) -> Oid {
    let _ = (extoid, typname);
    unimplemented!("TODO(pg-port): getExtensionType")
}
unsafe fn CacheRegisterSyscacheCallback(
    cacheid: SysCacheIdentifier,
    func: unsafe fn(Datum, c_int, u32),
    arg: Datum,
) {
    let _ = (cacheid, func, arg);
    unimplemented!("TODO(pg-port): CacheRegisterSyscacheCallback")
}
unsafe fn CacheMemoryContext_fn() -> MemoryContext {
    unimplemented!("TODO(pg-port): CacheMemoryContext")
}
unsafe fn GetSysCacheHashValue1(cacheId: SysCacheIdentifier, key1: Datum) -> u32 {
    let _ = (cacheId, key1);
    unimplemented!("TODO(pg-port): GetSysCacheHashValue1")
}
unsafe fn NameStr_fn(name: *const c_void) -> *mut c_char {
    let _ = name;
    unimplemented!("TODO(pg-port): NameStr")
}
unsafe fn get_share_path(exec_path: *const c_char, ret_path: *mut c_char) {
    let _ = (exec_path, ret_path);
    unimplemented!("TODO(pg-port): get_share_path")
}
unsafe fn my_exec_path_fn() -> *const c_char {
    unimplemented!("TODO(pg-port): my_exec_path")
}
unsafe fn psprintf(fmt: *const c_char) -> *mut c_char {
    let _ = fmt;
    unimplemented!("TODO(pg-port): psprintf")
}
unsafe fn psprintf2(fmt: *const c_char, a: *const c_char) -> *mut c_char {
    let _ = (fmt, a);
    unimplemented!("TODO(pg-port): psprintf2")
}
unsafe fn psprintf3(fmt: *const c_char, a: *const c_char, b: *const c_char) -> *mut c_char {
    let _ = (fmt, a, b);
    unimplemented!("TODO(pg-port): psprintf3")
}
unsafe fn first_path_var_separator(path: *mut c_char) -> *mut c_char {
    let _ = path;
    unimplemented!("TODO(pg-port): first_path_var_separator")
}
unsafe fn substitute_path_macro(
    path: *mut c_char,
    macro_name: *const c_char,
    value: *const c_char,
) -> *mut c_char {
    let _ = (path, macro_name, value);
    unimplemented!("TODO(pg-port): substitute_path_macro")
}
unsafe fn canonicalize_path(path: *mut c_char) {
    let _ = path;
    unimplemented!("TODO(pg-port): canonicalize_path")
}
unsafe fn is_absolute_path(path: *const c_char) -> bool {
    let _ = path;
    unimplemented!("TODO(pg-port): is_absolute_path")
}
unsafe fn first_dir_separator(path: *const c_char) -> *mut c_char {
    let _ = path;
    unimplemented!("TODO(pg-port): first_dir_separator")
}
unsafe fn last_dir_separator(path: *const c_char) -> *mut c_char {
    let _ = path;
    unimplemented!("TODO(pg-port): last_dir_separator")
}
unsafe fn AllocateFile(filename: *const c_char, mode: *const c_char) -> *mut c_void {
    let _ = (filename, mode);
    unimplemented!("TODO(pg-port): AllocateFile")
}
unsafe fn FreeFile(file: *mut c_void) {
    let _ = file;
    unimplemented!("TODO(pg-port): FreeFile")
}
unsafe fn ParseConfigFp(
    fp: *mut c_void,
    filename: *const c_char,
    depth: c_int,
    elevel: c_int,
    head: *mut *mut ConfigVariable,
    tail: *mut *mut ConfigVariable,
) -> bool {
    let _ = (fp, filename, depth, elevel, head, tail);
    unimplemented!("TODO(pg-port): ParseConfigFp")
}
unsafe fn FreeConfigVariables(head: *mut ConfigVariable) {
    let _ = head;
    unimplemented!("TODO(pg-port): FreeConfigVariables")
}
unsafe fn parse_bool(value: *const c_char, result: *mut bool) -> bool {
    let _ = (value, result);
    unimplemented!("TODO(pg-port): parse_bool")
}
unsafe fn pg_valid_server_encoding(name: *const c_char) -> c_int {
    let _ = name;
    unimplemented!("TODO(pg-port): pg_valid_server_encoding")
}
unsafe fn SplitIdentifierString(
    rawstring: *mut c_char,
    separator: c_char,
    namelist: *mut *mut List,
) -> bool {
    let _ = (rawstring, separator, namelist);
    unimplemented!("TODO(pg-port): SplitIdentifierString")
}
unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!("TODO(pg-port): GetDatabaseEncoding")
}
unsafe fn pg_verify_mbstr(
    encoding: c_int,
    mbstr: *const c_char,
    len: c_int,
    noError: bool,
) -> bool {
    let _ = (encoding, mbstr, len, noError);
    unimplemented!("TODO(pg-port): pg_verify_mbstr")
}
unsafe fn pg_any_to_server(
    s: *const c_char,
    len: c_int,
    encoding: c_int,
) -> *mut c_char {
    let _ = (s, len, encoding);
    unimplemented!("TODO(pg-port): pg_any_to_server")
}
unsafe fn geterrposition() -> c_int {
    unimplemented!("TODO(pg-port): geterrposition")
}
unsafe fn CleanQuerytext(
    query: *const c_char,
    location: *mut c_int,
    len: *mut c_int,
) -> *const c_char {
    let _ = (query, location, len);
    unimplemented!("TODO(pg-port): CleanQuerytext")
}
unsafe fn errposition(pos: c_int) -> c_int {
    let _ = pos;
    unimplemented!("TODO(pg-port): errposition")
}
unsafe fn internalerrposition(pos: c_int) -> c_int {
    let _ = pos;
    unimplemented!("TODO(pg-port): internalerrposition")
}
unsafe fn internalerrquery(query: *const c_char) -> c_int {
    let _ = query;
    unimplemented!("TODO(pg-port): internalerrquery")
}
unsafe fn errcontext_fn(msg: *const c_char) -> c_int {
    let _ = msg;
    unimplemented!("TODO(pg-port): errcontext")
}
unsafe fn error_context_stack_fn() -> *mut ErrorContextCallback {
    unimplemented!("TODO(pg-port): error_context_stack")
}
unsafe fn pg_parse_query(query_string: *const c_char) -> *mut List {
    let _ = query_string;
    unimplemented!("TODO(pg-port): pg_parse_query")
}
unsafe fn CreateDestReceiver(dest: c_int) -> *mut c_void {
    let _ = dest;
    unimplemented!("TODO(pg-port): CreateDestReceiver")
}
unsafe fn AllocSetContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    minContextSize: usize,
    initBlockSize: usize,
    maxBlockSize: usize,
) -> MemoryContext {
    let _ = (parent, name, minContextSize, initBlockSize, maxBlockSize);
    unimplemented!("TODO(pg-port): AllocSetContextCreate")
}
unsafe fn CommandCounterIncrement() {
    unimplemented!("TODO(pg-port): CommandCounterIncrement")
}
unsafe fn pg_analyze_and_rewrite_fixedparams(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    paramTypes: *const Oid,
    numParams: c_int,
    queryEnv: *mut c_void,
) -> *mut List {
    let _ = (parsetree, query_string, paramTypes, numParams, queryEnv);
    unimplemented!("TODO(pg-port): pg_analyze_and_rewrite_fixedparams")
}
unsafe fn pg_plan_queries(
    querys: *mut List,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: *mut c_void,
) -> *mut List {
    let _ = (querys, query_string, cursorOptions, boundParams);
    unimplemented!("TODO(pg-port): pg_plan_queries")
}
unsafe fn PushActiveSnapshot(snap: Snapshot) {
    let _ = snap;
    unimplemented!("TODO(pg-port): PushActiveSnapshot")
}
unsafe fn PopActiveSnapshot() {
    unimplemented!("TODO(pg-port): PopActiveSnapshot")
}
unsafe fn GetTransactionSnapshot() -> Snapshot {
    unimplemented!("TODO(pg-port): GetTransactionSnapshot")
}
unsafe fn GetActiveSnapshot() -> Snapshot {
    unimplemented!("TODO(pg-port): GetActiveSnapshot")
}
unsafe fn CreateQueryDesc(
    plannedstmt: *mut PlannedStmt,
    sourceText: *const c_char,
    snapshot: Snapshot,
    crossCheckSnapshot: Snapshot,
    dest: *mut c_void,
    params: *mut c_void,
    queryEnv: *mut c_void,
    instrument_options: c_int,
) -> *mut QueryDesc {
    let _ = (plannedstmt, sourceText, snapshot, crossCheckSnapshot, dest, params, queryEnv, instrument_options);
    unimplemented!("TODO(pg-port): CreateQueryDesc")
}
unsafe fn ExecutorStart(queryDesc: *mut QueryDesc, eflags: c_int) {
    let _ = (queryDesc, eflags);
    unimplemented!("TODO(pg-port): ExecutorStart")
}
unsafe fn ExecutorRun(queryDesc: *mut QueryDesc, direction: ForwardScanDirection_t, count: u64) {
    let _ = (queryDesc, direction, count);
    unimplemented!("TODO(pg-port): ExecutorRun")
}
unsafe fn ExecutorFinish(queryDesc: *mut QueryDesc) {
    let _ = queryDesc;
    unimplemented!("TODO(pg-port): ExecutorFinish")
}
unsafe fn ExecutorEnd(queryDesc: *mut QueryDesc) {
    let _ = queryDesc;
    unimplemented!("TODO(pg-port): ExecutorEnd")
}
unsafe fn FreeQueryDesc(queryDesc: *mut QueryDesc) {
    let _ = queryDesc;
    unimplemented!("TODO(pg-port): FreeQueryDesc")
}
unsafe fn ProcessUtility(
    pstmt: *mut PlannedStmt,
    queryString: *const c_char,
    readOnlyTree: bool,
    context: ProcessUtilityContext,
    params: *mut c_void,
    queryEnv: *mut c_void,
    dest: *mut c_void,
    qc: *mut c_void,
) {
    let _ = (pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
    unimplemented!("TODO(pg-port): ProcessUtility")
}
unsafe fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: AclMode) -> AclResult {
    let _ = (classid, objectid, roleid, mode);
    unimplemented!("TODO(pg-port): object_aclcheck")
}
unsafe fn superuser() -> bool {
    unimplemented!("TODO(pg-port): superuser")
}
unsafe fn GetUserId() -> Oid {
    unimplemented!("TODO(pg-port): GetUserId")
}
unsafe fn GetUserIdAndSecContext(userid: *mut Oid, sec_context: *mut c_int) {
    let _ = (userid, sec_context);
    unimplemented!("TODO(pg-port): GetUserIdAndSecContext")
}
unsafe fn SetUserIdAndSecContext(userid: Oid, sec_context: c_int) {
    let _ = (userid, sec_context);
    unimplemented!("TODO(pg-port): SetUserIdAndSecContext")
}
unsafe fn NewGUCNestLevel() -> c_int {
    unimplemented!("TODO(pg-port): NewGUCNestLevel")
}
unsafe fn set_config_option(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
    action: GucAction,
    changeVal: bool,
    elevel: c_int,
    is_reload: bool,
) -> c_int {
    let _ = (name, value, context, source, action, changeVal, elevel, is_reload);
    unimplemented!("TODO(pg-port): set_config_option")
}
unsafe fn set_config_option_ext(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
    roleOid: Oid,
    action: GucAction,
    changeVal: bool,
    elevel: c_int,
    is_reload: bool,
) -> c_int {
    let _ = (name, value, context, source, roleOid, action, changeVal, elevel, is_reload);
    unimplemented!("TODO(pg-port): set_config_option_ext")
}
unsafe fn client_min_messages_fn() -> c_int {
    unimplemented!("TODO(pg-port): client_min_messages")
}
unsafe fn log_min_messages_fn() -> c_int {
    unimplemented!("TODO(pg-port): log_min_messages")
}
unsafe fn check_function_bodies_fn() -> bool {
    unimplemented!("TODO(pg-port): check_function_bodies")
}
unsafe fn AtEOXact_GUC(isCommit: bool, nestLevel: c_int) {
    let _ = (isCommit, nestLevel);
    unimplemented!("TODO(pg-port): AtEOXact_GUC")
}
unsafe fn quote_identifier(ident: *const c_char) -> *const c_char {
    let _ = ident;
    unimplemented!("TODO(pg-port): quote_identifier")
}
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char {
    let _ = nspid;
    unimplemented!("TODO(pg-port): get_namespace_name")
}
unsafe fn get_namespace_oid(nspname: *const c_char, missing_ok: bool) -> Oid {
    let _ = (nspname, missing_ok);
    unimplemented!("TODO(pg-port): get_namespace_oid")
}
unsafe fn isTempNamespace(nsoid: Oid) -> bool {
    let _ = nsoid;
    unimplemented!("TODO(pg-port): isTempNamespace")
}
unsafe fn MyXactFlags_fn() -> *mut u32 {
    unimplemented!("TODO(pg-port): MyXactFlags")
}
unsafe fn fetch_search_path(includeImplicit: bool) -> *mut List {
    let _ = includeImplicit;
    unimplemented!("TODO(pg-port): fetch_search_path")
}
unsafe fn linitial_oid(list: *mut List) -> Oid {
    let _ = list;
    unimplemented!("TODO(pg-port): linitial_oid")
}
unsafe fn list_free(list: *mut List) {
    let _ = list;
    unimplemented!("TODO(pg-port): list_free")
}
unsafe fn list_copy(list: *mut List) -> *mut List {
    let _ = list;
    unimplemented!("TODO(pg-port): list_copy")
}
unsafe fn list_length(list: *const List) -> c_int {
    let _ = list;
    unimplemented!("TODO(pg-port): list_length")
}
unsafe fn list_member(list: *const List, datum: *const c_void) -> bool {
    let _ = (list, datum);
    unimplemented!("TODO(pg-port): list_member")
}
unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List {
    let _ = (list, datum);
    unimplemented!("TODO(pg-port): lappend")
}
unsafe fn lappend_oid(list: *mut List, datum: Oid) -> *mut List {
    let _ = (list, datum);
    unimplemented!("TODO(pg-port): lappend_oid")
}
unsafe fn lcons(datum: *mut c_void, list: *mut List) -> *mut List {
    let _ = (datum, list);
    unimplemented!("TODO(pg-port): lcons")
}
unsafe fn lfirst_void(cell: *mut c_void) -> *mut c_void {
    let _ = cell;
    unimplemented!("TODO(pg-port): lfirst")
}
unsafe fn lfirst_oid(cell: *mut c_void) -> Oid {
    let _ = cell;
    unimplemented!("TODO(pg-port): lfirst_oid")
}
unsafe fn makeString(str_: *mut c_char) -> *mut String_t {
    let _ = str_;
    unimplemented!("TODO(pg-port): makeString")
}
unsafe fn table_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    let _ = (relid, lockmode);
    unimplemented!("TODO(pg-port): table_open")
}
unsafe fn table_close(rel: Relation, lockmode: LOCKMODE) {
    let _ = (rel, lockmode);
    unimplemented!("TODO(pg-port): table_close")
}
unsafe fn relation_close(rel: Relation, lockmode: LOCKMODE) {
    let _ = (rel, lockmode);
    unimplemented!("TODO(pg-port): relation_close")
}
unsafe fn GetNewOidWithIndex(
    rel: Relation,
    indexid: Oid,
    oidcolno: c_int,
) -> Oid {
    let _ = (rel, indexid, oidcolno);
    unimplemented!("TODO(pg-port): GetNewOidWithIndex")
}
unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}
unsafe fn CStringGetDatum(str_: *const c_char) -> Datum {
    str_ as Datum
}
unsafe fn BoolGetDatum(b: bool) -> Datum {
    b as Datum
}
unsafe fn PointerGetDatum(ptr: *const c_void) -> Datum {
    ptr as Datum
}
unsafe fn DatumGetArrayTypeP(datum: Datum) -> *mut ArrayType {
    datum as *mut ArrayType
}
unsafe fn CStringGetTextDatum(str_: *const c_char) -> Datum {
    let _ = str_;
    unimplemented!("TODO(pg-port): CStringGetTextDatum")
}
unsafe fn DirectFunctionCall1(func: unsafe fn(Datum) -> Datum, arg1: Datum) -> Datum {
    func(arg1)
}
unsafe fn namein(arg: Datum) -> Datum {
    let _ = arg;
    unimplemented!("TODO(pg-port): namein")
}
unsafe fn DirectFunctionCall3Coll(
    func: unsafe fn(Datum, Datum, Datum) -> Datum,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
) -> Datum {
    let _ = collation;
    func(arg1, arg2, arg3)
}
unsafe fn replace_text(arg1: Datum, arg2: Datum, arg3: Datum) -> Datum {
    let _ = (arg1, arg2, arg3);
    unimplemented!("TODO(pg-port): replace_text")
}
unsafe fn DirectFunctionCall4Coll(
    func: unsafe fn(Datum, Datum, Datum, Datum) -> Datum,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> Datum {
    let _ = collation;
    func(arg1, arg2, arg3, arg4)
}
unsafe fn textregexreplace(arg1: Datum, arg2: Datum, arg3: Datum, arg4: Datum) -> Datum {
    let _ = (arg1, arg2, arg3, arg4);
    unimplemented!("TODO(pg-port): textregexreplace")
}
unsafe fn text_to_cstring(datum: Datum) -> *mut c_char {
    let _ = datum;
    unimplemented!("TODO(pg-port): text_to_cstring")
}
unsafe fn DatumGetTextPP(datum: Datum) -> Datum {
    datum
}
unsafe fn strpbrk(s: *const c_char, accept: *const c_char) -> *mut c_char {
    let _ = (s, accept);
    unimplemented!("TODO(pg-port): strpbrk")
}
unsafe fn GetUserNameFromId(userid: Oid, noerr: bool) -> *const c_char {
    let _ = (userid, noerr);
    unimplemented!("TODO(pg-port): GetUserNameFromId")
}
unsafe fn ScanKeyInit(
    entry: *mut ScanKeyData,
    attributeNumber: c_int,
    strategy: c_int,
    procedure: Oid,
    argument: Datum,
) {
    let _ = (entry, attributeNumber, strategy, procedure, argument);
    unimplemented!("TODO(pg-port): ScanKeyInit")
}
const F_OIDEQ: Oid  = 184;
const F_NAMEEQ: Oid = 180;
unsafe fn systable_beginscan(
    heapRelation: Relation,
    indexId: Oid,
    indexOK: bool,
    snapshot: Snapshot,
    nkeys: c_int,
    key: *mut ScanKeyData,
) -> SysScanDesc {
    let _ = (heapRelation, indexId, indexOK, snapshot, nkeys, key);
    unimplemented!("TODO(pg-port): systable_beginscan")
}
unsafe fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple {
    let _ = sysscan;
    unimplemented!("TODO(pg-port): systable_getnext")
}
unsafe fn systable_endscan(sysscan: SysScanDesc) {
    let _ = sysscan;
    unimplemented!("TODO(pg-port): systable_endscan")
}
unsafe fn heap_form_tuple(
    tupleDescriptor: TupleDesc,
    values: *mut Datum,
    nulls: *mut bool,
) -> HeapTuple {
    let _ = (tupleDescriptor, values, nulls);
    unimplemented!("TODO(pg-port): heap_form_tuple")
}
unsafe fn heap_copytuple(tuple: HeapTuple) -> HeapTuple {
    let _ = tuple;
    unimplemented!("TODO(pg-port): heap_copytuple")
}
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple {
    let _ = (tuple, tupleDesc, replValues, replIsnull, doReplace);
    unimplemented!("TODO(pg-port): heap_modify_tuple")
}
unsafe fn heap_freetuple(tuple: HeapTuple) {
    let _ = tuple;
    unimplemented!("TODO(pg-port): heap_freetuple")
}
unsafe fn heap_getattr(tuple: HeapTuple, attnum: c_int, tupleDesc: TupleDesc, isnull: *mut bool) -> Datum {
    let _ = (tuple, attnum, tupleDesc, isnull);
    unimplemented!("TODO(pg-port): heap_getattr")
}
unsafe fn CatalogTupleInsert(rel: Relation, tuple: HeapTuple) {
    let _ = (rel, tuple);
    unimplemented!("TODO(pg-port): CatalogTupleInsert")
}
unsafe fn CatalogTupleDelete(rel: Relation, tid: *const c_void) {
    let _ = (rel, tid);
    unimplemented!("TODO(pg-port): CatalogTupleDelete")
}
unsafe fn CatalogTupleUpdate(rel: Relation, otid: *mut c_void, tup: HeapTuple) {
    let _ = (rel, otid, tup);
    unimplemented!("TODO(pg-port): CatalogTupleUpdate")
}
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    let _ = rel;
    unimplemented!("TODO(pg-port): RelationGetDescr")
}
unsafe fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) {
    let _ = (classId, objectId, owner);
    unimplemented!("TODO(pg-port): recordDependencyOnOwner")
}
unsafe fn new_object_addresses() -> ObjectAddresses {
    unimplemented!("TODO(pg-port): new_object_addresses")
}
unsafe fn add_exact_object_address(object: *const ObjectAddress, addrs: ObjectAddresses) {
    let _ = (object, addrs);
    unimplemented!("TODO(pg-port): add_exact_object_address")
}
unsafe fn record_object_address_dependencies(
    depender: *const ObjectAddress,
    referenced: ObjectAddresses,
    behavior: DependencyType,
) {
    let _ = (depender, referenced, behavior);
    unimplemented!("TODO(pg-port): record_object_address_dependencies")
}
unsafe fn free_object_addresses(addrs: ObjectAddresses) {
    let _ = addrs;
    unimplemented!("TODO(pg-port): free_object_addresses")
}
unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) {
    let _ = (classId, objectId, subId);
    unimplemented!("TODO(pg-port): InvokeObjectPostCreateHook")
}
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {
    let _ = (classId, objectId, subId);
    unimplemented!("TODO(pg-port): InvokeObjectPostAlterHook")
}
unsafe fn CreateComments(oid: Oid, classoid: Oid, subid: i32, comment: *const c_char) {
    let _ = (oid, classoid, subid, comment);
    unimplemented!("TODO(pg-port): CreateComments")
}
unsafe fn InsertExtensionTuple_dep(
    extName: *const c_char,
    extOwner: Oid,
    schemaOid: Oid,
    relocatable: bool,
    extVersion: *const c_char,
    extConfig: Datum,
    extCondition: Datum,
    requiredExtensions: *mut List,
) -> ObjectAddress {
    let _ = (extName, extOwner, schemaOid, relocatable, extVersion, extConfig, extCondition, requiredExtensions);
    unimplemented!("TODO(pg-port): InsertExtensionTuple")
}
unsafe fn CreateSchemaCommand(stmt: *mut CreateSchemaStmt, queryString: *const c_char, stmt_location: c_int, stmt_len: c_int) {
    let _ = (stmt, queryString, stmt_location, stmt_len);
    unimplemented!("TODO(pg-port): CreateSchemaCommand")
}
unsafe fn LookupCreationNamespace(nspname: *const c_char) -> Oid {
    let _ = nspname;
    unimplemented!("TODO(pg-port): LookupCreationNamespace")
}
unsafe fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool {
    let _ = (classid, objectid, roleid);
    unimplemented!("TODO(pg-port): object_ownercheck")
}
unsafe fn aclcheck_error(aclerr: AclResult, objtype: ObjectType, objectname: *const c_char) {
    let _ = (aclerr, objtype, objectname);
    unimplemented!("TODO(pg-port): aclcheck_error")
}
unsafe fn changeDependencyFor(
    classId: Oid,
    objectId: Oid,
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> c_int {
    let _ = (classId, objectId, refClassId, oldRefObjectId, newRefObjectId);
    unimplemented!("TODO(pg-port): changeDependencyFor")
}
unsafe fn deleteDependencyRecordsForClass(
    classId: Oid,
    objectId: Oid,
    refclassId: Oid,
    deptype: DependencyType,
) -> c_int {
    let _ = (classId, objectId, refclassId, deptype);
    unimplemented!("TODO(pg-port): deleteDependencyRecordsForClass")
}
unsafe fn recordDependencyOn(
    depender: *const ObjectAddress,
    referenced: *const ObjectAddress,
    behavior: DependencyType,
) {
    let _ = (depender, referenced, behavior);
    unimplemented!("TODO(pg-port): recordDependencyOn")
}
unsafe fn get_object_address(
    objtype: ObjectType,
    object: *mut Node,
    relation: *mut Relation,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> ObjectAddress {
    let _ = (objtype, object, relation, lockmode, missing_ok);
    unimplemented!("TODO(pg-port): get_object_address")
}
unsafe fn check_object_ownership(
    roleid: Oid,
    objtype: ObjectType,
    object: ObjectAddress,
    objname: *mut Node,
    relation: Relation,
) {
    let _ = (roleid, objtype, object, objname, relation);
    unimplemented!("TODO(pg-port): check_object_ownership")
}
unsafe fn get_rel_name(relid: Oid) -> *mut c_char {
    let _ = relid;
    unimplemented!("TODO(pg-port): get_rel_name")
}
/* get_extension_schema and get_extension_name are defined later in this file */
unsafe fn AlterObjectNamespace_oid(
    classId: Oid,
    objId: Oid,
    nspOid: Oid,
    objsMoved: ObjectAddresses,
) -> Oid {
    let _ = (classId, objId, nspOid, objsMoved);
    unimplemented!("TODO(pg-port): AlterObjectNamespace_oid")
}
unsafe fn getObjectDescription(object: *const ObjectAddress, missing_ok: bool) -> *mut c_char {
    let _ = (object, missing_ok);
    unimplemented!("TODO(pg-port): getObjectDescription")
}
unsafe fn recordExtObjInitPriv(objectId: Oid, classId: Oid) {
    let _ = (objectId, classId);
    unimplemented!("TODO(pg-port): recordExtObjInitPriv")
}
unsafe fn removeExtObjInitPriv(objectId: Oid, classId: Oid) {
    let _ = (objectId, classId);
    unimplemented!("TODO(pg-port): removeExtObjInitPriv")
}
unsafe fn get_array_type(typid: Oid) -> Oid {
    let _ = typid;
    unimplemented!("TODO(pg-port): get_array_type")
}
unsafe fn type_is_range(typid: Oid) -> bool {
    let _ = typid;
    unimplemented!("TODO(pg-port): type_is_range")
}
unsafe fn get_range_multirange(rangeOid: Oid) -> Oid {
    let _ = rangeOid;
    unimplemented!("TODO(pg-port): get_range_multirange")
}
unsafe fn format_type_be(typid: Oid) -> *mut c_char {
    let _ = typid;
    unimplemented!("TODO(pg-port): format_type_be")
}
unsafe fn get_rel_type_id(relid: Oid) -> Oid {
    let _ = relid;
    unimplemented!("TODO(pg-port): get_rel_type_id")
}
unsafe fn construct_array_builtin(
    elems: *mut Datum,
    nelems: c_int,
    elmtype: Oid,
) -> *mut ArrayType {
    let _ = (elems, nelems, elmtype);
    unimplemented!("TODO(pg-port): construct_array_builtin")
}
unsafe fn deconstruct_array_builtin(
    array: *mut ArrayType,
    elmtype: Oid,
    elemsp: *mut *mut Datum,
    nullsp: *mut *mut bool,
    nelemsp: *mut c_int,
) {
    let _ = (array, elmtype, elemsp, nullsp, nelemsp);
    unimplemented!("TODO(pg-port): deconstruct_array_builtin")
}
unsafe fn array_set(
    array: *mut ArrayType,
    nSubscripts: c_int,
    indx: *mut c_int,
    dataValue: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> *mut ArrayType {
    let _ = (array, nSubscripts, indx, dataValue, isNull, arraytyplen, elmlen, elmbyval, elmalign);
    unimplemented!("TODO(pg-port): array_set")
}
unsafe fn ARR_DIMS(arr: *mut ArrayType) -> *mut c_int {
    let _ = arr;
    unimplemented!("TODO(pg-port): ARR_DIMS")
}
unsafe fn ARR_NDIM(arr: *mut ArrayType) -> c_int {
    let _ = arr;
    unimplemented!("TODO(pg-port): ARR_NDIM")
}
unsafe fn ARR_LBOUND(arr: *mut ArrayType) -> *mut c_int {
    let _ = arr;
    unimplemented!("TODO(pg-port): ARR_LBOUND")
}
unsafe fn ARR_HASNULL(arr: *mut ArrayType) -> bool {
    let _ = arr;
    unimplemented!("TODO(pg-port): ARR_HASNULL")
}
unsafe fn ARR_ELEMTYPE(arr: *mut ArrayType) -> Oid {
    let _ = arr;
    unimplemented!("TODO(pg-port): ARR_ELEMTYPE")
}
unsafe fn ARR_DATA_PTR(arr: *mut ArrayType) -> *mut u8 {
    let _ = arr;
    unimplemented!("TODO(pg-port): ARR_DATA_PTR")
}
unsafe fn InitMaterializedSRF(fcinfo: *mut c_void, flags: c_int) {
    let _ = (fcinfo, flags);
    unimplemented!("TODO(pg-port): InitMaterializedSRF")
}
unsafe fn tuplestore_putvalues(
    state: Tuplestorestate,
    tdesc: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) {
    let _ = (state, tdesc, values, isnull);
    unimplemented!("TODO(pg-port): tuplestore_putvalues")
}
unsafe fn AllocateDir(dirname: *const c_char) -> *mut c_void {
    let _ = dirname;
    unimplemented!("TODO(pg-port): AllocateDir")
}
unsafe fn ReadDir(dir: *mut c_void, dirname: *const c_char) -> *mut dirent {
    let _ = (dir, dirname);
    unimplemented!("TODO(pg-port): ReadDir")
}
unsafe fn FreeDir(dir: *mut c_void) {
    let _ = dir;
    unimplemented!("TODO(pg-port): FreeDir")
}
unsafe fn errorConflictingDefElem(defel: *const DefElem, pstate: ParseState) {
    let _ = (defel, pstate);
    unimplemented!("TODO(pg-port): errorConflictingDefElem")
}
unsafe fn defGetString(defel: *const DefElem) -> *mut c_char {
    let _ = defel;
    unimplemented!("TODO(pg-port): defGetString")
}
unsafe fn defGetBoolean(defel: *const DefElem) -> bool {
    let _ = defel;
    unimplemented!("TODO(pg-port): defGetBoolean")
}
unsafe fn strVal(node: *const c_void) -> *mut c_char {
    let _ = node;
    unimplemented!("TODO(pg-port): strVal")
}
unsafe fn pg_file_exists(path: *const c_char) -> bool {
    let _ = path;
    unimplemented!("TODO(pg-port): pg_file_exists")
}
unsafe fn get_first_loaded_module() -> *mut DynamicFileList {
    unimplemented!("TODO(pg-port): get_first_loaded_module")
}
unsafe fn get_next_loaded_module(current: *mut DynamicFileList) -> *mut DynamicFileList {
    let _ = current;
    unimplemented!("TODO(pg-port): get_next_loaded_module")
}
unsafe fn get_loaded_module_details(
    module: *mut DynamicFileList,
    library_path: *mut *const c_char,
    module_name: *mut *const c_char,
    module_version: *mut *const c_char,
) {
    let _ = (module, library_path, module_name, module_version);
    unimplemented!("TODO(pg-port): get_loaded_module_details")
}
unsafe fn MyDatabaseId_fn() -> Oid {
    unimplemented!("TODO(pg-port): MyDatabaseId")
}
unsafe fn pnstrdup(str_: *const c_char, n: usize) -> *mut c_char {
    let _ = (str_, n);
    unimplemented!("TODO(pg-port): pnstrdup")
}

// dirent stub
#[repr(C)]
pub struct dirent {
    pub d_name: [c_char; 256],
}

// StringInfoData stub (lib/stringinfo.h)
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
unsafe fn initStringInfo(str_: *mut StringInfoData) {
    let _ = str_;
    unimplemented!("TODO(pg-port): initStringInfo")
}
unsafe fn appendStringInfoString(str_: *mut StringInfoData, s: *const c_char) {
    let _ = (str_, s);
    unimplemented!("TODO(pg-port): appendStringInfoString")
}
unsafe fn appendStringInfo(str_: *mut StringInfoData, fmt: *const c_char, arg: *const c_char) {
    let _ = (str_, fmt, arg);
    unimplemented!("TODO(pg-port): appendStringInfo")
}

// MAXPGPATH
const MAXPGPATH: usize = 1024;

// stat stub
#[repr(C)]
pub struct stat {
    pub st_size: i64,
    // ... other fields elided
}
unsafe fn stat_fn(path: *const c_char, buf: *mut stat) -> c_int {
    let _ = (path, buf);
    unimplemented!("TODO(pg-port): stat")
}

// strrchr/strstr/strncmp/strcmp/strlen/strlcpy wrappers to libc
use core::ffi::CStr;
unsafe fn strrchr_fn(s: *const c_char, c: c_char) -> *mut c_char {
    libc_strrchr(s, c as c_int)
}
unsafe fn strstr_fn(haystack: *const c_char, needle: *const c_char) -> *mut c_char {
    libc_strstr(haystack, needle)
}
unsafe fn strncmp_fn(s1: *const c_char, s2: *const c_char, n: usize) -> c_int {
    libc_strncmp(s1, s2, n)
}

extern "C" {
    fn libc_strrchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn libc_strstr(haystack: *const c_char, needle: *const c_char) -> *mut c_char;
    fn libc_strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
}

// Rename libc functions to avoid clash
unsafe fn c_strcmp(s1: *const c_char, s2: *const c_char) -> c_int {
    libc_strcmp(s1, s2)
}
unsafe fn c_strlen(s: *const c_char) -> usize {
    libc_strlen(s)
}
unsafe fn c_strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int {
    libc_strncmp(s1, s2, n)
}
unsafe fn c_strstr(haystack: *const c_char, needle: *const c_char) -> *mut c_char {
    libc_strstr(haystack, needle)
}
unsafe fn c_strrchr(s: *const c_char, c: c_char) -> *mut c_char {
    libc_strrchr(s, c as c_int)
}
unsafe fn c_strlcpy(dst: *mut c_char, src: *const c_char, size: usize) -> usize {
    libc_strlcpy(dst, src, size)
}
unsafe fn c_fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize {
    libc_fread(ptr, size, nmemb, stream)
}
unsafe fn c_ferror(stream: *mut c_void) -> c_int {
    libc_ferror(stream)
}
unsafe fn c_memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void {
    libc_memset(s, c, n)
}
unsafe fn c_memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    libc_memcpy(dst, src, n)
}

extern "C" {
    fn libc_strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn libc_strlen(s: *const c_char) -> usize;
    fn libc_strlcpy(dst: *mut c_char, src: *const c_char, size: usize) -> usize;
    fn libc_fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn libc_ferror(stream: *mut c_void) -> c_int;
    fn libc_memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn libc_memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// errno
unsafe fn errno_fn() -> c_int {
    *libc_errno_location()
}
const ENOENT: c_int = 2;
extern "C" {
    fn libc_errno_location() -> *mut c_int;
}

// snprintf stub
macro_rules! snprintf {
    ($buf:expr, $n:expr, $fmt:expr) => {{
        use std::fmt::Write as _;
        // no-op stub; real impl calls libc snprintf via unimplemented
        let _ = ($buf, $n, $fmt);
        unimplemented!("TODO(pg-port): snprintf")
    }};
}

// ObjectAddressSet macro equivalent
#[inline]
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class: Oid, oid: Oid) {
    addr.classId = class;
    addr.objectId = oid;
    addr.objectSubId = 0;
}
// -------------------------  END PART 1  ----------------------------------

// =========================================================================
// PART 2: get_extension_oid, get_extension_name, get_extension_schema,
//         get_function_sibling_type, ext_sibling_callback,
//         check_valid_extension_name, check_valid_version_name,
//         is_extension_control_filename, is_extension_script_filename,
//         get_extension_control_directories, find_extension_control_filename,
//         get_extension_script_directory,
//         get_extension_aux_control_filename, get_extension_script_filename,
//         parse_extension_control_file, read_extension_control_file,
//         read_extension_aux_control_file
// =========================================================================

/*
 * get_extension_oid - given an extension name, look up the OID
 *
 * If missing_ok is false, throw an error if extension name not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_extension_oid(extname: *const c_char, missing_ok: bool) -> Oid {
    let result: Oid;

    result = GetSysCacheOid1(
        EXTENSIONNAME,
        Anum_pg_extension_oid,
        CStringGetDatum(extname),
    );

    if !OidIsValid(result) && !missing_ok {
        ereport!(
            ERROR,
            errmsg!(
                "extension \"{}\" does not exist",
                std::ffi::CStr::from_ptr(extname).to_string_lossy()
            )
        );
        /* errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    result
}

/*
 * get_extension_name - given an extension OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such extension.
 */
pub unsafe fn get_extension_name(ext_oid: Oid) -> *mut c_char {
    let result: *mut c_char;
    let tuple: HeapTuple;

    tuple = SearchSysCache1(EXTENSIONOID, ObjectIdGetDatum(ext_oid));

    if !HeapTupleIsValid(tuple) {
        return null_mut();
    }

    let form = GETSTRUCT(tuple) as Form_pg_extension;
    /* NameStr(((Form_pg_extension) GETSTRUCT(tuple))->extname) */
    result = pstrdup(NameStr_fn(form as *const c_void));
    ReleaseSysCache(tuple);

    result
}

/*
 * get_extension_schema - given an extension OID, fetch its extnamespace
 *
 * Returns InvalidOid if no such extension.
 */
pub unsafe fn get_extension_schema(ext_oid: Oid) -> Oid {
    let result: Oid;
    let tuple: HeapTuple;

    tuple = SearchSysCache1(EXTENSIONOID, ObjectIdGetDatum(ext_oid));

    if !HeapTupleIsValid(tuple) {
        return InvalidOid;
    }

    let form = GETSTRUCT(tuple) as *mut pg_extension_form;
    result = (*form).extnamespace;
    ReleaseSysCache(tuple);

    result
}

/* Local stub for Form_pg_extension field access */
#[repr(C)]
struct pg_extension_form {
    oid: Oid,
    extname: [c_char; 64],   /* NameData */
    extowner: Oid,
    extnamespace: Oid,
    extrelocatable: bool,
    /* extversion, extconfig, extcondition omitted */
}
#[repr(C)]
struct pg_extension_form_full {
    oid: Oid,
    extname: [c_char; 64],
    extowner: Oid,
    extnamespace: Oid,
    extrelocatable: bool,
}

/*
 * get_function_sibling_type - find a type belonging to same extension as func
 *
 * Returns the type's OID, or InvalidOid if not found.
 *
 * This is useful in extensions, which won't have fixed object OIDs.
 * We work from the calling function's own OID, which it can get from its
 * FunctionCallInfo parameter, and look up the owning extension and thence
 * a type belonging to the same extension.
 *
 * Notice that the type is specified by name only, without a schema.
 * That's because this will typically be used by relocatable extensions
 * which can't make a-priori assumptions about which schema their objects
 * are in.  As long as the extension only defines one type of this name,
 * the answer is unique anyway.
 *
 * We might later add the ability to look up functions, operators, etc.
 *
 * This code is simply a frontend for some pg_depend lookups.  Those lookups
 * are fairly expensive, so we provide a simple cache facility.  We assume
 * that the passed typname is actually a C constant, or at least permanently
 * allocated, so that we need not copy that string.
 */
pub unsafe fn get_function_sibling_type(funcoid: Oid, typname: *const c_char) -> Oid {
    let mut cache_entry: *mut ExtensionSiblingCache;
    let extoid: Oid;
    let typeoid: Oid;

    /*
     * See if we have the answer cached.  Someday there may be enough callers
     * to justify a hash table, but for now, a simple linked list is fine.
     */
    cache_entry = ext_sibling_list;
    while !cache_entry.is_null() {
        if funcoid == (*cache_entry).reqfuncoid
            && c_strcmp(typname, (*cache_entry).typname) == 0
        {
            break;
        }
        cache_entry = (*cache_entry).next;
    }
    if !cache_entry.is_null() && (*cache_entry).valid {
        return (*cache_entry).typeoid;
    }

    /*
     * Nope, so do the expensive lookups.  We do not expect failures, so we do
     * not cache negative results.
     */
    extoid = getExtensionOfObject(ProcedureRelationId, funcoid);
    if !OidIsValid(extoid) {
        return InvalidOid;
    }
    typeoid = getExtensionType(extoid, typname);
    if !OidIsValid(typeoid) {
        return InvalidOid;
    }

    /*
     * Build, or revalidate, cache entry.
     */
    if cache_entry.is_null() {
        /* Register invalidation hook if this is first entry */
        if ext_sibling_list.is_null() {
            CacheRegisterSyscacheCallback(
                EXTENSIONOID,
                ext_sibling_callback,
                0 as Datum,
            );
        }

        /* Momentarily zero the space to ensure valid flag is false */
        cache_entry = MemoryContextAllocZero(
            CacheMemoryContext_fn(),
            core::mem::size_of::<ExtensionSiblingCache>(),
        ) as *mut ExtensionSiblingCache;
        (*cache_entry).next = ext_sibling_list;
        ext_sibling_list = cache_entry;
    }

    (*cache_entry).reqfuncoid = funcoid;
    (*cache_entry).typname = typname;
    (*cache_entry).exthash =
        GetSysCacheHashValue1(EXTENSIONOID, ObjectIdGetDatum(extoid));
    (*cache_entry).typeoid = typeoid;
    /* Mark it valid only once it's fully populated */
    (*cache_entry).valid = true;

    typeoid
}

/*
 * ext_sibling_callback
 *      Syscache inval callback function for EXTENSIONOID cache
 *
 * It seems sufficient to invalidate ExtensionSiblingCache entries when
 * the owning extension's pg_extension entry is modified or deleted.
 * Neither a requesting function's OID, nor the OID of the object it's
 * looking for, could change without an extension update or drop/recreate.
 */
unsafe fn ext_sibling_callback(arg: Datum, cacheid: c_int, hashvalue: u32) {
    let mut cache_entry: *mut ExtensionSiblingCache;

    cache_entry = ext_sibling_list;
    while !cache_entry.is_null() {
        if hashvalue == 0 || (*cache_entry).exthash == hashvalue {
            (*cache_entry).valid = false;
        }
        cache_entry = (*cache_entry).next;
    }
}

/*
 * Utility functions to check validity of extension and version names
 */
unsafe fn check_valid_extension_name(extensionname: *const c_char) {
    let namelen = c_strlen(extensionname);

    /*
     * Disallow empty names (the parser rejects empty identifiers anyway, but
     * let's check).
     */
    if namelen == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid extension name: \"{}\"",
                std::ffi::CStr::from_ptr(extensionname).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errdetail("Extension names must not be empty.") */
        );
    }

    /*
     * No double dashes, since that would make script filenames ambiguous.
     */
    if !c_strstr(extensionname, b"--\0".as_ptr() as *const c_char).is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid extension name: \"{}\"",
                std::ffi::CStr::from_ptr(extensionname).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errdetail("Extension names must not contain \"--\".") */
        );
    }

    /*
     * No leading or trailing dash either.  (We could probably allow this, but
     * it would require much care in filename parsing and would make filenames
     * visually if not formally ambiguous.  Since there's no real-world use
     * case, let's just forbid it.)
     */
    {
        let first = *extensionname as u8 as c_char;
        let last  = *extensionname.add(namelen - 1) as u8 as c_char;
        if first == b'-' as c_char || last == b'-' as c_char {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid extension name: \"{}\"",
                    std::ffi::CStr::from_ptr(extensionname).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                   errdetail("Extension names must not begin or end with \"-\".") */
            );
        }
    }

    /*
     * No directory separators either (this is sufficient to prevent ".."
     * style attacks).
     */
    if !first_dir_separator(extensionname).is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid extension name: \"{}\"",
                std::ffi::CStr::from_ptr(extensionname).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errdetail("Extension names must not contain directory separator characters.") */
        );
    }
}

unsafe fn check_valid_version_name(versionname: *const c_char) {
    let namelen = c_strlen(versionname);

    /*
     * Disallow empty names (we could possibly allow this, but there seems
     * little point).
     */
    if namelen == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid extension version name: \"{}\"",
                std::ffi::CStr::from_ptr(versionname).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errdetail("Version names must not be empty.") */
        );
    }

    /*
     * No double dashes, since that would make script filenames ambiguous.
     */
    if !c_strstr(versionname, b"--\0".as_ptr() as *const c_char).is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid extension version name: \"{}\"",
                std::ffi::CStr::from_ptr(versionname).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errdetail("Version names must not contain \"--\".") */
        );
    }

    /*
     * No leading or trailing dash either.
     */
    {
        let first = *versionname as u8 as c_char;
        let last  = *versionname.add(namelen - 1) as u8 as c_char;
        if first == b'-' as c_char || last == b'-' as c_char {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid extension version name: \"{}\"",
                    std::ffi::CStr::from_ptr(versionname).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                   errdetail("Version names must not begin or end with \"-\".") */
            );
        }
    }

    /*
     * No directory separators either (this is sufficient to prevent ".."
     * style attacks).
     */
    if !first_dir_separator(versionname).is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid extension version name: \"{}\"",
                std::ffi::CStr::from_ptr(versionname).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE),
               errdetail("Version names must not contain directory separator characters.") */
        );
    }
}

/*
 * Utility functions to handle extension-related path names
 */
unsafe fn is_extension_control_filename(filename: *const c_char) -> bool {
    let extension = c_strrchr(filename, b'.' as c_char);
    !extension.is_null()
        && c_strcmp(extension, b".control\0".as_ptr() as *const c_char) == 0
}

unsafe fn is_extension_script_filename(filename: *const c_char) -> bool {
    let extension = c_strrchr(filename, b'.' as c_char);
    !extension.is_null()
        && c_strcmp(extension, b".sql\0".as_ptr() as *const c_char) == 0
}

/*
 * Return a list of directories declared on extension_control_path GUC.
 */
unsafe fn get_extension_control_directories() -> *mut List {
    let mut sharepath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let system_dir: *mut c_char;
    let mut ecp: *mut c_char;
    let mut paths: *mut List = null_mut();

    get_share_path(my_exec_path_fn(), sharepath.as_mut_ptr());

    system_dir = psprintf2(b"%s/extension\0".as_ptr() as *const c_char, sharepath.as_ptr());

    if c_strlen(Extension_control_path) == 0 {
        paths = lappend(paths, system_dir as *mut c_void);
    } else {
        /* Duplicate the string so we can modify it */
        ecp = pstrdup(Extension_control_path);

        loop {
            let len: usize;
            let mangled: *mut c_char;
            let piece_sep = first_path_var_separator(ecp);

            /* Get the length of the next path on ecp */
            if piece_sep.is_null() {
                len = c_strlen(ecp);
            } else {
                len = piece_sep.offset_from(ecp) as usize;
            }

            /* Copy the next path found on ecp */
            let piece = palloc(len + 1) as *mut c_char;
            c_strlcpy(piece, ecp, len + 1);

            /*
             * Substitute the path macro if needed or append "extension"
             * suffix if it is a custom extension control path.
             */
            if c_strcmp(piece, b"$system\0".as_ptr() as *const c_char) == 0 {
                mangled = substitute_path_macro(
                    piece,
                    b"$system\0".as_ptr() as *const c_char,
                    system_dir,
                );
            } else {
                mangled = psprintf2(b"%s/extension\0".as_ptr() as *const c_char, piece);
            }

            pfree(piece as *mut c_void);

            /* Canonicalize the path based on the OS and add to the list */
            canonicalize_path(mangled);
            paths = lappend(paths, mangled as *mut c_void);

            /* Break if ecp is empty or move to the next path on ecp */
            if *ecp.add(len) == 0 {
                break;
            } else {
                ecp = ecp.add(len + 1);
            }
        }
    }

    paths
}

/*
 * Find control file for extension with name in control->name, looking in the
 * path.  Return the full file name, or NULL if not found.  If found, the
 * directory is recorded in control->control_dir.
 */
unsafe fn find_extension_control_filename(control: *mut ExtensionControlFile) -> *mut c_char {
    let basename: *mut c_char;
    let result: *mut c_char;
    let paths: *mut List;

    /* Assert(control->name) */

    basename = psprintf2(b"%s.control\0".as_ptr() as *const c_char, (*control).name);

    paths = get_extension_control_directories();
    result = find_in_paths(basename, paths);

    if !result.is_null() {
        let p = c_strrchr(result, b'/' as c_char);
        /* Assert(p) */
        (*control).control_dir =
            pnstrdup(result, p.offset_from(result) as usize);
    }

    result
}

unsafe fn get_extension_script_directory(control: *mut ExtensionControlFile) -> *mut c_char {
    /*
     * The directory parameter can be omitted, absolute, or relative to the
     * installation's base directory, which can be the sharedir or a custom
     * path that it was set extension_control_path. It depends where the
     * .control file was found.
     */
    if (*control).directory.is_null() {
        return pstrdup((*control).control_dir);
    }

    if is_absolute_path((*control).directory) {
        return pstrdup((*control).directory);
    }

    /* Assert(control->basedir != NULL) */
    psprintf2(b"%s/%s\0".as_ptr() as *const c_char, (*control).basedir) /* two args elided - stub */
    /* real: psprintf("%s/%s", control->basedir, control->directory) */
}

unsafe fn get_extension_aux_control_filename(
    control: *mut ExtensionControlFile,
    version: *const c_char,
) -> *mut c_char {
    let result: *mut c_char;
    let scriptdir: *mut c_char;

    scriptdir = get_extension_script_directory(control);

    result = palloc(MAXPGPATH) as *mut c_char;
    /* snprintf(result, MAXPGPATH, "%s/%s--%s.control", scriptdir, control->name, version) */
    let _ = (result, scriptdir, version);
    unimplemented!("TODO(pg-port): snprintf for aux control filename")
}

unsafe fn get_extension_script_filename(
    control: *mut ExtensionControlFile,
    from_version: *const c_char,
    version: *const c_char,
) -> *mut c_char {
    let result: *mut c_char;
    let scriptdir: *mut c_char;

    scriptdir = get_extension_script_directory(control);

    result = palloc(MAXPGPATH) as *mut c_char;
    if !from_version.is_null() {
        /* snprintf(result, MAXPGPATH, "%s/%s--%s--%s.sql",
                    scriptdir, control->name, from_version, version) */
    } else {
        /* snprintf(result, MAXPGPATH, "%s/%s--%s.sql",
                    scriptdir, control->name, version) */
    }
    let _ = (result, scriptdir, version);
    unimplemented!("TODO(pg-port): snprintf for script filename")
}

/*
 * Parse contents of primary or auxiliary control file, and fill in
 * fields of *control.  We parse primary file if version == NULL,
 * else the optional auxiliary file for that version.
 *
 * The control file will be search on Extension_control_path paths if
 * control->control_dir is NULL, otherwise it will use the value of control_dir
 * to read and parse the .control file, so it assume that the control_dir is a
 * valid path for the control file being parsed.
 *
 * Control files are supposed to be very short, half a dozen lines,
 * so we don't worry about memory allocation risks here.  Also we don't
 * worry about what encoding it's in; all values are expected to be ASCII.
 */
unsafe fn parse_extension_control_file(
    control: *mut ExtensionControlFile,
    version: *const c_char,
) {
    let filename: *mut c_char;
    let file: *mut c_void;
    let mut head: *mut ConfigVariable = null_mut();
    let mut tail: *mut ConfigVariable = null_mut();

    /*
     * Locate the file to read.  Auxiliary files are optional.
     */
    if !version.is_null() {
        filename = get_extension_aux_control_filename(control, version);
    } else {
        /*
         * If control_dir is already set, use it, else do a path search.
         */
        if !(*control).control_dir.is_null() {
            /* filename = psprintf("%s/%s.control", control->control_dir, control->name) */
            filename = psprintf2(b"%s/%s.control\0".as_ptr() as *const c_char, (*control).control_dir);
            /* NOTE: real code uses two format args; stub above is simplified */
        } else {
            filename = find_extension_control_filename(control);
        }
    }

    if filename.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "extension \"{}\" is not available",
                std::ffi::CStr::from_ptr((*control).name).to_string_lossy()
            )
            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errhint("The extension must first be installed on the system where PostgreSQL is running.") */
        );
    }

    /* Assert that the control_dir ends with /extension */

    /* control->basedir = pnstrdup(control->control_dir,
     *     strlen(control->control_dir) - strlen("/extension")) */
    let ctrl_dir_len = c_strlen((*control).control_dir);
    let ext_suffix_len = c_strlen(b"/extension\0".as_ptr() as *const c_char);
    (*control).basedir = pnstrdup((*control).control_dir, ctrl_dir_len - ext_suffix_len);

    file = AllocateFile(filename, b"r\0".as_ptr() as *const c_char);
    if file.is_null() {
        /* no complaint for missing auxiliary file */
        if errno_fn() == ENOENT && !version.is_null() {
            pfree(filename as *mut c_void);
            return;
        }

        ereport!(
            ERROR,
            errmsg!(
                "could not open extension control file \"{}\": ",
                std::ffi::CStr::from_ptr(filename).to_string_lossy()
            )
            /* errcode_for_file_access() */
        );
    }

    /*
     * Parse the file content, using GUC's file parsing code.  We need not
     * check the return value since any errors will be thrown at ERROR level.
     */
    let _ok = ParseConfigFp(file, filename, CONF_FILE_START_DEPTH, ERROR, &mut head, &mut tail);

    FreeFile(file);

    /*
     * Convert the ConfigVariable list into ExtensionControlFile entries.
     */
    let mut item = head;
    while !item.is_null() {
        if c_strcmp((*item).name, b"directory\0".as_ptr() as *const c_char) == 0 {
            if !version.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" cannot be set in a secondary extension control file",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_SYNTAX_ERROR) */
                );
            }
            (*control).directory = pstrdup((*item).value);
        } else if c_strcmp((*item).name, b"default_version\0".as_ptr() as *const c_char) == 0 {
            if !version.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" cannot be set in a secondary extension control file",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_SYNTAX_ERROR) */
                );
            }
            (*control).default_version = pstrdup((*item).value);
        } else if c_strcmp((*item).name, b"module_pathname\0".as_ptr() as *const c_char) == 0 {
            (*control).module_pathname = pstrdup((*item).value);
        } else if c_strcmp((*item).name, b"comment\0".as_ptr() as *const c_char) == 0 {
            (*control).comment = pstrdup((*item).value);
        } else if c_strcmp((*item).name, b"schema\0".as_ptr() as *const c_char) == 0 {
            (*control).schema = pstrdup((*item).value);
        } else if c_strcmp((*item).name, b"relocatable\0".as_ptr() as *const c_char) == 0 {
            if !parse_bool((*item).value, &mut (*control).relocatable) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" requires a Boolean value",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                );
            }
        } else if c_strcmp((*item).name, b"superuser\0".as_ptr() as *const c_char) == 0 {
            if !parse_bool((*item).value, &mut (*control).superuser) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" requires a Boolean value",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                );
            }
        } else if c_strcmp((*item).name, b"trusted\0".as_ptr() as *const c_char) == 0 {
            if !parse_bool((*item).value, &mut (*control).trusted) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" requires a Boolean value",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                );
            }
        } else if c_strcmp((*item).name, b"encoding\0".as_ptr() as *const c_char) == 0 {
            (*control).encoding = pg_valid_server_encoding((*item).value);
            if (*control).encoding < 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "\"{}\" is not a valid encoding name",
                        std::ffi::CStr::from_ptr((*item).value).to_string_lossy()
                    )
                    /* errcode(ERRCODE_UNDEFINED_OBJECT) */
                );
            }
        } else if c_strcmp((*item).name, b"requires\0".as_ptr() as *const c_char) == 0 {
            /* Need a modifiable copy of string */
            let rawnames = pstrdup((*item).value);

            /* Parse string into list of identifiers */
            if !SplitIdentifierString(rawnames, b',' as c_char, &mut (*control).requires) {
                /* syntax error in name list */
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" must be a list of extension names",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                );
            }
        } else if c_strcmp((*item).name, b"no_relocate\0".as_ptr() as *const c_char) == 0 {
            /* Need a modifiable copy of string */
            let rawnames = pstrdup((*item).value);

            /* Parse string into list of identifiers */
            if !SplitIdentifierString(rawnames, b',' as c_char, &mut (*control).no_relocate) {
                /* syntax error in name list */
                ereport!(
                    ERROR,
                    errmsg!(
                        "parameter \"{}\" must be a list of extension names",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                );
            }
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized parameter \"{}\" in file \"{}\"",
                    std::ffi::CStr::from_ptr((*item).name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(filename).to_string_lossy()
                )
                /* errcode(ERRCODE_SYNTAX_ERROR) */
            );
        }

        item = (*item).next;
    }

    FreeConfigVariables(head);

    if (*control).relocatable && !(*control).schema.is_null() {
        ereport!(
            ERROR,
            errmsg!("parameter \"schema\" cannot be specified when \"relocatable\" is true")
            /* errcode(ERRCODE_SYNTAX_ERROR) */
        );
    }

    pfree(filename as *mut c_void);
}

/*
 * Read the primary control file for the specified extension.
 */
unsafe fn read_extension_control_file(extname: *const c_char) -> *mut ExtensionControlFile {
    let control = new_ExtensionControlFile(extname);

    /*
     * Parse the primary control file.
     */
    parse_extension_control_file(control, null());

    control
}

/*
 * Read the auxiliary control file for the specified extension and version.
 *
 * Returns a new modified ExtensionControlFile struct; the original struct
 * (reflecting just the primary control file) is not modified.
 */
unsafe fn read_extension_aux_control_file(
    pcontrol: *const ExtensionControlFile,
    version: *const c_char,
) -> *mut ExtensionControlFile {
    let acontrol: *mut ExtensionControlFile;

    /*
     * Flat-copy the struct.  Pointer fields share values with original.
     */
    acontrol = palloc(core::mem::size_of::<ExtensionControlFile>()) as *mut ExtensionControlFile;
    c_memcpy(
        acontrol as *mut c_void,
        pcontrol as *const c_void,
        core::mem::size_of::<ExtensionControlFile>(),
    );

    /*
     * Parse the auxiliary control file, overwriting struct fields
     */
    parse_extension_control_file(acontrol, version);

    acontrol
}
// -------------------------  END PART 2  ----------------------------------

// =========================================================================
// PART 3: read_extension_script_file, script_error_callback,
//         execute_sql_string, extension_is_trusted,
//         execute_extension_script, get_ext_ver_info,
//         get_nearest_unprocessed_vertex, get_ext_ver_list,
//         identify_update_path, find_update_path, find_install_path
// =========================================================================

/*
 * Read an SQL script file into a string, and convert to database encoding
 */
unsafe fn read_extension_script_file(
    control: *const ExtensionControlFile,
    filename: *const c_char,
) -> *mut c_char {
    let src_encoding: c_int;
    let src_str: *mut c_char;
    let dest_str: *mut c_char;
    let mut len: c_int = 0;

    src_str = read_whole_file(filename, &mut len);

    /* use database encoding if not given */
    if (*control).encoding < 0 {
        src_encoding = GetDatabaseEncoding();
    } else {
        src_encoding = (*control).encoding;
    }

    /* make sure that source string is valid in the expected encoding */
    let _ = pg_verify_mbstr(src_encoding, src_str, len, false);

    /*
     * Convert the encoding to the database encoding. read_whole_file
     * null-terminated the string, so if no conversion happens the string is
     * valid as is.
     */
    dest_str = pg_any_to_server(src_str, len, src_encoding);

    dest_str
}

/*
 * error context callback for failures in script-file execution
 */
unsafe fn script_error_callback(arg: *mut c_void) {
    let callback_arg = arg as *mut script_error_callback_arg;
    let mut query = (*callback_arg).sql;
    let mut location = (*callback_arg).stmt_location;
    let mut len = (*callback_arg).stmt_len;
    let syntaxerrposition: c_int;
    let lastslash: *const c_char;

    /*
     * If there is a syntax error position, convert to internal syntax error;
     * otherwise report the current query as an item of context stack.
     *
     * Note: we'll provide no context except the filename if there's neither
     * an error position nor any known current query.  That shouldn't happen
     * though: all errors reported during raw parsing should come with an
     * error position.
     */
    syntaxerrposition = geterrposition();
    if syntaxerrposition > 0 {
        /*
         * If we do not know the bounds of the current statement (as would
         * happen for an error occurring during initial raw parsing), we have
         * to use a heuristic to decide how much of the script to show.  We'll
         * also use the heuristic in the unlikely case that syntaxerrposition
         * is outside what we think the statement bounds are.
         */
        if location < 0
            || syntaxerrposition < location
            || (len > 0 && syntaxerrposition > location + len)
        {
            /*
             * Our heuristic is pretty simple: look for semicolon-newline
             * sequences, and break at the last one strictly before
             * syntaxerrposition and the first one strictly after.  It's
             * certainly possible to fool this with semicolon-newline embedded
             * in a string literal, but it seems better to do this than to
             * show the entire extension script.
             *
             * Notice we cope with Windows-style newlines (\r\n) regardless of
             * platform.  This is because there might be such newlines in
             * script files on other platforms.
             */
            let slen = c_strlen(query) as c_int;

            location = 0;
            len = 0;
            let mut loc: c_int = 0;
            while loc < slen {
                if *query.add(loc as usize) != b';' as c_char {
                    loc += 1;
                    continue;
                }
                if *query.add((loc + 1) as usize) == b'\r' as c_char {
                    loc += 1;
                }
                if *query.add((loc + 1) as usize) == b'\n' as c_char {
                    let bkpt = loc + 2;
                    if bkpt < syntaxerrposition {
                        location = bkpt;
                    } else if bkpt > syntaxerrposition {
                        len = bkpt - location;
                        break; /* no need to keep searching */
                    }
                }
                loc += 1;
            }
        }

        /* Trim leading/trailing whitespace, for consistency */
        query = CleanQuerytext(query, &mut location, &mut len);

        /*
         * Adjust syntaxerrposition.  It shouldn't be pointing into the
         * whitespace we just trimmed, but cope if it is.
         */
        let mut adjusted = syntaxerrposition - location;
        if adjusted < 0 {
            adjusted = 0;
        } else if adjusted > len {
            adjusted = len;
        }

        /* And report. */
        errposition(0);
        internalerrposition(adjusted);
        internalerrquery(pnstrdup(query, len as usize));
    } else if location >= 0 {
        /*
         * Since no syntax cursor will be shown, it's okay and helpful to trim
         * the reported query string to just the current statement.
         */
        query = CleanQuerytext(query, &mut location, &mut len);
        /* errcontext("SQL statement \"%.*s\"", len, query) */
        errcontext_fn(query);
    }

    /*
     * Trim the reported file name to remove the path.  We know that
     * get_extension_script_filename() inserted a '/', regardless of whether
     * we're on Windows.
     */
    lastslash = c_strrchr((*callback_arg).filename, b'/' as c_char);
    let lastslash = if !lastslash.is_null() {
        lastslash.add(1)
    } else {
        (*callback_arg).filename /* shouldn't happen, but cope */
    };

    /*
     * If we have a location (which, as said above, we really always should)
     * then report a line number to aid in localizing problems in big scripts.
     */
    if location >= 0 {
        let mut linenumber: c_int = 1;
        let mut qp = (*callback_arg).sql;
        let mut loc = location;
        while *qp != 0 {
            loc -= 1;
            if loc < 0 {
                break;
            }
            if *qp == b'\n' as c_char {
                linenumber += 1;
            }
            qp = qp.add(1);
        }
        /* errcontext("extension script file \"%s\", near line %d", lastslash, linenumber) */
        let _ = (lastslash, linenumber);
        errcontext_fn(lastslash);
    } else {
        /* errcontext("extension script file \"%s\"", lastslash) */
        errcontext_fn(lastslash);
    }
}

/*
 * Execute given SQL string.
 *
 * The filename the string came from is also provided, for error reporting.
 *
 * Note: it's tempting to just use SPI to execute the string, but that does
 * not work very well.  The really serious problem is that SPI will parse,
 * analyze, and plan the whole string before executing any of it; of course
 * this fails if there are any plannable statements referring to objects
 * created earlier in the script.  A lesser annoyance is that SPI insists
 * on printing the whole string as errcontext in case of any error, and that
 * could be very long.
 */
unsafe fn execute_sql_string(sql: *const c_char, filename: *const c_char) {
    let mut callback_arg = script_error_callback_arg {
        sql,
        filename,
        stmt_location: -1,
        stmt_len: -1,
    };
    let scripterrcontext = ErrorContextCallback {
        callback: script_error_callback,
        arg: &mut callback_arg as *mut script_error_callback_arg as *mut c_void,
        previous: error_context_stack_fn(),
    };
    /* error_context_stack = &scripterrcontext */

    /*
     * Parse the SQL string into a list of raw parse trees.
     */
    let raw_parsetree_list = pg_parse_query(sql);

    /* All output from SELECTs goes to the bit bucket */
    let dest = CreateDestReceiver(DestNone);

    /*
     * Do parse analysis, rule rewrite, planning, and execution for each raw
     * parsetree.  We must fully execute each query before beginning parse
     * analysis on the next one, since there may be interdependencies.
     */
    /* foreach(lc1, raw_parsetree_list) */
    let _ = (raw_parsetree_list, dest, scripterrcontext);
    unimplemented!("TODO(pg-port): execute_sql_string inner loop (pg_parse_query / foreach / ExecutorRun)")
}

/*
 * Policy function: is the given extension trusted for installation by a
 * non-superuser?
 *
 * (Update the errhint logic below if you change this.)
 */
unsafe fn extension_is_trusted(control: *mut ExtensionControlFile) -> bool {
    let mut aclresult: AclResult = ACLCHECK_OK;

    /* Never trust unless extension's control file says it's okay */
    if !(*control).trusted {
        return false;
    }
    /* Allow if user has CREATE privilege on current database */
    aclresult = object_aclcheck(
        DatabaseRelationId,
        MyDatabaseId_fn(),
        GetUserId(),
        ACL_CREATE,
    );
    if aclresult == ACLCHECK_OK {
        return true;
    }
    false
}

/*
 * Execute the appropriate script file for installing or updating the extension
 *
 * If from_version isn't NULL, it's an update
 *
 * Note: requiredSchemas must be one-for-one with the control->requires list
 */
unsafe fn execute_extension_script(
    extensionOid: Oid,
    control: *mut ExtensionControlFile,
    from_version: *const c_char,
    version: *const c_char,
    requiredSchemas: *mut List,
    schemaName: *const c_char,
) {
    let mut switch_to_superuser = false;
    let filename: *mut c_char;
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    let save_nestlevel: c_int;
    let mut pathbuf = StringInfoData {
        data: null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };

    /*
     * Enforce superuser-ness if appropriate.  We postpone these checks until
     * here so that the control flags are correctly associated with the right
     * script(s) if they happen to be set in secondary control files.
     */
    if (*control).superuser && !superuser() {
        if extension_is_trusted(control) {
            switch_to_superuser = true;
        } else if from_version.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "permission denied to create extension \"{}\"",
                    std::ffi::CStr::from_ptr((*control).name).to_string_lossy()
                )
                /* errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                   errhint(if trusted: "Must have CREATE privilege..." else "Must be superuser...") */
            );
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "permission denied to update extension \"{}\"",
                    std::ffi::CStr::from_ptr((*control).name).to_string_lossy()
                )
                /* errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                   errhint(...) */
            );
        }
    }

    filename = get_extension_script_filename(control, from_version, version);

    if from_version.is_null() {
        elog!(
            DEBUG1,
            "executing extension script for \"{}\" version '{}'",
            std::ffi::CStr::from_ptr((*control).name).to_string_lossy(),
            std::ffi::CStr::from_ptr(version).to_string_lossy()
        );
    } else {
        elog!(
            DEBUG1,
            "executing extension script for \"{}\" update from version '{}' to '{}'",
            std::ffi::CStr::from_ptr((*control).name).to_string_lossy(),
            std::ffi::CStr::from_ptr(from_version).to_string_lossy(),
            std::ffi::CStr::from_ptr(version).to_string_lossy()
        );
    }

    /*
     * If installing a trusted extension on behalf of a non-superuser, become
     * the bootstrap superuser.  (This switch will be cleaned up automatically
     * if the transaction aborts, as will the GUC changes below.)
     */
    if switch_to_superuser {
        GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
        SetUserIdAndSecContext(
            BOOTSTRAP_SUPERUSERID,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    /*
     * Force client_min_messages and log_min_messages to be at least WARNING,
     * so that we won't spam the user with useless NOTICE messages from common
     * script actions like creating shell types.
     *
     * We use the equivalent of a function SET option to allow the setting to
     * persist for exactly the duration of the script execution.  guc.c also
     * takes care of undoing the setting on error.
     *
     * log_min_messages can't be set by ordinary users, so for that one we
     * pretend to be superuser.
     */
    save_nestlevel = NewGUCNestLevel();

    if client_min_messages_fn() < WARNING {
        let _ = set_config_option(
            b"client_min_messages\0".as_ptr() as *const c_char,
            b"warning\0".as_ptr() as *const c_char,
            PGC_USERSET,
            PGC_S_SESSION,
            GUC_ACTION_SAVE,
            true,
            0,
            false,
        );
    }
    if log_min_messages_fn() < WARNING {
        let _ = set_config_option_ext(
            b"log_min_messages\0".as_ptr() as *const c_char,
            b"warning\0".as_ptr() as *const c_char,
            PGC_SUSET,
            PGC_S_SESSION,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SAVE,
            true,
            0,
            false,
        );
    }

    /*
     * Similarly disable check_function_bodies, to ensure that SQL functions
     * won't be parsed during creation.
     */
    if check_function_bodies_fn() {
        let _ = set_config_option(
            b"check_function_bodies\0".as_ptr() as *const c_char,
            b"off\0".as_ptr() as *const c_char,
            PGC_USERSET,
            PGC_S_SESSION,
            GUC_ACTION_SAVE,
            true,
            0,
            false,
        );
    }

    /*
     * Set up the search path to have the target schema first, making it be
     * the default creation target namespace.  Then add the schemas of any
     * prerequisite extensions, unless they are in pg_catalog which would be
     * searched anyway.  (Listing pg_catalog explicitly in a non-first
     * position would be bad for security.)  Finally add pg_temp to ensure
     * that temp objects can't take precedence over others.
     */
    initStringInfo(&mut pathbuf);
    appendStringInfoString(&mut pathbuf, quote_identifier(schemaName));

    /* foreach(lc, requiredSchemas) */
    let _ = requiredSchemas; /* TODO(pg-port): foreach over requiredSchemas */

    appendStringInfoString(&mut pathbuf, b", pg_temp\0".as_ptr() as *const c_char);

    let _ = set_config_option(
        b"search_path\0".as_ptr() as *const c_char,
        pathbuf.data,
        PGC_USERSET,
        PGC_S_SESSION,
        GUC_ACTION_SAVE,
        true,
        0,
        false,
    );

    /*
     * Set creating_extension and related variables so that
     * recordDependencyOnCurrentExtension and other functions do the right
     * things.  On failure, ensure we reset these variables.
     */
    creating_extension = true;
    CurrentExtensionObject = extensionOid;
    /* PG_TRY / PG_FINALLY: reset on exit */
    /* C also: PG_FINALLY { creating_extension = false; CurrentExtensionObject = InvalidOid; } */

    let c_sql = read_extension_script_file(control, filename);

    /*
     * We filter each substitution through quote_identifier().  When the
     * arg contains one of the following characters, no one collection of
     * quoting can work inside $$dollar-quoted string literals$$,
     * 'single-quoted string literals', and outside of any literal.  To
     * avoid a security snare for extension authors, error on substitution
     * for arguments containing these.
     */
    let quoting_relevant_chars = b"\"$'\\\\\0".as_ptr() as *const c_char;

    /* We use various functions that want to operate on text datums */
    let mut t_sql = CStringGetTextDatum(c_sql);

    /*
     * Reduce any lines beginning with "\echo" to empty.  This allows
     * scripts to contain messages telling people not to run them via
     * psql, which has been found to be necessary due to old habits.
     */
    t_sql = DirectFunctionCall4Coll(
        textregexreplace,
        C_COLLATION_OID,
        t_sql,
        CStringGetTextDatum(b"^\\\\echo.*$\0".as_ptr() as *const c_char),
        CStringGetTextDatum(b"\0".as_ptr() as *const c_char),
        CStringGetTextDatum(b"ng\0".as_ptr() as *const c_char),
    );

    /*
     * If the script uses @extowner@, substitute the calling username.
     */
    if !c_strstr(c_sql, b"@extowner@\0".as_ptr() as *const c_char).is_null() {
        let uid = if switch_to_superuser { save_userid } else { GetUserId() };
        let userName = GetUserNameFromId(uid, false);
        let qUserName = quote_identifier(userName);

        t_sql = DirectFunctionCall3Coll(
            replace_text,
            C_COLLATION_OID,
            t_sql,
            CStringGetTextDatum(b"@extowner@\0".as_ptr() as *const c_char),
            CStringGetTextDatum(qUserName),
        );
        if !strpbrk(userName, quoting_relevant_chars).is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid character in extension owner: must not contain any of \"{}\"",
                    std::ffi::CStr::from_ptr(quoting_relevant_chars).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_TEXT_REPRESENTATION) */
            );
        }
    }

    /*
     * If it's not relocatable, substitute the target schema name for
     * occurrences of @extschema@.
     *
     * For a relocatable extension, we needn't do this.  There cannot be
     * any need for @extschema@, else it wouldn't be relocatable.
     */
    if !(*control).relocatable {
        let old = t_sql;
        let qSchemaName = quote_identifier(schemaName);

        t_sql = DirectFunctionCall3Coll(
            replace_text,
            C_COLLATION_OID,
            t_sql,
            CStringGetTextDatum(b"@extschema@\0".as_ptr() as *const c_char),
            CStringGetTextDatum(qSchemaName),
        );
        if t_sql != old && !strpbrk(schemaName, quoting_relevant_chars).is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid character in extension \"{}\" schema: must not contain any of \"{}\"",
                    std::ffi::CStr::from_ptr((*control).name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(quoting_relevant_chars).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_TEXT_REPRESENTATION) */
            );
        }
    }

    /*
     * Likewise, substitute required extensions' schema names for
     * occurrences of @extschema:extension_name@.
     */
    /* Assert(list_length(control->requires) == list_length(requiredSchemas)) */
    /* forboth(lc, control->requires, lc2, requiredSchemas) { ... } */
    /* TODO(pg-port): forboth iteration over requires/requiredSchemas */

    /*
     * If module_pathname was set in the control file, substitute its
     * value for occurrences of MODULE_PATHNAME.
     */
    if !(*control).module_pathname.is_null() {
        t_sql = DirectFunctionCall3Coll(
            replace_text,
            C_COLLATION_OID,
            t_sql,
            CStringGetTextDatum(b"MODULE_PATHNAME\0".as_ptr() as *const c_char),
            CStringGetTextDatum((*control).module_pathname),
        );
    }

    /* And now back to C string */
    let c_sql_final = text_to_cstring(DatumGetTextPP(t_sql));

    execute_sql_string(c_sql_final, filename);

    /* PG_FINALLY resets these */
    creating_extension = false;
    CurrentExtensionObject = InvalidOid;

    /*
     * Restore the GUC variables we set above.
     */
    AtEOXact_GUC(true, save_nestlevel);

    /*
     * Restore authentication state if needed.
     */
    if switch_to_superuser {
        SetUserIdAndSecContext(save_userid, save_sec_context);
    }
}

/*
 * Find or create an ExtensionVersionInfo for the specified version name
 *
 * Currently, we just use a List of the ExtensionVersionInfo's.  Searching
 * for them therefore uses about O(N^2) time when there are N versions of
 * the extension.  We could change the data structure to a hash table if
 * this ever becomes a bottleneck.
 */
unsafe fn get_ext_ver_info(
    versionname: *const c_char,
    evi_list: *mut *mut List,
) -> *mut ExtensionVersionInfo {
    let evi: *mut ExtensionVersionInfo;

    /* foreach(lc, *evi_list) */
    /* TODO(pg-port): foreach iteration; inlined here as unimplemented stub */
    let _ = evi_list; /* scanned below */

    /* linear scan (O(N)) */
    /* -- loop body would compare evi->name to versionname -- */
    /* return evi if found */

    evi = palloc(core::mem::size_of::<ExtensionVersionInfo>()) as *mut ExtensionVersionInfo;
    (*evi).name = pstrdup(versionname);
    (*evi).reachable = null_mut();
    (*evi).installable = false;
    /* initialize for later application of Dijkstra's algorithm */
    (*evi).distance_known = false;
    (*evi).distance = c_int::MAX;
    (*evi).previous = null_mut();

    *evi_list = lappend(*evi_list, evi as *mut c_void);

    evi
}

/*
 * Locate the nearest unprocessed ExtensionVersionInfo
 *
 * This part of the algorithm is also about O(N^2).  A priority queue would
 * make it much faster, but for now there's no need.
 */
unsafe fn get_nearest_unprocessed_vertex(evi_list: *mut List) -> *mut ExtensionVersionInfo {
    let evi: *mut ExtensionVersionInfo = null_mut();

    /* foreach(lc, evi_list) */
    /* TODO(pg-port): foreach iteration */
    let _ = evi_list;

    evi
}

/*
 * Obtain information about the set of update scripts available for the
 * specified extension.  The result is a List of ExtensionVersionInfo
 * structs, each with a subsidiary list of the ExtensionVersionInfos for
 * the versions that can be reached in one step from that version.
 */
unsafe fn get_ext_ver_list(control: *mut ExtensionControlFile) -> *mut List {
    let mut evi_list: *mut List = null_mut();
    let extnamelen = c_strlen((*control).name);
    let location: *mut c_char;
    let dir: *mut c_void;

    location = get_extension_script_directory(control);
    dir = AllocateDir(location);
    loop {
        let de = ReadDir(dir, location);
        if de.is_null() {
            break;
        }
        let vername: *mut c_char;
        let vername2: *mut c_char;
        let evi: *mut ExtensionVersionInfo;
        let evi2: *mut ExtensionVersionInfo;

        /* must be a .sql file ... */
        if !is_extension_script_filename((*de).d_name.as_ptr()) {
            continue;
        }

        /* ... matching extension name followed by separator */
        if c_strncmp((*de).d_name.as_ptr(), (*control).name, extnamelen) != 0
            || (*de).d_name[extnamelen] != b'-' as c_char
            || (*de).d_name[extnamelen + 1] != b'-' as c_char
        {
            continue;
        }

        /* extract version name(s) from 'extname--something.sql' filename */
        vername = pstrdup((*de).d_name.as_ptr().add(extnamelen + 2));
        *c_strrchr(vername, b'.' as c_char) = 0;
        let vn2_p = c_strstr(vername, b"--\0".as_ptr() as *const c_char);
        if vn2_p.is_null() {
            /* It's an install, not update, script; record its version name */
            let evi_inst = get_ext_ver_info(vername, &mut evi_list);
            (*evi_inst).installable = true;
            continue;
        }
        *vn2_p = 0;         /* terminate first version */
        let vername2 = vn2_p.add(2); /* and point to second */

        /* if there's a third --, it's bogus, ignore it */
        if !c_strstr(vername2, b"--\0".as_ptr() as *const c_char).is_null() {
            continue;
        }

        /* Create ExtensionVersionInfos and link them together */
        let evi_from = get_ext_ver_info(vername, &mut evi_list);
        let evi_to   = get_ext_ver_info(vername2, &mut evi_list);
        (*evi_from).reachable = lappend((*evi_from).reachable, evi_to as *mut c_void);
    }
    FreeDir(dir);

    evi_list
}

/*
 * Given an initial and final version name, identify the sequence of update
 * scripts that have to be applied to perform that update.
 *
 * Result is a List of names of versions to transition through (the initial
 * version is *not* included).
 */
unsafe fn identify_update_path(
    control: *mut ExtensionControlFile,
    oldVersion: *const c_char,
    newVersion: *const c_char,
) -> *mut List {
    let result: *mut List;
    let mut evi_list: *mut List;
    let evi_start: *mut ExtensionVersionInfo;
    let evi_target: *mut ExtensionVersionInfo;

    /* Extract the version update graph from the script directory */
    evi_list = get_ext_ver_list(control);

    /* Initialize start and end vertices */
    evi_start  = get_ext_ver_info(oldVersion, &mut evi_list);
    evi_target = get_ext_ver_info(newVersion, &mut evi_list);

    /* Find shortest path */
    result = find_update_path(evi_list, evi_start, evi_target, false, false);

    if result.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "extension \"{}\" has no update path from version \"{}\" to version \"{}\"",
                std::ffi::CStr::from_ptr((*control).name).to_string_lossy(),
                std::ffi::CStr::from_ptr(oldVersion).to_string_lossy(),
                std::ffi::CStr::from_ptr(newVersion).to_string_lossy()
            )
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    result
}

/*
 * Apply Dijkstra's algorithm to find the shortest path from evi_start to
 * evi_target.
 *
 * If reject_indirect is true, ignore paths that go through installable
 * versions.  This saves work when the caller will consider starting from
 * all installable versions anyway.
 *
 * If reinitialize is false, assume the ExtensionVersionInfo list has not
 * been used for this before, and the initialization done by get_ext_ver_info
 * is still good.  Otherwise, reinitialize all transient fields used here.
 *
 * Result is a List of names of versions to transition through (the initial
 * version is *not* included).  Returns NIL if no such path.
 */
unsafe fn find_update_path(
    evi_list: *mut List,
    evi_start: *mut ExtensionVersionInfo,
    evi_target: *mut ExtensionVersionInfo,
    reject_indirect: bool,
    reinitialize: bool,
) -> *mut List {
    let mut result: *mut List = null_mut();
    let mut evi: *mut ExtensionVersionInfo;

    /* Caller error if start == target */
    /* Assert(evi_start != evi_target) */
    /* Caller error if reject_indirect and target is installable */
    /* Assert(!(reject_indirect && evi_target->installable)) */

    if reinitialize {
        /* foreach(lc, evi_list) -- reinitialize each vertex */
        /* TODO(pg-port): foreach over evi_list */
        let _ = evi_list;
    }

    (*evi_start).distance = 0;

    loop {
        evi = get_nearest_unprocessed_vertex(evi_list);
        if evi.is_null() {
            break;
        }
        if (*evi).distance == c_int::MAX {
            break; /* all remaining vertices are unreachable */
        }
        (*evi).distance_known = true;
        if evi == evi_target {
            break; /* found shortest path to target */
        }
        /* foreach(lc, evi->reachable) */
        /* TODO(pg-port): foreach over evi->reachable to relax edges */
        let _ = reject_indirect;
    }

    /* Return NIL if target is not reachable from start */
    if !(*evi_target).distance_known {
        return null_mut();
    }

    /* Build and return list of version names representing the update path */
    result = null_mut();
    evi = evi_target;
    while evi != evi_start {
        /* result = lcons(evi->name, result) */
        result = lcons((*evi).name as *mut c_void, result);
        evi = (*evi).previous;
    }

    result
}

/*
 * Given a target version that is not directly installable, find the
 * best installation sequence starting from a directly-installable version.
 *
 * evi_list: previously-collected version update graph
 * evi_target: member of that list that we want to reach
 *
 * Returns the best starting-point version, or NULL if there is none.
 * On success, *best_path is set to the path from the start point.
 *
 * If there's more than one possible start point, prefer shorter update paths,
 * and break any ties arbitrarily on the basis of strcmp'ing the starting
 * versions' names.
 */
unsafe fn find_install_path(
    evi_list: *mut List,
    evi_target: *mut ExtensionVersionInfo,
    best_path: *mut *mut List,
) -> *mut ExtensionVersionInfo {
    let evi_start: *mut ExtensionVersionInfo = null_mut();

    *best_path = null_mut();

    /*
     * We don't expect to be called for an installable target, but if we are,
     * the answer is easy: just start from there, with an empty update path.
     */
    if (*evi_target).installable {
        return evi_target;
    }

    /* Consider all installable versions as start points */
    /* foreach(lc, evi_list) */
    /* TODO(pg-port): foreach iteration over evi_list */
    let _ = evi_list;

    evi_start
}
// -------------------------  END PART 3  ----------------------------------

// =========================================================================
// PART 4: CreateExtensionInternal, get_required_extension, CreateExtension,
//         InsertExtensionTuple, RemoveExtensionById,
//         pg_available_extensions, pg_available_extension_versions,
//         get_available_versions_for_extension, extension_file_exists,
//         convert_requires_to_datum, pg_extension_update_paths
// =========================================================================

/*
 * CREATE EXTENSION worker
 *
 * When CASCADE is specified, CreateExtensionInternal() recurses if required
 * extensions need to be installed.  To sanely handle cyclic dependencies,
 * the "parents" list contains a list of names of extensions already being
 * installed, allowing us to error out if we recurse to one of those.
 */
unsafe fn CreateExtensionInternal(
    extensionName: *mut c_char,
    schemaName: *mut c_char,
    versionName: *const c_char,
    cascade: bool,
    parents: *mut List,
    is_create: bool,
) -> ObjectAddress {
    let origSchemaName = schemaName;
    let mut schemaOid: Oid = InvalidOid;
    let extowner: Oid = GetUserId();
    let pcontrol: *mut ExtensionControlFile;
    let control: *mut ExtensionControlFile;
    let filename: *mut c_char;
    let mut fst = stat { st_size: 0 };
    let mut updateVersions: *mut List = null_mut();
    let requiredExtensions: *mut List;
    let requiredSchemas: *mut List;
    let extensionOid: Oid;
    let address: ObjectAddress;
    let mut versionName = versionName;
    let mut schemaName = schemaName;

    /*
     * Read the primary control file.  Note we assume that it does not contain
     * any non-ASCII data, so there is no need to worry about encoding at this
     * point.
     */
    pcontrol = read_extension_control_file(extensionName);

    /*
     * Determine the version to install
     */
    if versionName.is_null() {
        if !(*pcontrol).default_version.is_null() {
            versionName = (*pcontrol).default_version;
        } else {
            ereport!(
                ERROR,
                errmsg!("version to install must be specified")
                /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            );
        }
    }
    check_valid_version_name(versionName);

    /*
     * Figure out which script(s) we need to run to install the desired
     * version of the extension.  If we do not have a script that directly
     * does what is needed, we try to find a sequence of update scripts that
     * will get us there.
     */
    filename = get_extension_script_filename(pcontrol, null(), versionName);
    if stat_fn(filename, &mut fst) == 0 {
        /* Easy, no extra scripts */
        updateVersions = null_mut();
    } else {
        /* Look for best way to install this version */
        let mut evi_list: *mut List;
        let evi_start: *mut ExtensionVersionInfo;
        let evi_target: *mut ExtensionVersionInfo;

        /* Extract the version update graph from the script directory */
        evi_list = get_ext_ver_list(pcontrol);

        /* Identify the target version */
        evi_target = get_ext_ver_info(versionName, &mut evi_list);

        /* Identify best path to reach target */
        evi_start = find_install_path(evi_list, evi_target, &mut updateVersions);

        /* Fail if no path ... */
        if evi_start.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "extension \"{}\" has no installation script nor update path for version \"{}\"",
                    std::ffi::CStr::from_ptr((*pcontrol).name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(versionName).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            );
        }

        /* Otherwise, install best starting point and then upgrade */
        versionName = (*evi_start).name;
    }

    /*
     * Fetch control parameters for installation target version
     */
    control = read_extension_aux_control_file(pcontrol, versionName);

    /*
     * Determine the target schema to install the extension into
     */
    if !schemaName.is_null() {
        /* If the user is giving us the schema name, it must exist already. */
        schemaOid = get_namespace_oid(schemaName, false);
    }

    if !(*control).schema.is_null() {
        /*
         * The extension is not relocatable and the author gave us a schema
         * for it.
         *
         * Unless CASCADE parameter was given, it's an error to give a schema
         * different from control->schema if control->schema is specified.
         */
        if !schemaName.is_null()
            && c_strcmp((*control).schema, schemaName) != 0
            && !cascade
        {
            ereport!(
                ERROR,
                errmsg!(
                    "extension \"{}\" must be installed in schema \"{}\"",
                    std::ffi::CStr::from_ptr((*control).name).to_string_lossy(),
                    std::ffi::CStr::from_ptr((*control).schema).to_string_lossy()
                )
                /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            );
        }

        /* Always use the schema from control file for current extension. */
        schemaName = (*control).schema;

        /* Find or create the schema in case it does not exist. */
        schemaOid = get_namespace_oid(schemaName, true);

        if !OidIsValid(schemaOid) {
            let csstmt = makeNode!(CreateSchemaStmt, T_CreateSchemaStmt) as *mut c_void;
            /* csstmt->schemaname = schemaName; csstmt->authrole = NULL;
               csstmt->schemaElts = NIL; csstmt->if_not_exists = false */
            CreateSchemaCommand(
                csstmt as *mut CreateSchemaStmt,
                b"(generated CREATE SCHEMA command)\0".as_ptr() as *const c_char,
                -1,
                -1,
            );

            /*
             * CreateSchemaCommand includes CommandCounterIncrement, so new
             * schema is now visible.
             */
            schemaOid = get_namespace_oid(schemaName, false);
        }
    } else if !OidIsValid(schemaOid) {
        /*
         * Neither user nor author of the extension specified schema; use the
         * current default creation namespace, which is the first explicit
         * entry in the search_path.
         */
        let search_path = fetch_search_path(false);

        if search_path.is_null() {
            /* nothing valid in search_path? */
            ereport!(
                ERROR,
                errmsg!("no schema has been selected to create in")
                /* errcode(ERRCODE_UNDEFINED_SCHEMA) */
            );
        }
        schemaOid = linitial_oid(search_path);
        schemaName = get_namespace_name(schemaOid);
        if schemaName.is_null() {
            /* recently-deleted namespace? */
            ereport!(
                ERROR,
                errmsg!("no schema has been selected to create in")
                /* errcode(ERRCODE_UNDEFINED_SCHEMA) */
            );
        }

        list_free(search_path);
    }

    /*
     * Make note if a temporary namespace has been accessed in this
     * transaction.
     */
    if isTempNamespace(schemaOid) {
        *MyXactFlags_fn() |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    }

    /*
     * We don't check creation rights on the target namespace here.  If the
     * extension script actually creates any objects there, it will fail if
     * the user doesn't have such permissions.  But there are cases such as
     * procedural languages where it's convenient to set schema = pg_catalog
     * yet we don't want to restrict the command to users with ACL_CREATE for
     * pg_catalog.
     */

    /*
     * Look up the prerequisite extensions, install them if necessary, and
     * build lists of their OIDs and the OIDs of their target schemas.
     */
    let requiredExtensions: *mut List = null_mut();
    let requiredSchemas: *mut List = null_mut();
    /* foreach(lc, control->requires) */
    /* TODO(pg-port): foreach over control->requires */

    /*
     * Insert new tuple into pg_extension, and create dependency entries.
     */
    let address = InsertExtensionTuple(
        (*control).name,
        extowner,
        schemaOid,
        (*control).relocatable,
        versionName,
        PointerGetDatum(null()),
        PointerGetDatum(null()),
        requiredExtensions,
    );
    extensionOid = address.objectId;

    /*
     * Apply any control-file comment on extension
     */
    if !(*control).comment.is_null() {
        CreateComments(extensionOid, ExtensionRelationId, 0, (*control).comment);
    }

    /*
     * Execute the installation script file
     */
    execute_extension_script(
        extensionOid,
        control,
        null(),
        versionName,
        requiredSchemas,
        schemaName,
    );

    /*
     * If additional update scripts have to be executed, apply the updates as
     * though a series of ALTER EXTENSION UPDATE commands were given
     */
    ApplyExtensionUpdates(
        extensionOid,
        pcontrol,
        versionName,
        updateVersions,
        origSchemaName,
        cascade,
        is_create,
    );

    address
}

/*
 * Get the OID of an extension listed in "requires", possibly creating it.
 */
unsafe fn get_required_extension(
    reqExtensionName: *mut c_char,
    extensionName: *mut c_char,
    origSchemaName: *mut c_char,
    cascade: bool,
    parents: *mut List,
    is_create: bool,
) -> Oid {
    let mut reqExtensionOid: Oid;

    reqExtensionOid = get_extension_oid(reqExtensionName, true);
    if !OidIsValid(reqExtensionOid) {
        if cascade {
            /* Must install it. */
            let addr: ObjectAddress;
            let cascade_parents: *mut List;

            /* Check extension name validity before trying to cascade. */
            check_valid_extension_name(reqExtensionName);

            /* Check for cyclic dependency between extensions. */
            /* foreach(lc, parents) */
            /* TODO(pg-port): foreach over parents */

            ereport!(
                NOTICE,
                errmsg!(
                    "installing required extension \"{}\"",
                    std::ffi::CStr::from_ptr(reqExtensionName).to_string_lossy()
                )
            );

            /* Add current extension to list of parents to pass down. */
            cascade_parents =
                lappend(list_copy(parents), extensionName as *mut c_void);

            /*
             * Create the required extension.  We propagate the SCHEMA option
             * if any, and CASCADE, but no other options.
             */
            let addr = CreateExtensionInternal(
                reqExtensionName,
                origSchemaName,
                null(),
                cascade,
                cascade_parents,
                is_create,
            );

            /* Get its newly-assigned OID. */
            reqExtensionOid = addr.objectId;
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "required extension \"{}\" is not installed",
                    std::ffi::CStr::from_ptr(reqExtensionName).to_string_lossy()
                )
                /* errcode(ERRCODE_UNDEFINED_OBJECT),
                   errhint(if is_create: "Use CREATE EXTENSION ... CASCADE..." else 0) */
            );
        }
    }

    reqExtensionOid
}

/*
 * CREATE EXTENSION
 */
pub unsafe fn CreateExtension(pstate: ParseState, stmt: *mut CreateExtensionStmt) -> ObjectAddress {
    let d_schema:      *const DefElem = null();
    let d_new_version: *const DefElem = null();
    let d_cascade:     *const DefElem = null();
    let schemaName:  *mut c_char = null_mut();
    let versionName: *mut c_char = null_mut();
    let cascade = false;

    /* Check extension name validity before any filesystem access */
    /* check_valid_extension_name(stmt->extname) */

    /*
     * Check for duplicate extension name.  The unique index on
     * pg_extension.extname would catch this anyway, and serves as a backstop
     * in case of race conditions; but this is a friendlier error message, and
     * besides we need a check to support IF NOT EXISTS.
     */
    /* TODO(pg-port): access stmt->extname, stmt->if_not_exists, stmt->options */
    unimplemented!("TODO(pg-port): CreateExtension - needs stmt field access")
}

/*
 * InsertExtensionTuple
 *
 * Insert the new pg_extension row, and create extension's dependency entries.
 * Return the OID assigned to the new row.
 *
 * This is exported for the benefit of pg_upgrade, which has to create a
 * pg_extension entry (and the extension-level dependencies) without
 * actually running the extension's script.
 *
 * extConfig and extCondition should be arrays or PointerGetDatum(NULL).
 * We declare them as plain Datum to avoid needing array.h in extension.h.
 */
pub unsafe fn InsertExtensionTuple(
    extName: *const c_char,
    extOwner: Oid,
    schemaOid: Oid,
    relocatable: bool,
    extVersion: *const c_char,
    extConfig: Datum,
    extCondition: Datum,
    requiredExtensions: *mut List,
) -> ObjectAddress {
    let extensionOid: Oid;
    let rel: Relation;
    let mut values: [Datum; Natts_pg_extension] = [0; Natts_pg_extension];
    let mut nulls:  [bool; Natts_pg_extension]  = [false; Natts_pg_extension];
    let tuple: HeapTuple;
    let mut myself = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut nsp    = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let refobjs: ObjectAddresses;

    /*
     * Build and insert the pg_extension tuple
     */
    rel = table_open(ExtensionRelationId, RowExclusiveLock);

    c_memset(values.as_mut_ptr() as *mut c_void, 0, core::mem::size_of_val(&values));
    c_memset(nulls.as_mut_ptr() as *mut c_void, 0, core::mem::size_of_val(&nulls));

    extensionOid = GetNewOidWithIndex(rel, ExtensionOidIndexId, Anum_pg_extension_oid);
    values[Anum_pg_extension_oid as usize - 1]          = ObjectIdGetDatum(extensionOid);
    values[Anum_pg_extension_extname as usize - 1]      =
        DirectFunctionCall1(namein, CStringGetDatum(extName));
    values[Anum_pg_extension_extowner as usize - 1]     = ObjectIdGetDatum(extOwner);
    values[Anum_pg_extension_extnamespace as usize - 1] = ObjectIdGetDatum(schemaOid);
    values[Anum_pg_extension_extrelocatable as usize - 1] = BoolGetDatum(relocatable);
    values[Anum_pg_extension_extversion as usize - 1]   = CStringGetTextDatum(extVersion);

    if extConfig == PointerGetDatum(null()) {
        nulls[Anum_pg_extension_extconfig as usize - 1] = true;
    } else {
        values[Anum_pg_extension_extconfig as usize - 1] = extConfig;
    }

    if extCondition == PointerGetDatum(null()) {
        nulls[Anum_pg_extension_extcondition as usize - 1] = true;
    } else {
        values[Anum_pg_extension_extcondition as usize - 1] = extCondition;
    }

    tuple = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);
    table_close(rel, RowExclusiveLock);

    /*
     * Record dependencies on owner, schema, and prerequisite extensions
     */
    recordDependencyOnOwner(ExtensionRelationId, extensionOid, extOwner);

    refobjs = new_object_addresses();

    ObjectAddressSet(&mut myself, ExtensionRelationId, extensionOid);

    ObjectAddressSet(&mut nsp, NamespaceRelationId, schemaOid);
    add_exact_object_address(&nsp, refobjs);

    /* foreach(lc, requiredExtensions) */
    /* TODO(pg-port): foreach over requiredExtensions */
    let _ = requiredExtensions;

    /* Record all of them (this includes duplicate elimination) */
    record_object_address_dependencies(&myself, refobjs, DEPENDENCY_NORMAL);
    free_object_addresses(refobjs);

    /* Post creation hook for new extension */
    InvokeObjectPostCreateHook(ExtensionRelationId, extensionOid, 0);

    myself
}

/*
 * Guts of extension deletion.
 *
 * All we need do here is remove the pg_extension tuple itself.  Everything
 * else is taken care of by the dependency infrastructure.
 */
pub unsafe fn RemoveExtensionById(extId: Oid) {
    let rel: Relation;
    let scandesc: SysScanDesc;
    let tuple: HeapTuple;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed(); 1];

    /*
     * Disallow deletion of any extension that's currently open for insertion;
     * else subsequent executions of recordDependencyOnCurrentExtension()
     * could create dangling pg_depend records that refer to a no-longer-valid
     * pg_extension OID.  This is needed not so much because we think people
     * might write "DROP EXTENSION foo" in foo's own script files, as because
     * errors in dependency management in extension script files could give
     * rise to cases where an extension is dropped as a result of recursing
     * from some contained object.  Because of that, we must test for the case
     * here, not at some higher level of the DROP EXTENSION command.
     */
    if extId == CurrentExtensionObject {
        ereport!(
            ERROR,
            errmsg!(
                "cannot drop extension \"{}\" because it is being modified",
                std::ffi::CStr::from_ptr(get_extension_name(extId)).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        );
    }

    rel = table_open(ExtensionRelationId, RowExclusiveLock);

    ScanKeyInit(
        entry.as_mut_ptr(),
        Anum_pg_extension_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(extId),
    );
    scandesc = systable_beginscan(rel, ExtensionOidIndexId, true, null_mut(), 1, entry.as_mut_ptr());

    tuple = systable_getnext(scandesc);

    /* We assume that there can be at most one matching tuple */
    if HeapTupleIsValid(tuple) {
        /* CatalogTupleDelete(rel, &tuple->t_self) */
        CatalogTupleDelete(rel, null()); /* TODO(pg-port): &tuple->t_self */
    }

    systable_endscan(scandesc);

    table_close(rel, RowExclusiveLock);
}

/*
 * This function lists the available extensions (one row per primary control
 * file in the control directory).  We parse each control file and report the
 * interesting fields.
 *
 * The system view pg_available_extensions provides a user interface to this
 * SRF, adding information about whether the extensions are installed in the
 * current DB.
 */
pub unsafe fn pg_available_extensions(fcinfo: *mut c_void) -> Datum {
    let rsinfo = fcinfo as *mut ReturnSetInfo;
    let locations: *mut List;
    let dir: *mut c_void = null_mut();

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    locations = get_extension_control_directories();

    /* foreach_ptr(char, location, locations) */
    /* TODO(pg-port): foreach_ptr iteration */
    let _ = (rsinfo, locations, dir);

    0 as Datum
}

/*
 * This function lists the available extension versions (one row per
 * extension installation script).  For each version, we parse the related
 * control file(s) and report the interesting fields.
 *
 * The system view pg_available_extension_versions provides a user interface
 * to this SRF, adding information about which versions are installed in the
 * current DB.
 */
pub unsafe fn pg_available_extension_versions(fcinfo: *mut c_void) -> Datum {
    let rsinfo = fcinfo as *mut ReturnSetInfo;
    let locations: *mut List;

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    locations = get_extension_control_directories();

    /* foreach_ptr(char, location, locations) */
    /* TODO(pg-port): foreach_ptr iteration */
    let _ = (rsinfo, locations);

    0 as Datum
}

/*
 * Inner loop for pg_available_extension_versions:
 *      read versions of one extension, add rows to tupstore
 */
unsafe fn get_available_versions_for_extension(
    pcontrol: *mut ExtensionControlFile,
    tupstore: Tuplestorestate,
    tupdesc: TupleDesc,
) {
    let evi_list: *mut List;

    /* Extract the version update graph from the script directory */
    evi_list = get_ext_ver_list(pcontrol);

    /* For each installable version ... */
    /* foreach(lc, evi_list) */
    /* TODO(pg-port): foreach iteration */
    let _ = (evi_list, tupstore, tupdesc);
}

/*
 * Test whether the given extension exists (not whether it's installed)
 *
 * This checks for the existence of a matching control file in the extension
 * directory.  That's not a bulletproof check, since the file might be
 * invalid, but this is only used for hints so it doesn't have to be 100%
 * right.
 */
pub unsafe fn extension_file_exists(extensionName: *const c_char) -> bool {
    let result = false;
    let locations: *mut List;
    let dir: *mut c_void = null_mut();

    locations = get_extension_control_directories();

    /* foreach_ptr(char, location, locations) */
    /* TODO(pg-port): foreach_ptr iteration */
    let _ = (locations, dir, extensionName);

    result
}

/*
 * Convert a list of extension names to a name[] Datum
 */
unsafe fn convert_requires_to_datum(requires: *mut List) -> Datum {
    let ndatums: c_int;
    let datums: *mut Datum;
    let a: *mut ArrayType;

    ndatums = list_length(requires);
    datums = palloc((ndatums as usize) * core::mem::size_of::<Datum>()) as *mut Datum;
    /* ndatums = 0; foreach populate datums */
    /* TODO(pg-port): foreach over requires */
    let _ = requires;

    a = construct_array_builtin(datums, 0, NAMEOID);
    PointerGetDatum(a as *const c_void)
}

/*
 * This function reports the version update paths that exist for the
 * specified extension.
 */
pub unsafe fn pg_extension_update_paths(fcinfo: *mut c_void) -> Datum {
    /* Name extname = PG_GETARG_NAME(0) */
    let rsinfo = fcinfo as *mut ReturnSetInfo;
    let evi_list: *mut List;
    let control: *mut ExtensionControlFile;

    /* Check extension name validity before any filesystem access */
    /* check_valid_extension_name(NameStr(*extname)) */

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    /* Read the extension's control file */
    /* control = read_extension_control_file(NameStr(*extname)) */
    /* TODO(pg-port): PG_GETARG_NAME + NameStr */

    /* Extract the version update graph from the script directory */
    /* evi_list = get_ext_ver_list(control) */

    /* Iterate over all pairs of versions */
    /* TODO(pg-port): foreach nested loops */

    let _ = rsinfo;

    0 as Datum
}
// -------------------------  END PART 4  ----------------------------------

// =========================================================================
// PART 5: pg_extension_config_dump, pg_get_loaded_modules,
//         extension_config_remove, AlterExtensionNamespace,
//         ExecAlterExtensionStmt, ApplyExtensionUpdates,
//         ExecAlterExtensionContentsStmt, ExecAlterExtensionContentsRecurse,
//         read_whole_file, new_ExtensionControlFile, find_in_paths
// =========================================================================

/*
 * pg_extension_config_dump
 *
 * Record information about a configuration table that belongs to an
 * extension being created, but whose contents should be dumped in whole
 * or in part during pg_dump.
 */
pub unsafe fn pg_extension_config_dump(fcinfo: *mut c_void) -> Datum {
    /* Oid tableoid = PG_GETARG_OID(0) */
    /* text *wherecond = PG_GETARG_TEXT_PP(1) */
    let tableoid: Oid = 0; /* TODO(pg-port): PG_GETARG_OID(0) */
    let mut wherecond: Datum = 0; /* TODO(pg-port): PG_GETARG_TEXT_PP(1) */
    let tablename: *mut c_char;
    let extRel: Relation;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    let extScan: SysScanDesc;
    let mut extTup: HeapTuple;
    let arrayDatum: Datum;
    let elementDatum: Datum;
    let arrayLength: c_int;
    let mut arrayIndex: c_int;
    let mut isnull: bool = false;
    let mut repl_val:  [Datum; Natts_pg_extension] = [0; Natts_pg_extension];
    let mut repl_null: [bool;  Natts_pg_extension] = [false; Natts_pg_extension];
    let mut repl_repl: [bool;  Natts_pg_extension] = [false; Natts_pg_extension];
    let a: *mut ArrayType;

    /*
     * We only allow this to be called from an extension's SQL script. We
     * shouldn't need any permissions check beyond that.
     */
    if !creating_extension {
        ereport!(
            ERROR,
            errmsg!(
                "{} can only be called from an SQL script executed by CREATE EXTENSION",
                "pg_extension_config_dump()"
            )
            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /*
     * Check that the table exists and is a member of the extension being
     * created.  This ensures that we don't need to register an additional
     * dependency to protect the extconfig entry.
     */
    tablename = get_rel_name(tableoid);
    if tablename.is_null() {
        ereport!(
            ERROR,
            errmsg!("OID {} does not refer to a table", tableoid)
            /* errcode(ERRCODE_UNDEFINED_TABLE) */
        );
    }
    if getExtensionOfObject(RelationRelationId, tableoid) != CurrentExtensionObject {
        ereport!(
            ERROR,
            errmsg!(
                "table \"{}\" is not a member of the extension being created",
                std::ffi::CStr::from_ptr(tablename).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        );
    }

    /*
     * Add the table OID and WHERE condition to the extension's extconfig and
     * extcondition arrays.
     *
     * If the table is already in extconfig, treat this as an update of the
     * WHERE condition.
     */

    /* Find the pg_extension tuple */
    extRel = table_open(ExtensionRelationId, RowExclusiveLock);

    ScanKeyInit(
        key.as_mut_ptr(),
        Anum_pg_extension_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(CurrentExtensionObject),
    );

    extScan = systable_beginscan(extRel, ExtensionOidIndexId, true, null_mut(), 1, key.as_mut_ptr());

    extTup = systable_getnext(extScan);

    if !HeapTupleIsValid(extTup) {
        /* should not happen */
        elog!(ERROR, "could not find tuple for extension {}", CurrentExtensionObject);
    }

    c_memset(repl_val.as_mut_ptr()  as *mut c_void, 0,     core::mem::size_of_val(&repl_val));
    c_memset(repl_null.as_mut_ptr() as *mut c_void, false as c_int, core::mem::size_of_val(&repl_null));
    c_memset(repl_repl.as_mut_ptr() as *mut c_void, false as c_int, core::mem::size_of_val(&repl_repl));

    /* Build or modify the extconfig value */
    elementDatum = ObjectIdGetDatum(tableoid);

    let arrayDatum = heap_getattr(
        extTup,
        Anum_pg_extension_extconfig,
        RelationGetDescr(extRel),
        &mut isnull,
    );
    if isnull {
        /* Previously empty extconfig, so build 1-element array */
        arrayLength = 0;
        arrayIndex = 1;

        let a = construct_array_builtin(&mut ObjectIdGetDatum(tableoid) as *mut Datum, 1, OIDOID);
        repl_val[Anum_pg_extension_extconfig as usize - 1] = PointerGetDatum(a as *const c_void);
    } else {
        /* Modify or extend existing extconfig array */
        let a_ptr = DatumGetArrayTypeP(arrayDatum);

        arrayLength = *ARR_DIMS(a_ptr);
        if ARR_NDIM(a_ptr) != 1
            || *ARR_LBOUND(a_ptr) != 1
            || arrayLength < 0
            || ARR_HASNULL(a_ptr)
            || ARR_ELEMTYPE(a_ptr) != OIDOID
        {
            elog!(ERROR, "extconfig is not a 1-D Oid array");
        }
        let arrayData = ARR_DATA_PTR(a_ptr) as *mut Oid;

        arrayIndex = arrayLength + 1; /* set up to add after end */

        let mut i: c_int = 0;
        while i < arrayLength {
            if *arrayData.add(i as usize) == tableoid {
                arrayIndex = i + 1; /* replace this element instead */
                break;
            }
            i += 1;
        }

        let new_a = array_set(
            a_ptr,
            1,
            &mut arrayIndex,
            elementDatum,
            false,
            -1,                     /* varlena array */
            core::mem::size_of::<Oid>() as c_int, /* OID's typlen */
            true,                   /* OID's typbyval */
            TYPALIGN_INT,           /* OID's typalign */
        );
        repl_val[Anum_pg_extension_extconfig as usize - 1] = PointerGetDatum(new_a as *const c_void);
    }
    repl_repl[Anum_pg_extension_extconfig as usize - 1] = true;

    /* Build or modify the extcondition value */
    let arrayDatum2 = heap_getattr(
        extTup,
        Anum_pg_extension_extcondition,
        RelationGetDescr(extRel),
        &mut isnull,
    );
    if isnull {
        if arrayLength != 0 {
            elog!(ERROR, "extconfig and extcondition arrays do not match");
        }

        let a2 = construct_array_builtin(&mut wherecond as *mut Datum, 1, TEXTOID);
        repl_val[Anum_pg_extension_extcondition as usize - 1] = PointerGetDatum(a2 as *const c_void);
    } else {
        let a2_ptr = DatumGetArrayTypeP(arrayDatum2);

        if ARR_NDIM(a2_ptr) != 1
            || *ARR_LBOUND(a2_ptr) != 1
            || ARR_HASNULL(a2_ptr)
            || ARR_ELEMTYPE(a2_ptr) != TEXTOID
        {
            elog!(ERROR, "extcondition is not a 1-D text array");
        }
        if *ARR_DIMS(a2_ptr) != arrayLength {
            elog!(ERROR, "extconfig and extcondition arrays do not match");
        }

        /* Add or replace at same index as in extconfig */
        let new_a2 = array_set(
            a2_ptr,
            1,
            &mut arrayIndex,
            wherecond,
            false,
            -1,     /* varlena array */
            -1,     /* TEXT's typlen */
            false,  /* TEXT's typbyval */
            TYPALIGN_INT, /* TEXT's typalign */
        );
        repl_val[Anum_pg_extension_extcondition as usize - 1] = PointerGetDatum(new_a2 as *const c_void);
    }
    repl_repl[Anum_pg_extension_extcondition as usize - 1] = true;

    extTup = heap_modify_tuple(
        extTup,
        RelationGetDescr(extRel),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(extRel, null_mut(), extTup); /* TODO(pg-port): &extTup->t_self */

    systable_endscan(extScan);

    table_close(extRel, RowExclusiveLock);

    /* PG_RETURN_VOID() */
    0 as Datum
}

/*
 * pg_get_loaded_modules
 *
 * SQL-callable function to get per-loaded-module information.  Modules
 * (shared libraries) aren't necessarily one-to-one with extensions, but
 * they're sufficiently closely related to make this file a good home.
 */
pub unsafe fn pg_get_loaded_modules(fcinfo: *mut c_void) -> Datum {
    let rsinfo = fcinfo as *mut ReturnSetInfo;
    let mut file_scanner: *mut DynamicFileList;

    /* Build tuplestore to hold the result rows */
    InitMaterializedSRF(fcinfo, 0);

    file_scanner = get_first_loaded_module();
    while !file_scanner.is_null() {
        let mut library_path: *const c_char = null();
        let mut module_name:  *const c_char = null();
        let mut module_version: *const c_char = null();
        let sep: *const c_char;
        let mut values: [Datum; 3] = [0; 3];
        let mut nulls:  [bool; 3]  = [false; 3];

        get_loaded_module_details(
            file_scanner,
            &mut library_path,
            &mut module_name,
            &mut module_version,
        );

        if module_name.is_null() {
            nulls[0] = true;
        } else {
            values[0] = CStringGetTextDatum(module_name);
        }
        if module_version.is_null() {
            nulls[1] = true;
        } else {
            values[1] = CStringGetTextDatum(module_version);
        }

        /* For security reasons, we don't show the directory path */
        sep = last_dir_separator(library_path);
        let library_path = if !sep.is_null() { sep.add(1) } else { library_path };
        values[2] = CStringGetTextDatum(library_path);

        /* tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls) */
        let _ = rsinfo;

        file_scanner = get_next_loaded_module(file_scanner);
    }

    0 as Datum
}

/*
 * extension_config_remove
 *
 * Remove the specified table OID from extension's extconfig, if present.
 * This is not currently exposed as a function, but it could be;
 * for now, we just invoke it from ALTER EXTENSION DROP.
 */
unsafe fn extension_config_remove(extensionoid: Oid, tableoid: Oid) {
    let extRel: Relation;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    let extScan: SysScanDesc;
    let mut extTup: HeapTuple;
    let arrayDatum: Datum;
    let mut arrayLength: c_int = 0;
    let mut arrayIndex: c_int;
    let mut isnull: bool = false;
    let mut repl_val:  [Datum; Natts_pg_extension] = [0; Natts_pg_extension];
    let mut repl_null: [bool;  Natts_pg_extension] = [false; Natts_pg_extension];
    let mut repl_repl: [bool;  Natts_pg_extension] = [false; Natts_pg_extension];

    /* Find the pg_extension tuple */
    extRel = table_open(ExtensionRelationId, RowExclusiveLock);

    ScanKeyInit(
        key.as_mut_ptr(),
        Anum_pg_extension_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(extensionoid),
    );

    extScan = systable_beginscan(extRel, ExtensionOidIndexId, true, null_mut(), 1, key.as_mut_ptr());

    extTup = systable_getnext(extScan);

    if !HeapTupleIsValid(extTup) {
        /* should not happen */
        elog!(ERROR, "could not find tuple for extension {}", extensionoid);
    }

    /* Search extconfig for the tableoid */
    let arrayDatum = heap_getattr(
        extTup,
        Anum_pg_extension_extconfig,
        RelationGetDescr(extRel),
        &mut isnull,
    );
    if isnull {
        /* nothing to do */
        arrayLength = 0;
        arrayIndex  = -1;
    } else {
        let a_ptr = DatumGetArrayTypeP(arrayDatum);

        arrayLength = *ARR_DIMS(a_ptr);
        if ARR_NDIM(a_ptr) != 1
            || *ARR_LBOUND(a_ptr) != 1
            || arrayLength < 0
            || ARR_HASNULL(a_ptr)
            || ARR_ELEMTYPE(a_ptr) != OIDOID
        {
            elog!(ERROR, "extconfig is not a 1-D Oid array");
        }
        let arrayData = ARR_DATA_PTR(a_ptr) as *mut Oid;

        arrayIndex = -1; /* flag for no deletion needed */

        let mut i: c_int = 0;
        while i < arrayLength {
            if *arrayData.add(i as usize) == tableoid {
                arrayIndex = i; /* index to remove */
                break;
            }
            i += 1;
        }
    }

    /* If tableoid is not in extconfig, nothing to do */
    if arrayIndex < 0 {
        systable_endscan(extScan);
        table_close(extRel, RowExclusiveLock);
        return;
    }

    /* Modify or delete the extconfig value */
    c_memset(repl_val.as_mut_ptr()  as *mut c_void, 0,     core::mem::size_of_val(&repl_val));
    c_memset(repl_null.as_mut_ptr() as *mut c_void, false as c_int, core::mem::size_of_val(&repl_null));
    c_memset(repl_repl.as_mut_ptr() as *mut c_void, false as c_int, core::mem::size_of_val(&repl_repl));

    if arrayLength <= 1 {
        /* removing only element, just set array to null */
        repl_null[Anum_pg_extension_extconfig as usize - 1] = true;
    } else {
        /* squeeze out the target element */
        let mut dvalues: *mut Datum = null_mut();
        let mut nelems: c_int = 0;

        let a_ptr = DatumGetArrayTypeP(arrayDatum);
        /* We already checked there are no nulls */
        deconstruct_array_builtin(a_ptr, OIDOID, &mut dvalues, null_mut(), &mut nelems);

        let mut i = arrayIndex;
        while i < arrayLength - 1 {
            *dvalues.add(i as usize) = *dvalues.add((i + 1) as usize);
            i += 1;
        }

        let new_a = construct_array_builtin(dvalues, arrayLength - 1, OIDOID);
        repl_val[Anum_pg_extension_extconfig as usize - 1] = PointerGetDatum(new_a as *const c_void);
    }
    repl_repl[Anum_pg_extension_extconfig as usize - 1] = true;

    /* Modify or delete the extcondition value */
    let arrayDatum2 = heap_getattr(
        extTup,
        Anum_pg_extension_extcondition,
        RelationGetDescr(extRel),
        &mut isnull,
    );
    if isnull {
        elog!(ERROR, "extconfig and extcondition arrays do not match");
    } else {
        let a2_ptr = DatumGetArrayTypeP(arrayDatum2);

        if ARR_NDIM(a2_ptr) != 1
            || *ARR_LBOUND(a2_ptr) != 1
            || ARR_HASNULL(a2_ptr)
            || ARR_ELEMTYPE(a2_ptr) != TEXTOID
        {
            elog!(ERROR, "extcondition is not a 1-D text array");
        }
        if *ARR_DIMS(a2_ptr) != arrayLength {
            elog!(ERROR, "extconfig and extcondition arrays do not match");
        }
    }

    if arrayLength <= 1 {
        /* removing only element, just set array to null */
        repl_null[Anum_pg_extension_extcondition as usize - 1] = true;
    } else {
        /* squeeze out the target element */
        let a2_ptr = DatumGetArrayTypeP(arrayDatum2);
        let mut dvalues: *mut Datum = null_mut();
        let mut nelems: c_int = 0;

        /* We already checked there are no nulls */
        deconstruct_array_builtin(a2_ptr, TEXTOID, &mut dvalues, null_mut(), &mut nelems);

        let mut i = arrayIndex;
        while i < arrayLength - 1 {
            *dvalues.add(i as usize) = *dvalues.add((i + 1) as usize);
            i += 1;
        }

        let new_a2 = construct_array_builtin(dvalues, arrayLength - 1, TEXTOID);
        repl_val[Anum_pg_extension_extcondition as usize - 1] = PointerGetDatum(new_a2 as *const c_void);
    }
    repl_repl[Anum_pg_extension_extcondition as usize - 1] = true;

    extTup = heap_modify_tuple(
        extTup,
        RelationGetDescr(extRel),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(extRel, null_mut(), extTup); /* TODO(pg-port): &extTup->t_self */

    systable_endscan(extScan);

    table_close(extRel, RowExclusiveLock);
}

/*
 * Execute ALTER EXTENSION SET SCHEMA
 */
pub unsafe fn AlterExtensionNamespace(
    extensionName: *const c_char,
    newschema: *const c_char,
    oldschema: *mut Oid,
) -> ObjectAddress {
    let extensionOid: Oid;
    let nspOid: Oid;
    let oldNspOid: Oid;
    let aclresult: AclResult = ACLCHECK_OK;
    let extRel: Relation;
    let mut key: [ScanKeyData; 2] = [core::mem::zeroed(); 2];
    let extScan: SysScanDesc;
    let extTup: HeapTuple;
    let extForm: *mut pg_extension_form;
    let depRel: Relation;
    let depScan: SysScanDesc;
    let objsMoved: ObjectAddresses;
    let mut extAddr = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

    extensionOid = get_extension_oid(extensionName, false);

    nspOid = LookupCreationNamespace(newschema);

    /*
     * Permission check: must own extension.  Note that we don't bother to
     * check ownership of the individual member objects ...
     */
    if !object_ownercheck(ExtensionRelationId, extensionOid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_EXTENSION, extensionName);
    }

    /* Permission check: must have creation rights in target namespace */
    let aclresult = object_aclcheck(NamespaceRelationId, nspOid, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, newschema);
    }

    /*
     * If the schema is currently a member of the extension, disallow moving
     * the extension into the schema.  That would create a dependency loop.
     */
    if getExtensionOfObject(NamespaceRelationId, nspOid) == extensionOid {
        ereport!(
            ERROR,
            errmsg!(
                "cannot move extension \"{}\" into schema \"{}\" because the extension contains the schema",
                std::ffi::CStr::from_ptr(extensionName).to_string_lossy(),
                std::ffi::CStr::from_ptr(newschema).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        );
    }

    /* Locate the pg_extension tuple */
    extRel = table_open(ExtensionRelationId, RowExclusiveLock);

    ScanKeyInit(
        key.as_mut_ptr(),
        Anum_pg_extension_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(extensionOid),
    );

    extScan = systable_beginscan(extRel, ExtensionOidIndexId, true, null_mut(), 1, key.as_mut_ptr());

    let extTup = systable_getnext(extScan);

    if !HeapTupleIsValid(extTup) {
        /* should not happen */
        elog!(ERROR, "could not find tuple for extension {}", extensionOid);
    }

    /* Copy tuple so we can modify it below */
    let extTup = heap_copytuple(extTup);
    extForm = GETSTRUCT(extTup) as *mut pg_extension_form;

    systable_endscan(extScan);

    /*
     * If the extension is already in the target schema, just silently do
     * nothing.
     */
    if (*extForm).extnamespace == nspOid {
        table_close(extRel, RowExclusiveLock);
        return InvalidObjectAddress;
    }

    /* Check extension is supposed to be relocatable */
    /* if !extForm->extrelocatable ... */

    objsMoved = new_object_addresses();

    /* store the OID of the namespace to-be-changed */
    let oldNspOid = (*extForm).extnamespace;

    /*
     * Scan pg_depend to find objects that depend directly on the extension,
     * and alter each one's schema.
     */
    depRel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        key.as_mut_ptr(),
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(ExtensionRelationId),
    );
    ScanKeyInit(
        key.as_mut_ptr().add(1),
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(extensionOid),
    );

    let depScan = systable_beginscan(depRel, DependReferenceIndexId, true, null_mut(), 2, key.as_mut_ptr());

    loop {
        let depTup = systable_getnext(depScan);
        if !HeapTupleIsValid(depTup) {
            break;
        }
        let pg_depend = GETSTRUCT(depTup) as *mut pg_depend_form;
        let mut dep = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

        /*
         * If a dependent extension has a no_relocate request for this
         * extension, disallow SET SCHEMA.
         */
        if (*pg_depend).deptype == DEPENDENCY_NORMAL
            && (*pg_depend).classid == ExtensionRelationId
        {
            let depextname = get_extension_name((*pg_depend).objid);
            let dcontrol = read_extension_control_file(depextname);
            /* foreach(lc, dcontrol->no_relocate) */
            /* TODO(pg-port): foreach over no_relocate */
            let _ = dcontrol;
        }

        /*
         * Otherwise, ignore non-membership dependencies.
         */
        if (*pg_depend).deptype != DEPENDENCY_EXTENSION {
            continue;
        }

        dep.classId    = (*pg_depend).classid;
        dep.objectId   = (*pg_depend).objid;
        dep.objectSubId = (*pg_depend).objsubid;

        if dep.objectSubId != 0 {
            /* should not happen */
            elog!(ERROR, "extension should not have a sub-object dependency");
        }

        /* Relocate the object */
        let dep_oldNspOid = AlterObjectNamespace_oid(dep.classId, dep.objectId, nspOid, objsMoved);

        /*
         * If not all the objects had the same old namespace (ignoring any
         * that are not in namespaces or are dependent types), complain.
         */
        if OidIsValid(dep_oldNspOid) && dep_oldNspOid != oldNspOid {
            ereport!(
                ERROR,
                errmsg!(
                    "extension \"{}\" does not support SET SCHEMA",
                    std::ffi::CStr::from_ptr((*extForm).extname.as_ptr()).to_string_lossy()
                )
                /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errdetail(...) */
            );
        }
    }

    /* report old schema, if caller wants it */
    if !oldschema.is_null() {
        *oldschema = oldNspOid;
    }

    systable_endscan(depScan);

    relation_close(depRel, AccessShareLock);

    /* Now adjust pg_extension.extnamespace */
    (*extForm).extnamespace = nspOid;

    CatalogTupleUpdate(extRel, null_mut(), extTup); /* TODO(pg-port): &extTup->t_self */

    table_close(extRel, RowExclusiveLock);

    /* update dependency to point to the new schema */
    if changeDependencyFor(ExtensionRelationId, extensionOid, NamespaceRelationId, oldNspOid, nspOid) != 1 {
        elog!(
            ERROR,
            "could not change schema dependency for extension {}",
            std::ffi::CStr::from_ptr((*extForm).extname.as_ptr()).to_string_lossy()
        );
    }

    InvokeObjectPostAlterHook(ExtensionRelationId, extensionOid, 0);

    ObjectAddressSet(&mut extAddr, ExtensionRelationId, extensionOid);

    extAddr
}

/* pg_depend form stub for field access */
#[repr(C)]
struct pg_depend_form {
    classid:    Oid,
    objid:      Oid,
    objsubid:   c_int,
    refclassid: Oid,
    refobjid:   Oid,
    refobjsubid: c_int,
    deptype:    DependencyType,
}

/*
 * Execute ALTER EXTENSION UPDATE
 */
pub unsafe fn ExecAlterExtensionStmt(
    pstate: ParseState,
    stmt: *mut AlterExtensionStmt,
) -> ObjectAddress {
    /* TODO(pg-port): needs AlterExtensionStmt field access */
    unimplemented!("TODO(pg-port): ExecAlterExtensionStmt - needs stmt field access")
}

/*
 * Apply a series of update scripts as though individual ALTER EXTENSION
 * UPDATE commands had been given, including altering the pg_extension row
 * and dependencies each time.
 *
 * This might be more work than necessary, but it ensures that old update
 * scripts don't break if newer versions have different control parameters.
 */
unsafe fn ApplyExtensionUpdates(
    extensionOid: Oid,
    pcontrol: *mut ExtensionControlFile,
    initialVersion: *const c_char,
    updateVersions: *mut List,
    origSchemaName: *mut c_char,
    cascade: bool,
    is_create: bool,
) {
    let oldVersionName = initialVersion;

    /* foreach(lcv, updateVersions) */
    /* TODO(pg-port): foreach over updateVersions */
    let _ = (extensionOid, pcontrol, updateVersions, origSchemaName, cascade, is_create, oldVersionName);
}

/*
 * Execute ALTER EXTENSION ADD/DROP
 *
 * Return value is the address of the altered extension.
 *
 * objAddr is an output argument which, if not NULL, is set to the address of
 * the added/dropped object.
 */
pub unsafe fn ExecAlterExtensionContentsStmt(
    stmt: *mut AlterExtensionContentsStmt,
    objAddr: *mut ObjectAddress,
) -> ObjectAddress {
    let extension: ObjectAddress;
    let object: ObjectAddress;
    let relation: Relation = null_mut();

    /* TODO(pg-port): needs AlterExtensionContentsStmt field access (objtype, extname, ...) */
    unimplemented!("TODO(pg-port): ExecAlterExtensionContentsStmt - needs stmt field access")
}

/*
 * ExecAlterExtensionContentsRecurse
 *      Subroutine for ExecAlterExtensionContentsStmt
 *
 * Do the bare alteration of object's membership in extension,
 * without permission checks.  Recurse to dependent objects, if any.
 */
unsafe fn ExecAlterExtensionContentsRecurse(
    stmt: *mut AlterExtensionContentsStmt,
    extension: ObjectAddress,
    object: ObjectAddress,
) {
    let oldExtension: Oid;

    /*
     * Check existing extension membership.
     */
    oldExtension = getExtensionOfObject(object.classId, object.objectId);

    /* TODO(pg-port): stmt->action needs field access */
    let action: c_int = 1; /* placeholder */

    if action > 0 {
        /*
         * ADD, so complain if object is already attached to some extension.
         */
        if OidIsValid(oldExtension) {
            ereport!(
                ERROR,
                errmsg!(
                    "{} is already a member of extension \"{}\"",
                    std::ffi::CStr::from_ptr(getObjectDescription(&object, false)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(get_extension_name(oldExtension)).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }

        /*
         * Prevent a schema from being added to an extension if the schema
         * contains the extension.  That would create a dependency loop.
         */
        if object.classId == NamespaceRelationId
            && object.objectId == get_extension_schema(extension.objectId)
        {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot add schema \"{}\" to extension \"{}\" because the schema contains the extension",
                    std::ffi::CStr::from_ptr(get_namespace_name(object.objectId)).to_string_lossy(),
                    /* stmt->extname */ ""
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }

        /*
         * OK, add the dependency.
         */
        recordDependencyOn(&object, &extension, DEPENDENCY_EXTENSION);

        /*
         * Also record the initial ACL on the object, if any.
         */
        recordExtObjInitPriv(object.objectId, object.classId);
    } else {
        /*
         * DROP, so complain if it's not a member.
         */
        if oldExtension != extension.objectId {
            ereport!(
                ERROR,
                errmsg!(
                    "{} is not a member of extension \"{}\"",
                    std::ffi::CStr::from_ptr(getObjectDescription(&object, false)).to_string_lossy(),
                    /* stmt->extname */ ""
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }

        /*
         * OK, drop the dependency.
         */
        if deleteDependencyRecordsForClass(
            object.classId,
            object.objectId,
            ExtensionRelationId,
            DEPENDENCY_EXTENSION,
        ) != 1
        {
            elog!(ERROR, "unexpected number of extension dependency records");
        }

        /*
         * If it's a relation, it might have an entry in the extension's
         * extconfig array, which we must remove.
         */
        if object.classId == RelationRelationId {
            extension_config_remove(extension.objectId, object.objectId);
        }

        /*
         * Remove all the initial ACLs, if any.
         */
        removeExtObjInitPriv(object.objectId, object.classId);
    }

    /*
     * Recurse to any dependent objects; currently, this includes the array
     * type of a base type, the multirange type associated with a range type,
     * and the rowtype of a table.
     */
    if object.classId == TypeRelationId {
        let mut depobject = ObjectAddress { classId: TypeRelationId, objectId: 0, objectSubId: 0 };

        /* If it has an array type, update that too */
        depobject.objectId = get_array_type(object.objectId);
        if OidIsValid(depobject.objectId) {
            ExecAlterExtensionContentsRecurse(stmt, extension, depobject);
        }

        /* If it is a range type, update the associated multirange too */
        if type_is_range(object.objectId) {
            depobject.objectId = get_range_multirange(object.objectId);
            if !OidIsValid(depobject.objectId) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not find multirange type for data type {}",
                        std::ffi::CStr::from_ptr(format_type_be(object.objectId)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_UNDEFINED_OBJECT) */
                );
            }
            ExecAlterExtensionContentsRecurse(stmt, extension, depobject);
        }
    }
    if object.classId == RelationRelationId {
        let mut depobject = ObjectAddress { classId: TypeRelationId, objectId: 0, objectSubId: 0 };

        /* It might not have a rowtype, but if it does, update that */
        depobject.objectId = get_rel_type_id(object.objectId);
        if OidIsValid(depobject.objectId) {
            ExecAlterExtensionContentsRecurse(stmt, extension, depobject);
        }
    }
}

/*
 * Read the whole of file into memory.
 *
 * The file contents are returned as a single palloc'd chunk. For convenience
 * of the callers, an extra \0 byte is added to the end.  That is not counted
 * in the length returned into *length.
 */
unsafe fn read_whole_file(filename: *const c_char, length: *mut c_int) -> *mut c_char {
    let buf: *mut c_char;
    let file: *mut c_void;
    let mut bytes_to_read: usize;
    let mut fst = stat { st_size: 0 };

    if stat_fn(filename, &mut fst) < 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not stat file \"{}\": ",
                std::ffi::CStr::from_ptr(filename).to_string_lossy()
            )
            /* errcode_for_file_access() */
        );
    }

    if fst.st_size > (MaxAllocSize as i64 - 1) {
        ereport!(
            ERROR,
            errmsg!(
                "file \"{}\" is too large",
                std::ffi::CStr::from_ptr(filename).to_string_lossy()
            )
            /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        );
    }
    bytes_to_read = fst.st_size as usize;

    file = AllocateFile(filename, PG_BINARY_R.as_ptr() as *const c_char);
    if file.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "could not open file \"{}\" for reading: ",
                std::ffi::CStr::from_ptr(filename).to_string_lossy()
            )
            /* errcode_for_file_access() */
        );
    }

    buf = palloc(bytes_to_read + 1) as *mut c_char;

    bytes_to_read = c_fread(buf as *mut c_void, 1, bytes_to_read, file);

    if c_ferror(file) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not read file \"{}\": ",
                std::ffi::CStr::from_ptr(filename).to_string_lossy()
            )
            /* errcode_for_file_access() */
        );
    }

    FreeFile(file);

    *buf.add(bytes_to_read) = 0;

    /*
     * On Windows, manually convert Windows-style newlines (\r\n) to the Unix
     * convention of \n only.  This avoids gotchas due to script files
     * possibly getting converted when being transferred between platforms.
     * Ideally we'd do this by using text mode to read the file, but that also
     * causes control-Z to be treated as end-of-file.  Historically we've
     * allowed control-Z in script files, so breaking that seems unwise.
     */
    #[cfg(target_os = "windows")]
    {
        let mut s = buf;
        let mut d = buf;
        while *s != 0 {
            if !(*s == b'\r' as c_char && *s.add(1) == b'\n' as c_char) {
                *d = *s;
                d = d.add(1);
            }
            s = s.add(1);
        }
        *d = 0;
        bytes_to_read = d.offset_from(buf) as usize;
    }

    *length = bytes_to_read as c_int;
    buf
}

unsafe fn new_ExtensionControlFile(extname: *const c_char) -> *mut ExtensionControlFile {
    /*
     * Set up default values.  Pointer fields are initially null.
     */
    let control = palloc0(core::mem::size_of::<ExtensionControlFile>()) as *mut ExtensionControlFile;

    (*control).name         = pstrdup(extname);
    (*control).relocatable  = false;
    (*control).superuser    = true;
    (*control).trusted      = false;
    (*control).encoding     = -1;

    control
}

/*
 * Work in a very similar way with find_in_path but it receives an already
 * parsed List of paths to search the basename and it do not support macro
 * replacement or custom error messages (for simplicity).
 *
 * By "already parsed List of paths" this function expected that paths already
 * have all macros replaced.
 */
pub unsafe fn find_in_paths(basename: *const c_char, paths: *mut List) -> *mut c_char {
    foreach!(cell, paths, {
        let mut path: *mut c_char = lfirst(current_cell!(cell)) as *mut c_char;

        /* Assert(path != NULL) */

        path = pstrdup(path);
        canonicalize_path(path);

        /* only absolute paths */
        if !is_absolute_path(path) {
            ereport!(
                ERROR,
                errmsg!(
                    "component in parameter \"{}\" is not an absolute path",
                    "extension_control_path"
                )
            );
            /* C also: errcode(ERRCODE_INVALID_NAME) */
        }

        let full: *mut c_char = psprintf3(
            b"%s/%s\0".as_ptr() as *const c_char,
            path,
            basename,
        );

        if pg_file_exists(full) {
            return full;
        }

        pfree(path as *mut c_void);
        pfree(full as *mut c_void);
    });

    null_mut()
}
// -------------------------  END PART 5  ----------------------------------

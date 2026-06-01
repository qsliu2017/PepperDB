/*-------------------------------------------------------------------------
 *
 * foreigncmds.rs
 *	  foreign-data wrapper/server creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/foreigncmds.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use crate::{foreach, current_cell, lfirst_node, IsA, makeNode};

use core::ffi::{c_char, c_int, c_void};

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::nodes::NodeTag::*;
use crate::nodes::pg_list::{lfirst, List, ListCell};
use crate::nodes::parsenodes::{
    AlterFdwStmt, AlterForeignServerStmt, AlterUserMappingStmt, CreateFdwStmt,
    CreateForeignServerStmt, CreateForeignTableStmt, CreateUserMappingStmt, DefElem,
    DefElemAction, DropUserMappingStmt, ImportForeignSchemaStmt, RoleSpec, RoleSpecType,
    DEFELEM_ADD, DEFELEM_DROP, DEFELEM_SET, DEFELEM_UNSPEC, ROLESPEC_PUBLIC,
};
use crate::nodes::plannodes::PlannedStmt;
use crate::parser::parse_node::ParseState;
use crate::catalog::objectaccess::ObjectAddress;

/* --------------------------------------------------------------------------
 * Local type aliases for unported dependencies
 * -------------------------------------------------------------------------- */

// HeapTuple is a single pointer (HeapTupleData*).
use crate::access::htup_details::HeapTupleData;
type HeapTuple = *mut HeapTupleData;

// Relation pointer
type RelationData = crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// TupleDesc  TODO(pg-port)
use crate::access::common::tupdesc::TupleDesc;

// ItemPointerData  TODO(pg-port)
#[repr(C)] pub struct ItemPointerData { _opaque: [u8; 6] }

// Acl  TODO(pg-port)
#[repr(C)] pub struct AclData { _opaque: [u8; 0] }
type Acl = AclData;

// AclResult  TODO(pg-port)
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const RowExclusiveLock: LOCKMODE = 3;

// AclMode bits  TODO(pg-port)
type AclMode = u64;
const ACL_USAGE: AclMode = 1 << 8;

// ACL_ID_PUBLIC  TODO(pg-port)
const ACL_ID_PUBLIC: Oid = 0;

// DropBehavior  TODO(pg-port)
type DropBehavior = c_int;
const DROP_RESTRICT: DropBehavior = 0;
const DROP_CASCADE: DropBehavior = 1;

// DependencyType  TODO(pg-port)
type DependencyType = c_int;
const DEPENDENCY_NORMAL: DependencyType = 0;

// ObjectType  TODO(pg-port)
type ObjectType = c_int;
const OBJECT_FDW: ObjectType = 0;
const OBJECT_FOREIGN_SERVER: ObjectType = 1;

// CmdType (nodes/nodes.h)
use crate::nodes::nodes::CmdType::CMD_UTILITY;

// CommandDest / DestReceiver  TODO(pg-port)
#[repr(C)] pub struct DestReceiver { _opaque: [u8; 0] }

// ProcessUtilityContext  TODO(pg-port)
type ProcessUtilityContext = c_int;
const PROCESS_UTILITY_SUBCOMMAND: ProcessUtilityContext = 2;

// QueryEnvironment / ParamListInfo / QueryCompletion  TODO(pg-port)
#[repr(C)] pub struct QueryEnvironment { _opaque: [u8; 0] }
#[repr(C)] pub struct ParamListInfoData { _opaque: [u8; 0] }
type ParamListInfo = *mut ParamListInfoData;
#[repr(C)] pub struct QueryCompletion { _opaque: [u8; 0] }

// RawStmt  TODO(pg-port)
#[repr(C)] pub struct RawStmt {
    pub r#type: NodeTag,
    pub stmt: *mut Node,
    pub stmt_location: c_int,
    pub stmt_len: c_int,
}

// ErrorContextCallback  TODO(pg-port)
#[repr(C)] pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(*mut c_void)>,
    pub arg: *mut c_void,
}

// ForeignDataWrapper / ForeignServer (foreign/foreign.h)  TODO(pg-port)
#[repr(C)] pub struct ForeignDataWrapper {
    pub fdwid: Oid,
    pub owner: Oid,
    pub fdwname: *mut c_char,
    pub fdwhandler: Oid,
    pub fdwvalidator: Oid,
    pub options: *mut List,
}
#[repr(C)] pub struct ForeignServer {
    pub serverid: Oid,
    pub fdwid: Oid,
    pub owner: Oid,
    pub servername: *mut c_char,
    pub servertype: *mut c_char,
    pub serverversion: *mut c_char,
    pub options: *mut List,
}

// FdwRoutine (foreign/fdwapi.h)  TODO(pg-port)
#[repr(C)] pub struct FdwRoutine {
    pub r#type: NodeTag,
    pub ImportForeignSchema:
        Option<unsafe extern "C" fn(*mut ImportForeignSchemaStmt, Oid) -> *mut List>,
}

// Form_pg_foreign_data_wrapper / Form_pg_foreign_server  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_foreign_data_wrapper {
    pub oid: Oid,
    pub fdwname: [c_char; 64],
    pub fdwowner: Oid,
    pub fdwhandler: Oid,
    pub fdwvalidator: Oid,
}
type Form_pg_foreign_data_wrapper = *mut FormData_pg_foreign_data_wrapper;
#[repr(C)] pub struct FormData_pg_foreign_server {
    pub oid: Oid,
    pub srvname: [c_char; 64],
    pub srvowner: Oid,
    pub srvfdw: Oid,
}
type Form_pg_foreign_server = *mut FormData_pg_foreign_server;

/* --------------------------------------------------------------------------
 * Catalog relation OIDs (catalog/pg_*.h)  TODO(pg-port)
 * -------------------------------------------------------------------------- */
const ForeignDataWrapperRelationId: Oid = 2328;
const ForeignServerRelationId: Oid = 1417;
const UserMappingRelationId: Oid = 1418;
const ForeignTableRelationId: Oid = 3118;
const ProcedureRelationId: Oid = 1255;
const RelationRelationId: Oid = 1259;

const ForeignDataWrapperOidIndexId: Oid = 2391;
const ForeignServerOidIndexId: Oid = 113;
const UserMappingOidIndexId: Oid = 174;

/* Catalog attribute numbers (1-based)  TODO(pg-port) */
const Natts_pg_foreign_data_wrapper: usize = 7;
const Anum_pg_foreign_data_wrapper_oid: c_int = 1;
const Anum_pg_foreign_data_wrapper_fdwname: c_int = 2;
const Anum_pg_foreign_data_wrapper_fdwowner: c_int = 3;
const Anum_pg_foreign_data_wrapper_fdwhandler: c_int = 4;
const Anum_pg_foreign_data_wrapper_fdwvalidator: c_int = 5;
const Anum_pg_foreign_data_wrapper_fdwacl: c_int = 6;
const Anum_pg_foreign_data_wrapper_fdwoptions: c_int = 7;

const Natts_pg_foreign_server: usize = 8;
const Anum_pg_foreign_server_oid: c_int = 1;
const Anum_pg_foreign_server_srvname: c_int = 2;
const Anum_pg_foreign_server_srvowner: c_int = 3;
const Anum_pg_foreign_server_srvfdw: c_int = 4;
const Anum_pg_foreign_server_srvtype: c_int = 5;
const Anum_pg_foreign_server_srvversion: c_int = 6;
const Anum_pg_foreign_server_srvacl: c_int = 7;
const Anum_pg_foreign_server_srvoptions: c_int = 8;

const Natts_pg_user_mapping: usize = 4;
const Anum_pg_user_mapping_oid: c_int = 1;
const Anum_pg_user_mapping_umuser: c_int = 2;
const Anum_pg_user_mapping_umserver: c_int = 3;
const Anum_pg_user_mapping_umoptions: c_int = 4;

const Natts_pg_foreign_table: usize = 3;
const Anum_pg_foreign_table_ftrelid: c_int = 1;
const Anum_pg_foreign_table_ftserver: c_int = 2;
const Anum_pg_foreign_table_ftoptions: c_int = 3;

/* Syscache IDs (utils/syscache.h)  TODO(pg-port) */
const FOREIGNDATAWRAPPERNAME: c_int = 0;
const FOREIGNDATAWRAPPEROID: c_int = 1;
const FOREIGNSERVERNAME: c_int = 2;
const FOREIGNSERVEROID: c_int = 3;
const USERMAPPINGUSERSERVER: c_int = 4;
const USERMAPPINGOID: c_int = 5;

/* Type OIDs (catalog/pg_type.h)  TODO(pg-port) */
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const TEXTARRAYOID: Oid = 1009;
const FDW_HANDLEROID: Oid = 3115;

/* VARHDRSZ (c.h)  TODO(pg-port) */
const VARHDRSZ: usize = 4;

/* InvalidObjectAddress  TODO(pg-port) */
const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

/* CurrentMemoryContext / MemoryContext  TODO(pg-port) */
#[repr(C)] pub struct MemoryContextData { _opaque: [u8; 0] }
type MemoryContext = *mut MemoryContextData;
extern "C" {
    static CurrentMemoryContext: MemoryContext;
}

/* ArrayBuildState (utils/array.h)  TODO(pg-port) */
#[repr(C)] pub struct ArrayBuildState { _opaque: [u8; 0] }

/* text (c.h)  TODO(pg-port) */
#[repr(C)] pub struct text { _opaque: [u8; 0] }

/* error_context_stack (utils/elog.h)  TODO(pg-port) */
extern "C" {
    static mut error_context_stack: *mut ErrorContextCallback;
}

/* None_Receiver (tcop/dest.h)  TODO(pg-port) */
extern "C" {
    static mut None_Receiver: *mut DestReceiver;
}

/* errcode classification constants (referenced in /* C also: */ folds) */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_FDW_NO_SCHEMAS: c_int = 0;

/* --------------------------------------------------------------------------
 * Stubs for functions defined in other .c files  TODO(pg-port)
 * -------------------------------------------------------------------------- */

unsafe fn table_open(relid: Oid, lockmode: LOCKMODE) -> Relation { unimplemented!() }
unsafe fn table_close(rel: Relation, lockmode: LOCKMODE) { unimplemented!() }

unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple { unimplemented!() }
unsafe fn GetSysCacheOid2(cacheId: c_int, oidAttNum: c_int, key1: Datum, key2: Datum) -> Oid {
    unimplemented!()
}
unsafe fn SysCacheGetAttr(
    cacheId: c_int,
    tup: HeapTuple,
    attributeNumber: c_int,
    isNull: *mut bool,
) -> Datum {
    unimplemented!()
}

unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool { !tup.is_null() }
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void { unimplemented!() }
unsafe fn heap_getattr(tup: HeapTuple, attnum: c_int, desc: TupleDesc, isnull: *mut bool) -> Datum {
    unimplemented!()
}
unsafe fn heap_form_tuple(tupleDescriptor: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple {
    unimplemented!()
}
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple {
    unimplemented!()
}
unsafe fn heap_freetuple(tup: HeapTuple) { unimplemented!() }

unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc { unimplemented!() }
unsafe fn rd_att(rel: Relation) -> TupleDesc { unimplemented!() }

unsafe fn CatalogTupleInsert(rel: Relation, tup: HeapTuple) -> Oid { unimplemented!() }
unsafe fn CatalogTupleUpdate(rel: Relation, otid: *mut ItemPointerData, tup: HeapTuple) {
    unimplemented!()
}
unsafe fn t_self(tup: HeapTuple) -> *mut ItemPointerData { unimplemented!() }

unsafe fn GetNewOidWithIndex(rel: Relation, indexId: Oid, oidcolno: c_int) -> Oid { unimplemented!() }

unsafe fn superuser() -> bool { unimplemented!() }
unsafe fn superuser_arg(roleid: Oid) -> bool { unimplemented!() }
unsafe fn GetUserId() -> Oid { unimplemented!() }

unsafe fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool { unimplemented!() }
unsafe fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: AclMode) -> AclResult {
    unimplemented!()
}
unsafe fn aclcheck_error(aclerr: AclResult, objtype: ObjectType, objectname: *const c_char) {
    unimplemented!()
}
unsafe fn check_can_set_role(member: Oid, role: Oid) { unimplemented!() }

unsafe fn aclnewowner(old_acl: *mut Acl, oldOwnerId: Oid, newOwnerId: Oid) -> *mut Acl {
    unimplemented!()
}
unsafe fn DatumGetAclP(X: Datum) -> *mut Acl { unimplemented!() }

unsafe fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwnerId: Oid) { unimplemented!() }
unsafe fn recordDependencyOn(
    depender: *const ObjectAddress,
    referenced: *const ObjectAddress,
    behavior: DependencyType,
) {
    unimplemented!()
}
unsafe fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) { unimplemented!() }
unsafe fn recordDependencyOnCurrentExtension(object: *const ObjectAddress, isReplace: bool) {
    unimplemented!()
}
unsafe fn deleteDependencyRecordsForClass(
    classId: Oid,
    objectId: Oid,
    refclassId: Oid,
    deptype: c_char,
) -> c_long {
    unimplemented!()
}
unsafe fn performDeletion(object: *const ObjectAddress, behavior: DropBehavior, flags: c_int) {
    unimplemented!()
}
unsafe fn checkMembershipInCurrentExtension(object: *const ObjectAddress) { unimplemented!() }

unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) {}
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {}

unsafe fn defGetString(def: *const DefElem) -> *mut c_char { unimplemented!() }
unsafe fn errorConflictingDefElem(def: *const DefElem, pstate: *mut ParseState) { unimplemented!() }

unsafe fn LookupFuncName(
    funcname: *const List,
    nargs: c_int,
    argtypes: *const Oid,
    missing_ok: bool,
) -> Oid {
    unimplemented!()
}
unsafe fn get_func_rettype(funcid: Oid) -> Oid { unimplemented!() }
unsafe fn NameListToString(names: *const List) -> *mut c_char { unimplemented!() }

unsafe fn untransformRelOptions(options: Datum) -> *mut List { unimplemented!() }
unsafe fn list_delete_cell(list: *mut List, cell: *mut ListCell) -> *mut List { unimplemented!() }
unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List { unimplemented!() }

unsafe fn accumArrayResult(
    astate: *mut ArrayBuildState,
    dvalue: Datum,
    disnull: bool,
    element_type: Oid,
    rcontext: MemoryContext,
) -> *mut ArrayBuildState {
    unimplemented!()
}
unsafe fn makeArrayResult(astate: *mut ArrayBuildState, rcontext: MemoryContext) -> Datum {
    unimplemented!()
}
unsafe fn construct_empty_array(elmtype: Oid) -> *mut c_void { unimplemented!() }

unsafe fn OidFunctionCall2(functionId: Oid, arg1: Datum, arg2: Datum) -> Datum { unimplemented!() }
unsafe fn DirectFunctionCall1(func: unsafe fn(*mut c_void) -> Datum, arg1: Datum) -> Datum {
    unimplemented!()
}
unsafe fn namein(fcinfo: *mut c_void) -> Datum { unimplemented!() }

unsafe fn GetForeignDataWrapper(fdwid: Oid) -> *mut ForeignDataWrapper { unimplemented!() }
unsafe fn GetForeignDataWrapperByName(name: *const c_char, missing_ok: bool) -> *mut ForeignDataWrapper {
    unimplemented!()
}
unsafe fn GetForeignServerByName(name: *const c_char, missing_ok: bool) -> *mut ForeignServer {
    unimplemented!()
}
unsafe fn GetFdwRoutine(fdwhandler: Oid) -> *mut FdwRoutine { unimplemented!() }
unsafe fn get_foreign_server_oid(servername: *const c_char, missing_ok: bool) -> Oid {
    unimplemented!()
}

unsafe fn get_rolespec_oid(role: *mut RoleSpec, missing_ok: bool) -> Oid { unimplemented!() }
unsafe fn MappingUserName(useid: Oid) -> *mut c_char { unimplemented!() }

unsafe fn LookupCreationNamespace(nspname: *const c_char) -> Oid { unimplemented!() }
unsafe fn IsImportableForeignTable(tablename: *const c_char, stmt: *mut ImportForeignSchemaStmt) -> bool {
    unimplemented!()
}
unsafe fn pg_parse_query(query_string: *const c_char) -> *mut List { unimplemented!() }
unsafe fn ProcessUtility(
    pstmt: *mut PlannedStmt,
    queryString: *const c_char,
    readOnlyTree: bool,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    unimplemented!()
}
unsafe fn CommandCounterIncrement() { unimplemented!() }

unsafe fn pstrdup(string: *const c_char) -> *mut c_char { unimplemented!() }
unsafe fn palloc(size: usize) -> *mut c_void { unimplemented!() }

use crate::nodes::nodes::nodeTag;

unsafe fn NameStr(name: *const c_char) -> *const c_char { name }
unsafe fn ObjectAddressSet(addr: *mut ObjectAddress, class_id: Oid, object_id: Oid) {
    (*addr).classId = class_id;
    (*addr).objectId = object_id;
    (*addr).objectSubId = 0;
}
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum { unimplemented!() }

/* errcontext-callback helpers (utils/elog.c)  TODO(pg-port) */
unsafe fn geterrposition() -> c_int { unimplemented!() }
unsafe fn errposition(cursorpos: c_int) -> c_int { unimplemented!() }
unsafe fn internalerrposition(cursorpos: c_int) -> c_int { unimplemented!() }
unsafe fn internalerrquery(query: *const c_char) -> c_int { unimplemented!() }

/* VARSIZE / VARDATA helpers (postgres.h)  TODO(pg-port) */
unsafe fn SET_VARSIZE(ptr: *mut text, len: usize) { unimplemented!() }
unsafe fn VARDATA(ptr: *mut text) -> *mut c_char { unimplemented!() }

/* Datum helpers (referenced explicitly)  TODO(pg-port) */
#[inline] unsafe fn PointerIsValid(p: *const c_void) -> bool { !p.is_null() }

/* libc string helpers (string.h)  TODO(pg-port) */
extern "C" {
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
}
#[inline] unsafe fn libc_strchr(s: *const c_char, c: c_int) -> *mut c_char { strchr(s, c) }
#[inline] unsafe fn libc_strlen(s: *const c_char) -> usize { strlen(s) }
#[inline] unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int { strcmp(a, b) }
/* sprintf(VARDATA(t), "%s=%s", name, value) */
#[inline] unsafe fn libc_sprintf_eq(dst: *mut c_char, name: *const c_char, value: *const c_char) {
    sprintf(dst, c"%s=%s".as_ptr(), name, value);
}

/* errhint!/errcontext! single-message shim macros (utils/elog.h) */
macro_rules! errhint { ($($arg:tt)*) => { () }; }
macro_rules! errcontext { ($($arg:tt)*) => { () }; }

#[repr(C)]
struct import_error_callback_arg {
    tablename: *mut c_char,
    cmd: *mut c_char,
}

/*
 * Convert a DefElem list to the text array format that is used in
 * pg_foreign_data_wrapper, pg_foreign_server, pg_user_mapping, and
 * pg_foreign_table.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * Note: The array is usually stored to database without further
 * processing, hence any validation should be done before this
 * conversion.
 */
unsafe fn optionListToArray(options: *mut List) -> Datum {
    let mut astate: *mut ArrayBuildState = null_mut();
    let mut cell: *mut ListCell;

    foreach!(cell, options, {
        let def: *mut DefElem = lfirst(current_cell!(cell)) as *mut DefElem;
        let name: *const c_char;
        let value: *const c_char;
        let len: usize;
        let t: *mut text;

        name = (*def).defname;
        value = defGetString(def);

        /* Insist that name not contain "=", else "a=b=c" is ambiguous */
        if libc_strchr(name, b'=' as c_int) != null_mut() {
            ereport!(ERROR,
                     errmsg!("invalid option name \"{}\": must not contain \"=\"",
                             std::ffi::CStr::from_ptr(name).to_string_lossy()));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        len = VARHDRSZ + libc_strlen(name) + 1 + libc_strlen(value);
        /* +1 leaves room for sprintf's trailing null */
        t = palloc(len + 1) as *mut text;
        SET_VARSIZE(t, len);
        libc_sprintf_eq(VARDATA(t), name, value);

        astate = accumArrayResult(astate, PointerGetDatum(t as *mut c_void),
                                  false, TEXTOID,
                                  CurrentMemoryContext);
    });

    if !astate.is_null() {
        return makeArrayResult(astate, CurrentMemoryContext);
    }

    return PointerGetDatum(null_mut());
}


/*
 * Transform a list of DefElem into text array format.  This is substantially
 * the same thing as optionListToArray(), except we recognize SET/ADD/DROP
 * actions for modifying an existing list of options, which is passed in
 * Datum form as oldOptions.  Also, if fdwvalidator isn't InvalidOid
 * it specifies a validator function to call on the result.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * This is used by CREATE/ALTER of FOREIGN DATA WRAPPER/SERVER/USER MAPPING/
 * FOREIGN TABLE.
 */
pub unsafe fn transformGenericOptions(
    catalogId: Oid,
    oldOptions: Datum,
    options: *mut List,
    fdwvalidator: Oid,
) -> Datum {
    let mut resultOptions: *mut List = untransformRelOptions(oldOptions);
    let mut optcell: *mut ListCell;
    let result: Datum;

    foreach!(optcell, options, {
        let od: *mut DefElem = lfirst(current_cell!(optcell)) as *mut DefElem;
        let mut cell: *mut ListCell;

        /*
         * Find the element in resultOptions.  We need this for validation in
         * all cases.
         */
        foreach!(cell, resultOptions, {
            let def: *mut DefElem = lfirst(current_cell!(cell)) as *mut DefElem;

            if libc_strcmp((*def).defname, (*od).defname) == 0 {
                break;
            }
        });

        /*
         * It is possible to perform multiple SET/DROP actions on the same
         * option.  The standard permits this, as long as the options to be
         * added are unique.  Note that an unspecified action is taken to be
         * ADD.
         */
        match (*od).defaction {
            DefElemAction::DEFELEM_DROP => {
                if cell.is_null() {
                    ereport!(ERROR,
                             errmsg!("option \"{}\" not found",
                                     std::ffi::CStr::from_ptr((*od).defname).to_string_lossy()));
                    /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
                }
                resultOptions = list_delete_cell(resultOptions, cell);
            }

            DefElemAction::DEFELEM_SET => {
                if cell.is_null() {
                    ereport!(ERROR,
                             errmsg!("option \"{}\" not found",
                                     std::ffi::CStr::from_ptr((*od).defname).to_string_lossy()));
                    /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
                }
                (*cell).ptr_value = od as *mut c_void;
            }

            DefElemAction::DEFELEM_ADD | DefElemAction::DEFELEM_UNSPEC => {
                if !cell.is_null() {
                    ereport!(ERROR,
                             errmsg!("option \"{}\" provided more than once",
                                     std::ffi::CStr::from_ptr((*od).defname).to_string_lossy()));
                    /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
                }
                resultOptions = lappend(resultOptions, od as *mut c_void);
            }

            #[allow(unreachable_patterns)]
            _ => {
                elog!(ERROR, "unrecognized action {} on option \"{}\"",
                      (*od).defaction as c_int,
                      std::ffi::CStr::from_ptr((*od).defname).to_string_lossy());
            }
        }
    });

    result = optionListToArray(resultOptions);

    if OidIsValid(fdwvalidator) {
        let mut valarg: Datum = result;

        /*
         * Pass a null options list as an empty array, so that validators
         * don't have to be declared non-strict to handle the case.
         */
        if DatumGetPointer(valarg).is_null() {
            valarg = PointerGetDatum(construct_empty_array(TEXTOID));
        }
        OidFunctionCall2(fdwvalidator, valarg, ObjectIdGetDatum(catalogId));
    }

    return result;
}


/*
 * Internal workhorse for changing a data wrapper's owner.
 *
 * Allow this only for superusers; also the new owner must be a
 * superuser.
 */
unsafe fn AlterForeignDataWrapperOwner_internal(mut rel: Relation, mut tup: HeapTuple, newOwnerId: Oid) {
    let form: Form_pg_foreign_data_wrapper;
    let mut repl_val: [Datum; Natts_pg_foreign_data_wrapper] = [0 as Datum; Natts_pg_foreign_data_wrapper];
    let mut repl_null: [bool; Natts_pg_foreign_data_wrapper] = [false; Natts_pg_foreign_data_wrapper];
    let mut repl_repl: [bool; Natts_pg_foreign_data_wrapper] = [false; Natts_pg_foreign_data_wrapper];
    let newAcl: *mut Acl;
    let aclDatum: Datum;
    let mut isNull: bool = false;

    form = GETSTRUCT(tup) as Form_pg_foreign_data_wrapper;

    /* Must be a superuser to change a FDW owner */
    if !superuser() {
        ereport!(ERROR,
                 errmsg!("permission denied to change owner of foreign-data wrapper \"{}\"",
                         std::ffi::CStr::from_ptr(NameStr(core::ptr::addr_of!((*form).fdwname) as *const c_char)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
         *         errhint("Must be superuser to change owner of a foreign-data wrapper.") */
    }

    /* New owner must also be a superuser */
    if !superuser_arg(newOwnerId) {
        ereport!(ERROR,
                 errmsg!("permission denied to change owner of foreign-data wrapper \"{}\"",
                         std::ffi::CStr::from_ptr(NameStr(core::ptr::addr_of!((*form).fdwname) as *const c_char)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
         *         errhint("The owner of a foreign-data wrapper must be a superuser.") */
    }

    if (*form).fdwowner != newOwnerId {
        repl_null = [false; Natts_pg_foreign_data_wrapper];
        repl_repl = [false; Natts_pg_foreign_data_wrapper];

        repl_repl[(Anum_pg_foreign_data_wrapper_fdwowner - 1) as usize] = true;
        repl_val[(Anum_pg_foreign_data_wrapper_fdwowner - 1) as usize] = ObjectIdGetDatum(newOwnerId);

        aclDatum = heap_getattr(tup,
                                Anum_pg_foreign_data_wrapper_fdwacl,
                                RelationGetDescr(rel),
                                &mut isNull);
        /* Null ACLs do not require changes */
        if !isNull {
            newAcl = aclnewowner(DatumGetAclP(aclDatum),
                                 (*form).fdwowner, newOwnerId);
            repl_repl[(Anum_pg_foreign_data_wrapper_fdwacl - 1) as usize] = true;
            repl_val[(Anum_pg_foreign_data_wrapper_fdwacl - 1) as usize] = PointerGetDatum(newAcl as *mut c_void);
        }

        tup = heap_modify_tuple(tup, RelationGetDescr(rel),
                                repl_val.as_mut_ptr(), repl_null.as_mut_ptr(),
                                repl_repl.as_mut_ptr());

        CatalogTupleUpdate(rel, t_self(tup), tup);

        /* Update owner dependency reference */
        changeDependencyOnOwner(ForeignDataWrapperRelationId,
                                (*form).oid,
                                newOwnerId);
    }

    InvokeObjectPostAlterHook(ForeignDataWrapperRelationId,
                              (*form).oid, 0);
}

/*
 * Change foreign-data wrapper owner -- by name
 *
 * Note restrictions in the "_internal" function, above.
 */
pub unsafe fn AlterForeignDataWrapperOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let fdwId: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let mut address: ObjectAddress = InvalidObjectAddress;
    let form: Form_pg_foreign_data_wrapper;


    rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(name));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 errmsg!("foreign-data wrapper \"{}\" does not exist",
                         std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    form = GETSTRUCT(tup) as Form_pg_foreign_data_wrapper;
    fdwId = (*form).oid;

    AlterForeignDataWrapperOwner_internal(rel, tup, newOwnerId);

    ObjectAddressSet(&mut address, ForeignDataWrapperRelationId, fdwId);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Change foreign-data wrapper owner -- by OID
 *
 * Note restrictions in the "_internal" function, above.
 */
pub unsafe fn AlterForeignDataWrapperOwner_oid(fwdId: Oid, newOwnerId: Oid) {
    let tup: HeapTuple;
    let rel: Relation;

    rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fwdId));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 errmsg!("foreign-data wrapper with OID {} does not exist", fwdId));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    AlterForeignDataWrapperOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Internal workhorse for changing a foreign server's owner
 */
unsafe fn AlterForeignServerOwner_internal(mut rel: Relation, mut tup: HeapTuple, newOwnerId: Oid) {
    let form: Form_pg_foreign_server;
    let mut repl_val: [Datum; Natts_pg_foreign_server] = [0 as Datum; Natts_pg_foreign_server];
    let mut repl_null: [bool; Natts_pg_foreign_server] = [false; Natts_pg_foreign_server];
    let mut repl_repl: [bool; Natts_pg_foreign_server] = [false; Natts_pg_foreign_server];
    let newAcl: *mut Acl;
    let aclDatum: Datum;
    let mut isNull: bool = false;

    form = GETSTRUCT(tup) as Form_pg_foreign_server;

    if (*form).srvowner != newOwnerId {
        /* Superusers can always do it */
        if !superuser() {
            let srvId: Oid;
            let aclresult: AclResult;

            srvId = (*form).oid;

            /* Must be owner */
            if !object_ownercheck(ForeignServerRelationId, srvId, GetUserId()) {
                aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER,
                               NameStr(core::ptr::addr_of!((*form).srvname) as *const c_char));
            }

            /* Must be able to become new owner */
            check_can_set_role(GetUserId(), newOwnerId);

            /* New owner must have USAGE privilege on foreign-data wrapper */
            aclresult = object_aclcheck(ForeignDataWrapperRelationId, (*form).srvfdw, newOwnerId, ACL_USAGE);
            if aclresult != ACLCHECK_OK {
                let fdw: *mut ForeignDataWrapper = GetForeignDataWrapper((*form).srvfdw);

                aclcheck_error(aclresult, OBJECT_FDW, (*fdw).fdwname);
            }
        }

        repl_null = [false; Natts_pg_foreign_server];
        repl_repl = [false; Natts_pg_foreign_server];

        repl_repl[(Anum_pg_foreign_server_srvowner - 1) as usize] = true;
        repl_val[(Anum_pg_foreign_server_srvowner - 1) as usize] = ObjectIdGetDatum(newOwnerId);

        aclDatum = heap_getattr(tup,
                                Anum_pg_foreign_server_srvacl,
                                RelationGetDescr(rel),
                                &mut isNull);
        /* Null ACLs do not require changes */
        if !isNull {
            newAcl = aclnewowner(DatumGetAclP(aclDatum),
                                 (*form).srvowner, newOwnerId);
            repl_repl[(Anum_pg_foreign_server_srvacl - 1) as usize] = true;
            repl_val[(Anum_pg_foreign_server_srvacl - 1) as usize] = PointerGetDatum(newAcl as *mut c_void);
        }

        tup = heap_modify_tuple(tup, RelationGetDescr(rel),
                                repl_val.as_mut_ptr(), repl_null.as_mut_ptr(),
                                repl_repl.as_mut_ptr());

        CatalogTupleUpdate(rel, t_self(tup), tup);

        /* Update owner dependency reference */
        changeDependencyOnOwner(ForeignServerRelationId, (*form).oid,
                                newOwnerId);
    }

    InvokeObjectPostAlterHook(ForeignServerRelationId,
                              (*form).oid, 0);
}

/*
 * Change foreign server owner -- by name
 */
pub unsafe fn AlterForeignServerOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let servOid: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let mut address: ObjectAddress = InvalidObjectAddress;
    let form: Form_pg_foreign_server;

    rel = table_open(ForeignServerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNSERVERNAME, CStringGetDatum(name));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 errmsg!("server \"{}\" does not exist",
                         std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    form = GETSTRUCT(tup) as Form_pg_foreign_server;
    servOid = (*form).oid;

    AlterForeignServerOwner_internal(rel, tup, newOwnerId);

    ObjectAddressSet(&mut address, ForeignServerRelationId, servOid);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Change foreign server owner -- by OID
 */
pub unsafe fn AlterForeignServerOwner_oid(srvId: Oid, newOwnerId: Oid) {
    let tup: HeapTuple;
    let rel: Relation;

    rel = table_open(ForeignServerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNSERVEROID, ObjectIdGetDatum(srvId));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 errmsg!("foreign server with OID {} does not exist", srvId));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    AlterForeignServerOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);
}


/*
 * Convert a handler function name passed from the parser to an Oid.
 */
unsafe fn lookup_fdw_handler_func(handler: *mut DefElem) -> Oid {
    let handlerOid: Oid;

    if handler.is_null() || (*handler).arg.is_null() {
        return InvalidOid;
    }

    /* handlers have no arguments */
    handlerOid = LookupFuncName((*handler).arg as *mut List, 0, null(), false);

    /* check that handler has correct return type */
    if get_func_rettype(handlerOid) != FDW_HANDLEROID {
        ereport!(ERROR,
                 errmsg!("function {} must return type {}",
                         std::ffi::CStr::from_ptr(NameListToString((*handler).arg as *mut List)).to_string_lossy(),
                         "fdw_handler"));
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
    }

    return handlerOid;
}

/*
 * Convert a validator function name passed from the parser to an Oid.
 */
unsafe fn lookup_fdw_validator_func(validator: *mut DefElem) -> Oid {
    let mut funcargtypes: [Oid; 2] = [InvalidOid; 2];

    if validator.is_null() || (*validator).arg.is_null() {
        return InvalidOid;
    }

    /* validators take text[], oid */
    funcargtypes[0] = TEXTARRAYOID;
    funcargtypes[1] = OIDOID;

    return LookupFuncName((*validator).arg as *mut List, 2, funcargtypes.as_ptr(), false);
    /* validator's return value is ignored, so we don't check the type */
}

/*
 * Process function options of CREATE/ALTER FDW
 */
unsafe fn parse_func_options(
    pstate: *mut ParseState,
    func_options: *mut List,
    handler_given: *mut bool,
    fdwhandler: *mut Oid,
    validator_given: *mut bool,
    fdwvalidator: *mut Oid,
) {
    let mut cell: *mut ListCell;

    *handler_given = false;
    *validator_given = false;
    /* return InvalidOid if not given */
    *fdwhandler = InvalidOid;
    *fdwvalidator = InvalidOid;

    foreach!(cell, func_options, {
        let def: *mut DefElem = lfirst(current_cell!(cell)) as *mut DefElem;

        if libc_strcmp((*def).defname, c"handler".as_ptr()) == 0 {
            if *handler_given {
                errorConflictingDefElem(def, pstate);
            }
            *handler_given = true;
            *fdwhandler = lookup_fdw_handler_func(def);
        } else if libc_strcmp((*def).defname, c"validator".as_ptr()) == 0 {
            if *validator_given {
                errorConflictingDefElem(def, pstate);
            }
            *validator_given = true;
            *fdwvalidator = lookup_fdw_validator_func(def);
        } else {
            elog!(ERROR, "option \"{}\" not recognized",
                  std::ffi::CStr::from_ptr((*def).defname).to_string_lossy());
        }
    });
}

/*
 * Create a foreign-data wrapper
 */
pub unsafe fn CreateForeignDataWrapper(pstate: *mut ParseState, stmt: *mut CreateFdwStmt) -> ObjectAddress {
    let rel: Relation;
    let mut values: [Datum; Natts_pg_foreign_data_wrapper] = [0 as Datum; Natts_pg_foreign_data_wrapper];
    let mut nulls: [bool; Natts_pg_foreign_data_wrapper] = [false; Natts_pg_foreign_data_wrapper];
    let tuple: HeapTuple;
    let fdwId: Oid;
    let mut handler_given: bool = false;
    let mut validator_given: bool = false;
    let mut fdwhandler: Oid = InvalidOid;
    let mut fdwvalidator: Oid = InvalidOid;
    let fdwoptions: Datum;
    let ownerId: Oid;
    let mut myself: ObjectAddress = InvalidObjectAddress;
    let mut referenced: ObjectAddress = InvalidObjectAddress;

    rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    /* Must be superuser */
    if !superuser() {
        ereport!(ERROR,
                 errmsg!("permission denied to create foreign-data wrapper \"{}\"",
                         std::ffi::CStr::from_ptr((*stmt).fdwname).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
         *         errhint("Must be superuser to create a foreign-data wrapper.") */
    }

    /* For now the owner cannot be specified on create. Use effective user ID. */
    ownerId = GetUserId();

    /*
     * Check that there is no other foreign-data wrapper by this name.
     */
    if !GetForeignDataWrapperByName((*stmt).fdwname, true).is_null() {
        ereport!(ERROR,
                 errmsg!("foreign-data wrapper \"{}\" already exists",
                         std::ffi::CStr::from_ptr((*stmt).fdwname).to_string_lossy()));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    /*
     * Insert tuple into pg_foreign_data_wrapper.
     */
    values = [0 as Datum; Natts_pg_foreign_data_wrapper];
    nulls = [false; Natts_pg_foreign_data_wrapper];

    fdwId = GetNewOidWithIndex(rel, ForeignDataWrapperOidIndexId,
                               Anum_pg_foreign_data_wrapper_oid);
    values[(Anum_pg_foreign_data_wrapper_oid - 1) as usize] = ObjectIdGetDatum(fdwId);
    values[(Anum_pg_foreign_data_wrapper_fdwname - 1) as usize] =
        DirectFunctionCall1(namein, CStringGetDatum((*stmt).fdwname));
    values[(Anum_pg_foreign_data_wrapper_fdwowner - 1) as usize] = ObjectIdGetDatum(ownerId);

    /* Lookup handler and validator functions, if given */
    parse_func_options(pstate, (*stmt).func_options,
                       &mut handler_given, &mut fdwhandler,
                       &mut validator_given, &mut fdwvalidator);

    values[(Anum_pg_foreign_data_wrapper_fdwhandler - 1) as usize] = ObjectIdGetDatum(fdwhandler);
    values[(Anum_pg_foreign_data_wrapper_fdwvalidator - 1) as usize] = ObjectIdGetDatum(fdwvalidator);

    nulls[(Anum_pg_foreign_data_wrapper_fdwacl - 1) as usize] = true;

    fdwoptions = transformGenericOptions(ForeignDataWrapperRelationId,
                                         PointerGetDatum(null_mut()),
                                         (*stmt).options,
                                         fdwvalidator);

    if PointerIsValid(DatumGetPointer(fdwoptions)) {
        values[(Anum_pg_foreign_data_wrapper_fdwoptions - 1) as usize] = fdwoptions;
    } else {
        nulls[(Anum_pg_foreign_data_wrapper_fdwoptions - 1) as usize] = true;
    }

    tuple = heap_form_tuple(rd_att(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);

    /* record dependencies */
    myself.classId = ForeignDataWrapperRelationId;
    myself.objectId = fdwId;
    myself.objectSubId = 0;

    if OidIsValid(fdwhandler) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = fdwhandler;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if OidIsValid(fdwvalidator) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = fdwvalidator;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    recordDependencyOnOwner(ForeignDataWrapperRelationId, fdwId, ownerId);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new foreign data wrapper */
    InvokeObjectPostCreateHook(ForeignDataWrapperRelationId, fdwId, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}


/*
 * Alter foreign-data wrapper
 */
pub unsafe fn AlterForeignDataWrapper(pstate: *mut ParseState, stmt: *mut AlterFdwStmt) -> ObjectAddress {
    let rel: Relation;
    let mut tp: HeapTuple;
    let fdwForm: Form_pg_foreign_data_wrapper;
    let mut repl_val: [Datum; Natts_pg_foreign_data_wrapper] = [0 as Datum; Natts_pg_foreign_data_wrapper];
    let mut repl_null: [bool; Natts_pg_foreign_data_wrapper] = [false; Natts_pg_foreign_data_wrapper];
    let mut repl_repl: [bool; Natts_pg_foreign_data_wrapper] = [false; Natts_pg_foreign_data_wrapper];
    let fdwId: Oid;
    let mut isnull: bool = false;
    let mut datum: Datum;
    let mut handler_given: bool = false;
    let mut validator_given: bool = false;
    let mut fdwhandler: Oid = InvalidOid;
    let mut fdwvalidator: Oid = InvalidOid;
    let mut myself: ObjectAddress = InvalidObjectAddress;

    rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    /* Must be superuser */
    if !superuser() {
        ereport!(ERROR,
                 errmsg!("permission denied to alter foreign-data wrapper \"{}\"",
                         std::ffi::CStr::from_ptr((*stmt).fdwname).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
         *         errhint("Must be superuser to alter a foreign-data wrapper.") */
    }

    tp = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME,
                             CStringGetDatum((*stmt).fdwname));

    if !HeapTupleIsValid(tp) {
        ereport!(ERROR,
                 errmsg!("foreign-data wrapper \"{}\" does not exist",
                         std::ffi::CStr::from_ptr((*stmt).fdwname).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    fdwForm = GETSTRUCT(tp) as Form_pg_foreign_data_wrapper;
    fdwId = (*fdwForm).oid;

    repl_val = [0 as Datum; Natts_pg_foreign_data_wrapper];
    repl_null = [false; Natts_pg_foreign_data_wrapper];
    repl_repl = [false; Natts_pg_foreign_data_wrapper];

    parse_func_options(pstate, (*stmt).func_options,
                       &mut handler_given, &mut fdwhandler,
                       &mut validator_given, &mut fdwvalidator);

    if handler_given {
        repl_val[(Anum_pg_foreign_data_wrapper_fdwhandler - 1) as usize] = ObjectIdGetDatum(fdwhandler);
        repl_repl[(Anum_pg_foreign_data_wrapper_fdwhandler - 1) as usize] = true;

        /*
         * It could be that the behavior of accessing foreign table changes
         * with the new handler.  Warn about this.
         */
        ereport!(WARNING,
                 errmsg!("changing the foreign-data wrapper handler can change behavior of existing foreign tables"));
    }

    if validator_given {
        repl_val[(Anum_pg_foreign_data_wrapper_fdwvalidator - 1) as usize] = ObjectIdGetDatum(fdwvalidator);
        repl_repl[(Anum_pg_foreign_data_wrapper_fdwvalidator - 1) as usize] = true;

        /*
         * It could be that existing options for the FDW or dependent SERVER,
         * USER MAPPING or FOREIGN TABLE objects are no longer valid according
         * to the new validator.  Warn about this.
         */
        if OidIsValid(fdwvalidator) {
            ereport!(WARNING,
                     errmsg!("changing the foreign-data wrapper validator can cause the options for dependent objects to become invalid"));
        }
    } else {
        /*
         * Validator is not changed, but we need it for validating options.
         */
        fdwvalidator = (*fdwForm).fdwvalidator;
    }

    /*
     * If options specified, validate and update.
     */
    if !(*stmt).options.is_null() {
        /* Extract the current options */
        datum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID,
                                tp,
                                Anum_pg_foreign_data_wrapper_fdwoptions,
                                &mut isnull);
        if isnull {
            datum = PointerGetDatum(null_mut());
        }

        /* Transform the options */
        datum = transformGenericOptions(ForeignDataWrapperRelationId,
                                        datum,
                                        (*stmt).options,
                                        fdwvalidator);

        if PointerIsValid(DatumGetPointer(datum)) {
            repl_val[(Anum_pg_foreign_data_wrapper_fdwoptions - 1) as usize] = datum;
        } else {
            repl_null[(Anum_pg_foreign_data_wrapper_fdwoptions - 1) as usize] = true;
        }

        repl_repl[(Anum_pg_foreign_data_wrapper_fdwoptions - 1) as usize] = true;
    }

    /* Everything looks good - update the tuple */
    tp = heap_modify_tuple(tp, RelationGetDescr(rel),
                           repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());

    CatalogTupleUpdate(rel, t_self(tp), tp);

    heap_freetuple(tp);

    ObjectAddressSet(&mut myself, ForeignDataWrapperRelationId, fdwId);

    /* Update function dependencies if we changed them */
    if handler_given || validator_given {
        let mut referenced: ObjectAddress = InvalidObjectAddress;

        /*
         * Flush all existing dependency records of this FDW on functions; we
         * assume there can be none other than the ones we are fixing.
         */
        deleteDependencyRecordsForClass(ForeignDataWrapperRelationId,
                                        fdwId,
                                        ProcedureRelationId,
                                        DEPENDENCY_NORMAL as c_char);

        /* And build new ones. */

        if OidIsValid(fdwhandler) {
            referenced.classId = ProcedureRelationId;
            referenced.objectId = fdwhandler;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }

        if OidIsValid(fdwvalidator) {
            referenced.classId = ProcedureRelationId;
            referenced.objectId = fdwvalidator;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }
    }

    InvokeObjectPostAlterHook(ForeignDataWrapperRelationId, fdwId, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}


/*
 * Create a foreign server
 */
pub unsafe fn CreateForeignServer(stmt: *mut CreateForeignServerStmt) -> ObjectAddress {
    let rel: Relation;
    let srvoptions: Datum;
    let mut values: [Datum; Natts_pg_foreign_server] = [0 as Datum; Natts_pg_foreign_server];
    let mut nulls: [bool; Natts_pg_foreign_server] = [false; Natts_pg_foreign_server];
    let tuple: HeapTuple;
    let mut srvId: Oid;
    let ownerId: Oid;
    let aclresult: AclResult;
    let mut myself: ObjectAddress = InvalidObjectAddress;
    let mut referenced: ObjectAddress = InvalidObjectAddress;
    let fdw: *mut ForeignDataWrapper;

    rel = table_open(ForeignServerRelationId, RowExclusiveLock);

    /* For now the owner cannot be specified on create. Use effective user ID. */
    ownerId = GetUserId();

    /*
     * Check that there is no other foreign server by this name.  If there is
     * one, do nothing if IF NOT EXISTS was specified.
     */
    srvId = get_foreign_server_oid((*stmt).servername, true);
    if OidIsValid(srvId) {
        if (*stmt).if_not_exists {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            ObjectAddressSet(&mut myself, ForeignServerRelationId, srvId);
            checkMembershipInCurrentExtension(&myself);

            /* OK to skip */
            ereport!(NOTICE,
                     errmsg!("server \"{}\" already exists, skipping",
                             std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            table_close(rel, RowExclusiveLock);
            return InvalidObjectAddress;
        } else {
            ereport!(ERROR,
                     errmsg!("server \"{}\" already exists",
                             std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /*
     * Check that the FDW exists and that we have USAGE on it. Also get the
     * actual FDW for option validation etc.
     */
    fdw = GetForeignDataWrapperByName((*stmt).fdwname, false);

    aclresult = object_aclcheck(ForeignDataWrapperRelationId, (*fdw).fdwid, ownerId, ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FDW, (*fdw).fdwname);
    }

    /*
     * Insert tuple into pg_foreign_server.
     */
    values = [0 as Datum; Natts_pg_foreign_server];
    nulls = [false; Natts_pg_foreign_server];

    srvId = GetNewOidWithIndex(rel, ForeignServerOidIndexId,
                               Anum_pg_foreign_server_oid);
    values[(Anum_pg_foreign_server_oid - 1) as usize] = ObjectIdGetDatum(srvId);
    values[(Anum_pg_foreign_server_srvname - 1) as usize] =
        DirectFunctionCall1(namein, CStringGetDatum((*stmt).servername));
    values[(Anum_pg_foreign_server_srvowner - 1) as usize] = ObjectIdGetDatum(ownerId);
    values[(Anum_pg_foreign_server_srvfdw - 1) as usize] = ObjectIdGetDatum((*fdw).fdwid);

    /* Add server type if supplied */
    if !(*stmt).servertype.is_null() {
        values[(Anum_pg_foreign_server_srvtype - 1) as usize] =
            CStringGetTextDatum((*stmt).servertype);
    } else {
        nulls[(Anum_pg_foreign_server_srvtype - 1) as usize] = true;
    }

    /* Add server version if supplied */
    if !(*stmt).version.is_null() {
        values[(Anum_pg_foreign_server_srvversion - 1) as usize] =
            CStringGetTextDatum((*stmt).version);
    } else {
        nulls[(Anum_pg_foreign_server_srvversion - 1) as usize] = true;
    }

    /* Start with a blank acl */
    nulls[(Anum_pg_foreign_server_srvacl - 1) as usize] = true;

    /* Add server options */
    srvoptions = transformGenericOptions(ForeignServerRelationId,
                                         PointerGetDatum(null_mut()),
                                         (*stmt).options,
                                         (*fdw).fdwvalidator);

    if PointerIsValid(DatumGetPointer(srvoptions)) {
        values[(Anum_pg_foreign_server_srvoptions - 1) as usize] = srvoptions;
    } else {
        nulls[(Anum_pg_foreign_server_srvoptions - 1) as usize] = true;
    }

    tuple = heap_form_tuple(rd_att(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);

    /* record dependencies */
    myself.classId = ForeignServerRelationId;
    myself.objectId = srvId;
    myself.objectSubId = 0;

    referenced.classId = ForeignDataWrapperRelationId;
    referenced.objectId = (*fdw).fdwid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    recordDependencyOnOwner(ForeignServerRelationId, srvId, ownerId);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new foreign server */
    InvokeObjectPostCreateHook(ForeignServerRelationId, srvId, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}


/*
 * Alter foreign server
 */
pub unsafe fn AlterForeignServer(stmt: *mut AlterForeignServerStmt) -> ObjectAddress {
    let rel: Relation;
    let mut tp: HeapTuple;
    let mut repl_val: [Datum; Natts_pg_foreign_server] = [0 as Datum; Natts_pg_foreign_server];
    let mut repl_null: [bool; Natts_pg_foreign_server] = [false; Natts_pg_foreign_server];
    let mut repl_repl: [bool; Natts_pg_foreign_server] = [false; Natts_pg_foreign_server];
    let srvId: Oid;
    let srvForm: Form_pg_foreign_server;
    let mut address: ObjectAddress = InvalidObjectAddress;

    rel = table_open(ForeignServerRelationId, RowExclusiveLock);

    tp = SearchSysCacheCopy1(FOREIGNSERVERNAME,
                             CStringGetDatum((*stmt).servername));

    if !HeapTupleIsValid(tp) {
        ereport!(ERROR,
                 errmsg!("server \"{}\" does not exist",
                         std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    srvForm = GETSTRUCT(tp) as Form_pg_foreign_server;
    srvId = (*srvForm).oid;

    /*
     * Only owner or a superuser can ALTER a SERVER.
     */
    if !object_ownercheck(ForeignServerRelationId, srvId, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER,
                       (*stmt).servername);
    }

    repl_val = [0 as Datum; Natts_pg_foreign_server];
    repl_null = [false; Natts_pg_foreign_server];
    repl_repl = [false; Natts_pg_foreign_server];

    if (*stmt).has_version {
        /*
         * Change the server VERSION string.
         */
        if !(*stmt).version.is_null() {
            repl_val[(Anum_pg_foreign_server_srvversion - 1) as usize] =
                CStringGetTextDatum((*stmt).version);
        } else {
            repl_null[(Anum_pg_foreign_server_srvversion - 1) as usize] = true;
        }

        repl_repl[(Anum_pg_foreign_server_srvversion - 1) as usize] = true;
    }

    if !(*stmt).options.is_null() {
        let fdw: *mut ForeignDataWrapper = GetForeignDataWrapper((*srvForm).srvfdw);
        let mut datum: Datum;
        let mut isnull: bool = false;

        /* Extract the current srvoptions */
        datum = SysCacheGetAttr(FOREIGNSERVEROID,
                                tp,
                                Anum_pg_foreign_server_srvoptions,
                                &mut isnull);
        if isnull {
            datum = PointerGetDatum(null_mut());
        }

        /* Prepare the options array */
        datum = transformGenericOptions(ForeignServerRelationId,
                                        datum,
                                        (*stmt).options,
                                        (*fdw).fdwvalidator);

        if PointerIsValid(DatumGetPointer(datum)) {
            repl_val[(Anum_pg_foreign_server_srvoptions - 1) as usize] = datum;
        } else {
            repl_null[(Anum_pg_foreign_server_srvoptions - 1) as usize] = true;
        }

        repl_repl[(Anum_pg_foreign_server_srvoptions - 1) as usize] = true;
    }

    /* Everything looks good - update the tuple */
    tp = heap_modify_tuple(tp, RelationGetDescr(rel),
                           repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());

    CatalogTupleUpdate(rel, t_self(tp), tp);

    InvokeObjectPostAlterHook(ForeignServerRelationId, srvId, 0);

    ObjectAddressSet(&mut address, ForeignServerRelationId, srvId);

    heap_freetuple(tp);

    table_close(rel, RowExclusiveLock);

    return address;
}


/*
 * Common routine to check permission for user-mapping-related DDL
 * commands.  We allow server owners to operate on any mapping, and
 * users to operate on their own mapping.
 */
unsafe fn user_mapping_ddl_aclcheck(umuserid: Oid, serverid: Oid, servername: *const c_char) {
    let curuserid: Oid = GetUserId();

    if !object_ownercheck(ForeignServerRelationId, serverid, curuserid) {
        if umuserid == curuserid {
            let aclresult: AclResult;

            aclresult = object_aclcheck(ForeignServerRelationId, serverid, curuserid, ACL_USAGE);
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, servername);
            }
        } else {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER,
                           servername);
        }
    }
}


/*
 * Create user mapping
 */
pub unsafe fn CreateUserMapping(stmt: *mut CreateUserMappingStmt) -> ObjectAddress {
    let rel: Relation;
    let useoptions: Datum;
    let mut values: [Datum; Natts_pg_user_mapping] = [0 as Datum; Natts_pg_user_mapping];
    let mut nulls: [bool; Natts_pg_user_mapping] = [false; Natts_pg_user_mapping];
    let tuple: HeapTuple;
    let useId: Oid;
    let mut umId: Oid;
    let mut myself: ObjectAddress = InvalidObjectAddress;
    let mut referenced: ObjectAddress = InvalidObjectAddress;
    let srv: *mut ForeignServer;
    let fdw: *mut ForeignDataWrapper;
    let role: *mut RoleSpec = (*stmt).user;

    rel = table_open(UserMappingRelationId, RowExclusiveLock);

    if (*role).roletype == ROLESPEC_PUBLIC {
        useId = ACL_ID_PUBLIC;
    } else {
        useId = get_rolespec_oid((*stmt).user, false);
    }

    /* Check that the server exists. */
    srv = GetForeignServerByName((*stmt).servername, false);

    user_mapping_ddl_aclcheck(useId, (*srv).serverid, (*stmt).servername);

    /*
     * Check that the user mapping is unique within server.
     */
    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
                           ObjectIdGetDatum(useId),
                           ObjectIdGetDatum((*srv).serverid));

    if OidIsValid(umId) {
        if (*stmt).if_not_exists {
            /*
             * Since user mappings aren't members of extensions (see comments
             * below), no need for checkMembershipInCurrentExtension here.
             */
            ereport!(NOTICE,
                     errmsg!("user mapping for \"{}\" already exists for server \"{}\", skipping",
                             std::ffi::CStr::from_ptr(MappingUserName(useId)).to_string_lossy(),
                             std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */

            table_close(rel, RowExclusiveLock);
            return InvalidObjectAddress;
        } else {
            ereport!(ERROR,
                     errmsg!("user mapping for \"{}\" already exists for server \"{}\"",
                             std::ffi::CStr::from_ptr(MappingUserName(useId)).to_string_lossy(),
                             std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    fdw = GetForeignDataWrapper((*srv).fdwid);

    /*
     * Insert tuple into pg_user_mapping.
     */
    values = [0 as Datum; Natts_pg_user_mapping];
    nulls = [false; Natts_pg_user_mapping];

    umId = GetNewOidWithIndex(rel, UserMappingOidIndexId,
                              Anum_pg_user_mapping_oid);
    values[(Anum_pg_user_mapping_oid - 1) as usize] = ObjectIdGetDatum(umId);
    values[(Anum_pg_user_mapping_umuser - 1) as usize] = ObjectIdGetDatum(useId);
    values[(Anum_pg_user_mapping_umserver - 1) as usize] = ObjectIdGetDatum((*srv).serverid);

    /* Add user options */
    useoptions = transformGenericOptions(UserMappingRelationId,
                                         PointerGetDatum(null_mut()),
                                         (*stmt).options,
                                         (*fdw).fdwvalidator);

    if PointerIsValid(DatumGetPointer(useoptions)) {
        values[(Anum_pg_user_mapping_umoptions - 1) as usize] = useoptions;
    } else {
        nulls[(Anum_pg_user_mapping_umoptions - 1) as usize] = true;
    }

    tuple = heap_form_tuple(rd_att(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);

    /* Add dependency on the server */
    myself.classId = UserMappingRelationId;
    myself.objectId = umId;
    myself.objectSubId = 0;

    referenced.classId = ForeignServerRelationId;
    referenced.objectId = (*srv).serverid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    if OidIsValid(useId) {
        /* Record the mapped user dependency */
        recordDependencyOnOwner(UserMappingRelationId, umId, useId);
    }

    /*
     * Perhaps someday there should be a recordDependencyOnCurrentExtension
     * call here; but since roles aren't members of extensions, it seems like
     * user mappings shouldn't be either.  Note that the grammar and pg_dump
     * would need to be extended too if we change this.
     */

    /* Post creation hook for new user mapping */
    InvokeObjectPostCreateHook(UserMappingRelationId, umId, 0);

    table_close(rel, RowExclusiveLock);

    return myself;
}


/*
 * Alter user mapping
 */
pub unsafe fn AlterUserMapping(stmt: *mut AlterUserMappingStmt) -> ObjectAddress {
    let rel: Relation;
    let mut tp: HeapTuple;
    let mut repl_val: [Datum; Natts_pg_user_mapping] = [0 as Datum; Natts_pg_user_mapping];
    let mut repl_null: [bool; Natts_pg_user_mapping] = [false; Natts_pg_user_mapping];
    let mut repl_repl: [bool; Natts_pg_user_mapping] = [false; Natts_pg_user_mapping];
    let useId: Oid;
    let umId: Oid;
    let srv: *mut ForeignServer;
    let mut address: ObjectAddress = InvalidObjectAddress;
    let role: *mut RoleSpec = (*stmt).user;

    rel = table_open(UserMappingRelationId, RowExclusiveLock);

    if (*role).roletype == ROLESPEC_PUBLIC {
        useId = ACL_ID_PUBLIC;
    } else {
        useId = get_rolespec_oid((*stmt).user, false);
    }

    srv = GetForeignServerByName((*stmt).servername, false);

    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
                           ObjectIdGetDatum(useId),
                           ObjectIdGetDatum((*srv).serverid));
    if !OidIsValid(umId) {
        ereport!(ERROR,
                 errmsg!("user mapping for \"{}\" does not exist for server \"{}\"",
                         std::ffi::CStr::from_ptr(MappingUserName(useId)).to_string_lossy(),
                         std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    user_mapping_ddl_aclcheck(useId, (*srv).serverid, (*stmt).servername);

    tp = SearchSysCacheCopy1(USERMAPPINGOID, ObjectIdGetDatum(umId));

    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for user mapping {}", umId);
    }

    repl_val = [0 as Datum; Natts_pg_user_mapping];
    repl_null = [false; Natts_pg_user_mapping];
    repl_repl = [false; Natts_pg_user_mapping];

    if !(*stmt).options.is_null() {
        let fdw: *mut ForeignDataWrapper;
        let mut datum: Datum;
        let mut isnull: bool = false;

        /*
         * Process the options.
         */

        fdw = GetForeignDataWrapper((*srv).fdwid);

        datum = SysCacheGetAttr(USERMAPPINGUSERSERVER,
                                tp,
                                Anum_pg_user_mapping_umoptions,
                                &mut isnull);
        if isnull {
            datum = PointerGetDatum(null_mut());
        }

        /* Prepare the options array */
        datum = transformGenericOptions(UserMappingRelationId,
                                        datum,
                                        (*stmt).options,
                                        (*fdw).fdwvalidator);

        if PointerIsValid(DatumGetPointer(datum)) {
            repl_val[(Anum_pg_user_mapping_umoptions - 1) as usize] = datum;
        } else {
            repl_null[(Anum_pg_user_mapping_umoptions - 1) as usize] = true;
        }

        repl_repl[(Anum_pg_user_mapping_umoptions - 1) as usize] = true;
    }

    /* Everything looks good - update the tuple */
    tp = heap_modify_tuple(tp, RelationGetDescr(rel),
                           repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());

    CatalogTupleUpdate(rel, t_self(tp), tp);

    InvokeObjectPostAlterHook(UserMappingRelationId,
                              umId, 0);

    ObjectAddressSet(&mut address, UserMappingRelationId, umId);

    heap_freetuple(tp);

    table_close(rel, RowExclusiveLock);

    return address;
}


/*
 * Drop user mapping
 */
pub unsafe fn RemoveUserMapping(stmt: *mut DropUserMappingStmt) -> Oid {
    let mut object: ObjectAddress = InvalidObjectAddress;
    let useId: Oid;
    let umId: Oid;
    let srv: *mut ForeignServer;
    let role: *mut RoleSpec = (*stmt).user;

    if (*role).roletype == ROLESPEC_PUBLIC {
        useId = ACL_ID_PUBLIC;
    } else {
        useId = get_rolespec_oid((*stmt).user, (*stmt).missing_ok);
        if !OidIsValid(useId) {
            /*
             * IF EXISTS specified, role not found and not public. Notice this
             * and leave.
             */
            elog!(NOTICE, "role \"{}\" does not exist, skipping",
                  std::ffi::CStr::from_ptr((*role).rolename).to_string_lossy());
            return InvalidOid;
        }
    }

    srv = GetForeignServerByName((*stmt).servername, true);

    if srv.is_null() {
        if !(*stmt).missing_ok {
            ereport!(ERROR,
                     errmsg!("server \"{}\" does not exist",
                             std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }
        /* IF EXISTS, just note it */
        ereport!(NOTICE,
                 errmsg!("server \"{}\" does not exist, skipping",
                         std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
        return InvalidOid;
    }

    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, Anum_pg_user_mapping_oid,
                           ObjectIdGetDatum(useId),
                           ObjectIdGetDatum((*srv).serverid));

    if !OidIsValid(umId) {
        if !(*stmt).missing_ok {
            ereport!(ERROR,
                     errmsg!("user mapping for \"{}\" does not exist for server \"{}\"",
                             std::ffi::CStr::from_ptr(MappingUserName(useId)).to_string_lossy(),
                             std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }

        /* IF EXISTS specified, just note it */
        ereport!(NOTICE,
                 errmsg!("user mapping for \"{}\" does not exist for server \"{}\", skipping",
                         std::ffi::CStr::from_ptr(MappingUserName(useId)).to_string_lossy(),
                         std::ffi::CStr::from_ptr((*stmt).servername).to_string_lossy()));
        return InvalidOid;
    }

    user_mapping_ddl_aclcheck(useId, (*srv).serverid, (*srv).servername);

    /*
     * Do the deletion
     */
    object.classId = UserMappingRelationId;
    object.objectId = umId;
    object.objectSubId = 0;

    performDeletion(&object, DROP_CASCADE, 0);

    return umId;
}


/*
 * Create a foreign table
 * call after DefineRelation().
 */
pub unsafe fn CreateForeignTable(stmt: *mut CreateForeignTableStmt, relid: Oid) {
    let ftrel: Relation;
    let ftoptions: Datum;
    let mut values: [Datum; Natts_pg_foreign_table] = [0 as Datum; Natts_pg_foreign_table];
    let mut nulls: [bool; Natts_pg_foreign_table] = [false; Natts_pg_foreign_table];
    let tuple: HeapTuple;
    let aclresult: AclResult;
    let mut myself: ObjectAddress = InvalidObjectAddress;
    let mut referenced: ObjectAddress = InvalidObjectAddress;
    let ownerId: Oid;
    let fdw: *mut ForeignDataWrapper;
    let server: *mut ForeignServer;

    /*
     * Advance command counter to ensure the pg_attribute tuple is visible;
     * the tuple might be updated to add constraints in previous step.
     */
    CommandCounterIncrement();

    ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

    /*
     * For now the owner cannot be specified on create. Use effective user ID.
     */
    ownerId = GetUserId();

    /*
     * Check that the foreign server exists and that we have USAGE on it. Also
     * get the actual FDW for option validation etc.
     */
    server = GetForeignServerByName((*stmt).servername, false);
    aclresult = object_aclcheck(ForeignServerRelationId, (*server).serverid, ownerId, ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, (*server).servername);
    }

    fdw = GetForeignDataWrapper((*server).fdwid);

    /*
     * Insert tuple into pg_foreign_table.
     */
    values = [0 as Datum; Natts_pg_foreign_table];
    nulls = [false; Natts_pg_foreign_table];

    values[(Anum_pg_foreign_table_ftrelid - 1) as usize] = ObjectIdGetDatum(relid);
    values[(Anum_pg_foreign_table_ftserver - 1) as usize] = ObjectIdGetDatum((*server).serverid);
    /* Add table generic options */
    ftoptions = transformGenericOptions(ForeignTableRelationId,
                                        PointerGetDatum(null_mut()),
                                        (*stmt).options,
                                        (*fdw).fdwvalidator);

    if PointerIsValid(DatumGetPointer(ftoptions)) {
        values[(Anum_pg_foreign_table_ftoptions - 1) as usize] = ftoptions;
    } else {
        nulls[(Anum_pg_foreign_table_ftoptions - 1) as usize] = true;
    }

    tuple = heap_form_tuple(rd_att(ftrel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(ftrel, tuple);

    heap_freetuple(tuple);

    /* Add pg_class dependency on the server */
    myself.classId = RelationRelationId;
    myself.objectId = relid;
    myself.objectSubId = 0;

    referenced.classId = ForeignServerRelationId;
    referenced.objectId = (*server).serverid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    table_close(ftrel, RowExclusiveLock);
}

/*
 * Import a foreign schema
 */
pub unsafe fn ImportForeignSchema(stmt: *mut ImportForeignSchemaStmt) {
    let server: *mut ForeignServer;
    let fdw: *mut ForeignDataWrapper;
    let fdw_routine: *mut FdwRoutine;
    let aclresult: AclResult;
    let cmd_list: *mut List;
    let mut lc: *mut ListCell;

    /* Check that the foreign server exists and that we have USAGE on it */
    server = GetForeignServerByName((*stmt).server_name, false);
    aclresult = object_aclcheck(ForeignServerRelationId, (*server).serverid, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, (*server).servername);
    }

    /* Check that the schema exists and we have CREATE permissions on it */
    LookupCreationNamespace((*stmt).local_schema);

    /* Get the FDW and check it supports IMPORT */
    fdw = GetForeignDataWrapper((*server).fdwid);
    if !OidIsValid((*fdw).fdwhandler) {
        ereport!(ERROR,
                 errmsg!("foreign-data wrapper \"{}\" has no handler",
                         std::ffi::CStr::from_ptr((*fdw).fdwname).to_string_lossy()));
        /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
    }
    fdw_routine = GetFdwRoutine((*fdw).fdwhandler);
    if (*fdw_routine).ImportForeignSchema.is_none() {
        ereport!(ERROR,
                 errmsg!("foreign-data wrapper \"{}\" does not support IMPORT FOREIGN SCHEMA",
                         std::ffi::CStr::from_ptr((*fdw).fdwname).to_string_lossy()));
        /* C also: errcode(ERRCODE_FDW_NO_SCHEMAS) */
    }

    /* Call FDW to get a list of commands */
    cmd_list = ((*fdw_routine).ImportForeignSchema.unwrap())(stmt, (*server).serverid);

    /* Parse and execute each command */
    foreach!(lc, cmd_list, {
        let cmd: *mut c_char = lfirst(current_cell!(lc)) as *mut c_char;
        let mut callback_arg: import_error_callback_arg = import_error_callback_arg {
            tablename: null_mut(),
            cmd: null_mut(),
        };
        let mut sqlerrcontext: ErrorContextCallback = ErrorContextCallback {
            previous: null_mut(),
            callback: None,
            arg: null_mut(),
        };
        let raw_parsetree_list: *mut List;
        let mut lc2: *mut ListCell;

        /*
         * Setup error traceback support for ereport().  This is so that any
         * error in the generated SQL will be displayed nicely.
         */
        callback_arg.tablename = null_mut();	/* not known yet */
        callback_arg.cmd = cmd;
        sqlerrcontext.callback = Some(import_error_callback);
        sqlerrcontext.arg = core::ptr::addr_of_mut!(callback_arg) as *mut c_void;
        sqlerrcontext.previous = error_context_stack;
        error_context_stack = core::ptr::addr_of_mut!(sqlerrcontext);

        /*
         * Parse the SQL string into a list of raw parse trees.
         */
        raw_parsetree_list = pg_parse_query(cmd);

        /*
         * Process each parse tree (we allow the FDW to put more than one
         * command per string, though this isn't really advised).
         */
        foreach!(lc2, raw_parsetree_list, {
            let rs: *mut RawStmt = lfirst_node!(RawStmt, T_RawStmt, current_cell!(lc2));
            let cstmt: *mut CreateForeignTableStmt = (*rs).stmt as *mut CreateForeignTableStmt;
            let pstmt: *mut PlannedStmt;

            /*
             * Because we only allow CreateForeignTableStmt, we can skip parse
             * analysis, rewrite, and planning steps here.
             */
            if !IsA!(cstmt, T_CreateForeignTableStmt) {
                elog!(ERROR,
                      "foreign-data wrapper \"{}\" returned incorrect statement type {}",
                      std::ffi::CStr::from_ptr((*fdw).fdwname).to_string_lossy(),
                      nodeTag(cstmt) as c_int);
            }

            /* Ignore commands for tables excluded by filter options */
            if !IsImportableForeignTable((*(*cstmt).base.relation).relname, stmt) {
                continue;
            }

            /* Enable reporting of current table's name on error */
            callback_arg.tablename = (*(*cstmt).base.relation).relname;

            /* Ensure creation schema is the one given in IMPORT statement */
            (*(*cstmt).base.relation).schemaname = pstrdup((*stmt).local_schema);

            /* No planning needed, just make a wrapper PlannedStmt */
            pstmt = makeNode!(PlannedStmt, T_PlannedStmt);
            (*pstmt).commandType = CMD_UTILITY;
            (*pstmt).canSetTag = false;
            (*pstmt).utilityStmt = cstmt as *mut Node;
            (*pstmt).stmt_location = (*rs).stmt_location;
            (*pstmt).stmt_len = (*rs).stmt_len;

            /* Execute statement */
            ProcessUtility(pstmt, cmd, false,
                           PROCESS_UTILITY_SUBCOMMAND, null_mut(), null_mut(),
                           None_Receiver, null_mut());

            /* Be sure to advance the command counter between subcommands */
            CommandCounterIncrement();

            callback_arg.tablename = null_mut();
        });

        error_context_stack = sqlerrcontext.previous;
    });
}

/*
 * error context callback to let us supply the failing SQL statement's text
 */
unsafe extern "C" fn import_error_callback(arg: *mut c_void) {
    let callback_arg: *mut import_error_callback_arg = arg as *mut import_error_callback_arg;
    let syntaxerrposition: c_int;

    /* If it's a syntax error, convert to internal syntax error report */
    syntaxerrposition = geterrposition();
    if syntaxerrposition > 0 {
        errposition(0);
        internalerrposition(syntaxerrposition);
        internalerrquery((*callback_arg).cmd);
    }

    if !(*callback_arg).tablename.is_null() {
        errcontext!("importing foreign table \"{}\"",
                    std::ffi::CStr::from_ptr((*callback_arg).tablename).to_string_lossy());
    }
}

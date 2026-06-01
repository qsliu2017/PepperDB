/*-------------------------------------------------------------------------
 *
 * user.rs
 *   Commands for manipulating roles (formerly called users).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/commands/user.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};
use std::ptr;

use crate::access::htup_details::{HeapTupleData, HeapTuple};
use crate::catalog::objectaccess::ObjectAddress;
use crate::nodes::pg_list::{List, ListCell};
use crate::nodes::parsenodes::{
    AlterRoleSetStmt, AlterRoleStmt, CreateRoleStmt, DefElem, DropBehavior,
    DropRoleStmt, GrantRoleStmt, ReassignOwnedStmt, DropOwnedStmt, RoleSpec,
    AccessPriv, RoleStmtType,
};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::{foreach, current_cell};

/* --------------------------------------------------------------------------
 * Local type stubs for unported dependencies
 * -------------------------------------------------------------------------- */

// Relation pointer  TODO(pg-port)
#[repr(C)] pub struct RelationData { _opaque: [u8; 0] }
type Relation = *mut RelationData;

// TupleDesc  TODO(pg-port)
#[repr(C)] pub struct TupleDescData { _opaque: [u8; 0] }
type TupleDesc = *mut TupleDescData;

// SysScanDesc / ScanKeyData  TODO(pg-port)
#[repr(C)] pub struct SysScanDescData { _opaque: [u8; 0] }
type SysScanDesc = *mut SysScanDescData;
#[repr(C)] pub struct ScanKeyDataStruct { _opaque: [u8; 64] }
type ScanKeyData = ScanKeyDataStruct;

// CatCList / CatCTup  TODO(pg-port)
#[repr(C)] pub struct CatCListData { _opaque: [u8; 0] }
type CatCList = *mut CatCListData;

// Datum  TODO(pg-port)
type Datum = usize;

// Form_pg_authid  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_authid { _opaque: [u8; 0] }
type Form_pg_authid = *mut FormData_pg_authid;

// Concrete pg_authid row layout for GETSTRUCT field access  TODO(pg-port)
const NAMEDATALEN: usize = 64;
#[repr(C)] pub struct FormData_pg_authid_real {
    pub oid: Oid,
    pub rolname: [c_char; NAMEDATALEN],
    pub rolsuper: bool,
    pub rolinherit: bool,
    pub rolcreaterole: bool,
    pub rolcreatedb: bool,
    pub rolcanlogin: bool,
    pub rolreplication: bool,
    pub rolbypassrls: bool,
}

// Form_pg_auth_members  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_auth_members { _opaque: [u8; 0] }
type Form_pg_auth_members = *mut FormData_pg_auth_members;

// Concrete pg_auth_members row layout for GETSTRUCT field access  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_auth_members_real {
    pub oid: Oid,
    pub roleid: Oid,
    pub member: Oid,
    pub grantor: Oid,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

// GucSource  TODO(pg-port)
#[repr(C)] pub enum GucSource { GUC_DEFAULT = 0, GUC_FILE, GUC_ENVIRON, GUC_CMDLINE, GUC_INTERACTIVE }

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const NoLock: LOCKMODE = 0;
const AccessShareLock: LOCKMODE = 1;
const RowExclusiveLock: LOCKMODE = 3;
const ShareUpdateExclusiveLock: LOCKMODE = 4;
const AccessExclusiveLock: LOCKMODE = 8;

// ItemPointerData  TODO(pg-port)
#[repr(C)] #[derive(Clone, Copy, Default)] pub struct ItemPointerData { ip_blkid: [u8; 2], ip_posid: u16 }

/* --------------------------------------------------------------------------
 * Extern stubs for catalog constants and functions  TODO(pg-port)
 * -------------------------------------------------------------------------- */
extern "C" {
    static AuthIdRelationId: Oid;
    static AuthMemRelationId: Oid;
    static DatabaseRelationId: Oid;
    static AuthIdOidIndexId: Oid;
    static AuthMemOidIndexId: Oid;
    static AuthMemRoleMemIndexId: Oid;
    static AuthMemMemRoleIndexId: Oid;
    static InvalidOid: Oid;
    static IsBinaryUpgrade: bool;
    // pg_authid attribute numbers
    static Natts_pg_authid: c_int;
    static Anum_pg_authid_oid: c_int;
    static Anum_pg_authid_rolname: c_int;
    static Anum_pg_authid_rolsuper: c_int;
    static Anum_pg_authid_rolinherit: c_int;
    static Anum_pg_authid_rolcreaterole: c_int;
    static Anum_pg_authid_rolcreatedb: c_int;
    static Anum_pg_authid_rolcanlogin: c_int;
    static Anum_pg_authid_rolreplication: c_int;
    static Anum_pg_authid_rolconnlimit: c_int;
    static Anum_pg_authid_rolpassword: c_int;
    static Anum_pg_authid_rolvaliduntil: c_int;
    static Anum_pg_authid_rolbypassrls: c_int;
    // pg_auth_members attribute numbers
    static Natts_pg_auth_members: c_int;
    static Anum_pg_auth_members_oid: c_int;
    static Anum_pg_auth_members_roleid: c_int;
    static Anum_pg_auth_members_member: c_int;
    static Anum_pg_auth_members_grantor: c_int;
    static Anum_pg_auth_members_admin_option: c_int;
    static Anum_pg_auth_members_inherit_option: c_int;
    static Anum_pg_auth_members_set_option: c_int;
    // syscache enum values
    static AUTHNAME: c_int;
    static AUTHOID: c_int;
    static AUTHMEMROLEMEM: c_int;
    // special role OIDs
    static BOOTSTRAP_SUPERUSERID: Oid;
    static ROLE_PG_DATABASE_OWNER: Oid;
    // PASSWORD_TYPE constants
    static PASSWORD_TYPE_SCRAM_SHA_256: c_int;
    static PASSWORD_TYPE_MD5: c_int;
    // lock modes (symbolic)
    static BTEqualStrategyNumber: c_int;
    static F_OIDEQ: Oid;
    // DROP_RESTRICT / DROP_CASCADE  (DropBehavior integers)
    static DROP_RESTRICT: c_int;
    static DROP_CASCADE: c_int;
}

/* --------------------------------------------------------------------------
 * Extern function stubs  TODO(pg-port)
 * -------------------------------------------------------------------------- */
extern "C" {
    fn table_open(relid: Oid, lockmode: LOCKMODE) -> Relation;
    fn table_close(rel: Relation, lockmode: LOCKMODE);
    fn RelationGetDescr(rel: Relation) -> TupleDesc;
    fn heap_form_tuple(tupdesc: TupleDesc, values: *mut Datum, nulls: *mut bool) -> HeapTuple;
    fn heap_modify_tuple(
        tuple: HeapTuple, tupdesc: TupleDesc,
        repl_values: *mut Datum, repl_nulls: *mut bool, repl_columns: *mut bool,
    ) -> HeapTuple;
    fn heap_freetuple(htup: HeapTuple);
    fn heap_getattr(tup: HeapTuple, attnum: c_int, tupdesc: TupleDesc, isnull: *mut bool) -> Datum;
    fn GETSTRUCT(tup: HeapTuple) -> *mut c_void;
    fn HeapTupleIsValid(tup: HeapTuple) -> bool;
    fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) -> ItemPointerData;
    fn CatalogTupleUpdate(heapRel: Relation, otid: *mut ItemPointerData, tup: HeapTuple);
    fn CatalogTupleDelete(heapRel: Relation, tid: *mut ItemPointerData);
    fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCache3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> HeapTuple;
    fn SearchSysCacheExists1(cacheId: c_int, key1: Datum) -> bool;
    fn SearchSysCacheList1(cacheId: c_int, key1: Datum) -> CatCList;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn ReleaseSysCacheList(list: CatCList);
    fn SysCacheGetAttr(cacheId: c_int, tup: HeapTuple, attnum: c_int, isnull: *mut bool) -> Datum;
    fn systable_beginscan(
        heapRelation: Relation, indexId: Oid, indexOK: bool,
        snapshot: *mut c_void, nkeys: c_int, key: *mut ScanKeyData,
    ) -> SysScanDesc;
    fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple;
    fn systable_endscan(sysscan: SysScanDesc);
    fn ScanKeyInit(entry: *mut ScanKeyData, attnum: c_int, strategy: c_int, proc_: Oid, argument: Datum);
    fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: c_int) -> Oid;
    fn CStringGetDatum(s: *const c_char) -> Datum;
    fn CStringGetTextDatum(s: *const c_char) -> Datum;
    fn TextDatumGetCString(d: Datum) -> *mut c_char;
    fn ObjectIdGetDatum(o: Oid) -> Datum;
    fn BoolGetDatum(b: bool) -> Datum;
    fn Int32GetDatum(i: i32) -> Datum;
    fn PointerGetDatum(p: *const c_void) -> Datum;
    fn DatumGetObjectId(d: Datum) -> Oid;
    fn DirectFunctionCall1(func: *const c_void, arg1: Datum) -> Datum;
    fn DirectFunctionCall3(func: *const c_void, arg1: Datum, arg2: Datum, arg3: Datum) -> Datum;
    fn namein(fcinfo: *mut c_void) -> Datum;
    fn timestamptz_in(fcinfo: *mut c_void) -> Datum;
    fn NameStr(name: *const c_void) -> *const c_char;
    fn pstrdup(s: *const c_char) -> *mut c_char;
    fn palloc(size: usize) -> *mut c_void;
    fn pfree(ptr: *mut c_void);
    fn GetUserId() -> Oid;
    fn GetOuterUserId() -> Oid;
    fn GetSessionUserId() -> Oid;
    fn superuser() -> bool;
    fn superuser_arg(rolen: Oid) -> bool;
    fn has_createrole_privilege(roleid: Oid) -> bool;
    fn have_createdb_privilege() -> bool;
    fn has_rolreplication(roleid: Oid) -> bool;
    fn has_bypassrls_privilege(roleid: Oid) -> bool;
    fn is_admin_of_role(member: Oid, role: Oid) -> bool;
    fn is_member_of_role_nosuper(member: Oid, role: Oid) -> bool;
    fn has_privs_of_role(member: Oid, role: Oid) -> bool;
    fn select_best_admin(member: Oid, role: Oid) -> Oid;
    fn get_role_oid(rolname: *const c_char, missing_ok: bool) -> Oid;
    fn get_rolespec_oid(role: *const RoleSpec, missing_ok: bool) -> Oid;
    fn get_rolespec_name(role: *const RoleSpec) -> *const c_char;
    fn get_rolespec_tuple(role: *const RoleSpec) -> HeapTuple;
    fn check_rolespec_name(role: *const RoleSpec, detail: *const c_char);
    fn GetUserNameFromId(roleid: Oid, noerr: bool) -> *mut c_char;
    fn get_database_oid(dbname: *const c_char, missing_ok: bool) -> Oid;
    fn get_password_type(shadow_pass: *const c_char) -> c_int;
    fn encrypt_password(target_type: c_int, rolname: *const c_char, password: *const c_char) -> *mut c_char;
    fn plain_crypt_verify(role: *const c_char, shadow_pass: *const c_char, client_pass: *const c_char, logdetail: *mut *const c_char) -> c_int;
    fn IsReservedName(name: *const c_char) -> bool;
    fn OidIsValid(oid: Oid) -> bool;
    fn CommandCounterIncrement();
    fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectDropHook(classId: Oid, objectId: Oid, subId: c_int);
    fn ObjectAddressSet(address: *mut ObjectAddress, classId: Oid, objectId: Oid);
    fn LockSharedObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE);
    fn shdepLockAndCheckObject(classid: Oid, objid: Oid);
    fn checkSharedDependencies(classId: Oid, objectId: Oid, detail: *mut *mut c_char, detail_log: *mut *mut c_char) -> bool;
    fn deleteSharedDependencyRecordsFor(classId: Oid, objectId: Oid, objectSubId: c_int);
    fn updateAclDependencies(classId: Oid, objectId: Oid, objectSubId: c_int, ownerId: Oid, noldmembers: c_int, oldmembers: *mut Oid, nnewmembers: c_int, newmembers: *mut Oid);
    fn DeleteSharedComments(objectId: Oid, classId: Oid);
    fn DeleteSharedSecurityLabel(objectId: Oid, classId: Oid);
    fn AlterSetting(databaseid: Oid, roleid: Oid, setstmt: *mut c_void);
    fn DropSetting(databaseid: Oid, roleid: Oid);
    fn shdepDropOwned(roleids: *mut List, behavior: DropBehavior);
    fn shdepReassignOwned(roleids: *mut List, newrole: Oid);
    fn aclcheck_error(acl_error: c_int, objtype: c_int, objectname: *const c_char);
    fn object_ownercheck(classid: Oid, objectid: Oid, userid: Oid) -> bool;
    fn list_make1(x1: *mut c_void) -> *mut List;
    fn list_make1_oid(x1: Oid) -> *mut List;
    fn lappend_oid(list: *mut List, datum: Oid) -> *mut List;
    fn list_append_unique_oid(list: *mut List, datum: Oid) -> *mut List;
    fn list_length(list: *const List) -> c_int;
    fn list_free(list: *mut List);
    fn lfirst_oid(lc: *mut ListCell) -> Oid;
    fn lfirst(lc: *mut ListCell) -> *mut c_void;
    fn strVal(val: *mut c_void) -> *mut c_char;
    fn boolVal(val: *mut c_void) -> bool;
    fn intVal(val: *mut c_void) -> i32;
    fn defGetString(def: *mut DefElem) -> *mut c_char;
    fn parse_bool(s: *const c_char, result: *mut bool) -> bool;
    fn SplitIdentifierString(rawstring: *mut c_char, separator: c_char, namelist: *mut *mut List) -> bool;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn pg_popcount32(x: u32) -> c_int;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn guc_malloc(elevel: c_int, size: usize) -> *mut c_void;
    fn parser_errposition(pstate: *mut ParseState, location: c_int) -> c_int;
    fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState);
    fn ereport_impl(elevel: c_int, ...) -> !;
    fn errcode(sqlerrcode: c_int) -> c_int;
    fn errmsg(fmt: *const c_char, ...) -> c_int;
    fn errdetail(fmt: *const c_char, ...) -> c_int;
    fn errdetail_internal(fmt: *const c_char, ...) -> c_int;
    fn errdetail_log(fmt: *const c_char, ...) -> c_int;
    fn errhint(fmt: *const c_char, ...) -> c_int;
    fn GUC_check_errdetail(fmt: *const c_char, ...) -> c_int;
    fn NIL() -> *mut List;
    fn pg_authid_members_n_members(list: CatCList) -> c_int;
    fn catclist_member_tuple(list: CatCList, i: c_int) -> HeapTuple;
}

// STATUS_OK  TODO(pg-port)
const STATUS_OK: c_int = 0;
// LOG
const LOG: c_int = 15;
// ERRCODE_* values (abbreviated)  TODO(pg-port)
const ERRCODE_RESERVED_NAME: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_OBJECT_IN_USE: c_int = 0;
const ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST: c_int = 0;
const ERRCODE_INVALID_GRANT_OPERATION: c_int = 0;
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ACLCHECK_NOT_OWNER: c_int = 1;
const OBJECT_DATABASE: c_int = 0;
const NOTICE: c_int = 18;
const WARNING: c_int = 19;
const ERROR: c_int = 20;

// ROLESPEC_CSTRING / ROLESPEC_CURRENT_ROLE  TODO(pg-port)
const ROLESPEC_CSTRING: c_int = 0;
const ROLESPEC_CURRENT_ROLE: c_int = 3;

/*
 * Removing a role grant - or the admin option on it - might recurse to
 * dependent grants. We use these values to reason about what would need to
 * be done in such cases.
 *
 * RRG_NOOP indicates a grant that would not need to be altered by the
 * operation.
 *
 * RRG_REMOVE_ADMIN_OPTION indicates a grant that would need to have
 * admin_option set to false by the operation.
 *
 * Similarly, RRG_REMOVE_INHERIT_OPTION and RRG_REMOVE_SET_OPTION indicate
 * grants that would need to have the corresponding options set to false.
 *
 * RRG_DELETE_GRANT indicates a grant that would need to be removed entirely
 * by the operation.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub enum RevokeRoleGrantAction {
    RRG_NOOP,
    RRG_REMOVE_ADMIN_OPTION,
    RRG_REMOVE_INHERIT_OPTION,
    RRG_REMOVE_SET_OPTION,
    RRG_DELETE_GRANT,
}
use RevokeRoleGrantAction::*;

/* Potentially set by pg_upgrade_support functions */
#[no_mangle]
pub static mut binary_upgrade_next_pg_authid_oid: Oid = 0; /* InvalidOid */

#[repr(C)]
pub struct GrantRoleOptions {
    pub specified: u32,
    pub admin: bool,
    pub inherit: bool,
    pub set: bool,
}

const GRANT_ROLE_SPECIFIED_ADMIN: u32 = 0x0001;
const GRANT_ROLE_SPECIFIED_INHERIT: u32 = 0x0002;
const GRANT_ROLE_SPECIFIED_SET: u32 = 0x0004;

/* GUC parameters */
#[no_mangle]
pub static mut Password_encryption: c_int = 0; /* PASSWORD_TYPE_SCRAM_SHA_256 */
#[no_mangle]
pub static mut createrole_self_grant: *mut c_char = ptr::null_mut();
static mut createrole_self_grant_enabled: bool = false;
static mut createrole_self_grant_options: GrantRoleOptions = GrantRoleOptions {
    specified: 0,
    admin: false,
    inherit: false,
    set: false,
};

/* Hook to check passwords in CreateRole() and AlterRole() */
pub type check_password_hook_type = Option<
    unsafe extern "C" fn(
        username: *const c_char,
        shadow_pass: *const c_char,
        password_type: c_int,
        validuntil_time: Datum,
        validuntil_null: bool,
    ),
>;
#[no_mangle]
pub static mut check_password_hook: check_password_hook_type = None;

/* Check if current user has createrole privileges */
unsafe fn have_createrole_privilege() -> bool {
    has_createrole_privilege(GetUserId())
}


/*
 * CREATE ROLE
 */
#[no_mangle]
pub unsafe extern "C" fn CreateRole(pstate: *mut ParseState, stmt: *mut CreateRoleStmt) -> Oid {
    let mut pg_authid_rel: Relation;
    let mut pg_authid_dsc: TupleDesc;
    let mut tuple: HeapTuple;
    // Datum new_record[Natts_pg_authid] = {0};
    // bool  new_record_nulls[Natts_pg_authid] = {0};
    // Use fixed-size arrays large enough; actual Natts_pg_authid is known at C link time.
    let mut new_record: [Datum; 32] = [0usize; 32];
    let mut new_record_nulls: [bool; 32] = [false; 32];
    let mut currentUserId: Oid = GetUserId();
    let mut roleid: Oid;
    let mut password: *mut c_char = ptr::null_mut(); /* user password */
    let mut issuper: bool = false;      /* Make the user a superuser? */
    let mut inherit: bool = true;       /* Auto inherit privileges? */
    let mut createrole: bool = false;   /* Can this user create roles? */
    let mut createdb: bool = false;     /* Can the user create databases? */
    let mut canlogin: bool = false;     /* Can this user login? */
    let mut isreplication: bool = false; /* Is this a replication role? */
    let mut bypassrls: bool = false;    /* Is this a row security enabled role? */
    let mut connlimit: i32 = -1;        /* maximum connections allowed */
    let mut addroleto: *mut List = ptr::null_mut();   /* roles to make this a member of */
    let mut rolemembers: *mut List = ptr::null_mut(); /* roles to be members of this role */
    let mut adminmembers: *mut List = ptr::null_mut(); /* roles to be admins of this role */
    let mut validUntil: *mut c_char = ptr::null_mut(); /* time the login is valid until */
    let mut validUntil_datum: Datum = 0;
    let mut validUntil_null: bool = false;
    let mut dpassword: *mut DefElem = ptr::null_mut();
    let mut dissuper: *mut DefElem = ptr::null_mut();
    let mut dinherit: *mut DefElem = ptr::null_mut();
    let mut dcreaterole: *mut DefElem = ptr::null_mut();
    let mut dcreatedb: *mut DefElem = ptr::null_mut();
    let mut dcanlogin: *mut DefElem = ptr::null_mut();
    let mut disreplication: *mut DefElem = ptr::null_mut();
    let mut dconnlimit: *mut DefElem = ptr::null_mut();
    let mut daddroleto: *mut DefElem = ptr::null_mut();
    let mut drolemembers: *mut DefElem = ptr::null_mut();
    let mut dadminmembers: *mut DefElem = ptr::null_mut();
    let mut dvalidUntil: *mut DefElem = ptr::null_mut();
    let mut dbypassRLS: *mut DefElem = ptr::null_mut();
    let mut popt: GrantRoleOptions = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: false };

    /* The defaults can vary depending on the original statement type */
    match (*stmt).stmt_type {
        RoleStmtType::ROLESTMT_ROLE => {}
        RoleStmtType::ROLESTMT_USER => {
            canlogin = true;
            /* may eventually want inherit to default to false here */
        }
        RoleStmtType::ROLESTMT_GROUP => {}
    }

    /* Extract options from the statement node tree */
    foreach!(option, (*stmt).options, {
        let defel: *mut DefElem = crate::current_cell!(option) as *mut DefElem;

        if strcmp((*defel).defname, c"password".as_ptr()) == 0 {
            if !dpassword.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dpassword = defel;
        } else if strcmp((*defel).defname, c"sysid".as_ptr()) == 0 {
            ereport!(NOTICE, errmsg!("SYSID can no longer be specified"));
        } else if strcmp((*defel).defname, c"superuser".as_ptr()) == 0 {
            if !dissuper.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dissuper = defel;
        } else if strcmp((*defel).defname, c"inherit".as_ptr()) == 0 {
            if !dinherit.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dinherit = defel;
        } else if strcmp((*defel).defname, c"createrole".as_ptr()) == 0 {
            if !dcreaterole.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dcreaterole = defel;
        } else if strcmp((*defel).defname, c"createdb".as_ptr()) == 0 {
            if !dcreatedb.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dcreatedb = defel;
        } else if strcmp((*defel).defname, c"canlogin".as_ptr()) == 0 {
            if !dcanlogin.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dcanlogin = defel;
        } else if strcmp((*defel).defname, c"isreplication".as_ptr()) == 0 {
            if !disreplication.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            disreplication = defel;
        } else if strcmp((*defel).defname, c"connectionlimit".as_ptr()) == 0 {
            if !dconnlimit.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dconnlimit = defel;
        } else if strcmp((*defel).defname, c"addroleto".as_ptr()) == 0 {
            if !daddroleto.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            daddroleto = defel;
        } else if strcmp((*defel).defname, c"rolemembers".as_ptr()) == 0 {
            if !drolemembers.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            drolemembers = defel;
        } else if strcmp((*defel).defname, c"adminmembers".as_ptr()) == 0 {
            if !dadminmembers.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dadminmembers = defel;
        } else if strcmp((*defel).defname, c"validUntil".as_ptr()) == 0 {
            if !dvalidUntil.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dvalidUntil = defel;
        } else if strcmp((*defel).defname, c"bypassrls".as_ptr()) == 0 {
            if !dbypassRLS.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dbypassRLS = defel;
        } else {
            elog!(ERROR, "option \"{}\" not recognized", unsafe { core::ffi::CStr::from_ptr((*defel).defname) }.to_string_lossy());
        }
    });

    if !dpassword.is_null() && !(*dpassword).arg.is_null() {
        password = strVal((*dpassword).arg as *mut c_void);
    }
    if !dissuper.is_null() {
        issuper = boolVal((*dissuper).arg as *mut c_void);
    }
    if !dinherit.is_null() {
        inherit = boolVal((*dinherit).arg as *mut c_void);
    }
    if !dcreaterole.is_null() {
        createrole = boolVal((*dcreaterole).arg as *mut c_void);
    }
    if !dcreatedb.is_null() {
        createdb = boolVal((*dcreatedb).arg as *mut c_void);
    }
    if !dcanlogin.is_null() {
        canlogin = boolVal((*dcanlogin).arg as *mut c_void);
    }
    if !disreplication.is_null() {
        isreplication = boolVal((*disreplication).arg as *mut c_void);
    }
    if !dconnlimit.is_null() {
        connlimit = intVal((*dconnlimit).arg as *mut c_void);
        if connlimit < -1 {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("invalid connection limit: {}", connlimit));
        }
    }
    if !daddroleto.is_null() {
        addroleto = (*daddroleto).arg as *mut List;
    }
    if !drolemembers.is_null() {
        rolemembers = (*drolemembers).arg as *mut List;
    }
    if !dadminmembers.is_null() {
        adminmembers = (*dadminmembers).arg as *mut List;
    }
    if !dvalidUntil.is_null() {
        validUntil = strVal((*dvalidUntil).arg as *mut c_void);
    }
    if !dbypassRLS.is_null() {
        bypassrls = boolVal((*dbypassRLS).arg as *mut c_void);
    }

    /* Check some permissions first */
    if !superuser_arg(currentUserId) {
        if !has_createrole_privilege(currentUserId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may create roles.".as_ptr(), c"CREATEROLE".as_ptr()) */ errmsg!("permission denied to create role"));
        }
        if issuper {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may create roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to create role"));
        }
        if createdb && !have_createdb_privilege() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may create roles with the %s attribute.".as_ptr(), c"CREATEDB".as_ptr(), c"CREATEDB".as_ptr()) */ errmsg!("permission denied to create role"));
        }
        if isreplication && !has_rolreplication(currentUserId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may create roles with the %s attribute.".as_ptr(), c"REPLICATION".as_ptr(), c"REPLICATION".as_ptr()) */ errmsg!("permission denied to create role"));
        }
        if bypassrls && !has_bypassrls_privilege(currentUserId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may create roles with the %s attribute.".as_ptr(), c"BYPASSRLS".as_ptr(), c"BYPASSRLS".as_ptr()) */ errmsg!("permission denied to create role"));
        }
    }

    /*
     * Check that the user is not trying to create a role in the reserved
     * "pg_" namespace.
     */
    if IsReservedName((*stmt).role) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_RESERVED_NAME); errdetail(c"Role names starting with \"pg_\" are reserved.".as_ptr()) */ errmsg!("role name \"{}\" is reserved", unsafe { core::ffi::CStr::from_ptr((*stmt).role) }.to_string_lossy()));
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for role names are violated.
     */
    #[cfg(feature = "enforce_regression_test_name_restrictions")]
    if strncmp((*stmt).role, c"regress_".as_ptr(), 8) != 0 {
        elog!(WARNING, "roles created by regression test cases should have names starting with \"regress_\"");
    }

    /*
     * Check the pg_authid relation to be certain the role doesn't already
     * exist.
     */
    pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
    pg_authid_dsc = RelationGetDescr(pg_authid_rel);

    if OidIsValid(get_role_oid((*stmt).role, true)) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */ errmsg!("role \"{}\" already exists", unsafe { core::ffi::CStr::from_ptr((*stmt).role) }.to_string_lossy()));
    }

    /* Convert validuntil to internal form */
    if !validUntil.is_null() {
        validUntil_datum = DirectFunctionCall3(
            timestamptz_in as *const c_void,
            CStringGetDatum(validUntil),
            ObjectIdGetDatum(InvalidOid),
            Int32GetDatum(-1),
        );
        validUntil_null = false;
    } else {
        validUntil_datum = 0;
        validUntil_null = true;
    }

    /*
     * Call the password checking hook if there is one defined
     */
    if let Some(hook) = check_password_hook {
        if !password.is_null() {
            hook((*stmt).role, password, get_password_type(password),
                 validUntil_datum, validUntil_null);
        }
    }

    /*
     * Build a tuple to insert
     */
    new_record[Anum_pg_authid_rolname as usize - 1] =
        DirectFunctionCall1(namein as *const c_void, CStringGetDatum((*stmt).role));
    new_record[Anum_pg_authid_rolsuper as usize - 1] = BoolGetDatum(issuper);
    new_record[Anum_pg_authid_rolinherit as usize - 1] = BoolGetDatum(inherit);
    new_record[Anum_pg_authid_rolcreaterole as usize - 1] = BoolGetDatum(createrole);
    new_record[Anum_pg_authid_rolcreatedb as usize - 1] = BoolGetDatum(createdb);
    new_record[Anum_pg_authid_rolcanlogin as usize - 1] = BoolGetDatum(canlogin);
    new_record[Anum_pg_authid_rolreplication as usize - 1] = BoolGetDatum(isreplication);
    new_record[Anum_pg_authid_rolconnlimit as usize - 1] = Int32GetDatum(connlimit);

    if !password.is_null() {
        let mut shadow_pass: *mut c_char;
        let mut logdetail: *const c_char = ptr::null();

        /*
         * Don't allow an empty password. Libpq treats an empty password the
         * same as no password at all, and won't even try to authenticate. But
         * other clients might, so allowing it would be confusing. By clearing
         * the password when an empty string is specified, the account is
         * consistently locked for all clients.
         *
         * Note that this only covers passwords stored in the database itself.
         * There are also checks in the authentication code, to forbid an
         * empty password from being used with authentication methods that
         * fetch the password from an external system, like LDAP or PAM.
         */
        if *password == 0
            || plain_crypt_verify((*stmt).role, password, c"".as_ptr(), &mut logdetail) == STATUS_OK
        {
            ereport!(NOTICE, errmsg!("empty string is not a valid password, clearing password"));
            new_record_nulls[Anum_pg_authid_rolpassword as usize - 1] = true;
        } else {
            /* Encrypt the password to the requested format. */
            shadow_pass = encrypt_password(Password_encryption, (*stmt).role, password);
            new_record[Anum_pg_authid_rolpassword as usize - 1] =
                CStringGetTextDatum(shadow_pass);
        }
    } else {
        new_record_nulls[Anum_pg_authid_rolpassword as usize - 1] = true;
    }

    new_record[Anum_pg_authid_rolvaliduntil as usize - 1] = validUntil_datum;
    new_record_nulls[Anum_pg_authid_rolvaliduntil as usize - 1] = validUntil_null;

    new_record[Anum_pg_authid_rolbypassrls as usize - 1] = BoolGetDatum(bypassrls);

    /*
     * pg_largeobject_metadata contains pg_authid.oid's, so we use the
     * binary-upgrade override.
     */
    if IsBinaryUpgrade {
        if !OidIsValid(binary_upgrade_next_pg_authid_oid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("pg_authid OID value not set when in binary upgrade mode"));
        }
        roleid = binary_upgrade_next_pg_authid_oid;
        binary_upgrade_next_pg_authid_oid = InvalidOid;
    } else {
        roleid = GetNewOidWithIndex(pg_authid_rel, AuthIdOidIndexId, Anum_pg_authid_oid);
    }

    new_record[Anum_pg_authid_oid as usize - 1] = ObjectIdGetDatum(roleid);

    tuple = heap_form_tuple(pg_authid_dsc, new_record.as_mut_ptr(), new_record_nulls.as_mut_ptr());

    /*
     * Insert new record in the pg_authid table
     */
    CatalogTupleInsert(pg_authid_rel, tuple);

    /*
     * Advance command counter so we can see new record; else tests in
     * AddRoleMems may fail.
     */
    if !addroleto.is_null() || !adminmembers.is_null() || !rolemembers.is_null() {
        CommandCounterIncrement();
    }

    /* Default grant. */
    InitGrantRoleOptions(&mut popt);

    /*
     * Add the new role to the specified existing roles.
     */
    if !addroleto.is_null() {
        let mut thisrole: *mut RoleSpec = makeNode!(RoleSpec, T_RoleSpec) as *mut RoleSpec;
        let thisrole_list: *mut List = list_make1(thisrole as *mut c_void);
        let thisrole_oidlist: *mut List = list_make1_oid(roleid);

        (*thisrole).roletype = ROLESPEC_CSTRING;
        (*thisrole).rolename = (*stmt).role;
        (*thisrole).location = -1;

        foreach!(item, addroleto, {
            let oldrole: *mut RoleSpec = crate::current_cell!(item) as *mut RoleSpec;
            let oldroletup: HeapTuple = get_rolespec_tuple(oldrole);
            let oldroleform: Form_pg_authid = GETSTRUCT(oldroletup) as Form_pg_authid;
            let oldroleid: Oid = (*(oldroleform as *mut FormData_pg_authid_real)).oid;
            let oldrolename: *const c_char = NameStr(&(*(oldroleform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void);

            /* can only add this role to roles for which you have rights */
            check_role_membership_authorization(currentUserId, oldroleid, true);
            AddRoleMems(currentUserId, oldrolename, oldroleid,
                        thisrole_list,
                        thisrole_oidlist,
                        InvalidOid, &mut popt);

            ReleaseSysCache(oldroletup);
        });
    }

    /*
     * If the current user isn't a superuser, make them an admin of the new
     * role so that they can administer the new object they just created.
     * Superusers will be able to do that anyway.
     *
     * The grantor of record for this implicit grant is the bootstrap
     * superuser, which means that the CREATEROLE user cannot revoke the
     * grant. They can however grant the created role back to themselves with
     * different options, since they enjoy ADMIN OPTION on it.
     */
    if !superuser() {
        let mut current_role: *mut RoleSpec = makeNode!(RoleSpec, T_RoleSpec) as *mut RoleSpec;
        let mut poptself: GrantRoleOptions = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: false };
        let memberSpecs: *mut List;
        let memberIds: *mut List = list_make1_oid(currentUserId);

        (*current_role).roletype = ROLESPEC_CURRENT_ROLE;
        (*current_role).location = -1;
        memberSpecs = list_make1(current_role as *mut c_void);

        poptself.specified = GRANT_ROLE_SPECIFIED_ADMIN
            | GRANT_ROLE_SPECIFIED_INHERIT
            | GRANT_ROLE_SPECIFIED_SET;
        poptself.admin = true;
        poptself.inherit = false;
        poptself.set = false;

        AddRoleMems(BOOTSTRAP_SUPERUSERID, (*stmt).role, roleid,
                    memberSpecs, memberIds,
                    BOOTSTRAP_SUPERUSERID, &mut poptself);

        /*
         * We must make the implicit grant visible to the code below, else the
         * additional grants will fail.
         */
        CommandCounterIncrement();

        /*
         * Because of the implicit grant above, a CREATEROLE user who creates
         * a role has the ability to grant that role back to themselves with
         * the INHERIT or SET options, if they wish to inherit the role's
         * privileges or be able to SET ROLE to it. The createrole_self_grant
         * GUC can be used to make this happen automatically. This has no
         * security implications since the same user is able to make the same
         * grant using an explicit GRANT statement; it's just convenient.
         */
        if createrole_self_grant_enabled {
            AddRoleMems(currentUserId, (*stmt).role, roleid,
                        memberSpecs, memberIds,
                        currentUserId, &mut createrole_self_grant_options);
        }
    }

    /*
     * Add the specified members to this new role. adminmembers get the admin
     * option, rolemembers don't.
     *
     * NB: No permissions check is required here. If you have enough rights to
     * create a role, you can add any members you like.
     */
    AddRoleMems(currentUserId, (*stmt).role, roleid,
                rolemembers, roleSpecsToIds(rolemembers),
                InvalidOid, &mut popt);
    popt.specified |= GRANT_ROLE_SPECIFIED_ADMIN;
    popt.admin = true;
    AddRoleMems(currentUserId, (*stmt).role, roleid,
                adminmembers, roleSpecsToIds(adminmembers),
                InvalidOid, &mut popt);

    /* Post creation hook for new role */
    InvokeObjectPostCreateHook(AuthIdRelationId, roleid, 0);

    /*
     * Close pg_authid, but keep lock till commit.
     */
    table_close(pg_authid_rel, NoLock);

    return roleid;
}

/*
 * ALTER ROLE
 *
 * Note: the rolemembers option accepted here is intended to support the
 * backwards-compatible ALTER GROUP syntax.  Although it will work to say
 * "ALTER ROLE role ROLE rolenames", we don't document it.
 */
#[no_mangle]
pub unsafe extern "C" fn AlterRole(pstate: *mut ParseState, stmt: *mut AlterRoleStmt) -> Oid {
    let mut new_record: [Datum; 32] = [0usize; 32];
    let mut new_record_nulls: [bool; 32] = [false; 32];
    let mut new_record_repl: [bool; 32] = [false; 32];
    let pg_authid_rel: Relation;
    let pg_authid_dsc: TupleDesc;
    let tuple: HeapTuple;
    let new_tuple: HeapTuple;
    let authform: Form_pg_authid;
    let rolename: *mut c_char;
    let mut password: *mut c_char = ptr::null_mut(); /* user password */
    let mut connlimit: c_int = -1;                   /* maximum connections allowed */
    let mut validUntil: *mut c_char = ptr::null_mut(); /* time the login is valid until */
    let mut validUntil_datum: Datum;                 /* same, as timestamptz Datum */
    let mut validUntil_null: bool = false;
    let mut dpassword: *mut DefElem = ptr::null_mut();
    let mut dissuper: *mut DefElem = ptr::null_mut();
    let mut dinherit: *mut DefElem = ptr::null_mut();
    let mut dcreaterole: *mut DefElem = ptr::null_mut();
    let mut dcreatedb: *mut DefElem = ptr::null_mut();
    let mut dcanlogin: *mut DefElem = ptr::null_mut();
    let mut disreplication: *mut DefElem = ptr::null_mut();
    let mut dconnlimit: *mut DefElem = ptr::null_mut();
    let mut drolemembers: *mut DefElem = ptr::null_mut();
    let mut dvalidUntil: *mut DefElem = ptr::null_mut();
    let mut dbypassRLS: *mut DefElem = ptr::null_mut();
    let roleid: Oid;
    let currentUserId: Oid = GetUserId();
    let mut popt: GrantRoleOptions = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: false };

    check_rolespec_name((*stmt).role, c"Cannot alter reserved roles.".as_ptr());

    /* Extract options from the statement node tree */
    foreach!(option, (*stmt).options, {
        let defel: *mut DefElem = crate::current_cell!(option) as *mut DefElem;

        if strcmp((*defel).defname, c"password".as_ptr()) == 0 {
            if !dpassword.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dpassword = defel;
        } else if strcmp((*defel).defname, c"superuser".as_ptr()) == 0 {
            if !dissuper.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dissuper = defel;
        } else if strcmp((*defel).defname, c"inherit".as_ptr()) == 0 {
            if !dinherit.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dinherit = defel;
        } else if strcmp((*defel).defname, c"createrole".as_ptr()) == 0 {
            if !dcreaterole.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dcreaterole = defel;
        } else if strcmp((*defel).defname, c"createdb".as_ptr()) == 0 {
            if !dcreatedb.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dcreatedb = defel;
        } else if strcmp((*defel).defname, c"canlogin".as_ptr()) == 0 {
            if !dcanlogin.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dcanlogin = defel;
        } else if strcmp((*defel).defname, c"isreplication".as_ptr()) == 0 {
            if !disreplication.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            disreplication = defel;
        } else if strcmp((*defel).defname, c"connectionlimit".as_ptr()) == 0 {
            if !dconnlimit.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dconnlimit = defel;
        } else if strcmp((*defel).defname, c"rolemembers".as_ptr()) == 0
            && (*stmt).action != 0
        {
            if !drolemembers.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            drolemembers = defel;
        } else if strcmp((*defel).defname, c"validUntil".as_ptr()) == 0 {
            if !dvalidUntil.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dvalidUntil = defel;
        } else if strcmp((*defel).defname, c"bypassrls".as_ptr()) == 0 {
            if !dbypassRLS.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            dbypassRLS = defel;
        } else {
            elog!(ERROR, "option \"{}\" not recognized", unsafe { core::ffi::CStr::from_ptr((*defel).defname) }.to_string_lossy());
        }
    });

    if !dpassword.is_null() && !(*dpassword).arg.is_null() {
        password = strVal((*dpassword).arg as *mut c_void);
    }
    if !dconnlimit.is_null() {
        connlimit = intVal((*dconnlimit).arg as *mut c_void);
        if connlimit < -1 {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("invalid connection limit: {}", connlimit));
        }
    }
    if !dvalidUntil.is_null() {
        validUntil = strVal((*dvalidUntil).arg as *mut c_void);
    }

    /*
     * Scan the pg_authid relation to be certain the user exists.
     */
    pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
    pg_authid_dsc = RelationGetDescr(pg_authid_rel);

    tuple = get_rolespec_tuple((*stmt).role);
    authform = GETSTRUCT(tuple) as Form_pg_authid;
    rolename = pstrdup(NameStr(&(*(authform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void));
    roleid = (*(authform as *mut FormData_pg_authid_real)).oid;

    /* To mess with a superuser in any way you gotta be superuser. */
    if !superuser() && (*(authform as *mut FormData_pg_authid_real)).rolsuper {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may alter roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to alter role"));
    }
    if !superuser() && !dissuper.is_null() {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may change the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to alter role"));
    }

    /*
     * Most changes to a role require that you both have CREATEROLE privileges
     * and also ADMIN OPTION on the role.
     */
    if !have_createrole_privilege() || !is_admin_of_role(GetUserId(), roleid) {
        /* things an unprivileged user certainly can't do */
        if !dinherit.is_null() || !dcreaterole.is_null() || !dcreatedb.is_null()
            || !dcanlogin.is_null() || !dconnlimit.is_null()
            || !dvalidUntil.is_null() || !disreplication.is_null() || !dbypassRLS.is_null()
        {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute and the %s option on role \"%s\" may alter this role.".as_ptr(), c"CREATEROLE".as_ptr(), c"ADMIN".as_ptr(), rolename) */ errmsg!("permission denied to alter role"));
        }

        /* an unprivileged user can change their own password */
        if !dpassword.is_null() && roleid != currentUserId {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"To change another role's password, the current user must have the %s attribute and the %s option on the role.".as_ptr(), c"CREATEROLE".as_ptr(), c"ADMIN".as_ptr()) */ errmsg!("permission denied to alter role"));
        }
    } else if !superuser() {
        /*
         * Even if you have both CREATEROLE and ADMIN OPTION on a role, you
         * can only change the CREATEDB, REPLICATION, or BYPASSRLS attributes
         * if they are set for your own role (or you are the superuser).
         */
        if !dcreatedb.is_null() && !have_createdb_privilege() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may change the %s attribute.".as_ptr(), c"CREATEDB".as_ptr(), c"CREATEDB".as_ptr()) */ errmsg!("permission denied to alter role"));
        }
        if !disreplication.is_null() && !has_rolreplication(currentUserId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may change the %s attribute.".as_ptr(), c"REPLICATION".as_ptr(), c"REPLICATION".as_ptr()) */ errmsg!("permission denied to alter role"));
        }
        if !dbypassRLS.is_null() && !has_bypassrls_privilege(currentUserId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may change the %s attribute.".as_ptr(), c"BYPASSRLS".as_ptr(), c"BYPASSRLS".as_ptr()) */ errmsg!("permission denied to alter role"));
        }
    }

    /* To add or drop members, you need ADMIN OPTION. */
    if !drolemembers.is_null() && !is_admin_of_role(currentUserId, roleid) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s option on role \"%s\" may add or drop members.".as_ptr(), c"ADMIN".as_ptr(), rolename) */ errmsg!("permission denied to alter role"));
    }

    /* Convert validuntil to internal form */
    if !dvalidUntil.is_null() {
        validUntil_datum = DirectFunctionCall3(
            timestamptz_in as *const c_void,
            CStringGetDatum(validUntil),
            ObjectIdGetDatum(InvalidOid),
            Int32GetDatum(-1),
        );
        validUntil_null = false;
    } else {
        /* fetch existing setting in case hook needs it */
        validUntil_datum = SysCacheGetAttr(AUTHNAME, tuple,
            Anum_pg_authid_rolvaliduntil, &mut validUntil_null);
    }

    /*
     * Call the password checking hook if there is one defined
     */
    if let Some(hook) = check_password_hook {
        if !password.is_null() {
            hook(rolename, password, get_password_type(password),
                 validUntil_datum, validUntil_null);
        }
    }

    /*
     * Build an updated tuple, perusing the information just obtained
     */

    /*
     * issuper/createrole/etc
     */
    if !dissuper.is_null() {
        let should_be_super: bool = boolVal((*dissuper).arg as *mut c_void);

        if !should_be_super && roleid == BOOTSTRAP_SUPERUSERID {
            ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); errdetail(c"The bootstrap superuser must have the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to alter role"));
        }

        new_record[Anum_pg_authid_rolsuper as usize - 1] = BoolGetDatum(should_be_super);
        new_record_repl[Anum_pg_authid_rolsuper as usize - 1] = true;
    }

    if !dinherit.is_null() {
        new_record[Anum_pg_authid_rolinherit as usize - 1] = BoolGetDatum(boolVal((*dinherit).arg as *mut c_void));
        new_record_repl[Anum_pg_authid_rolinherit as usize - 1] = true;
    }

    if !dcreaterole.is_null() {
        new_record[Anum_pg_authid_rolcreaterole as usize - 1] = BoolGetDatum(boolVal((*dcreaterole).arg as *mut c_void));
        new_record_repl[Anum_pg_authid_rolcreaterole as usize - 1] = true;
    }

    if !dcreatedb.is_null() {
        new_record[Anum_pg_authid_rolcreatedb as usize - 1] = BoolGetDatum(boolVal((*dcreatedb).arg as *mut c_void));
        new_record_repl[Anum_pg_authid_rolcreatedb as usize - 1] = true;
    }

    if !dcanlogin.is_null() {
        new_record[Anum_pg_authid_rolcanlogin as usize - 1] = BoolGetDatum(boolVal((*dcanlogin).arg as *mut c_void));
        new_record_repl[Anum_pg_authid_rolcanlogin as usize - 1] = true;
    }

    if !disreplication.is_null() {
        new_record[Anum_pg_authid_rolreplication as usize - 1] = BoolGetDatum(boolVal((*disreplication).arg as *mut c_void));
        new_record_repl[Anum_pg_authid_rolreplication as usize - 1] = true;
    }

    if !dconnlimit.is_null() {
        new_record[Anum_pg_authid_rolconnlimit as usize - 1] = Int32GetDatum(connlimit);
        new_record_repl[Anum_pg_authid_rolconnlimit as usize - 1] = true;
    }

    /* password */
    if !password.is_null() {
        let shadow_pass: *mut c_char;
        let mut logdetail: *const c_char = ptr::null();

        /* Like in CREATE USER, don't allow an empty password. */
        if *password == 0
            || plain_crypt_verify(rolename, password, c"".as_ptr(), &mut logdetail) == STATUS_OK
        {
            ereport!(NOTICE, errmsg!("empty string is not a valid password, clearing password"));
            new_record_nulls[Anum_pg_authid_rolpassword as usize - 1] = true;
        } else {
            /* Encrypt the password to the requested format. */
            shadow_pass = encrypt_password(Password_encryption, rolename, password);
            new_record[Anum_pg_authid_rolpassword as usize - 1] =
                CStringGetTextDatum(shadow_pass);
        }
        new_record_repl[Anum_pg_authid_rolpassword as usize - 1] = true;
    }

    /* unset password */
    if !dpassword.is_null() && (*dpassword).arg.is_null() {
        new_record_repl[Anum_pg_authid_rolpassword as usize - 1] = true;
        new_record_nulls[Anum_pg_authid_rolpassword as usize - 1] = true;
    }

    /* valid until */
    new_record[Anum_pg_authid_rolvaliduntil as usize - 1] = validUntil_datum;
    new_record_nulls[Anum_pg_authid_rolvaliduntil as usize - 1] = validUntil_null;
    new_record_repl[Anum_pg_authid_rolvaliduntil as usize - 1] = true;

    if !dbypassRLS.is_null() {
        new_record[Anum_pg_authid_rolbypassrls as usize - 1] = BoolGetDatum(boolVal((*dbypassRLS).arg as *mut c_void));
        new_record_repl[Anum_pg_authid_rolbypassrls as usize - 1] = true;
    }

    new_tuple = heap_modify_tuple(tuple, pg_authid_dsc, new_record.as_mut_ptr(),
                                  new_record_nulls.as_mut_ptr(), new_record_repl.as_mut_ptr());
    CatalogTupleUpdate(pg_authid_rel, &mut (*tuple).t_self, new_tuple);

    InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0);

    ReleaseSysCache(tuple);
    heap_freetuple(new_tuple);

    InitGrantRoleOptions(&mut popt);

    /*
     * Advance command counter so we can see new record; else tests in
     * AddRoleMems may fail.
     */
    if !drolemembers.is_null() {
        let rolemembers: *mut List = (*drolemembers).arg as *mut List;

        CommandCounterIncrement();

        if (*stmt).action == 1 {
            /* add members to role */
            AddRoleMems(currentUserId, rolename, roleid,
                        rolemembers, roleSpecsToIds(rolemembers),
                        InvalidOid, &mut popt);
        } else if (*stmt).action == -1 {
            /* drop members from role */
            DelRoleMems(currentUserId, rolename, roleid,
                        rolemembers, roleSpecsToIds(rolemembers),
                        InvalidOid, &mut popt, DropBehavior::DROP_RESTRICT);
        }
    }

    /*
     * Close pg_authid, but keep lock till commit.
     */
    table_close(pg_authid_rel, NoLock);

    return roleid;
}


/*
 * ALTER ROLE ... SET
 */
#[no_mangle]
pub unsafe extern "C" fn AlterRoleSet(stmt: *mut AlterRoleSetStmt) -> Oid {
    let roletuple: HeapTuple;
    let roleform: Form_pg_authid;
    let mut databaseid: Oid = InvalidOid;
    let mut roleid: Oid = InvalidOid;

    if !(*stmt).role.is_null() {
        check_rolespec_name((*stmt).role, c"Cannot alter reserved roles.".as_ptr());

        roletuple = get_rolespec_tuple((*stmt).role);
        roleform = GETSTRUCT(roletuple) as Form_pg_authid;
        roleid = (*(roleform as *mut FormData_pg_authid_real)).oid;

        /*
         * Obtain a lock on the role and make sure it didn't go away in the
         * meantime.
         */
        shdepLockAndCheckObject(AuthIdRelationId, roleid);

        /*
         * To mess with a superuser you gotta be superuser; otherwise you need
         * CREATEROLE plus admin option on the target role; unless you're just
         * trying to change your own settings
         */
        if (*(roleform as *mut FormData_pg_authid_real)).rolsuper {
            if !superuser() {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may alter roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to alter role"));
            }
        } else {
            if (!have_createrole_privilege() || !is_admin_of_role(GetUserId(), roleid))
                && roleid != GetUserId()
            {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute and the %s option on role \"%s\" may alter this role.".as_ptr(), c"CREATEROLE".as_ptr(), c"ADMIN".as_ptr(), NameStr(&(*(roleform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void)) */ errmsg!("permission denied to alter role"));
            }
        }

        ReleaseSysCache(roletuple);
    }

    /* look up and lock the database, if specified */
    if !(*stmt).database.is_null() {
        databaseid = get_database_oid((*stmt).database, false);
        shdepLockAndCheckObject(DatabaseRelationId, databaseid);

        if (*stmt).role.is_null() {
            /*
             * If no role is specified, then this is effectively the same as
             * ALTER DATABASE ... SET, so use the same permission check.
             */
            if !object_ownercheck(DatabaseRelationId, databaseid, GetUserId()) {
                aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, (*stmt).database);
            }
        }
    }

    if (*stmt).role.is_null() && (*stmt).database.is_null() {
        /* Must be superuser to alter settings globally. */
        if !superuser() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may alter settings globally.".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to alter setting"));
        }
    }

    AlterSetting(databaseid, roleid, (*stmt).setstmt as *mut c_void);

    return roleid;
}


/*
 * DROP ROLE
 */
#[no_mangle]
pub unsafe extern "C" fn DropRole(stmt: *mut DropRoleStmt) {
    let pg_authid_rel: Relation;
    let pg_auth_members_rel: Relation;
    let mut role_oids: *mut List = NIL();

    if !have_createrole_privilege() {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute and the %s option on the target roles may drop roles.".as_ptr(), c"CREATEROLE".as_ptr(), c"ADMIN".as_ptr()) */ errmsg!("permission denied to drop role"));
    }

    /*
     * Scan the pg_authid relation to find the Oid of the role(s) to be
     * deleted and perform preliminary permissions and sanity checks.
     */
    pg_authid_rel = table_open(AuthIdRelationId, RowExclusiveLock);
    pg_auth_members_rel = table_open(AuthMemRelationId, RowExclusiveLock);

    foreach!(item, (*stmt).roles, {
        let rolspec: *mut RoleSpec = crate::current_cell!(item) as *mut RoleSpec;
        let role: *mut c_char;
        let tuple: HeapTuple;
        let mut tmp_tuple: HeapTuple;
        let roleform: Form_pg_authid;
        let mut scankey: ScanKeyData = core::mem::zeroed();
        let mut sscan: SysScanDesc;
        let roleid: Oid;

        if (*rolspec).roletype as c_int != ROLESPEC_CSTRING {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */ errmsg!("cannot use special role specifier in DROP ROLE"));
        }
        role = (*rolspec).rolename;

        tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(role as *const c_void));
        if !HeapTupleIsValid(tuple) {
            if !(*stmt).missing_ok {
                ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */ errmsg!("role \"{}\" does not exist", unsafe { core::ffi::CStr::from_ptr(role) }.to_string_lossy()));
            } else {
                ereport!(NOTICE, errmsg!("role \"{}\" does not exist, skipping", unsafe { core::ffi::CStr::from_ptr(role) }.to_string_lossy()));
            }

            item.i += 1;
            continue;
        }

        roleform = GETSTRUCT(tuple) as Form_pg_authid;
        roleid = (*(roleform as *mut FormData_pg_authid_real)).oid;

        if roleid == GetUserId() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE) */ errmsg!("current user cannot be dropped"));
        }
        if roleid == GetOuterUserId() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE) */ errmsg!("current user cannot be dropped"));
        }
        if roleid == GetSessionUserId() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_OBJECT_IN_USE) */ errmsg!("session user cannot be dropped"));
        }

        /*
         * For safety's sake, we allow createrole holders to drop ordinary
         * roles but not superuser roles, and only if they also have ADMIN
         * OPTION.
         */
        if (*(roleform as *mut FormData_pg_authid_real)).rolsuper && !superuser() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may drop roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to drop role"));
        }
        if !is_admin_of_role(GetUserId(), roleid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute and the %s option on role \"%s\" may drop this role.".as_ptr(), c"CREATEROLE".as_ptr(), c"ADMIN".as_ptr(), NameStr(&(*(roleform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void)) */ errmsg!("permission denied to drop role"));
        }

        /* DROP hook for the role being removed */
        InvokeObjectDropHook(AuthIdRelationId, roleid, 0);

        /* Don't leak the syscache tuple */
        ReleaseSysCache(tuple);

        /*
         * Lock the role, so nobody can add dependencies to her while we drop
         * her.  We keep the lock until the end of transaction.
         */
        LockSharedObject(AuthIdRelationId, roleid, 0, AccessExclusiveLock);

        /*
         * If there is a pg_auth_members entry that has one of the roles to be
         * dropped as the roleid or member, it should be silently removed, but
         * if there is a pg_auth_members entry that has one of the roles to be
         * dropped as the grantor, the operation should fail.
         *
         * It's possible, however, that a single pg_auth_members entry could
         * fall into multiple categories - e.g. the user could do "GRANT foo
         * TO bar GRANTED BY baz" and then "DROP ROLE baz, bar". We want such
         * an operation to succeed regardless of the order in which the
         * to-be-dropped roles are passed to DROP ROLE.
         *
         * To make that work, we remove all pg_auth_members entries that can
         * be silently removed in this loop, and then below we'll make a
         * second pass over the list of roles to be removed and check for any
         * remaining dependencies.
         */
        ScanKeyInit(&mut scankey,
            Anum_pg_auth_members_roleid,
            BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(roleid));

        sscan = systable_beginscan(pg_auth_members_rel, AuthMemRoleMemIndexId,
            true, ptr::null_mut(), 1, &mut scankey);

        loop {
            tmp_tuple = systable_getnext(sscan);
            if !HeapTupleIsValid(tmp_tuple) {
                break;
            }
            let authmem_form: Form_pg_auth_members = GETSTRUCT(tmp_tuple) as Form_pg_auth_members;
            deleteSharedDependencyRecordsFor(AuthMemRelationId,
                (*(authmem_form as *mut FormData_pg_auth_members_real)).oid, 0);
            CatalogTupleDelete(pg_auth_members_rel, &mut (*tmp_tuple).t_self);
        }

        systable_endscan(sscan);

        ScanKeyInit(&mut scankey,
            Anum_pg_auth_members_member,
            BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(roleid));

        sscan = systable_beginscan(pg_auth_members_rel, AuthMemMemRoleIndexId,
            true, ptr::null_mut(), 1, &mut scankey);

        loop {
            tmp_tuple = systable_getnext(sscan);
            if !HeapTupleIsValid(tmp_tuple) {
                break;
            }
            let authmem_form: Form_pg_auth_members = GETSTRUCT(tmp_tuple) as Form_pg_auth_members;
            deleteSharedDependencyRecordsFor(AuthMemRelationId,
                (*(authmem_form as *mut FormData_pg_auth_members_real)).oid, 0);
            CatalogTupleDelete(pg_auth_members_rel, &mut (*tmp_tuple).t_self);
        }

        systable_endscan(sscan);

        /*
         * Advance command counter so that later iterations of this loop will
         * see the changes already made.  This is essential if, for example,
         * we are trying to drop both a role and one of its direct members ---
         * we'll get an error if we try to delete the linking pg_auth_members
         * tuple twice.  (We do not need a CCI between the two delete loops
         * above, because it's not allowed for a role to directly contain
         * itself.)
         */
        CommandCounterIncrement();

        /* Looks tentatively OK, add it to the list if not there yet. */
        role_oids = list_append_unique_oid(role_oids, roleid);
    });

    /*
     * Second pass over the roles to be removed.
     */
    foreach!(item, role_oids, {
        let roleid: Oid = lfirst_oid(crate::current_cell!(item));
        let tuple: HeapTuple;
        let roleform: Form_pg_authid;
        let mut detail: *mut c_char = ptr::null_mut();
        let mut detail_log: *mut c_char = ptr::null_mut();

        /*
         * Re-find the pg_authid tuple.
         *
         * Since we've taken a lock on the role OID, it shouldn't be possible
         * for the tuple to have been deleted -- or for that matter updated --
         * unless the user is manually modifying the system catalogs.
         */
        tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "could not find tuple for role {}", roleid);
        }
        roleform = GETSTRUCT(tuple) as Form_pg_authid;

        /*
         * Check for pg_shdepend entries depending on this role.
         *
         * This needs to happen after we've completed removing any
         * pg_auth_members entries that can be removed silently, in order to
         * avoid spurious failures. See notes above for more details.
         */
        if checkSharedDependencies(AuthIdRelationId, roleid, &mut detail, &mut detail_log) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST); errdetail_internal(c"%s".as_ptr(), detail); errdetail_log(c"%s".as_ptr(), detail_log) */ errmsg!("role \"{}\" cannot be dropped because some objects depend on it", unsafe { core::ffi::CStr::from_ptr(NameStr(&(*(roleform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void)) }.to_string_lossy()));
        }

        /*
         * Remove the role from the pg_authid table
         */
        CatalogTupleDelete(pg_authid_rel, &mut (*tuple).t_self);

        ReleaseSysCache(tuple);

        /*
         * Remove any comments or security labels on this role.
         */
        DeleteSharedComments(roleid, AuthIdRelationId);
        DeleteSharedSecurityLabel(roleid, AuthIdRelationId);

        /*
         * Remove settings for this role.
         */
        DropSetting(InvalidOid, roleid);
    });

    /*
     * Now we can clean up; but keep locks until commit.
     */
    table_close(pg_auth_members_rel, NoLock);
    table_close(pg_authid_rel, NoLock);
}

/*
 * Rename role
 */
#[no_mangle]
pub unsafe extern "C" fn RenameRole(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let oldtuple: HeapTuple;
    let newtuple: HeapTuple;
    let dsc: TupleDesc;
    let rel: Relation;
    let datum: Datum;
    let mut isnull: bool = false;
    let mut repl_val: [Datum; 32] = [0usize; 32];
    let mut repl_null: [bool; 32] = [false; 32];
    let mut repl_repl: [bool; 32] = [false; 32];
    let mut i: c_int;
    let roleid: Oid;
    let mut address: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let authform: Form_pg_authid;

    rel = table_open(AuthIdRelationId, RowExclusiveLock);
    dsc = RelationGetDescr(rel);

    oldtuple = SearchSysCache1(AUTHNAME, CStringGetDatum(oldname));
    if !HeapTupleIsValid(oldtuple) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */ errmsg!("role \"{}\" does not exist", unsafe { core::ffi::CStr::from_ptr(oldname) }.to_string_lossy()));
    }

    /*
     * XXX Client applications probably store the session user somewhere, so
     * renaming it could cause confusion.  On the other hand, there may not be
     * an actual problem besides a little confusion, so think about this and
     * decide.  Same for SET ROLE ... we don't restrict renaming the current
     * effective userid, though.
     */

    authform = GETSTRUCT(oldtuple) as Form_pg_authid;
    roleid = (*(authform as *mut FormData_pg_authid_real)).oid;

    if roleid == GetSessionUserId() {
        ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */ errmsg!("session user cannot be renamed"));
    }
    if roleid == GetOuterUserId() {
        ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */ errmsg!("current user cannot be renamed"));
    }

    /*
     * Check that the user is not trying to rename a system role and not
     * trying to rename a role into the reserved "pg_" namespace.
     */
    if IsReservedName(NameStr(&(*(authform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void)) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_RESERVED_NAME); errdetail(c"Role names starting with \"pg_\" are reserved.".as_ptr()) */ errmsg!("role name \"{}\" is reserved", unsafe { core::ffi::CStr::from_ptr(NameStr(&(*(authform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void)) }.to_string_lossy()));
    }

    if IsReservedName(newname) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_RESERVED_NAME); errdetail(c"Role names starting with \"pg_\" are reserved.".as_ptr()) */ errmsg!("role name \"{}\" is reserved", unsafe { core::ffi::CStr::from_ptr(newname) }.to_string_lossy()));
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for role names are violated.
     */
    #[cfg(feature = "enforce_regression_test_name_restrictions")]
    if strncmp(newname, c"regress_".as_ptr(), 8) != 0 {
        elog!(WARNING, "roles created by regression test cases should have names starting with \"regress_\"");
    }

    /* make sure the new name doesn't exist */
    if SearchSysCacheExists1(AUTHNAME, CStringGetDatum(newname)) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */ errmsg!("role \"{}\" already exists", unsafe { core::ffi::CStr::from_ptr(newname) }.to_string_lossy()));
    }

    /*
     * Only superusers can mess with superusers. Otherwise, a user with
     * CREATEROLE can rename a role for which they have ADMIN OPTION.
     */
    if (*(authform as *mut FormData_pg_authid_real)).rolsuper {
        if !superuser() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may rename roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to rename role"));
        }
    } else {
        if !have_createrole_privilege() || !is_admin_of_role(GetUserId(), roleid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute and the %s option on role \"%s\" may rename this role.".as_ptr(), c"CREATEROLE".as_ptr(), c"ADMIN".as_ptr(), NameStr(&(*(authform as *mut FormData_pg_authid_real)).rolname as *const _ as *const c_void)) */ errmsg!("permission denied to rename role"));
        }
    }

    /* OK, construct the modified tuple */
    i = 0;
    while i < Natts_pg_authid {
        repl_repl[i as usize] = false;
        i += 1;
    }

    repl_repl[Anum_pg_authid_rolname as usize - 1] = true;
    repl_val[Anum_pg_authid_rolname as usize - 1] = DirectFunctionCall1(namein as *const c_void,
        CStringGetDatum(newname));
    repl_null[Anum_pg_authid_rolname as usize - 1] = false;

    datum = heap_getattr(oldtuple, Anum_pg_authid_rolpassword, dsc, &mut isnull);

    if !isnull && get_password_type(TextDatumGetCString(datum)) == PASSWORD_TYPE_MD5 {
        /* MD5 uses the username as salt, so just clear it on a rename */
        repl_repl[Anum_pg_authid_rolpassword as usize - 1] = true;
        repl_null[Anum_pg_authid_rolpassword as usize - 1] = true;

        ereport!(NOTICE, errmsg!("MD5 password cleared because of role rename"));
    }

    newtuple = heap_modify_tuple(oldtuple, dsc, repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());
    CatalogTupleUpdate(rel, &mut (*oldtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0);

    ObjectAddressSet(&mut address, AuthIdRelationId, roleid);

    ReleaseSysCache(oldtuple);

    /*
     * Close pg_authid, but keep lock till commit.
     */
    table_close(rel, NoLock);

    return address;
}

/*
 * GrantRoleStmt
 *
 * Grant/Revoke roles to/from roles
 */
#[no_mangle]
pub unsafe extern "C" fn GrantRole(pstate: *mut ParseState, stmt: *mut GrantRoleStmt) {
    let pg_authid_rel: Relation;
    let grantor: Oid;
    let grantee_ids: *mut List;
    let mut popt: GrantRoleOptions = GrantRoleOptions { specified: 0, admin: false, inherit: false, set: false };
    let currentUserId: Oid = GetUserId();

    /* Parse options list. */
    InitGrantRoleOptions(&mut popt);
    foreach!(item, (*stmt).opt, {
        let opt: *mut DefElem = crate::current_cell!(item) as *mut DefElem;
        let optval: *mut c_char = defGetString(opt);

        if strcmp((*opt).defname, c"admin".as_ptr()) == 0 {
            popt.specified |= GRANT_ROLE_SPECIFIED_ADMIN;

            if parse_bool(optval, &mut popt.admin) {
                item.i += 1;
                continue;
            }
        } else if strcmp((*opt).defname, c"inherit".as_ptr()) == 0 {
            popt.specified |= GRANT_ROLE_SPECIFIED_INHERIT;
            if parse_bool(optval, &mut popt.inherit) {
                item.i += 1;
                continue;
            }
        } else if strcmp((*opt).defname, c"set".as_ptr()) == 0 {
            popt.specified |= GRANT_ROLE_SPECIFIED_SET;
            if parse_bool(optval, &mut popt.set) {
                item.i += 1;
                continue;
            }
        } else {
            ereport!(ERROR, /* C also: errcode(ERRCODE_SYNTAX_ERROR); parser_errposition(pstate, (*opt).location) */ errmsg!("unrecognized role option \"{}\"", unsafe { core::ffi::CStr::from_ptr((*opt).defname) }.to_string_lossy()));
        }

        ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE); parser_errposition(pstate, (*opt).location) */ errmsg!("unrecognized value for role option \"{}\": \"{}\"", unsafe { core::ffi::CStr::from_ptr((*opt).defname) }.to_string_lossy(), unsafe { core::ffi::CStr::from_ptr(optval) }.to_string_lossy()));
    });

    /* Lookup OID of grantor, if specified. */
    if !(*stmt).grantor.is_null() {
        grantor = get_rolespec_oid((*stmt).grantor, false);
    } else {
        grantor = InvalidOid;
    }

    grantee_ids = roleSpecsToIds((*stmt).grantee_roles);

    /* AccessShareLock is enough since we aren't modifying pg_authid */
    pg_authid_rel = table_open(AuthIdRelationId, AccessShareLock);

    /*
     * Step through all of the granted roles and add, update, or remove
     * entries in pg_auth_members as appropriate. If stmt->is_grant is true,
     * we are adding new grants or, if they already exist, updating options on
     * those grants. If stmt->is_grant is false, we are revoking grants or
     * removing options from them.
     */
    foreach!(item, (*stmt).granted_roles, {
        let priv_: *mut AccessPriv = crate::current_cell!(item) as *mut AccessPriv;
        let rolename: *mut c_char = (*priv_).priv_name;
        let roleid: Oid;

        /* Must reject priv(columns) and ALL PRIVILEGES(columns) */
        if rolename.is_null() || !(*priv_).cols.is_null() {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_GRANT_OPERATION) */ errmsg!("column names cannot be included in GRANT/REVOKE ROLE"));
        }

        roleid = get_role_oid(rolename, false);
        check_role_membership_authorization(currentUserId, roleid, (*stmt).is_grant);
        if (*stmt).is_grant {
            AddRoleMems(currentUserId, rolename, roleid,
                        (*stmt).grantee_roles, grantee_ids,
                        grantor, &mut popt);
        } else {
            DelRoleMems(currentUserId, rolename, roleid,
                        (*stmt).grantee_roles, grantee_ids,
                        grantor, &mut popt, (*stmt).behavior);
        }
    });

    /*
     * Close pg_authid, but keep lock till commit.
     */
    table_close(pg_authid_rel, NoLock);
}

/*
 * DropOwnedObjects
 *
 * Drop the objects owned by a given list of roles.
 */
#[no_mangle]
pub unsafe extern "C" fn DropOwnedObjects(stmt: *mut DropOwnedStmt) {
    let role_ids: *mut List = roleSpecsToIds((*stmt).roles);

    /* Check privileges */
    foreach!(cell, role_ids, {
        let roleid: Oid = lfirst_oid(crate::current_cell!(cell));

        if !has_privs_of_role(GetUserId(), roleid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with privileges of role \"%s\" may drop objects owned by it.".as_ptr(), GetUserNameFromId(roleid, false)) */ errmsg!("permission denied to drop objects"));
        }
    });

    /* Ok, do it */
    shdepDropOwned(role_ids, (*stmt).behavior);
}

/*
 * ReassignOwnedObjects
 *
 * Give the objects owned by a given list of roles away to another user.
 */
#[no_mangle]
pub unsafe extern "C" fn ReassignOwnedObjects(stmt: *mut ReassignOwnedStmt) {
    let role_ids: *mut List = roleSpecsToIds((*stmt).roles);
    let newrole: Oid;

    /* Check privileges */
    foreach!(cell, role_ids, {
        let roleid: Oid = lfirst_oid(crate::current_cell!(cell));

        if !has_privs_of_role(GetUserId(), roleid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with privileges of role \"%s\" may reassign objects owned by it.".as_ptr(), GetUserNameFromId(roleid, false)) */ errmsg!("permission denied to reassign objects"));
        }
    });

    /* Must have privileges on the receiving side too */
    newrole = get_rolespec_oid((*stmt).newrole, false);

    if !has_privs_of_role(GetUserId(), newrole) {
        ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with privileges of role \"%s\" may reassign objects to it.".as_ptr(), GetUserNameFromId(newrole, false)) */ errmsg!("permission denied to reassign objects"));
    }

    /* Ok, do it */
    shdepReassignOwned(role_ids, newrole);
}

/*
 * roleSpecsToIds
 *
 * Given a list of RoleSpecs, generate a list of role OIDs in the same order.
 *
 * ROLESPEC_PUBLIC is not allowed.
 */
#[no_mangle]
pub unsafe extern "C" fn roleSpecsToIds(memberNames: *mut List) -> *mut List {
    let mut result: *mut List = NIL();

    foreach!(l, memberNames, {
        let rolespec: *mut RoleSpec = lfirst_node!(RoleSpec, T_RoleSpec, crate::current_cell!(l));
        let roleid: Oid;

        roleid = get_rolespec_oid(rolespec, false);
        result = lappend_oid(result, roleid);
    });
    return result;
}

/*
 * AddRoleMems -- Add given members to the specified role
 *
 * currentUserId: OID of role performing the operation
 * rolename: name of role to add to (used only for error messages)
 * roleid: OID of role to add to
 * memberSpecs: list of RoleSpec of roles to add (used only for error messages)
 * memberIds: OIDs of roles to add
 * grantorId: OID that should be recorded as having granted the membership
 * (InvalidOid if not set explicitly)
 * popt: information about grant options
 */
unsafe fn AddRoleMems(currentUserId: Oid, rolename: *const c_char, roleid: Oid,
                      memberSpecs: *mut List, memberIds: *mut List,
                      mut grantorId: Oid, popt: *mut GrantRoleOptions) {
    let pg_authmem_rel: Relation;
    let pg_authmem_dsc: TupleDesc;

    Assert!(list_length(memberSpecs) == list_length(memberIds));

    /* Validate grantor (and resolve implicit grantor if not specified). */
    grantorId = check_role_grantor(currentUserId, roleid, grantorId, true);

    pg_authmem_rel = table_open(AuthMemRelationId, RowExclusiveLock);
    pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

    /*
     * Only allow changes to this role by one backend at a time, so that we
     * can check integrity constraints like the lack of circular ADMIN OPTION
     * grants without fear of race conditions.
     */
    LockSharedObject(AuthIdRelationId, roleid, 0, ShareUpdateExclusiveLock);

    /* Preliminary sanity checks. */
    forboth!(specitem, memberSpecs, iditem, memberIds, {
        let memberRole: *mut RoleSpec = lfirst_node!(RoleSpec, T_RoleSpec, specitem);
        let memberid: Oid = lfirst_oid(iditem);

        /*
         * pg_database_owner is never a role member.  Lifting this restriction
         * would require a policy decision about membership loops.  One could
         * prevent loops, which would include making "ALTER DATABASE x OWNER
         * TO proposed_datdba" fail if is_member_of_role(pg_database_owner,
         * proposed_datdba).  Hence, gaining a membership could reduce what a
         * role could do.  Alternately, one could allow these memberships to
         * complete loops.  A role could then have actual WITH ADMIN OPTION on
         * itself, prompting a decision about is_admin_of_role() treatment of
         * the case.
         *
         * Lifting this restriction also has policy implications for ownership
         * of shared objects (databases and tablespaces).  We allow such
         * ownership, but we might find cause to ban it in the future.
         * Designing such a ban would more troublesome if the design had to
         * address pg_database_owner being a member of role FOO that owns a
         * shared object.  (The effect of such ownership is that any owner of
         * another database can act as the owner of affected shared objects.)
         */
        if memberid == ROLE_PG_DATABASE_OWNER {
            ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */ errmsg!("role \"{}\" cannot be a member of any role", unsafe { core::ffi::CStr::from_ptr(get_rolespec_name(memberRole)) }.to_string_lossy()));
        }

        /*
         * Refuse creation of membership loops, including the trivial case
         * where a role is made a member of itself.  We do this by checking to
         * see if the target role is already a member of the proposed member
         * role.  We have to ignore possible superuserness, however, else we
         * could never grant membership in a superuser-privileged role.
         */
        if is_member_of_role_nosuper(roleid, memberid) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_GRANT_OPERATION) */ errmsg!("role \"{}\" is a member of role \"{}\"", unsafe { core::ffi::CStr::from_ptr(rolename) }.to_string_lossy(), unsafe { core::ffi::CStr::from_ptr(get_rolespec_name(memberRole)) }.to_string_lossy()));
        }
    });

    /*
     * Disallow attempts to grant ADMIN OPTION back to a user who granted it
     * to you, similar to what check_circularity does for ACLs. We want the
     * chains of grants to remain acyclic, so that it's always possible to use
     * REVOKE .. CASCADE to clean up all grants that depend on the one being
     * revoked.
     *
     * NB: This check might look redundant with the check for membership loops
     * above, but it isn't. That's checking for role-member loop (e.g. A is a
     * member of B and B is a member of A) while this is checking for a
     * member-grantor loop (e.g. A gave ADMIN OPTION on X to B and now B, who
     * has no other source of ADMIN OPTION on X, tries to give ADMIN OPTION on
     * X back to A).
     */
    if (*popt).admin && grantorId != BOOTSTRAP_SUPERUSERID {
        let memlist: CatCList;
        let actions: *mut RevokeRoleGrantAction;
        let mut i: c_int;

        /* Get the list of members for this role. */
        memlist = SearchSysCacheList1(AUTHMEMROLEMEM, ObjectIdGetDatum(roleid));

        /*
         * Figure out what would happen if we removed all existing grants to
         * every role to which we've been asked to make a new grant.
         */
        actions = initialize_revoke_actions(memlist);
        foreach!(iditem, memberIds, {
            let memberid: Oid = lfirst_oid(crate::current_cell!(iditem));

            if memberid == BOOTSTRAP_SUPERUSERID {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_GRANT_OPERATION) */ errmsg!("{} option cannot be granted back to your own grantor", unsafe { core::ffi::CStr::from_ptr(c"ADMIN".as_ptr()) }.to_string_lossy()));
            }
            plan_member_revoke(memlist, actions, memberid);
        });

        /*
         * If the result would be that the grantor role would no longer have
         * the ability to perform the grant, then the proposed grant would
         * create a circularity.
         */
        i = 0;
        while i < pg_authid_members_n_members(memlist) {
            let authmem_tuple: HeapTuple;
            let authmem_form: Form_pg_auth_members;

            authmem_tuple = catclist_member_tuple(memlist, i);
            authmem_form = GETSTRUCT(authmem_tuple) as Form_pg_auth_members;

            if *actions.add(i as usize) == RRG_NOOP
                && (*(authmem_form as *mut FormData_pg_auth_members_real)).member == grantorId
                && (*(authmem_form as *mut FormData_pg_auth_members_real)).admin_option
            {
                break;
            }
            i += 1;
        }
        if i >= pg_authid_members_n_members(memlist) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INVALID_GRANT_OPERATION) */ errmsg!("{} option cannot be granted back to your own grantor", unsafe { core::ffi::CStr::from_ptr(c"ADMIN".as_ptr()) }.to_string_lossy()));
        }

        ReleaseSysCacheList(memlist);
    }

    /* Now perform the catalog updates. */
    forboth!(specitem, memberSpecs, iditem, memberIds, {
        let memberRole: *mut RoleSpec = lfirst_node!(RoleSpec, T_RoleSpec, specitem);
        let memberid: Oid = lfirst_oid(iditem);
        let authmem_tuple: HeapTuple;
        let tuple: HeapTuple;
        let mut new_record: [Datum; 16] = [0usize; 16];
        let mut new_record_nulls: [bool; 16] = [false; 16];
        let mut new_record_repl: [bool; 16] = [false; 16];

        /* Common initialization for possible insert or update */
        new_record[Anum_pg_auth_members_roleid as usize - 1] = ObjectIdGetDatum(roleid);
        new_record[Anum_pg_auth_members_member as usize - 1] = ObjectIdGetDatum(memberid);
        new_record[Anum_pg_auth_members_grantor as usize - 1] = ObjectIdGetDatum(grantorId);

        /* Find any existing tuple */
        authmem_tuple = SearchSysCache3(AUTHMEMROLEMEM,
            ObjectIdGetDatum(roleid),
            ObjectIdGetDatum(memberid),
            ObjectIdGetDatum(grantorId));

        /*
         * If we found a tuple, update it with new option values, unless there
         * are no changes, in which case issue a WARNING.
         *
         * If we didn't find a tuple, just insert one.
         */
        if HeapTupleIsValid(authmem_tuple) {
            let authmem_form: Form_pg_auth_members;
            let mut at_least_one_change: bool = false;

            authmem_form = GETSTRUCT(authmem_tuple) as Form_pg_auth_members;

            if ((*popt).specified & GRANT_ROLE_SPECIFIED_ADMIN) != 0
                && (*(authmem_form as *mut FormData_pg_auth_members_real)).admin_option != (*popt).admin
            {
                new_record[Anum_pg_auth_members_admin_option as usize - 1] = BoolGetDatum((*popt).admin);
                new_record_repl[Anum_pg_auth_members_admin_option as usize - 1] = true;
                at_least_one_change = true;
            }

            if ((*popt).specified & GRANT_ROLE_SPECIFIED_INHERIT) != 0
                && (*(authmem_form as *mut FormData_pg_auth_members_real)).inherit_option != (*popt).inherit
            {
                new_record[Anum_pg_auth_members_inherit_option as usize - 1] = BoolGetDatum((*popt).inherit);
                new_record_repl[Anum_pg_auth_members_inherit_option as usize - 1] = true;
                at_least_one_change = true;
            }

            if ((*popt).specified & GRANT_ROLE_SPECIFIED_SET) != 0
                && (*(authmem_form as *mut FormData_pg_auth_members_real)).set_option != (*popt).set
            {
                new_record[Anum_pg_auth_members_set_option as usize - 1] = BoolGetDatum((*popt).set);
                new_record_repl[Anum_pg_auth_members_set_option as usize - 1] = true;
                at_least_one_change = true;
            }

            if !at_least_one_change {
                ereport!(NOTICE, errmsg!("role \"{}\" has already been granted membership in role \"{}\" by role \"{}\"", unsafe { core::ffi::CStr::from_ptr(get_rolespec_name(memberRole)) }.to_string_lossy(), unsafe { core::ffi::CStr::from_ptr(rolename) }.to_string_lossy(), unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(grantorId, false)) }.to_string_lossy()));
                ReleaseSysCache(authmem_tuple);
                __pg_multifor_state.i += 1;
                continue;
            }

            tuple = heap_modify_tuple(authmem_tuple, pg_authmem_dsc,
                new_record.as_mut_ptr(),
                new_record_nulls.as_mut_ptr(), new_record_repl.as_mut_ptr());
            CatalogTupleUpdate(pg_authmem_rel, &mut (*tuple).t_self, tuple);

            ReleaseSysCache(authmem_tuple);
        } else {
            let objectId: Oid;
            let newmembers: *mut Oid = palloc(core::mem::size_of::<Oid>()) as *mut Oid;

            /*
             * The values for these options can be taken directly from 'popt'.
             * Either they were specified, or the defaults as set by
             * InitGrantRoleOptions are correct.
             */
            new_record[Anum_pg_auth_members_admin_option as usize - 1] = BoolGetDatum((*popt).admin);
            new_record[Anum_pg_auth_members_set_option as usize - 1] = BoolGetDatum((*popt).set);

            /*
             * If the user specified a value for the inherit option, use
             * whatever was specified. Otherwise, set the default value based
             * on the role-level property.
             */
            if ((*popt).specified & GRANT_ROLE_SPECIFIED_INHERIT) != 0 {
                new_record[Anum_pg_auth_members_inherit_option as usize - 1] = (*popt).inherit as Datum;
            } else {
                let mrtup: HeapTuple;
                let mrform: Form_pg_authid;

                mrtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(memberid));
                if !HeapTupleIsValid(mrtup) {
                    elog!(ERROR, "cache lookup failed for role {}", memberid);
                }
                mrform = GETSTRUCT(mrtup) as Form_pg_authid;
                new_record[Anum_pg_auth_members_inherit_option as usize - 1] =
                    (*(mrform as *mut FormData_pg_authid_real)).rolinherit as Datum;
                ReleaseSysCache(mrtup);
            }

            /* get an OID for the new row and insert it */
            objectId = GetNewOidWithIndex(pg_authmem_rel, AuthMemOidIndexId,
                Anum_pg_auth_members_oid);
            new_record[Anum_pg_auth_members_oid as usize - 1] = ObjectIdGetDatum(objectId);
            tuple = heap_form_tuple(pg_authmem_dsc,
                new_record.as_mut_ptr(), new_record_nulls.as_mut_ptr());
            CatalogTupleInsert(pg_authmem_rel, tuple);

            /* updateAclDependencies wants to pfree array inputs */
            *newmembers.add(0) = grantorId;
            updateAclDependencies(AuthMemRelationId, objectId,
                0, InvalidOid,
                0, ptr::null_mut(),
                1, newmembers);
        }

        /* CCI after each change, in case there are duplicates in list */
        CommandCounterIncrement();
    });

    /*
     * Close pg_authmem, but keep lock till commit.
     */
    table_close(pg_authmem_rel, NoLock);
}

/*
 * DelRoleMems -- Remove given members from the specified role
 *
 * rolename: name of role to del from (used only for error messages)
 * roleid: OID of role to del from
 * memberSpecs: list of RoleSpec of roles to del (used only for error messages)
 * memberIds: OIDs of roles to del
 * grantorId: who is revoking the membership
 * popt: information about grant options
 * behavior: RESTRICT or CASCADE behavior for recursive removal
 */
unsafe fn DelRoleMems(currentUserId: Oid, rolename: *const c_char, roleid: Oid,
                      memberSpecs: *mut List, memberIds: *mut List,
                      mut grantorId: Oid, popt: *mut GrantRoleOptions,
                      behavior: DropBehavior) {
    let pg_authmem_rel: Relation;
    let pg_authmem_dsc: TupleDesc;
    let memlist: CatCList;
    let actions: *mut RevokeRoleGrantAction;
    let mut i: c_int;

    Assert!(list_length(memberSpecs) == list_length(memberIds));

    /* Validate grantor (and resolve implicit grantor if not specified). */
    grantorId = check_role_grantor(currentUserId, roleid, grantorId, false);

    pg_authmem_rel = table_open(AuthMemRelationId, RowExclusiveLock);
    pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

    /*
     * Only allow changes to this role by one backend at a time, so that we
     * can check for things like dependent privileges without fear of race
     * conditions.
     */
    LockSharedObject(AuthIdRelationId, roleid, 0, ShareUpdateExclusiveLock);

    memlist = SearchSysCacheList1(AUTHMEMROLEMEM, ObjectIdGetDatum(roleid));
    actions = initialize_revoke_actions(memlist);

    /*
     * We may need to recurse to dependent privileges if DROP_CASCADE was
     * specified, or refuse to perform the operation if dependent privileges
     * exist and DROP_RESTRICT was specified. plan_single_revoke() will figure
     * out what to do with each catalog tuple.
     */
    forboth!(specitem, memberSpecs, iditem, memberIds, {
        let memberRole: *mut RoleSpec = lfirst(specitem) as *mut RoleSpec;
        let memberid: Oid = lfirst_oid(iditem);

        if !plan_single_revoke(memlist, actions, memberid, grantorId, popt, behavior) {
            ereport!(WARNING, errmsg!("role \"{}\" has not been granted membership in role \"{}\" by role \"{}\"", unsafe { core::ffi::CStr::from_ptr(get_rolespec_name(memberRole)) }.to_string_lossy(), unsafe { core::ffi::CStr::from_ptr(rolename) }.to_string_lossy(), unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(grantorId, false)) }.to_string_lossy()));
            __pg_multifor_state.i += 1;
            continue;
        }
    });

    /*
     * We now know what to do with each catalog tuple: it should either be
     * left alone, deleted, or just have the admin_option flag cleared.
     * Perform the appropriate action in each case.
     */
    i = 0;
    while i < pg_authid_members_n_members(memlist) {
        let authmem_tuple: HeapTuple;
        let authmem_form: Form_pg_auth_members;

        if *actions.add(i as usize) == RRG_NOOP {
            i += 1;
            continue;
        }

        authmem_tuple = catclist_member_tuple(memlist, i);
        authmem_form = GETSTRUCT(authmem_tuple) as Form_pg_auth_members;

        if *actions.add(i as usize) == RRG_DELETE_GRANT {
            /*
             * Remove the entry altogether, after first removing its
             * dependencies
             */
            deleteSharedDependencyRecordsFor(AuthMemRelationId,
                (*(authmem_form as *mut FormData_pg_auth_members_real)).oid, 0);
            CatalogTupleDelete(pg_authmem_rel, &mut (*authmem_tuple).t_self);
        } else {
            /* Just turn off the specified option */
            let tuple: HeapTuple;
            let mut new_record: [Datum; 16] = [0usize; 16];
            let mut new_record_nulls: [bool; 16] = [false; 16];
            let mut new_record_repl: [bool; 16] = [false; 16];

            /* Build a tuple to update with */
            if *actions.add(i as usize) == RRG_REMOVE_ADMIN_OPTION {
                new_record[Anum_pg_auth_members_admin_option as usize - 1] = BoolGetDatum(false);
                new_record_repl[Anum_pg_auth_members_admin_option as usize - 1] = true;
            } else if *actions.add(i as usize) == RRG_REMOVE_INHERIT_OPTION {
                new_record[Anum_pg_auth_members_inherit_option as usize - 1] = BoolGetDatum(false);
                new_record_repl[Anum_pg_auth_members_inherit_option as usize - 1] = true;
            } else if *actions.add(i as usize) == RRG_REMOVE_SET_OPTION {
                new_record[Anum_pg_auth_members_set_option as usize - 1] = BoolGetDatum(false);
                new_record_repl[Anum_pg_auth_members_set_option as usize - 1] = true;
            } else {
                elog!(ERROR, "unknown role revoke action");
            }

            tuple = heap_modify_tuple(authmem_tuple, pg_authmem_dsc,
                new_record.as_mut_ptr(),
                new_record_nulls.as_mut_ptr(), new_record_repl.as_mut_ptr());
            CatalogTupleUpdate(pg_authmem_rel, &mut (*tuple).t_self, tuple);
        }
        i += 1;
    }

    ReleaseSysCacheList(memlist);

    /*
     * Close pg_authmem, but keep lock till commit.
     */
    table_close(pg_authmem_rel, NoLock);
}

/*
 * Check that currentUserId has permission to modify the membership list for
 * roleid. Throw an error if not.
 */
unsafe fn check_role_membership_authorization(currentUserId: Oid, roleid: Oid, is_grant: bool) {
    /*
     * The charter of pg_database_owner is to have exactly one, implicit,
     * situation-dependent member.  There's no technical need for this
     * restriction.  (One could lift it and take the further step of making
     * object_ownercheck(DatabaseRelationId, ...) equivalent to
     * has_privs_of_role(roleid, ROLE_PG_DATABASE_OWNER), in which case
     * explicit, situation-independent members could act as the owner of any
     * database.)
     */
    if is_grant && roleid == ROLE_PG_DATABASE_OWNER {
        ereport!(ERROR, /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */ errmsg!("role \"{}\" cannot have explicit members", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(roleid, false)) }.to_string_lossy()));
    }

    /* To mess with a superuser role, you gotta be superuser. */
    if superuser_arg(roleid) {
        if !superuser_arg(currentUserId) {
            if is_grant {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may grant roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to grant role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(roleid, false)) }.to_string_lossy()));
            } else {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s attribute may revoke roles with the %s attribute.".as_ptr(), c"SUPERUSER".as_ptr(), c"SUPERUSER".as_ptr()) */ errmsg!("permission denied to revoke role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(roleid, false)) }.to_string_lossy()));
            }
        }
    } else {
        /*
         * Otherwise, must have admin option on the role to be changed.
         */
        if !is_admin_of_role(currentUserId, roleid) {
            if is_grant {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s option on role \"%s\" may grant this role.".as_ptr(), c"ADMIN".as_ptr(), GetUserNameFromId(roleid, false)) */ errmsg!("permission denied to grant role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(roleid, false)) }.to_string_lossy()));
            } else {
                ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with the %s option on role \"%s\" may revoke this role.".as_ptr(), c"ADMIN".as_ptr(), GetUserNameFromId(roleid, false)) */ errmsg!("permission denied to revoke role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(roleid, false)) }.to_string_lossy()));
            }
        }
    }
}

/*
 * Sanity-check, or infer, the grantor for a GRANT or REVOKE statement
 * targeting a role.
 *
 * The grantor must always be either a role with ADMIN OPTION on the role in
 * which membership is being granted, or the bootstrap superuser. This is
 * similar to the restriction enforced by select_best_grantor, except that
 * roles don't have owners, so we regard the bootstrap superuser as the
 * implicit owner.
 *
 * If the grantor was not explicitly specified by the user, grantorId should
 * be passed as InvalidOid, and this function will infer the user to be
 * recorded as the grantor. In many cases, this will be the current user, but
 * things get more complicated when the current user doesn't possess ADMIN
 * OPTION on the role but rather relies on having SUPERUSER privileges, or
 * on inheriting the privileges of a role which does have ADMIN OPTION. See
 * below for details.
 *
 * If the grantor was specified by the user, then it must be a user that
 * can legally be recorded as the grantor, as per the rule stated above.
 * This is an integrity constraint, not a permissions check, and thus even
 * superusers are subject to this restriction. However, there is also a
 * permissions check: to specify a role as the grantor, the current user
 * must possess the privileges of that role. Superusers will always pass
 * this check, but for non-superusers it may lead to an error.
 *
 * The return value is the OID to be regarded as the grantor when executing
 * the operation.
 */
unsafe fn check_role_grantor(currentUserId: Oid, roleid: Oid, mut grantorId: Oid, is_grant: bool) -> Oid {
    /* If the grantor ID was not specified, pick one to use. */
    if !OidIsValid(grantorId) {
        /*
         * Grants where the grantor is recorded as the bootstrap superuser do
         * not depend on any other existing grants, so always default to this
         * interpretation when possible.
         */
        if superuser_arg(currentUserId) {
            return BOOTSTRAP_SUPERUSERID;
        }

        /*
         * Otherwise, the grantor must either have ADMIN OPTION on the role or
         * inherit the privileges of a role which does. In the former case,
         * record the grantor as the current user; in the latter, pick one of
         * the roles that is "most directly" inherited by the current role
         * (i.e. fewest "hops").
         *
         * (We shouldn't fail to find a best grantor, because we've already
         * established that the current user has permission to perform the
         * operation.)
         */
        grantorId = select_best_admin(currentUserId, roleid);
        if !OidIsValid(grantorId) {
            elog!(ERROR, "no possible grantors");
        }
        return grantorId;
    }

    /*
     * If an explicit grantor is specified, it must be a role whose privileges
     * the current user possesses.
     *
     * It should also be a role that has ADMIN OPTION on the target role, but
     * we check this condition only in case of GRANT. For REVOKE, no matching
     * grant should exist anyway, but if it somehow does, let the user get rid
     * of it.
     */
    if is_grant {
        if !has_privs_of_role(currentUserId, grantorId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with privileges of role \"%s\" may grant privileges as this role.".as_ptr(), GetUserNameFromId(grantorId, false)) */ errmsg!("permission denied to grant privileges as role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(grantorId, false)) }.to_string_lossy()));
        }

        if grantorId != BOOTSTRAP_SUPERUSERID
            && select_best_admin(grantorId, roleid) != grantorId
        {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"The grantor must have the %s option on role \"%s\".".as_ptr(), c"ADMIN".as_ptr(), GetUserNameFromId(roleid, false)) */ errmsg!("permission denied to grant privileges as role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(grantorId, false)) }.to_string_lossy()));
        }
    } else {
        if !has_privs_of_role(currentUserId, grantorId) {
            ereport!(ERROR, /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail(c"Only roles with privileges of role \"%s\" may revoke privileges granted by this role.".as_ptr(), GetUserNameFromId(grantorId, false)) */ errmsg!("permission denied to revoke privileges granted by role \"{}\"", unsafe { core::ffi::CStr::from_ptr(GetUserNameFromId(grantorId, false)) }.to_string_lossy()));
        }
    }

    /*
     * If a grantor was specified explicitly, always attribute the grant to
     * that role (unless we error out above).
     */
    return grantorId;
}

/*
 * Initialize an array of RevokeRoleGrantAction objects.
 *
 * 'memlist' should be a list of all grants for the target role.
 *
 * This constructs an array indicating that no actions are to be performed;
 * that is, every element is initially RRG_NOOP.
 */
unsafe fn initialize_revoke_actions(memlist: CatCList) -> *mut RevokeRoleGrantAction {
    let result: *mut RevokeRoleGrantAction;
    let mut i: c_int;

    if pg_authid_members_n_members(memlist) == 0 {
        return ptr::null_mut();
    }

    result = palloc(core::mem::size_of::<RevokeRoleGrantAction>() * pg_authid_members_n_members(memlist) as usize)
        as *mut RevokeRoleGrantAction;
    i = 0;
    while i < pg_authid_members_n_members(memlist) {
        *result.add(i as usize) = RRG_NOOP;
        i += 1;
    }
    return result;
}

/*
 * Figure out what we would need to do in order to revoke a grant, or just the
 * admin option on a grant, given that there might be dependent privileges.
 *
 * 'memlist' should be a list of all grants for the target role.
 *
 * Whatever actions prove to be necessary will be signalled by updating
 * 'actions'.
 *
 * If behavior is DROP_RESTRICT, an error will occur if there are dependent
 * role membership grants; if DROP_CASCADE, those grants will be scheduled
 * for deletion.
 *
 * The return value is true if the matching grant was found in the list,
 * and false if not.
 */
unsafe fn plan_single_revoke(memlist: CatCList, actions: *mut RevokeRoleGrantAction,
                             member: Oid, grantor: Oid, popt: *mut GrantRoleOptions,
                             behavior: DropBehavior) -> bool {
    let mut i: c_int;

    /*
     * If popt.specified == 0, we're revoking the grant entirely; otherwise,
     * we expect just one bit to be set, and we're revoking the corresponding
     * option. As of this writing, there's no syntax that would allow for an
     * attempt to revoke multiple options at once, and the logic below
     * wouldn't work properly if such syntax were added, so assert that our
     * caller isn't trying to do that.
     */
    Assert!(pg_popcount32((*popt).specified) <= 1);

    i = 0;
    while i < pg_authid_members_n_members(memlist) {
        let authmem_tuple: HeapTuple;
        let authmem_form: Form_pg_auth_members;

        authmem_tuple = catclist_member_tuple(memlist, i);
        authmem_form = GETSTRUCT(authmem_tuple) as Form_pg_auth_members;

        if (*(authmem_form as *mut FormData_pg_auth_members_real)).member == member
            && (*(authmem_form as *mut FormData_pg_auth_members_real)).grantor == grantor
        {
            if ((*popt).specified & GRANT_ROLE_SPECIFIED_INHERIT) != 0 {
                /*
                 * Revoking the INHERIT option doesn't change anything for
                 * dependent privileges, so we don't need to recurse.
                 */
                *actions.add(i as usize) = RRG_REMOVE_INHERIT_OPTION;
            } else if ((*popt).specified & GRANT_ROLE_SPECIFIED_SET) != 0 {
                /* Here too, no need to recurse. */
                *actions.add(i as usize) = RRG_REMOVE_SET_OPTION;
            } else {
                let revoke_admin_option_only: bool;

                /*
                 * Revoking the grant entirely, or ADMIN option on a grant,
                 * implicates dependent privileges, so we may need to recurse.
                 */
                revoke_admin_option_only =
                    ((*popt).specified & GRANT_ROLE_SPECIFIED_ADMIN) != 0;
                plan_recursive_revoke(memlist, actions, i, revoke_admin_option_only, behavior);
            }
            return true;
        }
        i += 1;
    }

    return false;
}

/*
 * Figure out what we would need to do in order to revoke all grants to
 * a given member, given that there might be dependent privileges.
 *
 * 'memlist' should be a list of all grants for the target role.
 *
 * Whatever actions prove to be necessary will be signalled by updating
 * 'actions'.
 */
unsafe fn plan_member_revoke(memlist: CatCList, actions: *mut RevokeRoleGrantAction, member: Oid) {
    let mut i: c_int;

    i = 0;
    while i < pg_authid_members_n_members(memlist) {
        let authmem_tuple: HeapTuple;
        let authmem_form: Form_pg_auth_members;

        authmem_tuple = catclist_member_tuple(memlist, i);
        authmem_form = GETSTRUCT(authmem_tuple) as Form_pg_auth_members;

        if (*(authmem_form as *mut FormData_pg_auth_members_real)).member == member {
            plan_recursive_revoke(memlist, actions, i, false, DropBehavior::DROP_CASCADE);
        }
        i += 1;
    }
}

/*
 * Workhorse for figuring out recursive revocation of role grants.
 *
 * This is similar to what recursive_revoke() does for ACLs.
 */
unsafe fn plan_recursive_revoke(memlist: CatCList, actions: *mut RevokeRoleGrantAction,
                                index: c_int, revoke_admin_option_only: bool,
                                behavior: DropBehavior) {
    let mut would_still_have_admin_option: bool = false;
    let authmem_tuple: HeapTuple;
    let authmem_form: Form_pg_auth_members;
    let mut i: c_int;

    /* If it's already been done, we can just return. */
    if *actions.add(index as usize) == RRG_DELETE_GRANT {
        return;
    }
    if *actions.add(index as usize) == RRG_REMOVE_ADMIN_OPTION && revoke_admin_option_only {
        return;
    }

    /* Locate tuple data. */
    authmem_tuple = catclist_member_tuple(memlist, index);
    authmem_form = GETSTRUCT(authmem_tuple) as Form_pg_auth_members;

    /*
     * If the existing tuple does not have admin_option set, then we do not
     * need to recurse. If we're just supposed to clear that bit we don't need
     * to do anything at all; if we're supposed to remove the grant, we need
     * to do something, but only to the tuple, and not any others.
     */
    if !revoke_admin_option_only {
        *actions.add(index as usize) = RRG_DELETE_GRANT;
        if !(*(authmem_form as *mut FormData_pg_auth_members_real)).admin_option {
            return;
        }
    } else {
        if !(*(authmem_form as *mut FormData_pg_auth_members_real)).admin_option {
            return;
        }
        *actions.add(index as usize) = RRG_REMOVE_ADMIN_OPTION;
    }

    /* Determine whether the member would still have ADMIN OPTION. */
    i = 0;
    while i < pg_authid_members_n_members(memlist) {
        let am_cascade_tuple: HeapTuple;
        let am_cascade_form: Form_pg_auth_members;

        am_cascade_tuple = catclist_member_tuple(memlist, i);
        am_cascade_form = GETSTRUCT(am_cascade_tuple) as Form_pg_auth_members;

        if (*(am_cascade_form as *mut FormData_pg_auth_members_real)).member
            == (*(authmem_form as *mut FormData_pg_auth_members_real)).member
            && (*(am_cascade_form as *mut FormData_pg_auth_members_real)).admin_option
            && *actions.add(i as usize) == RRG_NOOP
        {
            would_still_have_admin_option = true;
            break;
        }
        i += 1;
    }

    /* If the member would still have ADMIN OPTION, we need not recurse. */
    if would_still_have_admin_option {
        return;
    }

    /*
     * Recurse to grants that are not yet slated for deletion which have this
     * member as the grantor.
     */
    i = 0;
    while i < pg_authid_members_n_members(memlist) {
        let am_cascade_tuple: HeapTuple;
        let am_cascade_form: Form_pg_auth_members;

        am_cascade_tuple = catclist_member_tuple(memlist, i);
        am_cascade_form = GETSTRUCT(am_cascade_tuple) as Form_pg_auth_members;

        if (*(am_cascade_form as *mut FormData_pg_auth_members_real)).grantor
            == (*(authmem_form as *mut FormData_pg_auth_members_real)).member
            && *actions.add(i as usize) != RRG_DELETE_GRANT
        {
            if behavior == DropBehavior::DROP_RESTRICT {
                ereport!(ERROR, /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST); errhint(c"Use CASCADE to revoke them too.".as_ptr()) */ errmsg!("dependent privileges exist"));
            }

            plan_recursive_revoke(memlist, actions, i, false, behavior);
        }
        i += 1;
    }
}

/*
 * Initialize a GrantRoleOptions object with default values.
 */
unsafe fn InitGrantRoleOptions(popt: *mut GrantRoleOptions) {
    (*popt).specified = 0;
    (*popt).admin = false;
    (*popt).inherit = false;
    (*popt).set = true;
}

/*
 * GUC check_hook for createrole_self_grant
 */
#[no_mangle]
pub unsafe extern "C" fn check_createrole_self_grant(newval: *mut *mut c_char, extra: *mut *mut c_void, source: GucSource) -> bool {
    let rawstring: *mut c_char;
    let mut elemlist: *mut List = ptr::null_mut();
    let mut options: u32 = 0;
    let result: *mut u32;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);

    if !SplitIdentifierString(rawstring, b',' as c_char, &mut elemlist) {
        /* syntax error in list */
        GUC_check_errdetail(c"List syntax is invalid.".as_ptr());
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    foreach!(l, elemlist, {
        let tok: *mut c_char = lfirst(crate::current_cell!(l)) as *mut c_char;

        if pg_strcasecmp(tok, c"SET".as_ptr()) == 0 {
            options |= GRANT_ROLE_SPECIFIED_SET;
        } else if pg_strcasecmp(tok, c"INHERIT".as_ptr()) == 0 {
            options |= GRANT_ROLE_SPECIFIED_INHERIT;
        } else {
            GUC_check_errdetail(c"Unrecognized key word: \"%s\".".as_ptr(), tok);
            pfree(rawstring as *mut c_void);
            list_free(elemlist);
            return false;
        }
    });

    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    result = guc_malloc(LOG, core::mem::size_of::<u32>()) as *mut u32;
    if result.is_null() {
        return false;
    }
    *result = options;
    *extra = result as *mut c_void;

    return true;
}

/*
 * GUC assign_hook for createrole_self_grant
 */
#[no_mangle]
pub unsafe extern "C" fn assign_createrole_self_grant(newval: *const c_char, extra: *mut c_void) {
    let options: u32 = *(extra as *mut u32);

    createrole_self_grant_enabled = options != 0;
    createrole_self_grant_options.specified = GRANT_ROLE_SPECIFIED_ADMIN
        | GRANT_ROLE_SPECIFIED_INHERIT
        | GRANT_ROLE_SPECIFIED_SET;
    createrole_self_grant_options.admin = false;
    createrole_self_grant_options.inherit =
        (options & GRANT_ROLE_SPECIFIED_INHERIT) != 0;
    createrole_self_grant_options.set =
        (options & GRANT_ROLE_SPECIFIED_SET) != 0;
}

/*
 * RemoveRoleById - guts of DROP ROLE
 *
 * NB: RemoveRoleById is not defined in user.c; this is a stub for the
 * dependency.  TODO(pg-port)
 */
unsafe fn RemoveRoleById(roleid: Oid) {
    // TODO(pg-port) RemoveRoleById implementation
}

//! acl.rs
//!   Basic access control list data structures manipulation routines.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/acl.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"                 -> crate::prelude::*
//!   #include <ctype.h>                    -> isalpha/isalnum/isspace via extern "C"
//!   plus a large set of catalog/syscache/lsyscache/commands headers.
//!
//! The Acl/AclItem manipulation core (allocacl, aclcopy, aclconcat, aclmerge,
//! aclupdate, aclnewowner, aclmask, aclmembers, parse/out, the SQL-callable
//! has_*_privilege families, role membership engine, get_role_oid, etc.) is
//! translated faithfully.  Many leaf catalog/syscache/lsyscache lookups and the
//! aclchk.c-side object_aclcheck/pg_class_aclcheck/pg_attribute_aclcheck helpers
//! are not yet ported; those are provided as minimal local stubs marked
//! `// TODO(pg-port): real SYM lives in <file>`.

#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]
#![allow(unused_variables, unused_assignments, unused_mut, dead_code)]

use crate::prelude::*; // postgres.h: Datum, palloc/palloc0/pfree, elog!/ereport!/errmsg!, Size, etc.
use core::ffi::{c_char, c_int, c_void};
use crate::utils::cache::syscache_ids_gen::{ATTNAME, AUTHMEMMEMROLE, AUTHMEMROLEMEM, AUTHNAME, AUTHOID, DATABASEOID};

use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::{int32, uint32, uint64};
use crate::pg_config_manual::NAMEDATALEN;
use crate::varatt::SET_VARSIZE;
use crate::nodes::nodes::Node;

// utils/acl.h types + AclMode/ACL_* (the latter live in nodes/parsenodes.h).
use crate::nodes::parsenodes::{
    AclMode, ObjectType, RoleSpec, RoleSpecType, DropBehavior,
    ACL_ALTER_SYSTEM, ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE, ACL_EXECUTE,
    ACL_INSERT, ACL_MAINTAIN, ACL_NO_RIGHTS, ACL_REFERENCES, ACL_SELECT, ACL_SET, ACL_TRIGGER,
    ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use crate::nodes::parsenodes::ObjectType::*;
use crate::nodes::parsenodes::RoleSpecType::*;
use crate::nodes::parsenodes::DropBehavior::*;

// utils/array.h: ArrayType (== Acl) layout + ARR_* macros.
use crate::utils::array::{
    ArrayType, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_LBOUND, ARR_NDIM, ARR_OVERHEAD_NONULLS,
    ARR_SIZE,
};

// nodes/pg_list.h
use crate::nodes::pg_list::*;
use crate::{foreach, foreach_oid, current_cell, list_make1_oid};
// fmgr DirectFunctionCall1! (utils/fmgr.h)
use crate::DirectFunctionCall1;
// catalog/pg_attribute.h
use crate::catalog::pg_attribute::Form_pg_attribute;
// catalog/pg_type_d.h: OIDOID / TEXTOID / BOOLOID
use crate::catalog::pg_type_d::{BOOLOID, OIDOID, TEXTOID};

use crate::utils::adt::oid::oid_cmp; // oid_cmp comparator (utils/adt/oid.c)

// access/htup_details.h: HeapTuple + HeapTupleIsValid + GETSTRUCT.
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};
// catalog/pg_authid.h: Form_pg_authid + rolname.
use crate::catalog::pg_authid::Form_pg_authid;
// common/hashfn.h: hash_uint32_extended.
use crate::common::hashfn::hash_uint32_extended;
// utils/cache/syscache.h: the core lookups that ARE ported.
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1, SearchSysCache2};
// lib/qunique.h
use crate::lib::qunique::qunique;
// access/attnum.h
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
// utils/snapshot.h
use crate::utils::snapshot::Snapshot;

// fmgr.h: FunctionCallInfo + PG_GETARG_*!/PG_RETURN_*! (the macros are
// #[macro_export], hence imported by name from the crate root).
use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CHAR, PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_INT16,
    PG_GETARG_INT64, PG_GETARG_NAME, PG_GETARG_OID, PG_GETARG_TEXT_PP, PG_RETURN_BOOL,
    PG_RETURN_CSTRING, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_UINT32, PG_DETOAST_DATUM,
};

// fmgr DatumGet* helpers for the Acl / AclItem fmgr macros (utils/acl.h).
#[inline]
unsafe fn DatumGetAclItemP(x: Datum) -> *mut AclItem {
    DatumGetPointer(x) as *mut AclItem
}
#[inline]
unsafe fn DatumGetAclP(x: Datum) -> *mut Acl {
    PG_DETOAST_DATUM!(x) as *mut Acl
}

// ----------------------------------------------------------------------------
// Definitions from utils/acl.h (no Rust home yet -> defined here from acl.h).
// TODO(pg-port): these belong in crate::utils::acl once that header is ported.
// ----------------------------------------------------------------------------

/// Acl == one-dimensional array of AclItem.
pub type Acl = ArrayType;

/// AclItem (utils/acl.h).  Must be the same size on all platforms.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AclItem {
    pub ai_grantee: Oid, /* ID that this item grants privs to */
    pub ai_grantor: Oid, /* grantor of privs */
    pub ai_privs: AclMode, /* privilege bits */
}

/* placeholder for id in a PUBLIC acl item */
pub const ACL_ID_PUBLIC: Oid = 0;

#[inline]
pub fn ACLITEM_GET_PRIVS(item: AclItem) -> AclMode {
    item.ai_privs & 0xFFFFFFFF
}
#[inline]
pub fn ACLITEM_GET_GOPTIONS(item: AclItem) -> AclMode {
    (item.ai_privs >> 32) & 0xFFFFFFFF
}
#[inline]
pub fn ACLITEM_GET_RIGHTS(item: AclItem) -> AclMode {
    item.ai_privs
}

#[inline]
pub fn ACL_GRANT_OPTION_FOR(privs: AclMode) -> AclMode {
    ((privs as AclMode) & 0xFFFFFFFF) << 32
}
#[inline]
pub fn ACL_OPTION_TO_PRIVS(privs: AclMode) -> AclMode {
    ((privs as AclMode) >> 32) & 0xFFFFFFFF
}

#[inline]
pub fn ACLITEM_SET_PRIVS(item: &mut AclItem, privs: AclMode) {
    item.ai_privs = (item.ai_privs & !(0xFFFFFFFF as AclMode)) | ((privs as AclMode) & 0xFFFFFFFF);
}
#[inline]
pub fn ACLITEM_SET_GOPTIONS(item: &mut AclItem, goptions: AclMode) {
    item.ai_privs = (item.ai_privs & !((0xFFFFFFFF as AclMode) << 32))
        | (((goptions as AclMode) & 0xFFFFFFFF) << 32);
}
#[inline]
pub fn ACLITEM_SET_RIGHTS(item: &mut AclItem, rights: AclMode) {
    item.ai_privs = rights as AclMode;
}
#[inline]
pub fn ACLITEM_SET_PRIVS_GOPTIONS(item: &mut AclItem, privs: AclMode, goptions: AclMode) {
    item.ai_privs = ((privs as AclMode) & 0xFFFFFFFF) | (((goptions as AclMode) & 0xFFFFFFFF) << 32);
}

pub const ACLITEM_ALL_PRIV_BITS: AclMode = 0xFFFFFFFF;
pub const ACLITEM_ALL_GOPTION_BITS: AclMode = (0xFFFFFFFF as AclMode) << 32;

#[inline]
pub unsafe fn ACL_NUM(acl: *const Acl) -> c_int {
    *ARR_DIMS(acl)
}
#[inline]
pub unsafe fn ACL_DAT(acl: *const Acl) -> *mut AclItem {
    crate::utils::array::ARR_DATA_PTR(acl) as *mut AclItem
}
#[inline]
pub fn ACL_N_SIZE(n: c_int) -> usize {
    ARR_OVERHEAD_NONULLS(1) + (n as usize) * core::mem::size_of::<AclItem>()
}
#[inline]
pub unsafe fn ACL_SIZE(acl: *const Acl) -> u32 {
    ARR_SIZE(acl)
}

/* ACL modification opcodes for aclupdate */
pub const ACL_MODECHG_ADD: c_int = 1;
pub const ACL_MODECHG_DEL: c_int = 2;
pub const ACL_MODECHG_EQL: c_int = 3;

/* External representations of the privilege bits */
pub const ACL_INSERT_CHR: c_char = b'a' as c_char;
pub const ACL_SELECT_CHR: c_char = b'r' as c_char;
pub const ACL_UPDATE_CHR: c_char = b'w' as c_char;
pub const ACL_DELETE_CHR: c_char = b'd' as c_char;
pub const ACL_TRUNCATE_CHR: c_char = b'D' as c_char;
pub const ACL_REFERENCES_CHR: c_char = b'x' as c_char;
pub const ACL_TRIGGER_CHR: c_char = b't' as c_char;
pub const ACL_EXECUTE_CHR: c_char = b'X' as c_char;
pub const ACL_USAGE_CHR: c_char = b'U' as c_char;
pub const ACL_CREATE_CHR: c_char = b'C' as c_char;
pub const ACL_CREATE_TEMP_CHR: c_char = b'T' as c_char;
pub const ACL_CONNECT_CHR: c_char = b'c' as c_char;
pub const ACL_SET_CHR: c_char = b's' as c_char;
pub const ACL_ALTER_SYSTEM_CHR: c_char = b'A' as c_char;
pub const ACL_MAINTAIN_CHR: c_char = b'm' as c_char;

/* string holding all privilege code chars, in order by bitmask position */
pub const ACL_ALL_RIGHTS_STR: &[u8] = b"arwdDxtXUCTcsAm\0";

/* Bitmasks defining "all rights" for each supported object type */
pub const ACL_ALL_RIGHTS_COLUMN: AclMode = ACL_INSERT | ACL_SELECT | ACL_UPDATE | ACL_REFERENCES;
pub const ACL_ALL_RIGHTS_RELATION: AclMode = ACL_INSERT
    | ACL_SELECT
    | ACL_UPDATE
    | ACL_DELETE
    | ACL_TRUNCATE
    | ACL_REFERENCES
    | ACL_TRIGGER
    | ACL_MAINTAIN;
pub const ACL_ALL_RIGHTS_SEQUENCE: AclMode = ACL_USAGE | ACL_SELECT | ACL_UPDATE;
pub const ACL_ALL_RIGHTS_DATABASE: AclMode = ACL_CREATE | ACL_CREATE_TEMP | ACL_CONNECT;
pub const ACL_ALL_RIGHTS_FDW: AclMode = ACL_USAGE;
pub const ACL_ALL_RIGHTS_FOREIGN_SERVER: AclMode = ACL_USAGE;
pub const ACL_ALL_RIGHTS_FUNCTION: AclMode = ACL_EXECUTE;
pub const ACL_ALL_RIGHTS_LANGUAGE: AclMode = ACL_USAGE;
pub const ACL_ALL_RIGHTS_LARGEOBJECT: AclMode = ACL_SELECT | ACL_UPDATE;
pub const ACL_ALL_RIGHTS_PARAMETER_ACL: AclMode = ACL_SET | ACL_ALTER_SYSTEM;
pub const ACL_ALL_RIGHTS_SCHEMA: AclMode = ACL_USAGE | ACL_CREATE;
pub const ACL_ALL_RIGHTS_TABLESPACE: AclMode = ACL_CREATE;
pub const ACL_ALL_RIGHTS_TYPE: AclMode = ACL_USAGE;

/* operation codes for pg_*_aclmask */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AclMaskHow {
    ACLMASK_ALL = 0, /* normal case: compute all bits */
    ACLMASK_ANY,     /* return when result is known nonzero */
}
pub use AclMaskHow::*;

/* result codes for pg_*_aclcheck */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AclResult {
    ACLCHECK_OK = 0,
    ACLCHECK_NO_PRIV,
    ACLCHECK_NOT_OWNER,
}
pub use AclResult::*;

// ACLITEMOID / N_ACL_RIGHTS / NAMEDATALEN come from pg_type / acl headers.
// TODO(pg-port): ACLITEMOID lives in crate::catalog::pg_type_d.
pub const ACLITEMOID: Oid = 1033;
/* number of privilege bits (sync with ACL_ALL_RIGHTS_STR) */
pub const N_ACL_RIGHTS: c_int = 15;

// ----------------------------------------------------------------------------
// <ctype.h>
// ----------------------------------------------------------------------------
extern "C" {
    fn isspace(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    fn isalnum(c: c_int) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );
}

// priv_map: privilege name -> AclMode bitmask.
#[repr(C)]
#[derive(Clone, Copy)]
struct priv_map {
    name: *const c_char,
    value: AclMode,
}
// priv_map tables embed raw pointers; make them shareable as statics.
unsafe impl Sync for priv_map {}

/// const-fn form of ACL_GRANT_OPTION_FOR for use in the `static` priv_map tables
/// (the function form ACL_GRANT_OPTION_FOR is not const-callable in a static init).
const fn gopt(privs: AclMode) -> AclMode {
    ((privs as AclMode) & 0xFFFFFFFF) << 32
}

// ----------------------------------------------------------------------------
// Role-membership cache state (file-static in the C).
// ----------------------------------------------------------------------------

/// Each element of cached_roles is an OID list of constituent roles for the
/// corresponding element of cached_role.  Separate cache per RoleRecurseType.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RoleRecurseType {
    ROLERECURSE_MEMBERS = 0, /* recurse unconditionally */
    ROLERECURSE_PRIVS = 1,   /* recurse through inheritable grants */
    ROLERECURSE_SETROLE = 2, /* recurse through grants with set_option */
}
use RoleRecurseType::*;

static mut cached_role: [Oid; 3] = [InvalidOid, InvalidOid, InvalidOid];
static mut cached_roles: [*mut List; 3] = [core::ptr::null_mut(); 3];
static mut cached_db_hash: uint32 = 0;

/// If the role list grows past this threshold, a Bloom filter is created to
/// speed up membership checks.
const ROLES_LIST_BLOOM_THRESHOLD: c_int = 1024;

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ----------------------------------------------------------------------------
// TODO(pg-port): real symbols live in miscadmin.c / utils/init,
// utils/cache/syscache.c, utils/cache/lsyscache.c, catalog/namespace.c,
// commands/dbcommands.c, commands/proclang.c, commands/tablespace.c,
// foreign/foreign.c, storage/large_object.c, lib/bloomfilter.c.

// BOOTSTRAP_SUPERUSERID (catalog/pg_authid_d.h)
// TODO(pg-port): real value lives in crate::catalog::pg_authid_d.
pub const BOOTSTRAP_SUPERUSERID: Oid = 10;
pub const ROLE_PG_DATABASE_OWNER: Oid = 6171;

extern "C" {
    pub fn GetUserId() -> Oid;
    pub fn GetSessionUserId() -> Oid;
    pub fn superuser_arg(roleid: Oid) -> bool;
    fn pg_popcount64(word: uint64) -> c_int;
}
// utils/varlena.h: text_to_cstring.  port/pg_strcasecmp.
use crate::utils::adt::varlena::text_to_cstring;
use crate::port::pgstrcasecmp::pg_strcasecmp;

// Syscache cache-id constants (catalog/syscache_ids.h).  Most of these are not
// yet exported from the ported syscache module, so they are defined locally.
// TODO(pg-port): real values live in crate::catalog::syscache_ids / pg_*_d.h.

// Anum_pg_authid_oid (catalog/pg_authid.h).
// TODO(pg-port): real value lives in crate::catalog::pg_authid.
pub const Anum_pg_authid_oid: c_int = 1;

// CatCList / catcache list iteration support (utils/catcache.h).
// TODO(pg-port): real CatCList lives in crate::utils::cache::catcache.
#[repr(C)]
pub struct CatCTup {
    pub tuple: HeapTupleData,
}
#[repr(C)]
pub struct CatCList {
    pub n_members: c_int,
    pub members: *mut *mut CatCTup,
}

// lib/bloomfilter.h opaque type.
// TODO(pg-port): real bloom_filter lives in crate::lib::bloomfilter.
pub enum bloom_filter {}

// The remaining syscache / lsyscache / catalog / commands / foreign / large
// object / bloom-filter / cache-callback helpers below are not yet ported.
// They are declared here as stubs so this file has no undefined symbols.
// TODO(pg-port): real symbols live in utils/cache/syscache.c & lsyscache.c,
// catalog/namespace.c, commands/dbcommands.c, commands/proclang.c,
// commands/tablespace.c, foreign/foreign.c, storage/large_object.c,
// lib/bloomfilter.c, utils/cache/inval.c.
extern "C" {
    pub fn GetSysCacheOid1(cacheId: c_int, oidcol: c_int, key1: Datum) -> Oid;
    pub fn GetSysCacheHashValue1(cacheId: c_int, key1: Datum) -> uint32;
    pub fn SearchSysCacheList1(cacheId: c_int, key1: Datum) -> *mut CatCList;
    pub fn ReleaseSysCacheList(list: *mut CatCList);
    pub fn CacheRegisterSyscacheCallback(
        cacheid: c_int,
        func: unsafe extern "C" fn(Datum, c_int, uint32),
        arg: Datum,
    );

    pub fn get_rel_relkind(relid: Oid) -> c_char;
    pub fn get_rel_name(relid: Oid) -> *mut c_char;
    pub fn get_database_oid(dbname: *const c_char, missing_ok: bool) -> Oid;
    pub fn get_namespace_oid(nspname: *const c_char, missing_ok: bool) -> Oid;
    pub fn get_language_oid(langname: *const c_char, missing_ok: bool) -> Oid;
    pub fn get_tablespace_oid(spcname: *const c_char, missing_ok: bool) -> Oid;
    pub fn get_foreign_data_wrapper_oid(fdwname: *const c_char, missing_ok: bool) -> Oid;
    pub fn get_foreign_server_oid(servername: *const c_char, missing_ok: bool) -> Oid;
    pub fn GetUserNameFromId(roleid: Oid, noerr: bool) -> *mut c_char;
    pub fn IsReservedName(name: *const c_char) -> bool;
    pub fn IsBootstrapProcessingMode() -> bool;
    pub fn LargeObjectExistsWithSnapshot(loid: Oid, snapshot: Snapshot) -> bool;
    pub fn GetActiveSnapshot() -> Snapshot;

    pub fn bloom_create(total_elems: int64, bloom_work_mem: c_int, seed: uint64) -> *mut bloom_filter;
    pub fn bloom_free(filter: *mut bloom_filter);
    pub fn bloom_add_element(filter: *mut bloom_filter, elem: *const u8, len: usize);
    pub fn bloom_lacks_element(filter: *mut bloom_filter, elem: *const u8, len: usize) -> bool;

    // object_aclcheck / pg_class_aclcheck / pg_attribute_aclcheck (aclchk.c).
    pub fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: AclMode) -> AclResult;
    pub fn object_aclcheck_ext(
        classid: Oid,
        objectid: Oid,
        roleid: Oid,
        mode: AclMode,
        is_missing: *mut bool,
    ) -> AclResult;
    pub fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: AclMode) -> AclResult;
    pub fn pg_class_aclcheck_ext(
        table_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        is_missing: *mut bool,
    ) -> AclResult;
    pub fn pg_attribute_aclcheck_ext(
        table_oid: Oid,
        attnum: AttrNumber,
        roleid: Oid,
        mode: AclMode,
        is_missing: *mut bool,
    ) -> AclResult;
    pub fn pg_attribute_aclcheck_all(
        table_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        how: AclMaskHow,
    ) -> AclResult;
    pub fn pg_attribute_aclcheck_all_ext(
        table_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        how: AclMaskHow,
        is_missing: *mut bool,
    ) -> AclResult;
    pub fn pg_parameter_aclcheck(name: *const c_char, roleid: Oid, mode: AclMode) -> AclResult;
    pub fn pg_largeobject_aclcheck_snapshot(
        lobj_oid: Oid,
        roleid: Oid,
        mode: AclMode,
        snapshot: Snapshot,
    ) -> AclResult;
}

// Catalog relation-id constants used by object_aclcheck calls.
// TODO(pg-port): real values live in crate::catalog::pg_*_d.h.
pub const DatabaseRelationId: Oid = 1262;
pub const ForeignDataWrapperRelationId: Oid = 2328;
pub const ForeignServerRelationId: Oid = 1417;
pub const ProcedureRelationId: Oid = 1255;
pub const LanguageRelationId: Oid = 2612;
pub const NamespaceRelationId: Oid = 2615;
pub const TableSpaceRelationId: Oid = 1213;
pub const TypeRelationId: Oid = 1247;

// RELKIND_SEQUENCE (catalog/pg_class.h).
pub const RELKIND_SEQUENCE: c_char = b'S' as c_char;

// regprocedurein / regtypein (utils/adt/regproc.c) for convert_function_name /
// convert_type_name.  TODO(pg-port): real symbols live in crate::utils::adt::regproc.
use crate::utils::adt::regproc::{regprocedurein, regtypein};

// MyDatabaseId / lo_compat_privileges / work_mem (miscadmin.h / large_object.h / guc).
// TODO(pg-port): real globals live in crate::miscadmin / storage/large_object /
// utils/guc.
extern "C" {
    pub static MyDatabaseId: Oid;
    pub static lo_compat_privileges: bool;
    pub static work_mem: c_int;
}

// pg_database form (catalog/pg_database.h) -- only datdba is read here.
// TODO(pg-port): real FormData_pg_database lives in crate::catalog::pg_database.
#[repr(C)]
pub struct FormData_pg_database {
    pub oid: Oid,
    pub datname: NameData,
    pub datdba: Oid,
}
pub type Form_pg_database = *mut FormData_pg_database;

// pg_auth_members form (catalog/pg_auth_members.h).
// TODO(pg-port): real FormData_pg_auth_members lives in crate::catalog::pg_auth_members.
#[repr(C)]
pub struct FormData_pg_auth_members {
    pub oid: Oid,
    pub roleid: Oid,
    pub member: Oid,
    pub grantor: Oid,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}
pub type Form_pg_auth_members = *mut FormData_pg_auth_members;

// ----------------------------------------------------------------------------
// is_safe_acl_char
// ----------------------------------------------------------------------------

/// Test whether an identifier char can be left unquoted in ACLs.
#[inline]
unsafe fn is_safe_acl_char(c: u8, is_getid: bool) -> bool {
    if IS_HIGHBIT_SET(c) {
        return is_getid;
    }
    isalnum(c as c_int) != 0 || c == b'_'
}

// ----------------------------------------------------------------------------
// getid
// ----------------------------------------------------------------------------

/// getid - consume the first identifier in `s`, loading it into `n`.
unsafe fn getid(mut s: *const c_char, n: *mut c_char, escontext: *mut Node) -> *const c_char {
    let mut len: c_int = 0;
    let mut in_quotes = false;

    Assert!(!s.is_null() && !n.is_null());

    while isspace(*s as c_int) != 0 {
        s = s.add(1);
    }
    while *s != 0 && (in_quotes || *s == b'"' as c_char || is_safe_acl_char(*s as u8, true)) {
        if *s == b'"' as c_char {
            if !in_quotes {
                in_quotes = true;
                s = s.add(1);
                continue;
            }
            /* safe to look at next char (could be '\0' though) */
            if *s.add(1) != b'"' as c_char {
                in_quotes = false;
                s = s.add(1);
                continue;
            }
            /* it's an escaped double quote; skip the escaping char */
            s = s.add(1);
        }

        /* Add the character to the string */
        if len >= NAMEDATALEN as c_int - 1 {
            // ereturn(escontext, NULL, errmsg("identifier too long"), errdetail(...))
            ereport!(ERROR, errmsg!("identifier too long"));
        }

        *n.add(len as usize) = *s;
        len += 1;
        s = s.add(1);
    }
    *n.add(len as usize) = 0;
    while isspace(*s as c_int) != 0 {
        s = s.add(1);
    }
    s
}

// ----------------------------------------------------------------------------
// putid
// ----------------------------------------------------------------------------

/// Write a role name at *p, adding double quotes if needed.
unsafe fn putid(mut p: *mut c_char, s: *const c_char) {
    let mut src: *const c_char;
    let mut safe = true;

    /* Detect whether we need to use double quotes */
    src = s;
    while *src != 0 {
        if !is_safe_acl_char(*src as u8, false) {
            safe = false;
            break;
        }
        src = src.add(1);
    }
    if !safe {
        *p = b'"' as c_char;
        p = p.add(1);
    }
    src = s;
    while *src != 0 {
        /* A double quote character in a username is encoded as "" */
        if *src == b'"' as c_char {
            *p = b'"' as c_char;
            p = p.add(1);
        }
        *p = *src;
        p = p.add(1);
        src = src.add(1);
    }
    if !safe {
        *p = b'"' as c_char;
        p = p.add(1);
    }
    *p = 0;
}

// ----------------------------------------------------------------------------
// aclparse
// ----------------------------------------------------------------------------

/// aclparse - parse an ACL specification of the form
/// `[group|user] [A-Za-z0-9]*=[rwaR]*` from `s`.
unsafe fn aclparse(mut s: *const c_char, aip: *mut AclItem, escontext: *mut Node) -> *const c_char {
    let mut privs: AclMode;
    let mut goption: AclMode;
    let mut read: AclMode;
    let mut name = [0 as c_char; NAMEDATALEN as usize];
    let mut name2 = [0 as c_char; NAMEDATALEN as usize];

    Assert!(!s.is_null() && !aip.is_null());

    s = getid(s, name.as_mut_ptr(), escontext);
    if s.is_null() {
        return core::ptr::null();
    }
    if *s != b'=' as c_char {
        /* we just read a keyword, not a name */
        if strcmp(name.as_ptr(), c"group".as_ptr()) != 0
            && strcmp(name.as_ptr(), c"user".as_ptr()) != 0
        {
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized key word: \"{}\"",
                    std::ffi::CStr::from_ptr(name.as_ptr()).to_string_lossy()
                )
            );
        }
        /* move s to the name beyond the keyword */
        s = getid(s, name.as_mut_ptr(), escontext);
        if s.is_null() {
            return core::ptr::null();
        }
        if name[0] == 0 {
            ereport!(ERROR, errmsg!("missing name"));
        }
    }

    if *s != b'=' as c_char {
        ereport!(ERROR, errmsg!("missing \"=\" sign"));
    }

    privs = ACL_NO_RIGHTS;
    goption = ACL_NO_RIGHTS;

    s = s.add(1);
    read = 0;
    while isalpha(*s as c_int) != 0 || *s == b'*' as c_char {
        match *s {
            c if c == b'*' as c_char => {
                goption |= read;
            }
            c if c == ACL_INSERT_CHR => read = ACL_INSERT,
            c if c == ACL_SELECT_CHR => read = ACL_SELECT,
            c if c == ACL_UPDATE_CHR => read = ACL_UPDATE,
            c if c == ACL_DELETE_CHR => read = ACL_DELETE,
            c if c == ACL_TRUNCATE_CHR => read = ACL_TRUNCATE,
            c if c == ACL_REFERENCES_CHR => read = ACL_REFERENCES,
            c if c == ACL_TRIGGER_CHR => read = ACL_TRIGGER,
            c if c == ACL_EXECUTE_CHR => read = ACL_EXECUTE,
            c if c == ACL_USAGE_CHR => read = ACL_USAGE,
            c if c == ACL_CREATE_CHR => read = ACL_CREATE,
            c if c == ACL_CREATE_TEMP_CHR => read = ACL_CREATE_TEMP,
            c if c == ACL_CONNECT_CHR => read = ACL_CONNECT,
            c if c == ACL_SET_CHR => read = ACL_SET,
            c if c == ACL_ALTER_SYSTEM_CHR => read = ACL_ALTER_SYSTEM,
            c if c == ACL_MAINTAIN_CHR => read = ACL_MAINTAIN,
            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid mode character: must be one of \"{}\"",
                        std::ffi::CStr::from_ptr(ACL_ALL_RIGHTS_STR.as_ptr() as *const c_char)
                            .to_string_lossy()
                    )
                );
            }
        }

        privs |= read;
        s = s.add(1);
    }

    if name[0] == 0 {
        (*aip).ai_grantee = ACL_ID_PUBLIC;
    } else {
        (*aip).ai_grantee = get_role_oid(name.as_ptr(), true);
        if !OidIsValid((*aip).ai_grantee) {
            ereport!(
                ERROR,
                errmsg!(
                    "role \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(name.as_ptr()).to_string_lossy()
                )
            );
        }
    }

    /*
     * XXX Allow a degree of backward compatibility by defaulting the grantor
     * to the superuser.
     */
    if *s == b'/' as c_char {
        s = getid(s.add(1), name2.as_mut_ptr(), escontext);
        if s.is_null() {
            return core::ptr::null();
        }
        if name2[0] == 0 {
            ereport!(ERROR, errmsg!("a name must follow the \"/\" sign"));
        }
        (*aip).ai_grantor = get_role_oid(name2.as_ptr(), true);
        if !OidIsValid((*aip).ai_grantor) {
            ereport!(
                ERROR,
                errmsg!(
                    "role \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(name2.as_ptr()).to_string_lossy()
                )
            );
        }
    } else {
        (*aip).ai_grantor = BOOTSTRAP_SUPERUSERID;
        ereport!(
            WARNING,
            errmsg!(
                "defaulting grantor to user ID {}",
                BOOTSTRAP_SUPERUSERID
            )
        );
    }

    ACLITEM_SET_PRIVS_GOPTIONS(&mut *aip, privs, goption);

    s
}

// ----------------------------------------------------------------------------
// allocacl / make_empty_acl / aclcopy / aclconcat / aclmerge
// ----------------------------------------------------------------------------

/// allocacl - allocate storage for a new Acl with `n` entries.
unsafe fn allocacl(n: c_int) -> *mut Acl {
    let new_acl: *mut Acl;
    let size: Size;

    if n < 0 {
        elog!(ERROR, "invalid size: {}", n);
    }
    size = ACL_N_SIZE(n) as Size;
    new_acl = palloc0(size) as *mut Acl;
    SET_VARSIZE(new_acl as *mut c_char, size as int32);
    (*new_acl).ndim = 1;
    (*new_acl).dataoffset = 0; /* we never put in any nulls */
    (*new_acl).elemtype = ACLITEMOID;
    *ARR_LBOUND(new_acl).add(0) = 1;
    *ARR_DIMS(new_acl).add(0) = n;
    new_acl
}

/// Create a zero-entry ACL
pub unsafe fn make_empty_acl() -> *mut Acl {
    allocacl(0)
}

/// Copy an ACL
pub unsafe fn aclcopy(orig_acl: *const Acl) -> *mut Acl {
    let result_acl: *mut Acl;

    result_acl = allocacl(ACL_NUM(orig_acl));

    memcpy(
        ACL_DAT(result_acl) as *mut c_void,
        ACL_DAT(orig_acl) as *const c_void,
        ACL_NUM(orig_acl) as usize * core::mem::size_of::<AclItem>(),
    );

    result_acl
}

/// Concatenate two ACLs (may produce redundant entries).
pub unsafe fn aclconcat(left_acl: *const Acl, right_acl: *const Acl) -> *mut Acl {
    let result_acl: *mut Acl;

    result_acl = allocacl(ACL_NUM(left_acl) + ACL_NUM(right_acl));

    memcpy(
        ACL_DAT(result_acl) as *mut c_void,
        ACL_DAT(left_acl) as *const c_void,
        ACL_NUM(left_acl) as usize * core::mem::size_of::<AclItem>(),
    );

    memcpy(
        ACL_DAT(result_acl).add(ACL_NUM(left_acl) as usize) as *mut c_void,
        ACL_DAT(right_acl) as *const c_void,
        ACL_NUM(right_acl) as usize * core::mem::size_of::<AclItem>(),
    );

    result_acl
}

/// Merge two ACLs.  Returns NULL on NULL input.
pub unsafe fn aclmerge(left_acl: *const Acl, right_acl: *const Acl, ownerId: Oid) -> *mut Acl {
    let mut result_acl: *mut Acl;
    let mut aip: *mut AclItem;
    let mut i: c_int;
    let num: c_int;

    /* Check for cases where one or both are empty/null */
    if left_acl.is_null() || ACL_NUM(left_acl) == 0 {
        if right_acl.is_null() || ACL_NUM(right_acl) == 0 {
            return core::ptr::null_mut();
        } else {
            return aclcopy(right_acl);
        }
    } else {
        if right_acl.is_null() || ACL_NUM(right_acl) == 0 {
            return aclcopy(left_acl);
        }
    }

    /* Merge them the hard way, one item at a time */
    result_acl = aclcopy(left_acl);

    aip = ACL_DAT(right_acl);
    num = ACL_NUM(right_acl);

    i = 0;
    while i < num {
        let tmp_acl: *mut Acl;

        tmp_acl = aclupdate(result_acl, aip, ACL_MODECHG_ADD, ownerId, DROP_RESTRICT);
        pfree(result_acl as *mut c_void);
        result_acl = tmp_acl;

        i += 1;
        aip = aip.add(1);
    }

    result_acl
}

// ----------------------------------------------------------------------------
// aclitemsort / aclequal / check_acl
// ----------------------------------------------------------------------------

// extern-C shim so libc qsort can call the Rust-ABI oid_cmp.
unsafe extern "C" fn acl_oid_cmp_c(a: *const c_void, b: *const c_void) -> c_int {
    crate::utils::adt::oid::oid_cmp(a, b)
}

/// Sort the items in an ACL (into an arbitrary but consistent order)
pub unsafe fn aclitemsort(acl: *mut Acl) {
    if !acl.is_null() && ACL_NUM(acl) > 1 {
        qsort(
            ACL_DAT(acl) as *mut c_void,
            ACL_NUM(acl) as usize,
            core::mem::size_of::<AclItem>(),
            aclitemComparator,
        );
    }
}

/// Check if two ACLs are exactly equal
pub unsafe fn aclequal(left_acl: *const Acl, right_acl: *const Acl) -> bool {
    /* Check for cases where one or both are empty/null */
    if left_acl.is_null() || ACL_NUM(left_acl) == 0 {
        if right_acl.is_null() || ACL_NUM(right_acl) == 0 {
            return true;
        } else {
            return false;
        }
    } else {
        if right_acl.is_null() || ACL_NUM(right_acl) == 0 {
            return false;
        }
    }

    if ACL_NUM(left_acl) != ACL_NUM(right_acl) {
        return false;
    }

    if memcmp(
        ACL_DAT(left_acl) as *const c_void,
        ACL_DAT(right_acl) as *const c_void,
        ACL_NUM(left_acl) as usize * core::mem::size_of::<AclItem>(),
    ) == 0
    {
        return true;
    }

    false
}

/// Verify that an ACL array is acceptable (one-dimensional and has no nulls)
unsafe fn check_acl(acl: *const Acl) {
    if ARR_ELEMTYPE(acl) != ACLITEMOID {
        ereport!(ERROR, errmsg!("ACL array contains wrong data type"));
    }
    if ARR_NDIM(acl) != 1 {
        ereport!(ERROR, errmsg!("ACL arrays must be one-dimensional"));
    }
    if ARR_HASNULL(acl) {
        ereport!(ERROR, errmsg!("ACL arrays must not contain null values"));
    }
}

// ----------------------------------------------------------------------------
// aclitemin / aclitemout
// ----------------------------------------------------------------------------

/// aclitemin - parse a string into a new AclItem.
pub unsafe fn aclitemin(fcinfo: FunctionCallInfo) -> Datum {
    let mut s = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext = (*fcinfo).context as *mut Node;
    let aip: *mut AclItem;

    aip = palloc(core::mem::size_of::<AclItem>()) as *mut AclItem;

    s = aclparse(s, aip, escontext) as *mut c_char;
    if s.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    while isspace(*s as c_int) != 0 {
        s = s.add(1);
    }
    if *s != 0 {
        // ereturn(escontext, (Datum) 0, ...)
        ereport!(
            ERROR,
            errmsg!("extra garbage at the end of the ACL specification")
        );
    }

    PG_RETURN_POINTER!(aip)
}

/// aclitemout - format an AclItem back into a string.
pub unsafe fn aclitemout(fcinfo: FunctionCallInfo) -> Datum {
    let aip = DatumGetAclItemP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut p: *mut c_char;
    let out: *mut c_char;
    let mut htup: HeapTuple;
    let mut i: c_uint;

    out = palloc(
        "=/".len()
            + 2 * N_ACL_RIGHTS as usize
            + 2 * (2 * NAMEDATALEN + 2)
            + 1,
    ) as *mut c_char;

    p = out;
    *p = 0;

    if (*aip).ai_grantee != ACL_ID_PUBLIC {
        htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum((*aip).ai_grantee));
        if HeapTupleIsValid(htup) {
            putid(
                p,
                NameStr(&(*(GETSTRUCT(htup) as Form_pg_authid)).rolname),
            );
            ReleaseSysCache(htup);
        } else {
            /* Generate numeric OID if we don't find an entry */
            sprintf(p, c"%u".as_ptr(), (*aip).ai_grantee);
        }
    }
    while *p != 0 {
        p = p.add(1);
    }

    *p = b'=' as c_char;
    p = p.add(1);

    i = 0;
    while i < N_ACL_RIGHTS as c_uint {
        if ACLITEM_GET_PRIVS(*aip) & ((1u64) << i) != 0 {
            *p = ACL_ALL_RIGHTS_STR[i as usize] as c_char;
            p = p.add(1);
        }
        if ACLITEM_GET_GOPTIONS(*aip) & ((1u64) << i) != 0 {
            *p = b'*' as c_char;
            p = p.add(1);
        }
        i += 1;
    }

    *p = b'/' as c_char;
    p = p.add(1);
    *p = 0;

    htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum((*aip).ai_grantor));
    if HeapTupleIsValid(htup) {
        putid(
            p,
            NameStr(&(*(GETSTRUCT(htup) as Form_pg_authid)).rolname),
        );
        ReleaseSysCache(htup);
    } else {
        /* Generate numeric OID if we don't find an entry */
        sprintf(p, c"%u".as_ptr(), (*aip).ai_grantor);
    }

    PG_RETURN_CSTRING!(out)
}

// ----------------------------------------------------------------------------
// aclitem_match / aclitemComparator
// ----------------------------------------------------------------------------

/// Two AclItems match iff they have the same grantee and grantor.
unsafe fn aclitem_match(a1: *const AclItem, a2: *const AclItem) -> bool {
    (*a1).ai_grantee == (*a2).ai_grantee && (*a1).ai_grantor == (*a2).ai_grantor
}

/// qsort comparison function for AclItems
unsafe extern "C" fn aclitemComparator(arg1: *const c_void, arg2: *const c_void) -> c_int {
    let a1 = arg1 as *const AclItem;
    let a2 = arg2 as *const AclItem;

    if (*a1).ai_grantee > (*a2).ai_grantee {
        return 1;
    }
    if (*a1).ai_grantee < (*a2).ai_grantee {
        return -1;
    }
    if (*a1).ai_grantor > (*a2).ai_grantor {
        return 1;
    }
    if (*a1).ai_grantor < (*a2).ai_grantor {
        return -1;
    }
    if (*a1).ai_privs > (*a2).ai_privs {
        return 1;
    }
    if (*a1).ai_privs < (*a2).ai_privs {
        return -1;
    }
    0
}

/// aclitem equality operator
pub unsafe fn aclitem_eq(fcinfo: FunctionCallInfo) -> Datum {
    let a1 = DatumGetAclItemP(PG_GETARG_DATUM!(fcinfo, 0));
    let a2 = DatumGetAclItemP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;

    result = (*a1).ai_privs == (*a2).ai_privs
        && (*a1).ai_grantee == (*a2).ai_grantee
        && (*a1).ai_grantor == (*a2).ai_grantor;
    PG_RETURN_BOOL!(result)
}

/// aclitem hash function
pub unsafe fn hash_aclitem(fcinfo: FunctionCallInfo) -> Datum {
    let a = DatumGetAclItemP(PG_GETARG_DATUM!(fcinfo, 0));

    /* not very bright, but avoids any issue of padding in struct */
    PG_RETURN_UINT32!(((*a).ai_privs as uint32)
            .wrapping_add((*a).ai_grantee)
            .wrapping_add((*a).ai_grantor)
    )
}

/// 64-bit hash function for aclitem.
pub unsafe fn hash_aclitem_extended(fcinfo: FunctionCallInfo) -> Datum {
    let a = DatumGetAclItemP(PG_GETARG_DATUM!(fcinfo, 0));
    let seed = PG_GETARG_INT64!(fcinfo, 1) as uint64;
    let sum: uint32 = ((*a).ai_privs as uint32)
        .wrapping_add((*a).ai_grantee)
        .wrapping_add((*a).ai_grantor);

    if seed == 0 {
        UInt64GetDatum(sum as uint64)
    } else {
        hash_uint32_extended(sum, seed)
    }
}

// ----------------------------------------------------------------------------
// acldefault / acldefault_sql
// ----------------------------------------------------------------------------

/// acldefault - create an ACL describing default access permissions.
pub unsafe fn acldefault(objtype: ObjectType, ownerId: Oid) -> *mut Acl {
    let world_default: AclMode;
    let owner_default: AclMode;
    let mut nacl: c_int;
    let acl: *mut Acl;
    let mut aip: *mut AclItem;

    match objtype {
        OBJECT_COLUMN => {
            /* by default, columns have no extra privileges */
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_NO_RIGHTS;
        }
        OBJECT_TABLE => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_RELATION;
        }
        OBJECT_SEQUENCE => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_SEQUENCE;
        }
        OBJECT_DATABASE => {
            /* for backwards compatibility, grant some rights by default */
            world_default = ACL_CREATE_TEMP | ACL_CONNECT;
            owner_default = ACL_ALL_RIGHTS_DATABASE;
        }
        OBJECT_FUNCTION => {
            /* Grant EXECUTE by default, for now */
            world_default = ACL_EXECUTE;
            owner_default = ACL_ALL_RIGHTS_FUNCTION;
        }
        OBJECT_LANGUAGE => {
            /* Grant USAGE by default, for now */
            world_default = ACL_USAGE;
            owner_default = ACL_ALL_RIGHTS_LANGUAGE;
        }
        OBJECT_LARGEOBJECT => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_LARGEOBJECT;
        }
        OBJECT_SCHEMA => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_SCHEMA;
        }
        OBJECT_TABLESPACE => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_TABLESPACE;
        }
        OBJECT_FDW => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_FDW;
        }
        OBJECT_FOREIGN_SERVER => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_FOREIGN_SERVER;
        }
        OBJECT_DOMAIN | OBJECT_TYPE => {
            world_default = ACL_USAGE;
            owner_default = ACL_ALL_RIGHTS_TYPE;
        }
        OBJECT_PARAMETER_ACL => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_PARAMETER_ACL;
        }
        _ => {
            elog!(ERROR, "unrecognized object type: {}", objtype as c_int);
            #[allow(unreachable_code)]
            {
                world_default = ACL_NO_RIGHTS; /* keep compiler quiet */
                owner_default = ACL_NO_RIGHTS;
            }
        }
    }

    nacl = 0;
    if world_default != ACL_NO_RIGHTS {
        nacl += 1;
    }
    if owner_default != ACL_NO_RIGHTS {
        nacl += 1;
    }

    acl = allocacl(nacl);
    aip = ACL_DAT(acl);

    if world_default != ACL_NO_RIGHTS {
        (*aip).ai_grantee = ACL_ID_PUBLIC;
        (*aip).ai_grantor = ownerId;
        ACLITEM_SET_PRIVS_GOPTIONS(&mut *aip, world_default, ACL_NO_RIGHTS);
        aip = aip.add(1);
    }

    /*
     * Note that the owner's entry shows all ordinary privileges but no grant
     * options.  See the long comment in the C source.
     */
    if owner_default != ACL_NO_RIGHTS {
        (*aip).ai_grantee = ownerId;
        (*aip).ai_grantor = ownerId;
        ACLITEM_SET_PRIVS_GOPTIONS(&mut *aip, owner_default, ACL_NO_RIGHTS);
    }

    acl
}

/// SQL-accessible version of acldefault().
pub unsafe fn acldefault_sql(fcinfo: FunctionCallInfo) -> Datum {
    let objtypec = PG_GETARG_CHAR!(fcinfo, 0);
    let owner = PG_GETARG_OID!(fcinfo, 1);
    let objtype: ObjectType;

    objtype = match objtypec as u8 {
        b'c' => OBJECT_COLUMN,
        b'r' => OBJECT_TABLE,
        b's' => OBJECT_SEQUENCE,
        b'd' => OBJECT_DATABASE,
        b'f' => OBJECT_FUNCTION,
        b'l' => OBJECT_LANGUAGE,
        b'L' => OBJECT_LARGEOBJECT,
        b'n' => OBJECT_SCHEMA,
        b'p' => OBJECT_PARAMETER_ACL,
        b't' => OBJECT_TABLESPACE,
        b'F' => OBJECT_FDW,
        b'S' => OBJECT_FOREIGN_SERVER,
        b'T' => OBJECT_TYPE,
        _ => {
            elog!(
                ERROR,
                "unrecognized object type abbreviation: {}",
                objtypec as u8 as char
            );
            OBJECT_COLUMN /* keep compiler quiet */
        }
    };

    PG_RETURN_POINTER!(acldefault(objtype, owner))
}

// ----------------------------------------------------------------------------
// aclupdate
// ----------------------------------------------------------------------------

/// Update an ACL array to add or remove specified privileges.
pub unsafe fn aclupdate(
    old_acl: *const Acl,
    mod_aip: *const AclItem,
    modechg: c_int,
    ownerId: Oid,
    behavior: DropBehavior,
) -> *mut Acl {
    let mut new_acl: *mut Acl = core::ptr::null_mut();
    let old_aip: *mut AclItem;
    let mut new_aip: *mut AclItem = core::ptr::null_mut();
    let old_rights: AclMode;
    let old_goptions: AclMode;
    let new_rights: AclMode;
    let new_goptions: AclMode;
    let mut dst: c_int;
    let mut num: c_int;

    /* Caller probably already checked old_acl, but be safe */
    check_acl(old_acl);

    /* If granting grant options, check for circularity */
    if modechg != ACL_MODECHG_DEL && ACLITEM_GET_GOPTIONS(*mod_aip) != ACL_NO_RIGHTS {
        check_circularity(old_acl, mod_aip, ownerId);
    }

    num = ACL_NUM(old_acl);
    old_aip = ACL_DAT(old_acl);

    /*
     * Search the ACL for an existing entry for this grantee and grantor.
     */
    dst = 0;
    while dst < num {
        if aclitem_match(mod_aip, old_aip.add(dst as usize)) {
            /* found a match, so modify existing item */
            new_acl = allocacl(num);
            new_aip = ACL_DAT(new_acl);
            memcpy(
                new_acl as *mut c_void,
                old_acl as *const c_void,
                ACL_SIZE(old_acl) as usize,
            );
            break;
        }
        dst += 1;
    }

    if dst == num {
        /* need to append a new item */
        new_acl = allocacl(num + 1);
        new_aip = ACL_DAT(new_acl);
        memcpy(
            new_aip as *mut c_void,
            old_aip as *const c_void,
            num as usize * core::mem::size_of::<AclItem>(),
        );

        /* initialize the new entry with no permissions */
        (*new_aip.add(dst as usize)).ai_grantee = (*mod_aip).ai_grantee;
        (*new_aip.add(dst as usize)).ai_grantor = (*mod_aip).ai_grantor;
        ACLITEM_SET_PRIVS_GOPTIONS(&mut *new_aip.add(dst as usize), ACL_NO_RIGHTS, ACL_NO_RIGHTS);
        num += 1; /* set num to the size of new_acl */
    }

    old_rights = ACLITEM_GET_RIGHTS(*new_aip.add(dst as usize));
    old_goptions = ACLITEM_GET_GOPTIONS(*new_aip.add(dst as usize));

    /* apply the specified permissions change */
    match modechg {
        ACL_MODECHG_ADD => {
            ACLITEM_SET_RIGHTS(
                &mut *new_aip.add(dst as usize),
                old_rights | ACLITEM_GET_RIGHTS(*mod_aip),
            );
        }
        ACL_MODECHG_DEL => {
            ACLITEM_SET_RIGHTS(
                &mut *new_aip.add(dst as usize),
                old_rights & !ACLITEM_GET_RIGHTS(*mod_aip),
            );
        }
        ACL_MODECHG_EQL => {
            ACLITEM_SET_RIGHTS(&mut *new_aip.add(dst as usize), ACLITEM_GET_RIGHTS(*mod_aip));
        }
        _ => {}
    }

    new_rights = ACLITEM_GET_RIGHTS(*new_aip.add(dst as usize));
    new_goptions = ACLITEM_GET_GOPTIONS(*new_aip.add(dst as usize));

    /*
     * If the adjusted entry has no permissions, delete it from the list.
     */
    if new_rights == ACL_NO_RIGHTS {
        memmove(
            new_aip.add(dst as usize) as *mut c_void,
            new_aip.add(dst as usize + 1) as *const c_void,
            (num - dst - 1) as usize * core::mem::size_of::<AclItem>(),
        );
        /* Adjust array size to be 'num - 1' items */
        *ARR_DIMS(new_acl).add(0) = num - 1;
        SET_VARSIZE(new_acl as *mut c_char, ACL_N_SIZE(num - 1) as int32);
    }

    /*
     * Remove abandoned privileges (cascading revoke).
     */
    if (old_goptions & !new_goptions) != 0 {
        Assert!((*mod_aip).ai_grantee != ACL_ID_PUBLIC);
        new_acl = recursive_revoke(
            new_acl,
            (*mod_aip).ai_grantee,
            old_goptions & !new_goptions,
            ownerId,
            behavior,
        );
    }

    new_acl
}

// ----------------------------------------------------------------------------
// aclnewowner
// ----------------------------------------------------------------------------

/// Update an ACL array to reflect a change of owner.
pub unsafe fn aclnewowner(old_acl: *const Acl, oldOwnerId: Oid, newOwnerId: Oid) -> *mut Acl {
    let new_acl: *mut Acl;
    let new_aip: *mut AclItem;
    let old_aip: *mut AclItem;
    let mut dst_aip: *mut AclItem;
    let mut src_aip: *mut AclItem;
    let mut targ_aip: *mut AclItem;
    let mut newpresent = false;
    let mut dst: c_int;
    let mut src: c_int;
    let mut targ: c_int;
    let num: c_int;

    check_acl(old_acl);

    /*
     * Make a copy of the given ACL, substituting new owner ID for old wherever
     * it appears as either grantor or grantee.
     */
    num = ACL_NUM(old_acl);
    old_aip = ACL_DAT(old_acl);
    new_acl = allocacl(num);
    new_aip = ACL_DAT(new_acl);
    memcpy(
        new_aip as *mut c_void,
        old_aip as *const c_void,
        num as usize * core::mem::size_of::<AclItem>(),
    );
    dst = 0;
    dst_aip = new_aip;
    while dst < num {
        if (*dst_aip).ai_grantor == oldOwnerId {
            (*dst_aip).ai_grantor = newOwnerId;
        } else if (*dst_aip).ai_grantor == newOwnerId {
            newpresent = true;
        }
        if (*dst_aip).ai_grantee == oldOwnerId {
            (*dst_aip).ai_grantee = newOwnerId;
        } else if (*dst_aip).ai_grantee == newOwnerId {
            newpresent = true;
        }
        dst += 1;
        dst_aip = dst_aip.add(1);
    }

    /*
     * If the old ACL contained any references to the new owner, then merge any
     * duplicate entries.  (O(N^2), but unlikely to be the normal case.)
     */
    if newpresent {
        dst = 0;
        targ = 0;
        targ_aip = new_aip;
        while targ < num {
            /* ignore if deleted in an earlier pass */
            if ACLITEM_GET_RIGHTS(*targ_aip) == ACL_NO_RIGHTS {
                targ += 1;
                targ_aip = targ_aip.add(1);
                continue;
            }
            /* find and merge any duplicates */
            src = targ + 1;
            src_aip = targ_aip.add(1);
            while src < num {
                if ACLITEM_GET_RIGHTS(*src_aip) == ACL_NO_RIGHTS {
                    src += 1;
                    src_aip = src_aip.add(1);
                    continue;
                }
                if aclitem_match(targ_aip, src_aip) {
                    ACLITEM_SET_RIGHTS(
                        &mut *targ_aip,
                        ACLITEM_GET_RIGHTS(*targ_aip) | ACLITEM_GET_RIGHTS(*src_aip),
                    );
                    /* mark the duplicate deleted */
                    ACLITEM_SET_RIGHTS(&mut *src_aip, ACL_NO_RIGHTS);
                }
                src += 1;
                src_aip = src_aip.add(1);
            }
            /* and emit to output */
            *new_aip.add(dst as usize) = *targ_aip;
            dst += 1;
            targ += 1;
            targ_aip = targ_aip.add(1);
        }
        /* Adjust array size to be 'dst' items */
        *ARR_DIMS(new_acl).add(0) = dst;
        SET_VARSIZE(new_acl as *mut c_char, ACL_N_SIZE(dst) as int32);
    }

    new_acl
}

// ----------------------------------------------------------------------------
// check_circularity / recursive_revoke
// ----------------------------------------------------------------------------

/// Disallow circular chains of grant options.
unsafe fn check_circularity(old_acl: *const Acl, mod_aip: *const AclItem, ownerId: Oid) {
    let mut acl: *mut Acl;
    let mut aip: *mut AclItem;
    let mut i: c_int;
    let mut num: c_int;
    let mut own_privs: AclMode;

    check_acl(old_acl);

    /*
     * For now, grant options can only be granted to roles, not PUBLIC.
     */
    Assert!((*mod_aip).ai_grantee != ACL_ID_PUBLIC);

    /* The owner always has grant options, no need to check */
    if (*mod_aip).ai_grantor == ownerId {
        return;
    }

    /* Make a working copy */
    acl = allocacl(ACL_NUM(old_acl));
    memcpy(
        acl as *mut c_void,
        old_acl as *const c_void,
        ACL_SIZE(old_acl) as usize,
    );

    /* Zap all grant options of target grantee, plus what depends on 'em */
    'cc_restart: loop {
        num = ACL_NUM(acl);
        aip = ACL_DAT(acl);
        i = 0;
        while i < num {
            if (*aip.add(i as usize)).ai_grantee == (*mod_aip).ai_grantee
                && ACLITEM_GET_GOPTIONS(*aip.add(i as usize)) != ACL_NO_RIGHTS
            {
                let new_acl: *mut Acl;

                /* We'll actually zap ordinary privs too, but no matter */
                new_acl = aclupdate(acl, aip.add(i as usize), ACL_MODECHG_DEL, ownerId, DROP_CASCADE);

                pfree(acl as *mut c_void);
                acl = new_acl;

                continue 'cc_restart;
            }
            i += 1;
        }
        break;
    }

    /* Now we can compute grantor's independently-derived privileges */
    own_privs = aclmask(
        acl,
        (*mod_aip).ai_grantor,
        ownerId,
        ACL_GRANT_OPTION_FOR(ACLITEM_GET_GOPTIONS(*mod_aip)),
        ACLMASK_ALL,
    );
    own_privs = ACL_OPTION_TO_PRIVS(own_privs);

    if (ACLITEM_GET_GOPTIONS(*mod_aip) & !own_privs) != 0 {
        ereport!(
            ERROR,
            errmsg!("grant options cannot be granted back to your own grantor")
        );
    }

    pfree(acl as *mut c_void);
}

/// Ensure that no privilege is "abandoned" (cascading revoke).
unsafe fn recursive_revoke(
    mut acl: *mut Acl,
    grantee: Oid,
    mut revoke_privs: AclMode,
    ownerId: Oid,
    behavior: DropBehavior,
) -> *mut Acl {
    let still_has: AclMode;
    let mut aip: *mut AclItem;
    let mut i: c_int;
    let mut num: c_int;

    check_acl(acl);

    /* The owner can never truly lose grant options, so short-circuit */
    if grantee == ownerId {
        return acl;
    }

    /* The grantee might still have some grant options via another grantor */
    still_has = aclmask(
        acl,
        grantee,
        ownerId,
        ACL_GRANT_OPTION_FOR(revoke_privs),
        ACLMASK_ALL,
    );
    revoke_privs &= !ACL_OPTION_TO_PRIVS(still_has);
    if revoke_privs == ACL_NO_RIGHTS {
        return acl;
    }

    'restart: loop {
        num = ACL_NUM(acl);
        aip = ACL_DAT(acl);
        i = 0;
        while i < num {
            if (*aip.add(i as usize)).ai_grantor == grantee
                && (ACLITEM_GET_PRIVS(*aip.add(i as usize)) & revoke_privs) != 0
            {
                let mut mod_acl: AclItem = core::mem::zeroed();
                let new_acl: *mut Acl;

                if behavior == DROP_RESTRICT {
                    ereport!(ERROR, errmsg!("dependent privileges exist"));
                }

                mod_acl.ai_grantor = grantee;
                mod_acl.ai_grantee = (*aip.add(i as usize)).ai_grantee;
                ACLITEM_SET_PRIVS_GOPTIONS(&mut mod_acl, revoke_privs, revoke_privs);

                new_acl = aclupdate(acl, &mod_acl, ACL_MODECHG_DEL, ownerId, behavior);

                pfree(acl as *mut c_void);
                acl = new_acl;

                continue 'restart;
            }
            i += 1;
        }
        break;
    }

    acl
}

// ----------------------------------------------------------------------------
// aclmask / aclmask_direct / aclmembers
// ----------------------------------------------------------------------------

/// aclmask - compute bitmask of all privileges held by roleid.
pub unsafe fn aclmask(
    acl: *const Acl,
    roleid: Oid,
    ownerId: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> AclMode {
    let mut result: AclMode;
    let mut remaining: AclMode;
    let aidat: *mut AclItem;
    let mut i: c_int;
    let num: c_int;

    /* Null ACL should not happen */
    if acl.is_null() {
        elog!(ERROR, "null ACL");
    }

    check_acl(acl);

    /* Quick exit for mask == 0 */
    if mask == 0 {
        return 0;
    }

    result = 0;

    /* Owner always implicitly has all grant options */
    if (mask & ACLITEM_ALL_GOPTION_BITS) != 0 && has_privs_of_role(roleid, ownerId) {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if if how == ACLMASK_ALL { result == mask } else { result != 0 } {
            return result;
        }
    }

    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);

    /*
     * Check privileges granted directly to roleid or to public
     */
    i = 0;
    while i < num {
        let aidata = &*aidat.add(i as usize);

        if aidata.ai_grantee == ACL_ID_PUBLIC || aidata.ai_grantee == roleid {
            result |= aidata.ai_privs & mask;
            if if how == ACLMASK_ALL { result == mask } else { result != 0 } {
                return result;
            }
        }
        i += 1;
    }

    /*
     * Check privileges granted indirectly via role memberships.
     */
    remaining = mask & !result;
    i = 0;
    while i < num {
        let aidata = &*aidat.add(i as usize);

        if aidata.ai_grantee == ACL_ID_PUBLIC || aidata.ai_grantee == roleid {
            i += 1;
            continue; /* already checked it */
        }

        if (aidata.ai_privs & remaining) != 0 && has_privs_of_role(roleid, aidata.ai_grantee) {
            result |= aidata.ai_privs & mask;
            if if how == ACLMASK_ALL { result == mask } else { result != 0 } {
                return result;
            }
            remaining = mask & !result;
        }
        i += 1;
    }

    result
}

/// aclmask_direct - like aclmask but only directly-held privileges.
unsafe fn aclmask_direct(
    acl: *const Acl,
    roleid: Oid,
    ownerId: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> AclMode {
    let mut result: AclMode;
    let aidat: *mut AclItem;
    let mut i: c_int;
    let num: c_int;

    if acl.is_null() {
        elog!(ERROR, "null ACL");
    }

    check_acl(acl);

    if mask == 0 {
        return 0;
    }

    result = 0;

    /* Owner always implicitly has all grant options */
    if (mask & ACLITEM_ALL_GOPTION_BITS) != 0 && roleid == ownerId {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if if how == ACLMASK_ALL { result == mask } else { result != 0 } {
            return result;
        }
    }

    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);

    /*
     * Check privileges granted directly to roleid (and not to public)
     */
    i = 0;
    while i < num {
        let aidata = &*aidat.add(i as usize);

        if aidata.ai_grantee == roleid {
            result |= aidata.ai_privs & mask;
            if if how == ACLMASK_ALL { result == mask } else { result != 0 } {
                return result;
            }
        }
        i += 1;
    }

    result
}

/// aclmembers - find all roleids mentioned in an Acl (sorted, distinct).
pub unsafe fn aclmembers(acl: *const Acl, roleids: *mut *mut Oid) -> c_int {
    let list: *mut Oid;
    let acldat: *const AclItem;
    let mut i: c_int;
    let mut j: c_int;

    if acl.is_null() || ACL_NUM(acl) == 0 {
        *roleids = core::ptr::null_mut();
        return 0;
    }

    check_acl(acl);

    /* Allocate the worst-case space requirement */
    list = palloc(ACL_NUM(acl) as usize * 2 * core::mem::size_of::<Oid>()) as *mut Oid;
    acldat = ACL_DAT(acl);

    /*
     * Walk the ACL collecting mentioned RoleIds.
     */
    j = 0;
    i = 0;
    while i < ACL_NUM(acl) {
        let ai = &*acldat.add(i as usize);

        if ai.ai_grantee != ACL_ID_PUBLIC {
            *list.add(j as usize) = ai.ai_grantee;
            j += 1;
        }
        /* grantor is currently never PUBLIC, but let's check anyway */
        if ai.ai_grantor != ACL_ID_PUBLIC {
            *list.add(j as usize) = ai.ai_grantor;
            j += 1;
        }
        i += 1;
    }

    /* Sort the array */
    qsort(
        list as *mut c_void,
        j as usize,
        core::mem::size_of::<Oid>(),
        acl_oid_cmp_c,
    );

    *roleids = list;

    /* Remove duplicates from the array */
    qunique(list as *mut c_void, j as usize, core::mem::size_of::<Oid>(), oid_cmp) as c_int
}

// ----------------------------------------------------------------------------
// aclinsert / aclremove / aclcontains / makeaclitem
// ----------------------------------------------------------------------------

/// aclinsert (exported function) - no longer supported.
pub unsafe fn aclinsert(fcinfo: FunctionCallInfo) -> Datum {
    ereport!(ERROR, errmsg!("aclinsert is no longer supported"));
    
    PG_RETURN_NULL!(fcinfo) /* keep compiler quiet */
}

pub unsafe fn aclremove(fcinfo: FunctionCallInfo) -> Datum {
    ereport!(ERROR, errmsg!("aclremove is no longer supported"));
    
    PG_RETURN_NULL!(fcinfo) /* keep compiler quiet */
}

pub unsafe fn aclcontains(fcinfo: FunctionCallInfo) -> Datum {
    let acl = DatumGetAclP(PG_GETARG_DATUM!(fcinfo, 0));
    let aip = DatumGetAclItemP(PG_GETARG_DATUM!(fcinfo, 1));
    let aidat: *mut AclItem;
    let mut i: c_int;
    let num: c_int;

    check_acl(acl);
    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);
    i = 0;
    while i < num {
        if (*aip).ai_grantee == (*aidat.add(i as usize)).ai_grantee
            && (*aip).ai_grantor == (*aidat.add(i as usize)).ai_grantor
            && (ACLITEM_GET_RIGHTS(*aip) & ACLITEM_GET_RIGHTS(*aidat.add(i as usize)))
                == ACLITEM_GET_RIGHTS(*aip)
        {
            PG_RETURN_BOOL!(true);
        }
        i += 1;
    }
    PG_RETURN_BOOL!(false)
}

pub unsafe fn makeaclitem(fcinfo: FunctionCallInfo) -> Datum {
    let grantee = PG_GETARG_OID!(fcinfo, 0);
    let grantor = PG_GETARG_OID!(fcinfo, 1);
    let privtext = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let goption = PG_GETARG_BOOL!(fcinfo, 3);
    let result: *mut AclItem;
    let priv_: AclMode;
    static any_priv_map: [priv_map; 17] = [
        priv_map { name: c"SELECT".as_ptr(), value: ACL_SELECT },
        priv_map { name: c"INSERT".as_ptr(), value: ACL_INSERT },
        priv_map { name: c"UPDATE".as_ptr(), value: ACL_UPDATE },
        priv_map { name: c"DELETE".as_ptr(), value: ACL_DELETE },
        priv_map { name: c"TRUNCATE".as_ptr(), value: ACL_TRUNCATE },
        priv_map { name: c"REFERENCES".as_ptr(), value: ACL_REFERENCES },
        priv_map { name: c"TRIGGER".as_ptr(), value: ACL_TRIGGER },
        priv_map { name: c"EXECUTE".as_ptr(), value: ACL_EXECUTE },
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"CREATE".as_ptr(), value: ACL_CREATE },
        priv_map { name: c"TEMP".as_ptr(), value: ACL_CREATE_TEMP },
        priv_map { name: c"TEMPORARY".as_ptr(), value: ACL_CREATE_TEMP },
        priv_map { name: c"CONNECT".as_ptr(), value: ACL_CONNECT },
        priv_map { name: c"SET".as_ptr(), value: ACL_SET },
        priv_map { name: c"ALTER SYSTEM".as_ptr(), value: ACL_ALTER_SYSTEM },
        priv_map { name: c"MAINTAIN".as_ptr(), value: ACL_MAINTAIN },
        priv_map { name: core::ptr::null(), value: 0 },
    ];

    priv_ = convert_any_priv_string(privtext, any_priv_map.as_ptr());

    result = palloc(core::mem::size_of::<AclItem>()) as *mut AclItem;

    (*result).ai_grantee = grantee;
    (*result).ai_grantor = grantor;

    ACLITEM_SET_PRIVS_GOPTIONS(
        &mut *result,
        priv_,
        if goption { priv_ } else { ACL_NO_RIGHTS },
    );

    PG_RETURN_POINTER!(result)
}

// ----------------------------------------------------------------------------
// convert_any_priv_string / convert_aclright_to_string
// ----------------------------------------------------------------------------

/// convert_any_priv_string: recognize privilege strings for has_foo_privilege.
unsafe fn convert_any_priv_string(
    priv_type_text: *const text,
    privileges: *const priv_map,
) -> AclMode {
    let mut result: AclMode = 0;
    let priv_type = text_to_cstring(priv_type_text as *const text);
    let mut chunk = priv_type;
    let mut next_chunk: *mut c_char;

    /* We rely on priv_type being a private, modifiable string */
    while !chunk.is_null() {
        let mut chunk_len: c_int;
        let mut this_priv: *const priv_map;

        /* Split string at commas */
        next_chunk = strchr(chunk, b',' as c_int);
        if !next_chunk.is_null() {
            *next_chunk = 0;
            next_chunk = next_chunk.add(1);
        }

        /* Drop leading/trailing whitespace in this chunk */
        while *chunk != 0 && isspace(*chunk as c_int) != 0 {
            chunk = chunk.add(1);
        }
        chunk_len = strlen(chunk) as c_int;
        while chunk_len > 0 && isspace(*chunk.add(chunk_len as usize - 1) as c_int) != 0 {
            chunk_len -= 1;
        }
        *chunk.add(chunk_len as usize) = 0;

        /* Match to the privileges list */
        this_priv = privileges;
        while !(*this_priv).name.is_null() {
            if pg_strcasecmp((*this_priv).name, chunk) == 0 {
                result |= (*this_priv).value;
                break;
            }
            this_priv = this_priv.add(1);
        }
        if (*this_priv).name.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized privilege type: \"{}\"",
                    std::ffi::CStr::from_ptr(chunk).to_string_lossy()
                )
            );
        }

        chunk = next_chunk;
    }

    pfree(priv_type as *mut c_void);
    result
}

unsafe fn convert_aclright_to_string(aclright: c_int) -> *const c_char {
    match aclright as AclMode {
        ACL_INSERT => c"INSERT".as_ptr(),
        ACL_SELECT => c"SELECT".as_ptr(),
        ACL_UPDATE => c"UPDATE".as_ptr(),
        ACL_DELETE => c"DELETE".as_ptr(),
        ACL_TRUNCATE => c"TRUNCATE".as_ptr(),
        ACL_REFERENCES => c"REFERENCES".as_ptr(),
        ACL_TRIGGER => c"TRIGGER".as_ptr(),
        ACL_EXECUTE => c"EXECUTE".as_ptr(),
        ACL_USAGE => c"USAGE".as_ptr(),
        ACL_CREATE => c"CREATE".as_ptr(),
        ACL_CREATE_TEMP => c"TEMPORARY".as_ptr(),
        ACL_CONNECT => c"CONNECT".as_ptr(),
        ACL_SET => c"SET".as_ptr(),
        ACL_ALTER_SYSTEM => c"ALTER SYSTEM".as_ptr(),
        ACL_MAINTAIN => c"MAINTAIN".as_ptr(),
        _ => {
            elog!(ERROR, "unrecognized aclright: {}", aclright);
            #[allow(unreachable_code)]
            core::ptr::null()
        }
    }
}

// ----------------------------------------------------------------------------
// aclexplode  (set-returning function)
// ----------------------------------------------------------------------------

/// Convert an aclitem[] to a table of (grantor, grantee, priv, is_grantable).
pub unsafe fn aclexplode(fcinfo: FunctionCallInfo) -> Datum {
    let acl = DatumGetAclP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut funcctx: *mut FuncCallContext;
    let idx: *mut c_int;
    let aidat: *mut AclItem;

    if SRF_IS_FIRSTCALL() {
        let tupdesc: TupleDesc;
        let oldcontext: MemoryContext;

        check_acl(acl);

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(4);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"grantor".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"grantee".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(
            tupdesc,
            3 as AttrNumber,
            c"privilege_type".as_ptr(),
            TEXTOID,
            -1,
            0,
        );
        TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"is_grantable".as_ptr(), BOOLOID, -1, 0);

        (*funcctx).tuple_desc = BlessTupleDesc(tupdesc);

        /* allocate memory for user context */
        let idx0 = palloc(core::mem::size_of::<c_int>() * 2) as *mut c_int;
        *idx0.add(0) = 0; /* ACL array item index */
        *idx0.add(1) = -1; /* privilege type counter */
        (*funcctx).user_fctx = idx0 as *mut c_void;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    idx = (*funcctx).user_fctx as *mut c_int;
    aidat = ACL_DAT(acl);

    /* need test here in case acl has no items */
    while *idx.add(0) < ACL_NUM(acl) {
        let aidata: *mut AclItem;
        let priv_bit: AclMode;

        *idx.add(1) += 1;
        if *idx.add(1) == N_ACL_RIGHTS {
            *idx.add(1) = 0;
            *idx.add(0) += 1;
            if *idx.add(0) >= ACL_NUM(acl) {
                /* done */
                break;
            }
        }
        aidata = aidat.add(*idx.add(0) as usize);
        priv_bit = (1u64) << *idx.add(1);

        if ACLITEM_GET_PRIVS(*aidata) & priv_bit != 0 {
            let result: Datum;
            let mut values: [Datum; 4] = [0; 4];
            let nulls: [bool; 4] = [false; 4];
            let tuple: HeapTuple;

            values[0] = ObjectIdGetDatum((*aidata).ai_grantor);
            values[1] = ObjectIdGetDatum((*aidata).ai_grantee);
            values[2] = CStringGetTextDatum(convert_aclright_to_string(priv_bit as c_int));
            values[3] = BoolGetDatum((ACLITEM_GET_GOPTIONS(*aidata) & priv_bit) != 0);

            tuple = heap_form_tuple((*funcctx).tuple_desc, values.as_ptr(), nulls.as_ptr());
            result = HeapTupleGetDatum(tuple);

            return SRF_RETURN_NEXT(funcctx, result);
        }
    }

    SRF_RETURN_DONE(funcctx)
}

// ----------------------------------------------------------------------------
// has_table_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_table_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let rolename = PG_GETARG_NAME!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*rolename));
    let tableoid = convert_table_name(tablename);
    let mode = convert_table_priv_string(priv_type_text);
    let aclresult = pg_class_aclcheck(tableoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_table_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let tableoid = convert_table_name(tablename);
    let mode = convert_table_priv_string(priv_type_text);
    let aclresult = pg_class_aclcheck(tableoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_table_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_table_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_table_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let tableoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_table_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_table_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let tableoid = convert_table_name(tablename);
    let mode = convert_table_priv_string(priv_type_text);
    let aclresult = pg_class_aclcheck(tableoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_table_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_table_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

/// Given a table name expressed as a string, look it up and return Oid.
unsafe fn convert_table_name(tablename: *mut text) -> Oid {
    let relrv: *mut RangeVar;
    relrv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
    /* We might not even have permissions on this relation; don't lock it. */
    RangeVarGetRelid(relrv, NoLock, false)
}

unsafe fn convert_table_priv_string(priv_type_text: *mut text) -> AclMode {
    static table_priv_map: [priv_map; 17] = [
        priv_map { name: c"SELECT".as_ptr(), value: ACL_SELECT },
        priv_map { name: c"SELECT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_SELECT) },
        priv_map { name: c"INSERT".as_ptr(), value: ACL_INSERT },
        priv_map { name: c"INSERT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_INSERT) },
        priv_map { name: c"UPDATE".as_ptr(), value: ACL_UPDATE },
        priv_map { name: c"UPDATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_UPDATE) },
        priv_map { name: c"DELETE".as_ptr(), value: ACL_DELETE },
        priv_map { name: c"DELETE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_DELETE) },
        priv_map { name: c"TRUNCATE".as_ptr(), value: ACL_TRUNCATE },
        priv_map { name: c"TRUNCATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_TRUNCATE) },
        priv_map { name: c"REFERENCES".as_ptr(), value: ACL_REFERENCES },
        priv_map { name: c"REFERENCES WITH GRANT OPTION".as_ptr(), value: gopt(ACL_REFERENCES) },
        priv_map { name: c"TRIGGER".as_ptr(), value: ACL_TRIGGER },
        priv_map { name: c"TRIGGER WITH GRANT OPTION".as_ptr(), value: gopt(ACL_TRIGGER) },
        priv_map { name: c"MAINTAIN".as_ptr(), value: ACL_MAINTAIN },
        priv_map { name: c"MAINTAIN WITH GRANT OPTION".as_ptr(), value: gopt(ACL_MAINTAIN) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, table_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_sequence_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_sequence_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let rolename = PG_GETARG_NAME!(fcinfo, 0);
    let sequencename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*rolename));
    let mode = convert_sequence_priv_string(priv_type_text);
    let sequenceoid = convert_table_name(sequencename);
    if get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                std::ffi::CStr::from_ptr(text_to_cstring(sequencename as *const text))
                    .to_string_lossy()
            )
        );
    }
    let aclresult = pg_class_aclcheck(sequenceoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_sequence_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let sequencename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_sequence_priv_string(priv_type_text);
    let sequenceoid = convert_table_name(sequencename);
    if get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                std::ffi::CStr::from_ptr(text_to_cstring(sequencename as *const text))
                    .to_string_lossy()
            )
        );
    }
    let aclresult = pg_class_aclcheck(sequenceoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_sequence_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let sequenceoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_sequence_priv_string(priv_type_text);
    let relkind = get_rel_relkind(sequenceoid);
    if relkind == 0 {
        PG_RETURN_NULL!(fcinfo);
    } else if relkind != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                std::ffi::CStr::from_ptr(get_rel_name(sequenceoid)).to_string_lossy()
            )
        );
    }
    let mut is_missing = false;
    let aclresult = pg_class_aclcheck_ext(sequenceoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_sequence_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let sequenceoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_sequence_priv_string(priv_type_text);
    let relkind = get_rel_relkind(sequenceoid);
    if relkind == 0 {
        PG_RETURN_NULL!(fcinfo);
    } else if relkind != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                std::ffi::CStr::from_ptr(get_rel_name(sequenceoid)).to_string_lossy()
            )
        );
    }
    let mut is_missing = false;
    let aclresult = pg_class_aclcheck_ext(sequenceoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_sequence_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let sequencename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_sequence_priv_string(priv_type_text);
    let sequenceoid = convert_table_name(sequencename);
    if get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                std::ffi::CStr::from_ptr(text_to_cstring(sequencename as *const text))
                    .to_string_lossy()
            )
        );
    }
    let aclresult = pg_class_aclcheck(sequenceoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_sequence_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let sequenceoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_sequence_priv_string(priv_type_text);
    let relkind = get_rel_relkind(sequenceoid);
    if relkind == 0 {
        PG_RETURN_NULL!(fcinfo);
    } else if relkind != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                std::ffi::CStr::from_ptr(get_rel_name(sequenceoid)).to_string_lossy()
            )
        );
    }
    let mut is_missing = false;
    let aclresult = pg_class_aclcheck_ext(sequenceoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_sequence_priv_string(priv_type_text: *mut text) -> AclMode {
    static sequence_priv_map: [priv_map; 7] = [
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_USAGE) },
        priv_map { name: c"SELECT".as_ptr(), value: ACL_SELECT },
        priv_map { name: c"SELECT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_SELECT) },
        priv_map { name: c"UPDATE".as_ptr(), value: ACL_UPDATE },
        priv_map { name: c"UPDATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_UPDATE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, sequence_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_any_column_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_any_column_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let rolename = PG_GETARG_NAME!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*rolename));
    let tableoid = convert_table_name(tablename);
    let mode = convert_column_priv_string(priv_type_text);
    let mut aclresult = pg_class_aclcheck(tableoid, roleid, mode);
    if aclresult != ACLCHECK_OK {
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_any_column_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let tableoid = convert_table_name(tablename);
    let mode = convert_column_priv_string(priv_type_text);
    let mut aclresult = pg_class_aclcheck(tableoid, roleid, mode);
    if aclresult != ACLCHECK_OK {
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_any_column_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_column_priv_string(priv_type_text);
    let mut is_missing = false;
    let mut aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if aclresult != ACLCHECK_OK {
        if is_missing {
            PG_RETURN_NULL!(fcinfo);
        }
        aclresult = pg_attribute_aclcheck_all_ext(tableoid, roleid, mode, ACLMASK_ANY, &mut is_missing);
        if is_missing {
            PG_RETURN_NULL!(fcinfo);
        }
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_any_column_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let tableoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_column_priv_string(priv_type_text);
    let mut is_missing = false;
    let mut aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if aclresult != ACLCHECK_OK {
        if is_missing {
            PG_RETURN_NULL!(fcinfo);
        }
        aclresult = pg_attribute_aclcheck_all_ext(tableoid, roleid, mode, ACLMASK_ANY, &mut is_missing);
        if is_missing {
            PG_RETURN_NULL!(fcinfo);
        }
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_any_column_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let tableoid = convert_table_name(tablename);
    let mode = convert_column_priv_string(priv_type_text);
    let mut aclresult = pg_class_aclcheck(tableoid, roleid, mode);
    if aclresult != ACLCHECK_OK {
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_any_column_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_column_priv_string(priv_type_text);
    let mut is_missing = false;
    let mut aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if aclresult != ACLCHECK_OK {
        if is_missing {
            PG_RETURN_NULL!(fcinfo);
        }
        aclresult = pg_attribute_aclcheck_all_ext(tableoid, roleid, mode, ACLMASK_ANY, &mut is_missing);
        if is_missing {
            PG_RETURN_NULL!(fcinfo);
        }
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

// ----------------------------------------------------------------------------
// has_column_privilege variants
// ----------------------------------------------------------------------------

/// column_privilege_check: 1 if have priv, 0 if not, -1 if dropped column/table.
unsafe fn column_privilege_check(
    tableoid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mode: AclMode,
) -> c_int {
    let mut aclresult: AclResult;
    let mut is_missing = false;

    if attnum == InvalidAttrNumber {
        return -1;
    }

    aclresult = pg_attribute_aclcheck_ext(tableoid, attnum, roleid, mode, &mut is_missing);
    if aclresult == ACLCHECK_OK {
        return 1;
    } else if is_missing {
        return -1;
    }

    aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &mut is_missing);
    if aclresult == ACLCHECK_OK {
        1
    } else if is_missing {
        -1
    } else {
        0
    }
}

pub unsafe fn has_column_privilege_name_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let rolename = PG_GETARG_NAME!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let column = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*rolename));
    let tableoid = convert_table_name(tablename);
    let colattnum = convert_column_name(tableoid, column);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_name_name_attnum(fcinfo: FunctionCallInfo) -> Datum {
    let rolename = PG_GETARG_NAME!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let colattnum = PG_GETARG_INT16!(fcinfo, 2);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*rolename));
    let tableoid = convert_table_name(tablename);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_name_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let column = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let colattnum = convert_column_name(tableoid, column);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_name_id_attnum(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let colattnum = PG_GETARG_INT16!(fcinfo, 2);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_id_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let column = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let tableoid = convert_table_name(tablename);
    let colattnum = convert_column_name(tableoid, column);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_id_name_attnum(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let colattnum = PG_GETARG_INT16!(fcinfo, 2);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let tableoid = convert_table_name(tablename);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_id_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let column = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let colattnum = convert_column_name(tableoid, column);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_id_id_attnum(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tableoid = PG_GETARG_OID!(fcinfo, 1);
    let colattnum = PG_GETARG_INT16!(fcinfo, 2);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 3) as *mut text;
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let column = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = GetUserId();
    let tableoid = convert_table_name(tablename);
    let colattnum = convert_column_name(tableoid, column);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_name_attnum(fcinfo: FunctionCallInfo) -> Datum {
    let tablename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let colattnum = PG_GETARG_INT16!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = GetUserId();
    let tableoid = convert_table_name(tablename);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let tableoid = PG_GETARG_OID!(fcinfo, 0);
    let column = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = GetUserId();
    let colattnum = convert_column_name(tableoid, column);
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

pub unsafe fn has_column_privilege_id_attnum(fcinfo: FunctionCallInfo) -> Datum {
    let tableoid = PG_GETARG_OID!(fcinfo, 0);
    let colattnum = PG_GETARG_INT16!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = GetUserId();
    let mode = convert_column_priv_string(priv_type_text);
    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if privresult < 0 {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(privresult != 0)
}

/// Look up a column name (string) on a table OID; InvalidAttrNumber on caller-NULL cases.
unsafe fn convert_column_name(tableoid: Oid, column: *mut text) -> AttrNumber {
    let colname: *mut c_char;
    let attTuple: HeapTuple;
    let attnum: AttrNumber;

    colname = text_to_cstring(column as *const text);

    /*
     * We don't use get_attnum() here because it would report dropped columns
     * as nonexistent.  We need to treat dropped columns differently.
     */
    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(tableoid), CStringGetDatum(colname));
    if HeapTupleIsValid(attTuple) {
        let attributeForm = GETSTRUCT(attTuple) as Form_pg_attribute;
        /* We want to return NULL for dropped columns */
        if (*attributeForm).attisdropped {
            attnum = InvalidAttrNumber;
        } else {
            attnum = (*attributeForm).attnum;
        }
        ReleaseSysCache(attTuple);
    } else {
        let tablename = get_rel_name(tableoid);

        /*
         * If the table OID is bogus, or just dropped, NULL back -> behave like
         * attisdropped case.
         */
        if !tablename.is_null() {
            /* tableoid exists, colname does not, so throw error */
            ereport!(
                ERROR,
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(tablename).to_string_lossy()
                )
            );
        }
        /* tableoid doesn't exist, so act like attisdropped case */
        attnum = InvalidAttrNumber;
    }

    pfree(colname as *mut c_void);
    attnum
}

unsafe fn convert_column_priv_string(priv_type_text: *mut text) -> AclMode {
    static column_priv_map: [priv_map; 9] = [
        priv_map { name: c"SELECT".as_ptr(), value: ACL_SELECT },
        priv_map { name: c"SELECT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_SELECT) },
        priv_map { name: c"INSERT".as_ptr(), value: ACL_INSERT },
        priv_map { name: c"INSERT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_INSERT) },
        priv_map { name: c"UPDATE".as_ptr(), value: ACL_UPDATE },
        priv_map { name: c"UPDATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_UPDATE) },
        priv_map { name: c"REFERENCES".as_ptr(), value: ACL_REFERENCES },
        priv_map { name: c"REFERENCES WITH GRANT OPTION".as_ptr(), value: gopt(ACL_REFERENCES) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, column_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_database_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_database_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let databasename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let databaseoid = convert_database_name(databasename);
    let mode = convert_database_priv_string(priv_type_text);
    let aclresult = object_aclcheck(DatabaseRelationId, databaseoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_database_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let databasename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let databaseoid = convert_database_name(databasename);
    let mode = convert_database_priv_string(priv_type_text);
    let aclresult = object_aclcheck(DatabaseRelationId, databaseoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_database_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let databaseoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_database_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(DatabaseRelationId, databaseoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_database_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let databaseoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_database_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(DatabaseRelationId, databaseoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_database_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let databasename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let databaseoid = convert_database_name(databasename);
    let mode = convert_database_priv_string(priv_type_text);
    let aclresult = object_aclcheck(DatabaseRelationId, databaseoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_database_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let databaseoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_database_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(DatabaseRelationId, databaseoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_database_name(databasename: *mut text) -> Oid {
    let dbname = text_to_cstring(databasename as *const text);
    get_database_oid(dbname, false)
}

unsafe fn convert_database_priv_string(priv_type_text: *mut text) -> AclMode {
    static database_priv_map: [priv_map; 9] = [
        priv_map { name: c"CREATE".as_ptr(), value: ACL_CREATE },
        priv_map { name: c"CREATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"TEMPORARY".as_ptr(), value: ACL_CREATE_TEMP },
        priv_map { name: c"TEMPORARY WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE_TEMP) },
        priv_map { name: c"TEMP".as_ptr(), value: ACL_CREATE_TEMP },
        priv_map { name: c"TEMP WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE_TEMP) },
        priv_map { name: c"CONNECT".as_ptr(), value: ACL_CONNECT },
        priv_map { name: c"CONNECT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CONNECT) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, database_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_foreign_data_wrapper_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_foreign_data_wrapper_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let fdwname = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let fdwid = convert_foreign_data_wrapper_name(fdwname);
    let mode = convert_foreign_data_wrapper_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ForeignDataWrapperRelationId, fdwid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_foreign_data_wrapper_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let fdwname = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let fdwid = convert_foreign_data_wrapper_name(fdwname);
    let mode = convert_foreign_data_wrapper_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ForeignDataWrapperRelationId, fdwid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_foreign_data_wrapper_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let fdwid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_foreign_data_wrapper_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ForeignDataWrapperRelationId, fdwid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_foreign_data_wrapper_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let fdwid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_foreign_data_wrapper_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ForeignDataWrapperRelationId, fdwid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_foreign_data_wrapper_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let fdwname = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let fdwid = convert_foreign_data_wrapper_name(fdwname);
    let mode = convert_foreign_data_wrapper_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ForeignDataWrapperRelationId, fdwid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_foreign_data_wrapper_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let fdwid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_foreign_data_wrapper_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ForeignDataWrapperRelationId, fdwid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_foreign_data_wrapper_name(fdwname: *mut text) -> Oid {
    let fdwstr = text_to_cstring(fdwname as *const text);
    get_foreign_data_wrapper_oid(fdwstr, false)
}

unsafe fn convert_foreign_data_wrapper_priv_string(priv_type_text: *mut text) -> AclMode {
    static foreign_data_wrapper_priv_map: [priv_map; 3] = [
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_USAGE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, foreign_data_wrapper_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_function_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_function_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let functionname = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let functionoid = convert_function_name(functionname);
    let mode = convert_function_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ProcedureRelationId, functionoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_function_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let functionname = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let functionoid = convert_function_name(functionname);
    let mode = convert_function_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ProcedureRelationId, functionoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_function_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let functionoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_function_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ProcedureRelationId, functionoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_function_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let functionoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_function_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ProcedureRelationId, functionoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_function_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let functionname = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let functionoid = convert_function_name(functionname);
    let mode = convert_function_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ProcedureRelationId, functionoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_function_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let functionoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_function_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ProcedureRelationId, functionoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_function_name(functionname: *mut text) -> Oid {
    let funcname = text_to_cstring(functionname as *const text);
    let oid: Oid;

    oid = DatumGetObjectId(DirectFunctionCall1!(regprocedurein, CStringGetDatum(funcname)));

    if !OidIsValid(oid) {
        ereport!(
            ERROR,
            errmsg!(
                "function \"{}\" does not exist",
                std::ffi::CStr::from_ptr(funcname).to_string_lossy()
            )
        );
    }

    oid
}

unsafe fn convert_function_priv_string(priv_type_text: *mut text) -> AclMode {
    static function_priv_map: [priv_map; 3] = [
        priv_map { name: c"EXECUTE".as_ptr(), value: ACL_EXECUTE },
        priv_map { name: c"EXECUTE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_EXECUTE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, function_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_language_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_language_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let languagename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let languageoid = convert_language_name(languagename);
    let mode = convert_language_priv_string(priv_type_text);
    let aclresult = object_aclcheck(LanguageRelationId, languageoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_language_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let languagename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let languageoid = convert_language_name(languagename);
    let mode = convert_language_priv_string(priv_type_text);
    let aclresult = object_aclcheck(LanguageRelationId, languageoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_language_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let languageoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_language_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(LanguageRelationId, languageoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_language_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let languageoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_language_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(LanguageRelationId, languageoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_language_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let languagename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let languageoid = convert_language_name(languagename);
    let mode = convert_language_priv_string(priv_type_text);
    let aclresult = object_aclcheck(LanguageRelationId, languageoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_language_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let languageoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_language_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(LanguageRelationId, languageoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_language_name(languagename: *mut text) -> Oid {
    let langname = text_to_cstring(languagename as *const text);
    get_language_oid(langname, false)
}

unsafe fn convert_language_priv_string(priv_type_text: *mut text) -> AclMode {
    static language_priv_map: [priv_map; 3] = [
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_USAGE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, language_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_schema_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_schema_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let schemaname = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let schemaoid = convert_schema_name(schemaname);
    let mode = convert_schema_priv_string(priv_type_text);
    let aclresult = object_aclcheck(NamespaceRelationId, schemaoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_schema_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let schemaname = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let schemaoid = convert_schema_name(schemaname);
    let mode = convert_schema_priv_string(priv_type_text);
    let aclresult = object_aclcheck(NamespaceRelationId, schemaoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_schema_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let schemaoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_schema_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(NamespaceRelationId, schemaoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_schema_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let schemaoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_schema_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(NamespaceRelationId, schemaoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_schema_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let schemaname = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let schemaoid = convert_schema_name(schemaname);
    let mode = convert_schema_priv_string(priv_type_text);
    let aclresult = object_aclcheck(NamespaceRelationId, schemaoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_schema_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let schemaoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_schema_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(NamespaceRelationId, schemaoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_schema_name(schemaname: *mut text) -> Oid {
    let nspname = text_to_cstring(schemaname as *const text);
    get_namespace_oid(nspname, false)
}

unsafe fn convert_schema_priv_string(priv_type_text: *mut text) -> AclMode {
    static schema_priv_map: [priv_map; 5] = [
        priv_map { name: c"CREATE".as_ptr(), value: ACL_CREATE },
        priv_map { name: c"CREATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_USAGE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, schema_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_server_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_server_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let servername = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let serverid = convert_server_name(servername);
    let mode = convert_server_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ForeignServerRelationId, serverid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_server_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let servername = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let serverid = convert_server_name(servername);
    let mode = convert_server_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ForeignServerRelationId, serverid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_server_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let serverid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_server_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ForeignServerRelationId, serverid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_server_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let serverid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_server_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ForeignServerRelationId, serverid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_server_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let servername = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let serverid = convert_server_name(servername);
    let mode = convert_server_priv_string(priv_type_text);
    let aclresult = object_aclcheck(ForeignServerRelationId, serverid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_server_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let serverid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_server_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(ForeignServerRelationId, serverid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_server_name(servername: *mut text) -> Oid {
    let serverstr = text_to_cstring(servername as *const text);
    get_foreign_server_oid(serverstr, false)
}

unsafe fn convert_server_priv_string(priv_type_text: *mut text) -> AclMode {
    static server_priv_map: [priv_map; 3] = [
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_USAGE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, server_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_tablespace_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_tablespace_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let tablespacename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let tablespaceoid = convert_tablespace_name(tablespacename);
    let mode = convert_tablespace_priv_string(priv_type_text);
    let aclresult = object_aclcheck(TableSpaceRelationId, tablespaceoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_tablespace_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let tablespacename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let tablespaceoid = convert_tablespace_name(tablespacename);
    let mode = convert_tablespace_priv_string(priv_type_text);
    let aclresult = object_aclcheck(TableSpaceRelationId, tablespaceoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_tablespace_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let tablespaceoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_tablespace_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(TableSpaceRelationId, tablespaceoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_tablespace_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let tablespaceoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_tablespace_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(TableSpaceRelationId, tablespaceoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_tablespace_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tablespacename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let tablespaceoid = convert_tablespace_name(tablespacename);
    let mode = convert_tablespace_priv_string(priv_type_text);
    let aclresult = object_aclcheck(TableSpaceRelationId, tablespaceoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_tablespace_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let tablespaceoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_tablespace_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(TableSpaceRelationId, tablespaceoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_tablespace_name(tablespacename: *mut text) -> Oid {
    let spcname = text_to_cstring(tablespacename as *const text);
    get_tablespace_oid(spcname, false)
}

unsafe fn convert_tablespace_priv_string(priv_type_text: *mut text) -> AclMode {
    static tablespace_priv_map: [priv_map; 3] = [
        priv_map { name: c"CREATE".as_ptr(), value: ACL_CREATE },
        priv_map { name: c"CREATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, tablespace_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_type_privilege variants
// ----------------------------------------------------------------------------

pub unsafe fn has_type_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let typename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let typeoid = convert_type_name(typename);
    let mode = convert_type_priv_string(priv_type_text);
    let aclresult = object_aclcheck(TypeRelationId, typeoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_type_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let typename = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let typeoid = convert_type_name(typename);
    let mode = convert_type_priv_string(priv_type_text);
    let aclresult = object_aclcheck(TypeRelationId, typeoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_type_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let typeoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let mode = convert_type_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(TypeRelationId, typeoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_type_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let typeoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_type_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(TypeRelationId, typeoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_type_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let typename = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let typeoid = convert_type_name(typename);
    let mode = convert_type_priv_string(priv_type_text);
    let aclresult = object_aclcheck(TypeRelationId, typeoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn has_type_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let typeoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_type_priv_string(priv_type_text);
    let mut is_missing = false;
    let aclresult = object_aclcheck_ext(TypeRelationId, typeoid, roleid, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

unsafe fn convert_type_name(typename: *mut text) -> Oid {
    let typname = text_to_cstring(typename as *const text);
    let oid: Oid;

    oid = DatumGetObjectId(DirectFunctionCall1!(regtypein, CStringGetDatum(typname)));

    if !OidIsValid(oid) {
        ereport!(
            ERROR,
            errmsg!(
                "type \"{}\" does not exist",
                std::ffi::CStr::from_ptr(typname).to_string_lossy()
            )
        );
    }

    oid
}

unsafe fn convert_type_priv_string(priv_type_text: *mut text) -> AclMode {
    static type_priv_map: [priv_map; 3] = [
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_USAGE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, type_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_parameter_privilege variants
// ----------------------------------------------------------------------------

unsafe fn has_param_priv_byname(roleid: Oid, parameter: *const text, priv_: AclMode) -> bool {
    let paramstr = text_to_cstring(parameter as *const text);
    pg_parameter_aclcheck(paramstr, roleid, priv_) == ACLCHECK_OK
}

pub unsafe fn has_parameter_privilege_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let parameter = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_ = convert_parameter_priv_string(PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text);
    let roleid = get_role_oid_or_public(NameStr(&*username));
    PG_RETURN_BOOL!(has_param_priv_byname(roleid, parameter, priv_))
}

pub unsafe fn has_parameter_privilege_name(fcinfo: FunctionCallInfo) -> Datum {
    let parameter = PG_GETARG_TEXT_PP!(fcinfo, 0) as *mut text;
    let priv_ = convert_parameter_priv_string(PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text);
    PG_RETURN_BOOL!(has_param_priv_byname(GetUserId(), parameter, priv_))
}

pub unsafe fn has_parameter_privilege_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let parameter = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let priv_ = convert_parameter_priv_string(PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text);
    PG_RETURN_BOOL!(has_param_priv_byname(roleid, parameter, priv_))
}

unsafe fn convert_parameter_priv_string(priv_text: *mut text) -> AclMode {
    static parameter_priv_map: [priv_map; 5] = [
        priv_map { name: c"SET".as_ptr(), value: ACL_SET },
        priv_map { name: c"SET WITH GRANT OPTION".as_ptr(), value: gopt(ACL_SET) },
        priv_map { name: c"ALTER SYSTEM".as_ptr(), value: ACL_ALTER_SYSTEM },
        priv_map { name: c"ALTER SYSTEM WITH GRANT OPTION".as_ptr(), value: gopt(ACL_ALTER_SYSTEM) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_text, parameter_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// has_largeobject_privilege variants
// ----------------------------------------------------------------------------

unsafe fn has_lo_priv_byid(roleid: Oid, lobjId: Oid, priv_: AclMode, is_missing: *mut bool) -> bool {
    let snapshot: Snapshot;
    let aclresult: AclResult;

    if priv_ & ACL_UPDATE != 0 {
        snapshot = core::ptr::null_mut();
    } else {
        snapshot = GetActiveSnapshot();
    }

    if !LargeObjectExistsWithSnapshot(lobjId, snapshot) {
        Assert!(!is_missing.is_null());
        *is_missing = true;
        return false;
    }

    if lo_compat_privileges {
        return true;
    }

    aclresult = pg_largeobject_aclcheck_snapshot(lobjId, roleid, priv_, snapshot);
    aclresult == ACLCHECK_OK
}

pub unsafe fn has_largeobject_privilege_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let roleid = get_role_oid_or_public(NameStr(&*username));
    let lobjId = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_largeobject_priv_string(priv_type_text);
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobjId, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn has_largeobject_privilege_id(fcinfo: FunctionCallInfo) -> Datum {
    let lobjId = PG_GETARG_OID!(fcinfo, 0);
    let roleid = GetUserId();
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let mode = convert_largeobject_priv_string(priv_type_text);
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobjId, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn has_largeobject_privilege_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let lobjId = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_largeobject_priv_string(priv_type_text);
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobjId, mode, &mut is_missing);
    if is_missing {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_BOOL!(result)
}

unsafe fn convert_largeobject_priv_string(priv_type_text: *mut text) -> AclMode {
    static largeobject_priv_map: [priv_map; 5] = [
        priv_map { name: c"SELECT".as_ptr(), value: ACL_SELECT },
        priv_map { name: c"SELECT WITH GRANT OPTION".as_ptr(), value: gopt(ACL_SELECT) },
        priv_map { name: c"UPDATE".as_ptr(), value: ACL_UPDATE },
        priv_map { name: c"UPDATE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_UPDATE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, largeobject_priv_map.as_ptr())
}

// ----------------------------------------------------------------------------
// pg_has_role variants
// ----------------------------------------------------------------------------

pub unsafe fn pg_has_role_name_name(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let rolename = PG_GETARG_NAME!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid(NameStr(&*username), false);
    let roleoid = get_role_oid(NameStr(&*rolename), false);
    let mode = convert_role_priv_string(priv_type_text);
    let aclresult = pg_role_aclcheck(roleoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn pg_has_role_name(fcinfo: FunctionCallInfo) -> Datum {
    let rolename = PG_GETARG_NAME!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let roleoid = get_role_oid(NameStr(&*rolename), false);
    let mode = convert_role_priv_string(priv_type_text);
    let aclresult = pg_role_aclcheck(roleoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn pg_has_role_name_id(fcinfo: FunctionCallInfo) -> Datum {
    let username = PG_GETARG_NAME!(fcinfo, 0);
    let roleoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleid = get_role_oid(NameStr(&*username), false);
    let mode = convert_role_priv_string(priv_type_text);
    let aclresult = pg_role_aclcheck(roleoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn pg_has_role_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleoid = PG_GETARG_OID!(fcinfo, 0);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 1) as *mut text;
    let roleid = GetUserId();
    let mode = convert_role_priv_string(priv_type_text);
    let aclresult = pg_role_aclcheck(roleoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn pg_has_role_id_name(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let rolename = PG_GETARG_NAME!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let roleoid = get_role_oid(NameStr(&*rolename), false);
    let mode = convert_role_priv_string(priv_type_text);
    let aclresult = pg_role_aclcheck(roleoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

pub unsafe fn pg_has_role_id_id(fcinfo: FunctionCallInfo) -> Datum {
    let roleid = PG_GETARG_OID!(fcinfo, 0);
    let roleoid = PG_GETARG_OID!(fcinfo, 1);
    let priv_type_text = PG_GETARG_TEXT_PP!(fcinfo, 2) as *mut text;
    let mode = convert_role_priv_string(priv_type_text);
    let aclresult = pg_role_aclcheck(roleoid, roleid, mode);
    PG_RETURN_BOOL!(aclresult == ACLCHECK_OK)
}

/// convert_role_priv_string - USAGE=has_privs, MEMBER=is_member (cheats with
/// ACL_CREATE), MEMBER WITH ADMIN OPTION = is_admin.
unsafe fn convert_role_priv_string(priv_type_text: *mut text) -> AclMode {
    static role_priv_map: [priv_map; 10] = [
        priv_map { name: c"USAGE".as_ptr(), value: ACL_USAGE },
        priv_map { name: c"MEMBER".as_ptr(), value: ACL_CREATE },
        priv_map { name: c"SET".as_ptr(), value: ACL_SET },
        priv_map { name: c"USAGE WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"USAGE WITH ADMIN OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"MEMBER WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"MEMBER WITH ADMIN OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"SET WITH GRANT OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: c"SET WITH ADMIN OPTION".as_ptr(), value: gopt(ACL_CREATE) },
        priv_map { name: core::ptr::null(), value: 0 },
    ];
    convert_any_priv_string(priv_type_text, role_priv_map.as_ptr())
}

/// pg_role_aclcheck - quick-and-dirty support for pg_has_role.
unsafe fn pg_role_aclcheck(role_oid: Oid, roleid: Oid, mode: AclMode) -> AclResult {
    if mode & gopt(ACL_CREATE) != 0 {
        if is_admin_of_role(roleid, role_oid) {
            return ACLCHECK_OK;
        }
    }
    if mode & ACL_CREATE != 0 {
        if is_member_of_role(roleid, role_oid) {
            return ACLCHECK_OK;
        }
    }
    if mode & ACL_USAGE != 0 {
        if has_privs_of_role(roleid, role_oid) {
            return ACLCHECK_OK;
        }
    }
    if mode & ACL_SET != 0 {
        if member_can_set_role(roleid, role_oid) {
            return ACLCHECK_OK;
        }
    }
    ACLCHECK_NO_PRIV
}

// ----------------------------------------------------------------------------
// Role-membership engine
// ----------------------------------------------------------------------------

/// initialization function (called by InitPostgres)
pub unsafe fn initialize_acl() {
    if !IsBootstrapProcessingMode() {
        cached_db_hash = GetSysCacheHashValue1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));

        /*
         * In normal mode, set a callback on syscache invalidation of
         * pg_auth_members / pg_authid / pg_database rows.
         */
        CacheRegisterSyscacheCallback(AUTHMEMROLEMEM, RoleMembershipCacheCallback, 0 as Datum);
        CacheRegisterSyscacheCallback(AUTHOID, RoleMembershipCacheCallback, 0 as Datum);
        CacheRegisterSyscacheCallback(DATABASEOID, RoleMembershipCacheCallback, 0 as Datum);
    }
}

/// RoleMembershipCacheCallback - syscache inval callback.
unsafe extern "C" fn RoleMembershipCacheCallback(arg: Datum, cacheid: c_int, hashvalue: uint32) {
    if cacheid == DATABASEOID && hashvalue != cached_db_hash && hashvalue != 0 {
        return; /* ignore pg_database changes for other DBs */
    }

    /* Force membership caches to be recomputed on next use */
    cached_role[ROLERECURSE_MEMBERS as usize] = InvalidOid;
    cached_role[ROLERECURSE_PRIVS as usize] = InvalidOid;
    cached_role[ROLERECURSE_SETROLE as usize] = InvalidOid;
}

/// Optimized list_append_unique_oid() helper for roles_is_member_of() backed by
/// an optional Bloom filter.
unsafe fn roles_list_append(
    mut roles_list: *mut List,
    bf: *mut *mut bloom_filter,
    role: Oid,
) -> *mut List {
    let roleptr = &role as *const Oid as *const u8;

    /*
     * If a Bloom filter exists, use it to try to short-circuit the membership
     * check; otherwise fall back to a linear search.
     */
    if (!(*bf).is_null() && bloom_lacks_element(*bf, roleptr, core::mem::size_of::<Oid>()))
        || !list_member_oid(roles_list, role)
    {
        /*
         * If the list is large, create a Bloom filter to speed up future
         * calls.
         */
        if (*bf).is_null() && list_length(roles_list) > ROLES_LIST_BLOOM_THRESHOLD {
            *bf = bloom_create(
                (ROLES_LIST_BLOOM_THRESHOLD * 10) as int64,
                work_mem,
                0,
            );
            foreach_oid!(roleid, roles_list, {
                bloom_add_element(*bf, &roleid as *const Oid as *const u8, core::mem::size_of::<Oid>());
            });
        }

        /*
         * Finally, add the role to the list and the Bloom filter, if it exists.
         */
        roles_list = lappend_oid(roles_list, role);
        if !(*bf).is_null() {
            bloom_add_element(*bf, roleptr, core::mem::size_of::<Oid>());
        }
    }

    roles_list
}

/// Get a list of roles that the specified roleid is a member of.
unsafe fn roles_is_member_of(
    roleid: Oid,
    type_: RoleRecurseType,
    admin_of: Oid,
    admin_role: *mut Oid,
) -> *mut List {
    let dba: Oid;
    let mut roles_list: *mut List;
    let new_cached_roles: *mut List;
    let oldctx: MemoryContext;
    let mut bf: *mut bloom_filter = core::ptr::null_mut();

    Assert!(OidIsValid(admin_of) == PointerIsValid(admin_role as *const c_void));
    if !admin_role.is_null() {
        *admin_role = InvalidOid;
    }

    /* If cache is valid and ADMIN OPTION not sought, just return the list */
    if cached_role[type_ as usize] == roleid
        && !OidIsValid(admin_of)
        && OidIsValid(cached_role[type_ as usize])
    {
        return cached_roles[type_ as usize];
    }

    /*
     * Role expansion can happen in a non-database backend; in that case no
     * role gets pg_database_owner.
     */
    if !OidIsValid(MyDatabaseId) {
        dba = InvalidOid;
    } else {
        let dbtup: HeapTuple;

        dbtup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
        if !HeapTupleIsValid(dbtup) {
            elog!(ERROR, "cache lookup failed for database {}", MyDatabaseId);
        }
        dba = (*(GETSTRUCT(dbtup) as Form_pg_database)).datdba;
        ReleaseSysCache(dbtup);
    }

    /*
     * Find all the roles that roleid is a member of, including multi-level
     * recursion.  The role itself is always the first element.
     */
    roles_list = list_make1_oid!(roleid);

    foreach!(l, roles_list, {
        let memberid = lfirst_oid(current_cell!(l));
        let memlist: *mut CatCList;
        let mut i: c_int;

        /* Find roles that memberid is directly a member of */
        memlist = SearchSysCacheList1(AUTHMEMMEMROLE, ObjectIdGetDatum(memberid));
        i = 0;
        while i < (*memlist).n_members {
            let tup = &mut (**(*memlist).members.add(i as usize)).tuple as *mut HeapTupleData;
            let form = GETSTRUCT(tup) as Form_pg_auth_members;
            let otherid = (*form).roleid;

            /*
             * otherid==InvalidOid shouldn't appear, but OidIsValid() avoids
             * crashing if it does.
             */
            if otherid == admin_of
                && (*form).admin_option
                && OidIsValid(admin_of)
                && !OidIsValid(*admin_role)
            {
                *admin_role = memberid;
            }

            /* If we're supposed to ignore non-heritable grants, do so. */
            if type_ == ROLERECURSE_PRIVS && !(*form).inherit_option {
                i += 1;
                continue;
            }

            /* If we're supposed to ignore non-SET grants, do so. */
            if type_ == ROLERECURSE_SETROLE && !(*form).set_option {
                i += 1;
                continue;
            }

            /*
             * Test for having already seen this role (A->B and A->C->B is legal).
             */
            roles_list = roles_list_append(roles_list, &raw mut bf, otherid);
            i += 1;
        }
        ReleaseSysCacheList(memlist);

        /* implement pg_database_owner implicit membership */
        if memberid == dba && OidIsValid(dba) {
            roles_list = roles_list_append(roles_list, &raw mut bf, ROLE_PG_DATABASE_OWNER);
        }
    });

    /* Free the Bloom filter created by roles_list_append(), if there is one. */
    if !bf.is_null() {
        bloom_free(bf);
    }

    /* Copy the completed list into TopMemoryContext so it will persist. */
    oldctx = MemoryContextSwitchTo(TopMemoryContext);
    new_cached_roles = list_copy(roles_list);
    MemoryContextSwitchTo(oldctx);
    list_free(roles_list);

    /* Now safe to assign to state variable */
    cached_role[type_ as usize] = InvalidOid; /* just paranoia */
    list_free(cached_roles[type_ as usize]);
    cached_roles[type_ as usize] = new_cached_roles;
    cached_role[type_ as usize] = roleid;

    /* And now we can return the answer */
    cached_roles[type_ as usize]
}

/// Does member have the privileges of role (directly or indirectly)?
#[no_mangle]
pub unsafe fn has_privs_of_role(member: Oid, role: Oid) -> bool {
    /* Fast path for simple case */
    if member == role {
        return true;
    }

    /* Superusers have every privilege, so are part of every role */
    if superuser_arg(member) {
        return true;
    }

    list_member_oid(
        roles_is_member_of(member, ROLERECURSE_PRIVS, InvalidOid, core::ptr::null_mut()),
        role,
    )
}

/// Can member use SET ROLE to this role?
pub unsafe fn member_can_set_role(member: Oid, role: Oid) -> bool {
    if member == role {
        return true;
    }

    if superuser_arg(member) {
        return true;
    }

    list_member_oid(
        roles_is_member_of(member, ROLERECURSE_SETROLE, InvalidOid, core::ptr::null_mut()),
        role,
    )
}

/// Permission violation error unless able to SET ROLE to target role.
pub unsafe fn check_can_set_role(member: Oid, role: Oid) {
    if !member_can_set_role(member, role) {
        ereport!(
            ERROR,
            errmsg!(
                "must be able to SET ROLE \"{}\"",
                std::ffi::CStr::from_ptr(GetUserNameFromId(role, false)).to_string_lossy()
            )
        );
    }
}

/// Is member a member of role (directly or indirectly)?
pub unsafe fn is_member_of_role(member: Oid, role: Oid) -> bool {
    if member == role {
        return true;
    }

    if superuser_arg(member) {
        return true;
    }

    list_member_oid(
        roles_is_member_of(member, ROLERECURSE_MEMBERS, InvalidOid, core::ptr::null_mut()),
        role,
    )
}

/// Is member a member of role, not considering superuserness?
pub unsafe fn is_member_of_role_nosuper(member: Oid, role: Oid) -> bool {
    if member == role {
        return true;
    }

    list_member_oid(
        roles_is_member_of(member, ROLERECURSE_MEMBERS, InvalidOid, core::ptr::null_mut()),
        role,
    )
}

/// Is member an admin of role?
pub unsafe fn is_admin_of_role(member: Oid, role: Oid) -> bool {
    let mut admin_role: Oid = InvalidOid;

    if superuser_arg(member) {
        return true;
    }

    /* By policy, a role cannot have WITH ADMIN OPTION on itself. */
    if member == role {
        return false;
    }

    roles_is_member_of(member, ROLERECURSE_MEMBERS, role, &mut admin_role);
    OidIsValid(admin_role)
}

/// Find a role whose privileges "member" inherits which has ADMIN OPTION on "role".
pub unsafe fn select_best_admin(member: Oid, role: Oid) -> Oid {
    let mut admin_role: Oid = InvalidOid;

    /* By policy, a role cannot have WITH ADMIN OPTION on itself. */
    if member == role {
        return InvalidOid;
    }

    roles_is_member_of(member, ROLERECURSE_PRIVS, role, &mut admin_role);
    admin_role
}

/// Select the effective grantor ID for a GRANT or REVOKE operation.
pub unsafe fn select_best_grantor(
    roleId: Oid,
    privileges: AclMode,
    acl: *const Acl,
    ownerId: Oid,
    grantorId: *mut Oid,
    grantOptions: *mut AclMode,
) {
    let needed_goptions: AclMode = ACL_GRANT_OPTION_FOR(privileges);
    let roles_list: *mut List;
    let mut nrights: c_int;

    /*
     * The object owner (and superusers) are treated as having all grant
     * options.
     */
    if roleId == ownerId || superuser_arg(roleId) {
        *grantorId = ownerId;
        *grantOptions = needed_goptions;
        return;
    }

    /*
     * Otherwise, carefully search to see if roleId has the privileges of any
     * suitable role.
     */
    roles_list = roles_is_member_of(roleId, ROLERECURSE_PRIVS, InvalidOid, core::ptr::null_mut());

    /* initialize candidate result as default */
    *grantorId = roleId;
    *grantOptions = ACL_NO_RIGHTS;
    nrights = 0;

    foreach!(l, roles_list, {
        let otherrole = lfirst_oid(current_cell!(l));
        let otherprivs: AclMode;

        otherprivs = aclmask_direct(acl, otherrole, ownerId, needed_goptions, ACLMASK_ALL);
        if otherprivs == needed_goptions {
            /* Found a suitable grantor */
            *grantorId = otherrole;
            *grantOptions = otherprivs;
            return;
        }

        /*
         * If it has just some of the needed privileges, remember best candidate.
         */
        if otherprivs != ACL_NO_RIGHTS {
            let nnewrights = pg_popcount64(otherprivs);

            if nnewrights > nrights {
                *grantorId = otherrole;
                *grantOptions = otherprivs;
                nrights = nnewrights;
            }
        }
    });
}

// ----------------------------------------------------------------------------
// get_role_oid family
// ----------------------------------------------------------------------------

/// get_role_oid - given a role name, look up the role's OID.
pub unsafe fn get_role_oid(rolname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid1(AUTHNAME, Anum_pg_authid_oid, CStringGetDatum(rolname));
    if !OidIsValid(oid) && !missing_ok {
        ereport!(
            ERROR,
            errmsg!(
                "role \"{}\" does not exist",
                std::ffi::CStr::from_ptr(rolname).to_string_lossy()
            )
        );
    }
    oid
}

/// get_role_oid_or_public - as above, but ACL_ID_PUBLIC for "public".
pub unsafe fn get_role_oid_or_public(rolname: *const c_char) -> Oid {
    if strcmp(rolname, c"public".as_ptr()) == 0 {
        return ACL_ID_PUBLIC;
    }
    get_role_oid(rolname, false)
}

/// Given a RoleSpec node, return the OID it corresponds to.  PUBLIC disallowed.
pub unsafe fn get_rolespec_oid(role: *const RoleSpec, missing_ok: bool) -> Oid {
    let oid: Oid;

    match (*role).roletype {
        ROLESPEC_CSTRING => {
            Assert!(!(*role).rolename.is_null());
            oid = get_role_oid((*role).rolename, missing_ok);
        }
        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => {
            oid = GetUserId();
        }
        ROLESPEC_SESSION_USER => {
            oid = GetSessionUserId();
        }
        ROLESPEC_PUBLIC => {
            ereport!(ERROR, errmsg!("role \"{}\" does not exist", "public"));
            #[allow(unreachable_code)]
            {
                oid = InvalidOid; /* make compiler happy */
            }
        }
    }

    oid
}

/// Given a RoleSpec node, return the pg_authid HeapTuple.  Caller ReleaseSysCache.
pub unsafe fn get_rolespec_tuple(role: *const RoleSpec) -> HeapTuple {
    let tuple: HeapTuple;

    match (*role).roletype {
        ROLESPEC_CSTRING => {
            Assert!(!(*role).rolename.is_null());
            tuple = SearchSysCache1(AUTHNAME, CStringGetDatum((*role).rolename));
            if !HeapTupleIsValid(tuple) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "role \"{}\" does not exist",
                        std::ffi::CStr::from_ptr((*role).rolename).to_string_lossy()
                    )
                );
            }
        }
        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => {
            tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
            if !HeapTupleIsValid(tuple) {
                elog!(ERROR, "cache lookup failed for role {}", GetUserId());
            }
        }
        ROLESPEC_SESSION_USER => {
            tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetSessionUserId()));
            if !HeapTupleIsValid(tuple) {
                elog!(ERROR, "cache lookup failed for role {}", GetSessionUserId());
            }
        }
        ROLESPEC_PUBLIC => {
            ereport!(ERROR, errmsg!("role \"{}\" does not exist", "public"));
            #[allow(unreachable_code)]
            {
                tuple = core::ptr::null_mut(); /* make compiler happy */
            }
        }
    }

    tuple
}

/// Given a RoleSpec, return a palloc'ed copy of the corresponding role's name.
pub unsafe fn get_rolespec_name(role: *const RoleSpec) -> *mut c_char {
    let tp: HeapTuple;
    let authForm: Form_pg_authid;
    let rolename: *mut c_char;

    tp = get_rolespec_tuple(role);
    authForm = GETSTRUCT(tp) as Form_pg_authid;
    rolename = pstrdup(NameStr(&(*authForm).rolname));
    ReleaseSysCache(tp);

    rolename
}

/// Given a RoleSpec, throw an error if the name is reserved.
pub unsafe fn check_rolespec_name(role: *const RoleSpec, detail_msg: *const c_char) {
    if role.is_null() {
        return;
    }

    if (*role).roletype != ROLESPEC_CSTRING {
        return;
    }

    if IsReservedName((*role).rolename) {
        if !detail_msg.is_null() {
            // errdetail_internal("%s", detail_msg) dropped per single-errmsg rule.
            ereport!(
                ERROR,
                errmsg!(
                    "role name \"{}\" is reserved",
                    std::ffi::CStr::from_ptr((*role).rolename).to_string_lossy()
                )
            );
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "role name \"{}\" is reserved",
                    std::ffi::CStr::from_ptr((*role).rolename).to_string_lossy()
                )
            );
        }
    }
}

// ----------------------------------------------------------------------------
// Remaining unported helpers: namespace / RangeVar (catalog/namespace.c,
// nodes/makefuncs.c, utils/adt/varlena.c) + funcapi.h / access/tupdesc.c /
// access/heaptuple.c set-returning-function machinery.
// TODO(pg-port): real symbols live in those modules.
// ----------------------------------------------------------------------------

// nodes/primnodes.h RangeVar (only used opaquely here).
pub enum RangeVar {}

extern "C" {
    pub fn textToQualifiedNameList(textval: *const text) -> *mut List;
    pub fn makeRangeVarFromNameList(names: *const List) -> *mut RangeVar;
    pub fn RangeVarGetRelid(relation: *const RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> Oid;
}
// utils/builtins.h: CStringGetTextDatum.
use crate::utils::builtins::CStringGetTextDatum;

pub type LOCKMODE = c_int;
pub const NoLock: LOCKMODE = 0;

// funcapi.h / access/tupdesc.c / access/heaptuple.c machinery for aclexplode.
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}
pub type TupleDesc = *mut c_void;

unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!() // TODO(pg-port): funcapi.h
}
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): funcapi.h
}
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): funcapi.h
}
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h
}
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h
}
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO(pg-port): access/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attno: AttrNumber,
    _attname: *const c_char,
    _oidtypeid: Oid,
    _typmod: int32,
    _attdim: c_int,
) {
    unimplemented!() // TODO(pg-port): access/tupdesc.c
}
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!() // TODO(pg-port): access/tupdesc.c
}
unsafe fn heap_form_tuple(_tupdesc: TupleDesc, _values: *const Datum, _isnull: *const bool) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h / access/htup.h
}

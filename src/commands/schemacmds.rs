//! commands/schemacmds.c - schema creation/manipulation commands.

use crate::prelude::*;
use crate::{makeNode, foreach, current_cell};

use std::ffi::c_void;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog::IsReservedName;
use crate::catalog::catalog_oids::{DatabaseRelationId, NamespaceRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_authid::Form_pg_authid;
use crate::catalog::pg_namespace::Form_pg_namespace;
use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfoData};
use crate::miscadmin::{
    allowSystemTableMods, MyDatabaseId, SECURITY_LOCAL_USERID_CHANGE,
};
use crate::nodes::nodes::{CmdType, Node, NodeTag, ParseLoc};
use crate::nodes::parsenodes::CreateSchemaStmt;
use crate::nodes::plannodes::PlannedStmt;
use crate::parser::scansup::scanner_isspace;
use crate::postgres::{CStringGetDatum, ObjectIdGetDatum, PointerGetDatum};
use crate::storage::lockdefs::{NoLock, RowExclusiveLock, LOCKMODE};
use crate::utils::builtins::quote_identifier;
use crate::utils::rel::{Relation, RelationGetDescr, RelationGetRelid};
use crate::appendStringInfo;
use crate::c::NameStr;

// ----------------------------------------------------------------------------
// Stubs / constants for as-yet-unported dependencies.  Values match
// PostgreSQL 18.3 where they are concrete constants.
// ----------------------------------------------------------------------------

// utils/acl.h
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;
const ACL_CREATE: u64 = 1 << 11; // AclMode bit for CREATE
type Acl = c_void;

// nodes/parsenodes.h ObjectType subset
type ObjectType = c_int;
const OBJECT_DATABASE: ObjectType = 0;
const OBJECT_SCHEMA: ObjectType = 0;

// utils/syscache.h SysCacheIdentifier subset
type SysCacheIdentifier = c_int;
const AUTHOID: SysCacheIdentifier = 0;
const NAMESPACENAME: SysCacheIdentifier = 0;
const NAMESPACEOID: SysCacheIdentifier = 0;

// access/tableam.h / utility process flag subset
type ProcessUtilityContext = c_int;
const PROCESS_UTILITY_SUBCOMMAND: ProcessUtilityContext = 0;

// guc.h enums
type GucContext = c_int;
const PGC_USERSET: GucContext = 0;
type GucSource = c_int;
const PGC_S_SESSION: GucSource = 0;
type GucAction = c_int;
const GUC_ACTION_SAVE: GucAction = 0;

// catalog/pg_namespace.h Anum/Natts (1-based attribute numbers)
const Natts_pg_namespace: usize = 4;
const Anum_pg_namespace_nspowner: c_int = 3;
const Anum_pg_namespace_nspacl: c_int = 4;

// errcode helpers
fn ERRCODE_RESERVED_NAME() -> c_int {
    0
}
fn ERRCODE_DUPLICATE_SCHEMA() -> c_int {
    0
}
fn ERRCODE_UNDEFINED_SCHEMA() -> c_int {
    0
}

// dest receiver stub
struct DestReceiver;
unsafe fn None_Receiver() -> *mut DestReceiver {
    null_mut()
}

// --- Unported function stubs -------------------------------------------------

unsafe fn GetUserIdAndSecContext(userid: *mut Oid, sec_context: *mut c_int) {
    let _ = (userid, sec_context);
    unimplemented!()
}
unsafe fn SetUserIdAndSecContext(userid: Oid, sec_context: c_int) {
    let _ = (userid, sec_context);
    unimplemented!()
}
unsafe fn GetUserId() -> Oid {
    unimplemented!()
}
unsafe fn check_can_set_role(member: Oid, role: Oid) {
    let _ = (member, role);
    unimplemented!()
}
unsafe fn get_rolespec_oid(role: *mut c_void, missing_ok: bool) -> Oid {
    let _ = (role, missing_ok);
    unimplemented!()
}
unsafe fn SearchSysCache1(cacheId: SysCacheIdentifier, key1: Datum) -> HeapTuple {
    let _ = (cacheId, key1);
    unimplemented!()
}
unsafe fn SearchSysCacheCopy1(cacheId: SysCacheIdentifier, key1: Datum) -> HeapTuple {
    let _ = (cacheId, key1);
    unimplemented!()
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    let _ = tuple;
    unimplemented!()
}
unsafe fn SysCacheGetAttr(
    cacheId: SysCacheIdentifier,
    tup: HeapTuple,
    attributeNumber: c_int,
    isNull: *mut bool,
) -> Datum {
    let _ = (cacheId, tup, attributeNumber, isNull);
    unimplemented!()
}
unsafe fn object_aclcheck(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: u64,
) -> AclResult {
    let _ = (classid, objectid, roleid, mode);
    unimplemented!()
}
unsafe fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool {
    let _ = (classid, objectid, roleid);
    unimplemented!()
}
unsafe fn aclcheck_error(aclerr: AclResult, objtype: ObjectType, objectname: *const c_char) {
    let _ = (aclerr, objtype, objectname);
    unimplemented!()
}
unsafe fn get_database_name(dbid: Oid) -> *mut c_char {
    let _ = dbid;
    unimplemented!()
}
unsafe fn get_namespace_oid(nspname: *const c_char, missing_ok: bool) -> Oid {
    let _ = (nspname, missing_ok);
    unimplemented!()
}
unsafe fn NamespaceCreate(nspName: *const c_char, ownerId: Oid, isTemp: bool) -> Oid {
    let _ = (nspName, ownerId, isTemp);
    unimplemented!()
}
unsafe fn CommandCounterIncrement() {
    unimplemented!()
}
unsafe fn NewGUCNestLevel() -> c_int {
    unimplemented!()
}
unsafe fn AtEOXact_GUC(isCommit: bool, nestLevel: c_int) {
    let _ = (isCommit, nestLevel);
    unimplemented!()
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
    let _ = (
        name, value, context, source, action, changeVal, elevel, is_reload,
    );
    unimplemented!()
}
unsafe fn checkMembershipInCurrentExtension(object: *const ObjectAddress) {
    let _ = object;
    unimplemented!()
}
unsafe fn EventTriggerCollectSimpleCommand(
    address: ObjectAddress,
    secondaryObject: ObjectAddress,
    parsetree: *mut Node,
) {
    let _ = (address, secondaryObject, parsetree);
    unimplemented!()
}
unsafe fn transformCreateSchemaStmtElements(
    schemaElts: *mut c_void,
    schemaName: *const c_char,
) -> *mut crate::nodes::pg_list::List {
    let _ = (schemaElts, schemaName);
    unimplemented!()
}
unsafe fn ProcessUtility(
    pstmt: *mut PlannedStmt,
    queryString: *const c_char,
    readOnlyTree: bool,
    context: ProcessUtilityContext,
    params: *mut c_void,
    queryEnv: *mut c_void,
    dest: *mut DestReceiver,
    qc: *mut c_void,
) {
    let _ = (
        pstmt,
        queryString,
        readOnlyTree,
        context,
        params,
        queryEnv,
        dest,
        qc,
    );
    unimplemented!()
}
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *mut c_void, tup: HeapTuple) {
    let _ = (heapRel, otid, tup);
    unimplemented!()
}
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: crate::access::common::tupdesc::TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple {
    let _ = (tuple, tupleDesc, replValues, replIsnull, doReplace);
    unimplemented!()
}
unsafe fn heap_freetuple(htup: HeapTuple) {
    let _ = htup;
    unimplemented!()
}
unsafe fn namestrcpy(name: *mut c_void, str: *const c_char) -> c_int {
    let _ = (name, str);
    unimplemented!()
}
unsafe fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwnerId: Oid) {
    let _ = (classId, objectId, newOwnerId);
    unimplemented!()
}
unsafe fn aclnewowner(old_acl: *mut Acl, oldOwnerId: Oid, newOwnerId: Oid) -> *mut Acl {
    let _ = (old_acl, oldOwnerId, newOwnerId);
    unimplemented!()
}
unsafe fn DatumGetAclP(X: Datum) -> *mut Acl {
    let _ = X;
    unimplemented!()
}
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {
    let _ = (classId, objectId, subId);
}

// global from namespace.c
const namespace_search_path: *const c_char = null();

// ObjectAddressSet(addr, class, object): convenience builder.
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

fn InvalidObjectAddress() -> ObjectAddress {
    ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    }
}

/*
 * CREATE SCHEMA
 *
 * Note: caller should pass in location information for the whole
 * CREATE SCHEMA statement, which in turn we pass down as the location
 * of the component commands.  This comports with our general plan of
 * reporting location/len for the whole command even when executing
 * a subquery.
 */
pub unsafe fn CreateSchemaCommand(
    stmt: *mut CreateSchemaStmt,
    queryString: *const c_char,
    stmt_location: c_int,
    stmt_len: c_int,
) -> Oid {
    let mut schemaName: *const c_char = (*stmt).schemaname;
    let namespaceId: Oid;
    let parsetree_list;
    let owner_uid: Oid;
    let mut saved_uid: Oid = InvalidOid;
    let mut save_sec_context: c_int = 0;
    let save_nestlevel: c_int;
    let mut nsp: *const c_char = namespace_search_path;
    let aclresult: AclResult;
    let mut address: ObjectAddress = InvalidObjectAddress();
    let mut pathbuf: StringInfoData = std::mem::zeroed();

    GetUserIdAndSecContext(&mut saved_uid, &mut save_sec_context);

    /*
     * Who is supposed to own the new schema?
     */
    if !(*stmt).authrole.is_null() {
        owner_uid = get_rolespec_oid((*stmt).authrole as *mut c_void, false);
    } else {
        owner_uid = saved_uid;
    }

    /* fill schema name with the user name if not specified */
    if schemaName.is_null() {
        let tuple: HeapTuple;

        tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner_uid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for role {}", owner_uid);
        }
        schemaName = pstrdup(NameStr(
            &(*(GETSTRUCT(tuple) as Form_pg_authid)).rolname,
        ));
        ReleaseSysCache(tuple);
    }

    /*
     * To create a schema, must have schema-create privilege on the current
     * database and must be able to become the target role (this does not
     * imply that the target role itself must have create-schema privilege).
     * The latter provision guards against "giveaway" attacks.  Note that a
     * superuser will always have both of these privileges a fortiori.
     */
    aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, saved_uid, ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            OBJECT_DATABASE,
            get_database_name(MyDatabaseId),
        );
    }

    check_can_set_role(saved_uid, owner_uid);

    /* Additional check to protect reserved schema names */
    if !allowSystemTableMods && IsReservedName(schemaName) {
        ereport!(
            ERROR,
            "unacceptable schema name"
        );
    }

    /*
     * If if_not_exists was given and the schema already exists, bail out.
     * (Note: we needn't check this when not if_not_exists, because
     * NamespaceCreate will complain anyway.)  We could do this before making
     * the permissions checks, but since CREATE TABLE IF NOT EXISTS makes its
     * creation-permission check first, we do likewise.
     */
    if (*stmt).if_not_exists {
        let namespaceId = get_namespace_oid(schemaName, true);
        if OidIsValid(namespaceId) {
            /*
             * If we are in an extension script, insist that the pre-existing
             * object be a member of the extension, to avoid security risks.
             */
            ObjectAddressSet(&mut address, NamespaceRelationId, namespaceId);
            checkMembershipInCurrentExtension(&address);

            /* OK to skip */
            ereport!(NOTICE, "schema already exists, skipping");
            return InvalidOid;
        }
    }

    /*
     * If the requested authorization is different from the current user,
     * temporarily set the current user so that the object(s) will be created
     * with the correct ownership.
     *
     * (The setting will be restored at the end of this routine, or in case of
     * error, transaction abort will clean things up.)
     */
    if saved_uid != owner_uid {
        SetUserIdAndSecContext(
            owner_uid,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    /* Create the schema's namespace */
    namespaceId = NamespaceCreate(schemaName, owner_uid, false);

    /* Advance cmd counter to make the namespace visible */
    CommandCounterIncrement();

    /*
     * Prepend the new schema to the current search path.
     *
     * We use the equivalent of a function SET option to allow the setting to
     * persist for exactly the duration of the schema creation.  guc.c also
     * takes care of undoing the setting on error.
     */
    save_nestlevel = NewGUCNestLevel();

    initStringInfo(&mut pathbuf);
    appendStringInfoString(&mut pathbuf, quote_identifier(schemaName));

    while scanner_isspace(*nsp) {
        nsp = nsp.add(1);
    }

    if *nsp != b'\0' as c_char {
        let nsp_str = std::ffi::CStr::from_ptr(nsp).to_string_lossy();
        appendStringInfo!(&mut pathbuf, ", {}", nsp_str);
    }

    let _ = set_config_option(
        c"search_path".as_ptr(),
        pathbuf.data,
        PGC_USERSET,
        PGC_S_SESSION,
        GUC_ACTION_SAVE,
        true,
        0,
        false,
    );

    /*
     * Report the new schema to possibly interested event triggers.  Note we
     * must do this here and not in ProcessUtilitySlow because otherwise the
     * objects created below are reported before the schema, which would be
     * wrong.
     */
    ObjectAddressSet(&mut address, NamespaceRelationId, namespaceId);
    EventTriggerCollectSimpleCommand(address, InvalidObjectAddress(), stmt as *mut Node);

    /*
     * Examine the list of commands embedded in the CREATE SCHEMA command, and
     * reorganize them into a sequentially executable order with no forward
     * references.  Note that the result is still a list of raw parsetrees ---
     * we cannot, in general, run parse analysis on one statement until we
     * have actually executed the prior ones.
     */
    parsetree_list =
        transformCreateSchemaStmtElements((*stmt).schemaElts as *mut c_void, schemaName);

    /*
     * Execute each command contained in the CREATE SCHEMA.  Since the grammar
     * allows only utility commands in CREATE SCHEMA, there is no need to pass
     * them through parse_analyze_*() or the rewriter; we can just hand them
     * straight to ProcessUtility.
     */
    foreach!(parsetree_item, parsetree_list, {
        let inner_stmt = crate::nodes::pg_list::lfirst(current_cell!(parsetree_item)) as *mut Node;
        let wrapper: *mut PlannedStmt;

        /* need to make a wrapper PlannedStmt */
        wrapper = makeNode!(PlannedStmt, T_PlannedStmt);
        (*wrapper).commandType = CmdType::CMD_UTILITY;
        (*wrapper).canSetTag = false;
        (*wrapper).utilityStmt = inner_stmt;
        (*wrapper).stmt_location = stmt_location as ParseLoc;
        (*wrapper).stmt_len = stmt_len as ParseLoc;

        /* do this step */
        ProcessUtility(
            wrapper,
            queryString,
            false,
            PROCESS_UTILITY_SUBCOMMAND,
            null_mut(),
            null_mut(),
            None_Receiver(),
            null_mut(),
        );

        /* make sure later steps can see the object created here */
        CommandCounterIncrement();
    });

    /*
     * Restore the GUC variable search_path we set above.
     */
    AtEOXact_GUC(true, save_nestlevel);

    /* Reset current user and security context */
    SetUserIdAndSecContext(saved_uid, save_sec_context);

    namespaceId
}

/*
 * Rename schema
 */
pub unsafe fn RenameSchema(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let nspOid: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let aclresult: AclResult;
    let mut address: ObjectAddress = InvalidObjectAddress();
    let nspform: Form_pg_namespace;

    rel = table_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(NAMESPACENAME, CStringGetDatum(oldname));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, "schema does not exist");
    }

    nspform = GETSTRUCT(tup) as Form_pg_namespace;
    nspOid = (*nspform).oid;

    /* make sure the new name doesn't exist */
    if OidIsValid(get_namespace_oid(newname, true)) {
        ereport!(ERROR, "schema already exists");
    }

    /* must be owner */
    if !object_ownercheck(NamespaceRelationId, nspOid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA, oldname);
    }

    /* must have CREATE privilege on database */
    aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            OBJECT_DATABASE,
            get_database_name(MyDatabaseId),
        );
    }

    if !allowSystemTableMods && IsReservedName(newname) {
        ereport!(ERROR, "unacceptable schema name");
    }

    /* rename */
    namestrcpy(&mut (*nspform).nspname as *mut _ as *mut c_void, newname);
    CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut _ as *mut c_void, tup);

    InvokeObjectPostAlterHook(NamespaceRelationId, nspOid, 0);

    ObjectAddressSet(&mut address, NamespaceRelationId, nspOid);

    table_close(rel, NoLock);
    heap_freetuple(tup);

    address
}

pub unsafe fn AlterSchemaOwner_oid(schemaoid: Oid, newOwnerId: Oid) {
    let tup: HeapTuple;
    let rel: Relation;

    rel = table_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaoid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for schema {}", schemaoid);
    }

    AlterSchemaOwner_internal(tup, rel, newOwnerId);

    ReleaseSysCache(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Change schema owner
 */
pub unsafe fn AlterSchemaOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let nspOid: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let mut address: ObjectAddress = InvalidObjectAddress();
    let nspform: Form_pg_namespace;

    rel = table_open(NamespaceRelationId, RowExclusiveLock);

    tup = SearchSysCache1(NAMESPACENAME, CStringGetDatum(name));
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, "schema does not exist");
    }

    nspform = GETSTRUCT(tup) as Form_pg_namespace;
    nspOid = (*nspform).oid;

    AlterSchemaOwner_internal(tup, rel, newOwnerId);

    ObjectAddressSet(&mut address, NamespaceRelationId, nspOid);

    ReleaseSysCache(tup);

    table_close(rel, RowExclusiveLock);

    address
}

unsafe fn AlterSchemaOwner_internal(tup: HeapTuple, rel: Relation, newOwnerId: Oid) {
    let nspForm: Form_pg_namespace;

    Assert!((*tup).t_tableOid == NamespaceRelationId);
    Assert!(RelationGetRelid(rel) == NamespaceRelationId);

    nspForm = GETSTRUCT(tup) as Form_pg_namespace;

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (*nspForm).nspowner != newOwnerId {
        let mut repl_val: [Datum; Natts_pg_namespace] = [0; Natts_pg_namespace];
        let mut repl_null: [bool; Natts_pg_namespace] = [false; Natts_pg_namespace];
        let mut repl_repl: [bool; Natts_pg_namespace] = [false; Natts_pg_namespace];
        let newAcl: *mut Acl;
        let aclDatum: Datum;
        let mut isNull: bool = false;
        let newtuple: HeapTuple;
        let aclresult: AclResult;

        /* Otherwise, must be owner of the existing object */
        if !object_ownercheck(NamespaceRelationId, (*nspForm).oid, GetUserId()) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                OBJECT_SCHEMA,
                NameStr(&(*nspForm).nspname),
            );
        }

        /* Must be able to become new owner */
        check_can_set_role(GetUserId(), newOwnerId);

        /*
         * must have create-schema rights
         *
         * NOTE: This is different from other alter-owner checks in that the
         * current user is checked for create privileges instead of the
         * destination owner.  This is consistent with the CREATE case for
         * schemas.  Because superusers will always have this right, we need
         * no special case for them.
         */
        aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                OBJECT_DATABASE,
                get_database_name(MyDatabaseId),
            );
        }

        repl_null.iter_mut().for_each(|x| *x = false);
        repl_repl.iter_mut().for_each(|x| *x = false);

        repl_repl[(Anum_pg_namespace_nspowner - 1) as usize] = true;
        repl_val[(Anum_pg_namespace_nspowner - 1) as usize] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = SysCacheGetAttr(NAMESPACENAME, tup, Anum_pg_namespace_nspacl, &mut isNull);
        if !isNull {
            newAcl = aclnewowner(
                DatumGetAclP(aclDatum),
                (*nspForm).nspowner,
                newOwnerId,
            );
            repl_repl[(Anum_pg_namespace_nspacl - 1) as usize] = true;
            repl_val[(Anum_pg_namespace_nspacl - 1) as usize] =
                PointerGetDatum(newAcl as *const c_void);
        }

        newtuple = heap_modify_tuple(
            tup,
            RelationGetDescr(rel),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(rel, &mut (*newtuple).t_self as *mut _ as *mut c_void, newtuple);

        heap_freetuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(NamespaceRelationId, (*nspForm).oid, newOwnerId);
    }

    InvokeObjectPostAlterHook(NamespaceRelationId, (*nspForm).oid, 0);
}

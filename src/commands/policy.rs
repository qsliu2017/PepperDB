//! policy.rs
//!   Commands for manipulating policies.
//!
//! Translated 1:1 from postgres/src/backend/commands/policy.c
//! (declarations merged from postgres/src/include/commands/policy.h).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::{foreach, current_cell, DirectFunctionCall1};

// ---------------------------------------------------------------------------
// Real imports from already-translated modules.
// ---------------------------------------------------------------------------

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::{
    heap_copytuple, heap_form_tuple, heap_freetuple, heap_modify_tuple,
};
use crate::access::common::relation::{relation_close, relation_open};
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{heap_getattr, HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::relscan::SysScanDescData;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};

use crate::catalog::catalog_oids::{AuthIdRelationId, PolicyRelationId, RelationRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_class::{Form_pg_class, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use crate::catalog::pg_policy::Form_pg_policy;

use crate::miscadmin::allowSystemTableMods;

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterPolicyStmt, CreatePolicyStmt, RenameStmt, RoleSpec, ROLESPEC_PUBLIC,
};
use crate::nodes::pg_list::{lcons, lfirst, list_length, List, ListCell, NIL};
use crate::nodes::primnodes::RangeVar;

use crate::parser::parse_collate::assign_expr_collations;
use crate::parser::parse_node::{
    free_parsestate, make_parsestate, ParseNamespaceItem, ParseState,
};

use crate::storage::itemptr::ItemPointerData;
use crate::storage::lockdefs::{
    AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE,
};

use crate::utils::adt::acl::{
    get_rolespec_oid, ACL_DELETE_CHR, ACL_ID_PUBLIC, ACL_INSERT_CHR, ACL_SELECT_CHR,
    ACL_UPDATE_CHR,
};
use crate::utils::adt::arrayfuncs::construct_array_builtin;
use crate::utils::array::{ArrayType, ARR_DATA_PTR, ARR_DIMS};
use crate::utils::adt::name::{namein, namestrcpy};
use crate::utils::builtins::{CStringGetTextDatum, TextDatumGetCString};
use crate::utils::mmgr::mcxt::{
    CacheMemoryContext, MemoryContextSetParent, MemoryContextStrdup,
};
use crate::utils::rel::{
    RegProcedure, Relation, RelationGetDescr, RelationGetRelationName, RelationGetRelid,
};
use crate::utils::snapshot::SnapshotData;

// ---------------------------------------------------------------------------
// Constants from generated catalog/fmgr headers that are not yet ported.
// Values match PostgreSQL 18.3.
// ---------------------------------------------------------------------------

// catalog/pg_policy.h - generated Natts_/Anum_ constants.
// TODO(pg-port): replace with generated catalog/pg_policy_d.h constants.
const Natts_pg_policy: usize = 8;
const Anum_pg_policy_oid: AttrNumber = 1;
const Anum_pg_policy_polname: AttrNumber = 2;
const Anum_pg_policy_polrelid: AttrNumber = 3;
const Anum_pg_policy_polcmd: AttrNumber = 4;
const Anum_pg_policy_polpermissive: AttrNumber = 5;
const Anum_pg_policy_polroles: AttrNumber = 6;
const Anum_pg_policy_polqual: AttrNumber = 7;
const Anum_pg_policy_polwithcheck: AttrNumber = 8;

// catalog/indexing.h - pg_policy index OIDs.
// TODO(pg-port): replace with generated catalog/indexing.h constants.
const PolicyOidIndexId: Oid = 3257; // pg_policy_oid_index
const PolicyPolrelidPolnameIndexId: Oid = 3258; // pg_policy_polrelid_polname_index

// utils/fmgroids.h - regproc OIDs.
// TODO(pg-port): replace with the generated utils/fmgroids.h constants.
const F_OIDEQ: RegProcedure = 184;
const F_NAMEEQ: RegProcedure = 62;

// catalog/pg_type.h - OIDOID.
// TODO(pg-port): replace with generated catalog/pg_type_d.h constant.
const OIDOID: Oid = 26;

// utils/cache/syscache.h - SysCacheIdentifier RELOID.
// TODO(pg-port): replace with generated utils/syscache.h constant.
const RELOID: c_int = 0;

// catalog/dependency.h - DependencyType.
// TODO(pg-port): replace with the catalog/dependency.h DependencyType enum.
const DEPENDENCY_NORMAL: c_int = b'n' as c_int;
const DEPENDENCY_AUTO: c_int = b'a' as c_int;

// catalog/dependency.h - SharedDependencyType.
// TODO(pg-port): replace with the catalog/dependency.h SharedDependencyType enum.
const SHARED_DEPENDENCY_POLICY: c_int = b'r' as c_int;

// utils/acl.h - AclResult.
// TODO(pg-port): replace with generated utils/acl.h AclResult enum.
const ACLCHECK_NOT_OWNER: c_int = 1;

// utils/errcodes.h - SQLSTATE codes.
// TODO(pg-port): replace with the generated utils/errcodes.h constants.
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// parser/parse_node.h - ParseExprKind variant used here.
// EXPR_KIND_POLICY lives in parse_node.rs; the parser entry points that consume
// it (parser/parse_clause.c, parser/parse_relation.c) are not ported yet, so the
// value is passed through the local stubs below as a plain c_int.
// TODO(pg-port): use crate::parser::parse_node::ParseExprKind::EXPR_KIND_POLICY.
const EXPR_KIND_POLICY: c_int = 0;

// ---------------------------------------------------------------------------
// Stubs for called functions whose real home is not yet translated.
// ---------------------------------------------------------------------------

/* TODO(pg-port): access/genam.h - systable scan helpers not ported yet (the
 * ported crate::access::index::genam uses a *mut c_void HeapTuple alias, so the
 * scan helpers are stubbed here against the real HeapTupleData pointer type). */
type SysScanDesc = *mut SysScanDescData;

unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut SnapshotData,
    _nkeys: c_int,
    _key: ScanKey,
) -> SysScanDesc {
    unimplemented!()
}

unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!()
}

unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!()
}

/* TODO(pg-port): catalog/indexing.h - heap+index DML helpers not ported yet. */
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {
    unimplemented!()
}

unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) {
    unimplemented!()
}

unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut ItemPointerData) {
    unimplemented!()
}

/* TODO(pg-port): catalog/catalog.c - GetNewOidWithIndex not ported yet. */
unsafe fn GetNewOidWithIndex(
    _relation: Relation,
    _indexId: Oid,
    _oidcolumn: AttrNumber,
) -> Oid {
    unimplemented!()
}

/* TODO(pg-port): catalog/catalog.c - system relation predicates not ported yet. */
unsafe fn IsSystemRelation(_relation: Relation) -> bool {
    unimplemented!()
}

unsafe fn IsSystemClass(_relid: Oid, _reltuple: Form_pg_class) -> bool {
    unimplemented!()
}

/* TODO(pg-port): catalog/aclchk.c - ownership checks not ported yet. */
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!()
}

unsafe fn aclcheck_error(_aclerr: c_int, _objtype: c_int, _objectname: *const c_char) {
    unimplemented!()
}

/* TODO(pg-port): catalog/objectaddress.c - relkind -> object type mapping. */
unsafe fn get_relkind_objtype(_relkind: c_char) -> c_int {
    unimplemented!()
}

/* TODO(pg-port): catalog/namespace.c - RangeVarGetRelidExtended not ported yet. */
type RangeVarGetRelidCallback =
    Option<unsafe extern "C" fn(*const RangeVar, Oid, Oid, *mut c_void)>;

unsafe fn RangeVarGetRelidExtended(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _flags: u32,
    _callback: RangeVarGetRelidCallback,
    _callback_arg: *mut c_void,
) -> Oid {
    unimplemented!()
}

/* TODO(pg-port): catalog/pg_depend.c - dependency record helpers not ported yet. */
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_int,
) {
    unimplemented!()
}

unsafe fn deleteDependencyRecordsFor(
    _classId: Oid,
    _objectId: Oid,
    _skipExtensionDeps: bool,
) -> c_long {
    unimplemented!()
}

/* TODO(pg-port): catalog/pg_shdepend.c - shared dependency helpers not ported yet. */
unsafe fn recordSharedDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _deptype: c_int,
) {
    unimplemented!()
}

unsafe fn deleteSharedDependencyRecordsFor(_classId: Oid, _objectId: Oid, _objectSubId: c_int) {
    unimplemented!()
}

/* TODO(pg-port): rewrite/rewriteManip.c - recordDependencyOnExpr not ported yet. */
unsafe fn recordDependencyOnExpr(
    _depender: *const ObjectAddress,
    _expr: *const Node,
    _rtable: *mut List,
    _behavior: c_int,
) {
    unimplemented!()
}

/* TODO(pg-port): parser/parse_relation.c - range table entry helpers. */
unsafe fn addRangeTableEntryForRelation(
    _pstate: *mut ParseState,
    _rel: Relation,
    _lockmode: LOCKMODE,
    _alias: *mut c_void,
    _inh: bool,
    _inFromCl: bool,
) -> *mut ParseNamespaceItem {
    unimplemented!()
}

unsafe fn addNSItemToQuery(
    _pstate: *mut ParseState,
    _nsitem: *mut ParseNamespaceItem,
    _addToJoinList: bool,
    _addToRelNameSpace: bool,
    _addToVarNameSpace: bool,
) {
    unimplemented!()
}

/* TODO(pg-port): parser/parse_clause.c - transformWhereClause not ported yet. */
unsafe fn transformWhereClause(
    _pstate: *mut ParseState,
    _clause: *mut Node,
    _exprKind: c_int,
    _constructName: *const c_char,
) -> *mut Node {
    unimplemented!()
}

/* TODO(pg-port): optimizer/util/clauses.c - checkExprHasSubLink not ported yet. */
unsafe fn checkExprHasSubLink(_node: *mut Node) -> bool {
    unimplemented!()
}

/* TODO(pg-port): nodes/outfuncs.c - nodeToString not ported yet. */
unsafe fn nodeToString(_obj: *const c_void) -> *mut c_char {
    unimplemented!()
}

/* TODO(pg-port): nodes/read.c - stringToNode not ported yet. */
unsafe fn stringToNode(_str: *mut c_char) -> *mut c_void {
    unimplemented!()
}

/* TODO(pg-port): utils/cache/syscache.c - syscache lookups not ported yet. */
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

/* TODO(pg-port): utils/cache/inval.c - relcache invalidation not ported yet. */
unsafe fn CacheInvalidateRelcache(_relation: Relation) {
    unimplemented!()
}

unsafe fn CacheInvalidateRelcacheByTuple(_classTuple: HeapTuple) {
    unimplemented!()
}

/* TODO(pg-port): access/transam/xact.c - CommandCounterIncrement not ported yet. */
unsafe fn CommandCounterIncrement() {
    unimplemented!()
}

/* TODO(pg-port): utils/cache/lsyscache.c - relation name/kind lookups. */
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    unimplemented!()
}

unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    unimplemented!()
}

/* TODO(pg-port): utils/init/miscinit.c - current user id. */
unsafe fn GetUserId() -> Oid {
    unimplemented!()
}

/* TODO(pg-port): utils/array.c - DatumGetArrayTypePCopy not ported yet. */
unsafe fn DatumGetArrayTypePCopy(_d: Datum) -> *mut ArrayType {
    unimplemented!()
}

/*
 * TODO(pg-port): catalog/objectaddress.h - ObjectAddressSet sets the three
 * fields of an ObjectAddress.  This is a macro in C; rendered as a helper here.
 */
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, classId: Oid, objectId: Oid) {
    addr.classId = classId;
    addr.objectId = objectId;
    addr.objectSubId = 0;
}

/*
 * TODO(pg-port): catalog/objectaccess.h - InvokeObjectPostCreateHook /
 * InvokeObjectPostAlterHook are no-op wrapper macros unless an access hook is
 * registered; the hook plumbing is not ported yet.
 */
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}

unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}

/*
 * TODO(pg-port): utils/mmgr/mcxt.c - MemoryContextCopyAndSetIdentifier copies
 * the identifier string into the context for diagnostics; not ported yet.
 */
unsafe fn MemoryContextCopyAndSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Row-level security descriptor structs (rewrite/rowsecurity.h).  These live in
// crate::rewrite::rowsecurity; re-aliased here for RelationBuildRowSecurity.
// ---------------------------------------------------------------------------

use crate::rewrite::rowsecurity::{RowSecurityDesc, RowSecurityPolicy};

// ---------------------------------------------------------------------------

/*
 * Callback to RangeVarGetRelidExtended().
 *
 * Checks the following:
 *	- the relation specified is a table.
 *	- current user owns the table.
 *	- the table is not a system table.
 *
 * If any of these checks fails then an error is raised.
 */
unsafe extern "C" fn RangeVarCallbackForPolicy(
    rv: *const RangeVar,
    relid: Oid,
    _oldrelid: Oid,
    _arg: *mut c_void,
) {
    let tuple: HeapTuple;
    let classform: Form_pg_class;
    let relkind: c_char;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        return;
    }

    classform = GETSTRUCT(tuple) as Form_pg_class;
    relkind = (*classform).relkind;

    /* Must own relation. */
    if !object_ownercheck(RelationRelationId, relid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(relid)),
            (*rv).relname,
        );
    }

    /* No system table modifications unless explicitly allowed. */
    if !allowSystemTableMods && IsSystemClass(relid, classform) {
        ereport!(
            ERROR,
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                cstr_display((*rv).relname)
            )
        );
    }

    /* Relation type MUST be a table. */
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        ereport!(
            ERROR,
            errmsg!("\"{}\" is not a table", cstr_display((*rv).relname))
        );
    }

    ReleaseSysCache(tuple);
}

/*
 * parse_policy_command -
 *	 helper function to convert full command strings to their char
 *	 representation.
 *
 * cmd_name - full string command name. Valid values are 'all', 'select',
 *			  'insert', 'update' and 'delete'.
 *
 */
unsafe fn parse_policy_command(cmd_name: *const c_char) -> c_char {
    let polcmd: c_char;

    if cmd_name.is_null() {
        elog!(ERROR, "unrecognized policy command");
        unreachable!();
    }

    if strcmp(cmd_name, c"all".as_ptr()) == 0 {
        polcmd = b'*' as c_char;
    } else if strcmp(cmd_name, c"select".as_ptr()) == 0 {
        polcmd = ACL_SELECT_CHR;
    } else if strcmp(cmd_name, c"insert".as_ptr()) == 0 {
        polcmd = ACL_INSERT_CHR;
    } else if strcmp(cmd_name, c"update".as_ptr()) == 0 {
        polcmd = ACL_UPDATE_CHR;
    } else if strcmp(cmd_name, c"delete".as_ptr()) == 0 {
        polcmd = ACL_DELETE_CHR;
    } else {
        elog!(ERROR, "unrecognized policy command");
        unreachable!();
    }

    polcmd
}

/*
 * policy_role_list_to_array
 *	 helper function to convert a list of RoleSpecs to an array of
 *	 role id Datums.
 */
unsafe fn policy_role_list_to_array(roles: *mut List, num_roles: *mut c_int) -> *mut Datum {
    let role_oids: *mut Datum;
    let mut cell: *mut ListCell;
    let mut i: c_int = 0;

    /* Handle no roles being passed in as being for public */
    if roles == NIL {
        *num_roles = 1;
        role_oids =
            palloc((*num_roles as usize) * std::mem::size_of::<Datum>()) as *mut Datum;
        *role_oids.offset(0) = ObjectIdGetDatum(ACL_ID_PUBLIC);

        return role_oids;
    }

    *num_roles = list_length(roles);
    role_oids = palloc((*num_roles as usize) * std::mem::size_of::<Datum>()) as *mut Datum;

    foreach!(cell, roles, {
        let spec: *mut RoleSpec = lfirst(current_cell!(cell)) as *mut RoleSpec;

        /*
         * PUBLIC covers all roles, so it only makes sense alone.
         */
        if (*spec).roletype == ROLESPEC_PUBLIC {
            if *num_roles != 1 {
                ereport!(
                    WARNING,
                    errmsg!("ignoring specified roles other than PUBLIC")
                );
                /* errhint: All roles are members of the PUBLIC role. */
                *num_roles = 1;
            }
            *role_oids.offset(0) = ObjectIdGetDatum(ACL_ID_PUBLIC);

            return role_oids;
        } else {
            *role_oids.offset(i as isize) = ObjectIdGetDatum(get_rolespec_oid(spec, false));
            i += 1;
        }
    });

    role_oids
}

/*
 * Load row security policy from the catalog, and store it in
 * the relation's relcache entry.
 *
 * Note that caller should have verified that pg_class.relrowsecurity
 * is true for this relation.
 */
pub unsafe fn RelationBuildRowSecurity(relation: Relation) {
    let rscxt: MemoryContext;
    let oldcxt: MemoryContext = CurrentMemoryContext;
    let rsdesc: *mut RowSecurityDesc;
    let catalog: Relation;
    let mut skey: ScanKeyData = std::mem::zeroed();
    let sscan: SysScanDesc;
    let mut tuple: HeapTuple;

    /*
     * Create a memory context to hold everything associated with this
     * relation's row security policy.  This makes it easy to clean up during
     * a relcache flush.  However, to cover the possibility of an error
     * partway through, we don't make the context long-lived till we're done.
     */
    rscxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        "row security descriptor",
        ALLOCSET_SMALL_SIZES
    );
    MemoryContextCopyAndSetIdentifier(rscxt, RelationGetRelationName(relation));

    rsdesc = MemoryContextAllocZero(rscxt, std::mem::size_of::<RowSecurityDesc>())
        as *mut RowSecurityDesc;
    (*rsdesc).rscxt = rscxt;

    /*
     * Now scan pg_policy for RLS policies associated with this relation.
     * Because we use the index on (polrelid, polname), we should consistently
     * visit the rel's policies in name order, at least when system indexes
     * aren't disabled.  This simplifies equalRSDesc().
     */
    catalog = table_open(PolicyRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey,
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    sscan = systable_beginscan(
        catalog,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        1,
        &mut skey,
    );

    loop {
        tuple = systable_getnext(sscan);
        if !HeapTupleIsValid(tuple) {
            break;
        }

        let policy_form: Form_pg_policy = GETSTRUCT(tuple) as Form_pg_policy;
        let policy: *mut RowSecurityPolicy;
        let mut datum: Datum;
        let mut isnull: bool = false;
        let mut str_value: *mut c_char;

        policy = MemoryContextAllocZero(rscxt, std::mem::size_of::<RowSecurityPolicy>())
            as *mut RowSecurityPolicy;

        /*
         * Note: we must be sure that pass-by-reference data gets copied into
         * rscxt.  We avoid making that context current over wider spans than
         * we have to, though.
         */

        /* Get policy command */
        (*policy).polcmd = (*policy_form).polcmd;

        /* Get policy, permissive or restrictive */
        (*policy).permissive = (*policy_form).polpermissive;

        /* Get policy name */
        (*policy).policy_name =
            MemoryContextStrdup(rscxt as crate::utils::mmgr::memnodes::MemoryContext, NameStr(&(*policy_form).polname));

        /* Get policy roles */
        datum = heap_getattr(
            tuple,
            Anum_pg_policy_polroles as c_int,
            RelationGetDescr(catalog),
            &mut isnull,
        );
        /* shouldn't be null, but let's check for luck */
        if isnull {
            elog!(ERROR, "unexpected null value in pg_policy.polroles");
        }
        MemoryContextSwitchTo(rscxt);
        (*policy).roles = DatumGetArrayTypePCopy(datum);
        MemoryContextSwitchTo(oldcxt);

        /* Get policy qual */
        datum = heap_getattr(
            tuple,
            Anum_pg_policy_polqual as c_int,
            RelationGetDescr(catalog),
            &mut isnull,
        );
        if !isnull {
            str_value = TextDatumGetCString(datum);
            MemoryContextSwitchTo(rscxt);
            (*policy).qual = stringToNode(str_value) as *mut _;
            MemoryContextSwitchTo(oldcxt);
            pfree(str_value as *mut c_void);
        } else {
            (*policy).qual = null_mut();
        }

        /* Get WITH CHECK qual */
        datum = heap_getattr(
            tuple,
            Anum_pg_policy_polwithcheck as c_int,
            RelationGetDescr(catalog),
            &mut isnull,
        );
        if !isnull {
            str_value = TextDatumGetCString(datum);
            MemoryContextSwitchTo(rscxt);
            (*policy).with_check_qual = stringToNode(str_value) as *mut _;
            MemoryContextSwitchTo(oldcxt);
            pfree(str_value as *mut c_void);
        } else {
            (*policy).with_check_qual = null_mut();
        }

        /* We want to cache whether there are SubLinks in these expressions */
        (*policy).hassublinks = checkExprHasSubLink((*policy).qual as *mut Node)
            || checkExprHasSubLink((*policy).with_check_qual as *mut Node);

        /*
         * Add this object to list.  For historical reasons, the list is built
         * in reverse order.
         */
        MemoryContextSwitchTo(rscxt);
        (*rsdesc).policies = lcons(policy as *mut c_void, (*rsdesc).policies);
        MemoryContextSwitchTo(oldcxt);
    }

    systable_endscan(sscan);
    table_close(catalog, AccessShareLock);

    /*
     * Success.  Reparent the descriptor's memory context under
     * CacheMemoryContext so that it will live indefinitely, then attach the
     * policy descriptor to the relcache entry.
     */
    MemoryContextSetParent(rscxt as crate::utils::mmgr::memnodes::MemoryContext, CacheMemoryContext);

    (*relation).rd_rsdesc = rsdesc as *mut c_void;
}

/*
 * RemovePolicyById -
 *	 remove a policy by its OID.  If a policy does not exist with the provided
 *	 oid, then an error is raised.
 *
 * policy_id - the oid of the policy.
 */
pub unsafe fn RemovePolicyById(policy_id: Oid) {
    let pg_policy_rel: Relation;
    let sscan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = std::mem::zeroed();
    let tuple: HeapTuple;
    let relid: Oid;
    let rel: Relation;

    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    /*
     * Find the policy to delete.
     */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(policy_id),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyOidIndexId,
        true,
        null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    tuple = systable_getnext(sscan);

    /* If the policy exists, then remove it, otherwise raise an error. */
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "could not find tuple for policy {}", policy_id);
    }

    /*
     * Open and exclusive-lock the relation the policy belongs to.  (We need
     * exclusive lock to lock out queries that might otherwise depend on the
     * set of policies the rel has; furthermore we've got to hold the lock
     * till commit.)
     */
    relid = (*(GETSTRUCT(tuple) as Form_pg_policy)).polrelid;

    rel = table_open(relid, AccessExclusiveLock);
    if (*(*rel).rd_rel).relkind != RELKIND_RELATION
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a table",
                cstr_display(RelationGetRelationName(rel))
            )
        );
    }

    if !allowSystemTableMods && IsSystemRelation(rel) {
        ereport!(
            ERROR,
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                cstr_display(RelationGetRelationName(rel))
            )
        );
    }

    CatalogTupleDelete(pg_policy_rel, &mut (*tuple).t_self);

    systable_endscan(sscan);

    /*
     * Note that, unlike some of the other flags in pg_class, relrowsecurity
     * is not just an indication of if policies exist.  When relrowsecurity is
     * set by a user, then all access to the relation must be through a
     * policy.  If no policy is defined for the relation then a default-deny
     * policy is created and all records are filtered (except for queries from
     * the owner).
     */
    CacheInvalidateRelcache(rel);

    table_close(rel, NoLock);

    /* Clean up */
    table_close(pg_policy_rel, RowExclusiveLock);
}

/*
 * RemoveRoleFromObjectPolicy -
 *	 remove a role from a policy's applicable-roles list.
 *
 * Returns true if the role was successfully removed from the policy.
 * Returns false if the role was not removed because it would have left
 * polroles empty (which is disallowed, though perhaps it should not be).
 * On false return, the caller should instead drop the policy altogether.
 *
 * roleid - the oid of the role to remove
 * classid - should always be PolicyRelationId
 * policy_id - the oid of the policy.
 */
pub unsafe fn RemoveRoleFromObjectPolicy(roleid: Oid, classid: Oid, policy_id: Oid) -> bool {
    let pg_policy_rel: Relation;
    let sscan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = std::mem::zeroed();
    let tuple: HeapTuple;
    let relid: Oid;
    let policy_roles: *mut ArrayType;
    let roles_datum: Datum;
    let roles: *mut Oid;
    let mut num_roles: c_int;
    let role_oids: *mut Datum;
    let mut attr_isnull: bool = false;
    let mut keep_policy: bool = true;
    let mut i: c_int;
    let mut j: c_int;

    Assert!(classid == PolicyRelationId);

    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    /*
     * Find the policy to update.
     */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(policy_id),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyOidIndexId,
        true,
        null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    tuple = systable_getnext(sscan);

    /* Raise an error if we don't find the policy. */
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "could not find tuple for policy {}", policy_id);
    }

    /* Identify rel the policy belongs to */
    relid = (*(GETSTRUCT(tuple) as Form_pg_policy)).polrelid;

    /* Get the current set of roles */
    roles_datum = heap_getattr(
        tuple,
        Anum_pg_policy_polroles as c_int,
        RelationGetDescr(pg_policy_rel),
        &mut attr_isnull,
    );

    Assert!(!attr_isnull);

    policy_roles = DatumGetArrayTypePCopy(roles_datum);
    roles = ARR_DATA_PTR(policy_roles) as *mut Oid;
    num_roles = *ARR_DIMS(policy_roles).offset(0);

    /*
     * Rebuild the polroles array, without any mentions of the target role.
     * Ordinarily there'd be exactly one, but we must cope with duplicate
     * mentions, since CREATE/ALTER POLICY historically have allowed that.
     */
    role_oids = palloc((num_roles as usize) * std::mem::size_of::<Datum>()) as *mut Datum;
    i = 0;
    j = 0;
    while i < num_roles {
        if *roles.offset(i as isize) != roleid {
            *role_oids.offset(j as isize) = ObjectIdGetDatum(*roles.offset(i as isize));
            j += 1;
        }
        i += 1;
    }
    num_roles = j;

    /* If any roles remain, update the policy entry. */
    if num_roles > 0 {
        let role_ids: *mut ArrayType;
        let mut values: [Datum; Natts_pg_policy] = std::mem::zeroed();
        let mut isnull: [bool; Natts_pg_policy] = std::mem::zeroed();
        let mut replaces: [bool; Natts_pg_policy] = std::mem::zeroed();
        let new_tuple: HeapTuple;
        let reltup: HeapTuple;
        let mut target: ObjectAddress = std::mem::zeroed();
        let mut myself: ObjectAddress = std::mem::zeroed();

        /* zero-clear */
        for k in 0..Natts_pg_policy {
            values[k] = 0;
            replaces[k] = false;
            isnull[k] = false;
        }

        /* This is the array for the new tuple */
        role_ids = construct_array_builtin(role_oids, num_roles, OIDOID);

        replaces[(Anum_pg_policy_polroles - 1) as usize] = true;
        values[(Anum_pg_policy_polroles - 1) as usize] =
            PointerGetDatum(role_ids as *const c_void);

        new_tuple = heap_modify_tuple(
            tuple,
            RelationGetDescr(pg_policy_rel),
            values.as_ptr(),
            isnull.as_ptr(),
            replaces.as_ptr(),
        );
        CatalogTupleUpdate(pg_policy_rel, &mut (*new_tuple).t_self, new_tuple);

        /* Remove all the old shared dependencies (roles) */
        deleteSharedDependencyRecordsFor(PolicyRelationId, policy_id, 0);

        /* Record the new shared dependencies (roles) */
        myself.classId = PolicyRelationId;
        myself.objectId = policy_id;
        myself.objectSubId = 0;

        target.classId = AuthIdRelationId;
        target.objectSubId = 0;
        i = 0;
        while i < num_roles {
            target.objectId = DatumGetObjectId(*role_oids.offset(i as isize));
            /* no need for dependency on the public role */
            if target.objectId != ACL_ID_PUBLIC {
                recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY);
            }
            i += 1;
        }

        InvokeObjectPostAlterHook(PolicyRelationId, policy_id, 0);

        heap_freetuple(new_tuple);

        /* Make updates visible */
        CommandCounterIncrement();

        /*
         * Invalidate relcache entry for rel the policy belongs to, to force
         * redoing any dependent plans.  In case of a race condition where the
         * rel was just dropped, we need do nothing.
         */
        reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if HeapTupleIsValid(reltup) {
            CacheInvalidateRelcacheByTuple(reltup);
            ReleaseSysCache(reltup);
        }
    } else {
        /* No roles would remain, so drop the policy instead. */
        keep_policy = false;
    }

    /* Clean up. */
    systable_endscan(sscan);

    table_close(pg_policy_rel, RowExclusiveLock);

    keep_policy
}

/*
 * CreatePolicy -
 *	 handles the execution of the CREATE POLICY command.
 *
 * stmt - the CreatePolicyStmt that describes the policy to create.
 */
pub unsafe fn CreatePolicy(stmt: *mut CreatePolicyStmt) -> ObjectAddress {
    let pg_policy_rel: Relation;
    let policy_id: Oid;
    let target_table: Relation;
    let table_id: Oid;
    let polcmd: c_char;
    let role_oids: *mut Datum;
    let mut nitems: c_int = 0;
    let role_ids: *mut ArrayType;
    let qual_pstate: *mut ParseState;
    let with_check_pstate: *mut ParseState;
    let mut nsitem: *mut ParseNamespaceItem;
    let qual: *mut Node;
    let with_check_qual: *mut Node;
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let sscan: SysScanDesc;
    let mut policy_tuple: HeapTuple;
    let mut values: [Datum; Natts_pg_policy] = std::mem::zeroed();
    let mut isnull: [bool; Natts_pg_policy] = std::mem::zeroed();
    let mut target: ObjectAddress = std::mem::zeroed();
    let mut myself: ObjectAddress = std::mem::zeroed();
    let mut i: c_int;

    /* Parse command */
    polcmd = parse_policy_command((*stmt).cmd_name);

    /*
     * If the command is SELECT or DELETE then WITH CHECK should be NULL.
     */
    if (polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && (*stmt).with_check != null_mut() {
        ereport!(
            ERROR,
            errmsg!("WITH CHECK cannot be applied to SELECT or DELETE")
        );
    }

    /*
     * If the command is INSERT then WITH CHECK should be the only expression
     * provided.
     */
    if polcmd == ACL_INSERT_CHR && (*stmt).qual != null_mut() {
        ereport!(
            ERROR,
            errmsg!("only WITH CHECK expression allowed for INSERT")
        );
    }

    /* Collect role ids */
    role_oids = policy_role_list_to_array((*stmt).roles, &mut nitems);
    role_ids = construct_array_builtin(role_oids, nitems, OIDOID);

    /* Parse the supplied clause */
    qual_pstate = make_parsestate(null_mut());
    with_check_pstate = make_parsestate(null_mut());

    /* zero-clear */
    for k in 0..Natts_pg_policy {
        values[k] = 0;
        isnull[k] = false;
    }

    /* Get id of table.  Also handles permissions checks. */
    table_id = RangeVarGetRelidExtended(
        (*stmt).table,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForPolicy),
        stmt as *mut c_void,
    );

    /* Open target_table to build quals. No additional lock is necessary. */
    target_table = relation_open(table_id, NoLock);

    /* Add for the regular security quals */
    nsitem = addRangeTableEntryForRelation(
        qual_pstate,
        target_table,
        AccessShareLock,
        null_mut(),
        false,
        false,
    );
    addNSItemToQuery(qual_pstate, nsitem, false, true, true);

    /* Add for the with-check quals */
    nsitem = addRangeTableEntryForRelation(
        with_check_pstate,
        target_table,
        AccessShareLock,
        null_mut(),
        false,
        false,
    );
    addNSItemToQuery(with_check_pstate, nsitem, false, true, true);

    qual = transformWhereClause(
        qual_pstate,
        (*stmt).qual,
        EXPR_KIND_POLICY,
        c"POLICY".as_ptr(),
    );

    with_check_qual = transformWhereClause(
        with_check_pstate,
        (*stmt).with_check,
        EXPR_KIND_POLICY,
        c"POLICY".as_ptr(),
    );

    /* Fix up collation information */
    assign_expr_collations(qual_pstate, qual);
    assign_expr_collations(with_check_pstate, with_check_qual);

    /* Open pg_policy catalog */
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    /* Set key - policy's relation id. */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(table_id),
    );

    /* Set key - policy's name. */
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_policy_polname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum((*stmt).policy_name),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    policy_tuple = systable_getnext(sscan);

    /* Complain if the policy name already exists for the table */
    if HeapTupleIsValid(policy_tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "policy \"{}\" for table \"{}\" already exists",
                cstr_display((*stmt).policy_name),
                cstr_display(RelationGetRelationName(target_table))
            )
        );
    }

    policy_id = GetNewOidWithIndex(pg_policy_rel, PolicyOidIndexId, Anum_pg_policy_oid);
    values[(Anum_pg_policy_oid - 1) as usize] = ObjectIdGetDatum(policy_id);
    values[(Anum_pg_policy_polrelid - 1) as usize] = ObjectIdGetDatum(table_id);
    values[(Anum_pg_policy_polname - 1) as usize] =
        DirectFunctionCall1!(namein, CStringGetDatum((*stmt).policy_name));
    values[(Anum_pg_policy_polcmd - 1) as usize] = CharGetDatum(polcmd);
    values[(Anum_pg_policy_polpermissive - 1) as usize] = BoolGetDatum((*stmt).permissive);
    values[(Anum_pg_policy_polroles - 1) as usize] = PointerGetDatum(role_ids as *const c_void);

    /* Add qual if present. */
    if !qual.is_null() {
        values[(Anum_pg_policy_polqual - 1) as usize] =
            CStringGetTextDatum(nodeToString(qual as *const c_void));
    } else {
        isnull[(Anum_pg_policy_polqual - 1) as usize] = true;
    }

    /* Add WITH CHECK qual if present */
    if !with_check_qual.is_null() {
        values[(Anum_pg_policy_polwithcheck - 1) as usize] =
            CStringGetTextDatum(nodeToString(with_check_qual as *const c_void));
    } else {
        isnull[(Anum_pg_policy_polwithcheck - 1) as usize] = true;
    }

    policy_tuple = heap_form_tuple(
        RelationGetDescr(pg_policy_rel),
        values.as_ptr(),
        isnull.as_ptr(),
    );

    CatalogTupleInsert(pg_policy_rel, policy_tuple);

    /* Record Dependencies */
    target.classId = RelationRelationId;
    target.objectId = table_id;
    target.objectSubId = 0;

    myself.classId = PolicyRelationId;
    myself.objectId = policy_id;
    myself.objectSubId = 0;

    recordDependencyOn(&myself, &target, DEPENDENCY_AUTO);

    recordDependencyOnExpr(&myself, qual, (*qual_pstate).p_rtable, DEPENDENCY_NORMAL);

    recordDependencyOnExpr(
        &myself,
        with_check_qual,
        (*with_check_pstate).p_rtable,
        DEPENDENCY_NORMAL,
    );

    /* Register role dependencies */
    target.classId = AuthIdRelationId;
    target.objectSubId = 0;
    i = 0;
    while i < nitems {
        target.objectId = DatumGetObjectId(*role_oids.offset(i as isize));
        /* no dependency if public */
        if target.objectId != ACL_ID_PUBLIC {
            recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY);
        }
        i += 1;
    }

    InvokeObjectPostCreateHook(PolicyRelationId, policy_id, 0);

    /* Invalidate Relation Cache */
    CacheInvalidateRelcache(target_table);

    /* Clean up. */
    heap_freetuple(policy_tuple);
    free_parsestate(qual_pstate);
    free_parsestate(with_check_pstate);
    systable_endscan(sscan);
    relation_close(target_table, NoLock);
    table_close(pg_policy_rel, RowExclusiveLock);

    myself
}

/*
 * AlterPolicy -
 *	 handles the execution of the ALTER POLICY command.
 *
 * stmt - the AlterPolicyStmt that describes the policy and how to alter it.
 */
pub unsafe fn AlterPolicy(stmt: *mut AlterPolicyStmt) -> ObjectAddress {
    let pg_policy_rel: Relation;
    let policy_id: Oid;
    let target_table: Relation;
    let table_id: Oid;
    let mut role_oids: *mut Datum = null_mut();
    let mut nitems: c_int = 0;
    let mut role_ids: *mut ArrayType = null_mut();
    let mut qual_parse_rtable: *mut List = NIL as *mut List;
    let mut with_check_parse_rtable: *mut List = NIL as *mut List;
    let mut qual: *mut Node = null_mut();
    let mut with_check_qual: *mut Node = null_mut();
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let sscan: SysScanDesc;
    let policy_tuple: HeapTuple;
    let new_tuple: HeapTuple;
    let mut values: [Datum; Natts_pg_policy] = std::mem::zeroed();
    let mut isnull: [bool; Natts_pg_policy] = std::mem::zeroed();
    let mut replaces: [bool; Natts_pg_policy] = std::mem::zeroed();
    let mut target: ObjectAddress = std::mem::zeroed();
    let mut myself: ObjectAddress = std::mem::zeroed();
    let polcmd_datum: Datum;
    let polcmd: c_char;
    let mut polcmd_isnull: bool = false;
    let mut i: c_int;

    /* Parse role_ids */
    if (*stmt).roles != null_mut() {
        role_oids = policy_role_list_to_array((*stmt).roles, &mut nitems);
        role_ids = construct_array_builtin(role_oids, nitems, OIDOID);
    }

    /* Get id of table.  Also handles permissions checks. */
    table_id = RangeVarGetRelidExtended(
        (*stmt).table,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForPolicy),
        stmt as *mut c_void,
    );

    target_table = relation_open(table_id, NoLock);

    /* Parse the using policy clause */
    if !(*stmt).qual.is_null() {
        let nsitem: *mut ParseNamespaceItem;
        let qual_pstate: *mut ParseState = make_parsestate(null_mut());

        nsitem = addRangeTableEntryForRelation(
            qual_pstate,
            target_table,
            AccessShareLock,
            null_mut(),
            false,
            false,
        );

        addNSItemToQuery(qual_pstate, nsitem, false, true, true);

        qual = transformWhereClause(
            qual_pstate,
            (*stmt).qual,
            EXPR_KIND_POLICY,
            c"POLICY".as_ptr(),
        );

        /* Fix up collation information */
        assign_expr_collations(qual_pstate, qual);

        qual_parse_rtable = (*qual_pstate).p_rtable;
        free_parsestate(qual_pstate);
    }

    /* Parse the with-check policy clause */
    if !(*stmt).with_check.is_null() {
        let nsitem: *mut ParseNamespaceItem;
        let with_check_pstate: *mut ParseState = make_parsestate(null_mut());

        nsitem = addRangeTableEntryForRelation(
            with_check_pstate,
            target_table,
            AccessShareLock,
            null_mut(),
            false,
            false,
        );

        addNSItemToQuery(with_check_pstate, nsitem, false, true, true);

        with_check_qual = transformWhereClause(
            with_check_pstate,
            (*stmt).with_check,
            EXPR_KIND_POLICY,
            c"POLICY".as_ptr(),
        );

        /* Fix up collation information */
        assign_expr_collations(with_check_pstate, with_check_qual);

        with_check_parse_rtable = (*with_check_pstate).p_rtable;
        free_parsestate(with_check_pstate);
    }

    /* zero-clear */
    for k in 0..Natts_pg_policy {
        values[k] = 0;
        replaces[k] = false;
        isnull[k] = false;
    }

    /* Find policy to update. */
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    /* Set key - policy's relation id. */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(table_id),
    );

    /* Set key - policy's name. */
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_policy_polname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum((*stmt).policy_name),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    policy_tuple = systable_getnext(sscan);

    /* Check that the policy is found, raise an error if not. */
    if !HeapTupleIsValid(policy_tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "policy \"{}\" for table \"{}\" does not exist",
                cstr_display((*stmt).policy_name),
                cstr_display(RelationGetRelationName(target_table))
            )
        );
    }

    /* Get policy command */
    polcmd_datum = heap_getattr(
        policy_tuple,
        Anum_pg_policy_polcmd as c_int,
        RelationGetDescr(pg_policy_rel),
        &mut polcmd_isnull,
    );
    Assert!(!polcmd_isnull);
    polcmd = DatumGetChar(polcmd_datum);

    /*
     * If the command is SELECT or DELETE then WITH CHECK should be NULL.
     */
    if (polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && (*stmt).with_check != null_mut() {
        ereport!(
            ERROR,
            errmsg!("only USING expression allowed for SELECT, DELETE")
        );
    }

    /*
     * If the command is INSERT then WITH CHECK should be the only expression
     * provided.
     */
    if polcmd == ACL_INSERT_CHR && (*stmt).qual != null_mut() {
        ereport!(
            ERROR,
            errmsg!("only WITH CHECK expression allowed for INSERT")
        );
    }

    policy_id = (*(GETSTRUCT(policy_tuple) as Form_pg_policy)).oid;

    if role_ids != null_mut() {
        replaces[(Anum_pg_policy_polroles - 1) as usize] = true;
        values[(Anum_pg_policy_polroles - 1) as usize] =
            PointerGetDatum(role_ids as *const c_void);
    } else {
        let roles: *mut Oid;
        let roles_datum: Datum;
        let mut attr_isnull: bool = false;
        let policy_roles: *mut ArrayType;

        /*
         * We need to pull the set of roles this policy applies to from what's
         * in the catalog, so that we can recreate the dependencies correctly
         * for the policy.
         */

        roles_datum = heap_getattr(
            policy_tuple,
            Anum_pg_policy_polroles as c_int,
            RelationGetDescr(pg_policy_rel),
            &mut attr_isnull,
        );
        Assert!(!attr_isnull);

        policy_roles = DatumGetArrayTypePCopy(roles_datum);

        roles = ARR_DATA_PTR(policy_roles) as *mut Oid;

        nitems = *ARR_DIMS(policy_roles).offset(0);

        role_oids = palloc((nitems as usize) * std::mem::size_of::<Datum>()) as *mut Datum;

        i = 0;
        while i < nitems {
            *role_oids.offset(i as isize) = ObjectIdGetDatum(*roles.offset(i as isize));
            i += 1;
        }
    }

    if qual != null_mut() {
        replaces[(Anum_pg_policy_polqual - 1) as usize] = true;
        values[(Anum_pg_policy_polqual - 1) as usize] =
            CStringGetTextDatum(nodeToString(qual as *const c_void));
    } else {
        let value_datum: Datum;
        let mut attr_isnull: bool = false;

        /*
         * We need to pull the USING expression and build the range table for
         * the policy from what's in the catalog, so that we can recreate the
         * dependencies correctly for the policy.
         */

        /* Check if the policy has a USING expr */
        value_datum = heap_getattr(
            policy_tuple,
            Anum_pg_policy_polqual as c_int,
            RelationGetDescr(pg_policy_rel),
            &mut attr_isnull,
        );
        if !attr_isnull {
            let qual_value: *mut c_char;
            let qual_pstate: *mut ParseState;

            /* parsestate is built just to build the range table */
            qual_pstate = make_parsestate(null_mut());

            qual_value = TextDatumGetCString(value_datum);
            qual = stringToNode(qual_value) as *mut Node;

            /* Add this rel to the parsestate's rangetable, for dependencies */
            let _ = addRangeTableEntryForRelation(
                qual_pstate,
                target_table,
                AccessShareLock,
                null_mut(),
                false,
                false,
            );

            qual_parse_rtable = (*qual_pstate).p_rtable;
            free_parsestate(qual_pstate);
        }
    }

    if with_check_qual != null_mut() {
        replaces[(Anum_pg_policy_polwithcheck - 1) as usize] = true;
        values[(Anum_pg_policy_polwithcheck - 1) as usize] =
            CStringGetTextDatum(nodeToString(with_check_qual as *const c_void));
    } else {
        let value_datum: Datum;
        let mut attr_isnull: bool = false;

        /*
         * We need to pull the WITH CHECK expression and build the range table
         * for the policy from what's in the catalog, so that we can recreate
         * the dependencies correctly for the policy.
         */

        /* Check if the policy has a WITH CHECK expr */
        value_datum = heap_getattr(
            policy_tuple,
            Anum_pg_policy_polwithcheck as c_int,
            RelationGetDescr(pg_policy_rel),
            &mut attr_isnull,
        );
        if !attr_isnull {
            let with_check_value: *mut c_char;
            let with_check_pstate: *mut ParseState;

            /* parsestate is built just to build the range table */
            with_check_pstate = make_parsestate(null_mut());

            with_check_value = TextDatumGetCString(value_datum);
            with_check_qual = stringToNode(with_check_value) as *mut Node;

            /* Add this rel to the parsestate's rangetable, for dependencies */
            let _ = addRangeTableEntryForRelation(
                with_check_pstate,
                target_table,
                AccessShareLock,
                null_mut(),
                false,
                false,
            );

            with_check_parse_rtable = (*with_check_pstate).p_rtable;
            free_parsestate(with_check_pstate);
        }
    }

    new_tuple = heap_modify_tuple(
        policy_tuple,
        RelationGetDescr(pg_policy_rel),
        values.as_ptr(),
        isnull.as_ptr(),
        replaces.as_ptr(),
    );
    CatalogTupleUpdate(pg_policy_rel, &mut (*new_tuple).t_self, new_tuple);

    /* Update Dependencies. */
    deleteDependencyRecordsFor(PolicyRelationId, policy_id, false);

    /* Record Dependencies */
    target.classId = RelationRelationId;
    target.objectId = table_id;
    target.objectSubId = 0;

    myself.classId = PolicyRelationId;
    myself.objectId = policy_id;
    myself.objectSubId = 0;

    recordDependencyOn(&myself, &target, DEPENDENCY_AUTO);

    recordDependencyOnExpr(&myself, qual, qual_parse_rtable, DEPENDENCY_NORMAL);

    recordDependencyOnExpr(
        &myself,
        with_check_qual,
        with_check_parse_rtable,
        DEPENDENCY_NORMAL,
    );

    /* Register role dependencies */
    deleteSharedDependencyRecordsFor(PolicyRelationId, policy_id, 0);
    target.classId = AuthIdRelationId;
    target.objectSubId = 0;
    i = 0;
    while i < nitems {
        target.objectId = DatumGetObjectId(*role_oids.offset(i as isize));
        /* no dependency if public */
        if target.objectId != ACL_ID_PUBLIC {
            recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY);
        }
        i += 1;
    }

    InvokeObjectPostAlterHook(PolicyRelationId, policy_id, 0);

    heap_freetuple(new_tuple);

    /* Invalidate Relation Cache */
    CacheInvalidateRelcache(target_table);

    /* Clean up. */
    systable_endscan(sscan);
    relation_close(target_table, NoLock);
    table_close(pg_policy_rel, RowExclusiveLock);

    myself
}

/*
 * rename_policy -
 *	 change the name of a policy on a relation
 */
pub unsafe fn rename_policy(stmt: *mut RenameStmt) -> ObjectAddress {
    let pg_policy_rel: Relation;
    let target_table: Relation;
    let table_id: Oid;
    let opoloid: Oid;
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let mut sscan: SysScanDesc;
    let mut policy_tuple: HeapTuple;
    let mut address: ObjectAddress = std::mem::zeroed();

    /* Get id of table.  Also handles permissions checks. */
    table_id = RangeVarGetRelidExtended(
        (*stmt).relation,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForPolicy),
        stmt as *mut c_void,
    );

    target_table = relation_open(table_id, NoLock);

    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    /* First pass -- check for conflict */

    /* Add key - policy's relation id. */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(table_id),
    );

    /* Add key - policy's name. */
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_policy_polname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum((*stmt).newname),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    if HeapTupleIsValid(systable_getnext(sscan)) {
        ereport!(
            ERROR,
            errmsg!(
                "policy \"{}\" for table \"{}\" already exists",
                cstr_display((*stmt).newname),
                cstr_display(RelationGetRelationName(target_table))
            )
        );
    }

    systable_endscan(sscan);

    /* Second pass -- find existing policy and update */
    /* Add key - policy's relation id. */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(table_id),
    );

    /* Add key - policy's name. */
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_policy_polname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum((*stmt).subname),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    policy_tuple = systable_getnext(sscan);

    /* Complain if we did not find the policy */
    if !HeapTupleIsValid(policy_tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "policy \"{}\" for table \"{}\" does not exist",
                cstr_display((*stmt).subname),
                cstr_display(RelationGetRelationName(target_table))
            )
        );
    }

    opoloid = (*(GETSTRUCT(policy_tuple) as Form_pg_policy)).oid;

    policy_tuple = heap_copytuple(policy_tuple);

    namestrcpy(
        &mut (*(GETSTRUCT(policy_tuple) as Form_pg_policy)).polname as *mut _,
        (*stmt).newname,
    );

    CatalogTupleUpdate(pg_policy_rel, &mut (*policy_tuple).t_self, policy_tuple);

    InvokeObjectPostAlterHook(PolicyRelationId, opoloid, 0);

    ObjectAddressSet(&mut address, PolicyRelationId, opoloid);

    /*
     * Invalidate relation's relcache entry so that other backends (and this
     * one too!) are sent SI message to make them rebuild relcache entries.
     * (Ideally this should happen automatically...)
     */
    CacheInvalidateRelcache(target_table);

    /* Clean up. */
    systable_endscan(sscan);
    table_close(pg_policy_rel, RowExclusiveLock);
    relation_close(target_table, NoLock);

    address
}

/*
 * get_relation_policy_oid - Look up a policy by name to find its OID
 *
 * If missing_ok is false, throw an error if policy not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_relation_policy_oid(
    relid: Oid,
    policy_name: *const c_char,
    missing_ok: bool,
) -> Oid {
    let pg_policy_rel: Relation;
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let sscan: SysScanDesc;
    let policy_tuple: HeapTuple;
    let policy_oid: Oid;

    pg_policy_rel = table_open(PolicyRelationId, AccessShareLock);

    /* Add key - policy's relation id. */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );

    /* Add key - policy's name. */
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_policy_polname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(policy_name),
    );

    sscan = systable_beginscan(
        pg_policy_rel,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    policy_tuple = systable_getnext(sscan);

    if !HeapTupleIsValid(policy_tuple) {
        if !missing_ok {
            ereport!(
                ERROR,
                errmsg!(
                    "policy \"{}\" for table \"{}\" does not exist",
                    cstr_display(policy_name),
                    cstr_display(get_rel_name(relid))
                )
            );
        }

        policy_oid = InvalidOid;
    } else {
        policy_oid = (*(GETSTRUCT(policy_tuple) as Form_pg_policy)).oid;
    }

    /* Clean up. */
    systable_endscan(sscan);
    table_close(pg_policy_rel, AccessShareLock);

    policy_oid
}

/*
 * relation_has_policies - Determine if relation has any policies
 */
pub unsafe fn relation_has_policies(rel: Relation) -> bool {
    let catalog: Relation;
    let mut skey: ScanKeyData = std::mem::zeroed();
    let sscan: SysScanDesc;
    let policy_tuple: HeapTuple;
    let mut ret: bool = false;

    catalog = table_open(PolicyRelationId, AccessShareLock);
    ScanKeyInit(
        &mut skey,
        Anum_pg_policy_polrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    sscan = systable_beginscan(
        catalog,
        PolicyPolrelidPolnameIndexId,
        true,
        null_mut(),
        1,
        &mut skey,
    );
    policy_tuple = systable_getnext(sscan);
    if HeapTupleIsValid(policy_tuple) {
        ret = true;
    }

    systable_endscan(sscan);
    table_close(catalog, AccessShareLock);

    ret
}

// ---------------------------------------------------------------------------
// Small local helpers (no C analogue; support the translation above).
// ---------------------------------------------------------------------------

/* strcmp for C strings (string.h); used to match command names. */
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i: usize = 0;
    loop {
        let ca = *a.add(i);
        let cb = *b.add(i);
        if ca != cb {
            return (ca as c_int) - (cb as c_int);
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/* Render a C string for {} in errmsg!; printf %s -> Rust display. */
unsafe fn cstr_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() {
        std::borrow::Cow::Borrowed("(null)")
    } else {
        std::ffi::CStr::from_ptr(s).to_string_lossy()
    }
}

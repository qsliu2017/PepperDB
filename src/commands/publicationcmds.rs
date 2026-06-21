/*-------------------------------------------------------------------------
 *
 * publicationcmds.c
 *		publication manipulation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/commands/publicationcmds.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void, CStr};

use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::htup_details::HeapTupleData;
use crate::access::table::table::{table_close, table_open, table_openrv};
use crate::catalog::catalog_oids::{
    PublicationRelationId, PublicationRelRelationId, PublicationNamespaceRelationId,
};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_publication::{
    Form_pg_publication, FormData_pg_publication,
    PUBLISH_GENCOLS_NONE, PUBLISH_GENCOLS_STORED,
};
use crate::catalog::pg_publication_namespace::Form_pg_publication_namespace;
use crate::catalog::pg_publication_rel::Form_pg_publication_rel;
use crate::executor::execReplication::PublicationActions;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::pg_list::{List, ListCell};
use crate::nodes::parsenodes::{
    AlterPublicationStmt, AlterPublicationAction, AlterPublicationAction::*,
    CreatePublicationStmt, DefElem, PublicationObjSpec,
    PublicationObjSpecType::*, PublicationTable,
};
use crate::nodes::nodes::NodeTag;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;
use crate::{foreach, current_cell, IsA};

/* HeapTuple = *mut HeapTupleData */
type HeapTuple = *mut HeapTupleData;
type Relation = *mut RelationData;

/* Node = *mut c_void (opaque) */
use crate::nodes::nodes::Node;

/* ----------------------------------------------------------------
 * Local constants (Anum_ values matching pg_publication catalog layout)
 * ---------------------------------------------------------------- */

/* pg_publication attribute numbers */
const Anum_pg_publication_oid: AttrNumber      = 1;
const Anum_pg_publication_pubname: AttrNumber  = 2;
const Anum_pg_publication_pubowner: AttrNumber = 3;
const Anum_pg_publication_puballtables: AttrNumber = 4;
const Anum_pg_publication_pubinsert: AttrNumber    = 5;
const Anum_pg_publication_pubupdate: AttrNumber    = 6;
const Anum_pg_publication_pubdelete: AttrNumber    = 7;
const Anum_pg_publication_pubtruncate: AttrNumber  = 8;
const Anum_pg_publication_pubviaroot: AttrNumber   = 9;
const Anum_pg_publication_pubgencols: AttrNumber   = 10;
const Natts_pg_publication: usize = 10;

/* pg_publication_rel attribute numbers */
const Anum_pg_publication_rel_oid: AttrNumber    = 1;
const Anum_pg_publication_rel_prpubid: AttrNumber = 2;
const Anum_pg_publication_rel_prrelid: AttrNumber = 3;
const Anum_pg_publication_rel_prqual: AttrNumber  = 4;
const Anum_pg_publication_rel_prattrs: AttrNumber = 5;

/* pg_publication_namespace attribute numbers */
const Anum_pg_publication_namespace_oid: AttrNumber     = 1;
const Anum_pg_publication_namespace_pnpubid: AttrNumber = 2;
const Anum_pg_publication_namespace_pnnspid: AttrNumber = 3;

/* syscache IDs  TODO(pg-port): real values from syscache.h */
const PUBLICATIONNAME: c_int    = 48;
const PUBLICATIONOID: c_int     = 51;
const PUBLICATIONREL: c_int     = 52;
const PUBLICATIONNAMESPACE: c_int = 49;
const PUBLICATIONRELMAP: c_int  = 53;
const PUBLICATIONNAMESPACEMAP: c_int = 50;

/* catalog index OID for pg_publication */
const PublicationObjectIndexId: Oid = 6110;

/* MAX_RELCACHE_INVAL_MSGS  TODO(pg-port) */
const MAX_RELCACHE_INVAL_MSGS: c_int = 20;

/* Locking modes  TODO(pg-port) */
const RowExclusiveLock: c_int    = 5;
const ShareUpdateExclusiveLock: c_int = 4;
const AccessShareLock: c_int     = 1;
const AccessExclusiveLock: c_int = 8;
const NoLock: c_int              = 0;

/* ACL  TODO(pg-port) */
#[repr(C)]
pub struct AclResult_Opaque(c_int);
const ACLCHECK_OK: c_int = 0;
const ACLCHECK_NOT_OWNER: c_int = 1;
const ACL_CREATE: c_int  = 4;

/* ObjectType  TODO(pg-port) */
const OBJECT_DATABASE: c_int    = 10;
const OBJECT_PUBLICATION: c_int = 40;

/* WAL level  TODO(pg-port) */
const WAL_LEVEL_LOGICAL: c_int  = 2;

/* DropBehavior  TODO(pg-port) */
const DROP_CASCADE: c_int = 1;

/* RELKIND  TODO(pg-port) */
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

/* Publication part  TODO(pg-port) */
const PUBLICATION_PART_ROOT: c_int = 0;
const PUBLICATION_PART_ALL: c_int  = 1;

/* REPLICA IDENTITY */
const REPLICA_IDENTITY_FULL: c_char = b'f' as c_char;

/* ATTRIBUTE_GENERATED  TODO(pg-port) */
const ATTRIBUTE_GENERATED_STORED: c_char  = b's' as c_char;
const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;

/* INDEX_ATTR_BITMAP_IDENTITY_KEY  TODO(pg-port) */
const INDEX_ATTR_BITMAP_IDENTITY_KEY: c_int = 2;

/* FirstLowInvalidHeapAttributeNumber  TODO(pg-port) */
const FirstLowInvalidHeapAttributeNumber: AttrNumber = -8;

/* FirstNormalObjectId  TODO(pg-port) */
const FirstNormalObjectId: Oid = 16384;

/* PROVOLATILE_IMMUTABLE  TODO(pg-port) */
const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;

/* EXPR_KIND_WHERE  TODO(pg-port) */
const EXPR_KIND_WHERE: c_int = 10;

/* InvalidAttrNumber already from attnum crate, redeclare to match C usage */
use crate::access::attnum::InvalidAttrNumber as InvalidAttrNumber_local;

/* Datum  TODO(pg-port) */
type Datum = usize;

/* Form_pg_attribute  TODO(pg-port) */
#[repr(C)]
pub struct FormData_pg_attribute {
    pub attgenerated: c_char,
    _rest: [u8; 0],
}
type Form_pg_attribute = *mut FormData_pg_attribute;

/* TupleDesc  TODO(pg-port) */
type TupleDesc = *mut c_void;

/* ParseNamespaceItem  TODO(pg-port) */
#[repr(C)]
pub struct ParseNamespaceItem { _opaque: [u8; 0] }

/* PublicationRelInfo: opened relation + its where clause + column list */
#[repr(C)]
pub struct PublicationRelInfo {
    pub relation: Relation,
    pub whereClause: *mut Node,
    pub columns: *mut List,
}

/*
 * Information used to validate the columns in the row filter expression. See
 * contain_invalid_rfcolumn_walker for details.
 */
struct rf_context {
    bms_replident: *mut Bitmapset, /* bitset of replica identity columns */
    pubviaroot: bool,              /* true if we are validating the parent
                                    * relation's row filter */
    relid: Oid,                    /* relid of the relation */
    parentid: Oid,                 /* relid of the parent relation */
}

/* ----------------------------------------------------------------
 * Stub declarations for unported dependencies  TODO(pg-port)
 * ---------------------------------------------------------------- */

extern "C" {
    /* access/table */
    fn table_openrv_stub(rv: *mut crate::nodes::primnodes::RangeVar, lock: c_int) -> Relation;

    /* catalog/namespace */
    fn get_namespace_oid(nspname: *const c_char, missing_ok: bool) -> Oid;
    fn fetch_search_path(includeImplicit: bool) -> *mut List;

    /* catalog/objectaddress */
    fn ObjectAddressSet(addr: *mut ObjectAddress, classId: Oid, objectId: Oid);
    fn InvalidObjectAddress() -> ObjectAddress;
    fn performDeletion(object: *const ObjectAddress, behavior: c_int, flags: c_int);

    /* catalog/objectaccess */
    fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int);
    fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid);
    fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwner: Oid);

    /* commands/event_trigger */
    fn EventTriggerCollectSimpleCommand(
        address: ObjectAddress,
        secondaryObject: ObjectAddress,
        parsetree: *mut Node,
    );

    /* commands/defrem */
    fn defGetString(def: *mut DefElem) -> *mut c_char;
    fn defGetBoolean(def: *mut DefElem) -> bool;
    fn errorConflictingDefElem(def: *mut DefElem, pstate: *mut ParseState);

    /* catalog/catalog */
    fn GetNewOidWithIndex(rel: Relation, indexId: Oid, oidcolumn: AttrNumber) -> Oid;

    /* catalog/indexing */
    fn CatalogTupleInsert(rel: Relation, tup: HeapTuple) -> Oid;
    fn CatalogTupleUpdate(rel: Relation, otid: *mut c_void, tup: HeapTuple);
    fn CatalogTupleDelete(rel: Relation, tid: *mut c_void);

    /* catalog/objectaddress */
    fn get_relkind_objtype(relkind: c_char) -> c_int;

    /* utils/syscache */
    fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple;
    fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn GetSysCacheOid1(cacheId: c_int, oidattnum: AttrNumber, key1: Datum) -> Oid;
    fn GetSysCacheOid2(cacheId: c_int, oidattnum: AttrNumber, key1: Datum, key2: Datum) -> Oid;
    fn ReleaseSysCache(tup: HeapTuple);
    fn HeapTupleIsValid(tup: HeapTuple) -> bool;
    fn SearchSysCacheExists1(cacheId: c_int, key1: Datum) -> bool;
    fn SysCacheGetAttr(
        cacheId: c_int,
        tup: HeapTuple,
        attnum: AttrNumber,
        isnull: *mut bool,
    ) -> Datum;

    /* access/htup_details */
    fn GETSTRUCT(tup: HeapTuple) -> *mut c_void;
    fn heap_form_tuple(tupdesc: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple;
    fn heap_modify_tuple(
        tuple: HeapTuple,
        tupdesc: TupleDesc,
        replValues: *mut Datum,
        replIsnull: *mut bool,
        doReplace: *mut bool,
    ) -> HeapTuple;
    fn heap_freetuple(tup: HeapTuple);
    fn heap_attisnull(tup: HeapTuple, attnum: AttrNumber, tupdesc: TupleDesc) -> bool;

    /* utils/rel */
    fn RelationGetRelid(rel: Relation) -> Oid;
    fn RelationGetRelationName(rel: Relation) -> *const c_char;
    fn RelationGetNamespace(rel: Relation) -> Oid;
    fn RelationGetDescr(rel: Relation) -> TupleDesc;
    fn RelationGetIndexAttrBitmap(rel: Relation, attrKind: c_int) -> *mut Bitmapset;

    /* nodes/bitmapset */
    fn bms_is_member(x: c_int, a: *const Bitmapset) -> bool;
    fn bms_next_member(a: *const Bitmapset, prevbit: c_int) -> c_int;
    fn bms_free(a: *mut Bitmapset);
    fn bms_equal(a: *const Bitmapset, b: *const Bitmapset) -> bool;

    /* utils/acl */
    fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: c_int) -> c_int;
    fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool;
    fn aclcheck_error(aclerr: c_int, objtype: c_int, objectname: *const c_char);
    fn check_can_set_role(member: Oid, role: Oid);
    fn superuser() -> bool;
    fn superuser_arg(roleid: Oid) -> bool;

    /* miscadmin */
    fn GetUserId() -> Oid;
    fn MyDatabaseId() -> Oid;

    /* utils/builtins */
    fn SplitIdentifierString(rawstring: *mut c_char, separator: c_char, namelist: *mut *mut List) -> bool;
    fn pstrdup(s: *const c_char) -> *mut c_char;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn TextDatumGetCString(datum: Datum) -> *mut c_char;
    fn stringToNode(str: *const c_char) -> *mut Node;

    /* utils/inval */
    fn CacheInvalidateRelcacheAll();
    fn CacheInvalidateRelcacheByRelid(relid: Oid);
    fn CacheInvalidateRelSync(relid: Oid);
    fn CacheInvalidateRelSyncAll();
    fn CommandCounterIncrement();

    /* utils/lsyscache */
    fn get_attname(relid: Oid, attnum: AttrNumber, missing_ok: bool) -> *mut c_char;
    fn get_attnum(relid: Oid, attname: *const c_char) -> AttrNumber;
    fn get_rel_relkind(relid: Oid) -> c_char;
    fn get_rel_name(relid: Oid) -> *mut c_char;
    fn get_namespace_name(nsoid: Oid) -> *mut c_char;
    fn get_database_name(dboid: Oid) -> *mut c_char;

    /* catalog/pg_database */
    fn DatabaseRelationId() -> Oid;

    /* catalog/pg_proc */
    fn func_volatile(funcid: Oid) -> c_char;

    /* nodes/nodeFuncs */
    fn expression_tree_walker(
        node: *mut Node,
        walker: unsafe extern "C" fn(*mut Node, *mut c_void) -> bool,
        context: *mut c_void,
    ) -> bool;
    fn check_functions_in_node(
        node: *mut Node,
        checker: unsafe extern "C" fn(Oid, *mut c_void) -> bool,
        context: *mut c_void,
    ) -> bool;
    fn nodeTag(node: *const Node) -> NodeTag;
    fn exprType(node: *const Node) -> Oid;
    fn exprCollation(node: *const Node) -> Oid;
    fn exprInputCollation(node: *const Node) -> Oid;
    fn exprLocation(node: *const Node) -> c_int;
    fn equal(a: *const c_void, b: *const c_void) -> bool;
    fn copyObject(from: *const c_void) -> *mut c_void;

    /* utils/datum */
    fn ObjectIdGetDatum(oid: Oid) -> Datum;
    fn BoolGetDatum(b: bool) -> Datum;
    fn CharGetDatum(ch: c_char) -> Datum;
    fn CStringGetDatum(s: *const c_char) -> Datum;

    /* catalog/namespace */
    fn OidIsValid(oid: Oid) -> bool;

    /* parser/parse_clause */
    fn transformWhereClause(
        pstate: *mut ParseState,
        clause: *mut Node,
        exprKind: c_int,
        constructName: *const c_char,
    ) -> *mut Node;
    fn addNSItemToQuery(
        pstate: *mut ParseState,
        nsitem: *mut ParseNamespaceItem,
        addToJoinList: bool,
        addToRelNameSpace: bool,
        addToVarNameSpace: bool,
    );

    /* parser/parse_collate */
    fn assign_expr_collations(pstate: *mut ParseState, expr: *mut Node);

    /* parser/parse_relation */
    fn addRangeTableEntryForRelation(
        pstate: *mut ParseState,
        rel: Relation,
        lockmode: c_int,
        alias: *mut c_void,
        inh: bool,
        inFromCl: bool,
    ) -> *mut ParseNamespaceItem;
    fn make_parsestate(parentParseState: *mut ParseState) -> *mut ParseState;
    fn free_parsestate(pstate: *mut ParseState);
    fn parser_errposition(pstate: *mut ParseState, location: c_int) -> c_int;

    /* rewrite/rewriteHandler */
    fn expand_generated_columns_in_expr(
        node: *mut Node,
        rel: Relation,
        rtindex: c_int,
    ) -> *mut Node;

    /* storage/lmgr */
    fn LockDatabaseObject(classid: Oid, objid: Oid, objsubid: c_int, lockmode: c_int);

    /* utils/mmgr */
    fn palloc(size: usize) -> *mut c_void;
    fn pfree(ptr: *mut c_void);

    /* catalog/pg_publication backend API  TODO(pg-port) */
    fn publication_add_relation(
        pubid: Oid,
        pri: *mut PublicationRelInfo,
        if_not_exists: bool,
    ) -> ObjectAddress;
    fn publication_add_schema(
        pubid: Oid,
        schemaid: Oid,
        if_not_exists: bool,
    ) -> ObjectAddress;
    fn GetPublication(pubid: Oid) -> *mut c_void; /* Publication* */
    fn check_and_fetch_column_list(
        pub_: *mut c_void,
        relid: Oid,
        mcxt: *mut c_void,
        columns: *mut *mut Bitmapset,
    ) -> bool;
    fn pub_collist_to_bitmapset(
        columns: *mut Bitmapset,
        datum: Datum,
        mcxt: *mut c_void,
    ) -> *mut Bitmapset;
    fn pub_collist_validate(rel: Relation, columns: *mut List) -> *mut Bitmapset;
    fn GetPublicationRelations(pubid: Oid, which: c_int) -> *mut List;
    fn GetAllSchemaPublicationRelations(pubid: Oid, which: c_int) -> *mut List;
    fn GetSchemaPublicationRelations(schemaid: Oid, which: c_int) -> *mut List;
    fn GetPubPartitionOptionRelations(
        result: *mut List,
        which: c_int,
        relid: Oid,
    ) -> *mut List;
    fn GetPublicationSchemas(pubid: Oid) -> *mut List;
    fn is_schema_publication(pubid: Oid) -> bool;
    fn GetTopMostAncestorInPublication(
        pubid: Oid,
        ancestors: *mut List,
        out_ancestor_level: *mut c_int,
    ) -> Oid;
    fn InvalidatePublicationRels_internal(relids: *mut List);
    fn InvalidatePubRelSyncCache_internal(pubid: Oid, puballtables: bool);

    /* pg_list helpers */
    fn list_append_unique_oid(list: *mut List, datum: Oid) -> *mut List;
    fn list_concat_unique_oid(list1: *mut List, list2: *mut List) -> *mut List;
    fn list_difference_oid(list1: *mut List, list2: *mut List) -> *mut List;
    fn list_member_oid(list: *const List, datum: Oid) -> bool;
    fn list_free(list: *mut List);
    fn list_free_deep(list: *mut List);
    fn lappend(list: *mut List, datum: *mut c_void) -> *mut List;
    fn lappend_oid(list: *mut List, datum: Oid) -> *mut List;
    fn linitial_oid(list: *const List) -> Oid;
    fn lfirst_oid(lc: *const ListCell) -> Oid;
    fn lfirst(lc: *const ListCell) -> *mut c_void;
    fn list_length(list: *const List) -> c_int;
    fn find_all_inheritors(
        parentrelId: Oid,
        lockmode: c_int,
        numparents: *mut c_int,
    ) -> *mut List;
    fn CHECK_FOR_INTERRUPTS();

    /* errcode helpers */
    fn errcode(sqlerrcode: c_int) -> c_int;
    fn errmsg(fmt: *const c_char, ...) -> c_int;
    fn errdetail(fmt: *const c_char, ...) -> c_int;
    fn errdetail_internal(fmt: *const c_char, ...) -> c_int;
    fn errhint(fmt: *const c_char, ...) -> c_int;
    fn ereport_internal(elevel: c_int, ...) -> !;

    /* namein for catalog */
    fn namein(s: Datum) -> Datum;
    fn DirectFunctionCall1(func: unsafe extern "C" fn(Datum) -> Datum, arg1: Datum) -> Datum;
    fn NameStr(name: crate::c::NameData) -> *const c_char;

    /* wal_level GUC  TODO(pg-port) */
    fn wal_level_val() -> c_int;
}

/* Convenience: NIL list pointer */
const NIL: *mut List = core::ptr::null_mut();

/* errcode constants  TODO(pg-port) */
const ERRCODE_SYNTAX_ERROR: c_int               = 0x4_2601;
const ERRCODE_DUPLICATE_OBJECT: c_int           = 0x4_2710;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int     = 0x2_8000;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0x5_5000;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int    = 0x2_2023;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int      = 0x0_A000;
const ERRCODE_UNDEFINED_OBJECT: c_int           = 0x4_2704;
const ERRCODE_UNDEFINED_SCHEMA: c_int           = 0x3_F000;

const ERROR: c_int   = 21;
const WARNING: c_int = 19;

/* NodeTag short-names used in switch  */
use crate::nodes::nodes::NodeTag::*;

/*
 * parse_publication_options
 */
unsafe fn parse_publication_options(
    pstate: *mut ParseState,
    options: *mut List,
    publish_given: *mut bool,
    pubactions: *mut PublicationActions,
    publish_via_partition_root_given: *mut bool,
    publish_via_partition_root: *mut bool,
    publish_generated_columns_given: *mut bool,
    publish_generated_columns: *mut c_char,
) {
    *publish_given = false;
    *publish_via_partition_root_given = false;
    *publish_generated_columns_given = false;

    /* defaults */
    (*pubactions).pubinsert = true;
    (*pubactions).pubupdate = true;
    (*pubactions).pubdelete = true;
    (*pubactions).pubtruncate = true;
    *publish_via_partition_root = false;
    *publish_generated_columns = PUBLISH_GENCOLS_NONE;

    /* Parse options */
    foreach!(lc, options, {
        let defel = lfirst(current_cell!(lc)) as *mut DefElem;

        if libc_strcmp((*defel).defname, b"publish\0".as_ptr() as _) == 0 {
            let publish: *mut c_char;
            let mut publish_list: *mut List = NIL;

            if *publish_given {
                errorConflictingDefElem(defel, pstate);
            }

            /*
             * If publish option was given only the explicitly listed actions
             * should be published.
             */
            (*pubactions).pubinsert = false;
            (*pubactions).pubupdate = false;
            (*pubactions).pubdelete = false;
            (*pubactions).pubtruncate = false;

            *publish_given = true;

            /*
             * SplitIdentifierString destructively modifies its input, so make
             * a copy so we don't modify the memory of the executing statement
             */
            publish = pstrdup(defGetString(defel));

            if !SplitIdentifierString(publish, b',' as c_char, &mut publish_list) {
                ereport!(ERROR,
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                    errmsg!("invalid list syntax in parameter \"{}\"", "publish")
                );
            }

            /* Process the option list. */
            foreach!(lc2, publish_list, {
                let publish_opt = lfirst(current_cell!(lc2)) as *const c_char;

                if libc_strcmp(publish_opt, b"insert\0".as_ptr() as _) == 0 {
                    (*pubactions).pubinsert = true;
                } else if libc_strcmp(publish_opt, b"update\0".as_ptr() as _) == 0 {
                    (*pubactions).pubupdate = true;
                } else if libc_strcmp(publish_opt, b"delete\0".as_ptr() as _) == 0 {
                    (*pubactions).pubdelete = true;
                } else if libc_strcmp(publish_opt, b"truncate\0".as_ptr() as _) == 0 {
                    (*pubactions).pubtruncate = true;
                } else {
                    ereport!(ERROR,
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                        errmsg!("unrecognized value for publication option \"{}\": \"{}\"",
                               "publish", CStr::from_ptr(publish_opt).to_string_lossy())
                    );
                }
            });
        } else if libc_strcmp((*defel).defname, b"publish_via_partition_root\0".as_ptr() as _) == 0 {
            if *publish_via_partition_root_given {
                errorConflictingDefElem(defel, pstate);
            }
            *publish_via_partition_root_given = true;
            *publish_via_partition_root = defGetBoolean(defel);
        } else if libc_strcmp((*defel).defname, b"publish_generated_columns\0".as_ptr() as _) == 0 {
            if *publish_generated_columns_given {
                errorConflictingDefElem(defel, pstate);
            }
            *publish_generated_columns_given = true;
            *publish_generated_columns = defGetGeneratedColsOption(defel);
        } else {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                errmsg!("unrecognized publication parameter: \"{}\"", CStr::from_ptr((*defel).defname).to_string_lossy())
            );
        }
    });
}

/*
 * Convert the PublicationObjSpecType list into schema oid list and
 * PublicationTable list.
 */
unsafe fn ObjectsInPublicationToOids(
    pubobjspec_list: *mut List,
    pstate: *mut ParseState,
    rels: *mut *mut List,
    schemas: *mut *mut List,
) {
    if pubobjspec_list.is_null() {
        return;
    }

    foreach!(cell, pubobjspec_list, {
        let pubobj = lfirst(current_cell!(cell)) as *mut PublicationObjSpec;
        let schemaid: Oid;
        let search_path: *mut List;

        match (*pubobj).pubobjtype {
            PUBLICATIONOBJ_TABLE => {
                *rels = lappend(*rels, (*pubobj).pubtable as *mut c_void);
            }
            PUBLICATIONOBJ_TABLES_IN_SCHEMA => {
                let schemaid = get_namespace_oid((*pubobj).name, false);

                /* Filter out duplicates if user specifies "sch1, sch1" */
                *schemas = list_append_unique_oid(*schemas, schemaid);
            }
            PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA => {
                let search_path = fetch_search_path(false);
                if search_path.is_null() {
                    /* nothing valid in search_path? */
                    ereport!(ERROR,
                        /* C also: errcode(ERRCODE_UNDEFINED_SCHEMA) */
                        errmsg!("no schema has been selected for CURRENT_SCHEMA")
                    );
                }

                let schemaid = linitial_oid(search_path);
                list_free(search_path);

                /* Filter out duplicates if user specifies "sch1, sch1" */
                *schemas = list_append_unique_oid(*schemas, schemaid);
            }
            _ => {
                /* shouldn't happen */
                elog!(ERROR, "invalid publication object type {}", (*pubobj).pubobjtype as c_int);
            }
        }
    });
}

/*
 * Returns true if any of the columns used in the row filter WHERE expression is
 * not part of REPLICA IDENTITY, false otherwise.
 */
unsafe extern "C" fn contain_invalid_rfcolumn_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    let context = context as *mut rf_context;

    if IsA!(node, T_Var) {
        let var = node as *mut crate::nodes::primnodes::Var;
        let mut attnum: AttrNumber = (*var).varattno;

        /*
         * If pubviaroot is true, we are validating the row filter of the
         * parent table, but the bitmap contains the replica identity
         * information of the child table. So, get the column number of the
         * child table as parent and child column order could be different.
         */
        if (*context).pubviaroot {
            let colname = get_attname((*context).parentid, attnum, false);
            attnum = get_attnum((*context).relid, colname);
        }

        if !bms_is_member(
            (attnum - FirstLowInvalidHeapAttributeNumber) as c_int,
            (*context).bms_replident,
        ) {
            return true;
        }
    }

    return expression_tree_walker(
        node,
        contain_invalid_rfcolumn_walker,
        context as *mut c_void,
    );
}

/*
 * Check if all columns referenced in the filter expression are part of the
 * REPLICA IDENTITY index or not.
 *
 * Returns true if any invalid column is found.
 */
pub unsafe fn pub_rf_contains_invalid_column(
    pubid: Oid,
    relation: Relation,
    ancestors: *mut List,
    pubviaroot: bool,
) -> bool {
    let rftuple: HeapTuple;
    let relid = RelationGetRelid(relation);
    let mut publish_as_relid = RelationGetRelid(relation);
    let mut result = false;
    let rfdatum: Datum;
    let mut rfisnull: bool = false;

    /*
     * FULL means all columns are in the REPLICA IDENTITY, so all columns are
     * allowed in the row filter and we can skip the validation.
     */
    if (*(*relation).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
        return false;
    }

    /*
     * For a partition, if pubviaroot is true, find the topmost ancestor that
     * is published via this publication as we need to use its row filter
     * expression to filter the partition's changes.
     *
     * Note that even though the row filter used is for an ancestor, the
     * REPLICA IDENTITY used will be for the actual child table.
     */
    if pubviaroot && (*(*relation).rd_rel).relispartition {
        publish_as_relid =
            GetTopMostAncestorInPublication(pubid, ancestors, core::ptr::null_mut());

        if !OidIsValid(publish_as_relid) {
            publish_as_relid = relid;
        }
    }

    let rftuple = SearchSysCache2(
        PUBLICATIONRELMAP,
        ObjectIdGetDatum(publish_as_relid),
        ObjectIdGetDatum(pubid),
    );

    if !HeapTupleIsValid(rftuple) {
        return false;
    }

    let rfdatum = SysCacheGetAttr(
        PUBLICATIONRELMAP,
        rftuple,
        Anum_pg_publication_rel_prqual,
        &mut rfisnull,
    );

    if !rfisnull {
        let mut context = rf_context {
            bms_replident: core::ptr::null_mut(),
            pubviaroot,
            parentid: publish_as_relid,
            relid,
        };
        let rfnode: *mut Node;
        let bms: *mut Bitmapset;

        /* Remember columns that are part of the REPLICA IDENTITY */
        let bms = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);

        context.bms_replident = bms;
        let rfnode = stringToNode(TextDatumGetCString(rfdatum));
        result = contain_invalid_rfcolumn_walker(
            rfnode,
            &mut context as *mut rf_context as *mut c_void,
        );
    }

    ReleaseSysCache(rftuple);

    return result;
}

/*
 * Check for invalid columns in the publication table definition.
 *
 * This function evaluates two conditions:
 *
 * 1. Ensures that all columns referenced in the REPLICA IDENTITY are covered
 *    by the column list. If any column is missing, *invalid_column_list is set
 *    to true.
 * 2. Ensures that all the generated columns referenced in the REPLICA IDENTITY
 *    are published, either by being explicitly named in the column list or, if
 *    no column list is specified, by setting the option
 *    publish_generated_columns to stored. If any unpublished
 *    generated column is found, *invalid_gen_col is set to true.
 *
 * Returns true if any of the above conditions are not met.
 */
pub unsafe fn pub_contains_invalid_column(
    pubid: Oid,
    relation: Relation,
    ancestors: *mut List,
    pubviaroot: bool,
    pubgencols_type: c_char,
    invalid_column_list: *mut bool,
    invalid_gen_col: *mut bool,
) -> bool {
    let relid = RelationGetRelid(relation);
    let mut publish_as_relid = RelationGetRelid(relation);
    let idattrs: *mut Bitmapset;
    let mut columns: *mut Bitmapset = core::ptr::null_mut();
    let desc: TupleDesc = RelationGetDescr(relation);
    let pub_: *mut c_void;
    let mut x: c_int;

    *invalid_column_list = false;
    *invalid_gen_col = false;

    /*
     * For a partition, if pubviaroot is true, find the topmost ancestor that
     * is published via this publication as we need to use its column list for
     * the changes.
     *
     * Note that even though the column list used is for an ancestor, the
     * REPLICA IDENTITY used will be for the actual child table.
     */
    if pubviaroot && (*(*relation).rd_rel).relispartition {
        publish_as_relid =
            GetTopMostAncestorInPublication(pubid, ancestors, core::ptr::null_mut());

        if !OidIsValid(publish_as_relid) {
            publish_as_relid = relid;
        }
    }

    /* Fetch the column list */
    let pub_ = GetPublication(pubid);
    check_and_fetch_column_list(pub_, publish_as_relid, core::ptr::null_mut(), &mut columns);

    if (*(*relation).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
        /* With REPLICA IDENTITY FULL, no column list is allowed. */
        *invalid_column_list = !columns.is_null();

        /*
         * As we don't allow a column list with REPLICA IDENTITY FULL, the
         * publish_generated_columns option must be set to stored if the table
         * has any stored generated columns.
         */
        if pubgencols_type != PUBLISH_GENCOLS_STORED
            && !(*(*relation).rd_att).constr.is_null()
            && (*(*(*relation).rd_att).constr).has_generated_stored
        {
            *invalid_gen_col = true;
        }

        /*
         * Virtual generated columns are currently not supported for logical
         * replication at all.
         */
        if !(*(*relation).rd_att).constr.is_null()
            && (*(*(*relation).rd_att).constr).has_generated_virtual
        {
            *invalid_gen_col = true;
        }

        if *invalid_gen_col && *invalid_column_list {
            return true;
        }
    }

    /* Remember columns that are part of the REPLICA IDENTITY */
    let idattrs =
        RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    /*
     * Attnums in the bitmap returned by RelationGetIndexAttrBitmap are offset
     * (to handle system columns the usual way), while column list does not
     * use offset, so we can't do bms_is_subset(). Instead, we have to loop
     * over the idattrs and check all of them are in the list.
     */
    x = -1;
    while {
        x = bms_next_member(idattrs, x);
        x >= 0
    } {
        let mut attnum: AttrNumber = (x + FirstLowInvalidHeapAttributeNumber as c_int) as AttrNumber;
        let att = TupleDescAttr(desc, (attnum - 1) as usize);

        if columns.is_null() {
            /*
             * The publish_generated_columns option must be set to stored if
             * the REPLICA IDENTITY contains any stored generated column.
             */
            if (*att).attgenerated == ATTRIBUTE_GENERATED_STORED
                && pubgencols_type != PUBLISH_GENCOLS_STORED
            {
                *invalid_gen_col = true;
                break;
            }

            /*
             * The equivalent setting for virtual generated columns does not
             * exist yet.
             */
            if (*att).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                *invalid_gen_col = true;
                break;
            }

            /* Skip validating the column list since it is not defined */
            continue;
        }

        /*
         * If pubviaroot is true, we are validating the column list of the
         * parent table, but the bitmap contains the replica identity
         * information of the child table. The parent/child attnums may not
         * match, so translate them to the parent - get the attname from the
         * child, and look it up in the parent.
         */
        if pubviaroot {
            /* attribute name in the child table */
            let colname = get_attname(relid, attnum, false);

            /*
             * Determine the attnum for the attribute name in parent (we are
             * using the column list defined on the parent).
             */
            attnum = get_attnum(publish_as_relid, colname);
        }

        /* replica identity column, not covered by the column list */
        *invalid_column_list |= !bms_is_member(attnum as c_int, columns);

        if *invalid_column_list && *invalid_gen_col {
            break;
        }
    }

    bms_free(columns);
    bms_free(idattrs);

    return *invalid_column_list || *invalid_gen_col;
}

/*
 * Invalidate entries in the RelationSyncCache for relations included in the
 * specified publication, either via FOR TABLE or FOR TABLES IN SCHEMA.
 *
 * If 'puballtables' is true, invalidate all cache entries.
 */
pub unsafe fn InvalidatePubRelSyncCache(pubid: Oid, puballtables: bool) {
    if puballtables {
        CacheInvalidateRelSyncAll();
    } else {
        let mut relids: *mut List = NIL;
        let mut schemarelids: *mut List = NIL;

        /*
         * For partitioned tables, we must invalidate all partitions and
         * itself. WAL records for INSERT/UPDATE/DELETE specify leaf tables as
         * a target. However, WAL records for TRUNCATE specify both a root and
         * its leaves.
         */
        relids = GetPublicationRelations(pubid, PUBLICATION_PART_ALL);
        schemarelids = GetAllSchemaPublicationRelations(pubid, PUBLICATION_PART_ALL);

        relids = list_concat_unique_oid(relids, schemarelids);

        /* Invalidate the relsyncache */
        foreach!(lc, relids, {
            let relid = lfirst_oid(current_cell!(lc));
            CacheInvalidateRelSync(relid);
        });
    }

    return;
}

/* check_functions_in_node callback */
unsafe extern "C" fn contain_mutable_or_user_functions_checker(
    func_id: Oid,
    context: *mut c_void,
) -> bool {
    return func_volatile(func_id) != PROVOLATILE_IMMUTABLE || func_id >= FirstNormalObjectId;
}

/*
 * The row filter walker checks if the row filter expression is a "simple
 * expression".
 *
 * It allows only simple or compound expressions such as:
 * - (Var Op Const)
 * - (Var Op Var)
 * - (Var Op Const) AND/OR (Var Op Const)
 * - etc
 * (where Var is a column of the table this filter belongs to)
 *
 * The simple expression has the following restrictions:
 * - User-defined operators are not allowed;
 * - User-defined functions are not allowed;
 * - User-defined types are not allowed;
 * - User-defined collations are not allowed;
 * - Non-immutable built-in functions are not allowed;
 * - System columns are not allowed.
 *
 * NOTES
 *
 * We don't allow user-defined functions/operators/types/collations because
 * (a) if a user drops a user-defined object used in a row filter expression or
 * if there is any other error while using it, the logical decoding
 * infrastructure won't be able to recover from such an error even if the
 * object is recreated again because a historic snapshot is used to evaluate
 * the row filter;
 * (b) a user-defined function can be used to access tables that could have
 * unpleasant results because a historic snapshot is used. That's why only
 * immutable built-in functions are allowed in row filter expressions.
 *
 * We don't allow system columns because currently, we don't have that
 * information in the tuple passed to downstream. Also, as we don't replicate
 * those to subscribers, there doesn't seem to be a need for a filter on those
 * columns.
 *
 * We can allow other node types after more analysis and testing.
 */
unsafe extern "C" fn check_simple_rowfilter_expr_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    let pstate = context as *mut ParseState;
    let errdetail_msg: *const c_char;

    if node.is_null() {
        return false;
    }

    let mut errdetail_msg: *const c_char = core::ptr::null();

    match nodeTag(node) {
        T_Var => {
            /* System columns are not allowed. */
            let var = node as *mut crate::nodes::primnodes::Var;
            if (*var).varattno < InvalidAttrNumber_local {
                errdetail_msg = b"System columns are not allowed.\0".as_ptr() as _;
            }
        }
        T_OpExpr | T_DistinctExpr | T_NullIfExpr => {
            /* OK, except user-defined operators are not allowed. */
            let opexpr = node as *mut crate::nodes::primnodes::OpExpr;
            if (*opexpr).opno >= FirstNormalObjectId {
                errdetail_msg = b"User-defined operators are not allowed.\0".as_ptr() as _;
            }
        }
        T_ScalarArrayOpExpr => {
            /* OK, except user-defined operators are not allowed. */
            let saop = node as *mut ScalarArrayOpExpr_stub;
            if (*saop).opno >= FirstNormalObjectId {
                errdetail_msg = b"User-defined operators are not allowed.\0".as_ptr() as _;
            }
            /*
             * We don't need to check the hashfuncid and negfuncid of
             * ScalarArrayOpExpr as those functions are only built for a
             * subquery.
             */
        }
        T_RowCompareExpr => {
            /* OK, except user-defined operators are not allowed. */
            let rce = node as *mut RowCompareExpr_stub;
            let opnos = (*rce).opnos;
            if !opnos.is_null() {
                foreach!(opid, opnos, {
                    if lfirst_oid(current_cell!(opid)) >= FirstNormalObjectId {
                        errdetail_msg = b"User-defined operators are not allowed.\0".as_ptr() as _;
                        break;
                    }
                });
            }
        }
        T_Const
        | T_FuncExpr
        | T_BoolExpr
        | T_RelabelType
        | T_CollateExpr
        | T_CaseExpr
        | T_CaseTestExpr
        | T_ArrayExpr
        | T_RowExpr
        | T_CoalesceExpr
        | T_MinMaxExpr
        | T_XmlExpr
        | T_NullTest
        | T_BooleanTest
        | T_List => {
            /* OK, supported */
        }
        _ => {
            errdetail_msg = b"Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.\0".as_ptr() as _;
        }
    }

    /*
     * For all the supported nodes, if we haven't already found a problem,
     * check the types, functions, and collations used in it.  We check List
     * by walking through each element.
     */
    if errdetail_msg.is_null() && !IsA!(node, T_List) {
        if exprType(node) >= FirstNormalObjectId {
            errdetail_msg = b"User-defined types are not allowed.\0".as_ptr() as _;
        } else if check_functions_in_node(
            node,
            contain_mutable_or_user_functions_checker,
            pstate as *mut c_void,
        ) {
            errdetail_msg =
                b"User-defined or built-in mutable functions are not allowed.\0".as_ptr() as _;
        } else if exprCollation(node) >= FirstNormalObjectId
            || exprInputCollation(node) >= FirstNormalObjectId
        {
            errdetail_msg = b"User-defined collations are not allowed.\0".as_ptr() as _;
        }
    }

    /*
     * If we found a problem in this node, throw error now. Otherwise keep
     * going.
     */
    if !errdetail_msg.is_null() {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             * errdetail_internal("%s", errdetail_msg),
             * parser_errposition(pstate, exprLocation(node)) */
            errmsg!("invalid publication WHERE expression")
        );
    }

    return expression_tree_walker(
        node,
        check_simple_rowfilter_expr_walker,
        context,
    );
}

/*
 * Check if the row filter expression is a "simple expression".
 *
 * See check_simple_rowfilter_expr_walker for details.
 */
unsafe fn check_simple_rowfilter_expr(node: *mut Node, pstate: *mut ParseState) -> bool {
    return check_simple_rowfilter_expr_walker(node, pstate as *mut c_void);
}

/*
 * Transform the publication WHERE expression for all the relations in the list,
 * ensuring it is coerced to boolean and necessary collation information is
 * added if required, and add a new nsitem/RTE for the associated relation to
 * the ParseState's namespace list.
 *
 * Also check the publication row filter expression and throw an error if
 * anything not permitted or unexpected is encountered.
 */
unsafe fn TransformPubWhereClauses(
    tables: *mut List,
    queryString: *const c_char,
    pubviaroot: bool,
) {
    foreach!(lc, tables, {
        let pri = lfirst(current_cell!(lc)) as *mut PublicationRelInfo;

        if (*pri).whereClause.is_null() {
            continue;
        }

        /*
         * If the publication doesn't publish changes via the root partitioned
         * table, the partition's row filter will be used. So disallow using
         * WHERE clause on partitioned table in this case.
         */
        if !pubviaroot
            && (*(*(*pri).relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
        {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 * errdetail("WHERE clause cannot be used for a partitioned table when %s is false.", "publish_via_partition_root") */
                errmsg!("cannot use publication WHERE clause for relation \"{}\"",
                       CStr::from_ptr(RelationGetRelationName((*pri).relation)).to_string_lossy())
            );
        }

        /*
         * A fresh pstate is required so that we only have "this" table in its
         * rangetable
         */
        let pstate = make_parsestate(core::ptr::null_mut());
        (*pstate).p_sourcetext = queryString;
        let nsitem = addRangeTableEntryForRelation(
            pstate,
            (*pri).relation,
            AccessShareLock,
            core::ptr::null_mut(),
            false,
            false,
        );
        addNSItemToQuery(pstate, nsitem, false, true, true);

        let mut whereclause = transformWhereClause(
            pstate,
            copyObject((*pri).whereClause as *const c_void) as *mut Node,
            EXPR_KIND_WHERE,
            b"PUBLICATION WHERE\0".as_ptr() as _,
        );

        /* Fix up collation information */
        assign_expr_collations(pstate, whereclause);

        whereclause = expand_generated_columns_in_expr(whereclause, (*pri).relation, 1);

        /*
         * We allow only simple expressions in row filters. See
         * check_simple_rowfilter_expr_walker.
         */
        check_simple_rowfilter_expr(whereclause, pstate);

        free_parsestate(pstate);

        (*pri).whereClause = whereclause;
    });
}


/*
 * Given a list of tables that are going to be added to a publication,
 * verify that they fulfill the necessary preconditions, namely: no tables
 * have a column list if any schema is published; and partitioned tables do
 * not have column lists if publish_via_partition_root is not set.
 *
 * 'publish_schema' indicates that the publication contains any TABLES IN
 * SCHEMA elements (newly added in this command, or preexisting).
 * 'pubviaroot' is the value of publish_via_partition_root.
 */
unsafe fn CheckPubRelationColumnList(
    pubname: *mut c_char,
    tables: *mut List,
    publish_schema: bool,
    pubviaroot: bool,
) {
    foreach!(lc, tables, {
        let pri = lfirst(current_cell!(lc)) as *mut PublicationRelInfo;

        if (*pri).columns.is_null() {
            continue;
        }

        /*
         * Disallow specifying column list if any schema is in the
         * publication.
         *
         * XXX We could instead just forbid the case when the publication
         * tries to publish the table with a column list and a schema for that
         * table. However, if we do that then we need a restriction during
         * ALTER TABLE ... SET SCHEMA to prevent such a case which doesn't
         * seem to be a good idea.
         */
        if publish_schema {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 * errdetail("Column lists cannot be specified in publications containing FOR TABLES IN SCHEMA elements.") */
                errmsg!("cannot use column list for relation \"{}.{}\" in publication \"{}\"",
                       CStr::from_ptr(get_namespace_name(RelationGetNamespace((*pri).relation))).to_string_lossy(),
                       CStr::from_ptr(RelationGetRelationName((*pri).relation)).to_string_lossy(),
                       CStr::from_ptr(pubname).to_string_lossy())
            );
        }

        /*
         * If the publication doesn't publish changes via the root partitioned
         * table, the partition's column list will be used. So disallow using
         * a column list on the partitioned table in this case.
         */
        if !pubviaroot
            && (*(*(*pri).relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
        {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 * errdetail("Column lists cannot be specified for partitioned tables when %s is false.", "publish_via_partition_root") */
                errmsg!("cannot use column list for relation \"{}.{}\" in publication \"{}\"",
                       CStr::from_ptr(get_namespace_name(RelationGetNamespace((*pri).relation))).to_string_lossy(),
                       CStr::from_ptr(RelationGetRelationName((*pri).relation)).to_string_lossy(),
                       CStr::from_ptr(pubname).to_string_lossy())
            );
        }
    });
}

/*
 * Create new publication.
 */
pub unsafe fn CreatePublication(
    pstate: *mut ParseState,
    stmt: *mut CreatePublicationStmt,
) -> ObjectAddress {
    let rel: Relation;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let puboid: Oid;
    let mut nulls = [false; Natts_pg_publication];
    let mut values: [Datum; Natts_pg_publication] = [0; Natts_pg_publication];
    let tup: HeapTuple;
    let mut publish_given: bool = false;
    let mut pubactions: PublicationActions = core::mem::zeroed();
    let mut publish_via_partition_root_given: bool = false;
    let mut publish_via_partition_root: bool = false;
    let mut publish_generated_columns_given: bool = false;
    let mut publish_generated_columns: c_char = 0;
    let aclresult: c_int;
    let mut relations: *mut List = NIL;
    let mut schemaidlist: *mut List = NIL;

    /* must have CREATE privilege on database */
    let aclresult = object_aclcheck(
        DatabaseRelationId(),
        MyDatabaseId(),
        GetUserId(),
        ACL_CREATE,
    );
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            OBJECT_DATABASE,
            get_database_name(MyDatabaseId()),
        );
    }

    /* FOR ALL TABLES requires superuser */
    if (*stmt).for_all_tables && !superuser() {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
            errmsg!("must be superuser to create FOR ALL TABLES publication")
        );
    }

    let rel = table_open(PublicationRelationId, RowExclusiveLock);

    /* Check if name is used */
    let puboid = GetSysCacheOid1(
        PUBLICATIONNAME,
        Anum_pg_publication_oid,
        CStringGetDatum((*stmt).pubname),
    );
    if OidIsValid(puboid) {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            errmsg!("publication \"{}\" already exists", CStr::from_ptr((*stmt).pubname).to_string_lossy())
        );
    }

    /* Form a tuple. */
    core::ptr::write_bytes(values.as_mut_ptr(), 0, Natts_pg_publication);
    core::ptr::write_bytes(nulls.as_mut_ptr(), 0, Natts_pg_publication);

    values[(Anum_pg_publication_pubname - 1) as usize] =
        DirectFunctionCall1(namein, CStringGetDatum((*stmt).pubname));
    values[(Anum_pg_publication_pubowner - 1) as usize] = ObjectIdGetDatum(GetUserId());

    parse_publication_options(
        pstate,
        (*stmt).options,
        &mut publish_given,
        &mut pubactions,
        &mut publish_via_partition_root_given,
        &mut publish_via_partition_root,
        &mut publish_generated_columns_given,
        &mut publish_generated_columns,
    );

    let puboid = GetNewOidWithIndex(rel, PublicationObjectIndexId, Anum_pg_publication_oid);
    values[(Anum_pg_publication_oid - 1) as usize] = ObjectIdGetDatum(puboid);
    values[(Anum_pg_publication_puballtables - 1) as usize] = BoolGetDatum((*stmt).for_all_tables);
    values[(Anum_pg_publication_pubinsert - 1) as usize] = BoolGetDatum(pubactions.pubinsert);
    values[(Anum_pg_publication_pubupdate - 1) as usize] = BoolGetDatum(pubactions.pubupdate);
    values[(Anum_pg_publication_pubdelete - 1) as usize] = BoolGetDatum(pubactions.pubdelete);
    values[(Anum_pg_publication_pubtruncate - 1) as usize] = BoolGetDatum(pubactions.pubtruncate);
    values[(Anum_pg_publication_pubviaroot - 1) as usize] = BoolGetDatum(publish_via_partition_root);
    values[(Anum_pg_publication_pubgencols - 1) as usize] = CharGetDatum(publish_generated_columns);

    let tup = heap_form_tuple(
        RelationGetDescr(rel),
        values.as_mut_ptr(),
        nulls.as_mut_ptr() as *mut bool,
    );

    /* Insert tuple into catalog. */
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    recordDependencyOnOwner(PublicationRelationId, puboid, GetUserId());

    ObjectAddressSet(&mut myself, PublicationRelationId, puboid);

    /* Make the changes visible. */
    CommandCounterIncrement();

    /* Associate objects with the publication. */
    if (*stmt).for_all_tables {
        /* Invalidate relcache so that publication info is rebuilt. */
        CacheInvalidateRelcacheAll();
    } else {
        ObjectsInPublicationToOids(
            (*stmt).pubobjects,
            pstate,
            &mut relations,
            &mut schemaidlist,
        );

        /* FOR TABLES IN SCHEMA requires superuser */
        if !schemaidlist.is_null() && !superuser() {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
                errmsg!("must be superuser to create FOR TABLES IN SCHEMA publication")
            );
        }

        if !relations.is_null() {
            let rels = OpenTableList(relations);
            TransformPubWhereClauses(rels, (*pstate).p_sourcetext, publish_via_partition_root);

            CheckPubRelationColumnList(
                (*stmt).pubname,
                rels,
                !schemaidlist.is_null(),
                publish_via_partition_root,
            );

            PublicationAddTables(puboid, rels, true, core::ptr::null_mut());
            CloseTableList(rels);
        }

        if !schemaidlist.is_null() {
            /*
             * Schema lock is held until the publication is created to prevent
             * concurrent schema deletion.
             */
            LockSchemaList(schemaidlist);
            PublicationAddSchemas(puboid, schemaidlist, true, core::ptr::null_mut());
        }
    }

    table_close(rel, RowExclusiveLock);

    InvokeObjectPostCreateHook(PublicationRelationId, puboid, 0);

    if wal_level_val() != WAL_LEVEL_LOGICAL {
        ereport!(WARNING,
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             * errhint("Set \"wal_level\" to \"logical\" before creating subscriptions.") */
            errmsg!("\"wal_level\" is insufficient to publish logical changes")
        );
    }

    return myself;
}

/*
 * Change options of a publication.
 */
unsafe fn AlterPublicationOptions(
    pstate: *mut ParseState,
    stmt: *mut AlterPublicationStmt,
    rel: Relation,
    mut tup: HeapTuple,
) {
    let mut nulls = [false; Natts_pg_publication];
    let mut replaces = [false; Natts_pg_publication];
    let mut values: [Datum; Natts_pg_publication] = [0; Natts_pg_publication];
    let mut publish_given: bool = false;
    let mut pubactions: PublicationActions = core::mem::zeroed();
    let mut publish_via_partition_root_given: bool = false;
    let mut publish_via_partition_root: bool = false;
    let mut publish_generated_columns_given: bool = false;
    let mut publish_generated_columns: c_char = 0;
    let mut obj: ObjectAddress = core::mem::zeroed();
    let pubform: Form_pg_publication;
    let mut root_relids: *mut List = NIL;

    parse_publication_options(
        pstate,
        (*stmt).options,
        &mut publish_given,
        &mut pubactions,
        &mut publish_via_partition_root_given,
        &mut publish_via_partition_root,
        &mut publish_generated_columns_given,
        &mut publish_generated_columns,
    );

    let pubform = GETSTRUCT(tup) as Form_pg_publication;

    /*
     * If the publication doesn't publish changes via the root partitioned
     * table, the partition's row filter and column list will be used. So
     * disallow using WHERE clause and column lists on partitioned table in
     * this case.
     */
    if !(*pubform).puballtables
        && publish_via_partition_root_given
        && !publish_via_partition_root
    {
        /*
         * Lock the publication so nobody else can do anything with it. This
         * prevents concurrent alter to add partitioned table(s) with WHERE
         * clause(s) and/or column lists which we don't allow when not
         * publishing via root.
         */
        LockDatabaseObject(PublicationRelationId, (*pubform).oid, 0, AccessShareLock);

        root_relids = GetPublicationRelations((*pubform).oid, PUBLICATION_PART_ROOT);

        foreach!(lc, root_relids, {
            let relid = lfirst_oid(current_cell!(lc));
            let rftuple: HeapTuple;
            let relkind: c_char;
            let relname: *mut c_char;
            let has_rowfilter: bool;
            let has_collist: bool;

            /*
             * Beware: we don't have lock on the relations, so cope silently
             * with the cache lookups returning NULL.
             */

            let rftuple = SearchSysCache2(
                PUBLICATIONRELMAP,
                ObjectIdGetDatum(relid),
                ObjectIdGetDatum((*pubform).oid),
            );
            if !HeapTupleIsValid(rftuple) {
                continue;
            }
            let has_rowfilter =
                !heap_attisnull(rftuple, Anum_pg_publication_rel_prqual, core::ptr::null_mut());
            let has_collist =
                !heap_attisnull(rftuple, Anum_pg_publication_rel_prattrs, core::ptr::null_mut());
            if !has_rowfilter && !has_collist {
                ReleaseSysCache(rftuple);
                continue;
            }

            let relkind = get_rel_relkind(relid);
            if relkind != RELKIND_PARTITIONED_TABLE {
                ReleaseSysCache(rftuple);
                continue;
            }
            let relname = get_rel_name(relid);
            if relname.is_null() {
                /* table concurrently dropped */
                ReleaseSysCache(rftuple);
                continue;
            }

            if has_rowfilter {
                ereport!(ERROR,
                    /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     * errdetail("The publication contains a WHERE clause for partitioned table \"%s\", which is not allowed when \"%s\" is false.", relname, "publish_via_partition_root") */
                    errmsg!("cannot set parameter \"{}\" to false for publication \"{}\"",
                           "publish_via_partition_root", CStr::from_ptr((*stmt).pubname).to_string_lossy())
                );
            }
            /* Assert(has_collist) */
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 * errdetail("The publication contains a column list for partitioned table \"%s\", which is not allowed when \"%s\" is false.", relname, "publish_via_partition_root") */
                errmsg!("cannot set parameter \"{}\" to false for publication \"{}\"",
                       "publish_via_partition_root", CStr::from_ptr((*stmt).pubname).to_string_lossy())
            );
        });
    }

    /* Everything ok, form a new tuple. */
    core::ptr::write_bytes(values.as_mut_ptr(), 0, Natts_pg_publication);
    core::ptr::write_bytes(nulls.as_mut_ptr(), 0, Natts_pg_publication);
    core::ptr::write_bytes(replaces.as_mut_ptr(), 0, Natts_pg_publication);

    if publish_given {
        values[(Anum_pg_publication_pubinsert - 1) as usize] = BoolGetDatum(pubactions.pubinsert);
        replaces[(Anum_pg_publication_pubinsert - 1) as usize] = true;

        values[(Anum_pg_publication_pubupdate - 1) as usize] = BoolGetDatum(pubactions.pubupdate);
        replaces[(Anum_pg_publication_pubupdate - 1) as usize] = true;

        values[(Anum_pg_publication_pubdelete - 1) as usize] = BoolGetDatum(pubactions.pubdelete);
        replaces[(Anum_pg_publication_pubdelete - 1) as usize] = true;

        values[(Anum_pg_publication_pubtruncate - 1) as usize] = BoolGetDatum(pubactions.pubtruncate);
        replaces[(Anum_pg_publication_pubtruncate - 1) as usize] = true;
    }

    if publish_via_partition_root_given {
        values[(Anum_pg_publication_pubviaroot - 1) as usize] = BoolGetDatum(publish_via_partition_root);
        replaces[(Anum_pg_publication_pubviaroot - 1) as usize] = true;
    }

    if publish_generated_columns_given {
        values[(Anum_pg_publication_pubgencols - 1) as usize] = CharGetDatum(publish_generated_columns);
        replaces[(Anum_pg_publication_pubgencols - 1) as usize] = true;
    }

    tup = heap_modify_tuple(
        tup,
        RelationGetDescr(rel),
        values.as_mut_ptr(),
        nulls.as_mut_ptr() as *mut bool,
        replaces.as_mut_ptr() as *mut bool,
    );

    /* Update the catalog. */
    CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut _ as *mut c_void, tup);

    CommandCounterIncrement();

    let pubform = GETSTRUCT(tup) as Form_pg_publication;

    /* Invalidate the relcache. */
    if (*pubform).puballtables {
        CacheInvalidateRelcacheAll();
    } else {
        let mut relids: *mut List = NIL;
        let mut schemarelids: *mut List = NIL;

        /*
         * For any partitioned tables contained in the publication, we must
         * invalidate all partitions contained in the respective partition
         * trees, not just those explicitly mentioned in the publication.
         */
        if root_relids.is_null() {
            relids = GetPublicationRelations((*pubform).oid, PUBLICATION_PART_ALL);
        } else {
            /*
             * We already got tables explicitly mentioned in the publication.
             * Now get all partitions for the partitioned table in the list.
             */
            foreach!(lc, root_relids, {
                relids = GetPubPartitionOptionRelations(
                    relids,
                    PUBLICATION_PART_ALL,
                    lfirst_oid(current_cell!(lc)),
                );
            });
        }

        schemarelids =
            GetAllSchemaPublicationRelations((*pubform).oid, PUBLICATION_PART_ALL);
        relids = list_concat_unique_oid(relids, schemarelids);

        InvalidatePublicationRels(relids);
    }

    ObjectAddressSet(&mut obj, PublicationRelationId, (*pubform).oid);
    EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress(), stmt as *mut Node);

    InvokeObjectPostAlterHook(PublicationRelationId, (*pubform).oid, 0);
}

/*
 * Invalidate the relations.
 */
pub unsafe fn InvalidatePublicationRels(relids: *mut List) {
    /*
     * We don't want to send too many individual messages, at some point it's
     * cheaper to just reset whole relcache.
     */
    if list_length(relids) < MAX_RELCACHE_INVAL_MSGS {
        foreach!(lc, relids, {
            CacheInvalidateRelcacheByRelid(lfirst_oid(current_cell!(lc)));
        });
    } else {
        CacheInvalidateRelcacheAll();
    }
}

/*
 * Add or remove table to/from publication.
 */
unsafe fn AlterPublicationTables(
    stmt: *mut AlterPublicationStmt,
    tup: HeapTuple,
    tables: *mut List,
    queryString: *const c_char,
    publish_schema: bool,
) {
    let mut rels: *mut List = NIL;
    let pubform = GETSTRUCT(tup) as Form_pg_publication;
    let pubid = (*pubform).oid;

    /*
     * Nothing to do if no objects, except in SET: for that it is quite
     * possible that user has not specified any tables in which case we need
     * to remove all the existing tables.
     */
    if tables.is_null() && (*stmt).action != AP_SetObjects {
        return;
    }

    let rels = OpenTableList(tables);

    if (*stmt).action == AP_AddObjects {
        TransformPubWhereClauses(rels, queryString, (*pubform).pubviaroot);

        let publish_schema = publish_schema | is_schema_publication(pubid);

        CheckPubRelationColumnList((*stmt).pubname, rels, publish_schema, (*pubform).pubviaroot);

        PublicationAddTables(pubid, rels, false, stmt);
    } else if (*stmt).action == AP_DropObjects {
        PublicationDropTables(pubid, rels, false);
    } else {
        /* AP_SetObjects */
        let oldrelids = GetPublicationRelations(pubid, PUBLICATION_PART_ROOT);
        let mut delrels: *mut List = NIL;

        TransformPubWhereClauses(rels, queryString, (*pubform).pubviaroot);

        CheckPubRelationColumnList((*stmt).pubname, rels, publish_schema, (*pubform).pubviaroot);

        /*
         * To recreate the relation list for the publication, look for
         * existing relations that do not need to be dropped.
         */
        foreach!(oldlc, oldrelids, {
            let oldrelid = lfirst_oid(current_cell!(oldlc));
            let mut found = false;
            let mut oldrelwhereclause: *mut Node = core::ptr::null_mut();
            let mut oldcolumns: *mut Bitmapset = core::ptr::null_mut();

            /* look up the cache for the old relmap */
            let rftuple = SearchSysCache2(
                PUBLICATIONRELMAP,
                ObjectIdGetDatum(oldrelid),
                ObjectIdGetDatum(pubid),
            );

            /*
             * See if the existing relation currently has a WHERE clause or a
             * column list. We need to compare those too.
             */
            if HeapTupleIsValid(rftuple) {
                let mut isnull = true;

                /* Load the WHERE clause for this table. */
                let whereClauseDatum = SysCacheGetAttr(
                    PUBLICATIONRELMAP,
                    rftuple,
                    Anum_pg_publication_rel_prqual,
                    &mut isnull,
                );
                if !isnull {
                    oldrelwhereclause =
                        stringToNode(TextDatumGetCString(whereClauseDatum));
                }

                /* Transform the int2vector column list to a bitmap. */
                let columnListDatum = SysCacheGetAttr(
                    PUBLICATIONRELMAP,
                    rftuple,
                    Anum_pg_publication_rel_prattrs,
                    &mut isnull,
                );

                if !isnull {
                    oldcolumns =
                        pub_collist_to_bitmapset(core::ptr::null_mut(), columnListDatum, core::ptr::null_mut());
                }

                ReleaseSysCache(rftuple);
            }

            foreach!(newlc, rels, {
                let newpubrel = lfirst(current_cell!(newlc)) as *mut PublicationRelInfo;
                let newrelid = RelationGetRelid((*newpubrel).relation);
                let mut newcolumns: *mut Bitmapset = core::ptr::null_mut();

                /*
                 * Validate the column list.  If the column list or WHERE
                 * clause changes, then the validation done here will be
                 * duplicated inside PublicationAddTables().  The validation
                 * is cheap enough that that seems harmless.
                 */
                newcolumns = pub_collist_validate((*newpubrel).relation, (*newpubrel).columns);

                /*
                 * Check if any of the new set of relations matches with the
                 * existing relations in the publication. Additionally, if the
                 * relation has an associated WHERE clause, check the WHERE
                 * expressions also match. Same for the column list. Drop the
                 * rest.
                 */
                if newrelid == oldrelid {
                    if equal(
                        oldrelwhereclause as *const c_void,
                        (*newpubrel).whereClause as *const c_void,
                    ) && bms_equal(oldcolumns, newcolumns)
                    {
                        found = true;
                        break;
                    }
                }
            });

            /*
             * Add the non-matched relations to a list so that they can be
             * dropped.
             */
            if !found {
                let oldrel = palloc(core::mem::size_of::<PublicationRelInfo>())
                    as *mut PublicationRelInfo;
                (*oldrel).whereClause = core::ptr::null_mut();
                (*oldrel).columns = NIL;
                (*oldrel).relation = table_open(oldrelid, ShareUpdateExclusiveLock);
                delrels = lappend(delrels, oldrel as *mut c_void);
            }
        });

        /* And drop them. */
        PublicationDropTables(pubid, delrels, true);

        /*
         * Don't bother calculating the difference for adding, we'll catch and
         * skip existing ones when doing catalog update.
         */
        PublicationAddTables(pubid, rels, true, stmt);

        CloseTableList(delrels);
    }

    CloseTableList(rels);
}

/*
 * Alter the publication schemas.
 *
 * Add or remove schemas to/from publication.
 */
unsafe fn AlterPublicationSchemas(
    stmt: *mut AlterPublicationStmt,
    tup: HeapTuple,
    schemaidlist: *mut List,
) {
    let pubform = GETSTRUCT(tup) as Form_pg_publication;

    /*
     * Nothing to do if no objects, except in SET: for that it is quite
     * possible that user has not specified any schemas in which case we need
     * to remove all the existing schemas.
     */
    if schemaidlist.is_null() && (*stmt).action != AP_SetObjects {
        return;
    }

    /*
     * Schema lock is held until the publication is altered to prevent
     * concurrent schema deletion.
     */
    LockSchemaList(schemaidlist);
    if (*stmt).action == AP_AddObjects {
        let reloids = GetPublicationRelations((*pubform).oid, PUBLICATION_PART_ROOT);

        foreach!(lc, reloids, {
            let coltuple = SearchSysCache2(
                PUBLICATIONRELMAP,
                ObjectIdGetDatum(lfirst_oid(current_cell!(lc))),
                ObjectIdGetDatum((*pubform).oid),
            );

            if !HeapTupleIsValid(coltuple) {
                continue;
            }

            /*
             * Disallow adding schema if column list is already part of the
             * publication. See CheckPubRelationColumnList.
             */
            if !heap_attisnull(coltuple, Anum_pg_publication_rel_prattrs, core::ptr::null_mut()) {
                ereport!(ERROR,
                    /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     * errdetail("Schemas cannot be added if any tables that specify a column list are already part of the publication.") */
                    errmsg!("cannot add schema to publication \"{}\"", CStr::from_ptr((*stmt).pubname).to_string_lossy())
                );
            }

            ReleaseSysCache(coltuple);
        });

        PublicationAddSchemas((*pubform).oid, schemaidlist, false, stmt);
    } else if (*stmt).action == AP_DropObjects {
        PublicationDropSchemas((*pubform).oid, schemaidlist, false);
    } else {
        /* AP_SetObjects */
        let oldschemaids = GetPublicationSchemas((*pubform).oid);
        let delschemas = list_difference_oid(oldschemaids, schemaidlist);

        /* Identify which schemas should be dropped */

        /*
         * Schema lock is held until the publication is altered to prevent
         * concurrent schema deletion.
         */
        LockSchemaList(delschemas);

        /* And drop them */
        PublicationDropSchemas((*pubform).oid, delschemas, true);

        /*
         * Don't bother calculating the difference for adding, we'll catch and
         * skip existing ones when doing catalog update.
         */
        PublicationAddSchemas((*pubform).oid, schemaidlist, true, stmt);
    }
}

/*
 * Check if relations and schemas can be in a given publication and throw
 * appropriate error if not.
 */
unsafe fn CheckAlterPublication(
    stmt: *mut AlterPublicationStmt,
    tup: HeapTuple,
    tables: *mut List,
    schemaidlist: *mut List,
) {
    let pubform = GETSTRUCT(tup) as Form_pg_publication;

    if ((*stmt).action == AP_AddObjects || (*stmt).action == AP_SetObjects)
        && !schemaidlist.is_null()
        && !superuser()
    {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
            errmsg!("must be superuser to add or set schemas")
        );
    }

    /*
     * Check that user is allowed to manipulate the publication tables in
     * schema
     */
    if !schemaidlist.is_null() && (*pubform).puballtables {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             * errdetail("Schemas cannot be added to or dropped from FOR ALL TABLES publications.") */
            errmsg!("publication \"{}\" is defined as FOR ALL TABLES",
                   CStr::from_ptr(NameStr((*pubform).pubname)).to_string_lossy())
        );
    }

    /* Check that user is allowed to manipulate the publication tables. */
    if !tables.is_null() && (*pubform).puballtables {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             * errdetail("Tables cannot be added to or dropped from FOR ALL TABLES publications.") */
            errmsg!("publication \"{}\" is defined as FOR ALL TABLES",
                   CStr::from_ptr(NameStr((*pubform).pubname)).to_string_lossy())
        );
    }
}

/*
 * Alter the existing publication.
 *
 * This is dispatcher function for AlterPublicationOptions,
 * AlterPublicationSchemas and AlterPublicationTables.
 */
pub unsafe fn AlterPublication(pstate: *mut ParseState, stmt: *mut AlterPublicationStmt) {
    let rel: Relation;
    let mut tup: HeapTuple;
    let pubform: Form_pg_publication;

    let rel = table_open(PublicationRelationId, RowExclusiveLock);

    let mut tup = SearchSysCacheCopy1(PUBLICATIONNAME, CStringGetDatum((*stmt).pubname));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!("publication \"{}\" does not exist", CStr::from_ptr((*stmt).pubname).to_string_lossy())
        );
    }

    let pubform = GETSTRUCT(tup) as Form_pg_publication;

    /* must be owner */
    if !object_ownercheck(PublicationRelationId, (*pubform).oid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_PUBLICATION, (*stmt).pubname);
    }

    if !(*stmt).options.is_null() {
        AlterPublicationOptions(pstate, stmt, rel, tup);
    } else {
        let mut relations: *mut List = NIL;
        let mut schemaidlist: *mut List = NIL;
        let pubid = (*pubform).oid;

        ObjectsInPublicationToOids((*stmt).pubobjects, pstate, &mut relations, &mut schemaidlist);

        CheckAlterPublication(stmt, tup, relations, schemaidlist);

        heap_freetuple(tup);

        /* Lock the publication so nobody else can do anything with it. */
        LockDatabaseObject(PublicationRelationId, pubid, 0, AccessExclusiveLock);

        /*
         * It is possible that by the time we acquire the lock on publication,
         * concurrent DDL has removed it. We can test this by checking the
         * existence of publication. We get the tuple again to avoid the risk
         * of any publication option getting changed.
         */
        tup = SearchSysCacheCopy1(PUBLICATIONOID, ObjectIdGetDatum(pubid));
        if !HeapTupleIsValid(tup) {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
                errmsg!("publication \"{}\" does not exist", CStr::from_ptr((*stmt).pubname).to_string_lossy())
            );
        }

        AlterPublicationTables(
            stmt,
            tup,
            relations,
            (*pstate).p_sourcetext,
            !schemaidlist.is_null(),
        );
        AlterPublicationSchemas(stmt, tup, schemaidlist);
    }

    /* Cleanup. */
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);
}

/*
 * Remove relation from publication by mapping OID.
 */
pub unsafe fn RemovePublicationRelById(proid: Oid) {
    let rel: Relation;
    let tup: HeapTuple;
    let pubrel: Form_pg_publication_rel;
    let mut relids: *mut List = NIL;

    let rel = table_open(PublicationRelRelationId, RowExclusiveLock);

    let tup = SearchSysCache1(PUBLICATIONREL, ObjectIdGetDatum(proid));

    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for publication table {}", proid);
    }

    let pubrel = GETSTRUCT(tup) as Form_pg_publication_rel;

    /*
     * Invalidate relcache so that publication info is rebuilt.
     *
     * For the partitioned tables, we must invalidate all partitions contained
     * in the respective partition hierarchies, not just the one explicitly
     * mentioned in the publication. This is required because we implicitly
     * publish the child tables when the parent table is published.
     */
    let relids = GetPubPartitionOptionRelations(relids, PUBLICATION_PART_ALL, (*pubrel).prrelid);

    InvalidatePublicationRels(relids);

    CatalogTupleDelete(rel, &mut (*tup).t_self as *mut _ as *mut c_void);

    ReleaseSysCache(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Remove the publication by mapping OID.
 */
pub unsafe fn RemovePublicationById(pubid: Oid) {
    let rel: Relation;
    let tup: HeapTuple;
    let pubform: Form_pg_publication;

    let rel = table_open(PublicationRelationId, RowExclusiveLock);

    let tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for publication {}", pubid);
    }

    let pubform = GETSTRUCT(tup) as Form_pg_publication;

    /* Invalidate relcache so that publication info is rebuilt. */
    if (*pubform).puballtables {
        CacheInvalidateRelcacheAll();
    }

    CatalogTupleDelete(rel, &mut (*tup).t_self as *mut _ as *mut c_void);

    ReleaseSysCache(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Remove schema from publication by mapping OID.
 */
pub unsafe fn RemovePublicationSchemaById(psoid: Oid) {
    let rel: Relation;
    let tup: HeapTuple;
    let mut schemaRels: *mut List = NIL;
    let pubsch: Form_pg_publication_namespace;

    let rel = table_open(PublicationNamespaceRelationId, RowExclusiveLock);

    let tup = SearchSysCache1(PUBLICATIONNAMESPACE, ObjectIdGetDatum(psoid));

    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for publication schema {}", psoid);
    }

    let pubsch = GETSTRUCT(tup) as Form_pg_publication_namespace;

    /*
     * Invalidate relcache so that publication info is rebuilt. See
     * RemovePublicationRelById for why we need to consider all the
     * partitions.
     */
    let schemaRels =
        GetSchemaPublicationRelations((*pubsch).pnnspid, PUBLICATION_PART_ALL);
    InvalidatePublicationRels(schemaRels);

    CatalogTupleDelete(rel, &mut (*tup).t_self as *mut _ as *mut c_void);

    ReleaseSysCache(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Open relations specified by a PublicationTable list.
 * The returned tables are locked in ShareUpdateExclusiveLock mode in order to
 * add them to a publication.
 */
unsafe fn OpenTableList(tables: *mut List) -> *mut List {
    let mut relids: *mut List = NIL;
    let mut rels: *mut List = NIL;
    let mut relids_with_rf: *mut List = NIL;
    let mut relids_with_collist: *mut List = NIL;

    /*
     * Open, share-lock, and check all the explicitly-specified relations
     */
    foreach!(lc, tables, {
        let t = lfirst(current_cell!(lc)) as *mut PublicationTable;
        let recurse = (*(*t).relation).inh;
        let rel: Relation;
        let myrelid: Oid;
        let pub_rel: *mut PublicationRelInfo;

        /* Allow query cancel in case this takes a long time */
        CHECK_FOR_INTERRUPTS();

        let rel = table_openrv((*t).relation, ShareUpdateExclusiveLock);
        let myrelid = RelationGetRelid(rel);

        /*
         * Filter out duplicates if user specifies "foo, foo".
         *
         * Note that this algorithm is known to not be very efficient (O(N^2))
         * but given that it only works on list of tables given to us by user
         * it's deemed acceptable.
         */
        if list_member_oid(relids, myrelid) {
            /* Disallow duplicate tables if there are any with row filters. */
            if !(*t).whereClause.is_null()
                || list_member_oid(relids_with_rf, myrelid)
            {
                ereport!(ERROR,
                    /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
                    errmsg!("conflicting or redundant WHERE clauses for table \"{}\"",
                           CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
                );
            }

            /* Disallow duplicate tables if there are any with column lists. */
            if !(*t).columns.is_null()
                || list_member_oid(relids_with_collist, myrelid)
            {
                ereport!(ERROR,
                    /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
                    errmsg!("conflicting or redundant column lists for table \"{}\"",
                           CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
                );
            }

            table_close(rel, ShareUpdateExclusiveLock);
            continue;
        }

        let pub_rel = palloc(core::mem::size_of::<PublicationRelInfo>()) as *mut PublicationRelInfo;
        (*pub_rel).relation = rel;
        (*pub_rel).whereClause = (*t).whereClause;
        (*pub_rel).columns = (*t).columns;
        rels = lappend(rels, pub_rel as *mut c_void);
        relids = lappend_oid(relids, myrelid);

        if !(*t).whereClause.is_null() {
            relids_with_rf = lappend_oid(relids_with_rf, myrelid);
        }

        if !(*t).columns.is_null() {
            relids_with_collist = lappend_oid(relids_with_collist, myrelid);
        }

        /*
         * Add children of this rel, if requested, so that they too are added
         * to the publication.  A partitioned table can't have any inheritance
         * children other than its partitions, which need not be explicitly
         * added to the publication.
         */
        if recurse && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE {
            let children = find_all_inheritors(myrelid, ShareUpdateExclusiveLock, core::ptr::null_mut());

            foreach!(child, children, {
                let childrelid = lfirst_oid(current_cell!(child));

                /* Allow query cancel in case this takes a long time */
                CHECK_FOR_INTERRUPTS();

                /*
                 * Skip duplicates if user specified both parent and child
                 * tables.
                 */
                if list_member_oid(relids, childrelid) {
                    /*
                     * We don't allow to specify row filter for both parent
                     * and child table at the same time as it is not very
                     * clear which one should be given preference.
                     */
                    if childrelid != myrelid
                        && (!(*t).whereClause.is_null()
                            || list_member_oid(relids_with_rf, childrelid))
                    {
                        ereport!(ERROR,
                            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
                            errmsg!("conflicting or redundant WHERE clauses for table \"{}\"",
                                   CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
                        );
                    }

                    /*
                     * We don't allow to specify column list for both parent
                     * and child table at the same time as it is not very
                     * clear which one should be given preference.
                     */
                    if childrelid != myrelid
                        && (!(*t).columns.is_null()
                            || list_member_oid(relids_with_collist, childrelid))
                    {
                        ereport!(ERROR,
                            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
                            errmsg!("conflicting or redundant column lists for table \"{}\"",
                                   CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
                        );
                    }

                    continue;
                }

                /* find_all_inheritors already got lock */
                let child_rel = table_open(childrelid, NoLock);
                let pub_rel = palloc(core::mem::size_of::<PublicationRelInfo>()) as *mut PublicationRelInfo;
                (*pub_rel).relation = child_rel;
                /* child inherits WHERE clause from parent */
                (*pub_rel).whereClause = (*t).whereClause;

                /* child inherits column list from parent */
                (*pub_rel).columns = (*t).columns;
                rels = lappend(rels, pub_rel as *mut c_void);
                relids = lappend_oid(relids, childrelid);

                if !(*t).whereClause.is_null() {
                    relids_with_rf = lappend_oid(relids_with_rf, childrelid);
                }

                if !(*t).columns.is_null() {
                    relids_with_collist = lappend_oid(relids_with_collist, childrelid);
                }
            });
        }
    });

    list_free(relids);
    list_free(relids_with_rf);

    return rels;
}

/*
 * Close all relations in the list.
 */
unsafe fn CloseTableList(rels: *mut List) {
    foreach!(lc, rels, {
        let pub_rel = lfirst(current_cell!(lc)) as *mut PublicationRelInfo;
        table_close((*pub_rel).relation, NoLock);
    });

    list_free_deep(rels);
}

/*
 * Lock the schemas specified in the schema list in AccessShareLock mode in
 * order to prevent concurrent schema deletion.
 */
unsafe fn LockSchemaList(schemalist: *mut List) {
    foreach!(lc, schemalist, {
        let schemaid = lfirst_oid(current_cell!(lc));

        /* Allow query cancel in case this takes a long time */
        CHECK_FOR_INTERRUPTS();
        LockDatabaseObject(NamespaceRelationId, schemaid, 0, AccessShareLock);

        /*
         * It is possible that by the time we acquire the lock on schema,
         * concurrent DDL has removed it. We can test this by checking the
         * existence of schema.
         */
        if !SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(schemaid)) {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_UNDEFINED_SCHEMA) */
                errmsg!("schema with OID {} does not exist", schemaid)
            );
        }
    });
}

/*
 * Add listed tables to the publication.
 */
unsafe fn PublicationAddTables(
    pubid: Oid,
    rels: *mut List,
    if_not_exists: bool,
    stmt: *mut AlterPublicationStmt,
) {
    /* Assert(!stmt || !stmt->for_all_tables) */

    foreach!(lc, rels, {
        let pub_rel = lfirst(current_cell!(lc)) as *mut PublicationRelInfo;
        let rel = (*pub_rel).relation;

        /* Must be owner of the table or superuser. */
        if !object_ownercheck(RelationRelationId, RelationGetRelid(rel), GetUserId()) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                get_relkind_objtype((*(*rel).rd_rel).relkind),
                RelationGetRelationName(rel),
            );
        }

        let obj = publication_add_relation(pubid, pub_rel, if_not_exists);
        if !stmt.is_null() {
            EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress(), stmt as *mut Node);

            InvokeObjectPostCreateHook(PublicationRelRelationId, obj.objectId, 0);
        }
    });
}

/*
 * Remove listed tables from the publication.
 */
unsafe fn PublicationDropTables(pubid: Oid, rels: *mut List, missing_ok: bool) {
    let mut obj: ObjectAddress = core::mem::zeroed();
    let mut prid: Oid;

    foreach!(lc, rels, {
        let pubrel = lfirst(current_cell!(lc)) as *mut PublicationRelInfo;
        let rel = (*pubrel).relation;
        let relid = RelationGetRelid(rel);

        if !(*pubrel).columns.is_null() {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                errmsg!("column list must not be specified in ALTER PUBLICATION ... DROP")
            );
        }

        let prid = GetSysCacheOid2(
            PUBLICATIONRELMAP,
            Anum_pg_publication_rel_oid,
            ObjectIdGetDatum(relid),
            ObjectIdGetDatum(pubid),
        );
        if !OidIsValid(prid) {
            if missing_ok {
                continue;
            }

            ereport!(ERROR,
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
                errmsg!("relation \"{}\" is not part of the publication",
                       CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        }

        if !(*pubrel).whereClause.is_null() {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                errmsg!("cannot use a WHERE clause when removing a table from a publication")
            );
        }

        ObjectAddressSet(&mut obj, PublicationRelRelationId, prid);
        performDeletion(&obj, DROP_CASCADE, 0);
    });
}

/*
 * Add listed schemas to the publication.
 */
unsafe fn PublicationAddSchemas(
    pubid: Oid,
    schemas: *mut List,
    if_not_exists: bool,
    stmt: *mut AlterPublicationStmt,
) {
    /* Assert(!stmt || !stmt->for_all_tables) */

    foreach!(lc, schemas, {
        let schemaid = lfirst_oid(current_cell!(lc));

        let obj = publication_add_schema(pubid, schemaid, if_not_exists);
        if !stmt.is_null() {
            EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress(), stmt as *mut Node);

            InvokeObjectPostCreateHook(PublicationNamespaceRelationId, obj.objectId, 0);
        }
    });
}

/*
 * Remove listed schemas from the publication.
 */
unsafe fn PublicationDropSchemas(pubid: Oid, schemas: *mut List, missing_ok: bool) {
    let mut obj: ObjectAddress = core::mem::zeroed();
    let mut psid: Oid;

    foreach!(lc, schemas, {
        let schemaid = lfirst_oid(current_cell!(lc));

        let psid = GetSysCacheOid2(
            PUBLICATIONNAMESPACEMAP,
            Anum_pg_publication_namespace_oid,
            ObjectIdGetDatum(schemaid),
            ObjectIdGetDatum(pubid),
        );
        if !OidIsValid(psid) {
            if missing_ok {
                continue;
            }

            ereport!(ERROR,
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
                errmsg!("tables from schema \"{}\" are not part of the publication",
                       CStr::from_ptr(get_namespace_name(schemaid)).to_string_lossy())
            );
        }

        ObjectAddressSet(&mut obj, PublicationNamespaceRelationId, psid);
        performDeletion(&obj, DROP_CASCADE, 0);
    });
}

/*
 * Internal workhorse for changing a publication owner
 */
unsafe fn AlterPublicationOwner_internal(
    rel: Relation,
    tup: HeapTuple,
    newOwnerId: Oid,
) {
    let form = GETSTRUCT(tup) as Form_pg_publication;

    if (*form).pubowner == newOwnerId {
        return;
    }

    if !superuser() {
        let aclresult: c_int;

        /* Must be owner */
        if !object_ownercheck(PublicationRelationId, (*form).oid, GetUserId()) {
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_PUBLICATION, NameStr((*form).pubname));
        }

        /* Must be able to become new owner */
        check_can_set_role(GetUserId(), newOwnerId);

        /* New owner must have CREATE privilege on database */
        let aclresult =
            object_aclcheck(DatabaseRelationId(), MyDatabaseId(), newOwnerId, ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId()));
        }

        if (*form).puballtables && !superuser_arg(newOwnerId) {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 * errhint("The owner of a FOR ALL TABLES publication must be a superuser.") */
                errmsg!("permission denied to change owner of publication \"{}\"",
                       CStr::from_ptr(NameStr((*form).pubname)).to_string_lossy())
            );
        }

        if !superuser_arg(newOwnerId) && is_schema_publication((*form).oid) {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 * errhint("The owner of a FOR TABLES IN SCHEMA publication must be a superuser.") */
                errmsg!("permission denied to change owner of publication \"{}\"",
                       CStr::from_ptr(NameStr((*form).pubname)).to_string_lossy())
            );
        }
    }

    (*form).pubowner = newOwnerId;
    CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut _ as *mut c_void, tup);

    /* Update owner dependency reference */
    changeDependencyOnOwner(PublicationRelationId, (*form).oid, newOwnerId);

    InvokeObjectPostAlterHook(PublicationRelationId, (*form).oid, 0);
}

/*
 * Change publication owner -- by name
 */
pub unsafe fn AlterPublicationOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let pubid: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let mut address: ObjectAddress = core::mem::zeroed();
    let pubform: Form_pg_publication;

    let rel = table_open(PublicationRelationId, RowExclusiveLock);

    let tup = SearchSysCacheCopy1(PUBLICATIONNAME, CStringGetDatum(name));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!("publication \"{}\" does not exist", CStr::from_ptr(name).to_string_lossy())
        );
    }

    let pubform = GETSTRUCT(tup) as Form_pg_publication;
    let pubid = (*pubform).oid;

    AlterPublicationOwner_internal(rel, tup, newOwnerId);

    ObjectAddressSet(&mut address, PublicationRelationId, pubid);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Change publication owner -- by OID
 */
pub unsafe fn AlterPublicationOwner_oid(pubid: Oid, newOwnerId: Oid) {
    let tup: HeapTuple;
    let rel: Relation;

    let rel = table_open(PublicationRelationId, RowExclusiveLock);

    let tup = SearchSysCacheCopy1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            errmsg!("publication with OID {} does not exist", pubid)
        );
    }

    AlterPublicationOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Extract the publish_generated_columns option value from a DefElem. "stored"
 * and "none" values are accepted.
 */
unsafe fn defGetGeneratedColsOption(def: *mut DefElem) -> c_char {
    let mut sval: *const c_char = b"\0".as_ptr() as _;

    /*
     * A parameter value is required.
     */
    if !(*def).arg.is_null() {
        sval = defGetString(def);

        if pg_strcasecmp(sval, b"none\0".as_ptr() as _) == 0 {
            return PUBLISH_GENCOLS_NONE;
        }
        if pg_strcasecmp(sval, b"stored\0".as_ptr() as _) == 0 {
            return PUBLISH_GENCOLS_STORED;
        }
    }

    ereport!(ERROR,
        /* C also: errcode(ERRCODE_SYNTAX_ERROR),
         * errdetail("Valid values are \"%s\" and \"%s\".", "none", "stored") */
        errmsg!("invalid value for publication parameter \"{}\": \"{}\"",
               CStr::from_ptr((*def).defname).to_string_lossy(), CStr::from_ptr(sval).to_string_lossy())
    );

    return PUBLISH_GENCOLS_NONE; /* keep compiler quiet */
}

/* ----------------------------------------------------------------
 * Local helpers / stubs needed by expression walker
 * ---------------------------------------------------------------- */

/* ScalarArrayOpExpr stub  TODO(pg-port) */
#[repr(C)]
struct ScalarArrayOpExpr_stub {
    r#type: NodeTag,
    opno: Oid,
    _rest: [u8; 0],
}

/* RowCompareExpr stub  TODO(pg-port) */
#[repr(C)]
struct RowCompareExpr_stub {
    r#type: NodeTag,
    opnos: *mut List,
    _rest: [u8; 0],
}

/* TupleDescAttr helper  TODO(pg-port): real version does desc->attrs[n] */
unsafe fn TupleDescAttr(desc: TupleDesc, n: usize) -> Form_pg_attribute {
    /* The attrs array immediately follows the TupleDescData header;
     * cast desc to *mut *mut FormData_pg_attribute and index it. */
    let attrs_ptr = (desc as *mut *mut FormData_pg_attribute).add(1);
    *attrs_ptr.add(n)
}

/* strcmp wrapper since Rust libc may not be in scope */
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" { fn strcmp(a: *const c_char, b: *const c_char) -> c_int; }
    strcmp(a, b)
}

/* NamespaceRelationId  TODO(pg-port) */
const NamespaceRelationId: Oid = 2615;

/* RelationRelationId  TODO(pg-port) */
const RelationRelationId: Oid = 1259;

/* NAMESPACEOID syscache  TODO(pg-port) */
const NAMESPACEOID: c_int = 38;

/* ereport!/elog!/errmsg! come from crate::prelude::* (real shim macros) */

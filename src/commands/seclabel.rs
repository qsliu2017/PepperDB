//! seclabel.rs
//!   routines to support security label feature.
//!
//! Translated 1:1 from postgres/src/backend/commands/seclabel.c
//! (declarations merged from postgres/src/include/commands/seclabel.h).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple, heap_modify_tuple};
use crate::access::common::relation::relation_close;
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{heap_getattr, HeapTuple};
use crate::access::relscan::SysScanDescData;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog::IsSharedRelation;
use crate::catalog::catalog_oids::{SecLabelRelationId, SharedSecLabelRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_class::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use crate::miscadmin::GetUserId;
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_length, List, ListCell, NIL};
use crate::nodes::parsenodes::{
    ObjectType, ObjectType::OBJECT_ACCESS_METHOD, ObjectType::OBJECT_AGGREGATE,
    ObjectType::OBJECT_AMOP, ObjectType::OBJECT_AMPROC, ObjectType::OBJECT_ATTRIBUTE,
    ObjectType::OBJECT_CAST, ObjectType::OBJECT_COLLATION, ObjectType::OBJECT_COLUMN,
    ObjectType::OBJECT_CONVERSION, ObjectType::OBJECT_DATABASE, ObjectType::OBJECT_DEFACL,
    ObjectType::OBJECT_DEFAULT, ObjectType::OBJECT_DOMAIN, ObjectType::OBJECT_DOMCONSTRAINT,
    ObjectType::OBJECT_EVENT_TRIGGER, ObjectType::OBJECT_EXTENSION, ObjectType::OBJECT_FDW,
    ObjectType::OBJECT_FOREIGN_SERVER, ObjectType::OBJECT_FOREIGN_TABLE,
    ObjectType::OBJECT_FUNCTION, ObjectType::OBJECT_INDEX, ObjectType::OBJECT_LANGUAGE,
    ObjectType::OBJECT_LARGEOBJECT, ObjectType::OBJECT_MATVIEW, ObjectType::OBJECT_OPCLASS,
    ObjectType::OBJECT_OPERATOR, ObjectType::OBJECT_OPFAMILY, ObjectType::OBJECT_PARAMETER_ACL,
    ObjectType::OBJECT_POLICY, ObjectType::OBJECT_PROCEDURE, ObjectType::OBJECT_PUBLICATION,
    ObjectType::OBJECT_PUBLICATION_NAMESPACE, ObjectType::OBJECT_PUBLICATION_REL,
    ObjectType::OBJECT_ROLE, ObjectType::OBJECT_ROUTINE, ObjectType::OBJECT_RULE,
    ObjectType::OBJECT_SCHEMA, ObjectType::OBJECT_SEQUENCE, ObjectType::OBJECT_STATISTIC_EXT,
    ObjectType::OBJECT_SUBSCRIPTION, ObjectType::OBJECT_TABCONSTRAINT, ObjectType::OBJECT_TABLE,
    ObjectType::OBJECT_TABLESPACE, ObjectType::OBJECT_TRANSFORM, ObjectType::OBJECT_TRIGGER,
    ObjectType::OBJECT_TSCONFIGURATION, ObjectType::OBJECT_TSDICTIONARY,
    ObjectType::OBJECT_TSPARSER, ObjectType::OBJECT_TSTEMPLATE, ObjectType::OBJECT_TYPE,
    ObjectType::OBJECT_USER_MAPPING, ObjectType::OBJECT_VIEW, SecLabelStmt,
};
use crate::postgres::{Int32GetDatum, ObjectIdGetDatum};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lockdefs::{
    AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use crate::utils::builtins::{CStringGetTextDatum, TextDatumGetCString};
use crate::utils::palloc::{palloc, pstrdup, MemoryContext, MemoryContextSwitchTo, TopMemoryContext};
use crate::utils::rel::{
    RegProcedure, Relation, RelationGetDescr, RelationGetRelationName,
};
use crate::utils::snapshot::SnapshotData;

use crate::{current_cell, foreach};

// ----------------------------------------------------------------------------
// Constants from the generated catalog headers that are not yet ported.  Values
// match PostgreSQL 18.3.
// ----------------------------------------------------------------------------

// catalog/pg_seclabel.h
// TODO(pg-port): replace with generated Natts_pg_seclabel / Anum_* constants.
const Natts_pg_seclabel: usize = 5;
const Anum_pg_seclabel_objoid: AttrNumber = 1;
const Anum_pg_seclabel_classoid: AttrNumber = 2;
const Anum_pg_seclabel_objsubid: AttrNumber = 3;
const Anum_pg_seclabel_provider: AttrNumber = 4;
const Anum_pg_seclabel_label: AttrNumber = 5;

// catalog/pg_shseclabel.h
// TODO(pg-port): replace with generated Natts_pg_shseclabel / Anum_* constants.
const Natts_pg_shseclabel: usize = 4;
const Anum_pg_shseclabel_objoid: AttrNumber = 1;
const Anum_pg_shseclabel_classoid: AttrNumber = 2;
const Anum_pg_shseclabel_provider: AttrNumber = 3;
const Anum_pg_shseclabel_label: AttrNumber = 4;

// catalog/indexing.h
// TODO(pg-port): replace with generated catalog/indexing.h constants.
const SecLabelObjectIndexId: Oid = 3597; // pg_seclabel_object_index
const SharedSecLabelObjectIndexId: Oid = 3593; // pg_shseclabel_object_index

// utils/fmgroids.h
// TODO(pg-port): replace with the generated utils/fmgroids.h constants.
const F_OIDEQ: RegProcedure = 184;
const F_INT4EQ: RegProcedure = 65;
const F_TEXTEQ: RegProcedure = 67;

// ----------------------------------------------------------------------------
// Stubs for called functions that are not yet ported.
// ----------------------------------------------------------------------------

/* TODO(pg-port): access/genam.h - systable scan helpers not ported yet. */
type SysScanDesc = *mut SysScanDescData;

unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut SnapshotData,
    _nkeys: c_int,
    _key: ScanKey,
) -> SysScanDesc {
    crate::access::index::genam::systable_beginscan(_heapRelation as _, _indexId as _, _indexOK as _, _snapshot as _, _nkeys as _, _key as _) as _
}

unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    crate::access::index::genam::systable_getnext(_sysscan as _) as _
}

unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    crate::access::index::genam::systable_endscan(_sysscan as _)
}

/* TODO(pg-port): catalog/indexing.h - heap+index DML helpers not ported yet. */
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {
    unimplemented!()
}

unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) {
    unimplemented!()
}

unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut ItemPointerData) {
    crate::catalog::indexing::CatalogTupleDelete(_heapRel as _, _tid as _)
}

/* TODO(pg-port): catalog/objectaddress.h - object address resolution not ported yet. */
unsafe fn get_object_address(
    _objtype: ObjectType,
    _object: *mut Node,
    _relp: *mut Relation,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> ObjectAddress {
    unimplemented!()
}

unsafe fn check_object_ownership(
    _roleid: Oid,
    _objtype: ObjectType,
    _address: ObjectAddress,
    _object: *mut Node,
    _relation: Relation,
) {
    unimplemented!()
}

/* TODO(pg-port): access/table.h - errdetail_relkind_not_supported not ported yet. */
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int {
    unimplemented!()
}

/*
 * commands/seclabel.h
 *
 * Hook type invoked by a label provider when a new security label is applied.
 */
pub type check_object_relabel_type =
    Option<unsafe fn(object: *const ObjectAddress, seclabel: *const c_char)>;

#[repr(C)]
pub struct LabelProvider {
    pub provider_name: *const c_char,
    pub hook: check_object_relabel_type,
}

static mut label_provider_list: *mut List = NIL;

unsafe fn SecLabelSupportsObjectType(objtype: ObjectType) -> bool {
    match objtype {
        OBJECT_AGGREGATE
        | OBJECT_COLUMN
        | OBJECT_DATABASE
        | OBJECT_DOMAIN
        | OBJECT_EVENT_TRIGGER
        | OBJECT_FOREIGN_TABLE
        | OBJECT_FUNCTION
        | OBJECT_LANGUAGE
        | OBJECT_LARGEOBJECT
        | OBJECT_MATVIEW
        | OBJECT_PROCEDURE
        | OBJECT_PUBLICATION
        | OBJECT_ROLE
        | OBJECT_ROUTINE
        | OBJECT_SCHEMA
        | OBJECT_SEQUENCE
        | OBJECT_SUBSCRIPTION
        | OBJECT_TABLE
        | OBJECT_TABLESPACE
        | OBJECT_TYPE
        | OBJECT_VIEW => true,

        OBJECT_ACCESS_METHOD
        | OBJECT_AMOP
        | OBJECT_AMPROC
        | OBJECT_ATTRIBUTE
        | OBJECT_CAST
        | OBJECT_COLLATION
        | OBJECT_CONVERSION
        | OBJECT_DEFAULT
        | OBJECT_DEFACL
        | OBJECT_DOMCONSTRAINT
        | OBJECT_EXTENSION
        | OBJECT_FDW
        | OBJECT_FOREIGN_SERVER
        | OBJECT_INDEX
        | OBJECT_OPCLASS
        | OBJECT_OPERATOR
        | OBJECT_OPFAMILY
        | OBJECT_PARAMETER_ACL
        | OBJECT_POLICY
        | OBJECT_PUBLICATION_NAMESPACE
        | OBJECT_PUBLICATION_REL
        | OBJECT_RULE
        | OBJECT_STATISTIC_EXT
        | OBJECT_TABCONSTRAINT
        | OBJECT_TRANSFORM
        | OBJECT_TRIGGER
        | OBJECT_TSCONFIGURATION
        | OBJECT_TSDICTIONARY
        | OBJECT_TSPARSER
        | OBJECT_TSTEMPLATE
        | OBJECT_USER_MAPPING => false,
        /*
         * There's intentionally no default: case here; we want the
         * compiler to warn if a new ObjectType hasn't been handled above.
         */
    }
}

/*
 * ExecSecLabelStmt --
 *
 * Apply a security label to a database object.
 *
 * Returns the ObjectAddress of the object to which the policy was applied.
 */
pub unsafe fn ExecSecLabelStmt(stmt: *mut SecLabelStmt) -> ObjectAddress {
    let mut provider: *mut LabelProvider = null_mut();
    let address: ObjectAddress;
    let mut relation: Relation = null_mut();
    let mut lc: *mut ListCell;

    /*
     * Find the named label provider, or if none specified, check whether
     * there's exactly one, and if so use it.
     */
    if (*stmt).provider == null_mut() {
        if label_provider_list == NIL {
            ereport!(ERROR, "no security label providers have been loaded");
        }
        if list_length(label_provider_list) != 1 {
            ereport!(
                ERROR,
                "must specify provider when multiple security label providers have been loaded"
            );
        }
        provider = linitial(label_provider_list) as *mut LabelProvider;
    } else {
        foreach!(lc, label_provider_list, {
            let lp: *mut LabelProvider = lfirst(current_cell!(lc)) as *mut LabelProvider;

            if libc_strcmp((*stmt).provider, (*lp).provider_name) == 0 {
                provider = lp;
                break;
            }
        });
        if provider == null_mut() {
            ereport!(
                ERROR,
                errmsg!(
                    "security label provider \"{}\" is not loaded",
                    std::ffi::CStr::from_ptr((*stmt).provider).to_string_lossy()
                )
            );
        }
    }

    if !SecLabelSupportsObjectType((*stmt).objtype) {
        ereport!(
            ERROR,
            "security labels are not supported for this type of object"
        );
    }

    /*
     * Translate the parser representation which identifies this object into
     * an ObjectAddress. get_object_address() will throw an error if the
     * object does not exist, and will also acquire a lock on the target to
     * guard against concurrent modifications.
     */
    address = get_object_address(
        (*stmt).objtype,
        (*stmt).object,
        &mut relation,
        ShareUpdateExclusiveLock,
        false,
    );

    /* Require ownership of the target object. */
    check_object_ownership(
        GetUserId(),
        (*stmt).objtype,
        address,
        (*stmt).object,
        relation,
    );

    /* Perform other integrity checks as needed. */
    match (*stmt).objtype {
        OBJECT_COLUMN => {
            /*
             * Allow security labels only on columns of tables, views,
             * materialized views, composite types, and foreign tables (which
             * are the only relkinds for which pg_dump will dump labels).
             */
            if (*(*relation).rd_rel).relkind != RELKIND_RELATION
                && (*(*relation).rd_rel).relkind != RELKIND_VIEW
                && (*(*relation).rd_rel).relkind != RELKIND_MATVIEW
                && (*(*relation).rd_rel).relkind != RELKIND_COMPOSITE_TYPE
                && (*(*relation).rd_rel).relkind != RELKIND_FOREIGN_TABLE
                && (*(*relation).rd_rel).relkind != RELKIND_PARTITIONED_TABLE
            {
                let _ = errdetail_relkind_not_supported((*(*relation).rd_rel).relkind);
                let _ = RelationGetRelationName(relation);
                ereport!(ERROR, "cannot set security label on relation");
            }
        }
        _ => {}
    }

    /* Provider gets control here, may throw ERROR to veto new label. */
    ((*provider).hook.unwrap())(&address, (*stmt).label);

    /* Apply new label. */
    SetSecurityLabel(&address, (*provider).provider_name, (*stmt).label);

    /*
     * If get_object_address() opened the relation for us, we close it to keep
     * the reference count correct - but we retain any locks acquired by
     * get_object_address() until commit time, to guard against concurrent
     * activity.
     */
    if relation != null_mut() {
        relation_close(relation, NoLock);
    }

    address
}

/*
 * GetSharedSecurityLabel returns the security label for a shared object for
 * a given provider, or NULL if there is no such label.
 */
unsafe fn GetSharedSecurityLabel(
    object: *const ObjectAddress,
    provider: *const c_char,
) -> *mut c_char {
    let pg_shseclabel: Relation;
    let mut keys: [ScanKeyData; 3] = std::mem::zeroed();
    let scan: SysScanDesc;
    let tuple: HeapTuple;
    let datum: Datum;
    let mut isnull: bool = false;
    let mut seclabel: *mut c_char = null_mut();

    ScanKeyInit(
        &mut keys[0],
        Anum_pg_shseclabel_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    ScanKeyInit(
        &mut keys[1],
        Anum_pg_shseclabel_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut keys[2],
        Anum_pg_shseclabel_provider,
        BTEqualStrategyNumber,
        F_TEXTEQ,
        CStringGetTextDatum(provider),
    );

    pg_shseclabel = table_open(SharedSecLabelRelationId, AccessShareLock);

    scan = systable_beginscan(
        pg_shseclabel,
        SharedSecLabelObjectIndexId,
        criticalSharedRelcachesBuilt(),
        null_mut(),
        3,
        keys.as_mut_ptr(),
    );

    tuple = systable_getnext(scan);
    if tuple != null_mut() {
        datum = heap_getattr(
            tuple,
            Anum_pg_shseclabel_label as c_int,
            RelationGetDescr(pg_shseclabel),
            &mut isnull,
        );
        if !isnull {
            seclabel = TextDatumGetCString(datum);
        }
    }
    systable_endscan(scan);

    table_close(pg_shseclabel, AccessShareLock);

    seclabel
}

/*
 * GetSecurityLabel returns the security label for a shared or database object
 * for a given provider, or NULL if there is no such label.
 */
pub unsafe fn GetSecurityLabel(
    object: *const ObjectAddress,
    provider: *const c_char,
) -> *mut c_char {
    let pg_seclabel: Relation;
    let mut keys: [ScanKeyData; 4] = std::mem::zeroed();
    let scan: SysScanDesc;
    let tuple: HeapTuple;
    let datum: Datum;
    let mut isnull: bool = false;
    let mut seclabel: *mut c_char = null_mut();

    /* Shared objects have their own security label catalog. */
    if IsSharedRelation((*object).classId) {
        return GetSharedSecurityLabel(object, provider);
    }

    /* Must be an unshared object, so examine pg_seclabel. */
    ScanKeyInit(
        &mut keys[0],
        Anum_pg_seclabel_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    ScanKeyInit(
        &mut keys[1],
        Anum_pg_seclabel_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut keys[2],
        Anum_pg_seclabel_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum((*object).objectSubId),
    );
    ScanKeyInit(
        &mut keys[3],
        Anum_pg_seclabel_provider,
        BTEqualStrategyNumber,
        F_TEXTEQ,
        CStringGetTextDatum(provider),
    );

    pg_seclabel = table_open(SecLabelRelationId, AccessShareLock);

    scan = systable_beginscan(
        pg_seclabel,
        SecLabelObjectIndexId,
        true,
        null_mut(),
        4,
        keys.as_mut_ptr(),
    );

    tuple = systable_getnext(scan);
    if tuple != null_mut() {
        datum = heap_getattr(
            tuple,
            Anum_pg_seclabel_label as c_int,
            RelationGetDescr(pg_seclabel),
            &mut isnull,
        );
        if !isnull {
            seclabel = TextDatumGetCString(datum);
        }
    }
    systable_endscan(scan);

    table_close(pg_seclabel, AccessShareLock);

    seclabel
}

/*
 * SetSharedSecurityLabel is a helper function of SetSecurityLabel to
 * handle shared database objects.
 */
unsafe fn SetSharedSecurityLabel(
    object: *const ObjectAddress,
    provider: *const c_char,
    label: *const c_char,
) {
    let pg_shseclabel: Relation;
    let mut keys: [ScanKeyData; 4] = std::mem::zeroed();
    let scan: SysScanDesc;
    let oldtup: HeapTuple;
    let mut newtup: HeapTuple = null_mut();
    let mut values: [Datum; Natts_pg_shseclabel] = std::mem::zeroed();
    let mut nulls: [bool; Natts_pg_shseclabel] = std::mem::zeroed();
    let mut replaces: [bool; Natts_pg_shseclabel] = std::mem::zeroed();

    /* Prepare to form or update a tuple, if necessary. */
    for i in 0..Natts_pg_shseclabel {
        nulls[i] = false;
        replaces[i] = false;
    }
    values[(Anum_pg_shseclabel_objoid - 1) as usize] = ObjectIdGetDatum((*object).objectId);
    values[(Anum_pg_shseclabel_classoid - 1) as usize] = ObjectIdGetDatum((*object).classId);
    values[(Anum_pg_shseclabel_provider - 1) as usize] = CStringGetTextDatum(provider);
    if label != null() {
        values[(Anum_pg_shseclabel_label - 1) as usize] = CStringGetTextDatum(label);
    }

    /* Use the index to search for a matching old tuple */
    ScanKeyInit(
        &mut keys[0],
        Anum_pg_shseclabel_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    ScanKeyInit(
        &mut keys[1],
        Anum_pg_shseclabel_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut keys[2],
        Anum_pg_shseclabel_provider,
        BTEqualStrategyNumber,
        F_TEXTEQ,
        CStringGetTextDatum(provider),
    );

    pg_shseclabel = table_open(SharedSecLabelRelationId, RowExclusiveLock);

    scan = systable_beginscan(
        pg_shseclabel,
        SharedSecLabelObjectIndexId,
        true,
        null_mut(),
        3,
        keys.as_mut_ptr(),
    );

    oldtup = systable_getnext(scan);
    if oldtup != null_mut() {
        if label == null() {
            CatalogTupleDelete(pg_shseclabel, &mut (*oldtup).t_self);
        } else {
            replaces[(Anum_pg_shseclabel_label - 1) as usize] = true;
            newtup = heap_modify_tuple(
                oldtup,
                RelationGetDescr(pg_shseclabel),
                values.as_ptr(),
                nulls.as_ptr(),
                replaces.as_ptr(),
            );
            CatalogTupleUpdate(pg_shseclabel, &mut (*oldtup).t_self, newtup);
        }
    }
    systable_endscan(scan);

    /* If we didn't find an old tuple, insert a new one */
    if newtup == null_mut() && label != null() {
        newtup = heap_form_tuple(RelationGetDescr(pg_shseclabel), values.as_ptr(), nulls.as_ptr());
        CatalogTupleInsert(pg_shseclabel, newtup);
    }

    if newtup != null_mut() {
        heap_freetuple(newtup);
    }

    table_close(pg_shseclabel, RowExclusiveLock);
}

/*
 * SetSecurityLabel attempts to set the security label for the specified
 * provider on the specified object to the given value.  NULL means that any
 * existing label should be deleted.
 */
pub unsafe fn SetSecurityLabel(
    object: *const ObjectAddress,
    provider: *const c_char,
    label: *const c_char,
) {
    let pg_seclabel: Relation;
    let mut keys: [ScanKeyData; 4] = std::mem::zeroed();
    let scan: SysScanDesc;
    let oldtup: HeapTuple;
    let mut newtup: HeapTuple = null_mut();
    let mut values: [Datum; Natts_pg_seclabel] = std::mem::zeroed();
    let mut nulls: [bool; Natts_pg_seclabel] = std::mem::zeroed();
    let mut replaces: [bool; Natts_pg_seclabel] = std::mem::zeroed();

    /* Shared objects have their own security label catalog. */
    if IsSharedRelation((*object).classId) {
        SetSharedSecurityLabel(object, provider, label);
        return;
    }

    /* Prepare to form or update a tuple, if necessary. */
    for i in 0..Natts_pg_seclabel {
        nulls[i] = false;
        replaces[i] = false;
    }
    values[(Anum_pg_seclabel_objoid - 1) as usize] = ObjectIdGetDatum((*object).objectId);
    values[(Anum_pg_seclabel_classoid - 1) as usize] = ObjectIdGetDatum((*object).classId);
    values[(Anum_pg_seclabel_objsubid - 1) as usize] = Int32GetDatum((*object).objectSubId);
    values[(Anum_pg_seclabel_provider - 1) as usize] = CStringGetTextDatum(provider);
    if label != null() {
        values[(Anum_pg_seclabel_label - 1) as usize] = CStringGetTextDatum(label);
    }

    /* Use the index to search for a matching old tuple */
    ScanKeyInit(
        &mut keys[0],
        Anum_pg_seclabel_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    ScanKeyInit(
        &mut keys[1],
        Anum_pg_seclabel_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut keys[2],
        Anum_pg_seclabel_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum((*object).objectSubId),
    );
    ScanKeyInit(
        &mut keys[3],
        Anum_pg_seclabel_provider,
        BTEqualStrategyNumber,
        F_TEXTEQ,
        CStringGetTextDatum(provider),
    );

    pg_seclabel = table_open(SecLabelRelationId, RowExclusiveLock);

    scan = systable_beginscan(
        pg_seclabel,
        SecLabelObjectIndexId,
        true,
        null_mut(),
        4,
        keys.as_mut_ptr(),
    );

    oldtup = systable_getnext(scan);
    if oldtup != null_mut() {
        if label == null() {
            CatalogTupleDelete(pg_seclabel, &mut (*oldtup).t_self);
        } else {
            replaces[(Anum_pg_seclabel_label - 1) as usize] = true;
            newtup = heap_modify_tuple(
                oldtup,
                RelationGetDescr(pg_seclabel),
                values.as_ptr(),
                nulls.as_ptr(),
                replaces.as_ptr(),
            );
            CatalogTupleUpdate(pg_seclabel, &mut (*oldtup).t_self, newtup);
        }
    }
    systable_endscan(scan);

    /* If we didn't find an old tuple, insert a new one */
    if newtup == null_mut() && label != null() {
        newtup = heap_form_tuple(RelationGetDescr(pg_seclabel), values.as_ptr(), nulls.as_ptr());
        CatalogTupleInsert(pg_seclabel, newtup);
    }

    /* Update indexes, if necessary */
    if newtup != null_mut() {
        heap_freetuple(newtup);
    }

    table_close(pg_seclabel, RowExclusiveLock);
}

/*
 * DeleteSharedSecurityLabel is a helper function of DeleteSecurityLabel
 * to handle shared database objects.
 */
#[no_mangle]
pub unsafe fn DeleteSharedSecurityLabel(objectId: Oid, classId: Oid) {
    let pg_shseclabel: Relation;
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let scan: SysScanDesc;
    let mut oldtup: HeapTuple;

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_shseclabel_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(objectId),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_shseclabel_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classId),
    );

    pg_shseclabel = table_open(SharedSecLabelRelationId, RowExclusiveLock);

    scan = systable_beginscan(
        pg_shseclabel,
        SharedSecLabelObjectIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );
    loop {
        oldtup = systable_getnext(scan);
        if oldtup == null_mut() {
            break;
        }
        CatalogTupleDelete(pg_shseclabel, &mut (*oldtup).t_self);
    }
    systable_endscan(scan);

    table_close(pg_shseclabel, RowExclusiveLock);
}

/*
 * DeleteSecurityLabel removes all security labels for an object (and any
 * sub-objects, if applicable).
 */
pub unsafe fn DeleteSecurityLabel(object: *const ObjectAddress) {
    let pg_seclabel: Relation;
    let mut skey: [ScanKeyData; 3] = std::mem::zeroed();
    let scan: SysScanDesc;
    let mut oldtup: HeapTuple;
    let nkeys: c_int;

    /* Shared objects have their own security label catalog. */
    if IsSharedRelation((*object).classId) {
        Assert!((*object).objectSubId == 0);
        DeleteSharedSecurityLabel((*object).objectId, (*object).classId);
        return;
    }

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_seclabel_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_seclabel_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    if (*object).objectSubId != 0 {
        ScanKeyInit(
            &mut skey[2],
            Anum_pg_seclabel_objsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum((*object).objectSubId),
        );
        nkeys = 3;
    } else {
        nkeys = 2;
    }

    pg_seclabel = table_open(SecLabelRelationId, RowExclusiveLock);

    scan = systable_beginscan(
        pg_seclabel,
        SecLabelObjectIndexId,
        true,
        null_mut(),
        nkeys,
        skey.as_mut_ptr(),
    );
    loop {
        oldtup = systable_getnext(scan);
        if oldtup == null_mut() {
            break;
        }
        CatalogTupleDelete(pg_seclabel, &mut (*oldtup).t_self);
    }
    systable_endscan(scan);

    table_close(pg_seclabel, RowExclusiveLock);
}

pub unsafe fn register_label_provider(
    provider_name: *const c_char,
    hook: check_object_relabel_type,
) {
    let provider: *mut LabelProvider;
    let oldcxt: MemoryContext;

    oldcxt = MemoryContextSwitchTo(TopMemoryContext as MemoryContext);
    provider = palloc(std::mem::size_of::<LabelProvider>()) as *mut LabelProvider;
    (*provider).provider_name = pstrdup(provider_name);
    (*provider).hook = hook;
    label_provider_list = lappend(label_provider_list, provider as *mut c_void);
    MemoryContextSwitchTo(oldcxt);
}

// ----------------------------------------------------------------------------
// Stubs for symbols that have no home yet.
// ----------------------------------------------------------------------------

/*
 * TODO(pg-port): utils/cache/relcache.h - this global tracks whether the
 * critical shared relcache entries have been built; consulted as the indexOK
 * flag for the shared seclabel scan.
 */
unsafe fn criticalSharedRelcachesBuilt() -> bool {
    unimplemented!()
}

/* strcmp for C strings (string.h); used to match provider names. */
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
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

//! commands/comment.c - PostgreSQL object comments utility code.

use crate::prelude::*;
use crate::access::common::tupdesc::TupleDesc;
use crate::strVal;

use std::ffi::c_void;

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple, heap_modify_tuple};
use crate::access::common::relation::relation_close;
use crate::access::common::scankey::{ScanKey, ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{heap_getattr, HeapTuple};
use crate::access::relscan::SysScanDescData;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::{DescriptionRelationId, SharedDescriptionRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_class::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use crate::miscadmin::GetUserId;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    CommentStmt, ObjectType, ObjectType::OBJECT_COLUMN, ObjectType::OBJECT_DATABASE,
    ObjectType::OBJECT_ROLE, ObjectType::OBJECT_TABLESPACE,
};
use crate::postgres::{Int32GetDatum, ObjectIdGetDatum};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lockdefs::{
    AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use crate::utils::builtins::{CStringGetTextDatum, TextDatumGetCString};
use crate::utils::rel::{
    RegProcedure, Relation, RelationGetDescr, RelationGetRelationName,
};
use crate::utils::snapshot::SnapshotData;

// ----------------------------------------------------------------------------
// Constants from the generated catalog headers that are not yet ported.  Values
// match PostgreSQL 18.3.
// ----------------------------------------------------------------------------

// catalog/pg_description.h
// TODO(pg-port): replace with generated Natts_pg_description / Anum_* constants.
const Natts_pg_description: usize = 4;
const Anum_pg_description_objoid: AttrNumber = 1;
const Anum_pg_description_classoid: AttrNumber = 2;
const Anum_pg_description_objsubid: AttrNumber = 3;
const Anum_pg_description_description: AttrNumber = 4;

// catalog/pg_shdescription.h
// TODO(pg-port): replace with generated Natts_pg_shdescription / Anum_* constants.
const Natts_pg_shdescription: usize = 3;
const Anum_pg_shdescription_objoid: AttrNumber = 1;
const Anum_pg_shdescription_classoid: AttrNumber = 2;
const Anum_pg_shdescription_description: AttrNumber = 3;

// catalog/indexing.h
// TODO(pg-port): replace with generated catalog/indexing.h constants.
const DescriptionObjIndexId: Oid = 2675; // pg_description_o_c_o_index
const SharedDescriptionObjIndexId: Oid = 2397; // pg_shdescription_o_c_index

// utils/fmgroids.h
// TODO(pg-port): replace with the generated utils/fmgroids.h constants.
const F_OIDEQ: RegProcedure = 184;
const F_INT4EQ: RegProcedure = 65;

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

/* TODO(pg-port): commands/dbcommands.h - get_database_oid not ported yet. */
unsafe fn get_database_oid(_dbname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!()
}

/* TODO(pg-port): access/table.h - errdetail_relkind_not_supported not ported yet. */
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int {
    unimplemented!()
}

/*
 * InvalidObjectAddress - the all-zero / invalid object address.
 * catalog/objectaddress.h
 */
fn InvalidObjectAddress() -> ObjectAddress {
    ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    }
}

/*
 * CommentObject --
 *
 * This routine is used to add the associated comment into
 * pg_description for the object specified by the given SQL command.
 */
pub unsafe fn CommentObject(stmt: *mut CommentStmt) -> ObjectAddress {
    let mut relation: Relation = null_mut();
    let mut address: ObjectAddress = InvalidObjectAddress();

    /*
     * When loading a dump, we may see a COMMENT ON DATABASE for the old name
     * of the database.  Erroring out would prevent pg_restore from completing
     * (which is really pg_restore's fault, but for now we will work around
     * the problem here).  Consensus is that the best fix is to treat wrong
     * database name as a WARNING not an ERROR; hence, the following special
     * case.
     */
    if (*stmt).objtype == OBJECT_DATABASE {
        let database: *mut c_char = strVal!((*stmt).object);

        if !OidIsValid(get_database_oid(database, true)) {
            ereport!(WARNING, "database does not exist");
            return address;
        }
    }

    /*
     * Translate the parser representation that identifies this object into an
     * ObjectAddress.  get_object_address() will throw an error if the object
     * does not exist, and will also acquire a lock on the target to guard
     * against concurrent DROP operations.
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
             * Allow comments only on columns of tables, views, materialized
             * views, composite types, and foreign tables (which are the only
             * relkinds for which pg_dump will dump per-column comments).  In
             * particular we wish to disallow comments on index columns,
             * because the naming of an index's columns may change across PG
             * versions, so dumping per-column comments could create reload
             * failures.
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
                ereport!(ERROR, "cannot set comment on relation");
            }
        }
        _ => {}
    }

    /*
     * Databases, tablespaces, and roles are cluster-wide objects, so any
     * comments on those objects are recorded in the shared pg_shdescription
     * catalog.  Comments on all other objects are recorded in pg_description.
     */
    if (*stmt).objtype == OBJECT_DATABASE
        || (*stmt).objtype == OBJECT_TABLESPACE
        || (*stmt).objtype == OBJECT_ROLE
    {
        CreateSharedComments(address.objectId, address.classId, (*stmt).comment);
    } else {
        CreateComments(
            address.objectId,
            address.classId,
            address.objectSubId,
            (*stmt).comment,
        );
    }

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
 * CreateComments --
 *
 * Create a comment for the specified object descriptor.  Inserts a new
 * pg_description tuple, or replaces an existing one with the same key.
 *
 * If the comment given is null or an empty string, instead delete any
 * existing comment for the specified key.
 */
pub unsafe fn CreateComments(oid: Oid, classoid: Oid, subid: int32, comment: *const c_char) {
    let description: Relation;
    let mut skey: [ScanKeyData; 3] = std::mem::zeroed();
    let sd: SysScanDesc;
    let oldtuple: HeapTuple;
    let mut newtuple: HeapTuple = null_mut();
    let mut values: [Datum; Natts_pg_description] = std::mem::zeroed();
    let mut nulls: [bool; Natts_pg_description] = std::mem::zeroed();
    let mut replaces: [bool; Natts_pg_description] = std::mem::zeroed();
    let mut comment = comment;

    /* Reduce empty-string to NULL case */
    if comment != null() && libc_strlen(comment) == 0 {
        comment = null();
    }

    /* Prepare to form or update a tuple, if necessary */
    if comment != null() {
        for i in 0..Natts_pg_description {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[(Anum_pg_description_objoid - 1) as usize] = ObjectIdGetDatum(oid);
        values[(Anum_pg_description_classoid - 1) as usize] = ObjectIdGetDatum(classoid);
        values[(Anum_pg_description_objsubid - 1) as usize] = Int32GetDatum(subid);
        values[(Anum_pg_description_description - 1) as usize] = CStringGetTextDatum(comment);
    }

    /* Use the index to search for a matching old tuple */

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_description_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_description_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classoid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_description_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(subid),
    );

    description = table_open(DescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(
        description,
        DescriptionObjIndexId,
        true,
        null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    loop {
        oldtuple = systable_getnext(sd);
        if oldtuple == null_mut() {
            break;
        }

        /* Found the old tuple, so delete or update it */

        if comment == null() {
            CatalogTupleDelete(description, &mut (*oldtuple).t_self);
        } else {
            newtuple = heap_modify_tuple(
                oldtuple,
                RelationGetDescr(description),
                values.as_ptr(),
                nulls.as_ptr(),
                replaces.as_ptr(),
            );
            CatalogTupleUpdate(description, &mut (*oldtuple).t_self, newtuple);
        }

        break; /* Assume there can be only one match */
    }

    systable_endscan(sd);

    /* If we didn't find an old tuple, insert a new one */

    if newtuple == null_mut() && comment != null() {
        newtuple = heap_form_tuple(RelationGetDescr(description), values.as_ptr(), nulls.as_ptr());
        CatalogTupleInsert(description, newtuple);
    }

    if newtuple != null_mut() {
        heap_freetuple(newtuple);
    }

    /* Done */

    table_close(description, NoLock);
}

/*
 * CreateSharedComments --
 *
 * Create a comment for the specified shared object descriptor.  Inserts a
 * new pg_shdescription tuple, or replaces an existing one with the same key.
 *
 * If the comment given is null or an empty string, instead delete any
 * existing comment for the specified key.
 */
pub unsafe fn CreateSharedComments(oid: Oid, classoid: Oid, comment: *const c_char) {
    let shdescription: Relation;
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let sd: SysScanDesc;
    let oldtuple: HeapTuple;
    let mut newtuple: HeapTuple = null_mut();
    let mut values: [Datum; Natts_pg_shdescription] = std::mem::zeroed();
    let mut nulls: [bool; Natts_pg_shdescription] = std::mem::zeroed();
    let mut replaces: [bool; Natts_pg_shdescription] = std::mem::zeroed();
    let mut comment = comment;

    /* Reduce empty-string to NULL case */
    if comment != null() && libc_strlen(comment) == 0 {
        comment = null();
    }

    /* Prepare to form or update a tuple, if necessary */
    if comment != null() {
        for i in 0..Natts_pg_shdescription {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[(Anum_pg_shdescription_objoid - 1) as usize] = ObjectIdGetDatum(oid);
        values[(Anum_pg_shdescription_classoid - 1) as usize] = ObjectIdGetDatum(classoid);
        values[(Anum_pg_shdescription_description - 1) as usize] = CStringGetTextDatum(comment);
    }

    /* Use the index to search for a matching old tuple */

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_shdescription_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_shdescription_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classoid),
    );

    shdescription = table_open(SharedDescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(
        shdescription,
        SharedDescriptionObjIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    loop {
        oldtuple = systable_getnext(sd);
        if oldtuple == null_mut() {
            break;
        }

        /* Found the old tuple, so delete or update it */

        if comment == null() {
            CatalogTupleDelete(shdescription, &mut (*oldtuple).t_self);
        } else {
            newtuple = heap_modify_tuple(
                oldtuple,
                RelationGetDescr(shdescription),
                values.as_ptr(),
                nulls.as_ptr(),
                replaces.as_ptr(),
            );
            CatalogTupleUpdate(shdescription, &mut (*oldtuple).t_self, newtuple);
        }

        break; /* Assume there can be only one match */
    }

    systable_endscan(sd);

    /* If we didn't find an old tuple, insert a new one */

    if newtuple == null_mut() && comment != null() {
        newtuple =
            heap_form_tuple(RelationGetDescr(shdescription), values.as_ptr(), nulls.as_ptr());
        CatalogTupleInsert(shdescription, newtuple);
    }

    if newtuple != null_mut() {
        heap_freetuple(newtuple);
    }

    /* Done */

    table_close(shdescription, NoLock);
}

/*
 * DeleteComments -- remove comments for an object
 *
 * If subid is nonzero then only comments matching it will be removed.
 * If subid is zero, all comments matching the oid/classoid will be removed
 * (this corresponds to deleting a whole object).
 */
pub unsafe fn DeleteComments(oid: Oid, classoid: Oid, subid: int32) {
    let description: Relation;
    let mut skey: [ScanKeyData; 3] = std::mem::zeroed();
    let nkeys: c_int;
    let sd: SysScanDesc;
    let mut oldtuple: HeapTuple;

    /* Use the index to search for all matching old tuples */

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_description_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_description_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classoid),
    );

    if subid != 0 {
        ScanKeyInit(
            &mut skey[2],
            Anum_pg_description_objsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum(subid),
        );
        nkeys = 3;
    } else {
        nkeys = 2;
    }

    description = table_open(DescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(
        description,
        DescriptionObjIndexId,
        true,
        null_mut(),
        nkeys,
        skey.as_mut_ptr(),
    );

    loop {
        oldtuple = systable_getnext(sd);
        if oldtuple == null_mut() {
            break;
        }
        CatalogTupleDelete(description, &mut (*oldtuple).t_self);
    }

    /* Done */

    systable_endscan(sd);
    table_close(description, RowExclusiveLock);
}

/*
 * DeleteSharedComments -- remove comments for a shared object
 */
pub unsafe fn DeleteSharedComments(oid: Oid, classoid: Oid) {
    let shdescription: Relation;
    let mut skey: [ScanKeyData; 2] = std::mem::zeroed();
    let sd: SysScanDesc;
    let mut oldtuple: HeapTuple;

    /* Use the index to search for all matching old tuples */

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_shdescription_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_shdescription_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classoid),
    );

    shdescription = table_open(SharedDescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(
        shdescription,
        SharedDescriptionObjIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    loop {
        oldtuple = systable_getnext(sd);
        if oldtuple == null_mut() {
            break;
        }
        CatalogTupleDelete(shdescription, &mut (*oldtuple).t_self);
    }

    /* Done */

    systable_endscan(sd);
    table_close(shdescription, RowExclusiveLock);
}

/*
 * GetComment -- get the comment for an object, or null if not found.
 */
pub unsafe fn GetComment(oid: Oid, classoid: Oid, subid: int32) -> *mut c_char {
    let description: Relation;
    let mut skey: [ScanKeyData; 3] = std::mem::zeroed();
    let sd: SysScanDesc;
    let tupdesc: TupleDesc;
    let tuple: HeapTuple;
    let mut comment: *mut c_char;

    /* Use the index to search for a matching old tuple */

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_description_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(oid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_description_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(classoid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_description_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(subid),
    );

    description = table_open(DescriptionRelationId, AccessShareLock);
    tupdesc = RelationGetDescr(description);

    sd = systable_beginscan(
        description,
        DescriptionObjIndexId,
        true,
        null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    comment = null_mut();
    loop {
        tuple = systable_getnext(sd);
        if tuple == null_mut() {
            break;
        }

        let value: Datum;
        let mut isnull: bool = false;

        /* Found the tuple, get description field */
        value = heap_getattr(
            tuple,
            Anum_pg_description_description as c_int,
            tupdesc,
            &mut isnull,
        );
        if !isnull {
            comment = TextDatumGetCString(value);
        }
        break; /* Assume there can be only one match */
    }

    systable_endscan(sd);

    /* Done */
    table_close(description, AccessShareLock);

    comment
}

/* strlen for a C string (string.h); used for empty-string reduction. */
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut len: usize = 0;
    while *s.add(len) != 0 {
        len += 1;
    }
    len
}

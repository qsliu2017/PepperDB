//! Arc<RelationData> creation: the catalog orchestrator. Translated from the M2-reachable
//! parts of `src/backend/catalog/heap.c`.
//!
//! `heap_create_with_catalog` is the engine behind `CREATE TABLE`: assign an OID,
//! create the heap's storage (`heap_create`), write its pg_class row
//! (`AddNewRelationTuple` / `InsertPgClassTuple`), its pg_attribute rows
//! (`AddNewAttributeTuples`), and its composite rowtype in pg_type
//! (`AddNewRelationType` -> `TypeCreate`).
//!
//! Async coloring (rules.md s5): every step reaches the buffer pool (catalog
//! inserts, storage create), so the creation path is `async` and threads
//! `&Arc<SharedState>`.
//!
//! M2 scope: a plain heap table of fixed-width columns -- no toast (the int table
//! needs none), no defaults/constraints, no array type, no partitioning. Those
//! paths are staged stubs (rules.md s4).

#![allow(
    clippy::too_many_arguments,
    reason = "heap_create/heap_create_with_catalog mirror the C signatures 1:1 (port-inherent)"
)]

use std::sync::Arc;

use crate::access::tupdesc::TupleDesc;
use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::catalog::{get_new_rel_file_number, rel_default_tablespace};
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::catalog::pg_type::type_create;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::c::NameData;
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::{
    self as c, FormData_pg_class, Form_pg_class, RelationRelationId, RELKIND_COMPOSITE_TYPE,
    RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELPERSISTENCE_PERMANENT,
};
use crate::catalog::pg_type::{
    TYPALIGN_DOUBLE, TYPCATEGORY_COMPOSITE, TYPSTORAGE_EXTENDED, TYPTYPE_COMPOSITE,
};
use crate::common::relpath::{ForkNumber, RelFileNumber};
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Datum, Int16GetDatum, Int32GetDatum, NameGetDatum,
    ObjectIdGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;
use crate::utils::rel::{LockInfoData, LockRelId, RelationData};

const HEAP_TABLE_AM_OID: Oid = Oid::new(2);
const RECORDOID: Oid = Oid::new(2249);
const DEFAULT_TYPDELIM: i8 = b',' as i8;

/// Copy `src` into a `NameData`, NUL-padded to NAMEDATALEN (C namestrcpy).
pub(crate) fn name_data(src: &str) -> NameData {
    let mut nd = NameData { data: [0u8; crate::c::NAMEDATALEN] };
    let bytes = src.as_bytes();
    let n = bytes.len().min(crate::c::NAMEDATALEN - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// Build a leaked local `RelationData` for a freshly created relation, with the
/// load-bearing fields the catalog writes + heap insert read (PG
/// `RelationBuildLocalRelation`, the part the M2 path needs). The returned handle
/// is a relcache-shaped descriptor; the caller frees it via [`free_local_relation`].
fn build_local_relation(
    relname: &str,
    relnamespace: Oid,
    reltablespace: Oid,
    relid: Oid,
    relfilenumber: RelFileNumber,
    accessmtd: Oid,
    tupdesc: TupleDesc,
    relkind: i8,
    relpersistence: i8,
    shared_relation: bool,
) -> Arc<RelationData> {
    let dbid = if shared_relation {
        InvalidOid
    } else {
        crate::session::current().database_id()
    };
    let spc = rel_default_tablespace(reltablespace, shared_relation);

    let mut form: Box<FormData_pg_class> = Box::new(blank_pg_class());
    form.oid = relid;
    form.relname = name_data(relname);
    form.relnamespace = relnamespace;
    form.reltype = InvalidOid; // filled by AddNewRelationTuple
    form.reloftype = InvalidOid;
    form.relowner = InvalidOid; // filled by AddNewRelationTuple
    form.relam = accessmtd;
    form.relfilenode = relfilenumber;
    form.reltablespace = if spc == crate::common::relpath::DEFAULTTABLESPACE_OID {
        InvalidOid // never store the default tablespace in pg_class (PG rule)
    } else {
        spc
    };
    form.relpages = 0;
    form.reltuples = -1.0;
    form.relallvisible = 0;
    form.relallfrozen = 0;
    form.reltoastrelid = InvalidOid;
    form.relhasindex = false;
    form.relisshared = shared_relation;
    form.relpersistence = relpersistence;
    form.relkind = relkind;
    form.relnatts = i16::try_from(tupdesc.natts).unwrap_or(0);
    form.relchecks = 0;
    form.relispopulated = true;
    form.relreplident = b'd' as i8; // REPLICA_IDENTITY_DEFAULT
    form.relispartition = false;
    form.relfrozenxid = crate::access::transam::INVALID_TRANSACTION_ID;
    form.relminmxid = crate::access::transam::INVALID_TRANSACTION_ID;

    let mut rel = RelationData::blank();
    rel.rd_id = relid;
    rel.rd_isnailed = false;
    *rel.rd_isvalid.get_mut() = true;
    *rel.rd_refcnt.get_mut() = 1;
    rel.rd_rel = Some(form);
    rel.rd_att = Some(tupdesc);
    rel.rd_amhandler = accessmtd;
    rel.rd_locator = RelFileLocator { spcOid: spc, dbOid: dbid, relNumber: relfilenumber };
    rel.rd_lockInfo = LockInfoData { lockRelId: LockRelId { relId: relid, dbId: dbid } };

    Arc::new(rel)
}

/// Drop a leaked local relation when the `Arc` is unshared (the last holder); the
/// `RelationData` -- with its owned `rd_rel`/`Vec`/`Box<SmgrRelation>` fields --
/// drops with the `Arc`. A still-shared entry is left for the other holders.
fn free_local_relation(rel: Arc<RelationData>) {
    drop(Arc::into_inner(rel));
}

/// `heap_create`: create the storage-level relation. Builds the local
/// `RelationData` and, when `create_storage`, creates its main fork on disk.
/// Returns the local relation handle (the caller writes its catalog rows then
/// frees it).
pub async fn heap_create(
    shared: &Arc<SharedState>,
    relname: &str,
    relnamespace: Oid,
    reltablespace: Oid,
    relid: Oid,
    relfilenumber: RelFileNumber,
    accessmtd: Oid,
    tupdesc: TupleDesc,
    relkind: i8,
    relpersistence: i8,
    shared_relation: bool,
    create_storage: bool,
) -> Arc<RelationData> {
    debug_assert!(relid.is_valid(), "heap_create requires a valid relid");

    let rel = build_local_relation(
        relname,
        relnamespace,
        reltablespace,
        relid,
        relfilenumber,
        accessmtd,
        tupdesc,
        relkind,
        relpersistence,
        shared_relation,
    );

    // Create the physical storage (the main fork). PG routes table AMs through
    // table_relation_set_new_filelocator; for M2 the heap/index storage create is a
    // direct smgr create of the main fork at the relation's locator.
    if create_storage && c::RELKIND_HAS_STORAGE(relkind) {
        let locator = rel.rd_locator;
        let mut smgr = SmgrRelation::open(locator, crate::storage::procnumber::INVALID_PROC_NUMBER);
        smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
    }

    rel
}

/// `InsertPgClassTuple` (pub for `index_create`): build the pg_class tuple from a
/// relation's `rd_rel` and insert it via `CatalogTupleInsert`.
pub async fn insert_pg_class_tuple_pub(
    shared: &Arc<SharedState>,
    pg_class_desc: &RelationData,
    new_rel_desc: &RelationData,
    new_rel_oid: Oid,
    relacl: Option<Datum>,
    reloptions: Option<Datum>,
) {
    insert_pg_class_tuple(shared, pg_class_desc, new_rel_desc, new_rel_oid, relacl, reloptions)
        .await;
}

/// `InsertPgClassTuple`: build the pg_class tuple from a relation's `rd_rel` and
/// insert it via `CatalogTupleInsert`.
async fn insert_pg_class_tuple(
    shared: &Arc<SharedState>,
    pg_class_desc: &RelationData,
    new_rel_desc: &RelationData,
    new_rel_oid: Oid,
    relacl: Option<Datum>,
    reloptions: Option<Datum>,
) {
    let desc = pg_class_desc.rd_att.clone()
        .unwrap_or_else(|| unreachable!("pg_class has a descriptor"));
    let natts = desc.natts as usize;
    let rd_rel: &FormData_pg_class = new_rel_desc.form();

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;

    set(&mut values, c::Anum_pg_class_oid, ObjectIdGetDatum(new_rel_oid));
    set(&mut values, c::Anum_pg_class_relname, NameGetDatum(&rd_rel.relname));
    set(&mut values, c::Anum_pg_class_relnamespace, ObjectIdGetDatum(rd_rel.relnamespace));
    set(&mut values, c::Anum_pg_class_reltype, ObjectIdGetDatum(rd_rel.reltype));
    set(&mut values, c::Anum_pg_class_reloftype, ObjectIdGetDatum(rd_rel.reloftype));
    set(&mut values, c::Anum_pg_class_relowner, ObjectIdGetDatum(rd_rel.relowner));
    set(&mut values, c::Anum_pg_class_relam, ObjectIdGetDatum(rd_rel.relam));
    set(&mut values, c::Anum_pg_class_relfilenode, ObjectIdGetDatum(rd_rel.relfilenode));
    set(&mut values, c::Anum_pg_class_reltablespace, ObjectIdGetDatum(rd_rel.reltablespace));
    set(&mut values, c::Anum_pg_class_relpages, Int32GetDatum(rd_rel.relpages));
    set(&mut values, c::Anum_pg_class_reltuples, crate::postgres::Float4GetDatum(rd_rel.reltuples));
    set(&mut values, c::Anum_pg_class_relallvisible, Int32GetDatum(rd_rel.relallvisible));
    set(&mut values, c::Anum_pg_class_relallfrozen, Int32GetDatum(rd_rel.relallfrozen));
    set(&mut values, c::Anum_pg_class_reltoastrelid, ObjectIdGetDatum(rd_rel.reltoastrelid));
    set(&mut values, c::Anum_pg_class_relhasindex, BoolGetDatum(rd_rel.relhasindex));
    set(&mut values, c::Anum_pg_class_relisshared, BoolGetDatum(rd_rel.relisshared));
    set(&mut values, c::Anum_pg_class_relpersistence, CharGetDatum(rd_rel.relpersistence));
    set(&mut values, c::Anum_pg_class_relkind, CharGetDatum(rd_rel.relkind));
    set(&mut values, c::Anum_pg_class_relnatts, Int16GetDatum(rd_rel.relnatts));
    set(&mut values, c::Anum_pg_class_relchecks, Int16GetDatum(rd_rel.relchecks));
    set(&mut values, c::Anum_pg_class_relhasrules, BoolGetDatum(rd_rel.relhasrules));
    set(&mut values, c::Anum_pg_class_relhastriggers, BoolGetDatum(rd_rel.relhastriggers));
    set(&mut values, c::Anum_pg_class_relrowsecurity, BoolGetDatum(rd_rel.relrowsecurity));
    set(
        &mut values,
        c::Anum_pg_class_relforcerowsecurity,
        BoolGetDatum(rd_rel.relforcerowsecurity),
    );
    set(&mut values, c::Anum_pg_class_relhassubclass, BoolGetDatum(rd_rel.relhassubclass));
    set(&mut values, c::Anum_pg_class_relispopulated, BoolGetDatum(rd_rel.relispopulated));
    set(&mut values, c::Anum_pg_class_relreplident, CharGetDatum(rd_rel.relreplident));
    set(&mut values, c::Anum_pg_class_relispartition, BoolGetDatum(rd_rel.relispartition));
    set(&mut values, c::Anum_pg_class_relrewrite, ObjectIdGetDatum(rd_rel.relrewrite));
    set(
        &mut values,
        c::Anum_pg_class_relfrozenxid,
        crate::postgres::TransactionIdGetDatum(rd_rel.relfrozenxid),
    );
    set(
        &mut values,
        c::Anum_pg_class_relminmxid,
        crate::postgres::TransactionIdGetDatum(rd_rel.relminmxid),
    );

    match relacl {
        Some(d) => set(&mut values, c::Anum_pg_class_relacl, d),
        None => isnull[(c::Anum_pg_class_relacl - 1) as usize] = true,
    }
    match reloptions {
        Some(d) => set(&mut values, c::Anum_pg_class_reloptions, d),
        None => isnull[(c::Anum_pg_class_reloptions - 1) as usize] = true,
    }
    isnull[(c::Anum_pg_class_relpartbound - 1) as usize] = true;

    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, pg_class_desc, &mut tup).await;
    heap_freetuple(tup);
}

/// `AddNewRelationTuple`: finalize the new relation's `rd_rel` (stats, type, owner)
/// then insert its pg_class row.
async fn add_new_relation_tuple(
    shared: &Arc<SharedState>,
    pg_class_desc: &RelationData,
    new_rel_desc: &mut RelationData,
    new_rel_oid: Oid,
    new_type_oid: Oid,
    relowner: Oid,
    relkind: i8,
) {
    let rd_rel: &mut FormData_pg_class = new_rel_desc.rd_rel.as_deref_mut()
        .unwrap_or_else(|| unreachable!("new relation has a pg_class form"));
    rd_rel.relpages = 0;
    rd_rel.reltuples = -1.0;
    rd_rel.relallvisible = 0;
    rd_rel.relallfrozen = 0;
    if relkind == RELKIND_SEQUENCE {
        rd_rel.relpages = 1;
        rd_rel.reltuples = 1.0;
    }
    rd_rel.relowner = relowner;
    rd_rel.reltype = new_type_oid;
    rd_rel.relispartition = false;

    // The tuple descriptor's composite type id mirrors the rowtype (PG sets it
    // here on rd_att); the relcache rebuild reads it from pg_type on next open.
    if let Some(att) = new_rel_desc.rd_att.as_ref() {
        // rd_att is an Arc; only the next relcache build sets tdtypeid. Recording
        // it here on the shared Arc would need make_mut; M2 reads it back from disk.
        let _ = att;
    }

    insert_pg_class_tuple(shared, pg_class_desc, new_rel_desc, new_rel_oid, None, None).await;
}

/// `InsertPgAttributeTuples` (M2 form): insert one pg_attribute row per user
/// column from the tuple descriptor. System attributes are staged (M2 reads only
/// user columns back; heapam needs no negative attnums for a plain scan).
async fn add_new_attribute_tuples(
    shared: &Arc<SharedState>,
    new_rel_oid: Oid,
    tupdesc: &TupleDesc,
    _relkind: i8,
) {
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId, FormData_pg_attribute};

    let pg_attribute = relation_id_get_relation(AttributeRelationId)
        .unwrap_or_else(|| unreachable!("pg_attribute is nailed/open"));
    let desc = pg_attribute.rd_att.clone()
        .unwrap_or_else(|| unreachable!("pg_attribute has a descriptor"));
    let natts = desc.natts as usize;

    for i in 0..(tupdesc.natts as usize) {
        let attr: &FormData_pg_attribute = tupdesc.attr(i);
        let mut values = vec![Datum(0); natts];
        let mut isnull = vec![false; natts];
        let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;

        set(&mut values, a::Anum_pg_attribute_attrelid, ObjectIdGetDatum(new_rel_oid));
        set(&mut values, a::Anum_pg_attribute_attname, NameGetDatum(&attr.attname));
        set(&mut values, a::Anum_pg_attribute_atttypid, ObjectIdGetDatum(attr.atttypid));
        set(&mut values, a::Anum_pg_attribute_attlen, Int16GetDatum(attr.attlen));
        set(&mut values, a::Anum_pg_attribute_attnum, Int16GetDatum(attr.attnum));
        set(&mut values, a::Anum_pg_attribute_atttypmod, Int32GetDatum(attr.atttypmod));
        set(&mut values, a::Anum_pg_attribute_attndims, Int16GetDatum(attr.attndims));
        set(&mut values, a::Anum_pg_attribute_attbyval, BoolGetDatum(attr.attbyval));
        set(&mut values, a::Anum_pg_attribute_attalign, CharGetDatum(attr.attalign));
        set(&mut values, a::Anum_pg_attribute_attstorage, CharGetDatum(attr.attstorage));
        set(
            &mut values,
            a::Anum_pg_attribute_attcompression,
            CharGetDatum(attr.attcompression),
        );
        set(&mut values, a::Anum_pg_attribute_attnotnull, BoolGetDatum(attr.attnotnull));
        set(&mut values, a::Anum_pg_attribute_atthasdef, BoolGetDatum(attr.atthasdef));
        set(&mut values, a::Anum_pg_attribute_atthasmissing, BoolGetDatum(false));
        set(&mut values, a::Anum_pg_attribute_attidentity, CharGetDatum(attr.attidentity));
        set(&mut values, a::Anum_pg_attribute_attgenerated, CharGetDatum(attr.attgenerated));
        set(&mut values, a::Anum_pg_attribute_attisdropped, BoolGetDatum(false));
        set(&mut values, a::Anum_pg_attribute_attislocal, BoolGetDatum(true));
        set(&mut values, a::Anum_pg_attribute_attinhcount, Int16GetDatum(0));
        set(&mut values, a::Anum_pg_attribute_attcollation, ObjectIdGetDatum(attr.attcollation));
        // Variable-length / always-NULL-on-create columns.
        isnull[(a::Anum_pg_attribute_attstattarget - 1) as usize] = true;
        isnull[(a::Anum_pg_attribute_attoptions - 1) as usize] = true;
        isnull[(a::Anum_pg_attribute_attacl - 1) as usize] = true;
        isnull[(a::Anum_pg_attribute_attfdwoptions - 1) as usize] = true;
        isnull[(a::Anum_pg_attribute_attmissingval - 1) as usize] = true;

        let mut tup = heap_form_tuple(&desc, &values, &isnull);
        catalog_tuple_insert(shared, &pg_attribute, &mut tup).await;
        heap_freetuple(tup);
    }

    // STAGED (rules.md s4): the system attributes (ctid/xmin/..., negative attnums)
    // and per-column type/collation dependencies. M2 reads only user columns back.
    relation_close(pg_attribute);
}

/// `AddNewRelationType`: create the table's composite rowtype in pg_type.
async fn add_new_relation_type(
    shared: &Arc<SharedState>,
    type_name: &str,
    type_namespace: Oid,
    new_rel_oid: Oid,
    new_rel_kind: i8,
    ownerid: Oid,
    new_row_type: Oid,
    new_array_type: Oid,
) -> ObjectAddress {
    type_create(
        shared,
        new_row_type,
        type_name,
        type_namespace,
        new_rel_oid,
        new_rel_kind,
        ownerid,
        -1, // internalSize: varlena
        TYPTYPE_COMPOSITE,
        TYPCATEGORY_COMPOSITE,
        false, // typePreferred
        DEFAULT_TYPDELIM,
        crate::utils::fmgroids::F_RECORD_IN,
        crate::utils::fmgroids::F_RECORD_OUT,
        crate::utils::fmgroids::F_RECORD_RECV,
        crate::utils::fmgroids::F_RECORD_SEND,
        InvalidOid, // typmodin
        InvalidOid, // typmodout
        InvalidOid, // analyze
        InvalidOid, // subscript
        InvalidOid, // element type
        false,      // is array
        new_array_type,
        InvalidOid, // base type
        None,
        None,
        false, // passed by value
        TYPALIGN_DOUBLE,
        TYPSTORAGE_EXTENDED,
        -1, // typmod
        0,  // ndims
        false,
        InvalidOid, // collation
    )
    .await
}

/// `heap_create_with_catalog`: create a relation with its full catalog presence
/// (pg_class, pg_attribute, pg_type rowtype) + storage. Returns the new relation's
/// OID. The M2 path covers a plain heap table; toast/array-type/defaults/system-
/// attribute paths are staged (rules.md s4).
pub async fn heap_create_with_catalog(
    shared: &Arc<SharedState>,
    relname: &str,
    relnamespace: Oid,
    reltablespace: Oid,
    relid: Oid,
    reltypeid: Oid,
    ownerid: Oid,
    accessmtd: Oid,
    tupdesc: TupleDesc,
    relkind: i8,
    relpersistence: i8,
    shared_relation: bool,
) -> Oid {
    let pg_class_desc = relation_id_get_relation(RelationRelationId)
        .unwrap_or_else(|| unreachable!("pg_class is nailed/open"));

    // Assign the relation OID (== relfilenumber) collision-free.
    let relid = if relid.is_valid() {
        relid
    } else {
        get_new_rel_file_number(shared, reltablespace, Some(Arc::clone(&pg_class_desc)), relpersistence).await
    };
    let relfilenumber: RelFileNumber = relid; // bootstrap/M2: filenode == oid

    // 1. Storage-level create.
    let mut new_rel_desc = heap_create(
        shared,
        relname,
        relnamespace,
        reltablespace,
        relid,
        relfilenumber,
        accessmtd,
        tupdesc.clone(),
        relkind,
        relpersistence,
        shared_relation,
        true,
    )
    .await;

    // 2. The composite rowtype (skipped for sequences/toast/indexes).
    let new_type_oid = if relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX
    {
        InvalidOid
    } else {
        // STAGED (rules.md s4): the array type (makeArrayTypeName + a 2nd TypeCreate).
        // Not on the M2 name-resolution path; the rowtype alone is created.
        let new_array_oid = InvalidOid;
        // Assign a fresh rowtype OID if none predetermined (PG: TypeCreate would
        // call GetNewOidWithIndex; the M2 type_create takes the OID directly).
        let row_type_oid = if reltypeid.is_valid() {
            reltypeid
        } else {
            get_new_rel_file_number(shared, reltablespace, Some(Arc::clone(&pg_class_desc)), relpersistence).await
        };
        let addr = add_new_relation_type(
            shared,
            relname,
            relnamespace,
            relid,
            relkind,
            ownerid,
            row_type_oid,
            new_array_oid,
        )
        .await;
        addr.objectId
    };

    // 3. pg_class row. The new relation is unshared here (just built), so mutate
    // its rd_rel in place via Arc::get_mut.
    let new_rel_mut = Arc::get_mut(&mut new_rel_desc)
        .unwrap_or_else(|| unreachable!("freshly built relation is unshared"));
    add_new_relation_tuple(
        shared,
        &pg_class_desc,
        new_rel_mut,
        relid,
        new_type_oid,
        ownerid,
        relkind,
    )
    .await;

    // 4. pg_attribute rows.
    add_new_attribute_tuples(shared, relid, &tupdesc, relkind).await;

    // STAGED (rules.md s4): dependency recording (recordDependencyOnOwner / on the
    // namespace + AM + column types), StoreConstraints (no constraints on the M2
    // plain table), on-commit actions. The plain-table create is complete without
    // them for the M2 read path.
    let _ = RELKIND_COMPOSITE_TYPE; // (used by the staged dependency branch)

    relation_close(pg_class_desc);
    free_local_relation(new_rel_desc);
    relid
}

/// A zeroed pg_class form.
fn blank_pg_class() -> FormData_pg_class {
    // SAFETY: FormData_pg_class is repr(C) POD (Oid/int/bool/NameData/varlena
    // arrays); an all-zero bit pattern is a valid (empty) instance, then filled.
    unsafe { core::mem::zeroed() }
}

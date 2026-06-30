//! pg_constraint catalog manipulation. Translated from the M10/step-39 parts of
//! `src/backend/catalog/pg_constraint.c` (disposition: grow).
//!
//! `create_constraint_entry` forms + inserts a pg_constraint row. M10/step-39 reach
//! the CHECK constraint (the deparsed expression in `conbin`); the FK/UNIQUE/
//! EXCLUSION column-array fields STAGE (left NULL). Now that pg_constraint is seeded
//! on-disk (bootstrap), the row persists and is queryable.
//!
//! Async coloring (rules.md s5): the catalog insert reaches the buffer pool, so the
//! entry is `async` and threads `&Arc<SharedState>`.

#![allow(
    clippy::similar_names,
    reason = "conkey/confkey + conkey_buf/confkey_buf are the PG-canonical FK column names"
)]

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_constraint::{self as pc, ConstraintRelationId};
use crate::postgres::{BoolGetDatum, CharGetDatum, Datum, Int16GetDatum, NameGetDatum, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// The CHECK constraint type code (`pg_constraint.contype` 'c').
const CONSTRAINT_CHECK: i8 = b'c' as i8;

/// PG `CreateConstraintEntry` (M10 CHECK subset): form + insert a pg_constraint row
/// and return its `ObjectAddress`. `conname` is the constraint name, `conrelid` the
/// owning relation, `consrc` the deparsed CHECK expression (stored in `conbin`). The
/// FK/UNIQUE column arrays and the index linkage STAGE (left NULL/Invalid).
pub async fn create_constraint_entry(
    shared: &Arc<SharedState>,
    conname: &str,
    connamespace: Oid,
    conrelid: Oid,
    consrc: &str,
) -> ObjectAddress {
    let Some(pg_constraint) = relation_id_get_relation(ConstraintRelationId) else {
        // pg_constraint must be seeded on-disk (bootstrap nails it); if absent this
        // milestone has nowhere to store the row.
        return ObjectAddress { classId: ConstraintRelationId, objectId: InvalidOid, objectSubId: 0 };
    };
    let desc = pg_constraint.rd_att.clone().unwrap_or_else(|| unreachable!("pg_constraint desc"));
    let natts = desc.natts as usize;

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let conname_data = name_data(conname);
    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![true; natts]; // most columns NULL; set the ones we fill
    let set = |v: &mut [Datum], n: &mut [bool], anum: i32, d: Datum| {
        v[(anum - 1) as usize] = d;
        n[(anum - 1) as usize] = false;
    };
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_oid, ObjectIdGetDatum(new_oid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conname, NameGetDatum(&conname_data));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_connamespace, ObjectIdGetDatum(connamespace));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_contype, CharGetDatum(CONSTRAINT_CHECK));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_condeferrable, BoolGetDatum(false));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_condeferred, BoolGetDatum(false));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_convalidated, BoolGetDatum(true));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conrelid, ObjectIdGetDatum(conrelid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_contypid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conindid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conparentid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confrelid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confupdtype, CharGetDatum(b' ' as i8));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confdeltype, CharGetDatum(b' ' as i8));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confmatchtype, CharGetDatum(b' ' as i8));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conislocal, BoolGetDatum(true));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_coninhcount, Int16GetDatum(0));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_connoinherit, BoolGetDatum(false));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conenforced, BoolGetDatum(true));
    // conbin: the deparsed check expression (M10: text).
    set(
        &mut values,
        &mut isnull,
        pc::Anum_pg_constraint_conbin,
        crate::postgres::PointerGetDatum(
            crate::backend::utils::adt::varlena::cstring_to_text(consrc).cast::<u8>(),
        ),
    );

    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_constraint, &mut tup).await;
    heap_freetuple(tup);
    relation_close(pg_constraint);

    ObjectAddress { classId: ConstraintRelationId, objectId: new_oid, objectSubId: 0 }
}

/// The FOREIGN KEY constraint type code (`pg_constraint.contype` 'f').
const CONSTRAINT_FOREIGN: i8 = b'f' as i8;

/// FK fields for [`create_fk_constraint_entry`].
pub struct FkConstraintFields<'a> {
    pub conname: &'a str,
    pub conrelid: Oid,
    pub confrelid: Oid,
    pub conkey: &'a [i16],
    pub confkey: &'a [i16],
    pub confupdtype: i8,
    pub confdeltype: i8,
    pub confmatchtype: i8,
}

/// PG `CreateConstraintEntry` (FK form, step 41): form + insert a pg_constraint
/// row for a FOREIGN KEY (contype 'f'). The conkey/confkey column arrays are stored
/// in the compact i16-vector varlena that `ri_triggers::read_i16_vector` decodes
/// (the full int2[] array machinery stages). Returns the new constraint's OID.
pub async fn create_fk_constraint_entry(
    shared: &Arc<SharedState>,
    connamespace: Oid,
    f: &FkConstraintFields<'_>,
) -> Oid {
    use crate::backend::utils::adt::ri_triggers::encode_i16_vector;
    let Some(pg_constraint) = relation_id_get_relation(ConstraintRelationId) else {
        return InvalidOid;
    };
    let desc = pg_constraint.rd_att.clone().unwrap_or_else(|| unreachable!("pg_constraint desc"));
    let natts = desc.natts as usize;

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let conname_data = name_data(f.conname);
    let conkey_buf = encode_i16_vector(f.conkey);
    let confkey_buf = encode_i16_vector(f.confkey);

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![true; natts];
    let set = |v: &mut [Datum], n: &mut [bool], anum: i32, d: Datum| {
        v[(anum - 1) as usize] = d;
        n[(anum - 1) as usize] = false;
    };
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_oid, ObjectIdGetDatum(new_oid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conname, NameGetDatum(&conname_data));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_connamespace, ObjectIdGetDatum(connamespace));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_contype, CharGetDatum(CONSTRAINT_FOREIGN));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_condeferrable, BoolGetDatum(false));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_condeferred, BoolGetDatum(false));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_convalidated, BoolGetDatum(true));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conrelid, ObjectIdGetDatum(f.conrelid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_contypid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conindid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conparentid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confrelid, ObjectIdGetDatum(f.confrelid));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confupdtype, CharGetDatum(f.confupdtype));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confdeltype, CharGetDatum(f.confdeltype));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_confmatchtype, CharGetDatum(f.confmatchtype));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conislocal, BoolGetDatum(true));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_coninhcount, Int16GetDatum(0));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_connoinherit, BoolGetDatum(false));
    set(&mut values, &mut isnull, pc::Anum_pg_constraint_conenforced, BoolGetDatum(true));
    set(
        &mut values,
        &mut isnull,
        pc::Anum_pg_constraint_conkey,
        crate::postgres::PointerGetDatum(conkey_buf.as_ptr()),
    );
    set(
        &mut values,
        &mut isnull,
        pc::Anum_pg_constraint_confkey,
        crate::postgres::PointerGetDatum(confkey_buf.as_ptr()),
    );

    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_constraint, &mut tup).await;
    heap_freetuple(tup);
    relation_close(pg_constraint);
    drop(conkey_buf);
    drop(confkey_buf);

    new_oid
}

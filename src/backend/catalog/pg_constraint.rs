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

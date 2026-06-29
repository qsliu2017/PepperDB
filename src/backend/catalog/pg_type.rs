//! pg_type catalog manipulation. Translated from the M2-reachable parts of
//! `src/backend/catalog/pg_type.c`.
//!
//! `TypeCreate` writes a new type's row into pg_type. For a table's composite
//! rowtype (the M2 path) it builds the full pg_type Datum array (typtype = 'c',
//! typrelid = the table OID, the record_in/out I/O procs, varlena layout) and
//! inserts it via `CatalogTupleInsert`.
//!
//! Async coloring (rules.md s5): `CatalogTupleInsert` reaches the buffer pool, so
//! `TypeCreate` is `async` and threads `&Arc<SharedState>`.

#![allow(
    clippy::too_many_arguments,
    clippy::fn_params_excessive_bools,
    reason = "TypeCreate mirrors the C signature 1:1 (port-inherent)"
)]

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::c::NameData;
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_type::{self as t, TypeRelationId};
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Datum, Int16GetDatum, Int32GetDatum, NameGetDatum,
    ObjectIdGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// Copy `src` into a `NameData`, NUL-padded to NAMEDATALEN (C namestrcpy).
fn name_data(src: &str) -> NameData {
    let mut nd = NameData { data: [0u8; crate::c::NAMEDATALEN] };
    let bytes = src.as_bytes();
    let n = bytes.len().min(crate::c::NAMEDATALEN - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// `TypeCreate`: register a new type (or fill in a shell type) in pg_type.
///
/// M2 path: a fresh type with a caller-supplied OID (the rowtype/array type from
/// `heap_create_with_catalog`). Builds the full Datum array for every pg_type
/// column, forms the tuple, and inserts it via `CatalogTupleInsert`. Returns the
/// type's `ObjectAddress`.
///
/// STAGED (rules.md s4): the "existing shell type -> heap_modify_tuple +
/// CatalogTupleUpdate" branch (needs the heap update AM) and
/// `GenerateTypeDependencies` (pg_depend deep paths) are deferred; a fresh rowtype
/// has no shell and records its dependencies via `heap_create_with_catalog`.
pub async fn type_create(
    shared: &Arc<SharedState>,
    new_type_oid: Oid,
    type_name: &str,
    type_namespace: Oid,
    relation_oid: Oid,
    _relation_kind: i8,
    owner_id: Oid,
    internal_size: i16,
    type_type: i8,
    type_category: i8,
    type_preferred: bool,
    typ_delim: i8,
    input_procedure: Oid,
    output_procedure: Oid,
    receive_procedure: Oid,
    send_procedure: Oid,
    typmodin_procedure: Oid,
    typmodout_procedure: Oid,
    analyze_procedure: Oid,
    subscript_procedure: Oid,
    element_type: Oid,
    _is_implicit_array: bool,
    array_type: Oid,
    base_type: Oid,
    default_type_value: Option<&str>,
    default_type_bin: Option<&str>,
    passed_by_value: bool,
    alignment: i8,
    storage: i8,
    type_mod: i32,
    typ_ndims: i32,
    type_not_null: bool,
    type_collation: Oid,
) -> ObjectAddress {
    assert!(new_type_oid.0 != 0, "M2 TypeCreate requires a predetermined OID");

    let pg_type = relation_id_get_relation(TypeRelationId)
        .unwrap_or_else(|| unreachable!("pg_type is nailed/open"));
    let desc = pg_type.rd_att.clone()
        .unwrap_or_else(|| unreachable!("pg_type has a descriptor"));
    let natts = desc.natts as usize;

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;

    let name = name_data(type_name);
    set(&mut values, t::Anum_pg_type_oid, ObjectIdGetDatum(new_type_oid));
    set(&mut values, t::Anum_pg_type_typname, NameGetDatum(&name));
    set(&mut values, t::Anum_pg_type_typnamespace, ObjectIdGetDatum(type_namespace));
    set(&mut values, t::Anum_pg_type_typowner, ObjectIdGetDatum(owner_id));
    set(&mut values, t::Anum_pg_type_typlen, Int16GetDatum(internal_size));
    set(&mut values, t::Anum_pg_type_typbyval, BoolGetDatum(passed_by_value));
    set(&mut values, t::Anum_pg_type_typtype, CharGetDatum(type_type));
    set(&mut values, t::Anum_pg_type_typcategory, CharGetDatum(type_category));
    set(&mut values, t::Anum_pg_type_typispreferred, BoolGetDatum(type_preferred));
    set(&mut values, t::Anum_pg_type_typisdefined, BoolGetDatum(true));
    set(&mut values, t::Anum_pg_type_typdelim, CharGetDatum(typ_delim));
    set(&mut values, t::Anum_pg_type_typrelid, ObjectIdGetDatum(relation_oid));
    set(&mut values, t::Anum_pg_type_typsubscript, ObjectIdGetDatum(subscript_procedure));
    set(&mut values, t::Anum_pg_type_typelem, ObjectIdGetDatum(element_type));
    set(&mut values, t::Anum_pg_type_typarray, ObjectIdGetDatum(array_type));
    set(&mut values, t::Anum_pg_type_typinput, ObjectIdGetDatum(input_procedure));
    set(&mut values, t::Anum_pg_type_typoutput, ObjectIdGetDatum(output_procedure));
    set(&mut values, t::Anum_pg_type_typreceive, ObjectIdGetDatum(receive_procedure));
    set(&mut values, t::Anum_pg_type_typsend, ObjectIdGetDatum(send_procedure));
    set(&mut values, t::Anum_pg_type_typmodin, ObjectIdGetDatum(typmodin_procedure));
    set(&mut values, t::Anum_pg_type_typmodout, ObjectIdGetDatum(typmodout_procedure));
    set(&mut values, t::Anum_pg_type_typanalyze, ObjectIdGetDatum(analyze_procedure));
    set(&mut values, t::Anum_pg_type_typalign, CharGetDatum(alignment));
    set(&mut values, t::Anum_pg_type_typstorage, CharGetDatum(storage));
    set(&mut values, t::Anum_pg_type_typnotnull, BoolGetDatum(type_not_null));
    set(&mut values, t::Anum_pg_type_typbasetype, ObjectIdGetDatum(base_type));
    set(&mut values, t::Anum_pg_type_typtypmod, Int32GetDatum(type_mod));
    set(&mut values, t::Anum_pg_type_typndims, Int32GetDatum(typ_ndims));
    set(&mut values, t::Anum_pg_type_typcollation, ObjectIdGetDatum(type_collation));

    // Trailing varlena columns. M2 rowtypes have no default; cooking a default
    // (CStringGetTextDatum(default_type_bin/value)) lands with the expr machinery.
    let _ = (default_type_value, default_type_bin);
    isnull[(t::Anum_pg_type_typdefaultbin - 1) as usize] = true;
    isnull[(t::Anum_pg_type_typdefault - 1) as usize] = true;
    isnull[(t::Anum_pg_type_typacl - 1) as usize] = true;

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_type, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_type);

    ObjectAddress { classId: TypeRelationId, objectId: new_type_oid, objectSubId: 0 }
}

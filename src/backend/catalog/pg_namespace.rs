//! pg_namespace catalog manipulation. Translated from
//! `src/backend/catalog/pg_namespace.c`.
//!
//! Namespace LOOKUP (search-path resolution) is in `namespace.rs`; the built-in
//! namespaces (pg_catalog/public/pg_toast) are seeded by `bootstrap_catalogs`.
//! `NamespaceCreate` inserts a new row at runtime (CREATE SCHEMA and the
//! per-backend pg_temp_N namespaces).
//!
//! Async coloring (rules.md s5): the pg_namespace insert reaches the buffer pool,
//! so it is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_namespace::{self as ns, NamespaceRelationId};
use crate::postgres::{Datum, NameGetDatum, ObjectIdGetDatum};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// PG `NamespaceCreate`: insert a new pg_namespace row and return its OID. The
/// nspacl is NULL (default permissions); dependency/ownership recording and the
/// post-create event-trigger hook are staged (rules.md s4). `is_temp` suppresses
/// them in PG; both paths reduce to the plain insert here.
pub async fn namespace_create(
    shared: &Arc<SharedState>,
    nsp_name: &str,
    owner_id: Oid,
    is_temp: bool,
) -> Oid {
    let _ = is_temp;
    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let pg_namespace = relation_id_get_relation(NamespaceRelationId)
        .unwrap_or_else(|| unreachable!("pg_namespace is nailed"));
    let desc = pg_namespace.rd_att.clone().unwrap_or_else(|| unreachable!("pg_namespace desc"));
    let natts = desc.natts as usize;

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let nd = name_data(nsp_name);
    values[(ns::Anum_pg_namespace_oid - 1) as usize] = ObjectIdGetDatum(new_oid);
    values[(ns::Anum_pg_namespace_nspname - 1) as usize] = NameGetDatum(&nd);
    values[(ns::Anum_pg_namespace_nspowner - 1) as usize] = ObjectIdGetDatum(owner_id);
    isnull[(ns::Anum_pg_namespace_nspacl - 1) as usize] = true;

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_namespace, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_namespace);
    new_oid
}

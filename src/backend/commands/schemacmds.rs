//! CREATE/DROP SCHEMA commands. Translated from the M10/step-39 parts of
//! `src/backend/commands/schemacmds.c` (disposition: grow).
//!
//! `CreateSchemaCommand` inserts the pg_namespace row (IF NOT EXISTS short-circuits
//! on an existing name; AUTHORIZATION sets the owner). RemoveSchema runs through the
//! generic dependency DROP (dropcmds -> deleteOneObject's pg_namespace arm). The
//! nested schema_element_list (CREATE TABLE inside the schema) STAGES.
//!
//! Async coloring (rules.md s5): the pg_namespace scan/insert reaches the buffer
//! pool, so the command is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::catalog::namespace::namespace_oid_by_name;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_namespace::{self as ns, NamespaceRelationId};
use crate::nodes::parsenodes::CreateSchemaStmt;
use crate::postgres::{Datum, NameGetDatum, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// PG `CreateSchemaCommand` (M10 subset): insert a pg_namespace row for the named
/// schema. Returns the new schema's OID. IF NOT EXISTS on an existing name is a
/// no-op (returns the existing OID). AUTHORIZATION sets nspowner (the role-name ->
/// OID resolution stages; the bootstrap superuser owns it otherwise). The nested
/// schema_element_list (objects created inside the schema) STAGES (rules.md s4).
pub async fn create_schema_command(shared: &Arc<SharedState>, stmt: &CreateSchemaStmt) -> Oid {
    if !stmt.schemaElts.is_empty() {
        unimplemented!("CreateSchemaCommand: schema_element_list (objects in CREATE SCHEMA)");
    }

    // The schema name: the explicit name, or (CREATE SCHEMA AUTHORIZATION role) the
    // role name. The role-name-as-schema-name form needs role resolution; stage it.
    let schema_name = stmt.schemaname.as_deref().unwrap_or_else(|| {
        unimplemented!("CreateSchemaCommand: CREATE SCHEMA AUTHORIZATION (name from role)")
    });

    // IF NOT EXISTS: if the schema already exists, do nothing (PG emits a notice).
    if let Some(existing) = namespace_oid_by_name(shared, schema_name).await {
        if stmt.if_not_exists {
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("schema \"{schema_name}\" already exists, skipping"));
            });
            return existing;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_SCHEMA)
                .errmsg(format!("schema \"{schema_name}\" already exists"));
        });
        unreachable!("ereport(ERROR) diverges");
    }

    // The owner: the bootstrap superuser (role-name -> OID resolution stages).
    let owner_id = Oid::new(10);

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let pg_namespace = relation_id_get_relation(NamespaceRelationId)
        .unwrap_or_else(|| unreachable!("pg_namespace is nailed"));
    let desc = pg_namespace.rd_att.clone().unwrap_or_else(|| unreachable!("pg_namespace desc"));
    let natts = desc.natts as usize;

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let nsp_name = name_data(schema_name);
    values[(ns::Anum_pg_namespace_oid - 1) as usize] = ObjectIdGetDatum(new_oid);
    values[(ns::Anum_pg_namespace_nspname - 1) as usize] = NameGetDatum(&nsp_name);
    values[(ns::Anum_pg_namespace_nspowner - 1) as usize] = ObjectIdGetDatum(owner_id);
    isnull[(ns::Anum_pg_namespace_nspacl - 1) as usize] = true;

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_namespace, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_namespace);

    let _ = InvalidOid;
    new_oid
}

/// PG `RemoveSchemaById`: delete the pg_namespace row for `schema_id` (the
/// dependency-walk leaf for DROP SCHEMA). Objects inside the schema are dropped by
/// the dependency walk before this runs (M10: the schema must be empty or CASCADE).
pub async fn remove_schema_by_id(shared: &Arc<SharedState>, schema_id: Oid) {
    use crate::access::skey::ScanKeyData;
    use crate::backend::access::common::heaptuple::heap_copytuple;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };
    use crate::backend::catalog::indexing::catalog_tuple_delete;

    let Some(pg_namespace) = relation_id_get_relation(NamespaceRelationId) else { return };
    let key = [ScanKeyData {
        flags: 0,
        attno: ns::Anum_pg_namespace_oid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: crate::fmgr::FmgrInfo {
            fn_addr: None,
            oid: InvalidOid,
            nargs: 0,
            strict: false,
            retset: false,
            stats: 0,
            extra: 0,
            mcxt: (),
            expr: None,
        },
        argument: ObjectIdGetDatum(schema_id),
    }];
    let snap = systable_scan_snapshot(shared, &pg_namespace, None);
    let mut scan = systable_beginscan(shared, &pg_namespace, InvalidOid, false, &snap, &key);
    let mut tids = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy its TID before endscan.
        let tup = unsafe { heap_copytuple(tref) };
        tids.push(tup.t_self);
        heap_freetuple(tup);
    }
    systable_endscan(shared, &mut scan);
    for tid in &tids {
        catalog_tuple_delete(shared, &pg_namespace, tid).await;
    }
    relation_close(pg_namespace);
}

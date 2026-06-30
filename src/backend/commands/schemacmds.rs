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
    // role name. The role-name-as-schema-name form needs role resolution; stage it
    // with a catchable error (rules.md s4 -- only PUBLIC/bootstrap roles exist yet).
    let schema_name = stmt.schemaname.as_deref().unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("CREATE SCHEMA AUTHORIZATION is not yet supported");
        });
        unreachable!("ereport(ERROR) diverges");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::Node;

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        use crate::shared_state::SharedStateConfig;
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-schemacmds-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::catalog::indexing::scope_async as catalog_index_scope;
        use crate::backend::utils::cache::catcache::scope_async as catcache_scope;
        use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
        use crate::backend::utils::cache::typcache::scope_async as typcache_scope;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        sess.set_database_id(DB_OID);
        sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        sess.set_authenticated_user_id(crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID);
        sess.set_current_user_id(crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID);
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

        let body = Box::pin(typcache_scope(Box::pin(f(shared))));
        let body = Box::pin(catalog_index_scope(Box::pin(relcache_scope(body))));
        let body = Box::pin(catcache_scope(body));
        let body = Box::pin(with_insertion(body));
        let body = Box::pin(combocid_scope(body));
        let body = Box::pin(snapmgr_scope(body));
        let body = Box::pin(crate::backend::access::transam::xact::xact_scope(body));
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(owner, body),
        )
        .await
    }

    async fn init_db(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
        use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};

        StartTransactionCommand(shared).await;
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
        crate::backend::bootstrap::bootstrap::bootstrap_catalogs(shared).await;
        bump(shared);
    }

    fn bump(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::{CommandCounterIncrement, GetCurrentCommandId};
        use crate::backend::utils::time::snapmgr::{
            GetTransactionSnapshot, InvalidateCatalogSnapshot, PopActiveSnapshot, PushActiveSnapshot,
        };
        CommandCounterIncrement();
        InvalidateCatalogSnapshot();
        PopActiveSnapshot();
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
    }

    fn parse_schema(sql: &str) -> CreateSchemaStmt {
        let mut list = crate::backend::parser::parser::raw_parser(sql, crate::parser::parser::RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not RawStmt") };
        let Node::CreateSchemaStmt(s) = rs.stmt.unwrap() else { panic!("not CreateSchemaStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_schema_named_forms_succeed() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_schema("CREATE SCHEMA s");
            let oid = create_schema_command(&shared, &stmt).await;
            assert!(oid.is_valid(), "CREATE SCHEMA s yields a valid OID");
            bump(&shared);
            assert!(namespace_oid_by_name(&shared, "s").await.is_some(), "s exists");

            // name present + AUTHORIZATION role still works (name not derived from role).
            let stmt = parse_schema("CREATE SCHEMA s2 AUTHORIZATION r");
            let oid = create_schema_command(&shared, &stmt).await;
            assert!(oid.is_valid(), "CREATE SCHEMA s2 AUTHORIZATION r yields a valid OID");
            bump(&shared);
            assert!(namespace_oid_by_name(&shared, "s2").await.is_some(), "s2 exists");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_schema_authorization_only_errors_cleanly() {
        use futures_util::FutureExt;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            // CREATE SCHEMA AUTHORIZATION role (no name) -> catchable ERROR, not a panic.
            let stmt = parse_schema("CREATE SCHEMA AUTHORIZATION somerole");
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let res = std::panic::AssertUnwindSafe(create_schema_command(&shared, &stmt))
                .catch_unwind()
                .await;
            std::panic::set_hook(prev);

            let payload = res.expect_err("CREATE SCHEMA AUTHORIZATION role must raise an error");
            let edata = payload
                .downcast_ref::<crate::utils::elog::ErrorData>()
                .expect("structured ErrorData, not a raw panic");
            assert_eq!(
                edata.sqlerrcode,
                crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED
            );
        }))
        .await;
    }
}

//! CREATE/DROP TABLESPACE commands. Translated from
//! `src/backend/commands/tablespace.c` (disposition: grow).
//!
//! `create_tablespace` inserts a pg_tablespace row (spcname/spcowner; spcacl and
//! spcoptions NULL). The physical machinery -- creating the tablespace directory at
//! `LOCATION`, the `pg_tblspc/<oid>` symlink, and the version file -- STAGES; the
//! single-tablespace port keeps every relation in pg_default, so the catalog row is
//! the must-have. pg_default/pg_global exist by default (seeded at bootstrap).
//!
//! Async coloring (rules.md s5): the catalog insert reaches the buffer pool, so the
//! command is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_tablespace::{self as ts, TableSpaceRelationId};
use crate::nodes::parsenodes::CreateTableSpaceStmt;
use crate::postgres::{Datum, NameGetDatum, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// PG `CreateTableSpace`: insert a pg_tablespace row for the named tablespace and
/// return its OID. STAGED (rules.md s4): the `LOCATION` directory creation, the
/// `pg_tblspc/<oid>` symlink, the `PG_VERSION` file, and the per-tablespace storage
/// routing -- the single-tablespace port stores everything in pg_default, so only
/// the catalog row is materialized. A duplicate name is an error.
pub async fn create_tablespace(shared: &Arc<SharedState>, stmt: &CreateTableSpaceStmt) -> Oid {
    let spcname = stmt.tablespacename.as_deref().unwrap_or("");

    if tablespace_oid_by_name(shared, spcname).await.is_some() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("tablespace \"{spcname}\" already exists"));
        });
        unreachable!("ereport(ERROR) diverges");
    }

    let Some(pg_tablespace) = relation_id_get_relation(TableSpaceRelationId) else {
        return InvalidOid;
    };
    let desc = pg_tablespace.rd_att.clone().unwrap_or_else(|| unreachable!("pg_tablespace desc"));
    let natts = desc.natts as usize;

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let name = name_data(spcname);

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    values[(ts::Anum_pg_tablespace_oid - 1) as usize] = ObjectIdGetDatum(new_oid);
    values[(ts::Anum_pg_tablespace_spcname - 1) as usize] = NameGetDatum(&name);
    values[(ts::Anum_pg_tablespace_spcowner - 1) as usize] = ObjectIdGetDatum(Oid::new(10));
    isnull[(ts::Anum_pg_tablespace_spcacl - 1) as usize] = true;
    isnull[(ts::Anum_pg_tablespace_spcoptions - 1) as usize] = true;

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_tablespace, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_tablespace);

    // STAGED: create_tablespace_directories(stmt.location) + the pg_tblspc symlink.
    let _ = &stmt.location;
    new_oid
}

/// PG `RemoveTableSpace` (DropTableSpace): delete the named tablespace's
/// pg_tablespace row. STAGED: the directory-empty check + the symlink/directory
/// removal -- the single-tablespace port only retires the catalog row.
pub async fn drop_tablespace(shared: &Arc<SharedState>, tablespacename: &str, missing_ok: bool) {
    let Some(oid) = tablespace_oid_by_name(shared, tablespacename).await else {
        if missing_ok {
            return;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("tablespace \"{tablespacename}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };
    crate::backend::commands::dbcommands::delete_catalog_row_by_oid(
        shared,
        TableSpaceRelationId,
        ts::Anum_pg_tablespace_oid as i16,
        oid,
    )
    .await;
}

/// Resolve a tablespace OID by `spcname` via a heap scan of pg_tablespace. `None` if
/// absent (the TABLESPACEOID syscache path stages; the scan is the M10 fallback).
pub async fn tablespace_oid_by_name(shared: &Arc<SharedState>, spcname: &str) -> Option<Oid> {
    use crate::backend::access::common::heaptuple::heap_getattr;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };

    let pg_tablespace = relation_id_get_relation(TableSpaceRelationId)?;
    let desc = pg_tablespace.rd_att.clone()?;
    let snap = systable_scan_snapshot(shared, &pg_tablespace, None);
    let mut scan = systable_beginscan(shared, &pg_tablespace, InvalidOid, false, &snap, &[]);
    let mut found = None;
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; spcname is a `name` column, oid by-value.
        let (name_d, name_null) =
            unsafe { heap_getattr(tref, ts::Anum_pg_tablespace_spcname, &desc) };
        if name_null || name_d.0 == 0 {
            continue;
        }
        let nd = unsafe { &*(name_d.0 as *const crate::c::NameData) };
        let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
        if nd.data[..end] != *spcname.as_bytes() {
            continue;
        }
        let (oid_d, oid_null) = unsafe { heap_getattr(tref, ts::Anum_pg_tablespace_oid, &desc) };
        if !oid_null {
            found = Some(crate::postgres::DatumGetObjectId(oid_d));
            break;
        }
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_tablespace);
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::Node;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-tscmds-{}-{}", std::process::id(), n));
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
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        sess.set_database_id(DB_OID);
        sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

        let body = Box::pin(catalog_index_scope(Box::pin(relcache_scope(Box::pin(f(shared))))));
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
        bump_command(shared);
    }

    fn bump_command(shared: &Arc<SharedState>) {
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

    fn parse_create_tablespace(sql: &str) -> CreateTableSpaceStmt {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let Node::CreateTableSpaceStmt(s) = rs.stmt.unwrap() else { panic!("not a CreateTableSpaceStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_tablespaces_present_and_create_drop() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            // pg_default/pg_global are seeded at bootstrap.
            assert!(tablespace_oid_by_name(&shared, "pg_default").await.is_some(), "pg_default seeded");
            assert!(tablespace_oid_by_name(&shared, "pg_global").await.is_some(), "pg_global seeded");

            let stmt = parse_create_tablespace("CREATE TABLESPACE fast LOCATION '/tmp/fast'");
            let oid = create_tablespace(&shared, &stmt).await;
            assert!(oid.is_valid(), "create_tablespace returns a valid oid");
            bump_command(&shared);

            assert_eq!(
                tablespace_oid_by_name(&shared, "fast").await,
                Some(oid),
                "fast tablespace row is queryable after CREATE TABLESPACE"
            );

            drop_tablespace(&shared, "fast", false).await;
            bump_command(&shared);
            assert_eq!(tablespace_oid_by_name(&shared, "fast").await, None, "row gone after drop_tablespace");
        }))
        .await;
    }
}

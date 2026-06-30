//! CREATE/DROP DATABASE commands. Translated from
//! `src/backend/commands/dbcommands.c` (disposition: grow).
//!
//! PepperDB runs a single physical database, so `createdb`/`dropdb` translate the
//! pg_database catalog bookkeeping (a row that is queryable / removable) and STAGE
//! the heavy machinery: the per-database storage directory, template copying, the
//! cross-database connection routing, and the drop's file unlink. `createdb` writes
//! a pg_database row; `dropdb` removes it.
//!
//! Async coloring (rules.md s5): the pg_database scan/insert/delete reaches the
//! buffer pool, so both commands are `async` and thread `&Arc<SharedState>`.

use std::sync::Arc;

use crate::access::skey::ScanKeyData;
use crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER;
use crate::backend::access::common::heaptuple::{
    heap_copytuple, heap_form_tuple, heap_freetuple, heap_getattr,
};
use crate::backend::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::{catalog_tuple_delete, catalog_tuple_insert};
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_database::{self as db, DatabaseRelationId};
use crate::fmgr::FmgrInfo;
use crate::nodes::parsenodes::{CreatedbStmt, DropdbStmt};
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Datum, Int32GetDatum, NameGetDatum, ObjectIdGetDatum,
    TransactionIdGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// PG `createdb`: CREATE DATABASE. Writes a pg_database row for the new database and
/// returns its OID. STAGED (rules.md s4): the physical database directory creation,
/// template-database file copy, and tablespace placement -- the single-database port
/// keeps every database co-resident, so only the catalog row is materialized. The
/// createdb option list (ENCODING/OWNER/TEMPLATE/...) is recorded only where it maps
/// to a fixed column; the rest stage with the option-value plumbing.
pub async fn createdb(shared: &Arc<SharedState>, stmt: &CreatedbStmt) -> Oid {
    let dbname = stmt.dbname.as_deref().unwrap_or("");

    // CREATE DATABASE with an existing name is an error (PG: duplicate_database).
    if database_oid_by_name(shared, dbname).await.is_some() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_DATABASE)
                .errmsg(format!("database \"{dbname}\" already exists"));
        });
        unreachable!("ereport(ERROR) diverges");
    }

    let Some(pg_database) = relation_id_get_relation(DatabaseRelationId) else {
        return InvalidOid;
    };
    let desc = pg_database.rd_att.clone().unwrap_or_else(|| unreachable!("pg_database desc"));
    let natts = desc.natts as usize;

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let dat_name = name_data(dbname);

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    values[(db::Anum_pg_database_oid - 1) as usize] = ObjectIdGetDatum(new_oid);
    values[(db::Anum_pg_database_datname - 1) as usize] = NameGetDatum(&dat_name);
    values[(db::Anum_pg_database_datdba - 1) as usize] = ObjectIdGetDatum(Oid::new(10));
    values[(db::Anum_pg_database_encoding - 1) as usize] =
        Int32GetDatum(crate::mb::pg_wchar::pg_enc::PG_UTF8 as i32);
    values[(db::Anum_pg_database_datlocprovider - 1) as usize] =
        CharGetDatum(crate::catalog::pg_collation::COLLPROVIDER_LIBC);
    values[(db::Anum_pg_database_datistemplate - 1) as usize] = BoolGetDatum(false);
    values[(db::Anum_pg_database_datallowconn - 1) as usize] = BoolGetDatum(true);
    values[(db::Anum_pg_database_dathasloginevt - 1) as usize] = BoolGetDatum(false);
    values[(db::Anum_pg_database_datconnlimit - 1) as usize] =
        Int32GetDatum(db::DATCONNLIMIT_UNLIMITED);
    values[(db::Anum_pg_database_datfrozenxid - 1) as usize] =
        TransactionIdGetDatum(crate::c::TransactionId(0));
    values[(db::Anum_pg_database_datminmxid - 1) as usize] =
        TransactionIdGetDatum(crate::c::TransactionId(1));
    values[(db::Anum_pg_database_dattablespace - 1) as usize] =
        ObjectIdGetDatum(crate::common::relpath::DEFAULTTABLESPACE_OID);
    // The varlena locale columns + datacl are NULL (single-db: no per-db locale).
    isnull[(db::Anum_pg_database_datcollate - 1) as usize] = true;
    isnull[(db::Anum_pg_database_datctype - 1) as usize] = true;
    isnull[(db::Anum_pg_database_datlocale - 1) as usize] = true;
    isnull[(db::Anum_pg_database_daticurules - 1) as usize] = true;
    isnull[(db::Anum_pg_database_datcollversion - 1) as usize] = true;
    isnull[(db::Anum_pg_database_datacl - 1) as usize] = true;

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_database, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_database);

    // STAGED: createmm_smgr / CreateDirAndVersionFile / copy template DB files.
    if !stmt.options.is_empty() {
        // Option semantics (ENCODING/LOCALE/TEMPLATE/...) stage with the option-value
        // plumbing; the row above uses the server defaults regardless.
    }
    new_oid
}

/// PG `dropdb`: DROP DATABASE. Removes the named database's pg_database row. STAGED
/// (rules.md s4): killing other backends connected to the target, the physical
/// directory unlink (`remove_dbtablespaces`), and the shared-buffer drop -- the
/// single-database port only retires the catalog row.
pub async fn dropdb(shared: &Arc<SharedState>, stmt: &DropdbStmt) {
    let dbname = stmt.dbname.as_deref().unwrap_or("");

    let Some(dboid) = database_oid_by_name(shared, dbname).await else {
        if stmt.missing_ok {
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("database \"{dbname}\" does not exist, skipping"));
            });
            return;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{dbname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };

    delete_catalog_row_by_oid(shared, DatabaseRelationId, db::Anum_pg_database_oid as i16, dboid)
        .await;
}

/// Resolve a database OID by `datname` via a heap scan of pg_database (the DATNAME
/// syscache path stages; the scan is the M10 fallback). `None` if absent.
pub async fn database_oid_by_name(shared: &Arc<SharedState>, dbname: &str) -> Option<Oid> {
    let pg_database = relation_id_get_relation(DatabaseRelationId)?;
    let desc = pg_database.rd_att.clone()?;
    let snap = systable_scan_snapshot(shared, &pg_database, None);
    let mut scan = systable_beginscan(shared, &pg_database, InvalidOid, false, &snap, &[]);
    let mut found = None;
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; datname is a `name` column, oid a by-value column.
        let (name_d, name_null) =
            unsafe { heap_getattr(tref, db::Anum_pg_database_datname, &desc) };
        if name_null || name_d.0 == 0 {
            continue;
        }
        let nd = unsafe { &*(name_d.0 as *const crate::c::NameData) };
        let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
        if nd.data[..end] != *dbname.as_bytes() {
            continue;
        }
        let (oid_d, oid_null) = unsafe { heap_getattr(tref, db::Anum_pg_database_oid, &desc) };
        if !oid_null {
            found = Some(crate::postgres::DatumGetObjectId(oid_d));
            break;
        }
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_database);
    found
}

/// Delete the row of `relid` whose `oid_attno` column equals `target_oid` (the
/// catalog-row retire shared by the minor object-DDL DROP paths). Mirrors PG's
/// `RemoveXById` scan: find the tuple by OID, then `catalog_tuple_delete` its TID.
pub async fn delete_catalog_row_by_oid(
    shared: &Arc<SharedState>,
    relid: Oid,
    oid_attno: i16,
    target_oid: Oid,
) {
    let Some(rel) = relation_id_get_relation(relid) else { return };
    let key = [ScanKeyData {
        flags: 0,
        attno: oid_attno,
        strategy: BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: FmgrInfo {
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
        argument: ObjectIdGetDatum(target_oid),
    }];
    let snap = systable_scan_snapshot(shared, &rel, None);
    let mut scan = systable_beginscan(shared, &rel, InvalidOid, false, &snap, &key);
    let mut tids = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        let tup = unsafe { heap_copytuple(tref) };
        tids.push(tup.t_self);
        heap_freetuple(tup);
    }
    systable_endscan(shared, &mut scan);
    for tid in &tids {
        catalog_tuple_delete(shared, &rel, tid).await;
    }
    relation_close(rel);
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
        let dir = std::env::temp_dir().join(format!("pepperdb-dbcmds-{}-{}", std::process::id(), n));
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

    fn parse_one(sql: &str) -> Node {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        rs.stmt.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_then_drop_database() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            // The bootstrap rows exist.
            assert!(database_oid_by_name(&shared, "postgres").await.is_some(), "postgres seeded");
            assert!(database_oid_by_name(&shared, "template1").await.is_some(), "template1 seeded");

            let Node::CreatedbStmt(s) = parse_one("CREATE DATABASE appdb") else { panic!("not CreatedbStmt") };
            let oid = createdb(&shared, &s).await;
            assert!(oid.is_valid(), "createdb returns a valid oid");
            bump_command(&shared);

            assert_eq!(
                database_oid_by_name(&shared, "appdb").await,
                Some(oid),
                "appdb row is queryable after CREATE DATABASE"
            );

            let Node::DropdbStmt(d) = parse_one("DROP DATABASE appdb") else { panic!("not DropdbStmt") };
            dropdb(&shared, &d).await;
            bump_command(&shared);

            assert_eq!(
                database_oid_by_name(&shared, "appdb").await,
                None,
                "appdb row is gone after DROP DATABASE"
            );
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_missing_database_if_exists_is_noop() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            let Node::DropdbStmt(d) = parse_one("DROP DATABASE IF EXISTS nope") else { panic!("not DropdbStmt") };
            dropdb(&shared, &d).await; // must not panic
            bump_command(&shared);
            assert_eq!(database_oid_by_name(&shared, "nope").await, None);
        }))
        .await;
    }
}

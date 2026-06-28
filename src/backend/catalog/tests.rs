//! Integration tests for the M2 catalog-manipulation + initdb path.
//!
//! Each test stands up a real foundation `SharedState` over a tempdir, the full
//! per-task scope stack (session / resowner / xact / snapmgr / combocid / relcache
//! / catalog-index registry / WAL insertion), runs `bootstrap_catalogs` (initdb),
//! then exercises the read paths against the REAL on-disk catalogs:
//!  - SearchSysCache1(TYPEOID, INT4OID) returns the seeded pg_type row,
//!  - a type lookup by NAME ("int4") resolves to INT4OID via the search path,
//!  - heap_create_with_catalog creates a user table whose pg_class row is then
//!    resolvable by name (RangeVarGetRelid), and whose heap storage exists,
//!  - a catalog index scan (systable index path) finds a seeded pg_type row.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
#![allow(clippy::future_not_send, reason = "test bodies; not spawned on the runtime")]

use std::sync::Arc;

use crate::postgres::{DatumGetObjectId, ObjectIdGetDatum};
use crate::postgres_ext::Oid;
use crate::shared_state::{SharedState, SharedStateConfig};

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

const INT4OID: Oid = Oid(23);
const DB_OID: Oid = Oid(90000);

fn new_shared() -> Arc<SharedState> {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-catalog-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 256,
        ..Default::default()
    })
}

/// Set up the full per-task scope stack (session / resowner / xact / snapmgr /
/// combocid / WAL + relcache + catalog-index registry) and run the async body. The
/// session's database id is 90000 so the nailed catalogs' relfilenode locator
/// matches the heap files initdb creates.
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

/// Run initdb: start a transaction, push an active snapshot (the index build reads
/// it), and seed the catalogs.
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
    // A command boundary so the seeded rows are visible to later snapshots, and a
    // catalog-snapshot invalidation so a fresh one reflects the seeded rows (the
    // cached one was built by bootstrap's internal index scans, before seeding).
    crate::backend::access::transam::xact::CommandCounterIncrement();
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    refresh_active_snapshot(shared);
}

/// Re-take the active snapshot with curcid bumped so own-xact rows inserted by the
/// previous command are visible.
fn refresh_active_snapshot(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::GetCurrentCommandId;
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };
    PopActiveSnapshot();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);
}

#[tokio::test(flavor = "multi_thread")]
async fn initdb_seeds_pg_type_readable_by_searchsyscache() {
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        // SearchSysCache1(TYPEOID, INT4OID) reads the REAL on-disk pg_type row.
        let tup = crate::backend::utils::cache::syscache::search_sys_cache_populate(
            &shared,
            crate::utils::syscache::SysCacheIdentifier::TYPEOID,
            &[ObjectIdGetDatum(INT4OID)],
        )
        .await;
        assert!(tup.is_some(), "SearchSysCache1(TYPEOID, int4) must find the seeded row");

        // The row really describes int4: typoutput resolves to int4out.
        let (typoutput, _varlena) =
            crate::backend::utils::cache::lsyscache::get_type_output_info_populate(&shared, INT4OID)
                .await;
        assert_eq!(typoutput, crate::utils::fmgroids::F_INT4OUT);

        if let Some(t) = tup {
            crate::backend::utils::cache::syscache::release_sys_cache(t);
        }
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn initdb_type_lookup_by_name_resolves_int4() {
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        // Resolve "int4" by name through the search path (pg_catalog + public).
        let oid =
            crate::backend::catalog::namespace::typename_get_typid(&shared, "int4").await;
        assert_eq!(oid, Some(INT4OID), "type name lookup must resolve int4");

        // An unknown type name does not resolve.
        let none =
            crate::backend::catalog::namespace::typename_get_typid(&shared, "no_such_type").await;
        assert_eq!(none, None);
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_writes_catalog_rows_and_resolves_by_name() {
    use crate::access::tupdesc::TupleDescData;
    use crate::backend::catalog::heap::heap_create_with_catalog;
    use crate::backend::catalog::namespace::range_var_get_relid;
    use crate::catalog::pg_class::{RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
    use crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE;

    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        // Build the rowtype t(a int4) using the now-on-disk int4 type metadata.
        let mut td = TupleDescData::create_template(1);
        td.init_builtin_entry(1, "a", INT4OID, -1, 0);
        let tupdesc = Arc::new(td);

        let relid = heap_create_with_catalog(
            &shared,
            "t",
            PG_PUBLIC_NAMESPACE,
            crate::common::relpath::DEFAULTTABLESPACE_OID,
            Oid(0),     // assign a fresh OID
            Oid(0),     // assign a fresh rowtype OID
            Oid(10),    // owner
            Oid(2),     // heap AM
            tupdesc,
            RELKIND_RELATION,
            RELPERSISTENCE_PERMANENT,
            false,
        )
        .await;
        assert!(relid.0 != 0, "heap_create_with_catalog returns the new OID");

        crate::backend::access::transam::xact::CommandCounterIncrement();
        refresh_active_snapshot(&shared);

        // RangeVarGetRelid("t") resolves to the new relation's OID via pg_class.
        let resolved = range_var_get_relid(&shared, None, "t").await;
        assert_eq!(resolved, Some(relid), "the new table resolves by name");

        // The table's heap storage exists on disk.
        let loc = crate::storage::relfilelocator::RelFileLocator {
            spcOid: crate::common::relpath::DEFAULTTABLESPACE_OID,
            dbOid: DB_OID,
            relNumber: relid,
        };
        let mut smgr = crate::storage::smgr::SmgrRelation::open(
            loc,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
        );
        let exists = smgr
            .exists(&shared, crate::common::relpath::ForkNumber::MAIN_FORKNUM)
            .await;
        assert!(exists, "the new table's main fork file exists");

        // Its pg_attribute column row is present: rebuild the descriptor from disk.
        let rebuilt =
            crate::backend::utils::cache::relcache::relation_build_desc(&shared, relid).await;
        assert!(rebuilt.is_some(), "the new relation rebuilds from its on-disk catalog rows");
        if let Some(rd) = rebuilt {
            // SAFETY: live rebuilt relation.
            let natts = unsafe { (*rd).rd_att.as_ref().unwrap().natts };
            assert_eq!(natts, 1, "the rebuilt descriptor has the one user column");
        }
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_index_scan_finds_seeded_pg_type_row() {
    use crate::access::skey::ScanKeyData;
    use crate::backend::access::index::genam::{
        systable_beginscan_indexed, systable_endscan, systable_getnext,
    };
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
    use crate::catalog::pg_type::{Anum_pg_type_oid, TypeRelationId};

    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        // Build an index Relation over pg_type's oid column directly (the relcache
        // does not rebuild a nailed catalog's index from pg_index in M2), then drive
        // the systable INDEX path against it.
        let pg_type = relation_id_get_relation(TypeRelationId).expect("pg_type nailed");
        let index = make_oid_index_relation(&shared, TypeRelationId).await;

        let snap = crate::backend::utils::time::snapmgr::GetActiveSnapshot();
        let key = [ScanKeyData {
            flags: 0,
            attno: Anum_pg_type_oid as i16,
            strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
            subtype: crate::postgres_ext::InvalidOid,
            collation: crate::postgres_ext::InvalidOid,
            func: zero_fmgr_info(),
            argument: ObjectIdGetDatum(INT4OID),
        }];
        let mut scan = systable_beginscan_indexed(&shared, pg_type, index, snap, &key);
        let found = systable_getnext(&shared, &mut scan).await;
        assert!(found.is_some(), "the index scan finds the seeded int4 pg_type row");
        if let Some(t) = found {
            // SAFETY: live scan tuple; read its oid column.
            let desc = unsafe { (*pg_type).rd_att.clone().unwrap() };
            let (oid_d, isnull) =
                unsafe { crate::backend::access::common::heaptuple::heap_getattr(&*t, Anum_pg_type_oid, &desc) };
            assert!(!isnull);
            assert_eq!(DatumGetObjectId(oid_d), INT4OID);
        }
        systable_endscan(&shared, &mut scan);
        relation_close(pg_type);
    }))
    .await;
}

/// Build + populate a btree index Relation over pg_type's `oid` column, returning
/// the open index handle (the M2 systable index path needs an index Relation; the
/// catalog index built by initdb is registered but not relcache-rebuildable yet).
async fn make_oid_index_relation(shared: &Arc<SharedState>, heap_relid: Oid) -> *mut crate::utils::rel::RelationData {
    use crate::backend::catalog::index::{index_build, make_index_info};
    use crate::backend::utils::cache::relcache::{
        index_init_opclass_support, relation_id_get_relation, relation_init_index_access_info,
    };
    use crate::catalog::pg_type::Anum_pg_type_oid;

    let heap = relation_id_get_relation(heap_relid).expect("heap open");
    let info = make_index_info(&[Anum_pg_type_oid as i16], true);

    // A standalone index relation at a fresh locator; storage created, opclass
    // support filled so the btree comparator resolves.
    let iloc = crate::storage::relfilelocator::RelFileLocator {
        spcOid: crate::common::relpath::DEFAULTTABLESPACE_OID,
        dbOid: DB_OID,
        relNumber: Oid(70000),
    };
    let mut smgr = crate::storage::smgr::SmgrRelation::open(
        iloc,
        crate::storage::procnumber::INVALID_PROC_NUMBER,
    );
    smgr.create(shared, crate::common::relpath::ForkNumber::MAIN_FORKNUM, false).await;

    // SAFETY: heap relation has a descriptor; copy its oid column into a 1-col index
    // descriptor.
    let heap_td = unsafe { (*heap).rd_att.clone().unwrap() };
    let mut itd = crate::access::tupdesc::TupleDescData::create_template(1);
    itd.tdtypmod = -1;
    let from = heap_td.attr((Anum_pg_type_oid - 1) as usize);
    {
        let to = &mut itd.attrs[0];
        to.atttypid = from.atttypid;
        to.attlen = from.attlen;
        to.attbyval = from.attbyval;
        to.attalign = from.attalign;
        to.attstorage = from.attstorage;
        to.attnum = 1;
        to.attislocal = true;
        to.attname = crate::backend::catalog::heap::name_data("oid");
    }
    itd.populate_compact_attribute(0);

    let index = build_index_relation(iloc, Arc::new(itd));
    attach_rd_index(index);
    relation_init_index_access_info(index);
    index_init_opclass_support(index, &[Oid(1981)], &[Oid(0)], &[0]);

    index_build(shared, heap, index, &info).await;
    crate::backend::utils::cache::relcache::relation_close(heap);
    index
}

/// Attach a 1-column unique pg_index Form to an index relation (so the btree AM can
/// read its key counts).
fn attach_rd_index(index: *mut crate::utils::rel::RelationData) {
    use crate::catalog::pg_index::FormData_pg_index;
    // SAFETY: FormData_pg_index is repr(C) POD; zero then patch.
    let mut idx: Box<FormData_pg_index> = Box::new(unsafe { core::mem::zeroed() });
    idx.indnatts = 1;
    idx.indnkeyatts = 1;
    idx.indisunique = true;
    idx.indimmediate = true;
    // SAFETY: live index relation.
    unsafe {
        (*index).rd_index = Box::into_raw(idx);
    }
}

/// Build a minimal index `RelationData` (boxed, leaked) backed by `locator`.
fn build_index_relation(
    locator: crate::storage::relfilelocator::RelFileLocator,
    tupdesc: crate::access::tupdesc::TupleDesc,
) -> *mut crate::utils::rel::RelationData {
    use crate::catalog::pg_class::{FormData_pg_class, RELKIND_INDEX, RELPERSISTENCE_PERMANENT};
    use crate::utils::rel::{LockInfoData, LockRelId, RelationData};

    // SAFETY: FormData_pg_class is repr(C) POD; all-zero is valid, then patched.
    let mut form: Box<FormData_pg_class> = Box::new(unsafe { core::mem::zeroed() });
    form.relkind = RELKIND_INDEX;
    form.relpersistence = RELPERSISTENCE_PERMANENT;
    form.relnatts = 1;
    form.relam = Oid(403);
    let form_ptr = Box::into_raw(form);

    let mut rel = Box::new(RelationData::blank());
    rel.rd_id = locator.relNumber;
    rel.rd_isvalid = true;
    rel.rd_refcnt = 1;
    rel.rd_rel = form_ptr;
    rel.rd_att = Some(tupdesc);
    rel.rd_amhandler = Oid(403);
    rel.rd_locator = locator;
    rel.rd_lockInfo = LockInfoData {
        lockRelId: LockRelId { relId: locator.relNumber, dbId: locator.dbOid },
    };
    Box::into_raw(rel)
}

fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: crate::postgres_ext::InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: core::ptr::null_mut(),
        expr: core::ptr::null_mut(),
    }
}

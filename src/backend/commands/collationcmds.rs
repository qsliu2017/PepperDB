//! CREATE/DROP COLLATION commands. Translated from
//! `src/backend/commands/collationcmds.c` (disposition: grow).
//!
//! `define_collation` inserts a pg_collation row (collname/collnamespace/collowner/
//! collprovider/collencoding + the collcollate/collctype/colllocale varlena fields).
//! The actual ICU/libc collation behavior (collation versioning, the comparison
//! routine) STAGES; the catalog row is the must-have. The `DEFINITION` options
//! (LOCALE / LC_COLLATE / LC_CTYPE / PROVIDER) are read from their A_Const args.
//!
//! Async coloring (rules.md s5): the catalog insert reaches the buffer pool, so the
//! command is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_collation::{self as pc, CollationRelationId, COLLPROVIDER_LIBC};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{DefineStmt, ValUnion};
use crate::postgres::{BoolGetDatum, CharGetDatum, Datum, Int32GetDatum, NameGetDatum, ObjectIdGetDatum, PointerGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// The last component of a (possibly schema-qualified) name list of `String_` nodes.
fn last_name(parts: &[Node]) -> &str {
    match parts.last() {
        Some(Node::String_(s)) => s.sval.as_str(),
        _ => "",
    }
}

/// The string value of the named DEFINITION option (a `def_elem` carrying an A_Const
/// string), or `None`. PG `defGetString`; the M10 def_elem arg is an A_Const so the
/// value is read directly (the value-node `defGetString` arm stages separately).
fn def_string<'a>(definition: &'a [Node], name: &str) -> Option<&'a str> {
    definition.iter().find_map(|n| {
        let Node::DefElem(d) = n else { return None };
        if d.defname.as_deref() != Some(name) {
            return None;
        }
        match d.arg.as_ref() {
            Some(Node::A_Const(c)) => match &c.val {
                ValUnion::String(s) => Some(s.sval.as_str()),
                _ => None,
            },
            _ => None,
        }
    })
}

/// PG `DefineCollation`: insert a pg_collation row for the named collation. The
/// locale resolution + the collation version probe (`get_collation_actual_version`)
/// and the libc/ICU comparison behavior STAGE (rules.md s4); the row is the
/// must-have. Returns the new collation's `ObjectAddress`.
pub async fn define_collation(shared: &Arc<SharedState>, stmt: &DefineStmt) -> ObjectAddress {
    let collname = last_name(&stmt.defnames);
    // PROVIDER defaults to libc; LOCALE sets both collate+ctype, overridden by the
    // explicit LC_COLLATE/LC_CTYPE. ICU rules / version probe stage.
    let provider = match def_string(&stmt.definition, "provider") {
        Some("icu") => crate::catalog::pg_collation::COLLPROVIDER_ICU,
        Some("builtin") => crate::catalog::pg_collation::COLLPROVIDER_BUILTIN,
        _ => COLLPROVIDER_LIBC,
    };
    let locale = def_string(&stmt.definition, "locale");
    let collcollate = def_string(&stmt.definition, "lc_collate").or(locale);
    let collctype = def_string(&stmt.definition, "lc_ctype").or(locale);

    let Some(pg_collation) = relation_id_get_relation(CollationRelationId) else {
        return ObjectAddress { classId: CollationRelationId, objectId: InvalidOid, objectSubId: 0 };
    };
    let desc = pg_collation.rd_att.clone().unwrap_or_else(|| unreachable!("pg_collation desc"));
    let natts = desc.natts as usize;

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let collname_data = name_data(collname);
    // New objects land in `public` (the M10 grammar has no schema-qualified form).
    let collnamespace = crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE;

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    values[(pc::Anum_pg_collation_oid - 1) as usize] = ObjectIdGetDatum(new_oid);
    values[(pc::Anum_pg_collation_collname - 1) as usize] = NameGetDatum(&collname_data);
    values[(pc::Anum_pg_collation_collnamespace - 1) as usize] = ObjectIdGetDatum(collnamespace);
    values[(pc::Anum_pg_collation_collowner - 1) as usize] = ObjectIdGetDatum(Oid::new(10));
    values[(pc::Anum_pg_collation_collprovider - 1) as usize] = CharGetDatum(provider);
    values[(pc::Anum_pg_collation_collisdeterministic - 1) as usize] = BoolGetDatum(true);
    // -1 == "encoding-independent" (matches the default/C collations).
    values[(pc::Anum_pg_collation_collencoding - 1) as usize] = Int32GetDatum(-1);

    let set_text = |v: &mut [Datum], n: &mut [bool], anum: i32, s: Option<&str>| match s {
        Some(s) => {
            v[(anum - 1) as usize] = PointerGetDatum(
                crate::backend::utils::adt::varlena::cstring_to_text(s).cast::<u8>(),
            );
        }
        None => n[(anum - 1) as usize] = true,
    };
    set_text(&mut values, &mut isnull, pc::Anum_pg_collation_collcollate, collcollate);
    set_text(&mut values, &mut isnull, pc::Anum_pg_collation_collctype, collctype);
    set_text(&mut values, &mut isnull, pc::Anum_pg_collation_colllocale, locale);
    isnull[(pc::Anum_pg_collation_collicurules - 1) as usize] = true;
    isnull[(pc::Anum_pg_collation_collversion - 1) as usize] = true;

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_collation, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_collation);

    ObjectAddress { classId: CollationRelationId, objectId: new_oid, objectSubId: 0 }
}

/// PG `RemoveCollationById`: delete the pg_collation row for `collation_oid` (the
/// dependency-walk leaf for DROP COLLATION). Reached from `delete_one_object` once
/// its pg_collation arm lands (a shared-file wiring change).
pub async fn remove_collation_by_id(shared: &Arc<SharedState>, collation_oid: Oid) {
    crate::backend::commands::dbcommands::delete_catalog_row_by_oid(
        shared,
        CollationRelationId,
        pc::Anum_pg_collation_oid as i16,
        collation_oid,
    )
    .await;
}

/// Resolve a collation name to its OID via a pg_collation heap scan (first match;
/// the M10 grammar carries an unqualified name). Returns `None` if absent.
pub async fn collation_oid_by_name(shared: &Arc<SharedState>, name: &str) -> Option<Oid> {
    use crate::backend::access::common::heaptuple::heap_getattr;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };
    let rel = relation_id_get_relation(CollationRelationId)?;
    let desc = rel.rd_att.clone()?;
    let snap = systable_scan_snapshot(shared, &rel, None);
    let mut scan = systable_beginscan(shared, &rel, InvalidOid, false, &snap, &[]);
    let mut found = None;
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        let (name_d, name_null) =
            unsafe { heap_getattr(tref, pc::Anum_pg_collation_collname, &desc) };
        if name_null || name_d.0 == 0 {
            continue;
        }
        let nd = unsafe { &*(name_d.0 as *const crate::c::NameData) };
        let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
        if nd.data[..end] != *name.as_bytes() {
            continue;
        }
        let (oid_d, oid_null) = unsafe { heap_getattr(tref, pc::Anum_pg_collation_oid, &desc) };
        if !oid_null {
            found = Some(crate::postgres::DatumGetObjectId(oid_d));
            break;
        }
    }
    systable_endscan(shared, &mut scan);
    relation_close(rel);
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-collcmds-{}-{}", std::process::id(), n));
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

    /// Does pg_collation hold a row with this collname? (heap scan)
    async fn collation_exists(shared: &Arc<SharedState>, name: &str) -> bool {
        use crate::backend::access::common::heaptuple::heap_getattr;
        use crate::backend::access::index::genam::{
            systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
        };
        let Some(rel) = relation_id_get_relation(CollationRelationId) else { return false };
        let desc = rel.rd_att.clone().unwrap();
        let snap = systable_scan_snapshot(shared, &rel, None);
        let mut scan = systable_beginscan(shared, &rel, InvalidOid, false, &snap, &[]);
        let mut found = false;
        while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
            let (d, isnull) = unsafe { heap_getattr(tref, pc::Anum_pg_collation_collname, &desc) };
            if isnull || d.0 == 0 {
                continue;
            }
            let nd = unsafe { &*(d.0 as *const crate::c::NameData) };
            let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
            if nd.data[..end] == *name.as_bytes() {
                found = true;
                break;
            }
        }
        systable_endscan(shared, &mut scan);
        relation_close(rel);
        found
    }

    fn parse_define_collation(sql: &str) -> DefineStmt {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let Node::DefineStmt(s) = rs.stmt.unwrap() else { panic!("not a DefineStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_collation_writes_row() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_define_collation("CREATE COLLATION german (LC_COLLATE = 'de_DE', LC_CTYPE = 'de_DE')");
            let addr = define_collation(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "define_collation returns a valid oid");
            bump_command(&shared);

            assert!(collation_exists(&shared, "german").await, "german collation row present");
            assert!(!collation_exists(&shared, "nonesuch").await, "absent name is absent");

            remove_collation_by_id(&shared, addr.objectId).await;
            bump_command(&shared);
            assert!(!collation_exists(&shared, "german").await, "row gone after remove_collation_by_id");
        }))
        .await;
    }
}

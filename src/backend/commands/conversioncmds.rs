//! CREATE/DROP CONVERSION commands. Translated from
//! `src/backend/commands/conversioncmds.c` (disposition: grow).
//!
//! `create_conversion` inserts a pg_conversion row (conname/connamespace/conowner/
//! conforencoding/contoencoding/conproc/condefault). The conversion FUNCTION
//! execution (the actual byte transcoding) STAGES; the catalog row is the must-have.
//! The minimal M10 grammar parses only the conversion name, so the encoding labels
//! and the conproc lookup default to SQL_ASCII / InvalidOid until the grammar grows
//! the `FOR ... TO ... FROM func` clause.
//!
//! Async coloring (rules.md s5): the catalog insert reaches the buffer pool, so the
//! command is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_conversion::{self as pc, ConversionRelationId};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::CreateConversionStmt;
use crate::postgres::{BoolGetDatum, Datum, Int32GetDatum, NameGetDatum, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// The last component of a (possibly schema-qualified) name list of `String_` nodes.
fn last_name(parts: &[Node]) -> &str {
    match parts.last() {
        Some(Node::String_(s)) => s.sval.as_str(),
        _ => "",
    }
}

/// PG `CreateConversionCommand`: insert a pg_conversion row for the named conversion.
/// The encoding pair + conproc are resolved from the (currently stub-parsed) clause;
/// the byte-transcoding behavior STAGES (rules.md s4). Returns the new conversion's
/// `ObjectAddress`.
pub async fn create_conversion(
    shared: &Arc<SharedState>,
    stmt: &CreateConversionStmt,
) -> ObjectAddress {
    let conname = last_name(&stmt.conversion_name);
    // The encoding labels resolve via pg_char_to_encoding once the grammar carries
    // the FOR/TO clause; the M10 stub-parse leaves them None -> SQL_ASCII (0).
    let for_enc = stmt
        .for_encoding_name
        .as_deref()
        .map_or(0, crate::mb::pg_wchar::pg_char_to_encoding);
    let to_enc = stmt
        .to_encoding_name
        .as_deref()
        .map_or(0, crate::mb::pg_wchar::pg_char_to_encoding);
    // The conversion proc (conproc) lookup against pg_proc STAGES with the func
    // resolution; the row records InvalidOid until the FROM-func clause is parsed.
    let _ = &stmt.func_name;
    let conproc = InvalidOid;

    let Some(pg_conversion) = relation_id_get_relation(ConversionRelationId) else {
        return ObjectAddress { classId: ConversionRelationId, objectId: InvalidOid, objectSubId: 0 };
    };
    let desc = pg_conversion.rd_att.clone().unwrap_or_else(|| unreachable!("pg_conversion desc"));
    let natts = desc.natts as usize;

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let conname_data = name_data(conname);
    // New objects land in `public` (the M10 grammar has no schema-qualified form).
    let connamespace = crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE;

    let mut values = vec![Datum(0); natts];
    let isnull = vec![false; natts]; // every pg_conversion column is fixed-width/non-null
    values[(pc::Anum_pg_conversion_oid - 1) as usize] = ObjectIdGetDatum(new_oid);
    values[(pc::Anum_pg_conversion_conname - 1) as usize] = NameGetDatum(&conname_data);
    values[(pc::Anum_pg_conversion_connamespace - 1) as usize] = ObjectIdGetDatum(connamespace);
    values[(pc::Anum_pg_conversion_conowner - 1) as usize] = ObjectIdGetDatum(Oid::new(10));
    values[(pc::Anum_pg_conversion_conforencoding - 1) as usize] = Int32GetDatum(for_enc);
    values[(pc::Anum_pg_conversion_contoencoding - 1) as usize] = Int32GetDatum(to_enc);
    values[(pc::Anum_pg_conversion_conproc - 1) as usize] = ObjectIdGetDatum(conproc);
    values[(pc::Anum_pg_conversion_condefault - 1) as usize] = BoolGetDatum(stmt.def);

    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_conversion, &mut tuple).await;
    heap_freetuple(tuple);
    relation_close(pg_conversion);

    ObjectAddress { classId: ConversionRelationId, objectId: new_oid, objectSubId: 0 }
}

/// PG `RemoveConversionById`: delete the pg_conversion row for `conversion_oid` (the
/// dependency-walk leaf for DROP CONVERSION). Reached from `delete_one_object` once
/// its pg_conversion arm lands (a shared-file wiring change).
pub async fn remove_conversion_by_id(shared: &Arc<SharedState>, conversion_oid: Oid) {
    crate::backend::commands::dbcommands::delete_catalog_row_by_oid(
        shared,
        ConversionRelationId,
        pc::Anum_pg_conversion_oid as i16,
        conversion_oid,
    )
    .await;
}

/// Resolve a conversion name to its OID via a pg_conversion heap scan (first match;
/// the M10 grammar carries an unqualified name). Returns `None` if absent.
pub async fn conversion_oid_by_name(shared: &Arc<SharedState>, name: &str) -> Option<Oid> {
    use crate::backend::access::common::heaptuple::heap_getattr;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };
    let rel = relation_id_get_relation(ConversionRelationId)?;
    let desc = rel.rd_att.clone()?;
    let snap = systable_scan_snapshot(shared, &rel, None);
    let mut scan = systable_beginscan(shared, &rel, InvalidOid, false, &snap, &[]);
    let mut found = None;
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        let (name_d, name_null) =
            unsafe { heap_getattr(tref, pc::Anum_pg_conversion_conname, &desc) };
        if name_null || name_d.0 == 0 {
            continue;
        }
        let nd = unsafe { &*(name_d.0 as *const crate::c::NameData) };
        let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
        if nd.data[..end] != *name.as_bytes() {
            continue;
        }
        let (oid_d, oid_null) = unsafe { heap_getattr(tref, pc::Anum_pg_conversion_oid, &desc) };
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
        let dir = std::env::temp_dir().join(format!("pepperdb-convcmds-{}-{}", std::process::id(), n));
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

    async fn conversion_exists(shared: &Arc<SharedState>, name: &str) -> bool {
        use crate::backend::access::common::heaptuple::heap_getattr;
        use crate::backend::access::index::genam::{
            systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
        };
        let Some(rel) = relation_id_get_relation(ConversionRelationId) else { return false };
        let desc = rel.rd_att.clone().unwrap();
        let snap = systable_scan_snapshot(shared, &rel, None);
        let mut scan = systable_beginscan(shared, &rel, InvalidOid, false, &snap, &[]);
        let mut found = false;
        while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
            let (d, isnull) = unsafe { heap_getattr(tref, pc::Anum_pg_conversion_conname, &desc) };
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

    fn parse_create_conversion(sql: &str) -> CreateConversionStmt {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let Node::CreateConversionStmt(s) = rs.stmt.unwrap() else { panic!("not a CreateConversionStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_conversion_writes_row() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_create_conversion("CREATE CONVERSION myconv");
            let addr = create_conversion(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "create_conversion returns a valid oid");
            bump_command(&shared);

            assert!(conversion_exists(&shared, "myconv").await, "myconv conversion row present");

            remove_conversion_by_id(&shared, addr.objectId).await;
            bump_command(&shared);
            assert!(!conversion_exists(&shared, "myconv").await, "row gone after remove_conversion_by_id");
        }))
        .await;
    }
}

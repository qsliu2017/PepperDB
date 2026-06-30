//! Generic DROP dispatch for non-relation objects. Translated from the
//! M10-reachable parts of `src/backend/commands/dropcmds.c` (disposition: full leaf
//! for the dispatch).
//!
//! `remove_objects` is the `DROP <objtype>` path for objects that do NOT go through
//! `RemoveRelations` (tables/indexes route there): resolve each named object via
//! `get_object_address`, then `performDeletion`. M10 reaches DROP of a TYPE / SCHEMA
//! (the object kinds `get_object_address` resolves now); the long tail (FUNCTION,
//! OPERATOR, CAST, ...) STAGES with its object-address support.
//!
//! Async coloring (rules.md s5): resolution + deletion reach the buffer pool, so the
//! dispatcher is `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::catalog::dependency::PerformDeletion;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DropStmt;
use crate::shared_state::SharedState;

/// Panic for a DROP object kind not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `RemoveObjects`: the generic DROP dispatch for non-relation objects. Resolve
/// each named object to its `ObjectAddress` (`get_object_address`), collect them,
/// then `performMultipleDeletions` with the requested behavior. IF EXISTS on an
/// absent object emits a notice and skips it (PG `does not exist, skipping`).
pub async fn remove_objects(shared: &Arc<SharedState>, stmt: &DropStmt) {
    let mut addresses = Vec::with_capacity(stmt.objects.len());
    for obj in &stmt.objects {
        let Node::RangeVar(rv) = obj else {
            not_yet_reachable("RemoveObjects: object reference is not a name");
        };
        let addr = crate::backend::catalog::objectaddress::get_object_address(
            shared,
            stmt.removeType,
            rv,
            stmt.missing_ok,
        )
        .await;
        if addr.objectId == crate::postgres_ext::InvalidOid {
            // missing_ok resolved to "absent": emit the skip notice.
            let name = rv.relname.as_deref().unwrap_or("?");
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("object \"{name}\" does not exist, skipping"));
            });
            continue;
        }
        addresses.push(addr);
    }

    let _ = PerformDeletion::empty();
    crate::backend::catalog::dependency::perform_multiple_deletions(shared, &addresses, stmt.behavior)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::parsenodes::DropStmt as DropStmtTy;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-dropcmds-{}-{}", std::process::id(), n));
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
        sess.set_authenticated_user_id(crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID);
        sess.set_current_user_id(crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID);
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

    fn parse_drop(sql: &str) -> DropStmtTy {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let Node::DropStmt(s) = rs.stmt.unwrap() else { panic!("not a DropStmt") };
        *s
    }

    fn parse_one(sql: &str) -> Node {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        rs.stmt.unwrap()
    }

    /// DROP FUNCTION / TYPE / COLLATION / CONVERSION all route through the generic
    /// `remove_objects` path: parse -> get_object_address -> performDeletion ->
    /// delete_one_object -> the per-command leaf. Each create writes a catalog row;
    /// the parsed DROP removes it.
    #[tokio::test(flavor = "multi_thread")]
    async fn drop_dispatch_removes_each_object_kind() {
        use crate::backend::catalog::pg_proc::proc_lookup_by_name;
        use crate::backend::catalog::namespace::typename_get_typid;
        use crate::backend::commands::collationcmds::collation_oid_by_name;
        use crate::backend::commands::conversioncmds::conversion_oid_by_name;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            // --- FUNCTION ---
            let Node::CreateFunctionStmt(cf) = parse_one(
                "CREATE FUNCTION addone(int4) RETURNS int4 LANGUAGE 'sql' AS 'select $1 + 1'",
            ) else {
                panic!("not a CreateFunctionStmt");
            };
            crate::backend::commands::functioncmds::create_function(&shared, &cf).await;
            bump_command(&shared);
            assert!(proc_lookup_by_name(&shared, "addone").await.is_some(), "addone created");
            remove_objects(&shared, &parse_drop("DROP FUNCTION addone")).await;
            bump_command(&shared);
            assert!(proc_lookup_by_name(&shared, "addone").await.is_none(), "addone gone via DROP FUNCTION");

            // --- TYPE (domain) ---
            let Node::CreateDomainStmt(cd) = parse_one("CREATE DOMAIN posint AS int4") else {
                panic!("not a CreateDomainStmt");
            };
            crate::backend::commands::typecmds::define_domain(&shared, &cd).await;
            bump_command(&shared);
            assert!(typename_get_typid(&shared, "posint").await.is_some(), "posint created");
            remove_objects(&shared, &parse_drop("DROP TYPE posint")).await;
            bump_command(&shared);
            assert!(typename_get_typid(&shared, "posint").await.is_none(), "posint gone via DROP TYPE");

            // --- COLLATION ---
            let Node::DefineStmt(dc) = parse_one("CREATE COLLATION mycoll (LC_COLLATE = 'C', LC_CTYPE = 'C')") else {
                panic!("not a DefineStmt");
            };
            crate::backend::commands::collationcmds::define_collation(&shared, &dc).await;
            bump_command(&shared);
            assert!(collation_oid_by_name(&shared, "mycoll").await.is_some(), "mycoll created");
            remove_objects(&shared, &parse_drop("DROP COLLATION mycoll")).await;
            bump_command(&shared);
            assert!(collation_oid_by_name(&shared, "mycoll").await.is_none(), "mycoll gone via DROP COLLATION");

            // --- CONVERSION ---
            let Node::CreateConversionStmt(cc) = parse_one("CREATE CONVERSION myconv") else {
                panic!("not a CreateConversionStmt");
            };
            crate::backend::commands::conversioncmds::create_conversion(&shared, &cc).await;
            bump_command(&shared);
            assert!(conversion_oid_by_name(&shared, "myconv").await.is_some(), "myconv created");
            remove_objects(&shared, &parse_drop("DROP CONVERSION myconv")).await;
            bump_command(&shared);
            assert!(conversion_oid_by_name(&shared, "myconv").await.is_none(), "myconv gone via DROP CONVERSION");
        }))
        .await;
    }
}

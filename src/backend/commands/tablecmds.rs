//! Commands for creating and altering table structures and settings. Translated
//! from the M2-reachable parts of `src/backend/commands/tablecmds.c`
//! (disposition: grow).
//!
//! M2 translates `DefineRelation`'s CREATE path (build a `TupleDesc` from the
//! `ColumnDef` list via `BuildDescForRelation`, then `heap_create_with_catalog`)
//! and `BuildDescForRelation` itself. ALTER / DROP / TRUNCATE / RENAME and the
//! inheritance / partitioning / LIKE / OF-type / reloptions / on-commit / toast
//! machinery are staged guards (rules.md s4); the helpers (`AlterTable*`,
//! `Rename*`, on-commit actions, ...) keep their header stubs.
//!
//! Async coloring (rules.md s5): `DefineRelation`/`BuildDescForRelation` reach the
//! catalog scans (type-name resolution) and `heap_create_with_catalog` (buffer
//! pool + WAL), so they are `async` and thread `&Arc<SharedState>`.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: holds per-backend raw Relation/TupleDesc handles task-confined for the operation; same contract as heap/relcache"
)]

use std::sync::Arc;

use crate::access::tupdesc::{TupleDesc, TupleDescData};
use crate::backend::catalog::heap::heap_create_with_catalog;
use crate::backend::catalog::namespace::{lookup_explicit_namespace, typename_nsp_get_typid};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::RelationRelationId;
use crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CreateStmt, TypeName};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// The heap table access method OID (`pg_am`: heap). M2 has a single AM.
const HEAP_TABLE_AM_OID: Oid = Oid(2);

/// Panic for a DefineRelation feature path not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `DefineRelation` (CREATE path). Resolves the creation namespace + owner,
/// builds the `TupleDesc` from the analyzed `ColumnDef` list, and creates the
/// relation with full catalog presence via `heap_create_with_catalog`. Returns the
/// new relation's `ObjectAddress` (its `typaddress` out-param is unused on the M2
/// path; the C two-out-param signature folds to the single returned address).
///
/// Staged (rules.md s4): tablespace selection / ACL checks, reloptions, OF-type,
/// inheritance (`MergeAttributes`), partitioning, defaults / CHECK / NOT NULL
/// constraint storage, the toast table, on-commit registration, and the kept
/// AccessExclusiveLock. The plain heap-table create is complete without them.
pub async fn DefineRelation(
    shared: &Arc<SharedState>,
    stmt: &CreateStmt,
    relkind: i8,
    owner_id: Oid,
    _query_string: &str,
) -> ObjectAddress {
    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("CreateStmt always carries a RangeVar"));

    if !stmt.inhRelations.is_empty() {
        not_yet_reachable("DefineRelation: inheritance");
    }
    if stmt.partspec.is_some() || stmt.partbound.is_some() {
        not_yet_reachable("DefineRelation: partitioning");
    }
    if stmt.ofTypename.is_some() {
        not_yet_reachable("DefineRelation: OF type");
    }
    if !stmt.options.is_empty() {
        not_yet_reachable("DefineRelation: WITH storage options");
    }
    if stmt.tablespacename.is_some() {
        not_yet_reachable("DefineRelation: explicit tablespace");
    }

    let relname = relation.relname.as_deref().unwrap_or_else(|| {
        unreachable!("CREATE TABLE always names the relation");
    });

    // RangeVarGetAndCheckCreationNamespace (M2 subset): a schema qualifier
    // resolves in that schema; otherwise the default creation namespace is public.
    // (Permission/lock/temp-namespace handling is staged.)
    let namespace_id = relation.schemaname.as_deref().map_or(PG_PUBLIC_NAMESPACE, |schema| {
        lookup_explicit_namespace(schema, false).unwrap_or_else(|| {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_SCHEMA)
                    .errmsg(format!("schema \"{schema}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        })
    });

    // Identify the owning user (PG: default to GetUserId()).
    let owner_id = if owner_id == InvalidOid {
        crate::backend::utils::init::miscinit::get_user_id()
    } else {
        owner_id
    };

    let access_method_id = stmt.accessMethod.as_deref().map_or(HEAP_TABLE_AM_OID, |_am| {
        not_yet_reachable("DefineRelation: explicit USING access method")
    });

    // Build the tuple descriptor from the (already column-only) element list.
    let descriptor = build_desc_for_relation(shared, &stmt.tableElts).await;

    let tablespace_id = crate::common::relpath::DEFAULTTABLESPACE_OID;

    let relation_id = heap_create_with_catalog(
        shared,
        relname,
        namespace_id,
        tablespace_id,
        InvalidOid, // assign a fresh relation OID
        InvalidOid, // assign a fresh rowtype OID
        owner_id,
        access_method_id,
        descriptor,
        relkind,
        relation.relpersistence,
        false, // not a shared relation
    )
    .await;

    ObjectAddress { classId: RelationRelationId, objectId: relation_id, objectSubId: 0 }
}

/// PG `BuildDescForRelation`: build a `TupleDesc` from a list of `ColumnDef`.
/// Resolves each column's `TypeName` to its `(typeOid, typmod)` and fills the
/// attribute. M2 supports the built-in scalar types `init_builtin_entry` knows
/// (int4/int8/text/bool/oid); per-column ACL checks, collation, storage,
/// compression, array dimensions, and `SETOF` rejection are staged.
pub async fn build_desc_for_relation(
    shared: &Arc<SharedState>,
    columns: &[Node],
) -> TupleDesc {
    let natts = i32::try_from(columns.len()).unwrap_or(0);
    let mut desc = TupleDescData::create_template(natts);

    for (i, element) in columns.iter().enumerate() {
        let Node::ColumnDef(entry) = element else {
            not_yet_reachable("BuildDescForRelation: non-ColumnDef element");
        };
        let attnum = i16::try_from(i + 1).unwrap_or(0);

        let attname = entry.colname.as_deref().unwrap_or_else(|| {
            unreachable!("a CREATE TABLE ColumnDef always has a name");
        });
        let type_name = entry.typeName.as_ref().unwrap_or_else(|| {
            // A typeless ColumnDef only arises for OF-type/inheritance (staged).
            not_yet_reachable("BuildDescForRelation: column without a type name");
        });

        if entry.collClause.is_some() {
            not_yet_reachable("BuildDescForRelation: COLLATE clause");
        }
        if entry.compression.is_some() || entry.storage_name.is_some() {
            not_yet_reachable("BuildDescForRelation: column storage/compression");
        }

        let (atttypid, atttypmod) = typename_type_id_and_mod(shared, type_name).await;
        if !type_name.arrayBounds.is_empty() {
            not_yet_reachable("BuildDescForRelation: array type");
        }
        if type_name.setof {
            not_yet_reachable("BuildDescForRelation: SETOF column");
        }

        desc.init_builtin_entry(attnum, attname, atttypid, atttypmod, 0);

        // Override TupleDescInitEntry's settings as requested (the in-descriptor
        // NOT NULL flag; collation/storage/compression/identity/generated are
        // staged with their features).
        let att = &mut desc.attrs[i];
        att.attnotnull = entry.is_not_null;
        att.attislocal = entry.is_local;
        att.attinhcount = entry.inhcount;
        desc.populate_compact_attribute(i);
    }

    Arc::new(desc)
}

/// PG `typenameTypeIdAndMod` (M2 subset): resolve a `TypeName` to its
/// `(typeOid, typmod)`. The name is a list of String parts; a 2-part name is
/// `schema.type` (e.g. `pg_catalog.int4`), a 1-part name is searched in the
/// default search path. Raises `type "..." does not exist` if unresolved. typmod
/// handling (`typenameTypeMod`) is `-1` for the M2 typmod-less types.
async fn typename_type_id_and_mod(shared: &Arc<SharedState>, type_name: &TypeName) -> (Oid, i32) {
    // An internally generated TypeName carries the OID directly (names == NIL).
    if type_name.names.is_empty() {
        if type_name.typeOid == InvalidOid {
            not_yet_reachable("typenameTypeIdAndMod: OID-less internal TypeName");
        }
        return (type_name.typeOid, type_name.typemod);
    }
    if !type_name.typmods.is_empty() {
        not_yet_reachable("typenameTypeIdAndMod: type modifiers");
    }

    let names: Vec<&str> = type_name.names.iter().map(|s| s.sval.as_str()).collect();
    let resolved = match names.as_slice() {
        [typname] => {
            // Unqualified: search the default search path (pg_catalog, public).
            crate::backend::catalog::namespace::typename_get_typid(shared, typname).await
        }
        [schemaname, typname] => {
            // Qualified: resolve in the named schema only.
            match lookup_explicit_namespace(schemaname, false) {
                Some(nsp) => typename_nsp_get_typid(shared, typname, nsp).await,
                None => None,
            }
        }
        _ => not_yet_reachable("typenameTypeIdAndMod: 3+ part type name"),
    };

    let Some(typoid) = resolved else {
        let printed = names.join(".");
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("type \"{printed}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };
    (typoid, -1)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
#[allow(clippy::future_not_send, reason = "test bodies; not spawned on the runtime")]
mod tests {
    use std::sync::Arc;

    use crate::nodes::nodes::{CmdType, Node};
    use crate::parser::parser::RawParseMode;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("pepperdb-tablecmds-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    /// Set up the full per-task scope stack and run the async body (mirrors the
    /// catalog integration harness).
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
        // An owning user id so DefineRelation's GetUserId() default is valid.
        sess.set_authenticated_user_id(crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID);
        sess.set_current_user_id(crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID);
        let owner =
            crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

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
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
        refresh_active_snapshot(shared);
    }

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

    /// Raw-parse + analyze `sql` into the (CMD_UTILITY) PlannedStmt the utility
    /// dispatcher consumes (mirrors pg_plan_queries' utility wrapper).
    fn analyze_to_utility_plan(sql: &str) -> crate::nodes::plannodes::PlannedStmt {
        let mut list = crate::backend::parser::parser::raw_parser(sql, RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs = *rs;
        let q =
            crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, sql, &[], 0, None);
        assert_eq!(q.commandType, CmdType::UTILITY, "CREATE TABLE analyzes to CMD_UTILITY");
        utility_planned_stmt(&q)
    }

    /// pg_plan_queries' "make a wrapper PlannedStmt" for a CMD_UTILITY query.
    fn utility_planned_stmt(
        query: &crate::nodes::parsenodes::Query,
    ) -> crate::nodes::plannodes::PlannedStmt {
        crate::nodes::plannodes::PlannedStmt {
            command_type: CmdType::UTILITY,
            query_id: query.queryId,
            plan_id: 0,
            has_returning: false,
            has_modifying_cte: false,
            can_set_tag: query.canSetTag,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_needed: false,
            jit_flags: 0,
            // PG: a utility wrapper PlannedStmt has planTree == NULL (the utility
            // statement is in utilityStmt). plan_tree is not Option<Node> in the
            // M2 header, so a zero-field marker node stands in; ProcessUtility never
            // reads it (it dispatches on utilityStmt).
            plan_tree: Node::A_Star(Box::new(crate::nodes::parsenodes::A_Star {})),
            part_prune_infos: Vec::new(),
            rtable: Vec::new(),
            unprunable_relids: None,
            perm_infos: Vec::new(),
            result_relations: Vec::new(),
            append_relations: Vec::new(),
            subplans: Vec::new(),
            rewind_plan_ids: None,
            row_marks: Vec::new(),
            relation_oids: Vec::new(),
            inval_items: Vec::new(),
            param_exec_types: Vec::new(),
            utility_stmt: query.utilityStmt.clone(),
            stmt_location: query.stmt_location,
            stmt_len: query.stmt_len,
        }
    }

    /// Drive a CREATE TABLE end-to-end through ProcessUtility and return its tag.
    async fn run_create_table(shared: &Arc<SharedState>, sql: &str) -> crate::tcop::cmdtaglist::CommandTag {
        let pstmt = analyze_to_utility_plan(sql);
        let mut dest = crate::backend::tcop::dest::NoneReceiver;
        let mut qc = crate::tcop::cmdtag::QueryCompletion {
            command_tag: crate::tcop::cmdtaglist::CommandTag::Unknown,
            nprocessed: 0,
        };
        // The completion tag a caller derives for the message (CreateCommandTag).
        let tag = crate::backend::tcop::utility::create_command_tag(
            pstmt.utility_stmt.as_ref().unwrap(),
        );
        crate::backend::tcop::utility::process_utility(
            shared,
            &pstmt,
            sql,
            crate::tcop::utility::ProcessUtilityContext::Toplevel,
            &mut dest,
            Some(&mut qc),
        )
        .await;
        tag
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_table_end_to_end_resolves_and_persists() {
        use crate::backend::catalog::namespace::range_var_get_relid;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            // Full path: parse -> analyze (CMD_UTILITY) -> ProcessUtility ->
            // DefineRelation -> heap_create_with_catalog.
            let tag = run_create_table(&shared, "CREATE TABLE t (a int)").await;
            assert_eq!(tag, crate::tcop::cmdtaglist::CommandTag::CreateTable);

            crate::backend::access::transam::xact::CommandCounterIncrement();
            refresh_active_snapshot(&shared);

            // RangeVarGetRelid("t") now resolves via the new pg_class row.
            let relid = range_var_get_relid(&shared, None, "t").await;
            assert!(relid.is_some(), "the new table resolves by name");
            let relid = relid.unwrap();
            assert!(relid.0 != 0);

            // Its heap storage exists on disk.
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

            // The pg_attribute column row is present: rebuild from disk.
            let rebuilt =
                crate::backend::utils::cache::relcache::relation_build_desc(&shared, relid).await;
            assert!(rebuilt.is_some(), "the new relation rebuilds from its on-disk catalog rows");
            if let Some(rd) = rebuilt {
                // SAFETY: live rebuilt relation.
                let natts = unsafe { (*rd).rd_att.as_ref().unwrap().natts };
                assert_eq!(natts, 1, "the rebuilt descriptor has the one user column");
                let att0 = unsafe { (*rd).rd_att.as_ref().unwrap().attr(0) };
                assert_eq!(att0.atttypid, Oid(23), "column a is int4");
            }
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_table_two_columns_end_to_end() {
        use crate::backend::catalog::namespace::range_var_get_relid;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            run_create_table(&shared, "CREATE TABLE t2 (a int, b integer)").await;

            crate::backend::access::transam::xact::CommandCounterIncrement();
            refresh_active_snapshot(&shared);

            let relid = range_var_get_relid(&shared, None, "t2").await.expect("t2 resolves");
            let rebuilt =
                crate::backend::utils::cache::relcache::relation_build_desc(&shared, relid)
                    .await
                    .expect("t2 rebuilds");
            // SAFETY: live rebuilt relation.
            let natts = unsafe { (*rebuilt).rd_att.as_ref().unwrap().natts };
            assert_eq!(natts, 2, "two user columns");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_table_unknown_type_errors() {
        use futures_util::FutureExt;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let res = std::panic::AssertUnwindSafe(run_create_table(
                &shared,
                "CREATE TABLE bad (a nosuchtype)",
            ))
            .catch_unwind()
            .await;
            std::panic::set_hook(prev);

            let payload = res.expect_err("unknown type must raise an error");
            let edata = payload
                .downcast_ref::<crate::utils::elog::ErrorData>()
                .expect("structured ErrorData");
            assert_eq!(edata.sqlerrcode, crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT);
        }))
        .await;
    }
}

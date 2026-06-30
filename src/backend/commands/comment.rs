//! COMMENT ON command. Translated from `src/backend/commands/comment.c`
//! (disposition: grow).
//!
//! `comment_object` resolves the named object to its `ObjectAddress` (via
//! `get_object_address`, step 38) then `create_comments` stores/updates/deletes the
//! pg_description row keyed by `(objoid, classoid, objsubid)`. pg_description is
//! seeded on-disk (bootstrap), so the comment persists and is queryable.
//!
//! Async coloring (rules.md s5): the catalog scan + write reach the buffer pool, so
//! the entry is `async` and threads `&Arc<SharedState>`. No lock is held across an
//! `.await`: each scan is drained into owned rows before the write.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT / varlena reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::access::skey::ScanKeyData;
use crate::backend::access::common::heaptuple::{
    heap_deform_tuple, heap_form_tuple, heap_freetuple,
};
use crate::backend::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
};
use crate::backend::catalog::indexing::{catalog_tuple_delete, catalog_tuple_insert, catalog_tuple_update};
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::objectaddress::{ObjectAddress, INVALID_OBJECT_ADDRESS};
use crate::catalog::pg_description::{self as d, DescriptionRelationId, FormData_pg_description};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CommentStmt, ObjectType};
use crate::nodes::primnodes::RangeVar;
use crate::postgres::{Datum, Int32GetDatum, ObjectIdGetDatum, PointerGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;

fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

/// PG `CommentObject`: COMMENT ON <object> IS 'text' (NULL removes the comment).
/// Resolve the object's `ObjectAddress`, then store/delete its pg_description row.
pub async fn comment_object(shared: &Arc<SharedState>, stmt: &CommentStmt) -> ObjectAddress {
    let address = resolve_comment_object(shared, stmt).await;
    if address.objectId == InvalidOid {
        // IF the resolver returned the missing-object sentinel (it does not on the
        // reachable kinds -- it errors -- but be defensive), skip the write.
        return address;
    }

    create_comments(
        shared,
        address.objectId,
        address.classId,
        address.objectSubId,
        stmt.comment.as_deref(),
    )
    .await;

    address
}

/// Resolve the COMMENT target to an `ObjectAddress`. The relation-shaped kinds
/// (TABLE/INDEX/SEQUENCE/VIEW) and SCHEMA route through `get_object_address`; COLUMN
/// resolves the owning relation + the column's attnum (carried as `objectSubId`).
async fn resolve_comment_object(shared: &Arc<SharedState>, stmt: &CommentStmt) -> ObjectAddress {
    use crate::backend::catalog::objectaddress::{
        get_object_address, get_object_address_attribute,
    };
    match stmt.objtype {
        ObjectType::TABLE
        | ObjectType::INDEX
        | ObjectType::SEQUENCE
        | ObjectType::VIEW
        | ObjectType::MATVIEW
        | ObjectType::SCHEMA => {
            let rv = comment_range_var(stmt);
            get_object_address(shared, stmt.objtype, rv, false).await
        }
        ObjectType::COLUMN => {
            // COMMENT ON COLUMN rel.col: the object is a 2-part name (the C form is
            // a List; the M10 form is a RangeVar built by `make_any_name`, whose
            // `schemaname` holds the relation and `relname` holds the column). The
            // grammar form stages (Phase A does not parse COLUMN yet); this body is
            // exercised by a directly-built CommentStmt. (A schema-qualified
            // relation -- 3-part name -- stages until the grammar reaches it.)
            let (rel, col) = comment_column_ref(stmt);
            get_object_address_attribute(shared, &rel, &col, false).await
        }
        other => unimplemented!("CommentObject: object type {other:?} not yet reachable"),
    }
}

/// Extract the relation `RangeVar` from a COMMENT statement's object (the
/// relation-shaped kinds carry exactly one `RangeVar`).
fn comment_range_var(stmt: &CommentStmt) -> &RangeVar {
    match stmt.object.as_ref() {
        Some(Node::RangeVar(rv)) => rv,
        _ => unreachable!("COMMENT ON {:?} carries a RangeVar object", stmt.objtype),
    }
}

/// Extract `(relation, column)` for COMMENT ON COLUMN. The column object is a 2-part
/// name (`relation.column`) carried as a `RangeVar` whose `schemaname` holds the
/// relation and `relname` holds the column (the shape `make_any_name` produces for a
/// 2-element list). Returns an owned relation `RangeVar` (schema unqualified) plus
/// the column name.
fn comment_column_ref(stmt: &CommentStmt) -> (RangeVar, String) {
    match stmt.object.as_ref() {
        Some(Node::RangeVar(rv)) => {
            let relation = rv
                .schemaname
                .as_deref()
                .unwrap_or_else(|| unreachable!("COMMENT ON COLUMN names relation.column"));
            let col = rv
                .relname
                .as_deref()
                .unwrap_or_else(|| unreachable!("COMMENT ON COLUMN names the column"));
            (
                crate::nodes::makefuncs::makeRangeVar(None, Some(relation.to_string()), -1),
                col.to_string(),
            )
        }
        _ => unreachable!("COMMENT ON COLUMN carries a RangeVar object"),
    }
}

/// One pg_description row copied out of a scan: owned tuple + its on-disk TID.
struct DescRow {
    tuple: HeapTupleData,
    tid: ItemPointerData,
}

/// PG `CreateComments`: store (or replace, or delete) the pg_description row for
/// `(objoid, classoid, objsubid)`. `comment == None` deletes the row; a non-NULL
/// comment inserts a new row or updates the existing one.
pub async fn create_comments(
    shared: &Arc<SharedState>,
    objoid: Oid,
    classoid: Oid,
    objsubid: i32,
    comment: Option<&str>,
) {
    let existing = scan_description(shared, objoid, classoid, objsubid).await;

    let Some(pg_desc) = relation_id_get_relation(DescriptionRelationId) else {
        unreachable!("pg_description must be seeded on-disk to store a comment");
    };

    match comment {
        // IS NULL (or empty): delete the existing row, if any.
        None => {
            for row in existing {
                catalog_tuple_delete(shared, &pg_desc, &row.tid).await;
                heap_freetuple(row.tuple);
            }
        }
        Some(text) => {
            let desc = pg_desc.rd_att.clone().unwrap_or_else(|| unreachable!("pg_description desc"));
            let natts = desc.natts as usize;
            let desc_datum = PointerGetDatum(
                crate::backend::utils::adt::varlena::cstring_to_text(text).cast::<u8>(),
            );

            if let Some(row) = existing.into_iter().next() {
                // Update the description column of the existing row in place.
                // SAFETY: owned tuple + matching descriptor.
                let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
                vals[(d::Anum_pg_description_description - 1) as usize] = desc_datum;
                nulls[(d::Anum_pg_description_description - 1) as usize] = false;
                let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
                catalog_tuple_update(shared, &pg_desc, &row.tid, &mut newtup).await;
                heap_freetuple(newtup);
                heap_freetuple(row.tuple);
            } else {
                // Insert a fresh row.
                let mut values = vec![Datum(0); natts];
                let isnull = vec![false; natts];
                values[(d::Anum_pg_description_objoid - 1) as usize] = ObjectIdGetDatum(objoid);
                values[(d::Anum_pg_description_classoid - 1) as usize] = ObjectIdGetDatum(classoid);
                values[(d::Anum_pg_description_objsubid - 1) as usize] = Int32GetDatum(objsubid);
                values[(d::Anum_pg_description_description - 1) as usize] = desc_datum;
                let mut tup = heap_form_tuple(&desc, &values, &isnull);
                catalog_tuple_insert(shared, &pg_desc, &mut tup).await;
                heap_freetuple(tup);
            }
        }
    }

    relation_close(pg_desc);
}

/// Scan pg_description for rows matching `(objoid, classoid, objsubid)`, returning
/// each as an owned `(tuple, tid)`. The objoid key is pushed into the scan; the
/// classoid/objsubid are filtered post-fetch (the M10 heap scan applies one key).
async fn scan_description(
    shared: &Arc<SharedState>,
    objoid: Oid,
    classoid: Oid,
    objsubid: i32,
) -> Vec<DescRow> {
    let Some(pg_desc) = relation_id_get_relation(DescriptionRelationId) else {
        return Vec::new();
    };
    let key = [ScanKeyData {
        flags: 0,
        attno: d::Anum_pg_description_objoid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: zero_fmgr_info(),
        argument: ObjectIdGetDatum(objoid),
    }];
    let snap = systable_scan_snapshot(shared, &pg_desc, None);
    let mut scan = systable_beginscan(shared, &pg_desc, d::DescriptionObjIndexId, false, &snap, &key);
    let mut rows = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; read its (classoid, objsubid) for the filter.
        let descp = GETSTRUCT(tref).cast::<FormData_pg_description>();
        let form = unsafe { &*descp };
        if form.classoid == classoid && form.objsubid == objsubid {
            // SAFETY: live scan tuple; copy (with TID) before endscan.
            let tuple = unsafe { crate::backend::access::common::heaptuple::heap_copytuple(tref) };
            rows.push(DescRow { tid: tuple.t_self, tuple });
        }
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_desc);
    rows
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::nodes::nodes::{CmdType, Node};
    use crate::parser::parser::RawParseMode;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-comment-{}-{}", std::process::id(), n));
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

    async fn run_utility(shared: &Arc<SharedState>, sql: &str) {
        let mut list = crate::backend::parser::parser::raw_parser(sql, RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs = *rs;
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, sql, &[], 0, None);
        let pstmt = crate::nodes::plannodes::PlannedStmt {
            command_type: CmdType::UTILITY,
            query_id: q.queryId,
            plan_id: 0,
            has_returning: false,
            has_modifying_cte: false,
            can_set_tag: q.canSetTag,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_needed: false,
            jit_flags: 0,
            plan_tree: Node::A_Star(Box::new(crate::nodes::parsenodes::A_Star {})),
            part_prune_infos: Vec::new(),
            rtable: Vec::new(),
            unprunable_relids: None,
            perm_infos: Vec::new(),
            result_relations: Vec::new(),
            append_relations: Vec::new(),
            subplans: Vec::new(),
            subplan_nodes: Vec::new(),
            rewind_plan_ids: None,
            row_marks: Vec::new(),
            relation_oids: Vec::new(),
            inval_items: Vec::new(),
            param_exec_types: Vec::new(),
            utility_stmt: q.utilityStmt.clone(),
            stmt_location: q.stmt_location,
            stmt_len: q.stmt_len,
        };
        let mut dest = crate::backend::tcop::dest::NoneReceiver;
        crate::backend::tcop::utility::process_utility(
            shared,
            &pstmt,
            sql,
            crate::tcop::utility::ProcessUtilityContext::Toplevel,
            &mut dest,
            None,
        )
        .await;
        bump_command(shared);
    }

    /// Read the pg_description text for (objoid, classoid, objsubid), if any.
    async fn description_of(
        shared: &Arc<SharedState>,
        objoid: Oid,
        classoid: Oid,
        objsubid: i32,
    ) -> Option<String> {
        let rows = scan_description(shared, objoid, classoid, objsubid).await;
        let row = rows.into_iter().next()?;
        let pg_desc = relation_id_get_relation(DescriptionRelationId).unwrap();
        let desc = pg_desc.rd_att.clone().unwrap();
        let (vals, nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        relation_close(pg_desc);
        let i = (d::Anum_pg_description_description - 1) as usize;
        let out = if nulls[i] {
            None
        } else {
            let p = crate::postgres::DatumGetPointer(vals[i]);
            // SAFETY: the description column is a live non-toasted text varlena.
            let t = unsafe { &*p.cast::<crate::c::text>() };
            Some(crate::backend::utils::adt::varlena::text_to_cstring(t))
        };
        heap_freetuple(row.tuple);
        out
    }

    async fn relid(shared: &Arc<SharedState>, name: &str) -> Oid {
        crate::backend::catalog::namespace::range_var_get_relid(shared, None, name)
            .await
            .expect("relation resolves")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn comment_on_table_persists_and_clears() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_utility(&shared, "CREATE TABLE t (a int, b int)").await;
            let oid = relid(&shared, "t").await;
            let class = crate::catalog::pg_class::RelationRelationId;

            run_utility(&shared, "COMMENT ON TABLE t IS 'hello world'").await;
            assert_eq!(
                description_of(&shared, oid, class, 0).await.as_deref(),
                Some("hello world"),
                "table comment persists"
            );

            // Replace the comment.
            run_utility(&shared, "COMMENT ON TABLE t IS 'second'").await;
            assert_eq!(
                description_of(&shared, oid, class, 0).await.as_deref(),
                Some("second"),
                "comment replaced in place"
            );

            // IS NULL removes it.
            run_utility(&shared, "COMMENT ON TABLE t IS NULL").await;
            assert_eq!(
                description_of(&shared, oid, class, 0).await,
                None,
                "comment removed by IS NULL"
            );
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn comment_on_column_uses_attnum_subid() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_utility(&shared, "CREATE TABLE tc (a int, b int)").await;
            let oid = relid(&shared, "tc").await;
            let class = crate::catalog::pg_class::RelationRelationId;

            // COLUMN grammar stages in Phase A; build the CommentStmt directly to
            // exercise the resolver + (objsubid = attnum) write path.
            let rv = crate::nodes::makefuncs::makeRangeVar(
                Some("tc".to_string()),
                Some("b".to_string()),
                -1,
            );
            let stmt = CommentStmt {
                objtype: ObjectType::COLUMN,
                object: Some(Node::RangeVar(Box::new(rv))),
                comment: Some("col b".to_string()),
            };
            comment_object(&shared, &stmt).await;
            bump_command(&shared);

            // column b is attnum 2.
            assert_eq!(
                description_of(&shared, oid, class, 2).await.as_deref(),
                Some("col b"),
                "column comment keyed by attnum"
            );
            assert_eq!(
                description_of(&shared, oid, class, 0).await,
                None,
                "table-level comment untouched"
            );
        }))
        .await;
    }
}

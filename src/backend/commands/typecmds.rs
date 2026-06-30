//! Commands for CREATE TYPE / CREATE DOMAIN. Translated from
//! `src/backend/commands/typecmds.c` (disposition: grow).
//!
//! `define_type` handles the composite form `CREATE TYPE n AS (col type, ...)` and
//! the enum form `CREATE TYPE n AS ENUM (...)`; `define_domain` handles `CREATE
//! DOMAIN n AS base`. Composite reuses `DefineRelation` with
//! `RELKIND_COMPOSITE_TYPE` (PG's `DefineCompositeType`), which builds the pg_class
//! rowtype + pg_attribute rows + the composite ('c') pg_type row. Domain reads the
//! base type's pg_type metadata and writes a 'd' pg_type row with `typbasetype`.
//!
//! STAGED (rules.md s4): the base-type `CREATE TYPE n (INPUT=,OUTPUT=)` form;
//! enum *label storage* (pg_enum is not seeded at this milestone -- the 'e' pg_type
//! row is created so the type resolves, but `compare_values_of_enum` has no rows);
//! the domain DEFAULT / NOT NULL / CHECK constraint storage (the grammar does not
//! carry them to phase B yet); dependency recording; collations.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CreateDomainStmt, CreateStmt, DefineStmt, TypeName};
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

/// Panic for a CREATE TYPE / DOMAIN path not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `DEFAULT_TYPDELIM` (pg_type.h): the array element delimiter `,`.
const DEFAULT_TYPDELIM: i8 = b',' as i8;

/// PG `DefineType`: CREATE TYPE. The composite (`AS (...)`) and enum (`AS ENUM
/// (...)`) forms are reached here; the base-type `(INPUT=, OUTPUT=)` form stages.
/// The grammar distinguishes the two by whether `definition` holds ColumnDefs
/// (composite) or String labels (enum).
pub async fn define_type(shared: &Arc<SharedState>, stmt: &DefineStmt) -> ObjectAddress {
    match stmt.definition.first() {
        Some(Node::ColumnDef(_)) => define_composite_type(shared, stmt).await,
        // Enum labels arrive as A_Const string literals (gram.y `enum_val_list`).
        Some(Node::A_Const(_)) => define_enum(shared, stmt).await,
        None => not_yet_reachable("DefineType: base type (INPUT=/OUTPUT=)"),
        Some(_) => unreachable!("CREATE TYPE definition is ColumnDef or enum-label A_Const list"),
    }
}

/// PG `DefineCompositeType`: `CREATE TYPE n AS (col type, ...)`. Build a `CreateStmt`
/// from the column list and run it through `DefineRelation` with
/// `RELKIND_COMPOSITE_TYPE`; the rowtype pg_type ('c') row + pg_attribute rows are
/// created by the heap-create path. Returns the new relation's address (PG returns
/// the type address; both point at the same composite, and the relation address is
/// what the create path yields).
async fn define_composite_type(shared: &Arc<SharedState>, stmt: &DefineStmt) -> ObjectAddress {
    let typename = name_tail(&stmt.defnames);
    let create = CreateStmt {
        relation: Some(Box::new(RangeVar {
            catalogname: None,
            schemaname: name_schema(&stmt.defnames),
            relname: Some(typename),
            inh: false,
            relpersistence: crate::catalog::pg_class::RELPERSISTENCE_PERMANENT,
            alias: None,
            location: -1,
        })),
        tableElts: stmt.definition.clone(),
        inhRelations: Vec::new(),
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: Vec::new(),
        nnconstraints: Vec::new(),
        options: Vec::new(),
        oncommit: crate::nodes::primnodes::OnCommitAction::NOOP,
        tablespacename: None,
        accessMethod: None,
        if_not_exists: false,
    };
    crate::backend::commands::tablecmds::DefineRelation(
        shared,
        &create,
        crate::catalog::pg_class::RELKIND_COMPOSITE_TYPE,
        InvalidOid,
        "",
    )
    .await
}

/// PG `DefineEnum`: `CREATE TYPE n AS ENUM (...)`. Create the pg_type 'e' row so the
/// type resolves. STAGED: the pg_enum label rows (pg_enum is not seeded yet), so
/// `enum_in`/ordering have no values; the type exists and is resolvable.
pub async fn define_enum(shared: &Arc<SharedState>, stmt: &DefineStmt) -> ObjectAddress {
    use crate::catalog::pg_type::{TYPALIGN_INT, TYPCATEGORY_ENUM, TYPSTORAGE_PLAIN, TYPTYPE_ENUM};

    let typename = name_tail(&stmt.defnames);
    let type_namespace = namespace_for(shared, &stmt.defnames).await;
    let owner_id = crate::backend::utils::init::miscinit::get_user_id();
    let new_type_oid = crate::backend::catalog::catalog::get_new_object_id(shared);

    // An enum is a 4-byte by-value OID-coded value (typlen 4, byval, int-aligned).
    crate::backend::catalog::pg_type::type_create(
        shared,
        new_type_oid,
        &typename,
        type_namespace,
        InvalidOid,            // relation_oid (no rowtype)
        0,                     // relation_kind
        owner_id,
        4,                     // internal_size
        TYPTYPE_ENUM,
        TYPCATEGORY_ENUM,
        false,                 // preferred
        DEFAULT_TYPDELIM,
        crate::utils::fmgroids::F_ENUM_IN,
        crate::utils::fmgroids::F_ENUM_OUT,
        crate::utils::fmgroids::F_ENUM_RECV,
        crate::utils::fmgroids::F_ENUM_SEND,
        InvalidOid, InvalidOid, InvalidOid, InvalidOid, // typmodin/out/analyze/subscript
        InvalidOid,            // element type
        false,                 // is array
        InvalidOid,            // array type
        InvalidOid,            // base type
        None, None,
        true,                  // passed by value
        TYPALIGN_INT,
        TYPSTORAGE_PLAIN,
        -1, 0, false,
        InvalidOid,            // collation
    )
    .await
    // STAGED (rules.md s4): EnumValuesCreate would insert one pg_enum row per label.
}

/// PG `DefineDomain`: `CREATE DOMAIN n AS base [DEFAULT][NOT NULL][CHECK]`. Resolve
/// the base type, copy its physical layout (typlen/byval/align), and write a 'd'
/// pg_type row with `typbasetype`. The DEFAULT / NOT NULL / CHECK constraints stage
/// (the grammar does not carry them to phase B yet).
pub async fn define_domain(shared: &Arc<SharedState>, stmt: &CreateDomainStmt) -> ObjectAddress {
    use crate::catalog::pg_type::{TYPCATEGORY_STRING, TYPSTORAGE_PLAIN, TYPTYPE_DOMAIN};

    let domainname = name_tail(&stmt.domainname);
    let type_namespace = namespace_for(shared, &stmt.domainname).await;
    let owner_id = crate::backend::utils::init::miscinit::get_user_id();

    let base_typename = stmt.typeName.as_deref().unwrap_or_else(|| {
        unreachable!("CREATE DOMAIN always carries a base type name");
    });
    let basetypeoid = resolve_type(shared, base_typename).await;

    // Inherit the base type's physical representation (PG copies these from the
    // base pg_type row). Category/storage are copied too; the M10 reachable domains
    // are over fixed-width scalars, so the defaults below are corrected from the
    // base type's read where it matters (typlen/byval/align).
    let (typlen, typbyval, typalign) =
        crate::backend::utils::cache::lsyscache::get_typlenbyvalalign_populate(shared, basetypeoid).await;
    let storage = if typlen == -1 { crate::catalog::pg_type::TYPSTORAGE_EXTENDED } else { TYPSTORAGE_PLAIN };
    // Domains use domain_in/domain_recv for input but the base type's output/send
    // (PG has no domain_out function -- format_type/output go through the base).
    let (base_output, _) =
        crate::backend::utils::cache::lsyscache::get_type_output_info_populate(shared, basetypeoid).await;

    if !stmt.constraints.is_empty() {
        not_yet_reachable("DefineDomain: domain constraints (DEFAULT/NOT NULL/CHECK)");
    }
    if stmt.collClause.is_some() {
        not_yet_reachable("DefineDomain: COLLATE clause");
    }

    let new_type_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    crate::backend::catalog::pg_type::type_create(
        shared,
        new_type_oid,
        &domainname,
        type_namespace,
        InvalidOid,            // relation_oid
        0,                     // relation_kind
        owner_id,
        typlen,
        TYPTYPE_DOMAIN,
        TYPCATEGORY_STRING,    // category not on the resolution path; PG copies base's
        false,                 // preferred
        DEFAULT_TYPDELIM,
        crate::utils::fmgroids::F_DOMAIN_IN,
        base_output,           // domains output via the base type's output proc
        crate::utils::fmgroids::F_DOMAIN_RECV,
        InvalidOid,            // send
        InvalidOid, InvalidOid, InvalidOid, InvalidOid, // typmodin/out/analyze/subscript
        InvalidOid,            // element type
        false,                 // is array
        InvalidOid,            // array type
        basetypeoid,           // base type
        None, None,
        typbyval,
        i8::try_from(typalign).unwrap_or(crate::catalog::pg_type::TYPALIGN_INT),
        storage,
        -1, 0,
        false,                 // not null (domain NOT NULL stages)
        InvalidOid,            // collation
    )
    .await
}

/// PG `RemoveTypeById`: the DROP TYPE leaf. Delete the pg_type row by OID. A
/// composite type ('c') also backs a pg_class rowtype relation (with pg_attribute
/// rows and storage); PG drops that via the type-to-relation INTERNAL pg_depend entry,
/// but pg_depend recording is staged here (rules.md s4) so the walk never reaches the
/// relation. We drop it directly through `heap_drop_with_catalog`, which removes the
/// pg_class/pg_attribute/storage and the rowtype pg_type row by OID (no re-entry into
/// `remove_type`, hence no recursion or double-delete). Enum/domain/base types have no
/// backing relation and keep the single-row delete. STAGED with pg_enum (no enum-label
/// cleanup yet).
#[allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to Form_pg_type (MAXALIGN'd body covers the Form alignment)"
)]
pub async fn remove_type(shared: &Arc<SharedState>, type_id: Oid) {
    use crate::access::htup_details::GETSTRUCT;
    use crate::access::skey::ScanKeyData;
    use crate::backend::access::common::heaptuple::{heap_copytuple, heap_freetuple};
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
    use crate::catalog::pg_type::{Anum_pg_type_oid, FormData_pg_type, TYPTYPE_COMPOSITE, TypeRelationId};

    let pg_type = relation_id_get_relation(TypeRelationId)
        .unwrap_or_else(|| unreachable!("pg_type is nailed/open"));
    let key = [ScanKeyData {
        flags: 0,
        attno: Anum_pg_type_oid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: crate::fmgr::FmgrInfo {
            fn_addr: None, oid: InvalidOid, nargs: 0, strict: false, retset: false,
            stats: 0, extra: 0, mcxt: (), expr: None,
        },
        argument: crate::postgres::ObjectIdGetDatum(type_id),
    }];
    let snap = systable_scan_snapshot(shared, &pg_type, None);
    let mut scan = systable_beginscan(shared, &pg_type, InvalidOid, false, &snap, &key);
    let mut tids = Vec::new();
    let mut composite_relid = InvalidOid;
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy it, then read its fixed pg_type part + TID.
        let tuple = unsafe { heap_copytuple(tref) };
        // SAFETY: owned tuple; typ -> its fixed pg_type Form.
        let typ = unsafe { &*GETSTRUCT(&tuple).cast::<FormData_pg_type>() };
        if typ.typtype == TYPTYPE_COMPOSITE && typ.typrelid.is_valid() {
            composite_relid = typ.typrelid;
        } else {
            tids.push(tuple.t_self);
        }
        heap_freetuple(tuple);
    }
    systable_endscan(shared, &mut scan);
    for tid in tids {
        crate::backend::catalog::indexing::catalog_tuple_delete(shared, &pg_type, &tid).await;
    }
    relation_close(pg_type);

    // Composite: drop the backing relation, which removes pg_class/pg_attribute/
    // storage and the rowtype pg_type row directly (so its row is NOT deleted above).
    if composite_relid.is_valid() {
        crate::backend::commands::tablecmds::heap_drop_with_catalog(shared, composite_relid).await;
    }
}

/// Resolve a `TypeName` to its type OID (1- or 2-part name in the default path).
async fn resolve_type(shared: &Arc<SharedState>, type_name: &TypeName) -> Oid {
    if type_name.names.is_empty() {
        if type_name.typeOid == InvalidOid {
            not_yet_reachable("DefineDomain: OID-less internal base TypeName");
        }
        return type_name.typeOid;
    }
    let names: Vec<&str> = type_name.names.iter().map(|s| s.sval.as_str()).collect();
    let resolved = match names.as_slice() {
        [typname] => crate::backend::catalog::namespace::typename_get_typid(shared, typname).await,
        [schemaname, typname] => {
            match crate::backend::catalog::namespace::get_namespace_oid(schemaname, false) {
                Some(nsp) => {
                    crate::backend::catalog::namespace::typename_nsp_get_typid(shared, typname, nsp).await
                }
                None => None,
            }
        }
        _ => not_yet_reachable("DefineDomain: 3+ part base type name"),
    };
    resolved.unwrap_or_else(|| {
        let printed = names.join(".");
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("type \"{printed}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    })
}

/// The last element of a name-part list, as the object name.
fn name_tail(names: &[Node]) -> String {
    match names.last() {
        Some(Node::String_(s)) => s.sval.clone(),
        _ => unreachable!("a CREATE TYPE/DOMAIN name is a non-empty String_ list"),
    }
}

/// The explicit schema of a 2-part name, if any.
fn name_schema(names: &[Node]) -> Option<String> {
    match names {
        [Node::String_(schema), Node::String_(_name)] => Some(schema.sval.clone()),
        _ => None,
    }
}

/// The creation namespace for a name list: the explicit schema if 2-part, else
/// `public`.
async fn namespace_for(shared: &Arc<SharedState>, names: &[Node]) -> Oid {
    match name_schema(names) {
        Some(schema) => crate::backend::catalog::namespace::namespace_oid_by_name(shared, &schema)
            .await
            .unwrap_or_else(|| {
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_SCHEMA)
                        .errmsg(format!("schema \"{schema}\" does not exist"));
                });
                unreachable!("ereport(ERROR) diverges");
            }),
        None => crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::namespace::typename_get_typid;
    use crate::backend::utils::cache::typcache::lookup_type_cache;
    use crate::catalog::pg_type::{TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN};
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::utils::typcache::TypeCacheFlags;

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);
    const INT4OID: Oid = Oid::new(23);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-typecmds-{}-{}", std::process::id(), n));
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

    fn parse_define(sql: &str) -> DefineStmt {
        let mut list = crate::backend::parser::parser::raw_parser(sql, crate::parser::parser::RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not RawStmt") };
        let Node::DefineStmt(s) = rs.stmt.unwrap() else { panic!("not DefineStmt") };
        *s
    }

    fn parse_domain(sql: &str) -> CreateDomainStmt {
        let mut list = crate::backend::parser::parser::raw_parser(sql, crate::parser::parser::RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not RawStmt") };
        let Node::CreateDomainStmt(s) = rs.stmt.unwrap() else { panic!("not CreateDomainStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn composite_type_creates_rowtype_and_caches_tupdesc() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_define("CREATE TYPE pt AS (x int4, y text)");
            let addr = define_type(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "composite create yields a valid relation OID");
            bump(&shared);

            // The composite type resolves by name to a pg_type ('c') row.
            let typoid = typename_get_typid(&shared, "pt").await.expect("pt resolves");
            let tc = lookup_type_cache(&shared, typoid, TypeCacheFlags::TUPDESC).await;
            assert_eq!(tc.typtype, TYPTYPE_COMPOSITE, "pt is a composite type");
            assert!(tc.typrelid.is_valid(), "composite has a rowtype relation");
            let desc = tc.tup_desc.expect("composite tupdesc cached");
            assert_eq!(desc.natts, 2, "pt has two attributes x, y");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn domain_creates_d_type_with_basetype() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_domain("CREATE DOMAIN posint AS int4");
            let addr = define_domain(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "domain create yields a valid type OID");
            bump(&shared);

            let typoid = typename_get_typid(&shared, "posint").await.expect("posint resolves");
            let tc = lookup_type_cache(&shared, typoid, TypeCacheFlags::DOMAIN_BASE_INFO).await;
            assert_eq!(tc.typtype, TYPTYPE_DOMAIN, "posint is a domain");
            assert_eq!(tc.domain_base_type, INT4OID, "posint's base type is int4");

            // DROP TYPE removes the pg_type row.
            remove_type(&shared, typoid).await;
            bump(&shared);
            assert!(typename_get_typid(&shared, "posint").await.is_none(), "posint gone after drop");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_composite_type_clears_backing_relation_and_recreates() {
        use crate::backend::catalog::namespace::relname_get_relid;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_define("CREATE TYPE ct AS (a int4, b text)");
            define_type(&shared, &stmt).await;
            bump(&shared);

            let typoid = typename_get_typid(&shared, "ct").await.expect("ct resolves");
            let tc = lookup_type_cache(&shared, typoid, TypeCacheFlags::empty()).await;
            assert_eq!(tc.typtype, TYPTYPE_COMPOSITE, "ct is composite");
            let relid = tc.typrelid;
            assert!(relid.is_valid(), "ct has a backing rowtype relation");
            assert!(relname_get_relid(&shared, "ct").await.is_some(), "ct rowtype relation present");

            // DROP TYPE ct: the pg_type row AND the backing pg_class rowtype must go.
            remove_type(&shared, typoid).await;
            bump(&shared);
            assert!(typename_get_typid(&shared, "ct").await.is_none(), "ct pg_type row gone");
            assert!(
                relname_get_relid(&shared, "ct").await.is_none(),
                "ct backing pg_class rowtype row gone (not orphaned)"
            );

            // A subsequent CREATE TYPE ct must NOT collide on a stale pg_class row.
            let stmt = parse_define("CREATE TYPE ct AS (x int4)");
            let addr = define_type(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "re-create ct succeeds after clean drop");
            bump(&shared);
            let re = typename_get_typid(&shared, "ct").await.expect("re-created ct resolves");
            let rtc = lookup_type_cache(&shared, re, TypeCacheFlags::TUPDESC).await;
            assert_eq!(rtc.tup_desc.expect("desc").natts, 1, "re-created ct has one attribute");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enum_creates_e_type() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_define("CREATE TYPE color AS ENUM ('r', 'g', 'b')");
            let addr = define_type(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "enum create yields a valid type OID");
            bump(&shared);

            let typoid = typename_get_typid(&shared, "color").await.expect("color resolves");
            let tc = lookup_type_cache(&shared, typoid, TypeCacheFlags::empty()).await;
            assert_eq!(tc.typtype, crate::catalog::pg_type::TYPTYPE_ENUM, "color is an enum type");
        }))
        .await;
    }
}

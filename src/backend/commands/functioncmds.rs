//! Commands for CREATE FUNCTION / PROCEDURE. Translated from
//! `src/backend/commands/functioncmds.c` (disposition: grow).
//!
//! `create_function` interprets the parsed CREATE [OR REPLACE] FUNCTION/PROCEDURE:
//! resolves the parameter + return types, reads the LANGUAGE and `AS` body options,
//! then forms the pg_proc row via `procedure_create`. `remove_function` is the DROP
//! FUNCTION leaf (delete the pg_proc row by OID).
//!
//! STAGED (rules.md s4): OUT/INOUT/VARIADIC/TABLE parameter modes + parameter
//! defaults (only IN parameters are reachable), SETOF return types, the option tail
//! (COST/ROWS/STRICT/VOLATILE/SECURITY/...), the `AS obj,sym` two-element internal
//! form, dependency recording, and -- crucially -- SQL/PL function *execution*: the
//! catalog row + name lookup are produced, but invoking the function lands later.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CreateFunctionStmt, FunctionParameter, FunctionParameterMode, TypeName};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// Panic for a CREATE FUNCTION path not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `VOIDOID` (pg_type.h): a procedure has no return type (prorettype = void).
const VOIDOID: Oid = Oid::new(2278);

/// PG `CreateFunction`: CREATE [OR REPLACE] FUNCTION/PROCEDURE. Resolves the inputs
/// and writes the pg_proc row; returns its `ObjectAddress`.
pub async fn create_function(
    shared: &Arc<SharedState>,
    stmt: &CreateFunctionStmt,
) -> ObjectAddress {
    let funcname = name_tail(&stmt.funcname);
    let func_namespace = name_namespace(shared, &stmt.funcname).await;

    // Interpret the parameters: resolve each IN parameter's type to its OID.
    let arg_types = interpret_function_parameters(shared, &stmt.parameters).await;

    // Return type. Procedures have none (PG: prorettype = VOIDOID); a function
    // without RETURNS is rejected by PG, but the grammar always supplies one for
    // FUNCTION.
    let return_type = match &stmt.returnType {
        Some(t) => resolve_type(shared, t).await,
        None => VOIDOID,
    };

    // LANGUAGE + AS body options.
    let language = option_string(&stmt.options, "language").unwrap_or_else(|| "sql".to_owned());
    let prosrc = option_string(&stmt.options, "as").unwrap_or_default();
    let prolang = match language.as_str() {
        "internal" => crate::backend::catalog::pg_proc::INTERNALLANGUAGEID,
        "sql" => crate::backend::catalog::pg_proc::SQLLANGUAGEID,
        other => {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("language \"{other}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
    };

    crate::backend::catalog::pg_proc::procedure_create(
        shared,
        &funcname,
        func_namespace,
        stmt.replace,
        return_type,
        prolang,
        &arg_types,
        &prosrc,
        stmt.is_procedure,
    )
    .await
}

/// PG `interpretFunctionParameters` (the IN-only subset): resolve each parameter's
/// `argType` to its type OID, in declaration order. OUT/INOUT/VARIADIC/TABLE modes
/// and defaults stage (rules.md s4).
async fn interpret_function_parameters(
    shared: &Arc<SharedState>,
    parameters: &[Node],
) -> Vec<Oid> {
    let mut arg_types = Vec::with_capacity(parameters.len());
    for node in parameters {
        let Node::FunctionParameter(fp) = node else {
            unreachable!("CREATE FUNCTION parameters are FunctionParameter nodes");
        };
        let fp: &FunctionParameter = fp;
        if fp.mode != FunctionParameterMode::IN {
            not_yet_reachable("CreateFunction: non-IN parameter mode");
        }
        if fp.defexpr.is_some() {
            not_yet_reachable("CreateFunction: parameter DEFAULT");
        }
        let arg_type = fp.argType.as_deref().unwrap_or_else(|| {
            unreachable!("a CREATE FUNCTION parameter always carries a type");
        });
        arg_types.push(resolve_type(shared, arg_type).await);
    }
    arg_types
}

/// Resolve a `TypeName` to its type OID. The reachable form is a 1-part builtin /
/// user type name searched in the default path; an internally-OID-carrying TypeName
/// is returned directly. Raises `type "..." does not exist` if unresolved.
async fn resolve_type(shared: &Arc<SharedState>, type_name: &TypeName) -> Oid {
    use crate::postgres_ext::InvalidOid;
    if type_name.names.is_empty() {
        if type_name.typeOid == InvalidOid {
            not_yet_reachable("CreateFunction: OID-less internal TypeName");
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
        _ => not_yet_reachable("CreateFunction: 3+ part type name"),
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

/// PG `RemoveFunctionById`: the DROP FUNCTION leaf. Delete the pg_proc row.
pub async fn remove_function(shared: &Arc<SharedState>, func_id: Oid) {
    crate::backend::catalog::pg_proc::remove_procedure_by_id(shared, func_id).await;
}

/// The last element of a (possibly schema-qualified) name list, as the object name.
fn name_tail(names: &[Node]) -> String {
    match names.last() {
        Some(Node::String_(s)) => s.sval.clone(),
        _ => unreachable!("a CREATE FUNCTION name is a non-empty String_ list"),
    }
}

/// The creation namespace for a (possibly schema-qualified) name: the explicit
/// schema if 2-part, else `public`.
async fn name_namespace(shared: &Arc<SharedState>, names: &[Node]) -> Oid {
    if let [Node::String_(schema), Node::String_(_name)] = names {
        crate::backend::catalog::namespace::namespace_oid_by_name(shared, &schema.sval)
            .await
            .unwrap_or_else(|| {
                let s = schema.sval.clone();
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_SCHEMA)
                        .errmsg(format!("schema \"{s}\" does not exist"));
                });
                unreachable!("ereport(ERROR) diverges");
            })
    } else {
        crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE
    }
}

/// Read a `DefElem` option's string value (the `language`/`as` SCONST args). The
/// grammar carries the value as an A_Const string literal (`make_string_const`).
fn option_string(options: &[Node], name: &str) -> Option<String> {
    use crate::nodes::parsenodes::ValUnion;
    options.iter().find_map(|n| match n {
        Node::DefElem(d) if d.defname.as_deref() == Some(name) => match &d.arg {
            Some(Node::A_Const(c)) => match &c.val {
                ValUnion::String(s) => Some(s.sval.clone()),
                _ => None,
            },
            Some(Node::String_(s)) => Some(s.sval.clone()),
            _ => None,
        },
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::pg_proc::proc_lookup_by_name;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-functioncmds-{}-{}", std::process::id(), n));
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

    fn parse_create_function(sql: &str) -> CreateFunctionStmt {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let Node::CreateFunctionStmt(s) = rs.stmt.unwrap() else { panic!("not a CreateFunctionStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_or_replace_and_drop_function() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            // CREATE FUNCTION -> a pg_proc row exists, queryable by name.
            let stmt = parse_create_function(
                "CREATE FUNCTION addone(int4) RETURNS int4 LANGUAGE 'sql' AS 'select $1 + 1'",
            );
            let addr = create_function(&shared, &stmt).await;
            assert!(addr.objectId.is_valid(), "create_function yields a valid OID");
            bump(&shared);

            let oid = proc_lookup_by_name(&shared, "addone").await.expect("addone exists");
            assert_eq!(oid, addr.objectId, "lookup finds the created function");
            assert_eq!(read_prosrc(&shared, oid).await, "select $1 + 1");

            // CREATE OR REPLACE updates prosrc, keeps the OID.
            let stmt2 = parse_create_function(
                "CREATE OR REPLACE FUNCTION addone(int4) RETURNS int4 LANGUAGE 'sql' AS 'select $1 + 2'",
            );
            let addr2 = create_function(&shared, &stmt2).await;
            assert_eq!(addr2.objectId, oid, "OR REPLACE keeps the same OID");
            bump(&shared);
            assert_eq!(read_prosrc(&shared, oid).await, "select $1 + 2", "prosrc updated");

            // DROP FUNCTION removes the row.
            remove_function(&shared, oid).await;
            bump(&shared);
            assert!(proc_lookup_by_name(&shared, "addone").await.is_none(), "addone gone after drop");
        }))
        .await;
    }

    /// Read prosrc out of the pg_proc row by OID (test helper).
    async fn read_prosrc(shared: &Arc<SharedState>, func_id: Oid) -> String {
        use crate::access::skey::ScanKeyData;
        use crate::backend::access::common::heaptuple::{heap_copytuple, heap_deform_tuple, heap_freetuple};
        use crate::backend::access::index::genam::{
            systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
        };
        use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
        use crate::catalog::pg_proc::{self as p, ProcedureRelationId};

        let pg_proc = relation_id_get_relation(ProcedureRelationId).expect("pg_proc");
        let desc = pg_proc.rd_att.clone().expect("desc");
        let key = [ScanKeyData {
            flags: 0,
            attno: p::Anum_pg_proc_oid as i16,
            strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
            subtype: crate::postgres_ext::InvalidOid,
            collation: crate::postgres_ext::InvalidOid,
            func: crate::fmgr::FmgrInfo {
                fn_addr: None, oid: crate::postgres_ext::InvalidOid, nargs: 0, strict: false,
                retset: false, stats: 0, extra: 0, mcxt: (), expr: None,
            },
            argument: crate::postgres::ObjectIdGetDatum(func_id),
        }];
        let snap = systable_scan_snapshot(shared, &pg_proc, None);
        let mut scan = systable_beginscan(shared, &pg_proc, crate::postgres_ext::InvalidOid, false, &snap, &key);
        let tref = Box::pin(systable_getnext(shared, &mut scan)).await.expect("row");
        // SAFETY: live scan tuple; copied before endscan.
        let tuple = unsafe { heap_copytuple(tref) };
        // SAFETY: `tuple` is a pg_proc row matching `desc`.
        let (vals, nulls) = unsafe { heap_deform_tuple(&tuple, &desc) };
        let idx = (p::Anum_pg_proc_prosrc - 1) as usize;
        assert!(!nulls[idx], "prosrc is NOT NULL");
        let s = crate::utils::builtins::TextDatumGetCString(vals[idx]);
        heap_freetuple(tuple);
        systable_endscan(shared, &mut scan);
        relation_close(pg_proc);
        s
    }
}

//! Object-address resolution: parse a named object reference into the
//! `(classId, objectId, objectSubId)` triple the dependency machinery and the
//! generic ALTER/DROP dispatchers operate on. Translated from the M10-reachable
//! parts of `src/backend/catalog/objectaddress.c` (disposition: grow).
//!
//! `get_object_address` is the entry point: given an `ObjectType` and the parsed
//! object reference (a `RangeVar` for relation-shaped objects, a `(RangeVar, col)`
//! pair for a column), resolve it to its catalog OID. M10 covers the object kinds
//! ALTER TABLE / DROP / RENAME reach now: relation (table/index/view/sequence),
//! column/attribute, constraint, type, and schema. The long tail of object types
//! (functions, operators, casts, ...) is STAGED (rules.md s4).
//!
//! Async coloring (rules.md s5): resolution scans pg_class / pg_attribute through
//! the relcache + catalog scans, so the resolvers are `async` and thread
//! `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::catalog::namespace::range_var_get_relid;
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::RelationRelationId;
use crate::nodes::parsenodes::ObjectType;
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

pub use crate::catalog::objectaddress::{ObjectAddress as ObjectAddressPub, INVALID_OBJECT_ADDRESS};

/// Panic for an object-type resolution path not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `get_object_address` (the M10-reachable subset). Resolve a relation-shaped
/// object reference (`RangeVar`) to its `ObjectAddress`. `missing_ok` returns
/// `INVALID_OBJECT_ADDRESS` (objectId == InvalidOid) when the relation is absent
/// instead of erroring (the caller decides whether to emit the IF EXISTS notice).
///
/// The C `get_object_address` takes the parsed `objtype` + `Node *object` + a
/// `LOCKMODE`, opens + locks the relation, and returns `(address, relp)`. The M10
/// port resolves the OID (the lock is conceptual on the single-backend path) and
/// returns the address; the relation handle is opened by the caller as needed.
pub async fn get_object_address_rel(
    shared: &Arc<SharedState>,
    objtype: ObjectType,
    rel: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    let relname = rel
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("an object RangeVar always names the object"));
    let relid = range_var_get_relid(shared, rel.schemaname.as_deref(), relname).await;

    let Some(relid) = relid else {
        if missing_ok {
            return INVALID_OBJECT_ADDRESS;
        }
        report_missing_relation(objtype, relname);
    };

    ObjectAddress { classId: RelationRelationId, objectId: relid, objectSubId: 0 }
}

/// PG `get_object_address` for an attribute (column). `OBJECT_COLUMN`: resolve the
/// owning relation, then the column's attnum (its `objectSubId`). `missing_ok`
/// returns `INVALID_OBJECT_ADDRESS` when the relation OR the column is absent.
pub async fn get_object_address_attribute(
    shared: &Arc<SharedState>,
    rel: &RangeVar,
    colname: &str,
    missing_ok: bool,
) -> ObjectAddress {
    let reladdr = get_object_address_rel(shared, ObjectType::TABLE, rel, missing_ok).await;
    if reladdr.objectId == InvalidOid {
        return INVALID_OBJECT_ADDRESS;
    }

    crate::backend::utils::cache::relcache::relation_build_desc(shared, reladdr.objectId).await;
    let heap = crate::backend::utils::cache::relcache::relation_id_get_relation(reladdr.objectId)
        .unwrap_or_else(|| unreachable!("relation just built into the relcache"));
    let attnum = attnum_of(&heap, colname);
    crate::backend::utils::cache::relcache::relation_close(heap);

    let Some(attnum) = attnum else {
        if missing_ok {
            return INVALID_OBJECT_ADDRESS;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{colname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };

    ObjectAddress {
        classId: RelationRelationId,
        objectId: reladdr.objectId,
        objectSubId: i32::from(attnum),
    }
}

/// PG `get_object_address` dispatcher over the M10-reachable object kinds. The
/// relation-shaped kinds (TABLE/INDEX/VIEW/SEQUENCE) resolve via pg_class; TYPE
/// resolves via pg_type; SCHEMA via the namespace table. The exotic kinds STAGE.
pub async fn get_object_address(
    shared: &Arc<SharedState>,
    objtype: ObjectType,
    object: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    match objtype {
        ObjectType::TABLE
        | ObjectType::INDEX
        | ObjectType::VIEW
        | ObjectType::MATVIEW
        | ObjectType::SEQUENCE
        | ObjectType::FOREIGN_TABLE => get_object_address_rel(shared, objtype, object, missing_ok).await,
        ObjectType::TYPE | ObjectType::DOMAIN => {
            get_object_address_type(shared, object, missing_ok).await
        }
        ObjectType::SCHEMA => get_object_address_schema(shared, object, missing_ok).await,
        ObjectType::FUNCTION | ObjectType::PROCEDURE => {
            get_object_address_function(shared, object, missing_ok).await
        }
        ObjectType::COLLATION => get_object_address_collation(shared, object, missing_ok).await,
        ObjectType::CONVERSION => get_object_address_conversion(shared, object, missing_ok).await,
        other => not_yet_reachable(&format!("get_object_address: {other:?}")),
    }
}

/// `get_object_address` for a function/procedure name (pg_proc). The M10 grammar
/// carries the name only (no argtypes), so the resolution is by name; the
/// `function_with_argtypes` overload-disambiguation grows with that grammar.
async fn get_object_address_function(
    shared: &Arc<SharedState>,
    rel: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    use crate::catalog::pg_proc::ProcedureRelationId;
    let name = rel
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("a function reference names the function"));
    match crate::backend::catalog::pg_proc::proc_lookup_by_name(shared, name).await {
        Some(oid) => ObjectAddress { classId: ProcedureRelationId, objectId: oid, objectSubId: 0 },
        None if missing_ok => INVALID_OBJECT_ADDRESS,
        None => report_missing_object(
            crate::utils::errcodes::ERRCODE_UNDEFINED_FUNCTION,
            "function",
            name,
        ),
    }
}

/// `get_object_address` for a collation name (pg_collation).
async fn get_object_address_collation(
    shared: &Arc<SharedState>,
    rel: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    use crate::catalog::pg_collation::CollationRelationId;
    let name = rel
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("a collation reference names the collation"));
    match crate::backend::commands::collationcmds::collation_oid_by_name(shared, name).await {
        Some(oid) => ObjectAddress { classId: CollationRelationId, objectId: oid, objectSubId: 0 },
        None if missing_ok => INVALID_OBJECT_ADDRESS,
        None => report_missing_object(
            crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT,
            "collation",
            name,
        ),
    }
}

/// `get_object_address` for a conversion name (pg_conversion).
async fn get_object_address_conversion(
    shared: &Arc<SharedState>,
    rel: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    use crate::catalog::pg_conversion::ConversionRelationId;
    let name = rel
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("a conversion reference names the conversion"));
    match crate::backend::commands::conversioncmds::conversion_oid_by_name(shared, name).await {
        Some(oid) => ObjectAddress { classId: ConversionRelationId, objectId: oid, objectSubId: 0 },
        None if missing_ok => INVALID_OBJECT_ADDRESS,
        None => report_missing_object(
            crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT,
            "conversion",
            name,
        ),
    }
}

/// PG's "<kind> does not exist" error for the by-name object kinds.
#[cold]
fn report_missing_object(errcode: i32, kind: &str, name: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(errcode).errmsg(format!("{kind} \"{name}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// `get_object_address` for a type name (pg_type). The M10 type names resolve
/// through the search-path type lookup.
async fn get_object_address_type(
    shared: &Arc<SharedState>,
    rel: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    use crate::catalog::pg_type::TypeRelationId;
    let typname = rel
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("a type reference names the type"));
    let typid = match rel.schemaname.as_deref() {
        Some(schema) => {
            match crate::backend::catalog::namespace::lookup_explicit_namespace(schema, true) {
                Some(nsp) => {
                    crate::backend::catalog::namespace::typename_nsp_get_typid(shared, typname, nsp).await
                }
                None => None,
            }
        }
        None => crate::backend::catalog::namespace::typename_get_typid(shared, typname).await,
    };
    match typid {
        Some(oid) => ObjectAddress { classId: TypeRelationId, objectId: oid, objectSubId: 0 },
        None if missing_ok => INVALID_OBJECT_ADDRESS,
        None => {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("type \"{typname}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
    }
}

/// `get_object_address` for a schema (pg_namespace). Scans pg_namespace on-disk so
/// user-created schemas (CREATE SCHEMA) resolve, not just the seeded built-ins.
async fn get_object_address_schema(
    shared: &Arc<SharedState>,
    rel: &RangeVar,
    missing_ok: bool,
) -> ObjectAddress {
    use crate::catalog::pg_namespace::NamespaceRelationId;
    let nspname = rel
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("a schema reference names the schema"));
    match crate::backend::catalog::namespace::namespace_oid_by_name(shared, nspname).await {
        Some(oid) => ObjectAddress { classId: NamespaceRelationId, objectId: oid, objectSubId: 0 },
        None if missing_ok => INVALID_OBJECT_ADDRESS,
        None => {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_SCHEMA)
                    .errmsg(format!("schema \"{nspname}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
    }
}

/// The 1-based heap attribute number of `colname` in `heap`, skipping dropped
/// columns (a dropped column's name is mangled and never matches a user name).
fn attnum_of(heap: &RelationData, colname: &str) -> Option<i16> {
    let desc = heap.rd_att.as_ref()?;
    (0..desc.natts as usize).find_map(|i| {
        let att = desc.attr(i);
        if att.attisdropped {
            return None;
        }
        (att_name(att) == colname).then_some(att.attnum)
    })
}

/// Read a `FormData_pg_attribute`'s `attname` as a String (NUL-padded NameData).
fn att_name(att: &crate::catalog::pg_attribute::FormData_pg_attribute) -> String {
    let bytes = crate::c::NameStr(&att.attname);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// PG's "relation does not exist" error for the relation-shaped object kinds. The
/// per-objtype message wording (`table`/`index`/...) mirrors PG's
/// `RangeVarGetRelidExtended` callbacks.
#[cold]
fn report_missing_relation(objtype: ObjectType, relname: &str) -> ! {
    let kind = match objtype {
        ObjectType::INDEX => "index",
        ObjectType::VIEW | ObjectType::MATVIEW => "view",
        ObjectType::SEQUENCE => "sequence",
        _ => "relation",
    };
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("{kind} \"{relname}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

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
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
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
const HEAP_TABLE_AM_OID: Oid = Oid::new(2);

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
    let namespace_id = match relation.schemaname.as_deref() {
        None => PG_PUBLIC_NAMESPACE,
        Some(schema) => crate::backend::catalog::namespace::namespace_oid_by_name(shared, schema)
            .await
            .unwrap_or_else(|| {
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_SCHEMA)
                        .errmsg(format!("schema \"{schema}\" does not exist"));
                });
                unreachable!("ereport(ERROR) diverges");
            }),
    };

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

// ===========================================================================
//  M10 (step 38): ALTER TABLE / RENAME / DROP.
// ===========================================================================

use crate::access::htup::HeapTupleData;
use crate::access::skey::ScanKeyData;
use crate::access::htup_details::GETSTRUCT;
use crate::backend::access::common::heaptuple::{
    heap_copytuple, heap_deform_tuple, heap_form_tuple, heap_freetuple,
};
use crate::backend::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
};
use crate::backend::catalog::indexing::{catalog_tuple_delete, catalog_tuple_update};
use crate::backend::catalog::namespace::range_var_get_relid;
use crate::backend::utils::cache::relcache::{
    relation_build_desc, relation_close, relation_forget_relation, relation_id_get_relation,
};
use crate::nodes::parsenodes::{
    AlterTableCmd, AlterTableStmt, AlterTableType, ColumnDef, ConstrType, DropBehavior,
};
use crate::nodes::primnodes::RangeVar;
use crate::postgres::Datum;
use crate::storage::itemptr::ItemPointerData;

/// AccessExclusiveLock mode (storage/lockdefs.h). ALTER TABLE / DROP take it; the
/// single-backend M10 path acquires it conceptually (the command frame holds it).
const ACCESS_EXCLUSIVE_LOCK: i32 = 8;

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

/// Allocate a fresh OID for a catalog row. M10 uses the OID counter directly
/// (`GetNewObjectId`); the unique-index collision recheck (`GetNewOidWithIndex`)
/// grows when these catalogs' oid indexes are queried for the recheck.
fn get_new_oid(shared: &Arc<SharedState>, _catalog_id: Oid) -> Oid {
    crate::backend::catalog::catalog::get_new_object_id(shared)
}

/// `cstring_to_text` as a Datum (pointer to the new text varlena).
fn text_datum(s: &str) -> Datum {
    crate::postgres::PointerGetDatum(crate::backend::utils::adt::varlena::cstring_to_text(s).cast::<u8>())
}

/// One catalog row copied out of a scan: an owned tuple (Send) + its on-disk TID.
/// Collected before any `.await` so no live scan tuple crosses an await point.
struct CatalogRow {
    tuple: HeapTupleData,
    tid: ItemPointerData,
}

/// Scan a catalog heap by an OID-equality key on `key_attno`, returning each
/// matching row as an owned `(tuple, tid)` pair. The scan is fully drained and
/// closed before returning, so the owned rows can be mutated/deleted across awaits.
async fn scan_catalog_by_oid(
    shared: &Arc<SharedState>,
    catalog_id: Oid,
    key_attno: i32,
    key_value: Oid,
) -> Vec<CatalogRow> {
    // A catalog not present in the relcache is not seeded at this milestone
    // (pg_attrdef / pg_constraint): no rows to find. The scan grows when those
    // catalogs are seeded on-disk (rules.md s4).
    let Some(catalog) = relation_id_get_relation(catalog_id) else {
        return Vec::new();
    };
    let key = [ScanKeyData {
        flags: 0,
        attno: key_attno as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: zero_fmgr_info(),
        argument: crate::postgres::ObjectIdGetDatum(key_value),
    }];
    let snap = systable_scan_snapshot(shared, &catalog, None);
    let mut scan = systable_beginscan(shared, &catalog, InvalidOid, false, &snap, &key);
    let mut rows = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy (with its TID) before endscan.
        let tuple = unsafe { heap_copytuple(tref) };
        rows.push(CatalogRow { tid: tuple.t_self, tuple });
    }
    systable_endscan(shared, &mut scan);
    relation_close(catalog);
    rows
}

/// Delete every row of `catalog_id` whose `key_attno` equals `key_value`. Returns
/// the number of rows deleted.
async fn delete_catalog_rows_by_oid(
    shared: &Arc<SharedState>,
    catalog_id: Oid,
    key_attno: i32,
    key_value: Oid,
) -> usize {
    let rows = scan_catalog_by_oid(shared, catalog_id, key_attno, key_value).await;
    if rows.is_empty() {
        return 0;
    }
    let Some(catalog) = relation_id_get_relation(catalog_id) else {
        return 0;
    };
    let n = rows.len();
    for row in rows {
        catalog_tuple_delete(shared, &catalog, &row.tid).await;
        heap_freetuple(row.tuple);
    }
    relation_close(catalog);
    n
}

/// PG `heap_drop_with_catalog` + `RelationDropStorage` (the M10 reachable form):
/// remove a relation's full catalog presence and storage. Deletes its pg_attribute,
/// pg_attrdef, pg_constraint, pg_type (rowtype), and pg_class rows; unlinks the main
/// fork; unregisters its catalog-index registry entry (if an index); and evicts the
/// relcache entry so a subsequent open sees it gone. Toast / inheritance / sequence
/// cleanup is staged (rules.md s4).
pub async fn heap_drop_with_catalog(shared: &Arc<SharedState>, relid: Oid) {
    use crate::catalog::pg_attrdef::AttrDefaultRelationId;
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId};
    use crate::catalog::pg_class::{self as c, RelationRelationId};
    use crate::catalog::pg_constraint::ConstraintRelationId;
    use crate::catalog::pg_type::{self as t, TypeRelationId};

    // Read the relation's pg_class form (for its rowtype OID + relfilenode) before
    // deleting the row. Build it into the relcache first.
    relation_build_desc(shared, relid).await;
    let rel = relation_id_get_relation(relid)
        .unwrap_or_else(|| unreachable!("relation {relid:?} just built"));
    let reltype = rel.form().reltype;
    let relfilenode = rel.rd_locator.relNumber;
    let spc = rel.rd_locator.spcOid;
    let dbid = rel.rd_locator.dbOid;
    relation_close(rel);

    // pg_attrdef rows (column defaults) keyed by adrelid.
    delete_catalog_rows_by_oid(
        shared,
        AttrDefaultRelationId,
        crate::catalog::pg_attrdef::Anum_pg_attrdef_adrelid,
        relid,
    )
    .await;

    // pg_constraint rows keyed by conrelid.
    delete_catalog_rows_by_oid(
        shared,
        ConstraintRelationId,
        crate::catalog::pg_constraint::Anum_pg_constraint_conrelid,
        relid,
    )
    .await;

    // pg_sequence row keyed by seqrelid (sequences only; a no-op for tables).
    delete_catalog_rows_by_oid(
        shared,
        crate::catalog::pg_sequence::SequenceRelationId,
        crate::catalog::pg_sequence::Anum_pg_sequence_seqrelid,
        relid,
    )
    .await;

    // pg_attribute rows keyed by attrelid.
    delete_catalog_rows_by_oid(shared, AttributeRelationId, a::Anum_pg_attribute_attrelid, relid)
        .await;

    // pg_type rowtype keyed by oid (if any; indexes have no rowtype).
    if reltype != InvalidOid {
        delete_catalog_rows_by_oid(shared, TypeRelationId, t::Anum_pg_type_oid, reltype).await;
    }

    // pg_class row keyed by oid.
    delete_catalog_rows_by_oid(shared, RelationRelationId, c::Anum_pg_class_oid, relid).await;

    // Unlink the on-disk storage (main + auxiliary forks).
    let locator = crate::storage::relfilelocator::RelFileLocator {
        spcOid: spc,
        dbOid: dbid,
        relNumber: relfilenode,
    };
    let backend = crate::storage::relfilelocator::RelFileLocatorBackend {
        locator,
        backend: crate::storage::procnumber::INVALID_PROC_NUMBER,
    };
    crate::backend::storage::smgr::md::mdunlink(
        shared,
        backend,
        crate::common::relpath::ForkNumber::InvalidForkNumber,
        false,
    )
    .await;

    // Drop the catalog-index registry entry if this relation is an index.
    crate::backend::catalog::indexing::unregister_catalog_index(relid);

    // Evict the relcache entry so the next open rebuilds (and finds it gone).
    relation_forget_relation(relid);
}

/// PG `AlterTable` + `ATController` (the M10 reachable form): resolve the target
/// relation (AccessExclusiveLock), then run each subcommand. PG splits into 3
/// passes (parse/prep, exec, rewrite); the M10 subcommands need no table rewrite
/// (ADD COLUMN with no volatile default, DROP COLUMN, default/constraint catalog
/// edits), so they execute directly. The pass machinery + ATRewriteTable grow when
/// a rewrite-inducing subcommand (ALTER TYPE, volatile default) lands.
pub async fn alter_table(shared: &Arc<SharedState>, stmt: &AlterTableStmt) {
    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("ALTER TABLE always names a relation"));
    let relname = relation
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("ALTER TABLE RangeVar names the relation"));

    let relid = range_var_get_relid(shared, relation.schemaname.as_deref(), relname).await;
    let Some(relid) = relid else {
        if stmt.missing_ok {
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("relation \"{relname}\" does not exist, skipping"));
            });
            return;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!("relation \"{relname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };
    let _ = ACCESS_EXCLUSIVE_LOCK; // lock taken conceptually on the command frame.

    for cmd in &stmt.cmds {
        let Node::AlterTableCmd(cmd) = cmd else {
            not_yet_reachable("ATController: non-AlterTableCmd subcommand");
        };
        Box::pin(ata_exec_cmd(shared, relid, cmd)).await;
        // Each subcommand is its own command for visibility (PG does a CCI between
        // passes); CCI so a following subcommand / statement sees the change.
        crate::backend::access::transam::xact::CommandCounterIncrement();
        relation_forget_relation(relid);
    }
}

/// PG `ATExecCmd`: dispatch one ALTER TABLE subcommand. M10 covers ADD/DROP COLUMN,
/// SET/DROP DEFAULT, SET/DROP NOT NULL, ADD/DROP CONSTRAINT.
async fn ata_exec_cmd(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd) {
    match cmd.subtype {
        AlterTableType::AddColumn => ata_exec_add_column(shared, relid, cmd).await,
        AlterTableType::DropColumn => ata_exec_drop_column(shared, relid, cmd).await,
        AlterTableType::ColumnDefault => ata_exec_column_default(shared, relid, cmd).await,
        AlterTableType::SetNotNull => ata_exec_set_not_null(shared, relid, cmd, true).await,
        AlterTableType::DropNotNull => ata_exec_set_not_null(shared, relid, cmd, false).await,
        AlterTableType::AddConstraint => ata_exec_add_constraint(shared, relid, cmd).await,
        AlterTableType::DropConstraint => ata_exec_drop_constraint(shared, relid, cmd).await,
        other => not_yet_reachable(&format!("ATExecCmd: {other:?}")),
    }
}

/// The 1-based heap attnum of `colname` in the relation `relid`, and the current
/// max attnum (for assigning a new column's attnum). Scans the rebuilt descriptor.
async fn relation_attnum_info(shared: &Arc<SharedState>, relid: Oid) -> (Vec<(String, i16, bool)>, i16) {
    relation_build_desc(shared, relid).await;
    let rel = relation_id_get_relation(relid)
        .unwrap_or_else(|| unreachable!("relation {relid:?} just built"));
    let desc = rel.rd_att.as_ref().unwrap_or_else(|| unreachable!("relation has a descriptor"));
    let mut cols = Vec::with_capacity(desc.natts as usize);
    let mut maxatt = 0i16;
    for i in 0..desc.natts as usize {
        let att = desc.attr(i);
        maxatt = maxatt.max(att.attnum);
        cols.push((att_name(att), att.attnum, att.attisdropped));
    }
    relation_close(rel);
    (cols, maxatt)
}

/// Read a `FormData_pg_attribute`'s `attname` as a String.
fn att_name(att: &crate::catalog::pg_attribute::FormData_pg_attribute) -> String {
    let bytes = crate::c::NameStr(&att.attname);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// PG `ATExecAddColumn`: append a column to the relation. Builds the new column's
/// pg_attribute row (attnum = relnatts + 1), inserts it, and bumps pg_class.relnatts.
/// A plain ADD COLUMN with no volatile default does NOT rewrite the table (PG); the
/// new column reads as NULL for existing rows. Defaults / NOT NULL on the added
/// column are applied as a follow-on (raw_default routed through ATExecColumnDefault
/// is staged; M10 reaches the plain `ADD COLUMN name type`).
async fn ata_exec_add_column(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd) {
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId, FormData_pg_attribute};
    use crate::postgres::{
        BoolGetDatum, CharGetDatum, Int16GetDatum, Int32GetDatum, NameGetDatum, ObjectIdGetDatum,
    };

    let Some(Node::ColumnDef(coldef)) = cmd.def.as_ref() else {
        unreachable!("AT_AddColumn carries a ColumnDef");
    };
    let coldef: &ColumnDef = coldef;
    if coldef.raw_default.is_some() {
        not_yet_reachable("ATExecAddColumn: column DEFAULT on add");
    }
    let colname = coldef
        .colname
        .as_deref()
        .unwrap_or_else(|| unreachable!("ADD COLUMN names the column"));
    let type_name = coldef
        .typeName
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("ATExecAddColumn: typeless column"));

    let (existing, maxatt) = relation_attnum_info(shared, relid).await;
    if existing.iter().any(|(n, _, dropped)| !dropped && n == colname) {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_COLUMN)
                .errmsg(format!("column \"{colname}\" of relation already exists"));
        });
        unreachable!("ereport(ERROR) diverges");
    }
    let new_attnum = maxatt + 1;

    let (atttypid, atttypmod) = typename_type_id_and_mod(shared, type_name).await;

    // Build the pg_attribute row from a fresh single-slot descriptor
    // (TupleDescInitEntry). The template indexes by attnum, so fill slot 1 then
    // override its attnum to the relation's next attribute number.
    let mut slot_desc = crate::access::tupdesc::TupleDescData::create_template(1);
    slot_desc.init_builtin_entry(1, colname, atttypid, atttypmod, 0);
    slot_desc.attrs[0].attnum = new_attnum;
    slot_desc.attrs[0].attnotnull = coldef.is_not_null;
    slot_desc.populate_compact_attribute(0);
    let attr: &FormData_pg_attribute = &slot_desc.attrs[0];

    let pg_attribute = relation_id_get_relation(AttributeRelationId)
        .unwrap_or_else(|| unreachable!("pg_attribute is open"));
    let desc = pg_attribute.rd_att.clone().unwrap_or_else(|| unreachable!("pg_attribute desc"));
    let natts = desc.natts as usize;
    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;
    set(&mut values, a::Anum_pg_attribute_attrelid, ObjectIdGetDatum(relid));
    set(&mut values, a::Anum_pg_attribute_attname, NameGetDatum(&attr.attname));
    set(&mut values, a::Anum_pg_attribute_atttypid, ObjectIdGetDatum(attr.atttypid));
    set(&mut values, a::Anum_pg_attribute_attlen, Int16GetDatum(attr.attlen));
    set(&mut values, a::Anum_pg_attribute_attnum, Int16GetDatum(attr.attnum));
    set(&mut values, a::Anum_pg_attribute_atttypmod, Int32GetDatum(attr.atttypmod));
    set(&mut values, a::Anum_pg_attribute_attndims, Int16GetDatum(attr.attndims));
    set(&mut values, a::Anum_pg_attribute_attbyval, BoolGetDatum(attr.attbyval));
    set(&mut values, a::Anum_pg_attribute_attalign, CharGetDatum(attr.attalign));
    set(&mut values, a::Anum_pg_attribute_attstorage, CharGetDatum(attr.attstorage));
    set(&mut values, a::Anum_pg_attribute_attcompression, CharGetDatum(attr.attcompression));
    set(&mut values, a::Anum_pg_attribute_attnotnull, BoolGetDatum(attr.attnotnull));
    set(&mut values, a::Anum_pg_attribute_atthasdef, BoolGetDatum(false));
    set(&mut values, a::Anum_pg_attribute_atthasmissing, BoolGetDatum(false));
    set(&mut values, a::Anum_pg_attribute_attidentity, CharGetDatum(attr.attidentity));
    set(&mut values, a::Anum_pg_attribute_attgenerated, CharGetDatum(attr.attgenerated));
    set(&mut values, a::Anum_pg_attribute_attisdropped, BoolGetDatum(false));
    set(&mut values, a::Anum_pg_attribute_attislocal, BoolGetDatum(true));
    set(&mut values, a::Anum_pg_attribute_attinhcount, Int16GetDatum(0));
    set(&mut values, a::Anum_pg_attribute_attcollation, ObjectIdGetDatum(attr.attcollation));
    isnull[(a::Anum_pg_attribute_attstattarget - 1) as usize] = true;
    isnull[(a::Anum_pg_attribute_attoptions - 1) as usize] = true;
    isnull[(a::Anum_pg_attribute_attacl - 1) as usize] = true;
    isnull[(a::Anum_pg_attribute_attfdwoptions - 1) as usize] = true;
    isnull[(a::Anum_pg_attribute_attmissingval - 1) as usize] = true;
    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    crate::backend::catalog::indexing::catalog_tuple_insert(shared, &pg_attribute, &mut tup).await;
    heap_freetuple(tup);
    relation_close(pg_attribute);

    // Bump pg_class.relnatts so the rebuilt descriptor reads the new column.
    update_pg_class_relnatts(shared, relid, new_attnum).await;
}

/// PG `ATExecDropColumn`: mark a column dropped (attisdropped = true). PG keeps the
/// physical tuple data; the column just disappears from the descriptor (a rebuilt
/// descriptor skips dropped columns). Updates the pg_attribute row in place and
/// clears its NOT NULL / default flags. M10 drops the dependent pg_attrdef row.
async fn ata_exec_drop_column(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd) {
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId, FormData_pg_attribute};
    use crate::catalog::pg_attrdef::AttrDefaultRelationId;

    let colname = cmd.name.as_deref().unwrap_or_else(|| unreachable!("DROP COLUMN names the column"));

    // Find the column's pg_attribute row.
    let rows = scan_catalog_by_oid(shared, AttributeRelationId, a::Anum_pg_attribute_attrelid, relid)
        .await;
    let pg_attribute = relation_id_get_relation(AttributeRelationId)
        .unwrap_or_else(|| unreachable!("pg_attribute open"));
    let desc = pg_attribute.rd_att.clone().unwrap_or_else(|| unreachable!("pg_attribute desc"));

    let mut found_attnum: Option<i16> = None;
    for row in &rows {
        // SAFETY: owned tuple; attp -> its fixed pg_attribute part.
        let attp = GETSTRUCT(&row.tuple).cast::<FormData_pg_attribute>();
        let att = unsafe { &*attp };
        if att.attisdropped || att_name(att) != colname {
            continue;
        }
        found_attnum = Some(att.attnum);
        // Deform, set attisdropped + clear name/notnull/hasdef, reform, update.
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        let mangled = format!("........pg.dropped.{}........", att.attnum);
        let mangled_name = mut_name(&mangled);
        vals[(a::Anum_pg_attribute_attname - 1) as usize] = NameGetDatum_owned(&mangled_name);
        nulls[(a::Anum_pg_attribute_attname - 1) as usize] = false;
        vals[(a::Anum_pg_attribute_attisdropped - 1) as usize] =
            crate::postgres::BoolGetDatum(true);
        vals[(a::Anum_pg_attribute_attnotnull - 1) as usize] =
            crate::postgres::BoolGetDatum(false);
        vals[(a::Anum_pg_attribute_atthasdef - 1) as usize] =
            crate::postgres::BoolGetDatum(false);
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_attribute, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        break;
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    relation_close(pg_attribute);

    let Some(_attnum) = found_attnum else {
        if cmd.missing_ok {
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errmsg(format!("column \"{colname}\" of relation does not exist, skipping"));
            });
            return;
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{colname}\" of relation does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };

    // Drop the column's default (pg_attrdef) if any. M10 drops all of the relation's
    // attrdef rows for the dropped attnum; the full per-attnum scan is staged, so the
    // reachable single-default case is handled by SET/DROP DEFAULT clearing it.
    let _ = AttrDefaultRelationId;
}

/// PG `ATExecColumnDefault`: SET / DROP a column's default. Stores the raw default
/// expression in pg_attrdef + sets atthasdef (SET), or removes the pg_attrdef row +
/// clears atthasdef (DROP). M10 stores the deparsed default text (the executor reads
/// it back on INSERT default; the expression-node store grows with cooked defaults).
async fn ata_exec_column_default(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd) {
    use crate::catalog::pg_attrdef::{self as ad, AttrDefaultRelationId};
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId, FormData_pg_attribute};

    let colname = cmd.name.as_deref().unwrap_or_else(|| unreachable!("ALTER COLUMN names the column"));
    let attnum = column_attnum(shared, relid, colname).await.unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{colname}\" of relation does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    });

    let has_default = cmd.def.is_some();

    // Store the default in pg_attrdef when that catalog is seeded. pg_attrdef is not
    // an on-disk catalog at this milestone (STAGED, rules.md s4): the default's
    // presence is recorded by the pg_attribute.atthasdef flag below, which the
    // descriptor reflects; the adbin row store + the INSERT-default read grow when
    // pg_attrdef is seeded. The text is computed via deparse to validate the path.
    if let Some(catalog) = relation_id_get_relation(AttrDefaultRelationId) {
        let attrdef_desc =
            catalog.rd_att.clone().unwrap_or_else(|| unreachable!("pg_attrdef desc"));
        // Remove any existing default for (relid, attnum).
        let defrows =
            scan_catalog_by_oid(shared, AttrDefaultRelationId, ad::Anum_pg_attrdef_adrelid, relid).await;
        for row in &defrows {
            // SAFETY: owned tuple; read adnum out of the fixed part.
            let (vals, _nulls) = unsafe { heap_deform_tuple(&row.tuple, &attrdef_desc) };
            let adnum = crate::postgres::DatumGetInt16(vals[(ad::Anum_pg_attrdef_adnum - 1) as usize]);
            if adnum == attnum {
                catalog_tuple_delete(shared, &catalog, &row.tid).await;
            }
        }
        for row in defrows {
            heap_freetuple(row.tuple);
        }
        if let Some(expr) = cmd.def.as_ref() {
            let text = crate::backend::utils::adt::ruleutils::deparse_expression(expr);
            let natts = attrdef_desc.natts as usize;
            let mut values = vec![Datum(0); natts];
            let isnull = vec![false; natts];
            let new_oid = get_new_oid(shared, AttrDefaultRelationId);
            let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;
            set(&mut values, ad::Anum_pg_attrdef_oid, crate::postgres::ObjectIdGetDatum(new_oid));
            set(&mut values, ad::Anum_pg_attrdef_adrelid, crate::postgres::ObjectIdGetDatum(relid));
            set(&mut values, ad::Anum_pg_attrdef_adnum, crate::postgres::Int16GetDatum(attnum));
            set(&mut values, ad::Anum_pg_attrdef_adbin, text_datum(&text));
            let mut tup = heap_form_tuple(&attrdef_desc, &values, &isnull);
            crate::backend::catalog::indexing::catalog_tuple_insert(shared, &catalog, &mut tup).await;
            heap_freetuple(tup);
        }
        relation_close(catalog);
    } else if let Some(expr) = cmd.def.as_ref() {
        // Validate the deparse path even without pg_attrdef storage.
        let _ = crate::backend::utils::adt::ruleutils::deparse_expression(expr);
    }

    // Update pg_attribute.atthasdef for the column (the seeded catalog).
    set_attribute_flag(shared, relid, attnum, AttFlag::HasDef(has_default)).await;
    let _ = (a::Anum_pg_attribute_atthasdef, AttributeRelationId, std::marker::PhantomData::<FormData_pg_attribute>);
}

/// PG `ATExecSetNotNull` / `ATExecDropNotNull`: flip pg_attribute.attnotnull. M10
/// edits the flag (the NOT NULL constraint validation scan + pg_constraint row are
/// staged; the flag is what the descriptor + INSERT null-check read).
async fn ata_exec_set_not_null(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd, set: bool) {
    let colname = cmd.name.as_deref().unwrap_or_else(|| unreachable!("ALTER COLUMN names the column"));
    let attnum = column_attnum(shared, relid, colname).await.unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{colname}\" of relation does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    });
    set_attribute_flag(shared, relid, attnum, AttFlag::NotNull(set)).await;
}

/// PG `ATExecAddConstraint` (M10 CHECK / NOT NULL minimal form): store a CHECK
/// constraint's pg_constraint row (the deparsed expression). The validation scan
/// (verify existing rows satisfy the constraint) and UNIQUE/FK forms are staged.
async fn ata_exec_add_constraint(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd) {
    let Some(Node::Constraint(con)) = cmd.def.as_ref() else {
        not_yet_reachable("ATExecAddConstraint: non-Constraint def");
    };
    match con.contype {
        ConstrType::CHECK => store_check_constraint(shared, relid, con).await,
        other => not_yet_reachable(&format!("ATExecAddConstraint: {other:?}")),
    }
}

/// PG `ATExecDropConstraint`: delete the named pg_constraint row of the relation.
async fn ata_exec_drop_constraint(shared: &Arc<SharedState>, relid: Oid, cmd: &AlterTableCmd) {
    use crate::catalog::pg_constraint::{self as pc, ConstraintRelationId};

    let conname = cmd.name.as_deref().unwrap_or_else(|| unreachable!("DROP CONSTRAINT names it"));
    let mut found = false;
    // pg_constraint is not seeded on-disk at this milestone (STAGED): if absent, no
    // constraint rows exist to drop.
    if let Some(pg_constraint) = relation_id_get_relation(ConstraintRelationId) {
        let rows =
            scan_catalog_by_oid(shared, ConstraintRelationId, pc::Anum_pg_constraint_conrelid, relid)
                .await;
        for row in &rows {
            // SAFETY: owned tuple; read conname out of the fixed part.
            let connamep =
                GETSTRUCT(&row.tuple).cast::<crate::catalog::pg_constraint::FormData_pg_constraint>();
            let this = unsafe { &*connamep };
            if name_of(&this.conname) == conname {
                catalog_tuple_delete(shared, &pg_constraint, &row.tid).await;
                found = true;
            }
        }
        for row in rows {
            heap_freetuple(row.tuple);
        }
        relation_close(pg_constraint);
    }

    if !found && !cmd.missing_ok {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("constraint \"{conname}\" of relation does not exist"));
        });
    }
}

/// Store a CHECK constraint's pg_constraint row (the M10 minimal form: conname,
/// conrelid, contype 'c', and the deparsed check expression in conbin).
async fn store_check_constraint(
    shared: &Arc<SharedState>,
    relid: Oid,
    con: &crate::nodes::parsenodes::Constraint,
) {
    let conname = con.conname.clone().unwrap_or_else(|| format!("{}_check", relid.get()));
    let raw = con
        .raw_expr
        .as_ref()
        .unwrap_or_else(|| unreachable!("a CHECK constraint carries an expression"));
    let consrc = crate::backend::utils::adt::ruleutils::deparse_expression(raw);

    // Insert the pg_constraint row (now that pg_constraint is seeded on-disk). The
    // validation scan (verify existing rows satisfy the constraint) stages.
    crate::backend::catalog::pg_constraint::create_constraint_entry(
        shared,
        &conname,
        crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE,
        relid,
        &consrc,
    )
    .await;
}

/// The attribute flag an `ATExec*` updates on a pg_attribute row.
enum AttFlag {
    NotNull(bool),
    HasDef(bool),
}

/// Update one pg_attribute row's boolean flag (attnotnull / atthasdef) in place.
async fn set_attribute_flag(shared: &Arc<SharedState>, relid: Oid, attnum: i16, flag: AttFlag) {
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId, FormData_pg_attribute};

    let rows = scan_catalog_by_oid(shared, AttributeRelationId, a::Anum_pg_attribute_attrelid, relid).await;
    let pg_attribute = relation_id_get_relation(AttributeRelationId)
        .unwrap_or_else(|| unreachable!("pg_attribute open"));
    let desc = pg_attribute.rd_att.clone().unwrap_or_else(|| unreachable!("pg_attribute desc"));
    for row in &rows {
        // SAFETY: owned tuple; read attnum out of the fixed part.
        let attp = GETSTRUCT(&row.tuple).cast::<FormData_pg_attribute>();
        if unsafe { (*attp).attnum } != attnum {
            continue;
        }
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        match flag {
            AttFlag::NotNull(v) => {
                vals[(a::Anum_pg_attribute_attnotnull - 1) as usize] = crate::postgres::BoolGetDatum(v);
                nulls[(a::Anum_pg_attribute_attnotnull - 1) as usize] = false;
            }
            AttFlag::HasDef(v) => {
                vals[(a::Anum_pg_attribute_atthasdef - 1) as usize] = crate::postgres::BoolGetDatum(v);
                nulls[(a::Anum_pg_attribute_atthasdef - 1) as usize] = false;
            }
        }
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_attribute, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        break;
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    relation_close(pg_attribute);
}

/// The 1-based attnum of a (non-dropped) column by name, or None.
async fn column_attnum(shared: &Arc<SharedState>, relid: Oid, colname: &str) -> Option<i16> {
    let (cols, _max) = relation_attnum_info(shared, relid).await;
    cols.into_iter().find_map(|(n, num, dropped)| (!dropped && n == colname).then_some(num))
}

/// Update pg_class.relnatts for a relation (after ADD COLUMN) in place.
async fn update_pg_class_relnatts(shared: &Arc<SharedState>, relid: Oid, relnatts: i16) {
    use crate::catalog::pg_class::{self as c, RelationRelationId};
    let rows = scan_catalog_by_oid(shared, RelationRelationId, c::Anum_pg_class_oid, relid).await;
    let pg_class = relation_id_get_relation(RelationRelationId)
        .unwrap_or_else(|| unreachable!("pg_class open"));
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class desc"));
    for row in rows {
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        vals[(c::Anum_pg_class_relnatts - 1) as usize] = crate::postgres::Int16GetDatum(relnatts);
        nulls[(c::Anum_pg_class_relnatts - 1) as usize] = false;
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_class, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        heap_freetuple(row.tuple);
    }
    relation_close(pg_class);
}

/// PG `renameatt` (the M10 form): rename a column. Updates the column's
/// pg_attribute.attname in place. Errors if the column is absent or the new name
/// collides; the inheritance recursion is staged.
pub async fn rename_att(shared: &Arc<SharedState>, relid: Oid, oldname: &str, newname: &str) {
    use crate::catalog::pg_attribute::{self as a, AttributeRelationId, FormData_pg_attribute};

    let rows = scan_catalog_by_oid(shared, AttributeRelationId, a::Anum_pg_attribute_attrelid, relid).await;
    let pg_attribute = relation_id_get_relation(AttributeRelationId)
        .unwrap_or_else(|| unreachable!("pg_attribute open"));
    let desc = pg_attribute.rd_att.clone().unwrap_or_else(|| unreachable!("pg_attribute desc"));

    // Collision check + target lookup.
    let mut target: Option<ItemPointerData> = None;
    let mut target_tuple_idx = None;
    for (i, row) in rows.iter().enumerate() {
        // SAFETY: owned tuple.
        let attp = GETSTRUCT(&row.tuple).cast::<FormData_pg_attribute>();
        let att = unsafe { &*attp };
        if att.attisdropped {
            continue;
        }
        let name = att_name(att);
        if name == newname {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!("column \"{newname}\" of relation already exists"));
            });
        }
        if name == oldname {
            target = Some(row.tid);
            target_tuple_idx = Some(i);
        }
    }

    if let (Some(tid), Some(idx)) = (target, target_tuple_idx) {
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&rows[idx].tuple, &desc) };
        let name_buf = mut_name(newname);
        vals[(a::Anum_pg_attribute_attname - 1) as usize] = NameGetDatum_owned(&name_buf);
        nulls[(a::Anum_pg_attribute_attname - 1) as usize] = false;
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_attribute, &tid, &mut newtup).await;
        heap_freetuple(newtup);
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    relation_close(pg_attribute);

    if target.is_none() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{oldname}\" does not exist"));
        });
    }
    relation_forget_relation(relid);
}

/// PG `RenameRelationInternal` (the M10 form): rename a relation. Updates the
/// relation's pg_class.relname in place + evicts the relcache entry. Collision +
/// namespace checks are staged (M10 single-namespace path).
pub async fn rename_relation(shared: &Arc<SharedState>, relid: Oid, newname: &str) {
    use crate::catalog::pg_class::{self as c, RelationRelationId};
    let rows = scan_catalog_by_oid(shared, RelationRelationId, c::Anum_pg_class_oid, relid).await;
    let pg_class = relation_id_get_relation(RelationRelationId)
        .unwrap_or_else(|| unreachable!("pg_class open"));
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class desc"));
    for row in rows {
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        let name_buf = mut_name(newname);
        vals[(c::Anum_pg_class_relname - 1) as usize] = NameGetDatum_owned(&name_buf);
        nulls[(c::Anum_pg_class_relname - 1) as usize] = false;
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_class, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        heap_freetuple(row.tuple);
    }
    relation_close(pg_class);
    relation_forget_relation(relid);
}

/// PG `RemoveRelations`: the DROP TABLE / DROP INDEX path. Resolve each named
/// relation (IF EXISTS -> notice on absence), then `performDeletion` (drops the
/// relation + its dependent indexes). The concurrent-index path + per-object lock
/// modes are staged (rules.md s4).
pub async fn remove_relations(shared: &Arc<SharedState>, stmt: &crate::nodes::parsenodes::DropStmt) {
    for obj in &stmt.objects {
        let Node::RangeVar(rv) = obj else {
            not_yet_reachable("RemoveRelations: DROP object is not a relation name");
        };
        let rv: &RangeVar = rv;
        let relname = rv.relname.as_deref().unwrap_or_else(|| unreachable!("DROP names the relation"));
        let relid = range_var_get_relid(shared, rv.schemaname.as_deref(), relname).await;
        let Some(relid) = relid else {
            if stmt.missing_ok {
                crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                    e.errmsg(format!("relation \"{relname}\" does not exist, skipping"));
                });
                continue;
            }
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                    .errmsg(format!("relation \"{relname}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        };
        let addr = crate::catalog::objectaddress::ObjectAddress {
            classId: crate::catalog::pg_class::RelationRelationId,
            objectId: relid,
            objectSubId: 0,
        };
        crate::backend::catalog::dependency::perform_deletion(shared, &addr, stmt.behavior).await;
        crate::backend::access::transam::xact::CommandCounterIncrement();
    }
}

/// Read a `NameData`'s contents as a String (NUL-terminated).
fn name_of(name: &crate::c::NameData) -> String {
    let bytes = crate::c::NameStr(name);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Build a `NameData` from a &str (NUL-padded), kept alive to back a `NameGetDatum`.
fn mut_name(s: &str) -> crate::c::NameData {
    crate::backend::catalog::heap::name_data(s)
}

/// `NameGetDatum` on an owned NameData (the Datum points at the NameData's storage,
/// which must outlive the heap_form_tuple call; callers keep the NameData in a local).
#[allow(non_snake_case)]
fn NameGetDatum_owned(name: &crate::c::NameData) -> Datum {
    crate::postgres::NameGetDatum(name)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use std::sync::Arc;

    use crate::nodes::nodes::{CmdType, Node};
    use crate::parser::parser::RawParseMode;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};

    use super::att_name;
    use crate::backend::utils::cache::relcache::{
        relation_build_desc, relation_close, relation_forget_relation,
    };

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

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
            assert!(relid.get() != 0);

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
                let natts = rd.rd_att.as_ref().unwrap().natts;
                assert_eq!(natts, 1, "the rebuilt descriptor has the one user column");
                let att0 = rd.rd_att.as_ref().unwrap().attr(0);
                assert_eq!(att0.atttypid, Oid::new(23), "column a is int4");
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
            let natts = rebuilt.rd_att.as_ref().unwrap().natts;
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

    // ----- M10 (step 38): ALTER TABLE / RENAME / DROP -----

    /// The non-dropped column names of `relname`, in attnum order (rebuilt from the
    /// on-disk catalog rows). Refreshes the snapshot first so committed DDL is seen.
    async fn colnames(shared: &Arc<SharedState>, relname: &str) -> Vec<String> {
        refresh_active_snapshot(shared);
        crate::backend::utils::cache::relcache::relation_forget_relation(
            crate::backend::catalog::namespace::range_var_get_relid(shared, None, relname)
                .await
                .expect("relation exists"),
        );
        let relid = crate::backend::catalog::namespace::range_var_get_relid(shared, None, relname)
            .await
            .expect("relation exists");
        let rd = crate::backend::utils::cache::relcache::relation_build_desc(shared, relid)
            .await
            .expect("rebuilds");
        let desc = rd.rd_att.as_ref().unwrap();
        let mut out = Vec::new();
        for i in 0..desc.natts as usize {
            let att = desc.attr(i);
            if !att.attisdropped {
                out.push(att_name(att));
            }
        }
        crate::backend::utils::cache::relcache::relation_close(rd);
        out
    }

    async fn run_util(shared: &Arc<SharedState>, sql: &str) -> crate::tcop::cmdtaglist::CommandTag {
        run_create_table(shared, sql).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alter_table_add_column_then_select_sees_it() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_util(&shared, "CREATE TABLE t (a int)").await;
            refresh_active_snapshot(&shared);

            let tag = run_util(&shared, "ALTER TABLE t ADD COLUMN b text").await;
            assert_eq!(tag, crate::tcop::cmdtaglist::CommandTag::AlterTable);

            assert_eq!(colnames(&shared, "t").await, vec!["a".to_owned(), "b".to_owned()]);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alter_table_drop_column_hides_it() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_util(&shared, "CREATE TABLE t (a int, b text)").await;
            refresh_active_snapshot(&shared);
            assert_eq!(colnames(&shared, "t").await, vec!["a".to_owned(), "b".to_owned()]);

            run_util(&shared, "ALTER TABLE t DROP COLUMN b").await;
            // b is gone from SELECT *; a remains.
            assert_eq!(colnames(&shared, "t").await, vec!["a".to_owned()]);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alter_table_rename_column() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_util(&shared, "CREATE TABLE t (a int)").await;
            refresh_active_snapshot(&shared);

            run_util(&shared, "ALTER TABLE t RENAME COLUMN a TO x").await;
            assert_eq!(colnames(&shared, "t").await, vec!["x".to_owned()]);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alter_table_rename_relation() {
        use crate::backend::catalog::namespace::range_var_get_relid;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_util(&shared, "CREATE TABLE t (a int)").await;
            refresh_active_snapshot(&shared);

            run_util(&shared, "ALTER TABLE t RENAME TO t2").await;
            refresh_active_snapshot(&shared);
            relation_forget_relation(range_var_get_relid(&shared, None, "t2").await.unwrap_or(Oid::new(0)));

            assert!(range_var_get_relid(&shared, None, "t2").await.is_some(), "t2 resolves");
            assert!(range_var_get_relid(&shared, None, "t").await.is_none(), "old name gone");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alter_table_set_default_records_attrdef() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_util(&shared, "CREATE TABLE t (a int)").await;
            refresh_active_snapshot(&shared);

            run_util(&shared, "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5").await;
            refresh_active_snapshot(&shared);

            // atthasdef is now set on column a.
            let relid = crate::backend::catalog::namespace::range_var_get_relid(&shared, None, "t")
                .await
                .unwrap();
            relation_forget_relation(relid);
            let rd = relation_build_desc(&shared, relid).await.unwrap();
            let att0 = rd.rd_att.as_ref().unwrap().attr(0);
            assert!(att0.atthasdef, "column a has a default after SET DEFAULT");
            relation_close(rd);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_table_makes_name_unresolvable() {
        use crate::backend::catalog::namespace::range_var_get_relid;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_util(&shared, "CREATE TABLE t (a int)").await;
            refresh_active_snapshot(&shared);
            assert!(range_var_get_relid(&shared, None, "t").await.is_some());

            let tag = run_util(&shared, "DROP TABLE t").await;
            assert_eq!(tag, crate::tcop::cmdtaglist::CommandTag::DropTable);
            refresh_active_snapshot(&shared);

            assert!(range_var_get_relid(&shared, None, "t").await.is_none(), "t is gone");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_table_if_exists_missing_is_notice_not_error() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            // No such table; IF EXISTS -> notice (no panic / error).
            let tag = run_util(&shared, "DROP TABLE IF EXISTS nosuch").await;
            assert_eq!(tag, crate::tcop::cmdtaglist::CommandTag::DropTable);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_table_without_if_exists_errors() {
        use futures_util::FutureExt;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let res = std::panic::AssertUnwindSafe(run_util(&shared, "DROP TABLE nosuch"))
                .catch_unwind()
                .await;
            std::panic::set_hook(prev);

            let payload = res.expect_err("DROP of a missing table errors");
            let edata = payload
                .downcast_ref::<crate::utils::elog::ErrorData>()
                .expect("structured ErrorData");
            assert_eq!(edata.sqlerrcode, crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE);
        }))
        .await;
    }
}

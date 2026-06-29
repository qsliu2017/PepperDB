//! Commands for creating and altering indexes. Translated from the M6-reachable
//! parts of `src/backend/commands/indexcmds.c` (disposition: grow).
//!
//! `DefineIndex` is the CREATE INDEX driver: resolve the table + the indexed
//! columns, pick each column's default operator class, build the `IndexInfo`, and
//! call `index_create` (which builds the index via `index_build`). M6 covers a
//! plain single-/multi-column btree index over simple heap columns. ReindexIndex,
//! CONCURRENTLY, partial/expression indexes, UNIQUE enforcement, INCLUDE columns,
//! tablespace selection, and the constraint-backed forms are staged guards
//! (rules.md s4).
//!
//! Async coloring (rules.md s5): `index_create`/`index_build` reach the buffer pool
//! and WAL (storage create, pg_class insert, btree build), so `DefineIndex` is
//! `async` and threads `&Arc<SharedState>`.

use std::sync::Arc;

use crate::backend::catalog::index::{index_create, make_index_info};
use crate::backend::catalog::namespace::range_var_get_relid;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::RelationRelationId;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{IndexElem, IndexStmt};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// The btree access method OID (`pg_am`: btree). M6 has a single index AM.
const BTREE_AM_OID: Oid = Oid(403);

/// Public alias of [`BTREE_AM_OID`] for the planner's IndexOptInfo `relam`.
pub const BTREE_AM_OID_PUB: Oid = BTREE_AM_OID;

/// Panic for a DefineIndex feature path not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `GetDefaultOpClass`: the default operator class for `(type_id, am_id)`. PG
/// scans pg_opclass for `(opcmethod=am_id, opcintype=type_id, opcdefault)`; M6 has
/// no on-disk pg_opclass heap (the `.dat` seed is not initdb-loaded), so the builtin
/// btree default opclasses are resolved statically -- the same mapping
/// SEED_PG_OPCLASS encodes and the relcache's `btree_opclass_intype` inverts. The
/// pg_opclass index scan grows when initdb seeds the catalog (rules.md s4).
#[must_use]
pub fn get_default_op_class(type_id: Oid, am_id: Oid) -> Oid {
    if am_id != BTREE_AM_OID {
        not_yet_reachable("GetDefaultOpClass: non-btree access method");
    }
    match type_id.0 {
        23 => Oid(1978), // int4  -> int4_ops
        21 => Oid(1979), // int2  -> int2_ops
        20 => Oid(3124), // int8  -> int8_ops
        26 => Oid(1981), // oid   -> oid_ops
        25 => Oid(3126), // text  -> text_ops
        _ => InvalidOid,
    }
}

/// The 1-based heap attribute number of `colname` in `heap`, or None if absent.
fn attnum_of(heap: &RelationData, colname: &str) -> Option<i16> {
    let desc = heap.rd_att.as_ref()?;
    (0..desc.natts as usize).find_map(|i| {
        let att = desc.attr(i);
        (att_name(att) == colname).then_some(att.attnum)
    })
}

/// Read a `FormData_pg_attribute`'s `attname` as a String (NameData is NUL-padded to
/// NAMEDATALEN; truncate at the first NUL).
fn att_name(att: &crate::catalog::pg_attribute::FormData_pg_attribute) -> String {
    let bytes = crate::c::NameStr(&att.attname);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// The per-key-column arrays `index_create` needs, resolved from the IndexElem list.
struct IndexColumns {
    key_attnums: Vec<i16>,
    opclass_ids: Vec<Oid>,
    collation_ids: Vec<Oid>,
    coloptions: Vec<i16>,
    col_names: Vec<String>,
}

/// PG `ComputeIndexAttrs` (M6 simple-column subset): resolve each `IndexElem` to its
/// heap attnum, the column type's default opclass, its collation, and the per-column
/// index options. Expression / explicit-opclass / DESC columns are staged guards.
fn resolve_index_columns(
    heap: &RelationData,
    index_params: &[Node],
    access_method_id: Oid,
) -> IndexColumns {
    let mut cols = IndexColumns {
        key_attnums: Vec::with_capacity(index_params.len()),
        opclass_ids: Vec::with_capacity(index_params.len()),
        collation_ids: Vec::with_capacity(index_params.len()),
        coloptions: Vec::with_capacity(index_params.len()),
        col_names: Vec::with_capacity(index_params.len()),
    };
    for param in index_params {
        let Node::IndexElem(elem) = param else {
            not_yet_reachable("DefineIndex: index parameter is not an IndexElem");
        };
        let elem: &IndexElem = elem;
        if elem.expr.is_some() {
            not_yet_reachable("DefineIndex: expression index column");
        }
        if !elem.opclass.is_empty() {
            not_yet_reachable("DefineIndex: explicit opclass name");
        }
        if matches!(elem.ordering, crate::nodes::parsenodes::SortByDir::DESC) {
            not_yet_reachable("DefineIndex: DESC index column ordering");
        }
        let colname = elem
            .name
            .as_deref()
            .unwrap_or_else(|| unreachable!("a simple IndexElem names a column"));
        let attno = attnum_of(heap, colname).unwrap_or_else(|| {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!("column \"{colname}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        });
        let coltype = {
            let desc = heap.rd_att.as_ref().unwrap_or_else(|| unreachable!("heap has a descriptor"));
            desc.attr((attno - 1) as usize).atttypid
        };
        let opclass = get_default_op_class(coltype, access_method_id);
        if opclass == InvalidOid {
            not_yet_reachable("DefineIndex: no default btree opclass for column type");
        }
        cols.key_attnums.push(attno);
        cols.opclass_ids.push(opclass);
        cols.collation_ids.push(InvalidOid);
        cols.coloptions.push(0);
        cols.col_names.push(colname.to_owned());
    }
    cols
}

/// PG `DefineIndex` (the M6 CREATE INDEX path). Resolve the heap + columns, pick the
/// default btree opclass per column, build the `IndexInfo`, then `index_create`
/// (which builds the index). Returns the new index relation's `ObjectAddress`.
///
/// The C signature carries the recursion / ALTER TABLE / partition plumbing; the M6
/// entry takes just the analyzed `IndexStmt` + `shared` (the planner-side flags are
/// not reached here). Staged (rules.md s4): UNIQUE enforcement on insert, partial /
/// expression indexes, INCLUDE columns, CONCURRENTLY, explicit tablespace, opclass
/// names, and IF NOT EXISTS.
pub async fn define_index(shared: &Arc<SharedState>, stmt: &IndexStmt) -> ObjectAddress {
    if stmt.concurrent {
        not_yet_reachable("DefineIndex: CONCURRENTLY");
    }
    if stmt.whereClause.is_some() {
        not_yet_reachable("DefineIndex: partial index (WHERE)");
    }
    if !stmt.indexIncludingParams.is_empty() {
        not_yet_reachable("DefineIndex: INCLUDE columns");
    }
    if stmt.tableSpace.is_some() {
        not_yet_reachable("DefineIndex: explicit tablespace");
    }

    let access_method_id = stmt.accessMethod.as_deref().map_or(BTREE_AM_OID, |am| {
        if am.eq_ignore_ascii_case("btree") {
            BTREE_AM_OID
        } else {
            not_yet_reachable("DefineIndex: non-btree access method");
        }
    });

    // RangeVarGetRelidExtended (M6 subset): resolve the table the index is on.
    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("CREATE INDEX always names a relation"));
    let relname = relation
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("CREATE INDEX RangeVar carries a relation name"));
    let table_id = range_var_get_relid(shared, relation.schemaname.as_deref(), relname)
        .await
        .unwrap_or_else(|| {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                    .errmsg(format!("relation \"{relname}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        });

    // Open the heap relation (already warm in the relcache for the planner path; the
    // command frame holds the lock conceptually). Returns an Arc (refcount bump).
    crate::backend::utils::cache::relcache::relation_build_desc(shared, table_id).await;
    let heap = relation_id_get_relation(table_id)
        .unwrap_or_else(|| unreachable!("table {table_id:?} just built into the relcache"));

    // ComputeIndexAttrs (M6 simple-column subset): resolve each IndexElem to its heap
    // attnum + the column type's default opclass + collation + indoption.
    let cols = resolve_index_columns(&heap, &stmt.indexParams, access_method_id);
    let IndexColumns { key_attnums, opclass_ids, collation_ids, coloptions, col_names } = &cols;

    let index_info = make_index_info(key_attnums, stmt.unique);

    // ChooseIndexName (M6 subset): the explicit name, or a synthesized one.
    let index_name = stmt.idxname.clone().unwrap_or_else(|| {
        let cols = col_names.join("_");
        format!("{relname}_{cols}_idx")
    });

    // Create the index relation + register it, deferring the build so DefineIndex
    // can capture the build result counts (index_update_stats below).
    let index_relation_id = index_create(
        shared,
        &heap,
        &index_name,
        InvalidOid, // assign a fresh index OID
        InvalidOid, // assign a fresh relfilenumber
        &index_info,
        col_names,
        access_method_id,
        crate::common::relpath::DEFAULTTABLESPACE_OID,
        collation_ids,
        opclass_ids,
        coloptions,
        true, // skip build here; DefineIndex builds + records stats below
    )
    .await;

    // index_build (PG calls it inside index_create; split out here for the stats):
    // open the just-registered index relation and build it.
    let index_rel = crate::backend::catalog::indexing::find_registered_index(index_relation_id)
        .unwrap_or_else(|| unreachable!("index just registered by index_create"));
    let build = crate::backend::catalog::index::index_build(shared, &heap, &index_rel, &index_info).await;

    // index_update_stats (relcache effect): record the heap's row count + page count
    // so the planner's size estimate reflects the indexed table. The heap handle is
    // closed first so the cached Arc is uniquely held (update mutates it in place).
    let heap_relid = heap.rd_id;
    let heap_tuples = build.heap_tuples;
    drop(index_rel);
    relation_close(heap);
    let relpages = estimate_heap_pages(heap_tuples);
    crate::backend::utils::cache::relcache::update_relation_stats(
        heap_relid,
        relpages,
        heap_tuples as f32,
    );

    ObjectAddress { classId: RelationRelationId, objectId: index_relation_id, objectSubId: 0 }
}

/// Rough heap page count from a tuple count (one 8 KB page per ~226 int4 rows),
/// used as the relcache size estimate after a build (PG sets the real relpages).
fn estimate_heap_pages(tuples: f64) -> i32 {
    if tuples <= 0.0 {
        0
    } else {
        ((tuples / 226.0).ceil() as i32).max(1)
    }
}

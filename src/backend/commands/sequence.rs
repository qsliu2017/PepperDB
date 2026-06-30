//! Sequence commands. Translated from the M10/step-39 parts of
//! `src/backend/commands/sequence.c` (disposition: grow).
//!
//! `define_sequence` creates the sequence relation (RELKIND_SEQUENCE, the three
//! fixed columns `last_value int8`, `log_cnt int8`, `is_called bool`), writes the
//! single data tuple to block 0, and inserts the pg_sequence catalog row.
//! `nextval`/`currval`/`setval` read/update the on-disk data tuple. The cache,
//! WAL `log_cnt` lookahead, RESTART/RELATION-rename plumbing, OWNED BY, and the
//! multi-backend session table all STAGE: the single-backend on-disk increment is
//! reproduced faithfully (the value persists across calls), which is what SERIAL /
//! identity will build on later.
//!
//! Async coloring (rules.md s5): every path reaches the buffer pool, so the
//! commands are `async` and thread `&Arc<SharedState>`. The in-buffer tuple update
//! is done under the exclusive content lock with no `.await` in between (the C
//! critical section), mirroring `heap_insert`.

use std::sync::Arc;

use crate::backend::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple, heap_freetuple};
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::access::transam::xact::GetCurrentCommandId;
use crate::backend::catalog::catalog::get_new_object_id;
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::catalog::namespace::range_var_get_relid;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::access::htup::tuple_body_from_raw;
use crate::access::htup::HeapTupleData;
use crate::access::tupdesc::TupleDescData;
use crate::catalog::genbki::{BOOLOID, INT8OID};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::{RelationRelationId, RELKIND_SEQUENCE, RELPERSISTENCE_PERMANENT};
use crate::catalog::pg_sequence::{self as ps, SequenceRelationId};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{AlterSeqStmt, CreateSeqStmt};
use crate::postgres::{
    BoolGetDatum, Datum, DatumGetBool, DatumGetInt64, Int64GetDatum, ObjectIdGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::off::FIRST_OFFSET_NUMBER;

/// `pg_sequence_data` (commands/sequence.h): the on-disk sequence tuple's columns.
/// All three are fixed-width / NOT NULL, so the tuple's byte layout is constant and
/// `nextval`/`setval` can overwrite the value bytes in place.
const SEQ_COL_LASTVAL: usize = 0;
const SEQ_COL_LOG: usize = 1;
const SEQ_COL_CALLED: usize = 2;

/// The parameters parsed from a sequence option list, mirroring PG's
/// `FormData_pg_sequence` plus the data-tuple seed (`last_value`, `is_called`).
struct SeqParams {
    seqtypid: Oid,
    increment: i64,
    minvalue: i64,
    maxvalue: i64,
    start: i64,
    cache: i64,
    cycle: bool,
    last_value: i64,
}

/// PG `init_params` (M10 subset): resolve a sequence's option list into its
/// `FormData_pg_sequence` parameters + the data-tuple seed. AS type defaults to
/// int8; the ascending/descending MIN/MAX/START defaults follow PG. RESTART, the
/// type-range revalidation, and OWNED BY stage.
fn init_params(options: &[Node]) -> SeqParams {
    // Sequence option values are A_Const literals: INCREMENT/START/... are Float
    // consts (PG's NumericOnly carries ICONST as a Float text); CYCLE is an int
    // const 0/1. Read the const value out of the DefElem's arg.
    let opt_const = |name: &str| -> Option<&crate::nodes::parsenodes::ValUnion> {
        options.iter().find_map(|n| match n {
            Node::DefElem(d) if d.defname.as_deref() == Some(name) => match &d.arg {
                Some(Node::A_Const(c)) => Some(&c.val),
                _ => None,
            },
            _ => None,
        })
    };
    let opt_int = |name: &str| -> Option<i64> {
        use crate::nodes::parsenodes::ValUnion;
        match opt_const(name)? {
            ValUnion::Float(f) => f.fval.parse::<i64>().ok(),
            ValUnion::Integer(i) => Some(i64::from(i.ival)),
            _ => None,
        }
    };
    let opt_bool = |name: &str| -> Option<bool> {
        use crate::nodes::parsenodes::ValUnion;
        match opt_const(name)? {
            ValUnion::Integer(i) => Some(i.ival != 0),
            ValUnion::Boolean(b) => Some(b.boolval),
            _ => Some(true),
        }
    };

    // AS type: the grammar drops the typename (stores a marker); default int8.
    let seqtypid = INT8OID;

    let increment = opt_int("increment").unwrap_or(1);
    let cycle = opt_bool("cycle").unwrap_or(false);

    // MAX/MIN default by direction and type. int8 bounds are the reachable case.
    let maxvalue = opt_int("maxvalue").unwrap_or(if increment > 0 { i64::MAX } else { -1 });
    let minvalue = opt_int("minvalue").unwrap_or(if increment > 0 { 1 } else { i64::MIN });

    // START defaults to minvalue (ascending) or maxvalue (descending).
    let start = opt_int("start").unwrap_or(if increment > 0 { minvalue } else { maxvalue });

    let cache = opt_int("cache").unwrap_or(1);

    SeqParams {
        seqtypid,
        increment,
        minvalue,
        maxvalue,
        start,
        cache,
        cycle,
        // is_called starts false: the first nextval returns `start` itself.
        last_value: start,
    }
}

/// The default-heap-AM OID (pg_am `heap`). Sequences are stored as heap relations.
const HEAP_TABLE_AM_OID: Oid = Oid::new(2);

/// The three-column tuple descriptor of a sequence relation: `last_value int8`,
/// `log_cnt int8`, `is_called bool`. Built directly (PG forms `ColumnDef`s and runs
/// them through DefineRelation; the descriptor is the same shape).
fn seq_tuple_desc() -> crate::access::tupdesc::TupleDesc {
    let mut desc = TupleDescData::create_template(3);
    desc.init_entry(1, Some("last_value"), INT8OID, -1, 0);
    desc.init_entry(2, Some("log_cnt"), INT8OID, -1, 0);
    desc.init_entry(3, Some("is_called"), BOOLOID, -1, 0);
    Arc::new(desc)
}

/// PG `DefineSequence`: CREATE SEQUENCE. Creates the sequence relation, writes its
/// single data tuple to block 0, then inserts the pg_sequence catalog row. IF NOT
/// EXISTS on an existing sequence name is a no-op. OWNED BY, TEMP persistence, and
/// the schema-qualified namespace resolution beyond `public` stage.
pub async fn define_sequence(shared: &Arc<SharedState>, stmt: &CreateSeqStmt) -> ObjectAddress {
    let rv = stmt
        .sequence
        .as_deref()
        .unwrap_or_else(|| unreachable!("CREATE SEQUENCE always carries a RangeVar"));
    let seqname = rv
        .relname
        .as_deref()
        .unwrap_or_else(|| unreachable!("CREATE SEQUENCE always names the sequence"));

    // IF NOT EXISTS: a no-op if the sequence already exists.
    if stmt.if_not_exists
        && let Some(existing) =
            range_var_get_relid(shared, rv.schemaname.as_deref(), seqname).await
    {
        crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
            e.errmsg(format!("relation \"{seqname}\" already exists, skipping"));
        });
        return ObjectAddress { classId: RelationRelationId, objectId: existing, objectSubId: 0 };
    }

    let params = init_params(&stmt.options);

    // 1. Create the sequence relation (storage + pg_class/pg_attribute/pg_type).
    let namespace_id = match rv.schemaname.as_deref() {
        None => crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE,
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
    let owner_id = crate::backend::utils::init::miscinit::get_user_id();

    let seqrelid = crate::backend::catalog::heap::heap_create_with_catalog(
        shared,
        seqname,
        namespace_id,
        crate::common::relpath::DEFAULTTABLESPACE_OID,
        InvalidOid,
        InvalidOid,
        owner_id,
        HEAP_TABLE_AM_OID,
        seq_tuple_desc(),
        RELKIND_SEQUENCE,
        RELPERSISTENCE_PERMANENT,
        false,
    )
    .await;
    crate::backend::access::transam::xact::CommandCounterIncrement();

    // 2. Write the initial data tuple {last_value=start, log_cnt=0, is_called=false}.
    let seqrel = open_seqrel(shared, seqrelid).await;
    let desc = seqrel.rd_att.clone().unwrap_or_else(|| unreachable!("sequence has a descriptor"));
    let values = [
        Int64GetDatum(params.last_value),
        Int64GetDatum(0),
        BoolGetDatum(false),
    ];
    let isnull = [false, false, false];
    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, &seqrel, &mut tup, cid, 0).await;
    heap_freetuple(tup);
    relation_close(seqrel);

    // 3. Insert the pg_sequence catalog row.
    insert_pg_sequence_row(shared, seqrelid, &params).await;

    ObjectAddress { classId: RelationRelationId, objectId: seqrelid, objectSubId: 0 }
}

/// Insert (or, for ALTER, the caller deletes the old first) the pg_sequence row that
/// records a sequence's parameters.
async fn insert_pg_sequence_row(shared: &Arc<SharedState>, seqrelid: Oid, p: &SeqParams) {
    let pg_sequence = relation_id_get_relation(SequenceRelationId)
        .unwrap_or_else(|| unreachable!("pg_sequence is seeded/open"));
    let desc = pg_sequence.rd_att.clone().unwrap_or_else(|| unreachable!("pg_sequence desc"));
    let natts = desc.natts as usize;

    let mut values = vec![Datum(0); natts];
    let isnull = vec![false; natts];
    values[(ps::Anum_pg_sequence_seqrelid - 1) as usize] = ObjectIdGetDatum(seqrelid);
    values[(ps::Anum_pg_sequence_seqtypid - 1) as usize] = ObjectIdGetDatum(p.seqtypid);
    values[(ps::Anum_pg_sequence_seqstart - 1) as usize] = Int64GetDatum(p.start);
    values[(ps::Anum_pg_sequence_seqincrement - 1) as usize] = Int64GetDatum(p.increment);
    values[(ps::Anum_pg_sequence_seqmax - 1) as usize] = Int64GetDatum(p.maxvalue);
    values[(ps::Anum_pg_sequence_seqmin - 1) as usize] = Int64GetDatum(p.minvalue);
    values[(ps::Anum_pg_sequence_seqcache - 1) as usize] = Int64GetDatum(p.cache);
    values[(ps::Anum_pg_sequence_seqcycle - 1) as usize] = BoolGetDatum(p.cycle);

    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_sequence, &mut tup).await;
    heap_freetuple(tup);
    relation_close(pg_sequence);
}

/// PG `AlterSequence` (M10 subset): re-resolve the sequence and apply new option
/// values. The simplest faithful form re-reads the existing pg_sequence row, applies
/// the changed options, deletes the old catalog row, and inserts the new one; the
/// data tuple (last_value/is_called) is left as-is unless RESTART is given (staged).
pub async fn alter_sequence(shared: &Arc<SharedState>, stmt: &AlterSeqStmt) -> ObjectAddress {
    let rv = stmt
        .sequence
        .as_deref()
        .unwrap_or_else(|| unreachable!("ALTER SEQUENCE always carries a RangeVar"));
    let seqname = rv.relname.as_deref().unwrap_or("");
    let Some(seqrelid) = range_var_get_relid(shared, rv.schemaname.as_deref(), seqname).await
    else {
        if stmt.missing_ok {
            return ObjectAddress { classId: RelationRelationId, objectId: InvalidOid, objectSubId: 0 };
        }
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!("relation \"{seqname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    };

    if !stmt.options.is_empty() {
        // Read the current parameters, overlay the changed ones, re-store the row.
        let cur = read_pg_sequence_row(shared, seqrelid).await;
        let next = init_params_alter(&stmt.options, &cur);
        delete_pg_sequence_row(shared, seqrelid).await;
        insert_pg_sequence_row(shared, seqrelid, &next).await;
    }

    ObjectAddress { classId: RelationRelationId, objectId: seqrelid, objectSubId: 0 }
}

/// ALTER variant of `init_params`: overlay only the named options on the existing
/// parameters (`cur`), leaving the rest unchanged.
fn init_params_alter(options: &[Node], cur: &SeqParams) -> SeqParams {
    let parsed = init_params(options);
    let has = |name: &str| options.iter().any(|n| matches!(n, Node::DefElem(d) if d.defname.as_deref() == Some(name)));
    SeqParams {
        seqtypid: cur.seqtypid,
        increment: if has("increment") { parsed.increment } else { cur.increment },
        minvalue: if has("minvalue") { parsed.minvalue } else { cur.minvalue },
        maxvalue: if has("maxvalue") { parsed.maxvalue } else { cur.maxvalue },
        start: if has("start") { parsed.start } else { cur.start },
        cache: if has("cache") { parsed.cache } else { cur.cache },
        cycle: if has("cycle") { parsed.cycle } else { cur.cycle },
        last_value: cur.last_value,
    }
}

/// PG `nextval_internal` (single-backend core): increment the sequence's on-disk
/// data tuple and return the new value. The first call (`is_called == false`)
/// returns `last_value` itself and sets `is_called`. Overflow/underflow either
/// wraps (CYCLE) or errors. The WAL `log_cnt` lookahead cache stages: every call
/// persists the value, which is the correctness contract the tests check.
pub async fn nextval(shared: &Arc<SharedState>, seqrelid: Oid) -> i64 {
    let p = read_pg_sequence_row(shared, seqrelid).await;
    let (last_value, _log_cnt, is_called) = read_seq_data(shared, seqrelid).await;

    let incby = p.increment;
    let maxv = p.maxvalue;
    let minv = p.minvalue;

    let result: i64 = if !is_called {
        // First call: the seed value itself, no increment.
        last_value
    } else if incby > 0 {
        // Ascending. Overflow if last_value + incby would exceed maxv.
        if (maxv >= 0 && last_value > maxv - incby) || (maxv < 0 && last_value + incby > maxv) {
            if !p.cycle {
                seq_range_error(seqrelid, true);
            }
            minv
        } else {
            last_value + incby
        }
    } else {
        // Descending. Underflow if last_value + incby would fall below minv.
        if (minv < 0 && last_value < minv - incby) || (minv >= 0 && last_value + incby < minv) {
            if !p.cycle {
                seq_range_error(seqrelid, false);
            }
            maxv
        } else {
            last_value + incby
        }
    };

    write_seq_data(shared, seqrelid, result, 0, true).await;
    result
}

/// PG `currval_oid` (single-backend form): the value most recently returned by
/// `nextval` for this sequence. Single-backend, the persisted `last_value` (when
/// `is_called`) is exactly that. Errors if `nextval` has not been called.
pub async fn currval(shared: &Arc<SharedState>, seqrelid: Oid) -> i64 {
    let (last_value, _log_cnt, is_called) = read_seq_data(shared, seqrelid).await;
    if !is_called {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE).errmsg(
                format!("currval of sequence with relid {} is not yet defined in this session", seqrelid.get()),
            );
        });
        unreachable!("ereport(ERROR) diverges");
    }
    last_value
}

/// PG `setval_internal` / `do_setval`: set the sequence's `last_value` and
/// `is_called`. Validates the value is within [minvalue, maxvalue]. `is_called`
/// controls whether the NEXT nextval returns this value (`false`) or the next one
/// (`true`); the 2-arg setval uses `true`.
pub async fn setval(shared: &Arc<SharedState>, seqrelid: Oid, next: i64, iscalled: bool) -> i64 {
    let p = read_pg_sequence_row(shared, seqrelid).await;
    if next < p.minvalue || next > p.maxvalue {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                .errmsg(format!("setval: value {next} is out of bounds for sequence (relid {})", seqrelid.get()));
        });
        unreachable!("ereport(ERROR) diverges");
    }
    write_seq_data(shared, seqrelid, next, 0, iscalled).await;
    next
}

/// Raise the ascending-overflow / descending-underflow error for a non-cycling
/// sequence (PG's "nextval: reached maximum/minimum value").
#[cold]
fn seq_range_error(seqrelid: Oid, ascending: bool) -> ! {
    let dir = if ascending { "maximum" } else { "minimum" };
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED)
            .errmsg(format!("nextval: reached {dir} value of sequence (relid {})", seqrelid.get()));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// Read the sequence's on-disk data tuple from block 0 -> `(last_value, log_cnt,
/// is_called)`. The sequence always has exactly one tuple at FIRST_OFFSET_NUMBER.
async fn read_seq_data(shared: &Arc<SharedState>, seqrelid: Oid) -> (i64, i64, bool) {
    let seqrel = open_seqrel(shared, seqrelid).await;
    let desc = seqrel.rd_att.clone().unwrap_or_else(|| unreachable!("sequence descriptor"));

    let buffer = read_seq_block0(shared, &seqrel).await;
    let pool = shared.buffers();
    let (values, _isnull) = {
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        let item_id = page.get_item_id(FIRST_OFFSET_NUMBER);
        let item = page.get_item(&item_id);
        let len = item.len();
        // SAFETY: a sequence data tuple begins with a HeapTupleHeaderData and the
        // item slice is `len` readable page bytes; copy into an owned body.
        let body = unsafe { tuple_body_from_raw(item.as_ptr(), len) };
        let tid = crate::storage::itemptr::ItemPointerData {
            blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
            posid: FIRST_OFFSET_NUMBER,
        };
        let mut tup = HeapTupleData::null(tid, seqrelid);
        tup.body = Some(body);
        tup.t_len = len as u32;
        // SAFETY: tup owns a valid heap-tuple body of the sequence's descriptor.
        unsafe { heap_deform_tuple(&tup, &desc) }
    };
    pool.release_buffer(buffer);
    relation_close(seqrel);

    let last_value = DatumGetInt64(values[SEQ_COL_LASTVAL]);
    let log_cnt = DatumGetInt64(values[SEQ_COL_LOG]);
    let is_called = DatumGetBool(values[SEQ_COL_CALLED]);
    (last_value, log_cnt, is_called)
}

/// Overwrite the sequence's on-disk data tuple in place. The three columns are
/// fixed-width and NOT NULL, so the byte layout is constant and we can patch the
/// value bytes (under the exclusive content lock, no `.await` -- the C critical
/// section) without re-forming the tuple.
async fn write_seq_data(
    shared: &Arc<SharedState>,
    seqrelid: Oid,
    last_value: i64,
    log_cnt: i64,
    is_called: bool,
) {
    let seqrel = open_seqrel(shared, seqrelid).await;
    let buffer = read_seq_block0(shared, &seqrel).await;
    let pool = shared.buffers();
    {
        let _g = pool.content_exclusive(buffer);
        let buf_id = buf_id_of(buffer);
        // SAFETY: exclusive content lock held -> sole writer to this slot.
        let page = unsafe { pool.block_mut(buf_id) };
        let item_id = page.get_item_id(FIRST_OFFSET_NUMBER);
        let off = item_id.lp_off() as usize;
        // The heap-tuple body starts at t_hoff within the item. Read t_hoff (the
        // header's last byte before the optional null bitmap) and patch the three
        // fixed-width fields {int8, int8, bool}.
        let t_hoff = page_byte(page, off + T_HOFF_OFFSET) as usize;
        let data = off + t_hoff;
        write_i64(page, data, last_value);
        write_i64(page, data + 8, log_cnt);
        write_bool(page, data + 16, is_called);
        pool.mark_buffer_dirty(buffer);
    }
    pool.release_buffer(buffer);
    relation_close(seqrel);
}

/// Offset of `t_hoff` within a `HeapTupleHeaderData` (asserted == 22 in htup_details).
const T_HOFF_OFFSET: usize = 22;

/// Read a byte from the page's raw image at `offset`.
fn page_byte(page: &crate::storage::bufpage::Page, offset: usize) -> u8 {
    page.as_bytes()[offset]
}

/// Overwrite an 8-byte little-endian i64 in the page's raw image.
fn write_i64(page: &mut crate::storage::bufpage::Page, offset: usize, v: i64) {
    page.as_mut_bytes()[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
}

/// Overwrite a bool byte in the page's raw image.
fn write_bool(page: &mut crate::storage::bufpage::Page, offset: usize, v: bool) {
    page.as_mut_bytes()[offset] = u8::from(v);
}

/// Open a sequence relation by OID, building its relcache descriptor first so a
/// just-created sequence (whose entry is not yet cached) resolves. Returns the
/// pinned relcache entry (caller closes it).
async fn open_seqrel(
    shared: &Arc<SharedState>,
    seqrelid: Oid,
) -> Arc<crate::utils::rel::RelationData> {
    if let Some(rel) = relation_id_get_relation(seqrelid) {
        return rel;
    }
    // The just-created sequence's pg_class/pg_attribute rows were inserted in this
    // command; drop the cached catalog snapshot so the rebuild scan sees them.
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    crate::backend::utils::cache::relcache::relation_build_desc(shared, seqrelid)
        .await
        .unwrap_or_else(|| unreachable!("sequence relation {seqrelid:?} exists"))
}

/// Read block 0 of the sequence relation into the buffer pool (pinned, not locked).
async fn read_seq_block0(
    shared: &Arc<SharedState>,
    seqrel: &crate::utils::rel::RelationData,
) -> crate::storage::buf::Buffer {
    let relpersistence = seqrel.form().relpersistence;
    let smgr_ptr = seqrel.smgr();
    // SAFETY: relcache-owned smgr handle valid while the relation is open.
    let smgr = unsafe { &mut *smgr_ptr };
    crate::backend::storage::buffer::bufmgr::read_buffer_common(
        shared,
        smgr,
        relpersistence,
        crate::common::relpath::ForkNumber::MAIN_FORKNUM,
        0,
        crate::storage::bufmgr::ReadBufferMode::NORMAL,
        None,
    )
    .await
}

/// The global buffer-pool slot index for a pinned shared buffer.
fn buf_id_of(buffer: crate::storage::buf::Buffer) -> i32 {
    match buffer {
        crate::storage::buf::BufId::Global(id) => id as i32,
        crate::storage::buf::BufId::Local(_) => unreachable!("sequence pages are shared buffers"),
        crate::storage::buf::BufId::Invalid => unreachable!("a pinned buffer is valid"),
    }
}

/// Read a sequence's pg_sequence catalog row into `SeqParams` (the data-tuple seed
/// `last_value` is read separately by `read_seq_data`; here it is left at `start`).
async fn read_pg_sequence_row(shared: &Arc<SharedState>, seqrelid: Oid) -> SeqParams {
    use crate::backend::access::common::heaptuple::heap_copytuple;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };

    let pg_sequence = relation_id_get_relation(SequenceRelationId)
        .unwrap_or_else(|| unreachable!("pg_sequence is open"));
    let desc = pg_sequence.rd_att.clone().unwrap_or_else(|| unreachable!("pg_sequence desc"));

    let key = [seq_relid_scankey(seqrelid)];
    let snap = systable_scan_snapshot(shared, &pg_sequence, None);
    let mut scan = systable_beginscan(shared, &pg_sequence, InvalidOid, false, &snap, &key);

    let mut params = SeqParams {
        seqtypid: INT8OID,
        increment: 1,
        minvalue: 1,
        maxvalue: i64::MAX,
        start: 1,
        cache: 1,
        cycle: false,
        last_value: 1,
    };
    if let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple of the pg_sequence descriptor.
        let tup = unsafe { heap_copytuple(tref) };
        // SAFETY: tup owns a valid pg_sequence body.
        let (values, _isnull) = unsafe { heap_deform_tuple(&tup, &desc) };
        params.seqtypid = oid_from_datum(values[(ps::Anum_pg_sequence_seqtypid - 1) as usize]);
        params.start = DatumGetInt64(values[(ps::Anum_pg_sequence_seqstart - 1) as usize]);
        params.increment = DatumGetInt64(values[(ps::Anum_pg_sequence_seqincrement - 1) as usize]);
        params.maxvalue = DatumGetInt64(values[(ps::Anum_pg_sequence_seqmax - 1) as usize]);
        params.minvalue = DatumGetInt64(values[(ps::Anum_pg_sequence_seqmin - 1) as usize]);
        params.cache = DatumGetInt64(values[(ps::Anum_pg_sequence_seqcache - 1) as usize]);
        params.cycle = DatumGetBool(values[(ps::Anum_pg_sequence_seqcycle - 1) as usize]);
        params.last_value = params.start;
        heap_freetuple(tup);
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_sequence);
    params
}

/// Read an OID-typed Datum (a 32-bit value carried in a Datum).
fn oid_from_datum(d: Datum) -> Oid {
    Oid::new(d.0 as u32)
}

/// Delete a sequence's pg_sequence catalog row (the ALTER re-store path).
async fn delete_pg_sequence_row(shared: &Arc<SharedState>, seqrelid: Oid) {
    use crate::backend::access::common::heaptuple::heap_copytuple;
    use crate::backend::access::index::genam::{
        systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
    };
    use crate::backend::catalog::indexing::catalog_tuple_delete;

    let pg_sequence = relation_id_get_relation(SequenceRelationId)
        .unwrap_or_else(|| unreachable!("pg_sequence is open"));
    let key = [seq_relid_scankey(seqrelid)];
    let snap = systable_scan_snapshot(shared, &pg_sequence, None);
    let mut scan = systable_beginscan(shared, &pg_sequence, InvalidOid, false, &snap, &key);
    let mut tids = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy its TID before endscan.
        let tup = unsafe { heap_copytuple(tref) };
        tids.push(tup.t_self);
        heap_freetuple(tup);
    }
    systable_endscan(shared, &mut scan);
    for tid in &tids {
        catalog_tuple_delete(shared, &pg_sequence, tid).await;
    }
    relation_close(pg_sequence);
}

/// A scan key on `pg_sequence.seqrelid` (the pkey column).
fn seq_relid_scankey(seqrelid: Oid) -> crate::access::skey::ScanKeyData {
    crate::access::skey::ScanKeyData {
        flags: 0,
        attno: ps::Anum_pg_sequence_seqrelid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: crate::fmgr::FmgrInfo {
            fn_addr: None,
            oid: InvalidOid,
            nargs: 0,
            strict: false,
            retset: false,
            stats: 0,
            extra: 0,
            mcxt: (),
            expr: None,
        },
        argument: ObjectIdGetDatum(seqrelid),
    }
}

/// Resolve a sequence name (for the `nextval('seq')`-style call sites in tests) to
/// its relation OID via the search path.
pub async fn seqrelid_by_name(shared: &Arc<SharedState>, name: &str) -> Option<Oid> {
    range_var_get_relid(shared, None, name).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-sequence-{}-{}", std::process::id(), n));
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

    /// A command boundary + fresh active snapshot so own-xact rows from the previous
    /// command are visible to the next one.
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

    fn parse_create_seq(sql: &str) -> CreateSeqStmt {
        let mut list = crate::backend::parser::parser::raw_parser(
            sql,
            crate::parser::parser::RawParseMode::Default,
        );
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let Node::CreateSeqStmt(s) = rs.stmt.unwrap() else { panic!("not a CreateSeqStmt") };
        *s
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nextval_increments_and_persists() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_create_seq("CREATE SEQUENCE s");
            define_sequence(&shared, &stmt).await;
            bump_command(&shared);

            let id = seqrelid_by_name(&shared, "s").await.expect("sequence s exists");
            assert_eq!(nextval(&shared, id).await, 1, "first nextval is 1");
            assert_eq!(nextval(&shared, id).await, 2, "second nextval is 2");
            assert_eq!(nextval(&shared, id).await, 3, "third nextval is 3");
            assert_eq!(currval(&shared, id).await, 3, "currval is the last nextval");

            setval(&shared, id, 10, true).await;
            assert_eq!(nextval(&shared, id).await, 11, "after setval(10,true) nextval is 11");
            setval(&shared, id, 20, false).await;
            assert_eq!(nextval(&shared, id).await, 20, "after setval(20,false) nextval is 20");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nextval_with_increment_and_start() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let stmt = parse_create_seq("CREATE SEQUENCE s2 INCREMENT BY 5 START WITH 100");
            define_sequence(&shared, &stmt).await;
            bump_command(&shared);

            let id = seqrelid_by_name(&shared, "s2").await.expect("sequence s2 exists");
            assert_eq!(nextval(&shared, id).await, 100, "first nextval is START 100");
            assert_eq!(nextval(&shared, id).await, 105, "increments by 5");
            assert_eq!(nextval(&shared, id).await, 110, "increments by 5 again");
        }))
        .await;
    }
}

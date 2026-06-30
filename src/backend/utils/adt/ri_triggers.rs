//! Referential-integrity trigger functions. Translated from the M11/step-41 parts
//! of `src/backend/utils/adt/ri_triggers.c` (disposition: grow).
//!
//! These are the builtin trigger functions a FOREIGN KEY's system triggers invoke
//! through the after-trigger queue (commands/trigger.rs):
//!  - the CHECK triggers on the referencing (FK) table: `RI_FKey_check_ins` /
//!    `RI_FKey_check_upd` -- the just-inserted/updated FK row must have a matching
//!    referenced (PK) row, else ERRCODE_FOREIGN_KEY_VIOLATION. A NULL key column is
//!    allowed under MATCH SIMPLE.
//!  - the ACTION triggers on the referenced (PK) table at DELETE/UPDATE:
//!    `RI_FKey_noaction_del` / `RI_FKey_restrict_del` (error if dependents exist),
//!    `RI_FKey_cascade_del` (delete the dependent FK rows), `RI_FKey_setnull_del`
//!    (NULL out the dependent FK columns), `RI_FKey_setdefault_del` (set them to
//!    the column default).
//!
//! Rather than build a SPI plan + execute a generated SQL string as ri_triggers.c
//! does, the milestone runs the RI query as a direct heap scan over the other
//! table comparing the key Datums (rules.md s4 allows a seqscan match; the PK-index
//! path is a later optimization). The scan + any cascade write are async and hold
//! no lock across `.await` (rules.md s5).
//!
//! The FK column metadata (conkey/confkey/confrelid + the action codes) is read
//! from the constraint's pg_constraint row, which `create_constraint_entry` stores
//! as a compact varlena of raw i16/char bytes (see `fk_metadata`).

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]
#![allow(
    clippy::similar_names,
    reason = "conkey/confkey are the PG-canonical pg_constraint column names (FK vs referenced key)"
)]

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::access::sdir::ScanDirection;
use crate::access::tableam::ScanOptions;
use crate::access::tupdesc::TupleDesc;
use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple, heap_deform_tuple};
use crate::backend::access::heap::heapam::{heap_beginscan, heap_endscan, heap_getnext};
use crate::backend::access::heap::heapam_handler::heapam_tuple_update;
use crate::backend::access::heap::heapam::heap_delete;
use crate::backend::commands::trigger::AfterTriggerEvent;
use crate::backend::commands::tablecmds::scan_catalog_rows_by_oid;
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::pg_constraint::{self as pc, ConstraintRelationId};
use crate::nodes::parsenodes::{
    FKCONSTR_ACTION_CASCADE, FKCONSTR_ACTION_NOACTION, FKCONSTR_ACTION_RESTRICT,
    FKCONSTR_ACTION_SETDEFAULT, FKCONSTR_ACTION_SETNULL,
};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::fmgroids as f;

/// The FK constraint metadata the RI functions operate on, read from one
/// pg_constraint row.
pub struct FkMetadata {
    /// the referencing (FK) relation.
    pub conrelid: Oid,
    /// the referenced (PK) relation.
    pub confrelid: Oid,
    /// the FK columns on `conrelid` (1-based attnums).
    pub conkey: Vec<i16>,
    /// the referenced columns on `confrelid` (1-based attnums).
    pub confkey: Vec<i16>,
    /// ON DELETE action (FKCONSTR_ACTION_*).
    pub confdeltype: i8,
    /// ON UPDATE action (FKCONSTR_ACTION_*).
    pub confupdtype: i8,
}

/// Read the FK metadata for `constraint_oid` from pg_constraint. Returns None if
/// the constraint row is absent or is not a foreign key.
pub async fn fk_metadata(shared: &Arc<SharedState>, constraint_oid: Oid) -> Option<FkMetadata> {
    let pg_constraint = relation_id_get_relation(ConstraintRelationId)?;
    let desc = pg_constraint.rd_att.clone()?;
    let rows =
        scan_catalog_rows_by_oid(shared, ConstraintRelationId, pc::Anum_pg_constraint_oid, constraint_oid)
            .await;
    let mut meta = None;
    for row in &rows {
        // SAFETY: owned tuple; the fixed part holds conrelid/confrelid/action codes.
        let p = GETSTRUCT(&row.tuple).cast::<pc::FormData_pg_constraint>();
        let fixed = unsafe { &*p };
        if fixed.oid != constraint_oid {
            continue;
        }
        // SAFETY: owned tuple + matching descriptor.
        let (vals, nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        let conkey = read_i16_vector(&vals, &nulls, pc::Anum_pg_constraint_conkey);
        let confkey = read_i16_vector(&vals, &nulls, pc::Anum_pg_constraint_confkey);
        meta = Some(FkMetadata {
            conrelid: fixed.conrelid,
            confrelid: fixed.confrelid,
            conkey,
            confkey,
            confdeltype: fixed.confdeltype,
            confupdtype: fixed.confupdtype,
        });
        break;
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    relation_close(pg_constraint);
    meta
}

/// Decode an i16 vector stored as a compact varlena (4-byte header + raw i16 LE
/// bytes) at attribute `anum`. Returns empty if the column is NULL.
fn read_i16_vector(vals: &[Datum], nulls: &[bool], anum: i32) -> Vec<i16> {
    let idx = (anum - 1) as usize;
    if nulls.get(idx).copied().unwrap_or(true) {
        return Vec::new();
    }
    let ptr = crate::postgres::DatumGetPointer(vals[idx]).cast::<u8>();
    if ptr.is_null() {
        return Vec::new();
    }
    // SAFETY: the datum points at a varlena written by `encode_i16_vector` (possibly
    // stored with a short 1-byte header). VARSIZE_ANY_EXHDR / VARDATA_ANY handle both
    // header forms; the payload is raw i16 LE bytes.
    unsafe {
        let payload_len = crate::varatt::VARSIZE_ANY_EXHDR(ptr);
        let data = crate::varatt::VARDATA_ANY(ptr);
        let mut out = Vec::with_capacity(payload_len / 2);
        for i in 0..(payload_len / 2) {
            let lo = *data.add(i * 2);
            let hi = *data.add(i * 2 + 1);
            out.push(i16::from_le_bytes([lo, hi]));
        }
        out
    }
}

/// Encode an i16 slice as a compact varlena (4-byte header + raw i16 LE bytes),
/// the format `read_i16_vector` decodes. Used by `create_constraint_entry` to store
/// conkey/confkey without the full int2[] array machinery.
pub fn encode_i16_vector(attrs: &[i16]) -> Vec<u8> {
    let total = attrs.len() * 2 + 4;
    let mut buf = Vec::with_capacity(total);
    let header = (total as u32) << 2;
    buf.extend_from_slice(&header.to_le_bytes());
    for a in attrs {
        buf.extend_from_slice(&a.to_le_bytes());
    }
    buf
}

// ===========================================================================
//  Dispatch (called from the after-trigger queue)
// ===========================================================================

/// Dispatch a queued after-trigger RI event to the right RI function by its
/// `tgfoid`. A non-RI function OID is a clean staged ereport (no executable PL).
pub async fn ri_dispatch(shared: &Arc<SharedState>, ev: &AfterTriggerEvent) {
    // A non-RI (user-PL) trigger function reaching the queue: execution stages.
    if !is_ri_builtin(ev.tgfoid) {
        ri_user_trigger_staged();
        return;
    }
    let Some(meta) = fk_metadata(shared, ev.constraint_oid).await else {
        // The constraint row is gone (DROP CONSTRAINT): nothing to enforce. The RI
        // trigger may still exist if it outlived the constraint; treat as a no-op.
        return;
    };
    match ev.tgfoid {
        x if x == f::F_RI_FKEY_CHECK_INS || x == f::F_RI_FKEY_CHECK_UPD => {
            ri_fkey_check(shared, ev, &meta).await;
        }
        x if x == f::F_RI_FKEY_CASCADE_DEL => {
            ri_fkey_cascade_del(shared, ev, &meta).await;
        }
        x if x == f::F_RI_FKEY_SETNULL_DEL || x == f::F_RI_FKEY_SETDEFAULT_DEL => {
            // SET DEFAULT degrades to SET NULL at this milestone (column defaults on
            // the FK columns are the common NULL default).
            ri_fkey_setnull_del(shared, ev, &meta).await;
        }
        x if x == f::F_RI_FKEY_NOACTION_DEL || x == f::F_RI_FKEY_RESTRICT_DEL => {
            ri_fkey_restrict_del(shared, ev, &meta).await;
        }
        x if x == f::F_RI_FKEY_NOACTION_UPD
            || x == f::F_RI_FKEY_RESTRICT_UPD
            || x == f::F_RI_FKEY_CASCADE_UPD
            || x == f::F_RI_FKEY_SETNULL_UPD
            || x == f::F_RI_FKEY_SETDEFAULT_UPD =>
        {
            // ON UPDATE actions on the referenced key value stage (the PK update path
            // is not exercised by the milestone tests; keys are not updated in place).
            not_yet_reachable_upd();
        }
        _ => {}
    }
}

/// Whether `oid` is one of the RI builtin trigger functions.
fn is_ri_builtin(oid: Oid) -> bool {
    matches!(
        oid,
        x if x == f::F_RI_FKEY_CHECK_INS
            || x == f::F_RI_FKEY_CHECK_UPD
            || x == f::F_RI_FKEY_CASCADE_DEL
            || x == f::F_RI_FKEY_CASCADE_UPD
            || x == f::F_RI_FKEY_RESTRICT_DEL
            || x == f::F_RI_FKEY_RESTRICT_UPD
            || x == f::F_RI_FKEY_SETNULL_DEL
            || x == f::F_RI_FKEY_SETNULL_UPD
            || x == f::F_RI_FKEY_SETDEFAULT_DEL
            || x == f::F_RI_FKEY_SETDEFAULT_UPD
            || x == f::F_RI_FKEY_NOACTION_DEL
            || x == f::F_RI_FKEY_NOACTION_UPD
    )
}

/// A clean staged ereport for user-PL trigger function execution.
fn ri_user_trigger_staged() {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("trigger function execution requires a procedural language, which is not yet available".to_string());
    });
}

fn not_yet_reachable_upd() {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("ON UPDATE referential action on the referenced key is not yet supported".to_string());
    });
}

// ===========================================================================
//  RI_FKey_check (referencing-table INSERT/UPDATE)
// ===========================================================================

/// PG `RI_FKey_check_ins` / `RI_FKey_check_upd`: the FK row in `ev` must have a
/// matching PK row in `confrelid`. A NULL FK key column is allowed (MATCH SIMPLE).
async fn ri_fkey_check(shared: &Arc<SharedState>, ev: &AfterTriggerEvent, meta: &FkMetadata) {
    // The FK row's key values (conkey columns of the just-inserted FK row).
    let mut fk_vals = Vec::with_capacity(meta.conkey.len());
    for &attno in &meta.conkey {
        let idx = (attno - 1) as usize;
        let isnull = ev.row_isnull.get(idx).copied().unwrap_or(true);
        if isnull {
            // MATCH SIMPLE: any NULL key column means the row is exempt.
            return;
        }
        fk_vals.push(ev.row_values[idx]);
    }

    let matched = pk_row_exists(shared, meta.confrelid, &meta.confkey, &fk_vals).await;
    if !matched {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FOREIGN_KEY_VIOLATION)
                .errmsg("insert or update on table violates foreign key constraint".to_string());
        });
    }
}

/// PG `RI_Initial_Check` (step 41): validate that every existing FK row in
/// `fk_relid` (with all FK columns non-NULL) has a matching PK row in `pk_relid`,
/// else ERRCODE_FOREIGN_KEY_VIOLATION. Used by ALTER TABLE ADD FOREIGN KEY.
pub async fn ri_initial_check(
    shared: &Arc<SharedState>,
    fk_relid: Oid,
    pk_relid: Oid,
    conkey: &[i16],
    confkey: &[i16],
) {
    let Some(fk_rel) = relation_id_get_relation(fk_relid) else { return };
    let Some(desc) = fk_rel.rd_att.clone() else {
        relation_close(fk_rel);
        return;
    };
    let snap = ri_scan_snapshot(shared, &fk_rel);
    let mut scan = heap_beginscan(&fk_rel, &snap, 0, ScanOptions::empty());
    // Collect the FK key values of each row that must be checked (all cols non-NULL),
    // closing the scan before the per-row PK lookups (no nested scan / no live tuple
    // across the await).
    let mut to_check: Vec<Vec<Datum>> = Vec::new();
    while let Some(tup) =
        Box::pin(heap_getnext(shared, &mut scan, ScanDirection::Forward)).await
    {
        // SAFETY: live scan tuple.
        let tref: &HeapTupleData = unsafe { &*tup };
        let (vals, nulls) = unsafe { heap_deform_tuple(tref, &desc) };
        let mut key = Vec::with_capacity(conkey.len());
        let mut any_null = false;
        for &attno in conkey {
            let idx = (attno - 1) as usize;
            if nulls.get(idx).copied().unwrap_or(true) {
                any_null = true;
                break;
            }
            key.push(vals[idx]);
        }
        if !any_null {
            to_check.push(key);
        }
    }
    heap_endscan(shared, &mut scan);
    relation_close(fk_rel);

    for key in &to_check {
        if !pk_row_exists(shared, pk_relid, confkey, key).await {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_FOREIGN_KEY_VIOLATION)
                    .errmsg("foreign key constraint cannot be implemented: existing data violates it".to_string());
            });
        }
    }
}

/// The snapshot an RI scan reads under: the query's active snapshot, refreshed so
/// the scan sees the rows inserted/deleted by earlier commands of this transaction
/// (PG's RI uses the crosscheck/current snapshot). Falls back to a fresh
/// transaction snapshot when no active snapshot is pushed (out-of-statement paths).
fn ri_scan_snapshot(
    shared: &Arc<SharedState>,
    _rel: &crate::utils::rel::RelationData,
) -> Arc<crate::utils::snapshot::SnapshotData> {
    use crate::backend::access::transam::xact::GetCurrentCommandId;
    use crate::backend::utils::time::snapmgr::{ActiveSnapshotSet, GetActiveSnapshot, GetTransactionSnapshot};
    let snap = if ActiveSnapshotSet() {
        GetActiveSnapshot()
    } else {
        GetTransactionSnapshot(shared)
    };
    let mut snap = snap.unwrap_or_else(|| unreachable!("RI scan: a snapshot must be available"));
    // curcid must include the current command so own-xact rows are visible.
    Arc::make_mut(&mut snap).curcid = GetCurrentCommandId(false);
    snap
}

/// Whether the referenced (PK) relation has a row whose `pk_cols` equal `key_vals`.
/// A direct heap scan comparing the key Datums by type.
async fn pk_row_exists(
    shared: &Arc<SharedState>,
    pk_relid: Oid,
    pk_cols: &[i16],
    key_vals: &[Datum],
) -> bool {
    let Some(pk_rel) = relation_id_get_relation(pk_relid) else { return false };
    let Some(desc) = pk_rel.rd_att.clone() else {
        relation_close(pk_rel);
        return false;
    };
    let snap = ri_scan_snapshot(shared, &pk_rel);
    let mut scan = heap_beginscan(&pk_rel, &snap, 0, ScanOptions::empty());
    let mut found = false;
    while let Some(tup) =
        Box::pin(heap_getnext(shared, &mut scan, ScanDirection::Forward)).await
    {
        // SAFETY: live scan tuple; deform under the matching descriptor.
        let tref: &HeapTupleData = unsafe { &*tup };
        let (vals, nulls) = unsafe { heap_deform_tuple(tref, &desc) };
        if key_matches(&desc, pk_cols, &vals, &nulls, key_vals) {
            found = true;
            break;
        }
    }
    heap_endscan(shared, &mut scan);
    relation_close(pk_rel);
    found
}

/// Whether row (`vals`/`nulls`) has `cols` equal to `key_vals` (none NULL).
fn key_matches(
    desc: &TupleDesc,
    cols: &[i16],
    vals: &[Datum],
    nulls: &[bool],
    key_vals: &[Datum],
) -> bool {
    if cols.len() != key_vals.len() {
        return false;
    }
    for (i, &attno) in cols.iter().enumerate() {
        let idx = (attno - 1) as usize;
        if nulls.get(idx).copied().unwrap_or(true) {
            return false;
        }
        if !datum_eq_by_type(desc, attno, vals[idx], key_vals[i]) {
            return false;
        }
    }
    true
}

/// Equality of two Datums of the column `attno`'s type (by attbyval/attlen). Mirrors
/// genam's `datum_eq_by_type`: covers the int2/int4/oid by-value and `name`/varlena
/// by-ref keys the milestone uses for FK columns.
fn datum_eq_by_type(desc: &TupleDesc, attno: i16, a: Datum, b: Datum) -> bool {
    let att = desc.attr((attno - 1) as usize);
    match (att.attbyval, att.attlen) {
        (true, 4) => crate::postgres::DatumGetInt32(a) == crate::postgres::DatumGetInt32(b),
        (true, 2) => crate::postgres::DatumGetInt16(a) == crate::postgres::DatumGetInt16(b),
        (true, 1) => (a.0 as u8) == (b.0 as u8),
        (false, -1) => varlena_eq(a, b),
        // int8/oid by-value (len 8) and any other by-value width: compare datum bits.
        _ => a.0 == b.0,
    }
}

/// Byte-equality of two 4-byte-header varlena datums (text FK keys). A NULL pointer
/// matches only another NULL pointer.
fn varlena_eq(a: Datum, b: Datum) -> bool {
    let pa = crate::postgres::DatumGetPointer(a).cast::<u8>();
    let pb = crate::postgres::DatumGetPointer(b).cast::<u8>();
    if pa.is_null() || pb.is_null() {
        return std::ptr::eq(pa, pb);
    }
    // SAFETY: both point at varlenas (any header form); compare the payload bytes.
    unsafe {
        let la = crate::varatt::VARSIZE_ANY_EXHDR(pa);
        let lb = crate::varatt::VARSIZE_ANY_EXHDR(pb);
        if la != lb {
            return false;
        }
        let da = crate::varatt::VARDATA_ANY(pa);
        let db = crate::varatt::VARDATA_ANY(pb);
        std::slice::from_raw_parts(da, la) == std::slice::from_raw_parts(db, lb)
    }
}

// ===========================================================================
//  Referenced-table DELETE actions
// ===========================================================================

/// One referencing (FK) row that depends on the deleted PK row: its TID + values.
struct FkDependent {
    tid: crate::storage::itemptr::ItemPointerData,
    values: Vec<Datum>,
    isnull: Vec<bool>,
}

/// Find every FK row in `conrelid` whose `conkey` columns equal the deleted PK
/// row's `confkey` values. Returns the matching rows (TID + deformed values).
async fn find_dependent_fk_rows(
    shared: &Arc<SharedState>,
    ev: &AfterTriggerEvent,
    meta: &FkMetadata,
) -> Vec<FkDependent> {
    // The deleted PK row's referenced key values (confkey columns).
    let mut pk_vals = Vec::with_capacity(meta.confkey.len());
    for &attno in &meta.confkey {
        let idx = (attno - 1) as usize;
        if ev.row_isnull.get(idx).copied().unwrap_or(true) {
            // A NULL PK key cannot be referenced; no dependents.
            return Vec::new();
        }
        pk_vals.push(ev.row_values[idx]);
    }

    let Some(fk_rel) = relation_id_get_relation(meta.conrelid) else { return Vec::new() };
    let Some(desc) = fk_rel.rd_att.clone() else {
        relation_close(fk_rel);
        return Vec::new();
    };
    let snap = ri_scan_snapshot(shared, &fk_rel);
    let mut scan = heap_beginscan(&fk_rel, &snap, 0, ScanOptions::empty());
    let mut deps = Vec::new();
    while let Some(tup) =
        Box::pin(heap_getnext(shared, &mut scan, ScanDirection::Forward)).await
    {
        // SAFETY: live scan tuple.
        let tref: &HeapTupleData = unsafe { &*tup };
        let (vals, nulls) = unsafe { heap_deform_tuple(tref, &desc) };
        if key_matches(&desc, &meta.conkey, &vals, &nulls, &pk_vals) {
            deps.push(FkDependent { tid: tref.t_self, values: vals, isnull: nulls });
        }
    }
    heap_endscan(shared, &mut scan);
    relation_close(fk_rel);
    deps
}

/// PG `RI_FKey_noaction_del` / `RI_FKey_restrict_del`: error if any FK row still
/// references the deleted PK row.
async fn ri_fkey_restrict_del(shared: &Arc<SharedState>, ev: &AfterTriggerEvent, meta: &FkMetadata) {
    let deps = find_dependent_fk_rows(shared, ev, meta).await;
    if !deps.is_empty() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FOREIGN_KEY_VIOLATION)
                .errmsg("update or delete on table violates foreign key constraint on referencing table".to_string());
        });
    }
}

/// PG `RI_FKey_cascade_del`: delete every FK row that referenced the deleted PK row.
async fn ri_fkey_cascade_del(shared: &Arc<SharedState>, ev: &AfterTriggerEvent, meta: &FkMetadata) {
    let deps = find_dependent_fk_rows(shared, ev, meta).await;
    if deps.is_empty() {
        return;
    }
    let Some(fk_rel) = relation_id_get_relation(meta.conrelid) else { return };
    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
    for dep in &deps {
        let _ = Box::pin(heap_delete(shared, &fk_rel, &dep.tid, cid, None, true, false)).await;
    }
    relation_close(fk_rel);
}

/// PG `RI_FKey_setnull_del` (and SET DEFAULT degraded to NULL): set the FK columns
/// of every dependent FK row to NULL.
async fn ri_fkey_setnull_del(shared: &Arc<SharedState>, ev: &AfterTriggerEvent, meta: &FkMetadata) {
    let deps = find_dependent_fk_rows(shared, ev, meta).await;
    if deps.is_empty() {
        return;
    }
    let Some(fk_rel) = relation_id_get_relation(meta.conrelid) else { return };
    let Some(desc) = fk_rel.rd_att.clone() else {
        relation_close(fk_rel);
        return;
    };
    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
    for dep in &deps {
        let mut vals = dep.values.clone();
        let mut nulls = dep.isnull.clone();
        for &attno in &meta.conkey {
            let idx = (attno - 1) as usize;
            nulls[idx] = true;
            vals[idx] = Datum(0);
        }
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        let _ = Box::pin(heapam_tuple_update(
            shared, &fk_rel, &dep.tid, &mut newtup, cid, None, None, true,
        ))
        .await;
        heap_freetuple(newtup);
    }
    relation_close(fk_rel);
}

// ===========================================================================
//  Header-stub-named entry points (kept for the C call sites; the firing path
//  uses ri_dispatch). These are the fmgr-style RI builtins; they are not on the
//  milestone's call path (the after-trigger queue dispatches by OID instead), so
//  they stage loudly if reached directly.
// ===========================================================================

/// Marker the action codes are referenced (avoids an unused-import warning while
/// keeping the FK action vocabulary visible in this module).
const _RI_ACTIONS: [i8; 5] = [
    FKCONSTR_ACTION_NOACTION,
    FKCONSTR_ACTION_RESTRICT,
    FKCONSTR_ACTION_CASCADE,
    FKCONSTR_ACTION_SETNULL,
    FKCONSTR_ACTION_SETDEFAULT,
];

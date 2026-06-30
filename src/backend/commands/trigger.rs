//! Trigger commands + the after-trigger event queue. Translated from the
//! M11/step-41 parts of `src/backend/commands/trigger.c` (disposition: grow).
//!
//! This lands the trigger substrate the foreign-key machinery rides on:
//!  - `create_trigger` (PG `CreateTrigger`): insert a pg_trigger row, set
//!    `pg_class.relhastriggers`. Used both for user `CREATE TRIGGER` and for the
//!    system triggers ADD FOREIGN KEY creates internally.
//!  - `relation_build_triggers` (PG `RelationBuildTriggers`): build the in-memory
//!    `TriggerDesc` from a relation's pg_trigger rows (cached on the relcache entry).
//!  - the firing points `exec_bs/as/br/ar_*_triggers` the executor calls around
//!    INSERT/UPDATE/DELETE, replacing the step-34 "triggers grow later" gap.
//!  - the after-trigger event queue (`after_trigger_begin_query` /
//!    `after_trigger_end_query` / `after_trigger_save_event` /
//!    `after_trigger_invoke_events`): AFTER ROW triggers are queued during the
//!    statement and fired at its end. RI (foreign-key) checks ride this queue.
//!
//! The queue is per-task state (task_local!, rules.md s10): a backend runs on a
//! tokio worker thread, so it never crosses `.await` while borrowed -- the drain
//! snapshots the pending events, drops the borrow, then fires them async.
//!
//! STAGED (rules.md s4): user-PL trigger function execution (no executable PL yet
//! -> a clean staged ereport), deferred-constraint timing (INITIALLY DEFERRED ->
//! fire at commit), transition tables (REFERENCING NEW/OLD TABLE), INSTEAD OF
//! triggers, statement-level WHEN, and BEFORE-ROW tuple modification/skip beyond
//! the pass-through.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]
#![allow(
    clippy::ref_option,
    reason = "the AR-trigger firing points take &Option<TupleDesc> to mirror the executor's RowSnapshot field without a clone at the call site"
)]

use std::cell::RefCell;
use std::sync::Arc;

use crate::access::tupdesc::TupleDesc;
use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::catalog::catalog::get_new_object_id;
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::catalog_tuple_insert;
use crate::backend::utils::cache::relcache::{
    relation_close, relation_id_get_relation,
};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_class::{self as pc, RelationRelationId};
use crate::catalog::pg_trigger::{self as pgt, TriggerRelationId, TRIGGER_TYPE_ROW};
use crate::commands::trigger::{TRIGGER_FIRES_ON_ORIGIN, TRIGGER_DISABLED};
use crate::nodes::execnodes::{EState, ResultRelInfo};
use crate::nodes::parsenodes::CreateTrigStmt;
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Datum, Int16GetDatum, NameGetDatum, ObjectIdGetDatum,
    PointerGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::reltrigger::{Trigger, TriggerDesc};

/// A queued AFTER ROW trigger event. PG's `AfterTriggerEventData` is a compact
/// per-event record keyed into the shared event list; here it carries everything
/// the (RI builtin) trigger function needs to run at query end: the firing
/// function OID, the two relations, the constraint's column lists, and a snapshot
/// of the affected row's values.
#[derive(Clone)]
pub struct AfterTriggerEvent {
    /// the trigger function OID (RI_FKey_check_ins, ..._cascade_del, ...).
    pub tgfoid: Oid,
    /// the relation the trigger is defined on (the FK rel for a check trigger,
    /// the PK rel for an action trigger).
    pub tgrelid: Oid,
    /// the other relation in the FK (confrelid for a check trigger).
    pub confrelid: Oid,
    /// the pg_constraint OID this trigger enforces; the conkey/confkey/action are
    /// read from it at fire time.
    pub constraint_oid: Oid,
    /// the affected row's values + nulls (the FK row for a check, the PK row for an
    /// action), keyed by attno-1. By-reference (varlena) values are DEEP-COPIED at
    /// save time: each such Datum points into `row_backing[idx]` (an owned buffer the
    /// event holds), not into the node-owned source slot that `exec_proc_node` reuses.
    pub row_values: Vec<Datum>,
    pub row_isnull: Vec<bool>,
    /// the owned backing buffers for the by-reference `row_values` (one slot per
    /// column; `None` for by-value or NULL columns). Keeps the event self-contained:
    /// the varlena Datums above stay valid until the event is fired, regardless of
    /// later `exec_proc_node` slot reuse. The `Box<[u8]>` heap allocation is stable
    /// across moves, so the pointing Datums remain valid; `Datum` is `usize`-shaped,
    /// so the whole event is `Send`.
    pub row_backing: Vec<Option<Box<[u8]>>>,
    /// the affected row's descriptor (so the values can be re-read by type).
    pub row_desc: Option<TupleDesc>,
}

/// Per-query/per-statement after-trigger event list. PG nests these per query
/// depth; the milestone runs one ModifyTable per command, so a single Vec drained
/// at query end is faithful enough (deferred-to-commit timing stages).
#[derive(Default)]
struct AfterTriggerState {
    /// nesting depth of AfterTriggerBeginQuery/EndQuery (>0 means a query is open).
    query_depth: i32,
    events: Vec<AfterTriggerEvent>,
}

tokio::task_local! {
    static AFTER_TRIGGERS: RefCell<AfterTriggerState>;
}

/// Run `f` with a fresh per-task after-trigger state in scope. Mirrors the other
/// per-task scopes (combocid/snapmgr); installed by the backend entry + tests.
pub async fn after_trigger_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    AFTER_TRIGGERS
        .scope(RefCell::new(AfterTriggerState::default()), f)
        .await
}

fn after_triggers_in_scope() -> bool {
    AFTER_TRIGGERS.try_with(|_| ()).is_ok()
}

// ===========================================================================
//  CreateTrigger
// ===========================================================================

/// The utility-dispatch entry for a user `CREATE TRIGGER`: resolve the relation
/// and the trigger function, then `create_trigger`. The function-language check
/// (no executable PL yet) happens when the trigger first fires, not here, so the
/// pg_trigger row is stored and `relhastriggers` set as in PG.
pub async fn create_trigger_command(
    shared: &Arc<SharedState>,
    stmt: &CreateTrigStmt,
    query_string: &str,
) -> ObjectAddress {
    let rel = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("CREATE TRIGGER names a relation"));
    let relname = rel.relname.as_deref().unwrap_or_else(|| unreachable!("trigger rel has a name"));
    let relid = crate::backend::catalog::namespace::range_var_get_relid(
        shared,
        rel.schemaname.as_deref(),
        relname,
    )
    .await
    .unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!("relation \"{relname}\" does not exist"));
        });
        unreachable!("ereport(ERROR) diverges");
    });

    // Resolve the trigger function name to its OID (last name part).
    let funcname = stmt
        .funcname
        .iter()
        .filter_map(|n| match n {
            crate::nodes::nodes::Node::String_(s) => Some(s.sval.clone()),
            _ => None,
        })
        .next_back()
        .unwrap_or_default();
    let funcoid = lookup_trigger_function(shared, &funcname).await;

    create_trigger(shared, stmt, query_string, relid, InvalidOid, InvalidOid, funcoid, false)
        .await
}

/// Resolve a trigger function by (unqualified) name to its OID via pg_proc. The RI
/// builtin names resolve to their fixed OIDs; an unknown name is a clean error.
async fn lookup_trigger_function(shared: &Arc<SharedState>, name: &str) -> Oid {
    if let Some(oid) = ri_builtin_oid_by_name(name) {
        return oid;
    }
    // A non-RI trigger function: resolve through pg_proc by name (the lookup proves
    // it exists; firing it needs executable PL, which stages -> a clean ereport).
    if let Some(oid) = proc_oid_by_name(shared, name).await {
        return oid;
    }
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function \"{name}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// Find a pg_proc OID by (unqualified) function name via a heap scan. Returns the
/// first match (the milestone has no overloaded trigger functions).
async fn proc_oid_by_name(shared: &Arc<SharedState>, name: &str) -> Option<Oid> {
    use crate::access::htup_details::GETSTRUCT;
    use crate::access::sdir::ScanDirection;
    use crate::access::tableam::ScanOptions;
    use crate::backend::access::heap::heapam::{heap_beginscan, heap_endscan, heap_getnext};
    use crate::backend::access::index::genam::systable_scan_snapshot;
    use crate::catalog::pg_proc::{FormData_pg_proc, ProcedureRelationId};

    let pg_proc = relation_id_get_relation(ProcedureRelationId)?;
    let snap = systable_scan_snapshot(shared, &pg_proc, None);
    let mut scan = heap_beginscan(&pg_proc, &snap, 0, ScanOptions::empty());
    let mut found = None;
    while let Some(tup) =
        Box::pin(heap_getnext(shared, &mut scan, ScanDirection::Forward)).await
    {
        // SAFETY: live scan tuple; proname is in the fixed part.
        let tref: &crate::access::htup::HeapTupleData = unsafe { &*tup };
        let p = GETSTRUCT(tref).cast::<FormData_pg_proc>();
        if crate::backend::commands::tablecmds::name_to_string(unsafe { &(*p).proname }) == name {
            found = Some(unsafe { (*p).oid });
            break;
        }
    }
    heap_endscan(shared, &mut scan);
    relation_close(pg_proc);
    found
}

/// The RI action function OID for an ON DELETE action code (FKCONSTR_ACTION_*).
/// NO ACTION/RESTRICT -> the noaction/restrict del check; CASCADE/SET NULL/SET
/// DEFAULT -> the corresponding del action.
pub fn ri_action_func_for_delete(del_action: i8) -> Oid {
    use crate::nodes::parsenodes::{
        FKCONSTR_ACTION_CASCADE, FKCONSTR_ACTION_RESTRICT, FKCONSTR_ACTION_SETDEFAULT,
        FKCONSTR_ACTION_SETNULL,
    };
    use crate::utils::fmgroids as f;
    match del_action {
        x if x == FKCONSTR_ACTION_CASCADE => f::F_RI_FKEY_CASCADE_DEL,
        x if x == FKCONSTR_ACTION_SETNULL => f::F_RI_FKEY_SETNULL_DEL,
        x if x == FKCONSTR_ACTION_SETDEFAULT => f::F_RI_FKEY_SETDEFAULT_DEL,
        x if x == FKCONSTR_ACTION_RESTRICT => f::F_RI_FKEY_RESTRICT_DEL,
        // NO ACTION (the default) and anything else.
        _ => f::F_RI_FKEY_NOACTION_DEL,
    }
}

/// The RI builtin function OID for a name, if it is one (used by the function
/// lookup so RI system triggers resolve without a pg_proc row dependency).
pub fn ri_builtin_oid_by_name(name: &str) -> Option<Oid> {
    use crate::utils::fmgroids as f;
    Some(match name {
        "RI_FKey_check_ins" => f::F_RI_FKEY_CHECK_INS,
        "RI_FKey_check_upd" => f::F_RI_FKEY_CHECK_UPD,
        "RI_FKey_cascade_del" => f::F_RI_FKEY_CASCADE_DEL,
        "RI_FKey_cascade_upd" => f::F_RI_FKEY_CASCADE_UPD,
        "RI_FKey_restrict_del" => f::F_RI_FKEY_RESTRICT_DEL,
        "RI_FKey_restrict_upd" => f::F_RI_FKEY_RESTRICT_UPD,
        "RI_FKey_setnull_del" => f::F_RI_FKEY_SETNULL_DEL,
        "RI_FKey_setnull_upd" => f::F_RI_FKEY_SETNULL_UPD,
        "RI_FKey_setdefault_del" => f::F_RI_FKEY_SETDEFAULT_DEL,
        "RI_FKey_setdefault_upd" => f::F_RI_FKEY_SETDEFAULT_UPD,
        "RI_FKey_noaction_del" => f::F_RI_FKEY_NOACTION_DEL,
        "RI_FKey_noaction_upd" => f::F_RI_FKEY_NOACTION_UPD,
        _ => return None,
    })
}

/// PG `CreateTrigger`: form + insert the pg_trigger row, then set the owning
/// relation's `pg_class.relhastriggers`. `constraint_oid`/`ref_rel_oid` link the
/// trigger to a constraint when it is an internal (system) RI trigger. Returns the
/// new trigger's `ObjectAddress`.
#[allow(clippy::too_many_arguments)]
pub async fn create_trigger(
    shared: &Arc<SharedState>,
    stmt: &CreateTrigStmt,
    _query_string: &str,
    rel_oid: Oid,
    constraint_oid: Oid,
    constr_rel_oid: Oid,
    funcoid: Oid,
    is_internal: bool,
) -> ObjectAddress {
    let Some(pg_trigger) = relation_id_get_relation(TriggerRelationId) else {
        // pg_trigger must be seeded (bootstrap nails it); nowhere to store otherwise.
        return ObjectAddress { classId: TriggerRelationId, objectId: InvalidOid, objectSubId: 0 };
    };
    let desc = pg_trigger.rd_att.clone().unwrap_or_else(|| unreachable!("pg_trigger desc"));
    let natts = desc.natts as usize;

    // tgtype bitmask: ROW/timing/events folded from the statement (gram set
    // timing|events; row adds TRIGGER_TYPE_ROW).
    let mut tgtype: i16 = stmt.timing | stmt.events;
    if stmt.row {
        tgtype |= TRIGGER_TYPE_ROW;
    }

    let trigname = stmt
        .trigname
        .clone()
        .unwrap_or_else(|| unreachable!("a trigger always has a name"));
    let trigname_data = name_data(&trigname);
    let new_oid = get_new_object_id(shared);

    // tgargs: the user-supplied args, NUL-terminated and concatenated (PG's
    // first\000second\000 bytea). tgattr: the column list (empty here).
    let args: Vec<String> = stmt
        .args
        .iter()
        .filter_map(|n| match n {
            crate::nodes::nodes::Node::String_(s) => Some(s.sval.clone()),
            _ => None,
        })
        .collect();
    let tgargs_bytea = build_tgargs_bytea(&args);
    let tgattr_bytes = build_int2vector(&[]);

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let set = |v: &mut [Datum], n: &mut [bool], anum: i32, d: Datum| {
        v[(anum - 1) as usize] = d;
        n[(anum - 1) as usize] = false;
    };
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_oid, ObjectIdGetDatum(new_oid));
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgrelid, ObjectIdGetDatum(rel_oid));
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgparentid, ObjectIdGetDatum(InvalidOid));
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgname, NameGetDatum(&trigname_data));
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgfoid, ObjectIdGetDatum(funcoid));
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgtype, Int16GetDatum(tgtype));
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tgenabled,
        CharGetDatum(TRIGGER_FIRES_ON_ORIGIN as i8),
    );
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgisinternal, BoolGetDatum(is_internal));
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tgconstrrelid,
        ObjectIdGetDatum(constr_rel_oid),
    );
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgconstrindid, ObjectIdGetDatum(InvalidOid));
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tgconstraint,
        ObjectIdGetDatum(constraint_oid),
    );
    set(&mut values, &mut isnull, pgt::Anum_pg_trigger_tgdeferrable, BoolGetDatum(stmt.deferrable));
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tginitdeferred,
        BoolGetDatum(stmt.initdeferred),
    );
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tgnargs,
        Int16GetDatum(i16::try_from(args.len()).unwrap_or(0)),
    );
    // tgattr (int2vector) + tgargs (bytea) are FORCE_NOT_NULL: store the bytes.
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tgattr,
        PointerGetDatum(tgattr_bytes.as_ptr()),
    );
    set(
        &mut values,
        &mut isnull,
        pgt::Anum_pg_trigger_tgargs,
        PointerGetDatum(tgargs_bytea.as_ptr()),
    );
    // tgqual / tgoldtable / tgnewtable are NULL at this milestone.
    isnull[(pgt::Anum_pg_trigger_tgqual - 1) as usize] = true;
    isnull[(pgt::Anum_pg_trigger_tgoldtable - 1) as usize] = true;
    isnull[(pgt::Anum_pg_trigger_tgnewtable - 1) as usize] = true;

    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_trigger, &mut tup).await;
    heap_freetuple(tup);
    relation_close(pg_trigger);

    // keep the varlena backing buffers alive until after the insert copied them.
    drop(tgargs_bytea);
    drop(tgattr_bytes);

    // Set pg_class.relhastriggers on the owning relation, and drop its relcache
    // entry so the next open rebuilds the TriggerDesc.
    set_relation_has_triggers(shared, rel_oid, true).await;
    crate::backend::utils::cache::relcache::relation_forget_relation(rel_oid);

    ObjectAddress { classId: TriggerRelationId, objectId: new_oid, objectSubId: 0 }
}

/// Build a `bytea` whose payload is the NUL-terminated trigger args (PG's
/// `first\000second\000...`). Returns a heap buffer with a 4-byte varlena header.
fn build_tgargs_bytea(args: &[String]) -> Vec<u8> {
    let mut payload = Vec::new();
    for a in args {
        payload.extend_from_slice(a.as_bytes());
        payload.push(0);
    }
    make_varlena(&payload)
}

/// Build an int2vector varlena from a slice of attribute numbers. An int2vector is
/// physically a 1-D array; we store it as a varlena holding the raw i16 LE bytes,
/// which is sufficient for the round-trip the RI machinery does not read.
fn build_int2vector(attrs: &[i16]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(attrs.len() * 2);
    for a in attrs {
        payload.extend_from_slice(&a.to_le_bytes());
    }
    make_varlena(&payload)
}

/// Wrap `payload` in a 4-byte-header varlena (the standard long format).
fn make_varlena(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + 4;
    let mut buf = Vec::with_capacity(total);
    // VARSIZE_4B: total length in the high 30 bits, low 2 bits = 0 (4B header).
    let header = (total as u32) << 2;
    buf.extend_from_slice(&header.to_le_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Flip `pg_class.relhastriggers` for `relid` (PG `SetRelationHasTriggers` via
/// `setRelhastriggers`). A no-op if the row cannot be found.
async fn set_relation_has_triggers(shared: &Arc<SharedState>, relid: Oid, value: bool) {
    use crate::access::htup_details::GETSTRUCT;
    use crate::backend::access::common::heaptuple::heap_deform_tuple;
    use crate::backend::catalog::indexing::catalog_tuple_update;

    let Some(pg_class) = relation_id_get_relation(RelationRelationId) else { return };
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class desc"));
    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        RelationRelationId,
        pc::Anum_pg_class_oid,
        relid,
    )
    .await;
    for row in &rows {
        // SAFETY: owned tuple; check it is the target relation.
        let p = GETSTRUCT(&row.tuple).cast::<pc::FormData_pg_class>();
        if unsafe { (*p).oid } != relid {
            continue;
        }
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        vals[(pc::Anum_pg_class_relhastriggers - 1) as usize] = BoolGetDatum(value);
        nulls[(pc::Anum_pg_class_relhastriggers - 1) as usize] = false;
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_class, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        break;
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    relation_close(pg_class);
}

// ===========================================================================
//  RelationBuildTriggers / TriggerDesc
// ===========================================================================

/// PG `RelationBuildTriggers`: build a `TriggerDesc` from `relid`'s pg_trigger
/// rows. Returns None if the relation has no triggers.
pub async fn relation_build_triggers(
    shared: &Arc<SharedState>,
    relid: Oid,
) -> Option<TriggerDesc> {
    use crate::access::htup_details::GETSTRUCT;

    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        TriggerRelationId,
        pgt::Anum_pg_trigger_tgrelid,
        relid,
    )
    .await;
    if rows.is_empty() {
        return None;
    }

    let mut desc = TriggerDesc {
        triggers: Vec::new(),
        ..empty_trigger_desc()
    };
    for row in &rows {
        // SAFETY: owned tuple; the fixed part holds every field RI reads.
        let p = GETSTRUCT(&row.tuple).cast::<pgt::FormData_pg_trigger>();
        let f = unsafe { &*p };
        let trig = Trigger {
            tgoid: f.oid,
            tgname: crate::backend::commands::tablecmds::name_to_string(&f.tgname),
            tgfoid: f.tgfoid,
            tgtype: f.tgtype,
            tgenabled: f.tgenabled as u8,
            tgisinternal: f.tgisinternal,
            tgisclone: false,
            tgconstrrelid: f.tgconstrrelid,
            tgconstrindid: f.tgconstrindid,
            tgconstraint: f.tgconstraint,
            tgdeferrable: f.tgdeferrable,
            tginitdeferred: f.tginitdeferred,
            tgnargs: f.tgnargs,
            tgnattr: 0,
            tgattr: Vec::new(),
            tgargs: Vec::new(),
            tgqual: None,
            tgoldtable: None,
            tgnewtable: None,
        };
        accumulate_trigger_flags(&mut desc, &trig);
        desc.triggers.push(trig);
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    Some(desc)
}

/// A `TriggerDesc` with all presence flags false and no triggers.
fn empty_trigger_desc() -> TriggerDesc {
    TriggerDesc {
        triggers: Vec::new(),
        trig_insert_before_row: false,
        trig_insert_after_row: false,
        trig_insert_instead_row: false,
        trig_insert_before_statement: false,
        trig_insert_after_statement: false,
        trig_update_before_row: false,
        trig_update_after_row: false,
        trig_update_instead_row: false,
        trig_update_before_statement: false,
        trig_update_after_statement: false,
        trig_delete_before_row: false,
        trig_delete_after_row: false,
        trig_delete_instead_row: false,
        trig_delete_before_statement: false,
        trig_delete_after_statement: false,
        trig_truncate_before_statement: false,
        trig_truncate_after_statement: false,
        trig_insert_new_table: false,
        trig_update_old_table: false,
        trig_update_new_table: false,
        trig_delete_old_table: false,
    }
}

/// PG `RelationBuildTriggers`'s per-trigger flag accumulation: set the
/// `trig_<event>_<timing>_<level>` presence flags so the executor can skip a row
/// when no matching trigger exists.
fn accumulate_trigger_flags(desc: &mut TriggerDesc, trig: &Trigger) {
    let t = trig.tgtype;
    let row = pgt::TRIGGER_FOR_ROW(t);
    let before = pgt::TRIGGER_FOR_BEFORE(t);
    let instead = pgt::TRIGGER_FOR_INSTEAD(t);
    let after = !before && !instead;
    if pgt::TRIGGER_FOR_INSERT(t) {
        set_event_flags(
            row, before, after, instead,
            &mut desc.trig_insert_before_row, &mut desc.trig_insert_after_row,
            &mut desc.trig_insert_instead_row,
            &mut desc.trig_insert_before_statement, &mut desc.trig_insert_after_statement,
        );
    }
    if pgt::TRIGGER_FOR_UPDATE(t) {
        set_event_flags(
            row, before, after, instead,
            &mut desc.trig_update_before_row, &mut desc.trig_update_after_row,
            &mut desc.trig_update_instead_row,
            &mut desc.trig_update_before_statement, &mut desc.trig_update_after_statement,
        );
    }
    if pgt::TRIGGER_FOR_DELETE(t) {
        set_event_flags(
            row, before, after, instead,
            &mut desc.trig_delete_before_row, &mut desc.trig_delete_after_row,
            &mut desc.trig_delete_instead_row,
            &mut desc.trig_delete_before_statement, &mut desc.trig_delete_after_statement,
        );
    }
}

#[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
fn set_event_flags(
    row: bool, before: bool, after: bool, instead: bool,
    before_row: &mut bool, after_row: &mut bool, instead_row: &mut bool,
    before_stmt: &mut bool, after_stmt: &mut bool,
) {
    if row {
        if before { *before_row = true; }
        if after { *after_row = true; }
        if instead { *instead_row = true; }
    } else {
        if before { *before_stmt = true; }
        if after { *after_stmt = true; }
    }
}

// ===========================================================================
//  Firing points (called by the executor's ExecInsert/Update/Delete)
// ===========================================================================

/// Whether a trigger is enabled to fire in the current session replication role
/// (PG's `TRIGGER_FIRES_ON_ORIGIN` default: fire unless DISABLED).
fn trigger_enabled(trig: &Trigger) -> bool {
    trig.tgenabled != TRIGGER_DISABLED
}

/// PG `ExecBSInsertTriggers` (statement BEFORE): the milestone has no statement
/// triggers to fire; the call marks the firing point reached.
pub fn exec_bs_insert_triggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {}

/// PG `ExecASInsertTriggers` (statement AFTER): nothing to fire at this milestone.
pub fn exec_as_insert_triggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {}

/// PG `ExecARInsertTriggers` (row AFTER INSERT): queue each matching AFTER ROW
/// INSERT trigger as an after-trigger event over `row` (the inserted tuple).
pub fn exec_ar_insert_triggers(
    trigdesc: Option<&TriggerDesc>,
    fk_relid: Oid,
    row_values: &[Datum],
    row_isnull: &[bool],
    row_desc: &Option<TupleDesc>,
) {
    let Some(td) = trigdesc else { return };
    if !td.trig_insert_after_row {
        return;
    }
    for trig in &td.triggers {
        if !trigger_enabled(trig) || !pgt::TRIGGER_FOR_ROW(trig.tgtype) {
            continue;
        }
        if pgt::TRIGGER_FOR_INSERT(trig.tgtype) && pgt::TRIGGER_FOR_AFTER(trig.tgtype) {
            save_ri_event(trig, fk_relid, row_values, row_isnull, row_desc);
        }
    }
}

/// PG `ExecARDeleteTriggers` (row AFTER DELETE): queue each matching AFTER ROW
/// DELETE trigger (the RI action triggers on the referenced/PK table) over the
/// deleted row.
pub fn exec_ar_delete_triggers(
    trigdesc: Option<&TriggerDesc>,
    pk_relid: Oid,
    row_values: &[Datum],
    row_isnull: &[bool],
    row_desc: &Option<TupleDesc>,
) {
    let Some(td) = trigdesc else { return };
    if !td.trig_delete_after_row {
        return;
    }
    for trig in &td.triggers {
        if !trigger_enabled(trig) || !pgt::TRIGGER_FOR_ROW(trig.tgtype) {
            continue;
        }
        if pgt::TRIGGER_FOR_DELETE(trig.tgtype) && pgt::TRIGGER_FOR_AFTER(trig.tgtype) {
            save_ri_event(trig, pk_relid, row_values, row_isnull, row_desc);
        }
    }
}

/// PG `ExecARUpdateTriggers` (row AFTER UPDATE): queue matching AFTER ROW UPDATE
/// triggers over the new row. (The RI _upd checks/actions ride this.)
pub fn exec_ar_update_triggers(
    trigdesc: Option<&TriggerDesc>,
    relid: Oid,
    row_values: &[Datum],
    row_isnull: &[bool],
    row_desc: &Option<TupleDesc>,
) {
    let Some(td) = trigdesc else { return };
    if !td.trig_update_after_row {
        return;
    }
    for trig in &td.triggers {
        if !trigger_enabled(trig) || !pgt::TRIGGER_FOR_ROW(trig.tgtype) {
            continue;
        }
        if pgt::TRIGGER_FOR_UPDATE(trig.tgtype) && pgt::TRIGGER_FOR_AFTER(trig.tgtype) {
            save_ri_event(trig, relid, row_values, row_isnull, row_desc);
        }
    }
}

/// Record an after-trigger event for an RI trigger. Reads the constraint's column
/// lists from pg_constraint via `trig.tgconstraint` lazily at fire time, so here we
/// only snapshot the trigger + row; the conkey/confkey are filled at save time from
/// the trigger's constraint. The borrow is released before any await (none here).
fn save_ri_event(
    trig: &Trigger,
    tgrelid: Oid,
    row_values: &[Datum],
    row_isnull: &[bool],
    row_desc: &Option<TupleDesc>,
) {
    if !after_triggers_in_scope() {
        return;
    }
    // Deep-copy the row's Datums into the event (PG datumCopy): a by-reference
    // (varlena) Datum is a pointer into the node-owned source slot, which is REUSED
    // on the next `exec_proc_node`; the RI check fires at end-of-query, so without a
    // copy it would dereference a stale/overwritten pointer. By-value Datums copy
    // verbatim; by-ref Datums are copied into owned buffers the event holds.
    let (owned_values, row_backing) = deep_copy_row(row_values, row_isnull, row_desc);
    let ev = AfterTriggerEvent {
        tgfoid: trig.tgfoid,
        tgrelid,
        confrelid: trig.tgconstrrelid,
        constraint_oid: trig.tgconstraint,
        row_values: owned_values,
        row_isnull: row_isnull.to_vec(),
        row_backing,
        row_desc: row_desc.clone(),
    };
    AFTER_TRIGGERS.with(|s| s.borrow_mut().events.push(ev));
}

/// The owned datums + per-column backing buffers a deep-copied row holds.
type DeepCopiedRow = (Vec<Datum>, Vec<Option<Box<[u8]>>>);

/// Deep-copy a row's Datums for a queued after-trigger event (PG `datumCopy` per
/// column). Returns the owned Datum vector plus the per-column backing buffers (the
/// varlena bytes the by-ref Datums point into). The per-column by-value/by-ref
/// metadata comes from `row_desc`; a NULL column or a column past the descriptor is
/// copied verbatim (no backing).
fn deep_copy_row(
    row_values: &[Datum],
    row_isnull: &[bool],
    row_desc: &Option<TupleDesc>,
) -> DeepCopiedRow {
    use crate::utils::datum::datum_copy_owned;
    let mut values = Vec::with_capacity(row_values.len());
    let mut backing = Vec::with_capacity(row_values.len());
    for (idx, &v) in row_values.iter().enumerate() {
        let is_null = row_isnull.get(idx).copied().unwrap_or(false);
        let att = row_desc.as_ref().and_then(|d| (idx < d.natts as usize).then(|| d.attr(idx)));
        if let (false, Some(att)) = (is_null, att) {
            let (d, owned) = datum_copy_owned(v, att.attbyval, i32::from(att.attlen));
            values.push(d);
            backing.push(owned);
        } else {
            values.push(v);
            backing.push(None);
        }
    }
    (values, backing)
}

// ===========================================================================
//  After-trigger event queue
// ===========================================================================

/// PG `AfterTriggerBeginQuery`: open a new after-trigger query level.
pub fn after_trigger_begin_query() {
    if !after_triggers_in_scope() {
        return;
    }
    AFTER_TRIGGERS.with(|s| s.borrow_mut().query_depth += 1);
}

/// PG `AfterTriggerEndQuery` -> `afterTriggerInvokeEvents`: fire all queued AFTER
/// ROW events for this query, then close the level. Snapshots the events under the
/// borrow, drops it, and fires async (no lock across await, rules.md s5).
pub async fn after_trigger_end_query(shared: &Arc<SharedState>) {
    if !after_triggers_in_scope() {
        return;
    }
    let events = AFTER_TRIGGERS.with(|s| {
        let mut st = s.borrow_mut();
        st.query_depth -= 1;
        std::mem::take(&mut st.events)
    });
    for ev in events {
        Box::pin(fire_after_event(shared, &ev)).await;
    }
}

/// Fire one queued after-trigger event: dispatch to the RI builtin named by its
/// `tgfoid`. A non-RI (user-PL) trigger function is a clean staged ereport.
async fn fire_after_event(shared: &Arc<SharedState>, ev: &AfterTriggerEvent) {
    crate::backend::utils::adt::ri_triggers::ri_dispatch(shared, ev).await;
}

/// PG `AfterTriggerEndXact`: drop any leftover events at end of transaction.
pub fn after_trigger_end_xact() {
    if after_triggers_in_scope() {
        AFTER_TRIGGERS.with(|s| {
            let mut st = s.borrow_mut();
            st.events.clear();
            st.query_depth = 0;
        });
    }
}

// ===========================================================================
//  RemoveTriggerById
// ===========================================================================

/// PG `RemoveTriggerById`: delete the pg_trigger row with `trig_oid` and clear the
/// owning relation's `relhastriggers` if it was the last trigger.
pub async fn remove_trigger_by_id(shared: &Arc<SharedState>, trig_oid: Oid) {
    use crate::access::htup_details::GETSTRUCT;
    use crate::backend::catalog::indexing::catalog_tuple_delete;

    let Some(pg_trigger) = relation_id_get_relation(TriggerRelationId) else { return };
    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        TriggerRelationId,
        pgt::Anum_pg_trigger_oid,
        trig_oid,
    )
    .await;
    let mut owning_rel = InvalidOid;
    for row in &rows {
        // SAFETY: owned tuple.
        let p = GETSTRUCT(&row.tuple).cast::<pgt::FormData_pg_trigger>();
        if unsafe { (*p).oid } != trig_oid {
            continue;
        }
        owning_rel = unsafe { (*p).tgrelid };
        catalog_tuple_delete(shared, &pg_trigger, &row.tid).await;
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    relation_close(pg_trigger);

    // If no triggers remain on the owning relation, clear relhastriggers.
    if owning_rel.is_valid() {
        let remaining = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
            shared,
            TriggerRelationId,
            pgt::Anum_pg_trigger_tgrelid,
            owning_rel,
        )
        .await;
        let empty = remaining.is_empty();
        for row in remaining {
            heap_freetuple(row.tuple);
        }
        if empty {
            set_relation_has_triggers(shared, owning_rel, false).await;
        }
        crate::backend::utils::cache::relcache::relation_forget_relation(owning_rel);
    }
}

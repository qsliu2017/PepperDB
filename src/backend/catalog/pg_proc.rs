//! pg_proc catalog manipulation. Translated from the step-39-relevant parts of
//! `src/backend/catalog/pg_proc.c` (disposition: grow).
//!
//! `procedure_create` forms + inserts the pg_proc row for CREATE FUNCTION, with the
//! CREATE OR REPLACE update path (a name+argtypes match in the same namespace
//! updates the existing row instead of inserting). `proc_lookup_by_name` resolves a
//! function name to its OID (the CREATE duplicate check + DROP + tests use it), and
//! `remove_procedure_by_id` deletes the row (the DROP FUNCTION leaf).
//!
//! Async coloring (rules.md s5): every path reaches the buffer pool, so the entries
//! are `async` and thread `&Arc<SharedState>`.
//!
//! STAGED (rules.md s4): proargmodes/proargnames/proallargtypes (only IN-mode args
//! are reachable; the OUT/INOUT/VARIADIC tail columns stay NULL), proargdefaults,
//! the parsed `prosqlbody`, GenerateDependencies (pg_depend), and the C
//! `fmgr_internal_validator`/`fmgr_sql_validator` LANGUAGE validation. The row +
//! lookup are the must-have; SQL/PL function execution lands in a later milestone.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::access::skey::ScanKeyData;
use crate::backend::access::common::heaptuple::{heap_copytuple, heap_form_tuple, heap_freetuple};
use crate::backend::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
};
use crate::backend::catalog::heap::name_data;
use crate::backend::catalog::indexing::{catalog_tuple_delete, catalog_tuple_insert, catalog_tuple_update};
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::catalog::objectaddress::ObjectAddress;
use crate::catalog::pg_proc::{
    self as p, FormData_pg_proc, ProcedureRelationId, PROKIND_FUNCTION, PROKIND_PROCEDURE,
    PROPARALLEL_SAFE, PROVOLATILE_VOLATILE,
};
use crate::postgres::{
    BoolGetDatum, CharGetDatum, Datum, Float4GetDatum, Int16GetDatum, NameGetDatum,
    ObjectIdGetDatum, PointerGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;

/// Language OIDs (pg_language). PG: `internal` is 12, `sql` is 14.
pub const INTERNALLANGUAGEID: Oid = Oid::new(12);
pub const SQLLANGUAGEID: Oid = Oid::new(14);

/// PG `ProcedureCreate` (reachable async form): form + insert the pg_proc row for a
/// new function/procedure. On CREATE OR REPLACE, an existing row with the same
/// (proname, proargtypes, pronamespace) is updated in place rather than inserted.
///
/// `arg_types` are the resolved IN-parameter type OIDs (in order); `is_procedure`
/// selects prokind; `prolang` is the resolved language OID. Returns the function's
/// `ObjectAddress`.
#[allow(clippy::too_many_arguments, reason = "mirrors the C ProcedureCreate inputs")]
pub async fn procedure_create(
    shared: &Arc<SharedState>,
    procedure_name: &str,
    proc_namespace: Oid,
    replace: bool,
    return_type: Oid,
    prolang: Oid,
    arg_types: &[Oid],
    prosrc: &str,
    is_procedure: bool,
) -> ObjectAddress {
    let owner_id = crate::backend::utils::init::miscinit::get_user_id();
    let prokind = if is_procedure { PROKIND_PROCEDURE } else { PROKIND_FUNCTION };

    let pg_proc = relation_id_get_relation(ProcedureRelationId)
        .unwrap_or_else(|| unreachable!("pg_proc is nailed/open"));
    let desc = pg_proc.rd_att.clone().unwrap_or_else(|| unreachable!("pg_proc has a descriptor"));
    let natts = desc.natts as usize;

    // CREATE OR REPLACE: an existing same-signature row is reused (its OID kept).
    let existing = proc_match_in_namespace(shared, procedure_name, proc_namespace, arg_types).await;
    if let Some((oid, tid)) = existing {
        if !replace {
            relation_close(pg_proc);
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_FUNCTION)
                    .errmsg(format!("function \"{procedure_name}\" already exists with same argument types"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
        let argvec = crate::utils::builtins::buildoidvector(arg_types);
        let (values, isnull) = build_proc_values(
            natts, oid, procedure_name, proc_namespace, owner_id, prolang, prokind,
            return_type, arg_types.len(), argvec, prosrc,
        );
        let mut tup = heap_form_tuple(&desc, &values, &isnull);
        catalog_tuple_update(shared, &pg_proc, &tid, &mut tup).await;
        heap_freetuple(tup);
        relation_close(pg_proc);
        return ObjectAddress { classId: ProcedureRelationId, objectId: oid, objectSubId: 0 };
    }

    let new_oid = crate::backend::catalog::catalog::get_new_object_id(shared);
    let argvec = crate::utils::builtins::buildoidvector(arg_types);
    let (values, isnull) = build_proc_values(
        natts, new_oid, procedure_name, proc_namespace, owner_id, prolang, prokind,
        return_type, arg_types.len(), argvec, prosrc,
    );
    let mut tup = heap_form_tuple(&desc, &values, &isnull);
    catalog_tuple_insert(shared, &pg_proc, &mut tup).await;
    heap_freetuple(tup);
    relation_close(pg_proc);

    ObjectAddress { classId: ProcedureRelationId, objectId: new_oid, objectSubId: 0 }
}

/// Build the pg_proc Datum/isnull arrays for one row. The OUT/INOUT tail columns
/// (proallargtypes/proargmodes/proargnames/proargdefaults/protrftypes/probin/
/// prosqlbody/proconfig/proacl) stay NULL on the IN-only reachable path.
#[allow(clippy::too_many_arguments, reason = "one row's columns, set in one place")]
fn build_proc_values(
    natts: usize,
    oid: Oid,
    name: &str,
    proc_namespace: Oid,
    owner_id: Oid,
    prolang: Oid,
    prokind: i8,
    return_type: Oid,
    nargs: usize,
    argvec: *mut crate::c::oidvector,
    prosrc: &str,
) -> (Vec<Datum>, Vec<bool>) {
    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;

    let nd = name_data(name);
    set(&mut values, p::Anum_pg_proc_oid, ObjectIdGetDatum(oid));
    set(&mut values, p::Anum_pg_proc_proname, NameGetDatum(&nd));
    set(&mut values, p::Anum_pg_proc_pronamespace, ObjectIdGetDatum(proc_namespace));
    set(&mut values, p::Anum_pg_proc_proowner, ObjectIdGetDatum(owner_id));
    set(&mut values, p::Anum_pg_proc_prolang, ObjectIdGetDatum(prolang));
    set(&mut values, p::Anum_pg_proc_procost, Float4GetDatum(1.0));
    set(&mut values, p::Anum_pg_proc_prorows, Float4GetDatum(0.0));
    set(&mut values, p::Anum_pg_proc_provariadic, ObjectIdGetDatum(InvalidOid));
    set(&mut values, p::Anum_pg_proc_prosupport, ObjectIdGetDatum(InvalidOid));
    set(&mut values, p::Anum_pg_proc_prokind, CharGetDatum(prokind));
    set(&mut values, p::Anum_pg_proc_prosecdef, BoolGetDatum(false));
    set(&mut values, p::Anum_pg_proc_proleakproof, BoolGetDatum(false));
    set(&mut values, p::Anum_pg_proc_proisstrict, BoolGetDatum(false));
    set(&mut values, p::Anum_pg_proc_proretset, BoolGetDatum(false));
    set(&mut values, p::Anum_pg_proc_provolatile, CharGetDatum(PROVOLATILE_VOLATILE));
    set(&mut values, p::Anum_pg_proc_proparallel, CharGetDatum(PROPARALLEL_SAFE));
    set(&mut values, p::Anum_pg_proc_pronargs, Int16GetDatum(i16::try_from(nargs).unwrap_or(0)));
    set(&mut values, p::Anum_pg_proc_pronargdefaults, Int16GetDatum(0));
    set(&mut values, p::Anum_pg_proc_prorettype, ObjectIdGetDatum(return_type));
    set(&mut values, p::Anum_pg_proc_proargtypes, PointerGetDatum(argvec.cast::<u8>()));
    set(&mut values, p::Anum_pg_proc_prosrc, crate::utils::builtins::CStringGetTextDatum(prosrc));

    for anum in [
        p::Anum_pg_proc_proallargtypes, p::Anum_pg_proc_proargmodes,
        p::Anum_pg_proc_proargnames, p::Anum_pg_proc_proargdefaults,
        p::Anum_pg_proc_protrftypes, p::Anum_pg_proc_probin,
        p::Anum_pg_proc_prosqlbody, p::Anum_pg_proc_proconfig, p::Anum_pg_proc_proacl,
    ] {
        isnull[(anum - 1) as usize] = true;
    }
    (values, isnull)
}

/// Scan pg_proc by `proname`, returning every matching row's
/// (oid, proargtypes, pronamespace, tid). Heap-scan with a name equality key (the
/// PROCNAMEARGSNSP on-disk index is not built at this milestone; the scan applies
/// the key, then the caller filters on argtypes + namespace).
async fn proc_rows_by_name(
    shared: &Arc<SharedState>,
    name: &str,
) -> Vec<(Oid, Vec<Oid>, Oid, ItemPointerData)> {
    let pg_proc = relation_id_get_relation(ProcedureRelationId)
        .unwrap_or_else(|| unreachable!("pg_proc is nailed/open"));
    let nd = name_data(name);
    let key = [name_scankey(p::Anum_pg_proc_proname as i16, NameGetDatum(&nd))];
    let snap = systable_scan_snapshot(shared, &pg_proc, None);
    let mut scan = systable_beginscan(shared, &pg_proc, InvalidOid, false, &snap, &key);
    let mut out = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy before endscan, then read its Form fields.
        let tuple = unsafe { heap_copytuple(tref) };
        if proc_name_of(&tuple) == name {
            let (oid, ns, args) = read_proc_signature(&tuple);
            out.push((oid, args, ns, tuple.t_self));
        }
        heap_freetuple(tuple);
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_proc);
    out
}

/// Find the pg_proc row matching (name, argtypes, namespace) exactly, if any.
async fn proc_match_in_namespace(
    shared: &Arc<SharedState>,
    name: &str,
    proc_namespace: Oid,
    arg_types: &[Oid],
) -> Option<(Oid, ItemPointerData)> {
    proc_rows_by_name(shared, name).await.into_iter().find_map(|(oid, args, ns, tid)| {
        (ns == proc_namespace && args == arg_types).then_some((oid, tid))
    })
}

/// A BTEqual equality scankey on a Name-typed attribute (or, with an OID argument,
/// an OID attribute -- the strategy/flags are the same).
fn name_scankey(attno: i16, argument: Datum) -> ScanKeyData {
    ScanKeyData {
        flags: 0,
        attno,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: crate::fmgr::FmgrInfo {
            fn_addr: None, oid: InvalidOid, nargs: 0, strict: false, retset: false,
            stats: 0, extra: 0, mcxt: (), expr: None,
        },
        argument,
    }
}

/// Read (oid, pronamespace, proargtypes) out of a pg_proc tuple's fixed part.
fn read_proc_signature(tuple: &HeapTupleData) -> (Oid, Oid, Vec<Oid>) {
    // SAFETY: `tuple` is a pg_proc row; GETSTRUCT yields its Form fixed part.
    let pf = unsafe { &*GETSTRUCT(tuple).cast::<FormData_pg_proc>() };
    let args = oidvector_oids(std::ptr::addr_of!(pf.proargtypes).cast::<crate::c::oidvector>());
    (pf.oid, pf.pronamespace, args)
}

/// Read the proname out of a pg_proc tuple's fixed part.
fn proc_name_of(tuple: &HeapTupleData) -> String {
    // SAFETY: `tuple` is a pg_proc row; GETSTRUCT yields its Form fixed part.
    let pf = unsafe { &*GETSTRUCT(tuple).cast::<FormData_pg_proc>() };
    let bytes = &pf.proname.data;
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Decode the OIDs out of an on-disk `oidvector` (the trailing flexible `Oid` array
/// after the fixed header). Inverse of `buildoidvector` for the M10 read path.
fn oidvector_oids(v: *const crate::c::oidvector) -> Vec<Oid> {
    // SAFETY: `v` points into a live pg_proc tuple body (the proargtypes column);
    // `dim1` is its element count and `values` heads the trailing Oid array.
    unsafe {
        let n = (*v).dim1.max(0) as usize;
        let base = std::ptr::addr_of!((*v).values).cast::<Oid>();
        (0..n).map(|i| *base.add(i)).collect()
    }
}

/// Resolve a function/procedure name to its OID via a name search (first match;
/// the M10 reachable case is the unqualified `public`/`pg_catalog` name). Returns
/// `None` if absent.
pub async fn proc_lookup_by_name(shared: &Arc<SharedState>, name: &str) -> Option<Oid> {
    proc_rows_by_name(shared, name).await.into_iter().map(|(oid, ..)| oid).next()
}

/// PG `RemoveFunctionById` (the M10 leaf): delete the pg_proc row for `func_id`.
/// pg_aggregate / dependency teardown stages with their catalogs.
pub async fn remove_procedure_by_id(shared: &Arc<SharedState>, func_id: Oid) {
    let pg_proc = relation_id_get_relation(ProcedureRelationId)
        .unwrap_or_else(|| unreachable!("pg_proc is nailed/open"));
    let key = [name_scankey(p::Anum_pg_proc_oid as i16, ObjectIdGetDatum(func_id))];
    let snap = systable_scan_snapshot(shared, &pg_proc, None);
    let mut scan = systable_beginscan(shared, &pg_proc, InvalidOid, false, &snap, &key);
    let mut tids = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; only its TID is retained.
        let tuple = unsafe { heap_copytuple(tref) };
        tids.push(tuple.t_self);
        heap_freetuple(tuple);
    }
    systable_endscan(shared, &mut scan);
    for tid in tids {
        catalog_tuple_delete(shared, &pg_proc, &tid).await;
    }
    relation_close(pg_proc);
}

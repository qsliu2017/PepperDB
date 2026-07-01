//! Convenience catalog lookups over the syscache. Translated from the
//! M2-reachable accessors of `src/backend/utils/cache/lsyscache.c`.
//!
//! These are thin readers: `SearchSysCache1(...)` + `GETSTRUCT` to pull a few
//! columns out of a catalog row. Two flavors:
//!  - SYNC accessors (`get_type_output_info`, `get_typlenbyval`, ...) read an
//!    already-warm syscache entry (a cache HIT). They serve the sync callers
//!    (printtup/makefuncs/tupdesc). `get_type_output_info` additionally falls back
//!    to the builtin int2/4/8 output map when the catalog isn't warmed yet -- this
//!    is the M1 SELECT-1 path, where no pg_type heap exists. (This replaces the
//!    step-02 shim: when the catalog IS warm the value comes from real syscache;
//!    the builtin fallback only covers the catalog-less bootstrap window.)
//!  - ASYNC accessors warm the syscache (scan) then read, for callers in async
//!    context (executor, tests).

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]
use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::backend::utils::cache::syscache::{
    release_sys_cache, search_sys_cache, search_sys_cache_populate,
};
use crate::catalog::pg_type::{Form_pg_type, FormData_pg_type};
use crate::postgres::{Datum, ObjectIdGetDatum};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::syscache::SysCacheIdentifier;

/// Read the `Form_pg_type` out of a held TYPEOID syscache tuple, returning a borrow
/// tied to the tuple borrow (rule 10: `&FormData_pg_type` lifetime-rooted in the
/// `&HeapTupleData`, not a fake `'static`). The caller holds the syscache reference
/// (and releases it) for at least as long as this borrow lives.
///
/// SAFETY: `tuple`'s fixed part is a pg_type row (a held TYPEOID syscache hit).
unsafe fn type_form(tuple: &HeapTupleData) -> &FormData_pg_type {
    let pt: Form_pg_type = GETSTRUCT(tuple).cast::<FormData_pg_type>();
    // SAFETY: `pt` points into `tuple`'s body (GETSTRUCT offset); the returned
    // borrow is tied to `tuple`'s lifetime so it cannot outlive the held tuple.
    unsafe { &*pt }
}

// ---------------------------------------------------------------------------
// getTypeOutputInfo (sync, hit-or-builtin) + async variant
// ---------------------------------------------------------------------------

/// `getTypeOutputInfo` (SYNC): return `(typoutput, typIsVarlena)` for `type`.
/// Reads pg_type via a warm TYPEOID syscache HIT; if the catalog is not yet warmed
/// (M1 SELECT 1, catalog-less), falls back to the builtin int2/4/8 output map.
///
/// The step-02 shim is gone: when the syscache is warm this returns the REAL
/// `typoutput` from the pg_type row (so M2 type output works for any seeded type);
/// the builtin fallback covers only the bootstrap window before any pg_type heap
/// exists, which is exactly where M1's printtup runs.
#[must_use]
pub fn get_type_output_info(r#type: Oid) -> (Oid, bool) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(r#type)]) {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            (pt.typoutput, (!pt.typbyval) && pt.typlen == -1)
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_type_output(r#type)
}

/// ASYNC `getTypeOutputInfo`: warm the TYPEOID cache then read. Use in async
/// context to guarantee the real catalog value.
pub async fn get_type_output_info_populate(shared: &Arc<SharedState>, r#type: Oid) -> (Oid, bool) {
    if let Some(tuple) =
        search_sys_cache_populate(shared, SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(r#type)]).await
    {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            (pt.typoutput, (!pt.typbyval) && pt.typlen == -1)
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_type_output(r#type)
}

/// The builtin int2/4/8 output map (bootstrap-window fallback; the int types are
/// pass-by-value fixed-length, so `typIsVarlena` is false).
fn builtin_type_output(r#type: Oid) -> (Oid, bool) {
    use crate::catalog::genbki::{
        BOOLOID, BPCHAROID, BYTEAOID, CHAROID, INT2OID, INT4OID, INT8OID, NAMEOID, TEXTOID,
        VARCHAROID,
    };
    use crate::utils::fmgroids::{
        F_BOOLOUT, F_BPCHAROUT, F_BYTEAOUT, F_CHAROUT, F_INT2OUT, F_INT4OUT, F_INT8OUT, F_NAMEOUT,
        F_TEXTOUT, F_VARCHAROUT,
    };
    // The varlena string types set typIsVarlena (typlen -1); name/char are fixed.
    let (typoutput, is_varlena) = match r#type {
        t if t == INT4OID => (F_INT4OUT, false),
        t if t == INT2OID => (F_INT2OUT, false),
        t if t == INT8OID => (F_INT8OUT, false),
        t if t == BOOLOID => (F_BOOLOUT, false),
        t if t == CHAROID => (F_CHAROUT, false),
        t if t == NAMEOID => (F_NAMEOUT, false),
        t if t == TEXTOID => (F_TEXTOUT, true),
        t if t == BPCHAROID => (F_BPCHAROUT, true),
        t if t == VARCHAROID => (F_VARCHAROUT, true),
        t if t == BYTEAOID => (F_BYTEAOUT, true),
        _ => cache_lookup_failed(r#type),
    };
    (typoutput, is_varlena)
}

/// Raise PG's "cache lookup failed for type" error (used when a sync accessor hits
/// a cold cache for a non-builtin type). `elog!(ERROR, ..)` raises; the trailing
/// `unreachable!` gives the call site a diverging value.
fn cache_lookup_failed(typid: Oid) -> ! {
    crate::elog!(
        crate::utils::elog::ERROR,
        format!("cache lookup failed for type {} (syscache not warm)", typid.get())
    );
    unreachable!("elog!(ERROR) raises")
}

// ---------------------------------------------------------------------------
// get_typlenbyval / get_typlenbyvalalign (sync, hit) + async variants
// ---------------------------------------------------------------------------

/// `get_typlenbyval` (SYNC, hit): `(typlen, typbyval)` from a warm TYPEOID entry.
/// For the builtin int types (catalog not warm) returns the known layout.
#[must_use]
pub fn get_typlenbyval(typid: Oid) -> (i16, bool) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]) {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            (pt.typlen, pt.typbyval)
        };
        release_sys_cache(tuple);
        return out;
    }
    match builtin_typlenbyvalalign(typid) {
        Some((l, b, _)) => (l, b),
        None => cache_lookup_failed(typid),
    }
}

/// `get_typlenbyvalalign` (SYNC, hit): `(typlen, typbyval, typalign)`.
#[must_use]
pub fn get_typlenbyvalalign(typid: Oid) -> (i16, bool, u8) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]) {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            (pt.typlen, pt.typbyval, pt.typalign as u8)
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_typlenbyvalalign(typid).unwrap_or_else(|| cache_lookup_failed(typid))
}

/// ASYNC `get_typlenbyvalalign`: warm + read.
pub async fn get_typlenbyvalalign_populate(shared: &Arc<SharedState>, typid: Oid) -> (i16, bool, u8) {
    if let Some(tuple) =
        search_sys_cache_populate(shared, SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]).await
    {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            (pt.typlen, pt.typbyval, pt.typalign as u8)
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_typlenbyvalalign(typid).unwrap_or_else(|| cache_lookup_failed(typid))
}

/// `get_typcollation` (SYNC): the type's `typcollation` (the collation to use for
/// a value of this type, or InvalidOid if the type is not collatable). Reads a warm
/// TYPEOID entry; for the catalog-less bootstrap window the builtin scalar types
/// (int2/4/8) are non-collatable, so the fallback returns InvalidOid.
#[must_use]
pub fn get_typcollation(typid: Oid) -> Oid {
    use crate::catalog::genbki::{UNKNOWNOID, VOIDOID};
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)]) {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            pt.typcollation
        };
        release_sys_cache(tuple);
        return out;
    }
    // Bootstrap-window fallback: the builtin scalar types reachable before the
    // catalog is warm (int2/4/8, plus the pseudo-types UNKNOWN/VOID used for
    // not-yet-resolved $n params) are all non-collatable.
    if builtin_typlenbyvalalign(typid).is_some() || typid == UNKNOWNOID || typid == VOIDOID {
        return crate::postgres_ext::InvalidOid;
    }
    cache_lookup_failed(typid)
}

/// `type_is_collatable` (SYNC): does this type have a nonzero `typcollation`?
#[must_use]
pub fn type_is_collatable(typid: Oid) -> bool {
    crate::c::OidIsValid(get_typcollation(typid))
}

/// Builtin layout for the base types reachable in the bootstrap window (before
/// the pg_type catalog is warm): the int/bool set plus the step-10 string types.
fn builtin_typlenbyvalalign(typid: Oid) -> Option<(i16, bool, u8)> {
    use crate::catalog::genbki::{
        BOOLOID, BPCHAROID, BYTEAOID, CHAROID, INT2OID, INT4OID, INT8OID, NAMEOID, TEXTOID,
        VARCHAROID,
    };
    use crate::catalog::pg_type::{TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT};
    let r = match typid {
        t if t == BOOLOID => (1, true, TYPALIGN_CHAR as u8),
        t if t == INT2OID => (2, true, TYPALIGN_SHORT as u8),
        t if t == INT4OID => (4, true, TYPALIGN_INT as u8),
        t if t == INT8OID => (8, true, TYPALIGN_DOUBLE as u8),
        t if t == CHAROID => (1, true, TYPALIGN_CHAR as u8),
        t if t == NAMEOID => (64, false, TYPALIGN_CHAR as u8),
        t if t == TEXTOID || t == BPCHAROID || t == VARCHAROID || t == BYTEAOID => {
            (-1, false, TYPALIGN_INT as u8)
        }
        _ => return None,
    };
    Some(r)
}

// ---------------------------------------------------------------------------
// get_rel_name / get_rel_namespace (sync, via relcache) + async
// ---------------------------------------------------------------------------

/// `get_rel_name` (SYNC): the relation name, from a cached relcache entry.
#[must_use]
pub fn get_rel_name(relid: Oid) -> Option<String> {
    let rel = crate::utils::relcache::RelationIdGetRelation(relid)?;
    let name = rel.rd_rel.as_deref().map(|form| name_to_string(&form.relname));
    crate::utils::relcache::RelationClose(rel);
    name
}

/// `get_rel_namespace` (SYNC): the relation's namespace OID, or InvalidOid.
#[must_use]
pub fn get_rel_namespace(relid: Oid) -> Oid {
    crate::utils::relcache::RelationIdGetRelation(relid).map_or(
        crate::postgres_ext::InvalidOid,
        |rel| {
            let ns = rel.rd_rel.as_deref()
                .map_or(crate::postgres_ext::InvalidOid, |form| form.relnamespace);
            crate::utils::relcache::RelationClose(rel);
            ns
        },
    )
}

fn name_to_string(name: &crate::c::NameData) -> String {
    let end = name.data.iter().position(|&b| b == 0).unwrap_or(name.data.len());
    String::from_utf8_lossy(&name.data[..end]).into_owned()
}

// ---------------------------------------------------------------------------
// pg_proc accessors (SYNC, warm-hit): read a few columns of a pg_proc row.
// ---------------------------------------------------------------------------

/// Raise PG's "cache lookup failed for function" error (a sync accessor hit a cold
/// PROCOID cache). Diverges (>= ERROR).
#[cold]
fn proc_cache_lookup_failed(funcid: Oid) -> ! {
    crate::elog!(
        crate::utils::elog::ERROR,
        format!("cache lookup failed for function {} (syscache not warm)", funcid.get())
    );
    unreachable!("elog!(ERROR) raises")
}

/// Read the `Form_pg_proc` out of a held PROCOID syscache tuple. The borrow is tied
/// to the tuple borrow (rule 10).
///
/// SAFETY: `tuple`'s fixed part is a pg_proc row (a held PROCOID syscache hit).
unsafe fn proc_form(tuple: &HeapTupleData) -> &crate::catalog::pg_proc::FormData_pg_proc {
    let p = GETSTRUCT(tuple).cast::<crate::catalog::pg_proc::FormData_pg_proc>();
    // SAFETY: `p` points into `tuple`'s body; the borrow is rooted in `tuple`.
    unsafe { &*p }
}

/// `get_func_retset` (SYNC): whether the function returns a set (proretset). Reads
/// pg_proc via a warm PROCOID hit. Used by make_op to set `OpExpr.opretset`.
#[must_use]
pub fn get_func_retset(funcid: Oid) -> bool {
    let Some(tuple) = search_sys_cache(SysCacheIdentifier::PROCOID, &[ObjectIdGetDatum(funcid)])
    else {
        proc_cache_lookup_failed(funcid);
    };
    // SAFETY: `tuple` is a held PROCOID hit -> a pg_proc row.
    let retset = unsafe { proc_form(&*tuple) }.proretset;
    release_sys_cache(tuple);
    retset
}

/// `get_func_rettype` (SYNC): the function's result type (prorettype).
#[must_use]
pub fn get_func_rettype(funcid: Oid) -> Oid {
    let Some(tuple) = search_sys_cache(SysCacheIdentifier::PROCOID, &[ObjectIdGetDatum(funcid)])
    else {
        proc_cache_lookup_failed(funcid);
    };
    // SAFETY: `tuple` is a held PROCOID hit -> a pg_proc row.
    let rettype = unsafe { proc_form(&*tuple) }.prorettype;
    release_sys_cache(tuple);
    rettype
}

/// `get_func_nargs` (SYNC): the function's declared input-argument count
/// (pronargs). Used by the typmod-coercion path to decide whether to append the
/// typmod / isExplicit arguments to a length-coercion FuncExpr.
#[must_use]
pub fn get_func_nargs(funcid: Oid) -> i16 {
    let Some(tuple) = search_sys_cache(SysCacheIdentifier::PROCOID, &[ObjectIdGetDatum(funcid)])
    else {
        proc_cache_lookup_failed(funcid);
    };
    // SAFETY: `tuple` is a held PROCOID hit -> a pg_proc row.
    let nargs = unsafe { proc_form(&*tuple) }.pronargs;
    release_sys_cache(tuple);
    nargs
}

// ---------------------------------------------------------------------------
// M4 (step 23): cast-resolution accessors (getTypeInputInfo / get_cast_func /
// get_type_category_preferred). Sync (warm-hit) readers over the syscache.
// ---------------------------------------------------------------------------

/// `getTypeInputInfo` (SYNC, hit): `(typinput, typioparam)` for `type`. The
/// typioparam is the type's element type if it is an array type, else the type's
/// own OID (PG `getTypeIOParam`). Reads pg_type via a warm TYPEOID hit; falls back
/// to the builtin int2/4/8 input map for the catalog-less bootstrap window.
#[must_use]
pub fn get_type_input_info(r#type: Oid) -> (Oid, Oid) {
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(r#type)]) {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            let typioparam = if pt.typelem == crate::postgres_ext::InvalidOid {
                r#type
            } else {
                pt.typelem
            };
            (pt.typinput, typioparam)
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_type_input(r#type)
}

/// Async warm-and-read variant of [`get_type_input_info`]: warms the TYPEOID cache
/// (so a later sync read hits) and returns `(typinput, typioparam)`.
pub async fn get_type_input_info_populate(shared: &Arc<SharedState>, r#type: Oid) -> (Oid, Oid) {
    if let Some(tuple) =
        search_sys_cache_populate(shared, SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(r#type)]).await
    {
        // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
        let out = {
            let pt = unsafe { type_form(&*tuple) };
            let typioparam = if pt.typelem == crate::postgres_ext::InvalidOid {
                r#type
            } else {
                pt.typelem
            };
            (pt.typinput, typioparam)
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_type_input(r#type)
}

/// The builtin int2/4/8 input map (bootstrap-window fallback; typioparam is the
/// type's own OID since none is an array type).
fn builtin_type_input(r#type: Oid) -> (Oid, Oid) {
    use crate::catalog::genbki::{
        BOOLOID, BPCHAROID, BYTEAOID, CHAROID, INT2OID, INT4OID, INT8OID, NAMEOID, TEXTOID,
        VARCHAROID,
    };
    use crate::utils::fmgroids::{
        F_BOOLIN, F_BPCHARIN, F_BYTEAIN, F_CHARIN, F_INT2IN, F_INT4IN, F_INT8IN, F_NAMEIN,
        F_TEXTIN, F_VARCHARIN,
    };
    let typinput = match r#type {
        t if t == INT4OID => F_INT4IN,
        t if t == INT2OID => F_INT2IN,
        t if t == INT8OID => F_INT8IN,
        t if t == BOOLOID => F_BOOLIN,
        t if t == CHAROID => F_CHARIN,
        t if t == NAMEOID => F_NAMEIN,
        t if t == TEXTOID => F_TEXTIN,
        t if t == BPCHAROID => F_BPCHARIN,
        t if t == VARCHAROID => F_VARCHARIN,
        t if t == BYTEAOID => F_BYTEAIN,
        _ => cache_lookup_failed(r#type),
    };
    (typinput, r#type)
}

/// `get_type_category_preferred` (SYNC, hit): `(typcategory, typispreferred)`.
/// Reads pg_type via a warm TYPEOID hit (used by `select_common_type`).
#[must_use]
pub fn get_type_category_preferred(typid: Oid) -> (i8, bool) {
    let Some(tuple) = search_sys_cache(SysCacheIdentifier::TYPEOID, &[ObjectIdGetDatum(typid)])
    else {
        cache_lookup_failed(typid);
    };
    // SAFETY: held syscache tuple; borrow ends before release_sys_cache.
    let out = {
        let pt = unsafe { type_form(&*tuple) };
        (pt.typcategory, pt.typispreferred)
    };
    release_sys_cache(tuple);
    out
}

/// Read the `(castfunc, castcontext, castmethod)` of a pg_cast row for
/// `(source, target)` via a warm CASTSOURCETARGET hit. `None` if no such cast.
/// PG's `find_coercion_pathway` inlines this `SearchSysCache2(CASTSOURCETARGET)`.
#[must_use]
pub fn get_cast_info(source: Oid, target: Oid) -> Option<(Oid, i8, i8)> {
    use crate::catalog::pg_cast::{Form_pg_cast, FormData_pg_cast};
    let tuple = search_sys_cache(
        SysCacheIdentifier::CASTSOURCETARGET,
        &[ObjectIdGetDatum(source), ObjectIdGetDatum(target)],
    )?;
    // SAFETY: a held CASTSOURCETARGET hit -> a pg_cast row.
    let out = {
        let pc: Form_pg_cast = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_cast>();
        let pc = unsafe { &*pc };
        (pc.castfunc, pc.castcontext, pc.castmethod)
    };
    release_sys_cache(tuple);
    Some(out)
}

// Keep Datum referenced (used by the public header re-exports' signatures).
const _: fn(Oid) -> Datum = ObjectIdGetDatum;

// ---------------------------------------------------------------------------
// Join-operator interpretation (op_mergejoinable / op_hashjoinable /
// get_mergejoin_opfamilies / get_op_index_interpretation)
// ---------------------------------------------------------------------------
//
// PG answers these from pg_amop (the AMOPOPID syscache list) and the
// pg_operator.oprcanmerge/oprcanhash flags. In this port the AMOPOPID list-scan
// syscache is not wired and bootstrap seeds oprcanmerge/oprcanhash as false, so
// neither source is usable yet. For M7 we recognize the builtin "=" operators of
// the seeded types and map each to its btree/hash opfamily, exactly as
// pg_amop.dat/pg_operator.dat declare. This is the builtin-table form of the
// pg_amop lookup; it lights up EC absorption (initsplan check_mergejoinable) and
// the merge/hash clause discovery in joinpath. The cross-type "=" operators and
// the array_eq/record_eq typcache cases grow with the AMOPOPID syscache.
// TODO(syscache): replace with the AMOPOPID list-scan once it lands.

use crate::access::cmptype::CompareType;

/// One builtin "=" operator's opfamily memberships (the rows pg_amop.dat declares
/// for that operator). `btree_opfamily`/`hash_opfamily` are the integer/text/...
/// opfamily OIDs; either may be `None` if the type has no such index AM (none of
/// the M7 types lack one, but the shape is kept general).
struct EqOpFamilies {
    opno: u32,
    lefttype: u32,
    righttype: u32,
    btree_opfamily: Oid,
    hash_opfamily: Option<Oid>,
}

// btree/hash opfamily OIDs (catalog_oids_generated): integer_ops 1976/1977,
// text_ops 1994/1995, oid_ops 1989/1990, bool_ops 424/2222.
const BTREE_INTEGER_FAM: Oid = Oid::new(1976);
const HASH_INTEGER_FAM: Oid = Oid::new(1977);
const BTREE_TEXT_FAM: Oid = Oid::new(1994);
const HASH_TEXT_FAM: Oid = Oid::new(1995);
const BTREE_OID_FAM: Oid = Oid::new(1989);
const HASH_OID_FAM: Oid = Oid::new(1990);
const BTREE_BOOL_FAM: Oid = Oid::new(424);
const HASH_BOOL_FAM: Oid = Oid::new(2222);

// The builtin same-type "=" operators of the M7 seed (pg_operator.dat). int2/int4/
// int8 share the integer opfamilies; bool/text/oid each have their own.
const BUILTIN_EQ_OPS: &[EqOpFamilies] = &[
    EqOpFamilies { opno: 94, lefttype: 21, righttype: 21, btree_opfamily: BTREE_INTEGER_FAM, hash_opfamily: Some(HASH_INTEGER_FAM) }, // int2 =
    EqOpFamilies { opno: 96, lefttype: 23, righttype: 23, btree_opfamily: BTREE_INTEGER_FAM, hash_opfamily: Some(HASH_INTEGER_FAM) }, // int4 =
    EqOpFamilies { opno: 410, lefttype: 20, righttype: 20, btree_opfamily: BTREE_INTEGER_FAM, hash_opfamily: Some(HASH_INTEGER_FAM) }, // int8 =
    EqOpFamilies { opno: 91, lefttype: 16, righttype: 16, btree_opfamily: BTREE_BOOL_FAM, hash_opfamily: Some(HASH_BOOL_FAM) }, // bool =
    EqOpFamilies { opno: 98, lefttype: 25, righttype: 25, btree_opfamily: BTREE_TEXT_FAM, hash_opfamily: Some(HASH_TEXT_FAM) }, // text =
    EqOpFamilies { opno: 607, lefttype: 26, righttype: 26, btree_opfamily: BTREE_OID_FAM, hash_opfamily: Some(HASH_OID_FAM) }, // oid =
];

fn lookup_builtin_eq(opno: Oid) -> Option<&'static EqOpFamilies> {
    BUILTIN_EQ_OPS.iter().find(|e| e.opno == opno.get())
}

/// PG `op_mergejoinable`: true if the operator can be used as a mergejoin clause
/// (a btree equality member of some opfamily). M7 recognizes the builtin "="
/// operators; `inputtype` is unused for these (only array_eq/record_eq need it).
#[must_use]
pub fn op_mergejoinable(opno: Oid, _inputtype: Oid) -> bool {
    lookup_builtin_eq(opno).is_some()
}

/// PG `op_hashjoinable`: true if the operator can be used as a hashjoin clause
/// (a hash equality member of some opfamily). M7 recognizes the builtin "="
/// operators that have a hash opfamily.
#[must_use]
pub fn op_hashjoinable(opno: Oid, _inputtype: Oid) -> bool {
    lookup_builtin_eq(opno).is_some_and(|e| e.hash_opfamily.is_some())
}

/// PG `get_mergejoin_opfamilies`: the btree opfamilies in which `opno` is the
/// equality operator. M7 returns the single builtin btree opfamily for a known
/// "=" operator, else empty.
#[must_use]
pub fn get_mergejoin_opfamilies(opno: Oid) -> Vec<Oid> {
    lookup_builtin_eq(opno).map_or_else(Vec::new, |e| vec![e.btree_opfamily])
}

/// PG `get_opcode`: the operator's implementing function OID (`pg_operator.oprcode`).
/// Reads the OPEROID syscache when warm; falls back to the builtin "=" table for the
/// seeded operators (catalog cold / unit tests). Used to fill the `opfuncid` of an
/// OpExpr the planner synthesizes (the EC-derived join equalities), which the parser
/// never saw and so never resolved.
#[must_use]
pub fn get_opcode(opno: Oid) -> Oid {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_operator::{Form_pg_operator, FormData_pg_operator};
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::OPEROID, &[ObjectIdGetDatum(opno)]) {
        // SAFETY: a held OPEROID hit -> a pg_operator row.
        let out = {
            let op: Form_pg_operator = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_operator>();
            Oid::new(unsafe { &*op }.oprcode.get())
        };
        release_sys_cache(tuple);
        return out;
    }
    // Builtin fallback for the seeded "=" operators (the join-key equalities).
    builtin_eq_opcode(opno).map_or(crate::postgres_ext::InvalidOid, Oid::new)
}

/// The implementing function OID of a builtin "=" operator (pg_proc seed OIDs).
fn builtin_eq_opcode(opno: Oid) -> Option<u32> {
    Some(match opno.get() {
        94 => 63,   // int2eq
        96 => 65,   // int4eq
        410 => 467, // int8eq
        91 => 60,   // booleq
        98 => 67,   // texteq
        607 => 184, // oideq
        _ => return None,
    })
}

/// PG `op_input_types`: the operator's `(oprleft, oprright)` input types. Reads the
/// OPEROID syscache when warm; falls back to the builtin "=" table for the seeded
/// operators (the bootstrap window / unit tests where pg_operator isn't warmed).
#[must_use]
pub fn op_input_types(opno: Oid) -> (Oid, Oid) {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_operator::{Form_pg_operator, FormData_pg_operator};
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::OPEROID, &[ObjectIdGetDatum(opno)]) {
        // SAFETY: a held OPEROID hit -> a pg_operator row.
        let out = {
            let op: Form_pg_operator = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_operator>();
            let op = unsafe { &*op };
            (op.oprleft, op.oprright)
        };
        release_sys_cache(tuple);
        return out;
    }
    // Builtin fallback (catalog not warm): the known "=" operators.
    if let Some(e) = lookup_builtin_eq(opno) {
        return (Oid::new(e.lefttype), Oid::new(e.righttype));
    }
    (crate::postgres_ext::InvalidOid, crate::postgres_ext::InvalidOid)
}

/// PG `get_oprrest`: the operator's restriction-selectivity estimator proc OID
/// (the seeded `pg_operator.oprrest` column). Reads the OPEROID syscache when warm;
/// falls back to the builtin operator table (catalog cold / unit tests). Selfuncs
/// dispatches on this proc OID. Returns `InvalidOid` for an unknown operator.
#[must_use]
pub fn get_oprrest(opno: Oid) -> Oid {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_operator::{Form_pg_operator, FormData_pg_operator};
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::OPEROID, &[ObjectIdGetDatum(opno)]) {
        // SAFETY: a held OPEROID hit -> a pg_operator row.
        let out = {
            let op: Form_pg_operator = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_operator>();
            Oid::new(unsafe { &*op }.oprrest.get())
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_op_selectivity(opno).map_or(crate::postgres_ext::InvalidOid, |s| Oid::new(s.oprrest))
}

/// PG `get_oprjoin`: the operator's join-selectivity estimator proc OID (the seeded
/// `pg_operator.oprjoin` column). Mirrors `get_oprrest`.
#[must_use]
pub fn get_oprjoin(opno: Oid) -> Oid {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_operator::{Form_pg_operator, FormData_pg_operator};
    if let Some(tuple) = search_sys_cache(SysCacheIdentifier::OPEROID, &[ObjectIdGetDatum(opno)]) {
        // SAFETY: a held OPEROID hit -> a pg_operator row.
        let out = {
            let op: Form_pg_operator = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_operator>();
            Oid::new(unsafe { &*op }.oprjoin.get())
        };
        release_sys_cache(tuple);
        return out;
    }
    builtin_op_selectivity(opno).map_or(crate::postgres_ext::InvalidOid, |s| Oid::new(s.oprjoin))
}

/// Cold-catalog fallback for `get_oprrest`/`get_oprjoin`: the (oprrest, oprjoin)
/// estimator proc OIDs for the seeded same-type comparison operators. The "="
/// operators use eqsel/eqjoinsel; the ordering comparisons use scalar{in}eqsel /
/// scalar*joinsel (pg_operator.dat). This covers the M7 join/restriction operators
/// when pg_operator isn't warmed (the syscache path is used otherwise).
struct OpSelectivity {
    oprrest: u32,
    oprjoin: u32,
}

fn builtin_op_selectivity(opno: Oid) -> Option<OpSelectivity> {
    use crate::backend::utils::adt::selfuncs::{
        F_EQJOINSEL, F_EQSEL, F_NEQJOINSEL, F_NEQSEL, F_SCALARGEJOINSEL, F_SCALARGESEL,
        F_SCALARGTJOINSEL, F_SCALARGTSEL, F_SCALARLEJOINSEL, F_SCALARLESEL, F_SCALARLTJOINSEL,
        F_SCALARLTSEL,
    };
    // Equality operators (pg_operator.dat: int2/int4/int8/bool/text/oid "=").
    if lookup_builtin_eq(opno).is_some() {
        return Some(OpSelectivity { oprrest: F_EQSEL, oprjoin: F_EQJOINSEL });
    }
    // Ordering comparisons + "<>" for the integer types (the M7 inequality set).
    let (rest, join) = match opno.get() {
        // int2/int4/int8 "<"
        37 | 95 | 97 | 412 | 418 | 534 | 535 | 1864 | 1865 => (F_SCALARLTSEL, F_SCALARLTJOINSEL),
        // ">"
        76 | 413 | 419 | 520 | 521 | 536 | 537 => (F_SCALARGTSEL, F_SCALARGTJOINSEL),
        // "<="
        80 | 414 | 522 | 523 | 540 | 541 => (F_SCALARLESEL, F_SCALARLEJOINSEL),
        // ">="
        82 | 415 | 524 | 525 | 542 | 543 => (F_SCALARGESEL, F_SCALARGEJOINSEL),
        // "<>"
        518 | 519 | 85 | 411 => (F_NEQSEL, F_NEQJOINSEL),
        _ => return None,
    };
    Some(OpSelectivity { oprrest: rest, oprjoin: join })
}

/// PG `get_op_index_interpretation`: the amcanorder (btree) opfamilies the
/// operator belongs to, with its strategy/cmptype and input types. M7 returns the
/// single builtin btree-equality interpretation for a known "=" operator. The
/// "<>-as-negator-of-=" case (COMPARE_NE) grows with the AMOPOPID list-scan.
#[must_use]
pub fn get_op_index_interpretation(opno: Oid) -> Vec<crate::utils::lsyscache::OpIndexInterpretation> {
    lookup_builtin_eq(opno).map_or_else(Vec::new, |e| {
        vec![crate::utils::lsyscache::OpIndexInterpretation {
            opfamily_id: e.btree_opfamily,
            cmptype: CompareType::Eq,
            oplefttype: Oid::new(e.lefttype),
            oprighttype: Oid::new(e.righttype),
        }]
    })
}

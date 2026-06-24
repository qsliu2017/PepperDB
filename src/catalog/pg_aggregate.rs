//! Translated from PostgreSQL src/include/catalog/pg_aggregate.h

use crate::c::{regproc, text};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

pub const AggregateRelationId: Oid = Oid(2600);

#[repr(C)]
pub struct FormData_pg_aggregate {
    pub aggfnoid: regproc, // BKI_LOOKUP(pg_proc)
    pub aggkind: i8,       // see AGGKIND_* categories
    pub aggnumdirectargs: i16,
    pub aggtransfn: regproc,    // BKI_LOOKUP(pg_proc)
    pub aggfinalfn: regproc,    // BKI_LOOKUP_OPT(pg_proc)
    pub aggcombinefn: regproc,  // BKI_LOOKUP_OPT(pg_proc)
    pub aggserialfn: regproc,   // BKI_LOOKUP_OPT(pg_proc)
    pub aggdeserialfn: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub aggmtransfn: regproc,   // BKI_LOOKUP_OPT(pg_proc)
    pub aggminvtransfn: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub aggmfinalfn: regproc,   // BKI_LOOKUP_OPT(pg_proc)
    pub aggfinalextra: bool,
    pub aggmfinalextra: bool,
    pub aggfinalmodify: i8,  // see AGGMODIFY_*
    pub aggmfinalmodify: i8, // see AGGMODIFY_*
    pub aggsortop: Oid,      // BKI_LOOKUP_OPT(pg_operator)
    pub aggtranstype: Oid,   // BKI_LOOKUP(pg_type)
    pub aggtransspace: i32,
    pub aggmtranstype: Oid, // BKI_LOOKUP_OPT(pg_type)
    pub aggmtransspace: i32,
    // CATALOG_VARLEN (not in fixed part):
    pub agginitval: text,  // can be NULL
    pub aggminitval: text, // can be NULL
}

pub type Form_pg_aggregate = *mut FormData_pg_aggregate; // TODO(ptr)

// DECLARE_TOAST(pg_aggregate, 4159, 4160)
// DECLARE_UNIQUE_INDEX_PKEY(pg_aggregate_fnoid_index, 2650, AggregateFnoidIndexId, ...)
// MAKE_SYSCACHE(AGGFNOID, pg_aggregate_fnoid_index, 16)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_aggregate_aggfnoid: i32 = 1;
pub const Anum_pg_aggregate_aggkind: i32 = 2;
pub const Anum_pg_aggregate_aggnumdirectargs: i32 = 3;
pub const Anum_pg_aggregate_aggtransfn: i32 = 4;
pub const Anum_pg_aggregate_aggfinalfn: i32 = 5;
pub const Anum_pg_aggregate_aggcombinefn: i32 = 6;
pub const Anum_pg_aggregate_aggserialfn: i32 = 7;
pub const Anum_pg_aggregate_aggdeserialfn: i32 = 8;
pub const Anum_pg_aggregate_aggmtransfn: i32 = 9;
pub const Anum_pg_aggregate_aggminvtransfn: i32 = 10;
pub const Anum_pg_aggregate_aggmfinalfn: i32 = 11;
pub const Anum_pg_aggregate_aggfinalextra: i32 = 12;
pub const Anum_pg_aggregate_aggmfinalextra: i32 = 13;
pub const Anum_pg_aggregate_aggfinalmodify: i32 = 14;
pub const Anum_pg_aggregate_aggmfinalmodify: i32 = 15;
pub const Anum_pg_aggregate_aggsortop: i32 = 16;
pub const Anum_pg_aggregate_aggtranstype: i32 = 17;
pub const Anum_pg_aggregate_aggtransspace: i32 = 18;
pub const Anum_pg_aggregate_aggmtranstype: i32 = 19;
pub const Anum_pg_aggregate_aggmtransspace: i32 = 20;
pub const Anum_pg_aggregate_agginitval: i32 = 21;
pub const Anum_pg_aggregate_aggminitval: i32 = 22;
pub const Natts_pg_aggregate: i32 = 22;

// Symbolic values for aggkind column.
pub const AGGKIND_NORMAL: i8 = b'n' as i8;
pub const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
pub const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

// Test for "ordered-set agg including hypothetical case".
pub const fn AGGKIND_IS_ORDERED_SET(kind: i8) -> bool {
    kind != AGGKIND_NORMAL
}

// Symbolic values for aggfinalmodify and aggmfinalmodify columns.
pub const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;
pub const AGGMODIFY_SHAREABLE: i8 = b's' as i8;
pub const AGGMODIFY_READ_WRITE: i8 = b'w' as i8;

// Forward refs; repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::catalog::objectaddress::ObjectAddress in Phase 2")]
pub struct ObjectAddress; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to oidvector type in Phase 2")]
pub struct Oidvector; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to Vec<T> (pg_list List) in Phase 2")]
pub struct List; // TODO(struct-forward)

#[allow(deprecated)]
pub fn AggregateCreate(
    _agg_name: &str,
    _agg_namespace: Oid,
    _replace: bool,
    _agg_kind: i8,
    _num_args: i32,
    _num_direct_args: i32,
    _parameter_types: &Oidvector,
    _all_parameter_types: Datum,
    _parameter_modes: Datum,
    _parameter_names: Datum,
    _parameter_defaults: &List,
    _variadic_arg_type: Oid,
    _aggtransfn_name: &List,
    _aggfinalfn_name: &List,
    _aggcombinefn_name: &List,
    _aggserialfn_name: &List,
    _aggdeserialfn_name: &List,
    _aggmtransfn_name: &List,
    _aggminvtransfn_name: &List,
    _aggmfinalfn_name: &List,
    _finalfn_extra_args: bool,
    _mfinalfn_extra_args: bool,
    _finalfn_modify: i8,
    _mfinalfn_modify: i8,
    _aggsortop_name: &List,
    _agg_trans_type: Oid,
    _agg_trans_space: i32,
    _aggm_trans_type: Oid,
    _aggm_trans_space: i32,
    _agginitval: &str,
    _aggminitval: &str,
    _proparallel: i8,
) -> ObjectAddress {
    unimplemented!()
}

//! statistics.h - Extended statistics and selectivity estimation functions.

use std::ffi::{c_char, c_int, c_void};

use crate::access::attnum::AttrNumber;
use crate::c::{uint32, FLEXIBLE_ARRAY_MEMBER};
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{JoinType, Selectivity};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, SpecialJoinInfo, StatisticExtInfo,
};
use crate::nodes::pg_list::List;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

// HeapTuple comes from access/htup_details.rs (which re-exports HeapTupleData).
use crate::access::htup_details::HeapTuple;

// TODO: dedup when commands/vacuum.h lands.
pub type VacAttrStats = c_void;

// MAX_STATISTICS_TARGET is defined in commands/vacuum.h (currently 10000).
// TODO: dedup when commands/vacuum.h lands.
pub const MAX_STATISTICS_TARGET: c_int = 10000;

pub const STATS_MAX_DIMENSIONS: usize = 8; // max number of attributes

/* Multivariate distinct coefficients */
pub const STATS_NDISTINCT_MAGIC: uint32 = 0xA352BFA4; // struct identifier
pub const STATS_NDISTINCT_TYPE_BASIC: uint32 = 1; // struct version

/* MVNDistinctItem represents a single combination of columns */
#[repr(C)]
pub struct MVNDistinctItem {
    pub ndistinct: f64,            // ndistinct value for this combination
    pub nattributes: c_int,        // number of attributes
    pub attributes: *mut AttrNumber, // attribute numbers
}

/* A MVNDistinct object, comprising all possible combinations of columns */
#[repr(C)]
pub struct MVNDistinct {
    pub magic: uint32,  // magic constant marker
    pub r#type: uint32, // type of ndistinct (BASIC)
    pub nitems: uint32, // number of items in the statistic
    pub items: [MVNDistinctItem; FLEXIBLE_ARRAY_MEMBER],
}

/* Multivariate functional dependencies */
pub const STATS_DEPS_MAGIC: uint32 = 0xB4549A2C; // marks serialized bytea
pub const STATS_DEPS_TYPE_BASIC: uint32 = 1; // basic dependencies type

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
#[repr(C)]
pub struct MVDependency {
    pub degree: f64,            // degree of validity (0-1)
    pub nattributes: AttrNumber, // number of attributes
    pub attributes: [AttrNumber; FLEXIBLE_ARRAY_MEMBER], // attribute numbers
}

#[repr(C)]
pub struct MVDependencies {
    pub magic: uint32,  // magic constant marker
    pub r#type: uint32, // type of MV Dependencies (BASIC)
    pub ndeps: uint32,  // number of dependencies
    pub deps: [*mut MVDependency; FLEXIBLE_ARRAY_MEMBER], // dependencies
}

/* used to flag stats serialized to bytea */
pub const STATS_MCV_MAGIC: uint32 = 0xE1A651C2; // marks serialized bytea
pub const STATS_MCV_TYPE_BASIC: uint32 = 1; // basic MCV list type

/* max items in MCV list */
pub const STATS_MCVLIST_MAX_ITEMS: c_int = MAX_STATISTICS_TARGET;

/*
 * Multivariate MCV (most-common value) lists
 *
 * A straightforward extension of MCV items - i.e. a list (array) of
 * combinations of attribute values, together with a frequency and null flags.
 */
#[repr(C)]
pub struct MCVItem {
    pub frequency: f64,      // frequency of this combination
    pub base_frequency: f64, // frequency if independent
    pub isnull: *mut bool,   // NULL flags
    pub values: *mut Datum,  // item values
}

/* multivariate MCV list - essentially an array of MCV items */
#[repr(C)]
pub struct MCVList {
    pub magic: uint32,           // magic constant marker
    pub r#type: uint32,          // type of MCV list (BASIC)
    pub nitems: uint32,          // number of MCV items in the array
    pub ndimensions: AttrNumber, // number of dimensions
    pub types: [Oid; STATS_MAX_DIMENSIONS], // OIDs of data types
    pub items: [MCVItem; FLEXIBLE_ARRAY_MEMBER], // array of MCV items
}

pub unsafe fn statext_ndistinct_load(mvoid: Oid, inh: bool) -> *mut MVNDistinct {
    unimplemented!()
}

pub unsafe fn statext_dependencies_load(mvoid: Oid, inh: bool) -> *mut MVDependencies {
    unimplemented!()
}

pub unsafe fn statext_mcv_load(mvoid: Oid, inh: bool) -> *mut MCVList {
    unimplemented!()
}

pub unsafe fn BuildRelationExtStatistics(
    onerel: Relation,
    inh: bool,
    totalrows: f64,
    numrows: c_int,
    rows: *mut HeapTuple,
    natts: c_int,
    vacattrstats: *mut *mut VacAttrStats,
) {
    unimplemented!()
}

pub unsafe fn ComputeExtStatisticsRows(
    onerel: Relation,
    natts: c_int,
    vacattrstats: *mut *mut VacAttrStats,
) -> c_int {
    unimplemented!()
}

pub unsafe fn statext_is_kind_built(htup: HeapTuple, r#type: c_char) -> bool {
    unimplemented!()
}

pub unsafe fn dependencies_clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    estimatedclauses: *mut *mut Bitmapset,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn statext_clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    estimatedclauses: *mut *mut Bitmapset,
    is_or: bool,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn has_stats_of_kind(stats: *mut List, requiredkind: c_char) -> bool {
    unimplemented!()
}

pub unsafe fn choose_best_statistics(
    stats: *mut List,
    requiredkind: c_char,
    inh: bool,
    clause_attnums: *mut *mut Bitmapset,
    clause_exprs: *mut *mut List,
    nclauses: c_int,
) -> *mut StatisticExtInfo {
    unimplemented!()
}

pub unsafe fn statext_expressions_load(stxoid: Oid, inh: bool, idx: c_int) -> HeapTuple {
    unimplemented!()
}

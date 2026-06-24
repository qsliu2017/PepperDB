//! Translated from PostgreSQL src/include/statistics/statistics.h

use crate::access::htup::HeapTuple;
use crate::commands::vacuum::{VacAttrStats, MAX_STATISTICS_TARGET};
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, SpecialJoinInfo, StatisticExtInfo};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

pub type AttrNumber = i16; // c.h AttrNumber

pub const STATS_MAX_DIMENSIONS: usize = 8; // max number of attributes

// Multivariate distinct coefficients
pub const STATS_NDISTINCT_MAGIC: u32 = 0xA352BFA4; // struct identifier
pub const STATS_NDISTINCT_TYPE_BASIC: u32 = 1; // struct version

// In-memory: the on-disk form is a pg_ndistinct varlena (magic/type/nitems header
// then packed items); serialization lives in mvdistinct.c. Modeled idiomatically.

/// MVNDistinctItem represents a single combination of columns.
pub struct MVNDistinctItem {
    pub ndistinct: f64,              // ndistinct value for this combination
    pub attributes: Vec<AttrNumber>, // attribute numbers (nattributes folded into len)
}

/// A MVNDistinct object, comprising all possible combinations of columns.
pub struct MVNDistinct {
    pub magic: u32,                  // magic constant marker
    pub r#type: u32,                 // type of ndistinct (BASIC)
    pub items: Vec<MVNDistinctItem>, // nitems folded into len
}

// Multivariate functional dependencies
pub const STATS_DEPS_MAGIC: u32 = 0xB4549A2C; // marks serialized bytea
pub const STATS_DEPS_TYPE_BASIC: u32 = 1; // basic dependencies type

/// Functional dependencies, tracking column-level relationships (values in one
/// column determine values in another one).
// In-memory: on-disk form is a pg_dependencies varlena (dependencies.c).
pub struct MVDependency {
    pub degree: f64,                 // degree of validity (0-1)
    pub attributes: Vec<AttrNumber>, // attribute numbers (nattributes folded into len)
}

pub struct MVDependencies {
    pub magic: u32,                // magic constant marker
    pub r#type: u32,               // type of MV Dependencies (BASIC)
    pub deps: Vec<MVDependency>,   // ndeps folded into len
}

// used to flag stats serialized to bytea
pub const STATS_MCV_MAGIC: u32 = 0xE1A651C2; // marks serialized bytea
pub const STATS_MCV_TYPE_BASIC: u32 = 1; // basic MCV list type

// max items in MCV list
pub const STATS_MCVLIST_MAX_ITEMS: i32 = MAX_STATISTICS_TARGET;

/// Multivariate MCV (most-common value) lists - a list of combinations of
/// attribute values, together with a frequency and null flags.
// In-memory: on-disk form is a pg_mcv_list varlena (mcv.c).
pub struct MCVItem {
    pub frequency: f64,      // frequency of this combination
    pub base_frequency: f64, // frequency if independent
    pub isnull: Vec<bool>,   // NULL flags
    pub values: Vec<Datum>,  // item values
}

/// Multivariate MCV list - essentially an array of MCV items.
pub struct MCVList {
    pub magic: u32,                          // magic constant marker
    pub r#type: u32,                         // type of MCV list (BASIC)
    pub ndimensions: AttrNumber,             // number of dimensions
    pub types: [Oid; STATS_MAX_DIMENSIONS],  // OIDs of data types
    pub items: Vec<MCVItem>,                 // nitems folded into len
}

// InvalidOid sentinel for "not found" -> Option (the C returns NULL).
pub fn statext_ndistinct_load(_mvoid: Oid, _inh: bool) -> Option<MVNDistinct> {
    unimplemented!()
}

pub fn statext_dependencies_load(_mvoid: Oid, _inh: bool) -> Option<MVDependencies> {
    unimplemented!()
}

pub fn statext_mcv_load(_mvoid: Oid, _inh: bool) -> Option<MCVList> {
    unimplemented!()
}

pub fn BuildRelationExtStatistics(
    _onerel: Relation,
    _inh: bool,
    _totalrows: f64,
    _numrows: i32,
    _rows: &mut [HeapTuple],
    _natts: i32,
    _vacattrstats: &mut [*mut VacAttrStats], // TODO(ptr): VacAttrStats **
) {
    unimplemented!()
}

pub fn ComputeExtStatisticsRows(
    _onerel: Relation,
    _natts: i32,
    _vacattrstats: &mut [*mut VacAttrStats], // TODO(ptr): VacAttrStats **
) -> i32 {
    unimplemented!()
}

pub fn statext_is_kind_built(_htup: HeapTuple, _type: u8) -> bool {
    unimplemented!()
}

// `Bitmapset **estimatedclauses` is an in/out accumulator; modeled as &mut.
pub fn dependencies_clauselist_selectivity(
    _root: &mut PlannerInfo,
    _clauses: &[Box<Node>],
    _varRelid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
    _rel: &RelOptInfo,
    _estimatedclauses: &mut Option<Bitmapset>,
) -> Selectivity {
    unimplemented!()
}

pub fn statext_clauselist_selectivity(
    _root: &mut PlannerInfo,
    _clauses: &[Box<Node>],
    _varRelid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
    _rel: &RelOptInfo,
    _estimatedclauses: &mut Option<Bitmapset>,
    _is_or: bool,
) -> Selectivity {
    unimplemented!()
}

pub fn has_stats_of_kind(_stats: &[StatisticExtInfo], _requiredkind: u8) -> bool {
    unimplemented!()
}

// InvalidPointer sentinel -> Option; clause_attnums/clause_exprs are out-params.
pub fn choose_best_statistics(
    _stats: &[StatisticExtInfo],
    _requiredkind: u8,
    _inh: bool,
    _clause_attnums: &mut [Option<Bitmapset>], // Bitmapset **clause_attnums (array)
    _clause_exprs: &mut Vec<Vec<Box<Node>>>,   // List **clause_exprs (out list-of-lists)
    _nclauses: i32,
) -> Option<&'static StatisticExtInfo> {
    unimplemented!()
}

// invalid HeapTuple sentinel -> Option.
pub fn statext_expressions_load(_stxoid: Oid, _inh: bool, _idx: i32) -> Option<HeapTuple> {
    unimplemented!()
}

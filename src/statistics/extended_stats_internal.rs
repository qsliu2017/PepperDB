//! Translated from PostgreSQL src/include/statistics/extended_stats_internal.h
//! POSTGRES extended statistics internal declarations.

use crate::access::attnum::AttrNumber;
use crate::c::bytea;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, SpecialJoinInfo, StatisticExtInfo};
use crate::nodes::primnodes::Const;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::statistics::statistics::{MCVList, MVDependencies, MVNDistinct};
use crate::utils::sortsupport::{SortSupport, SortSupportData};

// VacAttrStats lives in commands/vacuum; used as a `**` array in StatsBuildData.
use crate::commands::vacuum::VacAttrStats;

/// Standard analyze comparison operators/functions for a datatype.
pub struct StdAnalyzeData {
    pub eqopr: Oid,  // '=' operator for datatype, if any
    pub eqfunc: Oid, // and associated function
    pub ltopr: Oid,  // '<' operator for datatype, if any
}

/// A data value paired with the index of the tuple it came from.
pub struct ScalarItem {
    pub value: Datum,
    pub tupno: i32, // position index for tuple it came from
}

/// (de)serialization info for one dimension.
pub struct DimensionInfo {
    pub nvalues: i32,        // number of deduplicated values
    pub nbytes: i32,         // number of bytes (serialized)
    pub nbytes_aligned: i32, // size of deserialized data with alignment
    pub typlen: i32,         // pg_type.typlen
    pub typbyval: bool,      // pg_type.typbyval
}

/// Multi-dimensional sort support. C trailing `ssup[FLEXIBLE_ARRAY_MEMBER]`
/// (in-memory FAM) becomes a `Vec`; `ndims` folds into its length.
pub struct MultiSortSupportData {
    pub ssup: Vec<SortSupportData>, // sort support data for each dimension
}

/// C: `typedef MultiSortSupportData *MultiSortSupport;`.
pub type MultiSortSupport<'a> = &'a mut MultiSortSupportData;

/// One row's worth of values across all sorted dimensions.
pub struct SortItem {
    pub values: *mut Datum, // TODO(ptr): &mut [Datum] once length is threaded
    pub isnull: *mut bool,  // TODO(ptr): &mut [bool]
    pub count: i32,
}

/// A unified representation of the data the statistics is built on.
pub struct StatsBuildData {
    pub numrows: i32,
    pub nattnums: i32,
    pub attnums: *mut AttrNumber,    // TODO(ptr): &mut [AttrNumber]
    pub stats: *mut *mut VacAttrStats, // TODO(ptr): &mut [&mut VacAttrStats]
    pub values: *mut *mut Datum,     // TODO(ptr): per-attribute value arrays
    pub nulls: *mut *mut bool,       // TODO(ptr): per-attribute null arrays
}

pub fn statext_ndistinct_build(_totalrows: f64, _data: &StatsBuildData) -> *mut MVNDistinct {
    unimplemented!()
}

pub fn statext_ndistinct_serialize(_ndistinct: &MVNDistinct) -> *mut bytea {
    unimplemented!()
}

pub fn statext_ndistinct_deserialize(_data: &bytea) -> *mut MVNDistinct {
    unimplemented!()
}

pub fn statext_dependencies_build(_data: &StatsBuildData) -> *mut MVDependencies {
    unimplemented!()
}

pub fn statext_dependencies_serialize(_dependencies: &MVDependencies) -> *mut bytea {
    unimplemented!()
}

pub fn statext_dependencies_deserialize(_data: &bytea) -> *mut MVDependencies {
    unimplemented!()
}

pub fn statext_mcv_build(
    _data: &StatsBuildData,
    _totalrows: f64,
    _stattarget: i32,
) -> *mut MCVList {
    unimplemented!()
}

pub fn statext_mcv_serialize(_mcvlist: &MCVList, _stats: &mut [*mut VacAttrStats]) -> *mut bytea {
    unimplemented!()
}

pub fn statext_mcv_deserialize(_data: &bytea) -> *mut MCVList {
    unimplemented!()
}

pub fn multi_sort_init(_ndims: i32) -> MultiSortSupport<'static> {
    unimplemented!()
}

pub fn multi_sort_add_dimension(
    _mss: MultiSortSupport,
    _sortdim: i32,
    _oper: Oid,
    _collation: Oid,
) {
    unimplemented!()
}

/// qsort comparator (`const void *a, b`, `void *arg`); typed as SortItem here.
pub fn multi_sort_compare(_a: &SortItem, _b: &SortItem, _mss: MultiSortSupport) -> i32 {
    unimplemented!()
}

pub fn multi_sort_compare_dim(
    _dim: i32,
    _a: &SortItem,
    _b: &SortItem,
    _mss: MultiSortSupport,
) -> i32 {
    unimplemented!()
}

pub fn multi_sort_compare_dims(
    _start: i32,
    _end: i32,
    _a: &SortItem,
    _b: &SortItem,
    _mss: MultiSortSupport,
) -> i32 {
    unimplemented!()
}

/// qsort comparator over ScalarItem (`const void *a, b`, `void *arg`).
pub fn compare_scalars_simple(_a: &ScalarItem, _b: &ScalarItem, _ssup: SortSupport) -> i32 {
    unimplemented!()
}

pub fn compare_datums_simple(_a: Datum, _b: Datum, _ssup: SortSupport) -> i32 {
    unimplemented!()
}

/// C fills `*numattrs` out-param -> returned alongside the array.
pub fn build_attnums_array(_attrs: &Bitmapset, _nexprs: i32) -> (*mut AttrNumber, i32) {
    unimplemented!()
}

/// C fills `*nitems` out-param -> returned alongside the array.
pub fn build_sorted_items(
    _data: &StatsBuildData,
    _mss: MultiSortSupport,
    _numattrs: i32,
    _attnums: &mut [AttrNumber],
) -> (*mut SortItem, i32) {
    unimplemented!()
}

/// Returns the parsed (expr, const, expr-on-left) on success, or None.
/// C: `bool` status + `exprp`/`cstp`/`expronleftp` out-params.
pub fn examine_opclause_args(_args: &[Node]) -> Option<(Node, Box<Const>, bool)> {
    unimplemented!()
}

pub fn mcv_combine_selectivities(
    _simple_sel: Selectivity,
    _mcv_sel: Selectivity,
    _mcv_basesel: Selectivity,
    _mcv_totalsel: Selectivity,
) -> Selectivity {
    unimplemented!()
}

/// Out-params `basesel`/`totalsel` returned alongside the selectivity.
pub fn mcv_clauselist_selectivity(
    _root: &mut PlannerInfo,
    _stat: &StatisticExtInfo,
    _clauses: &[Node],
    _var_relid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
    _rel: &RelOptInfo,
) -> (Selectivity, Selectivity, Selectivity) {
    unimplemented!()
}

/// `or_matches` is an in/out accumulator; the trailing selectivity out-params
/// (basesel/overlap_mcvsel/overlap_basesel/totalsel) are returned as a tuple.
pub fn mcv_clause_selectivity_or(
    _root: &mut PlannerInfo,
    _stat: &StatisticExtInfo,
    _mcv: &MCVList,
    _clause: &Node,
    _or_matches: &mut Option<Vec<bool>>,
) -> (Selectivity, Selectivity, Selectivity, Selectivity) {
    unimplemented!()
}

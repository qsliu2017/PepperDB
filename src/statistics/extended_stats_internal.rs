//! statistics/extended_stats_internal.h - POSTGRES extended statistics internal declarations.

use std::ffi::c_int;
use std::ffi::c_void;

use crate::access::attnum::AttrNumber;
use crate::c::bytea;
use crate::c::FLEXIBLE_ARRAY_MEMBER;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::JoinType;
use crate::nodes::nodes::Node;
use crate::nodes::nodes::Selectivity;
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::pathnodes::RelOptInfo;
use crate::nodes::pathnodes::SpecialJoinInfo;
use crate::nodes::pathnodes::StatisticExtInfo;
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::Const;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::sort::sortsupport::SortSupport;
use crate::utils::sort::sortsupport::SortSupportData;

// --- Local stubs for types from not-yet-ported headers ---
// TODO: dedup when statistics/statistics.h lands.
pub type MVNDistinct = c_void;
// TODO: dedup when statistics/statistics.h lands.
pub type MVDependencies = c_void;
// TODO: dedup when statistics/statistics.h lands.
pub type MCVList = c_void;
// TODO: dedup when commands/vacuum.h lands.
pub type VacAttrStats = c_void;

#[repr(C)]
pub struct StdAnalyzeData {
    pub eqopr: Oid,   // '=' operator for datatype, if any
    pub eqfunc: Oid,  // and associated function
    pub ltopr: Oid,   // '<' operator for datatype, if any
}

#[repr(C)]
pub struct ScalarItem {
    pub value: Datum,  // a data value
    pub tupno: c_int,  // position index for tuple it came from
}

/* (de)serialization info */
#[repr(C)]
pub struct DimensionInfo {
    pub nvalues: c_int,         // number of deduplicated values
    pub nbytes: c_int,          // number of bytes (serialized)
    pub nbytes_aligned: c_int,  // size of deserialized data with alignment
    pub typlen: c_int,          // pg_type.typlen
    pub typbyval: bool,         // pg_type.typbyval
}

/* multi-sort */
#[repr(C)]
pub struct MultiSortSupportData {
    pub ndims: c_int,  // number of dimensions
    /* sort support data for each dimension: */
    pub ssup: [SortSupportData; FLEXIBLE_ARRAY_MEMBER],
}

pub type MultiSortSupport = *mut MultiSortSupportData;

#[repr(C)]
pub struct SortItem {
    pub values: *mut Datum,
    pub isnull: *mut bool,
    pub count: c_int,
}

/* a unified representation of the data the statistics is built on */
#[repr(C)]
pub struct StatsBuildData {
    pub numrows: c_int,
    pub nattnums: c_int,
    pub attnums: *mut AttrNumber,
    pub stats: *mut *mut VacAttrStats,
    pub values: *mut *mut Datum,
    pub nulls: *mut *mut bool,
}

pub unsafe fn statext_ndistinct_build(totalrows: f64, data: *mut StatsBuildData) -> *mut MVNDistinct {
    unimplemented!()
}
pub unsafe fn statext_ndistinct_serialize(ndistinct: *mut MVNDistinct) -> *mut bytea {
    unimplemented!()
}
pub unsafe fn statext_ndistinct_deserialize(data: *mut bytea) -> *mut MVNDistinct {
    unimplemented!()
}

pub unsafe fn statext_dependencies_build(data: *mut StatsBuildData) -> *mut MVDependencies {
    unimplemented!()
}
pub unsafe fn statext_dependencies_serialize(dependencies: *mut MVDependencies) -> *mut bytea {
    unimplemented!()
}
pub unsafe fn statext_dependencies_deserialize(data: *mut bytea) -> *mut MVDependencies {
    unimplemented!()
}

pub unsafe fn statext_mcv_build(
    data: *mut StatsBuildData,
    totalrows: f64,
    stattarget: c_int,
) -> *mut MCVList {
    unimplemented!()
}
pub unsafe fn statext_mcv_serialize(
    mcvlist: *mut MCVList,
    stats: *mut *mut VacAttrStats,
) -> *mut bytea {
    unimplemented!()
}
pub unsafe fn statext_mcv_deserialize(data: *mut bytea) -> *mut MCVList {
    unimplemented!()
}

pub unsafe fn multi_sort_init(ndims: c_int) -> MultiSortSupport {
    unimplemented!()
}
pub unsafe fn multi_sort_add_dimension(
    mss: MultiSortSupport,
    sortdim: c_int,
    oper: Oid,
    collation: Oid,
) {
    unimplemented!()
}
pub unsafe fn multi_sort_compare(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    unimplemented!()
}
pub unsafe fn multi_sort_compare_dim(
    dim: c_int,
    a: *const SortItem,
    b: *const SortItem,
    mss: MultiSortSupport,
) -> c_int {
    unimplemented!()
}
pub unsafe fn multi_sort_compare_dims(
    start: c_int,
    end: c_int,
    a: *const SortItem,
    b: *const SortItem,
    mss: MultiSortSupport,
) -> c_int {
    unimplemented!()
}
pub unsafe fn compare_scalars_simple(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    unimplemented!()
}
pub unsafe fn compare_datums_simple(a: Datum, b: Datum, ssup: SortSupport) -> c_int {
    unimplemented!()
}

pub unsafe fn build_attnums_array(
    attrs: *mut Bitmapset,
    nexprs: c_int,
    numattrs: *mut c_int,
) -> *mut AttrNumber {
    unimplemented!()
}

pub unsafe fn build_sorted_items(
    data: *mut StatsBuildData,
    nitems: *mut c_int,
    mss: MultiSortSupport,
    numattrs: c_int,
    attnums: *mut AttrNumber,
) -> *mut SortItem {
    unimplemented!()
}

pub unsafe fn examine_opclause_args(
    args: *mut List,
    exprp: *mut *mut Node,
    cstp: *mut *mut Const,
    expronleftp: *mut bool,
) -> bool {
    unimplemented!()
}

pub unsafe fn mcv_combine_selectivities(
    simple_sel: Selectivity,
    mcv_sel: Selectivity,
    mcv_basesel: Selectivity,
    mcv_totalsel: Selectivity,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn mcv_clauselist_selectivity(
    root: *mut PlannerInfo,
    stat: *mut StatisticExtInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    basesel: *mut Selectivity,
    totalsel: *mut Selectivity,
) -> Selectivity {
    unimplemented!()
}

pub unsafe fn mcv_clause_selectivity_or(
    root: *mut PlannerInfo,
    stat: *mut StatisticExtInfo,
    mcv: *mut MCVList,
    clause: *mut Node,
    or_matches: *mut *mut bool,
    basesel: *mut Selectivity,
    overlap_mcvsel: *mut Selectivity,
    overlap_basesel: *mut Selectivity,
    totalsel: *mut Selectivity,
) -> Selectivity {
    unimplemented!()
}

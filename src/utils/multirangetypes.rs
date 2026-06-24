//! Translated from PostgreSQL src/include/utils/multirangetypes.h
//!
//! Postgres multirange types. Multiranges are varlena objects (ON-DISK).

use crate::fmgr::FunctionCallInfo;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::rangetypes::{RangeBound, RangeType};
use crate::utils::typcache::TypeCacheEntry;

/// MultirangeType - a varlena multirange datum (ON-DISK). Fixed header is
/// `vl_len_` (varlena) + `multirangetypid` + `rangeCount`; the ShortRangeType
/// objects follow in the buffer (themselves varlena, so not indexable directly).
#[repr(C)]
pub struct MultirangeType {
    /// varlena header (do not touch directly!)
    pub vl_len_: i32,
    /// multirange type's own OID
    pub multirangetypid: Oid,
    /// the number of ranges
    pub range_count: u32,
    // Following: rangeCount ShortRangeType structs (varlena) in the buffer.
}
const _: () = assert!(core::mem::size_of::<MultirangeType>() == 12);

/// C: `MultirangeTypeGetOid(mr)`.
pub fn multirange_type_get_oid(mr: &MultirangeType) -> Oid {
    mr.multirangetypid
}

/// C: `MultirangeIsEmpty(mr)`.
pub fn multirange_is_empty(mr: &MultirangeType) -> bool {
    mr.range_count == 0
}

/// C: `DatumGetMultirangeTypeP` - detoast.
pub fn datum_get_multirange_type_p(_x: Datum) -> *mut MultirangeType {
    unimplemented!()
}

/// C: `DatumGetMultirangeTypePCopy` - detoast + copy.
pub fn datum_get_multirange_type_p_copy(_x: Datum) -> *mut MultirangeType {
    unimplemented!()
}

/// C: `MultirangeTypePGetDatum`.
pub fn multirange_type_p_get_datum(_x: &MultirangeType) -> Datum {
    unimplemented!()
}

pub fn multirange_eq_internal(_rangetyp: &TypeCacheEntry, _mr1: &MultirangeType, _mr2: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn multirange_ne_internal(_rangetyp: &TypeCacheEntry, _mr1: &MultirangeType, _mr2: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn multirange_contains_elem_internal(_rangetyp: &TypeCacheEntry, _mr: &MultirangeType, _val: Datum) -> bool {
    unimplemented!()
}

pub fn multirange_contains_range_internal(_rangetyp: &TypeCacheEntry, _mr: &MultirangeType, _r: &RangeType) -> bool {
    unimplemented!()
}

pub fn range_contains_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn multirange_contains_multirange_internal(_rangetyp: &TypeCacheEntry, _mr1: &MultirangeType, _mr2: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn range_overlaps_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn multirange_overlaps_multirange_internal(_rangetyp: &TypeCacheEntry, _mr1: &MultirangeType, _mr2: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn range_overleft_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn range_overright_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn range_before_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn range_after_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn range_adjacent_multirange_internal(_rangetyp: &TypeCacheEntry, _r: &RangeType, _mr: &MultirangeType) -> bool {
    unimplemented!()
}

pub fn multirange_before_multirange_internal(_rangetyp: &TypeCacheEntry, _mr1: &MultirangeType, _mr2: &MultirangeType) -> bool {
    unimplemented!()
}

/// `multirange_minus_internal` - C takes `RangeType **ranges` + counts; map to slices.
pub fn multirange_minus_internal(
    _mltrngtypoid: Oid,
    _rangetyp: &TypeCacheEntry,
    _ranges1: &[&RangeType],
    _ranges2: &[&RangeType],
) -> *mut MultirangeType {
    unimplemented!()
}

pub fn multirange_intersect_internal(
    _mltrngtypoid: Oid,
    _rangetyp: &TypeCacheEntry,
    _ranges1: &[&RangeType],
    _ranges2: &[&RangeType],
) -> *mut MultirangeType {
    unimplemented!()
}

pub fn multirange_get_typcache(_fcinfo: FunctionCallInfo<'_>, _mltrngtypid: Oid) -> *mut TypeCacheEntry {
    unimplemented!()
}

/// `multirange_deserialize` - out-params `range_count` + `ranges` -> Vec.
pub fn multirange_deserialize(_rangetyp: &TypeCacheEntry, _multirange: &MultirangeType) -> Vec<*mut RangeType> {
    unimplemented!()
}

/// `make_multirange` - C takes `RangeType **ranges` + count; map to slice.
pub fn make_multirange(_mltrngtypoid: Oid, _rangetyp: &TypeCacheEntry, _ranges: &[&RangeType]) -> *mut MultirangeType {
    unimplemented!()
}

pub fn make_empty_multirange(_mltrngtypoid: Oid, _rangetyp: &TypeCacheEntry) -> *mut MultirangeType {
    unimplemented!()
}

/// `multirange_get_bounds` - lower/upper out-params -> tuple.
pub fn multirange_get_bounds(_rangetyp: &TypeCacheEntry, _multirange: &MultirangeType, _i: u32) -> (RangeBound, RangeBound) {
    unimplemented!()
}

pub fn multirange_get_range(_rangetyp: &TypeCacheEntry, _multirange: &MultirangeType, _i: i32) -> *mut RangeType {
    unimplemented!()
}

pub fn multirange_get_union_range(_rangetyp: &TypeCacheEntry, _mr: &MultirangeType) -> *mut RangeType {
    unimplemented!()
}

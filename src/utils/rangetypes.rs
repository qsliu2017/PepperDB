//! Translated from PostgreSQL src/include/utils/rangetypes.h
//! Postgres range types.

use crate::fmgr::FunctionCallInfo;
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::typcache::TypeCacheEntry;
use bitflags::bitflags;

/// On-disk varlena range object: 4-byte varlena header, range type OID, then
/// zero-to-two bound values followed by a flags byte. Use VARSIZE/SET_VARSIZE
/// (not the header field) for the length.
#[repr(C)]
pub struct RangeType {
    /// varlena header (do not touch directly!)
    pub vl_len_: i32,
    /// range type's own OID
    pub rangetypid: Oid,
    // Following the OID: zero to two bound values, then a flags byte.
}

pub const RANGE_EMPTY_LITERAL: &str = "empty";

/// C: `RangeTypeGetOid(r)` -- prefer this over reading the field directly.
pub fn RangeTypeGetOid(r: &RangeType) -> Oid {
    r.rangetypid
}

bitflags! {
    /// A range's flags byte. GOOD single-bit set over u8.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RangeFlags: u8 {
        const EMPTY         = 0x01; // range is empty
        const LB_INC        = 0x02; // lower bound inclusive
        const UB_INC        = 0x04; // upper bound inclusive
        const LB_INF        = 0x08; // lower bound is -infinity
        const UB_INF        = 0x10; // upper bound is +infinity
        const LB_NULL       = 0x20; // lower bound is null (NOT USED)
        const UB_NULL       = 0x40; // upper bound is null (NOT USED)
        const CONTAIN_EMPTY = 0x80; // GiST internal entry whose subtree has empties
    }
}

/// C: `RANGE_HAS_LBOUND(flags)`.
pub fn range_has_lbound(flags: RangeFlags) -> bool {
    !flags.intersects(RangeFlags::EMPTY | RangeFlags::LB_NULL | RangeFlags::LB_INF)
}

/// C: `RANGE_HAS_UBOUND(flags)`.
pub fn range_has_ubound(flags: RangeFlags) -> bool {
    !flags.intersects(RangeFlags::EMPTY | RangeFlags::UB_NULL | RangeFlags::UB_INF)
}

/// C: `RangeIsEmpty(r)`.
pub fn RangeIsEmpty(r: &RangeType) -> bool {
    RangeFlags::from_bits_truncate(range_get_flags(r) as u8).contains(RangeFlags::EMPTY)
}

/// C: `RangeIsOrContainsEmpty(r)`.
pub fn RangeIsOrContainsEmpty(r: &RangeType) -> bool {
    RangeFlags::from_bits_truncate(range_get_flags(r) as u8)
        .intersects(RangeFlags::EMPTY | RangeFlags::CONTAIN_EMPTY)
}

/// Internal (in-memory) representation of either bound of a range.
pub struct RangeBound {
    /// the bound value, if any
    pub val: Datum,
    /// bound is +/- infinity
    pub infinite: bool,
    /// bound is inclusive (vs exclusive)
    pub inclusive: bool,
    /// this is the lower (vs upper) bound
    pub lower: bool,
}

// fmgr accessor functions. PG_DETOAST yields a (possibly copied) RangeType.
pub fn DatumGetRangeTypeP(_x: Datum) -> *mut RangeType {
    unimplemented!() // TODO(ptr): PG_DETOAST_DATUM
}
pub fn DatumGetRangeTypePCopy(_x: Datum) -> *mut RangeType {
    unimplemented!() // TODO(ptr): PG_DETOAST_DATUM_COPY
}
pub fn RangeTypePGetDatum(_x: &RangeType) -> Datum {
    unimplemented!() // TODO(ptr): PointerGetDatum
}

// Operator strategy numbers for GiST/SP-GiST range opclasses. These alias
// RT*StrategyNumber from access/stratnum.h. Values per stratnum.h.
pub const RANGESTRAT_BEFORE: u16 = 1; // RTLeftStrategyNumber
pub const RANGESTRAT_OVERLEFT: u16 = 2; // RTOverLeftStrategyNumber
pub const RANGESTRAT_OVERLAPS: u16 = 3; // RTOverlapStrategyNumber
pub const RANGESTRAT_OVERRIGHT: u16 = 4; // RTOverRightStrategyNumber
pub const RANGESTRAT_AFTER: u16 = 5; // RTRightStrategyNumber
pub const RANGESTRAT_ADJACENT: u16 = 6; // RTSameStrategyNumber
pub const RANGESTRAT_CONTAINS: u16 = 7; // RTContainsStrategyNumber
pub const RANGESTRAT_CONTAINED_BY: u16 = 8; // RTContainedByStrategyNumber
pub const RANGESTRAT_CONTAINS_ELEM: u16 = 16; // RTContainsElemStrategyNumber
pub const RANGESTRAT_EQ: u16 = 18; // RTEqualStrategyNumber

// rangetypes.c prototypes (stubs).
pub fn range_contains_elem_internal(
    _typcache: &TypeCacheEntry,
    _r: &RangeType,
    _val: Datum,
) -> bool {
    unimplemented!()
}
pub fn range_eq_internal(_typcache: &TypeCacheEntry, _r1: &RangeType, _r2: &RangeType) -> bool {
    unimplemented!()
}
pub fn range_ne_internal(_typcache: &TypeCacheEntry, _r1: &RangeType, _r2: &RangeType) -> bool {
    unimplemented!()
}
pub fn range_contains_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> bool {
    unimplemented!()
}
pub fn range_contained_by_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> bool {
    unimplemented!()
}
pub fn range_before_internal(_typcache: &TypeCacheEntry, _r1: &RangeType, _r2: &RangeType) -> bool {
    unimplemented!()
}
pub fn range_after_internal(_typcache: &TypeCacheEntry, _r1: &RangeType, _r2: &RangeType) -> bool {
    unimplemented!()
}
pub fn range_adjacent_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> bool {
    unimplemented!()
}
pub fn range_overlaps_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> bool {
    unimplemented!()
}
pub fn range_overleft_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> bool {
    unimplemented!()
}
pub fn range_overright_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> bool {
    unimplemented!()
}
pub fn range_union_internal(
    _typcache: &TypeCacheEntry,
    _r1: &mut RangeType,
    _r2: &mut RangeType,
    _strict: bool,
) -> *mut RangeType {
    unimplemented!() // TODO(ptr)
}
pub fn range_minus_internal(
    _typcache: &TypeCacheEntry,
    _r1: &mut RangeType,
    _r2: &mut RangeType,
) -> *mut RangeType {
    unimplemented!() // TODO(ptr)
}
pub fn range_intersect_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> *mut RangeType {
    unimplemented!() // TODO(ptr)
}

pub fn range_get_typcache(_fcinfo: FunctionCallInfo, _rngtypid: Oid) -> *mut TypeCacheEntry {
    unimplemented!() // TODO(ptr)
}
pub fn range_serialize(
    _typcache: &TypeCacheEntry,
    _lower: &mut RangeBound,
    _upper: &mut RangeBound,
    _empty: bool,
    _escontext: Option<&mut Node>,
) -> *mut RangeType {
    unimplemented!() // TODO(ptr)
}
/// Out-params (lower, upper, empty) folded into a returned tuple.
pub fn range_deserialize(
    _typcache: &TypeCacheEntry,
    _range: &RangeType,
) -> (RangeBound, RangeBound, bool) {
    unimplemented!()
}
pub fn range_get_flags(_range: &RangeType) -> i8 {
    unimplemented!()
}
pub fn range_set_contain_empty(_range: &mut RangeType) {
    unimplemented!()
}
pub fn make_range(
    _typcache: &TypeCacheEntry,
    _lower: &mut RangeBound,
    _upper: &mut RangeBound,
    _empty: bool,
    _escontext: Option<&mut Node>,
) -> *mut RangeType {
    unimplemented!() // TODO(ptr)
}
pub fn range_cmp_bounds(_typcache: &TypeCacheEntry, _b1: &RangeBound, _b2: &RangeBound) -> i32 {
    unimplemented!()
}
pub fn range_cmp_bound_values(
    _typcache: &TypeCacheEntry,
    _b1: &RangeBound,
    _b2: &RangeBound,
) -> i32 {
    unimplemented!()
}
/// C: `int range_compare(const void *key1, const void *key2, void *arg)`.
pub fn range_compare(_key1: &RangeType, _key2: &RangeType, _arg: &TypeCacheEntry) -> i32 {
    unimplemented!()
}
pub fn bounds_adjacent(_typcache: &TypeCacheEntry, _bound_a: RangeBound, _bound_b: RangeBound) -> bool {
    unimplemented!()
}
pub fn make_empty_range(_typcache: &TypeCacheEntry) -> *mut RangeType {
    unimplemented!() // TODO(ptr)
}
/// Out-params output1/output2 folded into the return: `Some((r1, r2))` when the
/// difference splits into two ranges, `None` otherwise (C returns bool).
pub fn range_split_internal(
    _typcache: &TypeCacheEntry,
    _r1: &RangeType,
    _r2: &RangeType,
) -> Option<(*mut RangeType, *mut RangeType)> {
    unimplemented!() // TODO(ptr)
}

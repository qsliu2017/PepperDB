//! Translated from PostgreSQL src/include/utils/sortsupport.h
//! Framework for accelerated sorting.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use crate::access::attnum::AttrNumber;
use crate::c::INVERT_COMPARE_RESULT;
use crate::postgres::{DatumGetInt32, DatumGetInt64, Datum};
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;
use core::cmp::Ordering;
use crate::utils::rel::RelationData;

/// C: `typedef struct SortSupportData *SortSupport;` non-null handle.
pub type SortSupport<'a> = &'a mut SortSupportData;

/// Comparator fn: returns <0, 0, >0 (x<y, x=y, x>y); x and y are non-null.
pub type SortComparator = fn(x: Datum, y: Datum, ssup: &SortSupportData) -> i32;
/// Converter to abbreviated (pass-by-value) key from original representation.
pub type AbbrevConverter = fn(original: Datum, ssup: &mut SortSupportData) -> Datum;
/// Abort/costing callback for the abbreviated-key strategy.
pub type AbbrevAbort = fn(memtupcount: i32, ssup: &mut SortSupportData) -> bool;

/// Opclass-private workspace pointer (`void *ssup_extra`). TODO(ptr): a
/// closure/enum capturing opclass state in Phase 2.
pub type SsupExtra = *mut core::ffi::c_void;

pub struct SortSupportData {
    /// Context containing sort info. Set before BTSORTSUPPORT, not changed after.
    pub ssup_cxt: MemoryContext,
    /// Collation to use, or InvalidOid. Set before BTSORTSUPPORT.
    pub ssup_collation: Oid,

    /// descending-order sort? (may change after BTSORTSUPPORT)
    pub ssup_reverse: bool,
    /// sort nulls first? (may change after BTSORTSUPPORT)
    pub ssup_nulls_first: bool,

    /// column number to sort (caller workspace)
    pub ssup_attno: AttrNumber,

    /// Workspace for opclass functions; zeroed before BTSORTSUPPORT.
    pub ssup_extra: SsupExtra,

    /// Authoritative or abbreviated comparator; must be set.
    pub comparator: Option<SortComparator>,

    /// True if abbreviation is applicable in principle.
    pub abbreviate: bool,
    /// Set => abbreviation in play. None => do not abbreviate.
    pub abbrev_converter: Option<AbbrevConverter>,
    pub abbrev_abort: Option<AbbrevAbort>,
    /// Full comparator used to tie-break inconclusive abbreviated comparisons.
    pub abbrev_full_comparator: Option<SortComparator>,
}

/// Apply a sort comparator and return a 3-way result, handling reverse-sort and
/// NULLs-ordering. (C `static inline ApplySortComparator`.)
pub fn ApplySortComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData,
) -> i32 {
    if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = (ssup.comparator.unwrap())(datum1, datum2, ssup);
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    }
}

/// C `static inline ApplyUnsignedSortComparator`.
pub fn ApplyUnsignedSortComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData,
) -> i32 {
    if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = match datum1.0.cmp(&datum2.0) {
            Ordering::Less => -1,
            Ordering::Greater => 1,
            Ordering::Equal => 0,
        };
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    }
}

/// C `static inline ApplySignedSortComparator` (SIZEOF_DATUM >= 8; always true).
pub fn ApplySignedSortComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData,
) -> i32 {
    if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = match DatumGetInt64(datum1).cmp(&DatumGetInt64(datum2)) {
            Ordering::Less => -1,
            Ordering::Greater => 1,
            Ordering::Equal => 0,
        };
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    }
}

/// C `static inline ApplyInt32SortComparator`.
pub fn ApplyInt32SortComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData,
) -> i32 {
    if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = match DatumGetInt32(datum1).cmp(&DatumGetInt32(datum2)) {
            Ordering::Less => -1,
            Ordering::Greater => 1,
            Ordering::Equal => 0,
        };
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    }
}

/// C `static inline ApplySortAbbrevFullComparator`.
pub fn ApplySortAbbrevFullComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: &SortSupportData,
) -> i32 {
    if is_null1 {
        if is_null2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if is_null2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let mut compare = (ssup.abbrev_full_comparator.unwrap())(datum1, datum2, ssup);
        if ssup.ssup_reverse {
            compare = INVERT_COMPARE_RESULT(compare);
        }
        compare
    }
}

// Datum comparison functions with specialized sort routines (defined in PG's
// tuplesort.c, declared in sortsupport.h; the canonical home in this port).

/// PG `ssup_datum_unsigned_cmp`: 3-way compare treating the Datum bits unsigned.
pub fn ssup_datum_unsigned_cmp(x: Datum, y: Datum, _ssup: &SortSupportData) -> i32 {
    match x.0.cmp(&y.0) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    }
}

/// PG `ssup_datum_signed_cmp`: 3-way compare of int64 datums.
pub fn ssup_datum_signed_cmp(x: Datum, y: Datum, _ssup: &SortSupportData) -> i32 {
    match DatumGetInt64(x).cmp(&DatumGetInt64(y)) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    }
}

/// PG `ssup_datum_int32_cmp`: 3-way compare of int32 datums.
pub fn ssup_datum_int32_cmp(x: Datum, y: Datum, _ssup: &SortSupportData) -> i32 {
    match DatumGetInt32(x).cmp(&DatumGetInt32(y)) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    }
}

pub fn PrepareSortSupportComparisonShim(_cmp_func: Oid, _ssup: SortSupport) {
    unimplemented!()
}
pub fn PrepareSortSupportFromOrderingOp(_ordering_op: Oid, _ssup: SortSupport) {
    unimplemented!()
}
pub fn PrepareSortSupportFromIndexRel(_index_rel: &RelationData, _reverse: bool, _ssup: SortSupport) {
    unimplemented!()
}
pub fn PrepareSortSupportFromGistIndexRel(_index_rel: &RelationData, _ssup: SortSupport) {
    unimplemented!()
}

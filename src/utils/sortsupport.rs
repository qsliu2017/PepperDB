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

pub fn PrepareSortSupportComparisonShim(cmp_func: Oid, ssup: SortSupport) {
    // PG allocates a SortShimExtra (FmgrInfo + reusable fcinfo) in ssup_cxt and sets
    // ssup->comparator = comparison_shim. The shim reads the looked-up function from
    // ssup_extra and FunctionCall2-invokes it. In this port `SortKey`/the sort only
    // copy `comparator` (a plain `fn`, kept `Send`), so rather than stash a raw
    // FmgrInfo behind `ssup_extra` we resolve a per-proc `fn` comparator directly
    // (the builtin btree cmp procs the M5 opclasses use). The resolved `fn` calls the
    // builtin through `OidFunctionCall2Coll`, which is the same FunctionCall the C
    // shim makes; no captured state, so it stays a plain `fn`.
    ssup.comparator = Some(shim_comparator_for(cmp_func));
}

/// The set of builtin BTORDER_PROC comparators reachable for M5 sorts. Each maps to
/// a `SortComparator` `fn` that invokes the builtin (via `OidFunctionCall2Coll`),
/// reading the collation from the live `SortSupportData` (text cmp honors it). PG
/// looks the proc up in pg_amproc and shims it generically; here the proc OID picks
/// a fixed `fn` so the comparator stays a plain (Send) `fn` for `SortKey`.
fn shim_comparator_for(cmp_func: Oid) -> SortComparator {
    use crate::utils::fmgroids as f;
    match cmp_func {
        c if c == f::F_BTINT4CMP => ssup_datum_int32_cmp,
        c if c == f::F_BTINT8CMP => ssup_datum_signed_cmp,
        c if c == f::F_BTTEXTCMP => call_bttextcmp,
        c if c == f::F_BPCHARCMP => call_bpcharcmp,
        c if c == f::F_BTNAMECMP => call_btnamecmp,
        c if c == f::F_DATE_CMP => call_date_cmp,
        c if c == f::F_NUMERIC_CMP => call_numeric_cmp,
        c if c == f::F_BTINT2CMP => call_btint2cmp,
        c if c == f::F_BTOIDCMP => call_btoidcmp,
        c if c == f::F_BTBOOLCMP => call_btboolcmp,
        _ => {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("no sort-support comparator for function {}", cmp_func.get())
            );
            unreachable!("elog!(ERROR) raises")
        }
    }
}

/// Invoke a builtin btree comparator by OID, returning its int4 result as i32.
/// Mirrors the C `comparison_shim` (FunctionCall2 over the looked-up proc); the
/// collation comes from the call-site `SortSupportData` (text needs it).
fn call_cmp(cmp_func: Oid, x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    let r = crate::fmgr::OidFunctionCall2Coll(cmp_func, ssup.ssup_collation, x, y)
        .unwrap_or_else(|| {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("comparator function {} returned NULL", cmp_func.get())
            );
            unreachable!("elog!(ERROR) raises")
        });
    DatumGetInt32(r)
}

fn call_bttextcmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_BTTEXTCMP, x, y, ssup)
}
fn call_btnamecmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_BTNAMECMP, x, y, ssup)
}
fn call_bpcharcmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_BPCHARCMP, x, y, ssup)
}
fn call_date_cmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_DATE_CMP, x, y, ssup)
}
fn call_numeric_cmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_NUMERIC_CMP, x, y, ssup)
}
fn call_btint2cmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_BTINT2CMP, x, y, ssup)
}
fn call_btoidcmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_BTOIDCMP, x, y, ssup)
}
fn call_btboolcmp(x: Datum, y: Datum, ssup: &SortSupportData) -> i32 {
    call_cmp(crate::utils::fmgroids::F_BTBOOLCMP, x, y, ssup)
}

/// PG `PrepareSortSupportFromOrderingOp`: fill `ssup` from a btree "<" or ">"
/// ordering operator. Resolves the operator's btree opfamily + input type +
/// compare direction (`get_ordering_op_properties`), sets `ssup_reverse` for ">",
/// then resolves the BTORDER_PROC support function (`FinishSortSupportFunction`)
/// into `ssup.comparator`.
///
/// The `get_ordering_op_properties` step searches pg_amop by the operator OID; PG
/// uses the AMOPOPID cat-list cache, which is not translated yet, so M5 resolves
/// the (opfamily, opcintype, cmptype) of the seeded int4/int8/text/date/numeric
/// btree ordering operators from a static table -- the same `(op -> opclass info)`
/// the seed data encodes (the established M-stand-in pattern; rules.md s4). The
/// BTORDER_PROC is then resolved through `get_opfamily_proc` over the (warm)
/// AMPROCNUM syscache when available, else the matching static opfamily->proc map.
pub fn PrepareSortSupportFromOrderingOp(ordering_op: Oid, ssup: SortSupport) {
    crate::assert!(ssup.comparator.is_none());
    let Some((opfamily, opcintype, cmptype)) = get_ordering_op_properties(ordering_op) else {
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("operator {} is not a valid ordering operator", ordering_op.get())
        );
        unreachable!("elog!(ERROR) raises")
    };
    ssup.ssup_reverse = cmptype == crate::access::cmptype::CompareType::Gt;
    finish_sort_support_function(opfamily, opcintype, ssup);
}

/// PG `FinishSortSupportFunction`: resolve the BTSORTSUPPORT_PROC (none builtin
/// here) then fall back to the BTORDER_PROC comparator shim. M5 uses the order
/// proc directly.
fn finish_sort_support_function(opfamily: Oid, opcintype: Oid, ssup: SortSupport) {
    let sort_function = get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC);
    if sort_function == crate::postgres_ext::InvalidOid {
        crate::elog!(
            crate::utils::elog::ERROR,
            format!(
                "missing support function {}({},{}) in opfamily {}",
                BTORDER_PROC, opcintype.get(), opcintype.get(), opfamily.get()
            )
        );
    }
    PrepareSortSupportComparisonShim(sort_function, ssup);
}

/// BTORDER_PROC support-function number (access/nbtree.h `BTORDER_PROC`).
const BTORDER_PROC: i16 = 1;

/// PG `get_ordering_op_properties` (M5 static subset): map a btree "<"/">"
/// ordering operator OID to `(opfamily, opcintype, cmptype)`. Covers the seeded
/// single-type int2/int4/int8/oid/name/text/date/numeric btree ordering operators.
fn get_ordering_op_properties(opno: Oid) -> Option<(Oid, Oid, crate::access::cmptype::CompareType)> {
    use crate::access::cmptype::CompareType::{Gt, Lt};
    // (opfamily, opcintype) for each btree default opclass family; OIDs from the
    // seeded pg_opfamily / pg_type rows.
    const INTEGER_OPS: Oid = Oid::new(1976); // btree/integer_ops
    const OID_OPS: Oid = Oid::new(1989); // btree/oid_ops
    const TEXT_OPS: Oid = Oid::new(1994); // btree/text_ops
    const DATETIME_OPS: Oid = Oid::new(434); // btree/datetime_ops
    const NUMERIC_OPS: Oid = Oid::new(1988); // btree/numeric_ops
    const BOOL_OPS: Oid = Oid::new(424); // btree/bool_ops
    const BPCHAR_OPS: Oid = Oid::new(426); // btree/bpchar_ops
    const INT2: Oid = Oid::new(21);
    const INT4: Oid = Oid::new(23);
    const INT8: Oid = Oid::new(20);
    const OID_T: Oid = Oid::new(26);
    const TEXT: Oid = Oid::new(25);
    const NAME: Oid = Oid::new(19);
    const DATE: Oid = Oid::new(1082);
    const NUMERIC: Oid = Oid::new(1700);
    const BOOL: Oid = Oid::new(16);
    const BPCHAR: Oid = Oid::new(1042);
    let (family, intype, cmp) = match opno.get() {
        58 => (BOOL_OPS, BOOL, Lt),      // boollt
        59 => (BOOL_OPS, BOOL, Gt),      // boolgt
        95 => (INTEGER_OPS, INT2, Lt),   // int2lt
        520 => (INTEGER_OPS, INT2, Gt),  // int2gt
        97 => (INTEGER_OPS, INT4, Lt),   // int4lt
        521 => (INTEGER_OPS, INT4, Gt),  // int4gt
        412 => (INTEGER_OPS, INT8, Lt),  // int8lt
        413 => (INTEGER_OPS, INT8, Gt),  // int8gt
        609 => (OID_OPS, OID_T, Lt),     // oidlt
        610 => (OID_OPS, OID_T, Gt),     // oidgt
        664 => (TEXT_OPS, TEXT, Lt),     // text_lt
        666 => (TEXT_OPS, TEXT, Gt),     // text_gt
        1058 => (BPCHAR_OPS, BPCHAR, Lt), // bpcharlt
        1060 => (BPCHAR_OPS, BPCHAR, Gt), // bpchargt
        // name_ops btree opclass lives in the text_ops family (PG 12+).
        660 => (TEXT_OPS, NAME, Lt),     // namelt
        662 => (TEXT_OPS, NAME, Gt),     // namegt
        1095 => (DATETIME_OPS, DATE, Lt), // date_lt
        1097 => (DATETIME_OPS, DATE, Gt), // date_gt
        1754 => (NUMERIC_OPS, NUMERIC, Lt), // numeric_lt
        1756 => (NUMERIC_OPS, NUMERIC, Gt), // numeric_gt
        _ => return None,
    };
    Some((family, intype, cmp))
}

/// PG `get_opfamily_proc`: the support-function OID for
/// `(opfamily, lefttype, righttype, procnum)` via the AMPROCNUM syscache, when the
/// cache is warm. Falls back to the builtin opfamily->BTORDER_PROC map for the
/// catalog-cold bootstrap window (the same value the pg_amproc seed encodes).
fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> Oid {
    use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache};
    use crate::postgres::{Int16GetDatum, ObjectIdGetDatum};
    use crate::utils::syscache::SysCacheIdentifier;
    if let Some(tuple) = search_sys_cache(
        SysCacheIdentifier::AMPROCNUM,
        &[
            ObjectIdGetDatum(opfamily),
            ObjectIdGetDatum(lefttype),
            ObjectIdGetDatum(righttype),
            Int16GetDatum(procnum),
        ],
    ) {
        // SAFETY: held AMPROCNUM hit -> a pg_amproc row; borrow ends before release.
        #[allow(
            clippy::cast_ptr_alignment,
            reason = "faithful GETSTRUCT reinterpretation of a heap tuple to Form_pg_amproc (MAXALIGN'd body covers the Form alignment)"
        )]
        let proc = {
            use crate::access::htup_details::GETSTRUCT;
            use crate::catalog::pg_amproc::FormData_pg_amproc;
            let p = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_amproc>();
            unsafe { &*p }.amproc
        };
        release_sys_cache(tuple);
        if proc != crate::postgres_ext::InvalidOid {
            return proc;
        }
    }
    builtin_opfamily_order_proc(opfamily, lefttype, procnum)
}

/// Builtin opfamily -> BTORDER_PROC map (bootstrap-window fallback; the pg_amproc
/// seed encodes the same proc).
fn builtin_opfamily_order_proc(opfamily: Oid, lefttype: Oid, procnum: i16) -> Oid {
    use crate::utils::fmgroids as f;
    if procnum != BTORDER_PROC {
        return crate::postgres_ext::InvalidOid;
    }
    match lefttype.get() {
        16 => f::F_BTBOOLCMP,
        21 => f::F_BTINT2CMP,
        23 => f::F_BTINT4CMP,
        20 => f::F_BTINT8CMP,
        26 => f::F_BTOIDCMP,
        25 => f::F_BTTEXTCMP,
        1042 => f::F_BPCHARCMP,
        19 => f::F_BTNAMECMP,
        1082 => f::F_DATE_CMP,
        1700 => f::F_NUMERIC_CMP,
        _ => {
            let _ = opfamily;
            crate::postgres_ext::InvalidOid
        }
    }
}
pub fn PrepareSortSupportFromIndexRel(_index_rel: &RelationData, _reverse: bool, _ssup: SortSupport) {
    unimplemented!()
}
pub fn PrepareSortSupportFromGistIndexRel(_index_rel: &RelationData, _ssup: SortSupport) {
    unimplemented!()
}

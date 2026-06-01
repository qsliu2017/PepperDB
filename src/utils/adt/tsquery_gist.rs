//! Translation of postgres/src/backend/utils/adt/tsquery_gist.c
//!
//! GiST index support for the tsquery type (signature-based).
//!
//! #include mapping:
//!   "postgres.h"                 -> crate::prelude::*
//!   "access/gist.h"              -> NOT PORTED; GISTENTRY / GistEntryVector /
//!                                   GIST_SPLITVEC / GIST_LEAF / gistentryinit are
//!                                   defined LOCALLY below (see TODO(pg-port)).
//!   "access/stratnum.h"          -> crate::access::stratnum (RT* strategy consts,
//!                                   StrategyNumber)
//!   "common/int.h"               -> crate::common::int (pg_cmp_s32)
//!   "tsearch/ts_utils.h"         -> TSQuery/QueryItem live in
//!                                   crate::utils::adt::tsquery_util;
//!                                   TSQuerySign + makeTSQuerySign live in
//!                                   crate::utils::adt::tsquery_op.
//!                                   TSQuerySignGetDatum / DatumGetTSQuerySign /
//!                                   PG_GETARG_TSQUERYSIGN / PG_RETURN_TSQUERYSIGN
//!                                   (ts_utils.h static inlines/macros) are defined
//!                                   LOCALLY here.
//!   "utils/fmgrprotos.h"         -> crate::utils::fmgr (FunctionCallInfo) + fmgr macros
//!
//! The bit-signature math (consistent/union/same/penalty/picksplit) is REAL.
//! Anything that needs the real (unported) GiST page layout -- reading the page
//! flags for GIST_LEAF, palloc'ing a GISTENTRY in gtsquery_compress, and
//! DatumGetTSQuery -- is STUBBED with unimplemented!() + TODO(pg-port).

use crate::prelude::*;

use crate::access::stratnum::{
    RTContainedByStrategyNumber, RTContainsStrategyNumber, StrategyNumber,
};
use crate::common::int::pg_cmp_s32;
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::adt::tsquery_op::{makeTSQuerySign, TSQuerySign};
use crate::utils::adt::tsquery_util::TSQuery;
use crate::utils::fmgr::FunctionCallInfo;

use crate::{
    PG_GETARG_POINTER, PG_GETARG_TSQUERY, PG_GETARG_UINT16, PG_RETURN_BOOL,
    PG_RETURN_POINTER,
};

// ================================================================
//   tsearch/ts_utils.h  --  TSQuerySign Datum glue (declared locally).
//   TSQuerySign itself is imported from tsquery_op; TSQS_SIGLEN is private
//   there so we redeclare it (it is unconditionally sizeof(u64)*8 == 64).
// ================================================================

/* #define TSQS_SIGLEN (sizeof(TSQuerySign)*BITS_PER_BYTE) */
const TSQS_SIGLEN: i32 = (core::mem::size_of::<TSQuerySign>() as i32) * 8;

/* static inline Datum TSQuerySignGetDatum(TSQuerySign X) { return Int64GetDatum((int64) X); } */
#[inline]
fn TSQuerySignGetDatum(x: TSQuerySign) -> Datum {
    crate::postgres::Int64GetDatum(x as int64)
}

/* static inline TSQuerySign DatumGetTSQuerySign(Datum X) { return (TSQuerySign) DatumGetInt64(X); } */
#[inline]
unsafe fn DatumGetTSQuerySign(x: Datum) -> TSQuerySign {
    crate::postgres::DatumGetInt64(x) as TSQuerySign
}

// ================================================================
//   access/gist.h  --  NOT PORTED.  Minimal local definitions.
//   TODO(pg-port): replace with the real crate::access::gist once ported.
// ================================================================

/*
 * struct GISTENTRY (access/gist.h):
 *   Datum        key;
 *   Relation     rel;
 *   Page         page;
 *   OffsetNumber offset;
 *   bool         leafkey;
 * `rel` and `page` are opaque pointers here (Relation / Page are unported).
 */
#[repr(C)]
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: *mut c_void,
    pub page: *mut c_void,
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

/*
 * struct GIST_SPLITVEC (access/gist.h): the split vector returned by PickSplit.
 */
#[repr(C)]
pub struct GIST_SPLITVEC {
    pub spl_left: *mut OffsetNumber,
    pub spl_nleft: c_int,
    pub spl_ldatum: Datum,
    pub spl_ldatum_exists: bool,

    pub spl_right: *mut OffsetNumber,
    pub spl_nright: c_int,
    pub spl_rdatum: Datum,
    pub spl_rdatum_exists: bool,
}

/*
 * struct GistEntryVector (access/gist.h): vector of GISTENTRY with a leading
 * count.  `vector` is a C flexible-array member; modeled here as a zero-length
 * array we index past the end of (matching the on-heap layout).
 */
#[repr(C)]
pub struct GistEntryVector {
    pub n: int32,
    pub vector: [GISTENTRY; 0],
}

impl GistEntryVector {
    /* &entryvec->vector[pos] */
    #[inline]
    unsafe fn entry(&self, pos: usize) -> *const GISTENTRY {
        (self.vector.as_ptr()).add(pos)
    }
}

/*
 * #define gistentryinit(e, k, r, pg, o, l) ...  -- initialize a GISTENTRY.
 */
#[inline]
unsafe fn gistentryinit(
    e: *mut GISTENTRY,
    k: Datum,
    r: *mut c_void,
    pg: *mut c_void,
    o: OffsetNumber,
    l: bool,
) {
    (*e).key = k;
    (*e).rel = r;
    (*e).page = pg;
    (*e).offset = o;
    (*e).leafkey = l;
}

/*
 * #define GIST_LEAF(entry) (GistPageIsLeaf((entry)->page))
 *
 * STUB: needs the real GiST page layout (GistPageGetOpaque -> flags & F_LEAF)
 * from the unported storage/bufpage + access/gist page opaque area.
 */
#[inline]
unsafe fn GIST_LEAF(_entry: *const GISTENTRY) -> bool {
    // TODO(pg-port): GistPageIsLeaf((entry)->page) once the GiST page layout
    // (GISTPageOpaqueData / PageGetSpecialPointer) is ported.
    unimplemented!("GIST_LEAF requires the unported GiST page layout")
}

/*
 * #define DatumGetTSQuery(X) ...  (ts_type.h) -- detoasts a TSQuery datum.
 * STUB: the TSQuery varlena detoast path (ts_type.h) is not yet ported.
 */
#[inline]
unsafe fn DatumGetTSQuery(_x: Datum) -> TSQuery {
    // TODO(pg-port): DatumGetTSQueryP / PG_DETOAST path from ts_type.h.
    unimplemented!("DatumGetTSQuery requires the unported ts_type.h detoast path")
}

// ================================================================
//   ts_utils.h: #define PG_RETURN_TSQUERYSIGN / PG_GETARG_TSQUERYSIGN
// ================================================================

macro_rules! PG_GETARG_TSQUERYSIGN {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTSQuerySign($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}

macro_rules! PG_RETURN_TSQUERYSIGN {
    ($x:expr) => {
        return TSQuerySignGetDatum($x)
    };
}

/* #define GETENTRY(vec,pos) DatumGetTSQuerySign((vec)->vector[pos].key) */
#[inline]
unsafe fn GETENTRY(vec: *const GistEntryVector, pos: usize) -> TSQuerySign {
    DatumGetTSQuerySign((*(*vec).entry(pos)).key)
}

// ================================================================
//   Functions
// ================================================================

/*
 * gtsquery_compress: leaf keys get replaced by their signature.
 *
 * STUB: the leafkey branch palloc's a fresh GISTENTRY and detoasts the TSQuery
 * key (DatumGetTSQuery) -- both need the unported GiST page layout / ts_type.h.
 * The signature production (makeTSQuerySign) and gistentryinit are kept real so
 * the only hole is the unported palloc-of-GISTENTRY plumbing.
 */
pub unsafe fn gtsquery_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let mut retval = entry;

    if (*entry).leafkey {
        // TODO(pg-port): real path:
        //   retval = palloc(sizeof(GISTENTRY));
        //   sign   = makeTSQuerySign(DatumGetTSQuery(entry->key));
        //   gistentryinit(*retval, TSQuerySignGetDatum(sign),
        //                 entry->rel, entry->page, entry->offset, false);
        let sign: TSQuerySign = makeTSQuerySign(DatumGetTSQuery((*entry).key));
        retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
        gistentryinit(
            retval,
            TSQuerySignGetDatum(sign),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );
    }

    PG_RETURN_POINTER!(retval)
}

/*
 * We do not need a decompress function, because the other gtsquery support
 * functions work with the compressed representation.  (PG provides none; we
 * provide an identity passthrough so the amproc slot can be wired up if needed.)
 */
pub unsafe fn gtsquery_decompress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    PG_RETURN_POINTER!(entry)
}

/*
 * gtsquery_consistent: signature containment test.
 *
 * The bit-math (key & sq) is REAL; only GIST_LEAF (page-flag read) is stubbed.
 */
pub unsafe fn gtsquery_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 1);
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let key: TSQuerySign = DatumGetTSQuerySign((*entry).key);
    let sq: TSQuerySign = makeTSQuerySign(query);
    let retval: bool;

    /* All cases served by this function are inexact */
    *recheck = true;

    match strategy {
        RTContainsStrategyNumber => {
            if GIST_LEAF(entry) {
                retval = (key & sq) == sq;
            } else {
                retval = (key & sq) != 0;
            }
        }
        RTContainedByStrategyNumber => {
            if GIST_LEAF(entry) {
                retval = (key & sq) == key;
            } else {
                retval = (key & sq) != 0;
            }
        }
        _ => {
            retval = false;
        }
    }
    PG_RETURN_BOOL!(retval)
}

/*
 * gtsquery_union: OR together the signatures of all child entries.  REAL.
 */
pub unsafe fn gtsquery_union(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let size = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_int;
    let mut sign: TSQuerySign = 0;

    let mut i = 0;
    while i < (*entryvec).n {
        sign |= GETENTRY(entryvec, i as usize);
        i += 1;
    }

    *size = core::mem::size_of::<TSQuerySign>() as c_int;

    PG_RETURN_TSQUERYSIGN!(sign)
}

/*
 * gtsquery_same: equality of two signatures.  REAL.
 */
pub unsafe fn gtsquery_same(fcinfo: FunctionCallInfo) -> Datum {
    let a: TSQuerySign = PG_GETARG_TSQUERYSIGN!(fcinfo, 0);
    let b: TSQuerySign = PG_GETARG_TSQUERYSIGN!(fcinfo, 1);
    let result = PG_GETARG_POINTER!(fcinfo, 2) as *mut bool;

    *result = a == b;

    PG_RETURN_POINTER!(result)
}

/* sizebitvec: popcount of the signature. */
fn sizebitvec(sign: TSQuerySign) -> c_int {
    /* C loops bit-by-bit over TSQS_SIGLEN; u64::count_ones is equivalent. */
    let _ = TSQS_SIGLEN; // documents the loop bound from the C source
    sign.count_ones() as c_int
}

/* hemdist: Hamming distance == popcount(a ^ b). */
fn hemdist(a: TSQuerySign, b: TSQuerySign) -> c_int {
    let res: TSQuerySign = a ^ b;
    sizebitvec(res)
}

/*
 * gtsquery_penalty: penalty == Hamming distance between the two signatures. REAL.
 */
pub unsafe fn gtsquery_penalty(fcinfo: FunctionCallInfo) -> Datum {
    let origval: TSQuerySign =
        DatumGetTSQuerySign((*(PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY)).key);
    let newval: TSQuerySign =
        DatumGetTSQuerySign((*(PG_GETARG_POINTER!(fcinfo, 1) as *mut GISTENTRY)).key);
    let penalty = PG_GETARG_POINTER!(fcinfo, 2) as *mut f32;

    *penalty = hemdist(origval, newval) as f32;

    PG_RETURN_POINTER!(penalty)
}

#[repr(C)]
struct SPLITCOST {
    pos: OffsetNumber,
    cost: int32,
}

/* comparecost: qsort comparator on .cost via pg_cmp_s32. */
fn comparecost(a: &SPLITCOST, b: &SPLITCOST) -> c_int {
    pg_cmp_s32(a.cost, b.cost)
}

/* #define WISH_F(a,b,c) (double)( -(double)(((a)-(b))*((a)-(b))*((a)-(b)))*(c) ) */
#[inline]
fn WISH_F(a: c_int, b: c_int, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

/*
 * gtsquery_picksplit: Guttman-style pick-split over the signatures.
 *
 * The signature bit-math (hemdist seeds, datum_l/datum_r OR accumulation,
 * cost vector + WISH_F balancing) is REAL.  The reads of child signatures via
 * GETENTRY and writes of spl_left/spl_right/spl_ldatum/spl_rdatum operate on
 * the locally-defined GistEntryVector / GIST_SPLITVEC structs.
 */
pub unsafe fn gtsquery_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let v = PG_GETARG_POINTER!(fcinfo, 1) as *mut GIST_SPLITVEC;
    let mut maxoff: OffsetNumber = ((*entryvec).n - 2) as OffsetNumber;
    let mut k: OffsetNumber;
    let mut j: OffsetNumber;
    let mut datum_l: TSQuerySign;
    let mut datum_r: TSQuerySign;
    let mut size_alpha: int32;
    let mut size_beta: int32;
    let mut size_waste: int32;
    let mut waste: int32 = -1;
    let nbytes: int32;
    let mut seed_1: OffsetNumber = 0;
    let mut seed_2: OffsetNumber = 0;

    nbytes = ((maxoff as int32) + 2) * core::mem::size_of::<OffsetNumber>() as int32;
    let left_base = palloc(nbytes as Size) as *mut OffsetNumber;
    let right_base = palloc(nbytes as Size) as *mut OffsetNumber;
    (*v).spl_left = left_base;
    (*v).spl_right = right_base;
    let mut left = left_base;
    let mut right = right_base;
    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;

    k = FirstOffsetNumber;
    while k < maxoff {
        j = OffsetNumberNext(k);
        while j <= maxoff {
            size_waste = hemdist(GETENTRY(entryvec, j as usize), GETENTRY(entryvec, k as usize));
            if size_waste > waste {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
            j = OffsetNumberNext(j);
        }
        k = OffsetNumberNext(k);
    }

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    datum_l = GETENTRY(entryvec, seed_1 as usize);
    datum_r = GETENTRY(entryvec, seed_2 as usize);

    maxoff = OffsetNumberNext(maxoff);
    let mut costvector: Vec<SPLITCOST> = Vec::with_capacity(maxoff as usize);
    j = FirstOffsetNumber;
    while j <= maxoff {
        size_alpha = hemdist(GETENTRY(entryvec, seed_1 as usize), GETENTRY(entryvec, j as usize));
        size_beta = hemdist(GETENTRY(entryvec, seed_2 as usize), GETENTRY(entryvec, j as usize));
        costvector.push(SPLITCOST {
            pos: j,
            cost: (size_alpha - size_beta).abs(),
        });
        j = OffsetNumberNext(j);
    }
    costvector.sort_by(|a, b| comparecost(a, b).cmp(&0));

    for kk in 0..(maxoff as usize) {
        j = costvector[kk].pos;
        if j == seed_1 {
            *left = j;
            left = left.add(1);
            (*v).spl_nleft += 1;
            continue;
        } else if j == seed_2 {
            *right = j;
            right = right.add(1);
            (*v).spl_nright += 1;
            continue;
        }
        size_alpha = hemdist(datum_l, GETENTRY(entryvec, j as usize));
        size_beta = hemdist(datum_r, GETENTRY(entryvec, j as usize));

        if (size_alpha as f64)
            < (size_beta as f64) + WISH_F((*v).spl_nleft, (*v).spl_nright, 0.05)
        {
            datum_l |= GETENTRY(entryvec, j as usize);
            *left = j;
            left = left.add(1);
            (*v).spl_nleft += 1;
        } else {
            datum_r |= GETENTRY(entryvec, j as usize);
            *right = j;
            right = right.add(1);
            (*v).spl_nright += 1;
        }
    }

    *right = FirstOffsetNumber;
    *left = FirstOffsetNumber;
    (*v).spl_ldatum = TSQuerySignGetDatum(datum_l);
    (*v).spl_rdatum = TSQuerySignGetDatum(datum_r);

    PG_RETURN_POINTER!(v)
}

/*
 * Compatibility shim for pre-9.6 contrib/tsearch2 opclass declarations.
 */
pub unsafe fn gtsquery_consistent_oldsig(fcinfo: FunctionCallInfo) -> Datum {
    gtsquery_consistent(fcinfo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_is_bitwise_or() {
        // The signature union of two signatures is their bitwise OR.
        let a: TSQuerySign = 0b1010;
        let b: TSQuerySign = 0b0110;
        let union = a | b;
        assert_eq!(union, 0b1110);
    }

    #[test]
    fn test_same_equal_signs_true() {
        // gtsquery_same of equal signs is true; unequal is false.
        let a: TSQuerySign = 0xDEAD_BEEF;
        let b: TSQuerySign = 0xDEAD_BEEF;
        let c: TSQuerySign = 0xDEAD_BEEE;
        assert!(a == b);
        assert!(!(a == c));
    }

    #[test]
    fn test_hemdist_is_popcount_of_xor() {
        let a: TSQuerySign = 0b1111_0000;
        let b: TSQuerySign = 0b0000_1111;
        assert_eq!(hemdist(a, b), 8);
        assert_eq!(hemdist(a, a), 0);
    }

    #[test]
    fn test_sizebitvec_popcount() {
        assert_eq!(sizebitvec(0), 0);
        assert_eq!(sizebitvec(0xFFFF_FFFF_FFFF_FFFF), 64);
        assert_eq!(sizebitvec(0b1011), 3);
    }

    #[test]
    fn test_wish_f_sign() {
        // WISH_F is -((a-b)^3)*c: positive when a<b, negative when a>b.
        assert!(WISH_F(1, 3, 0.05) > 0.0);
        assert!(WISH_F(3, 1, 0.05) < 0.0);
        assert_eq!(WISH_F(2, 2, 0.05), 0.0);
    }
}

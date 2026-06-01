//! Translation of postgres/src/backend/utils/adt/network_gist.c
//!
//! GiST support for network types (the inet_ops opclass for inet/cidr).
//!
//! The key thing to understand about this code is the definition of the
//! "union" of a set of INET/CIDR values.  It works like this:
//! 1. If the values are not all of the same IP address family, the "union"
//! is a dummy value with family number zero, minbits zero, commonbits zero,
//! address all zeroes.  Otherwise:
//! 2. The union has the common IP address family number.
//! 3. The union's minbits value is the smallest netmask length ("ip_bits")
//! of all the input values.
//! 4. Let C be the number of leading address bits that are in common among
//! all the input values.  The union's commonbits value is C.
//! 5. The union's address value is the same as the common prefix for its
//! first C bits, and is zeroes to the right of that.
//!
//! In a leaf index entry (representing a single key), commonbits is equal to
//! ip_maxbits for the address family, minbits is the same as the represented
//! value's ip_bits, and the address is equal to the represented address.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   postgres.h            -> crate::prelude::*
//!   access/gist.h         -> NOT PORTED.  GISTENTRY / GistEntryVector /
//!                            GIST_SPLITVEC / gistentryinit / GIST_LEAF are
//!                            defined LOCALLY below, mirroring the minimal
//!                            definitions in tsquery_gist.rs / tsgistidx.rs.
//!                            TODO(pg-port): replace with crate::access::gist.
//!   access/stratnum.h     -> crate::access::stratnum (RT* strategy consts)
//!   utils/inet.h          -> the inet/cidr type lives in
//!                            crate::utils::adt::network (inet / inet_struct).
//!                            Its ip_family/ip_bits/ip_addr/ip_addrsize/ip_maxbits
//!                            accessors are private there, so the small subset we
//!                            need over the *query* inet is mirrored LOCALLY
//!                            (note below).  bitncmp / bitncommon are REUSED from
//!                            network.rs (they are pub).
//!   varatt.h              -> crate::varatt (VARDATA_ANY / SET_VARSIZE_SHORT)
//!
//! REAL: the entire bit-prefix core -- gk_* key accessors, calc_inet_union_params
//! (and the _indexed variant), build_inet_union_key (common-prefix copy + last
//! partial byte masking + short varlena header), inet_gist_consistent (all 5
//! checks across every strategy), inet_gist_union, inet_gist_fetch's key->inet
//! reconstruction, inet_gist_penalty, inet_gist_picksplit (family split / next-bit
//! split / 50-50 fallback + per-side union recompute), and inet_gist_same.
//!
//! STUBBED: GIST_LEAF (reads the GiST page leaf flag -- unported page layout) and
//! the inet_gist_compress leafkey branch's palloc-of-GISTENTRY plus DatumGetInetPP
//! detoast path (exercised only against a real stored/toasted key at runtime).

use crate::prelude::*;

use crate::access::stratnum::{
    RTEqualStrategyNumber, RTGreaterEqualStrategyNumber, RTGreaterStrategyNumber,
    RTLessEqualStrategyNumber, RTLessStrategyNumber, RTNotEqualStrategyNumber,
    RTOverlapStrategyNumber, RTSubEqualStrategyNumber, RTSubStrategyNumber,
    RTSuperEqualStrategyNumber, RTSuperStrategyNumber, StrategyNumber,
};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::adt::network::{bitncmp, bitncommon, inet, inet_struct};
use crate::utils::fmgr::FunctionCallInfo;
use crate::varatt::{SET_VARSIZE_SHORT, VARDATA_ANY};

use crate::{
    PG_GETARG_DATUM, PG_GETARG_POINTER, PG_GETARG_UINT16, PG_RETURN_BOOL, PG_RETURN_POINTER,
};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

/*
 * We use these values for the "family" field, matching network.rs.  AF_INET is
 * 2 on both Linux and macOS.
 */
const PGSQL_AF_INET: u8 = 2 + 0;
const PGSQL_AF_INET6: u8 = 2 + 1;

// ================================================================
//   utils/inet.h  --  the subset of inet accessors we need over the
//   *query* inet.  The same logic lives (private) in network.rs; mirrored
//   here so we don't widen network.rs's API surface.  inet/inet_struct
//   themselves are imported from network.rs.
//   TODO(pg-port): make network.rs export these and drop the mirror.
// ================================================================

/* ((inet_struct *) VARDATA_ANY(inetptr))->family */
#[inline]
unsafe fn ip_family(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).family
}
#[inline]
unsafe fn set_ip_family(inetptr: *mut inet, v: u8) {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct)).family = v;
}

/* ((inet_struct *) VARDATA_ANY(inetptr))->bits */
#[inline]
unsafe fn ip_bits(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).bits
}
#[inline]
unsafe fn set_ip_bits(inetptr: *mut inet, v: u8) {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct)).bits = v;
}

/* ((inet_struct *) VARDATA_ANY(inetptr))->ipaddr (an array) */
#[inline]
unsafe fn ip_addr(inetptr: *const inet) -> *mut u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct))
        .ipaddr
        .as_mut_ptr()
}

/* ip_addrsize(inetptr) = (family == PGSQL_AF_INET ? 4 : 16) */
#[inline]
unsafe fn ip_addrsize(inetptr: *const inet) -> c_int {
    if ip_family(inetptr) == PGSQL_AF_INET {
        4
    } else {
        16
    }
}

/*
 * SET_INET_VARSIZE(dst) = SET_VARSIZE(dst, VARHDRSZ + offsetof(inet_struct, ipaddr)
 *                                            + ip_addrsize(dst))
 */
#[inline]
unsafe fn SET_INET_VARSIZE(dst: *mut inet) {
    crate::varatt::SET_VARSIZE(
        dst as *mut c_char,
        crate::varatt::VARHDRSZ
            + core::mem::offset_of!(inet_struct, ipaddr) as int32
            + ip_addrsize(dst),
    );
}

#[inline]
unsafe fn InetPGetDatum(X: *const inet) -> Datum {
    crate::postgres::PointerGetDatum(X as *const c_void)
}

// ================================================================
//   access/gist.h  --  NOT PORTED.  Minimal local definitions, mirroring
//   tsquery_gist.rs / tsgistidx.rs.
//   TODO(pg-port): replace with the real crate::access::gist once ported.
// ================================================================

/*
 * struct GISTENTRY (access/gist.h).  `rel`/`page` (Relation/Page) are opaque.
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
 * count.  `vector` is a C flexible-array member; modeled as a zero-length array
 * we index past the end of (matching the on-heap layout).
 */
#[repr(C)]
pub struct GistEntryVector {
    pub n: int32,
    pub vector: [GISTENTRY; 0],
}

impl GistEntryVector {
    /* &entryvec->vector[pos] */
    #[inline]
    unsafe fn entry(&self, pos: usize) -> *mut GISTENTRY {
        (self.vector.as_ptr() as *mut GISTENTRY).add(pos)
    }
}

/* #define gistentryinit(e, k, r, pg, o, l) ...  -- initialize a GISTENTRY. */
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
    // TODO(pg-port): GistPageIsLeaf((entry)->page) once the GiST page layout is ported.
    unimplemented!("GIST_LEAF requires the unported GiST page layout")
}

// ================================================================
//   Operator strategy numbers used in the GiST inet_ops opclass
// ================================================================

const INETSTRAT_OVERLAPS: StrategyNumber = RTOverlapStrategyNumber;
const INETSTRAT_EQ: StrategyNumber = RTEqualStrategyNumber;
const INETSTRAT_NE: StrategyNumber = RTNotEqualStrategyNumber;
const INETSTRAT_LT: StrategyNumber = RTLessStrategyNumber;
const INETSTRAT_LE: StrategyNumber = RTLessEqualStrategyNumber;
const INETSTRAT_GT: StrategyNumber = RTGreaterStrategyNumber;
const INETSTRAT_GE: StrategyNumber = RTGreaterEqualStrategyNumber;
const INETSTRAT_SUB: StrategyNumber = RTSubStrategyNumber;
const INETSTRAT_SUBEQ: StrategyNumber = RTSubEqualStrategyNumber;
const INETSTRAT_SUP: StrategyNumber = RTSuperStrategyNumber;
const INETSTRAT_SUPEQ: StrategyNumber = RTSuperEqualStrategyNumber;

// ================================================================
//   Representation of a GiST INET/CIDR index key (GistInetKey).
// ================================================================

/*
 * Representation of a GiST INET/CIDR index key.  This is not identical to
 * INET/CIDR because we need to keep track of the length of the common address
 * prefix as well as the minimum netmask length.  However, as long as it
 * follows varlena header rules, the core GiST code won't know the difference.
 * For simplicity we always use 1-byte-header varlena format.
 */
#[repr(C)]
pub struct GistInetKey {
    pub va_header: u8,         /* varlena header --- don't touch directly */
    pub family: u8,            /* PGSQL_AF_INET, PGSQL_AF_INET6, or zero */
    pub minbits: u8,           /* minimum number of bits in netmask */
    pub commonbits: u8,        /* number of common prefix bits in addresses */
    pub ipaddr: [u8; 16],      /* up to 128 bits of common address */
}

#[inline]
unsafe fn DatumGetInetKeyP(x: Datum) -> *mut GistInetKey {
    crate::postgres::DatumGetPointer(x) as *mut GistInetKey
}

/*
 * Access macros; not really exciting, but we use these for notational
 * consistency with access to INET/CIDR values.  Note that family-zero values
 * are stored with 4 bytes of address, not 16.
 */
#[inline]
unsafe fn gk_ip_family(gkptr: *const GistInetKey) -> u8 {
    (*gkptr).family
}
#[inline]
unsafe fn set_gk_ip_family(gkptr: *mut GistInetKey, v: u8) {
    (*gkptr).family = v;
}
#[inline]
unsafe fn gk_ip_minbits(gkptr: *const GistInetKey) -> u8 {
    (*gkptr).minbits
}
#[inline]
unsafe fn set_gk_ip_minbits(gkptr: *mut GistInetKey, v: u8) {
    (*gkptr).minbits = v;
}
#[inline]
unsafe fn gk_ip_commonbits(gkptr: *const GistInetKey) -> u8 {
    (*gkptr).commonbits
}
#[inline]
unsafe fn set_gk_ip_commonbits(gkptr: *mut GistInetKey, v: u8) {
    (*gkptr).commonbits = v;
}
#[inline]
unsafe fn gk_ip_addr(gkptr: *const GistInetKey) -> *mut u8 {
    (*(gkptr as *mut GistInetKey)).ipaddr.as_mut_ptr()
}

/* #define ip_family_maxbits(fam) ((fam) == PGSQL_AF_INET6 ? 128 : 32) */
#[inline]
fn ip_family_maxbits(fam: u8) -> c_int {
    if fam == PGSQL_AF_INET6 {
        128
    } else {
        32
    }
}

/* These require that the family field has been set: */
#[inline]
unsafe fn gk_ip_addrsize(gkptr: *const GistInetKey) -> c_int {
    if gk_ip_family(gkptr) == PGSQL_AF_INET6 {
        16
    } else {
        4
    }
}
#[inline]
unsafe fn gk_ip_maxbits(gkptr: *const GistInetKey) -> c_int {
    ip_family_maxbits(gk_ip_family(gkptr))
}

/*
 * SET_GK_VARSIZE(dst) =
 *   SET_VARSIZE_SHORT(dst, offsetof(GistInetKey, ipaddr) + gk_ip_addrsize(dst))
 */
#[inline]
unsafe fn SET_GK_VARSIZE(dst: *mut GistInetKey) {
    SET_VARSIZE_SHORT(
        dst as *mut c_char,
        core::mem::offset_of!(GistInetKey, ipaddr) as int32 + gk_ip_addrsize(dst),
    );
}

// ================================================================
//   Functions
// ================================================================

/*
 * The GiST query consistency check
 */
pub unsafe fn inet_gist_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let ent = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = DatumGetInetPP(PG_GETARG_DATUM!(fcinfo, 1)); // PG_GETARG_INET_PP
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let key = DatumGetInetKeyP((*ent).key);
    let mut minbits: c_int;
    let mut order: c_int;

    /* All operators served by this function are exact. */
    *recheck = false;

    /*
     * Check 0: different families
     *
     * If key represents multiple address families, its children could match
     * anything.  This can only happen on an inner index page.
     */
    if gk_ip_family(key) == 0 {
        Assert!(!GIST_LEAF(ent));
        PG_RETURN_BOOL!(true);
    }

    /*
     * Check 1: different families
     *
     * Matching families do not help any of the strategies.
     */
    if gk_ip_family(key) != ip_family(query) {
        match strategy {
            INETSTRAT_LT | INETSTRAT_LE => {
                if gk_ip_family(key) < ip_family(query) {
                    PG_RETURN_BOOL!(true);
                }
            }
            INETSTRAT_GE | INETSTRAT_GT => {
                if gk_ip_family(key) > ip_family(query) {
                    PG_RETURN_BOOL!(true);
                }
            }
            INETSTRAT_NE => {
                PG_RETURN_BOOL!(true);
            }
            _ => {}
        }
        /* For all other cases, we can be sure there is no match */
        PG_RETURN_BOOL!(false);
    }

    /*
     * Check 2: network bit count
     *
     * Network bit count (ip_bits) helps to check leaves for sub network and
     * sup network operators.  At non-leaf nodes, we know every child value
     * has ip_bits >= gk_ip_minbits(key), so we can avoid descending in some
     * cases too.
     */
    match strategy {
        INETSTRAT_SUB => {
            if GIST_LEAF(ent) && gk_ip_minbits(key) <= ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        INETSTRAT_SUBEQ => {
            if GIST_LEAF(ent) && gk_ip_minbits(key) < ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        INETSTRAT_SUPEQ | INETSTRAT_EQ => {
            if gk_ip_minbits(key) > ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        INETSTRAT_SUP => {
            if gk_ip_minbits(key) >= ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        _ => {}
    }

    /*
     * Check 3: common network bits
     *
     * Compare available common prefix bits to the query, but not beyond
     * either the query's netmask or the minimum netmask among the represented
     * values.
     */
    minbits = Min(gk_ip_commonbits(key) as c_int, gk_ip_minbits(key) as c_int);
    minbits = Min(minbits, ip_bits(query) as c_int);

    order = bitncmp(gk_ip_addr(key), ip_addr(query), minbits);

    match strategy {
        INETSTRAT_SUB | INETSTRAT_SUBEQ | INETSTRAT_OVERLAPS | INETSTRAT_SUPEQ
        | INETSTRAT_SUP => {
            PG_RETURN_BOOL!(order == 0);
        }
        INETSTRAT_LT | INETSTRAT_LE => {
            if order > 0 {
                PG_RETURN_BOOL!(false);
            }
            if order < 0 || !GIST_LEAF(ent) {
                PG_RETURN_BOOL!(true);
            }
        }
        INETSTRAT_EQ => {
            if order != 0 {
                PG_RETURN_BOOL!(false);
            }
            if !GIST_LEAF(ent) {
                PG_RETURN_BOOL!(true);
            }
        }
        INETSTRAT_GE | INETSTRAT_GT => {
            if order < 0 {
                PG_RETURN_BOOL!(false);
            }
            if order > 0 || !GIST_LEAF(ent) {
                PG_RETURN_BOOL!(true);
            }
        }
        INETSTRAT_NE => {
            if order != 0 || !GIST_LEAF(ent) {
                PG_RETURN_BOOL!(true);
            }
        }
        _ => {}
    }

    /*
     * Remaining checks are only for leaves and basic comparison strategies.
     * Note that in a leaf key, commonbits should equal the address length, so
     * we compared the whole network parts above.
     */
    Assert!(GIST_LEAF(ent));

    /*
     * Check 4: network bit count.  Next step is to compare netmask widths.
     */
    match strategy {
        INETSTRAT_LT | INETSTRAT_LE => {
            if gk_ip_minbits(key) < ip_bits(query) {
                PG_RETURN_BOOL!(true);
            }
            if gk_ip_minbits(key) > ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        INETSTRAT_EQ => {
            if gk_ip_minbits(key) != ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        INETSTRAT_GE | INETSTRAT_GT => {
            if gk_ip_minbits(key) > ip_bits(query) {
                PG_RETURN_BOOL!(true);
            }
            if gk_ip_minbits(key) < ip_bits(query) {
                PG_RETURN_BOOL!(false);
            }
        }
        INETSTRAT_NE => {
            if gk_ip_minbits(key) != ip_bits(query) {
                PG_RETURN_BOOL!(true);
            }
        }
        _ => {}
    }

    /*
     * Check 5: whole address.  Netmask bit counts are the same, so check all
     * the address bits.
     */
    order = bitncmp(gk_ip_addr(key), ip_addr(query), gk_ip_maxbits(key));

    match strategy {
        INETSTRAT_LT => {
            PG_RETURN_BOOL!(order < 0);
        }
        INETSTRAT_LE => {
            PG_RETURN_BOOL!(order <= 0);
        }
        INETSTRAT_EQ => {
            PG_RETURN_BOOL!(order == 0);
        }
        INETSTRAT_GE => {
            PG_RETURN_BOOL!(order >= 0);
        }
        INETSTRAT_GT => {
            PG_RETURN_BOOL!(order > 0);
        }
        INETSTRAT_NE => {
            PG_RETURN_BOOL!(order != 0);
        }
        _ => {}
    }

    elog!(ERROR, "unknown strategy for inet GiST");
    /* elog(ERROR) panics; the C trailing PG_RETURN_BOOL(false) is unreachable. */
    unreachable!()
}

/*
 * Calculate parameters of the union of some GistInetKeys.
 *
 * Examine the keys in elements m..n inclusive of the GISTENTRY array, and
 * compute minfamily/maxfamily/minbits/commonbits.  minbits and commonbits are
 * forced to zero if there's more than one address family.
 */
unsafe fn calc_inet_union_params(
    ent: *mut GISTENTRY,
    m: c_int,
    n: c_int,
    minfamily_p: *mut c_int,
    maxfamily_p: *mut c_int,
    minbits_p: *mut c_int,
    commonbits_p: *mut c_int,
) {
    let mut minfamily: c_int;
    let mut maxfamily: c_int;
    let mut minbits: c_int;
    let mut commonbits: c_int;
    let addr: *mut u8;
    let mut tmp: *mut GistInetKey;

    /* Must be at least one key. */
    Assert!(m <= n);

    /* Initialize variables using the first key. */
    tmp = DatumGetInetKeyP((*ent.add(m as usize)).key);
    minfamily = gk_ip_family(tmp) as c_int;
    maxfamily = minfamily;
    minbits = gk_ip_minbits(tmp) as c_int;
    commonbits = gk_ip_commonbits(tmp) as c_int;
    addr = gk_ip_addr(tmp);

    /* Scan remaining keys. */
    let mut i = m + 1;
    while i <= n {
        tmp = DatumGetInetKeyP((*ent.add(i as usize)).key);

        /* Determine range of family numbers */
        if minfamily > gk_ip_family(tmp) as c_int {
            minfamily = gk_ip_family(tmp) as c_int;
        }
        if maxfamily < gk_ip_family(tmp) as c_int {
            maxfamily = gk_ip_family(tmp) as c_int;
        }

        /* Find minimum minbits */
        if minbits > gk_ip_minbits(tmp) as c_int {
            minbits = gk_ip_minbits(tmp) as c_int;
        }

        /* Find minimum number of bits in common */
        if commonbits > gk_ip_commonbits(tmp) as c_int {
            commonbits = gk_ip_commonbits(tmp) as c_int;
        }
        if commonbits > 0 {
            commonbits = bitncommon(addr, gk_ip_addr(tmp), commonbits);
        }

        i += 1;
    }

    /* Force minbits/commonbits to zero if more than one family. */
    if minfamily != maxfamily {
        minbits = 0;
        commonbits = 0;
    }

    *minfamily_p = minfamily;
    *maxfamily_p = maxfamily;
    *minbits_p = minbits;
    *commonbits_p = commonbits;
}

/*
 * Same as above, but the GISTENTRY elements to examine are those with indices
 * listed in the offsets[] array.
 */
unsafe fn calc_inet_union_params_indexed(
    ent: *mut GISTENTRY,
    offsets: *const OffsetNumber,
    noffsets: c_int,
    minfamily_p: *mut c_int,
    maxfamily_p: *mut c_int,
    minbits_p: *mut c_int,
    commonbits_p: *mut c_int,
) {
    let mut minfamily: c_int;
    let mut maxfamily: c_int;
    let mut minbits: c_int;
    let mut commonbits: c_int;
    let addr: *mut u8;
    let mut tmp: *mut GistInetKey;

    /* Must be at least one key. */
    Assert!(noffsets > 0);

    /* Initialize variables using the first key. */
    tmp = DatumGetInetKeyP((*ent.add(*offsets.add(0) as usize)).key);
    minfamily = gk_ip_family(tmp) as c_int;
    maxfamily = minfamily;
    minbits = gk_ip_minbits(tmp) as c_int;
    commonbits = gk_ip_commonbits(tmp) as c_int;
    addr = gk_ip_addr(tmp);

    /* Scan remaining keys. */
    let mut i = 1;
    while i < noffsets {
        tmp = DatumGetInetKeyP((*ent.add(*offsets.add(i as usize) as usize)).key);

        if minfamily > gk_ip_family(tmp) as c_int {
            minfamily = gk_ip_family(tmp) as c_int;
        }
        if maxfamily < gk_ip_family(tmp) as c_int {
            maxfamily = gk_ip_family(tmp) as c_int;
        }

        if minbits > gk_ip_minbits(tmp) as c_int {
            minbits = gk_ip_minbits(tmp) as c_int;
        }

        if commonbits > gk_ip_commonbits(tmp) as c_int {
            commonbits = gk_ip_commonbits(tmp) as c_int;
        }
        if commonbits > 0 {
            commonbits = bitncommon(addr, gk_ip_addr(tmp), commonbits);
        }

        i += 1;
    }

    /* Force minbits/commonbits to zero if more than one family. */
    if minfamily != maxfamily {
        minbits = 0;
        commonbits = 0;
    }

    *minfamily_p = minfamily;
    *maxfamily_p = maxfamily;
    *minbits_p = minbits;
    *commonbits_p = commonbits;
}

/*
 * Construct a GistInetKey representing a union value.
 *
 * Inputs are the family/minbits/commonbits values to use, plus a pointer to the
 * address field of one of the union inputs.  (Since we're going to copy just the
 * bits-in-common, it doesn't matter which one.)
 */
unsafe fn build_inet_union_key(
    family: c_int,
    minbits: c_int,
    commonbits: c_int,
    addr: *const u8,
) -> *mut GistInetKey {
    /* Make sure any unused bits are zeroed. */
    let result = palloc0(core::mem::size_of::<GistInetKey>()) as *mut GistInetKey;

    set_gk_ip_family(result, family as u8);
    set_gk_ip_minbits(result, minbits as u8);
    set_gk_ip_commonbits(result, commonbits as u8);

    /* Clone appropriate bytes of the address. */
    if commonbits > 0 {
        memcpy(
            gk_ip_addr(result) as *mut c_void,
            addr as *const c_void,
            ((commonbits + 7) / 8) as usize,
        );
    }

    /* Clean any unwanted bits in the last partial byte. */
    if commonbits % 8 != 0 {
        *gk_ip_addr(result).add((commonbits / 8) as usize) &= !(0xFFu8 >> (commonbits % 8));
    }

    /* Set varlena header correctly. */
    SET_GK_VARSIZE(result);

    result
}

/*
 * The GiST union function
 */
pub unsafe fn inet_gist_union(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let ent = (*entryvec).vector.as_ptr() as *mut GISTENTRY;
    let mut minfamily: c_int = 0;
    let mut maxfamily: c_int = 0;
    let mut minbits: c_int = 0;
    let mut commonbits: c_int = 0;
    let addr: *mut u8;
    let tmp: *mut GistInetKey;

    /* Determine parameters of the union. */
    calc_inet_union_params(
        ent,
        0,
        (*entryvec).n - 1,
        &mut minfamily,
        &mut maxfamily,
        &mut minbits,
        &mut commonbits,
    );

    /* If more than one family, emit family number zero. */
    if minfamily != maxfamily {
        minfamily = 0;
    }

    /* Initialize address using the first key. */
    tmp = DatumGetInetKeyP((*ent.add(0)).key);
    addr = gk_ip_addr(tmp);

    /* Construct the union value. */
    let result = build_inet_union_key(minfamily, minbits, commonbits, addr);

    PG_RETURN_POINTER!(result);
}

/*
 * The GiST compress function -- convert an inet value to GistInetKey.
 */
pub unsafe fn inet_gist_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let retval: *mut GISTENTRY;

    if (*entry).leafkey {
        retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
        if crate::postgres::DatumGetPointer((*entry).key) != null_mut() {
            let in_ = DatumGetInetPP((*entry).key);

            let r = palloc0(core::mem::size_of::<GistInetKey>()) as *mut GistInetKey;

            set_gk_ip_family(r, ip_family(in_));
            set_gk_ip_minbits(r, ip_bits(in_));
            set_gk_ip_commonbits(r, gk_ip_maxbits(r) as u8);
            memcpy(
                gk_ip_addr(r) as *mut c_void,
                ip_addr(in_) as *const c_void,
                gk_ip_addrsize(r) as usize,
            );
            SET_GK_VARSIZE(r);

            gistentryinit(
                retval,
                crate::postgres::PointerGetDatum(r as *const c_void),
                (*entry).rel,
                (*entry).page,
                (*entry).offset,
                false,
            );
        } else {
            gistentryinit(
                retval,
                0 as Datum,
                (*entry).rel,
                (*entry).page,
                (*entry).offset,
                false,
            );
        }
    } else {
        retval = entry;
    }
    PG_RETURN_POINTER!(retval);
}

/*
 * We do not need a decompress function, because the other GiST inet support
 * functions work with the GistInetKey representation.
 */

/*
 * The GiST fetch function -- reconstruct the original inet datum from a
 * GistInetKey.
 */
pub unsafe fn inet_gist_fetch(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let key = DatumGetInetKeyP((*entry).key);
    let retval: *mut GISTENTRY;
    let dst: *mut inet;

    dst = palloc0(core::mem::size_of::<inet>()) as *mut inet;

    set_ip_family(dst, gk_ip_family(key));
    set_ip_bits(dst, gk_ip_minbits(key));
    memcpy(
        ip_addr(dst) as *mut c_void,
        gk_ip_addr(key) as *const c_void,
        ip_addrsize(dst) as usize,
    );
    SET_INET_VARSIZE(dst);

    retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
    gistentryinit(
        retval,
        InetPGetDatum(dst),
        (*entry).rel,
        (*entry).page,
        (*entry).offset,
        false,
    );

    PG_RETURN_POINTER!(retval);
}

/*
 * The GiST page split penalty function.
 *
 * Charge a large penalty if address family doesn't match, or a somewhat smaller
 * one if the new value would degrade the union's minbits (minimum netmask
 * width).  Otherwise, penalty is inverse of the new number of common address
 * bits.
 */
pub unsafe fn inet_gist_penalty(fcinfo: FunctionCallInfo) -> Datum {
    let origent = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let newent = PG_GETARG_POINTER!(fcinfo, 1) as *mut GISTENTRY;
    let penalty = PG_GETARG_POINTER!(fcinfo, 2) as *mut f32;
    let orig = DatumGetInetKeyP((*origent).key);
    let new = DatumGetInetKeyP((*newent).key);
    let commonbits: c_int;

    if gk_ip_family(orig) == gk_ip_family(new) {
        if gk_ip_minbits(orig) <= gk_ip_minbits(new) {
            commonbits = bitncommon(
                gk_ip_addr(orig),
                gk_ip_addr(new),
                Min(gk_ip_commonbits(orig) as c_int, gk_ip_commonbits(new) as c_int),
            );
            if commonbits > 0 {
                *penalty = 1.0f32 / commonbits as f32;
            } else {
                *penalty = 2.0;
            }
        } else {
            *penalty = 3.0;
        }
    } else {
        *penalty = 4.0;
    }

    PG_RETURN_POINTER!(penalty);
}

/*
 * The GiST PickSplit method.
 *
 * There are two ways to split.  First is to split by address families, if there
 * are multiple families appearing in the input.  The second and more common way
 * is to split by addresses: determine the number of leading bits shared by all
 * the keys, then split on the next bit.  If we fail to get a nontrivial split
 * that way, split 50-50.
 */
pub unsafe fn inet_gist_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let splitvec = PG_GETARG_POINTER!(fcinfo, 1) as *mut GIST_SPLITVEC;
    let ent = (*entryvec).vector.as_ptr() as *mut GISTENTRY;
    let mut minfamily: c_int = 0;
    let mut maxfamily: c_int = 0;
    let mut minbits: c_int = 0;
    let mut commonbits: c_int = 0;
    let mut addr: *mut u8;
    let mut tmp: *mut GistInetKey;
    let left_union: *mut GistInetKey;
    let right_union: *mut GistInetKey;
    let maxoff: c_int;
    let nbytes: c_int;
    let mut i: OffsetNumber;

    maxoff = (*entryvec).n - 1;
    nbytes = (maxoff + 1) * core::mem::size_of::<OffsetNumber>() as c_int;

    let left = palloc(nbytes as Size) as *mut OffsetNumber;
    let right = palloc(nbytes as Size) as *mut OffsetNumber;

    (*splitvec).spl_left = left;
    (*splitvec).spl_right = right;

    (*splitvec).spl_nleft = 0;
    (*splitvec).spl_nright = 0;

    /* Determine parameters of the union of all the inputs. */
    calc_inet_union_params(
        ent,
        FirstOffsetNumber as c_int,
        maxoff,
        &mut minfamily,
        &mut maxfamily,
        &mut minbits,
        &mut commonbits,
    );

    if minfamily != maxfamily {
        /* Multiple families, so split by family. */
        i = FirstOffsetNumber;
        while i as c_int <= maxoff {
            /*
             * If there's more than 2 families, all but maxfamily go into the
             * left union.
             */
            tmp = DatumGetInetKeyP((*ent.add(i as usize)).key);
            if gk_ip_family(tmp) as c_int != maxfamily {
                *left.add((*splitvec).spl_nleft as usize) = i;
                (*splitvec).spl_nleft += 1;
            } else {
                *right.add((*splitvec).spl_nright as usize) = i;
                (*splitvec).spl_nright += 1;
            }
            i = OffsetNumberNext(i);
        }
    } else {
        /*
         * Split on the next bit after the common bits.  If that yields a
         * trivial split, try the next bit position to the right.  Repeat till
         * success; or if we run out of bits, do an arbitrary 50-50 split.
         */
        let maxbits = ip_family_maxbits(minfamily as u8);

        while commonbits < maxbits {
            /* Split using the commonbits'th bit position. */
            let bitbyte = commonbits / 8;
            let bitmask = 0x80u8 >> (commonbits % 8);

            (*splitvec).spl_nleft = 0;
            (*splitvec).spl_nright = 0;

            i = FirstOffsetNumber;
            while i as c_int <= maxoff {
                tmp = DatumGetInetKeyP((*ent.add(i as usize)).key);
                let a = gk_ip_addr(tmp);
                if (*a.add(bitbyte as usize) & bitmask) == 0 {
                    *left.add((*splitvec).spl_nleft as usize) = i;
                    (*splitvec).spl_nleft += 1;
                } else {
                    *right.add((*splitvec).spl_nright as usize) = i;
                    (*splitvec).spl_nright += 1;
                }
                i = OffsetNumberNext(i);
            }

            if (*splitvec).spl_nleft > 0 && (*splitvec).spl_nright > 0 {
                break; /* success */
            }
            commonbits += 1;
        }

        if commonbits >= maxbits {
            /* Failed ... do a 50-50 split. */
            (*splitvec).spl_nleft = 0;
            (*splitvec).spl_nright = 0;

            i = FirstOffsetNumber;
            while i as c_int <= maxoff / 2 {
                *left.add((*splitvec).spl_nleft as usize) = i;
                (*splitvec).spl_nleft += 1;
                i = OffsetNumberNext(i);
            }
            while i as c_int <= maxoff {
                *right.add((*splitvec).spl_nright as usize) = i;
                (*splitvec).spl_nright += 1;
                i = OffsetNumberNext(i);
            }
        }
    }

    /*
     * Compute the union value for each side from scratch.  This ensures that
     * each side has minbits and commonbits set as high as possible.
     */
    calc_inet_union_params_indexed(
        ent,
        left,
        (*splitvec).spl_nleft,
        &mut minfamily,
        &mut maxfamily,
        &mut minbits,
        &mut commonbits,
    );
    if minfamily != maxfamily {
        minfamily = 0;
    }
    tmp = DatumGetInetKeyP((*ent.add(*left.add(0) as usize)).key);
    addr = gk_ip_addr(tmp);
    left_union = build_inet_union_key(minfamily, minbits, commonbits, addr);
    (*splitvec).spl_ldatum = crate::postgres::PointerGetDatum(left_union as *const c_void);

    calc_inet_union_params_indexed(
        ent,
        right,
        (*splitvec).spl_nright,
        &mut minfamily,
        &mut maxfamily,
        &mut minbits,
        &mut commonbits,
    );
    if minfamily != maxfamily {
        minfamily = 0;
    }
    tmp = DatumGetInetKeyP((*ent.add(*right.add(0) as usize)).key);
    addr = gk_ip_addr(tmp);
    right_union = build_inet_union_key(minfamily, minbits, commonbits, addr);
    (*splitvec).spl_rdatum = crate::postgres::PointerGetDatum(right_union as *const c_void);

    PG_RETURN_POINTER!(splitvec);
}

/*
 * The GiST equality function.
 */
pub unsafe fn inet_gist_same(fcinfo: FunctionCallInfo) -> Datum {
    let left = DatumGetInetKeyP(PG_GETARG_DATUM!(fcinfo, 0));
    let right = DatumGetInetKeyP(PG_GETARG_DATUM!(fcinfo, 1));
    let result = PG_GETARG_POINTER!(fcinfo, 2) as *mut bool;

    *result = gk_ip_family(left) == gk_ip_family(right)
        && gk_ip_minbits(left) == gk_ip_minbits(right)
        && gk_ip_commonbits(left) == gk_ip_commonbits(right)
        && memcmp(
            gk_ip_addr(left) as *const c_void,
            gk_ip_addr(right) as *const c_void,
            gk_ip_addrsize(left) as usize,
        ) == 0;

    PG_RETURN_POINTER!(result);
}

/*
 * DatumGetInetPP (inet.h) -- detoast an inet/cidr datum.  network.rs keeps this
 * private; the GiST path only needs to deref an already-materialized pointer in
 * tests, but at runtime the key/query datum may be short-header.  Mirror the
 * detoast-packed form.
 */
#[inline]
unsafe fn DatumGetInetPP(x: Datum) -> *mut inet {
    crate::varatt::pg_detoast_datum_packed(crate::postgres::DatumGetPointer(x) as *mut c_void)
        as *mut inet
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * Hand-build a GistInetKey leaf for an IPv4 address (4 octets), with the
     * given minbits.  commonbits is set to ip_maxbits (32) as for a leaf key.
     */
    unsafe fn make_v4_key(octets: [u8; 4], minbits: u8) -> *mut GistInetKey {
        let k = palloc0(core::mem::size_of::<GistInetKey>()) as *mut GistInetKey;
        set_gk_ip_family(k, PGSQL_AF_INET);
        set_gk_ip_minbits(k, minbits);
        set_gk_ip_commonbits(k, 32);
        for j in 0..4 {
            *gk_ip_addr(k).add(j) = octets[j];
        }
        SET_GK_VARSIZE(k);
        k
    }

    /* Build an inet (query) for an IPv4 address with the given bits. */
    unsafe fn make_v4_inet(octets: [u8; 4], bits: u8) -> *mut inet {
        let p = palloc0(core::mem::size_of::<inet>()) as *mut inet;
        set_ip_family(p, PGSQL_AF_INET);
        set_ip_bits(p, bits);
        for j in 0..4 {
            *ip_addr(p).add(j) = octets[j];
        }
        SET_INET_VARSIZE(p);
        p
    }

    /* Build a GISTENTRY referencing a key (leaf or not, but GIST_LEAF stubbed). */
    unsafe fn make_entry(key: *mut GistInetKey) -> GISTENTRY {
        GISTENTRY {
            key: crate::postgres::PointerGetDatum(key as *const c_void),
            rel: null_mut(),
            page: null_mut(),
            offset: 0,
            leafkey: false,
        }
    }

    /* Allocate a GistEntryVector holding `keys`. */
    unsafe fn make_vector(keys: &[*mut GistInetKey]) -> *mut GistEntryVector {
        let n = keys.len();
        let sz = core::mem::size_of::<GistEntryVector>()
            + n * core::mem::size_of::<GISTENTRY>();
        let v = palloc0(sz) as *mut GistEntryVector;
        (*v).n = n as int32;
        let base = (*v).vector.as_ptr() as *mut GISTENTRY;
        for (idx, &k) in keys.iter().enumerate() {
            *base.add(idx) = make_entry(k);
        }
        v
    }

    #[test]
    fn test_common_prefix_length_math() {
        unsafe {
            // 192.168.0.0 vs 192.168.128.0 share 16 full bits, then octet 3 is
            // 0x00 vs 0x80 which differ in the very first bit => 16 common bits.
            let a = make_v4_key([192, 168, 0, 0], 16);
            let b = make_v4_key([192, 168, 128, 0], 16);
            let c = bitncommon(gk_ip_addr(a), gk_ip_addr(b), 32);
            assert_eq!(c, 16);

            // 10.0.0.0 vs 10.128.0.0: octet0 equal (8 bits), octet1 0x00 vs 0x80
            // differ at first bit => 8 common bits.
            let d = make_v4_key([10, 0, 0, 0], 8);
            let e = make_v4_key([10, 128, 0, 0], 8);
            assert_eq!(bitncommon(gk_ip_addr(d), gk_ip_addr(e), 32), 8);

            // Identical addresses => all 32 bits common.
            let f = make_v4_key([10, 1, 2, 3], 24);
            let g = make_v4_key([10, 1, 2, 3], 24);
            assert_eq!(bitncommon(gk_ip_addr(f), gk_ip_addr(g), 32), 32);
        }
    }

    #[test]
    fn test_union_covers_both_inputs() {
        unsafe {
            // Two /24 leaves under 192.168.x.0.  Their union's prefix (commonbits)
            // must be a prefix of both inputs and have the right common length.
            let a = make_v4_key([192, 168, 1, 0], 24);
            let b = make_v4_key([192, 168, 2, 0], 24);
            let v = make_vector(&[a, b]);

            // Mirror inet_gist_union's body (avoids PG_FUNCTION_ARGS plumbing).
            let ent = (*v).vector.as_ptr() as *mut GISTENTRY;
            let (mut minf, mut maxf, mut minb, mut comb) = (0, 0, 0, 0);
            calc_inet_union_params(ent, 0, (*v).n - 1, &mut minf, &mut maxf, &mut minb, &mut comb);
            let mut fam = minf;
            if minf != maxf {
                fam = 0;
            }
            let tmp = DatumGetInetKeyP((*ent.add(0)).key);
            let u = build_inet_union_key(fam, minb, comb, gk_ip_addr(tmp));

            assert_eq!(gk_ip_family(u), PGSQL_AF_INET);
            // minbits is the min netmask width (both /24).
            assert_eq!(gk_ip_minbits(u), 24);
            // 192.168.1 vs 192.168.2: 16 bits equal, octet2 0x01 vs 0x02 differ at
            // bit 6 (0000000 1 vs 0000001 0) => common = 22.
            let comb_u = gk_ip_commonbits(u) as c_int;
            assert_eq!(comb_u, 22);
            // The union prefix must be a prefix of BOTH inputs over commonbits.
            assert_eq!(bitncmp(gk_ip_addr(u), gk_ip_addr(a), comb_u), 0);
            assert_eq!(bitncmp(gk_ip_addr(u), gk_ip_addr(b), comb_u), 0);
        }
    }

    #[test]
    fn test_union_idempotent() {
        unsafe {
            // union(k, k) must equal k bitwise: same family/minbits/commonbits and
            // the full address bytes match.
            let k = make_v4_key([172, 16, 254, 1], 32);
            let v = make_vector(&[k, k]);
            let ent = (*v).vector.as_ptr() as *mut GISTENTRY;
            let (mut minf, mut maxf, mut minb, mut comb) = (0, 0, 0, 0);
            calc_inet_union_params(ent, 0, (*v).n - 1, &mut minf, &mut maxf, &mut minb, &mut comb);
            let mut fam = minf;
            if minf != maxf {
                fam = 0;
            }
            let tmp = DatumGetInetKeyP((*ent.add(0)).key);
            let u = build_inet_union_key(fam, minb, comb, gk_ip_addr(tmp));

            assert_eq!(gk_ip_family(u), gk_ip_family(k));
            assert_eq!(gk_ip_minbits(u), gk_ip_minbits(k));
            assert_eq!(gk_ip_commonbits(u), gk_ip_commonbits(k));
            assert_eq!(
                memcmp(
                    gk_ip_addr(u) as *const c_void,
                    gk_ip_addr(k) as *const c_void,
                    gk_ip_addrsize(u) as usize,
                ),
                0
            );
        }
    }

    #[test]
    fn test_build_union_key_masks_partial_byte() {
        unsafe {
            // commonbits=20 means the last partial byte (octet 2) keeps only its
            // top 4 bits.  Feed 0xFF there and check it is masked to 0xF0.
            let u = build_inet_union_key(
                PGSQL_AF_INET as c_int,
                20,
                20,
                [10u8, 0xFF, 0xFF, 0xFF].as_ptr(),
            );
            assert_eq!(*gk_ip_addr(u).add(0), 10);
            assert_eq!(*gk_ip_addr(u).add(1), 0xFF);
            assert_eq!(*gk_ip_addr(u).add(2), 0xF0); // top 4 bits kept
            // Byte beyond commonbits stays zero (palloc0).
            assert_eq!(*gk_ip_addr(u).add(3), 0x00);
        }
    }

    #[test]
    fn test_consistent_subeq_maybe_true_on_inner_node() {
        unsafe {
            // Inner node (GIST_LEAF stubbed -> only branches that don't touch it
            // are exercised).  Key = 10.0.0.0 with commonbits=16, minbits=16.
            // Query 10.0.5.7/24 falls under the key prefix, so for INETSTRAT_SUBEQ
            // (subnet-or-equal, network-only) check 3 should return order==0 => true.
            let k = palloc0(core::mem::size_of::<GistInetKey>()) as *mut GistInetKey;
            set_gk_ip_family(k, PGSQL_AF_INET);
            set_gk_ip_minbits(k, 16);
            set_gk_ip_commonbits(k, 16);
            *gk_ip_addr(k).add(0) = 10;
            *gk_ip_addr(k).add(1) = 0;
            SET_GK_VARSIZE(k);
            let ent = make_entry(k);

            let q = make_v4_inet([10, 0, 5, 7], 24);

            // Reproduce check-3 for INETSTRAT_SUBEQ: minbits = min(common, minbits,
            // ip_bits(query)) = min(16,16,24)=16; order over 16 bits == 0.
            let minbits = Min(
                Min(gk_ip_commonbits(k) as c_int, gk_ip_minbits(k) as c_int),
                ip_bits(q) as c_int,
            );
            let order = bitncmp(gk_ip_addr(k), ip_addr(q), minbits);
            assert_eq!(minbits, 16);
            assert_eq!(order, 0); // SUBEQ returns (order == 0) => maybe-true
            let _ = ent;

            // A query NOT under the prefix (172.16.x) must give order != 0.
            let q2 = make_v4_inet([172, 16, 0, 0], 16);
            let mb2 = Min(
                Min(gk_ip_commonbits(k) as c_int, gk_ip_minbits(k) as c_int),
                ip_bits(q2) as c_int,
            );
            assert_ne!(bitncmp(gk_ip_addr(k), ip_addr(q2), mb2), 0);
        }
    }

    #[test]
    fn test_same_is_reflexive_and_discriminating() {
        unsafe {
            let a = make_v4_key([192, 0, 2, 1], 32);
            // same(a, a) is reflexive => true.
            let mut r = false;
            let same = |l: *mut GistInetKey, rt: *mut GistInetKey| -> bool {
                gk_ip_family(l) == gk_ip_family(rt)
                    && gk_ip_minbits(l) == gk_ip_minbits(rt)
                    && gk_ip_commonbits(l) == gk_ip_commonbits(rt)
                    && memcmp(
                        gk_ip_addr(l) as *const c_void,
                        gk_ip_addr(rt) as *const c_void,
                        gk_ip_addrsize(l) as usize,
                    ) == 0
            };
            r = same(a, a);
            assert!(r);

            // Differs in address => not same.
            let b = make_v4_key([192, 0, 2, 2], 32);
            assert!(!same(a, b));

            // Differs only in minbits => not same.
            let c = make_v4_key([192, 0, 2, 1], 24);
            assert!(!same(a, c));
        }
    }
}

//! utils/adt/network_spgist.c
//!
//! SP-GiST support for network types.
//!
//! We split inet index entries first by address family (IPv4 or IPv6).
//! If the entries below a given inner tuple are all of the same family,
//! we identify their common prefix and split by the next bit of the address,
//! and by whether their masklens exceed the length of the common prefix.
//!
//! An inner tuple that has both IPv4 and IPv6 children has a null prefix
//! and exactly two nodes, the first being for IPv4 and the second for IPv6.
//!
//! Otherwise, the prefix is a CIDR value representing the common prefix,
//! and there are exactly four nodes.  Node numbers 0 and 1 are for addresses
//! with the same masklen as the prefix, while node numbers 2 and 3 are for
//! addresses with larger masklen.  (We do not allow a tuple to contain
//! entries with masklen smaller than its prefix's.)  Node numbers 0 and 1
//! are distinguished by the next bit of the address after the common prefix,
//! and likewise for node numbers 2 and 3.  If there are no more bits in
//! the address family, everything goes into node 0 (which will probably
//! lead to creating an allTheSame tuple).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!			src/backend/utils/adt/network_spgist.c

use crate::prelude::*;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_VOID};

// #include <sys/socket.h>
// #include "access/spgist.h"
use crate::access::spgist::spgist::{
    spgChooseIn, spgChooseOut, spgConfigOut, spgInnerConsistentIn, spgInnerConsistentOut,
    spgLeafConsistentIn, spgLeafConsistentOut, spgMatchNode, spgPickSplitIn, spgPickSplitOut,
    spgSplitTuple,
};
// #include "access/stratnum.h"
use crate::access::stratnum::{
    StrategyNumber, RTEqualStrategyNumber, RTGreaterEqualStrategyNumber, RTGreaterStrategyNumber,
    RTLessEqualStrategyNumber, RTLessStrategyNumber, RTNotEqualStrategyNumber,
    RTSubEqualStrategyNumber, RTSubStrategyNumber, RTSuperEqualStrategyNumber,
    RTSuperStrategyNumber,
};
// #include "catalog/pg_type.h"
use crate::catalog::pg_type_d::{CIDROID, VOIDOID};
// #include "utils/fmgrprotos.h"
use crate::access::common::scankey::ScanKey;
use crate::utils::fmgr::FunctionCallInfo;
// #include "utils/inet.h"
use crate::utils::adt::network::{bitncmp, bitncommon, cidr_set_masklen_internal, inet, inet_struct};
// #include "varatt.h"
use crate::varatt::{pg_detoast_datum_packed, VARDATA_ANY};

/*
 * The "family" field values, mirrored from utils/adt/network.rs (where they
 * are private).  PGSQL_AF_INET = AF_INET + 0; PGSQL_AF_INET6 = AF_INET + 1.
 */
const PGSQL_AF_INET: u8 = 2 + 0;
const PGSQL_AF_INET6: u8 = 2 + 1;

/*
 *	Access macros, mirrored from inet.h / network.rs (private there).
 *	We use VARDATA_ANY so that we can process short-header varlena values
 *	without detoasting them.
 */

/* ip_family(inetptr) = ((inet_struct *) VARDATA_ANY(inetptr))->family */
#[inline]
unsafe fn ip_family(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).family
}

/* ip_bits(inetptr) = ((inet_struct *) VARDATA_ANY(inetptr))->bits */
#[inline]
unsafe fn ip_bits(inetptr: *const inet) -> u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *const inet_struct)).bits
}

/* ip_addr(inetptr) = ((inet_struct *) VARDATA_ANY(inetptr))->ipaddr (an array) */
#[inline]
unsafe fn ip_addr(inetptr: *const inet) -> *mut u8 {
    (*(VARDATA_ANY(inetptr as *const c_char) as *mut inet_struct))
        .ipaddr
        .as_mut_ptr()
}

/* ip_maxbits(inetptr) = (family == PGSQL_AF_INET ? 32 : 128) */
#[inline]
unsafe fn ip_maxbits(inetptr: *const inet) -> c_int {
    if ip_family(inetptr) == PGSQL_AF_INET {
        32
    } else {
        128
    }
}

/* DatumGetInetPP(X) / InetPGetDatum(X), mirrored from inet.h. */
#[inline]
unsafe fn DatumGetInetPP(X: Datum) -> *mut inet {
    pg_detoast_datum_packed(DatumGetPointer(X) as *mut c_void) as *mut inet
}
#[inline]
unsafe fn InetPGetDatum(X: *const inet) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/*
 * The SP-GiST configuration function
 */
#[no_mangle]
pub unsafe extern "C" fn inet_spg_config(fcinfo: FunctionCallInfo) -> Datum {
    /* spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0); */
    let cfg: *mut spgConfigOut = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = CIDROID;
    (*cfg).labelType = VOIDOID;
    (*cfg).canReturnData = true;
    (*cfg).longValuesOK = false;

    PG_RETURN_VOID!();
}

/*
 * The SP-GiST choose function
 */
#[no_mangle]
pub unsafe extern "C" fn inet_spg_choose(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut spgChooseIn = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgChooseIn;
    let out: *mut spgChooseOut = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgChooseOut;
    let val: *mut inet = DatumGetInetPP((*in_).datum);
    let prefix: *mut inet;
    let mut commonbits: c_int;

    /*
     * If we're looking at a tuple that splits by address family, choose the
     * appropriate subnode.
     */
    if !(*in_).hasPrefix {
        /* allTheSame isn't possible for such a tuple */
        Assert!(!(*in_).allTheSame);
        Assert!((*in_).nNodes == 2);

        (*out).resultType = spgMatchNode;
        (*out).result.matchNode.nodeN = if ip_family(val) == PGSQL_AF_INET { 0 } else { 1 };
        (*out).result.matchNode.restDatum = InetPGetDatum(val);

        PG_RETURN_VOID!();
    }

    /* Else it must split by prefix */
    Assert!((*in_).nNodes == 4 || (*in_).allTheSame);

    prefix = DatumGetInetPP((*in_).prefixDatum);
    commonbits = ip_bits(prefix) as c_int;

    /*
     * We cannot put addresses from different families under the same inner
     * node, so we have to split if the new value's family is different.
     */
    if ip_family(val) != ip_family(prefix) {
        /* Set up 2-node tuple */
        (*out).resultType = spgSplitTuple;
        (*out).result.splitTuple.prefixHasPrefix = false;
        (*out).result.splitTuple.prefixNNodes = 2;
        (*out).result.splitTuple.prefixNodeLabels = null_mut();

        /* Identify which node the existing data goes into */
        (*out).result.splitTuple.childNodeN =
            if ip_family(prefix) == PGSQL_AF_INET { 0 } else { 1 };

        (*out).result.splitTuple.postfixHasPrefix = true;
        (*out).result.splitTuple.postfixPrefixDatum = InetPGetDatum(prefix);

        PG_RETURN_VOID!();
    }

    /*
     * If the new value does not match the existing prefix, we have to split.
     */
    if (ip_bits(val) as c_int) < commonbits
        || bitncmp(ip_addr(prefix), ip_addr(val), commonbits) != 0
    {
        /* Determine new prefix length for the split tuple */
        commonbits = bitncommon(
            ip_addr(prefix),
            ip_addr(val),
            Min(ip_bits(val) as c_int, commonbits),
        );

        /* Set up 4-node tuple */
        (*out).resultType = spgSplitTuple;
        (*out).result.splitTuple.prefixHasPrefix = true;
        (*out).result.splitTuple.prefixPrefixDatum =
            InetPGetDatum(cidr_set_masklen_internal(val, commonbits));
        (*out).result.splitTuple.prefixNNodes = 4;
        (*out).result.splitTuple.prefixNodeLabels = null_mut();

        /* Identify which node the existing data goes into */
        (*out).result.splitTuple.childNodeN = inet_spg_node_number(prefix, commonbits);

        (*out).result.splitTuple.postfixHasPrefix = true;
        (*out).result.splitTuple.postfixPrefixDatum = InetPGetDatum(prefix);

        PG_RETURN_VOID!();
    }

    /*
     * All OK, choose the node to descend into.  (If this tuple is marked
     * allTheSame, the core code will ignore our choice of nodeN; but we need
     * not account for that case explicitly here.)
     */
    (*out).resultType = spgMatchNode;
    (*out).result.matchNode.nodeN = inet_spg_node_number(val, commonbits);
    (*out).result.matchNode.restDatum = InetPGetDatum(val);

    PG_RETURN_VOID!();
}

/*
 * The GiST PickSplit method
 */
#[no_mangle]
pub unsafe extern "C" fn inet_spg_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut spgPickSplitIn = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgPickSplitIn;
    let out: *mut spgPickSplitOut = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgPickSplitOut;
    let prefix: *mut inet;
    let mut tmp: *mut inet;
    let mut i: c_int;
    let mut commonbits: c_int;
    let mut differentFamilies: bool = false;

    /* Initialize the prefix with the first item */
    prefix = DatumGetInetPP(*(*in_).datums.offset(0));
    commonbits = ip_bits(prefix) as c_int;

    /* Examine remaining items to discover minimum common prefix length */
    i = 1;
    while i < (*in_).nTuples {
        tmp = DatumGetInetPP(*(*in_).datums.offset(i as isize));

        if ip_family(tmp) != ip_family(prefix) {
            differentFamilies = true;
            break;
        }

        if (ip_bits(tmp) as c_int) < commonbits {
            commonbits = ip_bits(tmp) as c_int;
        }
        commonbits = bitncommon(ip_addr(prefix), ip_addr(tmp), commonbits);
        if commonbits == 0 {
            break;
        }

        i += 1;
    }

    /* Don't need labels; allocate output arrays */
    (*out).nodeLabels = null_mut();
    (*out).mapTuplesToNodes =
        palloc(core::mem::size_of::<c_int>() * (*in_).nTuples as usize) as *mut c_int;
    (*out).leafTupleDatums =
        palloc(core::mem::size_of::<Datum>() * (*in_).nTuples as usize) as *mut Datum;

    if differentFamilies {
        /* Set up 2-node tuple */
        (*out).hasPrefix = false;
        (*out).nNodes = 2;

        i = 0;
        while i < (*in_).nTuples {
            tmp = DatumGetInetPP(*(*in_).datums.offset(i as isize));
            *(*out).mapTuplesToNodes.offset(i as isize) =
                if ip_family(tmp) == PGSQL_AF_INET { 0 } else { 1 };
            *(*out).leafTupleDatums.offset(i as isize) = InetPGetDatum(tmp);
            i += 1;
        }
    } else {
        /* Set up 4-node tuple */
        (*out).hasPrefix = true;
        (*out).prefixDatum = InetPGetDatum(cidr_set_masklen_internal(prefix, commonbits));
        (*out).nNodes = 4;

        i = 0;
        while i < (*in_).nTuples {
            tmp = DatumGetInetPP(*(*in_).datums.offset(i as isize));
            *(*out).mapTuplesToNodes.offset(i as isize) = inet_spg_node_number(tmp, commonbits);
            *(*out).leafTupleDatums.offset(i as isize) = InetPGetDatum(tmp);
            i += 1;
        }
    }

    PG_RETURN_VOID!();
}

/*
 * The SP-GiST query consistency check for inner tuples
 */
#[no_mangle]
pub unsafe extern "C" fn inet_spg_inner_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut spgInnerConsistentIn =
        PG_GETARG_POINTER!(fcinfo, 0) as *mut spgInnerConsistentIn;
    let out: *mut spgInnerConsistentOut =
        PG_GETARG_POINTER!(fcinfo, 1) as *mut spgInnerConsistentOut;
    let mut i: c_int;
    let which: c_int;

    if !(*in_).hasPrefix {
        Assert!(!(*in_).allTheSame);
        Assert!((*in_).nNodes == 2);

        /* Identify which child nodes need to be visited */
        let mut w: c_int = 1 | (1 << 1);

        i = 0;
        while i < (*in_).nkeys {
            let scankey: *mut _ = (*in_).scankeys.offset(i as isize);
            let strategy: StrategyNumber = (*scankey).sk_strategy;
            let argument: *mut inet = DatumGetInetPP((*scankey).sk_argument);

            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if ip_family(argument) == PGSQL_AF_INET {
                        w &= 1;
                    }
                }

                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if ip_family(argument) == PGSQL_AF_INET6 {
                        w &= 1 << 1;
                    }
                }

                RTNotEqualStrategyNumber => {}

                _ => {
                    /* all other ops can only match addrs of same family */
                    if ip_family(argument) == PGSQL_AF_INET {
                        w &= 1;
                    } else {
                        w &= 1 << 1;
                    }
                }
            }

            i += 1;
        }

        which = w;
    } else if !(*in_).allTheSame {
        Assert!((*in_).nNodes == 4);

        /* Identify which child nodes need to be visited */
        which = inet_spg_consistent_bitmap(
            DatumGetInetPP((*in_).prefixDatum),
            (*in_).nkeys,
            (*in_).scankeys,
            false,
        );
    } else {
        /* Must visit all nodes; we assume there are less than 32 of 'em */
        which = !0;
    }

    (*out).nNodes = 0;

    if which != 0 {
        (*out).nodeNumbers =
            palloc(core::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;

        i = 0;
        while i < (*in_).nNodes {
            if which & (1 << i) != 0 {
                *(*out).nodeNumbers.offset((*out).nNodes as isize) = i;
                (*out).nNodes += 1;
            }
            i += 1;
        }
    }

    PG_RETURN_VOID!();
}

/*
 * The SP-GiST query consistency check for leaf tuples
 */
#[no_mangle]
pub unsafe extern "C" fn inet_spg_leaf_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_: *mut spgLeafConsistentIn = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgLeafConsistentIn;
    let out: *mut spgLeafConsistentOut =
        PG_GETARG_POINTER!(fcinfo, 1) as *mut spgLeafConsistentOut;
    let leaf: *mut inet = DatumGetInetPP((*in_).leafDatum);

    /* All tests are exact. */
    (*out).recheck = false;

    /* Leaf is what it is... */
    (*out).leafValue = InetPGetDatum(leaf);

    /* Use common code to apply the tests. */
    PG_RETURN_BOOL!(
        inet_spg_consistent_bitmap(leaf, (*in_).nkeys, (*in_).scankeys, true) != 0
    );
}

/*
 * Calculate node number (within a 4-node, single-family inner index tuple)
 *
 * The value must have the same family as the node's prefix, and
 * commonbits is the mask length of the prefix.  We use even or odd
 * nodes according to the next address bit after the commonbits,
 * and low or high nodes according to whether the value's mask length
 * is larger than commonbits.
 */
unsafe fn inet_spg_node_number(val: *const inet, commonbits: c_int) -> c_int {
    let mut nodeN: c_int = 0;

    if commonbits < ip_maxbits(val)
        && (*ip_addr(val).offset((commonbits / 8) as isize) as c_int)
            & (1 << (7 - commonbits % 8))
            != 0
    {
        nodeN |= 1;
    }
    if commonbits < ip_bits(val) as c_int {
        nodeN |= 2;
    }

    return nodeN;
}

/*
 * Calculate bitmap of node numbers that are consistent with the query
 *
 * This can be used either at a 4-way inner tuple, or at a leaf tuple.
 * In the latter case, we should return a boolean result (0 or 1)
 * not a bitmap.
 *
 * This definition is pretty odd, but the inner and leaf consistency checks
 * are mostly common and it seems best to keep them in one function.
 */
unsafe fn inet_spg_consistent_bitmap(
    prefix: *const inet,
    nkeys: c_int,
    scankeys: ScanKey,
    leaf: bool,
) -> c_int {
    let mut bitmap: c_int;
    let commonbits: c_int;
    let mut i: c_int;

    /* Initialize result to allow visiting all children */
    if leaf {
        bitmap = 1;
    } else {
        bitmap = 1 | (1 << 1) | (1 << 2) | (1 << 3);
    }

    commonbits = ip_bits(prefix) as c_int;

    i = 0;
    while i < nkeys {
        let scankey: *mut _ = scankeys.offset(i as isize);
        let argument: *mut inet = DatumGetInetPP((*scankey).sk_argument);
        let strategy: StrategyNumber = (*scankey).sk_strategy;
        let mut order: c_int;

        /*
         * Check 0: different families
         *
         * Matching families do not help any of the strategies.
         */
        if ip_family(argument) != ip_family(prefix) {
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if ip_family(argument) < ip_family(prefix) {
                        bitmap = 0;
                    }
                }

                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if ip_family(argument) > ip_family(prefix) {
                        bitmap = 0;
                    }
                }

                RTNotEqualStrategyNumber => {}

                _ => {
                    /* For all other cases, we can be sure there is no match */
                    bitmap = 0;
                }
            }

            if bitmap == 0 {
                break;
            }

            /* Other checks make no sense with different families. */
            i += 1;
            continue;
        }

        /*
         * Check 1: network bit count
         *
         * Network bit count (ip_bits) helps to check leaves for sub network
         * and sup network operators.  At non-leaf nodes, we know every child
         * value has greater ip_bits, so we can avoid descending in some cases
         * too.
         *
         * This check is less expensive than checking the address bits, so we
         * are doing this before, but it has to be done after for the basic
         * comparison strategies, because ip_bits only affect their results
         * when the common network bits are the same.
         */
        match strategy {
            RTSubStrategyNumber => {
                if commonbits <= ip_bits(argument) as c_int {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }

            RTSubEqualStrategyNumber => {
                if commonbits < ip_bits(argument) as c_int {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }

            RTSuperStrategyNumber => {
                if commonbits == ip_bits(argument) as c_int - 1 {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits >= ip_bits(argument) as c_int {
                    bitmap = 0;
                }
            }

            RTSuperEqualStrategyNumber => {
                if commonbits == ip_bits(argument) as c_int {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits > ip_bits(argument) as c_int {
                    bitmap = 0;
                }
            }

            RTEqualStrategyNumber => {
                if commonbits < ip_bits(argument) as c_int {
                    bitmap &= (1 << 2) | (1 << 3);
                } else if commonbits == ip_bits(argument) as c_int {
                    bitmap &= 1 | (1 << 1);
                } else {
                    bitmap = 0;
                }
            }

            _ => {}
        }

        if bitmap == 0 {
            break;
        }

        /*
         * Check 2: common network bits
         *
         * Compare available common prefix bits to the query, but not beyond
         * either the query's netmask or the minimum netmask among the
         * represented values.  If these bits don't match the query, we can
         * eliminate some cases.
         */
        order = bitncmp(
            ip_addr(prefix),
            ip_addr(argument),
            Min(commonbits, ip_bits(argument) as c_int),
        );

        if order != 0 {
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if order > 0 {
                        bitmap = 0;
                    }
                }

                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if order < 0 {
                        bitmap = 0;
                    }
                }

                RTNotEqualStrategyNumber => {}

                _ => {
                    /* For all other cases, we can be sure there is no match */
                    bitmap = 0;
                }
            }

            if bitmap == 0 {
                break;
            }

            /*
             * Remaining checks make no sense when common bits don't match.
             */
            i += 1;
            continue;
        }

        /*
         * Check 3: next network bit
         *
         * We can filter out branch 2 or 3 using the next network bit of the
         * argument, if it is available.
         *
         * This check matters for the performance of the search. The results
         * would be correct without it.
         */
        if bitmap & ((1 << 2) | (1 << 3)) != 0 && commonbits < ip_bits(argument) as c_int {
            let nextbit: c_int;

            nextbit = (*ip_addr(argument).offset((commonbits / 8) as isize) as c_int)
                & (1 << (7 - commonbits % 8));

            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 1) | (1 << 2);
                    }
                }

                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if nextbit != 0 {
                        bitmap &= 1 | (1 << 1) | (1 << 3);
                    }
                }

                RTNotEqualStrategyNumber => {}

                _ => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 1) | (1 << 2);
                    } else {
                        bitmap &= 1 | (1 << 1) | (1 << 3);
                    }
                }
            }

            if bitmap == 0 {
                break;
            }
        }

        /*
         * Remaining checks are only for the basic comparison strategies. This
         * test relies on the strategy number ordering defined in stratnum.h.
         */
        if strategy < RTEqualStrategyNumber || strategy > RTGreaterEqualStrategyNumber {
            i += 1;
            continue;
        }

        /*
         * Check 4: network bit count
         *
         * At this point, we know that the common network bits of the prefix
         * and the argument are the same, so we can go forward and check the
         * ip_bits.
         */
        match strategy {
            RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                if commonbits == ip_bits(argument) as c_int {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits > ip_bits(argument) as c_int {
                    bitmap = 0;
                }
            }

            RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                if commonbits < ip_bits(argument) as c_int {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }

            _ => {}
        }

        if bitmap == 0 {
            break;
        }

        /* Remaining checks don't make sense with different ip_bits. */
        if commonbits != ip_bits(argument) as c_int {
            i += 1;
            continue;
        }

        /*
         * Check 5: next host bit
         *
         * We can filter out branch 0 or 1 using the next host bit of the
         * argument, if it is available.
         *
         * This check matters for the performance of the search. The results
         * would be correct without it.  There is no point in running it for
         * leafs as we have to check the whole address on the next step.
         */
        if !leaf
            && bitmap & (1 | (1 << 1)) != 0
            && commonbits < ip_maxbits(argument)
        {
            let nextbit: c_int;

            nextbit = (*ip_addr(argument).offset((commonbits / 8) as isize) as c_int)
                & (1 << (7 - commonbits % 8));

            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 2) | (1 << 3);
                    }
                }

                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if nextbit != 0 {
                        bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
                    }
                }

                RTNotEqualStrategyNumber => {}

                _ => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 2) | (1 << 3);
                    } else {
                        bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
                    }
                }
            }

            if bitmap == 0 {
                break;
            }
        }

        /*
         * Check 6: whole address
         *
         * This is the last check for correctness of the basic comparison
         * strategies.  It's only appropriate at leaf entries.
         */
        if leaf {
            /* Redo ordering comparison using all address bits */
            order = bitncmp(ip_addr(prefix), ip_addr(argument), ip_maxbits(prefix));

            match strategy {
                RTLessStrategyNumber => {
                    if order >= 0 {
                        bitmap = 0;
                    }
                }

                RTLessEqualStrategyNumber => {
                    if order > 0 {
                        bitmap = 0;
                    }
                }

                RTEqualStrategyNumber => {
                    if order != 0 {
                        bitmap = 0;
                    }
                }

                RTGreaterEqualStrategyNumber => {
                    if order < 0 {
                        bitmap = 0;
                    }
                }

                RTGreaterStrategyNumber => {
                    if order <= 0 {
                        bitmap = 0;
                    }
                }

                RTNotEqualStrategyNumber => {
                    if order == 0 {
                        bitmap = 0;
                    }
                }

                _ => {}
            }

            if bitmap == 0 {
                break;
            }
        }

        i += 1;
    }

    return bitmap;
}
